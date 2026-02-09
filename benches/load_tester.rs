//! Rust-native load tester for PgREST
//!
//! Custom load testing implementation using tokio + reqwest for concurrent
//! HTTP load testing. Provides type-safe request definitions, configurable
//! concurrency, rate limiting, and statistical analysis.

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use reqwest::Client;
use serde::Serialize;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tokio::time::sleep;

// ============================================================================
// Core Types
// ============================================================================

#[derive(Debug, Clone)]
pub struct LoadTestConfig {
    pub base_url: String,
    pub duration: Duration,
    pub workers: usize,
    pub rate_per_worker: Option<u64>, // req/s per worker, None = unlimited
}

#[derive(Debug, Clone)]
pub enum RequestType {
    Get { path: String },
    Post { path: String, body: serde_json::Value },
    Patch { path: String, body: serde_json::Value },
    Delete { path: String },
}

#[derive(Debug, Clone)]
pub struct LoadTestScenario {
    pub name: String,
    pub requests: Vec<(f64, RequestType)>, // (weight, request) - weights should sum to ~1.0
}

mod load_test;

pub use load_test::scenarios::{errors_scenario, mixed_scenario, streaming_scenario};

#[derive(Debug, Clone)]
struct RequestResult {
    success: bool,
    latency_ms: f64,
    status_code: u16,
}

#[derive(Debug, Serialize)]
pub struct LoadTestResult {
    pub scenario: String,
    pub duration_secs: f64,
    pub total_requests: u64,
    pub successful_requests: u64,
    pub failed_requests: u64,
    pub throughput_req_per_sec: f64,
    pub latency_p50_ms: f64,
    pub latency_p95_ms: f64,
    pub latency_p99_ms: f64,
    pub latency_mean_ms: f64,
    pub error_rate: f64,
}

// ============================================================================
// Request Execution
// ============================================================================

async fn execute_request(
    client: &Client,
    base_url: &str,
    request: &RequestType,
) -> Result<reqwest::Response, reqwest::Error> {
    match request {
        RequestType::Get { path } => client.get(format!("{}{}", base_url, path)).send().await,
        RequestType::Post { path, body } => {
            client
                .post(format!("{}{}", base_url, path))
                .json(body)
                .send()
                .await
        }
        RequestType::Patch { path, body } => {
            client
                .patch(format!("{}{}", base_url, path))
                .json(body)
                .send()
                .await
        }
        RequestType::Delete { path } => {
            client
                .delete(format!("{}{}", base_url, path))
                .send()
                .await
        }
    }
}

fn select_weighted_request<'a>(
    requests: &'a [(f64, RequestType)],
    rng: &mut StdRng,
) -> &'a RequestType {
    let mut r: f64 = rng.gen_range(0.0..1.0);
    for (weight, request) in requests {
        r -= weight;
        if r <= 0.0 {
            return request;
        }
    }
    // Fallback to last request if rounding errors
    &requests.last().unwrap().1
}

// ============================================================================
// Worker Implementation
// ============================================================================

async fn run_worker(
    client: Client,
    config: LoadTestConfig,
    scenario: LoadTestScenario,
    results_tx: mpsc::UnboundedSender<RequestResult>,
) {
    let mut rng = StdRng::from_entropy();
    let start = Instant::now();

    while start.elapsed() < config.duration {
        // Select request based on weights
        let request = select_weighted_request(&scenario.requests, &mut rng);

        // Rate limiting
        if let Some(rate) = config.rate_per_worker {
            sleep(Duration::from_secs_f64(1.0 / rate as f64)).await;
        }

        // Execute request
        let request_start = Instant::now();
        let result = execute_request(&client, &config.base_url, request).await;
        let latency = request_start.elapsed();

        let success = result.is_ok();
        let status_code = result.as_ref().map(|r| r.status().as_u16()).unwrap_or(0);

        results_tx
            .send(RequestResult {
                success,
                latency_ms: latency.as_secs_f64() * 1000.0,
                status_code,
            })
            .unwrap();
    }
}

// ============================================================================
// Statistics Calculation
// ============================================================================

fn calculate_statistics(results: &[RequestResult]) -> LoadTestResult {
    let total = results.len() as u64;
    let successful = results.iter().filter(|r| r.success).count() as u64;
    let failed = total - successful;

    let mut latencies: Vec<f64> = results.iter().map(|r| r.latency_ms).collect();
    latencies.sort_by(|a, b| a.partial_cmp(b).unwrap());

    let mean = if !latencies.is_empty() {
        latencies.iter().sum::<f64>() / latencies.len() as f64
    } else {
        0.0
    };

    let p50 = percentile(&latencies, 0.50);
    let p95 = percentile(&latencies, 0.95);
    let p99 = percentile(&latencies, 0.99);

    LoadTestResult {
        scenario: String::new(), // Will be set by caller
        duration_secs: 0.0,      // Will be set by caller
        total_requests: total,
        successful_requests: successful,
        failed_requests: failed,
        throughput_req_per_sec: 0.0, // Will be set by caller
        latency_p50_ms: p50,
        latency_p95_ms: p95,
        latency_p99_ms: p99,
        latency_mean_ms: mean,
        error_rate: if total > 0 {
            failed as f64 / total as f64
        } else {
            0.0
        },
    }
}

fn percentile(sorted_data: &[f64], p: f64) -> f64 {
    if sorted_data.is_empty() {
        return 0.0;
    }
    let index = (sorted_data.len() as f64 * p).ceil() as usize - 1;
    sorted_data[index.min(sorted_data.len() - 1)]
}

// ============================================================================
// Load Test Runner
// ============================================================================

async fn run_load_test(
    config: LoadTestConfig,
    scenario: LoadTestScenario,
) -> LoadTestResult {
    let (results_tx, mut results_rx) = mpsc::unbounded_channel::<RequestResult>();

    let client = Client::builder()
        .timeout(Duration::from_secs(30))
        .build()
        .expect("Failed to create HTTP client");

    // Spawn workers
    let mut handles = Vec::new();
    for _ in 0..config.workers {
        let client = client.clone();
        let config = config.clone();
        let scenario = scenario.clone();
        let results_tx = results_tx.clone();

        handles.push(tokio::spawn(run_worker(
            client, config, scenario, results_tx,
        )));
    }

    // Collect results
    drop(results_tx); // Close sender so receiver knows when to stop

    let start = Instant::now();
    let mut results = Vec::new();

    // Collect results until all workers finish
    while let Some(result) = results_rx.recv().await {
        results.push(result);
    }

    // Wait for all workers to finish
    for handle in handles {
        handle.await.unwrap();
    }

    let duration = start.elapsed();
    let mut stats = calculate_statistics(&results);
    stats.scenario = scenario.name.clone();
    stats.duration_secs = duration.as_secs_f64();
    stats.throughput_req_per_sec = stats.total_requests as f64 / stats.duration_secs;

    stats
}

// ============================================================================
// Criterion Benchmarks
// ============================================================================

fn bench_load_test_mixed(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let scenario = mixed_scenario();

    for workers in [10, 50, 100].iter() {
        let config = LoadTestConfig {
            base_url: "http://localhost:3000".to_string(),
            duration: Duration::from_secs(10), // Shorter for benchmarks
            workers: *workers,
            rate_per_worker: None, // Unlimited rate
        };

        // Warmup run to get actual request count for throughput reporting
        let warmup = rt
            .block_on(run_load_test(config.clone(), scenario.clone()));

        let mut group = c.benchmark_group("load_test_mixed");
        group.throughput(Throughput::Elements(warmup.total_requests));

        group.bench_with_input(
            BenchmarkId::new("workers", workers),
            &config,
            |b, cfg| {
                b.to_async(&rt).iter(|| async {
                    run_load_test(cfg.clone(), scenario.clone()).await
                });
            },
        );
        group.finish();
    }
}

fn bench_load_test_errors(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let scenario = errors_scenario();

    let config = LoadTestConfig {
        base_url: "http://localhost:3000".to_string(),
        duration: Duration::from_secs(10),
        workers: 50,
        rate_per_worker: None,
    };

    // Warmup run to get actual request count for throughput reporting
    let warmup = rt.block_on(run_load_test(config.clone(), scenario.clone()));

    let mut group = c.benchmark_group("load_test_errors");
    group.throughput(Throughput::Elements(warmup.total_requests));

    group.bench_function("error_scenarios", |b| {
        b.to_async(&rt).iter(|| async {
            run_load_test(config.clone(), scenario.clone()).await
        });
    });

    group.finish();
}

fn bench_load_test_streaming(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let scenario = streaming_scenario();

    let config = LoadTestConfig {
        base_url: "http://localhost:3000".to_string(),
        duration: Duration::from_secs(10),
        workers: 20,
        rate_per_worker: None,
    };

    // Warmup run to get actual request count for throughput reporting
    let warmup = rt.block_on(run_load_test(config.clone(), scenario.clone()));

    let mut group = c.benchmark_group("load_test_streaming");
    group.throughput(Throughput::Elements(warmup.total_requests));

    group.bench_function("streaming_large", |b| {
        b.to_async(&rt).iter(|| async {
            run_load_test(config.clone(), scenario.clone()).await
        });
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_load_test_mixed,
    bench_load_test_errors,
    bench_load_test_streaming
);
criterion_main!(benches);
