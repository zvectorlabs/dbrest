//! Standalone metrics runner for benchmark comparison.
//! Outputs JSON with req/s, latency percentiles, error rate for script parsing.
//! Run: cargo bench --bench bench_metrics

// Types required by load_test/scenarios (must be defined before mod load_test)
#[derive(Debug, Clone)]
pub enum RequestType {
    Get {
        path: String,
    },
    Post {
        path: String,
        body: serde_json::Value,
    },
    Patch {
        path: String,
        body: serde_json::Value,
    },
    Delete {
        path: String,
    },
}

#[derive(Debug, Clone)]
pub struct LoadTestScenario {
    pub name: String,
    pub requests: Vec<(f64, RequestType)>,
}

mod load_test;

use load_test::scenarios::{errors_scenario, mixed_scenario, streaming_scenario};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use reqwest::Client;
use serde::Serialize;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;

#[derive(Debug, Clone)]
struct LoadTestConfig {
    base_url: String,
    duration: Duration,
    workers: usize,
}

#[derive(Debug, Clone)]
struct RequestResult {
    success: bool,
    latency_ms: f64,
}

#[derive(Debug, Serialize)]
struct LoadTestResult {
    scenario: String,
    duration_secs: f64,
    total_requests: u64,
    successful_requests: u64,
    failed_requests: u64,
    throughput_req_per_sec: f64,
    latency_p50_ms: f64,
    latency_p95_ms: f64,
    latency_p99_ms: f64,
    error_rate: f64,
}

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
        RequestType::Delete { path } => client.delete(format!("{}{}", base_url, path)).send().await,
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
    &requests.last().unwrap().1
}

async fn run_worker(
    client: Client,
    config: LoadTestConfig,
    scenario: LoadTestScenario,
    results_tx: mpsc::UnboundedSender<RequestResult>,
) {
    let mut rng = StdRng::from_entropy();
    let start = Instant::now();

    while start.elapsed() < config.duration {
        let request = select_weighted_request(&scenario.requests, &mut rng);
        let request_start = Instant::now();
        let result = execute_request(&client, &config.base_url, request).await;
        let latency = request_start.elapsed();

        results_tx
            .send(RequestResult {
                success: result.is_ok(),
                latency_ms: latency.as_secs_f64() * 1000.0,
            })
            .unwrap();
    }
}

fn percentile(sorted_data: &[f64], p: f64) -> f64 {
    if sorted_data.is_empty() {
        return 0.0;
    }
    let index = (sorted_data.len() as f64 * p).ceil() as usize - 1;
    sorted_data[index.min(sorted_data.len() - 1)]
}

async fn run_load_test(config: LoadTestConfig, scenario: LoadTestScenario) -> LoadTestResult {
    let (results_tx, mut results_rx) = mpsc::unbounded_channel::<RequestResult>();

    let client = Client::builder()
        .timeout(Duration::from_secs(30))
        .build()
        .expect("Failed to create HTTP client");

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

    drop(results_tx);
    let start = Instant::now();
    let mut results = Vec::new();

    while let Some(result) = results_rx.recv().await {
        results.push(result);
    }

    for handle in handles {
        handle.await.unwrap();
    }

    let duration = start.elapsed();
    let total = results.len() as u64;
    let successful = results.iter().filter(|r| r.success).count() as u64;
    let failed = total - successful;

    let mut latencies: Vec<f64> = results.iter().map(|r| r.latency_ms).collect();
    latencies.sort_by(|a, b| a.partial_cmp(b).unwrap());

    LoadTestResult {
        scenario: scenario.name.clone(),
        duration_secs: duration.as_secs_f64(),
        total_requests: total,
        successful_requests: successful,
        failed_requests: failed,
        throughput_req_per_sec: total as f64 / duration.as_secs_f64(),
        latency_p50_ms: percentile(&latencies, 0.50),
        latency_p95_ms: percentile(&latencies, 0.95),
        latency_p99_ms: percentile(&latencies, 0.99),
        error_rate: if total > 0 {
            failed as f64 / total as f64
        } else {
            0.0
        },
    }
}

fn main() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let base_url =
        std::env::var("DBREST_BENCH_URL").unwrap_or_else(|_| "http://localhost:3000".to_string());

    let scenarios_and_configs: Vec<(LoadTestScenario, LoadTestConfig)> = vec![
        (
            {
                let mut s = mixed_scenario();
                s.name = "mixed_10".to_string();
                s
            },
            LoadTestConfig {
                base_url: base_url.clone(),
                duration: Duration::from_secs(10),
                workers: 10,
            },
        ),
        (
            {
                let mut s = mixed_scenario();
                s.name = "mixed_50".to_string();
                s
            },
            LoadTestConfig {
                base_url: base_url.clone(),
                duration: Duration::from_secs(10),
                workers: 50,
            },
        ),
        (
            {
                let mut s = mixed_scenario();
                s.name = "mixed_100".to_string();
                s
            },
            LoadTestConfig {
                base_url: base_url.clone(),
                duration: Duration::from_secs(10),
                workers: 100,
            },
        ),
        (
            errors_scenario(),
            LoadTestConfig {
                base_url: base_url.clone(),
                duration: Duration::from_secs(10),
                workers: 50,
            },
        ),
        (
            streaming_scenario(),
            LoadTestConfig {
                base_url: base_url.clone(),
                duration: Duration::from_secs(10),
                workers: 20,
            },
        ),
    ];

    let mut results = Vec::new();
    for (scenario, config) in scenarios_and_configs {
        let r = rt.block_on(run_load_test(config, scenario));
        results.push(r);
    }

    println!(
        "DBREST_BENCH_JSON:{}",
        serde_json::to_string(&results).unwrap()
    );
}
