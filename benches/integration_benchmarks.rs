//! Integration benchmarks - HTTP request/response performance
//!
//! These benchmarks require a running dbrest server on localhost:3000
//! with the test database schema loaded and seeded with benchmark data.

use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use reqwest::Client;
use serde_json::json;
use std::time::Duration;

const BASE_URL: &str = "http://localhost:3000";

// Helper to create HTTP client
fn create_client() -> Client {
    Client::builder()
        .timeout(Duration::from_secs(30))
        .build()
        .expect("Failed to create HTTP client")
}

// ============================================================================
// Simple GET Requests
// ============================================================================

fn bench_simple_get(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let client = create_client();

    let mut group = c.benchmark_group("simple_get");
    group.throughput(Throughput::Elements(1));

    // Simple GET all
    group.bench_function("get_all_users", |b| {
        b.to_async(&rt).iter(|| async {
            client
                .get(format!("{}/users", BASE_URL))
                .send()
                .await
                .unwrap()
        });
    });

    // GET with limit
    group.bench_function("get_users_limit_10", |b| {
        b.to_async(&rt).iter(|| async {
            client
                .get(format!("{}/users?limit=10", BASE_URL))
                .send()
                .await
                .unwrap()
        });
    });

    // GET single row by ID
    group.bench_function("get_user_by_id", |b| {
        b.to_async(&rt).iter(|| async {
            client
                .get(format!("{}/users?id=eq.1", BASE_URL))
                .send()
                .await
                .unwrap()
        });
    });

    // GET with multiple filters
    group.bench_function("get_filtered", |b| {
        b.to_async(&rt).iter(|| async {
            client
                .get(format!("{}/users?status=eq.active&limit=20", BASE_URL))
                .send()
                .await
                .unwrap()
        });
    });

    group.finish();
}

// ============================================================================
// Embedded Queries
// ============================================================================

fn bench_embedded_get(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let client = create_client();

    let mut group = c.benchmark_group("embedded_get");

    // Depth 1 embedding
    group.bench_function("depth_1", |b| {
        b.to_async(&rt).iter(|| async {
            client
                .get(format!(
                    "{}/users?select=id,name,posts(*)&limit=10",
                    BASE_URL
                ))
                .send()
                .await
                .unwrap()
        });
    });

    // Depth 2 embedding
    group.bench_function("depth_2", |b| {
        b.to_async(&rt).iter(|| async {
            client
                .get(format!(
                    "{}/users?select=id,name,posts(id,title,comments(*))&limit=5",
                    BASE_URL
                ))
                .send()
                .await
                .unwrap()
        });
    });

    // Multiple embeddings same level
    group.bench_function("multiple_embeddings", |b| {
        b.to_async(&rt).iter(|| async {
            client
                .get(format!(
                    "{}/users?select=id,name,posts(*),profiles(*)&limit=5",
                    BASE_URL
                ))
                .send()
                .await
                .unwrap()
        });
    });

    group.finish();
}

// ============================================================================
// Mutations
// ============================================================================

fn bench_mutations(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let client = create_client();

    let mut group = c.benchmark_group("mutations");

    // Single row insert
    group.bench_function("insert_single", |b| {
        let body = json!({
            "name": "Test User",
            "email": format!("test{}@example.com", rand::random::<u64>())
        });

        b.to_async(&rt).iter(|| async {
            client
                .post(format!("{}/users", BASE_URL))
                .json(&body)
                .header("Prefer", "return=minimal")
                .send()
                .await
                .unwrap()
        });
    });

    // Bulk insert (10 rows)
    group.bench_function("insert_bulk_10", |b| {
        let body: Vec<_> = (0..10)
            .map(|i| {
                json!({
                    "name": format!("User {}", i),
                    "email": format!("user{}@example.com", rand::random::<u64>())
                })
            })
            .collect();

        b.to_async(&rt).iter(|| async {
            client
                .post(format!("{}/users", BASE_URL))
                .json(&body)
                .header("Prefer", "return=minimal")
                .send()
                .await
                .unwrap()
        });
    });

    // Single row update
    group.bench_function("update_single", |b| {
        let body = json!({ "name": "Updated Name" });

        b.to_async(&rt).iter(|| async {
            client
                .patch(format!("{}/users?id=eq.1", BASE_URL))
                .json(&body)
                .header("Prefer", "return=minimal")
                .send()
                .await
                .unwrap()
        });
    });

    group.finish();
}

// ============================================================================
// RPC Calls
// ============================================================================

fn bench_rpc_calls(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let client = create_client();

    let mut group = c.benchmark_group("rpc_calls");

    // RPC GET
    group.bench_function("rpc_get", |b| {
        b.to_async(&rt).iter(|| async {
            client
                .get(format!("{}/rpc/get_active_users", BASE_URL))
                .send()
                .await
                .unwrap()
        });
    });

    // RPC POST
    group.bench_function("rpc_post", |b| {
        let body = json!({ "name": "John" });

        b.to_async(&rt).iter(|| async {
            client
                .post(format!("{}/rpc/call_me", BASE_URL))
                .json(&body)
                .send()
                .await
                .unwrap()
        });
    });

    group.finish();
}

// ============================================================================
// Streaming (dbrest-Specific)
// ============================================================================

fn bench_streaming(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let client = create_client();

    let mut group = c.benchmark_group("streaming");

    // Large response that should trigger streaming
    group.bench_function("streaming_large", |b| {
        b.to_async(&rt).iter(|| async {
            let resp = client
                .get(format!("{}/users?limit=1000", BASE_URL))
                .send()
                .await
                .unwrap();
            // Consume the stream
            resp.text().await.unwrap()
        });
    });

    // Small response that should NOT stream
    group.bench_function("no_streaming_small", |b| {
        b.to_async(&rt).iter(|| async {
            let resp = client
                .get(format!("{}/users?limit=10", BASE_URL))
                .send()
                .await
                .unwrap();
            resp.text().await.unwrap()
        });
    });

    group.finish();
}

// ============================================================================
// Computed Fields (dbrest-Specific)
// ============================================================================

fn bench_computed_fields(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let client = create_client();

    let mut group = c.benchmark_group("computed_fields");

    // Computed field in select
    group.bench_function("computed_in_select", |b| {
        b.to_async(&rt).iter(|| async {
            client
                .get(format!(
                    "{}/users?select=id,name,full_name&limit=10",
                    BASE_URL
                ))
                .send()
                .await
                .unwrap()
        });
    });

    // Computed field in filter
    group.bench_function("computed_in_filter", |b| {
        b.to_async(&rt).iter(|| async {
            client
                .get(format!(
                    "{}/users?full_name=like.*@example.com*&limit=10",
                    BASE_URL
                ))
                .send()
                .await
                .unwrap()
        });
    });

    // Computed field in order
    group.bench_function("computed_in_order", |b| {
        b.to_async(&rt).iter(|| async {
            client
                .get(format!(
                    "{}/users?select=name,full_name&order=full_name.asc&limit=10",
                    BASE_URL
                ))
                .send()
                .await
                .unwrap()
        });
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_simple_get,
    bench_embedded_get,
    bench_mutations,
    bench_rpc_calls,
    bench_streaming,
    bench_computed_fields
);
criterion_main!(benches);
