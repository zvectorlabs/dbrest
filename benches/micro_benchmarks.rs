use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use pgrest::api_request::query_params;
use std::collections::HashMap;

// ============================================================================
// Query Parameter Parsing Benchmarks
// ============================================================================

fn bench_query_parsing(c: &mut Criterion) {
    let mut group = c.benchmark_group("query_params");

    // Simple select
    group.bench_function("simple_select", |b| {
        b.iter(|| {
            query_params::parse(
                black_box(false),
                black_box("select=id,name,email&order=created_at.desc"),
            )
        });
    });

    // Complex select with embeddings
    group.bench_function("complex_select", |b| {
        b.iter(|| {
            query_params::parse(
                black_box(false),
                black_box("select=id,name,posts(id,title,comments(id,body,author:users(name)))&order=name.asc"),
            )
        });
    });

    // Multiple filters
    group.bench_function("multiple_filters", |b| {
        b.iter(|| {
            query_params::parse(
                black_box(false),
                black_box("select=*&id=gt.10&name=ilike.*test*&status=in.(active,pending)&created_at=gte.2024-01-01"),
            )
        });
    });

    // Logic tree (and/or)
    group.bench_function("logic_tree", |b| {
        b.iter(|| {
            query_params::parse(
                black_box(false),
                black_box("select=*&or=(status.eq.active,and(created_at.gte.2024-01-01,created_at.lte.2024-12-31))"),
            )
        });
    });

    // Scaling: increasing number of select fields
    for n in [5, 10, 20, 50].iter() {
        let fields: String = (0..*n)
            .map(|i| format!("field{}", i))
            .collect::<Vec<_>>()
            .join(",");
        let query = format!("select={}", fields);

        group.bench_with_input(
            BenchmarkId::new("select_fields", n),
            &query,
            |b, q| {
                b.iter(|| query_params::parse(black_box(false), black_box(q)));
            },
        );
    }

    group.finish();
}

// ============================================================================
// JSON Parsing Benchmarks
// ============================================================================

fn bench_json_parsing(c: &mut Criterion) {
    let mut group = c.benchmark_group("json_parsing");

    // Single object
    let single_obj = r#"{"id": 1, "name": "Test User", "email": "test@example.com"}"#;
    group.throughput(Throughput::Bytes(single_obj.len() as u64));
    group.bench_function("single_object", |b| {
        b.iter(|| {
            serde_json::from_str::<serde_json::Value>(black_box(single_obj))
        });
    });

    // Array of objects (typical bulk insert)
    let array_10 = generate_json_array(10);
    let array_100 = generate_json_array(100);
    let array_1000 = generate_json_array(1000);

    group.throughput(Throughput::Bytes(array_10.len() as u64));
    group.bench_function("array_10_objects", |b| {
        b.iter(|| serde_json::from_str::<serde_json::Value>(black_box(&array_10)));
    });

    group.throughput(Throughput::Bytes(array_100.len() as u64));
    group.bench_function("array_100_objects", |b| {
        b.iter(|| serde_json::from_str::<serde_json::Value>(black_box(&array_100)));
    });

    group.throughput(Throughput::Bytes(array_1000.len() as u64));
    group.bench_function("array_1000_objects", |b| {
        b.iter(|| serde_json::from_str::<serde_json::Value>(black_box(&array_1000)));
    });

    group.finish();
}

fn generate_json_array(count: usize) -> String {
    let objects: Vec<String> = (0..count)
        .map(|i| {
            format!(
                r#"{{"id": {}, "name": "User {}", "email": "user{}@example.com", "active": true}}"#,
                i, i, i
            )
        })
        .collect();
    format!("[{}]", objects.join(","))
}

// ============================================================================
// JSON Serialization Benchmarks
// ============================================================================

fn bench_json_serialization(c: &mut Criterion) {
    let mut group = c.benchmark_group("json_serialization");

    // Single object
    let single_obj: serde_json::Value = serde_json::json!({
        "id": 1,
        "name": "Test User",
        "email": "test@example.com"
    });
    group.bench_function("single_object", |b| {
        b.iter(|| serde_json::to_string(black_box(&single_obj)));
    });

    // Array of objects
    let array_100: Vec<serde_json::Value> = (0..100)
        .map(|i| {
            serde_json::json!({
                "id": i,
                "name": format!("User {}", i),
                "email": format!("user{}@example.com", i),
                "active": true
            })
        })
        .collect();

    group.bench_function("array_100_objects", |b| {
        b.iter(|| serde_json::to_string(black_box(&array_100)));
    });

    // Nested structure (embedded query response)
    let nested: serde_json::Value = serde_json::json!({
        "id": 1,
        "name": "User 1",
        "posts": [
            {
                "id": 1,
                "title": "Post 1",
                "comments": [
                    {"id": 1, "body": "Comment 1"},
                    {"id": 2, "body": "Comment 2"}
                ]
            }
        ]
    });

    group.bench_function("nested_structure", |b| {
        b.iter(|| serde_json::to_string(black_box(&nested)));
    });

    group.finish();
}

// ============================================================================
// HashMap Lookup Benchmarks (simulating schema cache lookups)
// ============================================================================

fn bench_hashmap_lookups(c: &mut Criterion) {
    let mut group = c.benchmark_group("hashmap_lookups");

    // Create mock schema cache with 100 tables
    let mut cache: HashMap<String, usize> = HashMap::new();
    for i in 0..100 {
        cache.insert(format!("table_{}", i), i);
    }

    // Table lookup
    let key = "table_50".to_string();
    group.bench_function("table_lookup", |b| {
        b.iter(|| cache.get(black_box(&key)));
    });

    // Non-existent key lookup
    let missing_key = "table_nonexistent".to_string();
    group.bench_function("missing_key_lookup", |b| {
        b.iter(|| cache.get(black_box(&missing_key)));
    });

    group.finish();
}

// ============================================================================
// String Operations Benchmarks
// ============================================================================

fn bench_string_operations(c: &mut Criterion) {
    let mut group = c.benchmark_group("string_operations");

    // String concatenation (for SQL building)
    group.bench_function("string_concat_10", |b| {
        b.iter(|| {
            let mut s = String::new();
            for i in 0..10 {
                s.push_str(&format!("field{},", i));
            }
            black_box(s)
        });
    });

    // String formatting (for SQL building)
    group.bench_function("string_format", |b| {
        b.iter(|| {
            format!(
                "SELECT {} FROM {} WHERE {} = {}",
                black_box("id,name"),
                black_box("users"),
                black_box("id"),
                black_box("1")
            )
        });
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_query_parsing,
    bench_json_parsing,
    bench_json_serialization,
    bench_hashmap_lookups,
    bench_string_operations
);
criterion_main!(benches);
