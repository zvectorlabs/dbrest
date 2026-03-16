# PgREST Benchmark Suite

Comprehensive benchmark suite for PgREST using 100% Rust-native tools.

## Overview

This benchmark suite consists of three tiers:

1. **Micro-benchmarks** (`micro_benchmarks.rs`) - Component-level performance testing
2. **Integration benchmarks** (`integration_benchmarks.rs`) - HTTP request/response performance
3. **Load testing** (`load_tester.rs`) - Concurrent HTTP load testing with statistical analysis

## Prerequisites

- Rust 1.91+
- PostgreSQL 17+ (for integration benchmarks and load tests)
- Running PgREST server on `localhost:3000` (for integration benchmarks and load tests)

## Running Benchmarks

### Micro-benchmarks

Micro-benchmarks test individual components and don't require a running server:

```bash
cargo bench --bench micro_benchmarks
```

This will benchmark:
- Query parameter parsing (simple, complex, filters, logic trees)
- JSON parsing and serialization
- HashMap lookups (simulating schema cache)
- String operations

Results are saved to `target/criterion/micro_benchmarks/` with HTML reports.

### Integration Benchmarks

Integration benchmarks require a running PgREST server with test data:

1. **Start PostgreSQL** (if not already running):
   ```bash
   # Using Docker
   docker run -d --name pgrest-bench \
     -e POSTGRES_PASSWORD=postgres \
     -p 5432:5432 \
     postgres:17
   ```

2. **Load schema and seed data**:
   ```bash
   psql -h localhost -U postgres -f tests/fixtures/schema.sql
   psql -h localhost -U postgres -f benches/fixtures/seed_data.sql
   ```

3. **Start PgREST server**:
   ```bash
   cargo run --release &
   # Wait for server to start
   sleep 2
   ```

4. **Run integration benchmarks**:
   ```bash
   cargo bench --bench integration_benchmarks
   ```

This will benchmark:
- Simple GET requests (all rows, filtered, single row)
- Embedded queries (depth 1, depth 2, multiple embeds)
- Mutations (POST, PATCH, DELETE)
- RPC calls (GET and POST)
- Streaming responses (PgREST-specific)
- Computed fields (PgREST-specific)

### Load Testing

Load tests also require a running server:

```bash
cargo bench --bench load_tester
```

This runs three scenarios:
- **Mixed workload** - 80% reads, 20% writes (10, 50, 100 workers)
- **Error scenarios** - Misspelled paths, non-existent resources
- **Streaming** - Large dataset responses

### Metrics Capture (req/s for comparison)

For benchmark comparison scripts that capture requests-per-second:

```bash
cargo bench --bench bench_metrics
```

Outputs JSON with throughput (req/s), latency (p50/p95/p99), and error rate. Use `./scripts/benchmark-compare.sh` for a full PgREST vs PostgREST comparison with req/s comparison table.

## Interpreting Results

### Criterion HTML Reports

Criterion generates detailed HTML reports in `target/criterion/`. Open `target/criterion/<benchmark_name>/report/index.html` in a browser to view:

- **Throughput** - Operations per second
- **Latency distributions** - Mean, median, min, max
- **Regression detection** - Automatically highlights performance regressions > 5%
- **Statistical significance** - Confidence intervals and t-tests

### Load Test Results

Load test benchmarks output statistics including:
- **Throughput** - Requests per second
- **Latency percentiles** - p50, p95, p99
- **Error rate** - Percentage of failed requests
- **Success rate** - Percentage of successful requests

## Performance Targets

| Metric | Target | PostgREST Baseline |
|--------|--------|-------------------|
| Simple GET p95 latency | < 20ms | ~15ms |
| Simple GET throughput | **> 20,000 req/s** | ~15,000 req/s |
| Embedded GET p95 latency | < 50ms | ~40ms |
| Single INSERT p95 latency | < 30ms | ~25ms |
| Bulk INSERT (10 rows) p95 | < 100ms | ~80ms |
| Error handling overhead | < 5ms | ~3ms |
| Streaming memory usage | < 200MB for 10k rows | N/A |
| Idle memory | < 30MB | ~20MB |

## Benchmark Data

The `benches/fixtures/seed_data.sql` file generates:
- 10,000 users
- 50,000 posts
- 200,000 comments
- Additional test data for relationships

This provides realistic data volumes for benchmarking.

## Troubleshooting

### "Connection refused" errors

Ensure PgREST is running on `localhost:3000`:
```bash
curl http://localhost:3000/
```

### "Table not found" errors

Ensure the schema and seed data are loaded:
```bash
psql -h localhost -U postgres -c "\dt test_api.*"
```

### Benchmark times out

Increase timeout in `integration_benchmarks.rs` or `load_tester.rs`:
```rust
.timeout(Duration::from_secs(60)) // Increase from 30
```

## CI Integration

Benchmarks can be integrated into CI/CD pipelines. See `.github/workflows/benchmark.yml` (to be created) for an example GitHub Actions workflow.

## Notes

- All benchmarks use **100% Rust-native tools** - no external dependencies (Vegeta, k6, etc.)
- Load tester provides **type-safe** request definitions
- Criterion automatically detects **performance regressions**
- Results are **statistically rigorous** with confidence intervals
