#!/usr/bin/env bash
# PgREST vs PostgREST benchmark comparison
# Run from pgrest/ directory: ./scripts/benchmark-compare.sh
#
# Prerequisites: Docker, Rust (cargo bench), jq (for JSON parsing), .env.bench

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
ENV_FILE="${PROJECT_DIR}/.env.bench"
RESULTS_DIR="${PROJECT_DIR}/bench-results"

cd "$PROJECT_DIR"

if [[ ! -f "$ENV_FILE" ]]; then
    echo "Error: .env.bench not found. Create it with resource limits (see plan)."
    exit 1
fi

if ! command -v jq &>/dev/null; then
    echo "Error: jq is required for parsing metrics. Install with: brew install jq"
    exit 1
fi

echo "=== PgREST vs PostgREST Benchmark Comparison ==="
echo "Project dir: $PROJECT_DIR"
echo ""

# Ensure no existing containers on port 3000
if curl -s -o /dev/null -w "%{http_code}" http://localhost:3000/ 2>/dev/null | grep -q 200; then
    echo "Warning: Port 3000 already in use. Stop any running services first."
    exit 1
fi

mkdir -p "$RESULTS_DIR"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
PGREST_JSON="${RESULTS_DIR}/pgrest_metrics_${TIMESTAMP}.json"
POSTGREST_JSON="${RESULTS_DIR}/postgrest_metrics_${TIMESTAMP}.json"
COMPARISON_FILE="${RESULTS_DIR}/comparison_${TIMESTAMP}.md"

# --- Run PgREST benchmarks ---
echo ">>> 1. Starting PgREST stack..."
docker compose -f docker-compose.bench.yml --env-file "$ENV_FILE" up -d --build

echo ">>> Waiting for PgREST to be healthy..."
TIMEOUT=60
while [ $TIMEOUT -gt 0 ]; do
    if curl -s -f http://localhost:3000/ > /dev/null 2>&1; then
        echo "    PgREST is ready."
        break
    fi
    sleep 2
    TIMEOUT=$((TIMEOUT - 2))
done
if [ $TIMEOUT -le 0 ]; then
    echo "Error: PgREST failed to become healthy"
    docker compose -f docker-compose.bench.yml --env-file "$ENV_FILE" logs
    docker compose -f docker-compose.bench.yml --env-file "$ENV_FILE" down
    exit 1
fi

echo ">>> 2. Running load benchmarks against PgREST (capturing req/s metrics)..."
cargo bench --bench bench_metrics 2>&1 | tee "$RESULTS_DIR/pgrest_${TIMESTAMP}.log"
grep -o 'PGREST_BENCH_JSON:.*' "$RESULTS_DIR/pgrest_${TIMESTAMP}.log" 2>/dev/null | tail -1 | sed 's/PGREST_BENCH_JSON://' > "$PGREST_JSON" || true

echo ">>> 3. Stopping PgREST stack..."
docker compose -f docker-compose.bench.yml --env-file "$ENV_FILE" down

echo ""
echo ">>> Waiting 30s for cooldown (avoid thermal throttling)..."
sleep 30
echo ""

# --- Run PostgREST benchmarks ---
echo ">>> 4. Starting PostgREST stack..."
docker compose -f docker-compose.bench-postgrest.yml --env-file "$ENV_FILE" up -d

echo ">>> Waiting for PostgREST to be healthy..."
TIMEOUT=60
while [ $TIMEOUT -gt 0 ]; do
    if curl -s -f http://localhost:3000/ > /dev/null 2>&1; then
        echo "    PostgREST is ready."
        break
    fi
    sleep 2
    TIMEOUT=$((TIMEOUT - 2))
done
if [ $TIMEOUT -le 0 ]; then
    echo "Error: PostgREST failed to become healthy"
    docker compose -f docker-compose.bench-postgrest.yml --env-file "$ENV_FILE" logs
    docker compose -f docker-compose.bench-postgrest.yml --env-file "$ENV_FILE" down
    exit 1
fi

echo ">>> 5. Running load benchmarks against PostgREST (capturing req/s metrics)..."
cargo bench --bench bench_metrics 2>&1 | tee "$RESULTS_DIR/postgrest_${TIMESTAMP}.log"
grep -o 'PGREST_BENCH_JSON:.*' "$RESULTS_DIR/postgrest_${TIMESTAMP}.log" 2>/dev/null | tail -1 | sed 's/PGREST_BENCH_JSON://' > "$POSTGREST_JSON" || true

echo ">>> 6. Stopping PostgREST stack..."
docker compose -f docker-compose.bench-postgrest.yml --env-file "$ENV_FILE" down

# --- Build comparison table ---
echo ""
echo ">>> 7. Building comparison table..."

{
    echo "# PgREST vs PostgREST Benchmark Comparison"
    echo ""
    echo "**Date:** $(date '+%Y-%m-%d %H:%M:%S')"
    echo ""
    echo "## Requests per Second (req/s)"
    echo ""
    echo "| Scenario | PgREST (req/s) | PostgREST (req/s) |"
    echo "|----------|----------------|-------------------|"

    if [[ -s "$PGREST_JSON" && -s "$POSTGREST_JSON" ]]; then
        for i in 0 1 2 3 4; do
            scenario=$(jq -r ".[$i].scenario" "$PGREST_JSON" 2>/dev/null)
            pgrest_rps=$(jq -r ".[$i].throughput_req_per_sec" "$PGREST_JSON" 2>/dev/null)
            postgrest_rps=$(jq -r ".[$i].throughput_req_per_sec" "$POSTGREST_JSON" 2>/dev/null)
            [[ "$scenario" != "null" && -n "$scenario" ]] && echo "| $scenario | $pgrest_rps | $postgrest_rps |"
        done
    else
        echo "| (metrics not captured - check logs) | - | - |"
    fi

    echo ""
    echo "## Latency (ms) - p50 / p95 / p99"
    echo ""
    echo "| Scenario | PgREST (p50/p95/p99) | PostgREST (p50/p95/p99) |"
    echo "|----------|----------------------|-------------------------|"

    if [[ -s "$PGREST_JSON" && -s "$POSTGREST_JSON" ]]; then
        for i in 0 1 2 3 4; do
            scenario=$(jq -r ".[$i].scenario" "$PGREST_JSON" 2>/dev/null)
            p50_a=$(jq -r ".[$i].latency_p50_ms" "$PGREST_JSON" 2>/dev/null)
            p95_a=$(jq -r ".[$i].latency_p95_ms" "$PGREST_JSON" 2>/dev/null)
            p99_a=$(jq -r ".[$i].latency_p99_ms" "$PGREST_JSON" 2>/dev/null)
            p50_b=$(jq -r ".[$i].latency_p50_ms" "$POSTGREST_JSON" 2>/dev/null)
            p95_b=$(jq -r ".[$i].latency_p95_ms" "$POSTGREST_JSON" 2>/dev/null)
            p99_b=$(jq -r ".[$i].latency_p99_ms" "$POSTGREST_JSON" 2>/dev/null)
            [[ "$scenario" != "null" && -n "$scenario" ]] && echo "| $scenario | $p50_a / $p95_a / $p99_a | $p50_b / $p95_b / $p99_b |"
        done
    fi

    echo ""
    echo "## Error Rate (%)"
    echo ""
    echo "| Scenario | PgREST | PostgREST |"
    echo "|----------|--------|-----------|"

    if [[ -s "$PGREST_JSON" && -s "$POSTGREST_JSON" ]]; then
        for i in 0 1 2 3 4; do
            scenario=$(jq -r ".[$i].scenario" "$PGREST_JSON" 2>/dev/null)
            err_a=$(jq -r ".[$i].error_rate * 100" "$PGREST_JSON" 2>/dev/null)
            err_b=$(jq -r ".[$i].error_rate * 100" "$POSTGREST_JSON" 2>/dev/null)
            [[ "$scenario" != "null" && -n "$scenario" ]] && echo "| $scenario | ${err_a}% | ${err_b}% |"
        done
    fi
} > "$COMPARISON_FILE"

echo ""
echo "=== Benchmark complete ==="
echo "Results saved to:"
echo "  - $RESULTS_DIR/pgrest_${TIMESTAMP}.log"
echo "  - $RESULTS_DIR/postgrest_${TIMESTAMP}.log"
echo "  - $COMPARISON_FILE"
echo ""
echo "Comparison table (req/s, latency, error rate):"
echo ""
cat "$COMPARISON_FILE"
