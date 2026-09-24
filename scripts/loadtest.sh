#!/usr/bin/env bash
# Runs one benchmark suite against the local Docker Compose stack.
#
#   scripts/loadtest.sh search       [baseline|staged|limits|rps|validation]
#   scripts/loadtest.sh indexing     [document count]
#   scripts/loadtest.sh workers      [comma-separated worker counts]
#   scripts/loadtest.sh backpressure [events/sec]
#
# The stack must already be running, with the deterministic fake embedding
# provider, which is what makes a benchmark repeatable:
#
#   EMBEDDING_PROVIDER=fake docker compose up -d --build
#
# Nothing here removes containers or volumes. Delete the rows a run created
# with scripts/cleanup-loadtest.sh.
set -euo pipefail

suite="${1:-search}"
arg="${2:-}"
run_id="${LOADTEST_RUN_ID:-$(date -u +%Y%m%d-%H%M%S)}"

cd "$(dirname "$0")/.."

require_stack() {
    if ! docker info >/dev/null 2>&1; then
        echo "Docker does not answer. Start it and try again." >&2
        exit 1
    fi
    local running
    running="$(docker compose ps --services --filter status=running)"
    for service in postgres kafka opensearch qdrant api consumer; do
        if ! grep -qx "$service" <<<"$running"; then
            echo "The $service service is not running. Start the stack: docker compose up -d" >&2
            exit 1
        fi
    done
    local provider
    provider="$(docker compose exec -T consumer printenv EMBEDDING_PROVIDER 2>/dev/null | tr -d '\r' || true)"
    if [ -n "$provider" ] && [ "$provider" != "fake" ]; then
        echo "WARNING: the consumer uses the '$provider' embedding provider; results will include" >&2
        echo "         its network latency, rate limits and cost. EMBEDDING_PROVIDER=fake is reproducible." >&2
    fi
}

show_environment() {
    echo
    echo "--- environment ---------------------------------------------"
    echo "date          $(date -u)"
    echo "os            $(uname -srm)"
    echo "cpus          $(getconf _NPROCESSORS_ONLN 2>/dev/null || echo '?')"
    echo "go            $(go version)"
    echo "docker        $(docker --version)"
    echo "compose       $(docker compose version --short)"
    echo "(record these with the results: a number without its environment means nothing)"
    echo "-------------------------------------------------------------"
    echo
}

run_search() {
    local scenario="${arg:-baseline}"
    if ! command -v k6 >/dev/null 2>&1; then
        echo "k6 is not installed. See benchmarks/README.md." >&2
        exit 1
    fi
    echo "Running the k6 '$scenario' scenario..."
    LOADTEST_SCENARIO="$scenario" k6 run benchmarks/search/search.js || \
        echo "k6 reported a failed threshold. The run is still valid data; read it."
}

run_indexing() {
    local count="${arg:-1000}"
    echo "Running the indexing benchmark ($count documents, run $run_id)..."
    go run ./cmd/loadgen -mode index -run "$run_id" -count "$count"
}

run_workers() {
    local counts="${arg:-1,2,4,8}"
    echo "Comparing worker counts. Each step restarts the consumer."
    echo "More workers is not automatically faster; the point is to find where it stops helping."
    IFS=',' read -ra workers <<<"$counts"
    for workers_count in "${workers[@]}"; do
        echo
        echo "=== INDEXING_WORKERS=$workers_count ==="
        INDEXING_WORKERS="$workers_count" docker compose up -d consumer
        sleep 10 # let the consumer join the group before anything is measured
        go run ./cmd/loadgen -mode index -run "${run_id}-w${workers_count}" -count "${LOADTEST_DOCUMENT_COUNT:-1000}"
    done
    echo
    echo "Restoring the configured worker count..."
    docker compose up -d consumer
}

run_backpressure() {
    local rate="${arg:-500}"
    local count="${LOADTEST_DOCUMENT_COUNT:-5000}"
    echo "Publishing $count events at $rate events/sec, straight to Kafka."
    echo "Watch the lag column: if it grows, the consumer is behind and Kafka holds the backlog."
    echo "Memory should stay flat, because the queue inside the consumer is bounded."
    echo "In another terminal: docker stats"
    echo

    go run ./cmd/loadgen -mode watch -run "$run_id" -interval 2s -timeout 5m &
    local watcher=$!
    trap 'kill "$watcher" 2>/dev/null || true' EXIT

    go run ./cmd/loadgen -mode publish -run "$run_id" -count "$count" -rate "$rate"
    echo
    echo "Publishing finished; the watcher keeps printing until the lag is back to zero."
    wait "$watcher" || true
}

require_stack
show_environment

case "$suite" in
search) run_search ;;
indexing) run_indexing ;;
workers) run_workers ;;
backpressure) run_backpressure ;;
*)
    echo "Unknown suite '$suite'. Use search, indexing, workers or backpressure." >&2
    echo "The recovery suite is PowerShell-only for now; see benchmarks/README.md for the manual steps." >&2
    exit 1
    ;;
esac

echo
echo "Record the results in benchmarks/results/README.md together with the environment above."
