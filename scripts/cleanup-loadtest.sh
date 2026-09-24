#!/usr/bin/env bash
# Deletes the rows a benchmark created.
#
#   scripts/cleanup-loadtest.sh            every benchmark run
#   scripts/cleanup-loadtest.sh <run id>   one run
#
# Benchmark documents are identified by their generated URL prefix
# (https://example.com/loadtest/...), so this can only delete rows a benchmark
# made: seeded and hand-written documents have different URLs.
#
# Deleting the rows also removes them from both search indexes, through the
# same delete events any other delete produces. Nothing here removes
# containers, volumes or images.
set -euo pipefail

run_id="${1:-}"
cd "$(dirname "$0")/.."

if ! docker info >/dev/null 2>&1; then
    echo "Docker does not answer. Start it and try again." >&2
    exit 1
fi

if [ -n "$run_id" ]; then
    scope="run $run_id"
else
    scope="every benchmark run"
fi

if [ -z "${LOADTEST_YES:-}" ]; then
    read -r -p "Delete the benchmark rows of $scope? [y/N] " answer
    case "$answer" in
    y | Y | yes | YES) ;;
    *)
        echo "Nothing was deleted."
        exit 0
        ;;
    esac
fi

if [ -n "$run_id" ]; then
    go run ./cmd/loadgen -mode cleanup -run "$run_id"
else
    go run ./cmd/loadgen -mode cleanup -all
fi

echo
echo "Watch the indexes shrink as the deletes travel through the pipeline:"
echo "  go run ./cmd/loadgen -mode watch -interval 2s -timeout 2m"
