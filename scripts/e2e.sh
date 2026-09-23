#!/usr/bin/env bash
# Runs the end-to-end suite against the Docker Compose stack.
#
#   scripts/e2e.sh                 fake embeddings, stack left running
#   E2E_DOWN=1 scripts/e2e.sh      stop the stack afterwards (volumes kept)
#   E2E_REAL_EMBEDDING=true scripts/e2e.sh
#                                  use the configured provider and run the
#                                  opt-in semantic smoke test
#
# It never removes volumes or touches Docker resources outside this project.
set -euo pipefail

cd "$(dirname "$0")/.."

if ! docker info > /dev/null 2>&1; then
    echo "Docker does not answer. Start Docker and try again." >&2
    exit 1
fi
if [ ! -f .env ]; then
    echo "No .env file. Copy .env.example to .env first." >&2
    exit 1
fi

if [ "${E2E_REAL_EMBEDDING:-}" = "true" ]; then
    echo "Using the configured embedding provider (EMBEDDING_API_KEY must be set)."
else
    # Deterministic local vectors: no API key, no network, no cost.
    export EMBEDDING_PROVIDER=fake
    echo "Using the fake embedding provider."
fi

echo "Starting the stack..."
docker compose up -d --build

# Idempotent: creates the connector, or updates it if it already exists.
echo "Ensuring the Debezium connector is registered..."
docker compose run --rm debezium-register

# The tests wait for each service to be ready before they touch anything.
echo "Running the end-to-end tests..."
export E2E=1
status=0
go test ./tests/e2e -v -timeout "${E2E_GO_TIMEOUT:-15m}" || status=$?

if [ "$status" -ne 0 ]; then
    echo "Tests failed; last 200 log lines per service:" >&2
    docker compose logs --tail=200 --no-color consumer api debezium kafka >&2 || true
fi

if [ "${E2E_DOWN:-0}" = "1" ]; then
    echo "Stopping the stack (volumes are kept)..."
    docker compose down
fi

exit "$status"
