#!/bin/sh
# Registers the Debezium PostgreSQL connector with Kafka Connect, or updates it
# if it already exists, then waits until it is running.
#
# Safe to run any number of times: PUT /connectors/<name>/config creates the
# connector when it is missing and replaces its configuration when it is not.
# The configuration file holds no secrets; Kafka Connect resolves the
# ${env:...} placeholders from its own environment.
#
# Docker Compose runs this automatically (service debezium-register). To run
# it again after editing the configuration:
#
#   docker compose run --rm debezium-register        # any OS
#   ./scripts/register-debezium.sh                    # from the host, needs curl
#
# Settings, all optional:
#   CONNECT_URL        Kafka Connect REST endpoint   (default http://localhost:8083)
#   CONNECTOR_NAME     connector name                (default documents-cdc)
#   CONNECTOR_CONFIG   configuration file            (default docker/debezium/connector.json)
#   WAIT_SECONDS       how long to wait for Connect  (default 120)

set -eu

CONNECT_URL="${CONNECT_URL:-http://localhost:8083}"
CONNECTOR_NAME="${CONNECTOR_NAME:-documents-cdc}"
CONNECTOR_CONFIG="${CONNECTOR_CONFIG:-docker/debezium/connector.json}"
WAIT_SECONDS="${WAIT_SECONDS:-120}"
RESPONSE="$(mktemp)"
trap 'rm -f "$RESPONSE"' EXIT

if [ ! -f "$CONNECTOR_CONFIG" ]; then
    echo "connector configuration not found: $CONNECTOR_CONFIG" >&2
    exit 1
fi

# 1. Wait for the Kafka Connect REST API.
waited=0
until curl -fsS -o /dev/null "$CONNECT_URL/connectors"; do
    if [ "$waited" -ge "$WAIT_SECONDS" ]; then
        echo "Kafka Connect did not answer at $CONNECT_URL within ${WAIT_SECONDS}s" >&2
        exit 1
    fi
    echo "waiting for Kafka Connect at $CONNECT_URL ..."
    sleep 3
    waited=$((waited + 3))
done

# 2. Create or update the connector.
status=$(curl -sS -o "$RESPONSE" -w '%{http_code}' -X PUT \
    -H 'Content-Type: application/json' \
    --data "@$CONNECTOR_CONFIG" \
    "$CONNECT_URL/connectors/$CONNECTOR_NAME/config")
case "$status" in
    201) echo "connector $CONNECTOR_NAME created" ;;
    200) echo "connector $CONNECTOR_NAME already existed; configuration updated" ;;
    *)
        echo "registering connector $CONNECTOR_NAME failed with HTTP $status:" >&2
        cat "$RESPONSE" >&2
        echo >&2
        exit 1
        ;;
esac

# 3. Wait until the connector and its task are running, or report the failure.
waited=0
while :; do
    curl -fsS -o "$RESPONSE" "$CONNECT_URL/connectors/$CONNECTOR_NAME/status" || true
    if grep -q '"state":"FAILED"' "$RESPONSE"; then
        echo "connector $CONNECTOR_NAME failed:" >&2
        cat "$RESPONSE" >&2
        echo >&2
        exit 1
    fi
    # One RUNNING for the connector and at least one for its task.
    if [ "$(grep -o '"state":"RUNNING"' "$RESPONSE" | wc -l)" -ge 2 ]; then
        echo "connector $CONNECTOR_NAME is running:"
        cat "$RESPONSE"
        echo
        exit 0
    fi
    if [ "$waited" -ge "$WAIT_SECONDS" ]; then
        echo "connector $CONNECTOR_NAME did not reach RUNNING within ${WAIT_SECONDS}s; last status:" >&2
        cat "$RESPONSE" >&2
        echo >&2
        exit 1
    fi
    sleep 2
    waited=$((waited + 2))
done
