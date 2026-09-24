#!/usr/bin/env bash
# Prints the consumer group's lag, using Kafka's own tooling.
#
#   scripts/kafka-lag.sh                  once
#   scripts/kafka-lag.sh watch            until Ctrl+C
#   scripts/kafka-lag.sh watch 5          every 5 seconds
#
# Lag is how many messages have been produced that the group has not committed
# yet. A consumer that is falling behind still reports healthy throughput,
# because it is busy; only the growing gap says work is arriving faster than it
# leaves.
set -euo pipefail

mode="${1:-once}"
interval="${2:-2}"
group="${KAFKA_CONSUMER_GROUP:-near-realtime-search}"

cd "$(dirname "$0")/.."

if ! docker info >/dev/null 2>&1; then
    echo "Docker does not answer. Start it and try again." >&2
    exit 1
fi

describe() {
    echo "--- $(date +%H:%M:%S) ---"
    docker compose exec -T kafka /opt/kafka/bin/kafka-consumer-groups.sh \
        --bootstrap-server kafka:9092 --describe --group "$group"
}

if [ "$mode" = "watch" ]; then
    while true; do
        describe
        sleep "$interval"
    done
else
    describe
fi
