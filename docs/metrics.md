# Metrics

Both Go services expose Prometheus metrics. Together they answer: is the
system healthy, how much traffic is it taking, how fast is it working, what is
failing, and where.

```text
api       :8080/metrics   HTTP traffic, search latency, the calls each search makes
consumer  :9091/metrics   Kafka, retries, dead letters, indexing, the worker pool
                ↓ scrape every 15s
           Prometheus :9090
```

Metrics are observational. Nothing in this feature changes how a search is
answered, how an event is indexed, when an offset is committed or how a
failure is retried.

## Endpoints

| Service | URL | Served by |
|---|---|---|
| API | `http://localhost:8080/metrics` | the same HTTP server as `/api/v1/search`, `/health` and `/ready` |
| Consumer | `http://localhost:9091/metrics` | a listener of its own, because the consumer has no HTTP API |
| Prometheus | `http://localhost:9090` | `docker/prometheus/prometheus.yml` |

```powershell
curl.exe http://localhost:8080/metrics | Select-String hybrid_search_search_requests_total
curl.exe http://localhost:9091/metrics | Select-String hybrid_search_kafka
curl.exe "http://localhost:9090/api/v1/targets?state=active"   # is Prometheus scraping?
```

Both endpoints answer plain HTTP with no authentication, which is right for a
local stack and is not right for a public one. The whole stack is
local-development only; see the security note in the root README.

## Configuration

| Variable | Default | Meaning |
|---|---|---|
| `METRICS_ENABLED` | `true` | When false, no endpoint is served and nothing is recorded |
| `METRICS_PATH` | `/metrics` | Path on both services |
| `METRICS_ADDR` | `:9091` | Address the consumer's metrics listener binds to; the API ignores it |

## Naming

Every metric is prefixed `hybrid_search_`. Counters end in `_total`,
histograms in `_duration_seconds` (seconds, always), gauges are named for what
they hold. Percentiles are not exported; Prometheus computes them from the
histogram buckets:

```promql
histogram_quantile(0.95, sum by (le) (rate(hybrid_search_http_request_duration_seconds_bucket[5m])))
```

### Buckets

Each kind of operation has buckets that fit what it costs. Using one set
everywhere would put every HTTP request in one bucket and every indexing event
in another, and neither would give a usable percentile.

| Metric | Buckets |
|---|---|
| `http_request_duration_seconds`, `search_duration_seconds` | 5ms … 10s |
| `dependency_duration_seconds` | 1ms … 5s |
| `kafka_message_processing_duration_seconds`, `indexing_processing_duration_seconds` | 5ms … 60s (retries and their backoff are inside this) |
| `kafka_retry_delay_seconds` | 100ms … 60s, matching the configured backoff |

## The metrics

### HTTP (API)

| Metric | Type | Labels | Meaning |
|---|---|---|---|
| `hybrid_search_http_requests_total` | counter | method, route, status | Requests answered |
| `hybrid_search_http_request_duration_seconds` | histogram | method, route | Time to answer |
| `hybrid_search_http_requests_in_flight` | gauge | — | Requests being handled now; a number that keeps climbing means the server is not keeping up |
| `hybrid_search_http_response_errors_total` | counter | route, class (4xx/5xx) | Failed responses, split from successes |

`route` is the endpoint, never the path: every search is `/api/v1/search`
whatever was searched for, and anything unknown is `other`.

### Search (API)

| Metric | Type | Labels | Meaning |
|---|---|---|---|
| `hybrid_search_search_requests_total` | counter | status (success/error/cancelled) | Hybrid searches run |
| `hybrid_search_search_duration_seconds` | histogram | status | Time for one search, request to fused results |
| `hybrid_search_search_errors_total` | counter | stage (keyword/embedding/vector/fusion) | Which step failed |
| `hybrid_search_search_results_returned` | histogram | — | Results per successful search; a spike at 0 is a corpus problem, not a latency one |

`cancelled` is kept apart from `error`: the caller gave up or ran out of time,
and counting that as a failure would make a client timeout look like an
outage.

### Dependencies (both services)

| Metric | Type | Labels | Meaning |
|---|---|---|---|
| `hybrid_search_dependency_requests_total` | counter | dependency, operation, status | Calls to OpenSearch, Qdrant and the embedding provider |
| `hybrid_search_dependency_duration_seconds` | histogram | dependency, operation | Time for one call |
| `hybrid_search_dependency_errors_total` | counter | dependency, operation | Failed calls |

`dependency` is `opensearch`, `qdrant` or `embedder`. `operation` is `search`,
`index`, `upsert`, `delete` or `embed`. The same names are used on both sides,
so one query covers searching and indexing:

```promql
# which dependency is making searches slow
histogram_quantile(0.95, sum by (dependency, le) (rate(hybrid_search_dependency_duration_seconds_bucket[5m])))
```

### Kafka (consumer)

| Metric | Type | Labels | Meaning |
|---|---|---|---|
| `hybrid_search_kafka_messages_total` | counter | topic, outcome | Messages by what happened: consumed, indexed, skipped, dead_lettered, failed |
| `hybrid_search_kafka_message_processing_duration_seconds` | histogram | topic | Time from the pipeline receiving a message until it is finished — not how long it waited in Kafka |
| `hybrid_search_kafka_offset_commits_total` | counter | — | Offset commits made |
| `hybrid_search_kafka_consumer_errors_total` | counter | type (fetch/process/commit) | Consumer errors; the error text is logged, never labelled |

`consumed` counts messages fetched; the other outcomes count messages
finished. Work that is processed but never committed shows up as the processed
outcomes rising while `kafka_offset_commits_total` does not.

### Retries and the dead-letter topic (consumer)

| Metric | Type | Labels | Meaning |
|---|---|---|---|
| `hybrid_search_kafka_retries_total` | counter | error_type (retryable/non_retryable/unknown) | Retried attempts |
| `hybrid_search_kafka_retry_exhausted_total` | counter | error_type | Messages that used every attempt |
| `hybrid_search_kafka_retry_delay_seconds` | histogram | — | Backoff waited before a retry |
| `hybrid_search_kafka_dlq_messages_total` | counter | reason (retry_exhausted/non_retryable) | Messages stored in the dead-letter topic |
| `hybrid_search_kafka_dlq_publish_errors_total` | counter | — | Dead-letter publishes that failed |

The last two mean opposite things and are deliberately separate. A stored
dead letter means the message is finished and its offset is committed. A
failed publish means the offset is **not** committed and the message will be
delivered again: the consumer is stuck, not losing data. Alert on the second,
watch the first.

### Indexing (consumer)

| Metric | Type | Labels | Meaning |
|---|---|---|---|
| `hybrid_search_indexing_events_total` | counter | operation (create/update/delete/read), effect | Change events applied |
| `hybrid_search_indexing_processing_duration_seconds` | histogram | operation | Time to apply one event to both indexes |
| `hybrid_search_indexing_dependency_errors_total` | counter | dependency | Which store failed |

`effect` comes from the idempotency work: `indexed` (new data), `reapplied` (a
redelivery of an event already applied), `stale` (newer data is already
indexed, so nothing was written), `deleted`, or `failed`. Duplicates are
therefore countable:

```promql
rate(hybrid_search_indexing_events_total{effect="reapplied"}[5m])
```

`indexing_dependency_errors_total` is what tells a partial write from a
rejected event: OpenSearch is written first, so a failure labelled `qdrant` or
`embedder` means the keyword index is ahead of the vector index until the
event is processed again.

### Worker pool (consumer)

| Metric | Type | Labels | Meaning |
|---|---|---|---|
| `hybrid_search_indexing_queue_size` | gauge | — | Messages waiting in the worker queues now |
| `hybrid_search_indexing_queue_capacity` | gauge | — | How many could wait |
| `hybrid_search_indexing_workers_active` | gauge | — | Workers applying an event now |
| `hybrid_search_indexing_workers_total` | gauge | — | Workers in the pool (`INDEXING_WORKERS`) |

These are read when Prometheus scrapes, not written while messages are
processed, so watching the queue costs the pipeline nothing.

Queue size at capacity is backpressure working, not a fault: `Submit` blocks,
the consumer stops fetching, and the backlog stays in Kafka rather than in
memory. Active sitting at total means the pool is the limit on throughput,
which is the signal to try more workers — after measuring, per
`benchmarks/README.md`.

### Application (both)

| Metric | Type | Labels | Meaning |
|---|---|---|---|
| `hybrid_search_application_ready` | gauge | — | 1 when every readiness check passed, 0 when one did not |
| `hybrid_search_application_info` | gauge | application, environment, role | Always 1; its labels say what this process is |

`application_ready` is set by the same checks `/ready` runs — there is no
second definition of readiness. The API also samples them every 10 seconds, so
the gauge is current even when nothing is probing the endpoint. `role` is
`api` or `consumer`. No configuration values and no secrets are ever labels.

### Runtime

The Prometheus Go client's default registry already exposes goroutines, heap
and GC (`go_*`) and process CPU, memory and file descriptors (`process_*`).
They are not redefined here.

## Cardinality

Every label above has a small, fixed set of values. Nothing that varies per
request or per document is ever a label: no query text, no document or event
ID, no Kafka offset or partition, no user, no error message, no URL. One
unbounded label turns a metric into millions of series and takes Prometheus
down with it.

`internal/metrics` has a test that fails if a label value looks unbounded, and
the route label is mapped through a fixed table so that a flood of unknown
paths produces one `other` series rather than one series each.

## Where the numbers come from

The application packages know nothing about Prometheus. Each offers a small
observation hook, and `internal/metrics` turns what those hooks report into
metrics:

| Hook | Reported by | Becomes |
|---|---|---|
| `hybrid.Service.Observe` | one search, with its per-stage timings | search and dependency metrics |
| `cdc.Service.Observe` | each store call and each applied event | dependency and indexing metrics |
| `indexing.Pipeline.Observe` | one finished message | Kafka outcome, dead-letter, exhausted-retry metrics |
| `indexing.Pipeline.ObserveRetry` | each retried attempt | retry count and backoff |
| `kafka.Consumer.Observe` | fetches, commits and their failures | consumption and commit metrics |
| `indexing.WorkerPool.Stats` | queue depth and busy workers, read at scrape time | worker pool gauges |
| `api.ReadinessHandler.Observe` | the result of a readiness check | `application_ready` |

The dependency points one way — metrics towards the application — so
instrumentation can change without touching the code being measured, and every
package's own tests still run without a registry. A nil `*Metrics` is valid
and does nothing, which is how components are built in tests.

## Checking it works

```powershell
# 1. the endpoints answer
curl.exe -s http://localhost:8080/metrics | Select-String "^hybrid_search_" | Select-Object -First 5
curl.exe -s http://localhost:9091/metrics | Select-String "^hybrid_search_kafka"

# 2. searching moves the counters
curl.exe -s "http://localhost:8080/api/v1/search?q=kafka&limit=5" > $null
curl.exe -s http://localhost:8080/metrics | Select-String "search_requests_total|search_duration_seconds_count"

# 3. indexing moves them too
docker compose exec postgres psql -U postgres -d searchdb -c "INSERT INTO documents (id, title, body) VALUES (gen_random_uuid(), 'Metrics check', 'A row inserted to watch the indexing counters move.');"
Start-Sleep -Seconds 5
curl.exe -s http://localhost:9091/metrics | Select-String "indexing_events_total|kafka_messages_total"

# 4. Prometheus is scraping both services
curl.exe -s "http://localhost:9090/api/v1/targets?state=active" | Select-String '"health":"up"'
```

To watch failure and recovery, use the benchmark suite's recovery run
(`./scripts/loadtest.ps1 -Suite recovery`): OpenSearch stops, and
`dependency_errors_total{dependency="opensearch"}`, `kafka_retries_total` and
then `kafka_dlq_messages_total` rise in that order; starting it again brings
`application_ready` back to 1 and the failure rates back to zero.

## Dashboards

Grafana draws these metrics, provisioned with the stack at
http://localhost:3000. See [grafana.md](grafana.md).

## What is not here

No tracing, no OpenTelemetry, no alerting rules, and no exporters for
PostgreSQL or the Kafka broker. This exposes what the application measures
about itself and its dependencies; monitoring those services from the inside
is a separate decision.
