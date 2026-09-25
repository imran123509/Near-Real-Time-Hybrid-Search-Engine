# Near-Real-Time Hybrid Search Engine

Hybrid keyword + vector search built with Go, OpenSearch, Qdrant, Kafka and PostgreSQL.
Changes to PostgreSQL rows reach both search indexes within seconds through Debezium
change data capture, and search results from both engines are merged with Reciprocal
Rank Fusion (RRF).

```text
PostgreSQL ──WAL──> Debezium ──> Kafka ──> consumer ──┬──> OpenSearch   (BM25 keyword index)
                                                       └──> embedding ──> Qdrant (vector index)

HTTP client ──> api ──┬──> OpenSearch ─────────────┐
                      └──> embedding ──> Qdrant ───┴──> RRF ──> results
```

## Layout

- `cmd/api` – HTTP search API
- `cmd/consumer` – Kafka consumer that applies Debezium change events to both indexes
- `cmd/loadgen` – indexing benchmarks: generates changes and measures the pipeline
- `internal/api` – HTTP handlers, routes and middleware
- `internal/search/hybrid` – runs keyword and vector search and fuses them with RRF
- `internal/search/opensearch`, `internal/search/qdrant` – the two search indexes
- `internal/search/rrf` – Reciprocal Rank Fusion
- `internal/indexing` – worker pool, retries and dead-lettering
- `internal/indexing/cdc` – Debezium change events, normalized and applied to both indexes
- `internal/retry` – retry policy, exponential backoff and error classification
- `internal/dlq` – the dead-letter message and the publisher interface
- `internal/embedding` – embedding provider interface and the Gemini implementation
- `internal/kafka` – Kafka consumer and dead-letter writer
- `internal/postgres` – PostgreSQL connection pool and repository
- `internal/startup` – bounded retries while dependencies start
- `internal/metrics` – Prometheus metrics and the /metrics endpoint
- `internal/config` – configuration loading
- `tests/e2e` – end-to-end tests against the running stack
- `migrations/` – database migrations
- `docker/` – PostgreSQL initialisation, the Debezium connector, and the Prometheus and Grafana configuration
- `docs/` – [metrics reference](docs/metrics.md), [dashboards](docs/grafana.md)
- `scripts/` – Debezium connector registration, the end-to-end runner and the benchmarks
- `benchmarks/` – k6 search load tests and the results template

## Running locally with Docker Compose

### Prerequisites

- Docker with Compose v2 (Docker Desktop on Windows or macOS). Give Docker at least
  4 GB of memory; OpenSearch, Kafka and Kafka Connect are JVM services.
- A Gemini API key for embeddings, or `EMBEDDING_PROVIDER=fake` to run without one.

### 1. Configure

```powershell
git clone https://github.com/imran123509/Near-Real-Time-Hybrid-Search-Engine.git
cd Near-Real-Time-Hybrid-Search-Engine
Copy-Item .env.example .env      # bash: cp .env.example .env
```

Edit `.env` and set `EMBEDDING_API_KEY`. The other values are development defaults.
`.env` is git-ignored; never commit it.

To run without an embedding account, set `EMBEDDING_PROVIDER=fake` instead. It
produces deterministic local vectors, which is enough to exercise the pipeline
but not real semantic search. The services refuse to use it when `APP_ENV` is
`production`.

### 2. Start

```powershell
docker compose up -d --build
docker compose ps
```

Compose starts everything in dependency order, waiting on health checks rather than
on containers merely having started:

1. **postgres** runs `migrations/` (schema), creates the `debezium` replication role
   and publication, and inserts ten sample documents.
2. **kafka** starts (KRaft, no ZooKeeper), then **kafka-init** creates the topics.
3. **debezium** (Kafka Connect) starts, then **debezium-register** registers the
   connector and exits. The connector snapshots the existing rows, then streams changes.
4. **opensearch** and **qdrant** start. The **consumer** creates the index and the
   collection if they are missing and indexes events. The **api** serves search.

`kafka-init` and `debezium-register` show as `Exited (0)` when done. That is expected.
The Go services also retry their dependencies for up to `APP_STARTUP_TIMEOUT`, so
they tolerate starting before them outside Compose too, without retrying forever.

### Ports on the host

| Service | Address | Notes |
|---|---|---|
| Search API | http://localhost:8080 | `/api/v1/search`, `/health`, `/ready`, `/metrics` |
| PostgreSQL | localhost:5432 | database `searchdb` |
| Kafka | localhost:29092 | containers use `kafka:9092` |
| Kafka Connect REST | http://localhost:8083 | connector `documents-cdc` |
| OpenSearch | http://localhost:9200 | security disabled: local development only |
| Qdrant | http://localhost:6333 (REST, dashboard at `/dashboard`), localhost:6334 (gRPC) | |
| Consumer metrics | http://localhost:9091/metrics | the consumer has no other HTTP endpoint |
| Prometheus | http://localhost:9090 | scrapes the api and the consumer every 15s |
| Grafana | http://localhost:3000 | dashboards; reading them needs no login |

### Everyday commands

```powershell
docker compose ps                       # status and health
docker compose logs -f                  # all logs
docker compose logs -f consumer api     # just the Go services
docker compose up -d --build api        # rebuild and restart one service
docker compose down                     # stop; data is kept
docker compose down -v                  # stop and DELETE all data
```

`docker compose down -v` deletes the volumes: the database, Kafka topics and offsets,
the OpenSearch index and the Qdrant collection. The next `up` starts from scratch,
re-running the PostgreSQL initialisation and a fresh Debezium snapshot. It is also
the way to re-run the PostgreSQL init scripts, which only run on an empty volume.

In Windows PowerShell 5.1, `curl` is an alias for `Invoke-WebRequest`; type
`curl.exe` in the commands below, or use PowerShell 7.

## The CDC pipeline

### Topic naming

Debezium names topics `<topic.prefix>.<schema>.<table>`. The prefix is fixed to
`search` in `docker/debezium/connector.json`, so the documents table always produces:

| Topic | Contents |
|---|---|
| `search.public.documents` | change events, keyed by primary key; `KAFKA_TOPIC` |
| `search.public.documents.dlq` | events the consumer could not index; `KAFKA_DLQ_TOPIC` |

Both are created by `kafka-init`; Kafka's topic auto-creation is off, so a misspelled
name fails loudly. To change the prefix, change it in `connector.json`,
`docker-compose.yml` (`KAFKA_TOPIC` and `kafka-init`) and `.env` together.

### PostgreSQL setup

- `wal_level=logical`, so Debezium can decode the write-ahead log with `pgoutput`.
- `max_slot_wal_keep_size=1GB`, so a stopped connector cannot fill the disk.
- A `debezium` role with only `REPLICATION` and `SELECT` on `documents`.
- The publication `search_documents`, created ahead of time so the connector needs no
  ownership of the table (`publication.autocreate.mode=disabled`).
- The replication slot `search_documents`, created by the connector.
- A trigger that sets `version` from a sequence and `updated_at` to `now()` on every
  insert and update (`migrations/0002_cdc_support.up.sql`). OpenSearch only applies
  a write with a higher version than it holds, so an `UPDATE` that forgot to bump the
  version would otherwise never reach the keyword index.

### Event format

The connector emits Debezium's standard envelope (`before`, `after`, `op`, `source`,
`ts_ms`) as plain JSON without the schema wrapper, keyed by `{"id": "..."}`. The
consumer handles:

| `op` | Meaning | Consumer action |
|---|---|---|
| `r` | row read by the initial snapshot | upsert into OpenSearch, embed, upsert into Qdrant |
| `c` | insert | same as `r` |
| `u` | update | same as `r` |
| `d` | delete | delete from both indexes |
| (null value) | tombstone following each delete | skipped |

The table keeps PostgreSQL's default replica identity: a delete event's `before`
holds the primary key, which is all the consumer needs.

### Idempotency and recovery

Kafka delivers at least once. After a crash, a rebalance or a retry, the consumer
sees messages it has already applied, and there is no way to stop that from the
consumer side. So the pipeline is built to make a repeat harmless rather than to
prevent it:

```text
at-least-once delivery  +  idempotent indexing  =  safe processing
```

Everything rests on identity being derived, never invented:

| Identity | Derived from | Why it matters |
|---|---|---|
| Document ID | the row's primary key | the OpenSearch `_id`; the same row always writes the same document |
| Qdrant point ID | the document ID (used as-is when it is a UUID, otherwise a name-based UUID) | an upsert replaces that document's point instead of adding one |
| Event ID | `schema.table` + primary key + operation + LSN | a redelivery carries the identity it had the first time, so `event_id` appearing twice in the logs *is* the duplicate |

A document ID and an event ID are not the same thing: one document has many events
(`CREATE`, `UPDATE`, …, `DELETE`), each with its own ID, all writing the same
document. Nothing here calls `uuid.New()`; a random identity would make every
delivery look new.

Because of that, `Process(event)` three times leaves what `Process(event)` once
leaves: one document, one point, the same content. Deleting something already
deleted succeeds in both stores, so a replayed `DELETE` is not retried forever.

**No processed-event store.** There is no Redis, no table of seen event IDs, and
none is needed: every write already states the final state rather than a change to
it. An event store would be another system to keep consistent with the two indexes,
and it still would not make the two writes atomic.

**Out-of-order changes.** Events for one row share a Kafka key, so Debezium puts
them on one partition, and the worker pool routes by that key, so one worker applies
them in the order the database made them. Where that order can still break — two
consumers briefly overlapping during a rebalance, a dead-lettered event, a manual
replay — the `version` column decides instead. It is assigned by a database sequence
(`migrations/0002`) and sent to OpenSearch as an external version, which refuses any
write that is not newer. The consumer then stops there and does **not** embed the
row or upsert the vector, because Qdrant has no version check of its own; that
event is logged as `event_skipped_as_stale`.

**Partial failure.** OpenSearch and Qdrant are separate systems with no shared
transaction, and none is simulated. An upsert writes OpenSearch, then embeds, then
writes Qdrant, and stops at the first failure, so the reachable partial state is
always the same one: the keyword index is ahead of the vector index. The event is
reported as failed, retried, and the repeated writes converge both stores.

```text
crash before commit -> Kafka redelivers -> idempotent reprocessing -> correct final state

OpenSearch ok, Qdrant fails    -> retry -> OpenSearch upsert (no-op), Qdrant upsert  -> consistent
Qdrant ok, OpenSearch fails    -> retry -> OpenSearch upsert, Qdrant upsert (no-op)  -> consistent
```

**Why not exactly-once.** Kafka transactions cover Kafka, not OpenSearch and Qdrant;
a two-phase commit across all three would need prepared writes both stores lack,
would make every event wait on the slower of the two, and would still leave failure
windows of its own. At-least-once delivery with idempotent writes gives the same
final state for far less machinery. The cost is that a document can be briefly
visible in one index and not the other, which is exactly what the retry closes.

The matrix, covered by `internal/indexing/idempotency_test.go` and repeated against
the running stack by `tests/e2e/idempotency_test.go` and
`tests/e2e/failure_recovery_test.go`:

| Scenario | First attempt | Retry | Final state |
|---|---|---|---|
| CREATE | success | no | 1 document + 1 point |
| CREATE duplicate | success | no | still 1 and 1 |
| UPDATE | success | no | latest version |
| UPDATE duplicate | success | no | latest version |
| DELETE | success | no | removed |
| DELETE duplicate | success | no | still removed |
| OpenSearch down | failure, nothing written | yes | 1 and 1 once it is back |
| Qdrant down | partial: keyword only | yes | 1 and 1 once it is back |
| embedder down | partial: keyword only | yes | 1 and 1, or dead-lettered, never "done" |
| consumer crash | partial, uncommitted | redelivery | 1 and 1 |
| stale replay | refused by both indexes | no | newer row kept |
| invalid event | failure | dead-lettered | neither index touched |

### Retries and the dead-letter queue

Indexing fails for two very different reasons, and the consumer tells them apart by
the errors' types, never by their text:

| Kind | Examples | What happens |
|---|---|---|
| retryable | OpenSearch or Qdrant unavailable, timeout, 5xx, embedding rate limit | retried with backoff |
| non-retryable | unparseable event, unknown operation, missing key, nothing to embed, a store rejecting the request (4xx), wrong vector dimension | dead-lettered at once |
| unknown | anything unrecognised | retried within the policy, then dead-lettered |

The wait before attempt *n* is `KAFKA_RETRY_INITIAL_BACKOFF x KAFKA_RETRY_MULTIPLIER^(n-1)`,
capped at `KAFKA_RETRY_MAX_BACKOFF`: with the defaults 500ms, 1s, 2s, 4s across
`KAFKA_RETRY_MAX_ATTEMPTS` tries. Waiting is interruptible, so a shutdown does not sit
out a backoff. Retries run on the worker that owns the message, so a failing store
slows consumption instead of spawning work in the background.

An offset is committed only once the message is finished, which means either indexed
or safely stored in `KAFKA_DLQ_TOPIC`. If the dead-letter publish itself fails, nothing
is committed and the consumer stops: a duplicate after a restart is harmless, because
indexing is idempotent, while a lost event is not recoverable. Delivery is
at-least-once, not exactly-once.

Each dead-letter record keeps the original payload byte for byte, keyed by the original
key so a document's failures stay in one partition and in order:

```json
{
  "original_topic": "search.public.documents",
  "original_partition": 0,
  "original_offset": 4711,
  "event_key": "{\"id\":\"7f1c9b2e-...\"}",
  "payload": "<the original message value, base64-encoded by JSON>",
  "error": "index in opensearch: status 400: mapping conflict",
  "error_type": "non_retryable",
  "attempts": 1,
  "failed_at": "2026-05-01T10:00:00Z"
}
```

The consumer logs `event_processing_failed` for each failed attempt,
`event_processing_retry_succeeded` when a retry works, and `event_sent_to_dlq` when it
gives up, each with the topic, partition, offset, attempt, `max_attempts` and
`error_type`. It also logs `event_reprocessed` when a delivery turns out to be a
repeat and `event_skipped_as_stale` when newer data is already indexed, both with
`event_id` and `document_id`, so a duplicate is one grep away. Payloads, keys,
vectors and credentials are never logged.

Ordering: events for one document share a key, so they land on one partition and one
worker and are applied in order. Dead-lettering one of them breaks that chain — the
next event for the same document is applied to indexes that never saw the failed one.
Because every event carries the whole row and OpenSearch only accepts higher versions,
the indexes still converge on the newest row; what is lost is the intermediate state.

There is no reprocessor yet. Replaying a dead-letter record means reading it, fixing
the cause, and publishing its `payload` back onto `KAFKA_TOPIC` under its `event_key`.
A future `cmd/dlq-replay` would do exactly that: read the dead-letter topic as its own
consumer group, republish each payload with its original key, and commit only what it
republished, with a dry-run mode and a limit so a bad replay cannot flood the pipeline.

#### Watching a retry happen

With the stack up, stop the keyword index and insert a row:

```powershell
docker compose stop opensearch
docker compose exec postgres psql -U postgres -d searchdb -c "INSERT INTO documents (id, title, body) VALUES (gen_random_uuid(), 'Retry drill', 'This row is indexed only after OpenSearch comes back.');"
docker compose logs -f consumer     # event_processing_failed, with a growing backoff
docker compose start opensearch     # within the retry window
```

The consumer logs `event_processing_retry_succeeded` and the document appears in both
indexes. Leave OpenSearch down past `KAFKA_RETRY_MAX_ATTEMPTS` instead, and the event is
logged as `event_sent_to_dlq` and shows up on `search.public.documents.dlq`; the
consumer keeps working on the next messages.

## Manual pipeline walkthrough

The automated version of this is `tests/e2e`; run these by hand to watch the
pipeline work. Run them from the repository root with the stack up. Use a second terminal where
noted.

### 0. Snapshot of the existing rows

On first start, the connector snapshots the ten seeded rows as `op: "r"` events.
Within a minute of `up`, both indexes hold ten documents:

```powershell
curl "http://localhost:9200/documents/_count?pretty"          # "count" : 10
curl "http://localhost:6333/collections/documents"            # "points_count": 10
docker compose logs consumer | Select-String "event indexed"  # bash: | grep "event indexed"
```

The consumer logs `"operation":"READ"` for snapshot events.

### 1. Watch the topic (second terminal)

```powershell
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh `
  --bootstrap-server kafka:9092 --topic search.public.documents --property print.key=true
```

In bash, replace the backticks with `\`. Leave it running; Ctrl+C stops it.

### 2. Insert a document

```powershell
docker compose exec postgres psql -U postgres -d searchdb -c "INSERT INTO documents (id, title, body, url) VALUES ('7a1e0c52-2f4b-4d8e-9a3c-5b6d7e8f9012', 'Raft Consensus', 'Raft elects a leader that replicates a log to its followers, so a cluster agrees on the same sequence of commands even when servers crash.', 'https://example.com/articles/raft');"
```

- **Kafka:** the second terminal prints `{"id":"7a1e0c52-..."}` and an event with `"op":"c"`.
- **Consumer:** `docker compose logs --tail 20 consumer` shows `event indexed` with
  `"document_id":"7a1e0c52-2f4b-4d8e-9a3c-5b6d7e8f9012"` and `"operation":"CREATE"`.
- **OpenSearch:** `curl "http://localhost:9200/documents/_doc/7a1e0c52-2f4b-4d8e-9a3c-5b6d7e8f9012?pretty"`
  returns `"found" : true`, and `_version` equals the row's `version`.
- **Qdrant:** `curl "http://localhost:6333/collections/documents/points/7a1e0c52-2f4b-4d8e-9a3c-5b6d7e8f9012"`
  returns the point with its `title` and `document_id` payload. A UUID document ID is
  used unchanged as the point ID.
- **Search:**

  ```powershell
  curl "http://localhost:8080/api/v1/search?q=leader+election+consensus&limit=5"
  ```

  The Raft document is among the results. Try a query that shares no words with it,
  such as `q=how+do+servers+agree+on+an+order+of+operations`: vector search should
  still find it.

### 3. Update it

```powershell
docker compose exec postgres psql -U postgres -d searchdb -c "UPDATE documents SET body = 'Raft keeps a replicated state machine consistent: the leader appends entries, followers acknowledge them, and an entry is committed once a majority has stored it.' WHERE id = '7a1e0c52-2f4b-4d8e-9a3c-5b6d7e8f9012';"
```

The trigger raises `version`, and Debezium emits `"op":"u"`. OpenSearch now shows the
new body with a higher `_version`, and the Qdrant point holds a vector for the new
text. `q=replicated+state+machine+majority` finds it.

### 4. Delete it

```powershell
docker compose exec postgres psql -U postgres -d searchdb -c "DELETE FROM documents WHERE id = '7a1e0c52-2f4b-4d8e-9a3c-5b6d7e8f9012';"
```

Kafka shows `"op":"d"` followed by a tombstone (the same key with a `null` value).
Then:

- OpenSearch: `_doc/7a1e0c52-...` returns `"found" : false`.
- Qdrant: the point URL returns 404.
- Search: the document no longer appears for `q=raft`.

### Inspecting the connector and the dead-letter topic

```powershell
curl http://localhost:8083/connectors/documents-cdc/status
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh `
  --bootstrap-server kafka:9092 --topic search.public.documents.dlq --from-beginning `
  --property print.key=true
```

Each record is the JSON envelope shown under
[Retries and the dead-letter queue](#retries-and-the-dead-letter-queue): why the
message failed, where it came from, and its original payload, base64-encoded.

## Re-registering the connector

`debezium-register` runs on every `docker compose up`. After editing
`docker/debezium/connector.json`, run it again. Registration is idempotent (a `PUT`
of the configuration), so repeating it is safe:

```powershell
./scripts/register-debezium.ps1              # Windows; needs only Docker
docker compose run --rm debezium-register    # any OS
./scripts/register-debezium.sh               # from a host with curl
```

The configuration holds no secrets: `${env:DEBEZIUM_DB_PASSWORD}` and similar
placeholders are resolved by Kafka Connect from its own environment, and the REST
API returns the placeholders, not the values.

## Running a Go service on the host

To run `api` or `consumer` from your editor against the Compose infrastructure, stop
its container and load `.env` into the shell; the Go services read environment
variables, not `.env` files:

```powershell
docker compose stop api
Get-Content .env | Where-Object { $_ -match '^\s*[^#\s][^=]*=' } | ForEach-Object {
    $name, $value = $_ -split '=', 2
    Set-Item "env:$($name.Trim())" $value.Trim()
}
go run ./cmd/api
```

In bash: `set -a; source .env; set +a; go run ./cmd/api`. The application settings
in `.env.example` already point at the published localhost ports.

## Troubleshooting

- **The consumer or api keeps restarting:** `docker compose logs consumer`. The usual
  causes are an empty `EMBEDDING_API_KEY` or an `EMBEDDING_DIMENSION` that differs
  from an existing Qdrant collection. Both are reported by name. Events wait in Kafka
  meanwhile; once fixed, `docker compose up -d consumer` resumes where it stopped.
- **`debezium-register` exited with an error:** `docker compose logs debezium-register`
  prints the connector's status and error trace.
- **Changed `EMBEDDING_DIMENSION` or the model:** existing vectors are incompatible,
  and the consumer refuses to start against the old collection. The simple fix is
  `docker compose down -v`. To keep the database, remove only the vector data and
  re-read the topic from the start:

  ```powershell
  docker compose stop consumer
  docker compose rm -sf qdrant; docker volume rm near-realtime-search_qdrant-data
  docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka:9092 `
    --group near-realtime-search --topic search.public.documents --reset-offsets --to-earliest --execute
  docker compose up -d qdrant consumer
  ```

  Kafka keeps events for 7 days by default, so this rebuilds everything only while
  the snapshot is younger than that; otherwise use `down -v`.
- **OpenSearch exits mentioning `vm.max_map_count`** (Linux or WSL hosts):
  `sysctl -w vm.max_map_count=262144` on the host; with Docker Desktop on Windows,
  `wsl -d docker-desktop -u root sysctl -w vm.max_map_count=262144`.
- **Scripts fail with `\r: command not found`:** the checkout converted line endings
  to CRLF. `.gitattributes` keeps shell and SQL files at LF for new clones; for an
  existing clone run `git add --renormalize .` and check the files out again.
- **Debezium was stopped for a long time:** its replication slot keeps WAL up to
  `max_slot_wal_keep_size` (1 GB). Past that, the slot is invalidated and the
  connector needs a new snapshot; `docker compose down -v` is the simple reset.

## Build and test

```sh
go build ./...
go test ./...                     # no Docker needed
go test -tags=integration ./...   # needs the stack; see the integration test files
```

### End-to-end tests

`tests/e2e` drives the whole pipeline against the running stack: it writes to
PostgreSQL and waits until the change is searchable through the HTTP API,
covering create, update, delete, the initial snapshot, idempotency and
dead-lettering. One command starts the stack, registers the connector and runs
them:

```powershell
./scripts/e2e.ps1      # Windows
scripts/e2e.sh         # macOS, Linux, Git Bash
```

They need no embedding API key, and `go test ./...` skips them unless `E2E` is
set. See [tests/e2e/README.md](tests/e2e/README.md).

## Metrics and dashboards

Both services expose Prometheus metrics, Prometheus scrapes them every 15
seconds, and Grafana draws them:

```text
api       :8080/metrics   HTTP traffic, search latency, the calls each search makes
consumer  :9091/metrics   Kafka, retries, dead letters, indexing, the worker pool
                ↓
           Prometheus :9090
                ↓
             Grafana :3000
```

Grafana comes up with its datasource and five dashboards already provisioned —
Overview, Search API, Kafka & Indexing, Dependencies and Runtime — so
`docker compose up -d` is the whole setup. Reading them needs no login; the
whole stack is local-development only. See [docs/grafana.md](docs/grafana.md).

```powershell
curl.exe -s http://localhost:8080/metrics | Select-String hybrid_search_search_requests_total
curl.exe -s "http://localhost:9090/api/v1/targets?state=active" | Select-String '"health":"up"'
```

Every metric is prefixed `hybrid_search_`, and every label is a bounded set —
method, route, status, operation, dependency, outcome, error_type — so a query
string, a document ID or an error message can never turn one metric into a
million time series. The Go runtime and process collectors come along for free
on the same endpoints.

Instrumentation is observational: the packages being measured know nothing
about Prometheus, they report through small hooks, and stopping Prometheus
loses the graphs rather than the search engine. `METRICS_ENABLED=false` turns
the endpoints off entirely. See [docs/metrics.md](docs/metrics.md) for every
metric, its labels and what it means.

### Benchmarks

Two different measurements, kept apart:

```sh
# what one function costs: rank fusion, event parsing, document mapping
go test -run '^$' -bench . -benchmem ./internal/search/rrf/ ./internal/indexing/cdc/
```

```powershell
# what the system does under traffic, against the running stack
./scripts/loadtest.ps1 -Suite search   -Scenario baseline   # k6 against the API
./scripts/loadtest.ps1 -Suite indexing -Count 1000          # PostgreSQL to both indexes
./scripts/loadtest.ps1 -Suite workers  -Workers 1,2,4,8     # where more workers stop helping
./scripts/loadtest.ps1 -Suite backpressure -Rate 1000       # produce faster than the consumer
./scripts/loadtest.ps1 -Suite recovery                      # stop OpenSearch under load
./scripts/cleanup-loadtest.ps1                              # delete what a run created
```

Benchmarks always run with `EMBEDDING_PROVIDER=fake`: the real provider's
network latency, rate limits and cost would be in every number and no two runs
would be comparable. Results go in
[benchmarks/results/README.md](benchmarks/results/README.md), with the machine
they were measured on — the template is deliberately empty until someone runs
them. See [benchmarks/README.md](benchmarks/README.md).
