# End-to-end tests

These tests run against the real stack from `docker-compose.yml`. Nothing is
mocked: they write to PostgreSQL and then wait for the change to travel through
Debezium, Kafka, the consumer, OpenSearch, the embedding provider and Qdrant,
until the HTTP API returns it.

```text
PostgreSQL ──WAL──> Debezium ──> Kafka ──> consumer ──┬──> OpenSearch
                                                       └──> embedding ──> Qdrant
                                                                  │
                          GET /api/v1/search <── RRF <────────────┘
```

They never need an embedding API key: by default the stack runs with
`EMBEDDING_PROVIDER=fake`, which produces deterministic local vectors.

## Prerequisites

- Docker Desktop with Compose v2
- Go
- `.env` in the repository root (`cp .env.example .env`). No API key needed for
  the default run.

## Run everything with one command

```powershell
./scripts/e2e.ps1              # Windows
```

```bash
scripts/e2e.sh                 # macOS, Linux, Git Bash
```

The script starts the stack with the fake provider, makes sure the Debezium
connector is registered, runs the tests, and prints the tail of the service
logs if anything fails. It leaves the stack running; add `-Down`
(PowerShell) or `E2E_DOWN=1` (bash) to stop it afterwards. Volumes are never
removed by the script.

## Run the tests by hand

```powershell
$env:EMBEDDING_PROVIDER="fake"
docker compose up -d --build

$env:E2E="1"
go test ./tests/e2e -v -timeout 10m
```

Without `E2E` the package compiles and reports success without doing anything,
so `go test ./...` never needs Docker:

```text
E2E is not set: skipping the end-to-end suite. See tests/e2e/README.md.
ok      near-real-time-hybrid-search-engine/tests/e2e
```

Stop the stack when you are done:

```powershell
docker compose down       # keeps the data
docker compose down -v    # deletes the database, topics and indexes
```

## What the tests cover

| Test | What it proves |
|---|---|
| `TestDocumentCreateEndToEnd` | An `INSERT` reaches OpenSearch (fields intact), Qdrant (right dimension and payload) and the search API, with a score |
| `TestDocumentUpdateEndToEnd` | An `UPDATE` replaces the indexed title and content, raises the version, re-embeds the text, and the old content stops matching |
| `TestDocumentDeleteEndToEnd` | A `DELETE` removes the document from both indexes and from search results |
| `TestInitialSnapshot` | The rows seeded before the connector started arrived through Debezium's snapshot (`op: "r"`) |
| `TestDuplicateEventIsIdempotent` | Replaying an already-applied change onto the topic leaves exactly one document, one point, and the newest content |
| `TestUnprocessableEventGoesToTheDeadLetterQueue` | An event the consumer cannot parse lands on the dead-letter topic with its payload and origin intact, and consumption carries on |
| `TestDuplicateCreate`, `TestDuplicateUpdate`, `TestDuplicateDelete` | Replaying a CREATE, UPDATE or DELETE leaves one document, one point and one search result, with the latest row |
| `TestStaleEventDoesNotOverwriteNewerData` | An event from before the last update is refused by both indexes, vector included |
| `TestConsumerRestartRecovery` | Restarting the consumer mid-stream loses nothing and duplicates nothing: it catches up and rejects the replayed old event |
| `TestOpenSearchFailureRecovery`, `TestQdrantFailureRecovery`, `TestRetryIdempotency` | Opt-in: with one search index stopped, nothing is lost or duplicated, and both indexes converge on the newest row once it is back |
| `TestMetricsEndpointsAreServed`, `TestSearchMovesTheMetrics`, `TestIndexingMovesTheMetrics` | Both services expose Prometheus metrics, and searching and indexing move the counters that describe them |
| `TestDocumentIDsNeverReachLabels`, `TestRejectedRequestsAreCountedSeparately` | No document ID or search term reaches a metric label, and a rejected request is a 4xx rather than a search failure |
| `TestRealEmbeddingProvider` | Opt-in: finds a document by meaning alone, with words it does not contain |

Every test creates its own document with a unique ID and a unique marker word,
so runs never collide, and deletes it afterwards. Cleanup is best effort and
never hides a failure.

The tests assert that the expected document is **among** the results, not that
it ranks first: RRF ordering depends on the rest of the corpus, and asserting a
position would make them brittle.

## Waiting, not sleeping

The pipeline is eventually consistent, so the tests poll with a timeout instead
of sleeping for a fixed time. `eventually` in `helpers.go` re-checks a condition
until it holds, and reports the last failure when it times out, which is what
tells you how far the change travelled.

Replaying an event that is already applied changes nothing, so there is nothing
to poll for. The idempotency tests publish one unprocessable message with the
same key afterwards and wait for it on the dead-letter topic: events for one key
are processed in order, so once it lands there the replays in front of it are
finished, and the indexes can be checked. That barrier is why those tests leave
a message on `search.public.documents.dlq`.

## Failure injection

Three tests take a search index away from the running consumer with
`docker compose stop`, make a change, and start it again:

```powershell
$env:E2E="1"; $env:E2E_FAILURE_INJECTION="1"
go test ./tests/e2e -run "FailureRecovery|RetryIdempotency" -v -timeout 20m
```

They are opt-in because starting a container takes longer than the consumer's
retry window, so a run takes minutes, and because the rest of the stack is
degraded while a service is down. Each test brings its service back in a
cleanup that runs even when the test fails, and waits until it serves again.
Only the named service is ever stopped: nothing removes a container, a volume
or a network.

Whether the change survives its retries or reaches the dead-letter topic first
depends on how long the restart takes, so the tests handle both: anything
dead-lettered for that document is replayed from the envelope's payload, which
is what a reprocessor would do. The assertion afterwards is the same either
way — one document, one point, the newest row.

Failure is injected by stopping the real service. Nothing in `cmd/` or
`internal/` knows these tests exist.

## Settings

| Variable | Default | Meaning |
|---|---|---|
| `E2E` | (unset) | Must be set for the suite to run |
| `E2E_API_URL` | `http://localhost:8080` | Search API |
| `E2E_DATABASE_URL` | `postgres://postgres:postgres@localhost:5432/searchdb?sslmode=disable` | Source database |
| `E2E_OPENSEARCH_URL` | `http://localhost:9200` | |
| `E2E_QDRANT_URL` | `http://localhost:6333` | REST port |
| `E2E_CONNECT_URL` | `http://localhost:8083` | Kafka Connect REST API |
| `E2E_KAFKA_BROKERS` | `localhost:29092` | For the replay in the idempotency test |
| `E2E_TOPIC` | `search.public.documents` | |
| `E2E_DLQ_TOPIC` | `<E2E_TOPIC>.dlq` | Where the dead-letter test reads from |
| `E2E_METRICS_URL` | `<E2E_API_URL>/metrics` | The API's Prometheus endpoint |
| `E2E_CONSUMER_METRICS_URL` | `http://localhost:9091/metrics` | The consumer's Prometheus endpoint |
| `E2E_OPENSEARCH_INDEX` / `E2E_QDRANT_COLLECTION` | `documents` | |
| `E2E_VECTOR_SIZE` | `768` | Must match `EMBEDDING_DIMENSION` |
| `E2E_TIMEOUT` | `90s` | How long one "eventually" waits |
| `E2E_POLL_INTERVAL` | `1s` | How often it re-checks |
| `E2E_DUMP_LOGS` | `1` | Print service logs when the suite fails; `0` disables |
| `E2E_REAL_EMBEDDING` | (unset) | `true` runs the semantic smoke test |
| `E2E_FAILURE_INJECTION` | (unset) | Set it to run the tests that stop and start a search index |

Change the host ports in `.env` (`POSTGRES_HOST_PORT` and friends) if something
already uses them, and point the matching `E2E_*` variable at the new port.

## When a test fails

A failure names the service, the document, the condition and the timeout, for
example:

```text
opensearch: document 7a1e... did not reach index "documents" as expected within
1m30s: condition not met within 1m30s (90 attempts): document is not in the index yet
```

It then prints where the document actually is, which says where the pipeline
stopped:

```text
pipeline state for document 7a1e...:
  postgres:   present, title "E2E create e2e6f1c..."
  opensearch: absent
  qdrant:     absent
  connector:  RUNNING, tasks RUNNING
```

and the last 200 log lines from `consumer`, `api` and `debezium`.

Useful commands while debugging:

```powershell
docker compose ps
docker compose logs -f consumer
docker compose logs -f api
docker compose logs --tail=100 debezium
docker compose logs --tail=100 kafka
curl http://localhost:8083/connectors/documents-cdc/status
```

Common causes:

- **The consumer is restarting.** With the fake provider it should not be; check
  `docker compose logs consumer` for a dimension mismatch against an existing
  Qdrant collection from an earlier run with a different `EMBEDDING_DIMENSION`.
- **The connector is not running.** `scripts/register-debezium.ps1` re-registers
  it and prints its status.
- **Vectors were written by a different provider.** Fake and real vectors are
  not comparable. Switching between them is fine for these tests, which check
  that documents are found, but `docker compose down -v` gives a clean start.

## Running against the real embedding provider

```powershell
./scripts/e2e.ps1 -RealEmbedding
```

This starts the stack with the provider from `.env` (so `EMBEDDING_API_KEY`
must be set), and also runs `TestRealEmbeddingProvider`, which searches with
words the document does not contain. It costs embedding API calls.
