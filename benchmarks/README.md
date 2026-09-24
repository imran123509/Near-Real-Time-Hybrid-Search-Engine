# Benchmarks

Two different questions are measured here, and they are not the same thing:

```text
Go benchmark   what one function costs        go test -bench
Load test      what the system does under     k6 + cmd/loadgen
               traffic, end to end
```

A Go benchmark can tell you that fusing two result lists takes microseconds. It
cannot tell you what the search API does at 50 concurrent users, because almost
all of that time is spent waiting on OpenSearch, an embedding provider and
Qdrant. Both are here; do not quote one as if it were the other.

The point of this suite is **not** a big number. It is a baseline you can
repeat, so that the next change can be shown to have helped.

## Contents

```text
benchmarks/
├── README.md              this file
├── search/
│   ├── search.js          the k6 search benchmark
│   └── scenarios.js       query set, thresholds, shared configuration
└── results/
    └── README.md          where measurements are recorded

cmd/loadgen/               the indexing benchmarks (Go)
internal/loadtest/         their helpers, unit-tested without Docker
scripts/loadtest.*         one command per suite
scripts/cleanup-loadtest.* delete what a run created
scripts/kafka-lag.*        consumer lag, from Kafka's own tooling
```

The indexing side is a Go program rather than a k6 script because it does not
speak HTTP: it writes rows to PostgreSQL, publishes to Kafka and watches both
search indexes. It lives in `cmd/` with the other commands, and its helpers in
`internal/`, like everything else in this repository.

## Prerequisites

- Go (the version in `go.mod`)
- Docker Desktop with Compose v2
- [k6](https://grafana.com/docs/k6/latest/set-up/install-k6/) for the search
  benchmarks: `winget install k6` on Windows, `brew install k6` on macOS
- `.env` in the repository root (`cp .env.example .env`)

No embedding API key is needed, and none should be used — see below.

## Start the system

```powershell
$env:EMBEDDING_PROVIDER="fake"
docker compose up -d --build
docker compose ps
```

**Always benchmark with the fake embedding provider.** It produces
deterministic local vectors with no network call, no rate limit and no cost.
Benchmarking against the real provider measures somebody else's service:
network latency, throttling and availability end up in your numbers, the run
costs money, and two runs are never comparable. The production provider is
unchanged and still used everywhere else; `scripts/loadtest.ps1` warns if the
stack is not running with `fake`.

If you do want to measure the real provider, measure *it*, on its own, with a
small number of requests, and say so in the results.

## The five benchmarks

Each answers one question. Keep them apart: a single number that mixes search
latency with indexing throughput cannot be interpreted.

### A. Search API

```powershell
./scripts/loadtest.ps1 -Suite search -Scenario baseline   # 10 VUs, 30s
./scripts/loadtest.ps1 -Suite search -Scenario staged     # rising load
./scripts/loadtest.ps1 -Suite search -Scenario limits     # limit=5,10,20,50
./scripts/loadtest.ps1 -Suite search -Scenario rps        # fixed arrival rate
./scripts/loadtest.ps1 -Suite search -Scenario validation # rejections, not speed
```

or k6 directly:

```bash
k6 run benchmarks/search/search.js
k6 run -e LOADTEST_SCENARIO=staged -e LOADTEST_MAX_VUS=100 benchmarks/search/search.js
```

Every scenario begins with a warm-up phase tagged `phase:warmup` and excluded
from the thresholds. Requests are valid traffic; invalid requests live in the
`validation` scenario alone, because a benchmark whose error rate includes
deliberate 400s tells you nothing about reliability.

Queries come from a fixed set (`LOADTEST_QUERY_SET=topics|phrases|mixed|marker`)
and are picked by counting rather than at random, so two runs send the same
requests in the same order.

### B. Indexing pipeline

```powershell
./scripts/loadtest.ps1 -Suite indexing -Count 1000
go run ./cmd/loadgen -mode index -count 10000 -batch 500
```

This writes rows to PostgreSQL and waits for them to appear in the keyword
index, so it measures the whole path — WAL, Debezium, Kafka, the worker pool,
the embedding provider, OpenSearch and Qdrant — not a stub. It reports
documents indexed per second, per-document end-to-end latency percentiles, and
how much each index grew, which is what shows nothing was duplicated or lost.

Scale it deliberately: 1,000 first, then 10,000, then 100,000 only if the
machine can take it. The default is 1,000.

### C. Worker scaling

```powershell
./scripts/loadtest.ps1 -Suite workers -Workers 1,2,4,8,16
```

Restarts the consumer at each `INDEXING_WORKERS` setting and repeats the
indexing benchmark. More workers is not automatically faster: at some point the
bottleneck moves to OpenSearch, the embedding provider or the machine's CPUs,
and past it more workers only add contention. The point is to find where that
happens here, not to assume it.

### D. Backpressure

```powershell
./scripts/loadtest.ps1 -Suite backpressure -Count 20000 -Rate 1000
```

Publishes change events straight to Kafka, faster than the consumer can process
them, and prints rows, index sizes, lag and dead-letter count every two
seconds. Run `docker stats` beside it.

What to look for: **lag grows, memory does not.** Kafka is the buffer; the
consumer's queue is bounded and its workers apply backpressure to the fetch
loop, so a backlog costs disk on the broker rather than memory in the consumer.
Lag falling back to zero after the producer stops is the proof that the backlog
drains.

### E. Failure recovery

```powershell
./scripts/loadtest.ps1 -Suite recovery -Count 5000 -Rate 500
```

Stops OpenSearch under load, waits, starts it again, and watches the pipeline
recover. Expect failures, then retries, then dead-lettered events once the
retries run out — that is the designed behaviour, and the dead-letter count is
one of the numbers to record. Events that were dead-lettered need a replay to
reach the indexes; see the dead-letter section in the root README.

### Go micro-benchmarks

```bash
go test -run '^$' -bench . -benchmem ./internal/search/rrf/
go test -run '^$' -bench . -benchmem ./internal/indexing/cdc/
```

These measure the CPU-bound parts: rank fusion, Debezium parsing, the row to
document mapping, the embedding text rule and event identity. They need no
Docker. Use `-count 6` and `benchstat` to compare two implementations
honestly.

## How to run a benchmark you can trust

1. **Start the stack** with `EMBEDDING_PROVIDER=fake` and wait for
   `docker compose ps` to show everything healthy.
2. **Load data.** The search benchmark needs a corpus; the indexing benchmark
   creates its own. For search, run the indexing benchmark first
   (`-Suite indexing -Count 5000`) so there is something to find.
3. **Warm up.** Every scenario does this on its own: k6 runs a low-load phase
   first, and the indexing benchmark indexes `-warmup` documents before it
   starts measuring. Cold connection pools, empty OpenSearch caches and a JVM
   that has not compiled anything yet are not what you want to measure.
4. **Run the benchmark**, with nothing else heavy on the machine.
5. **Repeat it.** `-Runs 3`, then report the median. A single run is a sample
   of one, and the fastest of three is not what the system does.
6. **Record the results** in `results/README.md`, with the environment.
7. **Clean up**: `./scripts/cleanup-loadtest.ps1`.

## Configuration

Everything has a default that suits a developer machine. Nothing defaults to a
production-sized load.

| Variable | Used by | Default | Meaning |
|---|---|---|---|
| `LOADTEST_BASE_URL` | k6 | `http://localhost:8080` | the API under test |
| `LOADTEST_SCENARIO` | k6 | `baseline` | baseline, staged, limits, rps, validation |
| `LOADTEST_VUS` | k6 | `10` | concurrent users in baseline and limits |
| `LOADTEST_MAX_VUS` | k6 | `50` | the top step of the staged scenario |
| `LOADTEST_DURATION` | k6 | `30s` | measured duration per scenario |
| `LOADTEST_WARMUP` | k6 | `15s` | warm-up before measuring |
| `LOADTEST_TARGET_RPS` | k6 | `50` | arrival rate for the rps scenario |
| `LOADTEST_LIMIT` / `LOADTEST_LIMITS` | k6 | `10` / `5,10,20,50` | page sizes |
| `LOADTEST_QUERY_SET` | k6 | `topics` | topics, phrases, mixed, marker |
| `LOADTEST_MARKER` | k6 | — | the run marker, for `QUERY_SET=marker` |
| `LOADTEST_DOCUMENT_COUNT` | loadgen | `1000` | documents per indexing run |
| `LOADTEST_BATCH_SIZE` | loadgen | `100` | rows per insert statement |
| `LOADTEST_WARMUP_DOCS` | loadgen | `50` | extra documents indexed before measuring |
| `LOADTEST_TARGET_RPS` | loadgen | `0` | publish rate; 0 is as fast as possible |
| `LOADTEST_REPEATS` | loadgen | `1` | deliveries per event (duplicates) |
| `LOADTEST_POLL_INTERVAL` | loadgen | `250ms` | how often arrival is checked |
| `LOADTEST_RUN_ID` | loadgen | timestamp | the same id regenerates the same documents |
| `LOADTEST_DATABASE_URL` | loadgen | localhost:5432 | where rows are written |
| `LOADTEST_KAFKA_BROKERS` | loadgen | `localhost:29092` | where events are published |
| `LOADTEST_OPENSEARCH_URL` | loadgen | `http://localhost:9200` | watched for arrivals |
| `LOADTEST_QDRANT_URL` | loadgen | `http://localhost:6333` | the REST port, not gRPC |
| `INDEXING_WORKERS` | compose | `10` | consumer workers; the worker suite varies it |

`go run ./cmd/loadgen -h` lists the flags, which override the variables.

The defaults assume the stack publishes its default host ports. If you changed
them in `.env` because something else already uses 5432 or 9200 — a local
PostgreSQL, for instance — point the benchmark at the new ones, or it will
happily connect to the wrong database:

```powershell
$env:LOADTEST_DATABASE_URL = "postgres://postgres:<password>@localhost:5433/searchdb?sslmode=disable"
$env:LOADTEST_KAFKA_BROKERS = "localhost:29092"
```

## Duplicate delivery

Because indexing is idempotent, delivering the same event repeatedly should
cost throughput and change nothing else:

```powershell
go run ./cmd/loadgen -mode duplicate -count 1000 -repeats 3
go run ./cmd/loadgen -mode watch -interval 2s -timeout 2m
```

The index sizes in the `watch` output must not grow with the repeats. If they
do, something about the deterministic IDs has broken, and the number to record
is that failure, not the throughput.

## Cross-checking with the metrics

The services expose Prometheus metrics while a benchmark runs, so a load test
can be checked against what the system itself reports:

```powershell
curl.exe -s http://localhost:8080/metrics | Select-String "search_requests_total|http_request_duration_seconds_count"
curl.exe -s http://localhost:9091/metrics | Select-String "kafka_messages_total|indexing_queue_size"
```

Expect them to agree roughly, not exactly. k6 counts requests it sent, and the
API counts requests it answered: a connection refused or a request that timed
out client-side never reaches the server's counter. A gap in one direction is
normal; a large gap is itself a finding. See [docs/metrics.md](../docs/metrics.md).

During the backpressure and recovery runs, `hybrid_search_indexing_queue_size`,
`hybrid_search_kafka_retries_total` and `hybrid_search_kafka_dlq_messages_total`
show the same story as the watcher's output, sampled by Prometheus rather than
printed.

## Reading the results

**Percentiles.** `p50` is what a typical request sees. `p95` and `p99` are the
tail, and in a system like this one they matter more: a search fans out to the
keyword index, the embedding provider and the vector index and waits for all of
them, so a dependency that is slow one time in twenty shows up in far more than
one request in twenty. An average hides exactly that.

**Observed is not sustainable.** The highest throughput a run reached is not
capacity. Sustainable throughput is the rate the system holds with a stable
error rate, a stable tail latency and no growing queue. Record both, and say
which is which.

**Say where the number came from.** Not:

> the system supports 10,000 requests/sec

but:

> on this machine, with 5,000 documents indexed, the fake embedding provider
> and 50 concurrent users, the API served 207 req/s with p95 91 ms and a 0.08%
> error rate

Performance numbers belong to an environment and a workload. Without both they
cannot be compared with anything, including a later run of the same benchmark.

## When something is slow

Measure before assuming. The candidates, roughly in the order they are worth
checking here:

| Suspect | How to tell |
|---|---|
| Embedding provider | search latency drops sharply with `EMBEDDING_PROVIDER=fake` |
| OpenSearch | `docker stats` shows it at its CPU limit; its JVM heap is the default 512 MB |
| Qdrant | indexing throughput falls while OpenSearch stays idle |
| Worker pool | the worker suite shows throughput still rising at the top setting |
| Kafka | lag grows while workers are idle |
| PostgreSQL | insert rate in the indexing report is the low number |
| Connection pools | latency rises with concurrency while every service is idle |
| The machine | Docker Desktop's CPU and memory limits, which are usually the real ceiling |

Then change **one** thing, and run the same benchmark again:

```text
before   throughput 180 req/s   p95 120 ms
after    throughput 240 req/s   p95  95 ms
         improvement (240-180)/180 = +33%
```

Do not add bulk indexing, batched upserts, Kafka tuning or caching because they
are usually good ideas. Add them when a benchmark shows the current behaviour is
the problem, and show the before and after. An optimisation without a
measurement is a guess with extra code.

## Safety

These benchmarks are for the local stack only. Do not point them at a shared
environment, a real OpenSearch or Qdrant cluster, or the real embedding
provider: they generate sustained write load and would cost money or take down
something somebody else is using. Every default here targets `localhost`.

Cleanup never removes containers, volumes or images. It deletes the rows the
benchmark created, found by their generated URL prefix, and nothing else.
