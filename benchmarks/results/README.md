# Results

**This file is a template. Every number in it is blank on purpose.**

Nothing here has been measured yet: no benchmark has been run against a live
stack. Fill a section in only from a run you did yourself, with the environment
that produced it. An invented or copied number is worse than no number, because
the next person will compare against it.

How to produce the numbers: [../README.md](../README.md). k6 also writes the
full run to `benchmarks/results/<scenario>-<timestamp>.json`, which is ignored
by git — keep the summary here, not the raw file.

---

## Environment

Record this once per machine, and again whenever it changes. Everything below
is meaningless without it.

```text
Date:
OS:
CPU (cores):
RAM:
Docker version:
Docker Desktop CPU limit:
Docker Desktop memory limit:
Go version:

Stack (from docker compose ps / images):
  PostgreSQL:
  Kafka:
  Debezium:
  OpenSearch:
  Qdrant:

Embedding provider:      fake        (anything else makes runs incomparable)
Documents indexed:
Consumer INDEXING_WORKERS:
```

---

## A. Search API

One table per scenario. Run each three times and record the median; note if the
runs disagreed by more than a few percent.

### Baseline

```text
Scenario:        baseline
Concurrency:     ... VUs
Warm-up:         ... (excluded)
Duration:        ...
Query set:       topics
limit:           10

Requests:
Throughput:              req/s
Error rate:              %

p50:                     ms
p90:                     ms
p95:                     ms
p99:                     ms
max:                     ms

Notes:
```

### Staged load

| VUs | Throughput (req/s) | p50 (ms) | p95 (ms) | p99 (ms) | Error rate |
|---|---|---|---|---|---|
| 10 | | | | | |
| 25 | | | | | |
| 50 | | | | | |
| 100 | | | | | |
| 200 | | | | | |

```text
Observed throughput:        req/s at ... VUs
Sustainable throughput:     req/s   (stable tail latency, no error rate growth)
Where it started to degrade:
Notes:
```

### Page size

| limit | Throughput (req/s) | p50 (ms) | p95 (ms) | p99 (ms) |
|---|---|---|---|---|
| 5 | | | | |
| 10 | | | | |
| 20 | | | | |
| 50 | | | | |

```text
Effect of page size on latency:
Effect on CPU / memory (docker stats):
```

### Validation

Correctness, not performance. Every case should answer as listed.

| Request | Expected | Observed |
|---|---|---|
| `?q=` | 400 | |
| no `q` | 400 | |
| `?q=kafka&limit=-1` | 400 | |
| `?q=kafka&limit=100000` | 400 | |
| `?q=kafka&limit=ten` | 400 | |
| unknown path | 404 | |
| `/health`, `/ready` | 200 | |

---

## B. Indexing pipeline

```text
Documents produced:
Batch size:
Warm-up documents:
Poll interval:              (bounds single-document latency resolution)

Produced:                   rows in ...  (... rows/sec into PostgreSQL)
Indexed:
Missing:
Wall clock:
Throughput:                 events/sec

End-to-end latency per document:
  p50:                      ms
  p90:                      ms
  p95:                      ms
  p99:                      ms
  max:                      ms

Index growth:               ... documents, ... points   (both should equal produced)
Dead-lettered during run:
Notes:
```

Repeat at 1,000 / 10,000 / 100,000 documents if the machine allows, and record
each separately — throughput usually does not stay flat across scales.

| Documents | Throughput (events/s) | p95 latency (ms) | Missing | DLQ |
|---|---|---|---|---|
| 1,000 | | | | |
| 10,000 | | | | |
| 100,000 | | | | |

---

## C. Worker scaling

Same indexing run at each `INDEXING_WORKERS` setting.

| Workers | Throughput (events/s) | p95 latency (ms) | Peak consumer CPU | Peak consumer memory | Max lag |
|---|---|---|---|---|---|
| 1 | | | | | |
| 2 | | | | | |
| 4 | | | | | |
| 8 | | | | | |
| 16 | | | | | |

```text
Throughput stopped improving at:      workers
What became the bottleneck there:
Setting chosen for the default, and why:
```

---

## D. Backpressure

Producer deliberately faster than the consumer.

```text
Publish rate:               events/sec
Events published:
Consumer throughput:        events/sec

Peak Kafka lag:
Time for lag to return to zero after the producer stopped:

Consumer memory at start:
Consumer memory at peak lag:          (it should not grow with the backlog)
Consumer queue behaviour:
Processing latency during the backlog:
Notes:
```

The claim to check here is that Kafka holds the backlog and the consumer's
memory stays flat, because its queue is bounded. Record what actually happened.

---

## E. Failure recovery

```text
Load at the time:           events/sec
Service stopped:            opensearch
Time it was down:

Events affected:
Retries observed:
Dead-lettered:
Time from restart until lag returned to zero:

Final state: documents / points / rows
Consistent (documents == points == rows)?      yes / no
Events needing a replay from the dead-letter topic:
Notes:
```

---

## Go micro-benchmarks

CPU-bound components only. These do not need Docker and are not a system
measurement.

```text
go test -run '^$' -bench . -benchmem ./internal/search/rrf/
go test -run '^$' -bench . -benchmem ./internal/indexing/cdc/
```

| Benchmark | ns/op | B/op | allocs/op |
|---|---|---|---|
| `BenchmarkFuse/size=10` | | | |
| `BenchmarkFuse/size=200` | | | |
| `BenchmarkParseChangeEvent/long` | | | |
| `BenchmarkDocumentMapping` | | | |
| `BenchmarkBuildEmbeddingText` | | | |

---

## Optimisations

One entry per change, with the measurement that justified it. No entry without
a before and an after from the same benchmark on the same machine.

### (template)

```text
Change:
Why (which benchmark showed the problem):

Before:   throughput ...   p95 ...
After:    throughput ...   p95 ...
Change:   ... %

Benchmark and environment used for both:
```
