# Near-Real-Time Hybrid Search Engine

Hybrid keyword + vector search built with Go, OpenSearch, Qdrant, Kafka and PostgreSQL.
Results from both engines are merged with Reciprocal Rank Fusion (RRF).

## Layout

- `cmd/api` – HTTP search API
- `cmd/consumer` – Kafka consumer that feeds the indexing pipeline
- `internal/api` – HTTP handlers and routes
- `internal/search` – OpenSearch and Qdrant clients, RRF fusion
- `internal/kafka` – Kafka consumer
- `internal/postgres` – PostgreSQL repository
- `internal/indexing` – indexing workers and pipeline
- `internal/indexing/cdc` – Debezium change events, normalized and applied to both indexes
- `internal/embedding` – embedding provider interface and the Gemini implementation
- `internal/config` – configuration loading
- `migrations/` – database migrations
- `benchmarks/` – benchmark code and results
- `docker/` – supporting Docker files

## Build

```sh
go build ./...
```
