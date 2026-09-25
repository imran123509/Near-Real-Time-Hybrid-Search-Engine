# Grafana dashboards

Five dashboards over the Prometheus metrics from
[docs/metrics.md](metrics.md). They are provisioned from files, so a fresh
`docker compose up -d` gives you a working datasource and every dashboard with
nothing to click.

```text
api :8080/metrics ─┐
                   ├─> Prometheus :9090 ─> Grafana :3000
consumer :9091/metrics ─┘
```

Nothing in the application depends on either. With Grafana or Prometheus
stopped, the API still searches and the consumer still indexes.

## Open it

```powershell
docker compose up -d
Start-Process http://localhost:3000       # or just open it in a browser
```

Reading the dashboards needs no login — anonymous access is Viewer-only and
this stack is local-development only. To edit anything, sign in with
`GRAFANA_ADMIN_USER` / `GRAFANA_ADMIN_PASSWORD` from your `.env` (defaults
`admin` / `admin`, and `.env` is git-ignored).

Grafana opens on the Overview dashboard. The rest are under
**Dashboards → Near Real-Time Search**.

| Dashboard | UID | Answers |
|---|---|---|
| Overview | `nrt-search-overview` | Is the system healthy, and is anything failing right now |
| Search API | `nrt-search-api` | How much search traffic, how fast, and which dependency is costing the time |
| Kafka & Indexing | `nrt-search-kafka-indexing` | Is the pipeline keeping up, what is retried, what is dead-lettered |
| Dependencies | `nrt-search-dependencies` | Every call to OpenSearch, Qdrant and the embedding provider, from both sides |
| Runtime | `nrt-search-runtime` | Goroutines, memory, CPU and GC — mainly during a benchmark |

Every dashboard defaults to the last 15 minutes and refreshes every 10
seconds; both are normal Grafana controls and can be changed per session.

## What is provisioned, and how

```text
docker/grafana/
├── provisioning/
│   ├── datasources/prometheus.yml   the Prometheus datasource, uid "prometheus"
│   └── dashboards/dashboards.yml    loads the folder below, rechecked every 30s
└── dashboards/
    ├── overview.json
    ├── search.json
    ├── kafka-indexing.json
    ├── dependencies.json
    └── runtime.json
```

The dashboards are mounted read-only and each carries a fixed UID, so
reloading updates the existing dashboard rather than adding a copy beside it.
Grafana has no volume: its datasource and dashboards come from files every
time it starts, so the only state a volume would keep is throwaway — an ad-hoc
panel or a changed password. That is what makes the setup reproducible.

### Changing a dashboard

Edit the panel in Grafana, then **Export → Save to file** (or copy the JSON
model) and write it over the file in `docker/grafana/dashboards/`. Keep the
`uid` and set `"id": null`. Grafana picks the file up within 30 seconds; an
edit left only in the browser is replaced on the next reload, on purpose.

## Reading the Overview

The top row is the health check:

| Panel | Healthy | Worth looking at |
|---|---|---|
| API ready / Consumer ready | Ready | Not ready: a store stopped answering |
| Search requests/sec | whatever your traffic is | — |
| Search error rate | 0 | anything sustained |
| Search p95 | steady | climbing while traffic is flat |
| Dead letters/sec | 0 | anything above 0 |

Below it: search latency, what Kafka is delivering against what the pipeline
is finishing, retries and dead letters, the indexing queue against its
capacity, and workers active against the pool size.

Every panel has a description (hover the **i**) explaining what it shows and
what a change in it means. The thresholds — amber at 500 ms, red at 2 s for
latency; amber at 1%, red at 5% for errors — are **starting points for this
local stack, not a service level objective**. Set your own from a baseline in
`benchmarks/results/README.md`.

## Seeing the system do something

### Search traffic

```powershell
./scripts/loadtest.ps1 -Suite search -Scenario staged
```

Open **Search API**. Requests/sec, the latency percentiles and the status
breakdown follow the load. The k6 summary and the dashboard will not match
exactly and are not meant to: k6 counts requests it sent, the API counts
requests it answered, and anything that fails before reaching the handler is
in one and not the other. See the cross-check note in
[benchmarks/README.md](../benchmarks/README.md).

### Indexing

```powershell
./scripts/loadtest.ps1 -Suite indexing -Count 2000
```

Open **Kafka & Indexing**. Consumed and finished rise together, the queue
stays near zero if the workers keep up, and *Indexing events/sec by effect*
shows `indexed` while the rows are new.

### Retries, dead letters and recovery

```powershell
docker compose stop opensearch
./scripts/loadtest.ps1 -Suite indexing -Count 200
```

On **Kafka & Indexing**, in this order: *Indexing failures by dependency*
rises for `opensearch`, then *Retries/sec*, then — once attempts run out —
*Retries exhausted* and *Dead letters/sec* with reason `retry_exhausted`.
*Processing duration* stretches, because the retry backoff is inside it.
Meanwhile **Overview** shows *Consumer ready* drop to Not ready.

```powershell
docker compose start opensearch
```

Failure rates return to zero and *Consumer ready* goes back to Ready.
Events that were dead-lettered stay dead-lettered: they need a replay, which
the dead-letter section of the [README](../README.md) describes.

The one line that means something is going nowhere is **dead-letter publish
failed**: those messages reached neither the indexes nor the dead-letter
topic, their offsets are not committed, and they will be delivered again.

### Backpressure

```powershell
./scripts/loadtest.ps1 -Suite backpressure -Count 20000 -Rate 1000
```

The *Backpressure* panel at the bottom of **Kafka & Indexing** puts arrivals,
completions and queue depth on one graph. Arrivals run ahead, the queue fills
to capacity, and then the consumer stops fetching — the backlog waits in
Kafka. Check **Runtime** at the same time: consumer memory should stay flat,
which is the claim the bounded queue makes.

## Deliberate gaps

- **No alerting.** Metrics, then dashboards. Alert rules are their own
  decision and come later.
- **No PostgreSQL or Kafka broker metrics.** The application measures the
  calls it makes, not the services themselves. Consumer lag comes from Kafka's
  own tooling (`scripts/kafka-lag.ps1`, or `go run ./cmd/loadgen -mode watch`).
  Adding exporters is infrastructure monitoring, and inventing metrics to fill
  a dashboard is worse than an honest gap.
- **No embedding provider label.** The dependency is labelled `embedder`; which
  provider is behind it is in `application_info` and the startup log.
  Benchmarks run with `EMBEDDING_PROVIDER=fake`.

## If a dashboard is empty

1. **Is Prometheus scraping?** `http://localhost:9090/targets` — both
   `hybrid-search-api` and `hybrid-search-consumer` should be *up*.
2. **Are the services exposing metrics?**
   `curl.exe -s http://localhost:8080/metrics | Select-String hybrid_search_`
   and the same for `http://localhost:9091/metrics`. If empty, check
   `METRICS_ENABLED` in `.env`.
3. **Is the datasource healthy?** Grafana → Connections → Data sources →
   Prometheus → *Save & test*. It should say the datasource is working.
4. **Has anything happened yet?** Counters only appear once something has been
   recorded. Run a search or insert a row.
5. **Port already in use?** Set `GRAFANA_HOST_PORT` in `.env`, then
   `docker compose up -d grafana`.

```powershell
docker compose logs --tail=50 grafana
docker compose logs --tail=50 prometheus
```
