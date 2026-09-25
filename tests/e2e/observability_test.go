package e2e

import (
	"encoding/json"
	"fmt"
	"net/url"
	"strings"
	"testing"
)

// These tests check the observability stack is wired up the way the files in
// docker/prometheus and docker/grafana say it is: Prometheus scraping both
// services, Grafana holding a working Prometheus datasource, and every
// dashboard provisioned without anyone importing anything.
//
// They are about the plumbing, not about the numbers. What the numbers should
// do when the system works is checked in metrics_test.go.

// dashboardUIDs are the dashboards docker/grafana/dashboards provisions. A
// dashboard that fails to load -- malformed JSON, a duplicate uid -- simply
// does not appear, which is exactly what this catches.
var dashboardUIDs = map[string]string{
	"nrt-search-overview":       "Overview",
	"nrt-search-api":            "Search API",
	"nrt-search-kafka-indexing": "Kafka & Indexing",
	"nrt-search-dependencies":   "Dependencies",
	"nrt-search-runtime":        "Runtime",
}

// TestPrometheusScrapesBothServices checks Prometheus found the API and the
// consumer. Everything else here depends on it.
func TestPrometheusScrapesBothServices(t *testing.T) {
	ctx := t.Context()

	var out struct {
		Status string `json:"status"`
		Data   struct {
			ActiveTargets []struct {
				Health string `json:"health"`
				Labels struct {
					Job     string `json:"job"`
					Service string `json:"service"`
				} `json:"labels"`
				LastError string `json:"lastError"`
			} `json:"activeTargets"`
		} `json:"data"`
	}
	status, err := getJSON(ctx, env.PrometheusURL+"/api/v1/targets?state=active", &out)
	if err != nil || status != 200 {
		t.Fatalf("Prometheus targets: status %d, err %v", status, err)
	}

	healthy := map[string]bool{}
	for _, target := range out.Data.ActiveTargets {
		if target.Health != "up" && strings.HasPrefix(target.Labels.Job, "hybrid-search") {
			t.Errorf("target %s (%s) is %s: %s",
				target.Labels.Job, target.Labels.Service, target.Health, target.LastError)
		}
		if target.Health == "up" {
			healthy[target.Labels.Service] = true
		}
	}
	for _, service := range []string{"api", "consumer"} {
		if !healthy[service] {
			t.Errorf("Prometheus is not scraping the %s service; targets = %+v", service, out.Data.ActiveTargets)
		}
	}
}

// TestPrometheusHasApplicationMetrics checks the scraped data is this
// application's, not just any target answering.
func TestPrometheusHasApplicationMetrics(t *testing.T) {
	for _, query := range []string{
		"hybrid_search_application_info",
		"hybrid_search_application_ready",
		"hybrid_search_indexing_workers_total",
	} {
		results := promQuery(t, query)
		if len(results) == 0 {
			t.Errorf("Prometheus holds no data for %s", query)
		}
	}

	// Both services must be reporting, which is what the dashboards' service
	// selector is built on.
	services := map[string]bool{}
	for _, result := range promQuery(t, "hybrid_search_application_info") {
		services[result.Metric["service"]] = true
	}
	for _, service := range []string{"api", "consumer"} {
		if !services[service] {
			t.Errorf("no application_info from the %s service; Prometheus sees %v", service, services)
		}
	}
}

// TestGrafanaIsProvisioned checks Grafana came up with its datasource and
// dashboards already configured, which is the point of provisioning them from
// files: a fresh container needs no clicking.
func TestGrafanaIsProvisioned(t *testing.T) {
	ctx := t.Context()

	t.Run("grafana is healthy", func(t *testing.T) {
		var health struct {
			Database string `json:"database"`
			Version  string `json:"version"`
		}
		status, err := getJSON(ctx, env.GrafanaURL+"/api/health", &health)
		if err != nil || status != 200 {
			t.Fatalf("Grafana health: status %d, err %v", status, err)
		}
		if health.Database != "ok" {
			t.Errorf("Grafana database is %q, want ok", health.Database)
		}
		t.Logf("Grafana %s", health.Version)
	})

	t.Run("every dashboard is loaded", func(t *testing.T) {
		var found []struct {
			UID   string `json:"uid"`
			Title string `json:"title"`
			Type  string `json:"type"`
		}
		status, err := getJSON(ctx, env.GrafanaURL+"/api/search?type=dash-db&limit=100", &found)
		if err != nil || status != 200 {
			t.Fatalf("Grafana dashboard search: status %d, err %v", status, err)
		}

		loaded := map[string]string{}
		for _, dashboard := range found {
			loaded[dashboard.UID] = dashboard.Title
		}
		for uid, name := range dashboardUIDs {
			title, ok := loaded[uid]
			if !ok {
				t.Errorf("the %s dashboard (%s) was not provisioned; Grafana has %v", name, uid, loaded)
				continue
			}
			if !strings.Contains(title, "Near Real-Time Search Engine") {
				t.Errorf("dashboard %s is titled %q", uid, title)
			}
		}
	})

	t.Run("the datasource answers through grafana", func(t *testing.T) {
		// Grafana proxies the query, so this proves the provisioned
		// datasource points at a Prometheus that holds our metrics -- which
		// is what every panel depends on.
		endpoint := env.GrafanaURL +
			"/api/datasources/proxy/uid/prometheus/api/v1/query?query=hybrid_search_application_ready"

		var out struct {
			Status string `json:"status"`
			Data   struct {
				Result []json.RawMessage `json:"result"`
			} `json:"data"`
		}
		status, err := getJSON(ctx, endpoint, &out)
		if err != nil || status != 200 {
			t.Fatalf("querying Prometheus through Grafana: status %d, err %v "+
				"(anonymous access is enabled in docker-compose.yml; has it been changed?)", status, err)
		}
		if out.Status != "success" || len(out.Data.Result) == 0 {
			t.Errorf("Grafana's datasource returned %q with %d series, want data",
				out.Status, len(out.Data.Result))
		}
	})
}

// TestDashboardQueriesReturnData runs the queries behind the most important
// panels, so that a dashboard cannot quietly go blank because a metric was
// renamed. It asserts the query is answerable and has series, not what the
// values are.
func TestDashboardQueriesReturnData(t *testing.T) {
	doc := newTestDocument(t, "dashboard-queries")
	ctx := t.Context()

	// Give the dashboards something to show: one indexed document and one
	// search.
	if err := Insert(ctx, pool, doc); err != nil {
		t.Fatal(err)
	}
	waitForOpenSearch(t, ctx, doc.ID, func(d OpenSearchDoc) error { return matchesDocument(d, doc) })
	if _, _, err := Search(ctx, env, doc.Token, 10); err != nil {
		t.Fatal(err)
	}

	panels := []struct {
		dashboard, panel, query string
	}{
		{"Overview", "API ready", `hybrid_search_application_ready{service="api"}`},
		{"Overview", "Search requests/sec", "sum(rate(hybrid_search_search_requests_total[5m]))"},
		{"Overview", "Search p95", "histogram_quantile(0.95, sum by (le) (rate(hybrid_search_search_duration_seconds_bucket[5m])))"},
		{"Overview", "Indexing queue", "sum(hybrid_search_indexing_queue_size)"},
		{"Overview", "Indexing workers", "sum(hybrid_search_indexing_workers_total)"},
		{"Search API", "Requests/sec", `sum(rate(hybrid_search_http_requests_total{route="/api/v1/search"}[5m]))`},
		{"Search API", "Where the time goes", `histogram_quantile(0.95, sum by (le, dependency) (rate(hybrid_search_dependency_duration_seconds_bucket{service="api"}[5m])))`},
		{"Kafka & Indexing", "Message outcomes", "sum by (outcome) (rate(hybrid_search_kafka_messages_total[5m]))"},
		{"Kafka & Indexing", "Indexing events", "sum by (operation) (rate(hybrid_search_indexing_events_total[5m]))"},
		{"Kafka & Indexing", "Commits", "sum(rate(hybrid_search_kafka_offset_commits_total[5m]))"},
		{"Dependencies", "Requests by dependency", "sum by (dependency, operation) (rate(hybrid_search_dependency_requests_total[5m]))"},
		{"Runtime", "Goroutines", `sum by (service) (go_goroutines{job=~"hybrid-search.*"})`},
		{"Runtime", "Memory", `sum by (service) (process_resident_memory_bytes{job=~"hybrid-search.*"})`},
	}
	for _, p := range panels {
		t.Run(fmt.Sprintf("%s/%s", p.dashboard, p.panel), func(t *testing.T) {
			if results := promQuery(t, p.query); len(results) == 0 {
				t.Errorf("the query behind this panel returns nothing:\n  %s", p.query)
			}
		})
	}
}

// promResult is one series from an instant query.
type promResult struct {
	Metric map[string]string `json:"metric"`
	Value  []any             `json:"value"`
}

// promQuery runs an instant query and returns its series. A query Prometheus
// refuses is a failure; a query that is valid but matches nothing returns an
// empty slice, which the caller decides what to make of.
func promQuery(t *testing.T, query string) []promResult {
	t.Helper()

	var out struct {
		Status string `json:"status"`
		Error  string `json:"error"`
		Data   struct {
			Result []promResult `json:"result"`
		} `json:"data"`
	}
	endpoint := fmt.Sprintf("%s/api/v1/query?query=%s", env.PrometheusURL, url.QueryEscape(query))
	status, err := getJSON(t.Context(), endpoint, &out)
	if err != nil {
		t.Fatalf("query %q: %v", query, err)
	}
	if status != 200 || out.Status != "success" {
		t.Fatalf("query %q: status %d, %s %s", query, status, out.Status, out.Error)
	}
	return out.Data.Result
}
