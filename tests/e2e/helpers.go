// Package e2e holds end-to-end tests that run against the real stack from
// docker-compose.yml: PostgreSQL, Debezium, Kafka, the consumer, OpenSearch,
// Qdrant and the HTTP API. Nothing here is mocked; the tests prove that a row
// written to PostgreSQL becomes searchable through the public API.
//
// They only run when E2E is set, so `go test ./...` never needs Docker.
// See README.md in this directory.
//
// The helpers deliberately avoid the testing package: they take a context and
// return errors, so the test file decides what is fatal and what is retried.
package e2e

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Environment locates the running stack. Every value has a default matching
// the ports docker-compose.yml publishes, and can be overridden.
type Environment struct {
	APIURL        string   // E2E_API_URL
	DatabaseURL   string   // E2E_DATABASE_URL
	OpenSearchURL string   // E2E_OPENSEARCH_URL
	QdrantURL     string   // E2E_QDRANT_URL, the REST port
	ConnectURL    string   // E2E_CONNECT_URL, the Kafka Connect REST API
	KafkaBrokers  []string // E2E_KAFKA_BROKERS

	Topic         string // E2E_TOPIC
	DLQTopic      string // E2E_DLQ_TOPIC, defaults to Topic + ".dlq"
	Index         string // E2E_OPENSEARCH_INDEX
	Collection    string // E2E_QDRANT_COLLECTION
	ConnectorName string // E2E_CONNECTOR_NAME

	// MetricsURL and ConsumerMetricsURL are the two Prometheus endpoints: the
	// API serves its own, and the consumer serves its on a listener of its
	// own because it has no HTTP API.
	MetricsURL         string // E2E_METRICS_URL
	ConsumerMetricsURL string // E2E_CONSUMER_METRICS_URL
	VectorSize         int    // E2E_VECTOR_SIZE

	// Timeout bounds one "eventually" wait, and PollInterval is how often it
	// re-checks. The pipeline is eventually consistent, so tests wait for
	// conditions instead of sleeping for a fixed time.
	Timeout      time.Duration // E2E_TIMEOUT
	PollInterval time.Duration // E2E_POLL_INTERVAL
}

// Enabled reports whether the E2E suite should run.
func Enabled() bool { return os.Getenv("E2E") != "" }

// LoadEnvironment reads the settings, falling back to the local stack.
func LoadEnvironment() (Environment, error) {
	env := Environment{
		APIURL:        getEnv("E2E_API_URL", "http://localhost:8080"),
		DatabaseURL:   getEnv("E2E_DATABASE_URL", "postgres://postgres:postgres@localhost:5432/searchdb?sslmode=disable"),
		OpenSearchURL: getEnv("E2E_OPENSEARCH_URL", "http://localhost:9200"),
		QdrantURL:     getEnv("E2E_QDRANT_URL", "http://localhost:6333"),
		ConnectURL:    getEnv("E2E_CONNECT_URL", "http://localhost:8083"),
		KafkaBrokers:  strings.Split(getEnv("E2E_KAFKA_BROKERS", "localhost:29092"), ","),
		Topic:         getEnv("E2E_TOPIC", "search.public.documents"),
		Index:         getEnv("E2E_OPENSEARCH_INDEX", "documents"),
		Collection:    getEnv("E2E_QDRANT_COLLECTION", "documents"),
		ConnectorName: getEnv("E2E_CONNECTOR_NAME", "documents-cdc"),

		MetricsURL:         getEnv("E2E_METRICS_URL", ""),
		ConsumerMetricsURL: getEnv("E2E_CONSUMER_METRICS_URL", "http://localhost:9091/metrics"),
	}
	if env.MetricsURL == "" {
		env.MetricsURL = strings.TrimRight(env.APIURL, "/") + "/metrics"
	}
	env.DLQTopic = getEnv("E2E_DLQ_TOPIC", env.Topic+".dlq")

	var err error
	if env.VectorSize, err = getEnvInt("E2E_VECTOR_SIZE", 768); err != nil {
		return Environment{}, err
	}
	if env.Timeout, err = getEnvDuration("E2E_TIMEOUT", 90*time.Second); err != nil {
		return Environment{}, err
	}
	if env.PollInterval, err = getEnvDuration("E2E_POLL_INTERVAL", time.Second); err != nil {
		return Environment{}, err
	}
	return env, nil
}

func getEnv(key, fallback string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return fallback
}

func getEnvInt(key string, fallback int) (int, error) {
	raw := getEnv(key, "")
	if raw == "" {
		return fallback, nil
	}
	var n int
	if _, err := fmt.Sscanf(raw, "%d", &n); err != nil {
		return 0, fmt.Errorf("invalid %s: %w", key, err)
	}
	return n, nil
}

func getEnvDuration(key string, fallback time.Duration) (time.Duration, error) {
	raw := getEnv(key, "")
	if raw == "" {
		return fallback, nil
	}
	d, err := time.ParseDuration(raw)
	if err != nil {
		return 0, fmt.Errorf("invalid %s: %w", key, err)
	}
	return d, nil
}

// eventually calls condition until it reports true, ctx ends or timeout
// passes. The first check runs immediately.
//
// An error from condition means "not yet": the pipeline is asynchronous, so a
// document that is not in an index is a 404 rather than a failure. The last
// error is kept and reported if the wait times out, which is what says how far
// the pipeline got.
func eventually(ctx context.Context, timeout, interval time.Duration, condition func(ctx context.Context) (bool, error)) error {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	attempts := 0
	var lastErr error
	for {
		attempts++
		ok, err := condition(ctx)
		if ok {
			return nil
		}
		if err != nil {
			lastErr = err
		}

		select {
		case <-ctx.Done():
			if lastErr != nil {
				return fmt.Errorf("condition not met within %s (%d attempts): %w", timeout, attempts, lastErr)
			}
			return fmt.Errorf("condition not met within %s (%d attempts)", timeout, attempts)
		case <-ticker.C:
		}
	}
}

// ---------------------------------------------------------------- PostgreSQL

// Connect opens a pool against the source database.
func Connect(ctx context.Context, env Environment) (*pgxpool.Pool, error) {
	pool, err := pgxpool.New(ctx, env.DatabaseURL)
	if err != nil {
		return nil, fmt.Errorf("connect to postgres: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping postgres: %w", err)
	}
	return pool, nil
}

// ---------------------------------------------------------------- OpenSearch

// OpenSearchDoc is one document as OpenSearch stores it.
type OpenSearchDoc struct {
	Found   bool           `json:"found"`
	Version int64          `json:"_version"`
	Source  DocumentSource `json:"_source"`
}

// DocumentSource mirrors the fields the indexer writes; see
// opensearch.Document.
type DocumentSource struct {
	ID        string    `json:"id"`
	Title     string    `json:"title"`
	Content   string    `json:"content"`
	URL       string    `json:"url"`
	Version   int64     `json:"version"`
	UpdatedAt time.Time `json:"updated_at"`
}

// GetOpenSearchDoc fetches one document by ID. A missing document is not an
// error: Found is false.
func GetOpenSearchDoc(ctx context.Context, env Environment, id string) (OpenSearchDoc, error) {
	var doc OpenSearchDoc
	status, err := getJSON(ctx, fmt.Sprintf("%s/%s/_doc/%s", env.OpenSearchURL, env.Index, id), &doc)
	if err != nil {
		return OpenSearchDoc{}, err
	}
	if status == http.StatusNotFound {
		return OpenSearchDoc{Found: false}, nil
	}
	if status != http.StatusOK {
		return OpenSearchDoc{}, fmt.Errorf("opensearch returned %d for document %s", status, id)
	}
	return doc, nil
}

// CountOpenSearchDocs counts the documents carrying an id, which is one for a
// correctly indexed document however many times its events were processed.
func CountOpenSearchDocs(ctx context.Context, env Environment, id string) (int, error) {
	body := fmt.Sprintf(`{"query":{"term":{"id":%q}}}`, id)
	var out struct {
		Count int `json:"count"`
	}
	status, err := postJSON(ctx, fmt.Sprintf("%s/%s/_count", env.OpenSearchURL, env.Index), body, &out)
	if err != nil {
		return 0, err
	}
	if status != http.StatusOK {
		return 0, fmt.Errorf("opensearch _count returned %d", status)
	}
	return out.Count, nil
}

// -------------------------------------------------------------------- Qdrant

// QdrantPoint is one stored vector with its payload.
type QdrantPoint struct {
	ID      string
	Vector  []float32
	Payload map[string]any
}

// GetQdrantPoint fetches one point by ID. found is false when it is not there.
func GetQdrantPoint(ctx context.Context, env Environment, id string) (point QdrantPoint, found bool, err error) {
	body := fmt.Sprintf(`{"ids":[%q],"with_payload":true,"with_vector":true}`, id)
	var out struct {
		Result []struct {
			ID      string         `json:"id"`
			Payload map[string]any `json:"payload"`
			Vector  []float32      `json:"vector"`
		} `json:"result"`
	}
	status, err := postJSON(ctx, fmt.Sprintf("%s/collections/%s/points", env.QdrantURL, env.Collection), body, &out)
	if err != nil {
		return QdrantPoint{}, false, err
	}
	if status != http.StatusOK {
		return QdrantPoint{}, false, fmt.Errorf("qdrant returned %d for point %s", status, id)
	}
	if len(out.Result) == 0 {
		return QdrantPoint{}, false, nil
	}
	p := out.Result[0]
	return QdrantPoint{ID: p.ID, Vector: p.Vector, Payload: p.Payload}, true, nil
}

// CountQdrantPoints counts the points whose payload names this document.
func CountQdrantPoints(ctx context.Context, env Environment, documentID string) (int, error) {
	body := fmt.Sprintf(`{"exact":true,"filter":{"must":[{"key":"document_id","match":{"value":%q}}]}}`, documentID)
	var out struct {
		Result struct {
			Count int `json:"count"`
		} `json:"result"`
	}
	status, err := postJSON(ctx, fmt.Sprintf("%s/collections/%s/points/count", env.QdrantURL, env.Collection), body, &out)
	if err != nil {
		return 0, err
	}
	if status != http.StatusOK {
		return 0, fmt.Errorf("qdrant count returned %d", status)
	}
	return out.Result.Count, nil
}

// ----------------------------------------------------------------- Search API

// SearchResponse is the public API's response; see api.SearchResponse.
type SearchResponse struct {
	Query   string `json:"query"`
	Results []struct {
		ID      string  `json:"id"`
		Title   string  `json:"title"`
		Content string  `json:"content"`
		URL     string  `json:"url"`
		Score   float64 `json:"score"`
	} `json:"results"`
	Total int `json:"total"`
}

// Find returns the result with this ID, if the response holds one.
func (r SearchResponse) Find(id string) (int, bool) {
	for i, result := range r.Results {
		if result.ID == id {
			return i, true
		}
	}
	return 0, false
}

// Count returns how many results carry this ID. One logical document must
// appear once however often its events were processed, so a count of more than
// one is what a duplicate looks like from outside.
func (r SearchResponse) Count(id string) int {
	n := 0
	for _, result := range r.Results {
		if result.ID == id {
			n++
		}
	}
	return n
}

// Search calls GET /api/v1/search, the same way any client would.
func Search(ctx context.Context, env Environment, query string, limit int) (SearchResponse, int, error) {
	endpoint := fmt.Sprintf("%s/api/v1/search?q=%s&limit=%d", env.APIURL, url.QueryEscape(query), limit)
	var out SearchResponse
	status, err := getJSON(ctx, endpoint, &out)
	if err != nil {
		return SearchResponse{}, 0, err
	}
	return out, status, nil
}

// ------------------------------------------------------------------ Debezium

// ConnectorStatus is the Kafka Connect status of one connector.
type ConnectorStatus struct {
	Name      string `json:"name"`
	Connector struct {
		State string `json:"state"`
	} `json:"connector"`
	Tasks []struct {
		ID    int    `json:"id"`
		State string `json:"state"`
		Trace string `json:"trace"`
	} `json:"tasks"`
}

// Running reports whether the connector and at least one task are running.
func (s ConnectorStatus) Running() bool {
	if s.Connector.State != "RUNNING" || len(s.Tasks) == 0 {
		return false
	}
	for _, task := range s.Tasks {
		if task.State != "RUNNING" {
			return false
		}
	}
	return true
}

// GetConnectorStatus reads the connector's state. found is false when Kafka
// Connect does not know it.
func GetConnectorStatus(ctx context.Context, env Environment) (status ConnectorStatus, found bool, err error) {
	code, err := getJSON(ctx, fmt.Sprintf("%s/connectors/%s/status", env.ConnectURL, env.ConnectorName), &status)
	if err != nil {
		return ConnectorStatus{}, false, err
	}
	switch code {
	case http.StatusOK:
		return status, true, nil
	case http.StatusNotFound:
		return ConnectorStatus{}, false, nil
	default:
		return ConnectorStatus{}, false, fmt.Errorf("kafka connect returned %d", code)
	}
}

// EnsureConnector registers the connector if Kafka Connect does not have it,
// reusing docker/debezium/connector.json rather than a second copy of the
// configuration, and waits until it is running.
func EnsureConnector(ctx context.Context, env Environment) error {
	status, found, err := GetConnectorStatus(ctx, env)
	if err != nil {
		return err
	}
	if !found {
		path := getEnv("E2E_CONNECTOR_CONFIG", "")
		if path == "" {
			root, err := repoRoot()
			if err != nil {
				return err
			}
			path = filepath.Join(root, "docker", "debezium", "connector.json")
		}
		config, err := os.ReadFile(path)
		if err != nil {
			return fmt.Errorf("read connector configuration: %w", err)
		}
		// PUT creates it, or replaces the configuration if it appeared
		// meanwhile; Kafka Connect resolves the ${env:...} placeholders.
		code, err := putJSON(ctx, fmt.Sprintf("%s/connectors/%s/config", env.ConnectURL, env.ConnectorName), string(config))
		if err != nil {
			return fmt.Errorf("register connector: %w", err)
		}
		if code != http.StatusOK && code != http.StatusCreated {
			return fmt.Errorf("registering connector %s returned %d", env.ConnectorName, code)
		}
	}

	err = eventually(ctx, env.Timeout, env.PollInterval, func(ctx context.Context) (bool, error) {
		status, found, err = GetConnectorStatus(ctx, env)
		if err != nil || !found {
			return false, err
		}
		for _, task := range status.Tasks {
			if task.State == "FAILED" {
				return false, fmt.Errorf("task %d failed: %s", task.ID, firstLine(task.Trace))
			}
		}
		return status.Running(), nil
	})
	if err != nil {
		return fmt.Errorf("connector %s is not running: %w", env.ConnectorName, err)
	}
	return nil
}

// --------------------------------------------------------------- Diagnostics

// PipelineState reports where a document currently is, which says how far
// along the pipeline a failing test got.
func PipelineState(ctx context.Context, env Environment, pool *pgxpool.Pool, id string) string {
	var b strings.Builder
	fmt.Fprintf(&b, "pipeline state for document %s:\n", id)

	var title string
	err := pool.QueryRow(ctx, `SELECT title FROM documents WHERE id = $1`, id).Scan(&title)
	switch {
	case err != nil:
		fmt.Fprintf(&b, "  postgres:   not present (%v)\n", err)
	default:
		fmt.Fprintf(&b, "  postgres:   present, title %q\n", title)
	}

	if doc, err := GetOpenSearchDoc(ctx, env, id); err != nil {
		fmt.Fprintf(&b, "  opensearch: error: %v\n", err)
	} else if doc.Found {
		fmt.Fprintf(&b, "  opensearch: present, version %d, title %q\n", doc.Version, doc.Source.Title)
	} else {
		fmt.Fprintf(&b, "  opensearch: absent\n")
	}

	if point, found, err := GetQdrantPoint(ctx, env, id); err != nil {
		fmt.Fprintf(&b, "  qdrant:     error: %v\n", err)
	} else if found {
		fmt.Fprintf(&b, "  qdrant:     present, %d dimensions, payload %v\n", len(point.Vector), point.Payload)
	} else {
		fmt.Fprintf(&b, "  qdrant:     absent\n")
	}

	if status, found, err := GetConnectorStatus(ctx, env); err != nil {
		fmt.Fprintf(&b, "  connector:  error: %v\n", err)
	} else if !found {
		fmt.Fprintf(&b, "  connector:  not registered\n")
	} else {
		fmt.Fprintf(&b, "  connector:  %s, tasks %s\n", status.Connector.State, taskStates(status))
	}
	return b.String()
}

func taskStates(status ConnectorStatus) string {
	states := make([]string, 0, len(status.Tasks))
	for _, task := range status.Tasks {
		state := task.State
		if task.Trace != "" {
			state += " (" + firstLine(task.Trace) + ")"
		}
		states = append(states, state)
	}
	if len(states) == 0 {
		return "none"
	}
	return strings.Join(states, ", ")
}

// RestartService restarts one Compose service and returns once Docker has
// accepted the command. It touches only the service it is given: nothing is
// removed, and no volume, network or other container is affected.
//
// It is how a test reproduces a consumer crash: the process stops without
// committing what it was working on, and Kafka delivers those messages again
// to the new one.
func RestartService(ctx context.Context, service string) error {
	return composeService(ctx, "restart", service)
}

// StopService stops one Compose service, leaving its container and volume in
// place so StartService can bring it back with its data. It is how a test
// takes one dependency away from the running consumer.
func StopService(ctx context.Context, service string) error {
	return composeService(ctx, "stop", service)
}

// StartService starts a stopped service again. Starting one that is already
// running succeeds and changes nothing.
func StartService(ctx context.Context, service string) error {
	return composeService(ctx, "start", service)
}

// composeService runs one Compose command against one named service.
//
// Only that service is ever touched: nothing here removes containers, volumes
// or networks, and no command applies to the project as a whole.
func composeService(ctx context.Context, action, service string) error {
	root, err := repoRoot()
	if err != nil {
		return fmt.Errorf("locate the repository root: %w", err)
	}
	cmd := exec.CommandContext(ctx, "docker", "compose", action, service)
	cmd.Dir = root
	if out, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("docker compose %s %s: %w: %s", action, service, err, firstLine(string(out)))
	}
	return nil
}

// DumpComposeLogs writes the tail of the stack's logs to w. It is best
// effort: without Docker on the path it writes a note and returns.
func DumpComposeLogs(w io.Writer, tail int, services ...string) {
	root, err := repoRoot()
	if err != nil {
		fmt.Fprintf(w, "cannot locate the repository root for docker compose logs: %v\n", err)
		return
	}
	args := append([]string{"compose", "logs", fmt.Sprintf("--tail=%d", tail), "--no-color"}, services...)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "docker", args...)
	cmd.Dir = root
	out, err := cmd.CombinedOutput()
	if err != nil {
		fmt.Fprintf(w, "docker compose logs failed: %v\n", err)
	}
	fmt.Fprintf(w, "--- docker compose logs --tail=%d %s ---\n%s\n", tail, strings.Join(services, " "), out)
}

// ------------------------------------------------------------------- plumbing

var httpClient = &http.Client{Timeout: 30 * time.Second}

func getJSON(ctx context.Context, url string, out any) (int, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return 0, err
	}
	return doJSON(req, out)
}

func postJSON(ctx context.Context, url, body string, out any) (int, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, strings.NewReader(body))
	if err != nil {
		return 0, err
	}
	req.Header.Set("Content-Type", "application/json")
	return doJSON(req, out)
}

func putJSON(ctx context.Context, url, body string) (int, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, url, strings.NewReader(body))
	if err != nil {
		return 0, err
	}
	req.Header.Set("Content-Type", "application/json")
	return doJSON(req, nil)
}

func doJSON(req *http.Request, out any) (int, error) {
	resp, err := httpClient.Do(req)
	if err != nil {
		return 0, fmt.Errorf("%s %s: %w", req.Method, req.URL.Host+req.URL.Path, err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return resp.StatusCode, fmt.Errorf("read response: %w", err)
	}
	if out != nil && len(bytes.TrimSpace(body)) > 0 {
		if err := json.Unmarshal(body, out); err != nil {
			return resp.StatusCode, fmt.Errorf("%s %s: decode response: %w", req.Method, req.URL.Path, err)
		}
	}
	return resp.StatusCode, nil
}

// repoRoot walks up from this package to the directory holding go.mod.
func repoRoot() (string, error) {
	dir, err := os.Getwd()
	if err != nil {
		return "", err
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir, nil
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return "", fmt.Errorf("no go.mod found above %s", dir)
		}
		dir = parent
	}
}

func firstLine(s string) string {
	if i := strings.IndexByte(s, '\n'); i >= 0 {
		return s[:i]
	}
	return s
}

// ----------------------------------------------------------------- Metrics

// FetchMetrics reads a Prometheus endpoint and returns its body.
func FetchMetrics(ctx context.Context, endpoint string) (string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return "", err
	}
	resp, err := httpClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("fetch %s: %w", endpoint, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("%s answered %d", endpoint, resp.StatusCode)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("read %s: %w", endpoint, err)
	}
	return string(body), nil
}

// MetricValue sums every series of a metric in an exposition body.
//
// Summing is what a counter question usually wants -- "how many searches",
// not "how many with these labels" -- and it means a test does not have to
// know which label sets exist. match, when given, keeps only the series whose
// line contains it, which is how one label value is singled out.
func MetricValue(body, name, match string) float64 {
	var total float64
	for line := range strings.SplitSeq(body, "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		// A series line is "name{labels} value" or "name value".
		if !strings.HasPrefix(line, name) {
			continue
		}
		rest := line[len(name):]
		if rest == "" || (rest[0] != ' ' && rest[0] != '{') {
			continue // a longer metric name that starts the same way
		}
		if match != "" && !strings.Contains(line, match) {
			continue
		}
		fields := strings.Fields(line)
		value, err := strconv.ParseFloat(fields[len(fields)-1], 64)
		if err != nil {
			continue
		}
		total += value
	}
	return total
}
