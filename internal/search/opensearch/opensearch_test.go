package opensearch

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"near-real-time-hybrid-search-engine/internal/config"
)

const (
	testUser     = "search"
	testPassword = "s3cret-pass"
)

// request is what the fake server saw.
type request struct {
	Method, Path, Query string
	Body                string
	User, Password      string
}

// fakeServer answers like OpenSearch with canned responses and records every
// request, so tests exercise the real client without a running cluster.
type fakeServer struct {
	mu       sync.Mutex
	requests []request
}

func (f *fakeServer) seen() []request {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]request(nil), f.requests...)
}

// newTestClient starts a fake server that answers each request with respond
// and returns a Client pointed at it.
func newTestClient(t *testing.T, respond func(r request) (status int, body string)) (*Client, *fakeServer) {
	t.Helper()
	fake := &fakeServer{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		user, pass, _ := r.BasicAuth()
		req := request{Method: r.Method, Path: r.URL.Path, Query: r.URL.RawQuery, Body: string(body), User: user, Password: pass}

		fake.mu.Lock()
		fake.requests = append(fake.requests, req)
		fake.mu.Unlock()

		status, respBody := respond(req)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		if r.Method != http.MethodHead {
			_, _ = io.WriteString(w, respBody)
		}
	}))
	t.Cleanup(srv.Close)

	c, err := New(config.OpenSearchConfig{URL: srv.URL, Username: testUser, Password: testPassword, Index: "documents"})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c, fake
}

func ok(request) (int, string) { return http.StatusOK, `{}` }

func TestNewValidatesConfig(t *testing.T) {
	valid := config.OpenSearchConfig{URL: "http://localhost:9200", Index: "documents"}

	tests := []struct {
		name        string
		change      func(*config.OpenSearchConfig)
		wantInError string
	}{
		{"valid without auth", func(*config.OpenSearchConfig) {}, ""},
		{"valid with auth and https", func(c *config.OpenSearchConfig) {
			c.URL, c.Username, c.Password = "https://search.internal:9200", "u", "p"
		}, ""},
		{"empty url", func(c *config.OpenSearchConfig) { c.URL = "" }, "url must look like"},
		{"url without scheme", func(c *config.OpenSearchConfig) { c.URL = "localhost:9200" }, "url must look like"},
		{"unsupported scheme", func(c *config.OpenSearchConfig) { c.URL = "ftp://localhost:9200" }, "url must look like"},
		{"credentials in url", func(c *config.OpenSearchConfig) { c.URL = "http://admin:hunter2@localhost:9200" }, "must not contain credentials"},
		{"password without username", func(c *config.OpenSearchConfig) { c.Password = "p" }, "set together"},
		{"username without password", func(c *config.OpenSearchConfig) { c.Username = "u" }, "set together"},
		{"empty index", func(c *config.OpenSearchConfig) { c.Index = "" }, "index name is required"},
		{"uppercase index", func(c *config.OpenSearchConfig) { c.Index = "Documents" }, "must be lowercase"},
		{"index with space", func(c *config.OpenSearchConfig) { c.Index = "my docs" }, "must not contain"},
		{"index starting with underscore", func(c *config.OpenSearchConfig) { c.Index = "_docs" }, "must not start with"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := valid
			tt.change(&cfg)

			c, err := New(cfg)
			if tt.wantInError == "" {
				if err != nil {
					t.Fatalf("New: %v", err)
				}
				_ = c.Close()
				return
			}
			if err == nil {
				t.Fatal("expected an error")
			}
			if !strings.Contains(err.Error(), tt.wantInError) {
				t.Errorf("error = %q, want it to contain %q", err, tt.wantInError)
			}
			if strings.Contains(err.Error(), "hunter2") {
				t.Errorf("error leaks the password: %q", err)
			}
		})
	}
}

func TestDocumentJSON(t *testing.T) {
	updated := time.Date(2026, 9, 19, 10, 30, 0, 0, time.UTC)

	full, err := json.Marshal(Document{
		ID: "doc-1", Title: "Go", Content: "Channels", URL: "https://example.com/go",
		UpdatedAt: updated, Version: 7,
	})
	if err != nil {
		t.Fatal(err)
	}
	want := `{"id":"doc-1","title":"Go","content":"Channels","url":"https://example.com/go","updated_at":"2026-09-19T10:30:00Z","version":7}`
	if string(full) != want {
		t.Errorf("got  %s\nwant %s", full, want)
	}

	minimal, err := json.Marshal(Document{ID: "doc-2", Title: "T", Content: "C"})
	if err != nil {
		t.Fatal(err)
	}
	if want := `{"id":"doc-2","title":"T","content":"C"}`; string(minimal) != want {
		t.Errorf("empty optional fields should be omitted: got %s, want %s", minimal, want)
	}
}

// TestMappingCoversDocument guards the strict mapping: every field Document
// sends must be mapped, or OpenSearch rejects the write.
func TestMappingCoversDocument(t *testing.T) {
	var mapping struct {
		Mappings struct {
			Dynamic    string `json:"dynamic"`
			Properties map[string]struct {
				Type string `json:"type"`
			} `json:"properties"`
		} `json:"mappings"`
	}
	if err := json.Unmarshal([]byte(indexMapping), &mapping); err != nil {
		t.Fatalf("indexMapping is not valid JSON: %v", err)
	}
	props := mapping.Mappings.Properties

	docType := reflect.TypeFor[Document]()
	for i := range docType.NumField() {
		name, _, _ := strings.Cut(docType.Field(i).Tag.Get("json"), ",")
		if _, found := props[name]; !found {
			t.Errorf("Document field %q is missing from indexMapping", name)
		}
	}

	wantTypes := map[string]string{"id": "keyword", "title": "text", "content": "text", "url": "keyword", "updated_at": "date"}
	for field, want := range wantTypes {
		if got := props[field].Type; got != want {
			t.Errorf("%s mapped as %q, want %q", field, got, want)
		}
	}
	if mapping.Mappings.Dynamic != "strict" {
		t.Errorf("dynamic = %q, want strict", mapping.Mappings.Dynamic)
	}
}

func TestPing(t *testing.T) {
	c, fake := newTestClient(t, ok)
	if err := c.Ping(context.Background()); err != nil {
		t.Fatalf("Ping: %v", err)
	}
	got := fake.seen()[0]
	if got.Method != http.MethodHead || got.Path != "/" {
		t.Errorf("Ping sent %s %s, want HEAD /", got.Method, got.Path)
	}
	if got.User != testUser || got.Password != testPassword {
		t.Errorf("basic auth = %q/%q, want the configured credentials", got.User, got.Password)
	}
}

func TestPingReportsRejectedCredentials(t *testing.T) {
	c, _ := newTestClient(t, func(request) (int, string) { return http.StatusUnauthorized, "" })

	err := c.Ping(context.Background())
	var reqErr *RequestError
	if !errors.As(err, &reqErr) || reqErr.StatusCode != http.StatusUnauthorized {
		t.Fatalf("err = %v, want a RequestError with status 401", err)
	}
	if reqErr.Temporary() {
		t.Error("401 should not be retried")
	}
	if strings.Contains(err.Error(), testPassword) {
		t.Errorf("error leaks the password: %q", err)
	}
}

func TestEnsureIndex(t *testing.T) {
	t.Run("existing index is left alone", func(t *testing.T) {
		c, fake := newTestClient(t, ok)
		if err := c.EnsureIndex(context.Background()); err != nil {
			t.Fatal(err)
		}
		if reqs := fake.seen(); len(reqs) != 1 || reqs[0].Method != http.MethodHead || reqs[0].Path != "/documents" {
			t.Fatalf("requests = %+v, want only HEAD /documents", reqs)
		}
	})

	t.Run("missing index is created with the mapping", func(t *testing.T) {
		c, fake := newTestClient(t, func(r request) (int, string) {
			if r.Method == http.MethodHead {
				return http.StatusNotFound, ""
			}
			return http.StatusOK, `{"acknowledged":true,"index":"documents"}`
		})
		if err := c.EnsureIndex(context.Background()); err != nil {
			t.Fatal(err)
		}
		create := fake.seen()[1]
		if create.Method != http.MethodPut || create.Path != "/documents" {
			t.Fatalf("second request = %s %s, want PUT /documents", create.Method, create.Path)
		}
		if !strings.Contains(create.Body, `"dynamic": "strict"`) {
			t.Errorf("create body does not carry the mapping: %s", create.Body)
		}
	})

	t.Run("index created concurrently by another instance", func(t *testing.T) {
		var heads atomic.Int32
		c, _ := newTestClient(t, func(r request) (int, string) {
			if r.Method == http.MethodHead {
				if heads.Add(1) == 1 {
					return http.StatusNotFound, ""
				}
				return http.StatusOK, ""
			}
			return http.StatusBadRequest, `{"error":{"type":"resource_already_exists_exception","reason":"exists"},"status":400}`
		})
		if err := c.EnsureIndex(context.Background()); err != nil {
			t.Fatalf("EnsureIndex: %v", err)
		}
	})

	t.Run("cluster error is returned", func(t *testing.T) {
		c, _ := newTestClient(t, func(request) (int, string) { return http.StatusForbidden, "" })
		if err := c.EnsureIndex(context.Background()); err == nil {
			t.Fatal("expected an error")
		}
	})
}

func TestIndexDocument(t *testing.T) {
	doc := Document{ID: "7f1c9b2e", Title: "Go", Content: "Channels", UpdatedAt: time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)}

	t.Run("uses the document id as the OpenSearch id", func(t *testing.T) {
		c, fake := newTestClient(t, func(request) (int, string) { return http.StatusCreated, `{"result":"created"}` })
		result, err := c.IndexDocument(context.Background(), doc)
		if err != nil {
			t.Fatal(err)
		}
		if result != WriteApplied {
			t.Errorf("result = %s, want applied", result)
		}
		got := fake.seen()[0]
		if got.Method != http.MethodPut || got.Path != "/documents/_doc/7f1c9b2e" {
			t.Errorf("request = %s %s, want PUT /documents/_doc/7f1c9b2e", got.Method, got.Path)
		}
		if strings.Contains(got.Query, "version") {
			t.Errorf("unversioned write sent version params: %q", got.Query)
		}
		var sent Document
		if err := json.Unmarshal([]byte(got.Body), &sent); err != nil || sent != doc {
			t.Errorf("body = %s (err %v), want the document", got.Body, err)
		}
	})

	t.Run("versioned write uses external versioning", func(t *testing.T) {
		c, fake := newTestClient(t, func(request) (int, string) { return http.StatusOK, `{"result":"updated"}` })
		versioned := doc
		versioned.Version = 42
		if _, err := c.IndexDocument(context.Background(), versioned); err != nil {
			t.Fatal(err)
		}
		if q := fake.seen()[0].Query; !strings.Contains(q, "version=42") || !strings.Contains(q, "version_type=external") {
			t.Errorf("query = %q, want version=42 and version_type=external", q)
		}
	})

	// A refused versioned write is not a failure, but the two reasons for it
	// mean very different things to the caller, and OpenSearch reports both
	// as 409. The stored version is what tells them apart.
	t.Run("a refused write reports whether the event is stale", func(t *testing.T) {
		const conflict = `{"error":{"type":"version_conflict_engine_exception","reason":"conflict"},"status":409}`

		tests := []struct {
			name       string
			getStatus  int
			getBody    string
			want       WriteResult
			wantLookup bool
		}{
			{
				name: "the index holds a newer version", getStatus: http.StatusOK,
				getBody: `{"_id":"7f1c9b2e","_version":9,"found":true}`, want: WriteStale, wantLookup: true,
			},
			{
				name: "the index holds this same version", getStatus: http.StatusOK,
				getBody: `{"_id":"7f1c9b2e","_version":3,"found":true}`, want: WriteDuplicate, wantLookup: true,
			},
			{
				// A delete overtook this event between the two requests.
				name: "the document is gone", getStatus: http.StatusNotFound,
				getBody: `{"_id":"7f1c9b2e","found":false}`, want: WriteStale, wantLookup: true,
			},
			{
				// Assuming "stale" here could leave the document out of the
				// vector index for good; repeating the write cannot.
				name: "the version cannot be read", getStatus: http.StatusServiceUnavailable,
				getBody: `{"error":{"type":"unavailable"},"status":503}`, want: WriteDuplicate, wantLookup: true,
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				c, fake := newTestClient(t, func(r request) (int, string) {
					if r.Method == http.MethodGet {
						return tt.getStatus, tt.getBody
					}
					return http.StatusConflict, conflict
				})

				stale := doc
				stale.Version = 3
				result, err := c.IndexDocument(context.Background(), stale)
				if err != nil {
					t.Fatalf("a refused versioned write should not be an error, got %v", err)
				}
				if result != tt.want {
					t.Errorf("result = %s, want %s", result, tt.want)
				}

				// The client retries a 5xx on its own, so count lookups
				// rather than expecting exactly one request each.
				seen := fake.seen()
				lookups := 0
				for _, r := range seen[1:] {
					if r.Method != http.MethodGet {
						t.Fatalf("unexpected request after the refused write: %+v", r)
					}
					lookups++
					if !strings.Contains(r.Query, "_source=false") {
						t.Errorf("version lookup fetched the document body: %q", r.Query)
					}
				}
				if (lookups > 0) != tt.wantLookup {
					t.Errorf("%d version lookups, want any: %v", lookups, tt.wantLookup)
				}
			})
		}
	})

	// Without a version there is nothing to compare, so every write wins and
	// no lookup is made.
	t.Run("an unversioned conflict is an error", func(t *testing.T) {
		c, fake := newTestClient(t, func(request) (int, string) {
			return http.StatusConflict, `{"error":{"type":"version_conflict_engine_exception"},"status":409}`
		})
		if _, err := c.IndexDocument(context.Background(), doc); err == nil {
			t.Fatal("expected an error for a conflict on an unversioned write")
		}
		if n := len(fake.seen()); n != 1 {
			t.Errorf("%d requests sent, want 1: there is no version to look up", n)
		}
	})

	t.Run("rejected document is a permanent error", func(t *testing.T) {
		c, _ := newTestClient(t, func(request) (int, string) {
			return http.StatusBadRequest, `{"error":{"type":"strict_dynamic_mapping_exception","reason":"unknown field"},"status":400}`
		})
		_, err := c.IndexDocument(context.Background(), doc)
		var reqErr *RequestError
		if !errors.As(err, &reqErr) || reqErr.StatusCode != http.StatusBadRequest || reqErr.Temporary() {
			t.Fatalf("err = %v, want a non-temporary RequestError with status 400", err)
		}
		if !strings.Contains(err.Error(), "index document 7f1c9b2e") {
			t.Errorf("error %q does not name the document", err)
		}
	})

	t.Run("invalid ids are rejected before sending", func(t *testing.T) {
		c, fake := newTestClient(t, ok)
		for _, id := range []string{"", "   ", strings.Repeat("x", maxIDBytes+1)} {
			bad := doc
			bad.ID = id
			if _, err := c.IndexDocument(context.Background(), bad); !errors.Is(err, ErrInvalidDocument) {
				t.Errorf("id of %d bytes: err = %v, want ErrInvalidDocument", len(id), err)
			}
		}
		if n := len(fake.seen()); n != 0 {
			t.Errorf("%d requests sent for invalid documents, want 0", n)
		}
	})
}

func TestDocumentVersion(t *testing.T) {
	tests := []struct {
		name        string
		status      int
		body        string
		wantVersion int64
		wantFound   bool
		wantErr     bool
	}{
		{"indexed document", http.StatusOK, `{"_id":"doc-1","_version":7,"found":true}`, 7, true, false},
		{"unknown document", http.StatusNotFound, `{"_id":"doc-1","found":false}`, 0, false, false},
		{"cluster error", http.StatusServiceUnavailable, `{"error":{"type":"unavailable"},"status":503}`, 0, false, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c, _ := newTestClient(t, func(request) (int, string) { return tt.status, tt.body })

			version, found, err := c.DocumentVersion(context.Background(), "doc-1")
			if (err != nil) != tt.wantErr {
				t.Fatalf("err = %v, wantErr %v", err, tt.wantErr)
			}
			if version != tt.wantVersion || found != tt.wantFound {
				t.Errorf("version = %d, found = %v; want %d, %v", version, found, tt.wantVersion, tt.wantFound)
			}
		})
	}

	t.Run("invalid ids are rejected before sending", func(t *testing.T) {
		c, fake := newTestClient(t, ok)
		if _, _, err := c.DocumentVersion(context.Background(), " "); !errors.Is(err, ErrInvalidDocument) {
			t.Errorf("err = %v, want ErrInvalidDocument", err)
		}
		if n := len(fake.seen()); n != 0 {
			t.Errorf("%d requests sent for an invalid id, want 0", n)
		}
	})
}

func TestDeleteDocument(t *testing.T) {
	tests := []struct {
		name    string
		status  int
		wantErr bool
	}{
		{"deleted", http.StatusOK, false},
		{"already gone", http.StatusNotFound, false},
		{"server error", http.StatusBadRequest, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c, fake := newTestClient(t, func(request) (int, string) { return tt.status, `{"result":"deleted"}` })

			err := c.DeleteDocument(context.Background(), "doc-1")
			if (err != nil) != tt.wantErr {
				t.Fatalf("err = %v, wantErr %v", err, tt.wantErr)
			}
			if got := fake.seen()[0]; got.Method != http.MethodDelete || got.Path != "/documents/_doc/doc-1" {
				t.Errorf("request = %s %s, want DELETE /documents/_doc/doc-1", got.Method, got.Path)
			}
		})
	}

	c, _ := newTestClient(t, ok)
	if err := c.DeleteDocument(context.Background(), ""); !errors.Is(err, ErrInvalidDocument) {
		t.Errorf("empty id: err = %v, want ErrInvalidDocument", err)
	}
}

const twoHits = `{
  "took": 3,
  "timed_out": false,
  "hits": {
    "total": {"value": 2, "relation": "eq"},
    "max_score": 2.5,
    "hits": [
      {"_index": "documents", "_id": "doc-1", "_score": 2.5,
       "_source": {"id": "doc-1", "title": "Go channels", "content": "Channels connect goroutines.", "url": "https://example.com/1", "updated_at": "2026-09-19T10:30:00Z", "version": 3}},
      {"_index": "documents", "_id": "doc-2", "_score": 1.25,
       "_source": {"title": "Go maps", "content": "Maps are hash tables."}}
    ]
  }
}`

func TestSearch(t *testing.T) {
	c, fake := newTestClient(t, func(request) (int, string) { return http.StatusOK, twoHits })

	results, err := c.Search(context.Background(), "  go channels  ", 10)
	if err != nil {
		t.Fatalf("Search: %v", err)
	}

	got := fake.seen()[0]
	if got.Method != http.MethodPost || got.Path != "/documents/_search" {
		t.Errorf("request = %s %s, want POST /documents/_search", got.Method, got.Path)
	}
	var sent searchRequest
	if err := json.Unmarshal([]byte(got.Body), &sent); err != nil {
		t.Fatalf("request body: %v", err)
	}
	if want := newSearchRequest("go channels", 10); !reflect.DeepEqual(sent, want) {
		t.Errorf("query = %+v, want %+v (trimmed query, title boosted)", sent, want)
	}

	if len(results) != 2 {
		t.Fatalf("got %d results, want 2", len(results))
	}
	first := results[0]
	if first.ID != "doc-1" || first.Score != 2.5 || first.Document.Title != "Go channels" ||
		first.Document.URL != "https://example.com/1" || first.Document.Version != 3 ||
		!first.Document.UpdatedAt.Equal(time.Date(2026, 9, 19, 10, 30, 0, 0, time.UTC)) {
		t.Errorf("first result = %+v", first)
	}
	// The second hit has no id in its source; the OpenSearch _id fills it in.
	if second := results[1]; second.ID != "doc-2" || second.Document.ID != "doc-2" || second.Score != 1.25 {
		t.Errorf("second result = %+v", second)
	}
}

func TestSearchNoHits(t *testing.T) {
	c, _ := newTestClient(t, func(request) (int, string) {
		return http.StatusOK, `{"hits":{"total":{"value":0,"relation":"eq"},"hits":[]}}`
	})
	results, err := c.Search(context.Background(), "nothing", 5)
	if err != nil || results == nil || len(results) != 0 {
		t.Fatalf("results = %v, err = %v; want an empty, non-nil slice", results, err)
	}
}

func TestSearchValidatesInput(t *testing.T) {
	c, fake := newTestClient(t, ok)

	tests := []struct {
		query   string
		limit   int
		wantErr error
	}{
		{"", 10, ErrEmptyQuery},
		{"   ", 10, ErrEmptyQuery},
		{"go", 0, ErrInvalidLimit},
		{"go", -1, ErrInvalidLimit},
		{"go", MaxSearchLimit + 1, ErrInvalidLimit},
	}
	for _, tt := range tests {
		if _, err := c.Search(context.Background(), tt.query, tt.limit); !errors.Is(err, tt.wantErr) {
			t.Errorf("Search(%q, %d) err = %v, want %v", tt.query, tt.limit, err, tt.wantErr)
		}
	}
	if n := len(fake.seen()); n != 0 {
		t.Errorf("%d requests sent for invalid input, want 0", n)
	}
}

func TestSearchErrors(t *testing.T) {
	t.Run("rejected query", func(t *testing.T) {
		c, _ := newTestClient(t, func(request) (int, string) {
			return http.StatusBadRequest, `{"error":{"type":"search_phase_execution_exception","reason":"bad"},"status":400}`
		})
		_, err := c.Search(context.Background(), "go", 10)
		var reqErr *RequestError
		if !errors.As(err, &reqErr) || reqErr.StatusCode != http.StatusBadRequest {
			t.Fatalf("err = %v, want a RequestError with status 400", err)
		}
	})

	t.Run("undecodable source", func(t *testing.T) {
		c, _ := newTestClient(t, func(request) (int, string) {
			return http.StatusOK, `{"hits":{"hits":[{"_id":"doc-1","_score":1,"_source":{"title":42}}]}}`
		})
		if _, err := c.Search(context.Background(), "go", 10); err == nil || !strings.Contains(err.Error(), "decode search hit doc-1") {
			t.Fatalf("err = %v, want a decode error naming the hit", err)
		}
	})

	t.Run("cancelled context", func(t *testing.T) {
		c, _ := newTestClient(t, func(request) (int, string) { return http.StatusOK, twoHits })
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if _, err := c.Search(ctx, "go", 10); !errors.Is(err, context.Canceled) {
			t.Fatalf("err = %v, want context.Canceled", err)
		}
	})
}

func TestRequestErrorTemporary(t *testing.T) {
	tests := []struct {
		status int
		want   bool
	}{
		{0, true}, // no response: network error or timeout
		{http.StatusRequestTimeout, true},
		{http.StatusTooManyRequests, true},
		{http.StatusInternalServerError, true},
		{http.StatusServiceUnavailable, true},
		{http.StatusBadRequest, false},
		{http.StatusUnauthorized, false},
		{http.StatusNotFound, false},
		{http.StatusConflict, false},
	}
	for _, tt := range tests {
		if got := (&RequestError{StatusCode: tt.status, Err: errors.New("x")}).Temporary(); got != tt.want {
			t.Errorf("status %d: Temporary() = %v, want %v", tt.status, got, tt.want)
		}
	}
}
