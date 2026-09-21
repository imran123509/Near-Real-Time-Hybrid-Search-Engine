package embedding

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/genai"

	"near-real-time-hybrid-search-engine/internal/config"
)

// These tests run the real GeminiProvider, and the real SDK underneath it,
// against a local server that speaks the Gemini API's wire format. Nothing
// here calls Google.

// testAPIKey is a placeholder. The tests check that it never appears in an
// error, because errors end up in logs.
const testAPIKey = "test-key-not-a-real-secret"

func testConfig() config.EmbeddingConfig {
	return config.EmbeddingConfig{
		Provider:  ProviderGemini,
		APIKey:    testAPIKey,
		Model:     "gemini-embedding-2",
		Dimension: 3,
		Timeout:   2 * time.Second,
	}
}

// batchRequest is the body the SDK sends to batchEmbedContents.
type batchRequest struct {
	Requests []struct {
		Model   string `json:"model"`
		Content struct {
			Parts []struct {
				Text string `json:"text"`
			} `json:"parts"`
		} `json:"content"`
		OutputDimensionality int `json:"outputDimensionality"`
	} `json:"requests"`
}

// newTestProvider returns a provider whose requests go to handler.
func newTestProvider(t *testing.T, cfg config.EmbeddingConfig, handler http.HandlerFunc) *GeminiProvider {
	t.Helper()
	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)

	p, err := newGeminiProvider(context.Background(), cfg, srv.Client(), genai.HTTPOptions{BaseURL: srv.URL})
	if err != nil {
		t.Fatalf("newGeminiProvider: %v", err)
	}
	return p
}

// respondWith answers every request with one embedding holding values.
func respondWith(values ...float32) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		body, _ := json.Marshal(map[string]any{
			"embeddings": []map[string]any{{"values": values}},
		})
		_, _ = w.Write(body)
	}
}

// newBlockingProvider returns a provider whose requests hang until the client
// gives up, which is how a slow or stuck provider looks from this side. The
// returned channel is closed when the first request arrives.
func newBlockingProvider(t *testing.T, cfg config.EmbeddingConfig) (*GeminiProvider, <-chan struct{}) {
	t.Helper()
	release := make(chan struct{})
	started := make(chan struct{})
	var once sync.Once

	p := newTestProvider(t, cfg, func(_ http.ResponseWriter, r *http.Request) {
		once.Do(func() { close(started) })
		// Reading the body is what lets the server notice the client hanging
		// up and cancel r.Context().
		_, _ = io.Copy(io.Discard, r.Body)
		select {
		case <-r.Context().Done():
		case <-release:
		}
	})
	// Registered after the server's cleanup, so it runs first: no handler is
	// still blocked when the server closes and waits for its connections.
	t.Cleanup(func() { close(release) })
	return p, started
}

// assertNoSecret fails the test if err leaks the API key into its message.
func assertNoSecret(t *testing.T, err error) {
	t.Helper()
	if err != nil && strings.Contains(err.Error(), testAPIKey) {
		t.Errorf("error leaks the API key: %v", err)
	}
}

func TestGeminiEmbedSendsTheTextAndDimension(t *testing.T) {
	var got batchRequest
	var calls atomic.Int32

	p := newTestProvider(t, testConfig(), func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if r.Method != http.MethodPost || r.URL.Path != "/v1beta/models/gemini-embedding-2:batchEmbedContents" {
			t.Errorf("request = %s %s, want POST to the model's batchEmbedContents", r.Method, r.URL.Path)
		}
		if key := r.Header.Get("x-goog-api-key"); key != testAPIKey {
			t.Errorf("x-goog-api-key = %q, want the configured key", key)
		}
		// Transport errors quote the URL, so the key must never be in it.
		if strings.Contains(r.URL.RawQuery, testAPIKey) {
			t.Errorf("the API key was sent in the URL: %q", r.URL.RawQuery)
		}
		if err := json.NewDecoder(r.Body).Decode(&got); err != nil {
			t.Errorf("decode request: %v", err)
		}
		respondWith(0.1, 0.2, 0.3)(w, r)
	})

	text := "Indexing with Debezium\nChange events keep the indexes current."
	vector, err := p.Embed(context.Background(), text)
	if err != nil {
		t.Fatalf("Embed: %v", err)
	}
	if want := []float32{0.1, 0.2, 0.3}; fmt.Sprint(vector) != fmt.Sprint(want) {
		t.Errorf("vector = %v, want %v", vector, want)
	}
	if calls.Load() != 1 {
		t.Errorf("requests = %d, want exactly 1", calls.Load())
	}

	if len(got.Requests) != 1 || len(got.Requests[0].Content.Parts) != 1 {
		t.Fatalf("request = %+v, want one request with one part", got)
	}
	req := got.Requests[0]
	// The provider embeds the text it is given, exactly. Deciding what the
	// text is belongs to the caller.
	if req.Content.Parts[0].Text != text {
		t.Errorf("sent text = %q, want it unchanged: %q", req.Content.Parts[0].Text, text)
	}
	if req.Model != "models/gemini-embedding-2" {
		t.Errorf("model = %q, want the configured model", req.Model)
	}
	if req.OutputDimensionality != 3 {
		t.Errorf("outputDimensionality = %d, want the configured dimension 3", req.OutputDimensionality)
	}
}

func TestGeminiProviderDescribesItself(t *testing.T) {
	p := newTestProvider(t, testConfig(), respondWith(1, 2, 3))
	if p.Name() != "gemini" {
		t.Errorf("Name = %q, want gemini", p.Name())
	}
	if p.Dimension() != 3 {
		t.Errorf("Dimension = %d, want 3", p.Dimension())
	}
}

func TestGeminiEmbedRejectsEmptyTextWithoutARequest(t *testing.T) {
	var calls atomic.Int32
	p := newTestProvider(t, testConfig(), func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		respondWith(1, 2, 3)(w, r)
	})

	for _, text := range []string{"", " ", "\n\t  \r\n"} {
		if _, err := p.Embed(context.Background(), text); !errors.Is(err, ErrEmptyText) {
			t.Errorf("Embed(%q) = %v, want ErrEmptyText", text, err)
		}
	}
	if calls.Load() != 0 {
		t.Errorf("sent %d requests for empty text, want none", calls.Load())
	}
}

// A vector of the wrong length must be refused, never trimmed or padded to
// fit: Qdrant would reject it, or worse, similarity scores would be wrong.
func TestGeminiEmbedRejectsTheWrongDimension(t *testing.T) {
	tests := []struct {
		name   string
		values []float32
		want   string
	}{
		{"too short", []float32{0.1, 0.2}, "embedding dimension mismatch: expected 3, got 2"},
		{"too long", []float32{0.1, 0.2, 0.3, 0.4}, "embedding dimension mismatch: expected 3, got 4"},
		{"empty", []float32{}, "embedding dimension mismatch: expected 3, got 0"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := newTestProvider(t, testConfig(), respondWith(tt.values...))

			vector, err := p.Embed(context.Background(), "text")
			if !errors.Is(err, ErrDimensionMismatch) {
				t.Fatalf("err = %v, want ErrDimensionMismatch", err)
			}
			if !strings.Contains(err.Error(), tt.want) {
				t.Errorf("err = %q, want it to contain %q", err, tt.want)
			}
			if vector != nil {
				t.Errorf("vector = %v, want nil alongside the error", vector)
			}
		})
	}
}

func TestGeminiEmbedRejectsUnusableResponses(t *testing.T) {
	tests := []struct {
		name string
		body string
	}{
		{"no embeddings", `{"embeddings": []}`},
		{"embeddings field missing", `{}`},
		{"more than one embedding", `{"embeddings": [{"values": [1,2,3]}, {"values": [4,5,6]}]}`},
		{"null embedding", `{"embeddings": [null]}`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := newTestProvider(t, testConfig(), func(w http.ResponseWriter, _ *http.Request) {
				_, _ = w.Write([]byte(tt.body))
			})
			if _, err := p.Embed(context.Background(), "text"); !errors.Is(err, ErrInvalidResponse) {
				t.Fatalf("err = %v, want ErrInvalidResponse", err)
			}
		})
	}

	// A body that is not JSON at all is reported, whatever kind it gets.
	p := newTestProvider(t, testConfig(), func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("<html>gateway error</html>"))
	})
	if _, err := p.Embed(context.Background(), "text"); err == nil {
		t.Fatal("Embed accepted a response that is not JSON")
	}
}

// Every way the API can refuse a request becomes an error the caller can
// classify without knowing HTTP status codes or the SDK's types.
func TestGeminiEmbedConvertsAPIErrors(t *testing.T) {
	tests := []struct {
		name          string
		status        int
		body          string
		wantKind      error // nil when the status matches no kind
		wantTemporary bool
	}{
		{
			name:     "invalid api key",
			status:   http.StatusBadRequest,
			body:     `{"error": {"code": 400, "message": "API key not valid. Please pass a valid API key.", "status": "INVALID_ARGUMENT"}}`,
			wantKind: nil,
		},
		{
			name:     "unauthenticated",
			status:   http.StatusUnauthorized,
			body:     `{"error": {"code": 401, "message": "Request had invalid authentication credentials.", "status": "UNAUTHENTICATED"}}`,
			wantKind: ErrUnauthenticated,
		},
		{
			name:     "permission denied",
			status:   http.StatusForbidden,
			body:     `{"error": {"code": 403, "message": "Permission denied.", "status": "PERMISSION_DENIED"}}`,
			wantKind: ErrUnauthenticated,
		},
		{
			name:          "rate limited",
			status:        http.StatusTooManyRequests,
			body:          `{"error": {"code": 429, "message": "Resource has been exhausted (e.g. check quota).", "status": "RESOURCE_EXHAUSTED"}}`,
			wantKind:      ErrRateLimited,
			wantTemporary: true,
		},
		{
			name:          "server error",
			status:        http.StatusInternalServerError,
			body:          `{"error": {"code": 500, "message": "Internal error.", "status": "INTERNAL"}}`,
			wantKind:      ErrUnavailable,
			wantTemporary: true,
		},
		{
			name:          "service unavailable",
			status:        http.StatusServiceUnavailable,
			body:          `{"error": {"code": 503, "message": "The model is overloaded.", "status": "UNAVAILABLE"}}`,
			wantKind:      ErrUnavailable,
			wantTemporary: true,
		},
		{
			name:          "gateway error with a plain-text body",
			status:        http.StatusBadGateway,
			body:          "upstream connect error",
			wantKind:      ErrUnavailable,
			wantTemporary: true,
		},
		{
			// A proxy's JSON error without a code: the status is unknown, so
			// the failure is retried rather than dead-lettered on a guess.
			name:          "error body without a status",
			status:        http.StatusBadGateway,
			body:          `{"error": {"message": "proxy said no"}}`,
			wantKind:      nil,
			wantTemporary: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := newTestProvider(t, testConfig(), func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(tt.status)
				_, _ = w.Write([]byte(tt.body))
			})

			_, err := p.Embed(context.Background(), "text")
			if err == nil {
				t.Fatal("Embed returned no error")
			}
			assertNoSecret(t, err)

			var apiErr *APIError
			if !errors.As(err, &apiErr) {
				t.Fatalf("err = %v, want an *APIError", err)
			}
			if apiErr.Provider != "gemini" {
				t.Errorf("Provider = %q, want gemini", apiErr.Provider)
			}
			if apiErr.Temporary() != tt.wantTemporary {
				t.Errorf("Temporary() = %v, want %v", apiErr.Temporary(), tt.wantTemporary)
			}
			for _, kind := range []error{ErrUnauthenticated, ErrRateLimited, ErrUnavailable} {
				if want := kind == tt.wantKind; errors.Is(err, kind) != want {
					t.Errorf("errors.Is(err, %v) = %v, want %v", kind, !want, want)
				}
			}
		})
	}
}

// The first request is the only one: the SDK's own retries stay off, so the
// pipeline alone decides how often to try.
func TestGeminiEmbedDoesNotRetry(t *testing.T) {
	var calls atomic.Int32
	p := newTestProvider(t, testConfig(), func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusServiceUnavailable)
	})

	if _, err := p.Embed(context.Background(), "text"); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("err = %v, want ErrUnavailable", err)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("requests = %d, want 1; retrying is the caller's decision", got)
	}
}

func TestGeminiEmbedWithCancelledContextSendsNothing(t *testing.T) {
	var calls atomic.Int32
	p := newTestProvider(t, testConfig(), func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		respondWith(1, 2, 3)(w, r)
	})

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := p.Embed(ctx, "text")
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
	if calls.Load() != 0 {
		t.Errorf("sent %d requests on a cancelled context, want none", calls.Load())
	}
}

// Cancelling while the request is in flight abandons it promptly.
func TestGeminiEmbedStopsWhenTheCallerCancels(t *testing.T) {
	p, started := newBlockingProvider(t, testConfig())

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		<-started
		cancel()
	}()

	begin := time.Now()
	_, err := p.Embed(ctx, "text")
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
	// Shutdown treats a cancelled caller as a clean stop, not a failure, so
	// it must not look like the provider being down.
	if errors.Is(err, ErrUnavailable) {
		t.Errorf("a caller cancellation was reported as the provider being unavailable: %v", err)
	}
	if elapsed := time.Since(begin); elapsed > time.Second {
		t.Errorf("Embed took %s to notice the cancellation", elapsed)
	}
}

// A caller deadline shorter than the provider timeout wins, and is reported as
// the caller's deadline.
func TestGeminiEmbedRespectsTheCallerDeadline(t *testing.T) {
	cfg := testConfig()
	cfg.Timeout = 10 * time.Second
	p, _ := newBlockingProvider(t, cfg)

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	begin := time.Now()
	_, err := p.Embed(ctx, "text")
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, want context.DeadlineExceeded", err)
	}
	if errors.Is(err, ErrUnavailable) {
		t.Errorf("the caller's own deadline was blamed on the provider: %v", err)
	}
	if elapsed := time.Since(begin); elapsed > 2*time.Second {
		t.Errorf("Embed took %s, want it to stop at the caller's 50ms deadline", elapsed)
	}
}

// When the provider is slower than the configured timeout, the request is
// abandoned and reported as the provider being unavailable, which is worth
// retrying later.
func TestGeminiEmbedAppliesTheConfiguredTimeout(t *testing.T) {
	cfg := testConfig()
	cfg.Timeout = 50 * time.Millisecond
	p, _ := newBlockingProvider(t, cfg)

	begin := time.Now()
	_, err := p.Embed(context.Background(), "text")
	if !errors.Is(err, ErrUnavailable) || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, want ErrUnavailable wrapping context.DeadlineExceeded", err)
	}
	if elapsed := time.Since(begin); elapsed > 2*time.Second {
		t.Errorf("Embed took %s, want it to give up after the 50ms timeout", elapsed)
	}
}

func TestGeminiEmbedReportsAnUnreachableProvider(t *testing.T) {
	srv := httptest.NewServer(respondWith(1, 2, 3))
	baseURL := srv.URL
	srv.Close() // nothing listens there any more

	p, err := newGeminiProvider(context.Background(), testConfig(), nil, genai.HTTPOptions{BaseURL: baseURL})
	if err != nil {
		t.Fatalf("newGeminiProvider: %v", err)
	}

	_, err = p.Embed(context.Background(), "text")
	if !errors.Is(err, ErrUnavailable) {
		t.Fatalf("err = %v, want ErrUnavailable", err)
	}
	assertNoSecret(t, err)
}

func TestNewGeminiProviderRejectsInvalidConfig(t *testing.T) {
	tests := []struct {
		name   string
		modify func(*config.EmbeddingConfig)
		want   string
	}{
		{"missing api key", func(c *config.EmbeddingConfig) { c.APIKey = "" }, "EMBEDDING_API_KEY"},
		{"blank model", func(c *config.EmbeddingConfig) { c.Model = "  " }, "EMBEDDING_MODEL"},
		{"zero dimension", func(c *config.EmbeddingConfig) { c.Dimension = 0 }, "dimension"},
		{"negative dimension", func(c *config.EmbeddingConfig) { c.Dimension = -768 }, "dimension"},
		{"dimension beyond int32", func(c *config.EmbeddingConfig) { c.Dimension = math.MaxInt32 + 1 }, "dimension"},
		{"zero timeout", func(c *config.EmbeddingConfig) { c.Timeout = 0 }, "EMBEDDING_TIMEOUT"},
		{"negative timeout", func(c *config.EmbeddingConfig) { c.Timeout = -time.Second }, "EMBEDDING_TIMEOUT"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := testConfig()
			tt.modify(&cfg)

			_, err := NewGeminiProvider(context.Background(), cfg)
			if err == nil {
				t.Fatal("NewGeminiProvider accepted an invalid config")
			}
			if !strings.Contains(err.Error(), tt.want) {
				t.Errorf("err = %q, want it to mention %q", err, tt.want)
			}
		})
	}
}

// The SDK falls back to GOOGLE_API_KEY or GEMINI_API_KEY when no key is given.
// The provider must refuse instead, so it only ever uses the key it was
// configured with.
func TestNewGeminiProviderIgnoresKeysInTheEnvironment(t *testing.T) {
	t.Setenv("GOOGLE_API_KEY", "key-from-the-environment")
	t.Setenv("GEMINI_API_KEY", "key-from-the-environment")

	cfg := testConfig()
	cfg.APIKey = ""
	if _, err := NewGeminiProvider(context.Background(), cfg); err == nil {
		t.Fatal("NewGeminiProvider picked up an API key from the environment")
	}
}

// The SDK switches to Vertex AI when GOOGLE_GENAI_USE_VERTEXAI is set and no
// backend is chosen. The provider pins the Gemini API so the environment
// cannot redirect requests.
func TestGeminiProviderIgnoresBackendSwitchInTheEnvironment(t *testing.T) {
	t.Setenv("GOOGLE_GENAI_USE_VERTEXAI", "true")
	t.Setenv("GOOGLE_CLOUD_PROJECT", "some-project")
	t.Setenv("GOOGLE_CLOUD_LOCATION", "us-central1")

	var path string
	p := newTestProvider(t, testConfig(), func(w http.ResponseWriter, r *http.Request) {
		path = r.URL.Path
		respondWith(1, 2, 3)(w, r)
	})
	if _, err := p.Embed(context.Background(), "text"); err != nil {
		t.Fatalf("Embed: %v", err)
	}
	if !strings.HasSuffix(path, ":batchEmbedContents") || strings.Contains(path, "projects/") {
		t.Fatalf("request went to %q, want the Gemini API endpoint", path)
	}
}

func TestNewSelectsTheConfiguredProvider(t *testing.T) {
	p, err := New(context.Background(), testConfig())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if _, ok := p.(*GeminiProvider); !ok {
		t.Fatalf("New returned %T, want *GeminiProvider", p)
	}
	if p.Name() != "gemini" || p.Dimension() != 3 {
		t.Errorf("Name/Dimension = %q/%d, want gemini/3", p.Name(), p.Dimension())
	}

	for _, name := range []string{"", "openai", "Gemini"} {
		cfg := testConfig()
		cfg.Provider = name
		if _, err := New(context.Background(), cfg); !errors.Is(err, ErrUnknownProvider) {
			t.Errorf("New(%q) = %v, want ErrUnknownProvider", name, err)
		}
	}
}

// One provider is shared by every indexing worker.
func TestGeminiProviderIsSafeForConcurrentUse(t *testing.T) {
	var calls atomic.Int32
	p := newTestProvider(t, testConfig(), func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		respondWith(0.1, 0.2, 0.3)(w, r)
	})

	const goroutines = 16
	var wg sync.WaitGroup
	for i := range goroutines {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := p.Embed(context.Background(), fmt.Sprintf("text %d", i)); err != nil {
				t.Errorf("Embed: %v", err)
			}
		}()
	}
	wg.Wait()
	if got := calls.Load(); got != goroutines {
		t.Errorf("requests = %d, want %d", got, goroutines)
	}
}

func TestCheckVector(t *testing.T) {
	nan, inf := float32(math.NaN()), float32(math.Inf(1))

	tests := []struct {
		name    string
		vector  []float32
		wantErr error
	}{
		{"valid", []float32{0.1, -0.2, 0.3}, nil},
		{"wrong length", []float32{0.1, 0.2}, ErrDimensionMismatch},
		{"nil", nil, ErrDimensionMismatch},
		{"not a number", []float32{0.1, nan, 0.3}, ErrInvalidResponse},
		{"infinite", []float32{inf, 0.2, 0.3}, ErrInvalidResponse},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := checkVector(tt.vector, 3); !errors.Is(err, tt.wantErr) {
				t.Fatalf("checkVector = %v, want %v", err, tt.wantErr)
			}
		})
	}
}

// Provider error bodies are kept short, so an unexpected response cannot
// flood the logs.
func TestAPIErrorKeepsMessagesShort(t *testing.T) {
	err := newAPIError("gemini", http.StatusBadGateway, strings.Repeat("x", 10*maxErrorMessage))
	if len(err.Message) > maxErrorMessage+len("...") {
		t.Fatalf("message is %d bytes, want at most %d", len(err.Message), maxErrorMessage+3)
	}
}
