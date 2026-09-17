package embedding

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"near-real-time-hybrid-search-engine/internal/config"
)

func newTestClient(t *testing.T, handler http.HandlerFunc) *Client {
	t.Helper()
	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)

	c, err := NewClient(config.GeminiConfig{APIKey: "test-key", Model: "gemini-embedding-2", Dimensions: 3})
	if err != nil {
		t.Fatal(err)
	}
	c.baseURL = srv.URL
	return c
}

func TestEmbedDocumentSendsDocumentFormat(t *testing.T) {
	tests := []struct {
		title, body, wantText string
	}{
		{"Go", "Channels", "title: Go | text: Channels"},
		{"", "Untitled", "title: none | text: Untitled"},
	}
	for _, tt := range tests {
		c := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path != "/models/gemini-embedding-2:embedContent" {
				t.Errorf("path = %q", r.URL.Path)
			}
			if got := r.Header.Get("x-goog-api-key"); got != "test-key" {
				t.Errorf("api key header = %q", got)
			}
			var req embedRequest
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				t.Errorf("decode request: %v", err)
				return
			}
			if req.Model != "models/gemini-embedding-2" || req.OutputDimensionality != 3 {
				t.Errorf("model = %q, dimensions = %d", req.Model, req.OutputDimensionality)
			}
			if got := req.Content.Parts[0].Text; got != tt.wantText {
				t.Errorf("text = %q, want %q", got, tt.wantText)
			}
			_, _ = w.Write([]byte(`{"embedding":{"values":[0.1,0.2,0.3]}}`))
		})

		got, err := c.EmbedDocument(context.Background(), tt.title, tt.body)
		if err != nil {
			t.Fatal(err)
		}
		if len(got) != 3 {
			t.Fatalf("got %d values, want 3", len(got))
		}
	}
}

func TestEmbedDocumentErrors(t *testing.T) {
	tests := []struct {
		name          string
		status        int
		body          string
		wantAPIErr    bool
		wantTemporary bool
	}{
		{"rate limited", http.StatusTooManyRequests, `{"error":{"message":"quota"}}`, true, true},
		{"server error", http.StatusServiceUnavailable, `{}`, true, true},
		{"bad request", http.StatusBadRequest, `{"error":{"message":"invalid"}}`, true, false},
		{"wrong dimensions", http.StatusOK, `{"embedding":{"values":[0.1]}}`, false, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := newTestClient(t, func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(tt.status)
				_, _ = w.Write([]byte(tt.body))
			})

			_, err := c.EmbedDocument(context.Background(), "t", "b")
			if err == nil {
				t.Fatal("expected an error")
			}
			var apiErr *APIError
			if errors.As(err, &apiErr) != tt.wantAPIErr {
				t.Fatalf("APIError = %v, want %v (err: %v)", !tt.wantAPIErr, tt.wantAPIErr, err)
			}
			if tt.wantAPIErr && apiErr.Temporary() != tt.wantTemporary {
				t.Fatalf("Temporary() = %v, want %v", apiErr.Temporary(), tt.wantTemporary)
			}
		})
	}
}

func TestNewClientRequiresAPIKey(t *testing.T) {
	if _, err := NewClient(config.GeminiConfig{Model: "gemini-embedding-2"}); err == nil {
		t.Fatal("expected an error without an API key")
	}
}
