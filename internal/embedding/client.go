// Package embedding turns text into vectors with the Gemini embedding API.
package embedding

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"

	"near-real-time-hybrid-search-engine/internal/config"
)

const (
	defaultBaseURL = "https://generativelanguage.googleapis.com/v1beta"
	requestTimeout = 30 * time.Second
	maxErrorBody   = 1 << 12
)

// APIError is a non-2xx response from the Gemini API.
type APIError struct {
	StatusCode int
	Message    string
}

func (e *APIError) Error() string {
	return fmt.Sprintf("gemini api: status %d: %s", e.StatusCode, e.Message)
}

// Temporary reports whether retrying the request may succeed.
func (e *APIError) Temporary() bool {
	return e.StatusCode == http.StatusTooManyRequests ||
		e.StatusCode == http.StatusRequestTimeout ||
		e.StatusCode >= http.StatusInternalServerError
}

// Client calls the Gemini embedContent endpoint. It is safe for concurrent use.
type Client struct {
	httpClient *http.Client
	baseURL    string
	apiKey     string
	model      string
	dimensions int
}

// NewClient returns a Client for the configured model.
func NewClient(cfg config.GeminiConfig) (*Client, error) {
	if cfg.APIKey == "" {
		return nil, errors.New("GEMINI_API_KEY is required")
	}
	if cfg.Model == "" {
		return nil, errors.New("gemini embedding model is required")
	}
	return &Client{
		httpClient: &http.Client{Timeout: requestTimeout},
		baseURL:    defaultBaseURL,
		apiKey:     cfg.APIKey,
		model:      cfg.Model,
		dimensions: cfg.Dimensions,
	}, nil
}

// EmbedDocument returns the vector for a document to be stored and retrieved.
// gemini-embedding-2 has no task type parameter; the documented
// "title: ... | text: ..." format marks the input as a retrieval document.
func (c *Client) EmbedDocument(ctx context.Context, title, body string) ([]float32, error) {
	if title == "" {
		title = "none"
	}
	return c.embed(ctx, "title: "+title+" | text: "+body)
}

type part struct {
	Text string `json:"text"`
}

type content struct {
	Parts []part `json:"parts"`
}

type embedRequest struct {
	Model                string  `json:"model"`
	Content              content `json:"content"`
	OutputDimensionality int     `json:"output_dimensionality,omitempty"`
}

type embedResponse struct {
	Embedding struct {
		Values []float32 `json:"values"`
	} `json:"embedding"`
}

func (c *Client) embed(ctx context.Context, text string) ([]float32, error) {
	reqBody := embedRequest{
		Model:                "models/" + c.model,
		Content:              content{Parts: []part{{Text: text}}},
		OutputDimensionality: c.dimensions,
	}

	payload, err := json.Marshal(reqBody)
	if err != nil {
		return nil, fmt.Errorf("encode request: %w", err)
	}

	endpoint := c.baseURL + "/models/" + url.PathEscape(c.model) + ":embedContent"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(payload))
	if err != nil {
		return nil, fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("x-goog-api-key", c.apiKey)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("send request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, maxErrorBody))
		return nil, &APIError{StatusCode: resp.StatusCode, Message: string(bytes.TrimSpace(msg))}
	}

	var out embedResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, fmt.Errorf("decode response: %w", err)
	}
	if c.dimensions > 0 && len(out.Embedding.Values) != c.dimensions {
		return nil, fmt.Errorf("gemini returned %d dimensions, want %d", len(out.Embedding.Values), c.dimensions)
	}
	return out.Embedding.Values, nil
}
