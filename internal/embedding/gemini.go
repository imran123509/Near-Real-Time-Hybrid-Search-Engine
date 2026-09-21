package embedding

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strings"
	"time"

	"google.golang.org/genai"

	"near-real-time-hybrid-search-engine/internal/config"
)

// ProviderGemini is the EMBEDDING_PROVIDER value that selects GeminiProvider.
const ProviderGemini = "gemini"

// GeminiProvider embeds text with the Gemini API through Google's official Go
// SDK, google.golang.org/genai. It is safe for concurrent use and meant to be
// shared for the life of the process.
//
// The model and the output dimension both come from configuration. The
// dimension is sent as outputDimensionality, so the API returns a vector of
// exactly that length; gemini-embedding-2 normalizes a shortened vector
// itself. The length of every response is still checked, because a model
// change or an API change must fail loudly rather than write a wrong-sized
// vector to Qdrant.
type GeminiProvider struct {
	client    *genai.Client
	model     string
	dimension int
	timeout   time.Duration
}

// NewGeminiProvider validates cfg and returns a provider. It does not contact
// the API.
func NewGeminiProvider(ctx context.Context, cfg config.EmbeddingConfig) (*GeminiProvider, error) {
	return newGeminiProvider(ctx, cfg, nil, genai.HTTPOptions{})
}

// newGeminiProvider lets tests point the SDK at a local server.
func newGeminiProvider(ctx context.Context, cfg config.EmbeddingConfig, httpClient *http.Client, httpOptions genai.HTTPOptions) (*GeminiProvider, error) {
	switch {
	// Checked here rather than left to the SDK, which would otherwise fall
	// back to GOOGLE_API_KEY or GEMINI_API_KEY and silently use a key this
	// application was never configured with.
	case cfg.APIKey == "":
		return nil, errors.New("gemini: EMBEDDING_API_KEY is required")
	case strings.TrimSpace(cfg.Model) == "":
		return nil, errors.New("gemini: EMBEDDING_MODEL is required")
	case cfg.Timeout <= 0:
		return nil, fmt.Errorf("gemini: EMBEDDING_TIMEOUT must be positive, got %s", cfg.Timeout)
	}
	if err := validateDimension(cfg.Dimension); err != nil {
		return nil, fmt.Errorf("gemini: %w", err)
	}

	// Retries are deliberately left off: with no RetryOptions the SDK sends
	// each request once, and the indexing pipeline alone decides on retries.
	client, err := genai.NewClient(ctx, &genai.ClientConfig{
		APIKey: cfg.APIKey,
		// Set explicitly so GOOGLE_GENAI_USE_VERTEXAI in the environment
		// cannot move requests to a different backend.
		Backend:     genai.BackendGeminiAPI,
		HTTPClient:  httpClient,
		HTTPOptions: httpOptions,
	})
	if err != nil {
		return nil, fmt.Errorf("gemini: create client: %w", err)
	}

	return &GeminiProvider{
		client:    client,
		model:     cfg.Model,
		dimension: cfg.Dimension,
		timeout:   cfg.Timeout,
	}, nil
}

// Name returns "gemini".
func (p *GeminiProvider) Name() string { return ProviderGemini }

// Dimension returns the configured vector length.
func (p *GeminiProvider) Dimension() int { return p.dimension }

// Embed returns the vector for text.
//
// The request runs under ctx with the configured timeout added on top, so the
// caller's cancellation or a shorter caller deadline still applies. Errors
// match the kinds in this package with errors.Is, and cancellation and
// deadline errors keep matching context.Canceled and context.DeadlineExceeded.
func (p *GeminiProvider) Embed(ctx context.Context, text string) ([]float32, error) {
	if strings.TrimSpace(text) == "" {
		return nil, ErrEmptyText
	}
	// A request that cannot finish is not worth starting.
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("gemini: embed: %w", err)
	}

	reqCtx, cancel := context.WithTimeout(ctx, p.timeout)
	defer cancel()

	dimension := int32(p.dimension) // validateDimension keeps it in range
	resp, err := p.client.Models.EmbedContent(reqCtx, p.model, genai.Text(text), &genai.EmbedContentConfig{
		OutputDimensionality: &dimension,
	})
	if err != nil {
		return nil, p.requestError(ctx, reqCtx, err)
	}

	if resp == nil || len(resp.Embeddings) != 1 || resp.Embeddings[0] == nil {
		got := 0
		if resp != nil {
			got = len(resp.Embeddings)
		}
		return nil, fmt.Errorf("gemini: %w: want 1 embedding, got %d", ErrInvalidResponse, got)
	}
	vector := resp.Embeddings[0].Values
	if err := checkVector(vector, p.dimension); err != nil {
		return nil, fmt.Errorf("gemini: model %s: %w", p.model, err)
	}
	return vector, nil
}

// requestError converts an SDK failure into this package's error kinds.
//
// It tells apart the three ways a request can run out of time: the caller
// cancelled or its own deadline passed (returned as the caller's context
// error, which the pipeline treats as shutdown rather than failure), or this
// provider's timeout fired while the caller was still waiting (reported as
// the provider being unavailable, which is worth retrying).
func (p *GeminiProvider) requestError(ctx, reqCtx context.Context, err error) error {
	if ctx.Err() != nil {
		return fmt.Errorf("gemini: embed: %w", ctx.Err())
	}
	if reqCtx.Err() != nil {
		return fmt.Errorf("gemini: %w: no response within %s: %w", ErrUnavailable, p.timeout, reqCtx.Err())
	}

	var apiErr genai.APIError
	if errors.As(err, &apiErr) {
		message := apiErr.Message
		if message == "" {
			message = apiErr.Status
		}
		return fmt.Errorf("gemini: %w", newAPIError(ProviderGemini, apiErr.Code, message))
	}

	// A transport failure: the connection was refused, reset or never made.
	// The API key travels in a header, never in the URL, so this error is
	// safe to log.
	var netErr net.Error
	if errors.As(err, &netErr) {
		return fmt.Errorf("gemini: %w: %w", ErrUnavailable, err)
	}
	return fmt.Errorf("gemini: embed: %w", err)
}
