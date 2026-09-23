package embedding

import (
	"context"
	"crypto/sha256"
	"fmt"
	"math"
	"math/rand/v2"
	"strings"
	"unicode"

	"near-real-time-hybrid-search-engine/internal/config"
)

// ProviderFake selects FakeProvider with EMBEDDING_PROVIDER=fake.
const ProviderFake = "fake"

// FakeProvider turns text into vectors locally, for tests and for running the
// stack without an embedding account. It contacts nothing, needs no API key
// and always returns the same vector for the same text.
//
// It is not a language model: it hashes words rather than understanding them.
// Two texts that share words come out closer together than two that share
// none, which is enough for a test to tell that vector search is wired up,
// and nowhere near enough for real retrieval quality. Never run it in
// production; the services refuse to when APP_ENV is production.
type FakeProvider struct {
	dimension int
}

// NewFakeProvider returns a provider producing cfg.Dimension values.
func NewFakeProvider(cfg config.EmbeddingConfig) (*FakeProvider, error) {
	if err := validateDimension(cfg.Dimension); err != nil {
		return nil, fmt.Errorf("%s: %w", ProviderFake, err)
	}
	return &FakeProvider{dimension: cfg.Dimension}, nil
}

// Name returns "fake".
func (p *FakeProvider) Name() string { return ProviderFake }

// Dimension returns the configured vector length.
func (p *FakeProvider) Dimension() int { return p.dimension }

// Embed returns the vector for text.
//
// Each word contributes a vector drawn from a generator seeded with that
// word, the contributions are summed, and the result is scaled to unit
// length. So the same text always gives the same vector, texts sharing words
// point in similar directions, and the vector is never all zeros, which
// cosine similarity cannot compare.
func (p *FakeProvider) Embed(ctx context.Context, text string) ([]float32, error) {
	if strings.TrimSpace(text) == "" {
		return nil, ErrEmptyText
	}
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("%s: embed: %w", ProviderFake, err)
	}

	sum := make([]float64, p.dimension)
	for _, word := range words(text) {
		addWordVector(sum, word)
	}

	vector := make([]float32, p.dimension)
	var norm float64
	for _, x := range sum {
		norm += x * x
	}
	norm = math.Sqrt(norm)
	if norm == 0 {
		// Cannot happen for non-empty text, but a zero vector would be
		// rejected by Qdrant rather than silently stored.
		return nil, fmt.Errorf("%s: %w: text produced a zero vector", ProviderFake, ErrInvalidResponse)
	}
	for i, x := range sum {
		vector[i] = float32(x / norm)
	}

	if err := checkVector(vector, p.dimension); err != nil {
		return nil, fmt.Errorf("%s: %w", ProviderFake, err)
	}
	return vector, nil
}

// addWordVector adds one word's contribution to sum.
func addWordVector(sum []float64, word string) {
	seed := sha256.Sum256([]byte(word))
	rng := rand.New(rand.NewChaCha8(seed))
	for i := range sum {
		sum[i] += rng.NormFloat64()
	}
}

// words splits text into lowercase words. Anything that is not a letter or a
// digit separates them, so punctuation and the "title: ... | text: ..." shape
// the indexer uses do not become part of a word.
func words(text string) []string {
	fields := strings.FieldsFunc(strings.ToLower(text), func(r rune) bool {
		return !unicode.IsLetter(r) && !unicode.IsDigit(r)
	})
	if len(fields) == 0 {
		// Text made only of punctuation still has to embed to something.
		return []string{strings.TrimSpace(text)}
	}
	return fields
}
