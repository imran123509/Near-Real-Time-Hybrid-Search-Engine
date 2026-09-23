package embedding

import (
	"context"
	"errors"
	"math"
	"slices"
	"sync"
	"testing"

	"near-real-time-hybrid-search-engine/internal/config"
)

func newFake(t *testing.T, dimension int) *FakeProvider {
	t.Helper()
	p, err := NewFakeProvider(config.EmbeddingConfig{Provider: ProviderFake, Dimension: dimension})
	if err != nil {
		t.Fatalf("NewFakeProvider: %v", err)
	}
	return p
}

// cosine is the similarity Qdrant compares stored vectors with.
func cosine(a, b []float32) float64 {
	var dot float64
	for i := range a {
		dot += float64(a[i]) * float64(b[i])
	}
	return dot // both vectors are unit length
}

func TestFakeProviderIsDeterministic(t *testing.T) {
	p := newFake(t, 64)
	ctx := context.Background()

	first, err := p.Embed(ctx, "title: Go concurrency | text: goroutines and channels")
	if err != nil {
		t.Fatal(err)
	}
	second, err := p.Embed(ctx, "title: Go concurrency | text: goroutines and channels")
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(first, second) {
		t.Error("the same text produced different vectors")
	}

	other, err := p.Embed(ctx, "title: Kafka partitions | text: brokers and offsets")
	if err != nil {
		t.Fatal(err)
	}
	if slices.Equal(first, other) {
		t.Error("different texts produced the same vector")
	}
}

func TestFakeProviderVectorShape(t *testing.T) {
	for _, dimension := range []int{4, 64, 768} {
		p := newFake(t, dimension)
		v, err := p.Embed(context.Background(), "distributed systems")
		if err != nil {
			t.Fatal(err)
		}
		if len(v) != dimension || p.Dimension() != dimension {
			t.Fatalf("got %d values, want %d", len(v), dimension)
		}
		// Unit length, so cosine similarity is the dot product and Qdrant
		// never sees a zero vector.
		if norm := math.Sqrt(cosine(v, v)); math.Abs(norm-1) > 1e-6 {
			t.Errorf("dimension %d: norm = %v, want 1", dimension, norm)
		}
	}
	if name := newFake(t, 8).Name(); name != ProviderFake {
		t.Errorf("Name() = %q, want %q", name, ProviderFake)
	}
}

// TestFakeProviderSharedWordsAreCloser is what makes the fake useful in the
// end-to-end tests: vector search has to rank a related document above an
// unrelated one, as a real model would.
func TestFakeProviderSharedWordsAreCloser(t *testing.T) {
	p := newFake(t, 256)
	ctx := context.Background()

	query, err := p.Embed(ctx, "task: search result | query: goroutines and channels")
	if err != nil {
		t.Fatal(err)
	}
	related, err := p.Embed(ctx, "title: Go concurrency | text: goroutines communicate over channels")
	if err != nil {
		t.Fatal(err)
	}
	unrelated, err := p.Embed(ctx, "title: Baking bread | text: flour water salt and yeast")
	if err != nil {
		t.Fatal(err)
	}

	if cosine(query, related) <= cosine(query, unrelated) {
		t.Errorf("related %.4f is not closer than unrelated %.4f",
			cosine(query, related), cosine(query, unrelated))
	}
}

func TestFakeProviderRejectsEmptyText(t *testing.T) {
	p := newFake(t, 16)
	for _, text := range []string{"", "   ", "\t\n"} {
		if _, err := p.Embed(context.Background(), text); !errors.Is(err, ErrEmptyText) {
			t.Errorf("Embed(%q) = %v, want ErrEmptyText", text, err)
		}
	}
	// Punctuation alone is still something to embed.
	if _, err := p.Embed(context.Background(), "!!!"); err != nil {
		t.Errorf("Embed(%q): %v", "!!!", err)
	}
}

func TestFakeProviderHonoursContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := newFake(t, 16).Embed(ctx, "anything"); !errors.Is(err, context.Canceled) {
		t.Errorf("err = %v, want context.Canceled", err)
	}
}

func TestFakeProviderRejectsBadDimension(t *testing.T) {
	for _, dimension := range []int{0, -1} {
		if _, err := NewFakeProvider(config.EmbeddingConfig{Dimension: dimension}); err == nil {
			t.Errorf("NewFakeProvider accepted dimension %d", dimension)
		}
	}
}

func TestFakeProviderIsSafeForConcurrentUse(t *testing.T) {
	p := newFake(t, 128)
	want, err := p.Embed(context.Background(), "concurrent access")
	if err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			got, err := p.Embed(context.Background(), "concurrent access")
			if err != nil || !slices.Equal(got, want) {
				t.Errorf("concurrent Embed = %v, err %v", got[:min(4, len(got))], err)
			}
		})
	}
	wg.Wait()
}

func TestNewReturnsTheFakeProvider(t *testing.T) {
	p, err := New(context.Background(), config.EmbeddingConfig{Provider: ProviderFake, Dimension: 32})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if p.Name() != ProviderFake || p.Dimension() != 32 {
		t.Errorf("got %s/%d, want fake/32", p.Name(), p.Dimension())
	}
}
