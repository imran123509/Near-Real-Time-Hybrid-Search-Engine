package cdc

import (
	"strings"
	"testing"
)

// Every change event passes through these three steps on the consumer's hot
// path, before any network call happens:
//
//	bytes -> ParseChangeEvent -> FieldMapping.Document -> BuildEmbeddingText
//
// They are pure computation, so they are the part of indexing that can be
// measured exactly. If the indexing benchmark shows a low event rate, these
// numbers say whether parsing and mapping could possibly be the reason, which
// they almost certainly are not: the interesting cost is downstream.
//
//	go test -run '^$' -bench . -benchmem ./internal/indexing/cdc/

func benchEvent(bodyWords int) []byte {
	body := strings.TrimSpace(strings.Repeat("change data capture keeps the indexes current ", bodyWords))
	return []byte(debeziumEvent("u", "null", documentRow(testDocumentID, "Benchmark document", body, 42)))
}

func BenchmarkParseChangeEvent(b *testing.B) {
	// A short row and a long one: parsing cost follows the payload, and a real
	// document body is closer to the long case.
	sizes := map[string]int{"short": 1, "long": 40}
	for name, words := range sizes {
		value := benchEvent(words)
		b.Run(name, func(b *testing.B) {
			b.SetBytes(int64(len(value)))
			b.ReportAllocs()
			for b.Loop() {
				if _, err := ParseChangeEvent(value); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// A tombstone is rejected before any JSON is decoded, which is what keeps
// routine delete traffic cheap.
func BenchmarkParseTombstone(b *testing.B) {
	b.ReportAllocs()
	for b.Loop() {
		if _, err := ParseChangeEvent(nil); err == nil {
			b.Fatal("a tombstone should be reported as one")
		}
	}
}

// BenchmarkDocumentMapping measures turning a parsed row into the document
// both stores are written from.
func BenchmarkDocumentMapping(b *testing.B) {
	event, err := ParseChangeEvent(benchEvent(40))
	if err != nil {
		b.Fatal(err)
	}
	mapping := DefaultMapping()

	b.ReportAllocs()
	for b.Loop() {
		if _, err := mapping.Document(event); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkBuildEmbeddingText measures the rule that decides what text is sent
// to the embedding provider. It runs once per upserted document.
func BenchmarkBuildEmbeddingText(b *testing.B) {
	event, err := ParseChangeEvent(benchEvent(40))
	if err != nil {
		b.Fatal(err)
	}
	doc, err := DefaultMapping().Document(event)
	if err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	for b.Loop() {
		if BuildEmbeddingText(doc) == "" {
			b.Fatal("no text to embed")
		}
	}
}

// BenchmarkEventIdentity measures building an event's identity, which every
// event does once for its log line.
func BenchmarkEventIdentity(b *testing.B) {
	event, err := ParseChangeEvent(benchEvent(1))
	if err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	for b.Loop() {
		if event.EventID() == "" {
			b.Fatal("no event id")
		}
	}
}
