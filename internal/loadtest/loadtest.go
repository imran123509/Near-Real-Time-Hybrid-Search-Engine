// Package loadtest generates deterministic traffic for the benchmark suite in
// benchmarks/ and measures what the system does with it.
//
// It is a measuring instrument, not part of the running system: nothing in
// cmd/api or cmd/consumer imports it, and it changes no production behaviour.
// What it does use is the real path — rows go into PostgreSQL and travel
// through Debezium, Kafka and the consumer like any other change — because a
// benchmark of a stubbed pipeline measures the stub.
//
//	generator -> PostgreSQL -> Debezium -> Kafka -> consumer -> OpenSearch, Qdrant
//	                                                                  ^
//	                                                            watcher polls
//
// Everything it produces is deterministic: the same run ID always produces the
// same documents with the same IDs, so a benchmark can be repeated, compared
// and cleaned up exactly.
package loadtest

import (
	"fmt"
	"strings"

	"github.com/google/uuid"
)

// URLPrefix marks every document this package creates. Cleanup deletes rows by
// this prefix, so it can never touch a document that a benchmark did not make.
const URLPrefix = "https://example.com/loadtest/"

// idNamespace roots the document IDs. Changing it changes every generated ID,
// which would leave older runs' rows behind under a different prefix.
var idNamespace = uuid.MustParse("9c1d5f30-6b21-4a2e-8f7c-3d0e5a6b7c80")

// topics are the subjects generated documents are written about. They match
// the query set the search benchmark sends, so those queries hit real
// documents instead of measuring how fast the system returns nothing.
var topics = []string{
	"distributed systems",
	"kafka consumer",
	"database indexing",
	"machine learning",
	"vector search",
	"microservices",
	"golang backend",
	"event driven architecture",
}

// Topics returns the subjects documents are generated about.
func Topics() []string { return append([]string(nil), topics...) }

// Document is one generated row, in the shape of the documents table.
type Document struct {
	ID    string
	Title string
	Body  string
	URL   string
}

// Generator produces documents for one benchmark run.
//
// A run ID separates one run's documents from another's while keeping each
// run reproducible: the same run ID always yields the same IDs and text, so a
// repeated run overwrites its own rows instead of growing the table.
type Generator struct {
	runID string
}

// NewGenerator returns a generator for runID, which must not be empty.
func NewGenerator(runID string) (*Generator, error) {
	if strings.TrimSpace(runID) == "" {
		return nil, fmt.Errorf("loadtest: a run id is required")
	}
	return &Generator{runID: strings.TrimSpace(runID)}, nil
}

// RunID returns the run this generator belongs to.
func (g *Generator) RunID() string { return g.runID }

// Document returns document n of this run, counting from 0. It is a pure
// function of the run ID and n: no clock, no randomness, nothing that would
// make two runs differ.
func (g *Generator) Document(n int) Document {
	topic := topics[n%len(topics)]
	id := uuid.NewSHA1(idNamespace, fmt.Appendf(nil, "%s/%d", g.runID, n)).String()

	// The run ID appears in the title and body as one word, so a search can
	// match this run's documents and only this run's.
	marker := "lt" + strings.ReplaceAll(g.runID, "-", "")
	title := fmt.Sprintf("%s notes %d %s", capitalize(topic), n, marker)
	body := fmt.Sprintf(
		"This document covers %s for benchmark run %s, entry %d. "+
			"It describes how %s behaves under load, which operations dominate its cost, "+
			"and what a reader should measure before changing anything. "+
			"Marker %s identifies it. The text is long enough to be scored like a real "+
			"document by BM25 and to give the embedding model something to work with.",
		topic, g.runID, n, topic, marker)

	return Document{
		ID:    id,
		Title: title,
		Body:  body,
		URL:   fmt.Sprintf("%s%s/%d", URLPrefix, g.runID, n),
	}
}

// Documents returns the first count documents of this run.
func (g *Generator) Documents(count int) []Document {
	docs := make([]Document, 0, max(count, 0))
	for n := range count {
		docs = append(docs, g.Document(n))
	}
	return docs
}

// Marker is the word that appears in every document of this run, for searching
// and for reading the logs.
func (g *Generator) Marker() string {
	return "lt" + strings.ReplaceAll(g.runID, "-", "")
}

func capitalize(s string) string {
	if s == "" {
		return s
	}
	return strings.ToUpper(s[:1]) + s[1:]
}
