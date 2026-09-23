package cdc

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// The fixtures in testdata/debezium are change events for the documents table
// in the shape docker/debezium/connector.json makes Debezium 3.6 emit:
// pgoutput, JsonConverter with schemas disabled, TIMESTAMPTZ as an ISO-8601
// string, BIGINT as a JSON number, and the table's default REPLICA IDENTITY,
// under which an update carries no "before" and a delete's "before" holds only
// the primary key. If the connector configuration changes, update these too.

func loadFixture(t *testing.T, name string) []byte {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("testdata", "debezium", name))
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func TestDebeziumFixturesParse(t *testing.T) {
	const newDoc = "5f2a9c1e-7b3d-4a6f-8e21-9c0d1b2a3f44"

	tests := []struct {
		fixture string
		op      Operation
		id      string
	}{
		{"snapshot-read.json", OperationRead, "0b7d4f6e-3c1a-4e2b-9f5d-1a2b3c4d5e03"},
		{"create.json", OperationCreate, newDoc},
		{"update.json", OperationUpdate, newDoc},
		{"delete.json", OperationDelete, newDoc},
	}
	for _, tt := range tests {
		t.Run(tt.fixture, func(t *testing.T) {
			ev, err := ParseChangeEvent(loadFixture(t, tt.fixture))
			if err != nil {
				t.Fatalf("ParseChangeEvent: %v", err)
			}
			if ev.Operation != tt.op || ev.ID != tt.id {
				t.Errorf("got %s %s, want %s %s", ev.Operation, ev.ID, tt.op, tt.id)
			}
			if ev.Schema != "public" || ev.Table != "documents" {
				t.Errorf("source = %s.%s, want public.documents", ev.Schema, ev.Table)
			}
			if ev.Timestamp.IsZero() {
				t.Error("no timestamp taken from source.ts_ms")
			}
			if err := ev.Validate(); err != nil {
				t.Errorf("Validate: %v", err)
			}
		})
	}
}

func TestDebeziumFixturesMapToSearchDocuments(t *testing.T) {
	mapping := DefaultMapping()

	t.Run("snapshot row with every column", func(t *testing.T) {
		ev, err := ParseChangeEvent(loadFixture(t, "snapshot-read.json"))
		if err != nil {
			t.Fatal(err)
		}
		doc, err := mapping.Document(ev)
		if err != nil {
			t.Fatalf("Document: %v", err)
		}
		wantTime := time.Date(2026, 9, 22, 8, 0, 0, 123456000, time.UTC)
		if doc.Title != "Go Concurrency Patterns" || doc.URL != "https://example.com/articles/go-concurrency" ||
			doc.Version != 3 || !doc.UpdatedAt.Equal(wantTime) || doc.Content == "" {
			t.Errorf("document = %+v", doc)
		}
	})

	t.Run("NULL url", func(t *testing.T) {
		ev, err := ParseChangeEvent(loadFixture(t, "create.json"))
		if err != nil {
			t.Fatal(err)
		}
		doc, err := mapping.Document(ev)
		if err != nil {
			t.Fatalf("Document: %v", err)
		}
		if doc.URL != "" || doc.Version != 11 {
			t.Errorf("document = %+v, want no URL and version 11", doc)
		}
	})

	t.Run("update carries a higher version", func(t *testing.T) {
		created, _ := ParseChangeEvent(loadFixture(t, "create.json"))
		updated, _ := ParseChangeEvent(loadFixture(t, "update.json"))
		before, _ := mapping.Document(created)
		after, err := mapping.Document(updated)
		if err != nil {
			t.Fatal(err)
		}
		// OpenSearch applies a write only when its version is higher, which
		// the database trigger in migrations/0002 guarantees.
		if after.Version <= before.Version {
			t.Errorf("update version %d is not above create version %d", after.Version, before.Version)
		}
	})
}

func TestDebeziumTombstoneIsNotAnEvent(t *testing.T) {
	// With tombstones.on.delete=true, every delete is followed by a record
	// with the same key and a null value.
	if _, err := ParseChangeEvent(nil); !errors.Is(err, ErrTombstone) {
		t.Errorf("err = %v, want ErrTombstone", err)
	}
}
