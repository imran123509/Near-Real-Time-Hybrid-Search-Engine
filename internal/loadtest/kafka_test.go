package loadtest

import (
	"testing"
	"time"

	"near-real-time-hybrid-search-engine/internal/indexing/cdc"
)

// The events this package publishes are only useful if the consumer reads
// them exactly as it reads Debezium's, so they are checked with the parser the
// consumer uses rather than against a copy of the expected JSON.
func TestChangeEventIsWhatTheConsumerParses(t *testing.T) {
	g, _ := NewGenerator("bench-kafka")
	doc := g.Document(3)
	at := time.Date(2026, 5, 1, 10, 0, 0, 0, time.UTC)

	tests := []struct {
		op      string
		want    cdc.Operation
		wantRow string // the title the parser should find
	}{
		{"c", cdc.OperationCreate, doc.Title},
		{"u", cdc.OperationUpdate, doc.Title},
		{"r", cdc.OperationRead, doc.Title},
		{"d", cdc.OperationDelete, doc.Title}, // a delete carries the old row
	}
	for _, tt := range tests {
		t.Run(tt.op, func(t *testing.T) {
			key, value, err := ChangeEvent(doc, tt.op, 42, at)
			if err != nil {
				t.Fatalf("ChangeEvent: %v", err)
			}

			ev, err := cdc.ParseChangeEvent(value)
			if err != nil {
				t.Fatalf("the consumer cannot parse this event: %v", err)
			}
			if ev.ID != doc.ID {
				t.Errorf("document id = %q, want %q", ev.ID, doc.ID)
			}
			if ev.Operation != tt.want {
				t.Errorf("operation = %q, want %q", ev.Operation, tt.want)
			}
			if title, _ := ev.Row["title"].(string); title != tt.wantRow {
				t.Errorf("row title = %q, want %q", title, tt.wantRow)
			}
			if ev.Schema != "public" || ev.Table != "documents" {
				t.Errorf("source = %s.%s, want public.documents", ev.Schema, ev.Table)
			}
			if ev.LSN != 42 {
				t.Errorf("lsn = %d, want the version 42 that stands in for it", ev.LSN)
			}
			if !ev.Timestamp.Equal(at) {
				t.Errorf("timestamp = %s, want %s", ev.Timestamp, at)
			}
			if want := `{"id":"` + doc.ID + `"}`; string(key) != want {
				t.Errorf("key = %s, want %s", key, want)
			}
		})
	}
}

// Two deliveries of one event must look identical to the consumer, or the
// duplicate benchmark would be measuring updates instead.
func TestRepeatedChangeEventsAreIdentical(t *testing.T) {
	g, _ := NewGenerator("bench-kafka")
	doc := g.Document(1)
	at := time.Date(2026, 5, 1, 10, 0, 0, 0, time.UTC)

	firstKey, firstValue, err := ChangeEvent(doc, "c", 7, at)
	if err != nil {
		t.Fatal(err)
	}
	secondKey, secondValue, err := ChangeEvent(doc, "c", 7, at)
	if err != nil {
		t.Fatal(err)
	}

	if string(firstKey) != string(secondKey) || string(firstValue) != string(secondValue) {
		t.Error("the same event built twice produced different messages")
	}

	first, err := cdc.ParseChangeEvent(firstValue)
	if err != nil {
		t.Fatal(err)
	}
	second, err := cdc.ParseChangeEvent(secondValue)
	if err != nil {
		t.Fatal(err)
	}
	if first.EventID() != second.EventID() {
		t.Errorf("event ids differ: %q and %q", first.EventID(), second.EventID())
	}
}

// A delete's row belongs in "before"; the consumer reads it from there and
// would otherwise reject the event.
func TestDeleteEventCarriesTheOldRow(t *testing.T) {
	g, _ := NewGenerator("bench-kafka")
	doc := g.Document(0)

	_, value, err := ChangeEvent(doc, "d", 5, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	ev, err := cdc.ParseChangeEvent(value)
	if err != nil {
		t.Fatalf("parse delete: %v", err)
	}
	if ev.Operation != cdc.OperationDelete || ev.ID != doc.ID {
		t.Errorf("event = %+v, want a delete for %s", ev, doc.ID)
	}
}

func TestLag(t *testing.T) {
	lags := []Lag{
		{Partition: 0, Committed: 100, End: 150},
		{Partition: 1, Committed: 90, End: 90},
		// A group that has committed past the end, which happens briefly
		// after a reset, must not report negative lag.
		{Partition: 2, Committed: 60, End: 50},
	}

	if got := lags[0].Behind(); got != 50 {
		t.Errorf("partition 0 is %d behind, want 50", got)
	}
	if got := lags[1].Behind(); got != 0 {
		t.Errorf("a caught-up partition reports %d", got)
	}
	if got := lags[2].Behind(); got != 0 {
		t.Errorf("lag = %d, want 0 rather than a negative number", got)
	}
	if got := TotalLag(lags); got != 50 {
		t.Errorf("TotalLag = %d, want 50", got)
	}
}

func TestNewPublisherRequiresATarget(t *testing.T) {
	if _, err := NewPublisher(nil, "topic"); err == nil {
		t.Error("NewPublisher accepted no brokers")
	}
	if _, err := NewPublisher([]string{"localhost:9092"}, ""); err == nil {
		t.Error("NewPublisher accepted no topic")
	}
}
