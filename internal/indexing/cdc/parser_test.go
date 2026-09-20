package cdc

import (
	"errors"
	"strings"
	"testing"
	"time"
)

const (
	testDocumentID = "7f1c9b2e-4a3d-4e8b-9c1a-2b3c4d5e6f70"
	// commitMillis is the source commit time used by the fixtures below.
	commitMillis = 1714557600000
)

// debeziumEvent builds a change event in the shape Debezium emits for the
// documents table with value.converter.schemas.enable=false.
func debeziumEvent(op, before, after string) string {
	return `{
	  "before": ` + before + `,
	  "after": ` + after + `,
	  "source": {
	    "version": "2.5.0.Final", "connector": "postgresql", "name": "pg",
	    "ts_ms": 1714557600000, "snapshot": "false", "db": "searchdb",
	    "sequence": "[null,\"24023128\"]", "schema": "public", "table": "documents",
	    "txId": 755, "lsn": 24023128
	  },
	  "op": "` + op + `",
	  "ts_ms": 1714557600987,
	  "transaction": null
	}`
}

// documentRow is an "after" or "before" record for the documents table.
func documentRow(id, title, body string, version int) string {
	return `{
	  "id": "` + id + `",
	  "title": "` + title + `",
	  "body": "` + body + `",
	  "version": ` + itoa(version) + `,
	  "updated_at": "2024-05-01T10:00:00Z"
	}`
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var digits []byte
	for ; n > 0; n /= 10 {
		digits = append([]byte{byte('0' + n%10)}, digits...)
	}
	return string(digits)
}

func TestParseChangeEventOperations(t *testing.T) {
	newRow := documentRow(testDocumentID, "New title", "New body", 7)
	oldRow := documentRow(testDocumentID, "Old title", "Old body", 6)

	tests := []struct {
		name      string
		value     string
		wantOp    Operation
		wantTitle string // the title of the row the parser chose
	}{
		{
			name:      "create uses the after row",
			value:     debeziumEvent("c", "null", newRow),
			wantOp:    OperationCreate,
			wantTitle: "New title",
		},
		{
			name:      "update uses the after row",
			value:     debeziumEvent("u", oldRow, newRow),
			wantOp:    OperationUpdate,
			wantTitle: "New title",
		},
		{
			name:      "delete uses the before row",
			value:     debeziumEvent("d", oldRow, "null"),
			wantOp:    OperationDelete,
			wantTitle: "Old title",
		},
		{
			name:      "snapshot read uses the after row",
			value:     debeziumEvent("r", "null", newRow),
			wantOp:    OperationRead,
			wantTitle: "New title",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ev, err := ParseChangeEvent([]byte(tt.value))
			if err != nil {
				t.Fatalf("ParseChangeEvent: %v", err)
			}
			if ev.Operation != tt.wantOp {
				t.Errorf("Operation = %q, want %q", ev.Operation, tt.wantOp)
			}
			if ev.ID != testDocumentID {
				t.Errorf("ID = %q, want %q", ev.ID, testDocumentID)
			}
			if got := ev.Row["title"]; got != tt.wantTitle {
				t.Errorf("Row[title] = %v, want %q", got, tt.wantTitle)
			}
			if ev.Schema != "public" || ev.Table != "documents" {
				t.Errorf("Schema/Table = %q/%q, want public/documents", ev.Schema, ev.Table)
			}
			if want := time.UnixMilli(commitMillis).UTC(); !ev.Timestamp.Equal(want) {
				t.Errorf("Timestamp = %s, want the source commit time %s", ev.Timestamp, want)
			}
			if err := ev.Validate(); err != nil {
				t.Errorf("Validate: %v", err)
			}
		})
	}
}

// A snapshot read must index exactly like a create, so that a backfill and the
// live stream converge on the same document.
func TestSnapshotReadIsAnUpsert(t *testing.T) {
	read, err := ParseChangeEvent([]byte(debeziumEvent("r", "null", documentRow(testDocumentID, "T", "B", 1))))
	if err != nil {
		t.Fatalf("ParseChangeEvent: %v", err)
	}
	create, err := ParseChangeEvent([]byte(debeziumEvent("c", "null", documentRow(testDocumentID, "T", "B", 1))))
	if err != nil {
		t.Fatalf("ParseChangeEvent: %v", err)
	}

	if !read.Operation.IsUpsert() {
		t.Error("OperationRead.IsUpsert() = false, want true")
	}
	if read.Operation == create.Operation {
		t.Error("a snapshot read must stay distinguishable from a create for logs and metrics")
	}
	if read.ID != create.ID {
		t.Errorf("snapshot ID %q and create ID %q differ, so a backfill would duplicate the document", read.ID, create.ID)
	}
}

func TestParseChangeEventRejectsBadInput(t *testing.T) {
	row := documentRow(testDocumentID, "T", "B", 1)

	tests := []struct {
		name    string
		value   string
		wantErr error
	}{
		{"invalid json", `{"op": "c", `, ErrMalformedEvent},
		{"not an envelope", `{"id": "1", "title": "t"}`, ErrMalformedEvent},
		{"json array", `["c"]`, ErrMalformedEvent},
		{"unknown operation", debeziumEvent("x", "null", row), ErrUnknownOperation},
		{"truncate operation", debeziumEvent("t", "null", "null"), ErrUnknownOperation},
		{"logical message operation", debeziumEvent("m", "null", "null"), ErrUnknownOperation},
		{"create without an after row", debeziumEvent("c", "null", "null"), ErrMalformedEvent},
		{"update without an after row", debeziumEvent("u", row, "null"), ErrMalformedEvent},
		{"delete without a before row", debeziumEvent("d", "null", "null"), ErrMalformedEvent},
		{"row is not an object", debeziumEvent("c", "null", `"just a string"`), ErrMalformedEvent},
		{"id column absent", debeziumEvent("c", "null", `{"title": "t", "body": "b"}`), ErrMissingID},
		{"id is null", debeziumEvent("c", "null", `{"id": null, "title": "t"}`), ErrMissingID},
		{"id is empty", debeziumEvent("c", "null", `{"id": "", "title": "t"}`), ErrMissingID},
		{"id is blank", debeziumEvent("c", "null", `{"id": "   ", "title": "t"}`), ErrMissingID},
		{"id is an object", debeziumEvent("c", "null", `{"id": {"v": 1}, "title": "t"}`), ErrMissingID},
		{"id is a boolean", debeziumEvent("c", "null", `{"id": true, "title": "t"}`), ErrMissingID},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ev, err := ParseChangeEvent([]byte(tt.value))
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("err = %v, want %v", err, tt.wantErr)
			}
			// Every rejection must be safe to dead-letter, and every
			// ErrMissingID and ErrUnknownOperation is also malformed.
			if !errors.Is(err, ErrMalformedEvent) {
				t.Errorf("err = %v, want it to wrap ErrMalformedEvent", err)
			}
			if ev.ID != "" {
				t.Errorf("ID = %q, want an empty event on failure", ev.ID)
			}
		})
	}
}

// Unknown operations must name the value, so an operator can tell from the
// dead-letter topic what the connector sent.
func TestUnknownOperationErrorNamesTheOperation(t *testing.T) {
	_, err := ParseChangeEvent([]byte(debeziumEvent("z", "null", documentRow(testDocumentID, "T", "B", 1))))
	if err == nil || !strings.Contains(err.Error(), `"z"`) {
		t.Fatalf("err = %v, want it to mention the operation %q", err, "z")
	}
}

func TestParseChangeEventTombstone(t *testing.T) {
	for _, tt := range []struct {
		name  string
		value []byte
	}{
		{"nil value", nil},
		{"empty value", []byte{}},
		{"json null", []byte("null")},
		{"whitespace", []byte("  \n")},
	} {
		t.Run(tt.name, func(t *testing.T) {
			_, err := ParseChangeEvent(tt.value)
			if !errors.Is(err, ErrTombstone) {
				t.Fatalf("err = %v, want ErrTombstone", err)
			}
			// A tombstone is ordinary traffic after a delete, so it must not
			// look like a fault or it would fill the dead-letter topic.
			if errors.Is(err, ErrMalformedEvent) {
				t.Error("a tombstone must not be reported as a malformed event")
			}
		})
	}
}

// With value.converter.schemas.enable=true the envelope arrives inside a
// "payload" field, alongside the schema describing it.
func TestParseChangeEventUnwrapsSchemaWrapper(t *testing.T) {
	wrapped := `{"schema": {"type": "struct", "name": "pg.public.documents.Envelope"}, "payload": ` +
		debeziumEvent("c", "null", documentRow(testDocumentID, "Wrapped", "Body", 2)) + `}`

	ev, err := ParseChangeEvent([]byte(wrapped))
	if err != nil {
		t.Fatalf("ParseChangeEvent: %v", err)
	}
	if ev.ID != testDocumentID || ev.Operation != OperationCreate {
		t.Fatalf("ID/Operation = %q/%q, want %q/CREATE", ev.ID, ev.Operation, testDocumentID)
	}
	if ev.Table != "documents" {
		t.Errorf("Table = %q, want documents", ev.Table)
	}
}

// A document ID must survive a large integer primary key untouched. Decoding
// through float64 would round 2^53+1 down to 2^53 and two neighbouring rows
// would share a document.
func TestDocumentIDKeepsLargeIntegerKeysExact(t *testing.T) {
	const bigKey = "9007199254740993"
	value := debeziumEvent("c", "null", `{"id": `+bigKey+`, "title": "t", "body": "b"}`)

	ev, err := ParseChangeEvent([]byte(value))
	if err != nil {
		t.Fatalf("ParseChangeEvent: %v", err)
	}
	if ev.ID != bigKey {
		t.Fatalf("ID = %q, want %q", ev.ID, bigKey)
	}
}

// The same row must always produce the same ID, whatever the rest of the event
// looks like, because the ID is the document's identity in both stores.
func TestDocumentIDIsDeterministic(t *testing.T) {
	create := debeziumEvent("c", "null", documentRow(testDocumentID, "First", "Body one", 1))
	update := debeziumEvent("u", documentRow(testDocumentID, "First", "Body one", 1),
		documentRow(testDocumentID, "Second", "Body two", 2))
	del := debeziumEvent("d", documentRow(testDocumentID, "Second", "Body two", 2), "null")

	var ids []string
	for _, value := range []string{create, update, del, create} {
		ev, err := ParseChangeEvent([]byte(value))
		if err != nil {
			t.Fatalf("ParseChangeEvent: %v", err)
		}
		ids = append(ids, ev.ID)
	}
	for _, id := range ids {
		if id != testDocumentID {
			t.Fatalf("ids = %v, want every event to map to %q", ids, testDocumentID)
		}
	}
}

func TestParserCompositeKey(t *testing.T) {
	p := Parser{KeyFields: []string{"tenant_id", "slug"}}

	ev, err := p.Parse([]byte(debeziumEvent("c", "null", `{"tenant_id": 42, "slug": "intro", "title": "t", "body": "b"}`)))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if want := "42" + compositeKeySeparator + "intro"; ev.ID != want {
		t.Fatalf("ID = %q, want %q", ev.ID, want)
	}

	// Order is part of the stored format: swapping the fields must give a
	// different document, not silently the same one.
	swapped := Parser{KeyFields: []string{"slug", "tenant_id"}}
	other, err := swapped.Parse([]byte(debeziumEvent("c", "null", `{"tenant_id": 42, "slug": "intro", "title": "t", "body": "b"}`)))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if other.ID == ev.ID {
		t.Errorf("both key orders produced %q", ev.ID)
	}

	// A key value holding the separator could collide with a different pair.
	if _, err := p.Parse([]byte(debeziumEvent("c", "null", `{"tenant_id": 42, "slug": "a|b", "title": "t", "body": "b"}`))); !errors.Is(err, ErrMissingID) {
		t.Errorf("err = %v, want ErrMissingID for a key containing the separator", err)
	}

	// One missing half of the key is still no identity.
	if _, err := p.Parse([]byte(debeziumEvent("c", "null", `{"tenant_id": 42, "title": "t"}`))); !errors.Is(err, ErrMissingID) {
		t.Errorf("err = %v, want ErrMissingID", err)
	}
}

// A parser with no key fields configured behaves like the default one rather
// than accepting every row without an identity.
func TestParserWithoutKeyFieldsUsesTheDefault(t *testing.T) {
	ev, err := (Parser{}).Parse([]byte(debeziumEvent("c", "null", documentRow(testDocumentID, "T", "B", 1))))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if ev.ID != testDocumentID {
		t.Fatalf("ID = %q, want %q", ev.ID, testDocumentID)
	}
}

func TestParseChangeEventTimestamps(t *testing.T) {
	row := documentRow(testDocumentID, "T", "B", 1)

	tests := []struct {
		name  string
		value string
		want  time.Time
	}{
		{
			name:  "prefers the source commit time",
			value: debeziumEvent("c", "null", row),
			want:  time.UnixMilli(commitMillis).UTC(),
		},
		{
			name:  "falls back to the connector time",
			value: `{"op":"c","before":null,"after":` + row + `,"source":{"schema":"public","table":"documents"},"ts_ms":1714557600987}`,
			want:  time.UnixMilli(1714557600987).UTC(),
		},
		{
			name:  "zero when the connector reports neither",
			value: `{"op":"c","before":null,"after":` + row + `,"source":{"schema":"public","table":"documents"}}`,
			want:  time.Time{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ev, err := ParseChangeEvent([]byte(tt.value))
			if err != nil {
				t.Fatalf("ParseChangeEvent: %v", err)
			}
			if !ev.Timestamp.Equal(tt.want) {
				t.Fatalf("Timestamp = %s, want %s", ev.Timestamp, tt.want)
			}
		})
	}
}

func TestChangeEventValidate(t *testing.T) {
	tests := []struct {
		name    string
		event   ChangeEvent
		wantErr error
	}{
		{"valid", ChangeEvent{ID: testDocumentID, Operation: OperationCreate}, nil},
		{"no id", ChangeEvent{Operation: OperationCreate}, ErrMissingID},
		{"no operation", ChangeEvent{ID: testDocumentID}, ErrUnknownOperation},
		{"operation not mapped", ChangeEvent{ID: testDocumentID, Operation: "TRUNCATE"}, ErrUnknownOperation},
		// The internal names are not the wire codes.
		{"raw debezium code", ChangeEvent{ID: testDocumentID, Operation: "c"}, ErrUnknownOperation},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.event.Validate(); !errors.Is(err, tt.wantErr) {
				t.Fatalf("Validate = %v, want %v", err, tt.wantErr)
			}
		})
	}
}

// Logs must identify a document without reproducing its contents.
func TestChangeEventLogAttrsLeaveOutTheRow(t *testing.T) {
	ev := ChangeEvent{
		ID:        testDocumentID,
		Operation: OperationUpdate,
		Schema:    "public",
		Table:     "documents",
		Row:       map[string]any{"body": "a secret paragraph", "title": "Confidential"},
	}

	attrs := ev.LogAttrs()
	if len(attrs)%2 != 0 {
		t.Fatalf("LogAttrs returned %d values, want pairs", len(attrs))
	}
	for _, attr := range attrs {
		if s, ok := attr.(string); ok && strings.Contains(s, "secret") {
			t.Fatalf("LogAttrs leaked document contents: %v", attrs)
		}
	}
}
