package cdc

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

// compositeKeySeparator joins the parts of a composite primary key into one
// document ID. Changing it changes every derived document ID, so treat it as
// part of the stored data format.
const compositeKeySeparator = "|"

// Parser turns raw Debezium message values into ChangeEvents.
//
// It understands both shapes Debezium emits: the bare envelope produced with
// value.converter.schemas.enable=false, and the {"schema":..,"payload":..}
// wrapper produced when schemas are enabled. In both cases the envelope holds
// "op", "before", "after", "source" and "ts_ms".
//
// The parser is not tied to one table. The only thing it needs to know about
// the source is which columns form the primary key, which is what KeyFields
// says; schema and table names travel through to the event untouched.
type Parser struct {
	// KeyFields are the row columns that form the primary key, in a fixed
	// order. Their values are joined to produce ChangeEvent.ID, so the order
	// must never change for a table already indexed. Empty means
	// []string{"id"}.
	KeyFields []string
}

// DefaultParser reads the primary key from an "id" column, matching the
// documents table.
var DefaultParser = Parser{KeyFields: []string{"id"}}

// ParseChangeEvent normalizes one Debezium message value with DefaultParser.
//
// Failures wrap ErrMalformedEvent, except for a tombstone, which wraps
// ErrTombstone and is expected rather than a fault.
func ParseChangeEvent(data []byte) (ChangeEvent, error) {
	return DefaultParser.Parse(data)
}

// Parse normalizes one Debezium message value.
//
// The row it reads depends on the operation: "after" for creates, updates and
// snapshot reads, and "before" for deletes, which is the only place a deleted
// row's key still exists.
func (p Parser) Parse(data []byte) (ChangeEvent, error) {
	if isNullValue(data) {
		return ChangeEvent{}, fmt.Errorf("%w: value is null", ErrTombstone)
	}

	env, err := decodeEnvelope(data)
	if err != nil {
		return ChangeEvent{}, err
	}

	op, err := parseOperation(env.Op)
	if err != nil {
		return ChangeEvent{}, err
	}

	// A delete's "after" is null, so the key can only come from "before".
	// Debezium only fills "before" when the table's REPLICA IDENTITY is set
	// to FULL or to an index, so say so when it is missing.
	raw, field := env.After, "after"
	if op == OperationDelete {
		raw, field = env.Before, "before"
	}
	row, err := decodeRow(raw)
	if err != nil {
		return ChangeEvent{}, fmt.Errorf("%w: decode %q: %w", ErrMalformedEvent, field, err)
	}
	if row == nil {
		if op == OperationDelete {
			return ChangeEvent{}, fmt.Errorf(
				"%w: delete event has no %q row; set the source table REPLICA IDENTITY so Debezium reports the old values",
				ErrMalformedEvent, field)
		}
		return ChangeEvent{}, fmt.Errorf("%w: %s event has no %q row", ErrMalformedEvent, op, field)
	}

	id, err := p.documentID(row)
	if err != nil {
		return ChangeEvent{}, err
	}

	return ChangeEvent{
		ID:        id,
		Operation: op,
		Schema:    env.Source.Schema,
		Table:     env.Source.Table,
		Row:       row,
		Timestamp: env.changeTime(),
		Database:  env.Source.DB,
		LSN:       value(env.Source.LSN),
		TxID:      value(env.Source.TxID),
		Snapshot:  env.Source.fromSnapshot(),
	}, nil
}

// documentID builds the document ID from the row's primary key columns.
//
// The ID is a pure function of the key values, so the same row always maps to
// the same OpenSearch document and the same Qdrant point, however many times
// its events are replayed. Nothing random or time-based goes into it.
func (p Parser) documentID(row map[string]any) (string, error) {
	fields := p.KeyFields
	if len(fields) == 0 {
		fields = DefaultParser.KeyFields
	}

	parts := make([]string, 0, len(fields))
	for _, field := range fields {
		value, ok := row[field]
		if !ok || value == nil {
			return "", fmt.Errorf("%w: row has no %q value", ErrMissingID, field)
		}
		part, err := keyString(value)
		if err != nil {
			return "", fmt.Errorf("%w: %q: %w", ErrMissingID, field, err)
		}
		if part == "" {
			return "", fmt.Errorf("%w: %q is empty", ErrMissingID, field)
		}
		// Two different key pairs must never join into the same ID.
		if len(fields) > 1 && strings.Contains(part, compositeKeySeparator) {
			return "", fmt.Errorf("%w: %q contains the composite key separator %q",
				ErrMissingID, field, compositeKeySeparator)
		}
		parts = append(parts, part)
	}
	return strings.Join(parts, compositeKeySeparator), nil
}

// keyString renders a primary key value. Only scalars can identify a row, and
// json.Number keeps large integer keys exact rather than rounding them
// through float64.
func keyString(value any) (string, error) {
	switch v := value.(type) {
	case string:
		return strings.TrimSpace(v), nil
	case json.Number:
		return v.String(), nil
	case bool:
		return "", fmt.Errorf("a boolean cannot identify a row")
	default:
		return "", fmt.Errorf("want a string or number key, got %T", value)
	}
}

// parseOperation maps the Debezium one-letter operation codes onto Operation.
// Anything else is rejected rather than guessed at, so a connector change
// shows up as a dead-lettered message instead of silently skipped data.
func parseOperation(op string) (Operation, error) {
	switch op {
	case "c":
		return OperationCreate, nil
	case "u":
		return OperationUpdate, nil
	case "d":
		return OperationDelete, nil
	case "r":
		return OperationRead, nil
	case "":
		return "", fmt.Errorf("%w: event has no %q field; is this a Debezium envelope?", ErrMalformedEvent, "op")
	case "t":
		return "", fmt.Errorf("%w: %q (truncate) is not supported; reindex the table instead", ErrUnknownOperation, op)
	case "m":
		return "", fmt.Errorf("%w: %q (logical message) carries no row", ErrUnknownOperation, op)
	default:
		return "", fmt.Errorf("%w: %q", ErrUnknownOperation, op)
	}
}

// envelope is the part of a Debezium change event this package reads.
type envelope struct {
	Op     string          `json:"op"`
	Before json.RawMessage `json:"before"`
	After  json.RawMessage `json:"after"`
	Source source          `json:"source"`
	TsMs   *int64          `json:"ts_ms"`

	// Payload holds the envelope itself when the converter was configured to
	// emit a schema alongside it.
	Payload json.RawMessage `json:"payload"`
}

// source is the origin metadata Debezium attaches to every event.
//
// LSN and TxID come from the PostgreSQL connector and say where in the
// write-ahead log the change was read: they make an event identifiable and
// orderable without the consumer having to invent either. Other connectors
// name their position differently, which is why both are optional here.
type source struct {
	Schema string `json:"schema"`
	Table  string `json:"table"`
	TsMs   *int64 `json:"ts_ms"`
	DB     string `json:"db"`
	LSN    *int64 `json:"lsn"`
	TxID   *int64 `json:"txId"`
	// Snapshot is a string in current Debezium ("true", "first", "last",
	// "false", "incremental") but was a boolean in older versions, so it is
	// decoded loosely: a field that only feeds diagnostics must never turn a
	// usable event into a malformed one.
	Snapshot any `json:"snapshot"`
}

// fromSnapshot reports whether a source block says the row was read during a
// snapshot rather than from the log.
func (s source) fromSnapshot() bool {
	switch v := s.Snapshot.(type) {
	case bool:
		return v
	case string:
		return v != "" && v != "false" && v != "incremental"
	default:
		return false
	}
}

// value reads an optional number, treating an absent one as zero.
func value(n *int64) int64 {
	if n == nil {
		return 0
	}
	return *n
}

// changeTime is when the change happened in the database. source.ts_ms is the
// commit time and is preferred; the envelope ts_ms is when the connector
// processed the change, and is only a fallback.
func (e envelope) changeTime() time.Time {
	for _, ms := range []*int64{e.Source.TsMs, e.TsMs} {
		if ms != nil {
			return time.UnixMilli(*ms).UTC()
		}
	}
	return time.Time{}
}

// decodeEnvelope reads the change envelope, unwrapping the schema wrapper
// when one is present.
func decodeEnvelope(data []byte) (envelope, error) {
	var env envelope
	if err := json.Unmarshal(data, &env); err != nil {
		return envelope{}, fmt.Errorf("%w: %w", ErrMalformedEvent, err)
	}
	if env.Op != "" || isNullValue(env.Payload) {
		return env, nil
	}

	var inner envelope
	if err := json.Unmarshal(env.Payload, &inner); err != nil {
		return envelope{}, fmt.Errorf("%w: decode %q: %w", ErrMalformedEvent, "payload", err)
	}
	return inner, nil
}

// decodeRow decodes a "before" or "after" record. A JSON null yields a nil map
// rather than an error, because the caller decides whether that is legal for
// the operation at hand.
func decodeRow(raw json.RawMessage) (map[string]any, error) {
	if isNullValue(raw) {
		return nil, nil
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	// Keep numbers exact: a bigint primary key would lose precision as a
	// float64, and a document ID must be reproducible byte for byte.
	dec.UseNumber()

	var row map[string]any
	if err := dec.Decode(&row); err != nil {
		return nil, err
	}
	return row, nil
}

// isNullValue reports whether raw is absent, empty or the JSON literal null.
func isNullValue(raw []byte) bool {
	trimmed := bytes.TrimSpace(raw)
	return len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null"))
}
