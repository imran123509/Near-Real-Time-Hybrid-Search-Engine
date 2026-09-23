// Package cdc turns change-data-capture events from a database into search
// index writes.
//
// The pipeline it belongs to is:
//
//	PostgreSQL -> Debezium -> Kafka -> consumer -> Parser -> ChangeEvent -> Service
//	                                                                       /      \
//	                                                              OpenSearch      Qdrant
//
// Parser is the only part that knows Debezium's wire format. Everything after
// it works with ChangeEvent, so a different connector, or a different source
// database, only needs a different parser.
//
// The package depends on the OpenSearch and Qdrant client packages and on the
// Embedder interface, and on nothing else in the application. It knows nothing
// about Kafka, HTTP or PostgreSQL drivers: the consumer hands it raw bytes and
// receives an error back.
package cdc

import (
	"errors"
	"fmt"
	"strconv"
	"time"
)

var (
	// ErrMalformedEvent means a message cannot be parsed as sent. Retrying
	// will not help; dead-letter it instead.
	ErrMalformedEvent = errors.New("malformed cdc event")

	// ErrUnknownOperation means the event carried an operation this package
	// does not handle. It wraps ErrMalformedEvent.
	ErrUnknownOperation = fmt.Errorf("%w: unknown operation", ErrMalformedEvent)

	// ErrMissingID means the event carried no usable primary key, so the
	// document it refers to cannot be identified. It wraps ErrMalformedEvent.
	ErrMissingID = fmt.Errorf("%w: missing document id", ErrMalformedEvent)

	// ErrTombstone means the message is a Kafka tombstone: a null value that
	// Debezium writes after a delete so log compaction can drop the key. It
	// carries no payload and always follows a real delete event, so callers
	// should skip it and commit its offset. It is not an ErrMalformedEvent.
	ErrTombstone = errors.New("tombstone message")
)

// Operation is a change that happened to one database row, independent of the
// connector that reported it.
type Operation string

const (
	// OperationCreate is a row insert.
	OperationCreate Operation = "CREATE"
	// OperationUpdate is a row update.
	OperationUpdate Operation = "UPDATE"
	// OperationDelete is a row delete.
	OperationDelete Operation = "DELETE"
	// OperationRead is a row observed during the connector's initial
	// snapshot rather than from the write-ahead log. It is kept apart from
	// OperationCreate so that a backfill can be told from live traffic in
	// logs and metrics, but it indexes exactly like one: see IsUpsert.
	OperationRead Operation = "READ"
)

// IsUpsert reports whether op writes the current row into the search indexes
// rather than removing it. Creates, updates and snapshot reads are all
// upserts, which is what makes replaying a snapshot over live data safe.
func (op Operation) IsUpsert() bool {
	switch op {
	case OperationCreate, OperationUpdate, OperationRead:
		return true
	default:
		return false
	}
}

// Valid reports whether op is one this package handles.
func (op Operation) Valid() bool {
	return op.IsUpsert() || op == OperationDelete
}

// ChangeEvent is one row change, normalized away from any connector's wire
// format. It is what the Service consumes.
//
// The event carries the row itself, so indexing never reads the source
// database back. Schema and Table are kept so that one consumer can serve
// several tables later without the parser having to be table-specific.
type ChangeEvent struct {
	// ID identifies the document across every store. It is derived from the
	// row's primary key, so the same database row always produces the same
	// ID, which is what makes indexing idempotent. See Parser.
	ID string

	// Operation is the change that produced this event.
	Operation Operation

	// Schema and Table name the source table, for example "public" and
	// "documents".
	Schema string
	Table  string

	// Row is the state of the row this event refers to: the new state for
	// creates, updates and snapshot reads, and the last known state for
	// deletes. JSON numbers are kept as json.Number so that large integer
	// keys survive without passing through float64.
	Row map[string]any

	// Timestamp is when the change was committed in the source database, or
	// the zero time when the connector did not report one.
	Timestamp time.Time

	// Database is the source database name, kept for diagnostics. It is not
	// part of the event identity: one connector serves one database here.
	Database string

	// LSN is the write-ahead log position of the change, and TxID the
	// transaction that made it. Both are reported by the PostgreSQL
	// connector on every event, snapshot reads included, and are 0 when a
	// connector leaves them out. LSN increases with the log, so it both
	// identifies an event and orders it against other events for the same
	// row.
	LSN  int64
	TxID int64
	// Snapshot is true when the connector read the row during its initial
	// snapshot rather than from the log.
	Snapshot bool
}

// EventID is this event's identity: the same database change always produces
// the same string, and two different changes never produce the same one.
//
// It is built only from what the connector reported -- source table, primary
// key, operation and log position -- and never from a clock or a random
// source. A redelivered message therefore carries the identity it had the
// first time, which is what makes a replay recognisable in a log line. A
// uuid.New() here would make every delivery look new and duplicate detection
// impossible.
//
//	public.documents:7f1c9b2e-4a3d-4e8b-9c1a-2b3c4d5e6f70:UPDATE@lsn:24100912
//
// The log position is what separates two otherwise identical changes, such as
// a column being set to the same value twice. Without one, the commit
// timestamp stands in and the identity is only as unique as that timestamp;
// PostgreSQL events always carry an LSN.
//
// It identifies an event, not a document: a document's whole history, from
// its CREATE to its DELETE, shares one ID (see ChangeEvent.ID) and produces a
// different EventID for every change along the way.
func (e ChangeEvent) EventID() string {
	return e.qualifiedTable() + ":" + e.ID + ":" + string(e.Operation) + "@" + e.position()
}

// position renders the point in the source log this event came from.
func (e ChangeEvent) position() string {
	switch {
	case e.LSN > 0:
		return "lsn:" + strconv.FormatInt(e.LSN, 10)
	case !e.Timestamp.IsZero():
		return "ts:" + strconv.FormatInt(e.Timestamp.UnixMilli(), 10)
	default:
		return "unknown"
	}
}

func (e ChangeEvent) qualifiedTable() string {
	switch {
	case e.Schema != "" && e.Table != "":
		return e.Schema + "." + e.Table
	case e.Table != "":
		return e.Table
	default:
		return "unknown"
	}
}

// Validate reports whether the event can be processed.
func (e ChangeEvent) Validate() error {
	if e.ID == "" {
		return fmt.Errorf("%w: event has no id", ErrMissingID)
	}
	if !e.Operation.Valid() {
		return fmt.Errorf("%w: %q", ErrUnknownOperation, e.Operation)
	}
	return nil
}

// LogAttrs returns the key/value pairs describing this event for structured
// logging. It deliberately leaves out Row: document contents do not belong in
// logs, and a row may hold personal data.
//
// event_id and document_id are both here and are not the same thing: one line
// per event, many events per document, which is what makes a redelivery
// visible as the same event_id appearing twice.
func (e ChangeEvent) LogAttrs() []any {
	attrs := []any{
		"event_id", e.EventID(),
		"document_id", e.ID,
		"operation", string(e.Operation),
		"schema", e.Schema,
		"table", e.Table,
	}
	if e.LSN > 0 {
		attrs = append(attrs, "lsn", e.LSN)
	}
	return attrs
}

// Effect is what applying an event did to the search indexes. It is how a
// caller tells an ordinary change from a redelivery and from an event the
// indexes have already moved past, which is what the duplicate and stale
// counters a metrics exporter will add are built on.
type Effect string

const (
	// EffectNone means nothing was applied. It accompanies every error.
	EffectNone Effect = ""
	// EffectIndexed means both stores now hold this event's data.
	EffectIndexed Effect = "indexed"
	// EffectReapplied means the keyword index already held exactly this
	// version, so this delivery was a repeat. The writes were made again
	// anyway, because an earlier attempt may have stopped part way through,
	// and repeating them changes nothing.
	EffectReapplied Effect = "reapplied"
	// EffectStale means the indexes hold data from a later event, so nothing
	// was written. The event is finished: there is nothing left to apply.
	EffectStale Effect = "stale"
	// EffectDeleted means the document was removed from both stores, or was
	// already absent from them.
	EffectDeleted Effect = "deleted"
)

// Store names a downstream system, so that a failure can be attributed to one
// of them. Counting failures per store is the hook a metrics exporter will
// use later; see StoreError.
type Store string

const (
	StoreKeyword  Store = "opensearch"
	StoreVector   Store = "qdrant"
	StoreEmbedder Store = "embedder"
)

// StoreError is a failure from one downstream system. Callers use errors.As
// to find out which store failed, for example to decide whether to retry or
// to increment a per-store counter.
type StoreError struct {
	Store      Store
	DocumentID string
	Err        error
}

func (e *StoreError) Error() string {
	return fmt.Sprintf("%s: document %s: %v", e.Store, e.DocumentID, e.Err)
}

func (e *StoreError) Unwrap() error { return e.Err }

// storeErr wraps err as coming from store, leaving a nil err nil.
func storeErr(store Store, id string, err error) error {
	if err == nil {
		return nil
	}
	return &StoreError{Store: store, DocumentID: id, Err: err}
}
