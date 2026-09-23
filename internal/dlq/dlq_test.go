package dlq

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"
)

func testSource() Source {
	return Source{
		Topic:     "search.public.documents",
		Partition: 2,
		Offset:    4711,
		Key:       []byte(`{"id":"7f1c9b2e-4a3d-4e8b-9c1a-2b3c4d5e6f70"}`),
		Payload:   []byte(`{"op":"c","after":{"id":"7f1c9b2e","title":"Go"}}`),
	}
}

func TestNewCarriesEveryPieceOfMetadata(t *testing.T) {
	src := testSource()
	before := time.Now().UTC()

	msg := New(src, errors.New("opensearch: status 400: mapping error"), "non_retryable", 3)

	if msg.OriginalTopic != src.Topic || msg.OriginalPartition != src.Partition || msg.OriginalOffset != src.Offset {
		t.Errorf("origin = %s/%d/%d, want %s/%d/%d", msg.OriginalTopic, msg.OriginalPartition, msg.OriginalOffset,
			src.Topic, src.Partition, src.Offset)
	}
	if msg.EventKey != string(src.Key) {
		t.Errorf("event key = %q, want %q", msg.EventKey, src.Key)
	}
	if msg.Error != "opensearch: status 400: mapping error" || msg.ErrorType != "non_retryable" || msg.Attempts != 3 {
		t.Errorf("failure = %+v", msg)
	}
	if msg.FailedAt.Before(before) || msg.FailedAt.After(time.Now().UTC().Add(time.Second)) {
		t.Errorf("failed_at = %s, want roughly now", msg.FailedAt)
	}
	if msg.FailedAt.Location() != time.UTC {
		t.Errorf("failed_at is in %s, want UTC", msg.FailedAt.Location())
	}
}

// TestPayloadIsPreservedExactly is the point of the dead-letter topic: what
// comes out must be what went in, so it can be replayed.
func TestPayloadIsPreservedExactly(t *testing.T) {
	payloads := [][]byte{
		[]byte(`{"op":"c","after":{"title":"Go & concurrency <tags>"}}`),
		[]byte("not json at all"),
		{0x00, 0x01, 0xff, 0xfe, 0x80}, // arbitrary bytes, not valid UTF-8
		{},
	}
	for _, payload := range payloads {
		src := testSource()
		src.Payload = payload

		msg := New(src, errors.New("boom"), "unknown", 1)
		encoded, err := json.Marshal(msg)
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}
		var decoded Message
		if err := json.Unmarshal(encoded, &decoded); err != nil {
			t.Fatalf("unmarshal: %v", err)
		}
		if !bytes.Equal(decoded.Payload, payload) {
			t.Errorf("payload round trip changed %q into %q", payload, decoded.Payload)
		}
	}
}

func TestNewCopiesThePayload(t *testing.T) {
	src := testSource()
	msg := New(src, errors.New("boom"), "retryable", 2)

	// The consumer may reuse its buffer once the message is handled.
	src.Payload[0] = 'X'
	if msg.Payload[0] == 'X' {
		t.Error("the dead-letter message shares the caller's payload buffer")
	}
}

func TestJSONFieldNames(t *testing.T) {
	encoded, err := json.Marshal(New(testSource(), errors.New("boom"), "retryable", 2))
	if err != nil {
		t.Fatal(err)
	}
	var raw map[string]any
	if err := json.Unmarshal(encoded, &raw); err != nil {
		t.Fatal(err)
	}
	for _, field := range []string{
		"original_topic", "original_partition", "original_offset",
		"event_key", "payload", "error", "error_type", "attempts", "failed_at",
	} {
		if _, ok := raw[field]; !ok {
			t.Errorf("message has no %q field: %s", field, encoded)
		}
	}
}

func TestLongErrorsAreTrimmed(t *testing.T) {
	huge := strings.Repeat("stack trace ", 1000)
	msg := New(testSource(), errors.New(huge), "unknown", 1)

	if len(msg.Error) > maxErrorLength+3 {
		t.Errorf("error is %d bytes, want it trimmed to about %d", len(msg.Error), maxErrorLength)
	}
	if !strings.HasSuffix(msg.Error, "...") {
		t.Error("a trimmed error should show that it was cut")
	}
	if !strings.HasPrefix(msg.Error, "stack trace") {
		t.Error("the start of the error was lost")
	}
}

func TestNilCauseIsHandled(t *testing.T) {
	if msg := New(testSource(), nil, "unknown", 1); msg.Error != "" {
		t.Errorf("error = %q, want empty", msg.Error)
	}
}

func TestLogAttrsLeaveOutThePayload(t *testing.T) {
	msg := New(testSource(), errors.New("boom"), "retryable", 2)
	attrs := msg.LogAttrs()

	joined := ""
	for _, attr := range attrs {
		joined += strings.ToLower(strings.TrimSpace(toString(attr))) + " "
	}
	if strings.Contains(joined, "payload") || strings.Contains(joined, "title") {
		t.Errorf("log attributes mention the payload: %v", attrs)
	}
	for _, want := range []string{"original_topic", "original_offset", "error_type", "attempts"} {
		if !strings.Contains(joined, want) {
			t.Errorf("log attributes are missing %q: %v", want, attrs)
		}
	}
}

func TestPublisherFunc(t *testing.T) {
	var got Message
	var publisher Publisher = PublisherFunc(func(_ context.Context, msg Message) error {
		got = msg
		return nil
	})
	want := New(testSource(), errors.New("boom"), "retryable", 2)
	if err := publisher.Publish(context.Background(), want); err != nil {
		t.Fatal(err)
	}
	if got.OriginalOffset != want.OriginalOffset {
		t.Errorf("publisher got %+v", got)
	}
}

func toString(v any) string {
	switch value := v.(type) {
	case string:
		return value
	default:
		return ""
	}
}
