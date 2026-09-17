package indexing

import (
	"errors"
	"testing"
)

const testDocumentID = "7f1c9b2e-4a3d-4e8b-9c1a-2b3c4d5e6f70"

func TestDecodeEvent(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		wantErr bool
	}{
		{"valid upsert", `{"event_id":"e1","document_id":"` + testDocumentID + `","operation":"upsert","version":3}`, false},
		{"valid delete", `{"event_id":"e2","document_id":"` + testDocumentID + `","operation":"delete","version":4}`, false},
		{"invalid json", `{"event_id":`, true},
		{"missing event id", `{"document_id":"` + testDocumentID + `","operation":"upsert","version":1}`, true},
		{"document id not a uuid", `{"event_id":"e1","document_id":"42","operation":"upsert","version":1}`, true},
		{"unknown operation", `{"event_id":"e1","document_id":"` + testDocumentID + `","operation":"merge","version":1}`, true},
		{"zero version", `{"event_id":"e1","document_id":"` + testDocumentID + `","operation":"upsert","version":0}`, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ev, err := DecodeEvent([]byte(tt.input))
			if tt.wantErr {
				if !errors.Is(err, ErrMalformedEvent) {
					t.Fatalf("err = %v, want ErrMalformedEvent", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if ev.DocumentID != testDocumentID {
				t.Fatalf("DocumentID = %q, want %q", ev.DocumentID, testDocumentID)
			}
		})
	}
}
