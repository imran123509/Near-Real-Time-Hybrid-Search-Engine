package loadtest

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

// Indexes observes the two search indexes over their HTTP APIs.
//
// It deliberately does not use the application's OpenSearch and Qdrant
// clients. A benchmark should watch the system from outside, the way an
// operator would, so that what it measures cannot depend on a client setting
// the services themselves do not have. That also keeps the measuring code out
// of the packages it measures.
type Indexes struct {
	OpenSearchURL string // http://localhost:9200
	Index         string // documents
	QdrantURL     string // http://localhost:6333, the REST port, not gRPC
	Collection    string // documents

	// HTTP is the client used for every request; a zero value gets a client
	// with a short timeout, because a probe that hangs stops the measurement.
	HTTP *http.Client
}

func (ix Indexes) client() *http.Client {
	if ix.HTTP != nil {
		return ix.HTTP
	}
	return &http.Client{Timeout: 10 * time.Second}
}

// DocumentExists reports whether the keyword index holds a document, without
// fetching its contents. It is the cheapest question that answers "has this
// change arrived yet".
func (ix Indexes) DocumentExists(ctx context.Context, id string) (bool, error) {
	endpoint := fmt.Sprintf("%s/%s/_doc/%s?_source=false", strings.TrimRight(ix.OpenSearchURL, "/"),
		url.PathEscape(ix.Index), url.PathEscape(id))
	req, err := http.NewRequestWithContext(ctx, http.MethodHead, endpoint, nil)
	if err != nil {
		return false, err
	}
	resp, err := ix.client().Do(req)
	if err != nil {
		return false, err
	}
	defer drain(resp)

	switch resp.StatusCode {
	case http.StatusOK:
		return true, nil
	case http.StatusNotFound:
		return false, nil
	default:
		return false, fmt.Errorf("opensearch answered %d for document %s", resp.StatusCode, id)
	}
}

// CountDocuments returns how many documents the keyword index holds.
func (ix Indexes) CountDocuments(ctx context.Context) (int, error) {
	endpoint := fmt.Sprintf("%s/%s/_count", strings.TrimRight(ix.OpenSearchURL, "/"), url.PathEscape(ix.Index))
	var out struct {
		Count int `json:"count"`
	}
	if err := ix.getJSON(ctx, endpoint, &out); err != nil {
		return 0, fmt.Errorf("count opensearch documents: %w", err)
	}
	return out.Count, nil
}

// CountPoints returns how many points the vector collection holds.
func (ix Indexes) CountPoints(ctx context.Context) (int, error) {
	endpoint := fmt.Sprintf("%s/collections/%s/points/count",
		strings.TrimRight(ix.QdrantURL, "/"), url.PathEscape(ix.Collection))
	var out struct {
		Result struct {
			Count int `json:"count"`
		} `json:"result"`
	}
	if err := ix.postJSON(ctx, endpoint, `{"exact":true}`, &out); err != nil {
		return 0, fmt.Errorf("count qdrant points: %w", err)
	}
	return out.Result.Count, nil
}

// Refresh makes recent writes searchable at once instead of waiting for
// OpenSearch's refresh interval.
//
// Counting without it would measure the refresh interval as if it were
// indexing latency. Per-document arrival is read with DocumentExists, which
// does not need a refresh, so this only affects the counts taken at the end.
func (ix Indexes) Refresh(ctx context.Context) error {
	endpoint := fmt.Sprintf("%s/%s/_refresh", strings.TrimRight(ix.OpenSearchURL, "/"), url.PathEscape(ix.Index))
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, nil)
	if err != nil {
		return err
	}
	resp, err := ix.client().Do(req)
	if err != nil {
		return err
	}
	defer drain(resp)
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("refresh index %s: status %d", ix.Index, resp.StatusCode)
	}
	return nil
}

func (ix Indexes) getJSON(ctx context.Context, endpoint string, out any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return err
	}
	return ix.do(req, out)
}

func (ix Indexes) postJSON(ctx context.Context, endpoint, body string, out any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, strings.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	return ix.do(req, out)
}

func (ix Indexes) do(req *http.Request, out any) error {
	resp, err := ix.client().Do(req)
	if err != nil {
		return err
	}
	defer drain(resp)
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("%s answered %d", req.URL.Host, resp.StatusCode)
	}
	return json.NewDecoder(resp.Body).Decode(out)
}

func drain(resp *http.Response) {
	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()
}

// Arrival is one document and the moment it was written to PostgreSQL.
type Arrival struct {
	ID       string
	Inserted time.Time
}

// Watcher waits for generated documents to appear in the keyword index and
// records how long each one took.
//
// It polls, because nothing in the pipeline pushes a notification: a document
// arrives when the consumer has written it. The poll interval therefore bounds
// how precisely a single document's latency can be measured, which is why the
// interval is reported beside the results. Throughput, measured across
// thousands of documents, is not affected by it.
type Watcher struct {
	Indexes  Indexes
	Interval time.Duration
	// Workers is how many documents are checked at once. The point is to keep
	// the poll cheap relative to the pipeline, not to load OpenSearch.
	Workers int
}

// WatchResult is what a watch ended with.
type WatchResult struct {
	Latency Summary
	// Last is when the final document appeared, which is the end of the run.
	Last time.Time
	// Pending are documents that never arrived before the deadline.
	Pending []string
}

// Wait polls until every arrival has been seen or ctx ends, and returns what
// it measured either way: a run that did not finish is a result too, and
// reporting it is the point of the Pending list.
func (w Watcher) Wait(ctx context.Context, arrivals []Arrival) (WatchResult, error) {
	interval := w.Interval
	if interval <= 0 {
		interval = 250 * time.Millisecond
	}
	workers := w.Workers
	if workers <= 0 {
		workers = 8
	}

	pending := make(map[string]time.Time, len(arrivals))
	for _, a := range arrivals {
		pending[a.ID] = a.Inserted
	}

	var (
		recorder Recorder
		last     time.Time
	)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for len(pending) > 0 {
		found, err := w.checkAll(ctx, pending, workers)
		if err != nil {
			return w.result(&recorder, last, pending), err
		}
		seenAt := time.Now()
		for _, id := range found {
			recorder.Record(seenAt.Sub(pending[id]))
			delete(pending, id)
			last = seenAt
		}
		if len(pending) == 0 {
			break
		}
		select {
		case <-ctx.Done():
			return w.result(&recorder, last, pending), ctx.Err()
		case <-ticker.C:
		}
	}
	return w.result(&recorder, last, pending), nil
}

func (w Watcher) result(recorder *Recorder, last time.Time, pending map[string]time.Time) WatchResult {
	ids := make([]string, 0, len(pending))
	for id := range pending {
		ids = append(ids, id)
	}
	return WatchResult{Latency: recorder.Summary(), Last: last, Pending: ids}
}

// checkAll asks the keyword index about every document still outstanding and
// returns the ones that have arrived.
func (w Watcher) checkAll(ctx context.Context, pending map[string]time.Time, workers int) ([]string, error) {
	ids := make(chan string)
	go func() {
		defer close(ids)
		for id := range pending {
			select {
			case ids <- id:
			case <-ctx.Done():
				return
			}
		}
	}()

	var (
		mu       sync.Mutex
		found    []string
		firstErr error
		wg       sync.WaitGroup
	)
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for id := range ids {
				exists, err := w.Indexes.DocumentExists(ctx, id)
				mu.Lock()
				switch {
				case err != nil && firstErr == nil && ctx.Err() == nil:
					firstErr = err
				case exists:
					found = append(found, id)
				}
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	return found, firstErr
}
