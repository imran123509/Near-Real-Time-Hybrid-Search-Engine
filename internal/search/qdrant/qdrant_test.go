package qdrant

import (
	"context"
	"errors"
	"math"
	"net"
	"strings"
	"sync"
	"testing"

	"github.com/google/uuid"
	qd "github.com/qdrant/go-client/qdrant"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"near-real-time-hybrid-search-engine/internal/config"
)

const (
	testCollection = "documents"
	testVectorSize = 4
	testAPIKey     = "s3cret-key"
	docUUID        = "7f1c9b2e-4a3d-4e8b-9c1a-2b3c4d5e6f70"
)

var testVector = []float32{0.1, 0.2, 0.3, 0.4}

// fakeQdrant is an in-process gRPC server built from the SDK's generated
// service interfaces. It records requests and returns canned responses, so
// tests exercise the real client without a running Qdrant.
type fakeQdrant struct {
	mu       sync.Mutex
	apiKeys  []string
	creates  []*qd.CreateCollection
	upserts  []*qd.UpsertPoints
	deletes  []*qd.DeletePoints
	queries  []*qd.QueryPoints
	existing []bool // successive CollectionExists answers; the last one repeats

	info      *qd.CollectionInfo
	results   []*qd.ScoredPoint
	err       error // returned by every RPC when set
	createErr error // returned only by Create
}

func (f *fakeQdrant) record(fn func()) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	fn()
	return f.err
}

type healthServer struct {
	qd.UnimplementedQdrantServer
	f *fakeQdrant
}

func (s healthServer) HealthCheck(context.Context, *qd.HealthCheckRequest) (*qd.HealthCheckReply, error) {
	if err := s.f.record(func() {}); err != nil {
		return nil, err
	}
	return &qd.HealthCheckReply{Title: "qdrant", Version: "test"}, nil
}

type collectionsServer struct {
	qd.UnimplementedCollectionsServer
	f *fakeQdrant
}

func (s collectionsServer) CollectionExists(context.Context, *qd.CollectionExistsRequest) (*qd.CollectionExistsResponse, error) {
	var exists bool
	err := s.f.record(func() {
		exists = s.f.existing[0]
		if len(s.f.existing) > 1 {
			s.f.existing = s.f.existing[1:]
		}
	})
	if err != nil {
		return nil, err
	}
	return &qd.CollectionExistsResponse{Result: &qd.CollectionExists{Exists: exists}}, nil
}

func (s collectionsServer) Create(_ context.Context, req *qd.CreateCollection) (*qd.CollectionOperationResponse, error) {
	if err := s.f.record(func() { s.f.creates = append(s.f.creates, req) }); err != nil {
		return nil, err
	}
	if s.f.createErr != nil {
		return nil, s.f.createErr
	}
	return &qd.CollectionOperationResponse{Result: true}, nil
}

func (s collectionsServer) Get(context.Context, *qd.GetCollectionInfoRequest) (*qd.GetCollectionInfoResponse, error) {
	if err := s.f.record(func() {}); err != nil {
		return nil, err
	}
	return &qd.GetCollectionInfoResponse{Result: s.f.info}, nil
}

type pointsServer struct {
	qd.UnimplementedPointsServer
	f *fakeQdrant
}

func (s pointsServer) Upsert(_ context.Context, req *qd.UpsertPoints) (*qd.PointsOperationResponse, error) {
	if err := s.f.record(func() { s.f.upserts = append(s.f.upserts, req) }); err != nil {
		return nil, err
	}
	return &qd.PointsOperationResponse{Result: &qd.UpdateResult{Status: qd.UpdateStatus_Completed}}, nil
}

func (s pointsServer) Delete(_ context.Context, req *qd.DeletePoints) (*qd.PointsOperationResponse, error) {
	if err := s.f.record(func() { s.f.deletes = append(s.f.deletes, req) }); err != nil {
		return nil, err
	}
	return &qd.PointsOperationResponse{Result: &qd.UpdateResult{Status: qd.UpdateStatus_Completed}}, nil
}

func (s pointsServer) Query(_ context.Context, req *qd.QueryPoints) (*qd.QueryResponse, error) {
	if err := s.f.record(func() { s.f.queries = append(s.f.queries, req) }); err != nil {
		return nil, err
	}
	return &qd.QueryResponse{Result: s.f.results}, nil
}

// startFake serves f on a local port and returns the port.
func startFake(t *testing.T, f *fakeQdrant) int {
	t.Helper()
	if f.existing == nil {
		f.existing = []bool{true}
	}
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := grpc.NewServer(grpc.UnaryInterceptor(func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		md, _ := metadata.FromIncomingContext(ctx)
		f.mu.Lock()
		f.apiKeys = append(f.apiKeys, md.Get("api-key")...)
		f.mu.Unlock()
		return handler(ctx, req)
	}))
	qd.RegisterQdrantServer(srv, healthServer{f: f})
	qd.RegisterCollectionsServer(srv, collectionsServer{f: f})
	qd.RegisterPointsServer(srv, pointsServer{f: f})
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)
	return lis.Addr().(*net.TCPAddr).Port
}

// newTestClient returns a Client connected to f, started on a local port.
func newTestClient(t *testing.T, f *fakeQdrant) *Client {
	t.Helper()
	c, err := New(config.QdrantConfig{
		Host:       "127.0.0.1",
		Port:       startFake(t, f),
		Collection: testCollection,
		VectorSize: testVectorSize,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

func collectionInfo(size uint64, dist qd.Distance) *qd.CollectionInfo {
	return &qd.CollectionInfo{Config: &qd.CollectionConfig{Params: &qd.CollectionParams{
		VectorsConfig: qd.NewVectorsConfig(&qd.VectorParams{Size: size, Distance: dist}),
	}}}
}

func TestNewValidatesConfig(t *testing.T) {
	valid := config.QdrantConfig{Host: "localhost", Port: 6334, Collection: "documents", VectorSize: 768}

	tests := []struct {
		name        string
		change      func(*config.QdrantConfig)
		wantInError string
	}{
		{"valid", func(*config.QdrantConfig) {}, ""},
		{"valid with api key and tls", func(c *config.QdrantConfig) { c.APIKey, c.UseTLS = "k", true }, ""},
		{"missing host", func(c *config.QdrantConfig) { c.Host = "" }, "host is required"},
		{"zero port", func(c *config.QdrantConfig) { c.Port = 0 }, "port must be between"},
		{"port too high", func(c *config.QdrantConfig) { c.Port = 70000 }, "port must be between"},
		{"zero vector size", func(c *config.QdrantConfig) { c.VectorSize = 0 }, "vector size must be between"},
		{"vector size too large", func(c *config.QdrantConfig) { c.VectorSize = maxVectorSize + 1 }, "vector size must be between"},
		{"empty collection", func(c *config.QdrantConfig) { c.Collection = " " }, "collection name is required"},
		{"collection with slash", func(c *config.QdrantConfig) { c.Collection = "docs/v2" }, "must not contain"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := valid
			tt.change(&cfg)

			c, err := New(cfg)
			if tt.wantInError == "" {
				if err != nil {
					t.Fatalf("New: %v", err)
				}
				_ = c.Close()
				return
			}
			if err == nil || !strings.Contains(err.Error(), tt.wantInError) {
				t.Fatalf("err = %v, want it to contain %q", err, tt.wantInError)
			}
		})
	}
}

func TestPointIDIsDeterministic(t *testing.T) {
	if got := pointID(docUUID).GetUuid(); got != docUUID {
		t.Errorf("canonical UUID mapped to %s, want it unchanged", got)
	}

	first, second := pointID("doc-1").GetUuid(), pointID("doc-1").GetUuid()
	if first != second {
		t.Fatalf("same id gave different point IDs: %s, %s", first, second)
	}
	if u, err := uuid.Parse(first); err != nil || u.Version() != 5 {
		t.Errorf("derived point ID %s is not a version 5 UUID (err %v)", first, err)
	}
	if pointID("doc-2").GetUuid() == first {
		t.Error("different ids gave the same point ID")
	}
	// A non-canonical spelling of a UUID is a different document ID, so it
	// must not collide with the canonical one.
	if pointID(strings.ToUpper(docUUID)).GetUuid() == docUUID {
		t.Error("uppercase UUID collided with the canonical form")
	}
}

func TestToPointStruct(t *testing.T) {
	payload := map[string]any{"title": "Go channels", "url": "https://example.com/go", "version": int64(3)}
	p := Point{ID: docUUID, Vector: testVector, Payload: payload}

	got, err := toPointStruct(p)
	if err != nil {
		t.Fatalf("toPointStruct: %v", err)
	}
	if got.GetId().GetUuid() != docUUID {
		t.Errorf("id = %v", got.GetId())
	}
	if data := got.GetVectors().GetVector().GetDense().GetData(); len(data) != testVectorSize || data[3] != 0.4 {
		t.Errorf("vector = %v, want %v", data, testVector)
	}
	fields := got.GetPayload()
	if fields[documentIDKey].GetStringValue() != docUUID || fields["title"].GetStringValue() != "Go channels" ||
		fields["version"].GetIntegerValue() != 3 {
		t.Errorf("payload = %v", fields)
	}
	if _, found := payload[documentIDKey]; found {
		t.Error("toPointStruct modified the caller's payload map")
	}

	for name, bad := range map[string]any{"invalid utf-8": "\xff\xfe", "unsupported type": struct{}{}} {
		p.Payload = map[string]any{"field": bad}
		if _, err := toPointStruct(p); !errors.Is(err, ErrInvalidPoint) {
			t.Errorf("%s: err = %v, want ErrInvalidPoint", name, err)
		}
	}
}

func TestValidateVector(t *testing.T) {
	c := &Client{vectorSize: testVectorSize}
	nan := float32(math.NaN())
	inf := float32(math.Inf(1))

	tests := []struct {
		name        string
		vector      []float32
		wantInError string
	}{
		{"valid", testVector, ""},
		{"nil", nil, "expected vector dimension 4, got 0"},
		{"too short", []float32{1, 2, 3}, "expected vector dimension 4, got 3"},
		{"too long", []float32{1, 2, 3, 4, 5}, "expected vector dimension 4, got 5"},
		{"nan", []float32{1, nan, 3, 4}, "value at index 1 is NaN"},
		{"infinity", []float32{1, 2, inf, 4}, "value at index 2 is +Inf"},
		{"all zeros", []float32{0, 0, 0, 0}, "all values are zero"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := c.validateVector(tt.vector)
			if tt.wantInError == "" {
				if err != nil {
					t.Fatalf("validateVector: %v", err)
				}
				return
			}
			if !errors.Is(err, ErrInvalidVector) || !strings.Contains(err.Error(), tt.wantInError) {
				t.Fatalf("err = %v, want ErrInvalidVector containing %q", err, tt.wantInError)
			}
		})
	}
}

func TestToResults(t *testing.T) {
	points := []*qd.ScoredPoint{
		{
			Id:    qd.NewIDUUID(pointID("doc-1").GetUuid()),
			Score: 0.91,
			Payload: qd.NewValueMap(map[string]any{
				documentIDKey: "doc-1",
				"title":       "Go channels",
				"version":     int64(3),
				"rating":      4.5,
				"draft":       false,
				"tags":        []any{"go", "concurrency"},
				"author":      map[string]any{"name": "Ada"},
				"deleted_at":  nil,
			}),
		},
		{Id: qd.NewIDUUID(docUUID), Score: 0.5},
		{Id: qd.NewIDNum(42), Score: 0.25},
	}

	results, err := toResults(points)
	if err != nil {
		t.Fatalf("toResults: %v", err)
	}
	if len(results) != 3 {
		t.Fatalf("got %d results, want 3", len(results))
	}

	first := results[0]
	if first.ID != "doc-1" {
		t.Errorf("ID = %q, want the document ID from the payload", first.ID)
	}
	if math.Abs(first.Score-0.91) > 1e-6 {
		t.Errorf("Score = %v, want 0.91", first.Score)
	}
	p := first.Payload
	tags, _ := p["tags"].([]any)
	author, _ := p["author"].(map[string]any)
	if p["title"] != "Go channels" || p["version"] != int64(3) || p["rating"] != 4.5 || p["draft"] != false ||
		len(tags) != 2 || tags[1] != "concurrency" || author["name"] != "Ada" || p["deleted_at"] != nil {
		t.Errorf("payload = %#v", p)
	}

	// Without a document_id in the payload, the point ID is used.
	if results[1].ID != docUUID || results[2].ID != "42" {
		t.Errorf("fallback IDs = %q, %q; want %q, %q", results[1].ID, results[2].ID, docUUID, "42")
	}
}

func TestPing(t *testing.T) {
	f := &fakeQdrant{}
	if err := newTestClient(t, f).Ping(context.Background()); err != nil {
		t.Fatalf("Ping: %v", err)
	}

	for _, tt := range []struct {
		code          codes.Code
		wantTemporary bool
	}{
		{codes.Unavailable, true},
		{codes.Unauthenticated, false},
	} {
		f := &fakeQdrant{err: status.Error(tt.code, "nope")}
		err := newTestClient(t, f).Ping(context.Background())
		var reqErr *RequestError
		if !errors.As(err, &reqErr) || reqErr.Temporary() != tt.wantTemporary {
			t.Errorf("%s: err = %v, want RequestError with Temporary() = %v", tt.code, err, tt.wantTemporary)
		}
	}
}

func TestAPIKeyIsSent(t *testing.T) {
	f := &fakeQdrant{}
	c, err := New(config.QdrantConfig{
		Host: "127.0.0.1", Port: startFake(t, f), APIKey: testAPIKey,
		Collection: testCollection, VectorSize: testVectorSize,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	if err := c.Ping(context.Background()); err != nil {
		t.Fatalf("Ping: %v", err)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.apiKeys) == 0 || f.apiKeys[len(f.apiKeys)-1] != testAPIKey {
		t.Errorf("api-key metadata = %v, want %q", f.apiKeys, testAPIKey)
	}
}

func TestEnsureCollection(t *testing.T) {
	t.Run("missing collection is created with the configured size and cosine", func(t *testing.T) {
		f := &fakeQdrant{existing: []bool{false}}
		if err := newTestClient(t, f).EnsureCollection(context.Background()); err != nil {
			t.Fatal(err)
		}
		if len(f.creates) != 1 {
			t.Fatalf("creates = %d, want 1", len(f.creates))
		}
		params := f.creates[0].GetVectorsConfig().GetParams()
		if f.creates[0].GetCollectionName() != testCollection || params.GetSize() != testVectorSize || params.GetDistance() != qd.Distance_Cosine {
			t.Errorf("create request = %v", f.creates[0])
		}
	})

	t.Run("existing matching collection is left unchanged", func(t *testing.T) {
		f := &fakeQdrant{existing: []bool{true}, info: collectionInfo(testVectorSize, qd.Distance_Cosine)}
		if err := newTestClient(t, f).EnsureCollection(context.Background()); err != nil {
			t.Fatal(err)
		}
		if len(f.creates) != 0 {
			t.Errorf("existing collection was re-created")
		}
	})

	t.Run("collection created concurrently by another instance", func(t *testing.T) {
		f := &fakeQdrant{
			existing:  []bool{false, true},
			createErr: status.Error(codes.AlreadyExists, "collection already exists"),
			info:      collectionInfo(testVectorSize, qd.Distance_Cosine),
		}
		if err := newTestClient(t, f).EnsureCollection(context.Background()); err != nil {
			t.Fatalf("EnsureCollection: %v", err)
		}
	})

	for _, tt := range []struct {
		name        string
		info        *qd.CollectionInfo
		wantInError string
	}{
		{"wrong vector size", collectionInfo(768, qd.Distance_Cosine), "stores 768-dimension vectors but the configured vector size is 4"},
		{"wrong distance", collectionInfo(testVectorSize, qd.Distance_Euclid), "uses Euclid distance"},
		{"named vectors", &qd.CollectionInfo{Config: &qd.CollectionConfig{Params: &qd.CollectionParams{}}}, "named vectors"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			f := &fakeQdrant{existing: []bool{true}, info: tt.info}
			err := newTestClient(t, f).EnsureCollection(context.Background())
			if err == nil || !strings.Contains(err.Error(), tt.wantInError) {
				t.Fatalf("err = %v, want it to contain %q", err, tt.wantInError)
			}
			if len(f.creates) != 0 {
				t.Error("mismatched collection was modified")
			}
		})
	}
}

func TestUpsert(t *testing.T) {
	f := &fakeQdrant{}
	c := newTestClient(t, f)

	err := c.Upsert(context.Background(), Point{ID: "doc-1", Vector: testVector, Payload: map[string]any{"title": "Go"}})
	if err != nil {
		t.Fatalf("Upsert: %v", err)
	}
	req := f.upserts[0]
	if req.GetCollectionName() != testCollection || !req.GetWait() || len(req.GetPoints()) != 1 {
		t.Fatalf("request = %v, want one point written to %s with wait=true", req, testCollection)
	}
	point := req.GetPoints()[0]
	if point.GetId().GetUuid() != pointID("doc-1").GetUuid() || point.GetPayload()[documentIDKey].GetStringValue() != "doc-1" {
		t.Errorf("point = %v, want the deterministic ID and document_id payload", point)
	}

	// Writing the same document again targets the same point.
	if err := c.Upsert(context.Background(), Point{ID: "doc-1", Vector: testVector}); err != nil {
		t.Fatal(err)
	}
	if again := f.upserts[1].GetPoints()[0].GetId().GetUuid(); again != point.GetId().GetUuid() {
		t.Errorf("second upsert used point %s, want %s", again, point.GetId().GetUuid())
	}

	for name, p := range map[string]Point{
		"empty id":       {ID: "", Vector: testVector},
		"wrong size":     {ID: "doc-1", Vector: []float32{1, 2}},
		"bad payload":    {ID: "doc-1", Vector: testVector, Payload: map[string]any{"x": struct{}{}}},
		"missing vector": {ID: "doc-1"},
	} {
		if err := c.Upsert(context.Background(), p); !errors.Is(err, ErrInvalidPoint) && !errors.Is(err, ErrInvalidVector) {
			t.Errorf("%s: err = %v, want ErrInvalidPoint or ErrInvalidVector", name, err)
		}
	}
	if len(f.upserts) != 2 {
		t.Errorf("%d upserts sent, want 2 (invalid points must not be sent)", len(f.upserts))
	}
}

func TestUpsertReportsRejection(t *testing.T) {
	f := &fakeQdrant{err: status.Error(codes.InvalidArgument, "wrong input: vector dimension error")}
	err := newTestClient(t, f).Upsert(context.Background(), Point{ID: "doc-1", Vector: testVector})

	var reqErr *RequestError
	if !errors.As(err, &reqErr) || reqErr.Temporary() {
		t.Fatalf("err = %v, want a non-temporary RequestError", err)
	}
	if !strings.Contains(err.Error(), "upsert qdrant point doc-1") {
		t.Errorf("error %q does not name the point", err)
	}
}

func TestDelete(t *testing.T) {
	f := &fakeQdrant{}
	c := newTestClient(t, f)

	if err := c.Delete(context.Background(), "doc-1"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	ids := f.deletes[0].GetPoints().GetPoints().GetIds()
	if len(ids) != 1 || ids[0].GetUuid() != pointID("doc-1").GetUuid() || !f.deletes[0].GetWait() {
		t.Errorf("delete request = %v, want the deterministic point ID with wait=true", f.deletes[0])
	}
	if err := c.Delete(context.Background(), ""); !errors.Is(err, ErrInvalidPoint) {
		t.Errorf("empty id: err = %v, want ErrInvalidPoint", err)
	}

	f.err = status.Error(codes.Unavailable, "down")
	var reqErr *RequestError
	if err := c.Delete(context.Background(), "doc-1"); !errors.As(err, &reqErr) || !reqErr.Temporary() {
		t.Errorf("err = %v, want a temporary RequestError", err)
	}
}

func TestSearch(t *testing.T) {
	f := &fakeQdrant{results: []*qd.ScoredPoint{
		{Id: pointID("doc-1"), Score: 0.9, Payload: qd.NewValueMap(map[string]any{documentIDKey: "doc-1", "title": "Go"})},
		{Id: pointID("doc-2"), Score: 0.4, Payload: qd.NewValueMap(map[string]any{documentIDKey: "doc-2"})},
	}}
	c := newTestClient(t, f)

	results, err := c.Search(context.Background(), testVector, 5)
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	q := f.queries[0]
	if q.GetCollectionName() != testCollection || q.GetLimit() != 5 || len(q.GetQuery().GetNearest().GetDense().GetData()) != testVectorSize {
		t.Errorf("query = %v", q)
	}
	if !q.GetWithPayload().GetEnable() {
		t.Error("query did not request payloads")
	}
	if len(results) != 2 || results[0].ID != "doc-1" || results[0].Payload["title"] != "Go" || results[1].ID != "doc-2" {
		t.Errorf("results = %+v", results)
	}
}

func TestSearchValidatesInput(t *testing.T) {
	f := &fakeQdrant{}
	c := newTestClient(t, f)

	tests := []struct {
		name    string
		vector  []float32
		limit   int
		wantErr error
	}{
		{"nil vector", nil, 10, ErrInvalidVector},
		{"wrong dimension", []float32{1, 2, 3}, 10, ErrInvalidVector},
		{"zero limit", testVector, 0, ErrInvalidLimit},
		{"negative limit", testVector, -1, ErrInvalidLimit},
		{"limit above maximum", testVector, MaxSearchLimit + 1, ErrInvalidLimit},
	}
	for _, tt := range tests {
		if _, err := c.Search(context.Background(), tt.vector, tt.limit); !errors.Is(err, tt.wantErr) {
			t.Errorf("%s: err = %v, want %v", tt.name, err, tt.wantErr)
		}
	}
	if len(f.queries) != 0 {
		t.Errorf("%d queries sent for invalid input, want 0", len(f.queries))
	}
}

func TestSearchCancelledContext(t *testing.T) {
	c := newTestClient(t, &fakeQdrant{})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := c.Search(ctx, testVector, 5)
	var reqErr *RequestError
	if !errors.As(err, &reqErr) || status.Code(reqErr.Err) != codes.Canceled {
		t.Fatalf("err = %v, want a RequestError with code Canceled", err)
	}
}

func TestRequestErrorTemporary(t *testing.T) {
	for code, want := range map[codes.Code]bool{
		codes.Unavailable:        true,
		codes.DeadlineExceeded:   true,
		codes.ResourceExhausted:  true,
		codes.Internal:           true,
		codes.InvalidArgument:    false,
		codes.NotFound:           false,
		codes.PermissionDenied:   false,
		codes.Unauthenticated:    false,
		codes.FailedPrecondition: false,
	} {
		if got := newRequestError(status.Error(code, "x")).Temporary(); got != want {
			t.Errorf("%s: Temporary() = %v, want %v", code, got, want)
		}
	}
}
