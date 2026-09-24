package loadtest

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	kafkago "github.com/segmentio/kafka-go"
)

// ChangeEvent builds the Kafka message Debezium would publish for a document,
// in the shape the connector produces with JsonConverter and schemas disabled:
// the envelope the consumer parses in production.
//
// Publishing these directly is how two benchmarks reach conditions the
// database cannot produce on demand: a sustained event rate higher than the
// consumer can absorb, and the same event delivered many times. Everything
// downstream of Kafka is the real system either way.
//
// op is Debezium's one-letter code: "c" create, "u" update, "d" delete,
// "r" snapshot read.
func ChangeEvent(doc Document, op string, version int64, at time.Time) (key, value []byte, err error) {
	row := map[string]any{
		"id":         doc.ID,
		"title":      doc.Title,
		"body":       doc.Body,
		"url":        doc.URL,
		"version":    version,
		"updated_at": at.UTC().Format(time.RFC3339Nano),
	}
	before, after := any(nil), any(row)
	if op == "d" {
		before, after = row, nil
	}

	envelope := map[string]any{
		"before": before,
		"after":  after,
		"op":     op,
		"ts_ms":  at.UnixMilli(),
		"source": map[string]any{
			"version":   "3.6.3.Final",
			"connector": "postgresql",
			"name":      "search",
			"db":        "searchdb",
			"schema":    "public",
			"table":     "documents",
			"snapshot":  "false",
			"ts_ms":     at.UnixMilli(),
			// The log position identifies the event; the row version stands in
			// for it here, since these events never went through the WAL.
			"lsn":  version,
			"txId": version,
		},
	}

	if key, err = json.Marshal(map[string]any{"id": doc.ID}); err != nil {
		return nil, nil, fmt.Errorf("encode key: %w", err)
	}
	if value, err = json.Marshal(envelope); err != nil {
		return nil, nil, fmt.Errorf("encode value: %w", err)
	}
	return key, value, nil
}

// Publisher writes change events to the topic the consumer reads.
type Publisher struct {
	writer *kafkago.Writer
}

// NewPublisher returns a publisher for topic. The caller owns it and must
// call Close, which flushes what is still buffered.
func NewPublisher(brokers []string, topic string) (*Publisher, error) {
	if len(brokers) == 0 || topic == "" {
		return nil, fmt.Errorf("loadtest: brokers and a topic are required")
	}
	return &Publisher{writer: &kafkago.Writer{
		Addr:     kafkago.TCP(brokers...),
		Topic:    topic,
		Balancer: &kafkago.Hash{}, // key by document, as Debezium does
		// Batched writes: the point of these benchmarks is to produce faster
		// than the consumer can keep up, which one message per request could
		// not do.
		BatchSize:    100,
		BatchTimeout: 10 * time.Millisecond,
		RequiredAcks: kafkago.RequireOne,
		Async:        false,
	}}, nil
}

// Close flushes and closes the writer.
func (p *Publisher) Close() error { return p.writer.Close() }

// PublishResult is what one publishing run did.
type PublishResult struct {
	Published int
	Duration  time.Duration
}

// Rate is the achieved publish rate, which is not always the requested one:
// it is what the benchmark actually produced.
func (r PublishResult) Rate() float64 { return Rate(r.Published, r.Duration) }

// PublishOptions configures a publishing run.
type PublishOptions struct {
	// Documents are published in order, repeated Repeats times in total. Two
	// repeats of one document are the same logical event delivered twice,
	// which is what the duplicate benchmark needs.
	Documents []Document
	Repeats   int
	// Operation is Debezium's one-letter code, "c" by default.
	Operation string
	// TargetRate caps how many events per second are published. Zero means as
	// fast as the broker accepts them.
	TargetRate float64
	// Version is the row version every event carries. Keeping it fixed makes
	// repeats duplicates of one another rather than a series of updates.
	Version int64
}

// Publish writes the configured events, pacing them to TargetRate.
//
// Pacing is done by sleeping until each event's scheduled time rather than by
// sleeping a fixed amount between events, so a slow write does not shift the
// whole schedule and quietly lower the rate being tested.
func (p *Publisher) Publish(ctx context.Context, opts PublishOptions) (PublishResult, error) {
	op := opts.Operation
	if op == "" {
		op = "c"
	}
	repeats := max(opts.Repeats, 1)
	version := opts.Version
	if version <= 0 {
		version = 1
	}

	var (
		interval time.Duration
		started  = time.Now()
		sent     int
	)
	if opts.TargetRate > 0 {
		interval = time.Duration(float64(time.Second) / opts.TargetRate)
	}

	at := started.UTC()
	for range repeats {
		for _, doc := range opts.Documents {
			key, value, err := ChangeEvent(doc, op, version, at)
			if err != nil {
				return PublishResult{Published: sent, Duration: time.Since(started)}, err
			}
			if interval > 0 {
				due := started.Add(time.Duration(sent) * interval)
				if wait := time.Until(due); wait > 0 {
					select {
					case <-ctx.Done():
						return PublishResult{Published: sent, Duration: time.Since(started)}, ctx.Err()
					case <-time.After(wait):
					}
				}
			}
			if err := p.writer.WriteMessages(ctx, kafkago.Message{Key: key, Value: value}); err != nil {
				return PublishResult{Published: sent, Duration: time.Since(started)}, fmt.Errorf("publish event: %w", err)
			}
			sent++
		}
	}
	return PublishResult{Published: sent, Duration: time.Since(started)}, nil
}

// Lag is one partition's consumer lag: how many messages have been produced
// that the group has not committed yet.
type Lag struct {
	Partition int
	Committed int64
	End       int64
}

// Behind is how many messages the group still has to process on this
// partition.
func (l Lag) Behind() int64 {
	if l.End <= l.Committed {
		return 0
	}
	return l.End - l.Committed
}

// ConsumerLag reads the committed offsets of a consumer group and the end of
// each partition, and returns the gap.
//
// Lag is the honest measure of whether the pipeline keeps up: a consumer that
// is falling behind still reports healthy throughput, because it is busy, and
// only the growing gap says the work is arriving faster than it leaves.
func ConsumerLag(ctx context.Context, brokers []string, topic, group string) ([]Lag, error) {
	if len(brokers) == 0 || topic == "" || group == "" {
		return nil, fmt.Errorf("loadtest: brokers, a topic and a group are required")
	}
	client := &kafkago.Client{Addr: kafkago.TCP(brokers...), Timeout: 10 * time.Second}

	conn, err := kafkago.DialContext(ctx, "tcp", brokers[0])
	if err != nil {
		return nil, fmt.Errorf("dial kafka: %w", err)
	}
	partitions, err := conn.ReadPartitions(topic)
	_ = conn.Close()
	if err != nil {
		return nil, fmt.Errorf("read partitions of %s: %w", topic, err)
	}

	ids := make([]int, 0, len(partitions))
	offsets := make(map[string][]kafkago.OffsetRequest, 1)
	for _, p := range partitions {
		ids = append(ids, p.ID)
		offsets[topic] = append(offsets[topic], kafkago.LastOffsetOf(p.ID))
	}

	ends, err := client.ListOffsets(ctx, &kafkago.ListOffsetsRequest{Topics: offsets})
	if err != nil {
		return nil, fmt.Errorf("list offsets of %s: %w", topic, err)
	}
	committed, err := client.OffsetFetch(ctx, &kafkago.OffsetFetchRequest{
		GroupID: group,
		Topics:  map[string][]int{topic: ids},
	})
	if err != nil {
		return nil, fmt.Errorf("read committed offsets of group %s: %w", group, err)
	}

	lastOffset := make(map[int]int64, len(ids))
	for _, partition := range ends.Topics[topic] {
		lastOffset[partition.Partition] = partition.LastOffset
	}

	lags := make([]Lag, 0, len(ids))
	for _, partition := range committed.Topics[topic] {
		lags = append(lags, Lag{
			Partition: partition.Partition,
			Committed: partition.CommittedOffset,
			End:       lastOffset[partition.Partition],
		})
	}
	return lags, nil
}

// TopicSize returns how many messages a topic has ever held, summed over its
// partitions.
//
// Taken before and after a run, the difference is how many messages that run
// produced. It is how the dead-letter count is measured without consuming the
// topic, which would move a consumer group's offsets and change what a later
// run sees.
func TopicSize(ctx context.Context, brokers []string, topic string) (int64, error) {
	if len(brokers) == 0 || topic == "" {
		return 0, fmt.Errorf("loadtest: brokers and a topic are required")
	}
	conn, err := kafkago.DialContext(ctx, "tcp", brokers[0])
	if err != nil {
		return 0, fmt.Errorf("dial kafka: %w", err)
	}
	partitions, err := conn.ReadPartitions(topic)
	_ = conn.Close()
	if err != nil {
		return 0, fmt.Errorf("read partitions of %s: %w", topic, err)
	}

	requests := make(map[string][]kafkago.OffsetRequest, 1)
	for _, p := range partitions {
		requests[topic] = append(requests[topic], kafkago.LastOffsetOf(p.ID))
	}
	client := &kafkago.Client{Addr: kafkago.TCP(brokers...), Timeout: 10 * time.Second}
	ends, err := client.ListOffsets(ctx, &kafkago.ListOffsetsRequest{Topics: requests})
	if err != nil {
		return 0, fmt.Errorf("list offsets of %s: %w", topic, err)
	}

	var total int64
	for _, partition := range ends.Topics[topic] {
		total += partition.LastOffset
	}
	return total, nil
}

// TotalLag sums the lag across partitions.
func TotalLag(lags []Lag) int64 {
	var total int64
	for _, lag := range lags {
		total += lag.Behind()
	}
	return total
}
