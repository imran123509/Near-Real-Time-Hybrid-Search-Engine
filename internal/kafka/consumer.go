// Package kafka reads document events from Kafka and publishes failed events
// to a dead-letter topic.
package kafka

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	kafkago "github.com/segmentio/kafka-go"

	"near-real-time-hybrid-search-engine/internal/config"
)

const (
	commitInterval   = time.Second
	fetchRetryDelay  = 2 * time.Second
	maxFetchFailures = 10
)

// Message is a record fetched from Kafka.
type Message struct {
	Topic     string
	Partition int
	Offset    int64
	Key       []byte
	Value     []byte

	done func()
}

// Done marks the message as fully handled, either indexed or dead-lettered,
// so its offset can be committed. Do not call Done for a message that was
// abandoned; it will be delivered again after a restart or rebalance.
func (m Message) Done() {
	if m.done != nil {
		m.done()
	}
}

// Consumer fetches messages for a consumer group and commits their offsets
// once they are done.
//
// Messages are handled concurrently, so they can finish out of order. A
// committed offset tells Kafka that every earlier message in the partition is
// finished, so the consumer only commits up to the first message that is still
// in flight. After a crash, messages past that point are delivered again,
// which is safe because indexing is idempotent.
type Consumer struct {
	reader  *kafkago.Reader
	offsets *offsetTracker
	logger  *slog.Logger
	observe func(Event)
}

// Event is something the consumer did with the broker, reported to the
// function registered with Observe. It exists so metrics can be attached
// without this package depending on a metrics library.
//
// Kind is one of a fixed set, so it is safe to use as a metric label; the
// error itself is logged here and never travels into a label.
type Event struct {
	Kind  string
	Topic string
	Err   error
}

// The kinds an Event can have.
const (
	// EventFetched is one message read from the broker.
	EventFetched = "fetched"
	// EventFetchFailed is a failed read. The reader reconnects by itself, so
	// a few of these are normal; a stream of them is not.
	EventFetchFailed = "fetch_failed"
	// EventCommitted is an offset commit the consumer asked for, which means
	// every message up to that offset is finished.
	EventCommitted = "committed"
	// EventCommitFailed is a commit the broker refused. The work was done;
	// the messages will simply be delivered again.
	EventCommitFailed = "commit_failed"
)

// Observe registers a function called for each Event. It runs on the fetch
// loop or on a worker finishing a message, so it must not block.
func (c *Consumer) Observe(f func(Event)) { c.observe = f }

func (c *Consumer) report(kind, topic string, err error) {
	if c.observe != nil {
		c.observe(Event{Kind: kind, Topic: topic, Err: err})
	}
}

// NewConsumer checks that the brokers are reachable and the topic exists, then
// returns a Consumer. The caller owns the Consumer and must call Close.
func NewConsumer(ctx context.Context, cfg config.KafkaConfig, logger *slog.Logger) (*Consumer, error) {
	if len(cfg.Brokers) == 0 {
		return nil, errors.New("KAFKA_BROKERS is required")
	}
	if cfg.Topic == "" || cfg.ConsumerGroup == "" {
		return nil, errors.New("kafka topic and group id are required")
	}
	if err := checkTopic(ctx, cfg.Brokers, cfg.Topic); err != nil {
		return nil, err
	}

	reader := kafkago.NewReader(kafkago.ReaderConfig{
		Brokers:        cfg.Brokers,
		GroupID:        cfg.ConsumerGroup,
		Topic:          cfg.Topic,
		StartOffset:    kafkago.FirstOffset,
		CommitInterval: commitInterval,
		ErrorLogger: kafkago.LoggerFunc(func(msg string, args ...any) {
			logger.Error("kafka reader", "detail", fmt.Sprintf(msg, args...))
		}),
	})

	return &Consumer{reader: reader, offsets: newOffsetTracker(), logger: logger}, nil
}

// Run fetches messages and hands each one to submit until ctx is cancelled.
//
// Backpressure: submit is expected to block while the workers are busy. Run
// does not fetch the next message until submit returns, so a slow OpenSearch,
// Qdrant or Gemini slows consumption instead of piling up messages or
// goroutines in memory.
//
// Run returns nil when ctx is cancelled and an error when Kafka keeps failing.
func (c *Consumer) Run(ctx context.Context, submit func(context.Context, Message) error) error {
	failures := 0
	for {
		km, err := c.reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			// The reader reconnects on its own; give it time before giving up.
			failures++
			c.report(EventFetchFailed, c.reader.Config().Topic, err)
			if failures >= maxFetchFailures {
				return fmt.Errorf("fetch message: %d consecutive failures: %w", failures, err)
			}
			c.logger.Warn("fetch message failed", "attempt", failures, "error", err)
			select {
			case <-ctx.Done():
				return nil
			case <-time.After(fetchRetryDelay):
			}
			continue
		}
		failures = 0
		c.report(EventFetched, km.Topic, nil)

		if err := submit(ctx, c.track(km)); err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("submit message: %w", err)
		}
	}
}

// Stats returns reader counters such as lag, fetch errors and rebalances.
// It is the hook for health checks or metrics exporters.
func (c *Consumer) Stats() kafkago.ReaderStats {
	return c.reader.Stats()
}

// Close stops the reader and flushes pending offset commits. Call it after
// the workers have stopped so their last completed offsets are included.
func (c *Consumer) Close() error {
	return c.reader.Close()
}

func (c *Consumer) track(km kafkago.Message) Message {
	c.offsets.add(km.Topic, km.Partition, km.Offset)
	return Message{
		Topic:     km.Topic,
		Partition: km.Partition,
		Offset:    km.Offset,
		Key:       km.Key,
		Value:     km.Value,
		done: func() {
			offset, ok := c.offsets.markDone(km.Topic, km.Partition, km.Offset)
			if !ok {
				return
			}
			// With CommitInterval set, this only queues the offset; the reader
			// commits in the background and on Close.
			commit := kafkago.Message{Topic: km.Topic, Partition: km.Partition, Offset: offset}
			if err := c.reader.CommitMessages(context.Background(), commit); err != nil {
				c.logger.Error("commit offset failed", "topic", km.Topic, "partition", km.Partition, "offset", offset, "error", err)
				c.report(EventCommitFailed, km.Topic, err)
				return
			}
			c.report(EventCommitted, km.Topic, nil)
		},
	}
}

func checkTopic(ctx context.Context, brokers []string, topic string) error {
	var errs []error
	for _, broker := range brokers {
		conn, err := kafkago.DialContext(ctx, "tcp", broker)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		partitions, err := conn.ReadPartitions(topic)
		_ = conn.Close()
		if err != nil {
			return fmt.Errorf("read partitions for topic %q: %w", topic, err)
		}
		if len(partitions) == 0 {
			return fmt.Errorf("topic %q has no partitions", topic)
		}
		return nil
	}
	return fmt.Errorf("connect to kafka brokers: %w", errors.Join(errs...))
}

type topicPartition struct {
	topic     string
	partition int
}

// offsetTracker records which fetched offsets are finished so that only
// offsets with no unfinished messages before them are committed.
type offsetTracker struct {
	mu         sync.Mutex
	partitions map[topicPartition]*partitionOffsets
}

type partitionOffsets struct {
	inFlight []int64 // offsets in fetch order, not yet committable
	done     map[int64]bool
}

func newOffsetTracker() *offsetTracker {
	return &offsetTracker{partitions: make(map[topicPartition]*partitionOffsets)}
}

func (t *offsetTracker) add(topic string, partition int, offset int64) {
	t.mu.Lock()
	defer t.mu.Unlock()

	key := topicPartition{topic, partition}
	p, ok := t.partitions[key]
	if !ok {
		p = &partitionOffsets{done: make(map[int64]bool)}
		t.partitions[key] = p
	}
	p.inFlight = append(p.inFlight, offset)
}

// markDone records offset as finished. It returns the highest offset whose
// predecessors are all finished, and false if nothing new can be committed.
func (t *offsetTracker) markDone(topic string, partition int, offset int64) (int64, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()

	p, ok := t.partitions[topicPartition{topic, partition}]
	if !ok {
		return 0, false
	}
	p.done[offset] = true

	committable, advanced := int64(0), false
	for len(p.inFlight) > 0 && p.done[p.inFlight[0]] {
		committable, advanced = p.inFlight[0], true
		delete(p.done, p.inFlight[0])
		p.inFlight = p.inFlight[1:]
	}
	return committable, advanced
}
