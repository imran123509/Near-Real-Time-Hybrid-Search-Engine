package indexing

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"log/slog"
	"sync"

	"near-real-time-hybrid-search-engine/internal/kafka"
)

// Handler processes one message. A nil result means the message is finished
// and its offset may be committed.
type Handler func(ctx context.Context, msg kafka.Message) error

// WorkerPool runs a fixed number of workers, each reading from its own
// bounded queue.
//
// Backpressure: Submit blocks while the chosen worker's queue is full, and the
// Kafka consumer does not fetch another message until Submit returns. When
// OpenSearch, Qdrant or Gemini slow down, workers take longer, queues fill, and
// consumption slows to match. Memory and goroutines stay bounded by
// workers + queueSize messages, whatever the topic's traffic.
//
// Ordering: messages are routed by Kafka key (the document ID), so all events
// for a document go to the same worker and are processed in order. Two
// workers never write the same document at the same time.
type WorkerPool struct {
	queues  []chan kafka.Message
	handler Handler
	onFatal func(error)
	logger  *slog.Logger

	wg       sync.WaitGroup
	quit     chan struct{}
	stopOnce sync.Once
	cancel   context.CancelFunc
}

// NewWorkerPool returns a pool of workers sharing queueSize buffered messages,
// split evenly between them with at least one each. onFatal is called when a
// message fails in a way that means consumption must stop, such as a failed
// dead-letter publish.
func NewWorkerPool(workers, queueSize int, handler Handler, onFatal func(error), logger *slog.Logger) (*WorkerPool, error) {
	if workers <= 0 || queueSize <= 0 {
		return nil, fmt.Errorf("workers and queue size must be positive, got %d and %d", workers, queueSize)
	}
	perWorker := max(queueSize/workers, 1)
	queues := make([]chan kafka.Message, workers)
	for i := range queues {
		queues[i] = make(chan kafka.Message, perWorker)
	}
	return &WorkerPool{
		queues:  queues,
		handler: handler,
		onFatal: onFatal,
		logger:  logger,
		quit:    make(chan struct{}),
	}, nil
}

// Start launches the workers. Every handler call receives a context derived
// from ctx, which stays alive during graceful shutdown so in-flight messages
// can finish; Shutdown cancels it only when its deadline passes.
func (p *WorkerPool) Start(ctx context.Context) {
	ctx, p.cancel = context.WithCancel(ctx)
	for i, queue := range p.queues {
		p.wg.Add(1)
		go p.work(ctx, i, queue)
	}
}

// Submit queues msg for its worker, blocking while that queue is full.
func (p *WorkerPool) Submit(ctx context.Context, msg kafka.Message) error {
	select {
	case p.queues[p.route(msg)] <- msg:
		return nil
	case <-p.quit:
		return errors.New("worker pool is shutting down")
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Shutdown stops the workers. Messages being processed are allowed to finish;
// queued messages that have not started are left uncommitted and will be
// delivered again. If ctx expires first, in-flight handlers are cancelled and
// Shutdown waits for them to return.
func (p *WorkerPool) Shutdown(ctx context.Context) error {
	p.stopOnce.Do(func() { close(p.quit) })

	done := make(chan struct{})
	go func() {
		p.wg.Wait()
		close(done)
	}()

	var err error
	select {
	case <-done:
	case <-ctx.Done():
		err = fmt.Errorf("workers did not finish in time, cancelling in-flight messages: %w", ctx.Err())
		if p.cancel != nil {
			p.cancel()
		}
		<-done
	}
	if p.cancel != nil {
		p.cancel()
	}
	return err
}

func (p *WorkerPool) work(ctx context.Context, id int, queue <-chan kafka.Message) {
	defer p.wg.Done()
	for {
		// Check quit on its own first: when both channels are ready, select
		// picks randomly, and a stopping pool must not start new messages.
		select {
		case <-p.quit:
			return
		default:
		}

		select {
		case <-p.quit:
			return
		case msg := <-queue:
			p.handle(ctx, id, msg)
		}
	}
}

func (p *WorkerPool) handle(ctx context.Context, id int, msg kafka.Message) {
	err := p.handler(ctx, msg)
	switch {
	case err == nil:
		msg.Done()
	case ctx.Err() != nil:
		// Shutdown deadline passed; leave the offset uncommitted.
	default:
		p.logger.Error("message could not be completed, stopping consumption",
			"worker", id, "partition", msg.Partition, "offset", msg.Offset, "error", err)
		p.onFatal(err)
	}
}

func (p *WorkerPool) route(msg kafka.Message) int {
	if len(msg.Key) == 0 {
		return int(msg.Offset % int64(len(p.queues)))
	}
	h := fnv.New32a()
	_, _ = h.Write(msg.Key)
	return int(h.Sum32() % uint32(len(p.queues)))
}
