-- Sample documents for local development only. They are inserted before the
-- Debezium connector starts, so they reach the search indexes through its
-- initial snapshot (op "r").
--
-- Some are phrased so that a query can find them by meaning without sharing
-- their keywords, e.g. "how do goroutines talk to each other" should find the
-- Go concurrency document through vector search. version and updated_at are
-- set by the documents_stamp_change trigger.

INSERT INTO documents (id, title, body, url) VALUES
('0b7d4f6e-3c1a-4e2b-9f5d-1a2b3c4d5e01',
 'Distributed Systems Fundamentals',
 'A distributed system is a group of computers that appear to users as one machine. Nodes coordinate by passing messages over an unreliable network, so designs must tolerate partial failure, clock skew and network partitions. The CAP theorem states that during a partition a system has to choose between consistency and availability.',
 'https://example.com/articles/distributed-systems'),

('0b7d4f6e-3c1a-4e2b-9f5d-1a2b3c4d5e02',
 'Kafka Architecture Explained',
 'Apache Kafka stores streams of records in topics that are split into partitions and replicated across brokers. Producers append to the end of a partition and consumers track their position with offsets, so many independent consumer groups can read the same data at their own pace. Since version 4.0 Kafka manages its metadata with KRaft instead of ZooKeeper.',
 'https://example.com/articles/kafka-architecture'),

('0b7d4f6e-3c1a-4e2b-9f5d-1a2b3c4d5e03',
 'Go Concurrency Patterns',
 'Goroutines are lightweight threads managed by the Go runtime. They communicate by sending values over channels rather than by sharing memory, and the select statement waits on several channel operations at once. Worker pools, fan-out and fan-in, and context cancellation are the building blocks of most concurrent Go services.',
 'https://example.com/articles/go-concurrency'),

('0b7d4f6e-3c1a-4e2b-9f5d-1a2b3c4d5e04',
 'Vector Databases and Embeddings',
 'An embedding model turns text into a list of numbers that places similar meanings close together. A vector database such as Qdrant indexes those vectors with HNSW graphs and answers nearest-neighbour queries by cosine similarity, which finds related passages even when they share no words with the question.',
 'https://example.com/articles/vector-databases'),

('0b7d4f6e-3c1a-4e2b-9f5d-1a2b3c4d5e05',
 'Hybrid Search with Reciprocal Rank Fusion',
 'Hybrid search runs keyword retrieval and semantic retrieval side by side and merges the two ranked lists. Reciprocal Rank Fusion scores each document by summing one over k plus its rank in every list, so a document that both retrievers rank moderately well can beat one that only a single retriever ranks first.',
 'https://example.com/articles/hybrid-search'),

('0b7d4f6e-3c1a-4e2b-9f5d-1a2b3c4d5e06',
 'Change Data Capture with Debezium',
 'Change data capture streams every insert, update and delete from a database as an event. Debezium reads the PostgreSQL write-ahead log through a logical replication slot and publishes each row change to Kafka, starting with a consistent snapshot of the existing rows.',
 'https://example.com/articles/change-data-capture'),

('0b7d4f6e-3c1a-4e2b-9f5d-1a2b3c4d5e07',
 'BM25 Ranking in Full-Text Search',
 'BM25 scores a document for a query from how often each query term appears in it, how rare the term is across the collection, and how long the document is. Term frequency saturates, so repeating a word many times stops helping. OpenSearch and Lucene use BM25 as their default similarity.',
 'https://example.com/articles/bm25'),

('0b7d4f6e-3c1a-4e2b-9f5d-1a2b3c4d5e08',
 'PostgreSQL Logical Replication',
 'Logical replication decodes the write-ahead log into row-level changes that other systems can apply. It needs wal_level set to logical, a publication naming the tables to replicate, and a replication slot that remembers how far each subscriber has read, which is why an abandoned slot makes the server keep old WAL files.',
 'https://example.com/articles/logical-replication'),

('0b7d4f6e-3c1a-4e2b-9f5d-1a2b3c4d5e09',
 'Idempotent Event Processing',
 'Message brokers deliver at least once, so a consumer must be able to handle the same event twice without corrupting its state. Deterministic identifiers and version checks turn a replayed write into a no-op, which makes retries and restarts safe.',
 'https://example.com/articles/idempotency'),

('0b7d4f6e-3c1a-4e2b-9f5d-1a2b3c4d5e10',
 'Backpressure in Streaming Pipelines',
 'When a downstream service slows down, a streaming pipeline has to slow its intake instead of buffering without limit. Bounded queues between stages make a slow consumer push back on the producer, keeping memory use predictable under load.',
 'https://example.com/articles/backpressure');
