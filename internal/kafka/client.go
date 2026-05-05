package kafka

import (
	"context"

	kgo "github.com/segmentio/kafka-go"
)

// Producer is a thin wrapper over kafka.Writer.
// We hide the segmentio import behind our own type so the rest of the
// codebase doesn't depend directly on this library.
type Producer struct {
	w *kgo.Writer
}

// NewProducer returns a Producer that writes to `topic` at the given brokers.
// Locally brokers is just []string{"localhost:9092"}.
func NewProducer(brokers []string, topic string) *Producer {
	w := &kgo.Writer{
		Addr:                   kgo.TCP(brokers...),
		Topic:                  topic,
		Balancer:               &kgo.Hash{}, // same key always lands on same partition
		AllowAutoTopicCreation: true,        // dev convenience; turn off in prod
	}
	return &Producer{w: w}
}

// Publish sends one message. Key is used for partitioning (same key -> same
// partition, preserving ordering for that key). Value is the payload bytes.
func (p *Producer) Publish(ctx context.Context, key, value []byte) error {
	return p.w.WriteMessages(ctx, kgo.Message{
		Key:   key,
		Value: value,
	})
}

// Close flushes any pending messages and closes the underlying writer.
// ALWAYS call this on shutdown — Writer batches messages, so without Close()
// the last batch may never send.
func (p *Producer) Close() error {
	return p.w.Close()
}
