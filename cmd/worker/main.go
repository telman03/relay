package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	kgo "github.com/segmentio/kafka-go"
)

// relay/worker — Kafka consumer
//
// Build progress:
//   - [x] Step 5: Kafka consumer subscribes to "relay.jobs"
//   - [x] Step 5: process(message) — for now just log it
//
// Run: go run ./cmd/worker
func main() {
	brokers := []string{getEnv("KAFKA_BROKER", "localhost:9092")}
	topic := getEnv("KAFKA_TOPIC", "relay.jobs")
	groupID := getEnv("KAFKA_GROUP", "relay-worker")

	log.Printf("relay/worker: starting (brokers=%v topic=%s group=%s)",
		brokers, topic, groupID)

	// GroupID = consumer group. If you scale to N worker pods, Kafka
	// distributes partitions across them automatically. That's how Kafka
	// scales consumers horizontally.
	reader := kgo.NewReader(kgo.ReaderConfig{
		Brokers: brokers,
		Topic:   topic,
		GroupID: groupID,
	})
	defer reader.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Graceful shutdown on SIGINT/SIGTERM (K8s sends SIGTERM on pod stop).
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sig
		log.Println("relay/worker: shutdown signal received")
		cancel()
	}()

	for {
		msg, err := reader.ReadMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				log.Println("relay/worker: stopped")
				return
			}
			log.Printf("relay/worker: read error: %v", err)
			continue
		}
		// "Processing" — just log. Real worker would do work here.
		log.Printf("processed: client=%s payload=%s partition=%d offset=%d",
			string(msg.Key), string(msg.Value), msg.Partition, msg.Offset)
	}
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
