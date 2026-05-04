package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"
)

// relay/worker — Kafka consumer
//
// Build progress (uncomment as you implement):
//   - [ ] Step 5: Kafka consumer subscribes to "relay.jobs"
//   - [ ] Step 5: process(message) — for now just log it
//
// Run: go run ./cmd/worker
func main() {
	log.Println("relay/worker: starting (stub)…")

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	log.Println("relay/worker: shutting down")
}
