package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"
)

// relay/api — gRPC server, Kafka producer, Redis rate limiter
//
// Build progress (uncomment as you implement):
//   - [ ] Step 2: gRPC server bound to :8080
//   - [ ] Step 3: Kafka producer publishes on each accepted RPC
//   - [ ] Step 4: Redis token-bucket rate limiter
//
// Run: go run ./cmd/api
func main() {
	log.Println("relay/api: starting (stub)…")

	// Block until SIGINT/SIGTERM so this looks like a real server already.
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	log.Println("relay/api: shutting down")
}
