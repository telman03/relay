package main

import (
	"context"
	"log"
	"net"
	"strconv"
	"time"

	relaykafka "github.com/telman03/relay/internal/kafka"
	pb "github.com/telman03/relay/internal/pb"
	"github.com/telman03/relay/internal/ratelimit"
	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

// relay/api — gRPC server, Kafka producer, Redis rate limiter
//
// Build progress:
//   - [x] Step 2: gRPC server bound to :8080
//   - [x] Step 3: Kafka producer publishes on each accepted RPC
//   - [x] Step 4: Redis token-bucket rate limiter
//
// Run: go run ./cmd/api

type relayServer struct {
	pb.UnimplementedRelayServer
	producer *relaykafka.Producer
	limiter  *ratelimit.TokenBucket
}

func (s *relayServer) SubmitJob(ctx context.Context, req *pb.SubmitJobRequest) (*pb.SubmitJobResponse, error) {
	// 1. Rate limit check FIRST — before doing any expensive work.
	allowed, err := s.limiter.Allow(ctx, req.GetClientId())
	if err != nil {
		// Redis down → fail OPEN (allow the request).
		// In production you'd alert; this is a deliberate availability choice.
		log.Printf("rate limiter error (failing open): %v", err)
	} else if !allowed {
		log.Printf("rate limited: client=%s", req.GetClientId())
		return &pb.SubmitJobResponse{
			Accepted: false,
			Reason:   "rate_limited",
		}, nil
	}

	jobID := strconv.FormatInt(time.Now().UnixNano(), 10)

	// 2. Publish to Kafka. Key = client_id so events for one client stay in order.
	if err := s.producer.Publish(ctx, []byte(req.GetClientId()), []byte(req.GetPayload())); err != nil {
		log.Printf("kafka publish failed: %v", err)
		return &pb.SubmitJobResponse{
			Accepted: false,
			Reason:   "publish_failed",
		}, nil
	}

	log.Printf("submit: client=%s job=%s payload_len=%d",
		req.GetClientId(), jobID, len(req.GetPayload()))

	return &pb.SubmitJobResponse{
		JobId:    jobID,
		Accepted: true,
	}, nil
}

func main() {
	// --- Redis client + rate limiter ---
	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
	defer rdb.Close()

	// capacity=5, refill=1/sec → first 5 requests instant, then 1/sec steady.
	// With this config, a tight loop of 20 requests should see ~5 accepted, ~15 rate_limited.
	limiter := ratelimit.NewTokenBucket(rdb, 5, 1.0)

	// --- Kafka producer ---
	producer := relaykafka.NewProducer([]string{"localhost:9092"}, "relay.jobs")
	defer producer.Close()

	// --- gRPC server ---
	lis, err := net.Listen("tcp", ":8080")
	if err != nil {
		log.Fatal(err)
	}

	s := grpc.NewServer()
	pb.RegisterRelayServer(s, &relayServer{
		producer: producer,
		limiter:  limiter,
	})
	reflection.Register(s)

	log.Println("listening on :8080")
	log.Fatal(s.Serve(lis))
}
