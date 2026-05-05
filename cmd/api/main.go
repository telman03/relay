package main

import (
	"context"
	"log"
	"net"
	"strconv"
	"time"

	relaykafka "github.com/telman03/relay/internal/kafka"
	pb "github.com/telman03/relay/internal/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

// relay/api — gRPC server, Kafka producer, Redis rate limiter
//
// Build progress (uncomment as you implement):
//   - [x] Step 2: gRPC server bound to :8080
//   - [x] Step 3: Kafka producer publishes on each accepted RPC
//   - [ ] Step 4: Redis token-bucket rate limiter
//
// Run: go run ./cmd/api

type relayServer struct {
	pb.UnimplementedRelayServer
	producer *relaykafka.Producer
}

func (s *relayServer) SubmitJob(ctx context.Context, req *pb.SubmitJobRequest) (*pb.SubmitJobResponse, error) {
	jobID := strconv.FormatInt(time.Now().UnixNano(), 10)

	// Publish to Kafka. Key = client_id so events for one client stay in order.
	err := s.producer.Publish(
		ctx,
		[]byte(req.GetClientId()),
		[]byte(req.GetPayload()),
	)
	if err != nil {
		log.Printf("kafka publish failed: %v", err)
		// App-level failure → tell client via response, NOT via gRPC error.
		// gRPC errors are for transport-level failures.
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
	// Construct the producer once, share across all RPC calls.
	producer := relaykafka.NewProducer([]string{"localhost:9092"}, "relay.jobs")
	defer producer.Close() // CRITICAL — flushes pending batch on shutdown.

	lis, err := net.Listen("tcp", ":8080")
	if err != nil {
		log.Fatal(err)
	}

	s := grpc.NewServer()
	pb.RegisterRelayServer(s, &relayServer{producer: producer})
	reflection.Register(s)

	log.Println("listening on :8080")
	log.Fatal(s.Serve(lis))
}
