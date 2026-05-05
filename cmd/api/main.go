package main

import (
	"context"
	"log"
	"net"
	"strconv"
	"time"

	pb "github.com/telman03/relay/internal/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

// relay/api — gRPC server, Kafka producer, Redis rate limiter
//
// Build progress (uncomment as you implement):
//   - [x] Step 2: gRPC server bound to :8080
//   - [ ] Step 3: Kafka producer publishes on each accepted RPC
//   - [ ] Step 4: Redis token-bucket rate limiter
//
// Run: go run ./cmd/api

type relayServer struct {
	pb.UnimplementedRelayServer
}

func (s *relayServer) SubmitJob(ctx context.Context, req *pb.SubmitJobRequest) (*pb.SubmitJobResponse, error) {
	jobID := strconv.FormatInt(time.Now().UnixNano(), 10)
	log.Printf("submit: client=%s job=%s", req.GetClientId(), jobID)
	return &pb.SubmitJobResponse{JobId: jobID, Accepted: true}, nil
}

func main() {
	lis, err := net.Listen("tcp", ":8080")
	if err != nil {
		log.Fatal(err)
	}

	s := grpc.NewServer()
	pb.RegisterRelayServer(s, &relayServer{})

	reflection.Register(s)
	log.Println("listening on :8080")
	log.Fatal(s.Serve(lis))
}
