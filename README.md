# relay

Tiny two-service Go playground for practicing **gRPC, Kafka, Redis, Docker, and Kubernetes** in one repo. Built as interview prep — not a real product.

```
[Client] --gRPC--> [api] --produce--> [Kafka] --consume--> [worker]
                     |
                     +--rate limit--> [Redis]
```

## Services

- **`cmd/api`** — gRPC server on `:8080`. Accepts `SubmitJob` RPCs, rate-limits per client via Redis token bucket, publishes accepted jobs to Kafka topic `relay.jobs`.
- **`cmd/worker`** — Kafka consumer. Pulls from `relay.jobs`, "processes" each message (just logs for now).

## Run locally

```bash
# Bring up Kafka + Redis with docker-compose (added in Step 6)
docker compose up -d

# In two separate terminals:
go run ./cmd/api
go run ./cmd/worker

# Send a test request (Step 2 adds a tiny client tool)
go run ./cmd/client submit "hello"
```

## Run in Kubernetes

```bash
kubectl apply -f k8s/
kubectl port-forward svc/relay-api 8080:8080
```

## What this teaches

| Topic | Where |
|---|---|
| Multi-stage Docker build | `docker/Dockerfile.api`, `docker/Dockerfile.worker` |
| K8s Deployment | `k8s/api-deployment.yaml`, `k8s/worker-deployment.yaml` |
| K8s Service | `k8s/api-service.yaml` |
| K8s StatefulSet | `k8s/kafka-statefulset.yaml` |
| ConfigMap + Secret | `k8s/configmap.yaml`, `k8s/secret.yaml` |
| gRPC unary + streaming | `proto/relay.proto`, `cmd/api/main.go` |
| Pub-sub via Kafka | `cmd/api` produces, `cmd/worker` consumes |
| Token-bucket rate limit | `internal/ratelimit/token.go` |
| Microservices | api ↔ worker, decoupled via Kafka |

See [BUILD.md](./BUILD.md) for the step-by-step guide.
