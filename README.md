# relay

A small two-service Go stack demonstrating **gRPC, Kafka, Redis, Docker, and Kubernetes** in one repo.

```
[Client] --gRPC--> [api] --produce--> [Kafka] --consume--> [worker]
                     |
                     +--rate limit--> [Redis]
```

## Services

- **`cmd/api`** — gRPC server on `:8080`. Exposes `SubmitJob`. Per-client token-bucket rate limit (Redis + atomic Lua script). Accepted jobs are published to Kafka topic `relay.jobs` with `client_id` as the partition key (preserves per-client ordering).
- **`cmd/worker`** — Kafka consumer in a consumer group. Pulls from `relay.jobs` and "processes" each message (logs it).

## Run locally with docker-compose

```bash
docker compose up -d            # brings up Kafka + Redis
go run ./cmd/api                # in terminal 1
go run ./cmd/worker             # in terminal 2

grpcurl -plaintext \
  -d '{"client_id":"alice","payload":"hi"}' \
  localhost:8080 relay.Relay/SubmitJob
```

## Run in Kubernetes (kind)

```bash
kind create cluster --name relay
kubectl config use-context kind-relay

# Build images and load into the kind cluster
docker build -f docker/Dockerfile.api    -t relay-api:local    .
docker build -f docker/Dockerfile.worker -t relay-worker:local .
kind load docker-image relay-api:local    --name relay
kind load docker-image relay-worker:local --name relay

# Apply manifests
kubectl apply -f k8s/

# Port-forward and test
kubectl port-forward -n relay svc/relay-api 8080:8080
grpcurl -plaintext -d '{"client_id":"alice","payload":"k8s"}' \
  localhost:8080 relay.Relay/SubmitJob

kubectl logs -n relay deployment/relay-worker
```

## Layout

| Topic | File |
|---|---|
| gRPC contract | `proto/relay.proto` |
| Multi-stage Docker build | `docker/Dockerfile.api`, `docker/Dockerfile.worker` |
| Token-bucket rate limit (Lua) | `internal/ratelimit/token_bucket.lua`, `internal/ratelimit/token.go` |
| Kafka producer wrapper | `internal/kafka/client.go` |
| K8s namespace / config / secret | `k8s/00-namespace.yaml`, `k8s/01-configmap.yaml`, `k8s/02-secret.yaml` |
| Redis Deployment + Service | `k8s/10-redis.yaml` |
| Kafka StatefulSet + headless Service | `k8s/20-kafka.yaml` |
| API Deployment + Service + probes | `k8s/30-api.yaml` |
| Worker Deployment | `k8s/40-worker.yaml` |

## Notes

- Single-broker Kafka in KRaft mode (no Zookeeper).
- Topic `relay.jobs` is auto-created on first publish for the local/kind setup. Production would pre-create with explicit partition count and replication factor (e.g. via Terraform or a topic operator).
- Rate limiter fails open on Redis errors and logs — availability over strictness for this scenario.
- Secrets are base64 (not encrypted) — this is intentionally a demo. Production would use sealed-secrets, External Secrets Operator, or Vault.
