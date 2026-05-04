# BUILD.md — relay step-by-step

> Sequential 7-step build. Each step has a **Goal**, **Do**, **Verify**. Don't skip Verify — it's the proof your changes work.
>
> **Hard time budget: 4 hours total.** If a step blows past 60 min, drop it and move on.

---

## ✅ Step 1 — Hello world (15 min)

**Goal:** Confirm Go module + folder structure compile and run.

**Do:**
- Folder + `go.mod` already scaffolded by Claude.
- `cmd/api/main.go` and `cmd/worker/main.go` are stub binaries that just log and wait for Ctrl-C.

**Verify:**

```bash
cd /Users/telman/Documents/relay
go run ./cmd/api
# should print: relay/api: starting (stub)…
# Ctrl-C to exit

go run ./cmd/worker
# should print: relay/worker: starting (stub)…
```

If both run, **Step 1 is done.** Commit:

```bash
git init
git add .
git commit -m "relay: initial scaffold"
```

---

## Step 2 — gRPC contract + server (45 min)

**Goal:** API exposes a `SubmitJob` RPC over gRPC.

**Do:**
1. Write `proto/relay.proto`:
   ```proto
   syntax = "proto3";
   package relay;
   option go_package = "github.com/telman03/relay/internal/pb;pb";

   service Relay {
     rpc SubmitJob(SubmitJobRequest) returns (SubmitJobResponse);
   }
   message SubmitJobRequest { string client_id = 1; string payload = 2; }
   message SubmitJobResponse { string job_id = 1; bool accepted = 2; string reason = 3; }
   ```

2. Generate Go code:
   ```bash
   go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
   go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
   protoc --go_out=. --go-grpc_out=. proto/relay.proto
   ```
   This creates `internal/pb/relay.pb.go` and `internal/pb/relay_grpc.pb.go`.

3. Implement the server in `cmd/api/main.go` — listen on `:8080`, register `RelayServer`, return a fake `job_id` for now (we'll add Kafka and rate-limit in later steps).

**Verify:** `go run ./cmd/api` boots. Use `grpcurl` (`brew install grpcurl`) to test:
```bash
grpcurl -plaintext -d '{"client_id":"alice","payload":"hi"}' localhost:8080 relay.Relay/SubmitJob
# expect: {"jobId": "...", "accepted": true}
```

---

## Step 3 — Kafka producer (30 min)

**Goal:** Each accepted `SubmitJob` publishes to topic `relay.jobs`.

**Do:**
1. Add dep: `go get github.com/segmentio/kafka-go`
2. In `internal/kafka/client.go`, write a thin wrapper around `kafka.Writer`.
3. In `cmd/api/main.go`, after rate-limit check passes, call `producer.WriteMessages(ctx, ...)` with the request payload.

**Verify:**
- Run docker-compose for Kafka (we'll add this in Step 6, or use a free Confluent Cloud cluster temporarily).
- Send a `SubmitJob` → confirm a message lands in `relay.jobs` (use `kafka-console-consumer` or any GUI).

---

## Step 4 — Redis token-bucket rate limit (30 min)

**Goal:** Per-`client_id` rate limit before publishing.

**Do:**
1. Add dep: `go get github.com/redis/go-redis/v9`
2. In `internal/ratelimit/token.go`, write `Allow(ctx, clientID string) (bool, error)`.
   - Use a Redis hash `relay:bucket:<clientID>` with `tokens` and `last_refill` fields.
   - On each call: compute refill, decrement if > 0, return result.
   - Use Lua script for atomicity (interview gold).
3. In `cmd/api/main.go`, call `ratelimit.Allow(...)` before the Kafka publish. Reject with `accepted=false, reason="rate_limited"` if denied.

**Verify:** Hammer with `for i in {1..20}; do grpcurl ...; done` and watch some get rejected.

---

## Step 5 — Worker consumer (30 min)

**Goal:** Worker consumes `relay.jobs` and "processes" each message.

**Do:**
1. In `cmd/worker/main.go`, build a `kafka.Reader` consuming `relay.jobs` with `GroupID: "relay-worker"`.
2. For each message: log it. That's "processing" for now.

**Verify:** Submit a job → see worker log it within a second.

---

## Step 6 — Docker + docker-compose (45 min)

**Goal:** Everything runs in containers locally.

**Do:**
1. Write `docker/Dockerfile.api` (multi-stage: builder + alpine runtime).
2. Write `docker/Dockerfile.worker` (same shape, different binary).
3. Write `docker-compose.yaml` with: `kafka`, `zookeeper` (or KRaft), `redis`, `api`, `worker`.

**Verify:**
```bash
docker compose up --build
# all 5 services come up green
# submit a job via grpcurl, see worker log it
```

---

## Step 7 — Kubernetes manifests (60 min)

**Goal:** Same stack runs in Docker Desktop's K8s.

**Do — write each YAML by hand (no copy-paste from internet):**

1. `k8s/configmap.yaml` — `KAFKA_BROKER`, `REDIS_ADDR`
2. `k8s/secret.yaml` — fake API key (just to demo the concept)
3. `k8s/redis-deployment.yaml` + `redis-service.yaml`
4. `k8s/kafka-statefulset.yaml` + `kafka-service.yaml` (use bitnami/kafka image, single replica is fine)
5. `k8s/api-deployment.yaml` + `api-service.yaml` (LoadBalancer or NodePort)
6. `k8s/worker-deployment.yaml`

**Verify:**
```bash
docker build -f docker/Dockerfile.api -t relay-api:local .
docker build -f docker/Dockerfile.worker -t relay-worker:local .
kubectl apply -f k8s/
kubectl get pods    # all Running
kubectl port-forward svc/relay-api 8080:8080
grpcurl -plaintext -d '{"client_id":"alice","payload":"k8s!"}' localhost:8080 relay.Relay/SubmitJob
kubectl logs -l app=relay-worker  # see the message
```

---

## Done? 🎉

Push to GitHub:
```bash
gh repo create telman03/relay --public --source=. --push
```

In the interview when they ask any K8s/gRPC/Kafka question, you can say:
> *"I built a small playground recently — two Go services, gRPC + Kafka + Redis, all in K8s with Deployments, StatefulSet, ConfigMap, Secret. Happy to walk through any piece."*

That sentence sells 5+ checklist items at once.
