package ratelimit

import (
	"context"
	_ "embed"
	"time"

	"github.com/redis/go-redis/v9"
)

//go:embed token_bucket.lua
var tokenBucketScript string

// TokenBucket is a Redis-backed per-key token bucket.
//
// Each client has a bucket holding up to `capacity` tokens. Tokens refill
// at `refillRate` per second. Each request consumes 1 token. Empty bucket
// → request denied.
//
// Why Redis (not in-memory): so multiple rate-limiter instances share state.
// In a real fleet, instance A and instance B both look at the same bucket.
type TokenBucket struct {
	client     *redis.Client
	script     *redis.Script
	capacity   int
	refillRate float64 // tokens per millisecond (Lua expects this)
}

// NewTokenBucket: capacity is burst size; refillPerSecond is steady-state rate.
//
// Example: capacity=5, refillPerSecond=1 → up to 5 in a quick burst,
// then 1 per second after that.
func NewTokenBucket(client *redis.Client, capacity int, refillPerSecond float64) *TokenBucket {
	return &TokenBucket{
		client:     client,
		script:     redis.NewScript(tokenBucketScript),
		capacity:   capacity,
		refillRate: refillPerSecond / 1000.0, // convert to per-ms for Lua
	}
}

// Allow checks if clientID has a token available; consumes one if so.
// Returns (allowed, error). Error means Redis is unreachable —
// caller decides whether to fail open or fail closed.
func (tb *TokenBucket) Allow(ctx context.Context, clientID string) (bool, error) {
	key := "relay:bucket:" + clientID
	now := time.Now().UnixMilli()

	result, err := tb.script.Run(
		ctx,
		tb.client,
		[]string{key},
		tb.capacity,
		tb.refillRate,
		now,
	).Int()
	if err != nil {
		return false, err
	}
	return result == 1, nil
}
