-- token_bucket.lua — atomic token bucket in one Redis round-trip.
--
-- Why Lua: a "naive" Go implementation would do
--   1) HMGET tokens last_refill
--   2) compute new state in Go
--   3) HMSET back
-- Two clients hitting Redis at the same time can both see tokens=1, both
-- decrement to 0 in their own copy, and both think they were allowed.
-- That's a TOCTOU race (time-of-check vs time-of-use).
--
-- Redis runs Lua scripts atomically — no other command executes during EVAL.
-- So the read + math + write all happen as one indivisible step.
--
-- KEYS[1] = bucket key, e.g. "relay:bucket:alice"
-- ARGV[1] = capacity         (max tokens, integer)
-- ARGV[2] = refill_per_ms    (float; e.g. 5/sec → 0.005)
-- ARGV[3] = now_ms           (current unix time in milliseconds)
--
-- Returns: 1 if allowed, 0 if denied.

local key           = KEYS[1]
local capacity      = tonumber(ARGV[1])
local refill_per_ms = tonumber(ARGV[2])
local now           = tonumber(ARGV[3])

local data          = redis.call("HMGET", key, "tokens", "last_refill")
local tokens        = tonumber(data[1])
local last_refill   = tonumber(data[2])

if tokens == nil then
    -- First time we see this client → start with a full bucket.
    tokens      = capacity
    last_refill = now
end

-- Refill based on elapsed time (lazy refill — no background job needed).
local elapsed  = now - last_refill
local refilled = elapsed * refill_per_ms
tokens         = math.min(capacity, tokens + refilled)
last_refill    = now

local allowed  = 0
if tokens >= 1 then
    tokens  = tokens - 1
    allowed = 1
end

redis.call("HMSET", key, "tokens", tokens, "last_refill", last_refill)
redis.call("EXPIRE", key, 3600) -- idle buckets clean themselves up after 1h

return allowed
