"""
algorithms/token_bucket.py — Token Bucket
==========================================

WHY THIS FILE EXISTS:
  The Token Bucket algorithm is the most widely used rate limiting algorithm
  in production systems. AWS API Gateway, Stripe, Nginx, and most major APIs
  use it because it allows controlled bursting while still enforcing a
  sustained rate limit.

  Fixed Window and Sliding Window answer: "how many requests in a time period?"
  Token Bucket answers: "does this client have accumulated credit?"

HOW IT WORKS (plain English):
  Every client has a "bucket" that holds tokens up to a maximum capacity.
  Tokens refill at a fixed rate (e.g. 2 per second). Each request costs
  one token. If the bucket is empty, the request is rejected.

  Key insight: a client who hasn't made requests for a while accumulates
  tokens (up to capacity). They can spend those tokens in a burst when
  they need to. But sustained traffic above the refill rate gets throttled.

  Example — capacity=10, refill_rate=2/sec:
    t=0s:   Client makes 10 requests rapidly → bucket drains to 0 → 11th rejected
    t=3s:   6 tokens have refilled (3s × 2/s) → client can make 6 more requests
    t=5s:   10 tokens refilled → bucket full again (capped at capacity)

  Compare to Fixed Window (limit=10, window=60s):
    A burst of 10 is allowed, then nothing for up to 60s.
    Token Bucket: a burst of 10, then 2 more allowed every second.
    Much smoother, much fairer for the client.

THE LAZY REFILL PATTERN:
  You can't run a background process in Redis that continuously adds tokens.
  Instead, we calculate how many tokens SHOULD have accumulated since the
  last request, add them all at once when the next request arrives.

  This is called "lazy refill" or "virtual token bucket":
    elapsed      = now - last_refill_time
    tokens_earned = elapsed × refill_rate
    tokens        = min(capacity, stored_tokens + tokens_earned)

  We only store two values per client in Redis:
    tokens_remaining  (float — fractional tokens are valid)
    last_refill_time  (float — Unix timestamp with microsecond precision)

THE LUA SCRIPT:
  Five operations must happen atomically:
    1. Read current tokens and last refill time (HMGET)
    2. Calculate elapsed time and refill tokens
    3. Cap tokens at capacity
    4. Decide allow/reject, subtract 1 token if allowed
    5. Write back new state + set TTL (HSET + EXPIRE)

  Without atomicity: two concurrent requests could both read "1 token left",
  both decide to allow, both subtract 1 — ending at -1 tokens.

REDIS DATA STRUCTURE:
  We use a Redis Hash (HMAP) to store two fields per client:
    ratelimit:token:{identifier}  →  { tokens: "9.5", last: "1700000000.123" }

  Hash vs two separate keys: atomic read/write of both fields in one command,
  and a single TTL covers both fields automatically.

PARAMETERS:
  capacity      — maximum tokens the bucket can hold (burst ceiling)
  refill_rate   — tokens added per second (sustained rate)

  Relationship: if capacity=10 and refill_rate=2, a client can burst up to
  10 requests instantly, but their sustained rate is capped at 2/second.
"""

import time
import redis.asyncio as aioredis

from app.models.rate_limit import RateLimitResult


# ---------------------------------------------------------------------------
# The Lua Script
# ---------------------------------------------------------------------------
# KEYS[1]  = the Redis hash key   (e.g. "ratelimit:token:user:42")
# ARGV[1]  = now                  (current Unix timestamp, float as string)
# ARGV[2]  = capacity             (max tokens)
# ARGV[3]  = refill_rate          (tokens per second)
# ARGV[4]  = ttl_seconds          (key expiry — set to capacity/refill_rate
#                                  so key auto-cleans after client goes quiet)
#
# Returns: {tokens_after (string), allowed (0 or 1)}
# ---------------------------------------------------------------------------
_TOKEN_BUCKET_LUA = """
local key         = KEYS[1]
local now         = tonumber(ARGV[1])
local capacity    = tonumber(ARGV[2])
local refill_rate = tonumber(ARGV[3])
local ttl         = tonumber(ARGV[4])

-- Read current state from the hash
-- HMGET returns a list: {tokens_value_or_false, last_time_value_or_false}
local data   = redis.call('HMGET', key, 'tokens', 'last')
local tokens = tonumber(data[1])
local last   = tonumber(data[2])

if tokens == nil then
    -- First request from this client — start with a full bucket
    tokens = capacity
    last   = now
end

-- Calculate how many tokens have accumulated since the last request
local elapsed      = now - last
local tokens_earned = elapsed * refill_rate

-- Add earned tokens, but cap at capacity (bucket can't overflow)
tokens = math.min(capacity, tokens + tokens_earned)

-- Decide whether to allow this request
local allowed = 0
if tokens >= 1 then
    tokens  = tokens - 1
    allowed = 1
end

-- Write back updated state and reset TTL
-- We always update 'last' to now, even if rejected —
-- so the next request still calculates elapsed correctly
redis.call('HSET', key, 'tokens', tostring(tokens), 'last', tostring(now))
redis.call('EXPIRE', key, ttl)

return {tostring(tokens), allowed}
"""


class TokenBucket:
    """
    Distributed Token Bucket backed by a Redis hash.

    Allows bursting up to `capacity` requests, with sustained traffic
    limited to `refill_rate` requests per second. Used in production by
    AWS API Gateway, Stripe, Nginx, and most major API platforms.
    """

    ALGORITHM = "token_bucket"

    def __init__(self, redis: aioredis.Redis) -> None:
        self._redis = redis
        self._script = self._redis.register_script(_TOKEN_BUCKET_LUA)

    def _make_key(self, identifier: str) -> str:
        return f"ratelimit:token:{identifier}"

    async def is_allowed(
        self,
        identifier: str,
        capacity: int,
        refill_rate: float,
    ) -> RateLimitResult:
        """
        Check whether this request can consume a token.

        Args:
            identifier:   Unique string for this client
            capacity:     Max tokens the bucket can hold (burst ceiling)
            refill_rate:  Tokens added per second (sustained rate limit)

        Returns:
            RateLimitResult with token counts mapped to the standard fields.

        Note on parameter mapping to RateLimitResult:
            limit     = capacity        (the ceiling)
            remaining = tokens left after this request
            reset_after_seconds = time until 1 token refills (if rejected)
                                  or 0 (if allowed)
        """
        key = self._make_key(identifier)
        now = time.time()

        # TTL: how long until a full bucket would refill from empty.
        # After this much idle time, the client is back to full — safe to
        # delete the key and let the next request start fresh.
        ttl = int(capacity / refill_rate) + 10

        tokens_str, allowed_int = await self._script(
            keys=[key],
            args=[now, capacity, refill_rate, ttl],
        )

        tokens_remaining = float(tokens_str)
        allowed = bool(int(allowed_int))

        # If rejected, tell the client how long until 1 token refills
        if not allowed:
            seconds_until_token = 1.0 / refill_rate
        else:
            seconds_until_token = 0

        return RateLimitResult(
            allowed=allowed,
            limit=capacity,
            remaining=int(tokens_remaining),
            reset_after_seconds=int(seconds_until_token) + 1 if not allowed else 0,
            algorithm=self.ALGORITHM,
            key=key,
        )
