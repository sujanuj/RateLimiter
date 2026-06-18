"""
algorithms/sliding_window_log.py — Sliding Window Log
======================================================

WHY THIS FILE EXISTS:
  Fixes the boundary burst problem in Fixed Window Counter. Instead of one
  counter per time bucket, it stores the timestamp of every individual request
  in a Redis sorted set, then counts only those within the last N seconds.

HOW A REDIS SORTED SET WORKS (you need this to understand the algorithm):
  A sorted set (ZSET) stores unique members, each with a numeric "score".
  Members are automatically kept sorted by score. You can efficiently query
  all members within a score range — which we use for time ranges.

  We store:  member = request_id (unique string)
             score  = Unix timestamp of the request (float, microseconds)

  This lets us ask: "how many requests happened between time X and time Y?"
  in a single Redis command: ZCOUNT key X Y

HOW THE ALGORITHM WORKS (plain English):
  On every request:
    1. Remove all entries with timestamp < (now - window_seconds)
       These are outside the sliding window — stale, don't count them.
    2. Count what remains — that's the real request count for this window.
    3. If count >= limit → reject (don't add this request's timestamp).
    4. If count < limit  → add current timestamp, allow.

  Example with limit=3, window=60s:

    t=0:   requests=[0]         count=1  allowed ✓
    t=10:  requests=[0,10]      count=2  allowed ✓
    t=20:  requests=[0,10,20]   count=3  allowed ✓
    t=30:  requests=[0,10,20]   count=3  rejected ✗  (still 3 in window)
    t=61:  requests=[10,20]     count=2  allowed ✓   (t=0 expired, now < 60s ago)
    t=62:  requests=[10,20,62]  count=3  allowed ✓   (still room)

  No boundary burst: the window always looks back exactly N seconds from NOW,
  regardless of what clock boundary the request falls on.

THE TRADE-OFF vs Fixed Window:
  Fixed Window:        O(1) space — one counter per client
  Sliding Window Log:  O(N) space — one Redis entry per request per client

  For a limit of 1000 req/min with 10,000 clients, Sliding Window needs to
  store up to 10 million entries. For most APIs this is fine (Redis handles
  millions of small entries easily), but it's the trade-off to know.

THE LUA SCRIPT:
  Four Redis operations need to happen atomically:
    ZREMRANGEBYSCORE  — remove stale timestamps
    ZCARD             — count remaining entries
    ZADD              — add this request's timestamp (if allowed)
    EXPIRE            — set TTL so Redis cleans up inactive clients

  Without Lua: four round-trips, and another process could add entries
  between our ZCARD and our decision. With Lua: atomic, single round-trip.

REDIS KEY SCHEMA:
  ratelimit:sliding:{identifier}

  Unlike Fixed Window, there's no bucket suffix — the sorted set IS the
  log of all recent requests, cleaned continuously by the Lua script.
"""

import time
import uuid
import redis.asyncio as aioredis

from app.models.rate_limit import RateLimitResult


# ---------------------------------------------------------------------------
# The Lua Script
# ---------------------------------------------------------------------------
# Runs atomically inside Redis. All four operations happen as one unit.
#
# KEYS[1]  = the sorted set key   (e.g. "ratelimit:sliding:user:42")
# ARGV[1]  = window_start         (Unix timestamp: now - window_seconds)
# ARGV[2]  = now                  (current Unix timestamp, used as score)
# ARGV[3]  = limit                (max requests allowed)
# ARGV[4]  = window_seconds       (TTL for the key)
# ARGV[5]  = unique member ID     (prevents duplicate members in the ZSET)
#
# Returns: {current_count_after_cleanup, was_allowed}
#   was_allowed = 1 if request was added, 0 if rejected
# ---------------------------------------------------------------------------
_SLIDING_WINDOW_LUA = """
local key          = KEYS[1]
local window_start = tonumber(ARGV[1])
local now          = tonumber(ARGV[2])
local limit        = tonumber(ARGV[3])
local window_secs  = tonumber(ARGV[4])
local member       = ARGV[5]

-- Step 1: Remove all entries older than the window start
-- ZREMRANGEBYSCORE removes members with score between -inf and window_start
redis.call('ZREMRANGEBYSCORE', key, '-inf', window_start)

-- Step 2: Count how many requests are in the current window
local count = redis.call('ZCARD', key)

-- Step 3: Decide whether to allow this request
local allowed = 0
if count < limit then
    -- Add this request's timestamp as a new entry
    -- Score = now (for time-based range queries)
    -- Member = unique ID (ZSET requires unique members; two requests at the
    --          exact same microsecond would otherwise overwrite each other)
    redis.call('ZADD', key, now, member)
    allowed = 1
    count = count + 1
end

-- Step 4: Reset TTL so the key expires if this client goes quiet
-- Without this, a client who stops sending requests leaves a key in Redis forever
redis.call('EXPIRE', key, window_secs)

return {count, allowed}
"""


class SlidingWindowLog:
    """
    Distributed Sliding Window Log backed by a Redis sorted set.

    Eliminates the boundary burst problem of Fixed Window Counter at the
    cost of O(N) space (one Redis entry per request in the window).
    """

    ALGORITHM = "sliding_window_log"

    def __init__(self, redis: aioredis.Redis) -> None:
        self._redis = redis
        self._script = self._redis.register_script(_SLIDING_WINDOW_LUA)

    def _make_key(self, identifier: str) -> str:
        """
        Unlike Fixed Window, there's no time bucket in the key.
        The sorted set itself IS the sliding log — it holds all recent
        timestamps and cleans itself via ZREMRANGEBYSCORE on each request.
        """
        return f"ratelimit:sliding:{identifier}"

    async def is_allowed(
        self,
        identifier: str,
        limit: int,
        window_seconds: int,
    ) -> RateLimitResult:
        """
        Check whether this request is within the sliding window rate limit.

        Args:
            identifier:     Unique string for this client
            limit:          Max requests in any rolling window_seconds period
            window_seconds: How long the sliding window is

        Returns:
            RateLimitResult — same shape as Fixed Window for easy comparison
        """
        key = self._make_key(identifier)
        now = time.time()
        window_start = now - window_seconds

        # Unique member ID — prevents two simultaneous requests from
        # overwriting each other in the sorted set (ZSET needs unique members)
        member = str(uuid.uuid4())

        count, allowed_int = await self._script(
            keys=[key],
            args=[window_start, now, limit, window_seconds, member],
        )
        count = int(count)
        allowed = bool(int(allowed_int))

        return RateLimitResult(
            allowed=allowed,
            limit=limit,
            remaining=max(0, limit - count),
            reset_after_seconds=window_seconds,  # worst case: full window
            algorithm=self.ALGORITHM,
            key=key,
        )
