"""
algorithms/fixed_window.py — Fixed Window Counter
==================================================

WHY THIS FILE EXISTS:
  This is the rate-limiting brain. It contains:
    1. The algorithm logic (how to build a Redis key, how to count requests)
    2. The Lua script (the atomic Redis operation that prevents race conditions)
    3. A clean class so main.py never needs to know Redis key details

HOW THE ALGORITHM WORKS (plain English):
  1. Divide wall-clock time into fixed buckets using integer division.
     All timestamps in the same N-second span produce the same bucket number.

     Example with window_seconds=60:
       Unix time 1700000000 ÷ 60 = 28333333   ← bucket ID
       Unix time 1700000059 ÷ 60 = 28333333   ← SAME bucket
       Unix time 1700000060 ÷ 60 = 28333334   ← NEW bucket, fresh counter

  2. Build a Redis key encoding both the client AND the bucket:
       ratelimit:fixed:user:42:28333333

  3. Atomically increment that counter and (on first request) set a TTL.
     Redis auto-deletes the key when the TTL expires → free, automatic reset.

  4. counter > limit → reject. Otherwise → allow.

THE KNOWN TRADE-OFF — boundary burst:
  A client can get ~2× the limit in a short burst by timing requests at the
  very end of one window and the very start of the next. This is acceptable
  for many use cases, and is fixed in Phase 2 (Sliding Window Log).

  Window 1 ─────────────────┤ Window 2 ─────────────────
  ░░░░░░░░░░░ ▓▓▓▓▓▓▓▓▓▓ (10)│▓▓▓▓▓▓▓▓▓▓ (10) ░░░░░░░░
                             ↑
            20 requests in ~2 seconds if timed at the boundary

  Mentioning this trade-off in interviews signals you understand the algorithm
  deeply, not just that you can implement it.

THE LUA SCRIPT — why it's critical:
  Without Lua, you'd write two separate Python calls:
    await redis.incr(key)            # step 1
    await redis.expire(key, window)  # step 2

  Problem 1 — race condition:
    Server A does step 1. Server B does step 1. Server A does step 2.
    Server B does step 2 — resetting the TTL! Clients get extra time.

  Problem 2 — crash gap:
    Server crashes after step 1, before step 2. The key has no TTL and
    lives forever. The rate limiter never resets for that client.

  The Lua script runs INSIDE Redis, atomically. Nothing else can execute
  between INCR and EXPIRE. One round-trip, no gap, no race, no crash risk.
"""

import time
import redis.asyncio as aioredis

from app.models.rate_limit import RateLimitResult


# ---------------------------------------------------------------------------
# The Lua Script
# ---------------------------------------------------------------------------
# This code runs on the Redis server itself, not in Python. Think of it as
# a stored procedure that Redis executes atomically.
#
# KEYS[1]  = the Redis key  (e.g. "ratelimit:fixed:user:42:28333333")
# ARGV[1]  = the limit      (e.g. "10")
# ARGV[2]  = window seconds (e.g. "60")
#
# Line by line:
#   INCR key       → add 1 to counter (creates the key at 1 if it didn't exist)
#   if current==1  → this is the FIRST request this window: set the TTL now
#   TTL key        → how many seconds until this window expires
#   return {count, ttl}
#
# The `if current == 1` check ensures we only set EXPIRE on the very first
# request — never overwriting an existing TTL mid-window.
# ---------------------------------------------------------------------------
_FIXED_WINDOW_LUA = """
local key     = KEYS[1]
local limit   = tonumber(ARGV[1])
local window  = tonumber(ARGV[2])

local current = redis.call('INCR', key)

if current == 1 then
    redis.call('EXPIRE', key, window)
end

local ttl = redis.call('TTL', key)

return {current, ttl}
"""


class FixedWindowCounter:
    """
    Distributed Fixed Window Counter backed by Redis.

    The algorithm lives here; main.py just calls is_allowed() and handles
    the HTTP response. This separation makes the algorithm independently
    testable without running an HTTP server at all.
    """

    ALGORITHM = "fixed_window"

    def __init__(self, redis: aioredis.Redis) -> None:
        self._redis = redis
        # Register the script once. redis-py computes a SHA hash and uses
        # EVALSHA on subsequent calls — avoids resending the full script
        # every time, which is faster at scale.
        self._script = self._redis.register_script(_FIXED_WINDOW_LUA)

    def _make_key(self, identifier: str, window_seconds: int) -> str:
        """
        Build the Redis key for this identifier + current time window.

        Integer division groups all timestamps in the same window into
        the same bucket number automatically.
        """
        bucket = int(time.time()) // window_seconds
        return f"ratelimit:fixed:{identifier}:{bucket}"

    async def is_allowed(
        self,
        identifier: str,
        limit: int,
        window_seconds: int,
    ) -> RateLimitResult:
        """
        Check whether this request is within the rate limit.

        Args:
            identifier:     Unique string for this client (IP, user ID, API key)
            limit:          Max requests allowed per window
            window_seconds: How long each window lasts in seconds

        Returns:
            RateLimitResult — the caller (main.py) decides the HTTP status.
            This method never raises exceptions; that's the HTTP layer's job.
        """
        key = self._make_key(identifier, window_seconds)

        # Single atomic round-trip — runs the Lua script on Redis
        current, ttl = await self._script(
            keys=[key],
            args=[limit, window_seconds],
        )
        current = int(current)
        ttl = int(ttl)

        allowed = current <= limit
        remaining = max(0, limit - current)

        return RateLimitResult(
            allowed=allowed,
            limit=limit,
            remaining=remaining,
            reset_after_seconds=ttl if ttl > 0 else window_seconds,
            algorithm=self.ALGORITHM,
            key=key,
        )
