"""
main.py — FastAPI Application Entry Point
=========================================

WHY THIS FILE EXISTS:
  This is the HTTP layer. It translates between HTTP concepts (URLs, status
  codes, query params) and our algorithm layer (FixedWindowCounter, results).

  It deliberately contains NO algorithm logic. If you want to understand how
  counting works, see fixed_window.py. This file only decides:
    - What URL to listen on
    - What parameters to accept
    - What HTTP status code to return

HOW FASTAPI LIFESPAN WORKS:
  The lifespan function runs startup code (before yield) when the server
  starts, and shutdown code (after yield) when it stops. We use it to verify
  Redis is reachable at startup and close the connection cleanly on exit.

THE THREE ENDPOINTS:
  POST /rate-limit/fixed-window        → check AND increment (the real gate)
  GET  /rate-limit/fixed-window/status → read-only peek (for monitoring)
  DELETE /rate-limit/reset/{id}        → admin reset (useful for testing)
  GET  /health                         → confirms server + Redis are alive
"""

import os
import time
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Query
from dotenv import load_dotenv

from app.redis_client import get_redis, close_redis
from app.algorithms.fixed_window import FixedWindowCounter
from app.models.rate_limit import RateLimitResult

load_dotenv()


@asynccontextmanager
async def lifespan(app: FastAPI):
    # ── Startup ──────────────────────────────────────────────────────────
    redis = await get_redis()
    await redis.ping()          # raises immediately if Redis isn't running
    print("✅ Redis connected")
    yield
    # ── Shutdown ─────────────────────────────────────────────────────────
    await close_redis()
    print("Redis connection closed")


app = FastAPI(
    title="Distributed Rate Limiter",
    description=(
        "Portfolio project demonstrating distributed rate limiting with Redis. "
        "Phase 1: Fixed Window Counter. Phase 2: Sliding Window Log (next). "
        "Phase 3: Token Bucket (planned)."
    ),
    version="0.1.0",
    lifespan=lifespan,
)

# Read defaults from .env so we're not hardcoding numbers in the routes
_DEFAULT_LIMIT  = int(os.getenv("DEFAULT_LIMIT", 10))
_DEFAULT_WINDOW = int(os.getenv("DEFAULT_WINDOW_SECONDS", 60))


# ---------------------------------------------------------------------------
# Health check
# ---------------------------------------------------------------------------

@app.get("/health", tags=["System"])
async def health():
    """Confirms both the FastAPI server and Redis are reachable."""
    redis = await get_redis()
    await redis.ping()
    return {"status": "ok", "redis": "connected"}


# ---------------------------------------------------------------------------
# Fixed Window — main gate (increments the counter)
# ---------------------------------------------------------------------------

@app.post(
    "/rate-limit/fixed-window",
    response_model=RateLimitResult,
    tags=["Fixed Window"],
    summary="Check rate limit and increment counter",
)
async def check_fixed_window(
    identifier: str = Query(..., description="Client ID — IP address, user ID, or API key"),
    limit: int      = Query(_DEFAULT_LIMIT,  ge=1, description="Max requests per window"),
    window_seconds: int = Query(_DEFAULT_WINDOW, ge=1, description="Window duration in seconds"),
):
    """
    The main gate — call this for every request you want to rate-limit.

    Returns 200 + result if allowed.
    Returns 429 (Too Many Requests) with details if the limit is exceeded.

    In production, this logic typically lives in middleware that runs before
    your business logic routes, so you never need to call it explicitly.
    """
    redis   = await get_redis()
    counter = FixedWindowCounter(redis)
    result  = await counter.is_allowed(identifier, limit=limit, window_seconds=window_seconds)

    if not result.allowed:
        raise HTTPException(
            status_code=429,   # 429 = Too Many Requests (the standard for rate limiting)
            detail={
                "error": "Rate limit exceeded",
                "limit": result.limit,
                "remaining": result.remaining,
                "reset_after_seconds": result.reset_after_seconds,
                "algorithm": result.algorithm,
            },
        )

    return result


# ---------------------------------------------------------------------------
# Fixed Window — read-only status (does NOT increment the counter)
# ---------------------------------------------------------------------------

@app.get(
    "/rate-limit/fixed-window/status",
    response_model=RateLimitResult,
    tags=["Fixed Window"],
    summary="Read current counter state without incrementing",
)
async def fixed_window_status(
    identifier: str = Query(..., description="Client ID to inspect"),
    limit: int      = Query(_DEFAULT_LIMIT,  ge=1),
    window_seconds: int = Query(_DEFAULT_WINDOW, ge=1),
):
    """
    Read-only peek at the current counter for an identifier.
    Does NOT count this request against the limit — safe for monitoring.
    """
    redis = await get_redis()

    # Reconstruct the key exactly the same way fixed_window.py does
    bucket = int(time.time()) // window_seconds
    key    = f"ratelimit:fixed:{identifier}:{bucket}"

    raw = await redis.get(key)
    ttl = await redis.ttl(key)
    current = int(raw) if raw else 0

    return RateLimitResult(
        allowed=current < limit,
        limit=limit,
        remaining=max(0, limit - current),
        reset_after_seconds=ttl if ttl > 0 else window_seconds,
        algorithm="fixed_window",
        key=key,
    )


# ---------------------------------------------------------------------------
# Admin — reset all rate-limit keys for an identifier
# ---------------------------------------------------------------------------

@app.delete(
    "/rate-limit/reset/{identifier}",
    tags=["Admin"],
    summary="Clear all rate limit keys for an identifier",
)
async def reset_identifier(identifier: str):
    """
    Deletes all Redis keys for this identifier across all algorithms.
    Useful in tests to get a clean slate, or as an admin override in production.

    Note: redis.keys() scans the whole keyspace — fine for dev/testing.
    In production with millions of keys, use cursor-based SCAN instead.
    """
    redis   = await get_redis()
    pattern = f"ratelimit:*:{identifier}:*"
    keys    = await redis.keys(pattern)
    if keys:
        await redis.delete(*keys)
    return {"deleted_keys": len(keys), "identifier": identifier}
