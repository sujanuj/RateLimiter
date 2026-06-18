"""
models/rate_limit.py — Response Shape Definitions
==================================================

WHY THIS FILE EXISTS:
  When our API responds to a rate-limit check, it needs to return consistent,
  well-typed data. Pydantic models enforce that shape at runtime AND generate
  the interactive API docs (at /docs) automatically.

HOW IT CONNECTS TO EVERYTHING:
  fixed_window.py constructs and returns a RateLimitResult.
  main.py declares `response_model=RateLimitResult` so FastAPI validates and
  serializes it automatically — no manual dict-building needed.

WHY THESE FIELDS:
  These map directly to the standard HTTP rate-limit headers that every major
  API (GitHub, Stripe, Twitter) uses:
    allowed             → whether to let this request through
    limit               → X-RateLimit-Limit
    remaining           → X-RateLimit-Remaining
    reset_after_seconds → X-RateLimit-Reset (seconds until the window resets)
    algorithm           → helpful when comparing multiple algorithms
    key                 → the actual Redis key, useful for debugging
"""

from pydantic import BaseModel


class RateLimitResult(BaseModel):
    allowed: bool
    limit: int
    remaining: int
    reset_after_seconds: int
    algorithm: str
    key: str
