"""
redis_client.py — Shared Redis Connection Manager
==================================================

WHY THIS FILE EXISTS:
  Opening a new network connection to Redis on every request is expensive —
  slow and wastes sockets. Instead, we create ONE connection when the server
  starts and reuse it for every request. This is called a "singleton" pattern.

WHY ASYNC:
  FastAPI is an async framework. A blocking (synchronous) Redis client would
  freeze the entire server while waiting for Redis to respond. The async
  client lets FastAPI keep serving other requests in the meantime.

HOW IT CONNECTS TO EVERYTHING:
  main.py calls get_redis() at startup to verify the connection is alive.
  Each algorithm file (fixed_window.py, etc.) receives this client as a
  parameter — so the entire app shares one connection.
"""

import os
import redis.asyncio as aioredis
from typing import Optional
from dotenv import load_dotenv

load_dotenv()  # reads values from .env into os.environ

# Module-level variable — None until the first call to get_redis()
_redis_client: Optional[aioredis.Redis] = None


async def get_redis() -> aioredis.Redis:
    """
    Return the shared async Redis client, creating it on first call.

    This is "lazy initialization" — we don't connect at import time
    (Redis might not be running yet), only when first needed.
    """
    global _redis_client
    if _redis_client is None:
        _redis_client = aioredis.Redis(
            host=os.getenv("REDIS_HOST", "localhost"),
            port=int(os.getenv("REDIS_PORT", 6379)),
            db=int(os.getenv("REDIS_DB", 0)),
            password=os.getenv("REDIS_PASSWORD") or None,
            decode_responses=True,  # return Python strings instead of raw bytes
        )
    return _redis_client


async def close_redis() -> None:
    """Clean up the connection when the server shuts down."""
    global _redis_client
    if _redis_client:
        await _redis_client.aclose()
        _redis_client = None
