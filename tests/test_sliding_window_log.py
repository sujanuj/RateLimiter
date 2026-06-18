"""
tests/test_sliding_window_log.py — Sliding Window Log Tests
============================================================

These tests cover everything Fixed Window tests do PLUS one critical
additional test: proving there's no boundary burst. That test is the
key differentiator — it's what proves Sliding Window actually solves
the problem Fixed Window has.
"""

import asyncio
import time
import pytest
import pytest_asyncio

from app.redis_client import get_redis, close_redis
from app.algorithms.sliding_window_log import SlidingWindowLog

TEST_ID = f"sliding-test-{int(time.time())}"


@pytest_asyncio.fixture(scope="function")
async def redis():
    client = await get_redis()
    yield client
    await close_redis()


@pytest_asyncio.fixture(autouse=True)
async def clean_keys(redis):
    """Wipe all test keys before each test."""
    for suffix in ["", "-A", "-B", "-burst"]:
        keys = await redis.keys(f"ratelimit:sliding:{TEST_ID}{suffix}")
        if keys:
            await redis.delete(*keys)
    yield


# ---------------------------------------------------------------------------
# Basic correctness — same guarantees as Fixed Window
# ---------------------------------------------------------------------------

async def test_first_request_is_allowed(redis):
    log = SlidingWindowLog(redis)
    result = await log.is_allowed(TEST_ID, limit=5, window_seconds=60)

    assert result.allowed is True
    assert result.remaining == 4
    assert result.limit == 5
    assert result.algorithm == "sliding_window_log"


async def test_requests_within_limit_are_all_allowed(redis):
    log = SlidingWindowLog(redis)
    for i in range(5):
        result = await log.is_allowed(TEST_ID, limit=5, window_seconds=60)
        assert result.allowed is True, f"Request {i+1} should be allowed"


async def test_request_exceeding_limit_is_rejected(redis):
    log = SlidingWindowLog(redis)
    for _ in range(5):
        await log.is_allowed(TEST_ID, limit=5, window_seconds=60)

    result = await log.is_allowed(TEST_ID, limit=5, window_seconds=60)
    assert result.allowed is False
    assert result.remaining == 0


async def test_remaining_never_goes_below_zero(redis):
    log = SlidingWindowLog(redis)
    for _ in range(20):
        result = await log.is_allowed(TEST_ID, limit=3, window_seconds=60)
    assert result.remaining == 0


async def test_different_identifiers_are_independent(redis):
    log = SlidingWindowLog(redis)
    id_a = f"{TEST_ID}-A"
    id_b = f"{TEST_ID}-B"

    for _ in range(3):
        await log.is_allowed(id_a, limit=3, window_seconds=60)

    a_result = await log.is_allowed(id_a, limit=3, window_seconds=60)
    assert a_result.allowed is False

    b_result = await log.is_allowed(id_b, limit=3, window_seconds=60)
    assert b_result.allowed is True


# ---------------------------------------------------------------------------
# The key test: no boundary burst
# ---------------------------------------------------------------------------

async def test_no_boundary_burst(redis):
    """
    This is the test Fixed Window CANNOT pass.

    Fixed Window allows 2× the limit at window boundaries because each
    window resets its counter independently. Sliding Window Log tracks
    the actual rolling window, so the limit is enforced continuously.

    We simulate a boundary burst:
      1. Make (limit) requests that are "old" — just inside the window
      2. Immediately make more requests "now"
      3. The sliding window should see all of them and reject the extras

    We do this by manually inserting old timestamps into Redis
    (simulating requests that happened 55 seconds ago in a 60s window),
    then making new requests. If the total exceeds the limit, they should
    be rejected — proving no burst is possible.
    """
    log = SlidingWindowLog(redis)
    burst_id = f"{TEST_ID}-burst"
    key = f"ratelimit:sliding:{burst_id}"
    limit = 5
    window_seconds = 60

    # Manually insert 5 "old" timestamps — 55 seconds ago, still inside window
    now = time.time()
    old_time = now - 55  # 55s ago, within a 60s window
    for i in range(limit):
        await redis.zadd(key, {f"old-req-{i}": old_time + i * 0.001})
    await redis.expire(key, window_seconds)

    # Now try to make new requests — the window already has 5 entries (the limit)
    result = await log.is_allowed(burst_id, limit=limit, window_seconds=window_seconds)

    # With Fixed Window this might be allowed (new bucket = fresh counter).
    # With Sliding Window Log it must be rejected — 5 requests are still in window.
    assert result.allowed is False, (
        "Sliding Window must reject this — the rolling window already has "
        f"{limit} requests. Fixed Window would allow it (new bucket), "
        "but that's the boundary burst bug we're fixing."
    )


async def test_result_key_contains_identifier(redis):
    log = SlidingWindowLog(redis)
    result = await log.is_allowed(TEST_ID, limit=5, window_seconds=60)
    assert TEST_ID in result.key
    assert result.key.startswith("ratelimit:sliding:")
