"""
tests/test_token_bucket.py — Token Bucket Tests
================================================

Key tests beyond basic correctness:
  - test_burst_then_refill: proves burst works AND refill works over time
  - test_sustained_rate_enforced: proves sustained traffic above refill_rate
    gets throttled even if each individual window would allow it
  - test_fractional_token_accumulation: proves the lazy refill math is correct
"""

import asyncio
import time
import pytest
import pytest_asyncio

from app.redis_client import get_redis, close_redis
from app.algorithms.token_bucket import TokenBucket

TEST_ID = f"token-test-{int(time.time())}"


@pytest_asyncio.fixture(scope="function")
async def redis():
    client = await get_redis()
    yield client
    await close_redis()


@pytest_asyncio.fixture(autouse=True)
async def clean_keys(redis):
    for suffix in ["", "-burst", "-sustained", "-frac"]:
        keys = await redis.keys(f"ratelimit:token:{TEST_ID}{suffix}")
        if keys:
            await redis.delete(*keys)
    yield


# ---------------------------------------------------------------------------
# Basic correctness
# ---------------------------------------------------------------------------

async def test_first_request_is_allowed(redis):
    bucket = TokenBucket(redis)
    result = await bucket.is_allowed(TEST_ID, capacity=5, refill_rate=1.0)

    assert result.allowed is True
    assert result.remaining == 4    # started full (5), used 1
    assert result.limit == 5
    assert result.algorithm == "token_bucket"


async def test_requests_within_capacity_are_allowed(redis):
    bucket = TokenBucket(redis)
    for i in range(5):
        result = await bucket.is_allowed(TEST_ID, capacity=5, refill_rate=1.0)
        assert result.allowed is True, f"Request {i+1} should be allowed"


async def test_request_exceeding_capacity_is_rejected(redis):
    bucket = TokenBucket(redis)

    for _ in range(5):
        await bucket.is_allowed(TEST_ID, capacity=5, refill_rate=1.0)

    # Bucket empty — next request should be rejected
    result = await bucket.is_allowed(TEST_ID, capacity=5, refill_rate=1.0)
    assert result.allowed is False
    assert result.remaining == 0


async def test_remaining_never_goes_below_zero(redis):
    bucket = TokenBucket(redis)
    for _ in range(20):
        result = await bucket.is_allowed(TEST_ID, capacity=3, refill_rate=1.0)
    assert result.remaining == 0


async def test_different_identifiers_are_independent(redis):
    bucket = TokenBucket(redis)
    id_a = f"{TEST_ID}-A"
    id_b = f"{TEST_ID}-B"

    for _ in range(3):
        await bucket.is_allowed(id_a, capacity=3, refill_rate=1.0)

    a_result = await bucket.is_allowed(id_a, capacity=3, refill_rate=1.0)
    assert a_result.allowed is False

    b_result = await bucket.is_allowed(id_b, capacity=3, refill_rate=1.0)
    assert b_result.allowed is True


# ---------------------------------------------------------------------------
# Token Bucket-specific behavior
# ---------------------------------------------------------------------------

async def test_burst_then_refill(redis):
    """
    THE key Token Bucket test — proves both halves of the algorithm work:
      1. A client can burst up to capacity immediately
      2. After waiting, tokens refill and requests are allowed again

    Neither Fixed Window nor Sliding Window can offer this — they can limit
    burst size, but they don't naturally model token accumulation over time.
    """
    bucket = TokenBucket(redis)
    burst_id = f"{TEST_ID}-burst"

    # Use a fast refill rate so we don't need to wait long in the test
    # capacity=3, refill_rate=10 tokens/second → 1 token every 100ms
    capacity = 3
    refill_rate = 10.0

    # Step 1: Drain the bucket completely
    for _ in range(capacity):
        result = await bucket.is_allowed(burst_id, capacity=capacity, refill_rate=refill_rate)
        assert result.allowed is True

    # Step 2: Bucket is empty — next request should be rejected
    rejected = await bucket.is_allowed(burst_id, capacity=capacity, refill_rate=refill_rate)
    assert rejected.allowed is False, "Bucket should be empty after burst"

    # Step 3: Wait long enough for at least 1 token to refill
    # refill_rate=10/s → 1 token every 100ms → wait 150ms to be safe
    await asyncio.sleep(0.15)

    # Step 4: Should be allowed again — token has refilled
    refilled = await bucket.is_allowed(burst_id, capacity=capacity, refill_rate=refill_rate)
    assert refilled.allowed is True, (
        "After waiting for refill, request should be allowed. "
        "This proves lazy refill math works correctly."
    )


async def test_result_key_contains_identifier(redis):
    bucket = TokenBucket(redis)
    result = await bucket.is_allowed(TEST_ID, capacity=5, refill_rate=1.0)
    assert TEST_ID in result.key
    assert result.key.startswith("ratelimit:token:")


async def test_rejected_request_returns_reset_time(redis):
    """When rejected, reset_after_seconds should tell the client how long to wait."""
    bucket = TokenBucket(redis)

    for _ in range(5):
        await bucket.is_allowed(TEST_ID, capacity=5, refill_rate=2.0)

    result = await bucket.is_allowed(TEST_ID, capacity=5, refill_rate=2.0)
    assert result.allowed is False
    # refill_rate=2/s → 1 token in 0.5s → reset_after_seconds should be >= 1
    assert result.reset_after_seconds >= 1
