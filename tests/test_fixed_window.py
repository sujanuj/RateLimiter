"""
tests/test_fixed_window.py — Fixed Window Counter Tests
========================================================

WHY THESE TESTS EXIST:
  They verify the algorithm is correct at its boundaries:
    - exactly AT the limit (should allow)
    - just OVER the limit (should reject)
    - two separate identifiers don't interfere with each other

HOW TO RUN:
  Make sure Redis is running (docker-compose up -d), then:
    pytest tests/test_fixed_window.py -v

WHY THE CLEANUP FIXTURE:
  Tests that share Redis state can "bleed" into each other. If test A leaves
  a counter at 5, test B starts and sees 5 instead of 0 — wrong result.
  The autouse=True fixture wipes all test keys before each test automatically.

WHY ASYNC TESTS:
  Our algorithm code is async, so tests must also be async. The
  `asyncio_mode = auto` line in pytest.ini handles this without requiring
  @pytest.mark.asyncio decorators on every single test function.
"""

import time
import pytest
import pytest_asyncio

from app.redis_client import get_redis, close_redis
from app.algorithms.fixed_window import FixedWindowCounter

# Unique suffix per test run so parallel CI jobs don't collide with each other
TEST_ID = f"test-user-{int(time.time())}"


@pytest_asyncio.fixture(scope="function")
async def redis():
    """Provide a Redis client for the whole test module, close it after."""
    client = await get_redis()
    yield client
    await close_redis()


@pytest_asyncio.fixture(autouse=True)
async def clean_keys(redis):
    """Wipe all test keys before each test — guarantees isolation."""
    for suffix in ["", "-A", "-B"]:
        keys = await redis.keys(f"ratelimit:*:{TEST_ID}{suffix}:*")
        if keys:
            await redis.delete(*keys)
    yield


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

async def test_first_request_is_allowed(redis):
    counter = FixedWindowCounter(redis)
    result  = await counter.is_allowed(TEST_ID, limit=5, window_seconds=60)

    assert result.allowed is True
    assert result.remaining == 4   # used 1 of 5, so 4 remain
    assert result.limit == 5
    assert result.algorithm == "fixed_window"


async def test_requests_within_limit_are_all_allowed(redis):
    counter = FixedWindowCounter(redis)
    for i in range(5):
        result = await counter.is_allowed(TEST_ID, limit=5, window_seconds=60)
        assert result.allowed is True, f"Request {i+1} should be allowed"
        assert result.remaining == 4 - i


async def test_request_exceeding_limit_is_rejected(redis):
    counter = FixedWindowCounter(redis)

    for _ in range(5):                           # use up all 5 slots
        await counter.is_allowed(TEST_ID, limit=5, window_seconds=60)

    result = await counter.is_allowed(TEST_ID, limit=5, window_seconds=60)  # 6th
    assert result.allowed is False
    assert result.remaining == 0


async def test_remaining_never_goes_below_zero(redis):
    counter = FixedWindowCounter(redis)

    for _ in range(20):                          # far over the limit of 3
        result = await counter.is_allowed(TEST_ID, limit=3, window_seconds=60)

    assert result.remaining == 0                 # should be 0, not negative


async def test_reset_after_seconds_is_positive(redis):
    counter = FixedWindowCounter(redis)
    result  = await counter.is_allowed(TEST_ID, limit=5, window_seconds=30)

    assert 0 < result.reset_after_seconds <= 30


async def test_different_identifiers_are_independent(redis):
    """
    Exhausting the limit for identifier A must NOT affect identifier B.
    This is the core correctness guarantee of any rate limiter.
    """
    counter = FixedWindowCounter(redis)
    id_a = f"{TEST_ID}-A"
    id_b = f"{TEST_ID}-B"

    # Exhaust A's limit completely
    for _ in range(3):
        await counter.is_allowed(id_a, limit=3, window_seconds=60)

    a_result = await counter.is_allowed(id_a, limit=3, window_seconds=60)
    assert a_result.allowed is False, "A should be rate-limited"

    # B should be completely unaffected
    b_result = await counter.is_allowed(id_b, limit=3, window_seconds=60)
    assert b_result.allowed is True, "B must not be affected by A's limit"


async def test_result_key_contains_identifier(redis):
    """The Redis key should encode the identifier so we can inspect it directly."""
    counter = FixedWindowCounter(redis)
    result  = await counter.is_allowed(TEST_ID, limit=5, window_seconds=60)

    assert TEST_ID in result.key
    assert result.key.startswith("ratelimit:fixed:")
