# Distributed Rate Limiter

A distributed rate limiter built with FastAPI and Redis, implementing three different rate limiting algorithms from scratch. I built this as a portfolio project while studying for my MS in Software Engineering at Arizona State University, mainly to get hands-on practice with distributed systems concepts that come up a lot in backend interviews: shared state across servers, race conditions, and atomic operations.

## Why I built this

Most rate limiter tutorials show you one algorithm and stop. I wanted to actually understand the tradeoffs between the common approaches, so I implemented all three of the algorithms you'll usually see discussed in system design interviews (Fixed Window, Sliding Window Log, and Token Bucket), backed by the same Redis instance, so I could compare them side by side instead of just reading about the differences.

The harder part wasn't the algorithms themselves — it was making them safe under concurrency. If two requests hit Redis at the same time, naive code (read counter, check it, increment it) has a race condition: both requests can read the same value before either writes back, and the limit silently breaks. I used Redis Lua scripts to make each check-and-update operation atomic, which was new to me going in and turned out to be the most useful thing I learned from this project.

## What's actually in here

```
app/
  algorithms/
    fixed_window.py        Counter per time bucket, resets on expiry
    sliding_window_log.py  Sorted set of timestamps, no boundary burst
    token_bucket.py        Lazy refill, allows controlled bursting
  middleware/
    rate_limit.py           Auto rate-limits every request via Token Bucket
  main.py                  FastAPI routes for all three algorithms
  redis_client.py          Shared async Redis connection
  dashboard.html           Live dashboard to watch the algorithms in action
tests/
  test_fixed_window.py
  test_sliding_window_log.py
  test_token_bucket.py
  test_middleware.py
```

## The three algorithms, briefly

**Fixed Window Counter** — divides time into fixed buckets (e.g. every 60s) and counts requests per bucket. Simple and O(1) space, but it has a known flaw: a client can send double the limit by timing requests around a window boundary (e.g. 10 requests at second 59, 10 more at second 61 — 20 requests in 2 seconds against a "10 per minute" limit).

**Sliding Window Log** — fixes the boundary problem by storing the actual timestamp of every request in a Redis sorted set, then counting only the ones within the last N seconds on every check. This is correct, but it costs O(N) space since you're storing one entry per request instead of one counter.

**Token Bucket** — the algorithm most production systems actually use (this is roughly how Stripe and AWS API Gateway do it). Each client has a bucket of tokens that refills over time; each request spends one token. This allows a client to burst if they've been idle, while still capping their sustained rate. I implemented "lazy refill" here, meaning there's no background job topping up tokens — instead, the Lua script calculates how many tokens should have accumulated since the last request based on elapsed time, right when the next request comes in.

## Why Lua scripts

This was the main technical challenge. Without atomicity, a sequence like:

```
1. Read current count from Redis
2. Check if count < limit
3. Increment count
4. Write back to Redis
```

has a race condition between steps 1-4 if two requests arrive close together — both can read the same starting value and both get approved, even if doing so exceeds the limit. Redis Lua scripts run as a single atomic operation on the Redis server itself, so the whole check-and-update sequence either fully completes or doesn't run at all, with nothing else able to interleave. Each algorithm here has its own Lua script handling this.

## Running it

You'll need Docker (for Redis) and Python 3.9+.

```bash
# start redis
docker-compose up -d

# set up the environment
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt

# run the server
uvicorn app.main:app --reload --port 8000
```

Then visit `http://localhost:8000/dashboard` to see all three algorithms running live against the same Redis instance — you can fire requests, burst past the limit, and (for Token Bucket specifically) watch the tokens refill over time.

API docs are at `http://localhost:8000/docs`.

## Running the tests

```bash
pytest tests/ -v
```

22 tests covering correctness (e.g. requests at the limit are allowed, one over is rejected), isolation between clients, and one test per algorithm specifically designed to expose its known weak point — for example, `test_no_boundary_burst` proves Sliding Window doesn't have the Fixed Window boundary problem.

## What I'd do differently / next steps

- The reset endpoint uses Redis's `KEYS` command, which is fine for a project like this but would be a problem at scale (it scans the whole keyspace). A production version should use `SCAN` with a cursor instead.
- Right now the middleware identifies clients by IP address. A more realistic version would support API keys or user IDs, since IP-based limiting breaks down behind NAT (many users sharing one IP) or for legitimate multi-server clients.
- I'd like to add a load test (Locust) to generate real concurrent traffic and produce actual latency numbers under load, rather than just unit tests against a single Redis instance.

## Tech stack

Python 3.9, FastAPI, Redis 7.2 (via Docker), Lua, pytest + pytest-asyncio

---

Built by Sujan Uppalli Jayadevappa — MS Software Engineering, Arizona State University
