# Clickstream Lambda Pipeline

A streaming + batch data pipeline implementing Lambda Architecture: the same e-commerce clickstream events flow through both a real-time speed layer and a scheduled batch layer, landing in the same store so their outputs can be reconciled against each other.

## Why Lambda Architecture

A pure streaming pipeline is fast but can be wrong — events that arrive late (a mobile client buffering offline, then reconnecting) get counted in whatever time window they *arrive* in, not the window they actually *happened* in. A pure batch pipeline is correct but slow — you wait for a full data dump before computing anything.

Lambda architecture runs both: a speed layer for low-latency approximate results, and a batch layer that periodically reprocesses everything from the permanent raw log to produce the correct numbers. This project deliberately simulates late-arriving events so the discrepancy between the two layers — and the correction — is visible and explainable, not theoretical.

## Project phases

| Phase | What | Status |
|-------|------|--------|
| 1 | Event producer + Kafka + raw event storage | ✅ |
| 2 | Speed layer — windowed aggregation consumer | ✅ |
| 3 | Batch layer — scheduled reprocessing job | ✅ this phase |
| 4 | Reconciliation — compare and correct speed vs batch | 🔜 next |

## Architecture (Phase 1 + 2 + 3)

```
producer/event_generator.py
        │  publishes ClickstreamEvent (some deliberately late)
        ▼
   Kafka topic: clickstream-events
        │
        ├──────────────────────────────────────────┐
        ▼                                          ▼
storage/raw_event_writer.py            speed_layer/aggregator.py
(group: raw-storage-writer)            (group: speed-layer-aggregator)
        │  writes every event,                 │  counts events per
        │  unaggregated, idempotently           │  minute-window, live,
        ▼                                       │  with a 30s grace period
   Postgres: raw_events                         ▼
   (permanent, complete log —          Postgres: speed_counts
    nothing is ever dropped)           (fast, approximately correct;
        │                              late_events_dropped tracks misses)
        │
        ▼
batch_layer/batch_job.py
   (run on a schedule, not continuously — reads ALL of raw_events,
    groups by TRUE event_time, no grace period needed)
        │
        ▼
   Postgres: batch_counts
   (slow, but always correct — includes every late event in its true window)
```

Both consumers (raw_event_writer and aggregator) read the SAME Kafka topic independently — different consumer groups mean Kafka delivers a full copy of every event to each. The batch job doesn't touch Kafka at all; it only reads from the permanent `raw_events` table, which is exactly why it can be correct without racing against time.

## Project structure

```
producer/
  schemas.py           ClickstreamEvent definition (event_time vs ingestion_time)
  event_generator.py   Simulates realistic traffic, including late arrivals
storage/
  db.py                Postgres connection + schema (raw_events, speed_counts, batch_counts)
  raw_event_writer.py  Kafka consumer → Postgres (idempotent, dumb on purpose)
speed_layer/
  aggregator.py        Kafka consumer → windowed counts, with grace period
batch_layer/
  batch_job.py         Scheduled SQL aggregation over raw_events → batch_counts
tests/
  test_event_generator.py
  test_aggregator.py
  test_batch_job.py
```

## The key design decision: event_time vs ingestion_time

Every event has two timestamps:
- **event_time** — when it actually happened, from the client's perspective
- **ingestion_time** — when our system received it

For most events these are the same moment. But the producer deliberately makes a small percentage of events "late": event_time is set in the past, while ingestion_time is now. This single mechanic is what creates the entire reason this project exists — a speed layer processing by arrival order will misplace late events into the wrong time window, while a batch layer re-reading by event_time later will get it right.

## Running it

```bash
# start kafka, zookeeper, postgres
docker-compose up -d

# environment
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt

# initialize the database schema
python -m storage.db

# terminal 1: start the raw event writer (consumer)
python -m storage.raw_event_writer

# terminal 2: start the speed layer (independent consumer, different group)
python -m speed_layer.aggregator

# terminal 3: start the producer
python -m producer.event_generator
```

Watch all three terminals. After a couple of minutes (windows finalize after window + grace period = 90s), check the speed layer's output:

```bash
docker exec -it pipeline-postgres psql -U pipeline -d clickstream -c "SELECT user_id, event_type, window_start, event_count, late_events_dropped FROM speed_counts ORDER BY window_start DESC LIMIT 10;"
```

Rows with `late_events_dropped > 0` are windows where a late-arriving event showed up after that window had already been finalized — proof the speed layer is fast but not perfectly accurate.

Now run the batch layer to compute the TRUE counts for the same period:

```bash
python -m batch_layer.batch_job
```

This runs once and exits (it's meant to be scheduled, not left running). Compare the two outputs directly:

```bash
docker exec -it pipeline-postgres psql -U pipeline -d clickstream -c "
SELECT s.user_id, s.event_type, s.window_start, s.event_count AS speed_count, b.event_count AS batch_count, s.late_events_dropped
FROM speed_counts s
JOIN batch_counts b ON s.user_id = b.user_id AND s.event_type = b.event_type AND s.window_start = b.window_start
WHERE s.event_count != b.event_count OR s.late_events_dropped > 0
ORDER BY s.window_start DESC LIMIT 10;
"
```

Any row here is a window where the speed layer's fast answer differs from the batch layer's correct answer — direct, queryable proof of why Lambda architecture exists.

## Running the tests

```bash
pytest tests/ -v
```

`test_event_generator.py` and `test_aggregator.py` are pure-Python unit tests — no infrastructure required. `test_batch_job.py` is an integration test that needs Postgres running (`docker-compose up -d` first), since the batch job's entire purpose is running real SQL against `raw_events`.

Key tests:
- `test_late_events_have_event_time_before_ingestion_time` — proves the late-arrival mechanism works (Phase 1)
- `test_event_after_finalization_is_dropped_not_counted` — proves the speed layer's defining limitation is real and observable (Phase 2)
- `test_batch_job_correctly_places_late_arriving_event` — proves the batch layer succeeds exactly where the speed layer is designed to fail, by grouping on event_time instead of arrival order (Phase 3)
- `test_batch_job_is_idempotent_when_run_twice` — proves re-running the "reprocess everything" batch job doesn't double-count, which is what makes that simple approach safe to actually schedule

## Tech stack

Python 3.9+, Kafka (Confluent images via Docker), Postgres 16, confluent-kafka, psycopg2, Pydantic, Faker
