# Pair 2 – Spark Streaming

Pipeline: Kafka topic `events` → Spark Structured Streaming → Redis.

Two streaming queries share one parsed source (`main.py`):

| Query | Steps | Redis keys | Week |
|---|---|---|---|
| `events` | parse → validate → watermark + dedup → `foreachBatch` | `user:recent`, `user:bought` (`sinks/user_state.py`) | 3 |
| | | `sim:cooc` (`sinks/cooc.py`) | 4 |
| `trending` | parse → validate → watermark → 1 h window sliding every 5 min, summed event weights, output mode `update` | `trend:cat`, `trend:global` (`sinks/trending.py`) | 4 |

Redis writes run on the executors (`foreachPartition`); the driver only
coordinates. Each query has its own checkpoint under `CHECKPOINT_LOCATION`.

## Files

| File | Purpose |
|---|---|
| `main.py` | Configuration, Spark session, source (Kafka or file), the two queries, batch-id guard, progress in `rt:meta:stream` |
| `parsing.py` | JSON → typed columns (`event_ts`, `epoch`), validation, watermark + dedup, event weights |
| `sinks/user_state.py` | `user:recent`, `user:bought` |
| `sinks/cooc.py` | Session co-occurrence counts and `sim:cooc` |
| `sinks/trending.py` | Per-window totals and publication of the current hour to `trend:*` |
| `redis_conn.py` | One Redis client per Python worker |
| `tests/` | Unit tests (fakeredis) and an end-to-end test of both queries |

## Configuration

Environment variables (defaults in brackets): `KAFKA_BOOTSTRAP`, `KAFKA_TOPIC_EVENTS`,
`REDIS_HOST`, `REDIS_PORT`, `REDIS_DB`, `STARTING_OFFSETS` [earliest],
`MAX_OFFSETS_PER_TRIGGER` [50000], `TRIGGER_SECONDS` [5],
`CHECKPOINT_LOCATION` [/checkpoints/pair2], `EVENT_WATERMARK` [10 minutes],
`TREND_WINDOW` [1 hour], `TREND_SLIDE` [5 minutes], `SHUFFLE_PARTITIONS` [6],
`STREAM_SOURCE` [kafka] (`file` reads JSON lines from `INPUT_DIR`, for development),
`AVAILABLE_NOW` [0] (`1` processes what exists, then stops).

## Run

```powershell
docker compose -f infra/compose.base.yml -f infra/compose.stream.yml up -d --build
docker compose -f infra/compose.base.yml -f infra/compose.stream.yml logs -f streaming
docker exec redis redis-cli HGETALL rt:meta:stream
```

The log prints one line per micro-batch and query, e.g.
`events batch=3 rows=49933 ...` and `trending batch=3 updated_rows=185294 published_window_end=...`.

Without Kafka (Linux, macOS or WSL; Java 17+):

```bash
python -m ingestion.replayer --file data/sample/oct_sample.csv --dry-run --speed 0 > data/stream_in/part-0.json
PYTHONPATH=. STREAM_SOURCE=file INPUT_DIR=data/stream_in CHECKPOINT_LOCATION=checkpoints/pair2 \
  AVAILABLE_NOW=1 spark-submit streaming/main.py
```

Tests: `python -m pytest streaming/tests -q` (the end-to-end test needs pyspark,
Java and `redis-server`; it is skipped otherwise).

Replaying from the beginning again needs fresh checkpoints, because events
older than the watermark are dropped:
`docker volume rm recsys_stream-checkpoints`.

## Redis contract

| Key | Type | Value | TTL |
|---|---|---|---|
| `user:recent:{user_id}` | ZSET | `product_id\|event_type\|epoch` → epoch, 20 newest (read with `ZREVRANGE`) | 24 h |
| `user:bought:{user_id}` | ZSET | product_id → latest purchase epoch (test with `ZSCORE`) | 30 days |
| `sim:cooc:{product_id}` | ZSET | top 50 product_id → co-occurrence similarity in [0, 1] | 7 days |
| `trend:cat:{category_id}`, `trend:global` | ZSET | top 100 product_id → weighted count in the last hour (view 1, cart 3, purchase 5) | 1 h |

`user:recent` and `user:bought` are ZSETs as in contract 1.1.0 (branch
`hien/contract-redis-zset`, not merged into `master` yet).

**`sim:cooc`.** Per session, the last 10 distinct products are kept. When a
product *i* enters that window, `cnt(i) += 1` and `cooc(i, j) += 1` for every
product *j* already in it; a product viewed again while still in the window
only moves to the front. Similarity = `cooc(i, j) / sqrt(cnt(i) · cnt(j))`
(`common.scoring.cooc_similarity`), clipped at 1.

**`trend:*`.** The current hour is the sliding window that ends at the first
5-minute boundary after the newest event time. When that window moves on,
every category seen in it is republished.

Internal keys (Pair 2 only, `rt:` prefix): `rt:sess:*`, `rt:cooc:*`,
`rt:trend:*`, `rt:meta:stream`, `rt:meta:last_batch:*`.

## Delivery guarantees

- `user:*` and `trend:*` are written with absolute values (ZADD of a stable
  member, or the full window total), so a replayed micro-batch changes nothing.
- The co-occurrence counters use HINCRBY. A micro-batch that was already
  committed is skipped (`rt:meta:last_batch:{query_id}`); only a crash in the
  middle of a batch can count some pairs twice.
- The trending query has no de-duplication: Spark does not chain two stateful
  operators in update mode. Exact duplicate events add a little weight.
