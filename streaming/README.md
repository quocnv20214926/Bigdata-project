# Pair 2 – Spark Streaming

Pipeline: Kafka topic `events` → Spark Structured Streaming → Redis.

Two streaming queries share one parsed source (`main.py`):

| Query | Steps | Redis keys | Week |
|---|---|---|---|
| `events` | parse → validate → watermark + dedup → `foreachBatch` | `user:recent`, `user:bought` (`sinks/user_state.py`) | 3 |
| | | `rt:prod:*` product attributes (`sinks/products.py`) | 5 |
| | | `sim:cooc` (`sinks/cooc.py`) | 4 |
| | | `rec:rt`, after `sim:cooc` so it uses the fresh similarities (`sinks/realtime.py`) | 5 |
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
| `sinks/realtime.py` | `rec:rt`: score propagation through `sim:cooc` with time decay |
| `sinks/products.py` | `rt:prod:*`: category, brand, price of each product (read by the API) |
| `redis_conn.py` | One Redis client per Python worker |
| `monitoring/exporter.py` | Prometheus exporter: stream progress, Kafka end offsets, Redis memory and keys (week 7) |
| `evaluation/replay_eval.py` | Speed-only offline evaluation: replay through these sinks, next-item Hit Rate (week 7) |
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
| `rec:rt:{user_id}` | ZSET | top 100 product_id → real-time score, decayed to the user's newest event | 6 h |

`user:recent` and `user:bought` are ZSETs as in contract 1.1.0 (branch
`hien/contract-redis-zset`, not merged into `master` yet).

**`sim:cooc`.** Per session, the last 10 distinct products are kept. When a
product *i* enters that window, `cnt(i) += 1` and `cooc(i, j) += 1` for every
product *j* already in it; a product viewed again while still in the window
only moves to the front. Similarity = `cooc(i, j) / sqrt(cnt(i) · cnt(j))`
(`common.scoring.cooc_similarity`), clipped at 1.

**`rec:rt`.** For an event of type *e* on product *i*:
`rt[j] += w_e · sim(i, j) · exp(−λ·Δt)` for the top 20 neighbours *j* in
`sim:cooc:{i}`, half-life 30 minutes (`common.scoring.propagate`, `time_decay`).
Stored scores are decayed to the user's newest event (`rt:rect:{user_id}`); a newer
event first multiplies them by `exp(−λ·Δt)`. All scores of a user decay by the same
factor, so the ranking does not depend on the reading time and the API needs no
decay (it min-max normalises before merging).

**`trend:*`.** The current hour is the sliding window that ends at the first
5-minute boundary after the newest event time. When that window moves on,
every category seen in it is republished.

Internal keys (Pair 2 only, `rt:` prefix): `rt:sess:*`, `rt:cooc:*`,
`rt:trend:*`, `rt:prod:*` (HASH category_id, category_code, brand, price; TTL 30 days),
`rt:rect:*`, `rt:meta:stream`, `rt:meta:last_batch:*`.

## Delivery guarantees

- `user:*` and `trend:*` are written with absolute values (ZADD of a stable
  member, or the full window total), so a replayed micro-batch changes nothing.
- The co-occurrence counters (HINCRBY) and `rec:rt` (read, decay, rewrite) are not
  idempotent. A micro-batch that was already
  committed is skipped (`rt:meta:last_batch:{query_id}`); only a crash in the
  middle of a batch can count some events twice.
- The trending query has no de-duplication: Spark does not chain two stateful
  operators in update mode. Exact duplicate events add a little weight.

## Monitoring (week 7)

`monitoring/exporter.py` runs as the `stream-exporter` service and exposes on
port 9108, for Prometheus:

| Metric | Source |
|---|---|
| `stream_events_total`, `stream_batch_rows{query}`, `stream_batch_duration_seconds{query}`, `stream_batch_id{query}`, `stream_last_batch_timestamp_seconds{query}` | `rt:meta:stream` (written after each micro-batch) |
| `stream_event_time_seconds` (stream clock), `stream_trending_window_end_seconds` | `rt:meta:stream` |
| `kafka_topic_end_offset{topic,partition}` | Kafka: messages written to the topic (the input rate) |
| `redis_used_memory_bytes`, `redis_keys`, `redis_keys_by_prefix{prefix}`, `redis_ops_per_second` | Redis `INFO`, `DBSIZE`, `SCAN` every minute |

Grafana (http://localhost:3000, admin / admin) opens on the provisioned
dashboard "Recommender: real-time pipeline and API" (`infra/monitoring/`).
Spark's own view of the queries stays in the Spark UI (http://localhost:4040,
tab *Structured Streaming*); topics, partitions and messages in Kafka UI
(http://localhost:8085).

## Speed-only evaluation (week 7)

```bash
redis-server --port 6390 --save ""          # a dedicated Redis: its database is emptied
python -m streaming.evaluation.replay_eval --file data/raw/2019-Oct.csv \
    --warmup-until 2019-10-02T00:00:00Z --end 2019-10-02T12:00:00Z --sample 0.25 \
    --redis-port 6390 --json docs/eval-pair2-week07.json
```

The events are replayed in event-time order through the sinks above (the same
code as the streaming job), grouped in micro-batches of `--batch-seconds` (60 s)
of event time, and the lists are read with the API's `RedisRepository`. For each
target event (a user's next product, different from the previous one), the lists
come from the state before the micro-batch of the event. Methods: `trending`,
`trending_cat`, `cooc_last` (neighbours of the last product only), `rec_rt`, `api`
(what `/recommend` returns without `rec:batch`). `--half-life` and `--neighbours`
change the `rec:rt` parameters. Results: `docs/week-07.md`.

