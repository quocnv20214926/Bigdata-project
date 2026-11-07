# Week 04

## Pair 2: Real-time

### 1. Weekly goals

- **C:** windowed trending: `trend:cat:{category_id}` and `trend:global`, weighted counts over the last hour.
- **D:** session co-occurrence similarity `sim:cooc:{product_id}` computed from the stream.
- Deliverable: trending and `sim:cooc` in Redis.

### 2. Completed work

| Task | Files |
|---|---|
| Second streaming query `trending`: `window(event_ts, "1 hour", "5 minutes")` per category and product, summed event weights (view 1, cart 3, purchase 5 from `common/weights.py`), output mode `update`, own checkpoint | `streaming/main.py`, `streaming/parsing.py` (`with_weight`) |
| Trending sink: full window totals stored per window (`ZADD GT`, idempotent), the current hour copied to `trend:cat:*` / `trend:global` (top 100, TTL 1 h) in one `MULTI`; late windows never overwrite a newer one; on window change every category of the new hour is republished | `streaming/sinks/trending.py` |
| Co-occurrence sink: per session the last 10 distinct products (`settings.session_window`); a product entering the window adds 1 to `cnt(i)` and to `cooc(i, j)` for each product already there; revisits only reorder. Similarity `cooc / sqrt(cnt(i) · cnt(j))` from `common/scoring.py`, top 50 (`settings.sim_top_n`), TTL 7 days, rewritten atomically | `streaming/sinks/cooc.py` |
| Session windows survive micro-batches (`rt:sess:{session}` in Redis, TTL 2 h) | `streaming/sinks/cooc.py` |
| Batch-id guard for the non-idempotent co-occurrence counters (`rt:meta:last_batch:{query_id}`, tied to the checkpoint) | `streaming/main.py` |
| `streaming/main.py` split into modules so each sink is unit-tested; Redis writes moved from the driver (`collect()`) to the executors (`foreachPartition`); `STREAM_SOURCE=file` and `AVAILABLE_NOW=1` for development and tests | `streaming/` |
| Timestamp parsing with `try_cast`: a malformed `event_time` is dropped instead of stopping the query (Spark 4 ANSI mode) | `streaming/parsing.py` |
| Tests: 9 sink unit tests (fakeredis), parsing test, end-to-end test of both queries on a temporary Redis (dedup, ZSET user state, `sim:cooc`, trending totals, restart without double counting) | `streaming/tests/` |
| README: queries, keys, guarantees, how to run | `streaming/README.md` |

### 3. Results / metrics

Tests: 14 / 14 pass on PySpark 3.5.3 (version of `streaming/Dockerfile`) and on PySpark 4.1.3.

**Run on real data.** The 200,000-event October sample (`data/sample/oct_sample.csv`, 2019-10-01 00:00 to 06:06 UTC), fed as 4 micro-batches of 50,000 events (`STREAM_SOURCE=file`), local Spark on 2 cores, Redis 7:

| Metric | Result |
|---|---|
| Events written (after dedup) | 199,862 of 200,000 |
| Time per 50,000-event micro-batch | ≈ 10 s for each query (both run in parallel), ≈ 5,000 events/s |
| Trending rows updated per micro-batch | ≈ 190,000 (each event belongs to 12 sliding windows) |
| Published window | [05:10, 06:10) UTC, i.e. the hour up to the newest event (06:06:57) |
| `trend:cat` keys / `trend:global` | 519 / 1 |
| Top of `trend:global` | 1004856 (samsung smartphone, 1,079), 5100816 (xiaomi, 821), 1005115 (apple smartphone, 809) |
| `sim:cooc` keys | 28,300 products, 16.1 neighbours on average (max 50) |
| Same category among the top 10 `sim:cooc` neighbours | 70.8 % (the neighbours of the most viewed product, an Apple smartphone, are all Apple smartphones) |
| Redis memory for all Pair 2 keys of the sample | 79 MB |

**Docker stack with Kafka** (`docker compose ... up -d --build`, the same sample replayed with `--speed 0`, `MAX_OFFSETS_PER_TRIGGER=50000`): the results are identical to the run above.

| Metric | Result |
|---|---|
| `events_total_rows` / `events_max_event_time` (`rt:meta:stream`) | 199,862 / 2019-10-01T06:06:57Z |
| `trending_window_end` | 2019-10-01T06:10:00Z |
| `ZREVRANGE trend:global 0 4 WITHSCORES` | 1004856 (1,079), 5100816 (821), 1005115 (809), 1004767 (737), 1004870 (553) |
| `sim:cooc:*` keys | 28,299 (`redis-cli --scan`) |
| `sim:cooc:1005115` top 3 | 1005105 (0.308), 1004249 (0.181), 1003317 (0.154) |
| Input rate / process rate (Spark UI, port 4040) | [TO FILL] |

### 4. Issues & resolutions

| Issue | Resolution |
|---|---|
| The newest sliding window only holds the last few minutes, not the last hour | Publish the window ending at the first 5-minute boundary after the newest event (`max(window_end) - 1 h + 5 min`); found by the end-to-end test |
| A category with no event in the current micro-batch kept the previous hour | Categories of each window are recorded (`rt:trend:cats:{end}`); when the window moves on, all of them are republished |
| The streaming container never started: `spark-submit` is not on the PATH of the `apache/spark` image (week 3 Dockerfile) | `CMD` calls `/opt/spark/bin/spark-submit` |
| Two stateful operators (dedup, then window) cannot be chained in update mode | The trending query has no dedup; exact duplicates add a little weight (138 of 200,000 events in the sample) |
| `HINCRBY` counters double count if a micro-batch is replayed | Batch-id guard per checkpoint; only a crash in the middle of a batch can still count twice |
| A pair can be counted twice when a product leaves the session window and comes back while the other is kept by a revisit (similarity up to 1.73 in the sample) | Similarity clipped at 1 |
| Writing from the driver (`collect()`) does not scale with larger micro-batches | Writes run in `foreachPartition`, data partitioned by `user_id` (a session belongs to one user, so its events stay in order in one partition) |
| `rt:cooc:{product}` hashes grow with the number of co-viewed products | TTL 7 days, refreshed on use; to monitor on the full month |

### 5. Plan for next week

- API reads real Redis data (`API_MODE=redis`): `rec:batch` + `rec:rt` merged with `common.scoring.merge_scores`, fallback to `trend:cat` of the last viewed category, then `pop:global`; `/similar` from `sim:cooc`, `/trending` from `trend:*`.
- `rec:rt`: score propagation through `sim:cooc` with time decay (`common.scoring.propagate`).
