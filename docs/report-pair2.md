# Final Report, Pair 2 Sections: Speed Layer, Serving, UI, Lambda vs Kappa

Sections of the final report written by Pair 2 (week 9). Every number comes from
the weekly reports (`docs/week-02.md` to `docs/week-08.md`) and the result files
in `docs/`. Pair 1 covers the data, the batch layer and its evaluation.

## 1. Speed layer

### 1.1 Ingestion: the Event Replayer

The dataset is historical, so a replayer (`ingestion/replayer.py`) turns the raw
CSV into a live stream: each row becomes a `common.schemas.Event`, sent to the
Kafka topic `events` (6 partitions, KRaft, one broker) as JSON with key
`user_id`. The key keeps each user's events in one partition, in order. Pacing
keeps the gaps between events: `--speed 60` plays one hour of data per minute,
`--speed 0` as fast as possible. Pair 2 reads the raw CSV directly, so it never
waits for Pair 1's ETL (decoupling rule 1).

### 1.2 Streaming job

`streaming/main.py` runs two Spark Structured Streaming queries on one parsed
source:

| Query | Processing | Output (Redis) |
|---|---|---|
| `events` | JSON → typed columns, invalid rows dropped, watermark 10 min, exact duplicates dropped (`dropDuplicatesWithinWatermark`), `foreachBatch` every 2 s (≤ 50,000 events) | `user:recent`, `user:bought`, `rt:prod`, `sim:cooc`, `rec:rt` |
| `trending` | 1 h windows sliding every 5 min on event time, sum of event weights per (category, product), output mode `update` | `trend:cat`, `trend:global` |

Each micro-batch is repartitioned by `user_id` and sorted by time, so a user's
events are applied in order by one task, with Redis pipelines. State (Kafka
offsets, dedup state) is checkpointed in RocksDB; a restart continues where it
stopped (week 3: the processed total did not change on restart and grew by
exactly the number of new events). Non-idempotent writes (co-occurrence counters,
`rec:rt`) are protected by a batch-id guard in Redis.

### 1.3 Session co-occurrence (`sim:cooc`)

For each session, the last 10 distinct products are kept. When a product *i*
enters the window, `cnt(i)` and `cooc(i, j)` for every *j* already there are
incremented; `sim(i, j) = cooc(i, j) / sqrt(cnt(i) · cnt(j))`, top 50 per product.
It captures new products immediately and needs no batch layer (decoupling rule 2).

### 1.4 Real-time scores (`rec:rt`)

For an event of type *e* (weight view 1, cart 3, purchase 5) on product *i*:
`rt[j] += w_e · sim(i, j) · exp(−λ · Δt)` for the top 20 neighbours *j* of *i*,
λ = ln 2 / half-life. Scores are stored already decayed to the user's newest
event, so the ranking never depends on when it is read and the API does no
decay. Since week 8 the neighbours can come from `sim:cooc`, `sim:item` or both
(`RT_SIMILARITY`), and the half-life is 5 minutes.

### 1.5 Results

| Measure | Result | Source |
|---|---|---|
| Event to updated recommendations | 2.5–2.9 s (trigger 2 s) | week 6 |
| Users with `rec:rt` after the October sample | 35,712 of 38,440 active users (92.9 %) | week 5 |
| Redis memory, all Pair 2 keys of the sample (200,000 events) | 131 MB | week 6 |
| Next item, HR@10 of `/recommend` (speed layer + fallback) | 0.196, against 0.040 for global trending and 0.160 for trending in the user's category | week 7 |
| Products the user has never touched, NDCG@10 (Pair 1's replay of 11-16) | speed only 0.1351, ALS 0.0454, Popularity 0.0396 | week 8 |

The speed layer is what finds the *next new* product: three times ALS on
products the user has never touched. On carts, 98 % of the products are already
in the user's recent views, and the batch list remains the best ranking
(week 8, Pair 1).

## 2. Serving layer

### 2.1 API

FastAPI (`serving/app/`), contract in `common/models.py`:
`/recommend/{user_id}`, `/similar/{product_id}`, `/trending[/{category_id}]`,
`/health`, plus `/metrics` for Prometheus. Routes only call a repository:
`MockRepository` (week 2, so the UI and Pair 1 could work against the frozen API
spec) or `RedisRepository` (week 5).

`/recommend` in Redis mode:

1. read `rec:batch` (top 50) and `rec:rt` (top 100);
2. `common.scoring.merge_scores`: min-max normalisation, `alpha · batch + (1 − alpha) · rt`,
   alpha = 0.1 (week 8); without one layer, the other gets full weight;
3. remove purchased products (`user:bought`);
4. fill short lists from `trend:cat` of the last viewed category, then
   `trend:global`, then `pop:global` (decoupling rule 3, graceful degradation);
5. add category, brand and price from `rt:prod`.

Every item carries its source (`batch`, `realtime`, `both`, `trending`,
`popular`). A Redis failure returns a clear 503.

### 2.2 Latency

Locust, real ids from Redis, 70 % `/recommend`, 10 % unknown users (fallback
path), API with 2 workers on a 2-core machine shared with Redis and Locust (week 6):

| Target | Achieved | p50 | p95 | p99 |
|---|---:|---:|---:|---:|
| 500 requests/s | 497 /s | 12 ms | 24 ms | 31 ms |
| 1,000 requests/s | 961 /s | 24 ms | 62 ms | 82 ms |

The plan's target, p95 < 100 ms at 500–1,000 requests/s, is met. A request costs
the API about 2 ms; the saturation point is about 1,050 requests/s on that machine.

## 3. User interface and monitoring

Streamlit (`ui/`), three pages:

1. **Recommendations**: stream status, a user's recent activity, recommendations
   with their source; *Send view / cart / purchase* publishes an event to Kafka
   (same contract as the replayer), waits until the speed layer has processed it
   and shows the new list with new products marked.
2. **Trending & similar**: trending by category (1 h of event time), similar products.
3. **Evaluation**: speed-only replay, similarity ablation, Pair 1's batch results, load test.

Simulated events are stamped just after the *stream clock* (the newest event time
processed), not with today's date: a 2026 timestamp in a 2019 replay would move the
watermark years ahead and decay every real-time score to zero.

Monitoring: Prometheus scrapes the API (requests, latency histogram, recommendation
sources) and a speed-layer exporter (stream progress, Kafka end offsets, Redis
memory and keys); Grafana shows one provisioned dashboard. Kafka UI and the Spark
UI cover topics and query progress.

## 4. Lambda vs Kappa

| | Lambda (this project) | Kappa |
|---|---|---|
| Idea | Batch layer recomputes long-term models nightly; speed layer handles recent events; serving merges | One streaming pipeline; reprocessing = replaying the log |
| Long-term model (implicit ALS on 46 days, 39.6 M interactions) | Natural: one nightly Spark job (21 min, week 10) | Needs incremental matrix factorisation or periodic retraining *inside* the stream, i.e. a batch job in disguise |
| Short-term intent | Speed layer, seconds | Same |
| Code bases | Two (batch and speed), kept consistent through `common/` (one scoring module, frozen contracts) | One |
| Failure isolation | Each layer degrades alone: no `rec:batch` → real-time + trending; no speed layer → batch only | A pipeline failure stops everything |
| Team fit | Two pairs, each with a complete pipeline (only two dependency points) | One shared pipeline |

Why Lambda here: the heavy model (ALS) needs the full history and fits batch
processing; the speed layer only needs light, incremental logic (counts, decayed
sums, windows). The evaluation supports keeping both: ALS is best for the user's
long-term taste and for carts (NDCG@10 0.128 batch only), the speed layer for the
next new product (0.135 vs 0.045). The cost is two code bases; the risk of drift
is controlled by the shared contracts and by evaluating the production code
itself (the Pair 2 replay runs the streaming sinks and the API repository).

A Kappa variant of this system would keep Kafka as the source of truth with a long
retention, compute `sim:cooc`, `rec:rt` and trending exactly as now, and replace
ALS by a periodically retrained model fed from the same topic; the nightly job
would become a scheduled replay, which in practice is the batch layer again.

## 5. Limits and future work

- The speed layer was measured on one machine (2 cores); Kafka has one broker
  and Spark runs in local mode.
- The Pair 2 replay processes about 500 events/s (the real sinks, one micro-batch
  at a time); full-day runs take about an hour.
- `sim:cooc` is noisy for rarely viewed products; products without co-views get
  no `rec:rt` (5 % of the targets, served by the fallback).
- A "recently viewed" row would serve the cart case directly (98 % of carts follow
  a view of the same product); it needs an API contract change agreed by both pairs.
- No authentication or rate limiting on the API; Kafka is not secured.
