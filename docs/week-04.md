# Week 04

## Pair 1: Batch

### 1. Weekly goals

- A: ALS grid search (rank, regParam, alpha) on a validation week cut from the end of train, so the test split stays untouched.
- B: `sim:item`: top 50 neighbours per product from the ALS item factors, compared with the Item-kNN table.
- Fix the size of `rec:batch` (top N and which users) so it fits in Redis (2 GB) for the week 5 loader.

### 2. Completed work

| Task | Files |
|---|---|
| Validation split: fit 2019-10-01 to 11-08, validation 11-09 to 11-15 (the last week of train) | `batch/split.py` |
| Parameter search with the week 3 protocol rebuilt on the fit period (same filter, 100,000 warm users, NDCG@10). Coordinate search: alpha at rank 32, then regParam, then rank, each step keeping the best so far (9 trials instead of 45 for the full grid, `--search grid` also works). The best parameters are retrained on the full train split and scored on test | `evaluation/tune_als.py`, `evaluation/offline.py` |
| Tuned parameters as the ALS defaults: rank 128, regParam 0.1, alpha 5 | `batch/als.py` |
| `sim:item` from ALS: exact cosine top 50 between item factors; the normalised factors are broadcast and each Spark task multiplies one block of 128 products with numpy (29 s for 155,423 products) | `batch/sim_item.py` |
| Comparison of the two similarity tables: size, overlap, same-category share, next-item hit rate on test sessions, each table used as an item-based recommender | `evaluation/similarity.py` |
| Redis memory per ZSET measured on Redis 8.10, a budget for the batch keys, and which users get `rec:batch` | `batch/rec_batch.py`, `batch/redis_sizing.py` |
| Full-data reports | `docs/eval-week04-tuning.json`, `docs/eval-week04-als.json`, `docs/eval-week04-sim.json`, `docs/redis-sizing-week04.json` |
| Tuned ALS model and the ALS `sim:item` table | `s3a://lake/models/als/week4/`, `s3a://lake/models/sim_item/week4/` |
| Unit tests (16 new, 34 in total): search order on a fake metric, the validation split, block top-N against brute force, overlap / same category / next item on toy tables, the size formula | `batch/tests/`, `evaluation/tests/` |

### 3. Results / metrics

**ALS search on the validation week.** Fit matrix after the filter: 29,152,789 pairs, 2,465,378 users, 140,077 products; 100,000 users with 940,118 validation pairs. 10 iterations, seed 42.

| Trial | rank | regParam | alpha | NDCG@10 | Recall@20 | Coverage@10 | Training |
|---|---|---|---|---|---|---|---|
| 1 | 32 | 0.1 | 1 | 0.1082 | 0.1454 | 0.0023 | 91 s |
| 2 | 32 | 0.1 | 5 | 0.1148 | 0.1581 | 0.0041 | 95 s |
| 3 | 32 | 0.1 | 10 | 0.1139 | 0.1607 | 0.0053 | 93 s |
| 4 | 32 | 0.1 | 20 | 0.1109 | 0.1617 | 0.0071 | 90 s |
| 5 | 32 | 0.1 | 40 | 0.1063 | 0.1610 | 0.0100 | 90 s |
| 6 | 32 | 0.01 | 5 | 0.1144 | 0.1572 | 0.0037 | 94 s |
| 7 | 32 | 1.0 | 5 | 0.1129 | 0.1579 | 0.0056 | 93 s |
| 8 | 64 | 0.1 | 5 | 0.1217 | 0.1646 | 0.0076 | 259 s |
| 9 | 128 | 0.1 | 5 | **0.1273** | **0.1677** | **0.0140** | 1,136 s |

- alpha 5 gives the best NDCG@10. A larger alpha trades a little NDCG for recall and coverage, a smaller one loses both.
- regParam changes NDCG@10 by less than 0.002 between 0.01 and 1.0.
- rank matters most: each doubling adds about 0.006 NDCG@10 and costs 2.7 to 4.4 times the training time. 128 is the edge of the grid, so a larger rank could still help; we stopped there for the nightly run time (week 5).

**Test split** (same 100,000 users and ground truth as week 3):

| Model | Precision@10 | Recall@10 | Hit Rate@10 | NDCG@10 | Recall@20 | NDCG@20 | Coverage@10 |
|---|---|---|---|---|---|---|---|
| Popularity | 0.0406 | 0.0767 | 0.2595 | 0.0691 | 0.1074 | 0.0764 | 0.0001 |
| Item-kNN (week 3) | 0.0606 | 0.0903 | 0.3022 | 0.0909 | 0.1367 | 0.1017 | 0.3612 |
| ALS week 3 (rank 64, alpha 10) | 0.0641 | 0.1144 | 0.3454 | 0.1158 | **0.1517** | 0.1229 | 0.0091 |
| ALS week 4 (rank 128, alpha 5) | **0.0660** | **0.1156** | **0.3536** | **0.1206** | 0.1495 | **0.1258** | 0.0130 |

- The tuned model gains 4% NDCG@10 over week 3 (0.1206 vs 0.1158) and 75% over Popularity. Recall@20 is slightly lower: the smaller alpha puts more weight on the strongest products at the top of the list.
- Run time on the laptop (Spark local, 14 GB driver): training 1,346 s, top-20 for 100,000 users 119 s. The whole search took 47 minutes.

**`sim:item` from ALS factors vs Item-kNN** (both from the train matrix, 50 neighbours; next item = 1,000,000 sampled test transitions, a product and the next different product of the same session):

| | ALS factors | Item-kNN |
|---|---|---|
| Products with neighbours | 155,423 | 148,414 |
| Neighbours per product | 50.0 | 36.4 |
| Mean similarity | 0.864 | 0.072 |
| Same category @10 / @50 | 0.660 / 0.595 | **0.885 / 0.798** |
| Next item: previous product has a list | **100%** | 99.8% |
| Next item hit@10 / @50 | 0.203 / 0.393 | **0.286 / 0.526** |
| Item-based NDCG@10 / Recall@20 | 0.024 / 0.037 | **0.091 / 0.137** |
| Item-based coverage@10 | 0.268 | **0.361** |
| Build time | 29 s | 116 s |

The two tables share few neighbours: overlap@10 0.177 and overlap@50 0.249 on the 148,414 products they both cover.

- Item-kNN is better on every quality measure. The ALS factors rank products well for a user, but as item-to-item similarity most pairs score close to 1 (mean 0.86): the factors of popular products point in nearly the same direction, so the neighbours follow popularity more than the product (only 66% share its category).
- Decision: from week 5 the nightly job fills `sim:item` from the Item-kNN table (`batch/nightly.py --sim-source item_knn`, the default). The ALS table stays available (`--sim-source als`) for the similarity ablation in week 7. The 7,009 products without Item-kNN neighbours share fewer than two users with any other product; the speed layer has `sim:cooc` for them.

**Redis memory per ZSET key** (Redis 8.10.2, `used_memory` per key over 5,000 keys of REES46-like ids):

| Members | Scores rounded to 4 decimals | Full doubles | Formula 72 + 15.2 x members |
|---|---|---|---|
| 20 | 383 B | 689 B | 376 B |
| 50 | 831 B | 1,585 B | 832 B |
| 100 | 1,597 B | 3,109 B | 1,592 B |

Rounding the scores halves the memory (the order of the list does not change). The formula is within 2% and the loader uses it to check the budget before writing.

**Budget.** Redis has `maxmemory` 2 GB with `volatile-lru`, which only evicts keys with a TTL (the speed layer). Batch keys have none, so they must fit on their own: 1 GiB for the batch layer (`rec:batch` 860 MiB, `sim:item` 150 MiB, `pop:*` 14 MiB) and the other half for the speed layer. A top-100 list for each of the 2,465,378 ALS users would need 3.7 GB, so `rec:batch` holds the most recently active users that fit. Coverage on the validation week (lists built from the fit period):

| Option | Users | Their last event since | Returning ALS users with a list | Cart and purchase events by users with a list |
|---|---|---|---|---|
| top-100 | 566,441 | 2019-11-06 | 40.2% | 38.0% |
| top-50 | 1,083,864 | 2019-11-01 | **66.2%** | **54.3%** |

ALS users made 69.5% of the validation cart and purchase events, the most any `rec:batch` could cover.

- Decision: top-50 for the 1.08 million most recently active ALS users (the API returns at most 50 products, `k` ≤ 50 with default 10 in `docs/openapi.json`, and `docs/plan.md` already lists "top-50 only for active users" as the Redis mitigation). `batch_top_n` is part of the shared contract in `common/config.py`, so the change is in PR #8 for Pair 2's review; until it is merged the loader writes top-100 for 566,441 users.

### 4. Issues & resolutions

| Issue | Resolution |
|---|---|
| A full grid at rank 128 would take hours on the laptop (one rank-128 run trains for 19 minutes, rank 32 for 1.6) | Coordinate search with rank last: 9 trials, 47 minutes for the whole search |
| ALS item factors give similarities near 1 for most product pairs | `sim:item` from Item-kNN instead (section 3) |
| Not every user can have `rec:batch`: 3.7 GB for all ALS users at top-100 | Most recently active users only, size checked against a budget before writing; top-50 doubles the users |
| On Redis 7.0 scores are stored with 17 digits, so rounding saves almost nothing | The project runs Redis 8.10, which stores the shortest form; sizes measured there |

### 5. Plan for next week

- Redis bulk loader for `rec:batch`, `sim:item`, `pop:global`, `pop:cat` and `meta:batch:last_run`, with the budget check above.
- Airflow DAG `nightly_batch`: popularity, ALS, recommendations, similarity, load and validation every night.

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
