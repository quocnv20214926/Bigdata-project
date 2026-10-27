# Week 03

## Pair 1: Batch

### 1. Weekly goals

- A: interaction matrix and the first implicit ALS model.
- B: metric functions, Popularity and Item-kNN baselines.
- Deliverable: first ALS model and baseline metrics on the time-based test split.

### 2. Completed work

| Task | Files |
|---|---|
| Interaction matrix: one row per (user, product), score = sum of the event weights in `common/weights.py` (view 1, cart 3, purchase 5); ALS filter chosen in the EDA (product >= 5 users, then user >= 2 products) | `batch/interactions.py` |
| Popularity baseline: products ranked by total score, global and per category (the ranking `pop:global` and `pop:cat` will hold) | `batch/popularity.py` |
| Implicit ALS (Spark ML) trained on `log1p(score)`, so Spark's confidence `1 + alpha * r` is exactly `common.weights.als_confidence` | `batch/als.py` |
| Item-kNN baseline: cosine similarity between products over users (each user capped at their 50 strongest products, at least 2 common users, top 50 neighbours per product); a user's score for a product is the sum of `log1p(score) * sim` over the products they interacted with | `batch/item_knn.py` |
| Metric functions in Spark: Precision, Recall, Hit Rate and NDCG at K (binary relevance), catalog coverage | `evaluation/metrics.py` |
| Offline evaluation: the same users and ground truth for every model; JSON report with parameters, data sizes and run times; can save the ALS model | `evaluation/offline.py`, `evaluation/README.md` |
| Full-data reports | `docs/eval-week03-als.json`, `docs/eval-week03-itemknn.json` |
| ALS model (rank 64) saved for `sim:item` in week 4 | `s3a://lake/models/als/week3/` |
| Unit tests (18): weights and filter, metrics checked by hand and against plain Python, Item-kNN on a toy matrix, the whole evaluation on a tiny dataset | `batch/tests/`, `evaluation/tests/` |

### 3. Results / metrics

Train 2019-10-01 to 2019-11-15, test 2019-11-16 to 2019-11-30. The three models are scored on the same 100,000 users, drawn with a fixed seed from the users that are in the ALS matrix and have at least one test event. Ground truth is every product the user viewed, added to cart or bought in the test period. Products already seen in train are not removed, because repeat views are common in this data.

| Model | Precision@10 | Recall@10 | Hit Rate@10 | NDCG@10 | Recall@20 | NDCG@20 | Coverage@10 |
|---|---|---|---|---|---|---|---|
| Popularity | 0.0406 | 0.0767 | 0.2595 | 0.0691 | 0.1074 | 0.0764 | 0.0001 |
| Item-kNN | 0.0606 | 0.0903 | 0.3022 | 0.0909 | 0.1367 | 0.1017 | 0.3612 |
| ALS (rank 64, 10 iterations) | **0.0641** | **0.1144** | **0.3454** | **0.1158** | **0.1517** | **0.1229** | 0.0091 |

Coverage@10 is the share of the 155,423 products in the ALS matrix that appear in at least one top-10 list.

| Metric | Result |
|---|---|
| Train interactions | 39,584,014 user-product pairs, 4,205,076 users, 191,909 products |
| ALS matrix after the filter | 38,195,148 pairs (96.5%), 2,890,519 users (68.7%), 155,423 products (81.0%) |
| Ground truth | 100,000 users, 1,077,669 user-product pairs |
| Item-kNN similarity table | 5,408,264 rows for 148,414 products |
| ALS run time (laptop, Spark local, 14 GB driver) | training 314 s, top-20 for 100,000 users 83 s, whole run 486 s |
| Item-kNN run time | similarities 124 s, whole run 235 s |
| ALS temporary disk at peak | about 11 GB (over 30 GB before the fix in section 4) |

- ALS beats Popularity on every ranking metric: NDCG@10 is 68% higher (0.1158 vs 0.0691), so the success criterion "ALS beats the Popularity baseline on NDCG@10" is met.
- Item-kNN is between the two on NDCG and recall, and slightly better than ALS on Precision@20 (0.0477 vs 0.0465). It covers 36% of the catalog against 0.9% for ALS, which mostly recommends popular products.

### 4. Issues & resolutions

| Issue | Resolution |
|---|---|
| ETL date filter failed on Linux: `date.min` reaches Java as "1-01-01" | Apply each date bound only when it is given (`batch/etl.py`) |
| On the Mac, `java_home -v 17` returns an x86 JDK 16 and Spark fails | Use Homebrew `openjdk@17`; path in `batch/README.md` |
| The first full ALS run filled the laptop disk: Spark kept the shuffle files of every iteration (over 30 GB) | Checkpoint the factors every 2 iterations (`--checkpoint-interval`), clean shuffle files every minute, release cached data early |
| Item-kNN compares every pair of products a user touched, which explodes for heavy users | Keep each user's 50 strongest products and require 2 common users; the full run takes 4 minutes |
| The Redis contract said LIST and SET for `user:recent` and `user:bought`, but the streaming job writes ZSETs | Contract changed to ZSET with the event time as score (`CONTRACT_VERSION` 1.1.0); no code change |

### 5. Plan for next week

- A: ALS grid search (rank, regParam, alpha) on a validation week cut from the end of train (2019-11-09 to 2019-11-15), so the test split stays untouched.
- B: `sim:item`: top 50 neighbours per product from the ALS item factors, compared with the Item-kNN table.
- Fix the size of `rec:batch` (top N and which users) so it fits in Redis (2 GB limit) for the week 5 loader.

## Pair 2: Real-time pipeline

### Scope
- Task D: read Kafka JSON events with Spark Structured Streaming, parse using the shared schema, validate required fields, apply event-time watermark/deduplication, and persist checkpoints.
- Task C: use `foreachBatch` to update Redis user state for recent activity and purchased products.

### Files added
- `streaming/main.py`: streaming source, parsing, watermark, deduplication, checkpoint and Redis sink.
- `streaming/Dockerfile`, `streaming/requirements.txt`: container/runtime setup.
- `streaming/tests/test_streaming.py`: shared Redis key contract smoke tests.

### Data contracts
- Kafka topic: `events`; JSON schema from `common.schemas.spark_event_schema()`.
- Redis keys and recent-member encoding from `common.redis_keys`.
- Recent state uses `user:recent:{user_id}`; purchase state uses `user:bought:{user_id}`.

### Validation checklist
- [ ] Build the Compose streaming image.
- [ ] Start Kafka and Redis services.
- [ ] Replay a small sample of events.
- [ ] Verify streaming logs show processed batches.
- [ ] Inspect Redis keys and compare the API's expected data structure.
- [ ] Restart the streaming service and confirm checkpoint recovery.

### Notes / follow-ups
The exact Redis data structure and purchase event names must match the shared API/data contract. Validate these with the team before merging. Avoid modifying `common/` without approval.
