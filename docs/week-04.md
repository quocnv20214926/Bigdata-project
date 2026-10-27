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
