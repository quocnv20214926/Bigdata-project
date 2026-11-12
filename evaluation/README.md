# evaluation/: Offline Evaluation (Pair 1)

Compares recommendation models on the time-based split. Code follows the contracts in `common/`.

| File | Purpose |
|---|---|
| `metrics.py` | Precision, Recall, Hit Rate, NDCG at k (plain Python for one user, Spark for many) and catalog coverage |
| `offline.py` | Runner: interaction matrix, Popularity and Item-kNN baselines, implicit ALS, metrics, JSON report |
| `tune_als.py` | ALS parameter search (rank, regParam, alpha) on a validation week cut from the end of train, then the best parameters on test |
| `similarity.py` | `sim:item` (ALS item factors) vs Item-kNN: size, overlap, same category, next-item hit rate, item-based metrics |
| `tests/` | Metric checks against hand-computed values, and an end-to-end run on tiny data |

## Protocol

| Item | Choice |
|---|---|
| Split | train = 2019-10-01 .. 11-15, test = 11-16 .. 11-30 (`batch/split.py`) |
| Models | Popularity (global top-k by summed event weights, same list for every user); Item-kNN (`batch/item_knn.py`); implicit ALS (`batch/als.py`). Item-kNN and ALS use the same filtered matrix |
| Item-kNN | cosine between product vectors over users, r = log(1 + score); top 50 neighbours per product sharing >= 2 users; each user's 50 strongest products enter the similarity; score(u, j) = sum of r(u, i) * sim(i, j) over the user's products |
| Ground truth | distinct products a user interacted with in the test period; `--truth-events cart purchase` keeps purchase intent only |
| Evaluated users | users with ground truth who are in the filtered ALS matrix (warm users); a deterministic sample of `--eval-users` (default 100,000); both models score the same users |
| Relevance | binary: a recommended product is relevant if it is in the ground truth |
| Metrics | Precision@10/20, Recall@10/20, Hit Rate@10/20, NDCG@10/20, catalog coverage@10 |
| Already-seen products | kept in both the recommendations and the ground truth (repeat views are common in REES46); to revisit in week 7 |
| Cold users | not part of this comparison: they are served by popularity in every setup; segment analysis (new / light / heavy) is week 7 |

Metric definitions for one user with recommendations r_1..r_k and ground truth T:
`precision@k = hits/k`, `recall@k = hits/|T|`, `hit_rate@k = 1 if hits > 0`,
`ndcg@k = sum(1/log2(i+1) for relevant r_i) / (same sum for min(|T|, k) relevant items at the top)`.
Each metric is the mean over evaluated users; a user without recommendations scores 0.

## Run

```bash
# From the repository root, with MinIO running (see batch/README.md)
SPARK_DRIVER_MEMORY=14g python -m evaluation.offline --report docs/eval-week03.json
python -m evaluation.offline --input data/clean/events --no-s3 --eval-users 20000   # local Parquet
python -m pytest evaluation/tests -q
```

ALS options: `--rank`, `--reg`, `--alpha`, `--iter`, `--seed`. Item-kNN options: `--knn-neighbours`,
`--knn-max-items`, `--knn-min-common`. `--models` picks a subset of `popularity item_knn als`.
The report records the protocol, the parameters, matrix sizes, metrics and the time of each step.

## Tuning ALS (week 4)

| Item | Choice |
|---|---|
| Split | fit = 2019-10-01 .. 11-08, validation = 11-09 .. 11-15 (`batch/split.py`); the test split is not read while choosing |
| Protocol | the one above, rebuilt on the fit period: matrix and filter, warm users with validation events, the same 100,000-user sample for every trial |
| Metric | NDCG@10 (`--metric`), with Recall@20, Hit Rate@10 and coverage reported |
| Grid | alpha {1, 5, 10, 20, 40}, regParam {0.01, 0.1, 1.0}, rank {32, 64, 128}, 10 iterations |
| Search | coordinate (default): alpha at rank 32, then regParam, then rank, each keeping the best so far (9 trials); `--search grid` runs all 45 |
| Final | best parameters trained on the full train split and evaluated on test with the same users as week 3 (`--final-report`, `--model-out`) |

## sim:item vs Item-kNN (week 4)

Both tables come from the same training matrix and keep 50 neighbours per product.

| Measure | Definition |
|---|---|
| overlap@k | for a product in both tables, shared top-k neighbours / min(sizes of the two top-k lists) |
| same_category@k | share of top-k (product, neighbour) pairs with the same `category_id` |
| next_item hit@k | test sessions cut into transitions (a product, then the next different product of the session; 1,000,000 sampled); share whose next product is in the previous one's top-k. This is how the speed layer uses a similarity table |
| item-based | each table used to score users as in Item-kNN (`batch/item_knn.recommend`), same users and metrics as the offline evaluation |

```bash
SPARK_DRIVER_MEMORY=14g python -m evaluation.tune_als --report docs/eval-week04-tuning.json \
  --final-report docs/eval-week04-als.json --model-out s3a://lake/models/als/week4/
SPARK_DRIVER_MEMORY=14g python -m evaluation.similarity --model s3a://lake/models/als/week4/ \
  --sim-out s3a://lake/models/sim_item/week4/ --report docs/eval-week04-sim.json
```
