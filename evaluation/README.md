# evaluation/: Offline Evaluation (Pair 1)

Compares recommendation models on the time-based split. Code follows the contracts in `common/`.

| File | Purpose |
|---|---|
| `metrics.py` | Precision, Recall, Hit Rate, NDCG at k (plain Python for one user, Spark for many, per user or averaged) and catalog coverage |
| `offline.py` | Runner: interaction matrix, Popularity and Item-kNN baselines, implicit ALS, metrics, JSON report |
| `tune_als.py` | ALS parameter search (rank, regParam, alpha) on a validation week cut from the end of train, then the best parameters on test |
| `similarity.py` | `sim:item` (ALS item factors) vs Item-kNN: size, overlap, same category, next-item hit rate, item-based metrics |
| `bootstrap.py` | 95% percentile bootstrap intervals over users, and paired intervals for the difference of two models |
| `final_eval.py` | Final evaluation (week 7): the three models with bootstrap intervals, by user segment, with and without already-seen products |
| `replay.py` | Batch only vs batch + speed (week 8): cart and purchase events replayed with the speed layer emulated, `common.scoring.merge_scores`, alpha and half-life chosen on validation, cooc / item / hybrid similarity |
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
| Already-seen products | kept in both the recommendations and the ground truth (repeat views are common in REES46); week 7 also scores them removed (below) |
| Cold users | not part of this comparison: they are served by popularity in every setup; week 7 scores them by segment (below) |

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

## Final evaluation (week 7)

Same split, ground truth and metrics as above, with three additions.

| Item | Choice |
|---|---|
| Intervals | each metric is first computed per user (`metrics.per_user_metrics`); the mean over users gets a 95% percentile bootstrap interval (1,000 resamples of the users, seed 42). Two models are compared on the per-user differences, resampled the same way (paired bootstrap): the difference is clear when its interval excludes 0 |
| Segments | by distinct training products with a positive weight: new (0), light (1 to 5), heavy (more than 5). 30,000 test users of each segment (`--segment-users`), drawn with the seed |
| Warm group | the week 3 sample (100,000 test users in the ALS matrix, `--eval-users`), so its numbers compare with weeks 3 to 6 |
| Fallback | a model without a list for a user, or with a short one, is completed with the Popularity list, as the API does when `rec:batch` is missing. Item-kNN reads the unfiltered history, so users outside the ALS matrix can still get its list |
| Unseen variant | products the user interacted with in train are removed from the lists (refilled from the model's top 120, then Popularity) and from the ground truth; users left without ground truth drop out |
| Report | per group (warm, new, light, heavy) and variant: users, the share of users with an own ALS / Item-kNN list, every metric as mean [low, high] for each model, and the differences ALS - Popularity, ALS - Item-kNN, Item-kNN - Popularity |

```bash
SPARK_DRIVER_MEMORY=14g python -m evaluation.final_eval --report docs/eval-week07.json
python -m evaluation.final_eval --input data/clean/events --no-s3 --eval-users 20000 --segment-users 5000
```

## Batch only vs batch + speed (week 8)

The experiment of `docs/plan.md` section 7: at each cart or purchase event, the list the API would
have returned just before it. The API's Redis mode is not on `master` yet, so the replay runs the
functions of `common/scoring.py` that it will use.

| Item | Choice |
|---|---|
| Days | validation 2019-11-09 (models from 10-01 .. 11-08), test 2019-11-16 (models from 10-01 .. 11-15): the morning after a nightly run |
| Targets | one cart or purchase event per user on the day, 30,000 users (`--targets`); relevant product = the product of that event |
| `--targets-mode new` | almost every cart is for a product already in `user:recent` (29,438 of 30,000 on 11-16), so the plan's targets mostly measure showing the products just viewed again. This mode takes instead one event per user whose product the user has never touched before, and leaves every product touched before the target out of every list (week 7's "unseen" filter): `rec:batch` is the ALS top 50 without them, out of the top 150, and `rec:rt` the top 50 without them. There are no "+self" variants, since the `user:recent` products are left out |
| Batch layer | ALS top 50 with scores (`rec:batch`), Item-kNN top 50 (`sim:item`), `pop:global`, all from the events before the day |
| `sim:cooc` | counted as `streaming/sinks/cooc.py` does (last 10 distinct products of a session, `cooc / sqrt(cnt_i * cnt_j)`, clipped at 1, top 50), from the events before the day, so frozen like `sim:item`; a test checks the counts and similarities against the streaming code |
| `user:recent`, `user:bought` | the user's 20 newest events before the target, if the newest is under 24 h old; the products bought before it, if the last purchase is under 30 days old |
| `rec:rt` | `rt[j] = sum of w_e * sim(i, j) * exp(-ln 2 * age / half_life)` over the `user:recent` events (`common.scoring.propagate`), top 50 |
| Variants | popularity only; batch only; speed only; batch + speed for alpha 0.1 .. 0.9 and 0.95; each with similarity cooc, item or hybrid (0.5 * item + 0.5 * cooc), each also "+self" (sim(i, i) = 1 added for the `user:recent` products, so `rec:rt` holds the products just viewed, which `propagate` leaves out), and half-life 5 min, 30 min, 2 h, 12 h or 24 h. Lists come from `merge_scores` (min-max per source; alpha 1 without `rec:rt`, 0 without `rec:batch`), without `user:bought`, completed with `pop:global` |
| Selection | for each similarity, the half-life (speed only) and the half-life and alpha (batch + speed) with the best NDCG@10 on the validation day; the test day reports those with 95% bootstrap intervals over targets and paired differences, plus the whole grid |
| Groups | targets whose user has `rec:batch` and `user:recent`, only one of them, or neither; the report also counts the targets whose product is already in `user:recent` or in `rec:batch` |

```bash
SPARK_DRIVER_MEMORY=14g python -m evaluation.replay --report docs/eval-week08-replay.json --cache-dir /tmp/replay-days
# one Spark process per day (less disk for shuffle files), then the report from the two saved days
python -m evaluation.replay --cache-dir /tmp/replay-days --only validation
python -m evaluation.replay --cache-dir /tmp/replay-days --only test
python -m evaluation.replay --cache-dir /tmp/replay-days --report docs/eval-week08-replay.json
# products the user has never touched (the days are saved as validation-new and test-new)
SPARK_DRIVER_MEMORY=14g python -m evaluation.replay --targets-mode new --report docs/eval-week08-new.json --cache-dir /tmp/replay-days
```
