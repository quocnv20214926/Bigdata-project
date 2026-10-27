# evaluation/: Offline Evaluation (Pair 1)

Compares recommendation models on the time-based split. Code follows the contracts in `common/`.

| File | Purpose |
|---|---|
| `metrics.py` | Precision, Recall, Hit Rate, NDCG at k (plain Python for one user, Spark for many) and catalog coverage |
| `offline.py` | Runner: interaction matrix, Popularity baseline, implicit ALS, metrics, JSON report |
| `tests/` | Metric checks against hand-computed values, and an end-to-end run on tiny data |

## Protocol

| Item | Choice |
|---|---|
| Split | train = 2019-10-01 .. 11-15, test = 11-16 .. 11-30 (`batch/split.py`) |
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

ALS options: `--rank`, `--reg`, `--alpha`, `--iter`, `--seed`. The report records the protocol,
ALS parameters, matrix sizes, metrics and the time of each step.
