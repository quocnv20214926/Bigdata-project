# Week 10

## Pair 1: Batch

### 1. Weekly goals

- Full-dataset scale test (`docs/assignment.md`, week 10, optional): how the nightly batch time and sizes grow with the amount of data.
- Review of the final report sections written in week 9.

### 2. Completed work

| Task | Files |
|---|---|
| Nightly Spark steps (check, popularity, train, recommend, sim) on the last 6, 12 and 23 days before 2019-11-16, each step its own process as in the Airflow DAG, on a separate MinIO root (`s3a://lake/scale/`, deleted afterwards); the 46-day point is the week 6 nightly run | `docs/scale-week10.json` |
| Final report, Pair 1 part: the scale test added as section 2.6 | shared report document |

### 3. Results / metrics

Laptop, Spark local mode, 14 GB driver, MinIO in Docker; ALS rank 128, 5 iterations, 10 blocks; `rec:batch` top 50; `sim:item` from Item-kNN. Times in seconds.

| Days | Events | Users | ALS pairs | ALS users | ALS products | check | popularity | train | recommend | sim | Total |
|---|---|---|---|---|---|---|---|---|---|---|---|
| 6 | 17,217,420 | 1,257,530 | 8,560,406 | 872,137 | 99,589 | 4.7 | 9.1 | 223.9 | 118.4 | 40.2 | **396** (6.6 min) |
| 12 | 27,986,684 | 1,994,735 | 14,143,191 | 1,374,508 | 118,561 | 5.4 | 10.0 | 333.6 | 217.3 | 73.2 | **640** (10.7 min) |
| 23 | 42,977,816 | 2,832,080 | 21,840,588 | 1,938,207 | 134,030 | 6.4 | 10.7 | 455.7 | 168.1 | 91.1 | **732** (12.2 min) |
| 46 | 74,971,956 | 4,205,076 | 38,195,148 | 2,890,519 | 155,423 | 7.9 | 12.3 | 770.2 | 218.9 | 168.1 | **1,177** (19.6 min) |

- From 6 to 46 days the events grow 4.4 times and the total time 3.0 times. Train is more than half of every run and grows with the number of ALS pairs (4.5 times the pairs, 3.4 times the time).
- `recommend` writes at most 1,083,864 users (the Redis budget of week 5), which every window from 12 days on reaches, so this step stops growing. The 12-day run (217 s) was slower than the 23-day one (168 s) for the same number of users; we did not look further into it, one run per point is not enough to separate it from noise.
- `sim` grows with the products kept in the matrix (99,589 to 155,423) and the pairs.
- With `load` and `validate` (96 s and 5 s in week 6), a nightly run on the full 46 days takes about 21 minutes.

### 4. Issues & resolutions

| Issue | Resolution |
|---|---|
| The scale runs must not touch the tables the API's nightly run uses | Separate root `s3a://lake/scale/` and stats folder `data/scale-runs/`; `load` and `validate` were not run; `cleanup --keep-runs 0` on that root removed the three runs |
| Only one run per window | Enough to see the trend; the variation between runs of the same window is not measured |

### 5. Next steps

- Answer review comments on the Pair 1 PRs.
- Final report review with Pair 2 and the defense rehearsal (whole team).
