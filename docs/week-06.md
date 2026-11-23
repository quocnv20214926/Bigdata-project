# Week 06

## Pair 1: Batch

### 1. Weekly goals

- Spark optimisation of the nightly batch: the full run took 44 minutes in week 5, 39 of them in ALS training and scoring. Target under 25 minutes, losing less than 0.002 NDCG@10 against the week 4 model.
- A batch demo: one user's history next to what the batch layer recommends, with what the user did afterwards.

### 2. Completed work

| Task | Files |
|---|---|
| `recommend` scores users with numpy instead of Spark's `recommendForUserSubset`: the product factors (155,423 x 128 floats) are broadcast once, each task multiplies blocks of 128 users by them and keeps the top k with `argpartition`; ties go to the smaller product id. The old path stays as `recommend_builtin`, and a test checks both give the same lists | `batch/als.py`, `batch/tests/test_interactions.py` |
| `train` saves the filtered interaction matrix (`matrix/`), so `sim` reads it back instead of rebuilding it from the events | `batch/nightly.py` |
| ALS defaults to 5 iterations instead of 10 (results below) | `batch/als.py` |
| Knobs for the timing runs: `--als-blocks`, `--checkpoint-interval`, and `SPARK_EXTRA_CONF` for any Spark setting (e.g. the Kryo serializer) | `batch/nightly.py`, `batch/spark_session.py` |
| Demo: for a user, the latest history events, then the ALS (`rec:batch`), Item-kNN (`sim:item` of the history products) and Popularity (`pop:cat` of the user's main category, else `pop:global`) top 10 side by side, read from Redis. Products the user viewed, carted or bought in the test period are marked, and each list gets its hits and NDCG@10. `--pick` / `--pick-cold` choose test-period buyers with or without `rec:batch` | `batch/demo_recommend.py`, `batch/README.md` |
| Unit tests (9 new, 49 in total): numpy vs Spark lists, ranking ties, the demo's scoring by hand, its Spark and Redis plumbing end to end | `batch/tests/` |
| Results | `docs/eval-week06-iter5.json`, `docs/eval-week06-als-iter5.json`, `docs/nightly-week06-host.json`, `docs/demo-week06.txt`, `docs/demo-week06.json` |

### 3. Results / metrics

All runs on the laptop (Apple M1 Max, 10 cores, 32 GB; Spark local, 14 GB driver), ALS rank 128, regParam 0.1, alpha 5.

**Where the time went.** In week 4 the rank 128 trial trained 12 times longer than rank 32 (1,136 s vs 91 s), so the cost is the per-user solve of a 128 x 128 system, not the matrix products. BLAS was checked first: on a Linux test machine, ALS with native OpenBLAS, the Java fallback the laptop uses, and the Java Vector API ran within 7% of each other (106, 111 and 104 s). Rank is the main quality lever, so the iterations and the scoring step were the places to cut.

**Fewer ALS iterations.** Same protocol as week 4 (validation week 11-09..11-15, then train vs test, 100,000 users):

| | Validation NDCG@10 | Test NDCG@10 | Test Recall@10 | Test Hit Rate@10 | Test coverage@10 | Training (test run) |
|---|---|---|---|---|---|---|
| 10 iterations (week 4) | 0.1273 | 0.1206 | 0.1156 | 0.3536 | 0.0130 | 1,346 s |
| **5 iterations** | **0.1298** | **0.1226** | **0.1162** | **0.3561** | **0.0132** | **676 s** |
| Popularity | | 0.0691 | 0.0767 | 0.2595 | 0.0001 | |

Five iterations are not worse: NDCG@10 is slightly higher on both the validation week and the test split, at half the training time. More passes seem to fit the training weeks more closely without helping the next weeks. Scoring the 100,000 test users took 23 s with the numpy path (119 s in week 4).

**Spark settings.** Training on 21 days (20,436,532 pairs), 3 iterations, each setting run once, the default twice:

| Setting | ALS training | vs default |
|---|---|---|
| 10 blocks (default), Java serializer | 247.7 s and 247.6 s | |
| 20 blocks | 258.9 s | 4.5% slower |
| 40 blocks | 264.7 s | 6.9% slower |
| Kryo serializer | 251.7 s | 1.6% slower |

Nothing beat the default: on one machine every block lives in the same JVM, so more blocks only add shuffle work. The defaults stay.

**Full nightly run** (run `20261112T044512`, `as_of` 2019-11-16, 74,971,956 events):

| Step | Week 5 | Week 6 | Note |
|---|---|---|---|
| check | 8 s | 8 s | |
| popularity | 12 s | 12 s | |
| train | 1,359 s | 770 s | ALS 698 s instead of 1,299 s (5 iterations); the step also saves `matrix/` |
| recommend | 981 s | 219 s | week 6 writes top-50 for 1,083,864 users (contract 1.2.0) instead of top-100 for 566,441 |
| sim | 170 s | 168 s | |
| load | 97 s | 96 s | |
| validate | 3 s | 5 s | |
| cleanup | 0.4 s | 0.6 s | |
| **total** | **2,630 s (44 min)** | **1,279 s (21 min)** | |

With the week 5 size (566,441 users, top-100), the numpy `recommend` took 180 s against 981 s, 5.4 times faster. `sim` did not get faster from reusing the matrix: Item-kNN's pair join dominates it, not reading the events.

**Redis after the run:** 943.1 MB of 2,048 MB, 1,232,939 keys: `rec:batch` 1,083,864, `sim:item` 148,414, `pop:cat` 659, `pop:global` 1 and `meta:batch:last_run`. Twice as many users have a batch list as in week 5, in slightly less memory, because top-50 lists replaced top-100.

**Demo** (`python -m batch.demo_recommend --pick 3 --pick-cold 1`, output in `docs/demo-week06.txt`):

Three test-period buyers that have `rec:batch`, picked at random (seed 42), and one buyer without it. Hits and NDCG@10 against every product the user touched from 11-16 to 11-30:

| User | Before 11-16 | 11-16 to 11-30 | ALS | Item-kNN | Popularity |
|---|---|---|---|---|---|
| 515813436 | 142 events on 61 products, mostly Apple smartphones, 10 carts | 8 products, 14 carts | 5 hits (4 carted), 0.577 | 5 hits (3 carted), 0.702 | 2 hits, 0.217 (smartphones) |
| 563535197 | 26 views of blenders | 3 products, carted blender 3100074 | 1 hit at rank 5, 0.181 | 1 hit at rank 8, 0.148 | 1 hit at rank 5, 0.181 |
| 515425546 | 129 events on 78 products, last visits on shoes | 55 products, 1 purchase | 1 hit at rank 10, 0.064 | 0 | 0 (memory cards) |
| 573026457 | none: a new user | 1 product, carted | no `rec:batch` | no history | `pop:global`, 0 |

- The lists stay in the user's category: Apple smartphones for the first user, blenders for the second.
- Most ALS products are already in the user's history (all ten for user 515813436). In REES46 people come back to products they viewed, so this pays off (4 of the 5 hits above), but the list shows few new products. Week 7 scores the lists with seen products removed.
- The fallback category is the one with the largest summed event weight in the whole history, so user 515425546 gets memory cards although the last visits were on shoes. Weighting recent events would fix it; noted for later.
- A new user only gets `pop:global` from the batch layer; after the first events the speed layer (Pair 2) takes over.

### 4. Issues & resolutions

| Issue | Resolution |
|---|---|
| `recommendForUserSubset` took 16 minutes for 566,441 users | Numpy block scoring with broadcast product factors (`als.recommend`): 180 s for the same users, same lists |
| ALS on the laptop runs the Java BLAS fallback (no native library on macOS) | Measured on Linux: native BLAS changes little for this workload, so not pursued |
| More ALS blocks and the Kryo serializer were expected to help | Measured slower in local mode; defaults kept, the knobs stay for a cluster |
| Event times collected from Spark come back in the machine's time zone | The demo formats times inside Spark (UTC session time zone) |
| The `pop:cat` key did not fit the demo's column header | Printed on its own line above the lists |

### 5. Plan for next week

- Final offline evaluation of ALS against Popularity and Item-kNN with bootstrap confidence intervals.
- Results by user segment: new, light (1 to 5 interactions) and heavy users.
- Revisit keeping already-seen products in the lists and in the ground truth (week 3 choice): the demo shows ALS lists made mostly of products the user has seen.
