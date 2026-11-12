# Week 08

## Pair 1: Batch

### 1. Weekly goals

- Batch only vs batch + speed (`docs/plan.md` section 7): replay cart and purchase events with the lists the API would have returned just before each one, and choose alpha and the half-life on a validation day.
- Similarity ablation for the speed layer: `sim:cooc`, `sim:item` (Item-kNN) and a hybrid of the two.

### 2. Completed work

| Task | Files |
|---|---|
| Offline replay of one day: batch layer built from the events before the day (ALS `rec:batch`, Item-kNN `sim:item`, `pop:global`); speed layer emulated just before each target event (`sim:cooc` counted as `streaming/sinks/cooc.py` does, `user:recent`, `user:bought`, `rec:rt` from `common.scoring.propagate`); lists from `common.scoring.merge_scores`, as the API will build them | `evaluation/replay.py` |
| Grid: popularity, batch only, speed only and batch + speed (alpha 0.1 to 0.95), similarity cooc / item / hybrid, half-life 5 min to 24 h; chosen on 11-09 by NDCG@10, reported on 11-16 with bootstrap intervals and paired differences, by group (user with `rec:batch`, `user:recent`, both or neither) | `evaluation/replay.py`, `evaluation/README.md` |
| `--cache-dir` / `--only`: each replayed day is saved and reused, so a failed run restarts from the saved day and each day can run in its own Spark process | `evaluation/replay.py` |
| "+self" variants (the products in `user:recent` themselves added to `rec:rt`) and two counts per day: targets whose product is already in `user:recent` or in `rec:batch` | `evaluation/replay.py` |
| `--targets-mode new`: one event per user on a product the user has never touched, with every product touched before left out of every list | `evaluation/replay.py`, `evaluation/README.md` |
| Unit tests (14): `sim:cooc` counts and similarities against the streaming sink (fakeredis), `propagate`, the API merge, target picking in both modes, the saved days, two end-to-end runs on tiny data | `evaluation/tests/test_replay.py` |
| Results | `docs/eval-week08-replay.json` (cart and purchase targets), `docs/eval-week08-new.json` (new products) |

### 3. Results / metrics

Validation day 2019-11-09 (models from 10-01 .. 11-08), test day 2019-11-16 (models from 10-01 .. 11-15), 30,000 users per day, ALS rank 128 with 5 iterations, on the laptop (14 GB driver). One run of both days takes 35 to 46 minutes, about 20 of them training ALS twice. Intervals are 95% bootstrap intervals over targets.

**Cart and purchase targets** (the plan's protocol; 26,199 carts and 3,801 purchases on 11-16). NDCG@10 and Hit Rate@10 on the test day, each variant with the half-life and alpha chosen on validation:

| Variant | Chosen on 11-09 | NDCG@10 | Hit Rate@10 | Hit Rate@20 |
|---|---|---|---|---|
| Popularity | | 0.0600 [0.0580, 0.0620] | 0.1155 | 0.1597 |
| Batch only (ALS) | | **0.1281 [0.1250, 0.1315]** | 0.2114 | 0.2681 |
| Speed only, cooc | 24 h | 0.0515 [0.0497, 0.0532] | 0.1206 | 0.2301 |
| Batch + speed, cooc | 24 h, alpha 0.95 | 0.1163 [0.1134, 0.1193] | 0.2002 | 0.2655 |
| Batch + speed, item | 24 h, alpha 0.95 | 0.1158 [0.1129, 0.1189] | 0.1991 | 0.2634 |
| Batch + speed, hybrid | 24 h, alpha 0.95 | 0.1162 [0.1133, 0.1192] | 0.2001 | 0.2651 |
| Speed only, cooc + self | 5 min | 0.8770 [0.8741, 0.8800] | 0.9359 | 0.9387 |
| Batch + speed, cooc + self | 5 min, alpha 0.1 | 0.8765 [0.8734, 0.8795] | 0.9361 | 0.9391 |

- Without "+self", every batch + speed variant is below batch only (cooc: −0.0118 [−0.0133, −0.0105]), and validation pushed alpha and the half-life to the edge of the grid, i.e. as close to batch only as the grid allows. Speed only is below Popularity on NDCG@10.
- The reason is in the data: 29,438 of the 30,000 targets (98%) are for a product already in `user:recent`, the user's 20 newest events before the cart (29,765 on the validation day). In this shop a cart almost always follows a view of the same product. `propagate` gives the neighbours of the viewed products, never the products themselves, so `rec:rt` pushes the right product down the list.
- With "+self" the speed layer reaches 0.877: re-showing the products just viewed, newest first, predicts almost every cart. This is a real effect (a "recently viewed" row would be shown at the right time), but it is not recommendation quality: no model is needed for it.
- The three similarities are within 0.001 of each other everywhere (speed only: item − cooc −0.0008 [−0.0016, −0.0001], hybrid − cooc +0.0001 [−0.0005, +0.0007]).
- ALS has the carted product in its top 50 for 31% of the targets (9,381), which is why batch only stays the best list without "+self".

**Products the user has never touched** (`--targets-mode new`; every product the user touched before the target is left out of every list, as in week 7's "unseen" variant):

On 11-16 the 30,000 targets are 29,877 views, 94 carts and 29 purchases (a first touch is almost always a product page view), so this measures which new product the user opens next. None of them is in `user:recent`, by construction.

| Variant | Chosen on 11-09 | NDCG@10 | Hit Rate@10 | Hit Rate@20 |
|---|---|---|---|---|
| Popularity | | 0.0396 [0.0378, 0.0412] | 0.0742 | 0.1027 |
| Batch only (ALS) | | 0.0454 [0.0436, 0.0473] | 0.0833 | 0.1188 |
| Speed only, cooc | 5 min | **0.1351 [0.1319, 0.1382]** | **0.2404** | **0.3178** |
| Speed only, item | 5 min | 0.1317 [0.1287, 0.1348] | 0.2352 | 0.3121 |
| Speed only, hybrid | 5 min | 0.1340 [0.1309, 0.1370] | 0.2380 | 0.3171 |
| Batch + speed, cooc | 5 min, alpha 0.1 | 0.1328 [0.1299, 0.1358] | 0.2348 | 0.3128 |
| Batch + speed, hybrid | 5 min, alpha 0.1 | 0.1318 [0.1289, 0.1348] | 0.2325 | 0.3127 |

| Difference (NDCG@10) | Mean [95% CI] |
|---|---|
| Batch + speed (hybrid) − batch only | +0.0863 [+0.0835, +0.0891] |
| Batch + speed, cooc − speed only, cooc | −0.0023 [−0.0036, −0.0011] |
| Speed only: item − cooc | −0.0034 [−0.0045, −0.0023] |
| Speed only: hybrid − cooc | −0.0011 [−0.0019, −0.0002] |
| Batch only − popularity | +0.0059 [+0.0039, +0.0081] |

By group on the test day (NDCG@10):

| Group | Targets | Popularity | Batch only | Speed only, cooc | Batch + speed, cooc |
|---|---|---|---|---|---|
| `rec:batch` and `user:recent` | 16,287 | 0.0225 | 0.0379 | 0.1593 | **0.1596** |
| `rec:batch` only | 5,569 | **0.0624** | 0.0491 | 0.0624 | 0.0491 |
| `user:recent` only | 4,369 | 0.0328 | 0.0328 | **0.1787** | 0.1787 |
| neither | 3,775 | 0.0873 | 0.0873 | 0.0873 | 0.0873 |

- Here the speed layer is what works: three times the NDCG@10 of ALS. The product a user opens next is mostly a neighbour of the product they have just viewed, and `propagate` with a short half-life finds it. On validation, NDCG@10 falls steadily as the half-life grows (speed only, cooc: 0.1128 at 5 min, 0.1036 at 24 h) and as alpha grows (0.1110 at alpha 0.1, 0.0651 at 0.95).
- ALS barely beats Popularity on products the user has not seen, as in week 7 (0.0534 vs 0.0560 there). Adding it to the speed list costs a little (−0.0023) on average, and for users with both lists the two are about the same (0.1596 vs 0.1593).
- `user:recent` only (4,369 targets, mostly users outside the ALS matrix) is where the speed layer matters most: 0.1787 against 0.0328 for the Popularity list they would get from the batch layer alone. These are mostly the new users that week 7 left to the speed layer (46% of the test users there).
- `sim:cooc` is slightly better than Item-kNN (`sim:item`) and the hybrid, but the gaps are small (0.001 to 0.003).

**What this means for the API merge.** The two protocols pull alpha in opposite directions: carts want the batch list (alpha 0.95), new products want the speed list (alpha 0.1, half-life 5 min). Together they suggest two rows rather than one tuned blend: the `user:recent` products ("recently viewed", which predicts most carts) and `rec:rt` merged with a low alpha for discovery. The defaults in `common/scoring.py` (alpha 0.5, half-life 30 min) are between the two; changing them or the API layout is Pair 2's call, so Pair 1 only reports the numbers.

### 4. Issues & resolutions

| Issue | Resolution |
|---|---|
| The API's Redis mode (merge in the API) is not on `master` yet | The replay calls the same `common/scoring.py` functions (`propagate`, `merge_scores`) the API will use; a test checks the emulated `sim:cooc` against `streaming/sinks/cooc.py` |
| A full run keeps the shuffle files of both days on the laptop disk (free space fell to 18 GB) | `--cache-dir` saves each day as soon as it is replayed; `--only` replays one day per Spark process |
| The first run put batch + speed below batch only and chose the edges of the grid (alpha 0.9, half-life 12 h) | Added alpha 0.95, a 24 h half-life and "+self"; the second run showed that the cart targets are almost all products in `user:recent`, which led to the new-product mode |

### 5. Plan for next week

- Scale test: the nightly batch on 6, 12, 23 and 46 days of data (time and sizes per step).
- Final report sections for the batch layer and the evaluation.
