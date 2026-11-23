# Week 07

## Pair 1: Batch

### 1. Weekly goals

- Final offline evaluation of ALS against Popularity and Item-kNN, with 95% confidence intervals.
- Results by user segment: new, light (1 to 5 products in train) and heavy users.
- Revisit the week 3 choice of keeping already-seen products in the lists and in the ground truth: the week 6 demo showed ALS lists made mostly of products the user had seen.

### 2. Completed work

| Task | Files |
|---|---|
| `per_user_metrics`: precision, recall, hit rate and NDCG at k for each user; `ranking_metrics` now averages it, so the week 3 to 6 numbers are unchanged | `evaluation/metrics.py` |
| Percentile bootstrap over users (1,000 resamples, seed 42) for every mean, and a paired bootstrap for the difference between two models scored on the same users | `evaluation/bootstrap.py` |
| Final evaluation: the week 3 warm sample plus 30,000 test users of each segment; a model without a list, or with a short one, is completed with the Popularity list, as the API does when `rec:batch` is missing; an "unseen" variant removes training products from the lists (refilled from each model's top 120) and from the ground truth | `evaluation/final_eval.py`, `evaluation/README.md` |
| Unit tests (9 new, 58 in total): bootstrap against known cases, segments and sampling, list completion, the unseen filter, an end-to-end run on tiny data with all three segments | `evaluation/tests/test_final_eval.py` |
| Results | `docs/eval-week07.json` (ground truth: views, carts, purchases), `docs/eval-week07-purchase.json` (carts and purchases) |

### 3. Results / metrics

Same split as weeks 3 to 6 (train 2019-10-01 .. 11-15, test 11-16 .. 11-30), ALS rank 128, regParam 0.1, alpha 5, 5 iterations, on the laptop (14 GB driver). The run took 20 minutes, 12 of them training ALS. Intervals are 95% bootstrap intervals over users; a difference is clear when its interval excludes 0.

**Warm users** (the week 3 sample: 100,000 test users who are in the ALS matrix). ALS reproduces the week 6 number (0.1226) and its lead over both baselines is far outside the noise:

| Model | NDCG@10 | Recall@20 | Hit Rate@10 | Precision@10 | Coverage@10 |
|---|---|---|---|---|---|
| Popularity | 0.0691 [0.0681, 0.0699] | 0.1074 | 0.2595 | 0.0406 | 0.0001 |
| Item-kNN | 0.0909 [0.0899, 0.0920] | 0.1367 | 0.3022 | 0.0606 | 0.3612 |
| **ALS** | **0.1226 [0.1213, 0.1239]** | **0.1497** | **0.3561** | **0.0667** | 0.0132 |
| ALS − Popularity | +0.0535 [0.0521, 0.0550] | +0.0423 [0.0405, 0.0443] | +0.0966 | | |
| ALS − Item-kNN | +0.0316 [0.0305, 0.0327] | +0.0130 [0.0116, 0.0143] | +0.0539 | | |

**By segment.** 2,438,946 users have test events: 1,111,573 new (46%), 521,763 light (21%) and 805,610 heavy (33%). NDCG@10 on 30,000 users of each:

| Segment | own ALS list | own Item-kNN list | Popularity | Item-kNN | ALS | ALS − Popularity |
|---|---|---|---|---|---|---|
| new | 0% | 0% | 0.0599 | 0.0599 | 0.0599 | 0 |
| light | 67% | 99.85% | 0.0711 | 0.0600 | **0.0957** | +0.0245 [0.0221, 0.0270] |
| heavy | 100% | 100% | 0.0678 | 0.1010 | **0.1287** | +0.0609 [0.0585, 0.0632] |

- Almost half of the test users are new. The batch layer can only give them the Popularity list (all three columns are the same list), so they are the speed layer's job.
- Light users still gain from ALS, although a third of them are outside the ALS matrix and get Popularity. Item-kNN, which reads their full history, is below Popularity for them: a few products give a narrow set of neighbours.
- Heavy users gain the most. The segment samples are drawn in the same hash order as the warm sample, so the sampled heavy users, and the light ones who are in the ALS matrix, are also warm users. Each sample is still a random sample of its group; the overlap only means 139,892 users were scored instead of 190,000.

**Without already-seen products** (training products removed from lists and ground truth):

| Group | Users | Popularity | Item-kNN | ALS | ALS − Popularity | ALS − Item-kNN |
|---|---|---|---|---|---|---|
| warm | 94,448 | 0.0560 | **0.0685** | 0.0534 | −0.0026 [−0.0039, −0.0013] | −0.0151 [−0.0159, −0.0142] |
| light | 28,297 | **0.0635** | 0.0613 | 0.0553 | −0.0082 [−0.0102, −0.0061] | −0.0060 [−0.0077, −0.0042] |
| heavy | 28,401 | 0.0525 | **0.0694** | 0.0545 | +0.0020 [−0.0002, +0.0041] | −0.0150 [−0.0164, −0.0134] |

- ALS loses its whole lead: NDCG@10 falls from 0.1226 to 0.0534, below Item-kNN and even slightly below Popularity. Products the user had already touched in train are only 10% of the ground truth pairs of the scored users (1,254,033 pairs, 1,130,056 without them), but ALS ranks them high, and that is where its hits come from. On the other products, which 94% of warm users touch, ALS does no better than Popularity.
- Item-kNN is the best model for products the user has not seen, and it also spreads over the catalogue: its top 10 lists cover 36% of the products against 1.3% for ALS.
- So the two models do different jobs. `rec:batch` keeps ALS, which predicts the next two weeks best, and `sim:item` keeps Item-kNN, which the speed layer can propagate from the products a user is viewing now. Week 8 measures that combination on cart and purchase events.

**Purchase intent** (ground truth = products carted or bought in the test period, `docs/eval-week07-purchase.json`):

565,881 users carted or bought something in the test period: 172,660 new (31%), 120,531 light (21%) and 272,690 heavy (48%). NDCG@10:

| Group | Users | Popularity | Item-kNN | ALS | ALS − Popularity |
|---|---|---|---|---|---|
| warm | 100,000 | 0.0686 | 0.0838 | **0.1374** | +0.0688 [0.0671, 0.0705] |
| warm, unseen | 79,499 | 0.0524 | **0.0616** | 0.0446 | −0.0078 [−0.0094, −0.0061] |
| new | 30,000 | 0.0930 | 0.0930 | 0.0930 | 0 |
| light | 30,000 | 0.0937 | 0.0734 | **0.1381** | +0.0444 [0.0414, 0.0477] |
| light, unseen | 26,370 | **0.0769** | 0.0739 | 0.0646 | −0.0122 [−0.0150, −0.0097] |
| heavy | 30,000 | 0.0623 | 0.0847 | **0.1336** | +0.0713 [0.0681, 0.0742] |
| heavy, unseen | 23,213 | 0.0464 | **0.0559** | 0.0408 | −0.0056 [−0.0082, −0.0029] |

- The same picture, sharper: on carts and purchases the ALS lead over Popularity is larger (+0.069 for warm users, Recall@20 0.2431 against 0.1587). A fifth of the carted or bought pairs are products the user had touched in train (254,266 pairs, 203,960 without them); without them ALS is below both baselines in every segment.
- For new users Popularity does better on purchases (0.0930) than on views (0.0599): purchases concentrate on best-sellers.

### 4. Issues & resolutions

| Issue | Resolution |
|---|---|
| The first full run ran out of the 14 GB driver heap inside ALS after 12 minutes: the Popularity and Item-kNN lists of ~140,000 users (120 products each) were cached before training | ALS now trains right after the matrix is built; the unfiltered interactions and the ground truth of all test users are kept on disk; the similarity table and the matrix are released once the Item-kNN lists exist |
| Models without a list for a user would score 0 and make the segment comparison meaningless | Every list is completed with the Popularity ranking, as the API will do |
| Removing seen products from ALS's top 20 would leave short lists | Each model keeps its top 120, so the lists are refilled from deeper in its own ranking before Popularity |

### 5. Plan for next week

- Batch only vs batch + speed, replaying cart and purchase events of a validation day (11-09) and the test day (11-16) with the speed layer emulated (`sim:cooc`, `user:recent`, `rec:rt`) and `common.scoring.merge_scores`; choose alpha and the half-life on validation.
- Similarity ablation for the speed layer: `sim:cooc`, `sim:item` (Item-kNN) and a hybrid of the two.

## Pair 2: Real-time

### 1. Weekly goals

- **C:** monitoring: Prometheus metrics of the API and the speed layer, Grafana dashboard; Kafka UI.
- **D:** speed-only evaluation: replay the events and measure the next-item Hit Rate@K.
- Deliverable: speed-only evaluation results; monitoring dashboard.

### 2. Completed work

| Task | Files | Owner |
|---|---|---|
| API metrics at `/metrics`: requests by route template and status, latency histogram, `/recommend` requests by available layers, recommended items by source. Shared by the 2 uvicorn workers (`PROMETHEUS_MULTIPROC_DIR`) | `serving/app/metrics.py`, `serving/app/main.py`, `serving/Dockerfile` | C |
| Speed-layer exporter: stream progress from `rt:meta:stream` (events, rows and duration of the last micro-batch, stream clock, trending window), Kafka end offsets per partition, Redis memory, keys, keys per prefix (SCAN every minute, off the scrape path) | `streaming/monitoring/exporter.py` | C |
| Prometheus (scrape every 5 s) and Grafana 13 with a provisioned dashboard of 23 panels: overview (requests/s, `/recommend` p95 against 100 ms, 5xx rate, stream clock, time since the last micro-batch), API, speed layer (input vs processed events/s, backlog, micro-batch duration, replay speed), Redis | `infra/monitoring/`, `infra/compose.stream.yml` | C |
| Replay evaluation through the **production code**: the events go through the real sinks (`user_state`, `products`, `cooc`, `realtime`, `trending`) on a dedicated Redis, in micro-batches of 30 s of event time; the lists are read with the API's `RedisRepository`. Every list is taken from the state before the micro-batch of the target event | `streaming/evaluation/replay_eval.py` | D |
| Tests: 3 exporter tests, 3 replay tests (metrics, trending windows, learning a co-view on a synthetic log), 1 `/metrics` test | `streaming/tests/`, `serving/tests/` | C, D |

**Protocol.** A target is an event of a user who had an event in the last 24 h, on a product different from the user's previous one (the "next item"). `unseen`: the product is not among the user's 20 recent events. Methods, top K: `trending` (`trend:global`), `trending_cat` (trending in the category of the user's last product, then global), `cooc_last` (`sim:cooc` of the last product only), `rec_rt` (`rec:rt` alone), `api` (`/recommend` without `rec:batch`: `rec:rt`, purchases removed, filled from trending). Metrics: HR@K, NDCG@K, coverage (share of targets with a non-empty list).

### 3. Results / metrics

Tests: 54 / 54 pass (`streaming`, `serving`, `ingestion`, `ui`).

**Speed-only evaluation.** October sample (199,862 events, Oct 1 00:00–06:06), first 3 hours as warm-up, half of the users evaluated: 47,310 targets (47,283 views, 23 purchases, 4 carts). `rec:rt` half-life 5 minutes (best value, see below), 20 neighbours per event. `docs/eval-pair2-week07.json`.

| Method | Coverage | HR@10 | NDCG@10 | HR@20 | NDCG@20 |
|---|---:|---:|---:|---:|---:|
| `trending` | 100 % | 0.0397 | 0.0197 | 0.0605 | 0.0250 |
| `trending_cat` | 100 % | 0.1601 | 0.0817 | 0.2410 | 0.1020 |
| `cooc_last` | 94.7 % | 0.1902 | **0.1079** | 0.2539 | 0.1239 |
| `rec_rt` | 95.1 % | 0.1901 | 0.1021 | 0.2625 | 0.1204 |
| **`api`** | 100 % | **0.1964** | 0.1050 | **0.2769** | **0.1253** |

| Unseen products only (40,352 targets) | HR@10 | HR@20 |
|---|---:|---:|
| `trending_cat` | 0.1272 | 0.1997 |
| `cooc_last` | 0.1279 | 0.1813 |
| `rec_rt` | 0.1283 | 0.1889 |
| `api` | **0.1364** | **0.2056** |

- What users get from the speed layer (`api`) finds the next product in the top 10 for **19.6 %** of the targets: 5× global trending and +23 % over trending in the user's category.
- `rec:rt` alone equals the neighbours of the last product at K = 10 and is better at K = 20 and on unseen products: the decayed history adds candidates beyond the last product. The fallback adds the 5 % of targets without `rec:rt` (products with no co-views yet).
- Purchases (23 targets, too few for a conclusion): `rec:rt` HR@10 0.30 against 0.04 for trending in the category.

**Half-life of `rec:rt`** (same replay, `docs/eval-pair2-week07-halflife-*.json`):

HALF_LIFE_TABLE

**Monitoring.** The API and exporter metrics were checked on the local run (the dashboard's 25 PromQL expressions parse, every metric they use is exposed; `kafka_topic_end_offset` needs Kafka, so it is only seen in Docker). The dashboard itself has to be checked in Docker (section 4 of `docs/run-guide.md`).

### 4. Issues & resolutions

| Issue | Resolution |
|---|---|
| A separate simulation of the speed layer could drift from the real job | The replay runs the production sinks and the API's repository; only Spark's grouping into micro-batches and the trending aggregation are re-done in Python |
| First results favoured `cooc_last`: it used the event just before the target even when that event was in the same micro-batch, which `rec:rt` cannot have seen yet | Every method now reads the user's last product from `user:recent`, i.e. the same state as `rec:rt` |
| With the default half-life (30 min) `rec:rt` was below `cooc_last` | Next-item intent changes within minutes; half-life tuned on the replay (table above) |
| The replay runs about 400–500 events/s on 2 cores (the real sinks, one micro-batch at a time) | `--warmup-batch-seconds` (600 s) for the warm-up, `--sample` for the evaluated users; the full-day run is part of the run guide |
| Spark does not commit offsets to Kafka, so there is no consumer lag metric | The exporter publishes the topic's end offsets and the processed events; the dashboard shows both rates and the approximate backlog |
| Two uvicorn workers would each expose their own counters | Prometheus multiprocess mode (`PROMETHEUS_MULTIPROC_DIR`, emptied at each start) |

### 5. Plan for next week

- **C:** integration in the API (batch + real-time merge with Pair 1's `rec:batch`), Streamlit page 3 (evaluation results).
- **D:** end-to-end compose of the whole system, `sim:item` in real-time scoring (`cooc` / `item` / `hybrid`).
