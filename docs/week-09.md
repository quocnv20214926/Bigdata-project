# Week 09

## Pair 1: Batch

### 1. Weekly goals

- Final report sections for the data, the batch layer and the evaluation results (`docs/assignment.md`, week 9).

### 2. Completed work

| Section of the final report | Content | Sources |
|---|---|---|
| 1. Data | REES46 October and November 2019, EDA findings that shaped the design (sparsity, event mix, missing fields, ALS filter), ETL and the time split | `notebooks/01_eda.ipynb`, `docs/etl-report.json`, `docs/week-02.md` |
| 2. Batch layer | interaction matrix and weights, Popularity, ALS and its tuning, Item-kNN for `sim:item`, Redis budget and loader, the nightly steps and the Airflow DAG, the week 6 speed-up, the demo script | `docs/week-03.md` to `docs/week-06.md`, `docs/nightly-week06-host.json` |
| 3. Evaluation of the batch layer | protocol, warm users, new / light / heavy segments, the "unseen" variant, carts and purchases only, all with bootstrap intervals | `docs/eval-week07.json`, `docs/eval-week07-purchase.json` |
| 4. Batch + speed | the offline replay, cart and purchase targets, products the user has never touched, the similarity ablation, what it means for the API merge | `docs/eval-week08-replay.json`, `docs/eval-week08-new.json` |
| 5. Limits and future work | offline-only evaluation, Redis budget, ALS re-recommending seen products, single-laptop runs, frozen `sim:cooc` in the replay | |

The report is written in Vietnamese for the lecturer, in a shared document with a diagram of the nightly run and a chart of the "unseen" results. Every number in it comes from the weekly reports and the JSON result files listed above.

### 3. Results / metrics

The main numbers the report rests on:

| Result | Value |
|---|---|
| ALS on warm test users, NDCG@10 | 0.1226 [0.1213, 0.1239], against 0.0909 for Item-kNN and 0.0691 for Popularity |
| Test users who are new (Popularity only from the batch layer) | 1,111,573 of 2,438,946 (46%) |
| ALS without already-seen products | 0.0534, below Item-kNN (0.0685) and Popularity (0.0560) |
| Nightly run on the full history | 21 minutes, 1,232,939 batch keys in 943 MB of Redis |
| Replay, cart and purchase targets | batch only 0.1281, best batch + speed 0.1163; 98% of the carted products are already in `user:recent` |
| Replay, products never touched | speed only 0.1351, ALS 0.0454, Popularity 0.0396 |

### 4. Issues & resolutions

| Issue | Resolution |
|---|---|
| The report is read by the lecturer, the weekly reports by the team | The report explains each choice in plain words and keeps the tables to the numbers that support a conclusion; the details stay in `docs/week-XX.md` |
| Section 4 depended on the week 8 replay | Written last, after both replay protocols had run |

### 5. Plan for next week

- Scale test of the nightly batch (optional in `docs/assignment.md`).
- Review of the report with Pair 2.
