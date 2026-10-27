# Project Plan: E-Commerce Product Recommendation System (Lambda Architecture)

## 1. Objectives

Build a product recommendation system that combines two signal sources:

- **Batch Layer:** long-term preferences learned from the full interaction history (implicit ALS), retrained nightly.
- **Speed Layer:** short-term intent; recommendation scores are updated as soon as a user views or adds a product to the cart.
- **Serving Layer:** merges both sources and returns recommendations with p95 latency < 100ms.

The project must also show with metrics that the speed layer improves recommendation quality over batch-only.

## 2. Dataset

**Source:** REES46, *eCommerce behavior data from multi category store* (Kaggle).

| Column | Description |
|---|---|
| `event_time` | Event timestamp (UTC) |
| `event_type` | `view`, `cart`, `purchase` |
| `product_id`, `category_id`, `category_code`, `brand`, `price` | Product attributes |
| `user_id`, `user_session` | User and session identifiers |

**Notes:**
- `remove_from_cart` only exists in the *cosmetics shop* version. The multi-category version has only 3 event types. Verify against the downloaded files.
- The dataset has no product names or images, so the UI displays products by brand, category, and price.
- Initial scope: **Oct–Nov 2019** (~110M rows). Use a single month on limited hardware.
- **Time-based split**: train = Oct 1 → Nov 15, test = Nov 16 → Nov 30.

## 3. Architecture

The system is built as **two independent pipelines** that both read the raw data and write to separate Redis keys. They meet only in the API.

```
                ┌──────────────── BATCH PIPELINE (Pair 1, nightly, Airflow) ──────────┐
           ┌──► │ S3/MinIO (raw) → PySpark ETL → Parquet (clean, by date)             │
           │    │   ├─ Implicit ALS → rec:batch (most recently active users)          │
           │    │   ├─ Item-kNN similarity (cosine over users) → sim:item             │
           │    │   └─ Long-term popularity → pop:cat, pop:global                     │
           │    └───────────────────────────────┬─────────────────────────────────────┘
REES46 CSV │                                    ▼ (bulk load)
           │                               ┌─────────┐
           │                               │  REDIS  │◄──── Recommendation API (FastAPI)
           │                               │ serving │      merge batch + real-time,
           │                               └─────────┘      filter, fallback → Streamlit UI
           │                                    ▲
           │    ┌──────────────── REAL-TIME PIPELINE (Pair 2) ───────────────────────┐
           └──► │ Event Replayer (raw CSV, by event_time) → Kafka → Structured       │
                │ Streaming:                                                          │
                │   • user:recent, user:bought                                        │
                │   • session co-occurrence → sim:cooc                                │
                │   • score propagation with time decay → rec:rt                      │
                │   • 1h windowed counts → trend:cat, trend:global                    │
                └─────────────────────────────────────────────────────────────────────┘
```

### 3.1. Redis Key Design

Each key has exactly one writer pipeline. The authoritative definition is `common/redis_keys.py`.

| Key | Type | Writer | TTL | Value |
|---|---|---|---|---|
| `rec:batch:{user_id}` | ZSET | Batch | Overwritten nightly | product → ALS score |
| `sim:item:{product_id}` | ZSET | Batch | Overwritten nightly | top-50 product → cosine similarity |
| `pop:cat:{category_id}`, `pop:global` | ZSET | Batch | Overwritten nightly | product → long-term popularity |
| `meta:batch:last_run` | STRING | Batch | none | timestamp of last load |
| `user:recent:{user_id}` | LIST | Speed | 24h | last 20 events, `product_id\|event_type\|epoch` |
| `user:bought:{user_id}` | SET | Speed | 30 days | purchased products |
| `rec:rt:{user_id}` | ZSET | Speed | 6h | product → real-time score |
| `sim:cooc:{product_id}` | ZSET | Speed | 7 days | top-50 product → normalized co-occurrence |
| `trend:cat:{category_id}`, `trend:global` | ZSET | Speed | 1h | product → weighted count in last hour |

### 3.2. Similarity

| | `sim:cooc` (speed) | `sim:item` (batch) |
|---|---|---|
| Method | Count of products viewed in the same session (within the last 10 items), normalized: `cooc(i, j) / sqrt(cnt(i) · cnt(j))` | Cosine between products over the users who interacted with them in the training window (Item-kNN, weights `log(1 + score)`, at least 2 users in common). Chosen in week 4 over the cosine of ALS item factors, which scored lower on every measure ([week-04.md](week-04.md)) |
| Update | Continuously, per event | Nightly |
| Strength | Simple; captures new products and trends immediately | Uses each user's whole history, so it links products seen in different sessions or weeks apart |
| Weakness | Noisy for rarely viewed products; only direct relations | New products wait for the next nightly run; products sharing fewer than 2 users with any other get no list |

The speed layer uses `sim:cooc` by default, so it does not depend on the batch layer. After integration, `sim:item` is added and the three variants (cooc / item / hybrid) are compared.

### 3.3. Scoring

- **Event weights** (`common/weights.py`): view = 1, cart = 3, purchase = 5.
- **Implicit ALS:** confidence = 1 + α · log(1 + total interaction score).
- **Real-time score:** for an event of type *e* by a user on item *i*, occurring Δt ago:
  `rt[j] += w_e · sim(i, j) · exp(−λ · Δt)` for every *j* in the top-K similar items of *i*, with λ = ln 2 / half-life (default half-life 30 minutes).
- **Final score:** `score = α · norm(batch) + (1 − α) · norm(rt)` with min-max normalization. α and the half-life are tuned through offline evaluation.
- **Graceful degradation:** if `rec:batch` is missing, α = 0; if `rec:rt` is missing, α = 1. If both are missing, the API falls back to `trend:cat` of the last viewed category, then `pop:global`.
- **Post-processing:** remove products in `user:bought`.

All formulas are implemented once in `common/scoring.py` and reused by the streaming job, the API, and offline evaluation.

### 3.4. Shared Contracts (`common/`)

The two pipelines communicate only through contracts in `common/`, frozen at the start of Week 1.

| Module | Contract |
|---|---|
| `config.py` | Environment variables, service addresses, list sizes (top-N) |
| `schemas.py` | Raw CSV columns, clean Parquet schema, Kafka message format (`Event`) |
| `weights.py` | Event weights, ALS confidence formula |
| `redis_keys.py` | Every Redis key, its type, writer, TTL, and value encoding |
| `scoring.py` | Time decay, co-occurrence similarity, score propagation, score merging |
| `models.py` | API response models (pydantic) |

**Kafka contract:** topic `events`, key = `user_id`, value = JSON produced by `Event.to_json()` with `event_time` in ISO 8601 UTC.

**Change rule:** any change to `common/` needs a PR approved by one member of each pair and a bump of `CONTRACT_VERSION`. See `common/README.md`.

### 3.5. Infrastructure Layout

Docker Compose is split so each pair owns its own services:

| File | Services | Owner |
|---|---|---|
| `infra/compose.base.yml` | MinIO, Redis | Shared |
| `infra/compose.batch.yml` | Spark master/workers, Airflow | Pair 1 |
| `infra/compose.stream.yml` | Kafka, Kafka UI, streaming job, API, Streamlit, Prometheus, Grafana | Pair 2 |

Run everything: `docker compose -f infra/compose.base.yml -f infra/compose.batch.yml -f infra/compose.stream.yml up`.

### 3.6. Why Lambda Instead of Kappa

ALS needs to train on the full history, which is a natural fit for batch processing. The speed layer only needs lightweight logic (co-occurrence counting, score propagation, windowed counts). The trade-off is maintaining two codebases; the risk of logic drift is reduced by the shared contracts in `common/`.

## 4. Tech Stack

| Component | Tool |
|---|---|
| Storage | MinIO (S3-compatible) or HDFS; Parquet |
| Batch | PySpark 3.5+, Spark MLlib ALS |
| Streaming | Kafka (KRaft), Spark Structured Streaming |
| Serving | Redis 7, FastAPI + redis-py (async) |
| UI | Streamlit |
| Orchestration | Apache Airflow |
| Monitoring | Prometheus, Grafana, Kafka UI |
| Load testing | Locust or k6 |
| Infrastructure | Docker Compose |

## 5. Final Product Features

1. **Personalized recommendations:** `GET /recommend/{user_id}?k=10`, updated within seconds of new activity; new users still receive recommendations.
2. **Similar products:** `GET /similar/{product_id}`.
3. **Trending by category:** `GET /trending/{category_id}`, 1-hour window.
4. **Self-running pipeline:** nightly batch via Airflow, continuous event replay into Kafka.
5. **Evaluation and monitoring:** metric comparison tables, Grafana dashboards.

### Demo UI (Streamlit, 3 pages)

- **Recommendation demo:** select a user to see their recent activity and current recommendations; a "Simulate viewing product X" button sends an event to Kafka and the recommendations refresh within seconds; each recommendation shows its source (batch / real-time / trending).
- **Trending & similar products:** select a category or a product.
- **Evaluation results:** NDCG, Hit Rate, and load test charts.

System monitoring uses Grafana and Kafka UI rather than custom code.

## 6. Roadmap

The team works in 2 fixed pairs, each owning one pipeline. The detailed weekly assignment is in [assignment.md](assignment.md).

### Week 1: Project Setup
- [x] Create repository structure, README, and plan
- [x] Draft shared contracts in `common/`
- [x] Write the three Docker Compose files in `infra/`
- [x] Download the dataset into `data/raw/`, verify columns and event types
- [x] Initialize the git repository

### Weeks 2–10: Summary

| Week | Pair 1: Batch | Pair 2: Real-time |
|---|---|---|
| 2 | Freeze contracts (whole team); EDA, ETL | Freeze contracts (whole team); Replayer, mock API |
| 3–4 | ALS, baselines, `sim:item` | Streaming core, `sim:cooc`, trending |
| 5 | Redis loader, Airflow DAG | API with fallback, `rec:rt` |
| 6 | Spark optimization, batch demo script | Streamlit pages 1–2, load test |
| 7 | Batch evaluation | Monitoring, speed-only evaluation |
| 8 | *Integration:* batch + speed experiment, ablation | *Integration:* merge in API, end-to-end compose |
| 9–10 | Report, scale test | Report, slides, demo video |

## 7. Evaluation

| Experiment | Owner | Measures |
|---|---|---|
| ALS vs baselines (Popularity, Item-kNN) | Pair 1, Week 6 | NDCG@10, Recall@20, Precision@10, coverage on the test split |
| Speed-only | Pair 2, Week 6 | Next-item Hit Rate@K when replaying the test split |
| Batch-only vs batch + speed | Both, Week 7 | Hit Rate@K, NDCG@K at each cart/purchase event, using recommendations just before the event |
| Similarity ablation (cooc / item / hybrid) | Both, Week 7 | Same as above |
| Segment analysis | Pair 1, Week 7 | Results for new / light / heavy users |
| Latency | Pair 2, Week 5 | p50/p95/p99 at 500–1000 RPS |

## 8. Risks & Mitigation

| Risk | Mitigation |
|---|---|
| Out of memory during ALS/similarity | Use a subset, filter rare items, compute similarity in blocks |
| High Redis memory usage | Store top-50 only for active users, set TTLs, use integer IDs |
| Logic drift between batch and speed | Shared contracts and scoring in `common/` |
| One pair is delayed or blocked | Independent pipelines; API degrades gracefully; fallback plans in `assignment.md` |
| Speed layer does not improve metrics | Analyze the cause, tune half-life and K; still a valid finding |
| Data leakage in evaluation | Time-based split; similarity computed on training data only |

## 9. Success Criteria

- [ ] End-to-end run with a single Docker Compose command
- [x] ALS beats the Popularity baseline on NDCG@10 (week 3: 0.1158 vs 0.0691, `docs/week-03.md`)
- [ ] Batch + speed beats batch-only on Hit Rate@10, especially for new users
- [ ] API reaches p95 < 100ms at the target load
- [ ] Batch job runs automatically via Airflow, with a monitoring dashboard

## 10. Weekly Report Convention

One file per week, `docs/week-XX.md` (e.g. `week-01.md`), with these sections:
1. Weekly goals
2. Completed work
3. Results / metrics
4. Issues & resolutions
5. Plan for next week
