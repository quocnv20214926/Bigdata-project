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

```
                ┌──────────────── BATCH LAYER (nightly, Airflow) ─────────────────┐
REES46 CSV ──►  │ S3/MinIO (raw) → PySpark ETL → Parquet (clean, by date)         │
   │            │   ├─ Implicit ALS → top-100 recommendations per user            │
   │            │   ├─ Item-item similarity (ALS factors + session co-occurrence) │
   │            │   └─ Category popularity                                        │
   │            └──────────────────────────┬──────────────────────────────────────┘
   │                                       ▼ (bulk load)
   │                                  ┌─────────┐
   ▼                                  │  REDIS  │◄──── Recommendation API (FastAPI)
Event Replayer ─► Kafka ─► Spark      │ serving │      merge batch + real-time,
(replays by event_time)   Structured ─►         │      filter, fallback → Streamlit UI
                          Streaming   └─────────┘
                ┌──────── SPEED LAYER ──────────────┐
                │ • keep last N events per user     │
                │ • propagate scores via sim(i, j)  │
                │ • windowed trending               │
                └───────────────────────────────────┘
```

### 3.1. Redis Key Design

| Key | Type | Source | TTL |
|---|---|---|---|
| `rec:batch:{user_id}` | ZSET (product → score) | Batch | Overwritten nightly |
| `sim:item:{product_id}` | ZSET of top-50 similar items | Batch | Overwritten nightly |
| `pop:cat:{category_id}` | ZSET of trending items | Batch + Speed | ~1h |
| `user:recent:{user_id}` | LIST of last 20 events | Speed | 24h |
| `rec:rt:{user_id}` | ZSET of real-time scores | Speed | 2–6h |
| `user:bought:{user_id}` | SET of purchased items | Speed | 30 days |

### 3.2. Scoring

- **Event weights** (shared by batch and speed, defined in `common/weights.py`): view = 1, cart = 3, purchase = 5.
- **Implicit ALS:** confidence = 1 + α · log(1 + total interaction score).
- **Real-time score:** for an event of type *e* by a user on item *i*, occurring Δt ago:
  `rt[j] += w_e · sim(i, j) · exp(−λ · Δt)` for every *j* in the top-K similar items of *i*.
- **Final score:** `score = α · norm(batch) + (1 − α) · norm(rt)`. α and λ are tuned through offline evaluation.
- **Post-processing:** remove already purchased items; users with no batch data fall back to `pop:cat` for the category they just viewed.

### 3.3. Why Lambda Instead of Kappa

ALS needs to train on the full history, which is a natural fit for batch processing. The speed layer only needs lightweight logic (score propagation, windowed counts). The trade-off is maintaining two codebases; the risk of logic drift is reduced by a shared module in `common/`.

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

## 6. Roadmap (10 Weeks)

### Week 0: Project Setup
- [x] Create repository structure, README, and plan
- [ ] Write `infra/docker-compose.yml`: MinIO, Kafka, Spark (1 master + 2 workers), Redis, Airflow
- [ ] Download the dataset into `data/raw/`, verify columns and event types
- [ ] Initialize the git repository
- **Deliverable:** stack runs with `docker compose up`, report `docs/week-00.md`

### Week 1: EDA & ETL
- EDA: event_type distribution, view→cart→purchase funnel, events per user, product long tail
- Cleaning: drop duplicates, handle null `brand`/`category_code`, drop price ≤ 0, filter bots (> 1000 events/day), normalize to UTC
- Write Parquet partitioned by `event_date` to MinIO; time-based train/test split
- **Deliverable:** EDA notebook, ETL job, dataset statistics table

### Weeks 2–3: Batch Layer
- Weighted user–item interaction matrix; map IDs to integers
- Baselines: Popularity, session co-occurrence Item-kNN
- Implicit ALS: grid search over `rank` ∈ {32, 64, 128}, `regParam`, `alpha`
- Item–item similarity: cosine on item factors, top-50 per item (Faiss or block-wise computation for large catalogs)
- Bulk load into Redis via pipelines; Airflow DAG `nightly_batch`: ETL → train → similarity → load
- **Deliverable:** ALS vs baselines comparison (NDCG@10, Recall@20, Precision@10, coverage)

### Weeks 4–5: Speed Layer
- Event Replayer: read Parquet in `event_time` order, publish to topic `events` (key = `user_id`), configurable replay speed
- Structured Streaming: watermarking; `foreachBatch` updates `user:recent`, `rec:rt`, `user:bought`; 1h window sliding every 5 minutes for trending; checkpointing
- **Deliverable:** demo where viewing product A changes recommendations within seconds

### Week 6: Serving Layer & UI
- FastAPI with 3 endpoints; merge, filter, and fallback logic; Redis reads batched in a single round-trip
- Load test at 500–1000 RPS, measure p50/p95/p99; target p95 < 100ms
- 3-page Streamlit UI
- **Deliverable:** API, latency report, demo UI

### Week 7: Evaluation
- Experiment A: batch-only (ALS)
- Experiment B: batch + speed. Replay the test set; at each cart/purchase event, take the recommendations just before it and measure Hit Rate@K, NDCG@K
- Tune α and λ; break down results by user segment (new / light / heavy)
- **Deliverable:** A/B comparison charts, ablation table

### Weeks 8–9: Monitoring, Optimization & Report
- Grafana: Kafka consumer lag, streaming throughput, API latency, Redis memory
- Spark tuning: partitioning, broadcast joins, caching; (optional) scale to the full 7 months of data
- Final report: architecture, Lambda vs Kappa, results, limitations, future work (GRU4Rec/SASRec)
- **Deliverable:** report, slides, demo video

## 7. Risks & Mitigation

| Risk | Mitigation |
|---|---|
| Out of memory during ALS/similarity | Use a subset, filter rare items, compute similarity in blocks |
| High Redis memory usage | Store top-50 only for active users, set TTLs, use integer IDs |
| Logic drift between batch and speed | Shared module in `common/` |
| Speed layer does not improve metrics | Analyze the cause, tune λ and K; still a valid finding |
| Data leakage in evaluation | Time-based split; similarity computed on training data only |

## 8. Success Criteria

- [ ] End-to-end run with a single Docker Compose command
- [ ] ALS beats the Popularity baseline on NDCG@10
- [ ] Batch + speed beats batch-only on Hit Rate@10, especially for new users
- [ ] API reaches p95 < 100ms at the target load
- [ ] Batch job runs automatically via Airflow, with a monitoring dashboard

## 9. Weekly Report Convention

One file per week, `docs/week-XX.md` (e.g. `week-00.md`), with these sections:
1. Weekly goals
2. Completed work
3. Results / metrics
4. Issues & resolutions
5. Plan for next week
