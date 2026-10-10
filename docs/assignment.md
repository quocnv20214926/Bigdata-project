# Team Assignment

The team of 4 is split into **2 fixed pairs** for the entire project. Each pair owns one **complete, self-contained pipeline** that can run and be demoed on its own. The two pipelines are integrated only in Week 8

| Member | Pair |
|---|---|
| A | Pair 1: Batch |
| B | Pair 1: Batch |
| C | Pair 2: Real-time |
| D | Pair 2: Real-time |

## 1. Pair Responsibilities

| | Pair 1: Batch (A, B) | Pair 2: Real-time (C, D) |
|---|---|---|
| **Pipeline** | Raw CSV → ETL → Parquet → ALS → Redis → offline evaluation | Raw CSV → Event Replayer → Kafka → Spark Streaming → Redis → API → UI |
| **Owns** | EDA, ETL, implicit ALS, baselines, item similarity (`sim:item`), Redis bulk loader, Airflow DAG, batch evaluation | Event Replayer, Kafka, Structured Streaming, co-occurrence similarity (`sim:cooc`), real-time scores, trending, FastAPI, Streamlit UI, Grafana |
| **Folders** | `batch/`, `notebooks/`, `evaluation/` | `ingestion/`, `streaming/`, `serving/`, `ui/` |
| **Redis keys written** | `rec:batch:*`, `sim:item:*`, `pop:*`, `meta:batch:*` (nightly) | `user:*`, `rec:rt:*`, `sim:cooc:*`, `trend:*` |
| **Compose file** | `infra/compose.batch.yml` (Spark, Airflow) | `infra/compose.stream.yml` (Kafka, streaming, API, UI) |
| **Standalone demo** | `batch/demo_recommend.py`: long-term recommendations per user, ALS vs baselines | Streamlit UI: session-based recommendations updating in real time, trending |

Shared: `common/` (contracts) and `infra/compose.base.yml` (MinIO, Redis). Changes to shared files require approval from both pairs.

### 1.1. Decoupling Rules

1. **Pair 2 reads the raw CSV directly.** The Replayer only parses the needed columns and does not wait for Pair 1's cleaned Parquet.
2. **Pair 2 computes its own similarity (`sim:cooc`)** from products viewed in the same session. Real-time scoring works without ALS; `sim:item` from Pair 1 is an optional extra used after integration.
3. **The API degrades gracefully.** If `rec:batch` is missing, it serves real-time + trending; if `rec:rt` is missing, it serves batch only.
4. **Each pair evaluates its own pipeline** (Week 7) before integration, so both have complete results even if integration fails.

### 1.2. Dependency Points

Only two points require both pairs:

| When | What | Risk control |
|---|---|---|
| **Start of Week 2** (1–2 days, whole team) | Freeze contracts in `common/`: event schema, Parquet schema, Redis keys, API spec (OpenAPI), event weights | Frozen after agreement; any change needs approval from both pairs |
| **Week 8** (integration) | Merge batch and real-time scores in the API; run the batch + speed experiment | Optional add-on; each pair's standalone results remain valid if it fails |

### 1.3. Contingency

| Situation | System still has | Fallback |
|---|---|---|
| Pair 1 is delayed or blocked | Real-time recommendations, trending, API, UI | API falls back automatically; if needed, Pair 2 writes a Popularity baseline into `rec:batch` (~1 day) |
| Pair 2 is delayed or blocked | Batch recommendations, evaluation results, demo script | Pair 1 builds a minimal API that reads only `rec:batch`, following the frozen API spec (1–2 days) |

To make takeover possible, every folder has a short README explaining how to run it.

## 2. Weekly Plan

| Week | Pair 1: Batch (A, B) | Pair 2: Real-time (C, D) |
|---|---|---|
| **2** | *Whole team: freeze contracts (1–2 days).* **A:** EDA. **B:** ETL to Parquet, time-based train/test split | *Whole team: freeze contracts.* **C:** FastAPI skeleton returning mock data. **D:** Replayer reading raw CSV, Kafka topic setup |
| **3** | **A:** interaction matrix, ALS training. **B:** metric functions, Popularity and Item-kNN baselines | **C:** `foreachBatch` writing `user:recent`, `user:bought`. **D:** streaming core: parsing, watermark, checkpoint |
| **4** | **A:** ALS grid search. **B:** item similarity `sim:item` | **C:** windowed trending. **D:** `sim:cooc` computed from the stream |
| **5** | **A:** Redis bulk loader. **B:** Airflow DAG `nightly_batch` | **C:** API reading real Redis data, fallback logic. **D:** `rec:rt` (score propagation via `sim:cooc`, time decay) |
| **6** | **A:** Spark optimization. **B:** demo script `batch/demo_recommend.py` | **C:** Streamlit pages 1–2. **D:** load test with Locust |
| **7** | **A:** final ALS vs baselines evaluation. **B:** analysis by user segment | **C:** Grafana, Kafka UI. **D:** speed-only evaluation: replay test set, next-item Hit Rate |
| **8** | *Integration.* **A:** batch + speed experiment. **B:** tune α, ablation table | *Integration.* **C:** merge batch scores into API, Streamlit page 3. **D:** end-to-end compose, add `sim:item` to real-time scoring |
| **9** | Report: data, batch layer, evaluation results | Report: speed layer, serving, UI, Lambda vs Kappa; slides |
| **10** | Full-dataset scale test (optional), report review | Demo video, README and run guide. **Whole team:** defense rehearsal |

### 2.1. Weekly Deliverables

| Week | Pair 1: Batch | Pair 2: Real-time |
|---|---|---|
| 2 | Frozen contracts; EDA notebook; clean Parquet on MinIO | Frozen contracts; events flowing into Kafka; mock API |
| 3 | First ALS model; baseline metrics | Streaming job writing user state to Redis |
| 4 | Tuned ALS; `sim:item` table | Trending and `sim:cooc` in Redis |
| 5 | Nightly batch running automatically via Airflow | API serving real-time recommendations |
| 6 | Optimized batch job; standalone batch demo | Streamlit demo; latency report (p50/p95/p99) |
| 7 | Batch evaluation results | Speed-only evaluation results; monitoring dashboard |
| 8 | Batch + speed comparison, ablation table | Integrated end-to-end system |
| 9 | Report sections (batch, evaluation) | Report sections (speed, serving), slides |
| 10 | Final report | Demo video; rehearsal done |

### 2.2. Working Rules

- At the start of each week, the two members of a pair split subtasks; at the end of the week, each demos their part to their partner.
- Members of a pair review each other's PRs, so either can continue the other's work.
- Each pair writes its own section in `docs/week-XX.md`.

