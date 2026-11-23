# E-Commerce Product Recommendation System with Lambda Architecture

This project builds a product recommendation pipeline that combines **long-term purchase history** (Batch Layer) with **users' most recent views and clicks** (Speed Layer). Recommendations are served through a low-latency API (p95 < 100ms).

## Architecture

| Layer | Technology | Role |
|---|---|---|
| Batch | MinIO/S3 + PySpark (implicit ALS), Airflow | Nightly training: top-K recommendations per user, item–item similarity, category popularity |
| Speed | Kafka + Spark Structured Streaming | Update recommendation scores from the latest events, compute trending products per category |
| Serving | Redis + FastAPI | Merge batch and real-time scores, filter purchased items, fall back for new users |
| UI / Monitoring | Streamlit, Grafana, Kafka UI | Demo of real-time recommendation updates, system monitoring |

```
REES46 ──► S3/MinIO ──► PySpark (ALS) ──────────────┐
   │                                                ▼
   └──► Event Replayer ──► Kafka ──► Spark Streaming ──► Redis ◄── FastAPI ◄── Streamlit UI
```

## Dataset

[eCommerce behavior data from multi category store (REES46, Kaggle)](https://www.kaggle.com/datasets/mkechinov/ecommerce-behavior-data-from-multi-category-store): user behavior events (`view`, `cart`, `purchase`) with columns `event_time`, `event_type`, `product_id`, `category_id`, `category_code`, `brand`, `price`, `user_id`, `user_session`. Initial scope: October–November 2019.

Raw data goes in `data/` (not committed to git).

## Quick Start

```powershell
Copy-Item .env.example .env
. .\scripts\load-env.ps1            # defines dc (real-time stack) and dcall (whole system)
dcall up -d --build                  # bash: scripts/stack.sh up -d --build
python -m ingestion.replayer --file data\sample\oct_sample.csv --speed 0
python scripts\check_stack.py
```

Then open the Streamlit demo at http://localhost:8501. The full guide, with the
demo script and the measurements to rerun, is [docs/run-guide.md](docs/run-guide.md).

| Service | URL |
|---|---|
| Streamlit demo | http://localhost:8501 |
| API (Swagger) | http://localhost:8000/docs |
| Grafana (admin / admin) | http://localhost:3000 |
| Kafka UI | http://localhost:8085 |
| Airflow | http://localhost:8080 |

## Project Structure

```
├── docs/          # Plan, assignment, weekly reports (week-XX.md), run guide, result files
├── infra/         # Docker Compose (base, batch, stream), Prometheus and Grafana (monitoring/)
├── ingestion/     # Event Replayer: pushes historical data into Kafka by event_time
├── batch/         # PySpark ETL, ALS training, similarity, Redis loading, Airflow DAGs (Pair 1)
├── streaming/     # Spark Structured Streaming job (speed layer), exporter, replay evaluation (Pair 2)
├── serving/       # Recommendation API (FastAPI) and load test (Pair 2)
├── ui/            # Streamlit demo app (Pair 2)
├── evaluation/    # Offline evaluation of the batch layer and batch + speed (Pair 1)
├── common/        # Shared contracts: schemas, Redis keys, weights, scoring, API models
├── scripts/       # Environment loading, whole-system compose, stack check
├── notebooks/     # EDA
└── data/          # Local data (gitignored)
```

## Key Features

- Personalized top-K recommendations per user, updated within seconds of new activity (2.5–3 s)
- Similar products by `product_id` (session co-occurrence, Item-kNN, or both)
- Trending products by category over the last hour
- Automated nightly batch pipeline (Airflow)
- Recommendation quality evaluation, load test (p95 24 ms at 500 requests/s) and Grafana monitoring

## Documentation

- [Project plan](docs/plan.md) and [team assignment](docs/assignment.md)
- Weekly reports: `docs/week-01.md` to `docs/week-10.md`
- [Run guide and demo script](docs/run-guide.md)
- [Final report, Pair 2 sections](docs/report-pair2.md)
- Each folder has a README explaining how to run it

## Status

Weeks 1–10: both pipelines complete and integrated in the API; see the weekly reports.
