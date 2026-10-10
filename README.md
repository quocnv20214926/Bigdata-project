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

## Project Structure

```
├── docs/          # Project plan and weekly reports (plan.md, week-XX.md)
├── infra/         # Docker Compose, Kafka/Spark/Redis/Airflow configuration
├── ingestion/     # Event Replayer: pushes historical data into Kafka by event_time
├── batch/         # PySpark ETL, ALS training, similarity, Redis loading, Airflow DAGs
├── streaming/     # Spark Structured Streaming job (speed layer)
├── serving/       # Recommendation API (FastAPI)
├── ui/            # Streamlit demo app
├── evaluation/    # Offline evaluation: NDCG, Recall, Hit Rate, batch vs batch+speed
├── common/        # Shared code (event weights, schemas, config)
├── notebooks/     # EDA
└── data/          # Local data (gitignored)
```

## Key Features

- Personalized top-K recommendations per user, updated within seconds of new activity
- Similar products by `product_id`
- Trending products by category
- Automated nightly batch pipeline
- Recommendation quality evaluation and system monitoring

## Documentation

- [Detailed project plan](docs/plan.md)
- Weekly reports: `docs/week-XX.md`

## Status

🚧 Week 0: project scaffolding.
