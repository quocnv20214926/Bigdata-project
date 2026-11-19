# Streamlit demo (Week 6)

The UI publishes synthetic REES46-compatible view/cart/purchase events to Kafka
and calls the FastAPI endpoints to show recommendations, similar products,
trending products, and health.

With Docker Compose, open http://localhost:8501. The UI container uses the
internal service names `api:8000` and `kafka:29092`.
