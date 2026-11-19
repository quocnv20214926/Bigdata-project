"""Streamlit demo for the Pair 2 real-time recommendation pipeline."""
from __future__ import annotations

import json
import os
import time
import uuid
from datetime import datetime, timezone

import requests
import streamlit as st
from kafka import KafkaProducer

API_URL = os.getenv("API_URL", "http://localhost:8000").rstrip("/")
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC_EVENTS", "events")

st.set_page_config(page_title="E-commerce Recommender", page_icon="🛍️", layout="wide")
st.title("E-commerce Product Recommendation Demo")
st.caption("Pair 2 · Kafka → Spark Structured Streaming → Redis → FastAPI")

with st.sidebar:
    st.subheader("Demo settings")
    user_id = st.number_input("User ID", min_value=1, value=541312143, step=1)
    k = st.slider("Number of recommendations", 1, 50, 10)
    st.caption(f"API: {API_URL}")
    st.caption(f"Kafka: {KAFKA_BOOTSTRAP}")

left, right = st.columns(2)
with left:
    st.subheader("Simulate a user event")
    product_id = st.number_input("Product ID", min_value=1, value=1000001, step=1)
    category_id = st.number_input("Category ID", min_value=1, value=2053013555631882655, step=1)
    event_type = st.selectbox("Event type", ["view", "cart", "purchase"])
    if st.button("Send event to Kafka", type="primary"):
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP,
                key_serializer=lambda value: str(value).encode("utf-8"),
                value_serializer=lambda value: json.dumps(value, separators=(",", ":")).encode("utf-8"),
                request_timeout_ms=5000,
            )
            event = {
                "event_time": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                "event_type": event_type,
                "product_id": int(product_id),
                "category_id": int(category_id),
                "category_code": None,
                "brand": None,
                "price": 0.0,
                "user_id": int(user_id),
                "user_session": f"demo-{user_id}-{uuid.uuid4().hex[:8]}",
            }
            producer.send(KAFKA_TOPIC, key=int(user_id), value=event).get(timeout=10)
            producer.flush()
            producer.close()
            st.success("Event published. Wait for the streaming micro-batch, then refresh recommendations.")
        except Exception as exc:
            st.error(f"Could not publish event: {exc}")

with right:
    st.subheader("Recommendations")
    if st.button("Refresh recommendations"):
        try:
            started = time.perf_counter()
            response = requests.get(f"{API_URL}/recommend/{int(user_id)}", params={"k": k}, timeout=5)
            response.raise_for_status()
            st.session_state["recommendation_result"] = response.json()
            st.session_state["recommendation_ms"] = (time.perf_counter() - started) * 1000
        except Exception as exc:
            st.error(f"API request failed: {exc}")

    result = st.session_state.get("recommendation_result")
    if result:
        c1, c2, c3 = st.columns(3)
        c1.metric("Batch available", "Yes" if result["batch_available"] else "No")
        c2.metric("Real-time available", "Yes" if result["realtime_available"] else "No")
        c3.metric("API latency", f'{st.session_state["recommendation_ms"]:.1f} ms')
        st.dataframe(result["items"], use_container_width=True, hide_index=True)

st.divider()
st.subheader("Other endpoints")
tab1, tab2, tab3 = st.tabs(["Similar products", "Trending", "Health"])
with tab1:
    similar_id = st.number_input("Product ID for similarity", min_value=1, value=int(product_id), key="similar_id")
    if st.button("Find similar products"):
        try:
            data = requests.get(f"{API_URL}/similar/{int(similar_id)}", params={"k": k}, timeout=5).json()
            st.write(f"Similarity source: **{data['similarity']}**")
            st.dataframe(data["items"], use_container_width=True, hide_index=True)
        except Exception as exc:
            st.error(str(exc))
with tab2:
    if st.button("Load global trending"):
        try:
            data = requests.get(f"{API_URL}/trending", params={"k": k}, timeout=5).json()
            st.dataframe(data["items"], use_container_width=True, hide_index=True)
        except Exception as exc:
            st.error(str(exc))
with tab3:
    if st.button("Check API health"):
        try:
            st.json(requests.get(f"{API_URL}/health", timeout=5).json())
        except Exception as exc:
            st.error(str(exc))
