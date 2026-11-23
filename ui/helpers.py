"""Shared helpers of the Streamlit demo (Pair 2, weeks 6 and 8).

The UI talks to three services:

- the recommendation API (FastAPI) for everything a client application would see;
- Redis, read-only, for what the API does not expose: the user's recent activity,
  the stream clock (rt:meta:stream) and product attributes (rt:prod:*);
- Kafka, to publish simulated events with the same contract as the replayer
  (common.schemas.Event, key = user_id, topic "events").

Simulated events are stamped with the *stream clock* (the newest event time the
streaming job has processed), not with the wall clock. The replayed data is from
2019: an event stamped "now" would move the watermark seven years ahead, so every
later replayed event would be dropped as late, and rec:rt would decay all the
user's 2019 scores to zero.
"""

from __future__ import annotations

import os
import time
from datetime import datetime, timedelta, timezone
from typing import Optional

import pandas as pd
import redis
import requests
import streamlit as st

from common import redis_keys as rk
from common.schemas import Event

API_URL = os.getenv("API_URL", "http://localhost:8000").rstrip("/")
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC_EVENTS", "events")
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))

STREAM_META = "rt:meta:stream"     # written by streaming/main.py


# ---- connections -----------------------------------------------------------

@st.cache_resource
def get_redis() -> redis.Redis:
    return redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True,
                       socket_connect_timeout=1, socket_timeout=1)


@st.cache_resource
def get_producer():
    from confluent_kafka import Producer          # imported lazily: page 2 works without Kafka
    return Producer({"bootstrap.servers": KAFKA_BOOTSTRAP, "client.id": "streamlit-demo",
                     "message.timeout.ms": 5000})


def api_get(path: str, **params) -> tuple[Optional[dict], float, Optional[str]]:
    """GET the API. Returns (json, latency in ms, error message)."""
    started = time.perf_counter()
    try:
        resp = requests.get(f"{API_URL}{path}", params=params, timeout=5)
        ms = (time.perf_counter() - started) * 1000
        if resp.status_code != 200:
            return None, ms, f"HTTP {resp.status_code}: {resp.text[:200]}"
        return resp.json(), ms, None
    except requests.RequestException as exc:
        return None, (time.perf_counter() - started) * 1000, f"API unreachable ({API_URL}): {exc.__class__.__name__}"


# ---- Redis reads -------------------------------------------------------------

def stream_status() -> dict:
    try:
        return get_redis().hgetall(STREAM_META)
    except redis.RedisError:
        return {}


def stream_clock() -> Optional[datetime]:
    value = stream_status().get("events_max_event_time")
    if not value:
        return None
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def product_attrs(product_ids) -> dict[int, dict]:
    ids = [int(p) for p in product_ids]
    if not ids:
        return {}
    pipe = get_redis().pipeline(transaction=False)
    for pid in ids:
        pipe.hgetall(f"rt:prod:{pid}")
    return dict(zip(ids, pipe.execute()))


def recent_activity(user_id: int, n: int = 20) -> pd.DataFrame:
    """user:recent newest first, with product attributes."""
    r = get_redis()
    try:
        members = r.zrevrange(rk.user_recent(user_id), 0, n - 1)
    except redis.ResponseError:                    # contract 1.0.0 stored a LIST
        members = r.lrange(rk.user_recent(user_id), 0, n - 1)
    rows = []
    for m in members:
        pid, etype, epoch = rk.decode_recent(m)
        rows.append({"time (UTC)": datetime.fromtimestamp(epoch, timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
                     "event": etype, "product_id": pid, "epoch": epoch})
    attrs = product_attrs(r["product_id"] for r in rows)
    for row in rows:
        a = attrs.get(row["product_id"], {})
        row.update(category=a.get("category_code") or a.get("category_id"), brand=a.get("brand"),
                   price=float(a["price"]) if a.get("price") else None)
    return pd.DataFrame(rows, columns=["time (UTC)", "event", "product_id", "category", "brand", "price", "epoch"])


def sample_users(n: int = 8) -> list[int]:
    """A few users that currently have real-time scores, for the demo picker."""
    r = get_redis()
    users: list[int] = []
    try:
        for key in r.scan_iter(match="rec:rt:*", count=500):
            users.append(int(key.rsplit(":", 1)[1]))
            if len(users) >= n:
                break
    except redis.RedisError:
        pass
    return users


# ---- simulated events ----------------------------------------------------------

def demo_event_time(user_id: int) -> datetime:
    """Just after the newest event the stream (or this user) has seen."""
    clock = stream_clock()
    activity = recent_activity(user_id, 1)
    candidates = [c for c in (clock,) if c]
    if not activity.empty:
        candidates.append(datetime.fromtimestamp(int(activity.iloc[0]["epoch"]), timezone.utc))
    base = max(candidates) if candidates else datetime.now(timezone.utc)
    return base + timedelta(seconds=1)


def send_event(user_id: int, product_id: int, event_type: str) -> Event:
    """Publish one event to Kafka, with the attributes the stream knows for the product."""
    a = product_attrs([product_id]).get(int(product_id), {})
    event = Event(
        event_time=demo_event_time(user_id),
        event_type=event_type,
        product_id=int(product_id),
        category_id=int(a.get("category_id") or 0),
        category_code=a.get("category_code") or None,
        brand=a.get("brand") or None,
        price=float(a.get("price") or 0.0),
        user_id=int(user_id),
        user_session=f"demo-{user_id}",
    )
    producer = get_producer()
    errors: list = []
    producer.produce(KAFKA_TOPIC, key=str(user_id), value=event.to_json(),
                     on_delivery=lambda err, _msg: errors.append(err) if err else None)
    remaining = producer.flush(10)
    if remaining or errors:
        raise RuntimeError(f"Kafka delivery failed ({KAFKA_BOOTSTRAP}): {errors[0] if errors else 'timeout'}")
    return event


def wait_for_event(user_id: int, event: Event, timeout: float = 30.0) -> Optional[float]:
    """Seconds until the speed layer has fully applied the event, or None.

    In one micro-batch the job writes user:recent first, then sim:cooc and rec:rt
    (streaming/main.py). Seeing the event in user:recent is therefore not enough:
    we also wait until rec:rt has been rewritten for it, i.e. its reference time
    rt:rect:{user} (written in the same transaction as rec:rt) reaches the event
    time. If the user has no rec:rt (no neighbours at all), user:recent suffices.
    """
    epoch = int(event.event_time.timestamp())
    member = rk.encode_recent(event.product_id, event.event_type, epoch)
    r = get_redis()
    started = time.time()
    while time.time() - started < timeout:
        try:
            if r.zscore(rk.user_recent(user_id), member) is not None:
                ref = r.get(f"rt:rect:{user_id}")
                if (ref is not None and int(ref) >= epoch) or not r.exists(rk.rec_rt(user_id)):
                    return time.time() - started
        except redis.RedisError:
            pass
        time.sleep(0.25)
    return None


def items_frame(items: list[dict]) -> pd.DataFrame:
    cols = ["product_id", "score", "source", "category_code", "brand", "price", "category_id"]
    df = pd.DataFrame(items, columns=cols)
    df.insert(0, "rank", range(1, len(df) + 1))
    return df
