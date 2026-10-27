# common/: Shared Contracts

This package holds every contract shared by **Pair 1 (Batch)** and **Pair 2 (Real-time)**. It is frozen at the start of Week 1 so the two pairs can work independently.

| Module | Contract | Used by |
|---|---|---|
| `config.py` | Environment variables, service addresses, list sizes (top-N) | Everyone |
| `schemas.py` | Raw CSV columns, clean Parquet schema, Kafka message format (`Event`) | Pair 1 ETL, Pair 2 Replayer and streaming |
| `weights.py` | Event weights, ALS confidence formula | Pair 1 ALS, Pair 2 real-time scoring |
| `redis_keys.py` | Every Redis key, its type, writer, TTL, and value encoding | Everyone writing or reading Redis |
| `scoring.py` | Time decay, co-occurrence similarity, score propagation, batch + real-time merge | Pair 2 streaming and API, Pair 1 evaluation |
| `models.py` | API response models (pydantic) | API (Pair 2), fallback API (Pair 1), UI |

## Rules

1. **Single writer per Redis key.** Pair 1 writes `rec:batch`, `sim:item`, `pop:*`, `meta:batch:*`. Pair 2 writes `user:*`, `rec:rt`, `sim:cooc`, `trend:*`. Pair 2's internal helper keys use the `rt:` prefix.
2. **Build keys with `redis_keys.py` helpers**, never with hand-written strings.
3. **Any change here needs a PR approved by one member of each pair**, and a bump of `CONTRACT_VERSION` in `__init__.py`.
4. **Keep dependencies light.** Only the standard library, plus `pydantic` in `models.py`. `pyspark` is imported lazily inside `spark_event_schema()`.

## Usage

```python
from common import redis_keys as rk
from common.schemas import Event
from common.scoring import merge_scores

key = rk.rec_batch(541312140)             # "rec:batch:541312140"
event = Event.from_json(kafka_message)    # parse a Kafka value
top = merge_scores(batch, realtime, k=10) # [(product_id, score, source), ...]
```

## Changes

| Version | Date | Change |
|---|---|---|
| 1.1.0 | 2026-10-27 | `user:recent` and `user:bought` are ZSETs scored by event time (epoch seconds), matching `streaming/main.py`; they were LIST and SET |
| 1.0.0 | Week 2 | Contracts frozen by both pairs |
