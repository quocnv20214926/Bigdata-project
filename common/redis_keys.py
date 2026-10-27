"""Redis key contract. Always build keys with these helpers, never by hand.

| Key                         | Type   | Writer        | TTL              | Value                                  |
|-----------------------------|--------|---------------|------------------|----------------------------------------|
| rec:batch:{user_id}         | ZSET   | Pair 1 batch  | none (nightly)   | product_id -> ALS score                |
| sim:item:{product_id}       | ZSET   | Pair 1 batch  | none (nightly)   | product_id -> cosine similarity        |
| pop:cat:{category_id}       | ZSET   | Pair 1 batch  | none (nightly)   | product_id -> long-term popularity     |
| pop:global                  | ZSET   | Pair 1 batch  | none (nightly)   | product_id -> long-term popularity     |
| meta:batch:last_run         | STRING | Pair 1 batch  | none             | ISO 8601 UTC timestamp of last load    |
| user:recent:{user_id}       | ZSET   | Pair 2 speed  | 24h              | "product_id|event_type|epoch" -> epoch |
| user:bought:{user_id}       | ZSET   | Pair 2 speed  | 30 days          | product_id -> purchase epoch_seconds   |
| rec:rt:{user_id}            | ZSET   | Pair 2 speed  | 6h               | product_id -> real-time score          |
| sim:cooc:{product_id}       | ZSET   | Pair 2 speed  | 7 days           | product_id -> normalized co-occurrence |
| trend:cat:{category_id}     | ZSET   | Pair 2 speed  | 1h               | product_id -> weighted count (1h)      |
| trend:global                | ZSET   | Pair 2 speed  | 1h               | product_id -> weighted count (1h)      |

user:recent keeps only the settings.recent_n newest members (ZREMRANGEBYRANK
after each write); read it newest first with ZREVRANGE. user:bought is checked
with ZSCORE (purchased if not nil). Both were LIST / SET before contract 1.1.0.

Each key has exactly one writer pipeline, so the two pairs never overwrite
each other. Pair 2 may keep internal helper keys under the "rt:" prefix
(e.g. raw co-occurrence counts); those are not part of the contract.
"""

from __future__ import annotations

HOUR = 3600
DAY = 24 * HOUR

# ---- Pair 1: batch layer ------------------------------------------------

def rec_batch(user_id: int) -> str:
    return f"rec:batch:{user_id}"


def sim_item(product_id: int) -> str:
    return f"sim:item:{product_id}"


def pop_cat(category_id: int) -> str:
    return f"pop:cat:{category_id}"


POP_GLOBAL = "pop:global"
META_BATCH_LAST_RUN = "meta:batch:last_run"

# ---- Pair 2: speed layer ------------------------------------------------

def user_recent(user_id: int) -> str:
    return f"user:recent:{user_id}"


def user_bought(user_id: int) -> str:
    return f"user:bought:{user_id}"


def rec_rt(user_id: int) -> str:
    return f"rec:rt:{user_id}"


def sim_cooc(product_id: int) -> str:
    return f"sim:cooc:{product_id}"


def trend_cat(category_id: int) -> str:
    return f"trend:cat:{category_id}"


TREND_GLOBAL = "trend:global"

TTL = {
    "user:recent": DAY,
    "user:bought": 30 * DAY,
    "rec:rt": 6 * HOUR,
    "sim:cooc": 7 * DAY,
    "trend": HOUR,
}

# ---- Value encoding for user:recent ------------------------------------

def encode_recent(product_id: int, event_type: str, epoch_seconds: int) -> str:
    return f"{product_id}|{event_type}|{epoch_seconds}"


def decode_recent(value: str | bytes) -> tuple[int, str, int]:
    if isinstance(value, bytes):
        value = value.decode()
    pid, etype, ts = value.split("|")
    return int(pid), etype, int(ts)
