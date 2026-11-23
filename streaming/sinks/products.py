"""Sink: product attributes seen in the stream (week 5, owner C).

The API shows category, brand and price with every recommendation, and its
fallback needs the category of the product a user viewed last. Neither is in
the shared Redis contract, so the speed layer keeps them in an internal key:

  rt:prod:{product_id}  HASH  category_id, category_code, brand, price   TTL 30 days

serving/app/redis_repository.py reads the same key (keep both in sync).
"""

from __future__ import annotations

from typing import Iterable

PRODUCT_TTL = 30 * 24 * 3600


def product_key(product_id) -> str:
    return f"rt:prod:{product_id}"


def write_products(rows: Iterable, r) -> int:
    """Store the latest attributes of each product (rows in time order). Returns products written."""
    latest = {}
    for row in rows:
        latest[int(row.product_id)] = row
    pipe = r.pipeline(transaction=False)
    for pid, row in latest.items():
        fields = {"category_id": row.category_id, "category_code": row.category_code,
                  "brand": row.brand, "price": row.price}
        fields = {k: v for k, v in fields.items() if v is not None}
        if fields:
            pipe.hset(product_key(pid), mapping=fields)
            pipe.expire(product_key(pid), PRODUCT_TTL)
    pipe.execute()
    return len(latest)
