"""Data access for the API.

The endpoints only talk to a Repository, so the data source can change
without touching the routes:
  - MockRepository  (week 2): deterministic fake data, no Redis needed.
  - RedisRepository (week 5): real batch/real-time data from Redis + fallback.

Both must return the pydantic models from common/models.py.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional, Protocol

from common.models import (
    HealthResponse, RecommendedItem, RecommendResponse, SimilarResponse, TrendingResponse,
)
from serving.app import mock_data as md


class Repository(Protocol):
    async def recommend(self, user_id: int, k: int) -> RecommendResponse: ...
    async def similar(self, product_id: int, k: int) -> SimilarResponse: ...
    async def trending(self, category_id: Optional[int], k: int) -> TrendingResponse: ...
    async def health(self) -> HealthResponse: ...


def _item(p: md.Product, score: float, source: str) -> RecommendedItem:
    return RecommendedItem(
        product_id=p.product_id, score=score, source=source,
        category_id=p.category_id, category_code=p.category_code,
        brand=p.brand, price=p.price,
    )


class MockRepository:
    """Fake but realistic responses, so the UI can be built before real data exists.

    Response shapes it can produce, chosen from the user_id:
      user_id % 10 == 0  -> new user: no batch, no real-time, falls back to "trending"
      user_id % 10 == 1  -> batch only (real-time missing)
      user_id % 10 == 2  -> real-time only (batch missing)
      otherwise          -> both sources, mixed "batch" / "realtime" / "both"
    """

    def __init__(self, redis_ping=None):
        self._redis_ping = redis_ping   # optional async callable, used only by /health

    async def recommend(self, user_id: int, k: int) -> RecommendResponse:
        case = user_id % 10
        picks = md.sample_products(user_id, k)
        if case == 0:
            batch_ok, rt_ok, sources = False, False, ["trending"]
        elif case == 1:
            batch_ok, rt_ok, sources = True, False, ["batch"]
        elif case == 2:
            batch_ok, rt_ok, sources = False, True, ["realtime"]
        else:
            batch_ok, rt_ok, sources = True, True, ["both", "realtime", "batch"]
        items = [_item(p, s, sources[i % len(sources)]) for i, (p, s) in enumerate(picks)]
        return RecommendResponse(
            user_id=user_id, items=items, generated_at=datetime.now(timezone.utc),
            batch_available=batch_ok, realtime_available=rt_ok,
        )

    async def similar(self, product_id: int, k: int) -> SimilarResponse:
        base = md.BY_ID.get(product_id)
        pool = md.BY_CATEGORY[base.category_id] + md.CATALOGUE if base else None
        picks = md.sample_products(product_id, k, pool=pool, exclude=product_id)
        # Same-category products first, like a real co-occurrence list would look.
        seen, items = set(), []
        for p, s in picks:
            if p.product_id not in seen:
                seen.add(p.product_id)
                items.append(_item(p, s, "realtime"))
        return SimilarResponse(product_id=product_id, items=items, similarity="cooc")

    async def trending(self, category_id: Optional[int], k: int) -> TrendingResponse:
        pool = md.BY_CATEGORY.get(category_id) if category_id is not None else None
        seed = (category_id or 0) + 7
        items = [_item(p, round(s * 500), "trending") for p, s in md.sample_products(seed, k, pool=pool)]
        return TrendingResponse(category_id=category_id, items=items, window_minutes=60)

    async def health(self) -> HealthResponse:
        redis_ok = False
        if self._redis_ping is not None:
            try:
                redis_ok = bool(await self._redis_ping())
            except Exception:
                redis_ok = False
        # In mock mode the API works without Redis, so it is always "ok".
        return HealthResponse(status="ok", redis=redis_ok, batch_last_run=None)
