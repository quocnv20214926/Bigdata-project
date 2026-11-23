"""RedisRepository: the API on real data (week 5, owner C).

/recommend/{user_id}
  1. Read rec:batch (Pair 1) and rec:rt (Pair 2), merge them with
     common.scoring.merge_scores (min-max normalised, alpha * batch +
     (1 - alpha) * real-time). The merge already degrades gracefully: without
     rec:batch alpha = 0, without rec:rt alpha = 1.
  2. Remove products in user:bought.
  3. If fewer than k products remain (new user, or short lists), fill up from
     trend:cat of the category the user viewed last, then trend:global, then
     pop:global. Filled products get scores below the merged ones, by position.
  4. Add category, brand and price from rt:prod:{product_id}.

/similar/{product_id}   sim:cooc (speed) and/or sim:item (batch); both -> "hybrid"
/trending[/{category}]  trend:cat / trend:global, else pop:cat / pop:global
/health                 Redis ping and meta:batch:last_run

user:recent and user:bought are read both as ZSET (contract 1.1.0, written by
the streaming job) and as LIST / SET (contract 1.0.0), so the API works before
and after the contract change is merged.
"""

from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import Optional

from redis.exceptions import ResponseError

from common import redis_keys as rk
from common.config import settings
from common.models import (
    HealthResponse, RecommendedItem, RecommendResponse, SimilarResponse, TrendingResponse,
)
from common.scoring import DEFAULT_BATCH_WEIGHT, merge_scores

ALPHA = float(os.getenv("API_ALPHA", str(DEFAULT_BATCH_WEIGHT)))
BATCH_CANDIDATES = int(os.getenv("API_BATCH_CANDIDATES", str(settings.batch_top_n)))
RT_CANDIDATES = int(os.getenv("API_RT_CANDIDATES", "100"))
SIM_CANDIDATES = settings.sim_top_n


def product_key(product_id) -> str:
    """Same key as streaming/sinks/products.py (internal key of Pair 2)."""
    return f"rt:prod:{product_id}"


def _scores(pairs) -> dict[int, float]:
    return {int(m): float(s) for m, s in pairs or []}


def _float(v) -> Optional[float]:
    try:
        return float(v) if v not in (None, "") else None
    except ValueError:
        return None


def _int(v) -> Optional[int]:
    try:
        return int(v) if v not in (None, "") else None
    except ValueError:
        return None


class RedisRepository:
    def __init__(self, redis_client, alpha: float = ALPHA):
        self.r = redis_client
        self.alpha = alpha

    # ---- helpers -------------------------------------------------------------

    async def _bought(self, user_id: int, first) -> set[int]:
        if isinstance(first, ResponseError):          # contract 1.0.0: SET
            return {int(x) for x in await self.r.smembers(rk.user_bought(user_id))}
        return {int(x) for x in first or []}

    async def _last_product(self, user_id: int, first) -> Optional[int]:
        if isinstance(first, ResponseError):          # contract 1.0.0: LIST, newest first
            first = [await self.r.lindex(rk.user_recent(user_id), 0)]
        if not first or first[0] is None:
            return None
        return rk.decode_recent(first[0])[0]

    async def _enrich(self, scored: list[tuple[int, float, str]]) -> list[RecommendedItem]:
        if not scored:
            return []
        pipe = self.r.pipeline(transaction=False)
        for pid, _, _ in scored:
            pipe.hmget(product_key(pid), "category_id", "category_code", "brand", "price")
        attrs = await pipe.execute()
        return [
            RecommendedItem(product_id=pid, score=round(score, 6), source=source,
                            category_id=_int(a[0]), category_code=a[1] or None,
                            brand=a[2] or None, price=_float(a[3]))
            for (pid, score, source), a in zip(scored, attrs)
        ]

    # ---- endpoints -----------------------------------------------------------

    async def recommend(self, user_id: int, k: int) -> RecommendResponse:
        pipe = self.r.pipeline(transaction=False)
        pipe.zrevrange(rk.rec_batch(user_id), 0, BATCH_CANDIDATES - 1, withscores=True)
        pipe.zrevrange(rk.rec_rt(user_id), 0, RT_CANDIDATES - 1, withscores=True)
        pipe.zrange(rk.user_bought(user_id), 0, -1)
        pipe.zrevrange(rk.user_recent(user_id), 0, 0)
        batch_raw, rt_raw, bought_raw, recent_raw = await pipe.execute(raise_on_error=False)
        batch = _scores(batch_raw if not isinstance(batch_raw, Exception) else [])
        realtime = _scores(rt_raw if not isinstance(rt_raw, Exception) else [])
        bought = await self._bought(user_id, bought_raw)

        scored = merge_scores(batch, realtime, alpha=self.alpha, exclude=bought, k=k)

        if len(scored) < k:
            scored += await self._fallback(user_id, recent_raw, k - len(scored),
                                           exclude=bought | {p for p, _, _ in scored},
                                           cap=min((s for _, s, _ in scored), default=1.0))
        return RecommendResponse(
            user_id=user_id, items=await self._enrich(scored), generated_at=datetime.now(timezone.utc),
            batch_available=bool(batch), realtime_available=bool(realtime),
        )

    async def _fallback(self, user_id: int, recent_raw, n: int, exclude: set[int], cap: float):
        """trend:cat of the last viewed category -> trend:global -> pop:global."""
        last = await self._last_product(user_id, recent_raw)
        category = _int(await self.r.hget(product_key(last), "category_id")) if last is not None else None
        sources = []
        if category is not None:
            sources.append((rk.trend_cat(category), "trending"))
        sources += [(rk.TREND_GLOBAL, "trending"), (rk.POP_GLOBAL, "popular")]

        size = n + len(exclude) + 10
        pipe = self.r.pipeline(transaction=False)
        for key, _ in sources:
            pipe.zrevrange(key, 0, size - 1, withscores=True)
        lists = await pipe.execute()

        picked: list[tuple[int, str]] = []
        taken = set(exclude)
        for (key, source), pairs in zip(sources, lists):
            for pid, _ in sorted(_scores(pairs).items(), key=lambda kv: kv[1], reverse=True):
                if len(picked) == n:
                    break
                if pid not in taken:
                    taken.add(pid)
                    picked.append((pid, source))
        # Scores by position, strictly below the lowest merged score: the
        # sources have different scales (weighted counts, popularity).
        return [(pid, cap * (len(picked) - i) / (len(picked) + 1), source)
                for i, (pid, source) in enumerate(picked)]

    async def similar(self, product_id: int, k: int) -> SimilarResponse:
        pipe = self.r.pipeline(transaction=False)
        pipe.zrevrange(rk.sim_cooc(product_id), 0, SIM_CANDIDATES - 1, withscores=True)
        pipe.zrevrange(rk.sim_item(product_id), 0, SIM_CANDIDATES - 1, withscores=True)
        cooc_raw, item_raw = await pipe.execute()
        cooc, item = _scores(cooc_raw), _scores(item_raw)
        kind = "hybrid" if cooc and item else ("item" if item else "cooc")
        if kind == "hybrid":
            # min-max normalised mix; merge_scores labels sim:item "batch", sim:cooc "realtime"
            scored = merge_scores(item, cooc, alpha=0.5, exclude={product_id}, k=k)
        else:                                   # one source: keep the real similarity values
            source = "batch" if kind == "item" else "realtime"
            ranked = sorted((item or cooc).items(), key=lambda kv: kv[1], reverse=True)
            scored = [(p, s, source) for p, s in ranked if p != product_id][:k]
        return SimilarResponse(product_id=product_id, items=await self._enrich(scored), similarity=kind)

    async def trending(self, category_id: Optional[int], k: int) -> TrendingResponse:
        trend_key = rk.TREND_GLOBAL if category_id is None else rk.trend_cat(category_id)
        pop_key = rk.POP_GLOBAL if category_id is None else rk.pop_cat(category_id)
        pairs = await self.r.zrevrange(trend_key, 0, k - 1, withscores=True)
        source = "trending"
        if not pairs:                                   # no activity in the last hour
            pairs = await self.r.zrevrange(pop_key, 0, k - 1, withscores=True)
            source = "popular"
        scored = [(int(p), float(s), source) for p, s in pairs]
        return TrendingResponse(category_id=category_id, items=await self._enrich(scored), window_minutes=60)

    async def health(self) -> HealthResponse:
        try:
            await self.r.ping()
            last = await self.r.get(rk.META_BATCH_LAST_RUN)
        except Exception:
            return HealthResponse(status="degraded", redis=False)
        when = None
        if last:
            try:
                when = datetime.fromisoformat(last.replace("Z", "+00:00"))
            except ValueError:
                when = None
        return HealthResponse(status="ok", redis=True, batch_last_run=when)
