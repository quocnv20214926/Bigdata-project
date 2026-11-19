"""Redis repository contract tests. Requires fakeredis and pytest."""
from __future__ import annotations

import asyncio
from datetime import datetime, timezone

import fakeredis.aioredis

from common import redis_keys as rk
from serving.app.repository import RedisRepository


def test_recommend_merges_sources_and_excludes_purchased_products():
    async def run():
        r = fakeredis.aioredis.FakeRedis(decode_responses=True)
        await r.zadd(rk.rec_batch(7), {"100": 10, "101": 5, "102": 1})
        await r.zadd(rk.rec_rt(7), {"101": 8, "103": 4})
        await r.zadd(rk.user_bought(7), {"100": 123})
        result = await RedisRepository(r).recommend(7, 10)
        ids = [x.product_id for x in result.items]
        assert 100 not in ids
        assert set(ids) == {101, 102, 103}
        assert result.batch_available and result.realtime_available
        assert next(x for x in result.items if x.product_id == 101).source == "both"
        await r.aclose()
    asyncio.run(run())


def test_new_user_falls_back_to_trending():
    async def run():
        r = fakeredis.aioredis.FakeRedis(decode_responses=True)
        await r.zadd(rk.TREND_GLOBAL, {"11": 9, "12": 5})
        await r.zadd(rk.user_bought(9), {"11": 1})
        result = await RedisRepository(r).recommend(9, 5)
        assert [x.product_id for x in result.items] == [12]
        assert result.items[0].source == "trending"
        assert not result.batch_available and not result.realtime_available
        await r.aclose()
    asyncio.run(run())


def test_similar_prefers_hybrid_when_both_sources_exist_and_excludes_self():
    async def run():
        r = fakeredis.aioredis.FakeRedis(decode_responses=True)
        await r.zadd(rk.sim_item(1), {"1": 1, "2": .9})
        await r.zadd(rk.sim_cooc(1), {"3": .8, "2": .7})
        result = await RedisRepository(r).similar(1, 10)
        assert result.similarity == "hybrid"
        assert 1 not in [x.product_id for x in result.items]
        assert {x.product_id for x in result.items} == {2, 3}
        await r.aclose()
    asyncio.run(run())


def test_trending_and_health():
    async def run():
        r = fakeredis.aioredis.FakeRedis(decode_responses=True)
        await r.zadd(rk.trend_cat(5), {"1": 4, "2": 2})
        await r.set(rk.META_BATCH_LAST_RUN, "2026-11-19T01:02:03Z")
        repo = RedisRepository(r)
        result = await repo.trending(5, 1)
        assert result.items[0].product_id == 1
        health = await repo.health()
        assert health.status == "ok" and health.redis
        assert health.batch_last_run is not None
        await r.aclose()
    asyncio.run(run())


def test_new_user_uses_batch_popularity_when_streaming_trending_is_empty():
    async def run():
        r = fakeredis.aioredis.FakeRedis(decode_responses=True)
        await r.zadd(rk.POP_GLOBAL, {"21": 8, "22": 4})
        result = await RedisRepository(r).recommend(88, 5)
        assert [x.product_id for x in result.items] == [21, 22]
        assert {x.source for x in result.items} == {"popular"}
        await r.aclose()
    asyncio.run(run())


def test_single_source_recommendations_degrade_gracefully():
    async def run():
        r = fakeredis.aioredis.FakeRedis(decode_responses=True)
        await r.zadd(rk.rec_batch(1), {"1": 2, "2": 1})
        batch = await RedisRepository(r).recommend(1, 5)
        assert batch.batch_available and not batch.realtime_available
        assert {x.source for x in batch.items} == {"batch"}
        await r.delete(rk.rec_batch(1))
        await r.zadd(rk.rec_rt(1), {"3": 2, "4": 1})
        realtime = await RedisRepository(r).recommend(1, 5)
        assert not realtime.batch_available and realtime.realtime_available
        assert {x.source for x in realtime.items} == {"realtime"}
        await r.aclose()
    asyncio.run(run())
