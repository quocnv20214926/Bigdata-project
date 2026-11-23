"""Tests of RedisRepository on fakeredis (no Redis server needed).
Run from the repository root:  python -m pytest serving/tests -q
"""

from __future__ import annotations

import asyncio

import fakeredis
import pytest
from fastapi.testclient import TestClient
from redis.exceptions import ConnectionError as RedisConnectionError

from common import redis_keys as rk
from common.models import RecommendResponse
from serving.app.main import app
from serving.app.redis_repository import RedisRepository, product_key


def run(coro):
    return asyncio.run(coro)


@pytest.fixture()
def r():
    return fakeredis.FakeAsyncRedis(decode_responses=True)


def ids(resp):
    return [i.product_id for i in resp.items]


async def seed_products(r):
    await r.hset(product_key(1), mapping={"category_id": 10, "category_code": "electronics.smartphone",
                                          "brand": "apple", "price": 999.5})
    await r.hset(product_key(2), mapping={"category_id": 10, "brand": "samsung", "price": 500})


def test_merge_batch_and_realtime_excluding_bought(r):
    async def go():
        await seed_products(r)
        await r.zadd(rk.rec_batch(7), {"1": 0.9, "2": 0.5, "3": 0.1})
        await r.zadd(rk.rec_rt(7), {"2": 4.0, "4": 2.0, "5": 0.0})
        await r.zadd(rk.user_bought(7), {"3": 100})                    # contract 1.1.0 ZSET
        return await RedisRepository(r, alpha=0.5).recommend(7, 3)
    resp = run(go())
    assert resp.batch_available and resp.realtime_available
    # normalised: batch 1 -> 1.0, 2 -> 0.5 ; rt 2 -> 1.0, 4 -> 0.5
    assert [(i.product_id, i.score, i.source) for i in resp.items] == [
        (2, 0.75, "both"), (1, 0.5, "batch"), (4, 0.25, "realtime")]
    assert 3 not in ids(resp)
    first = resp.items[1]
    assert (first.category_id, first.category_code, first.brand, first.price) == (10, "electronics.smartphone", "apple", 999.5)
    assert resp.items[2].brand is None                                 # no attributes stored


def test_realtime_only_and_batch_only(r):
    async def go():
        await r.zadd(rk.rec_rt(1), {"5": 1.0, "6": 0.5})
        await r.zadd(rk.rec_batch(2), {"8": 1.0, "9": 0.5})
        repo = RedisRepository(r)
        return await repo.recommend(1, 2), await repo.recommend(2, 2)
    rt_only, batch_only = run(go())
    assert (rt_only.batch_available, rt_only.realtime_available) == (False, True)
    assert {i.source for i in rt_only.items} == {"realtime"} and ids(rt_only) == [5, 6]
    assert (batch_only.batch_available, batch_only.realtime_available) == (True, False)
    assert ids(batch_only) == [8, 9]


def test_new_user_falls_back_to_category_trending_then_global(r):
    async def go():
        await seed_products(r)
        await r.zadd(rk.user_recent(5), {rk.encode_recent(1, "view", 100): 100})   # last viewed: product 1, category 10
        await r.zadd(rk.trend_cat(10), {"2": 30, "1": 20})
        await r.zadd(rk.TREND_GLOBAL, {"2": 30, "50": 25, "51": 1})
        await r.zadd(rk.POP_GLOBAL, {"60": 9})
        return await RedisRepository(r).recommend(5, 5)
    resp = run(go())
    assert not resp.batch_available and not resp.realtime_available
    assert ids(resp) == [2, 1, 50, 51, 60]
    assert [i.source for i in resp.items] == ["trending"] * 4 + ["popular"]
    scores = [i.score for i in resp.items]
    assert scores == sorted(scores, reverse=True)


def test_short_lists_are_filled_and_unknown_user_gets_global(r):
    async def go():
        await r.zadd(rk.rec_rt(3), {"5": 1.0})
        await r.zadd(rk.TREND_GLOBAL, {"5": 10, "7": 5})
        repo = RedisRepository(r)
        return await repo.recommend(3, 3), await repo.recommend(999, 2)
    filled, unknown = run(go())
    assert ids(filled) == [5, 7] and [i.source for i in filled.items] == ["realtime", "trending"]
    assert filled.items[1].score <= filled.items[0].score
    assert ids(unknown) == [5, 7]                                      # no history, no category


def test_contract_1_0_list_and_set_are_still_read(r):
    async def go():
        await seed_products(r)
        await r.lpush(rk.user_recent(4), rk.encode_recent(1, "view", 1))   # LIST
        await r.sadd(rk.user_bought(4), "2")                               # SET
        await r.zadd(rk.trend_cat(10), {"2": 5, "1": 3})
        return await RedisRepository(r).recommend(4, 5)
    resp = run(go())
    assert ids(resp) == [1]                                             # 2 was bought


def test_similar_cooc_item_hybrid(r):
    async def go():
        await r.zadd(rk.sim_cooc(1), {"2": 0.8, "3": 0.4, "1": 1.0})
        await r.zadd(rk.sim_item(1), {"3": 0.9, "4": 0.3})
        await r.zadd(rk.sim_item(9), {"4": 0.3})
        repo = RedisRepository(r)
        return await repo.similar(1, 10), await repo.similar(9, 10), await repo.similar(5, 10)
    hybrid, item_only, none = run(go())
    assert hybrid.similarity == "hybrid" and 1 not in ids(hybrid)
    assert set(ids(hybrid)) == {2, 3, 4} and ids(hybrid)[0] == 3       # 3 is in both lists
    assert item_only.similarity == "item" and item_only.items[0].source == "batch"
    assert item_only.items[0].score == 0.3                              # real similarity, not normalised
    assert none.similarity == "cooc" and none.items == []


def test_trending_and_popular_fallback(r):
    async def go():
        await r.zadd(rk.trend_cat(10), {"1": 5, "2": 9})
        await r.zadd(rk.pop_cat(20), {"7": 3})
        repo = RedisRepository(r)
        return await repo.trending(10, 5), await repo.trending(20, 5), await repo.trending(None, 5)
    cat, pop, glob = run(go())
    assert ids(cat) == [2, 1] and cat.items[0].source == "trending"
    assert ids(pop) == [7] and pop.items[0].source == "popular"
    assert glob.category_id is None and glob.items == []


def test_health_reports_batch_last_run(r):
    async def go():
        await r.set(rk.META_BATCH_LAST_RUN, "2026-11-09T02:00:00Z")
        return await RedisRepository(r).health()
    h = run(go())
    assert h.status == "ok" and h.redis and h.batch_last_run.isoformat() == "2026-11-09T02:00:00+00:00"


def test_http_endpoints_and_503_when_redis_is_down(r):
    run(r.zadd(rk.rec_rt(7), {"5": 1.0}))
    with TestClient(app) as c:
        app.state.repo = RedisRepository(r)
        body = RecommendResponse.model_validate(c.get("/recommend/7?k=1").json())
        assert ids(body) == [5]
        assert c.get("/similar/5").status_code == 200 and c.get("/trending/10").status_code == 200

        class Down:
            def pipeline(self, transaction=False):
                raise RedisConnectionError("down")
        app.state.repo = RedisRepository(Down())
        resp = c.get("/recommend/7")
        assert resp.status_code == 503 and "Redis unavailable" in resp.json()["detail"]


def test_metrics_endpoint_counts_requests_and_sources():
    """/metrics exposes request counts by route template and recommended items by source."""
    from fastapi.testclient import TestClient

    from serving.app import main

    with TestClient(main.app) as c:
        c.get("/recommend/123?k=5")
        c.get("/trending?k=3")
        body = c.get("/metrics").text
    assert 'api_requests_total{method="GET",route="/recommend/{user_id}",status="200"}' in body
    assert 'api_request_duration_seconds_bucket{le="0.005",route="/trending"}' in body
    assert "api_recommended_items_total" in body
    assert 'route="/metrics"' not in body
