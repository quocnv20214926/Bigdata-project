"""Contract tests for the mock API. Run from the repository root:
    python -m pytest serving/tests -q
Redis is not needed (health reports redis=false when it is down).
"""

from __future__ import annotations

from fastapi.testclient import TestClient

from common.models import RecommendResponse, SimilarResponse, TrendingResponse
from serving.app.main import app
from serving.app.mock_data import CATALOGUE, CATEGORY_IDS


def client() -> TestClient:
    return TestClient(app)


def test_recommend_shape_and_determinism():
    with client() as c:
        r1 = c.get("/recommend/541312143?k=7")
        r2 = c.get("/recommend/541312143?k=7")
    assert r1.status_code == 200
    body = RecommendResponse.model_validate(r1.json())
    assert len(body.items) == 7
    assert [i.product_id for i in body.items] == [i["product_id"] for i in r2.json()["items"]]
    scores = [i.score for i in body.items]
    assert scores == sorted(scores, reverse=True)
    assert body.batch_available and body.realtime_available
    assert r1.headers["X-API-Mode"] == "mock" and "X-Process-Time-Ms" in r1.headers


def test_recommend_fallback_cases():
    with client() as c:
        new_user = c.get("/recommend/100").json()
        batch_only = c.get("/recommend/101").json()
        rt_only = c.get("/recommend/102").json()
    assert {i["source"] for i in new_user["items"]} == {"trending"}
    assert not new_user["batch_available"] and not new_user["realtime_available"]
    assert batch_only["batch_available"] and not batch_only["realtime_available"]
    assert rt_only["realtime_available"] and not rt_only["batch_available"]


def test_k_validation():
    with client() as c:
        assert c.get("/recommend/1?k=0").status_code == 422
        assert c.get("/recommend/1?k=51").status_code == 422
        assert c.get("/recommend/abc").status_code == 422


def test_similar_excludes_itself():
    pid = CATALOGUE[3].product_id
    with client() as c:
        body = SimilarResponse.model_validate(c.get(f"/similar/{pid}?k=10").json())
    ids = [i.product_id for i in body.items]
    assert pid not in ids and len(ids) == len(set(ids)) == 10
    assert body.similarity == "cooc"


def test_trending_category_and_global():
    cat = CATEGORY_IDS[0]
    with client() as c:
        by_cat = TrendingResponse.model_validate(c.get(f"/trending/{cat}?k=5").json())
        glob = TrendingResponse.model_validate(c.get("/trending?k=5").json())
    assert by_cat.category_id == cat and all(i.category_id == cat for i in by_cat.items)
    assert glob.category_id is None and len(glob.items) == 5


def test_health():
    with client() as c:
        body = c.get("/health").json()
    assert body["status"] == "ok" and isinstance(body["redis"], bool)
