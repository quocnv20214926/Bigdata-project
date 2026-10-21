# serving/: Recommendation API (Pair 2)

FastAPI service implementing the API contract in `common/models.py`.

| Endpoint | Response model |
|---|---|
| `GET /recommend/{user_id}?k=10` | `RecommendResponse` |
| `GET /similar/{product_id}?k=10` | `SimilarResponse` |
| `GET /trending?k=10` | `TrendingResponse` (global) |
| `GET /trending/{category_id}?k=10` | `TrendingResponse` |
| `GET /health` | `HealthResponse` |

Every response carries `X-Process-Time-Ms` (server-side latency) and `X-API-Mode`.

## Modes

| `API_MODE` | Status | Data source |
|---|---|---|
| `mock` (default) | Week 2 | `MockRepository`: deterministic fake data, Redis not required |
| `redis` | Week 5 | `RedisRepository`: `rec:batch` + `rec:rt` merged with `common.scoring.merge_scores`, fallback to trending/popular |

The mock produces every response shape the UI must handle, chosen by `user_id % 10`:
`0` new user (trending fallback), `1` batch only, `2` real-time only, otherwise both.

## Run

```bash
# Local (from the repository root)
pip install -r serving/requirements.txt
uvicorn serving.app.main:app --reload --port 8000
# open http://localhost:8000/docs

# Tests
python -m pytest serving/tests -q

# Docker
docker compose -f infra/compose.base.yml -f infra/compose.stream.yml up -d --build api
```

## Layout

| File | Purpose |
|---|---|
| `app/main.py` | Routes, timing middleware, repository selection |
| `app/repository.py` | `Repository` interface and `MockRepository` |
| `app/mock_data.py` | Fake catalogue with REES46-like attributes |
| `tests/` | Contract tests (responses validate against `common/models.py`) |
