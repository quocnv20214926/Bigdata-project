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
When Redis cannot be reached the API answers `503` (`{"detail": "Redis unavailable: ..."}`).

## Modes

| `API_MODE` | Since | Data source |
|---|---|---|
| `redis` (default in Docker) | Week 5 | `RedisRepository` (`app/redis_repository.py`): real batch and real-time data |
| `mock` | Week 2 | `MockRepository`: deterministic fake data, Redis not required |

The mock produces every response shape the UI must handle, chosen by `user_id % 10`:
`0` new user (trending fallback), `1` batch only, `2` real-time only, otherwise both.

## How `redis` mode answers

**`/recommend/{user_id}`**

1. Read `rec:batch:{user}` (Pair 1, top 50) and `rec:rt:{user}` (Pair 2, top 100).
2. Merge with `common.scoring.merge_scores`: min-max normalisation, then
   `alpha · batch + (1 − alpha) · real-time` (`API_ALPHA`, default 0.5). Without
   `rec:batch`, alpha = 0; without `rec:rt`, alpha = 1. Each item says where it
   comes from: `batch`, `realtime` or `both`.
3. Remove products in `user:bought`.
4. If fewer than `k` products remain, fill up from `trend:cat` of the category
   the user viewed last, then `trend:global`, then `pop:global`
   (`source` = `trending` / `popular`), with scores below the merged ones.
5. Add `category_id`, `category_code`, `brand`, `price` from `rt:prod:{product}`
   (written by the streaming job).

`batch_available` / `realtime_available` tell whether `rec:batch` / `rec:rt` existed.

**`/similar/{product_id}`**: `sim:cooc` (speed layer) and `sim:item` (batch).
One source → its real similarity values (`similarity` = `cooc` or `item`);
both → min-max merged half and half (`hybrid`).

**`/trending[/{category_id}]`**: `trend:cat` / `trend:global` (last hour); when
empty, `pop:cat` / `pop:global` (long-term popularity, `source` = `popular`).

**`/health`**: Redis ping and `meta:batch:last_run` (`status` = `degraded` when Redis is down).

`user:recent` and `user:bought` are read as ZSETs (contract 1.1.0, written by the
streaming job) and also as LIST / SET (contract 1.0.0).

| Variable | Default | Meaning |
|---|---|---|
| `API_MODE` | `redis` in Docker, `mock` otherwise | Data source |
| `API_ALPHA` | 0.5 | Weight of `rec:batch` in the merge |
| `API_BATCH_CANDIDATES` | `settings.batch_top_n` | Products read from `rec:batch` |
| `API_RT_CANDIDATES` | 100 | Products read from `rec:rt` |
| `REDIS_HOST`, `REDIS_PORT`, `REDIS_DB` | localhost, 6379, 0 | Redis |

## Run

```bash
# Local (from the repository root)
pip install -r serving/requirements.txt
API_MODE=redis uvicorn serving.app.main:app --reload --port 8001
# open http://localhost:8001/docs

# Tests (no Redis needed: fakeredis)
python -m pytest serving/tests -q

# Docker
docker compose -f infra/compose.base.yml -f infra/compose.stream.yml up -d --build api
```

## Layout

| File | Purpose |
|---|---|
| `app/main.py` | Routes, timing middleware, 503 on Redis errors, repository selection |
| `app/redis_repository.py` | `RedisRepository`: merge, fallback, enrichment |
| `app/repository.py` | `Repository` interface and `MockRepository` |
| `app/mock_data.py` | Fake catalogue with REES46-like attributes |
| `tests/test_redis_api.py` | `RedisRepository` on fakeredis: merge, bought filter, fallback chain, contract 1.0 types, similar, trending, health, HTTP 503 |
| `tests/test_mock_api.py` | Contract tests of the mock |
