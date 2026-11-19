# Recommendation API (Pair 2)

The FastAPI service supports two modes:

- `API_MODE=mock`: deterministic demo data; Redis is optional.
- `API_MODE=redis`: reads `rec:batch`, `rec:rt`, `user:bought`, `sim:item`, `sim:cooc`, `trend:*`, and `meta:batch:last_run` from Redis.

Run from the repository root:

```bash
pip install -r serving/requirements.txt
API_MODE=redis REDIS_HOST=localhost uvicorn serving.app.main:app --reload --port 8000
```

Endpoints: `/recommend/{user_id}`, `/similar/{product_id}`, `/trending`, `/trending/{category_id}`, `/health`.

Recommendation flow:
1. Read batch and real-time ZSETs.
2. Exclude all products in `user:bought:{user_id}`.
3. Normalize and merge scores using `common.scoring.merge_scores` (default batch weight 0.5).
4. If both sources are empty, fall back to `trend:global`.

Product metadata is not currently stored in the Redis contract, so Redis-mode responses leave optional category/brand/price fields null except category-scoped trending.
