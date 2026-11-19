# Week 05 — Pair 2: Redis-backed recommendation serving

## 1. Goals

- Publish per-user real-time recommendation scores under the existing `rec:rt:{user_id}` Redis contract.
- Implement the production Redis repository behind the existing FastAPI endpoints.
- Merge Pair 1 batch recommendations with Pair 2 real-time recommendations.
- Exclude purchased products and provide a cold-start fallback.

## 2. Implementation

| Work item | Implementation |
|---|---|
| Real-time score propagation | `streaming/sinks/realtime.py`: each `view`, `cart`, or `purchase` on item *i* adds `event_weight × sim:cooc(i,j)` to candidate *j* in `rec:rt:{user_id}`; `remove_from_cart` is ignored |
| Streaming integration | `streaming/main.py` now writes `rec:rt` after refreshing the co-occurrence similarities |
| Redis serving repository | `serving/app/repository.py`: reads batch, real-time, bought, item similarity, co-occurrence, and trending keys |
| Score merge | `common.scoring.merge_scores`: min-max normalizes both sources and combines them with batch weight 0.5 by default; if one source is missing, the other gets full weight |
| Seen/purchased filter | `user:bought:{user_id}` is excluded from the final recommendations |
| Cold-start fallback | If both personalized sources are empty, use `trend:global` |
| Similar products | Merge `sim:item` and `sim:cooc` when both exist; otherwise serve whichever exists |
| Health | `/health` checks Redis and reads `meta:batch:last_run` where available |

## 3. Redis key contract

- `rec:rt:{user_id}` — ZSET, member `product_id`, score is accumulated real-time interest, TTL 6 hours.
- Existing Pair 1 keys remain read-only to Pair 2.
- Existing Pair 2 `user:bought`, `sim:cooc`, and `trend:*` keys are read using the documented ZSET contracts.

## 4. Correctness and limitations

- Candidate products are never the same as the source item in the co-occurrence list.
- `remove_from_cart` has weight 0 and cannot add recommendation score.
- The API returns the same Pydantic response models as mock mode.
- Redis does not contain product metadata in the current contract. Therefore brand, price, and category metadata are null for personalized recommendations in Redis mode; the API does not invent catalogue details.
- Delivery is at-least-once across the Redis write / Spark checkpoint boundary. If the process crashes after incrementing Redis but before committing the micro-batch guard, increments may be replayed. The report does not claim end-to-end exactly-once semantics.

## 5. Validation

Automated tests added:
- `streaming/tests/test_sinks.py`: weighted propagation, zero-weight event handling, self-item exclusion, TTL.
- `serving/tests/test_redis_repository.py`: merge, purchase exclusion, cold-start fallback, hybrid similarity, trending, health.

Run from repository root:

```bash
pip install -r serving/requirements.txt
pip install -r requirements-dev.txt
python -m pytest streaming/tests serving/tests -q
```

Record the actual test output and runtime environment before presenting this as a measured result. Docker integration and end-to-end streaming tests must be run in an environment with Docker available.
