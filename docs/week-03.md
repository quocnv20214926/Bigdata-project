# Week 03 – Pair 2: Real-time pipeline

## Scope
- Task D: read Kafka JSON events with Spark Structured Streaming, parse using the shared schema, validate required fields, apply event-time watermark/deduplication, and persist checkpoints.
- Task C: use `foreachBatch` to update Redis user state for recent activity and purchased products.

## Files added
- `streaming/main.py`: streaming source, parsing, watermark, deduplication, checkpoint and Redis sink.
- `streaming/Dockerfile`, `streaming/requirements.txt`: container/runtime setup.
- `streaming/tests/test_streaming.py`: shared Redis key contract smoke tests.

## Data contracts
- Kafka topic: `events`; JSON schema from `common.schemas.spark_event_schema()`.
- Redis keys and recent-member encoding from `common.redis_keys`.
- Recent state uses `user:recent:{user_id}`; purchase state uses `user:bought:{user_id}`.

## Validation checklist
- [ ] Build the Compose streaming image.
- [ ] Start Kafka and Redis services.
- [ ] Replay a small sample of events.
- [ ] Verify streaming logs show processed batches.
- [ ] Inspect Redis keys and compare the API's expected data structure.
- [ ] Restart the streaming service and confirm checkpoint recovery.

## Notes / follow-ups
The exact Redis data structure and purchase event names must match the shared API/data contract. Validate these with the team before merging. Avoid modifying `common/` without approval.
