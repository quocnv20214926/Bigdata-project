# Pair 2 – Spark Streaming

Pipeline: Kafka topic `events` → Spark Structured Streaming → Redis.

## Configuration
Reads `KAFKA_BOOTSTRAP`, `KAFKA_TOPIC_EVENTS`, `REDIS_HOST`, `REDIS_PORT`,
`REDIS_DB`, `STARTING_OFFSETS`, `MAX_OFFSETS_PER_TRIGGER`, `TRIGGER_SECONDS`,
`CHECKPOINT_LOCATION`, `EVENT_WATERMARK`, and `RECENT_N` from the environment.
Defaults are intended for local development; Docker Compose should use `kafka:29092`
and `redis` as service hosts.

## Run
Use the repository's documented streaming compose stack:
```powershell
docker compose -f infra/compose.base.yml -f infra/compose.stream.yml up -d --build
```

Check logs:
```powershell
docker compose -f infra/compose.base.yml -f infra/compose.stream.yml logs -f streaming
```

## Redis contract
- `user:recent:{user_id}`: sorted set; member is `product_id|event_type|epoch_seconds`, score is event epoch.
- `user:bought:{user_id}`: sorted set of purchased product IDs, score is event epoch.
- TTLs follow `common/redis_keys.py`.

This is an initial implementation and should be validated against the team's Redis/API expectations before merge. In particular, confirm whether the API expects Redis sorted sets or lists and whether `purchase` is the exact event type in the source data.
