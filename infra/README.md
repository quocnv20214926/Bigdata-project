# infra/: Docker Compose

| File | Services | Owner |
|---|---|---|
| `compose.base.yml` | Redis; MinIO + `minio-init` (bucket `lake`) behind profile `lake` | Shared |
| `compose.batch.yml` | Spark master/workers, Airflow | Pair 1 |
| `compose.stream.yml` | Kafka (KRaft), `kafka-init` (topic `events`), Kafka UI, streaming job, API, optional replayer | Pair 2 |

All files use project name `recsys`, so services reach each other by name
(`redis:6379`, `kafka:29092`, `minio:9000`).

## Commands (from the repository root)

```bash
# Real-time pipeline only
docker compose -f infra/compose.base.yml -f infra/compose.stream.yml up -d --build
docker compose -f infra/compose.base.yml -f infra/compose.stream.yml ps
docker compose -f infra/compose.base.yml -f infra/compose.stream.yml logs -f streaming
docker compose -f infra/compose.base.yml -f infra/compose.stream.yml down        # keep data
docker compose -f infra/compose.base.yml -f infra/compose.stream.yml down -v     # also delete volumes

# Everything (MinIO is enabled with --profile lake)
docker compose --profile lake -f infra/compose.base.yml -f infra/compose.batch.yml -f infra/compose.stream.yml up -d
```

## Ports

| Port | Service |
|---|---|
| 6379 | Redis |
| 9000 / 9001 | MinIO API / console (minioadmin / minioadmin), only with `--profile lake` |
| 9092 | Kafka for host clients (`localhost:9092`); containers use `kafka:29092` |
| 8085 | Kafka UI |
| 8000 | Recommendation API (`/docs`) |
| 4040 | Spark UI of the streaming job |
| 7077 | Spark master (`compose.batch.yml`) |
| 8090 / 8091 | Spark master / worker UI (`compose.batch.yml`) |
| 8080 | Airflow web UI (`compose.batch.yml`); admin password in `docker compose logs airflow` |
