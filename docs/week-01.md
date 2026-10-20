# Week 1 Report: Project Setup

## Pair 1: Batch 

_Written by Pair 1._

## Pair 2: Real-time 

### 1. Weekly goals

- Write the shared infrastructure file `infra/compose.base.yml` (Redis, MinIO) with the whole team.
- Write Pair 2(real-time)'s infrastructure file `infra/compose.stream.yml` (Kafka, Kafka UI, topic creation).
- Download the REES46 dataset (manually, from the dataset page) and verify its columns, event types and sort order.
- Create a small sample file for daily development.

### 2. Completed work

| Task | Files | Owner |
|---|---|---|
| Shared services: Redis 7.2 (2 GB, `volatile-lru`, healthcheck, volume); MinIO + one-shot `minio-init` (AWS CLI) creating bucket `lake`, behind compose profile `lake` | `infra/compose.base.yml` |  (whole team) |
| Kafka 3.8.1 in KRaft mode (no ZooKeeper), two listeners (`kafka:29092` for containers, `localhost:9092` for the host), auto topic creation disabled | `infra/compose.stream.yml` | Quoc|
| One-shot `kafka-init` creating topic `events` (6 partitions, 7-day retention) | `infra/compose.stream.yml` | Quoc |
| Kafka UI on port 8085 (8080 left free for Pair 1) | `infra/compose.stream.yml` | Quoc |
| Raw data inspector: header check against `RAW_COLUMNS`, event type counts, empty values, time range, events per day, out-of-order rows; writes a sample CSV | `ingestion/inspect_raw.py` | Quoc |
| `.dockerignore` (keeps `data/` out of build contexts), `.env.example`, `scripts/load-env.ps1` | repository root, `scripts/` | Quoc|
| README for `infra/` (commands, ports) | `infra/README.md` | Quoc |

### 3. Results / metrics

Data check (`python -m ingestion.inspect_raw data/raw/2019-Oct.csv`):

| Check | Result |
|---|---|
| Header matches contract | Yes (9 columns, same order as `RAW_COLUMNS`) |
| Rows (October) | 42,448,764 (0 rows with unparseable time) |
| Event types (view / cart / purchase) | 40,779,399 (96.07%) / 926,516 (2.18%) / 742,849 (1.75%) |
| `remove_from_cart` present | No (confirms the note in the plan) |
| Empty `category_code` / `brand` | 13,515,609 (31.84%) / 6,113,008 (14.40%); `user_session` empty in 2 rows; all other columns complete |
| Time range | 2019-10-01 00:00:00 → 2019-10-31 23:59:59 UTC |
| Out-of-order rows / max lag (s) | 0 / 0 (file is sorted by `event_time`) |
| Average events per day | 1,369,315 (min 1,127,303 on Oct 3, max 1,639,071 on Oct 13), about 15.8 events/s in real time |

Implications for the real-time pipeline:
- The file is strictly sorted by time, so the replayer can stream it in file order and a 10-minute watermark is more than enough.
- `category_code` and `brand` must be treated as optional by the streaming job, the API and the UI.
- Replay rates: `--speed 60` ≈ 950 events/s, `--speed 600` ≈ 9,500 events/s on average.
- Full scan took 1,737 s (≈ 24,400 rows/s); full report in `docs/data-check-oct.json`.

Infrastructure check (`docker compose ... ps`): `redis` and `kafka` healthy; `kafka-ui`, `api`, `streaming` running. `kafka-init` exited with code 0 (topic `events` created, 6 partitions). MinIO (`--profile lake`, used by Pair 1): [TO FILL after `dc --profile lake up -d`]

### 4. Issues & resolutions

| Issue | Resolution |
|---|---|
| Kafka must be reachable both from containers and from the host (replayer on Windows) | Two listeners: `INTERNAL://kafka:29092` and `EXTERNAL://localhost:9092` |
| Port 8080 is needed by Pair 1 (Spark master / Airflow) | Kafka UI mapped to 8085 |
| Raw CSV files (~14 GB) would be sent to Docker on every build | `.dockerignore` excludes `data/` |
| Docker Hub deleted `minio/minio` and `minio/mc` (September 2026); a failed pull stopped the whole stack | MinIO server from `cgr.dev/chainguard/minio`, bucket created with `amazon/aws-cli`; MinIO moved behind profile `lake` (only Pair 1 needs it), image configurable via `MINIO_IMAGE` |

### 5. Plan for next week

- Freeze the contracts in `common/` with Pair 1, export the OpenAPI spec.
- FastAPI skeleton returning mock data.
- Event Replayer (CSV → Kafka) with time-based pacing.
