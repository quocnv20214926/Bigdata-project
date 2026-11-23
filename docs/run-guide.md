# Run Guide and Demo Script (Pair 2: real-time pipeline)

All commands run in **PowerShell** from the repository root (`Bigdata-project`).
bash equivalents work the same (`scripts/stack.sh` instead of `dcall`).

## 0. Once per machine

1. Docker Desktop (WSL 2 backend), **at least 8 GB of RAM** for Docker (Settings → Resources).
2. Python 3.11+ and a virtual environment:
   ```powershell
   python -m venv .venv
   .\.venv\Scripts\Activate.ps1
   pip install -r ingestion\requirements.txt -r serving\requirements.txt -r ui\requirements.txt `
       -r serving\loadtest\requirements.txt pytest fakeredis httpx
   ```
3. `Copy-Item .env.example .env` (keep `API_MODE=redis`).
4. Data: `data\raw\2019-Oct.csv` and the sample `data\sample\oct_sample.csv`
   (`python -m ingestion.inspect_raw data\raw\2019-Oct.csv --limit 200000 --sample-out data\sample\oct_sample.csv --sample-rows 200000`).

Every new terminal:

```powershell
.\.venv\Scripts\Activate.ps1
. .\scripts\load-env.ps1          # defines dc (real-time stack) and dcall (whole system)
```

## 1. Start the stack

```powershell
dc up -d --build                  # real-time stack: Redis, Kafka, streaming, API, UI, Prometheus, Grafana
# or the whole system (adds Airflow of Pair 1):
dcall up -d --build
dc ps
```

| Service | URL |
|---|---|
| Streamlit demo | http://localhost:8501 |
| API (Swagger) | http://localhost:8000/docs |
| Grafana (admin / admin) | http://localhost:3000 |
| Prometheus | http://localhost:9090 |
| Kafka UI | http://localhost:8085 |
| Spark UI (streaming job) | http://localhost:4040 |
| Airflow (whole system only) | http://localhost:8080 |

`kafka-init` and `minio-init` show `exited (0)`: one-shot jobs, this is correct.

### Fresh start (recommended before a demo)

The streaming job remembers where it stopped (checkpoint) and drops events older
than its watermark. To replay from the beginning:

```powershell
dc down                                        # Kafka has no volume: the topic is emptied
docker volume rm recsys_stream-checkpoints     # name: docker volume ls | Select-String checkpoints
dc up -d redis
docker exec redis redis-cli FLUSHDB            # WARNING: also deletes Pair 1 keys (rec:batch, sim:item, pop:*)
dc up -d --build
```

## 2. Fill the speed layer

```powershell
# a) Warm-up: the 200,000-event sample (Oct 1, 00:00-06:06) as fast as possible (~1 min)
python -m ingestion.replayer --file data\sample\oct_sample.csv --speed 0

# b) Check that everything is connected
python scripts\check_stack.py
```

`check_stack.py` checks Redis, the streaming progress, the keys of both pairs, every
API endpoint, the UI, Prometheus targets and Grafana. Warnings about `rec:batch` /
`sim:item` / `pop:global` only mean Pair 1's nightly batch has not been loaded: the API then
serves real-time + trending (the designed fallback).

```powershell
# c) Live replay for the demo: continues after the sample, 1 hour of data per minute
python -m ingestion.replayer --file data\raw\2019-Oct.csv --start-time 2019-10-01T06:07:00Z --speed 60 --max-rate 2000
```

Leave (c) running in its own terminal during the demo (Ctrl+C stops it cleanly).

## 3. Demo script (about 5 minutes)

1. **Streamlit → Recommendations.** The header shows the stream clock moving (the 2019
   event time), events processed and the last micro-batch. Pick a user in the sidebar
   ("Users with real-time scores" → *Use this user*). Left: recent activity; right:
   recommendations with their source.
2. **Simulate.** Choose a product of another category (type an id, e.g. one from the
   *Trending & similar* page), choose `view` or `cart`, click *Send*. The page waits for
   the speed layer (2–3 s) and shows the new list; products that were not in the
   previous list are marked `new`. Repeat with `purchase`: the product disappears from
   the recommendations (it is in `user:bought`).
3. **New user.** Type an unknown id (e.g. `1999999999`): `source = trending`, no history.
4. **Trending & similar.** Pick a category: trending of the last hour of event time;
   pick a product: similar products from `sim:cooc` (and `sim:item` / `hybrid` when Pair 1
   has loaded its batch).
5. **Grafana.** Dashboard *Recommender: real-time pipeline and API*: input vs processed
   events/s, micro-batch duration, stream clock, API requests and p95, recommendation
   sources, Redis memory and keys.
6. **Evaluation page.** Speed-only results, similarity ablation, batch results of Pair 1,
   load test.
7. **Kafka UI** (topic `events`, 6 partitions, key = user_id) and **Spark UI**
   (*Structured Streaming* tab: input rate and process rate).

### Batch + real-time together (integration)

After Pair 1's nightly batch has loaded Redis (`batch/README.md`, Airflow DAG
`nightly_batch` or `python -m batch.nightly ... load`), `check_stack.py` shows the batch
keys and the API merges both layers: `source` = `batch`, `realtime` or `both`,
`batch_available` = `realtime_available` = true. Weight of the batch layer:
`API_ALPHA` (default 0.5). Neighbours of `rec:rt`: `RT_SIMILARITY=cooc|item|hybrid` in
`.env`, then `dc up -d streaming`.

## 4. Tests and measurements (send the outputs back for the reports)

```powershell
# Unit and end-to-end tests (the Spark test needs Java 17; on Windows run it in WSL)
python -m pytest ingestion serving ui streaming -q

# Load test (API and Redis filled): 500 and 1,000 requests/s
$env:LOADTEST_CLIENT_RPS=20
locust -f serving/loadtest/locustfile.py --host http://localhost:8000 --headless -u 25 -r 50 -t 60s --csv docs/loadtest/rate500 --only-summary
locust -f serving/loadtest/locustfile.py --host http://localhost:8000 --headless -u 50 -r 50 -t 60s --csv docs/loadtest/rate1000 --only-summary
python -m serving.loadtest.summarize docs/loadtest/rate500 --label "500 requests/s" --json docs/loadtest/rate500.json
python -m serving.loadtest.summarize docs/loadtest/rate1000 --label "1,000 requests/s" --json docs/loadtest/rate1000.json
Remove-Item Env:LOADTEST_CLIENT_RPS

# Speed-only evaluation (needs a second Redis: its database is emptied)
docker run -d --name redis-eval -p 6390:6379 redis:7
python -m streaming.evaluation.replay_eval --file data\raw\2019-Oct.csv `
    --warmup-until 2019-10-02T00:00:00Z --end 2019-10-02T12:00:00Z --sample 0.25 `
    --redis-port 6390 --json docs\eval-pair2-week07.json

# Similarity ablation: sim:item trained on the warm-up day only (Pair 1's Item-kNN; needs PySpark + Java)
python -m streaming.evaluation.build_sim_item --file data\raw\2019-Oct.csv --until 2019-10-02T00:00:00Z --out data\eval\sim_item_oct01.csv
foreach ($v in "cooc", "item", "hybrid") {
  python -m streaming.evaluation.replay_eval --file data\raw\2019-Oct.csv --warmup-until 2019-10-02T00:00:00Z `
      --end 2019-10-02T12:00:00Z --sample 0.25 --sim-item data\eval\sim_item_oct01.csv --rt-similarity $v `
      --redis-port 6390 --flush --json docs\eval-pair2-week08-$v.json
}
```

Each replay takes 30–60 minutes on a 2-core machine (it runs the real sinks event by event).

## 5. Common problems

| Symptom | Cause / fix |
|---|---|
| `check_stack.py`: streaming progress missing | `dc logs streaming`. Out of memory: more RAM for Docker, or stop Airflow (`dc` instead of `dcall`) |
| Simulated event "not processed within 30 s" | Streaming container stopped or restarting; or Kafka not reachable from the UI container (`dc logs ui`) |
| Recommendations never change during the live replay | The replay is older than the watermark (it restarted from an earlier time): fresh start (section 1) |
| Grafana panels "No data" | Prometheus targets: http://localhost:9090/targets must show `api` and `speed-layer` up |
| API answers 503 | Redis is down: `dc up -d redis` |
| Port already in use | Stop the program using it, or change the left side of `ports:` in `infra/compose.stream.yml` |
