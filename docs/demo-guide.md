# Demo Guide (Pair 2: real-time recommendations)

Step-by-step guide to run and present the demo on Windows (PowerShell, Docker Desktop).
Tested on 2026-11-23: `python scripts\check_stack.py` → 14 ok, 1 warning (batch keys, expected without Pair 1's nightly run), 0 failures.

The full reference (all services, tests, measurements, troubleshooting) is [run-guide.md](run-guide.md).

## 1. One-time setup

```powershell
cd C:\Users\<you>\Bigdata-project
py -3.12 -m venv .venv                     # use Python 3.12, not an older "python" on PATH
.\.venv\Scripts\Activate.ps1               # if blocked: Set-ExecutionPolicy -Scope CurrentUser RemoteSigned
python -m pip install --upgrade pip
pip install -r ingestion\requirements.txt -r serving\requirements.txt -r ui\requirements.txt -r serving\loadtest\requirements.txt pytest fakeredis httpx
Copy-Item .env.example .env                # only if .env does not exist; keep API_MODE=redis
```

Data files: `data\raw\2019-Oct.csv` (Kaggle, REES46) and the 200,000-event sample `data\sample\oct_sample.csv`.

Docker Desktop needs at least 8 GB of RAM (Settings → Resources).

## 2. Before each demo (about 5 minutes)

Every new terminal:

```powershell
.\.venv\Scripts\Activate.ps1
. .\scripts\load-env.ps1                   # defines dc (real-time stack) and dcall (whole system)
```

Fresh start, so the replay starts from the beginning:

```powershell
dc down
docker volume rm recsys_stream-checkpoints
dc up -d redis
docker exec redis redis-cli FLUSHDB        # also deletes Pair 1's keys
dc up -d --build                           # first build: 5-10 minutes
```

Wait about 1 minute (the streaming job starts Spark), then fill the speed layer and check:

```powershell
python -m ingestion.replayer --file data\sample\oct_sample.csv --speed 0
python scripts\check_stack.py
```

Expected: every line `ok` except `batch layer keys (Pair 1)` (`warn`). If the streaming lines fail, wait one more minute and run `check_stack.py` again.

Live replay, in a **second terminal** (leave it running during the demo; Ctrl+C stops it):

```powershell
.\.venv\Scripts\Activate.ps1
python -m ingestion.replayer --file data\raw\2019-Oct.csv --start-time 2019-10-01T06:07:00Z --speed 60 --max-rate 2000
```

`--speed 60` plays one hour of 2019 data per minute.

## 3. Demo script (5 minutes)

| # | Where | What to do | What to point out |
|---|---|---|---|
| 1 | http://localhost:8501, **Recommendations** | Look at the three numbers at the top | The stream clock (2019 event time) and the processed events grow: Kafka → Spark → Redis is live; a micro-batch every ~2 s |
| 2 | same page, sidebar | *Users with real-time scores* → pick one → **Use this user** | Left: the user's recent activity (`user:recent`). Right: recommendations, each with its source (`realtime`, `batch`, `both`, `trending`, `popular`) and the API latency (a few ms) |
| 3 | *Simulate an event* | Type a product of another category (e.g. `1005115`, a smartphone) in the product field, press Enter, choose `view`, click **Send** | Processed by the speed layer in 1–3 s; the list switches to that category, new products marked `new` |
| 4 | same | Pick a recommended product, choose `purchase`, **Send** | The purchased product disappears from the recommendations (`user:bought`) |
| 5 | sidebar → **User ID** | Type an unknown id, e.g. `1999999999` | New user: no history, served from trending (graceful fallback) |
| 6 | **Trending & similar** | Trending tab: pick a category. Similar tab: type a product id | Trending = weighted views/carts/purchases of the last hour. Similar = products viewed in the same sessions (`sim:cooc`) |
| 7 | **Evaluation** | Speed layer tab | Replay of real events: `/recommend` finds the user's next product in the top 10 for 19.6 % of the cases, 5× global trending and +23 % over trending in the category; half-life 5 min is best |
| 8 | **Evaluation** | Load test tab | p95 24 ms at 500 requests/s, 62 ms at ~1,000 requests/s (target < 100 ms) |
| 9 | http://localhost:3000 (admin / admin) | Dashboard *Recommender: real-time pipeline and API* | Input vs processed events/s, micro-batch duration, stream clock, API requests and p95, recommendation sources, Redis memory |
| 10 | http://localhost:8085 and http://localhost:4040 | Kafka UI: topic `events`. Spark UI: *Structured Streaming* tab | 6 partitions, key = user_id; input rate vs process rate |

## 4. Questions to expect

- **Why are the events from 2019?** The dataset is historical; the replayer plays it back by event time. Simulated events are stamped just after the newest event processed, otherwise the watermark would drop the replay.
- **Where does a recommendation come from?** `rec:rt` = for each event on product *i*, its co-viewed products *j* get `weight(event) × sim(i, j) × exp(−λ·Δt)`, half-life 5 minutes. Merged with Pair 1's ALS list (`rec:batch`, weight 0.1) when it exists; purchases removed; trending fills short lists.
- **What if a layer is down?** No `rec:batch` → real-time + trending (this demo). No Redis → the API answers 503 with a clear message. A restarted streaming job continues from its checkpoint.
- **Why "None" brands or categories?** The data itself: about a third of the events have no `category_code`, 15 % no `brand`; there are no product names or images.

## 5. If something goes wrong

| Symptom | Fix |
|---|---|
| `check_stack.py`: streaming progress missing | Spark is still starting: wait 1-2 minutes. Otherwise `dc logs --tail 80 streaming` |
| Simulated event "not processed within 30 s" | `dc ps`: is `streaming` up? `dc logs --tail 50 ui` |
| Recommendations do not change during the live replay | The replay went back in time: do the fresh start (section 2) |
| Grafana panels "No data" | http://localhost:9090/targets: `api` and `speed-layer` must be up |
| `.venv\Scripts\Activate.ps1` not found | Create the venv (section 1) |
| `pip` cannot resolve `confluent-kafka` | Pull the latest branch: all requirements pin 2.16.0 |
