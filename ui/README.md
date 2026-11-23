# Streamlit demo (Pair 2)

| Page | Week | Content |
|---|---|---|
| Recommendations | 6 | A user's recent activity (`user:recent`) and recommendations from `/recommend` with their source (`batch`, `realtime`, `both`, `trending`, `popular`). "Send view / cart / purchase" publishes an event to Kafka, waits until the streaming job has written it to `user:recent`, then shows the new list with the products that were not in the old one marked `new`. |
| Trending & similar | 6 | `/trending` and `/trending/{category}` (1-hour window, falls back to `pop:*`), `/similar/{product}` (`cooc`, `item` or `hybrid`) |
| Evaluation | 8 | Offline results and load test |

## Run

```bash
# Docker (with the rest of the Pair 2 stack)
docker compose -f infra/compose.base.yml -f infra/compose.stream.yml up -d --build ui
# open http://localhost:8501

# Local, from the repository root (API on :8000, Redis on :6379, Kafka on :9092)
pip install -r ui/requirements.txt
streamlit run ui/app.py

# Tests (fakeredis, no Kafka)
python -m pytest ui -q
```

| Variable | Default | Docker |
|---|---|---|
| `API_URL` | `http://localhost:8000` | `http://api:8000` |
| `KAFKA_BOOTSTRAP` | `localhost:9092` | `kafka:29092` |
| `REDIS_HOST`, `REDIS_PORT` | `localhost`, 6379 | `redis`, 6379 |

## Simulated events and the stream clock

The demo replays 2019 data, so a simulated event is stamped **one second after
the newest event the stream has processed** (`events_max_event_time` in
`rt:meta:stream`), not with the current date. An event stamped "now" would move
the watermark years ahead (every later replayed event would be dropped as late)
and would decay all of the user's real-time scores to zero.

The event carries the product's category, brand and price from `rt:prod:{product}`,
uses the session `demo-{user_id}` and is sent with the Kafka contract of the
replayer (`common.schemas.Event.to_json()`, key = `user_id`).

For the demo, keep the streaming container running. The replayer does not have
to run: the page works on whatever is already in Redis. If the replayer runs at
the same time, the stream clock moves on and the demo events follow it.

## Files

| File | Purpose |
|---|---|
| `app.py` | Entry point and page navigation |
| `helpers.py` | API, Redis and Kafka access; stream clock; simulated events |
| `views/recommend.py`, `views/explore.py`, `views/evaluation.py` | The three pages |
| `tests/test_helpers.py` | Recent activity decoding, event time, Kafka payload, wait for processing |
