# ingestion/: Event Replayer (Pair 2)

Replays the raw REES46 CSV into Kafka topic `events`, in `event_time` order,
following the Kafka contract in `common/schemas.py` (key = `user_id`,
value = `Event.to_json()`).

| File | Purpose |
|---|---|
| `inspect_raw.py` | One pass over a raw CSV: header check, event types, empty values, time range, sort order; can write a small sample |
| `replayer.py` | CSV -> Kafka with time-based pacing (`--speed`) and a rate cap (`--max-rate`) |
| `tests/` | Unit tests (no Kafka needed) |

## Run

All commands run from the repository root.

```bash
pip install -r ingestion/requirements.txt

# 1. Check the raw file and write a 200k-row sample for development
python -m ingestion.inspect_raw data/raw/2019-Oct.csv --limit 2000000 \
    --sample-out data/sample/oct_sample.csv --sample-rows 200000

# 2. Look at the messages without Kafka
python -m ingestion.replayer --file data/sample/oct_sample.csv --dry-run --limit 5

# 3. Send to Kafka (stack from infra/ must be running)
python -m ingestion.replayer --file data/sample/oct_sample.csv --speed 0          # as fast as possible
python -m ingestion.replayer --file data/raw/2019-Oct.csv --speed 600 --max-rate 5000
```

## Options

| Option | Default | Meaning |
|---|---|---|
| `--speed` | 60 | Replay speed-up; `0` = no pacing |
| `--max-rate` | 0 | Max events per second; `0` = unlimited |
| `--limit` | none | Stop after N events |
| `--start-time` / `--end-time` | none | Replay only a time range (ISO 8601) |
| `--event-types` | all | e.g. `view,cart` |
| `--dry-run` | off | Print JSON instead of sending |

The replayer assumes the CSV is sorted by `event_time`; `inspect_raw.py`
reports `out_of_order_rows` to confirm it.
