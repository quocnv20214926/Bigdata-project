"""Check a raw REES46 CSV before replaying it (week 1 task).

Streams the file once (constant memory) and reports:
  - whether the header matches common.schemas.RAW_COLUMNS
  - row count, event type counts, empty values per column
  - first/last event_time, events per day
  - whether rows are sorted by event_time (the replayer assumes they are)
Optionally writes the first N rows to a small sample CSV for development.

Usage (from the repository root):
  python -m ingestion.inspect_raw data/raw/2019-Oct.csv
  python -m ingestion.inspect_raw data/raw/2019-Oct.csv --limit 2000000 --sample-out data/sample/oct_sample.csv --sample-rows 200000
"""

from __future__ import annotations

import argparse
import csv
import gzip
import io
import json
import sys
import time
from collections import Counter
from pathlib import Path

from common.schemas import EVENT_TYPES, RAW_COLUMNS, Event


def open_text(path: str | Path) -> io.TextIOBase:
    path = Path(path)
    if path.suffix == ".gz":
        return io.TextIOWrapper(gzip.open(path, "rb"), encoding="utf-8", newline="")
    return open(path, "r", encoding="utf-8", newline="")


def inspect(path: str, limit: int | None = None, sample_out: str | None = None,
            sample_rows: int = 100_000, progress_every: int = 5_000_000) -> dict:
    started = time.time()
    rows = 0
    bad_rows = 0
    event_types: Counter[str] = Counter()
    empty: Counter[str] = Counter()
    per_day: Counter[str] = Counter()
    first_time = last_time = None
    max_time = None
    out_of_order = 0
    max_lag_seconds = 0.0

    sample_writer = None
    sample_file = None
    if sample_out:
        Path(sample_out).parent.mkdir(parents=True, exist_ok=True)
        sample_file = open(sample_out, "w", encoding="utf-8", newline="")

    with open_text(path) as f:
        reader = csv.DictReader(f)
        header = tuple(reader.fieldnames or ())
        if sample_file:
            sample_writer = csv.DictWriter(sample_file, fieldnames=list(header))
            sample_writer.writeheader()

        for row in reader:
            rows += 1
            for col in RAW_COLUMNS:
                if row.get(col) in ("", None):
                    empty[col] += 1
            event_types[row.get("event_type", "")] += 1
            try:
                t = Event.parse_time(row["event_time"])
            except (KeyError, ValueError):
                bad_rows += 1
                continue

            per_day[t.strftime("%Y-%m-%d")] += 1
            if first_time is None:
                first_time = t
            last_time = t
            if max_time is not None and t < max_time:
                out_of_order += 1
                max_lag_seconds = max(max_lag_seconds, (max_time - t).total_seconds())
            if max_time is None or t > max_time:
                max_time = t

            if sample_writer and rows <= sample_rows:
                sample_writer.writerow(row)

            if progress_every and rows % progress_every == 0:
                print(f"  ... {rows:,} rows ({time.time() - started:.0f}s)", file=sys.stderr)
            if limit and rows >= limit:
                break

    if sample_file:
        sample_file.close()

    return {
        "file": str(path),
        "header_matches_contract": header == RAW_COLUMNS,
        "header": list(header),
        "rows_read": rows,
        "unparseable_time_rows": bad_rows,
        "event_types": dict(event_types),
        "unknown_event_types": sorted(set(event_types) - set(EVENT_TYPES)),
        "empty_values": {c: empty[c] for c in RAW_COLUMNS},
        "first_event_time": first_time.isoformat() if first_time else None,
        "last_event_time": last_time.isoformat() if last_time else None,
        "out_of_order_rows": out_of_order,
        "max_out_of_order_seconds": max_lag_seconds,
        "events_per_day": dict(sorted(per_day.items())),
        "elapsed_seconds": round(time.time() - started, 1),
    }


def main(argv: list[str] | None = None) -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("file", help="raw REES46 CSV (.csv or .csv.gz)")
    p.add_argument("--limit", type=int, default=None, help="stop after N rows")
    p.add_argument("--sample-out", default=None, help="write the first rows to this CSV")
    p.add_argument("--sample-rows", type=int, default=100_000)
    p.add_argument("--json-out", default=None, help="also save the report as JSON")
    args = p.parse_args(argv)

    report = inspect(args.file, args.limit, args.sample_out, args.sample_rows)
    text = json.dumps(report, indent=2)
    print(text)
    if args.json_out:
        Path(args.json_out).parent.mkdir(parents=True, exist_ok=True)
        Path(args.json_out).write_text(text, encoding="utf-8")


if __name__ == "__main__":
    main()
