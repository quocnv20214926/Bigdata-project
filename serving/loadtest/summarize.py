"""Turn Locust CSV output into the latency table of the weekly report.

    python -m serving.loadtest.summarize docs/loadtest/run [--label "100 clients"] [--json out.json]

Reads {prefix}_stats.csv (written by `locust --csv {prefix}`) and prints, per
endpoint and in total: requests, failures, requests/s, p50 / p95 / p99 / max (ms).
"""

from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path


def read_stats(prefix: str) -> list[dict]:
    path = Path(f"{prefix}_stats.csv")
    with path.open(newline="") as f:
        rows = list(csv.DictReader(f))
    out = []
    for row in rows:
        if not row.get("Name"):
            continue
        out.append({
            "endpoint": "total" if row["Name"] == "Aggregated" else row["Name"],
            "requests": int(row["Request Count"]),
            "failures": int(row["Failure Count"]),
            "rps": round(float(row["Requests/s"]), 1),
            "p50_ms": float(row["50%"]),
            "p95_ms": float(row["95%"]),
            "p99_ms": float(row["99%"]),
            "max_ms": round(float(row["Max Response Time"]), 1),
        })
    return out


def markdown(rows: list[dict], label: str = "") -> str:
    head = "| Endpoint | Requests | Failures | Requests/s | p50 (ms) | p95 (ms) | p99 (ms) | max (ms) |"
    lines = [f"**{label}**", ""] if label else []
    lines += [head, "|---|---:|---:|---:|---:|---:|---:|---:|"]
    for r in rows:
        name = f"**{r['endpoint']}**" if r["endpoint"] == "total" else f"`{r['endpoint']}`"
        lines.append(f"| {name} | {r['requests']:,} | {r['failures']:,} | {r['rps']:,} | "
                     f"{r['p50_ms']:g} | {r['p95_ms']:g} | {r['p99_ms']:g} | {r['max_ms']:g} |")
    return "\n".join(lines)


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("prefix", help="the --csv prefix given to locust")
    p.add_argument("--label", default="")
    p.add_argument("--json", default=None, help="also write the rows as JSON")
    a = p.parse_args()
    rows = read_stats(a.prefix)
    print(markdown(rows, a.label))
    if a.json:
        Path(a.json).write_text(json.dumps({"label": a.label, "rows": rows}, indent=2) + "\n")


if __name__ == "__main__":
    main()
