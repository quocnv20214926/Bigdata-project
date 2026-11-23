"""Build a leakage-free sim:item for the replay evaluation (week 8, owner D).

Pair 1's nightly sim:item is trained on Oct 1 - Nov 15, so it cannot be used to
evaluate a replay of October: it already knows the future. This script runs
Pair 1's own Item-kNN (batch.interactions.interaction_scores +
batch.item_knn.similarities, same parameters) on the events BEFORE a cut-off
time only, and writes the table as CSV for replay_eval.py --sim-item.

    python -m streaming.evaluation.build_sim_item --file data/raw/2019-Oct.csv \
        --until 2019-10-02T00:00:00Z --out data/eval/sim_item_oct01.csv
"""

from __future__ import annotations

import argparse
import csv
import logging
import os
import sys
import tempfile
import time
from datetime import datetime, timezone

from batch.interactions import interaction_scores
from batch.item_knn import KNNParams, similarities
from ingestion.replayer import read_events

log = logging.getLogger("build_sim_item")


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--file", action="append", required=True)
    p.add_argument("--until", required=True, help="only events before this time (ISO 8601)")
    p.add_argument("--out", required=True)
    p.add_argument("--driver-memory", default="3g")
    a = p.parse_args()
    logging.basicConfig(level="INFO", format="%(asctime)s %(levelname)s %(message)s")
    until = datetime.fromisoformat(a.until.replace("Z", "+00:00")).astimezone(timezone.utc)

    # 1. The training events as a small CSV Spark can read (user, product, event type).
    started = time.time()
    tmp = tempfile.NamedTemporaryFile("w", suffix=".csv", delete=False, newline="")
    w = csv.writer(tmp)
    w.writerow(["user_id", "product_id", "event_type"])
    n = 0
    for ev in read_events(a.file, end_time=until):
        w.writerow([ev.user_id, ev.product_id, ev.event_type])
        n += 1
    tmp.close()
    log.info("%s training events before %s", f"{n:,}", a.until)

    # 2. Pair 1's Item-kNN.
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    from pyspark.sql import SparkSession
    spark = (SparkSession.builder.master("local[*]").appName("build-sim-item")
             .config("spark.driver.memory", a.driver_memory)
             .config("spark.sql.shuffle.partitions", "8")
             .config("spark.sql.session.timeZone", "UTC").getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    events = spark.read.csv(tmp.name, header=True, schema="user_id long, product_id long, event_type string")
    sims = similarities(interaction_scores(events), KNNParams()).toPandas()
    spark.stop()
    os.unlink(tmp.name)

    os.makedirs(os.path.dirname(a.out) or ".", exist_ok=True)
    sims.to_csv(a.out, index=False, columns=["product_id", "neighbour_id", "sim"])
    log.info("%s pairs for %s products -> %s (%.0f s)", f"{len(sims):,}",
             f"{sims['product_id'].nunique():,}", a.out, time.time() - started)


if __name__ == "__main__":
    main()
