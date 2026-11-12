"""End-to-end test of both streaming queries (STREAM_SOURCE=file, AVAILABLE_NOW=1)
against a throw-away redis-server.

Needs pyspark + Java 17+ and the redis-server binary; skipped otherwise
(e.g. on Windows: run it in WSL or in the streaming container).
"""

from __future__ import annotations

import json
import os
import shutil
import socket
import subprocess
import sys
import time
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
# Spark's Python workers are separate processes: they need the repo on PYTHONPATH
# and the Redis port before Spark starts.
os.environ["PYTHONPATH"] = os.pathsep.join(filter(None, [str(ROOT), os.environ.get("PYTHONPATH")]))
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


PORT = _free_port()
os.environ["REDIS_PORT"] = str(PORT)

pytest.importorskip("pyspark")
if not shutil.which("redis-server"):
    pytest.skip("redis-server not installed", allow_module_level=True)

import redis  # noqa: E402
from pyspark.sql import SparkSession  # noqa: E402

from common import redis_keys as rk  # noqa: E402


def ev(t: str, etype: str, pid: int, uid: int, session: str, cat: int = 10) -> str:
    return json.dumps({"event_time": t, "event_type": etype, "product_id": pid, "category_id": cat,
                       "category_code": None, "brand": None, "price": 1.0, "user_id": uid,
                       "user_session": session})


@pytest.fixture(scope="module")
def r():
    proc = subprocess.Popen(["redis-server", "--port", str(PORT), "--save", "", "--appendonly", "no"],
                            stdout=subprocess.DEVNULL)
    client = redis.Redis(port=PORT, decode_responses=True)
    for _ in range(50):
        try:
            client.ping()
            break
        except (redis.ConnectionError, redis.TimeoutError):
            time.sleep(0.1)
    yield client
    proc.terminate()
    proc.wait()


@pytest.fixture(scope="module")
def spark(r):  # Redis first, so that Spark cannot take its port
    s = (SparkSession.builder.master("local[2]").appName("pair2-tests")
         .config("spark.sql.session.timeZone", "UTC").config("spark.sql.shuffle.partitions", "2")
         .config("spark.ui.enabled", "false").getOrCreate())
    s.sparkContext.setLogLevel("ERROR")
    yield s
    s.stop()


def run(spark, inp: Path, ckpt: Path):
    from streaming.main import Config, build_queries

    cfg = Config(source="file", input_dir=str(inp), checkpoint=str(ckpt), available_now=True,
                 shuffle_partitions=2, trigger_seconds=1)
    for q in build_queries(spark, cfg):
        q.awaitTermination(180)
        assert q.exception() is None, q.exception()


def test_parse_rejects_bad_rows_without_failing(spark):
    from streaming.parsing import parse_events, valid_events

    raw = spark.createDataFrame([
        (ev("2019-10-01T00:00:05Z", "view", 10, 1, "s"),),
        ("not json",),
        ('{"event_type": "view"}',),
        (ev("2019-10-01T00:00:06Z", "click", 10, 1, "s"),),       # unknown event type
        (ev("2019-13-45 99:00", "view", 10, 1, "s"),),            # malformed time (ANSI mode)
    ], ["value"])
    rows = valid_events(parse_events(raw)).collect()
    assert [(x.epoch, x.user_id) for x in rows] == [(1569888005, 1)]


def test_pipeline_end_to_end(spark, r, tmp_path):
    inp, ckpt = tmp_path / "in", tmp_path / "ckpt"
    inp.mkdir()
    lines = [
        # user 1, session a: 100 -> 101 -> 102, then buys 101
        ev("2019-10-01T10:00:00Z", "view", 100, 1, "a"),
        ev("2019-10-01T10:01:00Z", "view", 101, 1, "a"),
        ev("2019-10-01T10:01:00Z", "view", 101, 1, "a"),          # exact duplicate -> dropped
        ev("2019-10-01T10:02:00Z", "view", 102, 1, "a"),
        ev("2019-10-01T10:03:00Z", "purchase", 101, 1, "a"),
        # user 2, session b: 100 -> 101 (another co-view of 100 and 101)
        ev("2019-10-01T10:04:00Z", "view", 100, 2, "b"),
        ev("2019-10-01T10:05:00Z", "cart", 101, 2, "b"),
        # another category
        ev("2019-10-01T10:06:00Z", "view", 200, 3, "c", cat=20),
        "garbage line",
    ]
    (inp / "part-0.json").write_text("\n".join(lines) + "\n")
    run(spark, inp, ckpt)

    # week 3: user state (ZSETs, contract 1.1.0)
    recent = [rk.decode_recent(m) for m in r.zrevrange(rk.user_recent(1), 0, -1)]
    assert [(p, t) for p, t, _ in recent] == [(101, "purchase"), (102, "view"), (101, "view"), (100, "view")]
    assert r.zscore(rk.user_bought(1), "101") is not None
    meta = r.hgetall("rt:meta:stream")
    assert int(meta["events_total_rows"]) == 7                    # 9 lines - 1 duplicate - 1 garbage

    # week 4: co-occurrence. 100 and 101 are co-viewed in sessions a and b.
    s100 = {int(j): s for j, s in r.zrange(rk.sim_cooc(100), 0, -1, withscores=True)}
    assert set(s100) == {101, 102}
    assert s100[101] > s100[102]
    assert not r.exists(rk.sim_cooc(200))                         # alone in its session

    # week 4: trending in the newest 1 h window (weights: view 1, cart 3, purchase 5).
    # The duplicate view is counted here (no dedup before the window).
    t10 = dict(r.zrevrange(rk.trend_cat(10), 0, -1, withscores=True))
    assert t10 == {"101": 1 + 1 + 5 + 3, "100": 2.0, "102": 1.0}
    assert r.zrevrange(rk.TREND_GLOBAL, 0, 0) == ["101"]
    assert r.zrange(rk.trend_cat(20), 0, -1) == ["200"]
    assert 0 < r.ttl(rk.trend_cat(10)) <= rk.TTL["trend"]

    # Restart with the same checkpoints and one new file: only the new event is processed.
    (inp / "part-1.json").write_text(ev("2019-10-01T10:07:00Z", "view", 103, 1, "a") + "\n")
    run(spark, inp, ckpt)
    assert int(r.hget("rt:meta:stream", "events_total_rows")) == 8
    assert r.zscore(rk.trend_cat(10), "101") == 10.0              # unchanged, not double counted
    assert rk.decode_recent(r.zrevrange(rk.user_recent(1), 0, 0)[0])[0] == 103
    assert r.hget("rt:cooc:101", "100") == "2"                    # not recounted on restart
