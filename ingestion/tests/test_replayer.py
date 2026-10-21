"""Unit tests for the replayer and the raw-file inspector (no Kafka needed).

Run from the repository root:  python -m pytest ingestion/tests -q
"""

from __future__ import annotations

import json

from common.schemas import RAW_COLUMNS, Event
from ingestion.inspect_raw import inspect
from ingestion.replayer import Pacer, read_events, replay

ROWS = [
    # event_time, event_type, product_id, category_id, category_code, brand, price, user_id, user_session
    ("2019-10-01 00:00:00 UTC", "view", "44600062", "2103807459595387724", "", "shiseido", "35.79", "541312140", "s1"),
    ("2019-10-01 00:00:10 UTC", "cart", "3900821", "2053013552326770905", "appliances.environment.water_heater", "aqua", "33.20", "554748717", "s2"),
    ("2019-10-01 00:00:20 UTC", "view", "bad_id", "1", "", "", "1.0", "1", "s3"),  # malformed
    ("2019-10-01 00:00:30 UTC", "purchase", "17200506", "2053013559792632471", "furniture.living_room.sofa", "", "543.10", "519107250", "s4"),
    ("2019-10-01 00:00:25 UTC", "view", "1307067", "2053013558920217191", "computers.notebook", "lenovo", "251.74", "550050854", "s5"),  # out of order
]


def write_csv(path, rows=ROWS):
    lines = [",".join(RAW_COLUMNS)] + [",".join(r) for r in rows]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return path


def test_read_events_skips_malformed(tmp_path):
    f = write_csv(tmp_path / "s.csv")
    stats: dict = {}
    events = list(read_events([str(f)], stats=stats))
    assert [e.product_id for e in events] == [44600062, 3900821, 17200506, 1307067]
    assert stats["skipped"] == 1
    assert events[0].category_code is None and events[0].brand == "shiseido"


def test_read_events_filters(tmp_path):
    f = write_csv(tmp_path / "s.csv")
    only_views = list(read_events([str(f)], event_types={"view"}))
    assert {e.event_type for e in only_views} == {"view"}
    later = list(read_events([str(f)], start_time=Event.parse_time("2019-10-01T00:00:15Z")))
    assert [e.event_type for e in later] == ["purchase", "view"]


def test_message_matches_kafka_contract(tmp_path):
    f = write_csv(tmp_path / "s.csv")
    ev = next(read_events([str(f)]))
    msg = json.loads(ev.to_json())
    assert msg["event_time"] == "2019-10-01T00:00:00Z"
    assert set(msg) == set(RAW_COLUMNS)
    assert ev.kafka_key() == b"541312140"
    assert Event.from_json(ev.to_json()) == ev


class FakeClock:
    def __init__(self):
        self.t = 1000.0
        self.slept = []

    def clock(self):
        return self.t

    def sleep(self, s):
        self.slept.append(round(s, 6))
        self.t += s


def test_pacer_speed_keeps_gaps():
    c = FakeClock()
    p = Pacer(speed=10, clock=c.clock, sleep=c.sleep)
    for epoch in (0, 10, 30):          # gaps of 10s and 20s in event time
        p.wait(epoch)
    assert c.slept == [1.0, 2.0]       # 10x faster


def test_pacer_no_pacing_and_rate_cap():
    c = FakeClock()
    Pacer(speed=0, clock=c.clock, sleep=c.sleep).wait(0)
    assert c.slept == []
    p = Pacer(speed=0, max_rate=4, clock=c.clock, sleep=c.sleep)
    for epoch in (0, 0, 0):
        p.wait(epoch)
    assert c.slept == [0.25, 0.25]


def test_replay_limit(tmp_path):
    f = write_csv(tmp_path / "s.csv")
    sent = []
    n = replay(read_events([str(f)]), sent.append, Pacer(0), limit=2, progress_every=0)
    assert n == 2 and len(sent) == 2


def test_inspect_report(tmp_path):
    f = write_csv(tmp_path / "s.csv")
    sample = tmp_path / "sample.csv"
    r = inspect(str(f), sample_out=str(sample), sample_rows=2, progress_every=0)
    assert r["header_matches_contract"] is True
    assert r["rows_read"] == 5
    assert r["event_types"] == {"view": 3, "cart": 1, "purchase": 1}
    assert r["out_of_order_rows"] == 1 and r["max_out_of_order_seconds"] == 5.0
    assert r["empty_values"]["category_code"] == 2
    assert len(sample.read_text().strip().splitlines()) == 3   # header + 2 rows
