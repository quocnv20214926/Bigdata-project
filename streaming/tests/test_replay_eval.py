"""Speed-only replay evaluation on a tiny synthetic log (fakeredis, no Spark)."""

from __future__ import annotations

from types import SimpleNamespace

import fakeredis

from streaming.evaluation import replay_eval as ev

HEADER = "event_time,event_type,product_id,category_id,category_code,brand,price,user_id,user_session\n"


def line(t: str, etype: str, pid: int, uid: int, session: str, cat: int = 7) -> str:
    return f"2019-10-01 {t} UTC,{etype},{pid},{cat},electronics.smartphone,acme,10.0,{uid},{session}\n"


def args(path, **kw):
    base = dict(file=[str(path)], warmup_until="2019-10-01T01:00:00Z", end=None, batch_seconds=30,
                half_life=1800.0, neighbours=20, sample=1.0, k=[1, 10], redis_host="", redis_port=0,
                flush=True, json=None)
    base.update(kw)
    return SimpleNamespace(**base)


def test_tally_hits_and_ndcg():
    t = ev.Tally()
    t.add([5, 6, 7], 6, [1, 10])
    t.add([], 6, [1, 10])
    row = t.row([1, 10])
    assert row["targets"] == 2 and row["coverage"] == 0.5
    assert row["hr@1"] == 0.0 and row["hr@10"] == 0.5
    assert abs(row["ndcg@10"] - (1 / 1.5849625) / 2) < 1e-4


def test_trend_windows_assign_each_event_to_12_windows():
    tw = ev.TrendWindows()
    rows = tw.add_batch([SimpleNamespace(event_type="cart", epoch=1000, category_id=7, product_id=1)])
    assert len(rows) == 12 and {r.score for r in rows} == {3.0}
    assert min(r.window_end for r in rows) == 1200 and max(r.window_end for r in rows) == 1200 + 11 * 300


def test_replay_learns_session_cooccurrence(tmp_path):
    lines = []
    # Warm-up: 30 sessions view 100 then 200 (and 300 then 400).
    for n in range(30):
        m = n * 2
        lines.append(line(f"00:{m:02d}:00", "view", 100, 1000 + n, f"s{n}"))
        lines.append(line(f"00:{m:02d}:10", "view", 200, 1000 + n, f"s{n}"))
        lines.append(line(f"00:{m:02d}:20", "view", 300, 2000 + n, f"t{n}"))
        lines.append(line(f"00:{m:02d}:30", "view", 400, 2000 + n, f"t{n}"))
    # Evaluated: a new user views 100, then 200 two minutes later (the target).
    lines.append(line("01:10:00", "view", 100, 9, "u9"))
    lines.append(line("01:12:00", "view", 200, 9, "u9"))
    path = tmp_path / "log.csv"
    path.write_text(HEADER + "".join(sorted(lines)))

    server = fakeredis.FakeServer()
    r = fakeredis.FakeRedis(server=server, decode_responses=True)
    ar = fakeredis.FakeAsyncRedis(server=server, decode_responses=True)
    out = ev.run(args(path), r=r, ar=ar)

    res = out["results"]["all"]
    assert res["cooc_last"]["targets"] == 1
    assert res["cooc_last"]["hr@1"] == 1.0          # 200 is the top co-view of 100
    assert res["rec_rt"]["hr@1"] == 1.0
    assert res["api"]["hr@10"] == 1.0
    assert out["stats"]["targets"] == 1
