"""ALS parameter search: the search logic (plain Python) and one tiny run (local Spark).

Run from the repository root:  python -m pytest evaluation/tests -q
"""

from __future__ import annotations

from batch.als import ALSParams
from evaluation.tune_als import coordinate_search, format_trials, grid_search, tune

GRID = {"alpha": [1.0, 10.0, 40.0], "reg_param": [0.01, 0.1], "rank": [8, 16]}


def fake_metric(p: ALSParams) -> float:
    """Best at alpha 10, reg 0.01, rank 16; the parameters do not interact."""
    return -abs(p.alpha - 10) - 100 * p.reg_param + p.rank / 100


def recorder():
    calls = []

    def evaluate(p):
        calls.append(p)
        return {"ndcg@10": fake_metric(p)}

    return calls, evaluate


def test_coordinate_search_tunes_one_parameter_at_a_time():
    calls, evaluate = recorder()
    start = ALSParams(rank=8, reg_param=0.1, alpha=10.0)
    best, trials = coordinate_search(evaluate, start, GRID)
    assert (best.alpha, best.reg_param, best.rank) == (10.0, 0.01, 16)
    # alpha: 3 trials; reg: 0.1 was already run with alpha 10 -> 1 new; rank: 8 already run -> 1 new
    assert len(calls) == len(set(calls)) == 5
    assert set(trials) == set(calls)
    assert all(p.max_iter == start.max_iter and p.seed == start.seed for p in calls)


def test_coordinate_search_keeps_the_first_value_on_ties():
    best, _ = coordinate_search(lambda p: {"ndcg@10": 0.5}, ALSParams(rank=8), {"rank": [16, 8, 32]})
    assert best.rank == 16


def test_grid_search_runs_every_combination():
    calls, evaluate = recorder()
    best, trials = grid_search(evaluate, ALSParams(), GRID)
    assert len(calls) == len(trials) == 3 * 2 * 2
    assert (best.alpha, best.reg_param, best.rank) == (10.0, 0.01, 16)


def test_tune_on_tiny_data(spark):
    """Two communities; in the validation week each user views the item it skipped."""
    cols = ["user_id", "product_id", "category_id", "event_type"]
    fit_rows, valid_rows = [], []
    for u in range(1, 41):
        base = 100 if u <= 20 else 200
        skipped = base + u % 5
        fit_rows += [(u, base + i, base, "view") for i in range(5) if base + i != skipped]
        valid_rows.append((u, skipped, base, "view"))
    fit = spark.createDataFrame(fit_rows, cols)
    valid = spark.createDataFrame(valid_rows, cols)

    progress = []
    report = tune(fit, valid, {"alpha": [1.0, 10.0], "rank": [2, 4]}, ALSParams(rank=2, max_iter=5),
                  eval_users=0, ks=(1, 5), metric="ndcg@5",
                  on_progress=lambda r: progress.append(len(r["trials"])))

    assert progress == [1, 2, 3]                       # alpha 1, alpha 10, then the other rank
    assert report["data"]["ground_truth"] == {"users": 40, "pairs": 40}
    assert len(report["trials"]) == 3
    best = report["best"]
    assert best["params"] in [t["params"] for t in report["trials"]]
    assert best["metrics"]["ndcg@5"] == max(t["metrics"]["ndcg@5"] for t in report["trials"])
    assert report["protocol"]["validation"] == ["2019-11-09", "2019-11-15"]
    assert format_trials(report).count("\n") == 1 + 3          # header, rule, 3 trials
    # factors of finished trials are released
    names = [r.name() for r in spark.sparkContext._jsc.getPersistentRDDs().values()]
    assert "userFactors" not in names and "itemFactors" not in names
