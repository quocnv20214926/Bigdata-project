"""ALS hyper-parameter search on a validation week (Pair 1, week 4).

The test split (2019-11-16 .. 11-30) is never read while parameters are chosen: every trial
is fitted on 2019-10-01 .. 11-08 and scored on the validation week 11-09 .. 11-15
(batch/split.py), with the protocol of evaluation/offline.py: interaction matrix and filter
rebuilt on the fit period, warm users with validation events, the same user sample for every
trial, ground truth = every product a user touched in the validation week.

Search (--search):
  coordinate (default)  one parameter at a time, in the order alpha, regParam, rank; each stage
                        keeps the best value found so far for the other two. With the default
                        grid that is 9 trials instead of 45.
  grid                  every combination of --alphas x --regs x --ranks.

Each trial is written to the report as soon as it ends, so a run that stops early keeps its
results. With --final-report the best parameters are then trained on the full train split and
evaluated on the test split next to the Popularity baseline (evaluation.offline.run), and
--model-out saves that model (used by sim:item).

Usage (from the repository root, MinIO running):
  SPARK_DRIVER_MEMORY=14g python -m evaluation.tune_als --report docs/eval-week04-tuning.json \\
      --final-report docs/eval-week04-als.json --model-out s3a://lake/models/als/week4/
  python -m evaluation.tune_als --search grid --alphas 5 10 --regs 0.1 --ranks 32 64
"""

from __future__ import annotations

import argparse
import itertools
import json
import shutil
import tempfile
import time
from dataclasses import replace
from pathlib import Path
from typing import Callable, Mapping, Sequence

from pyspark.sql import DataFrame

from batch import als
from batch.interactions import MIN_PRODUCTS_PER_USER, MIN_USERS_PER_PRODUCT
from batch.spark_session import get_spark
from batch.split import (
    FIT_END, TRAIN_START, VALID_END, VALID_START,
    load_split, load_validation_split,
)
from common.config import settings
from evaluation import offline

DEFAULT_ALPHAS = (1.0, 5.0, 10.0, 20.0, 40.0)
DEFAULT_REGS = (0.01, 0.1, 1.0)
DEFAULT_RANKS = (32, 64, 128)
DEFAULT_METRIC = "ndcg@10"

Evaluate = Callable[[als.ALSParams], dict]


def coordinate_search(evaluate: Evaluate, start: als.ALSParams, grid: Mapping[str, Sequence],
                      metric: str = DEFAULT_METRIC) -> tuple[als.ALSParams, dict]:
    """Tune the parameters of `grid` one at a time, in its order.

    evaluate(params) returns the metrics of one trial; each distinct set of parameters is
    evaluated once. Ties keep the value listed first. Returns (best params, {params: metrics}).
    """
    trials: dict[als.ALSParams, dict] = {}

    def value(p: als.ALSParams) -> float:
        if p not in trials:
            trials[p] = evaluate(p)
        return trials[p][metric]

    best = start
    for name, values in grid.items():
        best = max((replace(best, **{name: v}) for v in values), key=value)
    return best, trials


def grid_search(evaluate: Evaluate, start: als.ALSParams, grid: Mapping[str, Sequence],
                metric: str = DEFAULT_METRIC) -> tuple[als.ALSParams, dict]:
    """Every combination of the values in `grid`; same return value as coordinate_search."""
    trials: dict[als.ALSParams, dict] = {}
    for combo in itertools.product(*grid.values()):
        p = replace(start, **dict(zip(grid, combo)))
        if p not in trials:
            trials[p] = evaluate(p)
    best = max(trials, key=lambda p: trials[p][metric])
    return best, trials


SEARCHES = {"coordinate": coordinate_search, "grid": grid_search}


def spark_evaluator(prepared: offline.Prepared, ks, on_trial: Callable[[dict], None] = lambda t: None) -> Evaluate:
    """Train ALS on the prepared matrix and score it; `on_trial` receives each finished trial."""
    spark = prepared.matrix.sparkSession
    k_max, catalog = max(ks), prepared.data["als_matrix"]["products"]

    def evaluate(params: als.ALSParams) -> dict:
        t = time.time()
        # A fresh checkpoint dir per trial, removed afterwards: rank 128 checkpoints are large.
        checkpoint_dir = tempfile.mkdtemp(prefix="als-tune-")
        spark.sparkContext.setCheckpointDir(checkpoint_dir)
        try:
            model = als.train(prepared.matrix, params)
            train_s = time.time() - t
            recs = als.recommend(model, prepared.users, k_max).cache()
            recs.count()
            metrics = offline.score(recs, prepared.truth, ks, catalog)
            recs.unpersist()
        finally:
            als.release(spark)
            shutil.rmtree(checkpoint_dir, ignore_errors=True)
        on_trial({"params": params.as_dict(), "metrics": metrics,
                  "seconds": {"train": round(train_s, 1), "total": round(time.time() - t, 1)}})
        return metrics

    return evaluate


def tune(fit: DataFrame, valid: DataFrame, grid: Mapping[str, Sequence], start: als.ALSParams = als.ALSParams(),
         search: str = "coordinate", eval_users: int = offline.DEFAULT_EVAL_USERS,
         truth_events=offline.TRUTH_EVENTS, ks=offline.DEFAULT_KS, metric: str = DEFAULT_METRIC,
         on_progress: Callable[[dict], None] = lambda r: None) -> dict:
    """Search `grid` on (fit, valid); returns the report. on_progress gets the report after each trial."""
    started = time.time()
    prepared = offline.prepare(fit, valid, eval_users, truth_events, start.seed)
    report = {
        "protocol": {
            "fit": [str(TRAIN_START), str(FIT_END)],
            "validation": [str(VALID_START), str(VALID_END)],
            "truth_events": list(truth_events),
            "eval_users_requested": eval_users,
            "filter": {"min_users_per_product": MIN_USERS_PER_PRODUCT,
                       "min_products_per_user": MIN_PRODUCTS_PER_USER},
            "ks": list(ks),
            "metric": metric,
            "search": search,
        },
        "grid": {name: list(values) for name, values in grid.items()},
        "start": start.as_dict(),
        "data": prepared.data,
        "trials": [],
        "seconds": {"prepare": round(time.time() - started, 1)},
    }

    def on_trial(trial: dict) -> None:
        report["trials"].append(trial)
        on_progress(report)

    best, trials = SEARCHES[search](spark_evaluator(prepared, ks, on_trial), start, grid, metric)
    report["best"] = {"params": best.as_dict(), "metrics": trials[best]}
    report["seconds"]["total"] = round(time.time() - started, 1)
    for df in (prepared.matrix, prepared.users, prepared.truth):
        df.unpersist()
    return report


def format_trials(report: dict) -> str:
    metric = report["protocol"]["metric"]
    present = report["trials"][0]["metrics"] if report["trials"] else {}
    names = [metric] + [m for m in ("recall@20", "hit_rate@10", "coverage@10") if m != metric and m in present]
    lines = ["| rank | regParam | alpha | " + " | ".join(names) + " | train s |",
             "|---" * (len(names) + 4) + "|"]
    for t in report["trials"]:
        p, m = t["params"], t["metrics"]
        lines.append(f"| {p['rank']} | {p['reg_param']} | {p['alpha']} | "
                     + " | ".join(f"{m[n]:.4f}" for n in names) + f" | {t['seconds']['train']} |")
    return "\n".join(lines)


def write_json(path: str | None, data: dict) -> None:
    if path:
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        Path(path).write_text(json.dumps(data, indent=2) + "\n", encoding="utf-8")


def main() -> None:
    defaults = als.ALSParams()
    p = argparse.ArgumentParser(description="ALS grid search on the validation week cut from the end of train")
    p.add_argument("--input", default=settings.clean_path, help="clean Parquet from batch/etl.py")
    p.add_argument("--no-s3", action="store_true", help="local paths only, do not load the s3a jars")
    p.add_argument("--report", help="search report (JSON), rewritten after every trial")
    p.add_argument("--search", choices=sorted(SEARCHES), default="coordinate")
    p.add_argument("--alphas", type=float, nargs="+", default=list(DEFAULT_ALPHAS))
    p.add_argument("--regs", type=float, nargs="+", default=list(DEFAULT_REGS))
    p.add_argument("--ranks", type=int, nargs="+", default=list(DEFAULT_RANKS))
    p.add_argument("--start-rank", type=int,
                   help="coordinate search: rank used while alpha and regParam are tuned (default: first of --ranks)")
    p.add_argument("--start-reg", type=float, default=defaults.reg_param)
    p.add_argument("--start-alpha", type=float, default=defaults.alpha)
    p.add_argument("--iter", type=int, default=defaults.max_iter)
    p.add_argument("--seed", type=int, default=defaults.seed)
    p.add_argument("--checkpoint-interval", type=int, default=1,
                   help="save factors every N iterations; 1 keeps the shuffle disk lowest for rank 128")
    p.add_argument("--eval-users", type=int, default=offline.DEFAULT_EVAL_USERS, help="0 = every eligible user")
    p.add_argument("--truth-events", nargs="+", default=list(offline.TRUTH_EVENTS), choices=offline.TRUTH_EVENTS)
    p.add_argument("--metric", default=DEFAULT_METRIC, help="metric to maximise, e.g. ndcg@10 or recall@20")
    p.add_argument("--final-report", help="train the best parameters on the full train split, evaluate on test")
    p.add_argument("--model-out", help="with --final-report: save the final ALS model here")
    args = p.parse_args()

    spark = get_spark("batch-tune-als", s3=not args.no_s3)
    spark.sparkContext.setLogLevel("WARN")
    start = als.ALSParams(rank=args.start_rank or args.ranks[0], reg_param=args.start_reg, alpha=args.start_alpha,
                          max_iter=args.iter, seed=args.seed, checkpoint_interval=args.checkpoint_interval)
    grid = {"alpha": args.alphas, "reg_param": args.regs, "rank": args.ranks}

    def on_progress(report: dict) -> None:
        last = report["trials"][-1]
        print(f"trial {len(report['trials'])}: {last['params']} -> {args.metric} "
              f"{last['metrics'][args.metric]:.4f} ({last['seconds']['total']} s)", flush=True)
        write_json(args.report, report)

    fit, valid = load_validation_split(spark, args.input)
    report = tune(fit, valid, grid, start, args.search, args.eval_users, tuple(args.truth_events),
                  metric=args.metric, on_progress=on_progress)
    report["input"] = args.input
    write_json(args.report, report)
    print(format_trials(report))
    print("best:", json.dumps(report["best"]["params"]))

    if args.final_report:
        spark.catalog.clearCache()
        checkpoint_dir = tempfile.mkdtemp(prefix="als-checkpoint-")
        spark.sparkContext.setCheckpointDir(checkpoint_dir)
        best = als.ALSParams(**report["best"]["params"])
        train, test = load_split(spark, args.input)
        final = offline.run(train, test, best, args.eval_users, tuple(args.truth_events),
                            model_out=args.model_out, models=("popularity", "als"))
        final["input"] = args.input
        final["tuning"] = {"report": args.report, "validation_" + args.metric: report["best"]["metrics"][args.metric]}
        write_json(args.final_report, final)
        print(offline.format_table(final["results"]))
        shutil.rmtree(checkpoint_dir, ignore_errors=True)
    spark.stop()


if __name__ == "__main__":
    main()
