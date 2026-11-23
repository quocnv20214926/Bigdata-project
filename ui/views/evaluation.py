"""Page 3: evaluation results (week 8).

Reads the result files committed in docs/ (nothing is computed here):

  docs/eval-pair2-week07.json        speed-only replay: next-item HR / NDCG (Pair 2)
  docs/eval-pair2-week08-*.json      the same with rec:rt on sim:cooc / sim:item / hybrid (Pair 2)
  docs/eval-week07.json              batch: ALS vs Popularity vs Item-kNN, by segment (Pair 1)
  docs/loadtest/*.json               API load test (Pair 2)
"""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import streamlit as st

DOCS = Path(__file__).resolve().parents[2] / "docs"

METHOD_LABELS = {          # short: they label the bars
    "trending": "Trending (global)",
    "trending_cat": "Trending (category)",
    "cooc_last": "sim:cooc (last product)",
    "rec_rt": "rec:rt",
    "api": "/recommend",
}
PERCENT = st.column_config.NumberColumn(format="percent")


def load(name: str):
    path = DOCS / name
    if not path.exists():
        return None
    return json.loads(path.read_text())


def speed_table(result: dict, group: str, k: int) -> pd.DataFrame:
    rows = []
    for method, r in result["results"].get(group, {}).items():
        rows.append({"method": METHOD_LABELS.get(method, method), f"HR@{k}": r[f"hr@{k}"],
                     f"NDCG@{k}": r[f"ndcg@{k}"], "coverage": r["coverage"], "targets": r["targets"]})
    return pd.DataFrame(rows)


st.title("Evaluation")
tab_speed, tab_sim, tab_batch, tab_load = st.tabs(
    ["Speed layer (next item)", "Similarity ablation", "Batch layer (Pair 1)", "Load test"])

with tab_speed:
    res = load("eval-pair2-week07.json")
    if res is None:
        st.info("docs/eval-pair2-week07.json not found (streaming/evaluation/replay_eval.py).")
    else:
        cfg, stats = res["config"], res["stats"]
        until = cfg["end"] or "the end of the file"
        st.caption(f"Replay of {stats['events']:,} events, evaluated from {cfg['warmup_until']} until {until}; "
                   f"micro-batches of {cfg['batch_seconds']} s of event time; rec:rt half-life "
                   f"{cfg['half_life_seconds'] / 60:.0f} min, {cfg['neighbours']} neighbours per event; "
                   f"{stats['targets']:,} target events ({cfg['sample']:.0%} of users). A target is a user's next "
                   "product, different from the previous one; the lists are those of just before the event.")
        c1, c2 = st.columns(2)
        group = c1.selectbox("Targets", [g for g in ("all", "view", "cart", "purchase", "unseen")
                                         if g in res["results"]])
        k = c2.selectbox("K", cfg["k"], index=0)
        df = speed_table(res, group, k)
        st.bar_chart(df, x="method", y=f"HR@{k}", horizontal=True, sort=f"-HR@{k}")
        st.caption("Methods: trending of the last hour in the whole shop or in the category of the user's last "
                   "product; sim:cooc of the last product only; rec:rt alone; /recommend = what the API returns "
                   "(rec:rt, purchases removed, filled from trending).")
        st.dataframe(df, hide_index=True, width="stretch", column_config={"coverage": PERCENT})

    sweep = [load(f"eval-pair2-week07-halflife-{m}.json") for m in (5, 15, 30, 120)]
    sweep = [s for s in sweep if s]
    if sweep:
        st.subheader("Half-life of rec:rt")
        rows = [{"half-life (min)": s["config"]["half_life_seconds"] / 60,
                 "rec:rt HR@10": s["results"]["all"]["rec_rt"]["hr@10"],
                 "/recommend HR@10": s["results"]["all"]["api"]["hr@10"],
                 "sim:cooc of last product HR@10": s["results"]["all"]["cooc_last"]["hr@10"]} for s in sweep]
        df = pd.DataFrame(rows).sort_values("half-life (min)")
        st.line_chart(df.set_index("half-life (min)"))
        st.dataframe(df, hide_index=True, width="stretch")

with tab_sim:
    rows = []
    for variant in ("cooc", "item", "hybrid"):
        res = load(f"eval-pair2-week08-{variant}.json")
        if res:
            for method in ("rec_rt", "api"):
                r = res["results"]["all"][method]
                rows.append({"rec:rt similarity": variant, "list": METHOD_LABELS[method],
                             "HR@10": r["hr@10"], "NDCG@10": r["ndcg@10"], "HR@20": r["hr@20"],
                             "coverage": r["coverage"]})
    if not rows:
        st.info("No ablation results yet (docs/eval-pair2-week08-*.json).")
    else:
        st.caption("Same replay, rec:rt built from sim:cooc (speed layer), sim:item (Item-kNN of the batch layer, "
                   "trained only on the warm-up day) or both (0.5 / 0.5).")
        df = pd.DataFrame(rows)
        st.bar_chart(df[df["list"] == METHOD_LABELS["api"]].set_index("rec:rt similarity")[["HR@10", "NDCG@10"]],
                     stack=False)
        st.dataframe(df, hide_index=True, width="stretch", column_config={"coverage": PERCENT})

with tab_batch:
    res = load("eval-week07.json")
    if res is None:
        st.info("docs/eval-week07.json not found (Pair 1, evaluation/final_eval.py).")
    else:
        p = res["protocol"]
        st.caption(f"Train {p['train'][0]} to {p['train'][1]}, test {p['test'][0]} to {p['test'][1]}, "
                   f"truth = {', '.join(p['truth_events'])} events; mean and bootstrap interval.")
        segment = st.selectbox("Users", list(res["results"]))
        models = res["results"][segment]["all"]["models"]
        metric = st.selectbox("Metric", list(next(iter(models.values()))))
        df = pd.DataFrame([{"model": m, metric: v[metric]["mean"], "low": v[metric]["low"], "high": v[metric]["high"]}
                           for m, v in models.items()])
        st.bar_chart(df.set_index("model")[[metric]])
        st.dataframe(df, hide_index=True, width="stretch")

with tab_load:
    rows = []
    for name, label in (("rate500", "500 requests/s"), ("rate1000", "1,000 requests/s")):
        res = load(f"loadtest/{name}.json")
        if res:
            total = next(r for r in res["rows"] if r["endpoint"] == "total")
            rows.append({"target": label, "achieved (req/s)": total["rps"], "p50 (ms)": total["p50_ms"],
                         "p95 (ms)": total["p95_ms"], "p99 (ms)": total["p99_ms"], "failures": total["failures"]})
    if not rows:
        st.info("No load test results (serving/loadtest/).")
    else:
        st.caption("Locust, real ids from Redis, 70 % /recommend; API with 2 workers. Target: p95 < 100 ms.")
        df = pd.DataFrame(rows)
        st.bar_chart(df.set_index("target")[["p50 (ms)", "p95 (ms)", "p99 (ms)"]], stack=False)
        st.dataframe(df, hide_index=True, width="stretch")
