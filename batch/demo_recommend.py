"""What the batch layer recommends to a few users, side by side (Pair 1, week 6).

For each user it prints the history before as_of and three top-k lists built from what the
nightly batch loaded into Redis, with the products the user touched in the test period marked:

  ALS         rec:batch:{user_id}, the batch list the API serves
  Item-kNN    sum of log(1 + score) * sim over sim:item of the products in the user's history,
              the same scoring as batch/item_knn.py (products already seen are kept)
  Popularity  pop:cat of the user's main category, else pop:global: what a user without
              rec:batch gets (the fallback)

Under each list: hits, hit@k and NDCG@k against the test-period products (every event type,
the week 3 protocol) and against the carted or purchased ones only.

  python -m batch.demo_recommend --user-id 512386086 --user-id 518044530
  python -m batch.demo_recommend --pick 3 --seed 7      # users with rec:batch and a test cart/purchase
  python -m batch.demo_recommend --pick-cold 1          # a buyer without rec:batch (fallback)
  python -m batch.demo_recommend --pick 3 --json docs/demo-week06.json

The first run also saves a small product table (category, brand, last price) to
--products, so later runs only scan the events of the chosen users.
"""

from __future__ import annotations

import argparse
import json
import math
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path

from common import redis_keys as rk
from common.config import settings
from common.schemas import CLEAN_PARTITION_COLUMN
from common.weights import event_weight
from evaluation.metrics import hit_rate_at_k, ndcg_at_k

from batch.split import TEST_END, TEST_START

INTENT_EVENTS = ("cart", "purchase")
MARKS = {"purchase": "P", "cart": "C", "view": "V"}     # strongest test-period event wins
EVENT_COLUMNS = ["event_type", "product_id", "category_id"]
DEFAULT_PRODUCTS = "data/demo/products.parquet"
CANDIDATES_PER_PICK = 50        # buyers drawn per wanted user before checking rec:batch in Redis


# ---- scoring (plain Python, no Spark or Redis) ----------------------------------------------

def interaction_scores(events) -> dict[int, float]:
    """product_id -> summed event weight, as batch/interactions.py (only positive weights)."""
    scores: dict[int, float] = defaultdict(float)
    for e in events:
        w = event_weight(e["event_type"])
        if w > 0:
            scores[int(e["product_id"])] += w
    return dict(scores)


def ranked(scores: dict[int, float], k: int) -> list[tuple[int, float]]:
    """Top k (product_id, score), best first; ties broken by the smaller product_id."""
    return sorted(scores.items(), key=lambda kv: (-kv[1], kv[0]))[:k]


def knn_scores(history: dict[int, float], neighbours: dict[int, list[tuple[int, float]]],
               k: int) -> list[tuple[int, float]]:
    """Item-kNN list for one user: score(j) = sum of log(1 + score_i) * sim(i, j)."""
    scores: dict[int, float] = defaultdict(float)
    for product, score in history.items():
        r = math.log1p(score)
        for neighbour, sim in neighbours.get(product, ()):
            scores[neighbour] += r * sim
    return ranked(scores, k)


def main_category(events) -> int | None:
    """Category with the largest summed event weight in the history; ties: smaller id."""
    weights: dict[int, float] = defaultdict(float)
    for e in events:
        if e.get("category_id") is not None:
            weights[int(e["category_id"])] += event_weight(e["event_type"])
    best = ranked({c: w for c, w in weights.items() if w > 0}, 1)
    return best[0][0] if best else None


def period_marks(events) -> dict[int, str]:
    """product_id -> P / C / V, the strongest event type in the test period."""
    order = {t: i for i, t in enumerate(MARKS)}
    best: dict[int, str] = {}
    for e in events:
        t, p = e["event_type"], int(e["product_id"])
        if t in order and (p not in best or order[t] < order[best[p]]):
            best[p] = t
    return {p: MARKS[t] for p, t in best.items()}


def score_list(products: list[int], marks: dict[int, str], k: int) -> dict:
    """hits, hit@k and NDCG@k against all test products and against carted/purchased ones."""
    every = set(marks)
    intent = {p for p, m in marks.items() if m in ("C", "P")}
    top = products[:k]
    return {
        "hits": sum(1 for p in top if p in every),
        f"hit@{k}": hit_rate_at_k(top, every, k),
        f"ndcg@{k}": round(ndcg_at_k(top, every, k), 4),
        "intent_hits": sum(1 for p in top if p in intent),
        f"intent_ndcg@{k}": round(ndcg_at_k(top, intent, k), 4),
    }


# ---- Redis ---------------------------------------------------------------------------------

def zset_top(client, key: str, k: int) -> list[tuple[int, float]]:
    return [(int(m), float(s)) for m, s in client.zrevrange(key, 0, k - 1, withscores=True)]


def neighbours(client, products, n: int = settings.sim_top_n) -> dict[int, list[tuple[int, float]]]:
    """sim:item lists of the given products (one pipeline round trip)."""
    products = sorted(set(products))
    pipe = client.pipeline(transaction=False)
    for p in products:
        pipe.zrevrange(rk.sim_item(p), 0, n - 1, withscores=True)
    return {p: [(int(m), float(s)) for m, s in rows] for p, rows in zip(products, pipe.execute()) if rows}


def popular(client, category: int | None, k: int) -> tuple[str, list[tuple[int, float]]]:
    if category is not None:
        rows = zset_top(client, rk.pop_cat(category), k)
        if rows:
            return rk.pop_cat(category), rows
    return rk.POP_GLOBAL, zset_top(client, rk.POP_GLOBAL, k)


# ---- one user ------------------------------------------------------------------------------

@dataclass
class UserDemo:
    user_id: int
    history: list[dict]                 # events before as_of, oldest first
    test: list[dict]                    # events in [as_of, until]
    lists: dict[str, list[tuple[int, float]]] = field(default_factory=dict)
    pop_key: str = ""
    has_rec_batch: bool = False

    @property
    def marks(self) -> dict[int, str]:
        return period_marks(self.test)

    def metrics(self, k: int) -> dict[str, dict]:
        marks = self.marks
        return {name: score_list([p for p, _ in rows], marks, k) for name, rows in self.lists.items()}


def build(client, user_id: int, history: list[dict], test: list[dict], k: int) -> UserDemo:
    demo = UserDemo(user_id, history, test)
    als = zset_top(client, rk.rec_batch(user_id), k)
    demo.has_rec_batch = bool(als)
    scores = interaction_scores(history)
    demo.pop_key, pop = popular(client, main_category(history), k)
    demo.lists = {"ALS": als, "Item-kNN": knn_scores(scores, neighbours(client, scores), k), "Popularity": pop}
    return demo


# ---- Spark: events, products, picking users ------------------------------------------------

def read_events(spark, source: str, start: date | None, end: date):
    from pyspark.sql import functions as F

    day = F.col(CLEAN_PARTITION_COLUMN)
    cond = day <= F.lit(end)
    if start:
        cond &= day >= F.lit(start)
    return spark.read.parquet(source).where(cond)


def user_events(spark, source: str, users: list[int], as_of: date, until: date) -> dict[int, tuple[list, list]]:
    """{user_id: (history before as_of, events in [as_of, until])}, each oldest first."""
    from pyspark.sql import functions as F

    # Times are formatted by Spark (session time zone UTC): collect() would shift them to the
    # time zone of the machine.
    rows = (read_events(spark, source, None, until).where(F.col("user_id").isin(users))
            .select("user_id", CLEAN_PARTITION_COLUMN, *EVENT_COLUMNS,
                    F.unix_timestamp("event_time").alias("ts"),
                    F.date_format("event_time", "yyyy-MM-dd HH:mm").alias("time"))
            .collect())
    out: dict[int, tuple[list, list]] = {u: ([], []) for u in users}
    for r in sorted(rows, key=lambda r: (r["user_id"], r["ts"], r["product_id"])):
        e = {c: r[c] for c in (*EVENT_COLUMNS, "time")}
        out[r["user_id"]][0 if r[CLEAN_PARTITION_COLUMN] < as_of else 1].append(e)
    return out


def product_table(spark, source: str, cache: str):
    """product_id -> (category_code, brand, last price), saved to `cache` on the first run."""
    import pandas as pd

    path = Path(cache)
    if not path.exists():
        from pyspark.sql import functions as F

        pdf = (spark.read.parquet(source).groupBy("product_id")
               .agg(F.max("category_code").alias("category_code"), F.max("brand").alias("brand"),
                    F.max_by("price", "event_time").alias("price"))
               .toPandas())
        path.parent.mkdir(parents=True, exist_ok=True)
        pdf.to_parquet(path, index=False)
    return pd.read_parquet(path).set_index("product_id")


def pick_users(spark, client, source: str, as_of: date, until: date, n: int, seed: int,
               warm: bool) -> list[int]:
    """n test-period buyers (cart or purchase) with (warm) or without rec:batch, in a seeded order."""
    from pyspark.sql import functions as F

    if n <= 0:
        return []
    candidates = [r["user_id"] for r in (
        read_events(spark, source, as_of, until).where(F.col("event_type").isin(*INTENT_EVENTS))
        .select("user_id").distinct()
        .orderBy(F.xxhash64("user_id", F.lit(seed)), "user_id")
        .limit(n * CANDIDATES_PER_PICK).collect())]
    pipe = client.pipeline(transaction=False)
    for u in candidates:
        pipe.exists(rk.rec_batch(u))
    return [u for u, found in zip(candidates, pipe.execute()) if bool(found) == warm][:n]


# ---- output --------------------------------------------------------------------------------

def _info(products, product_id: int):
    return None if products is None or product_id not in products.index else products.loc[product_id]


def label(products, product_id: int) -> str:
    """'brand leaf-category', e.g. 'samsung smartphone'; '?' for a product missing from the table."""
    row = _info(products, product_id)
    if row is None:
        return "?"
    code = row["category_code"] if isinstance(row["category_code"], str) else ""
    parts = (row["brand"], code.rsplit(".", 1)[-1])
    return " ".join(x for x in parts if isinstance(x, str) and x) or "-"


def price(products, product_id: int) -> str:
    row = _info(products, product_id)
    value = None if row is None else row["price"]
    return f"{value:.2f}" if isinstance(value, (int, float)) and not math.isnan(value) else ""


def event_summary(events: list[dict]) -> str:
    c = Counter(e["event_type"] for e in events)
    products = len({e["product_id"] for e in events})
    kinds = ", ".join(f"{t} {c[t]}" for t in ("view", "cart", "purchase") if c[t])
    return f"{len(events)} events, {products} products" + (f" ({kinds})" if kinds else "")


def render(demo: UserDemo, products, k: int, as_of: date, until: date, recent: int = 8) -> str:
    marks = demo.marks
    seen = {int(e["product_id"]) for e in demo.history}
    lines = [
        f"User {demo.user_id}",
        f"  history before {as_of}: {event_summary(demo.history)}",
        f"  test period {as_of}..{until}: {event_summary(demo.test)}",
        f"  rec:batch: {'yes' if demo.has_rec_batch else 'no (the API falls back to popularity)'}",
        f"  latest history events, UTC (newest first, up to {recent}):",
    ]
    for e in reversed(demo.history[-recent:]):
        p = int(e["product_id"])
        lines.append(f"    {e['time']}  {e['event_type']:<8} {p:>9}  "
                     f"{label(products, p)[:30]:<30} {price(products, p):>8}")
    if not demo.history:
        lines.append("    (none: a new user)")

    width = 34
    names = list(demo.lists)
    heads = [n if n != "Popularity" else f"Popularity ({demo.pop_key})" for n in names]
    lines.append("")
    lines.append(("   #  " + "  ".join(f"{h[:width]:<{width}}" for h in heads)).rstrip())
    for i in range(max((len(rows) for rows in demo.lists.values()), default=0)):
        cells = []
        for n in names:
            rows = demo.lists[n]
            if i < len(rows):
                p = rows[i][0]
                tag = marks.get(p, " ") + ("*" if p in seen else " ")
                cells.append(f"{p:>9} {label(products, p)[:width - 13]:<{width - 13}} {tag}")
            else:
                cells.append("")
        lines.append((f"  {i + 1:>2}  " + "  ".join(f"{c:<{width}}" for c in cells)).rstrip())
    metrics = demo.metrics(k)
    for key, title in ((f"ndcg@{k}", "all test events"), (f"intent_ndcg@{k}", "cart/purchase only")):
        hits = "hits" if key.startswith("ndcg") else "intent_hits"
        cell = [f"hits {metrics[n][hits]}, NDCG@{k} {metrics[n][key]:.3f}" for n in names]
        lines.append("      " + "  ".join(f"{c:<{width}}" for c in cell) + f"  <- {title}")
    lines.append("  P/C/V = purchased/carted/viewed in the test period, * = already in the history")
    return "\n".join(lines)


def to_json(demo: UserDemo, products, k: int) -> dict:
    marks = demo.marks
    return {
        "user_id": demo.user_id,
        "history": {"events": len(demo.history), "products": len({e["product_id"] for e in demo.history})},
        "test": {"events": len(demo.test), "products": len(marks),
                 "intent_products": sum(1 for m in marks.values() if m in ("C", "P"))},
        "has_rec_batch": demo.has_rec_batch,
        "pop_key": demo.pop_key,
        "lists": {name: [{"product_id": p, "score": round(s, 4), "label": label(products, p),
                          "test_event": marks.get(p)} for p, s in rows]
                  for name, rows in demo.lists.items()},
        "metrics": demo.metrics(k),
    }


def parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Batch recommendations (ALS, Item-kNN, Popularity) for a few users")
    p.add_argument("--user-id", type=int, action="append", default=[], help="repeat for several users")
    p.add_argument("--pick", type=int, default=0, help="random test-period buyers that have rec:batch")
    p.add_argument("--pick-cold", type=int, default=0, help="random test-period buyers without rec:batch")
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--k", type=int, default=10)
    p.add_argument("--as-of", default=str(TEST_START), help="history = events before this date (the nightly as_of)")
    p.add_argument("--until", default=str(TEST_END), help="last day of the test period")
    p.add_argument("--input", default=settings.clean_path, help="clean Parquet from batch/etl.py")
    p.add_argument("--no-s3", action="store_true", help="local paths only, do not load the s3a jars")
    p.add_argument("--products", default=DEFAULT_PRODUCTS, help="product table cache (built on the first run)")
    p.add_argument("--redis-host", default=settings.redis_host)
    p.add_argument("--redis-port", type=int, default=settings.redis_port)
    p.add_argument("--redis-db", type=int, default=settings.redis_db)
    p.add_argument("--json", help="also write the lists and metrics to this file")
    return p


def main(argv=None) -> None:
    from batch.redis_loader import RedisTarget
    from batch.spark_session import get_spark

    cfg = parser().parse_args(argv)
    as_of, until = date.fromisoformat(cfg.as_of), date.fromisoformat(cfg.until)
    if until < as_of:
        raise SystemExit("--until must not be before --as-of")
    client = RedisTarget(cfg.redis_host, cfg.redis_port, cfg.redis_db).client()
    spark = get_spark("demo-recommend", s3=not cfg.no_s3)
    spark.sparkContext.setLogLevel("WARN")
    try:
        users = list(dict.fromkeys(
            cfg.user_id
            + pick_users(spark, client, cfg.input, as_of, until, cfg.pick, cfg.seed, warm=True)
            + pick_users(spark, client, cfg.input, as_of, until, cfg.pick_cold, cfg.seed, warm=False)))
        if not users:
            raise SystemExit("no users: pass --user-id, --pick or --pick-cold")
        products = product_table(spark, cfg.input, cfg.products)
        events = user_events(spark, cfg.input, users, as_of, until)
    finally:
        spark.stop()

    demos = [build(client, u, *events[u], cfg.k) for u in users]
    last_run = client.get(rk.META_BATCH_LAST_RUN)
    print(f"Batch lists from Redis {cfg.redis_host}:{cfg.redis_port}/{cfg.redis_db}, "
          f"meta:batch:last_run = {last_run}\n")
    print("\n\n".join(render(d, products, cfg.k, as_of, until) for d in demos))
    if cfg.json:
        out = Path(cfg.json)
        out.parent.mkdir(parents=True, exist_ok=True)
        report = {"as_of": str(as_of), "until": str(until), "k": cfg.k, "batch_last_run": last_run,
                  "users": [to_json(d, products, cfg.k) for d in demos]}
        out.write_text(json.dumps(report, indent=2, default=str) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
