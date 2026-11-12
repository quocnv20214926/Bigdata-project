"""Batch only vs batch + speed, by offline replay of cart and purchase events (Pair 1, week 8).

docs/plan.md, section 7: Hit Rate@K and NDCG@K at each cart / purchase event, using the
recommendations the API would have returned just before the event, and the similarity ablation
cooc / item / hybrid. The API's Redis mode is not on master yet, so the replay runs the functions
of common/scoring.py that it will use.

For a cutoff day C, the morning after a nightly run:
  - batch layer, from the events before C: ALS top 50 per user with its scores (rec:batch),
    Item-kNN top 50 neighbours (sim:item), global popularity (pop:global);
  - speed layer, emulated just before each target event:
      sim:cooc     session co-occurrence counted as streaming/sinks/cooc.py does (the last 10
                   distinct products of a session), from the events before C, so it is frozen at
                   C like sim:item;
      user:recent  the user's 20 newest events before the target, if the newest is under 24 h old
                   (the key's TTL);
      user:bought  the products the user bought before the target, if the last purchase is under
                   30 days old;
      rec:rt       common.scoring.propagate of every user:recent event through a similarity table,
                   decayed to the target time, top 50;
  - targets: one cart or purchase event per user on day C, for a sample of users (mode "cart",
    the plan's protocol); or, mode "new", one event per user on day C whose product the user has
    never touched before, with every product the user touched before the target left out of every
    list (week 7's "unseen" filter: rec:batch is then the ALS top 50 without them, out of the top 150);
  - variants: popularity only; batch only; speed only (cooc, item or hybrid similarity, each also
    "+self", with the user:recent products themselves added to rec:rt; five half-lives); batch +
    speed with alpha 0.1 .. 0.95. Each is ranked by common.scoring.merge_scores (what the API
    runs), without the user:bought products, and completed with pop:global;
  - hit@k and ndcg@k of the target product (one relevant product per target).

Alpha, the half-life and the similarity are chosen on the validation day (11-09, models trained on
10-01 .. 11-08); the test day (11-16, models trained on 10-01 .. 11-15) reports the chosen
variants with 95% bootstrap intervals over targets (one per user) and paired differences.

Usage (from the repository root, MinIO running):
  SPARK_DRIVER_MEMORY=14g python -m evaluation.replay --report docs/eval-week08-replay.json
  SPARK_DRIVER_MEMORY=14g python -m evaluation.replay --targets-mode new --report docs/eval-week08-new.json
"""

from __future__ import annotations

import argparse
import json
import math
import shutil
import tempfile
import time
from collections import Counter
from datetime import date
from pathlib import Path

import numpy as np
import pandas as pd
from pyspark.sql import DataFrame, Window, functions as F

from batch import als, item_knn
from batch.interactions import event_weight, filter_rare, interaction_scores, matrix_stats
from batch.popularity import product_popularity, top_global
from batch.spark_session import get_spark
from batch.split import TEST_START, TRAIN_START, VALID_START
from common import redis_keys as rk
from common.config import settings
from common.scoring import DEFAULT_HALF_LIFE_SECONDS, merge_scores
from evaluation import bootstrap

TARGET_EVENTS = ("cart", "purchase")
# "cart": a cart or purchase event (docs/plan.md section 7); "new": the first event ever of the user
# on a product. Almost every cart is for a product in user:recent, so "cart" mostly measures showing
# the products just viewed again; "new" measures finding products the user has not seen.
TARGET_MODES = ("cart", "new")
NEW_BATCH_POOL = 3 * settings.batch_top_n   # mode "new": ALS candidates before the seen products are removed
DEFAULT_TARGETS = 30_000
BASE_SIMS = ("cooc", "item", "hybrid")
# "+self": the same table plus sim(i, i) = 1 for the user:recent products, so rec:rt also holds the
# products the user has just viewed, not only their neighbours (common.scoring.propagate leaves them out)
SELF_SUFFIX = "+self"
SIMS = BASE_SIMS + tuple(s + SELF_SUFFIX for s in BASE_SIMS)
ALPHAS = tuple(round(0.1 * i, 1) for i in range(1, 10)) + (0.95,)
HALF_LIVES = (300, DEFAULT_HALF_LIFE_SECONDS, 7200, 43200, 86400)  # 5 min, 30 min (default), 2 h, 12 h, 24 h
KS = (10, 20)
HYBRID_ITEM_WEIGHT = 0.5        # hybrid sim(i, j) = 0.5 * sim:item + 0.5 * sim:cooc
RT_TOP_N = settings.batch_top_n
# streaming/parsing.DEDUP_COLUMNS: the speed layer drops exact duplicates before counting
DEDUP_COLUMNS = ["user_id", "product_id", "event_type", "event_time", "user_session"]
GROUPS = ("both", "batch_only", "recent_only", "neither")
COOC_PARTITIONS = 400           # fixed, so each Python worker counts a bounded share of the sessions


# ---- sim:cooc --------------------------------------------------------------------------------

def walk_sessions(sessions, products, cnt: Counter, pairs: Counter, state: dict,
                  window: int = settings.session_window) -> None:
    """The counting of streaming/sinks/cooc.update_cooc, for events sorted by session and time.

    cnt[i] += 1 when product i enters its session's window of the last `window` distinct
    products; pairs[(i, j)] (i < j) += 1 for every product j already in the window. A product
    seen again while in the window only moves to the front. `state` carries the open session
    from one call to the next.
    """
    current, prev = state.get("session"), state.get("window", [])
    for s, pid in zip(sessions, products):
        if s != current:
            current, prev = s, []
        if pid in prev:
            if prev[0] != pid:
                prev.remove(pid)
                prev.insert(0, pid)
            continue
        cnt[pid] += 1
        for j in prev:
            pairs[(pid, j) if pid < j else (j, pid)] += 1
        prev.insert(0, pid)
        del prev[window:]
    state["session"], state["window"] = current, prev


def cooc_counts(events: DataFrame, window: int = settings.session_window,
                partitions: int = COOC_PARTITIONS) -> DataFrame:
    """(i, j, c): co-occurrence counts with i < j, and each product's own count as (i, i, cnt(i))."""
    ordered = (events.where(F.col("user_session").isNotNull())
               .dropDuplicates(DEDUP_COLUMNS)
               .select("user_session", "event_time", "product_id")
               .repartition(partitions, "user_session")
               .sortWithinPartitions("user_session", "event_time", "product_id"))

    def count(batches):
        cnt, pairs, state = Counter(), Counter(), {}
        for pdf in batches:
            walk_sessions(pdf["user_session"].tolist(), pdf["product_id"].tolist(), cnt, pairs, state, window)
        if cnt:
            yield pd.DataFrame({"i": np.fromiter(cnt.keys(), np.int32), "j": np.fromiter(cnt.keys(), np.int32),
                                "c": np.fromiter(cnt.values(), np.int64)})
        if pairs:
            yield pd.DataFrame({"i": np.fromiter((p[0] for p in pairs), np.int32),
                                "j": np.fromiter((p[1] for p in pairs), np.int32),
                                "c": np.fromiter(pairs.values(), np.int64)})

    return ordered.mapInPandas(count, "i int, j int, c long").groupBy("i", "j").agg(F.sum("c").alias("c"))


def top_neighbours(sims: DataFrame, n: int) -> DataFrame:
    """(product_id, neighbour_id, sim): the n most similar products of each product."""
    order = Window.partitionBy("product_id").orderBy(F.col("sim").desc(), F.col("neighbour_id"))
    return (sims.withColumn("pos", F.row_number().over(order)).where(F.col("pos") <= n)
            .select("product_id", "neighbour_id", "sim"))


def cooc_similarities(counts: DataFrame, n: int = settings.sim_top_n) -> DataFrame:
    """sim:cooc: min(1, cooc(i, j) / sqrt(cnt(i) * cnt(j))) (common.scoring.cooc_similarity, clipped
    as in streaming/sinks/cooc.refresh_similarity), top n per product."""
    own = counts.where(F.col("i") == F.col("j"))
    ci = own.select("i", F.col("c").alias("cnt_i"))
    cj = own.select("j", F.col("c").alias("cnt_j"))
    half = (counts.where(F.col("i") < F.col("j")).join(ci, "i").join(cj, "j")
            .select("i", "j", F.least(F.lit(1.0), F.col("c") / F.sqrt(F.col("cnt_i") * F.col("cnt_j"))).alias("sim")))
    both = half.unionByName(half.select(F.col("j").alias("i"), F.col("i").alias("j"), "sim"))
    return top_neighbours(both.select(F.col("i").alias("product_id"), F.col("j").alias("neighbour_id"), "sim"), n)


def hybrid_similarities(item: DataFrame, cooc: DataFrame, n: int = settings.sim_top_n,
                        item_weight: float = HYBRID_ITEM_WEIGHT) -> DataFrame:
    """Weighted sum of the two tables (a missing pair counts 0), top n per product."""
    key = ["product_id", "neighbour_id"]
    both = (item.select(*key, F.col("sim").alias("s_item"))
            .join(cooc.select(*key, F.col("sim").alias("s_cooc")), key, "full")
            .select(*key, (item_weight * F.coalesce("s_item", F.lit(0.0))
                           + (1 - item_weight) * F.coalesce("s_cooc", F.lit(0.0))).alias("sim")))
    return top_neighbours(both, n)


# ---- targets and the speed layer state ---------------------------------------------------------

def pick_targets(events: DataFrame, day: date, n: int, seed: int, mode: str = "cart") -> DataFrame:
    """(target_id, user_id, product_id, event_type, event_time): one event of `day` per user, for n
    users (0 = all), the same ones for the same seed. Mode "cart": a cart or purchase event; mode
    "new": the user's first event ever on the product."""
    today = events.where(F.col("event_date") == F.lit(day))
    if mode == "cart":
        cand = today.where(F.col("event_type").isin(*TARGET_EVENTS))
    elif mode == "new":
        first = (events.where(F.col("event_date") <= F.lit(day))
                 .join(today.select("user_id").distinct(), "user_id", "left_semi")
                 .groupBy("user_id", "product_id").agg(F.min("event_time").alias("event_time")))
        cand = today.join(first, ["user_id", "product_id", "event_time"])
    else:
        raise ValueError(f"mode must be one of {TARGET_MODES}, not {mode!r}")
    cand = cand.select("user_id", "product_id", "event_type", "event_time").distinct()
    one = Window.partitionBy("user_id").orderBy(
        F.xxhash64("product_id", "event_type", "event_time", F.lit(seed)), "event_time", "product_id")
    per_user = cand.withColumn("pos", F.row_number().over(one)).where(F.col("pos") == 1).drop("pos")
    ordered = per_user.withColumn("h", F.xxhash64("user_id", F.lit(seed)))
    if n:
        ordered = ordered.orderBy("h", "user_id").limit(n)
    return (ordered.withColumn("target_id", F.row_number().over(Window.orderBy("h", "user_id")) - 1)
            .select("target_id", "user_id", "product_id", "event_type", "event_time"))


def speed_state(events: DataFrame, targets: DataFrame, recent_n: int = settings.recent_n) -> tuple[DataFrame, DataFrame]:
    """(recent, bought) just before each target.

    recent: (target_id, product_id, event_type, age) for the user:recent events, age in seconds;
    bought: (target_id, product_id) for the user:bought products.
    """
    t = targets.select("target_id", "user_id", F.col("event_time").alias("t"))
    history = (events.join(targets.select("user_id"), "user_id", "left_semi")
               .dropDuplicates(DEDUP_COLUMNS)
               .select("user_id", "product_id", "event_type", "event_time"))
    before = (t.join(history, "user_id").where(F.col("event_time") < F.col("t"))
              .withColumn("age", F.unix_timestamp("t") - F.unix_timestamp("event_time")))

    # a user:recent member is "product|event_type|epoch": the same event twice is one member
    members = before.select("target_id", "product_id", "event_type", "event_time", "age").distinct()
    newest = Window.partitionBy("target_id").orderBy(F.col("event_time").desc(), "product_id", "event_type")
    recent = (members.withColumn("pos", F.row_number().over(newest)).where(F.col("pos") <= recent_n)
              .withColumn("newest_age", F.min("age").over(Window.partitionBy("target_id")))
              .where(F.col("newest_age") <= rk.TTL["user:recent"])
              .select("target_id", "product_id", "event_type", "age"))

    purchases = before.where(F.col("event_type") == "purchase")
    bought = (purchases.withColumn("last_age", F.min("age").over(Window.partitionBy("target_id")))
              .where(F.col("last_age") <= rk.TTL["user:bought"])
              .select("target_id", "product_id").distinct())
    return recent, bought


def seen_products(events: DataFrame, targets: DataFrame) -> DataFrame:
    """(target_id, product_id): every product the user touched before the target event."""
    t = targets.select("target_id", "user_id", F.col("event_time").alias("t"))
    return (events.join(targets.select("user_id"), "user_id", "left_semi")
            .select("user_id", "product_id", "event_time")
            .join(t, "user_id").where(F.col("event_time") < F.col("t"))
            .select("target_id", "product_id").distinct())


def realtime_scores(recent: DataFrame, sims: DataFrame, half_lives=HALF_LIVES, top_n: int = RT_TOP_N,
                    exclude: DataFrame | None = None) -> DataFrame:
    """(target_id, half_life, product_id, score): rec:rt as common.scoring.propagate builds it,
    rt[j] = sum over user:recent events of w_e * sim(i, j) * exp(-ln 2 * age / half_life), top n.
    exclude: (target_id, product_id) pairs left out before the top n."""
    w = event_weight(F.col("event_type"))
    parts = (recent.withColumn("w", w).where(F.col("w") > 0)
             .join(sims.where(F.col("sim") > 0), "product_id")
             .withColumn("age", F.greatest(F.col("age"), F.lit(0))))
    sums = [F.sum(F.col("w") * F.col("sim") * F.exp(-math.log(2) * F.col("age") / F.lit(float(hl)))).alias(f"hl{hl}")
            for hl in half_lives]
    scores = parts.groupBy("target_id", F.col("neighbour_id").alias("product_id")).agg(*sums)
    if exclude is not None:
        scores = scores.join(exclude.select("target_id", "product_id"), ["target_id", "product_id"], "left_anti")
    long = scores.select("target_id", "product_id", F.explode(F.array(*[
        F.struct(F.lit(hl).alias("half_life"), F.col(f"hl{hl}").alias("score")) for hl in half_lives])).alias("x"))
    long = long.select("target_id", F.col("x.half_life").alias("half_life"), "product_id", F.col("x.score").alias("score"))
    order = Window.partitionBy("target_id", "half_life").orderBy(F.col("score").desc(), "product_id")
    return long.withColumn("pos", F.row_number().over(order)).where(F.col("pos") <= top_n).drop("pos")


def with_self(sims: DataFrame, recent: DataFrame) -> DataFrame:
    """The similarity table plus (i, i, 1.0) for every product in user:recent."""
    own = recent.select("product_id").distinct()
    return sims.select("product_id", "neighbour_id", "sim").unionByName(
        own.select("product_id", F.col("product_id").alias("neighbour_id"), F.lit(1.0).alias("sim")))


# ---- ranking the variants (plain Python, the API's merge) ----------------------------------------

def final_list(merged, fallback, exclude, k: int) -> list[int]:
    """The merged products, then fallback products until k (user:bought products never)."""
    out = [p for p, _, _ in merged]
    if len(out) < k:
        have = set(out)
        for p in fallback:
            if p not in have and p not in exclude:
                out.append(p)
                have.add(p)
                if len(out) == k:
                    break
    return out


def rank_in(product: int, items) -> int:
    """1-based position of product in items, 0 if absent."""
    for pos, p in enumerate(items, 1):
        if p == product:
            return pos
    return 0


def variant_names(half_lives=HALF_LIVES, alphas=ALPHAS, sims=SIMS) -> list[str]:
    names = ["popularity", "batch"]
    for s in sims:
        for hl in half_lives:
            names.append(f"speed:{s}:{hl}")
            names += [f"hybrid:{s}:{hl}:{a}" for a in alphas]
    return names


def to_dicts(pdf: pd.DataFrame, key: str) -> dict:
    """{key value: {product_id: score}} from a frame sorted by key."""
    out = {}
    if not len(pdf):
        return out
    pdf = pdf.sort_values(key, kind="stable")
    keys = pdf[key].to_numpy()
    cuts = np.flatnonzero(keys[1:] != keys[:-1]) + 1
    for part in np.split(np.arange(len(pdf)), cuts):
        sub = pdf.iloc[part]
        out[int(keys[part[0]])] = dict(zip(sub["product_id"].astype(int).tolist(), sub["score"].astype(float).tolist()))
    return out


def rank_variants(targets: pd.DataFrame, batch: dict, rt: dict, bought: dict, popular: list[int],
                  half_lives=HALF_LIVES, alphas=ALPHAS, sims=SIMS, k: int = max(KS)) -> tuple[list[str], np.ndarray]:
    """Rank of each target's product (0 = not in the top k) for every variant.

    targets: target_id, user_id, product_id. batch: user -> {product: ALS score}.
    rt: (sim, half_life) -> target -> {product: score}. bought: target -> set of products.
    """
    names = variant_names(half_lives, alphas, sims)
    col = {n: i for i, n in enumerate(names)}
    ranks = np.zeros((len(targets), len(names)), dtype=np.int16)
    rows = zip(targets["target_id"].tolist(), targets["user_id"].tolist(), targets["product_id"].tolist())
    for row, (tid, user, product) in enumerate(rows):
        b, ex = batch.get(user, {}), bought.get(tid, set())
        r_pop = rank_in(product, final_list([], popular, ex, k))
        r_batch = rank_in(product, final_list(merge_scores(b, {}, exclude=ex, k=k), popular, ex, k)) if b else r_pop
        ranks[row, col["popularity"]], ranks[row, col["batch"]] = r_pop, r_batch
        for s in sims:
            for hl in half_lives:
                r = rt[(s, hl)].get(tid)
                if not r:                              # no rec:rt: alpha is 1, the batch list
                    ranks[row, col[f"speed:{s}:{hl}"]] = r_pop
                    for a in alphas:
                        ranks[row, col[f"hybrid:{s}:{hl}:{a}"]] = r_batch
                    continue
                r_speed = rank_in(product, final_list(merge_scores({}, r, exclude=ex, k=k), popular, ex, k))
                ranks[row, col[f"speed:{s}:{hl}"]] = r_speed
                for a in alphas:
                    ranks[row, col[f"hybrid:{s}:{hl}:{a}"]] = (
                        rank_in(product, final_list(merge_scores(b, r, alpha=a, exclude=ex, k=k), popular, ex, k))
                        if b else r_speed)
    return names, ranks


def metrics_from_ranks(ranks: np.ndarray, ks=KS) -> dict[str, np.ndarray]:
    """Per target and variant: hit_rate@k and ndcg@k (one relevant product)."""
    out = {}
    for k in ks:
        hit = (ranks > 0) & (ranks <= k)
        out[f"hit_rate@{k}"] = hit.astype(np.float64)
        out[f"ndcg@{k}"] = np.where(hit, 1.0 / np.log2(np.maximum(ranks, 1) + 1.0), 0.0)
    return out


def mean_grid(names, per_target: dict[str, np.ndarray]) -> dict:
    return {n: {m: round(float(v[:, i].mean()), 6) if len(v) else None for m, v in per_target.items()}
            for i, n in enumerate(names)}


def choose(grid: dict, metric: str = "ndcg@10", half_lives=HALF_LIVES, alphas=ALPHAS, sims=SIMS) -> dict:
    """Best half-life for each speed-only variant, best (half-life, alpha) for each batch + speed
    variant, and the best batch + speed variant overall."""
    best = lambda names: max(names, key=lambda n: (grid[n][metric], n))
    chosen = {}
    for s in sims:
        chosen[f"speed:{s}"] = best([f"speed:{s}:{hl}" for hl in half_lives])
        chosen[f"hybrid:{s}"] = best([f"hybrid:{s}:{hl}:{a}" for hl in half_lives for a in alphas])
    chosen["hybrid"] = best([chosen[f"hybrid:{s}"] for s in sims])
    return chosen


# ---- one cutoff ------------------------------------------------------------------------------------

def replay_day(events: DataFrame, cutoff: date, params: als.ALSParams, knn_params: item_knn.KNNParams,
               n_targets: int, half_lives=HALF_LIVES, alphas=ALPHAS, ks=KS, mode: str = "cart",
               sims_run=SIMS) -> dict:
    """Models from the events before `cutoff`, targets on `cutoff`. Returns sizes, timings and,
    per target, the ranks of every variant."""
    timings, t = {}, time.time()
    spark = events.sparkSession
    k = max(ks)
    history = events.where((F.col("event_date") >= F.lit(TRAIN_START)) & (F.col("event_date") < F.lit(cutoff)))
    upto = events.where((F.col("event_date") >= F.lit(TRAIN_START)) & (F.col("event_date") <= F.lit(cutoff)))

    targets = pick_targets(events, cutoff, n_targets, params.seed, mode).cache()
    target_users = targets.select("user_id")
    data = {"history": [str(TRAIN_START), str(cutoff)], "target_day": str(cutoff),
            "targets": targets.count(),
            "target_events": {r["event_type"]: r["count"] for r in targets.groupBy("event_type").count().collect()}}

    # ALS first, while only the matrix is cached (evaluation/final_eval.py: rank 128 ran out of
    # a 14 GB driver heap when other tables were cached before it)
    interactions = interaction_scores(history).cache()
    matrix = filter_rare(interactions).cache()
    data["als_matrix"] = matrix_stats(matrix)
    interactions.unpersist()
    timings["prepare"] = round(time.time() - t, 1)

    t = time.time()
    model = als.train(matrix, params)
    timings["als_train"] = round(time.time() - t, 1)
    t = time.time()
    pool = NEW_BATCH_POOL if mode == "new" else settings.batch_top_n
    batch = to_dicts(als.recommend(model, target_users, pool)
                     .select("user_id", "product_id", "score").toPandas(), "user_id")
    popular = [int(r["product_id"]) for r in
               top_global(product_popularity(history), k + 200).orderBy("rank").collect()]
    als.release(spark)
    del model
    timings["als_recommend"] = round(time.time() - t, 1)

    sims, data["similarity"] = {}, {}
    builders = {"item": lambda: item_knn.similarities(matrix, knn_params),
                "cooc": lambda: cooc_similarities(cooc_counts(history)),
                "hybrid": lambda: hybrid_similarities(sims["item"], sims["cooc"])}
    for s in BASE_SIMS:
        t = time.time()
        sims[s] = builders[s]().cache()
        data["similarity"][s] = sims[s].agg(F.count("*").alias("pairs"),
                                            F.countDistinct("product_id").alias("products")).first().asDict()
        timings[f"sim_{s}"] = round(time.time() - t, 1)
        if s == "item":
            matrix.unpersist()

    t = time.time()
    recent, bought_df = speed_state(upto, targets)
    recent = recent.cache()
    bought = {}
    for r in bought_df.collect():
        bought.setdefault(int(r["target_id"]), set()).add(int(r["product_id"]))
    tpdf = targets.select("target_id", "user_id", "product_id").toPandas().sort_values("target_id")
    exclude, seen = {tid: set(ps) for tid, ps in bought.items()}, None
    if mode == "new":                  # leave every product touched before the target out of every list
        seen = seen_products(upto, targets).cache()
        spdf = seen.toPandas()
        data["seen_pairs"] = len(spdf)
        for tid, p in zip(spdf["target_id"].astype(int).tolist(), spdf["product_id"].astype(int).tolist()):
            exclude.setdefault(tid, set()).add(p)
        del spdf
        tid_of = dict(zip(tpdf["user_id"].tolist(), tpdf["target_id"].tolist()))
        batch = {u: dict(sorted(((p, s) for p, s in b.items() if p not in exclude.get(tid_of.get(u), ())),
                                key=lambda x: -x[1])[:settings.batch_top_n])
                 for u, b in batch.items()}
        batch = {u: b for u, b in batch.items() if b}
    recent_pairs = recent.select("target_id", "product_id").distinct().toPandas()
    with_recent = set(recent_pairs["target_id"].astype(int).tolist())
    in_recent = set(zip(recent_pairs["target_id"].astype(int).tolist(), recent_pairs["product_id"].astype(int).tolist()))
    rows = list(zip(tpdf["target_id"].tolist(), tpdf["user_id"].tolist(), tpdf["product_id"].tolist()))
    # how often the product of the target event is already in user:recent, or in the user's rec:batch
    data["target_in_recent"] = sum((tid, p) in in_recent for tid, _, p in rows)
    data["target_in_rec_batch"] = sum(p in batch.get(u, {}) for _, u, p in rows)
    del recent_pairs, in_recent, rows
    timings["speed_state"] = round(time.time() - t, 1)

    names, columns = [], []
    for s in sims_run:                 # one similarity at a time: its rec:rt lists, then its variants
        t = time.time()
        table = sims[s] if s in sims else with_self(sims[s.removesuffix(SELF_SUFFIX)], recent)
        scores = realtime_scores(recent, table, half_lives, exclude=seen).toPandas()
        rt = {(s, hl): to_dicts(scores[scores["half_life"] == hl], "target_id") for hl in half_lives}
        del scores
        timings[f"rt_{s}"] = round(time.time() - t, 1)
        t = time.time()
        part_names, part = rank_variants(tpdf, batch, rt, exclude, popular, half_lives, alphas, (s,), k)
        keep = [i for i, n in enumerate(part_names) if n not in names]
        names += [part_names[i] for i in keep]
        columns.append(part[:, keep])
        timings[f"rank_{s}"] = round(time.time() - t, 1)
    ranks = np.hstack(columns)

    has_batch = tpdf["user_id"].isin(set(batch)).to_numpy()
    has_recent = tpdf["target_id"].isin(with_recent).to_numpy()
    group = np.select([has_batch & has_recent, has_batch, has_recent], list(GROUPS[:3]), GROUPS[3])
    data["groups"] = {g: int((group == g).sum()) for g in GROUPS}
    data["with_bought"] = len(bought)
    spark.catalog.clearCache()
    return {"data": data, "seconds": timings, "names": names, "ranks": ranks, "group": group}


# ---- report ----------------------------------------------------------------------------------------

def summarise(day: dict, chosen: dict, ks=KS, resamples: int = bootstrap.DEFAULT_RESAMPLES, seed: int = 42) -> dict:
    """Intervals of the chosen variants, their paired differences, and their means per group."""
    names, ranks, group = day["names"], day["ranks"], day["group"]
    per_target = metrics_from_ranks(ranks, ks)
    metric_names = list(per_target)
    shown = {"popularity": "popularity", "batch": "batch", **chosen}
    col = {n: names.index(v) for n, v in shown.items()}

    def values(name, mask=None):
        x = np.column_stack([per_target[m][:, col[name]] for m in metric_names])
        return x if mask is None else x[mask]

    out = {"variants": {n: {"variant": shown[n], **dict(zip(metric_names, bootstrap.mean_ci(values(n), resamples, seed)))}
                        for n in shown}}
    pairs = [("hybrid", "batch"), ("hybrid", "popularity"), ("batch", "popularity")]
    pairs += [(f"hybrid:{s}", f"speed:{s}") for s in SIMS] + [(f"hybrid:{s}", "batch") for s in SIMS]
    pairs += [("speed:item", "speed:cooc"), ("speed:hybrid", "speed:cooc"), ("hybrid:item", "hybrid:cooc")]
    pairs += [(f"speed:{s}{SELF_SUFFIX}", f"speed:{s}") for s in BASE_SIMS]
    pairs += [(f"speed:{s}{SELF_SUFFIX}", "popularity") for s in BASE_SIMS]
    pairs += [(f"hybrid:{s}{SELF_SUFFIX}", f"hybrid:{s}") for s in BASE_SIMS]
    pairs = [(a, b) for a, b in pairs if a in shown and b in shown]
    out["differences"] = {f"{a} - {b}": dict(zip(metric_names, bootstrap.paired_diff_ci(values(a), values(b),
                                                                                     resamples, seed)))
                          for a, b in pairs}
    out["groups"] = {}
    for g in GROUPS:
        mask = group == g
        out["groups"][g] = {"targets": int(mask.sum()),
                            **{n: {m: round(float(per_target[m][mask, col[n]].mean()), 6) if mask.any() else None
                                   for m in metric_names} for n in shown}}
    return out


def save_day(day: dict, path: Path) -> None:
    """The ranks of one replayed day (path.npz) and its sizes and timings (path.json)."""
    path.parent.mkdir(parents=True, exist_ok=True)
    np.savez_compressed(path.with_suffix(".npz"), ranks=day["ranks"], group=day["group"].astype(str))
    path.with_suffix(".json").write_text(json.dumps({"data": day["data"], "seconds": day["seconds"],
                                                     "names": day["names"]}) + "\n", encoding="utf-8")


def load_day(path: Path) -> dict | None:
    if not (path.with_suffix(".npz").exists() and path.with_suffix(".json").exists()):
        return None
    meta = json.loads(path.with_suffix(".json").read_text(encoding="utf-8"))
    with np.load(path.with_suffix(".npz")) as arrays:
        return {**meta, "ranks": arrays["ranks"], "group": arrays["group"]}


def run(events: DataFrame, params: als.ALSParams = als.ALSParams(),
        knn_params: item_knn.KNNParams = item_knn.KNNParams(), n_targets: int = DEFAULT_TARGETS,
        validation_day: date = VALID_START, test_day: date = TEST_START, half_lives=HALF_LIVES,
        alphas=ALPHAS, ks=KS, resamples: int = bootstrap.DEFAULT_RESAMPLES,
        cache_dir: str | None = None, only: str | None = None, mode: str = "cart") -> dict:
    """Both days and the report. With cache_dir, a day already saved there is read back instead of
    replayed, and a replayed day is saved; only = "validation" or "test" replays that day alone and
    returns its sizes and timings (one Spark process per day keeps the shuffle files of a single day
    on disk). mode = "cart" or "new" (TARGET_MODES); "new" has no "+self" variants, since it
    leaves the user:recent products out of every list."""
    if mode not in TARGET_MODES:
        raise ValueError(f"mode must be one of {TARGET_MODES}, not {mode!r}")
    started = time.time()
    sims_run = SIMS if mode == "cart" else BASE_SIMS
    targets = (f"one {' or '.join(TARGET_EVENTS)} event per user on the day" if mode == "cart" else
               "one event per user on the day whose product the user has never touched before; every product "
               f"touched before the target is left out of every list (rec:batch: the ALS top {settings.batch_top_n} "
               f"without them, out of the top {NEW_BATCH_POOL}; rec:rt: the top {RT_TOP_N} without them)")
    report = {"protocol": {
        "mode": mode, "validation_day": str(validation_day), "test_day": str(test_day),
        "targets": f"{targets}, {n_targets or 'all'} users",
        "batch": "ALS top %d with scores (rec:batch), Item-kNN top %d (sim:item), pop:global, from the events "
                 "before the day" % (settings.batch_top_n, settings.sim_top_n),
        "speed": {"sim:cooc": f"session co-occurrence, last {settings.session_window} distinct products, from "
                              "the events before the day",
                  "user:recent": f"{settings.recent_n} newest events before the target, if the newest is under "
                                 f"{rk.TTL['user:recent'] // 3600} h old",
                  "user:bought": "products bought before the target, removed from every list",
                  "rec:rt": f"common.scoring.propagate through the similarity, decayed to the target time, top {RT_TOP_N}",
                  "hybrid similarity": f"{HYBRID_ITEM_WEIGHT} * sim:item + {1 - HYBRID_ITEM_WEIGHT} * sim:cooc",
                  "+self": "the same similarity plus sim(i, i) = 1 for the user:recent products, so rec:rt "
                           "also holds the products just viewed"},
        "merge": "common.scoring.merge_scores (min-max per source, alpha * batch + (1 - alpha) * rt), completed "
                 "with pop:global",
        "half_lives": list(half_lives), "alphas": list(alphas), "ks": list(ks),
        "selection": "best ndcg@10 on the validation day",
        "bootstrap": {"resamples": resamples, "level": 0.95, "seed": params.seed, "unit": "target (one per user)"},
    }, "als_params": params.as_dict(), "knn_params": knn_params.as_dict()}

    days = {}
    for name, day in (("validation", validation_day), ("test", test_day)):
        if only and name != only:
            continue
        path = Path(cache_dir) / (name if mode == "cart" else f"{name}-{mode}") if cache_dir else None
        saved = load_day(path) if path else None
        days[name] = saved or replay_day(events, day, params, knn_params, n_targets, half_lives, alphas, ks,
                                         mode, sims_run)
        if path and not saved:
            save_day(days[name], path)
    if only:
        return {"only": only, "data": days[only]["data"], "seconds": days[only]["seconds"]}
    grids = {n: mean_grid(d["names"], metrics_from_ranks(d["ranks"], ks)) for n, d in days.items()}
    chosen = choose(grids["validation"], "ndcg@10", half_lives, alphas, sims_run)
    report["chosen_on_validation"] = chosen
    report["validation"] = {"data": days["validation"]["data"], "grid": grids["validation"],
                            "seconds": days["validation"]["seconds"]}
    report["test"] = {"data": days["test"]["data"],
                      **summarise(days["test"], chosen, ks, resamples, params.seed),
                      "best_on_test": choose(grids["test"], "ndcg@10", half_lives, alphas, sims_run),
                      "grid": grids["test"], "seconds": days["test"]["seconds"]}
    report["seconds_total"] = round(time.time() - started, 1)
    return report


def format_report(report: dict, metric: str = "ndcg@10") -> str:
    lines = [f"| variant | chosen on validation | test {metric} | test hit_rate@10 |", "|---|---|---|---|"]
    cell = lambda c: f"{c['mean']:.4f} [{c['low']:.4f}, {c['high']:.4f}]"
    for name, v in report["test"]["variants"].items():
        lines.append(f"| {name} | {v['variant']} | {cell(v[metric])} | {cell(v['hit_rate@10'])} |")
    for name, d in report["test"]["differences"].items():
        lines.append(f"| {name} | | {cell(d[metric])} | {cell(d['hit_rate@10'])} |")
    return "\n".join(lines)


def main() -> None:
    defaults = als.ALSParams()
    p = argparse.ArgumentParser(description="Offline replay: batch only vs batch + speed, cooc / item / hybrid")
    p.add_argument("--input", default=settings.clean_path, help="clean Parquet from batch/etl.py")
    p.add_argument("--no-s3", action="store_true", help="local paths only, do not load the s3a jars")
    p.add_argument("--report", help="write the results (JSON) to this path")
    p.add_argument("--targets", type=int, default=DEFAULT_TARGETS, help="target users per day, 0 = all")
    p.add_argument("--resamples", type=int, default=bootstrap.DEFAULT_RESAMPLES)
    p.add_argument("--seed", type=int, default=defaults.seed)
    p.add_argument("--cache-dir", help="save each replayed day here, and reuse a day already saved")
    p.add_argument("--only", choices=("validation", "test"), help="replay this day only (needs --cache-dir)")
    p.add_argument("--targets-mode", choices=TARGET_MODES, default="cart",
                   help="cart: a cart or purchase event per user; new: a product the user has never touched")
    args = p.parse_args()
    if args.only and not args.cache_dir:
        p.error("--only needs --cache-dir")

    spark = get_spark("batch-replay", s3=not args.no_s3)
    spark.sparkContext.setLogLevel("WARN")
    checkpoint_dir = tempfile.mkdtemp(prefix="als-checkpoint-")
    spark.sparkContext.setCheckpointDir(checkpoint_dir)
    params = als.ALSParams(seed=args.seed)
    report = run(spark.read.parquet(args.input), params, n_targets=args.targets, resamples=args.resamples,
                 cache_dir=args.cache_dir, only=args.only, mode=args.targets_mode)
    report["input"] = args.input

    print(json.dumps(report, indent=2) if args.only else format_report(report))
    if args.report and not args.only:
        Path(args.report).parent.mkdir(parents=True, exist_ok=True)
        Path(args.report).write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    spark.stop()
    shutil.rmtree(checkpoint_dir, ignore_errors=True)


if __name__ == "__main__":
    main()
