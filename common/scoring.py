"""Scoring functions shared by the speed layer, the API, and offline evaluation.

Keeping them here guarantees that evaluation measures exactly the logic that
runs in production.
"""

from __future__ import annotations

import math
from typing import Iterable, Mapping

from common.weights import event_weight

DEFAULT_HALF_LIFE_SECONDS = 30 * 60   # real-time interest halves every 30 minutes
DEFAULT_BATCH_WEIGHT = 0.5            # alpha in: alpha * batch + (1 - alpha) * rt


# ---- Speed layer ---------------------------------------------------------

def time_decay(age_seconds: float, half_life_seconds: float = DEFAULT_HALF_LIFE_SECONDS) -> float:
    """exp(-lambda * dt) with lambda = ln(2) / half_life."""
    if age_seconds <= 0:
        return 1.0
    return math.exp(-math.log(2) * age_seconds / half_life_seconds)


def cooc_similarity(cooc_count: float, count_i: float, count_j: float) -> float:
    """Normalized co-occurrence: cooc(i, j) / sqrt(cnt(i) * cnt(j))."""
    if count_i <= 0 or count_j <= 0:
        return 0.0
    return cooc_count / math.sqrt(count_i * count_j)


def propagate(
    event_type: str,
    neighbours: Mapping[int, float],
    age_seconds: float = 0.0,
    half_life_seconds: float = DEFAULT_HALF_LIFE_SECONDS,
) -> dict[int, float]:
    """Score increments for an event on item i.

    rt[j] += w_e * sim(i, j) * exp(-lambda * dt)   for j in neighbours(i)
    """
    factor = event_weight(event_type) * time_decay(age_seconds, half_life_seconds)
    if factor == 0.0:
        return {}
    return {j: factor * s for j, s in neighbours.items() if s > 0}


# ---- Serving -------------------------------------------------------------

def minmax_normalize(scores: Mapping[int, float]) -> dict[int, float]:
    if not scores:
        return {}
    lo, hi = min(scores.values()), max(scores.values())
    if hi == lo:
        return {k: 1.0 for k in scores}
    return {k: (v - lo) / (hi - lo) for k, v in scores.items()}


def merge_scores(
    batch: Mapping[int, float],
    realtime: Mapping[int, float],
    alpha: float = DEFAULT_BATCH_WEIGHT,
    exclude: Iterable[int] = (),
    k: int = 10,
) -> list[tuple[int, float, str]]:
    """Combine batch and real-time scores into a top-k list.

    Degrades gracefully: if one source is empty, the other gets full weight.
    Returns (product_id, score, source) with source in {"batch", "realtime", "both"}.
    """
    if not batch:
        alpha = 0.0
    elif not realtime:
        alpha = 1.0

    b, r = minmax_normalize(batch), minmax_normalize(realtime)
    excluded = set(exclude)
    merged: list[tuple[int, float, str]] = []
    for pid in set(b) | set(r):
        if pid in excluded:
            continue
        score = alpha * b.get(pid, 0.0) + (1 - alpha) * r.get(pid, 0.0)
        source = "both" if pid in b and pid in r else ("batch" if pid in b else "realtime")
        merged.append((pid, score, source))
    merged.sort(key=lambda x: x[1], reverse=True)
    return merged[:k]
