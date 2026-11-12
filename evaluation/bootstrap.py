"""Bootstrap confidence intervals for metrics averaged over users (Pair 1, week 7).

Each evaluated user contributes one value per metric (evaluation/metrics.per_user_metrics). The
reported number is the mean over users; its 95% interval comes from resampling the users with
replacement `resamples` times and taking the 2.5th and 97.5th percentiles of the resampled means
(percentile bootstrap). Two models scored on the same users are compared on the per-user
differences, resampled the same way (paired bootstrap): the difference is clear when its interval
does not contain 0.
"""

from __future__ import annotations

import numpy as np

DEFAULT_RESAMPLES = 1000


def mean_ci(values, resamples: int = DEFAULT_RESAMPLES, seed: int = 42,
            level: float = 0.95) -> list[dict]:
    """values: (users, metrics) array. Returns one {mean, low, high} per metric (column)."""
    x = np.asarray(values, dtype=np.float64)
    if x.ndim == 1:
        x = x[:, None]
    n, m = x.shape
    if n == 0:
        return [{"mean": None, "low": None, "high": None} for _ in range(m)]
    rng = np.random.default_rng(seed)
    means = np.empty((resamples, m))
    for i in range(resamples):
        # how many times each user is drawn: a weighted mean avoids copying the rows
        counts = np.bincount(rng.integers(0, n, size=n), minlength=n)
        means[i] = counts @ x / n
    tail = (1 - level) / 2 * 100
    low, high = np.percentile(means, [tail, 100 - tail], axis=0)
    return [{"mean": round(float(mu), 6), "low": round(float(a), 6), "high": round(float(b_), 6)}
            for mu, a, b_ in zip(x.mean(axis=0), low, high)]


def paired_diff_ci(a, b, resamples: int = DEFAULT_RESAMPLES, seed: int = 42,
                   level: float = 0.95) -> list[dict]:
    """Interval of mean(a - b) per metric; a and b hold the same users in the same order."""
    a, b = np.asarray(a, dtype=np.float64), np.asarray(b, dtype=np.float64)
    if a.shape != b.shape:
        raise ValueError(f"paired arrays differ in shape: {a.shape} vs {b.shape}")
    return mean_ci(a - b, resamples, seed, level)
