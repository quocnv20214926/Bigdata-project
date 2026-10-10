"""Event weights shared by the batch layer (ALS input) and the speed layer
(real-time score propagation). Both pipelines must use these values so that
a "cart" means the same thing everywhere.
"""

from __future__ import annotations

import math

EVENT_WEIGHTS: dict[str, float] = {
    "view": 1.0,
    "cart": 3.0,
    "remove_from_cart": 0.0,  # only present in the cosmetics dataset; ignored
    "purchase": 5.0,
}

# Default ALS confidence scaling; the final value is chosen by grid search.
DEFAULT_ALS_ALPHA = 10.0


def event_weight(event_type: str) -> float:
    """Weight of a single event; unknown event types count as 0."""
    return EVENT_WEIGHTS.get(event_type, 0.0)


def als_confidence(interaction_score: float, alpha: float = DEFAULT_ALS_ALPHA) -> float:
    """Implicit ALS confidence: 1 + alpha * log(1 + summed event weights)."""
    return 1.0 + alpha * math.log1p(max(interaction_score, 0.0))
