"""Deterministic fake catalogue for the mock API (week 2).

Product attributes mimic REES46 (no names or images: category, brand, price).
The same user/product/category id always returns the same answer, so the UI
and tests can rely on it.
"""

from __future__ import annotations

import random
from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class Product:
    product_id: int
    category_id: int
    category_code: Optional[str]
    brand: Optional[str]
    price: float


_CATEGORIES = [
    # (category_id, category_code, brands, price range)
    (2053013555631882655, "electronics.smartphone", ["samsung", "apple", "xiaomi", "huawei", "oppo"], (90, 1500)),
    (2053013554658804075, "electronics.audio.headphone", ["apple", "sony", "jbl", "samsung"], (10, 350)),
    (2053013553559896355, "computers.notebook", ["lenovo", "hp", "acer", "asus", "apple"], (250, 2500)),
    (2053013565983425517, "appliances.environment.vacuum", ["samsung", "philips", "bosch"], (40, 600)),
    (2053013563810775923, "appliances.kitchen.washer", ["lg", "samsung", "bosch", "indesit"], (180, 900)),
    (2053013558920217191, "computers.desktop", ["hp", "dell", "lenovo"], (300, 1800)),
    (2053013552326770905, "apparel.shoes", ["nike", "adidas", "puma"], (25, 200)),
    (2053013559792632471, "furniture.living_room.sofa", [None, "brw"], (150, 1200)),
]

PRODUCTS_PER_CATEGORY = 15


def _build_catalogue() -> list[Product]:
    rng = random.Random(46)
    products: list[Product] = []
    next_id = 1_000_000
    for cat_id, code, brands, (lo, hi) in _CATEGORIES:
        for _ in range(PRODUCTS_PER_CATEGORY):
            next_id += rng.randint(1, 5000)
            products.append(Product(next_id, cat_id, code, rng.choice(brands), round(rng.uniform(lo, hi), 2)))
    return products


CATALOGUE: list[Product] = _build_catalogue()
BY_ID: dict[int, Product] = {p.product_id: p for p in CATALOGUE}
BY_CATEGORY: dict[int, list[Product]] = {}
for _p in CATALOGUE:
    BY_CATEGORY.setdefault(_p.category_id, []).append(_p)
CATEGORY_IDS: list[int] = list(BY_CATEGORY)


def sample_products(seed: int, k: int, pool: Optional[list[Product]] = None,
                    exclude: Optional[int] = None) -> list[tuple[Product, float]]:
    """k distinct products with descending scores in (0, 1], fixed for a given seed."""
    rng = random.Random(seed)
    pool = [p for p in (pool or CATALOGUE) if p.product_id != exclude]
    chosen = rng.sample(pool, min(k, len(pool)))
    scores = sorted((round(rng.uniform(0.05, 1.0), 4) for _ in chosen), reverse=True)
    return list(zip(chosen, scores))
