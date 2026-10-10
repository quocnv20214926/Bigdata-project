"""API response models (the API contract).

Endpoints served by serving/ (Pair 2):
  GET /recommend/{user_id}?k=10    -> RecommendResponse
  GET /similar/{product_id}?k=10   -> SimilarResponse
  GET /trending/{category_id}?k=10 -> TrendingResponse
  GET /health                      -> HealthResponse

Pair 1's fallback API (if needed) must return the same models.
"""

from __future__ import annotations

from datetime import datetime
from typing import Literal, Optional

from pydantic import BaseModel, Field

Source = Literal["batch", "realtime", "both", "trending", "popular"]


class RecommendedItem(BaseModel):
    product_id: int
    score: float
    source: Source
    category_id: Optional[int] = None
    category_code: Optional[str] = None
    brand: Optional[str] = None
    price: Optional[float] = None


class RecommendResponse(BaseModel):
    user_id: int
    items: list[RecommendedItem]
    generated_at: datetime
    batch_available: bool = Field(description="False when rec:batch is missing for this user")
    realtime_available: bool = Field(description="False when rec:rt is missing for this user")


class SimilarResponse(BaseModel):
    product_id: int
    items: list[RecommendedItem]
    similarity: Literal["item", "cooc", "hybrid"]


class TrendingResponse(BaseModel):
    category_id: Optional[int]
    items: list[RecommendedItem]
    window_minutes: int = 60


class HealthResponse(BaseModel):
    status: Literal["ok", "degraded"]
    redis: bool
    batch_last_run: Optional[datetime] = None
