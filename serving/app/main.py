"""Recommendation API (FastAPI).

Endpoints (contract: common/models.py):
  GET /recommend/{user_id}?k=10
  GET /similar/{product_id}?k=10
  GET /trending?k=10                (global)
  GET /trending/{category_id}?k=10
  GET /health

API_MODE=mock  (default, week 2): fake data from MockRepository.
API_MODE=redis (week 5): RedisRepository, not implemented yet.

Run locally from the repository root:
  uvicorn serving.app.main:app --reload --port 8000
then open http://localhost:8000/docs
"""

from __future__ import annotations

import os
import time
from contextlib import asynccontextmanager

from fastapi import FastAPI, Query, Request

from common import CONTRACT_VERSION
from common.config import settings
from common.models import HealthResponse, RecommendResponse, SimilarResponse, TrendingResponse
from serving.app.repository import MockRepository, Repository

API_MODE = os.getenv("API_MODE", "mock").lower()
K_QUERY = Query(10, ge=1, le=50, description="number of items to return")


def build_repository(redis_client) -> Repository:
    if API_MODE == "mock":
        return MockRepository(redis_ping=redis_client.ping if redis_client else None)
    raise RuntimeError(f"API_MODE={API_MODE!r} is not implemented yet (RedisRepository arrives in week 5)")


@asynccontextmanager
async def lifespan(app: FastAPI):
    redis_client = None
    try:
        import redis.asyncio as aioredis

        redis_client = aioredis.Redis(
            host=settings.redis_host, port=settings.redis_port, db=settings.redis_db,
            socket_connect_timeout=0.5, socket_timeout=0.5,
        )
    except ImportError:
        pass
    app.state.repo = build_repository(redis_client)
    yield
    if redis_client is not None:
        await redis_client.aclose()


app = FastAPI(
    title="E-commerce Recommendation API",
    version=CONTRACT_VERSION,
    description=f"Lambda Architecture recommender. Mode: **{API_MODE}**.",
    lifespan=lifespan,
)


@app.middleware("http")
async def add_timing_headers(request: Request, call_next):
    start = time.perf_counter()
    response = await call_next(request)
    response.headers["X-Process-Time-Ms"] = f"{(time.perf_counter() - start) * 1000:.2f}"
    response.headers["X-API-Mode"] = API_MODE
    return response


def repo(request: Request) -> Repository:
    return request.app.state.repo


@app.get("/recommend/{user_id}", response_model=RecommendResponse, tags=["recommendations"])
async def recommend(user_id: int, request: Request, k: int = K_QUERY):
    return await repo(request).recommend(user_id, k)


@app.get("/similar/{product_id}", response_model=SimilarResponse, tags=["recommendations"])
async def similar(product_id: int, request: Request, k: int = K_QUERY):
    return await repo(request).similar(product_id, k)


@app.get("/trending", response_model=TrendingResponse, tags=["trending"])
async def trending_global(request: Request, k: int = K_QUERY):
    return await repo(request).trending(None, k)


@app.get("/trending/{category_id}", response_model=TrendingResponse, tags=["trending"])
async def trending_category(category_id: int, request: Request, k: int = K_QUERY):
    return await repo(request).trending(category_id, k)


@app.get("/health", response_model=HealthResponse, tags=["ops"])
async def health(request: Request):
    return await repo(request).health()
