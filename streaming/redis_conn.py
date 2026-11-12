"""One Redis client per Python process (Spark driver or executor worker).

Spark reuses Python workers between tasks, so the connection pool is created
once per worker instead of once per partition or batch.
"""

from __future__ import annotations

import os

_client = None


def get_redis():
    global _client
    if _client is None:
        import redis

        _client = redis.Redis(
            host=os.getenv("REDIS_HOST", "localhost"),
            port=int(os.getenv("REDIS_PORT", "6379")),
            db=int(os.getenv("REDIS_DB", "0")),
            decode_responses=True,
            socket_timeout=30,
            health_check_interval=30,
        )
    return _client
