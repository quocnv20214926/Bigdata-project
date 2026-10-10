"""Runtime configuration shared by all services.

Values are read from environment variables so the same code runs locally
and inside Docker Compose. Defaults match the service names in infra/.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field


def _env(name: str, default: str) -> str:
    return os.getenv(name, default)


def _env_int(name: str, default: int) -> int:
    return int(os.getenv(name, str(default)))


@dataclass(frozen=True)
class Settings:
    # Redis (serving store)
    redis_host: str = field(default_factory=lambda: _env("REDIS_HOST", "localhost"))
    redis_port: int = field(default_factory=lambda: _env_int("REDIS_PORT", 6379))
    redis_db: int = field(default_factory=lambda: _env_int("REDIS_DB", 0))

    # Kafka (event stream)
    kafka_bootstrap: str = field(default_factory=lambda: _env("KAFKA_BOOTSTRAP", "localhost:9092"))
    kafka_topic_events: str = field(default_factory=lambda: _env("KAFKA_TOPIC_EVENTS", "events"))

    # MinIO / S3 (data lake)
    s3_endpoint: str = field(default_factory=lambda: _env("S3_ENDPOINT", "http://localhost:9000"))
    s3_access_key: str = field(default_factory=lambda: _env("S3_ACCESS_KEY", "minioadmin"))
    s3_secret_key: str = field(default_factory=lambda: _env("S3_SECRET_KEY", "minioadmin"))
    raw_path: str = field(default_factory=lambda: _env("RAW_PATH", "s3a://lake/raw/"))
    clean_path: str = field(default_factory=lambda: _env("CLEAN_PATH", "s3a://lake/clean/events/"))

    # Sizes of stored lists (part of the Redis contract)
    batch_top_n: int = 100      # items per user in rec:batch
    sim_top_n: int = 50         # neighbours per item in sim:item / sim:cooc
    recent_n: int = 20          # events per user in user:recent
    session_window: int = 10    # previous items in a session used for co-occurrence


settings = Settings()
