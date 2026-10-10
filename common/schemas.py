"""Data schemas: raw CSV, clean Parquet, and Kafka event messages.

Kafka contract
--------------
- Topic: settings.kafka_topic_events ("events")
- Key:   str(user_id)  (keeps each user's events ordered in one partition)
- Value: UTF-8 JSON produced by Event.to_json(), e.g.
  {"event_time": "2019-10-01T00:00:00Z", "event_type": "view",
   "product_id": 44600062, "category_id": 2103807459595387724,
   "category_code": null, "brand": "shiseido", "price": 35.79,
   "user_id": 541312140, "user_session": "72d76fde-..."}
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Optional

EVENT_TYPES = ("view", "cart", "remove_from_cart", "purchase")

# Column order of the REES46 CSV files.
RAW_COLUMNS = (
    "event_time", "event_type", "product_id", "category_id",
    "category_code", "brand", "price", "user_id", "user_session",
)

# Clean Parquet = raw columns (event_time as timestamp, UTC) + partition column.
CLEAN_PARTITION_COLUMN = "event_date"


@dataclass
class Event:
    event_time: datetime
    event_type: str
    product_id: int
    category_id: int
    category_code: Optional[str]
    brand: Optional[str]
    price: float
    user_id: int
    user_session: Optional[str]

    @staticmethod
    def parse_time(value: str) -> datetime:
        """Accepts REES46 format ('2019-10-01 00:00:00 UTC') or ISO 8601."""
        value = value.strip()
        if value.endswith(" UTC"):
            dt = datetime.strptime(value[:-4], "%Y-%m-%d %H:%M:%S")
        else:
            dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
        return dt.replace(tzinfo=timezone.utc) if dt.tzinfo is None else dt.astimezone(timezone.utc)

    def to_json(self) -> str:
        d = asdict(self)
        d["event_time"] = self.event_time.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        return json.dumps(d, separators=(",", ":"))

    @classmethod
    def from_json(cls, raw: str | bytes) -> "Event":
        d = json.loads(raw)
        d["event_time"] = cls.parse_time(d["event_time"])
        return cls(**d)

    @classmethod
    def from_csv_row(cls, row: dict[str, str]) -> "Event":
        def opt(v: str) -> Optional[str]:
            return v if v not in ("", None) else None

        return cls(
            event_time=cls.parse_time(row["event_time"]),
            event_type=row["event_type"],
            product_id=int(row["product_id"]),
            category_id=int(row["category_id"]),
            category_code=opt(row.get("category_code")),
            brand=opt(row.get("brand")),
            price=float(row["price"]),
            user_id=int(row["user_id"]),
            user_session=opt(row.get("user_session")),
        )

    def kafka_key(self) -> bytes:
        return str(self.user_id).encode()


def spark_event_schema():
    """Spark StructType for the Kafka JSON value (and raw CSV with event_time as string).

    Imported lazily so that non-Spark services (API, UI) do not need pyspark.
    """
    from pyspark.sql.types import (
        DoubleType, LongType, StringType, StructField, StructType,
    )

    return StructType([
        StructField("event_time", StringType(), False),
        StructField("event_type", StringType(), False),
        StructField("product_id", LongType(), False),
        StructField("category_id", LongType(), True),
        StructField("category_code", StringType(), True),
        StructField("brand", StringType(), True),
        StructField("price", DoubleType(), True),
        StructField("user_id", LongType(), False),
        StructField("user_session", StringType(), True),
    ])
