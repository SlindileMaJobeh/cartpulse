"""Lake -> warehouse raw tables: sessions (silver), CDC events and abandoned carts (bronze)."""
from __future__ import annotations

import json
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
import pyarrow.dataset as ds

from cartpulse import config
from cartpulse.batch import warehouse

SESSION_COLS = ["session_id", "customer_id", "session_date", "started_at", "ended_at", "duration_s", "events",
                "page_views", "product_views", "add_to_carts", "checkouts", "purchases", "late_events", "device",
                "traffic_source", "landing_page"]
CDC_COLS = ["topic", "kafka_partition", "kafka_offset", "table_name", "op", "ts_ms", "before_json", "after_json"]


def read_partitioned(path: Path, date_col: str, since: date) -> pd.DataFrame:
    """Read a hive-partitioned Parquet folder, only partitions on/after `since`."""
    if not path.exists() or not any(path.iterdir()):
        return pd.DataFrame()
    dataset = ds.dataset(str(path), format="parquet", partitioning="hive")
    if date_col not in dataset.schema.names:
        return pd.DataFrame()
    table = dataset.to_table(filter=ds.field(date_col).cast("string") >= since.isoformat())
    return table.to_pandas()


def load_sessions(days: int = 2) -> int:
    df = read_partitioned(config.SILVER / "sessions", "session_date", date.today() - timedelta(days=days))
    if df.empty:
        return 0
    df["session_date"] = pd.to_datetime(df["session_date"].astype(str)).dt.date
    for col in ("started_at", "ended_at"):
        df[col] = pd.to_datetime(df[col], utc=True)
    df["customer_id"] = df["customer_id"].astype("Int64")
    with warehouse.connect() as conn:
        # Sessions still in progress at the last run get updated, so upsert rather than ignore.
        return warehouse.upsert(conn, df[SESSION_COLS], "raw.sessions", ["session_id"], update=True)


def load_cdc(days: int = 2) -> int:
    df = read_partitioned(config.BRONZE / "cdc", "ingest_date", date.today() - timedelta(days=days))
    if df.empty:
        return 0
    df = df[CDC_COLS].copy()
    df["table_name"] = df["table_name"].astype(str)          # hive partition column arrives as a category
    for col in ("before_json", "after_json"):      # JSONB columns: COPY needs valid JSON or NULL
        df[col] = df[col].map(lambda v: None if v is None or v == "null" else json.dumps(json.loads(v)))
    with warehouse.connect() as conn:
        return warehouse.upsert(conn, df, "raw.cdc_events", ["topic", "kafka_partition", "kafka_offset"])


def load_abandoned() -> int:
    path = config.BRONZE / "abandoned_carts"
    if not path.exists():
        return 0
    df = ds.dataset(str(path), format="parquet").to_table().to_pandas()
    if df.empty:
        return 0
    for col in ("last_activity_at", "detected_at"):
        df[col] = pd.to_datetime(df[col], utc=True)
    df["customer_id"] = df["customer_id"].astype("Int64")
    with warehouse.connect() as conn:
        return warehouse.upsert(conn, df, "raw.abandoned_carts", ["session_id"])
