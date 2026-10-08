"""All settings come from environment variables (set in docker-compose.yml).
Defaults point at localhost so the code also runs outside Docker."""
import os
from pathlib import Path


def _env(name: str, default: str) -> str:
    return os.environ.get(name, default)


SHOP_DSN = _env("SHOP_DSN", "postgresql://shop:shop@localhost:5432/shop")
WAREHOUSE_DSN = _env("WAREHOUSE_DSN", "postgresql://loader:loader@localhost:5432/warehouse")
API_DB_DSN = _env("API_DB_DSN", "postgresql://api_reader:api_reader@localhost:5432/warehouse")
KAFKA_BOOTSTRAP = _env("KAFKA_BOOTSTRAP", "localhost:19092")
CONNECT_URL = _env("CONNECT_URL", "http://localhost:8083")
REDIS_URL = _env("REDIS_URL", "redis://localhost:6379/0")

CLICKSTREAM_TOPIC = _env("CLICKSTREAM_TOPIC", "shop.clickstream")
CDC_TOPIC_PATTERN = _env("CDC_TOPIC_PATTERN", r"shop\.public\..*")

ABANDON_AFTER_MINUTES = float(_env("ABANDON_AFTER_MINUTES", "5"))
LOW_STOCK_THRESHOLD = int(_env("LOW_STOCK_THRESHOLD", "10"))

DATA_DIR = Path(_env("DATA_DIR", "./data")).resolve()
LAKE = DATA_DIR / "lake"
BRONZE = LAKE / "bronze"
SILVER = LAKE / "silver"
QUARANTINE = LAKE / "quarantine"
INBOX = DATA_DIR / "inbox"                 # partner file drops (courier)
ARCHIVE = DATA_DIR / "archive"             # files after ingestion
CHECKPOINTS = DATA_DIR / "checkpoints"     # Spark streaming state + offsets
SPARK_IVY_DIR = _env("SPARK_IVY_DIR", str(DATA_DIR / ".ivy"))

# Maven package Spark downloads on first start (matches PySpark 3.5.3 / Scala 2.12).
PKG_KAFKA = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3"

ALL_DIRS = [BRONZE, SILVER, QUARANTINE, INBOX / "courier", ARCHIVE / "courier", CHECKPOINTS]
