"""Batch: bronze clickstream -> silver sessions (Spark).

One row per browsing session with its funnel counts. Runs every 30 minutes
from Airflow over the last N days, so sessions that span a run boundary (or
whose events arrived late) are simply recomputed.

Idempotent by construction: dynamic partition overwrite replaces only the
session_date partitions this run computed.

    python -m cartpulse.batch.sessionize --days 2
"""
from __future__ import annotations

import argparse
import json
import os
import time

from pyspark.sql import DataFrame, SparkSession, functions as F
from pyspark.sql.utils import AnalysisException

from cartpulse import config

os.environ["TZ"] = "UTC"
time.tzset()

LATE_AFTER_SECONDS = 60


def spark_session() -> SparkSession:
    spark = (SparkSession.builder.appName("cartpulse-sessionize")
             .master("local[2]")
             .config("spark.driver.memory", "1g")
             .config("spark.sql.shuffle.partitions", "4")
             .config("spark.sql.session.timeZone", "UTC")
             .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
             .config("spark.ui.enabled", "false")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    return spark


def sessionize(events: DataFrame) -> DataFrame:
    e = events.dropDuplicates(["event_id"])          # bronze is at-least-once upstream of the file sink
    count = lambda kind: F.sum(F.when(F.col("event_type") == kind, 1).otherwise(0)).cast("int")  # noqa: E731
    return (e.groupBy("session_id")
            .agg(F.max("customer_id").alias("customer_id"),        # set once a guest logs in
                 F.min("event_time").alias("started_at"),
                 F.max("event_time").alias("ended_at"),
                 F.count(F.lit(1)).cast("int").alias("events"),
                 count("page_view").alias("page_views"),
                 count("product_view").alias("product_views"),
                 count("add_to_cart").alias("add_to_carts"),
                 count("begin_checkout").alias("checkouts"),
                 count("purchase").alias("purchases"),
                 F.sum(F.when(F.col("ingested_at").cast("long") - F.col("event_time").cast("long") > LATE_AFTER_SECONDS, 1)
                       .otherwise(0)).cast("int").alias("late_events"),
                 F.first("device", ignorenulls=True).alias("device"),
                 F.first("traffic_source", ignorenulls=True).alias("traffic_source"),
                 F.min_by("page", "event_time").alias("landing_page"))
            .withColumn("duration_s", (F.col("ended_at").cast("long") - F.col("started_at").cast("long")).cast("int"))
            # Business date in South African time, not UTC.
            .withColumn("session_date", F.to_date(F.from_utc_timestamp("started_at", "Africa/Johannesburg"))))


def main(argv=None) -> dict:
    parser = argparse.ArgumentParser()
    parser.add_argument("--days", type=int, default=2)
    args = parser.parse_args(argv)

    spark = spark_session()
    src = str(config.BRONZE / "clickstream")
    try:
        bronze = spark.read.parquet(src)      # honours the stream's _spark_metadata commit log
    except AnalysisException:
        print(json.dumps({"sessions": 0, "note": "no clickstream landed yet"}))
        return {"sessions": 0}
    recent = bronze.where(F.col("event_date") >= F.date_sub(F.current_date(), args.days + 1))
    sessions = sessionize(recent).where(F.col("session_date") >= F.date_sub(F.current_date(), args.days)).cache()
    (sessions.write.mode("overwrite").partitionBy("session_date")
        .parquet(str(config.SILVER / "sessions")))
    summary = {"sessions": sessions.count(),
               "purchasing_sessions": sessions.where("purchases > 0").count(),
               "late_events": sessions.agg(F.sum("late_events")).first()[0] or 0}
    spark.stop()
    print(json.dumps(summary))
    return summary


if __name__ == "__main__":
    main()
