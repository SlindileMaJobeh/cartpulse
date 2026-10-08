"""CartPulse speed layer: one Spark application, four streaming queries.

  shop.clickstream ──┬─► [bronze]     raw events -> Parquet lake (exactly-once file sink)
                     ├─► [funnel]     1-minute event-time windows, 2-minute watermark -> Redis
                     └─► [carts]      stateful per-session cart tracking -> abandoned-cart alerts
  shop.public.* CDC ─────► [cdc]      change events -> Parquet lake + live order/stock views in Redis

Run inside Docker:  python -m cartpulse.streaming.app
"""
from __future__ import annotations

import json
import os
import time
from datetime import datetime, timezone

import pandas as pd
import redis
from pyspark.sql import DataFrame, SparkSession, functions as F
from pyspark.sql.streaming.state import GroupState, GroupStateTimeout
from pyspark.sql.types import (DoubleType, IntegerType, LongType, StringType, StructField, StructType,
                               TimestampType)

from cartpulse import config
from cartpulse.streaming import cart_state, live_store

os.environ["TZ"] = "UTC"      # Spark hands Python naive datetimes in the process time zone
time.tzset()

WATERMARK = "2 minutes"
WINDOW = "1 minute"

CLICK_SCHEMA = StructType([
    StructField("event_id", StringType()),
    StructField("event_type", StringType()),
    StructField("session_id", StringType()),
    StructField("customer_id", LongType()),
    StructField("device", StringType()),
    StructField("traffic_source", StringType()),
    StructField("event_time", StringType()),
    StructField("page", StringType()),
    StructField("product_id", LongType()),
    StructField("quantity", IntegerType()),
    StructField("price", DoubleType()),
    StructField("cart_value", DoubleType()),
    StructField("order_id", LongType()),
    StructField("items", IntegerType()),
])

ABANDONED_SCHEMA = StructType([
    StructField("session_id", StringType()),
    StructField("customer_id", LongType()),
    StructField("cart_value", DoubleType()),
    StructField("items", IntegerType()),
    StructField("last_activity_at", TimestampType()),
    StructField("detected_at", TimestampType()),
])
CART_STATE_SCHEMA = StructType([StructField("cart_json", StringType())])


def redis_client():
    return redis.Redis.from_url(config.REDIS_URL, decode_responses=True)


def spark_session(kafka: bool = True) -> SparkSession:
    builder = (SparkSession.builder.appName("cartpulse-stream")
               .master(os.environ.get("SPARK_MASTER", "local[2]"))
               .config("spark.sql.shuffle.partitions", "4")
               .config("spark.sql.session.timeZone", "UTC")
               .config("spark.jars.ivy", config.SPARK_IVY_DIR)
               .config("spark.ui.showConsoleProgress", "false")
               # Bound the state store: RocksDB keeps per-session cart state off the JVM heap.
               .config("spark.sql.streaming.stateStore.providerClass",
                       "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider"))
    if kafka:
        builder = builder.config("spark.jars.packages", config.PKG_KAFKA)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


# ============================================================== sources
def clickstream_source(spark, source_dir: str | None = None) -> DataFrame:
    """Kafka in production; a folder of JSON-lines files in tests (same parsing either way)."""
    if source_dir:
        raw = (spark.readStream.schema(CLICK_SCHEMA).option("maxFilesPerTrigger", 1).json(source_dir)
               .select(F.to_json(F.struct("*")).alias("value"),
                       F.lit(0).alias("kafka_partition"), F.lit(-1).cast("long").alias("kafka_offset")))
    else:
        raw = (spark.readStream.format("kafka")
               .option("kafka.bootstrap.servers", config.KAFKA_BOOTSTRAP)
               .option("subscribe", config.CLICKSTREAM_TOPIC)
               .option("startingOffsets", "earliest")
               .option("maxOffsetsPerTrigger", 20000)
               .option("failOnDataLoss", "false")
               .load()
               .select(F.col("value").cast("string").alias("value"),
                       F.col("partition").alias("kafka_partition"), F.col("offset").alias("kafka_offset")))
    return parse_clicks(raw)


def parse_clicks(raw: DataFrame) -> DataFrame:
    return (raw.select(F.from_json("value", CLICK_SCHEMA).alias("e"), "kafka_partition", "kafka_offset")
            .select("e.*", "kafka_partition", "kafka_offset")
            .where(F.col("event_id").isNotNull() & F.col("session_id").isNotNull())
            .withColumn("event_time", F.to_timestamp("event_time"))
            .where(F.col("event_time").isNotNull())
            .withColumn("event_date", F.to_date("event_time"))
            .withColumn("ingested_at", F.current_timestamp()))


def cdc_source(spark) -> DataFrame:
    raw = (spark.readStream.format("kafka")
           .option("kafka.bootstrap.servers", config.KAFKA_BOOTSTRAP)
           .option("subscribePattern", config.CDC_TOPIC_PATTERN)
           .option("startingOffsets", "earliest")
           .option("failOnDataLoss", "false")
           .load())
    return parse_cdc(raw)


def parse_cdc(raw: DataFrame) -> DataFrame:
    """Debezium envelope -> one row per change. Row images stay JSON; dbt types them per table."""
    v = F.col("value").cast("string")
    return (raw.where(F.col("value").isNotNull())
            .select(F.col("topic"),
                    F.col("partition").alias("kafka_partition"),
                    F.col("offset").alias("kafka_offset"),
                    F.get_json_object(v, "$.source.table").alias("table_name"),
                    F.get_json_object(v, "$.op").alias("op"),
                    F.get_json_object(v, "$.source.ts_ms").cast("long").alias("ts_ms"),
                    F.get_json_object(v, "$.before").alias("before_json"),
                    F.get_json_object(v, "$.after").alias("after_json"))
            .withColumn("ingest_date", F.current_date()))


# ============================================================== queries
def bronze_query(clicks: DataFrame, lake: str, checkpoints: str, trigger: dict):
    """File sink + checkpoint = exactly-once: Spark's _spark_metadata log records which
    files belong to committed batches, so readers never see a half-written batch."""
    return (clicks.writeStream.queryName("bronze_clicks")
            .format("parquet")
            .partitionBy("event_date")
            .option("path", f"{lake}/bronze/clickstream")
            .option("checkpointLocation", f"{checkpoints}/bronze_clicks")
            .trigger(**trigger)
            .start())


def funnel_frame(clicks: DataFrame) -> DataFrame:
    """Each event counts toward its own stage and toward 'any' (= active sessions)."""
    return (clicks.withWatermark("event_time", WATERMARK)
            .withColumn("stage", F.explode(F.array(F.col("event_type"), F.lit("any"))))
            .groupBy(F.window("event_time", WINDOW), "stage")
            .agg(F.count(F.lit(1)).alias("events"), F.approx_count_distinct("session_id").alias("sessions")))


def funnel_query(clicks: DataFrame, checkpoints: str, trigger: dict):
    def sink(batch: DataFrame, batch_id: int) -> None:
        rows = [(r["window"]["start"], r["stage"], r["events"], r["sessions"]) for r in batch.collect()]
        n = live_store.write_funnel(redis_client(), rows)
        print(f"[funnel {batch_id}] {n} window/stage cells updated", flush=True)

    return (funnel_frame(clicks).writeStream.queryName("funnel")
            .outputMode("update")                 # emit only windows that changed; late events update them
            .foreachBatch(sink)
            .option("checkpointLocation", f"{checkpoints}/funnel")
            .trigger(**trigger)
            .start())


def track_carts(key, pdfs, state: GroupState):
    """applyInPandasWithState: called per session with its new events, or on timeout."""
    abandon_ms = int(config.ABANDON_AFTER_MINUTES * 60_000)
    if state.hasTimedOut:
        cart = json.loads(state.get[0])
        state.remove()
        if cart_state.is_abandoned(cart):
            yield pd.DataFrame([{
                "session_id": key[0],
                "customer_id": cart["customer_id"],
                "cart_value": cart_state.cart_value(cart),
                "items": sum(q for q, _ in cart["items"].values()),
                "last_activity_at": pd.Timestamp(cart["last_activity_ms"], unit="ms"),
                "detected_at": pd.Timestamp(datetime.now(timezone.utc).replace(tzinfo=None)),
            }])
        return

    cart = json.loads(state.get[0]) if state.exists else cart_state.new_cart()
    for pdf in pdfs:
        # Unit-safe epoch millis (Arrow may hand us datetime64[us] or [ns]).
        events = pdf.assign(event_time_ms=(pdf["event_time"] - pd.Timestamp(0)) // pd.Timedelta(milliseconds=1))
        events = events.astype(object).where(events.notna(), None)
        cart = cart_state.apply_events(cart, events.to_dict("records"))
    state.update((json.dumps(cart),))
    # Converted sessions keep a marker until timeout so a late add_to_cart can't revive them.
    state.setTimeoutTimestamp(cart_state.timeout_ms(cart, abandon_ms, state.getCurrentWatermarkMs()))


def carts_frame(clicks: DataFrame) -> DataFrame:
    return (clicks.withWatermark("event_time", WATERMARK)
            .select("session_id", "customer_id", "event_type", "product_id", "quantity", "price", "event_time")
            .groupBy("session_id")
            .applyInPandasWithState(track_carts, ABANDONED_SCHEMA, CART_STATE_SCHEMA,
                                    outputMode="append", timeoutConf=GroupStateTimeout.EventTimeTimeout))


def carts_query(clicks: DataFrame, lake: str, checkpoints: str, trigger: dict):
    def sink(batch: DataFrame, batch_id: int) -> None:
        rows = [r.asDict() for r in batch.collect()]
        if not rows:
            return
        batch.write.mode("append").parquet(f"{lake}/bronze/abandoned_carts")
        n = live_store.write_abandoned(redis_client(), rows)
        print(f"[carts {batch_id}] {n} abandoned carts", flush=True)

    return (carts_frame(clicks).writeStream.queryName("abandoned_carts")
            .outputMode("append")
            .foreachBatch(sink)
            .option("checkpointLocation", f"{checkpoints}/carts")
            .trigger(**trigger)
            .start())


def cdc_query(spark, lake: str, checkpoints: str, trigger: dict):
    def apply_partition(rows):
        live_store.apply_cdc(redis_client(), (r.asDict() for r in rows), config.LOW_STOCK_THRESHOLD)

    def sink(batch: DataFrame, batch_id: int) -> None:
        if batch.isEmpty():
            return
        batch = batch.persist()
        batch.write.mode("append").partitionBy("table_name", "ingest_date").parquet(f"{lake}/bronze/cdc")
        # A Kafka partition maps to one Spark partition, so each table's changes apply in commit order.
        batch.foreachPartition(apply_partition)
        ops = {f"{r['table_name']}.{r['op']}": r["count"] for r in batch.groupBy("table_name", "op").count().collect()}
        print(f"[cdc {batch_id}] {ops}", flush=True)
        batch.unpersist()

    return (cdc_source(spark).writeStream.queryName("cdc")
            .foreachBatch(sink)
            .option("checkpointLocation", f"{checkpoints}/cdc")
            .trigger(**trigger)
            .start())


def main() -> None:
    spark = spark_session(kafka=True)
    lake, checkpoints = str(config.LAKE), str(config.CHECKPOINTS)
    clicks = clickstream_source(spark)
    cdc_query(spark, lake, checkpoints, {"processingTime": "5 seconds"})
    bronze_query(clicks, lake, checkpoints, {"processingTime": "30 seconds"})
    funnel_query(clicks, checkpoints, {"processingTime": "5 seconds"})
    carts_query(clicks, lake, checkpoints, {"processingTime": "10 seconds"})
    print("CartPulse streaming: 4 queries running", flush=True)
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
