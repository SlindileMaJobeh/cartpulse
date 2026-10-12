# CartPulse design notes

## 1. Problem

An online store needs two kinds of answers:

| Question | How fresh | How complete | Layer |
|---|---|---|---|
| How much have we sold today? Which carts were just abandoned? What's about to run out? | seconds | approximate is fine | speed (stream → Redis) |
| Did month-end payday lift sales? Which courier misses SLA? Who are our best customers? | 30 min | must be complete, reconciled and rebuildable | batch (lake → warehouse → dbt) |

Both layers read the same immutable bronze data, so the batch layer can always
recompute what the speed layer estimated. That is a Lambda-style design: kept
deliberately small, but with the real seams in place.

## 2. Data flow

### Clickstream (event stream)
Simulator → `shop.clickstream` (3 partitions, **keyed by session_id** so one
session's events stay ordered) → one Spark app running three queries over the
same topic:

1. **bronze_clicks:** a Parquet file sink partitioned by `event_date`. The
   checkpoint and the `_spark_metadata` commit log make this exactly-once:
   readers only see files from committed batches.
2. **funnel:** `withWatermark(event_time, 2 min)` → `groupBy(window 1 min,
   stage)` → events + `approx_count_distinct(session_id)`. Each event is
   exploded into its own stage plus `any`, so active sessions come from the
   same aggregation. Output mode `update`. The Redis writes are absolute
   `HSET`s, so a replay or a late-event update simply overwrites.
3. **abandoned_carts:** `groupBy(session_id).applyInPandasWithState(...)` with
   `EventTimeTimeout`. The state is the cart. Each event pushes the timeout to
   `last_activity + ABANDON_AFTER_MINUTES`. When the watermark passes it, a cart
   that still holds items is emitted. A purchase marks the session as converted
   and keeps that marker until timeout, so a late `add_to_cart` can't bring it
   back.

### Shop database (CDC)
Postgres WAL → Debezium (`pgoutput`, `REPLICA IDENTITY FULL`, so updates carry
before-images) → `shop.public.<table>` → the **cdc** query:
- appends the change envelopes to `bronze/cdc/table_name=…/ingest_date=…`;
- updates live views in Redis: revenue and orders today, the order status board
  (decrement old status, increment new), top products, low stock, and display
  names.

### Courier files (batch files)
The simulator writes `data/inbox/courier/*.csv` (as `.part`, then renamed
atomically). The Airflow task works through new files one by one:
1. standardise the file;
2. apply data-quality rules;
3. load good rows, update the ledger and write the DQ results in one transaction;
4. write rejected rows to `quarantine/` with the reasons;
5. move the file to `archive/`.

### Public holidays (REST API)
A weekly DAG calls the Nager.Date API. It retries with back-off, then falls back
to a built-in list. The result feeds `dim_date.is_public_holiday`.

### Warehouse modelling (dbt)
`raw` (insert-only, idempotent) → `staging` views (typed, one per source) →
`marts` tables:
- `dim_customers`, `dim_products`: **SCD2 built from CDC history**;
- `dim_date`: holidays, weekends, payday;
- `fct_orders`: latest state, status timestamps from the CDC history, the
  courier report, and a point-in-time customer key;
- `fct_order_items`: cost at the time of sale;
- `fct_sessions`;
- marts for sales, funnel, delivery SLA, RFM, abandoned carts and DQ.

## 3. Delivery guarantees

| Sink | Guarantee | Mechanism |
|---|---|---|
| bronze clickstream | exactly-once | file sink + checkpoint commit log |
| funnel (Redis) | effectively once | absolute values per window, overwritten |
| CDC → Redis | effectively once | `SET NX seen:cdc:{topic}:{partition}:{offset}` before applying |
| abandoned carts → Redis | effectively once | `SET NX seen:abandoned:{session}` |
| bronze CDC / abandoned Parquet | at-least-once | duplicates removed on load (`ON CONFLICT` on natural keys) |
| silver sessions | idempotent | dynamic partition overwrite |
| raw tables | idempotent | `COPY` to temp table → `INSERT … ON CONFLICT` in one transaction |
| courier files | exactly-once | ledger row written in the same transaction as the data |

`app/tests/test_live_store.py` replays CDC batches, and
`app/tests/test_streaming_spark.py` runs the real queries end to end.

## 4. Redis key design (one key per question)

| Key | Type | Question it answers |
|---|---|---|
| `funnel:{minute}` | hash `{stage}:events/sessions` | Funnel and active sessions per minute |
| `live:{date}` | hash | Today's orders, revenue, items, cancellations, abandoned carts/value |
| `live:status` | hash | How many orders are in each status right now |
| `live:orders` | list (20) | Latest orders |
| `top:{date}:revenue` / `:units` | sorted set | Best sellers today |
| `stock:low` | sorted set (score = stock) | What needs reordering, lowest first |
| `alerts:abandoned` | list (100) | Latest abandoned carts |
| `product:{id}`, `customer:{id}` | hash | Display names |
| `seen:*` | string, TTL 7 days | Idempotency guards |

## 5. Decisions and trade-offs

- **One Postgres server for shop, warehouse and Airflow.** This keeps the stack
  to 9 containers. They are still separate databases with separate roles, so
  splitting them later only means changing connection strings.
- **A local folder as the lake.** You can browse it, and it avoids the S3
  connector JARs. Moving to S3/GCS/ADLS means changing `LAKE` to an `s3a://`
  (or `gs://`, `abfss://`) URI and adding the matching Hadoop connector.
- **Three click queries read the topic independently.** Each has its own
  checkpoint and trigger, so they can fail and recover on their own. The cost
  is three Kafka consumers, which is fine at this volume.
- **RocksDB state store.** Cart state lives off the JVM heap, which scales to
  many concurrent sessions.
- **The CDC envelope stays generic in the lake (JSON images), and dbt types each
  table.** Adding a table to the connector needs no Spark change. The cost is
  that schema problems surface at modelling time, not at ingestion.
- **SCD2 first versions start at 1900-01-01.** A guest who signs up in the same
  transaction as their first order gets a `created_at` a few microseconds
  *after* the order's `placed_at`. A strict validity window would leave that
  order without a customer, and the `not_null` test on `customer_sk` caught
  exactly that. Opening the first version to the past is the standard fix.
- **Pure-Python cores.** Cart logic, Redis writers, DQ rules and the API parsing
  have no Spark dependency, so most tests run in milliseconds.

## 6. Known limitations, and what I'd do next

1. **Small files.** 30-second micro-batches write many small Parquet files. Next
   step: move bronze to Delta Lake or Iceberg and schedule `OPTIMIZE`/compaction.
2. **Redis can't be rebuilt from scratch.** If Redis is wiped, profiles return
   with the next CDC change, but today's counters restart at zero. Next step: a
   backfill job that replays today's bronze into Redis.
3. **Incremental dbt.** Marts are rebuilt in full every 30 minutes. That's fine
   for thousands of rows but not millions. Next step: make `fct_orders` and
   `fct_sessions` incremental on `loaded_at`.
4. **Schema contracts.** The clickstream schema is enforced only by `from_json`,
   which drops malformed events silently. Next step: a schema registry
   (Redpanda has one built in) plus a dead-letter topic with counts in the DQ
   scorecard.
5. **Alerting.** DQ pass rates and consumer lag are visible but don't page
   anyone. Next step: an Airflow task that fails, and notifies, when a critical
   rule drops below a threshold.
6. **Recovery emails.** Abandoned carts already sit in Redis and the
   warehouse. A small consumer could send a recovery nudge, and
   `mart_abandoned_carts_daily` already measures recovered value.

## 7. Laptop → cloud

| Here | AWS | GCP | Azure |
|---|---|---|---|
| Postgres (shop) | RDS / Aurora | Cloud SQL | Azure Database for PostgreSQL |
| Debezium on Kafka Connect | MSK Connect + Debezium | Datastream | Event Hubs + Debezium |
| Redpanda | MSK / Kinesis | Pub/Sub / Managed Kafka | Event Hubs |
| `./data/lake` | S3 | GCS | ADLS Gen2 |
| Spark (local) | EMR / Glue / Databricks | Dataproc / Databricks | Databricks |
| Airflow | MWAA | Cloud Composer | Data Factory managed Airflow |
| Postgres (warehouse) + dbt | Redshift / Snowflake | BigQuery | Synapse / Fabric |
| Redis | ElastiCache | Memorystore | Azure Cache for Redis |
| FastAPI | ECS / App Runner | Cloud Run | Container Apps |
