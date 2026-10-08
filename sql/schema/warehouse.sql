-- =====================================================================
-- warehouse: analytical store (Postgres standing in for BigQuery/Snowflake).
-- Runs as `loader`, which owns everything here.
--
--   raw        loaded by Airflow tasks, insert-only and idempotent
--   staging    dbt views, one per source
--   marts      dbt tables the API and analysts read
--   ops        pipeline bookkeeping + data-quality results
-- =====================================================================

CREATE SCHEMA raw;
CREATE SCHEMA staging;
CREATE SCHEMA marts;
CREATE SCHEMA ops;

-- The API only ever sees marts.
GRANT USAGE ON SCHEMA marts TO api_reader;
ALTER DEFAULT PRIVILEGES IN SCHEMA marts GRANT SELECT ON TABLES TO api_reader;

-- Every Debezium change event for every shop table.
CREATE TABLE raw.cdc_events (
    topic            TEXT    NOT NULL,
    kafka_partition  INT     NOT NULL,
    kafka_offset     BIGINT  NOT NULL,
    table_name       TEXT    NOT NULL,
    op               CHAR(1) NOT NULL,          -- r snapshot, c insert, u update, d delete
    ts_ms            BIGINT  NOT NULL,          -- when the change was committed in the shop DB
    before_json      JSONB,
    after_json       JSONB,
    loaded_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (topic, kafka_partition, kafka_offset)
);
CREATE INDEX ON raw.cdc_events (table_name, ts_ms);

-- One row per browsing session, built by the Spark sessionize job.
CREATE TABLE raw.sessions (
    session_id       TEXT PRIMARY KEY,
    customer_id      BIGINT,
    session_date     DATE        NOT NULL,
    started_at       TIMESTAMPTZ NOT NULL,
    ended_at         TIMESTAMPTZ NOT NULL,
    duration_s       INT         NOT NULL,
    events           INT         NOT NULL,
    page_views       INT         NOT NULL,
    product_views    INT         NOT NULL,
    add_to_carts     INT         NOT NULL,
    checkouts        INT         NOT NULL,
    purchases        INT         NOT NULL,
    late_events      INT         NOT NULL,      -- arrived > 60s after they happened
    device           TEXT,
    traffic_source   TEXT,
    landing_page     TEXT,
    loaded_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX ON raw.sessions (session_date);

-- Courier delivery files that passed data-quality checks.
CREATE TABLE raw.deliveries (
    waybill          TEXT        NOT NULL,
    order_id         BIGINT      NOT NULL,
    courier          TEXT        NOT NULL,
    province         TEXT        NOT NULL,
    collected_at     TIMESTAMPTZ NOT NULL,
    delivered_at     TIMESTAMPTZ,
    status           TEXT        NOT NULL,
    attempts         INT         NOT NULL,
    source_file      TEXT        NOT NULL,
    loaded_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (waybill, status)
);

CREATE TABLE raw.public_holidays (
    holiday_date     DATE PRIMARY KEY,
    name             TEXT        NOT NULL,
    source           TEXT        NOT NULL,
    loaded_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Abandoned carts detected by the stream (landed in the lake, loaded hourly).
CREATE TABLE raw.abandoned_carts (
    session_id       TEXT PRIMARY KEY,
    customer_id      BIGINT,
    cart_value       NUMERIC(12, 2) NOT NULL,
    items            INT         NOT NULL,
    last_activity_at TIMESTAMPTZ NOT NULL,
    detected_at      TIMESTAMPTZ NOT NULL,
    loaded_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Which files have been ingested (so re-runs never double-load).
CREATE TABLE ops.ingested_files (
    file_name        TEXT PRIMARY KEY,
    pipeline         TEXT        NOT NULL,
    rows_loaded      INT         NOT NULL,
    rows_rejected    INT         NOT NULL,
    ingested_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE ops.dq_results (
    run_id           TEXT        NOT NULL,
    dataset          TEXT        NOT NULL,
    rule_name        TEXT        NOT NULL,
    dimension        TEXT        NOT NULL,
    checked_rows     INT         NOT NULL,
    failed_rows      INT         NOT NULL,
    run_at           TIMESTAMPTZ NOT NULL DEFAULT now()
);
