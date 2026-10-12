# Talking about CartPulse (interview prep)

A crib sheet for explaining the project in an interview, on a CV, or in a
LinkedIn post. Make the words your own, and run every demo step yourself first.

## The 30-second version

> "CartPulse is an end-to-end data platform for an online store that I built to
> run on a laptop. It ingests from three kinds of source:
> - change data capture from Postgres with Debezium,
> - a clickstream on Kafka,
> - courier CSV files, plus a public REST API.
>
> Spark Structured Streaming powers a live dashboard: event-time funnel
> windows, and stateful abandoned-cart detection with event-time timeouts. On
> the batch side, Airflow runs a Spark sessionization job and dbt builds SCD2
> dimensions from the CDC history. A FastAPI service serves both layers. I made
> every sink idempotent, and the tests replay batches to prove nothing gets
> double-counted."

## CV bullet ideas

- Built a real-time e-commerce analytics platform (Kafka/Redpanda, Debezium CDC, Spark Structured Streaming, Airflow, dbt, PostgreSQL, Redis, FastAPI), fully containerised with Docker Compose.
- Implemented stateful abandoned-cart detection using Spark `applyInPandasWithState` with event-time timeouts and watermarks; covered by an integration test on event-time data.
- Built SCD Type 2 customer and product dimensions from CDC history, enabling point-in-time joins for margin analysis.
- Designed idempotent sinks across the pipeline (file-sink commit log, `SET NX` guards, `ON CONFLICT` merges), giving effectively-once results on an at-least-once stream.
- Added data-quality checks (completeness, validity, consistency, uniqueness) with quarantine and pass-rate tracking; masked PII in the marts and enforced least-privilege database roles.

## Questions you'll likely get, and the short answer

**Why both Redis and Postgres?**
They answer different questions. Redis holds pre-computed answers to a few
known live queries, at millisecond reads. The warehouse holds complete history
that can be rebuilt, for ad-hoc analysis. The API picks the right one per
endpoint.

**What's a watermark, and why 2 minutes?**
A watermark is how long the stream waits for late events before a window is
final. About 2% of events arrive 20–150 seconds late, so 2 minutes catches most
of them. Anything later still lands in bronze, and the batch sessionize job
counts it. Raising the watermark makes the live numbers more complete but holds
more state and delays abandoned-cart alerts.

**How does abandoned-cart detection work?**
Each session's events go through a state machine. Each event sets the timeout
to last activity + 5 minutes of *event time*. When the watermark passes that
point and the cart still holds items, the job emits an alert. Converted
sessions keep a marker so a late event can't revive them. A real bug is worth
mentioning here: Arrow handed over microsecond timestamps where the code
assumed nanoseconds, so every cart timed out immediately. The integration test
now checks that a cart idle for under 5 minutes is *not* flagged.

**What happens if the stream processor crashes?**
It restarts from its checkpoint and replays the last micro-batch. The bronze
file sink is exactly-once. The Redis funnel writes are absolute values, so
replaying them changes nothing. CDC and abandoned-cart writes check
`SET NX seen:…` first. Demo it with `docker compose restart stream`.

**Why CDC instead of querying the shop database?**
CDC doesn't load the production database, captures every intermediate state
(PLACED → PAID → SHIPPED, so you get time-to-ship), and captures deletes.
Polling `updated_at` would miss changes between polls, and it can't see
deletes.

**How did you build SCD2 from CDC?**
For each key, order the change events by commit time and Kafka offset. Each
version is valid from its own change time until the next one, and the latest is
current. The first version is open-ended into the past. A test caught why: a
guest who signs up while placing an order gets a `created_at` microseconds
after the order.

**What would break at 100× the load?**
Small files in the lake (fix with Iceberg or Delta plus compaction), full dbt
rebuilds (make them incremental), one Postgres server (split it, and use a
columnar warehouse), and Redis memory for `seen:*` keys (shorter TTLs, or a
Bloom filter).

**What would you do next?**
Pick one from `docs/DESIGN.md` §6 and be specific. A schema registry plus a
dead-letter topic is a strong answer, because it closes a real gap: malformed
events are dropped silently today.

## Demo script (5 minutes)

1. Open the dashboard and show the live tiles moving. Point out the two layers.
2. In Redpanda Console, open `shop.public.orders` and show a CDC event's `before` and `after`.
3. Run `UPDATE products SET price = 99.99 WHERE product_id = 3;`. Then `make batch`, and call `/api/products/3/price-history`: there's a new SCD2 version.
4. Show an abandoned cart in the dashboard and explain the watermark and timeout.
5. Run `docker compose restart stream` and show the counters don't jump.
6. Show the data-quality panel and a rejected row in `data/lake/quarantine/courier/`.
7. Open `docs/DESIGN.md` and walk through one trade-off you would change.

## Before you publish it

- Run it yourself end to end, then take your own dashboard screenshot for the
  README. It's more convincing than any description.
- Read every file you'll be asked about. If something is unclear, change it
  until you can explain it.
- Make it yours. Add one enhancement from the "next" list before you show it
  to anyone.
