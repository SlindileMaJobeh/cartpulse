"""Redis writes for the speed layer (the "high-velocity store").

Every writer here is idempotent, because Spark gives at-least-once delivery
(a micro-batch is replayed after a crash):

* funnel windows       HSET of absolute counts      -> replay overwrites with the same value
* CDC side effects     guarded by SET NX on the Kafka coordinates of the event
* abandoned carts      guarded by SET NX on the session id

Key layout (see docs/DESIGN.md for the full table):
  funnel:{minute_epoch}         hash  {stage}:events / {stage}:sessions
  live:{yyyy-mm-dd}             hash  orders, revenue, items, abandoned_carts, abandoned_value
  live:status                   hash  number of orders currently in each status
  live:orders                   list  last 20 orders (JSON)
  top:{date}:units|revenue      zset  product_id -> units / ZAR today
  stock:low                     zset  product_id -> stock on hand (only items at/below threshold)
  product:{id}, customer:{id}   hash  names for display
  alerts:abandoned              list  last 100 abandoned carts (JSON)
"""
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

SAST = timezone(timedelta(hours=2))
TTL_DAY = 3 * 24 * 3600
TTL_SEEN = 7 * 24 * 3600


def sast_date(ts_ms: int) -> str:
    return datetime.fromtimestamp(ts_ms / 1000, tz=SAST).date().isoformat()


# ------------------------------------------------------------------ funnel
def write_funnel(r, rows) -> int:
    """rows: (window_start: datetime, stage, events, sessions)."""
    pipe = r.pipeline(transaction=False)
    n = 0
    for window_start, stage, events, sessions in rows:
        key = f"funnel:{int(window_start.replace(tzinfo=timezone.utc).timestamp()) // 60}"
        pipe.hset(key, mapping={f"{stage}:events": int(events), f"{stage}:sessions": int(sessions)})
        pipe.expire(key, 6 * 3600)
        n += 1
    pipe.execute()
    return n


# ------------------------------------------------------------------- CDC
def apply_cdc(r, rows, low_stock_threshold: int = 10) -> int:
    """rows: dicts with topic, kafka_partition, kafka_offset, table_name, op, ts_ms,
    before_json, after_json - in Kafka order. Returns how many were applied (not replays)."""
    rows = list(rows)
    if not rows:
        return 0
    pipe = r.pipeline(transaction=False)
    for row in rows:
        pipe.set(f"seen:cdc:{row['topic']}:{row['kafka_partition']}:{row['kafka_offset']}", 1, nx=True, ex=TTL_SEEN)
    fresh = pipe.execute()

    pipe = r.pipeline(transaction=False)
    applied = 0
    for row, is_new in zip(rows, fresh):
        if not is_new:
            continue
        applied += 1
        op, table = row["op"], row["table_name"]
        before = json.loads(row["before_json"]) if row.get("before_json") else None
        after = json.loads(row["after_json"]) if row.get("after_json") else None
        day = sast_date(int(row["ts_ms"]))

        if table == "orders":
            if op in ("r", "c") and after:
                pipe.hincrby("live:status", after["status"], 1)
            if op == "c" and after:                          # a brand-new order, right now
                total = float(after["order_total"])
                pipe.hincrby(f"live:{day}", "orders", 1)
                pipe.hincrbyfloat(f"live:{day}", "revenue", total)
                pipe.expire(f"live:{day}", TTL_DAY)
                pipe.lpush("live:orders", json.dumps({
                    "order_id": after["order_id"], "customer_id": after["customer_id"], "total": total,
                    "province": after["ship_province"], "payment": after["payment_method"],
                    "placed_at": after["placed_at"]}))
                pipe.ltrim("live:orders", 0, 19)
            elif op == "u" and before and after and before["status"] != after["status"]:
                pipe.hincrby("live:status", before["status"], -1)
                pipe.hincrby("live:status", after["status"], 1)
                if after["status"] == "CANCELLED":
                    pipe.hincrby(f"live:{day}", "cancelled", 1)
            elif op == "d" and before:
                pipe.hincrby("live:status", before["status"], -1)

        elif table == "order_items" and op == "c" and after:
            qty, pid = int(after["quantity"]), after["product_id"]
            pipe.zincrby(f"top:{day}:units", qty, pid)
            pipe.zincrby(f"top:{day}:revenue", round(qty * float(after["unit_price"]), 2), pid)
            pipe.hincrby(f"live:{day}", "items", qty)
            pipe.expire(f"top:{day}:units", TTL_DAY)
            pipe.expire(f"top:{day}:revenue", TTL_DAY)

        elif table == "inventory" and after:
            if int(after["stock_on_hand"]) <= low_stock_threshold:
                pipe.zadd("stock:low", {after["product_id"]: int(after["stock_on_hand"])})
            else:
                pipe.zrem("stock:low", after["product_id"])

        elif table == "products" and after:
            pipe.hset(f"product:{after['product_id']}", mapping={
                "name": after["name"], "category": after["category"], "price": after["price"]})

        elif table == "customers" and after:
            pipe.hset(f"customer:{after['customer_id']}", mapping={
                "first_name": after["first_name"], "province": after["province"], "city": after["city"]})
    pipe.execute()
    return applied


# ------------------------------------------------------------- abandoned carts
def write_abandoned(r, rows) -> int:
    rows = list(rows)
    if not rows:
        return 0
    pipe = r.pipeline(transaction=False)
    for row in rows:
        pipe.set(f"seen:abandoned:{row['session_id']}", 1, nx=True, ex=TTL_SEEN)
    fresh = pipe.execute()
    pipe = r.pipeline(transaction=False)
    n = 0
    for row, is_new in zip(rows, fresh):
        if not is_new:
            continue
        n += 1
        last = row["last_activity_at"]
        day = last.astimezone(SAST).date().isoformat() if last.tzinfo else \
            last.replace(tzinfo=timezone.utc).astimezone(SAST).date().isoformat()
        pipe.lpush("alerts:abandoned", json.dumps({
            "session_id": row["session_id"],
            "customer_id": None if row["customer_id"] is None else int(row["customer_id"]),
            "cart_value": float(row["cart_value"]), "items": int(row["items"]),
            "last_activity_at": last.isoformat()}, default=str))
        pipe.ltrim("alerts:abandoned", 0, 99)
        pipe.hincrby(f"live:{day}", "abandoned_carts", 1)
        pipe.hincrbyfloat(f"live:{day}", "abandoned_value", float(row["cart_value"]))
        pipe.expire(f"live:{day}", TTL_DAY)
    pipe.execute()
    return n
