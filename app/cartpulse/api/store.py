"""Data access for the API: Redis for live views, warehouse marts for history."""
from __future__ import annotations

import json
import time
from datetime import datetime, timedelta, timezone

import psycopg
import redis
from psycopg.rows import dict_row

from cartpulse import config

SAST = timezone(timedelta(hours=2))
STAGES = ["any", "page_view", "product_view", "add_to_cart", "begin_checkout", "purchase"]
_redis = None
# SCD2 windows use 1900-01-01 / 9999-12-31 sentinels internally; the API shows them as null.
OPEN_ENDED = """CASE WHEN version = 1 THEN NULL ELSE valid_from END AS valid_from,
                CASE WHEN valid_to >= '9999-01-01' THEN NULL ELSE valid_to END AS valid_to"""


def get_redis():
    global _redis
    if _redis is None:
        _redis = redis.Redis.from_url(config.REDIS_URL, decode_responses=True)
    return _redis


def query(sql: str, params=None) -> list[dict]:
    """Read-only, as the api_reader role, which can only see the marts schema."""
    with psycopg.connect(config.API_DB_DSN, row_factory=dict_row, connect_timeout=5) as conn:
        conn.read_only = True
        with conn.cursor() as cur:
            cur.execute(sql, params)
            return cur.fetchall()


def today() -> str:
    return datetime.now(SAST).date().isoformat()


def _names(r, ids, kind="product") -> list[str | None]:
    if not ids:
        return []
    pipe = r.pipeline()
    for i in ids:
        pipe.hget(f"{kind}:{i}", "name")
    return pipe.execute()


# ------------------------------------------------------------------ live
def overview(r) -> dict:
    day = today()
    s = r.hgetall(f"live:{day}")
    orders, revenue = int(s.get("orders", 0)), float(s.get("revenue", 0))
    funnel = funnel_window(r, minutes=5)
    return {
        "date": day,
        "orders": orders,
        "revenue": round(revenue, 2),
        "avg_order_value": round(revenue / orders, 2) if orders else 0.0,
        "items_sold": int(s.get("items", 0)),
        "cancelled": int(s.get("cancelled", 0)),
        "abandoned_carts": int(s.get("abandoned_carts", 0)),
        "abandoned_value": round(float(s.get("abandoned_value", 0)), 2),
        "active_sessions_last_min": funnel["series"][-1]["sessions"] if funnel["series"] else 0,
        "order_status": {k: int(v) for k, v in r.hgetall("live:status").items()},
    }


def funnel_window(r, minutes: int = 15) -> dict:
    """Sum of per-minute counts over the last N complete-ish minutes."""
    now_min = int(time.time()) // 60
    keys = [f"funnel:{m}" for m in range(now_min - minutes + 1, now_min + 1)]
    pipe = r.pipeline()
    for k in keys:
        pipe.hgetall(k)
    cells = pipe.execute()
    totals = {stage: 0 for stage in STAGES}
    series = []
    for minute, cell in zip(range(now_min - minutes + 1, now_min + 1), cells):
        for stage in STAGES:
            totals[stage] += int(cell.get(f"{stage}:events", 0))
        series.append({
            "minute": datetime.fromtimestamp(minute * 60, tz=SAST).strftime("%H:%M"),
            "sessions": int(cell.get("any:sessions", 0)),
            "add_to_cart": int(cell.get("add_to_cart:events", 0)),
            "purchases": int(cell.get("purchase:events", 0)),
        })
    return {"minutes": minutes, "events": {k: v for k, v in totals.items() if k != "any"},
            "total_events": totals["any"], "series": series}


def recent_orders(r, limit: int) -> list[dict]:
    return [json.loads(o) for o in r.lrange("live:orders", 0, limit - 1)]


def abandoned(r, limit: int) -> list[dict]:
    carts = [json.loads(c) for c in r.lrange("alerts:abandoned", 0, limit - 1)]
    pipe = r.pipeline()
    for c in carts:
        pipe.hget(f"customer:{c['customer_id']}", "first_name")
    for c, name in zip(carts, pipe.execute() if carts else []):
        c["customer_name"] = name or ("Guest" if c["customer_id"] is None else None)
    return carts


def top_products(r, by: str, limit: int) -> list[dict]:
    rows = r.zrevrange(f"top:{today()}:{by}", 0, limit - 1, withscores=True)
    names = _names(r, [pid for pid, _ in rows])
    return [{"product_id": int(pid), "name": n, by: round(score, 2)} for (pid, score), n in zip(rows, names)]


def low_stock(r, limit: int) -> list[dict]:
    rows = r.zrange("stock:low", 0, limit - 1, withscores=True)
    names = _names(r, [pid for pid, _ in rows])
    return [{"product_id": int(pid), "name": n, "stock_on_hand": int(s)} for (pid, s), n in zip(rows, names)]


# ----------------------------------------------------------------- batch
def sales_daily(days: int) -> list[dict]:
    return query("""SELECT * FROM marts.mart_sales_daily
                    WHERE order_date > current_date - %(d)s::int ORDER BY order_date""", {"d": days})


def funnel_daily(days: int) -> list[dict]:
    return query("""SELECT session_date, sum(sessions) AS sessions, sum(viewed_product) AS viewed_product,
                           sum(added_to_cart) AS added_to_cart, sum(reached_checkout) AS reached_checkout,
                           sum(purchased) AS purchased, sum(abandoned_carts) AS abandoned_carts
                    FROM marts.mart_funnel_daily WHERE session_date > current_date - %(d)s::int
                    GROUP BY session_date ORDER BY session_date""", {"d": days})


def funnel_by_source(days: int) -> list[dict]:
    return query("""SELECT traffic_source, sum(sessions) AS sessions, sum(purchased) AS purchased,
                           round(sum(purchased)::numeric / nullif(sum(sessions), 0), 4) AS conversion_rate
                    FROM marts.mart_funnel_daily WHERE session_date > current_date - %(d)s::int
                    GROUP BY traffic_source ORDER BY conversion_rate DESC NULLS LAST""", {"d": days})


def delivery_sla() -> list[dict]:
    return query("""SELECT courier, sum(deliveries) AS deliveries,
                           round(sum(avg_hours * deliveries) / sum(deliveries), 1) AS avg_hours,
                           round(sum(sla_rate * deliveries) / sum(deliveries), 4) AS sla_rate
                    FROM marts.mart_delivery_sla GROUP BY courier ORDER BY sla_rate DESC""")


def delivery_sla_detail() -> list[dict]:
    return query("SELECT * FROM marts.mart_delivery_sla ORDER BY courier, ship_province")


def segments() -> list[dict]:
    return query("""SELECT segment, count(*) AS customers, round(sum(monetary), 2) AS revenue,
                           round(avg(recency_days), 1) AS avg_recency_days
                    FROM marts.mart_customer_rfm GROUP BY segment ORDER BY revenue DESC""")


def customer(customer_id: int) -> dict | None:
    rows = query("""SELECT c.customer_id, c.first_name, c.last_initial, c.email_masked, c.province, c.city,
                           c.marketing_opt_in, c.version, r.segment, r.recency_days, r.frequency, r.monetary
                    FROM marts.dim_customers c
                    LEFT JOIN marts.mart_customer_rfm r USING (customer_id)
                    WHERE c.customer_id = %(c)s AND c.is_current""", {"c": customer_id})
    if not rows:
        return None
    out = rows[0]
    out["history"] = query("""SELECT version, province, city, marketing_opt_in, """ + OPEN_ENDED + """
                              FROM marts.dim_customers WHERE customer_id = %(c)s ORDER BY version""",
                           {"c": customer_id})
    out["recent_orders"] = query("""SELECT order_id, placed_at, status, order_total, courier, delivery_hours
                                    FROM marts.fct_orders WHERE customer_id = %(c)s
                                    ORDER BY placed_at DESC LIMIT 10""", {"c": customer_id})
    return out


def price_history(product_id: int) -> list[dict]:
    return query("""SELECT version, product_name, price, cost, margin_pct, is_current, """ + OPEN_ENDED + """
                    FROM marts.dim_products WHERE product_id = %(p)s ORDER BY version""", {"p": product_id})


def abandoned_daily(days: int) -> list[dict]:
    return query("""SELECT * FROM marts.mart_abandoned_carts_daily
                    WHERE cart_date > current_date - %(d)s::int ORDER BY cart_date""", {"d": days})


def data_quality() -> list[dict]:
    return query("SELECT * FROM marts.mart_dq_scorecard ORDER BY pass_rate, dataset, rule_name")
