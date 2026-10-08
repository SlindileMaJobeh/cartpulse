import json
from datetime import datetime, timezone

import fakeredis

from cartpulse.streaming import live_store as ls


def change(offset, table, op, before=None, after=None, ts=1_790_000_000_000):
    return {"topic": f"shop.public.{table}", "kafka_partition": 0, "kafka_offset": offset, "table_name": table,
            "op": op, "ts_ms": ts, "before_json": json.dumps(before) if before else None,
            "after_json": json.dumps(after) if after else None}


ORDER = {"order_id": 1, "customer_id": 5, "status": "PLACED", "order_total": "250.00", "ship_province": "Gauteng",
         "payment_method": "CARD", "placed_at": "2026-10-01T10:00:00Z"}


def test_cdc_updates_live_views_and_is_replay_safe():
    r = fakeredis.FakeRedis(decode_responses=True)
    rows = [
        change(0, "orders", "c", after=ORDER),
        change(1, "orders", "u", before=ORDER, after={**ORDER, "status": "PAID"}),
        change(0, "order_items", "c", after={"order_id": 1, "product_id": 9, "quantity": 2, "unit_price": "125.00"}),
        change(0, "inventory", "u", after={"product_id": 9, "stock_on_hand": 4}),
        change(0, "products", "r", after={"product_id": 9, "name": "Kettle", "category": "Home", "price": "125.00"}),
    ]
    assert ls.apply_cdc(r, rows) == 5
    assert ls.apply_cdc(r, rows) == 0                      # replayed micro-batch: nothing double-counted
    day = ls.sast_date(rows[0]["ts_ms"])
    assert r.hget(f"live:{day}", "orders") == "1" and float(r.hget(f"live:{day}", "revenue")) == 250.0
    assert r.hgetall("live:status") == {"PLACED": "0", "PAID": "1"}
    assert r.zscore(f"top:{day}:units", "9") == 2
    assert r.zscore("stock:low", "9") == 4
    ls.apply_cdc(r, [change(1, "inventory", "u", after={"product_id": 9, "stock_on_hand": 80})])
    assert r.zscore("stock:low", "9") is None              # restocked -> off the list
    assert r.hget("product:9", "name") == "Kettle"


def test_funnel_overwrites_instead_of_adding():
    r = fakeredis.FakeRedis(decode_responses=True)
    w = datetime(2026, 10, 1, 10, 0)
    ls.write_funnel(r, [(w, "purchase", 3, 3)])
    ls.write_funnel(r, [(w, "purchase", 4, 4)])           # late event updated the window
    assert r.hget(f"funnel:{int(w.replace(tzinfo=timezone.utc).timestamp()) // 60}", "purchase:events") == "4"


def test_abandoned_alert_once_per_session():
    r = fakeredis.FakeRedis(decode_responses=True)
    row = {"session_id": "S1", "customer_id": None, "cart_value": 99.5, "items": 2,
           "last_activity_at": datetime(2026, 10, 1, 10, 0)}
    assert ls.write_abandoned(r, [row]) == 1
    assert ls.write_abandoned(r, [row]) == 0
    assert r.llen("alerts:abandoned") == 1
