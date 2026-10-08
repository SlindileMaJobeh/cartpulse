"""Runs the real streaming queries on a folder source (no Kafka) with event-time data."""
import json
from datetime import datetime, timedelta, timezone

import fakeredis
import pytest

pytest.importorskip("pyspark")

from cartpulse.streaming import app  # noqa: E402

T0 = datetime(2026, 10, 1, 10, 0, 0, tzinfo=timezone.utc)


def ev(session, kind, minute, **kw):
    t = T0 + timedelta(minutes=minute)
    return {"event_id": f"{session}-{kind}-{minute}-{kw.get('product_id', '')}", "event_type": kind,
            "session_id": session, "customer_id": kw.pop("customer_id", None), "device": "mobile",
            "traffic_source": "direct", "event_time": t.isoformat().replace("+00:00", "Z"), **kw}


@pytest.fixture(scope="module")
def spark():
    s = app.spark_session(kafka=False)
    yield s
    s.stop()


def write_batches(folder, batches):
    """One file per micro-batch. The file source orders by modification time, so set it explicitly."""
    import os
    for i, events in enumerate(batches):
        path = folder / f"{i:03d}.json"
        path.write_text("\n".join(json.dumps(e) for e in events) + "\n")
        os.utime(path, (1_700_000_000 + i, 1_700_000_000 + i))


def test_funnel_and_abandoned_carts(spark, tmp_path, monkeypatch):
    src = tmp_path / "src"
    src.mkdir()
    write_batches(src, [
        [ev("A", "page_view", 0), ev("A", "product_view", 0.2, product_id=1, price=100.0),
         ev("A", "add_to_cart", 0.5, product_id=1, quantity=2, price=100.0, customer_id=7),       # abandoned
         ev("B", "page_view", 0.1), ev("B", "add_to_cart", 0.3, product_id=2, quantity=1, price=50.0),
         ev("B", "begin_checkout", 0.6), ev("B", "purchase", 0.9, order_id=11)],                   # converted
        [ev("C", "page_view", 1.5), ev("C", "add_to_cart", 1.6, product_id=3, quantity=1, price=20.0),
         ev("C", "remove_from_cart", 1.7, product_id=3, quantity=1, price=20.0)],                  # empty cart
        [ev("D", "page_view", 12)],          # pushes the watermark past A's timeout (0.5 + 5 min)
        [ev("E", "page_view", 13),
         ev("F", "add_to_cart", 13.5, product_id=4, quantity=1, price=9.0)],   # idle < 5 min: NOT abandoned yet
        [ev("G", "page_view", 14)],          # one more batch so A's timeout fires
    ])
    r = fakeredis.FakeRedis(decode_responses=True)
    monkeypatch.setattr(app, "redis_client", lambda: r)

    clicks = app.clickstream_source(spark, str(src))
    trig = {"availableNow": True}
    lake, chk = str(tmp_path / "lake"), str(tmp_path / "chk")
    queries = [app.funnel_query(clicks, chk, trig), app.carts_query(clicks, lake, chk, trig),
               app.bronze_query(clicks, lake, chk, trig)]
    for q in queries:
        q.awaitTermination(180)

    minute0 = int(T0.timestamp()) // 60
    f0 = r.hgetall(f"funnel:{minute0}")
    assert f0["any:sessions"] == "2" and f0["add_to_cart:events"] == "2" and f0["purchase:events"] == "1"

    alerts = [json.loads(a) for a in r.lrange("alerts:abandoned", 0, -1)]
    assert [a["session_id"] for a in alerts] == ["A"]
    assert alerts[0]["cart_value"] == 200.0 and alerts[0]["customer_id"] == 7 and alerts[0]["items"] == 2
    assert alerts[0]["last_activity_at"].startswith("2026-10-01T10:00:30")

    bronze = spark.read.parquet(f"{lake}/bronze/clickstream")
    assert bronze.count() == 14
    assert spark.read.parquet(f"{lake}/bronze/abandoned_carts").count() == 1
