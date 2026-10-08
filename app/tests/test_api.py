import json
import time

import fakeredis
import psycopg
import pytest
from fastapi.testclient import TestClient

from cartpulse.api import main, store


@pytest.fixture
def r():
    return fakeredis.FakeRedis(decode_responses=True)


@pytest.fixture
def client(r):
    main.app.dependency_overrides[main.live] = lambda: r
    yield TestClient(main.app)
    main.app.dependency_overrides.clear()


def test_overview(client, r):
    r.hset(f"live:{store.today()}", mapping={"orders": 4, "revenue": 1000, "abandoned_carts": 2})
    r.hset("live:status", mapping={"PLACED": 1, "SHIPPED": 3})
    body = client.get("/api/live/overview").json()
    assert body["orders"] == 4 and body["avg_order_value"] == 250.0 and body["order_status"]["SHIPPED"] == 3


def test_funnel_sums_recent_minutes(client, r):
    now = int(time.time()) // 60
    r.hset(f"funnel:{now}", mapping={"purchase:events": 2, "any:sessions": 10})
    r.hset(f"funnel:{now - 1}", mapping={"purchase:events": 1, "any:sessions": 8})
    r.hset(f"funnel:{now - 30}", mapping={"purchase:events": 50})          # outside the window
    body = client.get("/api/live/funnel?minutes=5").json()
    assert body["events"]["purchase"] == 3 and body["series"][-1]["sessions"] == 10


def test_top_products_and_low_stock_have_names(client, r):
    r.zadd(f"top:{store.today()}:revenue", {"9": 500.0})
    r.zadd("stock:low", {"9": 3})
    r.hset("product:9", mapping={"name": "Kettle"})
    assert client.get("/api/live/top-products").json()[0]["name"] == "Kettle"
    assert client.get("/api/live/low-stock").json() == [{"product_id": 9, "name": "Kettle", "stock_on_hand": 3}]


def test_abandoned_guest_label(client, r):
    r.lpush("alerts:abandoned", json.dumps({"session_id": "S", "customer_id": None, "cart_value": 1, "items": 1,
                                            "last_activity_at": "2026-10-01T10:00:00"}))
    assert client.get("/api/live/abandoned-carts").json()[0]["customer_name"] == "Guest"


def test_warehouse_down_is_503(client, monkeypatch):
    def boom(*_):
        raise psycopg.OperationalError("refused")
    monkeypatch.setattr(store, "sales_daily", boom)
    assert client.get("/api/sales/daily").status_code == 503


def test_unknown_customer_404(client, monkeypatch):
    monkeypatch.setattr(store, "customer", lambda cid: None)
    assert client.get("/api/customers/999").status_code == 404


def test_dashboard_served(client):
    resp = client.get("/")
    assert resp.status_code == 200 and "CartPulse" in resp.text
