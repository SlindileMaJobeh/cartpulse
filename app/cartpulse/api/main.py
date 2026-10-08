"""CartPulse API.

  /api/live/*   Redis, updated by Spark Structured Streaming within seconds
  /api/*        warehouse marts, rebuilt by Airflow + dbt every 30 minutes
  /             the live dashboard        /docs  interactive API docs
"""
from __future__ import annotations

from pathlib import Path

import psycopg
from fastapi import Depends, FastAPI, HTTPException, Query
from fastapi.responses import FileResponse, JSONResponse

from cartpulse.api import store

app = FastAPI(title="CartPulse API", version="1.0.0",
              description="Real-time and batch analytics for an online store.")
STATIC = Path(__file__).parent / "static"


def live():
    return store.get_redis()


def from_warehouse(fn, *args):
    try:
        return fn(*args)
    except psycopg.errors.UndefinedTable:
        raise HTTPException(503, "Marts not built yet - the first cartpulse_batch DAG run creates them")
    except psycopg.OperationalError as exc:
        raise HTTPException(503, f"Warehouse unavailable ({exc.__class__.__name__})")


@app.get("/", include_in_schema=False)
def dashboard():
    return FileResponse(STATIC / "index.html")


@app.get("/api/health", tags=["ops"])
def health(r=Depends(live)):
    status = {"redis": "down", "warehouse": "down"}
    try:
        status["redis"] = "up" if r.ping() else "down"
    except Exception:  # noqa: BLE001
        pass
    try:
        store.query("SELECT 1")
        status["warehouse"] = "up"
    except Exception:  # noqa: BLE001
        pass
    return JSONResponse(status, status_code=200 if set(status.values()) == {"up"} else 503)


# ---------------------------------------------------------------- live
@app.get("/api/live/overview", tags=["live"])
def live_overview(r=Depends(live)):
    """Today's orders, revenue and abandoned carts, plus the order status board."""
    return store.overview(r)


@app.get("/api/live/funnel", tags=["live"])
def live_funnel(minutes: int = Query(15, ge=1, le=360), r=Depends(live)):
    """Event counts per funnel stage over the last N minutes, and a per-minute series."""
    return store.funnel_window(r, minutes)


@app.get("/api/live/orders", tags=["live"])
def live_orders(limit: int = Query(10, ge=1, le=20), r=Depends(live)):
    return store.recent_orders(r, limit)


@app.get("/api/live/abandoned-carts", tags=["live"])
def live_abandoned(limit: int = Query(10, ge=1, le=100), r=Depends(live)):
    """Most recent carts left with items in them (detected by the stateful stream)."""
    return store.abandoned(r, limit)


@app.get("/api/live/top-products", tags=["live"])
def live_top(by: str = Query("revenue", pattern="^(revenue|units)$"),
             limit: int = Query(5, ge=1, le=50), r=Depends(live)):
    return store.top_products(r, by, limit)


@app.get("/api/live/low-stock", tags=["live"])
def live_low_stock(limit: int = Query(10, ge=1, le=100), r=Depends(live)):
    """Products at or below their reorder level, lowest first (from inventory CDC)."""
    return store.low_stock(r, limit)


# --------------------------------------------------------------- batch
@app.get("/api/sales/daily", tags=["sales"])
def sales_daily(days: int = Query(30, ge=1, le=365)):
    return from_warehouse(store.sales_daily, days)


@app.get("/api/funnel/daily", tags=["funnel"])
def funnel_daily(days: int = Query(7, ge=1, le=90)):
    return from_warehouse(store.funnel_daily, days)


@app.get("/api/funnel/by-source", tags=["funnel"])
def funnel_by_source(days: int = Query(7, ge=1, le=90)):
    return from_warehouse(store.funnel_by_source, days)


@app.get("/api/delivery/sla", tags=["delivery"])
def delivery_sla(detail: bool = False):
    return from_warehouse(store.delivery_sla_detail if detail else store.delivery_sla)


@app.get("/api/customers/segments", tags=["customers"])
def segments():
    return from_warehouse(store.segments)


@app.get("/api/customers/{customer_id}", tags=["customers"])
def customer(customer_id: int):
    """Masked profile, RFM segment, SCD2 history and recent orders."""
    row = from_warehouse(store.customer, customer_id)
    if row is None:
        raise HTTPException(404, f"Customer {customer_id} not found")
    return row


@app.get("/api/products/{product_id}/price-history", tags=["products"])
def price_history(product_id: int):
    rows = from_warehouse(store.price_history, product_id)
    if not rows:
        raise HTTPException(404, f"Product {product_id} not found")
    return rows


@app.get("/api/abandoned-carts/daily", tags=["funnel"])
def abandoned_daily(days: int = Query(14, ge=1, le=90)):
    return from_warehouse(store.abandoned_daily, days)


@app.get("/api/data-quality", tags=["ops"])
def data_quality():
    return from_warehouse(store.data_quality)
