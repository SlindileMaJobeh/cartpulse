"""Shopping-cart state machine for abandoned-cart detection.

Pure Python (no Spark), so it is unit-tested directly; the stream job calls it
from `applyInPandasWithState`, one call per session per micro-batch.

A cart is *abandoned* when it still holds items and the session has had no
activity for ABANDON_AFTER_MINUTES of event time. A purchase converts the
session, so it can never be reported as abandoned afterwards.
"""
from __future__ import annotations


def new_cart() -> dict:
    return {"items": {}, "customer_id": None, "last_activity_ms": 0, "converted": False}


def apply_events(cart: dict, events: list[dict]) -> dict:
    """Fold events (dicts with event_type, product_id, quantity, price, customer_id,
    event_time_ms) into the cart. Events are applied in event-time order, so a
    late-arriving add_to_cart lands in the right place."""
    cart = {**cart, "items": dict(cart["items"])}
    for e in sorted(events, key=lambda e: e["event_time_ms"]):
        cart["last_activity_ms"] = max(cart["last_activity_ms"], int(e["event_time_ms"]))
        if e.get("customer_id") is not None:
            cart["customer_id"] = int(e["customer_id"])
        kind = e["event_type"]
        pid = None if e.get("product_id") is None else str(int(e["product_id"]))
        if kind == "add_to_cart" and pid is not None:
            qty, _ = cart["items"].get(pid, [0, 0.0])
            cart["items"][pid] = [qty + int(e.get("quantity") or 1), float(e.get("price") or 0.0)]
        elif kind == "remove_from_cart" and pid is not None:
            cart["items"].pop(pid, None)
        elif kind == "purchase":
            cart["converted"] = True
            cart["items"] = {}
    return cart


def cart_value(cart: dict) -> float:
    return round(sum(q * p for q, p in cart["items"].values()), 2)


def is_abandoned(cart: dict) -> bool:
    return not cart["converted"] and bool(cart["items"])


def timeout_ms(cart: dict, abandon_after_ms: int, watermark_ms: int) -> int:
    """When to fire. Spark rejects timeouts at or before the current watermark."""
    return max(cart["last_activity_ms"] + abandon_after_ms, watermark_ms + 1)
