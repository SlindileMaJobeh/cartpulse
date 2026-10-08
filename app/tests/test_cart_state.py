from cartpulse.streaming import cart_state as cs


def e(kind, t, **kw):
    return {"event_type": kind, "event_time_ms": t, **kw}


def test_add_remove_and_value():
    c = cs.apply_events(cs.new_cart(), [e("add_to_cart", 1000, product_id=1, quantity=2, price=50.0),
                                        e("add_to_cart", 2000, product_id=2, quantity=1, price=10.0),
                                        e("remove_from_cart", 3000, product_id=2)])
    assert cs.cart_value(c) == 100.0 and cs.is_abandoned(c) and c["last_activity_ms"] == 3000


def test_purchase_converts_and_late_add_cannot_revive():
    c = cs.apply_events(cs.new_cart(), [e("add_to_cart", 1000, product_id=1, quantity=1, price=5.0),
                                        e("purchase", 2000)])
    c = cs.apply_events(c, [e("add_to_cart", 1500, product_id=9, quantity=1, price=5.0)])   # late event
    assert c["converted"] and not cs.is_abandoned(c)


def test_out_of_order_events_are_applied_in_event_time():
    c = cs.apply_events(cs.new_cart(), [e("remove_from_cart", 2000, product_id=1),
                                        e("add_to_cart", 1000, product_id=1, quantity=1, price=5.0)])
    assert c["items"] == {}


def test_customer_is_remembered_after_login():
    c = cs.apply_events(cs.new_cart(), [e("page_view", 1), e("add_to_cart", 2, product_id=1, price=1.0, customer_id=42)])
    assert c["customer_id"] == 42


def test_timeout_never_before_watermark():
    c = {**cs.new_cart(), "last_activity_ms": 1_000}
    assert cs.timeout_ms(c, 300_000, 0) == 301_000
    assert cs.timeout_ms(c, 300_000, 900_000) == 900_001
