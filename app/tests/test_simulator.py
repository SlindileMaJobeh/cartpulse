import csv

from cartpulse import config
from cartpulse.simulator import Product, Simulator


class Capture:
    def __init__(self):
        self.events = []

    def produce(self, topic, key, value):
        import json
        self.events.append((key, json.loads(value)))

    def poll(self, *_):
        pass


def make_sim():
    sim = Simulator(seed=1, producer=Capture())
    sim.products = {i: Product(i, f"P{i}", "Home" if i % 2 else "Books", 100.0 + i) for i in range(1, 11)}
    sim.by_category = {"Home": [1, 3, 5, 7, 9], "Books": [2, 4, 6, 8, 10]}
    sim.customers = [1, 2, 3]
    return sim


def test_sessions_emit_keyed_valid_events():
    sim = make_sim()
    sim.purchase = lambda s: sim.end(s)            # no DB in this test
    for _ in range(30):
        sim.start_session()
    for _ in range(40):
        for s in list(sim.sessions.values()):
            sim.step(s)
    events = sim.producer.events + [(e["session_id"], e) for _, _, e in sim.late]
    kinds = {e["event_type"] for _, e in events}
    assert {"page_view", "product_view", "add_to_cart"} <= kinds
    assert all(key == e["session_id"] for key, e in events)        # partition key = session
    assert all(e["event_time"].endswith("Z") for _, e in events)


def test_courier_file_has_header_and_rows(tmp_path, monkeypatch):
    from datetime import datetime, timezone
    monkeypatch.setattr(config, "INBOX", tmp_path)
    sim = make_sim()
    now = datetime.now(timezone.utc)
    rows = [{"waybill": f"WB{i}", "order_id": i, "courier": "SwiftRoute", "province": "Gauteng",
             "collected_at": now, "delivered_at": now, "status": "DELIVERED", "attempts": 1} for i in range(200)]
    sim.write_courier_file(rows, "courier_test.csv")
    with open(tmp_path / "courier" / "courier_test.csv") as fh:
        out = list(csv.DictReader(fh))
    assert 200 <= len(out) <= 215 and set(out[0]) >= {"waybill", "order_id", "status"}
    assert not list((tmp_path / "courier").glob(".*.part"))         # atomic rename happened
