"""The store simulator: produces everything CartPulse ingests.

* Shop database (PostgreSQL): on first start it seeds a catalogue, customers
  and ~60 days of order history. Then it keeps the database busy: new orders
  (with ACID inventory updates), orders moving PLACED -> PAID -> SHIPPED ->
  DELIVERED, price changes, restocks and customer edits. Debezium captures
  every one of those changes (CDC).
* Clickstream (Kafka topic shop.clickstream): concurrent shopping sessions
  emit page views, product views, add/remove-from-cart, checkout and purchase
  events. ~2% of events are delayed by up to 150 s to exercise watermarks.
* Courier files (data/inbox/courier/*.csv): a delivery partner reports
  collections and deliveries, with realistic data-quality problems.
"""
from __future__ import annotations

import csv
import heapq
import json
import logging
import os
import random
import signal
import time
import uuid
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone

import psycopg

from cartpulse import catalog, config

log = logging.getLogger("simulator")

SESSIONS_PER_MINUTE = float(os.environ.get("SESSIONS_PER_MINUTE", "24"))
HISTORY_DAYS = int(os.environ.get("HISTORY_DAYS", "60"))
COURIER_FILE_EVERY_SEC = float(os.environ.get("COURIER_FILE_EVERY_SEC", "180"))
LATE_EVENT_RATE = 0.02
SAST = timezone(timedelta(hours=2))
FREE_SHIPPING_FROM = 500
SHIPPING_FEE = 75

running = True


def _stop(*_):
    global running
    running = False


def weighted(rnd: random.Random, options: dict):
    return rnd.choices(list(options), weights=list(options.values()))[0]


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def iso(ts: datetime) -> str:
    return ts.astimezone(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


@dataclass
class Product:
    product_id: int
    name: str
    category: str
    price: float


@dataclass
class Session:
    session_id: str
    customer_id: int | None
    device: str
    source: str
    interest: str
    intent: float
    next_at: float
    stage: str = "browse"
    events: int = 0
    last_product: int | None = None
    cart: dict = field(default_factory=dict)        # product_id -> [qty, unit_price]

    def cart_value(self) -> float:
        return round(sum(q * p for q, p in self.cart.values()), 2)


@dataclass
class LiveOrder:
    order_id: int
    status: str
    payment: str
    province: str
    next_at: float
    waybill: str | None = None
    courier: str | None = None
    collected_at: datetime | None = None
    attempts: int = 0


class Simulator:
    def __init__(self, seed: int | None = None, producer=None):
        self.rnd = random.Random(seed)
        self.conn: psycopg.Connection | None = None
        self.producer = producer
        self.products: dict[int, Product] = {}
        self.by_category: dict[str, list[int]] = {}
        self.customers: list[int] = []
        self.sessions: dict[str, Session] = {}
        self.orders: dict[int, LiveOrder] = {}
        self.late: list[tuple[float, int, dict]] = []    # heap of (release_at, seq, event)
        self._seq = 0
        self.stats = {"sessions": 0, "events": 0, "late_events": 0, "orders": 0, "abandoned": 0,
                      "out_of_stock": 0, "courier_files": 0}

    # ================================================================ database
    def connect(self) -> None:
        for attempt in range(60):
            try:
                self.conn = psycopg.connect(config.SHOP_DSN, autocommit=True)
                return
            except psycopg.OperationalError as exc:
                log.info("waiting for shop DB (%s): %s", attempt + 1, exc)
                time.sleep(2)
        raise SystemExit("shop DB unreachable")

    def fake_customer(self, created_at: datetime) -> tuple:
        rnd = self.rnd
        first, last = rnd.choice(catalog.FIRST_NAMES), rnd.choice(catalog.LAST_NAMES)
        province = weighted(rnd, {p: w for p, (w, _) in catalog.PROVINCES.items()})
        city = rnd.choice(catalog.PROVINCES[province][1])
        email = f"{first}.{last}.{uuid.uuid4().hex[:6]}@example.co.za".lower().replace(" ", "")
        phone = "+27" + rnd.choice("678") + "".join(str(rnd.randint(0, 9)) for _ in range(8))
        return first, last, email, phone, province, city, rnd.random() < 0.4, created_at

    def insert_customer(self, cur, created_at: datetime | None = None) -> int:
        cur.execute(
            """INSERT INTO customers (first_name, last_name, email, phone, province, city, marketing_opt_in, created_at)
               VALUES (%s,%s,%s,%s,%s,%s,%s,%s) RETURNING customer_id""",
            self.fake_customer(created_at or utcnow()))
        customer_id = cur.fetchone()[0]
        self.customers.append(customer_id)
        return customer_id

    def seed_if_empty(self) -> bool:
        with self.conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM products")
            if cur.fetchone()[0]:
                log.info("shop DB already seeded")
                return False
        log.info("seeding catalogue, customers and %s days of history...", HISTORY_DAYS)
        now = utcnow()
        with self.conn.transaction(), self.conn.cursor() as cur:
            sku = 1000
            for category, items in catalog.PRODUCTS.items():
                for name, typical in items:
                    sku += 1
                    price = round(typical * self.rnd.uniform(0.9, 1.1)) - 0.01
                    cost = round(price * self.rnd.uniform(0.45, 0.7), 2)
                    cur.execute(
                        """INSERT INTO products (sku, name, category, price, cost, created_at)
                           VALUES (%s,%s,%s,%s,%s,%s) RETURNING product_id""",
                        (f"CP-{sku}", name, category, price, cost, now - timedelta(days=400)))
                    pid = cur.fetchone()[0]
                    cur.execute("INSERT INTO inventory (product_id, stock_on_hand, reorder_level) VALUES (%s,%s,%s)",
                                (pid, self.rnd.randint(3, 14) if self.rnd.random() < 0.15 else self.rnd.randint(20, 160), 10))
            for _ in range(400):
                self.insert_customer(cur, now - timedelta(days=self.rnd.uniform(HISTORY_DAYS, 500)))
        self.load_state()
        self.backfill_history(now)
        return True

    def load_state(self) -> None:
        with self.conn.cursor() as cur:
            cur.execute("SELECT product_id, name, category, price FROM products WHERE is_active")
            self.products = {pid: Product(pid, n, c, float(p)) for pid, n, c, p in cur.fetchall()}
            cur.execute("SELECT customer_id FROM customers")
            self.customers = [r[0] for r in cur.fetchall()]
            cur.execute("""SELECT order_id, status, payment_method, ship_province FROM orders
                           WHERE status IN ('PLACED', 'PAID', 'SHIPPED')""")
            for oid, status, payment, province in cur.fetchall():
                o = LiveOrder(oid, status, payment, province, time.monotonic() + self.rnd.uniform(5, 60))
                if status == "SHIPPED":
                    o.waybill, o.courier, o.collected_at = self.new_waybill(), self.rnd.choice(catalog.COURIERS), utcnow()
                self.orders[oid] = o
        self.by_category = {}
        for p in self.products.values():
            self.by_category.setdefault(p.category, []).append(p.product_id)
        log.info("state: %s products, %s customers, %s open orders",
                 len(self.products), len(self.customers), len(self.orders))

    # ------------------------------------------------------------- history
    def demand_multiplier(self, day: date) -> float:
        m = 1.0
        if day.weekday() >= 5:
            m *= 1.3
        if day.day >= 25 or day.day == 1:          # month-end payday spike
            m *= 1.6
        if day.isoformat() in catalog.SA_HOLIDAYS_2026:
            m *= 1.4
        return m

    def backfill_history(self, now: datetime) -> None:
        """~60 days of finished orders so the marts (trends, RFM, SLA) are meaningful from minute one."""
        rnd = self.rnd
        pids = list(self.products)
        hour_weights = [1, 1, 1, 1, 1, 2, 3, 5, 6, 7, 8, 9, 11, 10, 8, 7, 7, 8, 10, 12, 12, 10, 6, 3]
        deliveries = []
        n_orders = 0
        with self.conn.transaction(), self.conn.cursor() as cur:
            for days_ago in range(HISTORY_DAYS, 0, -1):
                day = (now - timedelta(days=days_ago)).astimezone(SAST).date()
                n = max(5, int(rnd.gauss(38, 6) * self.demand_multiplier(day)))
                for _ in range(n):
                    hour = rnd.choices(range(24), weights=hour_weights)[0]
                    placed = datetime(day.year, day.month, day.day, hour, rnd.randint(0, 59),
                                      rnd.randint(0, 59), tzinfo=SAST)
                    # Exponential pick over customers: a loyal minority orders often (makes RFM interesting).
                    customer = self.customers[min(len(self.customers) - 1, int(rnd.expovariate(1 / 90)))]
                    basket = {}
                    for _ in range(rnd.choices([1, 2, 3, 4], weights=[55, 28, 12, 5])[0]):
                        pid = rnd.choice(pids)
                        basket[pid] = (rnd.choices([1, 2, 3], weights=[80, 15, 5])[0],
                                       round(self.products[pid].price * rnd.uniform(0.92, 1.05), 2))
                    subtotal = sum(q * p for q, p in basket.values())
                    fee = 0 if subtotal >= FREE_SHIPPING_FROM else SHIPPING_FEE
                    status = rnd.choices(["DELIVERED", "CANCELLED", "RETURNED"], weights=[90, 6, 4])[0]
                    if days_ago <= 2 and status == "DELIVERED" and rnd.random() < 0.6:
                        status = "SHIPPED"
                    payment = weighted(rnd, catalog.PAYMENT_METHODS)
                    province = weighted(rnd, {p: w for p, (w, _) in catalog.PROVINCES.items()})
                    cur.execute(
                        """INSERT INTO orders (customer_id, status, payment_method, shipping_fee, order_total,
                                               ship_province, placed_at)
                           VALUES (%s,%s,%s,%s,%s,%s,%s) RETURNING order_id""",
                        (customer, status, payment, fee, round(subtotal + fee, 2), province, placed))
                    oid = cur.fetchone()[0]
                    cur.executemany("INSERT INTO order_items VALUES (%s,%s,%s,%s)",
                                    [(oid, pid, q, p) for pid, (q, p) in basket.items()])
                    n_orders += 1
                    if status in ("DELIVERED", "RETURNED"):        # SHIPPED ones are delivered live later
                        deliveries.append(self.historic_delivery(oid, province, placed, now))
        log.info("backfilled %s historic orders", n_orders)
        for courier in catalog.COURIERS:
            rows = [d for d in deliveries if d["courier"] == courier]
            self.write_courier_file(rows, f"courier_{courier.split()[0].lower()}_history.csv")

    def historic_delivery(self, oid, province, placed, now) -> dict:
        rnd = self.rnd
        courier = rnd.choice(catalog.COURIERS)
        collected = placed + timedelta(hours=rnd.uniform(6, 30))
        days = max(0.3, rnd.gauss(catalog.COURIER_SPEED[courier], 0.9))
        if province in catalog.REMOTE_PROVINCES:
            days += 1
        delivered = min(collected + timedelta(days=days), now - timedelta(hours=1))
        return {"waybill": self.new_waybill(), "order_id": oid, "courier": courier, "province": province,
                "collected_at": collected, "delivered_at": delivered, "status": "DELIVERED",
                "attempts": 1 if rnd.random() < 0.88 else 2}

    def new_waybill(self) -> str:
        return "WB" + uuid.uuid4().hex[:10].upper()

    # =============================================================== clickstream
    def emit(self, session: Session, event_type: str, **fields) -> dict:
        event = {
            "event_id": str(uuid.uuid4()),
            "event_type": event_type,
            "session_id": session.session_id,
            "customer_id": session.customer_id,
            "device": session.device,
            "traffic_source": session.source,
            "event_time": iso(utcnow()),
            **fields,
        }
        session.events += 1
        self.stats["events"] += 1
        if self.rnd.random() < LATE_EVENT_RATE:          # network hiccup: deliver it late
            self._seq += 1
            heapq.heappush(self.late, (time.monotonic() + self.rnd.uniform(20, 150), self._seq, event))
            self.stats["late_events"] += 1
        else:
            self.send(event)
        return event

    def send(self, event: dict) -> None:
        if self.producer is not None:
            # Keyed by session: one session's events stay ordered on one partition.
            self.producer.produce(config.CLICKSTREAM_TOPIC, key=event["session_id"], value=json.dumps(event))
            self.producer.poll(0)

    def flush_late(self) -> None:
        now = time.monotonic()
        while self.late and self.late[0][0] <= now:
            self.send(heapq.heappop(self.late)[2])

    def start_session(self) -> None:
        rnd = self.rnd
        source = weighted(rnd, catalog.TRAFFIC_SOURCES)
        s = Session(
            session_id="S-" + uuid.uuid4().hex[:16],
            customer_id=rnd.choice(self.customers) if rnd.random() < 0.45 else None,   # logged in?
            device=weighted(rnd, catalog.DEVICES),
            source=source,
            interest=rnd.choice(list(self.by_category)),
            intent=catalog.SOURCE_INTENT[source] * rnd.uniform(0.6, 1.4),
            next_at=time.monotonic(),
        )
        self.sessions[s.session_id] = s
        self.stats["sessions"] += 1
        if source in ("paid_search", "social") and rnd.random() < 0.6:       # ads land on a product page
            pid = self.pick_product(s)
            s.last_product = pid
            self.emit(s, "product_view", page=f"/p/{pid}", product_id=pid, price=self.products[pid].price)
        else:
            self.emit(s, "page_view", page=rnd.choice(["/", "/", f"/c/{s.interest.lower().replace(' ', '-')}", "/deals"]))
        s.next_at = time.monotonic() + rnd.uniform(2, 20)

    def pick_product(self, s: Session) -> int:
        if self.rnd.random() < 0.7:
            return self.rnd.choice(self.by_category[s.interest])
        return self.rnd.choice(list(self.products))

    def step(self, s: Session) -> None:
        rnd = self.rnd
        if s.stage == "checkout":
            if rnd.random() < min(0.9, 0.62 * s.intent):
                self.purchase(s)
            else:
                self.end(s)                      # abandoned at checkout
            return
        fatigue = max(0, s.events - 6) * 3
        options = {"product_view": 45, "page_view": 18, "leave": 8 + fatigue}
        if s.last_product:
            options["add_to_cart"] = 16 * s.intent
        if s.cart:
            options["remove_from_cart"] = 3
            options["begin_checkout"] = 14 * s.intent
        action = weighted(rnd, options) if s.events < 30 else "leave"

        if action == "product_view":
            pid = self.pick_product(s)
            s.last_product = pid
            self.emit(s, "product_view", page=f"/p/{pid}", product_id=pid, price=self.products[pid].price)
        elif action == "page_view":
            self.emit(s, "page_view", page=rnd.choice(["/", "/search", "/deals", f"/c/{s.interest.lower().replace(' ', '-')}"]))
        elif action == "add_to_cart":
            pid, qty = s.last_product, rnd.choices([1, 2, 3], weights=[80, 15, 5])[0]
            price = self.products[pid].price
            s.cart.setdefault(pid, [0, price])[0] += qty
            self.emit(s, "add_to_cart", page=f"/p/{pid}", product_id=pid, quantity=qty, price=price)
            s.last_product = None
        elif action == "remove_from_cart":
            pid = rnd.choice(list(s.cart))
            qty, price = s.cart.pop(pid)
            self.emit(s, "remove_from_cart", page="/cart", product_id=pid, quantity=qty, price=price)
        elif action == "begin_checkout":
            s.stage = "checkout"
            self.emit(s, "begin_checkout", page="/checkout", cart_value=s.cart_value())
        else:
            self.end(s)
            return
        s.next_at = time.monotonic() + rnd.uniform(2, 25)

    def end(self, s: Session) -> None:
        if s.cart:
            self.stats["abandoned"] += 1
        self.sessions.pop(s.session_id, None)

    # =================================================================== orders
    def purchase(self, s: Session) -> None:
        """Place the order in ONE database transaction: stock check, order, items, stock decrement."""
        rnd = self.rnd
        with self.conn.transaction(), self.conn.cursor() as cur:
            if s.customer_id is None:            # guest logs in, or signs up (a CDC insert)
                s.customer_id = self.insert_customer(cur) if rnd.random() < 0.4 else rnd.choice(self.customers)
            pids = sorted(s.cart)                # fixed lock order: no deadlocks
            cur.execute("SELECT product_id, stock_on_hand FROM inventory WHERE product_id = ANY(%s) "
                        "ORDER BY product_id FOR UPDATE", (pids,))
            stock = dict(cur.fetchall())
            items = []
            for pid in pids:
                qty, price = s.cart[pid]
                qty = min(qty, stock.get(pid, 0))
                if qty > 0:
                    items.append((pid, qty, price))
            if not items:
                self.stats["out_of_stock"] += 1
                self.end(s)
                return
            subtotal = round(sum(q * p for _, q, p in items), 2)
            fee = 0 if subtotal >= FREE_SHIPPING_FROM else SHIPPING_FEE
            payment = weighted(rnd, catalog.PAYMENT_METHODS)
            cur.execute("SELECT province FROM customers WHERE customer_id = %s", (s.customer_id,))
            province = cur.fetchone()[0]
            cur.execute(
                """INSERT INTO orders (customer_id, session_id, payment_method, shipping_fee, order_total, ship_province)
                   VALUES (%s,%s,%s,%s,%s,%s) RETURNING order_id""",
                (s.customer_id, s.session_id, payment, fee, round(subtotal + fee, 2), province))
            order_id = cur.fetchone()[0]
            cur.executemany("INSERT INTO order_items VALUES (%s,%s,%s,%s)", [(order_id, *i) for i in items])
            cur.executemany("UPDATE inventory SET stock_on_hand = stock_on_hand - %s WHERE product_id = %s",
                            [(q, pid) for pid, q, _ in items])
        self.orders[order_id] = LiveOrder(order_id, "PLACED", payment, province,
                                          time.monotonic() + rnd.uniform(5, 40))
        self.stats["orders"] += 1
        self.emit(s, "purchase", page="/checkout/success", order_id=order_id,
                  cart_value=round(subtotal + fee, 2), items=sum(q for _, q, _ in items))
        s.cart.clear()
        self.end(s)

    def set_status(self, order_id: int, status: str) -> None:
        with self.conn.cursor() as cur:
            cur.execute("UPDATE orders SET status = %s WHERE order_id = %s", (status, order_id))

    def advance_orders(self) -> None:
        now = time.monotonic()
        rnd = self.rnd
        for o in list(self.orders.values()):
            if o.next_at > now or o.status == "SHIPPED":
                continue                                    # SHIPPED orders move when the courier reports
            if o.status == "PLACED":
                if rnd.random() < 0.04:
                    o.status = "CANCELLED"
                elif o.payment == "CASH_ON_DELIVERY":
                    o.status = "SHIPPED"
                else:
                    o.status = "PAID"
            elif o.status == "PAID":
                o.status = "SHIPPED"
            self.set_status(o.order_id, o.status)
            if o.status == "SHIPPED":
                o.waybill, o.courier, o.collected_at = self.new_waybill(), rnd.choice(catalog.COURIERS), utcnow()
            if o.status == "CANCELLED":
                self.orders.pop(o.order_id)
            o.next_at = now + rnd.uniform(40, 150)

    def courier_cycle(self) -> None:
        """Couriers report on shipped orders; deliveries also update the shop DB (via webhook, in real life)."""
        rnd = self.rnd
        now = utcnow()
        rows = []
        for o in list(self.orders.values()):
            if o.status != "SHIPPED" or (now - o.collected_at).total_seconds() < 90:
                continue
            o.attempts += 1
            roll = rnd.random()
            if roll < 0.82 or o.attempts >= 3:
                status = "RETURNED_TO_SENDER" if (o.attempts >= 3 and roll >= 0.82) else "DELIVERED"
                self.set_status(o.order_id, "DELIVERED" if status == "DELIVERED" else "RETURNED")
                self.orders.pop(o.order_id)
            else:
                status = "FAILED_ATTEMPT"
            rows.append({"waybill": o.waybill, "order_id": o.order_id, "courier": o.courier, "province": o.province,
                         "collected_at": o.collected_at, "delivered_at": now if status == "DELIVERED" else None,
                         "status": status, "attempts": o.attempts})
        if rows:
            for courier in {r["courier"] for r in rows}:
                stamp = now.astimezone(SAST).strftime("%Y%m%dT%H%M%S")
                self.write_courier_file([r for r in rows if r["courier"] == courier],
                                        f"courier_{courier.split()[0].lower()}_{stamp}.csv")

    def write_courier_file(self, rows: list[dict], name: str) -> None:
        """Partner CSV, deliberately imperfect: formats, duplicates, gaps, contradictions."""
        rnd = self.rnd
        out = []
        for r in rows:
            row = {
                "waybill": r["waybill"], "order_id": str(r["order_id"]), "courier": r["courier"],
                "province": r["province"],
                "collected_at": r["collected_at"].astimezone(SAST).strftime("%Y-%m-%d %H:%M:%S"),
                "delivered_at": r["delivered_at"].astimezone(SAST).strftime("%Y-%m-%d %H:%M:%S") if r["delivered_at"] else "",
                "status": r["status"], "attempts": str(r["attempts"]),
            }
            roll = rnd.random()
            if roll < 0.01:
                row["order_id"] = ""                                         # completeness
            elif roll < 0.02:
                row["status"] = row["status"].lower()                        # fixable: casing
            elif roll < 0.03 and row["delivered_at"]:
                d = datetime.strptime(row["delivered_at"], "%Y-%m-%d %H:%M:%S")
                row["delivered_at"] = d.strftime("%d/%m/%Y %H:%M")           # fixable: other date format
            elif roll < 0.035 and row["delivered_at"]:
                row["delivered_at"] = (r["collected_at"] - timedelta(hours=5)).astimezone(SAST).strftime("%Y-%m-%d %H:%M:%S")  # consistency
            elif roll < 0.045:
                out.append(dict(row))                                        # uniqueness: duplicate
            out.append(row)
        rnd.shuffle(out)
        folder = config.INBOX / "courier"
        folder.mkdir(parents=True, exist_ok=True)
        tmp = folder / f".{name}.part"
        with open(tmp, "w", newline="") as fh:
            writer = csv.DictWriter(fh, fieldnames=["waybill", "order_id", "courier", "province", "collected_at",
                                                    "delivered_at", "status", "attempts"])
            writer.writeheader()
            writer.writerows(out)
        os.chmod(tmp, 0o666)
        tmp.rename(folder / name)            # atomic: the pipeline never sees half a file
        self.stats["courier_files"] += 1
        log.info("courier file %s (%s rows)", name, len(out))

    # ============================================================= other changes
    def change_price(self) -> None:
        p = self.products[self.rnd.choice(list(self.products))]
        new = round(p.price * self.rnd.uniform(0.85, 1.15) / 10) * 10 - 0.01
        with self.conn.cursor() as cur:
            cur.execute("UPDATE products SET price = greatest(%s, cost * 1.1) WHERE product_id = %s RETURNING price",
                        (new, p.product_id))
            p.price = float(cur.fetchone()[0])

    def restock(self) -> None:
        with self.conn.cursor() as cur:
            cur.execute("""UPDATE inventory SET stock_on_hand = stock_on_hand + (40 + floor(random() * 80))::int
                           WHERE product_id IN (SELECT product_id FROM inventory
                                                WHERE stock_on_hand <= reorder_level AND random() < 0.3)""")

    def edit_customer(self) -> None:
        province = weighted(self.rnd, {p: w for p, (w, _) in catalog.PROVINCES.items()})
        with self.conn.cursor() as cur:
            if self.rnd.random() < 0.6:          # moved house
                cur.execute("UPDATE customers SET province = %s, city = %s WHERE customer_id = %s",
                            (province, self.rnd.choice(catalog.PROVINCES[province][1]), self.rnd.choice(self.customers)))
            else:
                cur.execute("UPDATE customers SET marketing_opt_in = NOT marketing_opt_in WHERE customer_id = %s",
                            (self.rnd.choice(self.customers),))

    # =================================================================== loop
    def run(self) -> None:
        from confluent_kafka import Producer
        self.connect()
        self.seed_if_empty()
        self.load_state()
        self.producer = Producer({"bootstrap.servers": config.KAFKA_BOOTSTRAP, "enable.idempotence": True,
                                  "linger.ms": 50, "compression.type": "lz4"})
        timers = {"orders": 0.0, "courier": time.monotonic() + COURIER_FILE_EVERY_SEC, "price": 0.0,
                  "restock": 0.0, "customer": 0.0, "log": 0.0}
        every = {"orders": 3, "courier": COURIER_FILE_EVERY_SEC, "price": 90, "restock": 60, "customer": 45, "log": 30}
        actions = {"orders": self.advance_orders, "courier": self.courier_cycle, "price": self.change_price,
                   "restock": self.restock, "customer": self.edit_customer,
                   "log": lambda: log.info("%s | live sessions %s", self.stats, len(self.sessions))}
        tick = 0.2
        log.info("store open: ~%s sessions/minute", SESSIONS_PER_MINUTE)
        while running:
            if self.rnd.random() < SESSIONS_PER_MINUTE / 60 * tick:
                self.start_session()
            now = time.monotonic()
            for s in [s for s in self.sessions.values() if s.next_at <= now]:
                try:
                    self.step(s)
                except psycopg.Error as exc:
                    log.warning("db error in session %s: %s", s.session_id, exc)
                    self.end(s)
            self.flush_late()
            for name, due in timers.items():
                if now >= due:
                    try:
                        actions[name]()
                    except psycopg.Error as exc:
                        log.warning("%s failed: %s", name, exc)
                    timers[name] = now + every[name]
            time.sleep(tick)
        log.info("closing: flushing producer")
        self.producer.flush(10)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
    signal.signal(signal.SIGTERM, _stop)
    signal.signal(signal.SIGINT, _stop)
    Simulator().run()


if __name__ == "__main__":
    main()
