-- =====================================================================
-- shop: the online store's operational database (OLTP), in 3NF.
-- Debezium streams every change from these tables to topics
-- named shop.public.<table>.
-- =====================================================================

CREATE TABLE customers (
    customer_id      BIGSERIAL PRIMARY KEY,
    first_name       TEXT        NOT NULL,
    last_name        TEXT        NOT NULL,
    email            TEXT        NOT NULL UNIQUE,      -- PII: masked in the warehouse
    phone            TEXT,                             -- PII
    province         TEXT        NOT NULL,
    city             TEXT        NOT NULL,
    marketing_opt_in BOOLEAN     NOT NULL DEFAULT false,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE products (
    product_id       BIGSERIAL PRIMARY KEY,
    sku              TEXT        NOT NULL UNIQUE,
    name             TEXT        NOT NULL,
    category         TEXT        NOT NULL,
    price            NUMERIC(10, 2) NOT NULL CHECK (price > 0),
    cost             NUMERIC(10, 2) NOT NULL CHECK (cost > 0),
    is_active        BOOLEAN     NOT NULL DEFAULT true,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE inventory (
    product_id       BIGINT PRIMARY KEY REFERENCES products (product_id),
    stock_on_hand    INT         NOT NULL CHECK (stock_on_hand >= 0),  -- can't sell what we don't have
    reorder_level    INT         NOT NULL DEFAULT 10,
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE orders (
    order_id         BIGSERIAL PRIMARY KEY,
    customer_id      BIGINT      NOT NULL REFERENCES customers (customer_id),
    session_id       TEXT,                             -- links the order to its clickstream session
    status           TEXT        NOT NULL DEFAULT 'PLACED'
                     CHECK (status IN ('PLACED', 'PAID', 'SHIPPED', 'DELIVERED', 'CANCELLED', 'RETURNED')),
    payment_method   TEXT        NOT NULL CHECK (payment_method IN ('CARD', 'EFT', 'CASH_ON_DELIVERY')),
    shipping_fee     NUMERIC(10, 2) NOT NULL DEFAULT 0,
    order_total      NUMERIC(12, 2) NOT NULL,          -- items + shipping
    ship_province    TEXT        NOT NULL,
    placed_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE order_items (
    order_id         BIGINT      NOT NULL REFERENCES orders (order_id),
    product_id       BIGINT      NOT NULL REFERENCES products (product_id),
    quantity         INT         NOT NULL CHECK (quantity > 0),
    unit_price       NUMERIC(10, 2) NOT NULL,          -- price at time of sale, not today's price
    PRIMARY KEY (order_id, product_id)
);

CREATE INDEX idx_orders_customer ON orders (customer_id);
CREATE INDEX idx_orders_status   ON orders (status);

CREATE FUNCTION touch_updated_at() RETURNS trigger AS $$
BEGIN
    NEW.updated_at := now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_customers BEFORE UPDATE ON customers FOR EACH ROW EXECUTE FUNCTION touch_updated_at();
CREATE TRIGGER trg_products  BEFORE UPDATE ON products  FOR EACH ROW EXECUTE FUNCTION touch_updated_at();
CREATE TRIGGER trg_inventory BEFORE UPDATE ON inventory FOR EACH ROW EXECUTE FUNCTION touch_updated_at();
CREATE TRIGGER trg_orders    BEFORE UPDATE ON orders    FOR EACH ROW EXECUTE FUNCTION touch_updated_at();

-- Full before-images on UPDATE/DELETE, so CDC events carry the old row too.
ALTER TABLE customers   REPLICA IDENTITY FULL;
ALTER TABLE products    REPLICA IDENTITY FULL;
ALTER TABLE inventory   REPLICA IDENTITY FULL;
ALTER TABLE orders      REPLICA IDENTITY FULL;
ALTER TABLE order_items REPLICA IDENTITY FULL;
