"""One-shot setup, run by the `bootstrap` service before anything else starts.

1. Create the data folders (world-writable, because Airflow runs as a
   different user from the other containers and shares ./data).
2. Create the clickstream topic with 3 partitions.
3. Register the Debezium connector that streams the shop DB into Kafka.

Everything is idempotent, so it is safe to run on every `docker compose up`.
"""
import logging
import os
import time

import requests
from confluent_kafka.admin import AdminClient, NewTopic

from cartpulse import config

log = logging.getLogger(__name__)

CONNECTOR = {
    "name": "shop-cdc",
    "config": {
        "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
        "plugin.name": "pgoutput",
        "database.hostname": "postgres",
        "database.port": "5432",
        "database.user": "shop",
        "database.password": "shop",
        "database.dbname": "shop",
        "topic.prefix": "shop",
        "table.include.list": "public.customers,public.products,public.inventory,public.orders,public.order_items",
        "slot.name": "cartpulse_slot",
        "publication.name": "cartpulse_pub",
        "publication.autocreate.mode": "filtered",
        "snapshot.mode": "initial",
        "decimal.handling.mode": "string",        # NUMERIC -> "123.45", no precision loss
        "tombstones.on.delete": "false",
        "key.converter": "org.apache.kafka.connect.json.JsonConverter",
        "key.converter.schemas.enable": "false",
        "value.converter": "org.apache.kafka.connect.json.JsonConverter",
        "value.converter.schemas.enable": "false",
        "topic.creation.default.replication.factor": "1",
        "topic.creation.default.partitions": "1",
    },
}


def wait_for(what: str, check, attempts: int = 90, delay: float = 2.0):
    for i in range(attempts):
        try:
            return check()
        except Exception as exc:  # noqa: BLE001
            if i % 10 == 0:
                log.info("waiting for %s: %s", what, exc)
            time.sleep(delay)
    raise SystemExit(f"{what} never became ready")


def make_dirs() -> None:
    for d in config.ALL_DIRS:
        d.mkdir(parents=True, exist_ok=True)
    for root, dirs, _ in os.walk(config.DATA_DIR):
        for name in dirs:
            os.chmod(os.path.join(root, name), 0o777)
    os.chmod(config.DATA_DIR, 0o777)
    log.info("data folders ready under %s", config.DATA_DIR)


def create_topics() -> None:
    admin = AdminClient({"bootstrap.servers": config.KAFKA_BOOTSTRAP})
    existing = wait_for("Redpanda", lambda: admin.list_topics(timeout=5).topics)
    if config.CLICKSTREAM_TOPIC in existing:
        log.info("topic %s exists", config.CLICKSTREAM_TOPIC)
        return
    futures = admin.create_topics([NewTopic(config.CLICKSTREAM_TOPIC, num_partitions=3, replication_factor=1)])
    for f in futures.values():
        f.result()
    log.info("created topic %s", config.CLICKSTREAM_TOPIC)


def register_connector() -> None:
    base = config.CONNECT_URL

    def ready():
        requests.get(f"{base}/connectors", timeout=5).raise_for_status()

    wait_for("Kafka Connect", ready)
    name = CONNECTOR["name"]
    # PUT /config creates or updates, so this is idempotent.
    resp = requests.put(f"{base}/connectors/{name}/config", json=CONNECTOR["config"], timeout=30)
    resp.raise_for_status()
    log.info("connector %s registered (HTTP %s)", name, resp.status_code)
    time.sleep(3)
    log.info("status: %s", requests.get(f"{base}/connectors/{name}/status", timeout=5).text)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s bootstrap %(message)s")
    make_dirs()
    create_topics()
    register_connector()


if __name__ == "__main__":
    main()
