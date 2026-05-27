"""
producer_ecommerce.py

Producer that reads rows from `ecommerce.csv` and publishes them to Kafka.

Run:
    python producer_ecommerce.py

Requirements:
    pip install confluent-kafka
"""

import csv
import json
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic


BOOTSTRAP_SERVERS = "localhost:19092,localhost:19093,localhost:19094"
TOPIC_NAME = "ecommerce_events"
PARTITIONS = 3
REPLICATION_FACTOR = 3
CSV_FILE = "ecommerce.csv"


def create_topic_if_needed():
    admin = AdminClient({"bootstrap.servers": BOOTSTRAP_SERVERS})

    existing_topics = admin.list_topics(timeout=10).topics

    if TOPIC_NAME in existing_topics:
        print(f"Topic already exists: {TOPIC_NAME}")
        return

    topic = NewTopic(
        topic=TOPIC_NAME,
        num_partitions=PARTITIONS,
        replication_factor=REPLICATION_FACTOR
    )

    futures = admin.create_topics([topic])

    try:
        futures[TOPIC_NAME].result()
        print(
            f"Created topic: {TOPIC_NAME} "
            f"with {PARTITIONS} partitions and replication factor {REPLICATION_FACTOR}"
        )
    except Exception as exc:
        print(f"Could not create topic {TOPIC_NAME}: {exc}")


def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed: {err}")
    else:
        print(
            f"Delivered key={msg.key().decode('utf-8') if msg.key() else None} "
            f"to topic={msg.topic()} partition={msg.partition()} offset={msg.offset()}"
        )


def read_csv_rows(path):
    with open(path, newline='', encoding='utf-8') as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            # Clean whitespace in column names and values
            clean_row = {k.strip(): (v.strip() if isinstance(v, str) else v) for k, v in row.items()}
            yield clean_row


def main():
    create_topic_if_needed()

    producer = Producer({
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "acks": "all"
    })

    print(f"Reading CSV: {CSV_FILE} and producing to topic: {TOPIC_NAME}")

    for idx, row in enumerate(read_csv_rows(CSV_FILE), start=1):
        # KEY CHOICE: Order ID
        # An Order ID uniquely identifies a single purchase transaction.
        # Using it as the Kafka key guarantees that all messages belonging to
        # the same order (e.g. multi-item orders split across rows) are always
        # routed to the same partition, preserving per-order ordering.
        # Consumers that reconstruct order-level state never have to merge
        # records scattered across partitions.
        key = row.get("Order ID") or row.get("Cust ID") or f"row-{idx}"

        producer.produce(
            topic=TOPIC_NAME,
            key=str(key),
            value=json.dumps(row),
            callback=delivery_report
        )

        producer.poll(0)

    producer.flush()
    print("Producer finished sending ecommerce rows.")


if __name__ == "__main__":
    main()
