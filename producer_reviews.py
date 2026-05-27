"""
producer_reviews.py  —  Member 3 data source

Producer that reads rows from `product_reviews.csv` and publishes them to
the `product_reviews` Kafka topic.

Business context:
    product_reviews carries customer-written star ratings and sentiment labels
    for fashion products sold across multiple e-commerce channels.  Combined
    with ecommerce_events (fulfilment) and sales_events (transactions), it
    gives the analytics pipeline a complete picture: what was sold, whether it
    arrived, and what customers thought of it.

CSV columns:
    ReviewID, ProductID, CustomerID, Category, Channel, Platform,
    Rating (1-5), Sentiment, ReviewDate, HelpfulVotes, VerifiedPurchase

Run:
    python producer_reviews.py

Requirements:
    pip install confluent-kafka
"""

import csv
import json
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

BOOTSTRAP_SERVERS  = "localhost:19092,localhost:19093,localhost:19094"
TOPIC_NAME         = "product_reviews"
PARTITIONS         = 3
REPLICATION_FACTOR = 3
CSV_FILE           = "product_reviews.csv"


# ---------------------------------------------------------------------------
# Topic management
# ---------------------------------------------------------------------------

def create_topic_if_needed() -> None:
    """Create the topic with the configured partition and replication settings."""
    admin = AdminClient({"bootstrap.servers": BOOTSTRAP_SERVERS})
    existing = admin.list_topics(timeout=10).topics

    if TOPIC_NAME in existing:
        print(f"Topic already exists: {TOPIC_NAME}")
        return

    topic = NewTopic(
        topic=TOPIC_NAME,
        num_partitions=PARTITIONS,
        replication_factor=REPLICATION_FACTOR,
    )
    futures = admin.create_topics([topic])
    try:
        futures[TOPIC_NAME].result()
        print(
            f"Created topic: {TOPIC_NAME} "
            f"({PARTITIONS} partitions, RF={REPLICATION_FACTOR})"
        )
    except Exception as exc:
        print(f"Could not create topic {TOPIC_NAME}: {exc}")


# ---------------------------------------------------------------------------
# Delivery callback
# ---------------------------------------------------------------------------

def delivery_report(err, msg) -> None:
    """Print confirmation or error for every produced record."""
    if err is not None:
        print(f"Delivery failed: {err}")
    else:
        key = msg.key().decode("utf-8") if msg.key() else None
        print(
            f"Delivered key={key} "
            f"to topic={msg.topic()} partition={msg.partition()} offset={msg.offset()}"
        )


# ---------------------------------------------------------------------------
# CSV reader
# ---------------------------------------------------------------------------

def read_csv_rows(path: str):
    """Yield each CSV row as a dict, stripping whitespace from keys and values."""
    with open(path, newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            yield {
                k.strip(): (v.strip() if isinstance(v, str) else v)
                for k, v in row.items()
            }


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    """Read product_reviews.csv and produce every row to the product_reviews topic."""
    create_topic_if_needed()

    producer = Producer({
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "acks": "all",
    })

    print(f"Reading CSV: {CSV_FILE}  →  topic: {TOPIC_NAME}")

    for idx, row in enumerate(read_csv_rows(CSV_FILE), start=1):
        # KEY CHOICE: ProductID
        # ProductID groups all reviews for the same product into the same
        # Kafka partition.  This ensures that a product-quality consumer sees
        # every review for a product in arrival order — no cross-partition
        # merge is needed to compute per-product average rating or sentiment
        # trend.  It also naturally co-locates high-volume products (which
        # attract many reviews) on a single partition, making per-product
        # aggregation highly efficient.
        key = row.get("ProductID") or f"row-{idx}"

        producer.produce(
            topic=TOPIC_NAME,
            key=str(key),
            value=json.dumps(row),
            callback=delivery_report,
        )

        producer.poll(0)

    producer.flush()
    print("Producer finished sending product review rows.")


if __name__ == "__main__":
    main()
