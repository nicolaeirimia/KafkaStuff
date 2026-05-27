"""
consumer_reviews.py  —  Member 3 raw-sink consumer

Subscribes to the `product_reviews` Kafka topic and appends every record
as a JSON line to `product_reviews.jsonl`.

Run:
    python consumer_reviews.py
    (Press Ctrl+C to stop.)

Requirements:
    pip install confluent-kafka
"""

import json
from confluent_kafka import Consumer, KafkaError


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

BOOTSTRAP_SERVERS = "localhost:19092,localhost:19093,localhost:19094"
TOPIC_NAME        = "product_reviews"
GROUP_ID          = "reviews-test-consumer-group"
OUTPUT_FILE       = "product_reviews.jsonl"


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    """Consume product_reviews topic and persist records to a JSONL sink."""
    consumer = Consumer({
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "group.id":          GROUP_ID,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,   # commit only after a successful file write
    })

    consumer.subscribe([TOPIC_NAME])

    print(f"Consuming from topic : {TOPIC_NAME}")
    print(f"Consumer group       : {GROUP_ID}")
    print(f"Sink file            : {OUTPUT_FILE}")
    print("Press Ctrl+C to stop.\n")

    with open(OUTPUT_FILE, "a", encoding="utf-8") as out_f:
        try:
            while True:
                msg = consumer.poll(1.0)

                if msg is None:
                    continue

                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    print("Consumer error:", msg.error())
                    continue

                key       = msg.key().decode("utf-8") if msg.key() else None
                raw_value = msg.value().decode("utf-8") if msg.value() else None

                try:
                    parsed_value = json.loads(raw_value)
                except Exception:
                    parsed_value = raw_value

                record = {
                    "topic":     msg.topic(),
                    "partition": msg.partition(),
                    "offset":    msg.offset(),
                    "timestamp": msg.timestamp(),
                    "key":       key,
                    "value":     parsed_value,
                }

                print("----- Kafka record -----")
                print("topic    :", record["topic"])
                print("partition:", record["partition"])
                print("offset   :", record["offset"])
                print("key      :", record["key"])
                print("value    :", record["value"])
                print()

                out_f.write(json.dumps(record, ensure_ascii=False) + "\n")
                out_f.flush()
                consumer.commit(message=msg)

        except KeyboardInterrupt:
            print("Stopping consumer...")

        finally:
            consumer.close()
            print(f"Consumer closed. Records appended to {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
