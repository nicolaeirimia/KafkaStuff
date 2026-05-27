"""
consumer_sales.py

Consumer for the `sales_events` topic that prints received CSV rows.

Run:
    python consumer_sales.py

Requirements:
    pip install confluent-kafka
"""

import json
from confluent_kafka import Consumer


BOOTSTRAP_SERVERS = "localhost:19092,localhost:19093,localhost:19094"
TOPIC_NAME = "sales_events"
GROUP_ID = "sales-test-consumer-group"
OUTPUT_FILE = "sales_events.jsonl"


def main():
    consumer = Consumer({
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "group.id": GROUP_ID,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,   # commit only after a successful file write
    })

    consumer.subscribe([TOPIC_NAME])

    print(f"Consuming from topic: {TOPIC_NAME}")
    print("Press Ctrl+C to stop.\n")

    with open(OUTPUT_FILE, "a", encoding="utf-8") as out_f:
        try:
            while True:
                msg = consumer.poll(1.0)

                if msg is None:
                    continue

                if msg.error():
                    print("Consumer error:", msg.error())
                    continue

                key = msg.key().decode("utf-8") if msg.key() else None
                raw_value = msg.value().decode("utf-8") if msg.value() else None

                try:
                    parsed_value = json.loads(raw_value)
                except Exception:
                    parsed_value = raw_value

                record = {
                    "topic": msg.topic(),
                    "partition": msg.partition(),
                    "offset": msg.offset(),
                    "timestamp": msg.timestamp(),
                    "key": key,
                    "value": parsed_value,
                }

                print("----- Kafka record -----")
                print("topic:", record["topic"]) 
                print("partition:", record["partition"]) 
                print("offset:", record["offset"]) 
                print("timestamp:", record["timestamp"]) 
                print("key:", record["key"]) 
                print("value:", record["value"]) 
                print()

                out_f.write(json.dumps(record, ensure_ascii=False) + "\n")
                out_f.flush()
                consumer.commit(message=msg)

        except KeyboardInterrupt:
            print("Stopping consumer...")

        finally:
            consumer.close()
            print("Consumer closed. Appended records to", OUTPUT_FILE)


if __name__ == "__main__":
    main()
