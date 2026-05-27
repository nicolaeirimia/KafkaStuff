# Unified Retail Analytics Pipeline — Kafka Final Project

## Business scenario

A real-time analytics pipeline for a fashion e-commerce company built on a
3-broker Kafka cluster (KRaft, no ZooKeeper).  Three team members each own
one data source, one producer, and one consumer.  Together the three streams
feed a unified analytics consumer that aggregates KPIs across all pipelines.

---

## Team responsibilities

| Member | Data source | Key | Topic | KPIs |
|--------|-------------|-----|-------|------|
| Member 1 | `ecommerce.csv` — Indian fashion orders (INR) | `Order ID` | `ecommerce_events` | Orders, revenue INR, delivery rate, channels, cities |
| Member 2 | `sales.csv` — Global retail transactions (USD) | `CustomerID` | `sales_events` | Transactions, revenue USD, discount, rating, seasons |
| Member 3 | `product_reviews.csv` — Customer ratings & sentiment | `ProductID` | `product_reviews` | Reviews, avg rating, sentiment, top products |

**Shared files** — `docker-compose.yml`, `consumer_analytics.py`, `analyze_jsonl.py`, `generate_html_report.py`

---

## Architecture

```
ecommerce.csv       → producer_ecommerce.py → Kafka: ecommerce_events (3 partitions, RF=3)
sales.csv           → producer_sales.py     → Kafka: sales_events     (3 partitions, RF=3)
product_reviews.csv → producer_reviews.py   → Kafka: product_reviews  (3 partitions, RF=3)

Kafka: ecommerce_events → consumer_ecommerce.py → ecommerce_events.jsonl
Kafka: sales_events     → consumer_sales.py     → sales_events.jsonl
Kafka: product_reviews  → consumer_reviews.py   → product_reviews.jsonl

Kafka: all 3 topics     → consumer_analytics.py → analytics_report.json
                                                 → analytics_summary.txt

All 3 JSONL files       → analyze_jsonl.py      → analytics_report.json (offline)
analytics_report.json   → generate_html_report.py → analytics_dashboard.html
```

---

## Grading criteria — how each requirement is met

| Requirement | Where implemented | Detail |
|-------------|-------------------|--------|
| **One data source per member** | `ecommerce.csv`, `sales.csv`, `product_reviews.csv` | 3 CSVs, one per member |
| **Producer per source** | `producer_ecommerce.py`, `producer_sales.py`, `producer_reviews.py` | Each reads its CSV and produces to its topic |
| **Producer delivery callback** | `delivery_report()` in every producer | Prints topic, partition, offset on success; prints error on failure |
| **One topic per source** | `ecommerce_events`, `sales_events`, `product_reviews` | Auto-created at first run |
| **Multiple partitions + explained** | 3 partitions per topic | Matches 3 brokers — each broker is leader of exactly 1 partition; allows up to 3 parallel consumers |
| **Meaningful key + justified** | `Order ID`, `CustomerID`, `ProductID` | Comment block above every `produce()` call explains the partition routing logic |
| **At least 2 consumers with group IDs** | 4 consumers total | `ecommerce-test-consumer-group`, `sales-test-consumer-group`, `reviews-test-consumer-group`, `analytics-consumer-group` |
| **Multi-broker Docker setup** | `docker-compose.yml` | 3 KRaft brokers on ports 19092 / 19093 / 19094 |
| **Structured sink / output** | `.jsonl` files + `analytics_report.json` + `analytics_summary.txt` + HTML dashboard | Every consumed record is persisted |
| **Durability config + trade-off justified** | `acks=all`, RF=3, `min.insync.replicas=2` | Explained in `kafka_concepts.md` Q15 |
| **Consumer group behaviour** | 4 distinct groups; parallelism demo in Step 7 below | Groups share load; each partition owned by exactly one consumer instance |
| **Replication / leader / follower / ISR** | `docker-compose.yml` settings; `kafka_concepts.md` Q14 | RF=3, ISR≥2 enforced |
| **Replay / offset reset** | `auto.offset.reset=earliest`; CLI reset command in Step CLI section | Any group can replay from offset 0 |
| **Manual offset commit (at-least-once)** | `enable.auto.commit=False` + `consumer.commit(message=msg)` in all 4 consumers | Commit happens only after write to disk succeeds |
| **Complete flow clearly explained** | Architecture diagram above + `kafka_concepts.md` Q1–Q15 | Producer → Kafka → Consumer → Sink documented end-to-end |

---

## How to run

### Prerequisites
```
pip install confluent-kafka
# Docker Desktop must be running
```

### Step 1 — Start the Kafka cluster
```
docker compose up -d
```
Starts 3 brokers in KRaft mode: `kafka-1:19092`, `kafka-2:19093`, `kafka-3:19094`.

Verify all three containers are up:
```
docker ps
docker exec -it kafka-1 kafka-broker-api-versions --bootstrap-server kafka-1:9092
```

### Step 2 — Run the producers (one per member)
```
python producer_ecommerce.py   # Member 1 — Order ID key
python producer_sales.py       # Member 2 — CustomerID key
python producer_reviews.py     # Member 3 — ProductID key
```
Each producer auto-creates its topic (3 partitions, RF=3) and prints a delivery
confirmation for every record: `Delivered key=... to topic=... partition=X offset=Y`

### Step 3 — Run the raw-sink consumers
Open 3 terminals:
```
python consumer_ecommerce.py   # group: ecommerce-test-consumer-group → ecommerce_events.jsonl
python consumer_sales.py       # group: sales-test-consumer-group     → sales_events.jsonl
python consumer_reviews.py     # group: reviews-test-consumer-group   → product_reviews.jsonl
```
Press Ctrl+C to stop each. Every record is flushed to disk before the offset is committed.

### Step 4 — Run the analytics consumer
```
python consumer_analytics.py
```
Subscribes to all 3 topics under `analytics-consumer-group`.
- Prints each record as it arrives
- Writes an interim `analytics_report.json` every 30 seconds
- On Ctrl+C → writes final `analytics_report.json` + `analytics_summary.txt`

### Step 5 — Run offline analysis (no Kafka needed)
```
python analyze_jsonl.py
```
Reads the 3 JSONL sinks and produces the same report without a Kafka connection.
Useful to regenerate the report after the cluster is stopped.

### Step 6 — Open the HTML dashboard
```
python generate_html_report.py
```
Reads `analytics_report.json` and opens `analytics_dashboard.html` in the browser
— KPI cards and interactive charts for all 3 pipelines.

### Step 7 — Live consumer group parallelism demo
Run the same consumer script in **two terminals at the same time**:
```
# Terminal A
python consumer_ecommerce.py

# Terminal B (same group ID — triggers rebalance)
python consumer_ecommerce.py
```
Kafka detects the second consumer and rebalances: 3 partitions split 2+1.
Each terminal prints only its assigned partitions.
Kill Terminal B → Kafka rebalances again, Terminal A takes all 3 partitions.
This is the live proof of consumer group partition assignment.

---

## Useful Kafka CLI commands

```bash
# List all topics
docker exec -it kafka-1 kafka-topics --bootstrap-server kafka-1:9092 --list

# Describe a topic — shows partitions, leader, replicas, ISR
docker exec -it kafka-1 kafka-topics --bootstrap-server kafka-1:9092 \
    --describe --topic ecommerce_events

# Read records with full metadata (timestamp, partition, offset, key, value)
docker exec -it kafka-1 kafka-console-consumer \
    --bootstrap-server kafka-1:9092 --topic ecommerce_events --from-beginning \
    --formatter kafka.tools.DefaultMessageFormatter \
    --property print.timestamp=true --property print.partition=true \
    --property print.offset=true --property print.key=true \
    --property print.value=true --property key.separator=" | " \
    --max-messages 10

# Show latest offset per partition (use to verify producer output)
docker exec -it kafka-1 kafka-get-offsets \
    --bootstrap-server kafka-1:9092 --topic ecommerce_events

# List all consumer groups
docker exec -it kafka-1 kafka-consumer-groups \
    --bootstrap-server kafka-1:9092 --list

# Describe a group — shows lag per partition
docker exec -it kafka-1 kafka-consumer-groups \
    --bootstrap-server kafka-1:9092 --describe --group analytics-consumer-group

# Replay: reset a consumer group to the earliest offset
docker exec -it kafka-1 kafka-consumer-groups \
    --bootstrap-server kafka-1:9092 \
    --group analytics-consumer-group \
    --topic ecommerce_events \
    --reset-offsets --to-earliest --execute

# Stop everything
docker compose down
```