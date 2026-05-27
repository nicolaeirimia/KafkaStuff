# Kafka Concepts — Unified Retail Analytics Pipeline

Answers to all exam questions, grounded in the actual implementation.

---

## 1. What is the role of the producer in your implementation?

The producer reads rows from a CSV file and publishes each row as a JSON-encoded
message to a Kafka topic.  This project has three producers — one per team member:
`producer_ecommerce.py` (order records → `ecommerce_events`),
`producer_sales.py` (retail transactions → `sales_events`), and
`producer_reviews.py` (product ratings → `product_reviews`).
Each producer serialises data, assigns a meaningful partition key, and confirms
delivery with `acks=all`.

---

## 2. What is the role of the Kafka broker?

A broker is a server process that receives messages from producers, stores them
durably on disk, and serves them to consumers.  This project runs three brokers
(`kafka-1`, `kafka-2`, `kafka-3`) in KRaft mode.  Each broker can act as a
leader for some partitions and a follower (replica) for others, so the cluster
keeps working even if one broker fails.

---

## 3. What is the difference between a topic and a partition?

A **topic** is a named logical channel for a stream of related events
(e.g. `ecommerce_events`).  A **partition** is the physical unit of storage
inside a topic — each partition is an ordered, append-only log of messages.
A topic with N partitions splits its data across N logs, which allows multiple
producers and consumers to work in parallel.  Messages within one partition are
strictly ordered; there is no global ordering guarantee across partitions.

---

## 4. How many partitions does your topic have, and why did you choose that number?

Both topics have **3 partitions**.  Three was chosen to match the cluster size:
with 3 brokers each broker holds exactly one partition as leader and two as
follower replicas, fully utilising the cluster and maximising write throughput.
Three partitions also allow a consumer group of up to three consumers to read in
parallel — one consumer per partition — so the pipeline can scale horizontally
without wasted capacity.

---

## 5. Which Kafka key did you choose for each producer, and why is it appropriate?

- **`producer_ecommerce.py` — key: `Order ID`.**
  An Order ID uniquely identifies a purchase.  Using it as the key routes every
  row belonging to the same order (multi-item orders can appear as multiple CSV
  rows) to the same partition, preserving per-order ordering and making it easy
  for consumers to reconstruct complete orders without cross-partition joins.

- **`producer_sales.py` — key: `CustomerID`.**
  A Customer ID groups all transactions by the same shopper.  Routing all
  purchases of one customer to the same partition means a consumer computing
  per-customer metrics (lifetime value, repeat-purchase rate) sees events in
  arrival order and never needs to merge records from multiple partitions.

- **`producer_reviews.py` — key: `ProductID`.**
  A Product ID groups all reviews for the same product into the same partition.
  A product-quality consumer can compute per-product average rating and sentiment
  trend without merging data across partitions, because every review for the
  product arrives on the same ordered log.

---

## 6. What does an offset represent?

An offset is a monotonically increasing integer that Kafka assigns to each
message within a partition.  The first message written to a partition gets
offset 0, the next gets offset 1, and so on.  An offset is a unique, permanent
address for a message inside a partition.  Consumers use offsets to track which
messages they have already processed, and can commit those offsets back to Kafka
so that they resume from the right position after a restart.

---

## 7. What happens if a consumer is stopped and restarted?

Kafka stores committed offsets in the internal `__consumer_offsets` topic.
When the consumer restarts it sends a `JoinGroup` request with its group ID,
Kafka replies with the last committed offsets for each assigned partition, and
the consumer resumes reading from the next uncommitted message.  No messages are
lost and no messages are skipped — as long as the consumer committed its offsets
before it stopped.  In this project, `enable.auto.commit=False` and
`consumer.commit(message=msg)` is called after every successful file write, so
a restart always resumes from the last record that was safely persisted.

---

## 8. What changes if multiple consumers use the same group ID?

Kafka treats all consumers that share a group ID as a single logical consumer
and distributes the topic's partitions among them — each partition is owned by
exactly one consumer at a time.  For example, with three partitions and two
consumers in the same group, one consumer reads two partitions and the other
reads one.  Adding a third consumer gives each one exactly one partition.  This
is how horizontal scaling works: more consumers in the group → more parallelism,
up to the number of partitions.

---

## 9. How can Kafka replay data? (offset reset)

Because messages are stored durably for a configurable retention period, a
consumer can replay past data by resetting its committed offset.  The CLI
command is:

```
kafka-consumer-groups --bootstrap-server kafka-1:9092 \
    --group analytics-consumer-group \
    --topic ecommerce_events \
    --reset-offsets --to-earliest --execute
```

Setting `auto.offset.reset=earliest` in the consumer config also tells Kafka
to start from offset 0 when there is no previously committed offset for the
group (e.g. a brand-new group ID).  This is the setting used in all consumers
in this project.

---

## 10. What happens when a broker fails in a replicated topic?

Each partition has one **leader** and two **follower** replicas spread across
the three brokers.  If the broker hosting a partition's leader fails, the Kafka
controller (running in KRaft mode in this project) detects the failure and
promotes one of the in-sync follower replicas to become the new leader within
seconds.  Producers and consumers transparently reconnect to the new leader.
Because `KAFKA_MIN_INSYNC_REPLICAS=2` and producers use `acks=all`, every
committed message already exists on at least two brokers before
acknowledgement, so no data is lost when one broker goes down.

---

## 11. Why is there no global ordering across partitions?

Each partition is an independent ordered log.  Kafka offers no mechanism to
coordinate ordering *between* partitions because that would require a global
lock — destroying the parallel throughput that partitioning provides.  Messages
produced to partition 0 and messages produced to partition 1 may be written
and consumed in any interleaved order relative to each other.  This is why key
design matters: related messages (same order, same customer) are sent to the
same partition so they remain ordered relative to each other.

---

## 12. How do you verify that consumed data reached the sink/output?

Three ways are used in this project:

1. **JSONL files** — `consumer_ecommerce.py` and `consumer_sales.py` append
   every consumed record to `.jsonl` files.  The number of lines in those files
   can be compared with the offset counts from `kafka-get-offsets`.

2. **analytics_report.json** — `consumer_analytics.py` writes a JSON report
   containing `total_kafka_records_consumed` and `partition_offsets` (the last
   offset seen per partition), making it easy to verify coverage.

3. **CLI inspection** — `kafka-console-consumer --from-beginning` and
   `kafka-get-offsets` let you read the raw topic and compare offsets with
   what the consumers have committed.

---

## 13. What is a consumer group, and how does your implementation use it?

A consumer group is a named set of consumer processes that cooperatively read
a topic.  Kafka balances partitions across the group members and tracks a single
set of offsets per group.  This project uses three groups:

| Group ID                        | Script                   | Topics consumed                                          |
|---------------------------------|--------------------------|----------------------------------------------------------|
| `ecommerce-test-consumer-group` | consumer_ecommerce.py    | ecommerce_events                                         |
| `sales-test-consumer-group`     | consumer_sales.py        | sales_events                                             |
| `reviews-test-consumer-group`   | consumer_reviews.py      | product_reviews                                          |
| `analytics-consumer-group`      | consumer_analytics.py    | ecommerce_events, sales_events, **and** product_reviews  |

Each group maintains its own offset independently, so the analytics consumer
can read from offset 0 without interfering with the raw-sink consumers.

---

## 14. What are leader and follower replicas, and what is ISR?

Every partition is replicated across brokers.  The **leader** replica handles
all reads and writes for that partition.  **Follower** replicas passively copy
the leader's log.  The **ISR (In-Sync Replica set)** is the subset of replicas
that are fully caught up with the leader (within `replica.lag.time.max.ms`).

With `KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=3` and
`KAFKA_MIN_INSYNC_REPLICAS=2`, a produce with `acks=all` is only acknowledged
when the leader *and at least one follower* have written the message — meaning
a minimum of 2 brokers must be in ISR.  This prevents data loss if the leader
crashes immediately after acknowledging.

---

## 15a. Why do your consumers use manual offset commit instead of auto-commit?

All consumers in this project set `enable.auto.commit=False` and call
`consumer.commit(message=msg)` only *after* the record has been successfully
written to the JSONL file (or aggregated in memory).  Auto-commit advances
the offset after every `poll()` call regardless of whether the application
actually processed the record — if the process crashes between poll and write,
the offset is already committed and the message is silently lost, directly
undermining the `acks=all` guarantee on the producer side.  Manual commit
gives **at-least-once delivery**: in the worst case a record is re-processed
on restart, but it is never silently dropped.

---

## 15. What durability configuration did you choose, and what is the trade-off vs speed?

All producers in this project use `acks="all"` (equivalent to `acks=-1`).

**What it guarantees:** A produce call only returns success once the leader and
all in-sync replicas have written the message to their logs.  With
`min.insync.replicas=2` this means at least 2 brokers hold every message before
it is considered committed, so losing one broker causes zero data loss.

**The trade-off:** Each produce call must wait for the slowest ISR replica to
acknowledge.  This adds latency (typically a few milliseconds on a LAN) compared
to `acks=1` (wait for leader only) or `acks=0` (fire-and-forget).  For a
real-time analytics pipeline where correctness matters more than raw throughput,
`acks=all` is the right choice.  A high-throughput ingestion pipeline that can
tolerate occasional data loss might choose `acks=1` to reduce latency.
