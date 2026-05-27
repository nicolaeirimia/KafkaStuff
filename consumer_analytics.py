"""
consumer_analytics.py

Real-time analytics consumer for the unified retail analytics pipeline.

Subscribes to all three topics under a single consumer group, aggregates
KPIs in memory, and writes reports periodically and on shutdown.

Business context:
    ecommerce_events — order-level data from an Indian fashion marketplace
                       (channel, category, delivery status, city, revenue INR).
    sales_events     — transaction-level data from a global retail dataset
                       (category, payment method, season, demographics, USD).
    product_reviews  — customer star ratings and sentiment for fashion products
                       across all channels (ProductID, Rating, Sentiment, etc.).
    Together they give a live view of order fulfilment, retail performance,
    and product quality feedback.

Run:
    python consumer_analytics.py
    (Press Ctrl+C to stop and write the final report.)

Requirements:
    pip install confluent-kafka
"""

import json
import signal
import threading
import time
from collections import defaultdict
from datetime import datetime, timezone
from confluent_kafka import Consumer, KafkaError


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

BOOTSTRAP_SERVERS  = "localhost:19092,localhost:19093,localhost:19094"
ECOMMERCE_TOPIC    = "ecommerce_events"
SALES_TOPIC        = "sales_events"
REVIEWS_TOPIC      = "product_reviews"
GROUP_ID           = "analytics-consumer-group"
REPORT_FILE        = "analytics_report.json"
SUMMARY_FILE       = "analytics_summary.txt"
INTERIM_INTERVAL_SECS = 30


# ---------------------------------------------------------------------------
# Aggregator classes
# ---------------------------------------------------------------------------

class EcommerceAggregator:
    """
    Accumulates KPIs from ecommerce_events records.

    Each call to add() accepts a dict that is the raw CSV row
    (already stripped of whitespace by the producer).
    """

    def __init__(self):
        self.total_orders = 0
        self.total_revenue = 0.0
        self.delivered = 0
        self.channels = defaultdict(int)
        self.categories = defaultdict(int)
        self.cities = defaultdict(int)
        self.b2b_count = 0
        self.b2c_count = 0
        self.status_counts = defaultdict(int)

    def add(self, row: dict) -> None:
        """Incorporate one ecommerce CSV row into the running aggregates."""
        self.total_orders += 1

        try:
            self.total_revenue += float(row.get("Amount") or 0)
        except (ValueError, TypeError):
            pass

        status = (row.get("Status") or "Unknown").strip()
        self.status_counts[status] += 1
        if status == "Delivered":
            self.delivered += 1

        # Producer strips the trailing space in "Channel ", so key is "Channel"
        channel = (row.get("Channel") or row.get("Channel ") or "Unknown").strip()
        self.channels[channel] += 1

        category = (row.get("Category") or "Unknown").strip()
        self.categories[category] += 1

        city = (row.get("ship-city") or "Unknown").strip()
        self.cities[city] += 1

        b2b_raw = str(row.get("B2B") or "FALSE").upper().strip()
        if b2b_raw == "TRUE":
            self.b2b_count += 1
        else:
            self.b2c_count += 1

    def to_kpis(self) -> dict:
        """Return computed KPI dict. Safe to call before any data arrives."""
        n = self.total_orders
        avg_order = round(self.total_revenue / n, 2) if n else 0.0
        delivery_rate = round(100.0 * self.delivered / n, 2) if n else 0.0

        top5_channels = sorted(self.channels.items(), key=lambda x: -x[1])[:5]
        top5_categories = sorted(self.categories.items(), key=lambda x: -x[1])[:5]
        top5_cities = sorted(self.cities.items(), key=lambda x: -x[1])[:5]

        return {
            "total_orders": n,
            "total_revenue_inr": round(self.total_revenue, 2),
            "avg_order_value_inr": avg_order,
            "delivery_rate_pct": delivery_rate,
            "top_5_channels": [{"name": k, "orders": v} for k, v in top5_channels],
            "top_5_categories": [{"name": k, "orders": v} for k, v in top5_categories],
            "top_5_cities": [{"name": k, "orders": v} for k, v in top5_cities],
            "b2b_vs_b2c": {"B2B": self.b2b_count, "B2C": self.b2c_count},
            "orders_by_status": dict(self.status_counts),
        }


class SalesAggregator:
    """
    Accumulates KPIs from sales_events records.

    Each call to add() accepts a dict that is the raw CSV row
    (already stripped of whitespace by the producer).
    """

    AGE_BUCKETS = [(25, "18-25"), (35, "26-35"), (45, "36-45"), (55, "46-55")]

    def __init__(self):
        self.total_transactions = 0
        self.total_revenue = 0.0
        self.total_discount = 0.0
        self.total_rating = 0.0
        self.category_revenue = defaultdict(float)
        self.seasons = defaultdict(int)
        self.payment_methods = defaultdict(int)
        self.genders = defaultdict(int)
        self.age_groups = defaultdict(int)

    @staticmethod
    def _age_group(age_str: str) -> str:
        """Map a raw age string to a display bucket label."""
        try:
            age = int(age_str)
        except (ValueError, TypeError):
            return "Unknown"
        for threshold, label in SalesAggregator.AGE_BUCKETS:
            if age <= threshold:
                return label
        return "56+"

    def add(self, row: dict) -> None:
        """Incorporate one sales CSV row into the running aggregates."""
        self.total_transactions += 1

        try:
            amt = float(row.get("Amount") or 0)
        except (ValueError, TypeError):
            amt = 0.0
        self.total_revenue += amt

        try:
            self.total_discount += float(row.get("DiscountApplied(%)") or 0)
        except (ValueError, TypeError):
            pass

        try:
            self.total_rating += float(row.get("ItemRating") or 0)
        except (ValueError, TypeError):
            pass

        category = (row.get("Category") or "Unknown").strip()
        self.category_revenue[category] += amt

        season = (row.get("Season") or "Unknown").strip()
        self.seasons[season] += 1

        payment = (row.get("PaymentMethod") or "Unknown").strip()
        self.payment_methods[payment] += 1

        gender = (row.get("Gender") or "Unknown").strip()
        self.genders[gender] += 1

        age_group = self._age_group(row.get("Age") or "")
        self.age_groups[age_group] += 1

    def to_kpis(self) -> dict:
        """Return computed KPI dict. Safe to call before any data arrives."""
        n = self.total_transactions
        avg_txn = round(self.total_revenue / n, 2) if n else 0.0
        avg_discount = round(self.total_discount / n, 2) if n else 0.0
        avg_rating = round(self.total_rating / n, 2) if n else 0.0

        top5_cats = sorted(self.category_revenue.items(), key=lambda x: -x[1])[:5]

        return {
            "total_transactions": n,
            "total_revenue_usd": round(self.total_revenue, 2),
            "avg_transaction_value_usd": avg_txn,
            "avg_discount_pct": avg_discount,
            "avg_item_rating": avg_rating,
            "top_5_categories_by_revenue": [
                {"name": k, "revenue_usd": round(v, 2)} for k, v in top5_cats
            ],
            "sales_by_season": dict(self.seasons),
            "sales_by_payment_method": dict(self.payment_methods),
            "sales_by_gender": dict(self.genders),
            "customers_by_age_group": dict(self.age_groups),
        }


class ReviewsAggregator:
    """
    Accumulates KPIs from product_reviews records.

    Each call to add() accepts a dict that is the raw CSV row
    (already stripped of whitespace by the producer).
    """

    def __init__(self):
        self.total_reviews = 0
        self.total_rating = 0.0
        self.sentiments = defaultdict(int)
        self.ratings_dist = defaultdict(int)
        self.top_products = defaultdict(lambda: {"reviews": 0, "total_rating": 0.0})
        self.by_category = defaultdict(int)
        self.by_channel = defaultdict(int)
        self.by_platform = defaultdict(int)
        self.verified_count = 0

    def add(self, row: dict) -> None:
        """Incorporate one product review row into the running aggregates."""
        self.total_reviews += 1

        try:
            rating = float(row.get("Rating") or 0)
            self.total_rating += rating
            self.ratings_dist[str(int(rating))] += 1
        except (ValueError, TypeError):
            rating = 0.0

        sentiment = (row.get("Sentiment") or "Unknown").strip()
        self.sentiments[sentiment] += 1

        product_id = (row.get("ProductID") or "Unknown").strip()
        self.top_products[product_id]["reviews"] += 1
        self.top_products[product_id]["total_rating"] += rating

        category = (row.get("Category") or "Unknown").strip()
        self.by_category[category] += 1

        channel = (row.get("Channel") or "Unknown").strip()
        self.by_channel[channel] += 1

        platform = (row.get("Platform") or "Unknown").strip()
        self.by_platform[platform] += 1

        verified = str(row.get("VerifiedPurchase") or "False").strip().lower()
        if verified == "true":
            self.verified_count += 1

    def to_kpis(self) -> dict:
        """Return computed KPI dict."""
        n = self.total_reviews
        avg_rating = round(self.total_rating / n, 2) if n else 0.0
        verified_pct = round(100.0 * self.verified_count / n, 2) if n else 0.0

        top5_products = sorted(
            self.top_products.items(),
            key=lambda x: -x[1]["reviews"],
        )[:5]

        return {
            "total_reviews": n,
            "avg_rating": avg_rating,
            "verified_purchase_pct": verified_pct,
            "sentiment_breakdown": dict(self.sentiments),
            "rating_distribution": dict(self.ratings_dist),
            "top_5_reviewed_products": [
                {
                    "product_id": pid,
                    "reviews": data["reviews"],
                    "avg_rating": round(data["total_rating"] / data["reviews"], 2)
                    if data["reviews"] else 0.0,
                }
                for pid, data in top5_products
            ],
            "reviews_by_category": dict(self.by_category),
            "reviews_by_channel": dict(self.by_channel),
            "reviews_by_platform": dict(self.by_platform),
        }


# ---------------------------------------------------------------------------
# Report helpers
# ---------------------------------------------------------------------------

def build_report(
    ecomm_agg: EcommerceAggregator,
    sales_agg: SalesAggregator,
    reviews_agg: ReviewsAggregator,
    total_records: int,
    partition_offsets: dict,
) -> dict:
    """Assemble the analytics_report.json payload."""
    return {
        "generated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "total_kafka_records_consumed": total_records,
        "partition_offsets": partition_offsets,
        "ecommerce_kpis": ecomm_agg.to_kpis(),
        "sales_kpis": sales_agg.to_kpis(),
        "reviews_kpis": reviews_agg.to_kpis(),
    }


def format_summary(report: dict) -> str:
    """
    Render a human-readable analytics summary for terminal or slide display.
    Uses fixed-width alignment so columns line up cleanly.
    """
    e = report["ecommerce_kpis"]
    s = report["sales_kpis"]
    ts = report["generated_at"]
    total = report["total_kafka_records_consumed"]

    W = 64  # total line width inside the border

    def hr(char="-"):
        return "+" + char * W + "+"

    def row(label, value, indent=0):
        pad = " " * indent
        content = f"{pad}{label}"
        content = content.ljust(W - len(str(value)) - 2) + str(value)
        return "| " + content + " |"

    def heading(title):
        return "| " + title.center(W) + " |"

    def blank():
        return "| " + " " * W + " |"

    lines = []
    lines.append(hr("="))
    lines.append(heading("RETAIL ANALYTICS PIPELINE — LIVE REPORT"))
    lines.append(hr("="))
    lines.append(row("Generated at     :", ts))
    lines.append(row("Kafka records    :", f"{total:,}"))
    lines.append(hr("-"))

    lines.append(heading("ECOMMERCE KPIs  (topic: ecommerce_events, currency: INR)"))
    lines.append(hr("-"))
    lines.append(row("Total orders             :", f"{e['total_orders']:,}"))
    lines.append(row("Total revenue            :", f"INR {e['total_revenue_inr']:,.2f}"))
    lines.append(row("Avg order value          :", f"INR {e['avg_order_value_inr']:,.2f}"))
    lines.append(row("Delivery rate            :", f"{e['delivery_rate_pct']}%"))
    lines.append(blank())

    lines.append(row("  B2B orders             :", f"{e['b2b_vs_b2c'].get('B2B', 0):,}"))
    lines.append(row("  B2C orders             :", f"{e['b2b_vs_b2c'].get('B2C', 0):,}"))
    lines.append(blank())

    lines.append("| " + "  Orders by status:".ljust(W) + " |")
    for status, count in sorted(e["orders_by_status"].items()):
        lines.append(row(f"    {status}", f"{count:,}", indent=0))
    lines.append(blank())

    lines.append("| " + "  Top 5 Sales Channels:".ljust(W) + " |")
    for i, ch in enumerate(e["top_5_channels"], 1):
        lines.append(row(f"    {i}. {ch['name']}", f"{ch['orders']:,} orders"))
    lines.append(blank())

    lines.append("| " + "  Top 5 Product Categories:".ljust(W) + " |")
    for i, cat in enumerate(e["top_5_categories"], 1):
        lines.append(row(f"    {i}. {cat['name']}", f"{cat['orders']:,} orders"))
    lines.append(blank())

    lines.append("| " + "  Top 5 Cities:".ljust(W) + " |")
    for i, city in enumerate(e["top_5_cities"], 1):
        lines.append(row(f"    {i}. {city['name']}", f"{city['orders']:,} orders"))

    lines.append(hr("-"))
    lines.append(heading("SALES KPIs  (topic: sales_events, currency: USD)"))
    lines.append(hr("-"))
    lines.append(row("Total transactions       :", f"{s['total_transactions']:,}"))
    lines.append(row("Total revenue            :", f"USD {s['total_revenue_usd']:,.2f}"))
    lines.append(row("Avg transaction value    :", f"USD {s['avg_transaction_value_usd']:,.2f}"))
    lines.append(row("Avg discount applied     :", f"{s['avg_discount_pct']}%"))
    lines.append(row("Avg item rating          :", f"{s['avg_item_rating']} / 5.0"))
    lines.append(blank())

    lines.append("| " + "  Top 5 Categories by Revenue:".ljust(W) + " |")
    for i, cat in enumerate(s["top_5_categories_by_revenue"], 1):
        lines.append(row(f"    {i}. {cat['name']}", f"USD {cat['revenue_usd']:,.2f}"))
    lines.append(blank())

    lines.append("| " + "  Sales by Season:".ljust(W) + " |")
    for season, count in sorted(s["sales_by_season"].items()):
        lines.append(row(f"    {season}", f"{count:,} transactions"))
    lines.append(blank())

    lines.append("| " + "  Sales by Payment Method:".ljust(W) + " |")
    for method, count in sorted(s["sales_by_payment_method"].items()):
        lines.append(row(f"    {method}", f"{count:,} transactions"))
    lines.append(blank())

    lines.append("| " + "  Sales by Gender:".ljust(W) + " |")
    for gender, count in sorted(s["sales_by_gender"].items()):
        lines.append(row(f"    {gender}", f"{count:,} transactions"))
    lines.append(blank())

    lines.append("| " + "  Customers by Age Group:".ljust(W) + " |")
    for group in ["18-25", "26-35", "36-45", "46-55", "56+", "Unknown"]:
        count = s["customers_by_age_group"].get(group, 0)
        if count:
            lines.append(row(f"    {group}", f"{count:,} customers"))

    r = report["reviews_kpis"]
    lines.append(hr("-"))
    lines.append(heading("REVIEWS KPIs  (topic: product_reviews)"))
    lines.append(hr("-"))
    lines.append(row("Total reviews            :", f"{r['total_reviews']:,}"))
    lines.append(row("Avg product rating       :", f"{r['avg_rating']} / 5.0"))
    lines.append(row("Verified purchase        :", f"{r['verified_purchase_pct']}%"))
    lines.append(blank())

    lines.append("| " + "  Sentiment Breakdown:".ljust(W) + " |")
    for sentiment, count in sorted(r["sentiment_breakdown"].items()):
        lines.append(row(f"    {sentiment}", f"{count:,} reviews"))
    lines.append(blank())

    lines.append("| " + "  Rating Distribution:".ljust(W) + " |")
    for star in ["5", "4", "3", "2", "1"]:
        count = r["rating_distribution"].get(star, 0)
        lines.append(row(f"    {star} stars", f"{count:,} reviews"))
    lines.append(blank())

    lines.append("| " + "  Top 5 Most-Reviewed Products:".ljust(W) + " |")
    for i, prod in enumerate(r["top_5_reviewed_products"], 1):
        lines.append(row(
            f"    {i}. {prod['product_id']}",
            f"{prod['reviews']:,} reviews  avg {prod['avg_rating']}"
        ))
    lines.append(blank())

    lines.append("| " + "  Reviews by Channel:".ljust(W) + " |")
    for ch, count in sorted(r["reviews_by_channel"].items(), key=lambda x: -x[1]):
        lines.append(row(f"    {ch}", f"{count:,} reviews"))
    lines.append(blank())

    lines.append("| " + "  Reviews by Platform:".ljust(W) + " |")
    for pl, count in sorted(r["reviews_by_platform"].items(), key=lambda x: -x[1]):
        lines.append(row(f"    {pl}", f"{count:,} reviews"))

    lines.append(hr("="))
    return "\n".join(lines)


def write_report(report: dict) -> None:
    """Write the JSON report to disk atomically-ish (overwrite in place)."""
    try:
        with open(REPORT_FILE, "w", encoding="utf-8") as f:
            json.dump(report, f, indent=2, ensure_ascii=False)
    except Exception as exc:
        print(f"[analytics] Failed to write {REPORT_FILE}: {exc}")


def write_summary(report: dict) -> None:
    """Write the human-readable summary to disk and print it."""
    text = format_summary(report)
    try:
        with open(SUMMARY_FILE, "w", encoding="utf-8") as f:
            f.write(text + "\n")
        print(text)
        print(f"\n[analytics] Summary written to {SUMMARY_FILE}")
    except Exception as exc:
        print(f"[analytics] Failed to write {SUMMARY_FILE}: {exc}")


# ---------------------------------------------------------------------------
# Main consumer loop
# ---------------------------------------------------------------------------

def main() -> None:
    """
    Subscribe to all three topics, aggregate KPIs in real time, write an
    interim JSON report every 30 seconds, and write the final JSON report +
    text summary on Ctrl+C shutdown.
    """
    ecomm_agg   = EcommerceAggregator()
    sales_agg   = SalesAggregator()
    reviews_agg = ReviewsAggregator()
    total_records = 0
    partition_offsets: dict = {}

    consumer = Consumer({
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "group.id":          GROUP_ID,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,   # commit explicitly after each record is aggregated
    })

    consumer.subscribe([ECOMMERCE_TOPIC, SALES_TOPIC, REVIEWS_TOPIC])
    print(f"[analytics] Subscribed to: {ECOMMERCE_TOPIC}, {SALES_TOPIC}, {REVIEWS_TOPIC}")
    print(f"[analytics] Group ID     : {GROUP_ID}")
    print(f"[analytics] Interim report every {INTERIM_INTERVAL_SECS}s  |  Ctrl+C to stop\n")

    stop_event = threading.Event()

    def interim_writer():
        """Background thread that persists an interim report every 30 s."""
        while not stop_event.wait(INTERIM_INTERVAL_SECS):
            report = build_report(
                ecomm_agg, sales_agg, reviews_agg, total_records, dict(partition_offsets)
            )
            write_report(report)
            print(
                f"[analytics] Interim report written — "
                f"ecommerce={ecomm_agg.total_orders:,} orders, "
                f"sales={sales_agg.total_transactions:,} transactions, "
                f"reviews={reviews_agg.total_reviews:,}"
            )

    writer_thread = threading.Thread(target=interim_writer, daemon=True)
    writer_thread.start()

    try:
        while True:
            msg = consumer.poll(1.0)

            if msg is None:
                continue

            if msg.error():
                # PARTITION_EOF is normal — the consumer has caught up with the
                # end of a partition; it is not an error condition.
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                print(f"[analytics] Consumer error: {msg.error()}")
                continue

            topic     = msg.topic()
            partition = msg.partition()
            offset    = msg.offset()
            key       = msg.key().decode("utf-8") if msg.key() else None

            try:
                row = json.loads(msg.value().decode("utf-8"))
            except Exception as exc:
                print(f"[analytics] Could not parse message value: {exc}")
                continue

            partition_key = f"{topic}:{partition}"
            partition_offsets[partition_key] = offset
            total_records += 1

            if topic == ECOMMERCE_TOPIC:
                ecomm_agg.add(row)
            elif topic == SALES_TOPIC:
                sales_agg.add(row)
            elif topic == REVIEWS_TOPIC:
                reviews_agg.add(row)

            # Commit after the record is safely in the aggregator.
            # Aligns consumer-side durability with the acks=all guarantee on producers.
            consumer.commit(message=msg)

            print(f"[{topic}] partition={partition} offset={offset} key={key}")

    except KeyboardInterrupt:
        print("\n[analytics] Shutdown signal received — writing final reports...")

    finally:
        stop_event.set()
        consumer.close()
        print("[analytics] Consumer closed.")

        report = build_report(
            ecomm_agg, sales_agg, reviews_agg, total_records, partition_offsets
        )
        write_report(report)
        print(f"[analytics] Final JSON report written to {REPORT_FILE}")
        write_summary(report)


if __name__ == "__main__":
    main()
