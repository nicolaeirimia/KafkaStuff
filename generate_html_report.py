"""
generate_html_report.py

Reads analytics_report.json and generates a self-contained HTML dashboard
with interactive charts.  Opens automatically in the default browser.

Run:
    python generate_html_report.py

No pip installs required — uses only the standard library.
Chart.js is loaded from CDN (requires internet connection).
"""

import json
import webbrowser
import os
from datetime import datetime, timezone

REPORT_FILE = "analytics_report.json"
OUTPUT_FILE = "analytics_dashboard.html"


def load_report() -> dict:
    with open(REPORT_FILE, encoding="utf-8") as f:
        return json.load(f)


def html_page(report: dict) -> str:
    e = report["ecommerce_kpis"]
    s = report["sales_kpis"]
    r = report["reviews_kpis"]
    ts = report["generated_at"]
    total = report["total_kafka_records_consumed"]

    # --- data helpers ---
    def labels(lst, key="name"):
        return json.dumps([x[key] for x in lst])

    def values(lst, key):
        return json.dumps([x[key] for x in lst])

    def dict_labels(d):
        return json.dumps(list(d.keys()))

    def dict_values(d):
        return json.dumps(list(d.values()))

    # ecommerce
    ch_labels  = labels(e["top_5_channels"])
    ch_vals    = values(e["top_5_channels"], "orders")
    cat_labels = labels(e["top_5_categories"])
    cat_vals   = values(e["top_5_categories"], "orders")
    city_labels= labels(e["top_5_cities"])
    city_vals  = values(e["top_5_cities"], "orders")
    st_labels  = dict_labels(e["orders_by_status"])
    st_vals    = dict_values(e["orders_by_status"])
    b2b_labels = json.dumps(["B2B", "B2C"])
    b2b_vals   = json.dumps([e["b2b_vs_b2c"]["B2B"], e["b2b_vs_b2c"]["B2C"]])

    # sales
    seas_labels = dict_labels(s["sales_by_season"])
    seas_vals   = dict_values(s["sales_by_season"])
    pay_labels  = dict_labels(s["sales_by_payment_method"])
    pay_vals    = dict_values(s["sales_by_payment_method"])
    gen_labels  = dict_labels(s["sales_by_gender"])
    gen_vals    = dict_values(s["sales_by_gender"])
    age_labels  = dict_labels(s["customers_by_age_group"])
    age_vals    = dict_values(s["customers_by_age_group"])
    scat_labels = labels(s["top_5_categories_by_revenue"])
    scat_vals   = values(s["top_5_categories_by_revenue"], "revenue_usd")

    # reviews
    sent_labels = dict_labels(r["sentiment_breakdown"])
    sent_vals   = dict_values(r["sentiment_breakdown"])
    rat_labels  = json.dumps(["5 ★", "4 ★", "3 ★", "2 ★", "1 ★"])
    rat_vals    = json.dumps([
        r["rating_distribution"].get("5", 0),
        r["rating_distribution"].get("4", 0),
        r["rating_distribution"].get("3", 0),
        r["rating_distribution"].get("2", 0),
        r["rating_distribution"].get("1", 0),
    ])
    rch_labels  = dict_labels(r["reviews_by_channel"])
    rch_vals    = dict_values(r["reviews_by_channel"])
    rpl_labels  = dict_labels(r["reviews_by_platform"])
    rpl_vals    = dict_values(r["reviews_by_platform"])
    rprod_labels= json.dumps([p["product_id"] for p in r["top_5_reviewed_products"]])
    rprod_vals  = json.dumps([p["avg_rating"] for p in r["top_5_reviewed_products"]])

    PALETTE = {
        "blue":   ["#3B82F6","#60A5FA","#93C5FD","#BFDBFE","#DBEAFE"],
        "green":  ["#10B981","#34D399","#6EE7B7","#A7F3D0","#D1FAE5"],
        "purple": ["#8B5CF6","#A78BFA","#C4B5FD","#DDD6FE","#EDE9FE"],
        "orange": ["#F59E0B","#FBBF24","#FCD34D","#FDE68A","#FEF3C7"],
        "red":    ["#EF4444","#F87171","#FCA5A5","#FECACA","#FEE2E2"],
        "teal":   ["#14B8A6","#2DD4BF","#5EEAD4","#99F6E4","#CCFBF1"],
    }

    def pal(name, n=5):
        return json.dumps(PALETTE[name][:n])

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Retail Analytics Pipeline — Dashboard</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
<style>
  * {{ box-sizing: border-box; margin: 0; padding: 0; }}
  body {{ font-family: 'Segoe UI', sans-serif; background: #0f172a; color: #e2e8f0; }}
  header {{ background: linear-gradient(135deg, #1e3a5f, #0f172a); padding: 28px 40px; border-bottom: 2px solid #3B82F6; }}
  header h1 {{ font-size: 1.8rem; font-weight: 700; color: #60A5FA; letter-spacing: 0.5px; }}
  header p  {{ margin-top: 6px; color: #94a3b8; font-size: 0.9rem; }}
  .badge {{ display: inline-block; background: #1e3a5f; border: 1px solid #3B82F6;
            border-radius: 20px; padding: 4px 14px; font-size: 0.8rem; color: #93C5FD;
            margin: 4px 4px 0 0; }}
  .section-title {{ font-size: 1.3rem; font-weight: 600; padding: 28px 40px 0;
                    border-left: 4px solid; padding-left: 16px; margin: 28px 40px 0; }}
  .ecommerce-title  {{ color: #3B82F6; border-color: #3B82F6; }}
  .sales-title      {{ color: #10B981; border-color: #10B981; }}
  .reviews-title    {{ color: #8B5CF6; border-color: #8B5CF6; }}
  .kpi-row {{ display: flex; flex-wrap: wrap; gap: 16px; padding: 16px 40px; }}
  .kpi {{ background: #1e293b; border-radius: 12px; padding: 20px 24px;
          flex: 1 1 160px; min-width: 140px; border-top: 3px solid; }}
  .kpi.blue   {{ border-color: #3B82F6; }}
  .kpi.green  {{ border-color: #10B981; }}
  .kpi.purple {{ border-color: #8B5CF6; }}
  .kpi .val {{ font-size: 1.6rem; font-weight: 700; margin-top: 6px; }}
  .kpi .lbl {{ font-size: 0.75rem; color: #94a3b8; text-transform: uppercase; letter-spacing: 0.8px; }}
  .charts-grid {{ display: grid; grid-template-columns: repeat(auto-fill, minmax(420px, 1fr));
                  gap: 20px; padding: 16px 40px; }}
  .chart-box {{ background: #1e293b; border-radius: 14px; padding: 22px; }}
  .chart-box h3 {{ font-size: 0.9rem; color: #94a3b8; text-transform: uppercase;
                   letter-spacing: 0.8px; margin-bottom: 16px; }}
  .chart-box canvas {{ max-height: 260px; }}
  footer {{ text-align: center; padding: 32px; color: #475569; font-size: 0.8rem; }}
</style>
</head>
<body>

<header>
  <h1>Unified Retail Analytics Pipeline</h1>
  <p>Kafka Final Project — Real-time analytics across 3 topics and 3 brokers (KRaft)</p>
  <div style="margin-top:12px">
    <span class="badge">Generated: {ts}</span>
    <span class="badge">Total Kafka records: {total:,}</span>
    <span class="badge">3-broker KRaft cluster</span>
    <span class="badge">acks=all · RF=3 · min.insync=2</span>
  </div>
</header>

<!-- ── ECOMMERCE ──────────────────────────────────────────── -->
<div class="section-title ecommerce-title">Ecommerce Events &nbsp;·&nbsp; topic: ecommerce_events &nbsp;·&nbsp; key: Order ID</div>
<div class="kpi-row">
  <div class="kpi blue"><div class="lbl">Total Orders</div><div class="val" style="color:#3B82F6">{e["total_orders"]:,}</div></div>
  <div class="kpi blue"><div class="lbl">Revenue (INR)</div><div class="val" style="color:#60A5FA">₹{e["total_revenue_inr"]:,.0f}</div></div>
  <div class="kpi blue"><div class="lbl">Avg Order Value</div><div class="val" style="color:#93C5FD">₹{e["avg_order_value_inr"]:,.2f}</div></div>
  <div class="kpi blue"><div class="lbl">Delivery Rate</div><div class="val" style="color:#BFDBFE">{e["delivery_rate_pct"]}%</div></div>
  <div class="kpi blue"><div class="lbl">B2B Orders</div><div class="val" style="color:#DBEAFE">{e["b2b_vs_b2c"]["B2B"]:,}</div></div>
  <div class="kpi blue"><div class="lbl">B2C Orders</div><div class="val" style="color:#DBEAFE">{e["b2b_vs_b2c"]["B2C"]:,}</div></div>
</div>
<div class="charts-grid">
  <div class="chart-box"><h3>Top 5 Sales Channels</h3><canvas id="ch"></canvas></div>
  <div class="chart-box"><h3>Top 5 Product Categories</h3><canvas id="cat"></canvas></div>
  <div class="chart-box"><h3>Orders by Status</h3><canvas id="st"></canvas></div>
  <div class="chart-box"><h3>Top 5 Cities</h3><canvas id="city"></canvas></div>
  <div class="chart-box"><h3>B2B vs B2C</h3><canvas id="b2b"></canvas></div>
</div>

<!-- ── SALES ─────────────────────────────────────────────── -->
<div class="section-title sales-title">Sales Events &nbsp;·&nbsp; topic: sales_events &nbsp;·&nbsp; key: CustomerID</div>
<div class="kpi-row">
  <div class="kpi green"><div class="lbl">Total Transactions</div><div class="val" style="color:#10B981">{s["total_transactions"]:,}</div></div>
  <div class="kpi green"><div class="lbl">Revenue (USD)</div><div class="val" style="color:#34D399">${s["total_revenue_usd"]:,.0f}</div></div>
  <div class="kpi green"><div class="lbl">Avg Transaction</div><div class="val" style="color:#6EE7B7">${s["avg_transaction_value_usd"]:,.2f}</div></div>
  <div class="kpi green"><div class="lbl">Avg Discount</div><div class="val" style="color:#A7F3D0">{s["avg_discount_pct"]}%</div></div>
  <div class="kpi green"><div class="lbl">Avg Item Rating</div><div class="val" style="color:#D1FAE5">{s["avg_item_rating"]} ★</div></div>
</div>
<div class="charts-grid">
  <div class="chart-box"><h3>Top 5 Categories by Revenue (USD)</h3><canvas id="scat"></canvas></div>
  <div class="chart-box"><h3>Sales by Season</h3><canvas id="seas"></canvas></div>
  <div class="chart-box"><h3>Payment Methods</h3><canvas id="pay"></canvas></div>
  <div class="chart-box"><h3>Sales by Gender</h3><canvas id="gen"></canvas></div>
  <div class="chart-box"><h3>Customers by Age Group</h3><canvas id="age"></canvas></div>
</div>

<!-- ── REVIEWS ───────────────────────────────────────────── -->
<div class="section-title reviews-title">Product Reviews &nbsp;·&nbsp; topic: product_reviews &nbsp;·&nbsp; key: ProductID</div>
<div class="kpi-row">
  <div class="kpi purple"><div class="lbl">Total Reviews</div><div class="val" style="color:#8B5CF6">{r["total_reviews"]:,}</div></div>
  <div class="kpi purple"><div class="lbl">Avg Rating</div><div class="val" style="color:#A78BFA">{r["avg_rating"]} ★</div></div>
  <div class="kpi purple"><div class="lbl">Verified Purchases</div><div class="val" style="color:#C4B5FD">{r["verified_purchase_pct"]}%</div></div>
</div>
<div class="charts-grid">
  <div class="chart-box"><h3>Sentiment Breakdown</h3><canvas id="sent"></canvas></div>
  <div class="chart-box"><h3>Rating Distribution</h3><canvas id="rat"></canvas></div>
  <div class="chart-box"><h3>Reviews by Channel</h3><canvas id="rch"></canvas></div>
  <div class="chart-box"><h3>Reviews by Platform</h3><canvas id="rpl"></canvas></div>
  <div class="chart-box"><h3>Top Products — Avg Rating</h3><canvas id="rprod"></canvas></div>
</div>

<footer>Kafka Final Project &nbsp;·&nbsp; 3-broker KRaft cluster &nbsp;·&nbsp; Docker &nbsp;·&nbsp; confluent-kafka</footer>

<script>
const cfg = (type, labels, data, colors, opts={{}}) => ({{
  type, data: {{ labels, datasets: [{{ data, backgroundColor: colors,
    borderColor: colors, borderWidth: type==='bar'?0:2,
    borderRadius: type==='bar'?6:0 }}] }},
  options: {{ responsive:true, plugins:{{ legend:{{ display: type!=='bar',
    labels:{{ color:'#cbd5e1' }} }}, tooltip:{{ callbacks:{{
      label: ctx => ' ' + ctx.formattedValue }} }} }},
    scales: type==='bar' ? {{ x:{{ ticks:{{ color:'#94a3b8' }}, grid:{{ color:'#1e3a5f' }} }},
      y:{{ ticks:{{ color:'#94a3b8' }}, grid:{{ color:'#1e3a5f' }} }} }} : {{}},
    ...opts }} }});

// Ecommerce
new Chart('ch',   cfg('bar',  {ch_labels},  {ch_vals},  {pal("blue")}));
new Chart('cat',  cfg('bar',  {cat_labels}, {cat_vals}, {pal("blue")}));
new Chart('st',   cfg('doughnut', {st_labels}, {st_vals}, {pal("blue")}));
new Chart('city', cfg('bar',  {city_labels},{city_vals},{pal("blue")}));
new Chart('b2b',  cfg('pie',  {b2b_labels}, {b2b_vals}, ['#3B82F6','#0ea5e9']));

// Sales
new Chart('scat', cfg('bar',  {scat_labels},{scat_vals},{pal("green")}));
new Chart('seas', cfg('bar',  {seas_labels},{seas_vals},{pal("green")}));
new Chart('pay',  cfg('doughnut',{pay_labels},{pay_vals},{pal("green")}));
new Chart('gen',  cfg('pie',  {gen_labels}, {gen_vals}, ['#10B981','#0d9488']));
new Chart('age',  cfg('bar',  {age_labels}, {age_vals}, {pal("green")}));

// Reviews
new Chart('sent', cfg('doughnut',{sent_labels},{sent_vals},['#10B981','#F59E0B','#EF4444']));
new Chart('rat',  cfg('bar',  {rat_labels}, {rat_vals}, {pal("purple")}));
new Chart('rch',  cfg('bar',  {rch_labels}, {rch_vals}, {pal("purple")}));
new Chart('rpl',  cfg('doughnut',{rpl_labels},{rpl_vals},{pal("purple")}));
new Chart('rprod',cfg('bar',  {rprod_labels},{rprod_vals},{pal("purple")}));
</script>
</body>
</html>"""


def main():
    try:
        report = load_report()
    except FileNotFoundError:
        print(f"ERROR: {REPORT_FILE} not found.")
        print("Run  python analyze_jsonl.py  first to generate it.")
        return

    html = html_page(report)
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        f.write(html)

    path = os.path.abspath(OUTPUT_FILE)
    print(f"Dashboard written to: {path}")
    webbrowser.open(f"file:///{path}")
    print("Opened in browser.")


if __name__ == "__main__":
    main()
