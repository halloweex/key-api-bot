#!/usr/bin/env python3
"""
Analyze purchase gaps to find the correct inactivity window for CLV calculations.

This script calculates:
- Median gap between purchases
- 95th percentile of time between orders
- Distribution of purchase gaps
"""
import duckdb
from pathlib import Path
import statistics

DB_PATH = Path(__file__).parent.parent / "data" / "analytics.duckdb"


def percentile(data: list, p: float) -> float:
    """Calculate percentile without numpy."""
    if not data:
        return 0
    sorted_data = sorted(data)
    k = (len(sorted_data) - 1) * (p / 100)
    f = int(k)
    c = f + 1 if f + 1 < len(sorted_data) else f
    if f == c:
        return sorted_data[f]
    return sorted_data[f] * (c - k) + sorted_data[c] * (k - f)


def analyze_purchase_gaps():
    """Analyze gaps between customer purchases."""
    # Use access_mode for better compatibility with concurrent access
    conn = duckdb.connect(str(DB_PATH), config={'access_mode': 'read_only'})

    # Get all purchase gaps for customers with multiple orders
    # Only consider retail orders (exclude B2B manager_id=15)
    gaps_query = """
    WITH customer_orders AS (
        -- Get all orders per customer, ordered by date
        SELECT
            buyer_id,
            ordered_at,
            ROW_NUMBER() OVER (PARTITION BY buyer_id ORDER BY ordered_at) as order_num
        FROM orders
        WHERE buyer_id IS NOT NULL
          AND status_id NOT IN (19, 22, 21, 23)  -- Exclude returns
          AND (manager_id IN (22, 4, 16) OR (manager_id IS NULL AND source_id = 4))  -- Retail only
    ),
    purchase_gaps AS (
        -- Calculate gap between consecutive orders
        SELECT
            c1.buyer_id,
            DATE_DIFF('day', c1.ordered_at, c2.ordered_at) as gap_days
        FROM customer_orders c1
        JOIN customer_orders c2
            ON c1.buyer_id = c2.buyer_id
            AND c1.order_num = c2.order_num - 1
        WHERE DATE_DIFF('day', c1.ordered_at, c2.ordered_at) > 0  -- Exclude same-day orders
    )
    SELECT gap_days FROM purchase_gaps ORDER BY gap_days
    """

    gaps = conn.execute(gaps_query).fetchall()

    if not gaps:
        print("No purchase gaps found. Ensure the database has customer data.")
        return

    gap_values = [g[0] for g in gaps]

    print("=" * 60)
    print("PURCHASE GAP ANALYSIS")
    print("=" * 60)
    print(f"\nTotal repeat purchases analyzed: {len(gap_values)}")
    print(f"\n--- Statistics ---")
    print(f"Min gap:              {min(gap_values):>6} days")
    print(f"Max gap:              {max(gap_values):>6} days")
    print(f"Mean gap:             {statistics.mean(gap_values):>6.1f} days")
    print(f"Median gap:           {int(statistics.median(gap_values)):>6} days")
    print(f"Standard deviation:   {statistics.stdev(gap_values):>6.1f} days")

    print(f"\n--- Percentiles ---")
    for p in [50, 75, 80, 85, 90, 95, 99]:
        pct = percentile(gap_values, p)
        print(f"P{p:>2}: {int(pct):>6} days  ({100-p}% of customers reorder within {int(pct)} days)")

    # Distribution analysis
    print(f"\n--- Distribution ---")
    brackets = [(0, 30), (31, 60), (61, 90), (91, 120), (121, 150), (151, 180), (181, 365), (366, 9999)]
    for low, high in brackets:
        count = sum(1 for g in gap_values if low <= g <= high)
        pct = count / len(gap_values) * 100
        label = f"{low}-{high}" if high < 9999 else f"{low}+"
        print(f"  {label:>10} days: {count:>5} ({pct:>5.1f}%)")

    # Customers with multiple orders
    multi_order_query = """
    SELECT COUNT(DISTINCT buyer_id) as repeat_customers
    FROM (
        SELECT buyer_id, COUNT(*) as order_count
        FROM orders
        WHERE buyer_id IS NOT NULL
          AND status_id NOT IN (19, 22, 21, 23)
          AND (manager_id IN (22, 4, 16) OR (manager_id IS NULL AND source_id = 4))
        GROUP BY buyer_id
        HAVING COUNT(*) > 1
    ) t
    """
    repeat_customers = conn.execute(multi_order_query).fetchone()[0]

    total_customers_query = """
    SELECT COUNT(DISTINCT buyer_id)
    FROM orders
    WHERE buyer_id IS NOT NULL
      AND status_id NOT IN (19, 22, 21, 23)
      AND (manager_id IN (22, 4, 16) OR (manager_id IS NULL AND source_id = 4))
    """
    total_customers = conn.execute(total_customers_query).fetchone()[0]

    print(f"\n--- Customer Metrics ---")
    print(f"Total unique customers:     {total_customers}")
    print(f"Customers with 2+ orders:   {repeat_customers}")
    print(f"Repeat customer rate:       {repeat_customers/total_customers*100:.1f}%")

    # Recommendation
    p95 = int(percentile(gap_values, 95))
    p90 = int(percentile(gap_values, 90))
    median = int(statistics.median(gap_values))

    print(f"\n" + "=" * 60)
    print("RECOMMENDATION")
    print("=" * 60)
    print(f"\nBased on the analysis:")
    print(f"  - Median purchase gap: {median} days")
    print(f"  - 90% of customers reorder within: {p90} days")
    print(f"  - 95% of customers reorder within: {p95} days")
    print(f"\nRecommended inactivity window: {p95} days")
    print(f"  (Using 95th percentile ensures you don't lose active customers)")

    if p95 < 90:
        print(f"\nNote: Your P95 is {p95} days, which is below 90 days.")
        print("   This suggests customers buy frequently. Use at least 90 days as buffer.")
    elif p95 > 150:
        print(f"\nNote: Your P95 is {p95} days, which is above 150 days.")
        print("   Consider using 150-180 days to balance retention tracking.")

    conn.close()

    return {
        "median_gap": median,
        "p90": p90,
        "p95": p95,
        "total_customers": total_customers,
        "repeat_customers": repeat_customers,
        "total_gaps_analyzed": len(gap_values)
    }


if __name__ == "__main__":
    analyze_purchase_gaps()
