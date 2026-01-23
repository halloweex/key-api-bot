#!/usr/bin/env python3
"""
Analyze purchase gaps to find the correct inactivity window for CLV calculations.

This script calculates:
- Median/mean gap between all consecutive purchases
- First-to-second purchase gap (conversion timing)
- Percentiles (P50, P75, P90, P95, P99)
- Distribution of purchase gaps

Usage:
    # Local (if DB not locked):
    PYTHONPATH=. python scripts/analyze_purchase_gaps.py

    # In Docker container:
    docker exec keycrm-web cp /app/data/analytics.duckdb /tmp/analytics_copy.duckdb
    docker exec keycrm-web python /app/scripts/analyze_purchase_gaps.py --db /tmp/analytics_copy.duckdb
"""
import argparse
import os
import shutil
import tempfile
import statistics
from pathlib import Path

import duckdb


def get_default_db_path() -> Path:
    """Get default database path based on environment."""
    # Check if running in Docker container
    if os.path.exists('/app/data/analytics.duckdb'):
        return Path('/app/data/analytics.duckdb')
    # Local development
    return Path(__file__).parent.parent / "data" / "analytics.duckdb"


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


def copy_db_if_locked(db_path: Path) -> Path:
    """Copy database to temp location if locked by another process."""
    try:
        # Try to connect read-only
        conn = duckdb.connect(str(db_path), read_only=True)
        conn.close()
        return db_path
    except duckdb.IOException:
        # Database is locked, copy to temp
        print(f"Database locked, copying to temp location...")
        temp_path = Path(tempfile.gettempdir()) / "analytics_copy.duckdb"
        shutil.copy2(db_path, temp_path)
        print(f"Copied to: {temp_path}")
        return temp_path


def analyze_all_gaps(conn: duckdb.DuckDBPyConnection) -> list:
    """Analyze gaps between all consecutive purchases."""
    query = """
    WITH customer_orders AS (
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
        SELECT
            c1.buyer_id,
            DATE_DIFF('day', c1.ordered_at, c2.ordered_at) as gap_days
        FROM customer_orders c1
        JOIN customer_orders c2
            ON c1.buyer_id = c2.buyer_id
            AND c1.order_num = c2.order_num - 1
        WHERE DATE_DIFF('day', c1.ordered_at, c2.ordered_at) > 0
    )
    SELECT gap_days FROM purchase_gaps ORDER BY gap_days
    """
    return [g[0] for g in conn.execute(query).fetchall()]


def analyze_first_to_second_gap(conn: duckdb.DuckDBPyConnection) -> list:
    """Analyze gap between first and second purchase only."""
    query = """
    WITH numbered AS (
        SELECT
            buyer_id,
            ordered_at,
            ROW_NUMBER() OVER (PARTITION BY buyer_id ORDER BY ordered_at) as n
        FROM orders
        WHERE buyer_id IS NOT NULL
          AND status_id NOT IN (19, 22, 21, 23)
          AND (manager_id IN (22, 4, 16) OR (manager_id IS NULL AND source_id = 4))
    )
    SELECT DATE_DIFF('day', o1.ordered_at, o2.ordered_at) as gap
    FROM numbered o1
    JOIN numbered o2 ON o1.buyer_id = o2.buyer_id
    WHERE o1.n = 1 AND o2.n = 2
      AND DATE_DIFF('day', o1.ordered_at, o2.ordered_at) > 0
    ORDER BY gap
    """
    return [g[0] for g in conn.execute(query).fetchall()]


def get_customer_stats(conn: duckdb.DuckDBPyConnection) -> dict:
    """Get overall customer statistics."""
    query = """
    WITH customer_orders AS (
        SELECT
            buyer_id,
            COUNT(*) as order_count
        FROM orders
        WHERE buyer_id IS NOT NULL
          AND status_id NOT IN (19, 22, 21, 23)
          AND (manager_id IN (22, 4, 16) OR (manager_id IS NULL AND source_id = 4))
        GROUP BY buyer_id
    )
    SELECT
        COUNT(*) as total_customers,
        SUM(CASE WHEN order_count > 1 THEN 1 ELSE 0 END) as repeat_customers,
        SUM(CASE WHEN order_count = 1 THEN 1 ELSE 0 END) as one_time_customers
    FROM customer_orders
    """
    result = conn.execute(query).fetchone()
    return {
        "total_customers": result[0],
        "repeat_customers": result[1],
        "one_time_customers": result[2],
    }


def print_gap_analysis(gaps: list, title: str):
    """Print statistics for a gap analysis."""
    if not gaps:
        print(f"\n{title}: No data found")
        return

    print(f"\n{'=' * 60}")
    print(title)
    print('=' * 60)
    print(f"Total data points: {len(gaps)}")
    print(f"\n--- Statistics ---")
    print(f"  Min:    {min(gaps):>6} days")
    print(f"  Max:    {max(gaps):>6} days")
    print(f"  Mean:   {statistics.mean(gaps):>6.1f} days")
    print(f"  Median: {int(statistics.median(gaps)):>6} days")
    print(f"  StdDev: {statistics.stdev(gaps):>6.1f} days")

    print(f"\n--- Percentiles ---")
    for p in [50, 75, 80, 85, 90, 95, 99]:
        pct = percentile(gaps, p)
        print(f"  P{p:>2}: {int(pct):>4} days  ({100-p}% return within {int(pct)} days)")

    print(f"\n--- Distribution ---")
    brackets = [(0, 30), (31, 60), (61, 90), (91, 120), (121, 180), (181, 365), (366, 9999)]
    for low, high in brackets:
        count = sum(1 for g in gaps if low <= g <= high)
        pct = count / len(gaps) * 100
        label = f"{low}-{high}" if high < 9999 else f"{low}+"
        bar = '#' * int(pct / 2)
        print(f"  {label:>8} days: {count:>5} ({pct:>5.1f}%) {bar}")


def main():
    parser = argparse.ArgumentParser(description='Analyze purchase gaps for CLV calculations')
    parser.add_argument('--db', type=str, help='Path to DuckDB database file')
    args = parser.parse_args()

    # Determine database path
    if args.db:
        db_path = Path(args.db)
    else:
        db_path = get_default_db_path()

    if not db_path.exists():
        print(f"Error: Database not found at {db_path}")
        print("Use --db to specify the database path")
        return

    print(f"Using database: {db_path}")

    # Handle locked database
    actual_path = copy_db_if_locked(db_path)

    # Connect and analyze
    conn = duckdb.connect(str(actual_path), read_only=True)

    # Customer stats
    stats = get_customer_stats(conn)
    print(f"\n{'=' * 60}")
    print("CUSTOMER OVERVIEW")
    print('=' * 60)
    print(f"  Total customers:     {stats['total_customers']:>6}")
    print(f"  Repeat customers:    {stats['repeat_customers']:>6} ({stats['repeat_customers']/stats['total_customers']*100:.1f}%)")
    print(f"  One-time customers:  {stats['one_time_customers']:>6} ({stats['one_time_customers']/stats['total_customers']*100:.1f}%)")

    # All consecutive gaps
    all_gaps = analyze_all_gaps(conn)
    print_gap_analysis(all_gaps, "ALL CONSECUTIVE PURCHASE GAPS")

    # First-to-second gap
    first_second_gaps = analyze_first_to_second_gap(conn)
    print_gap_analysis(first_second_gaps, "FIRST-TO-SECOND PURCHASE GAP")

    # Recommendations
    print(f"\n{'=' * 60}")
    print("RECOMMENDATIONS")
    print('=' * 60)
    if all_gaps:
        p95_all = int(percentile(all_gaps, 95))
        print(f"\n  Inactivity window (for CLV): {p95_all} days (P95 of all gaps)")
        print(f"  Suggested setting: {((p95_all // 30) + 1) * 30} days (rounded to months)")

    if first_second_gaps:
        median_first = int(statistics.median(first_second_gaps))
        p75_first = int(percentile(first_second_gaps, 75))
        print(f"\n  Retention campaign timing:")
        print(f"    - First reminder: ~{median_first} days after first purchase (median)")
        print(f"    - Follow-up: ~{p75_first} days (P75)")

    conn.close()


if __name__ == "__main__":
    main()
