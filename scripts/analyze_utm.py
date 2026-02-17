#!/usr/bin/env python3
"""
Analyze UTM tags from KeyCRM orders - Shopify only.
Fetches wider date range and filters by ordered_at locally.
"""
import asyncio
import os
import sys
import re
from datetime import datetime
from collections import defaultdict

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.keycrm import KeyCRMClient


def parse_utm_from_comment(comment: str) -> dict:
    """Extract UTM data from manager_comment field."""
    if not comment:
        return {}

    utm_data = {}

    # Find the UTM line
    utm_match = re.search(r'UTM:\s*(.+?)(?:\n\n|\n[A-Z]|$)', comment, re.DOTALL)
    if not utm_match:
        return {}

    utm_line = utm_match.group(1).strip()

    # Parse key-value pairs (format: key: value; key2: value2)
    pairs = re.findall(r'(\w+):\s*([^;]+)', utm_line)
    for key, value in pairs:
        utm_data[key.strip()] = value.strip()

    return utm_data


def parse_date(date_str: str) -> datetime:
    """Parse ISO date string to datetime."""
    if not date_str:
        return None
    try:
        return datetime.fromisoformat(date_str.replace("Z", "+00:00"))
    except:
        return None


async def fetch_orders_with_utm(start_date: str, end_date: str):
    """Fetch Shopify orders and extract UTM tags."""

    # Parse target dates for filtering by ordered_at
    target_start = datetime.strptime(start_date, "%Y-%m-%d").replace(tzinfo=None)
    target_end = datetime.strptime(end_date, "%Y-%m-%d").replace(hour=23, minute=59, second=59, tzinfo=None)

    print(f"Fetching SHOPIFY orders where ordered_at is between {start_date} and {end_date}...")
    print(f"(Using wider created_between range to ensure all orders are captured)")

    async with KeyCRMClient() as client:
        # Fetch with wider date range (orders might be created a day later than ordered)
        params = {
            "filter[source_id]": 4,  # Shopify only
            "filter[created_between]": f"2026-01-30,2026-02-17",  # Wider range
            "include": "products,buyer",
            "limit": 50,
        }

        all_orders_raw = await client.fetch_all("order", params, max_pages=100)
        print(f"Total orders fetched from API: {len(all_orders_raw)}")

        # Filter by ordered_at date
        all_orders = []
        for order in all_orders_raw:
            ordered_at = parse_date(order.get('ordered_at'))
            if ordered_at:
                # Convert to naive datetime for comparison
                ordered_at_naive = ordered_at.replace(tzinfo=None)
                if target_start <= ordered_at_naive <= target_end:
                    all_orders.append(order)

        print(f"Orders in date range (by ordered_at): {len(all_orders)}")

        # UTM stats
        utm_stats = defaultdict(lambda: defaultdict(int))

        orders_with_utm = 0
        orders_without_utm = 0

        # Revenue tracking
        revenue_with_utm = 0
        revenue_without_utm = 0
        revenue_by_source = defaultdict(float)
        revenue_by_medium = defaultdict(float)
        revenue_by_campaign = defaultdict(float)

        # Platform tracking (FB vs TikTok)
        fb_orders = 0
        fb_revenue = 0
        tiktok_orders = 0
        tiktok_revenue = 0
        both_orders = 0
        neither_orders = 0

        for order in all_orders:
            grand_total = float(order.get('grand_total', 0))

            comment = order.get('manager_comment', '')
            utm_data = parse_utm_from_comment(comment)

            if utm_data:
                orders_with_utm += 1
                revenue_with_utm += grand_total

                # Collect all UTM params
                for key, value in utm_data.items():
                    utm_stats[key][value] += 1

                # Track by source/medium/campaign
                if 'utm_source' in utm_data:
                    revenue_by_source[utm_data['utm_source']] += grand_total
                if 'utm_medium' in utm_data:
                    revenue_by_medium[utm_data['utm_medium']] += grand_total
                if 'utm_campaign' in utm_data:
                    revenue_by_campaign[utm_data['utm_campaign']] += grand_total

                # Platform detection
                has_fb = '_fbp' in utm_data or '_fbc' in utm_data
                has_tiktok = 'ttp' in utm_data

                if has_fb and has_tiktok:
                    both_orders += 1
                    fb_orders += 1
                    tiktok_orders += 1
                    fb_revenue += grand_total
                    tiktok_revenue += grand_total
                elif has_fb:
                    fb_orders += 1
                    fb_revenue += grand_total
                elif has_tiktok:
                    tiktok_orders += 1
                    tiktok_revenue += grand_total
                else:
                    neither_orders += 1
            else:
                orders_without_utm += 1
                revenue_without_utm += grand_total

        # Print results
        print("\n" + "="*80)
        print("SHOPIFY UTM ANALYSIS (Feb 1-15, 2026)")
        print("="*80)

        print(f"\nTotal Shopify orders: {len(all_orders)}")
        print(f"  With UTM: {orders_with_utm} ({orders_with_utm/len(all_orders)*100:.1f}%)")
        print(f"  Without UTM: {orders_without_utm} ({orders_without_utm/len(all_orders)*100:.1f}%)")

        total_revenue = revenue_with_utm + revenue_without_utm
        print(f"\nTotal revenue: {total_revenue:,.0f} UAH")
        print(f"  With UTM: {revenue_with_utm:,.0f} UAH ({revenue_with_utm/total_revenue*100:.1f}%)")
        print(f"  Without UTM: {revenue_without_utm:,.0f} UAH ({revenue_without_utm/total_revenue*100:.1f}%)")

        print("\n" + "-"*40)
        print("PLATFORM BREAKDOWN (pixel-based):")
        print("-"*40)
        print(f"  Facebook pixel: {fb_orders} orders, {fb_revenue:,.0f} UAH")
        print(f"  TikTok pixel: {tiktok_orders} orders, {tiktok_revenue:,.0f} UAH")
        print(f"  Both platforms: {both_orders} orders (counted in both above)")
        print(f"  Neither (UTM only): {neither_orders} orders")

        print("\n" + "-"*40)
        print("UTM_SOURCE (Top 15):")
        print("-"*40)
        sorted_sources = sorted(utm_stats['utm_source'].items(), key=lambda x: -x[1])
        for value, count in sorted_sources[:15]:
            rev = revenue_by_source.get(value, 0)
            print(f"  {value}: {count} orders, {rev:,.0f} UAH")

        print("\n" + "-"*40)
        print("UTM_MEDIUM (Top 10):")
        print("-"*40)
        sorted_medium = sorted(utm_stats['utm_medium'].items(), key=lambda x: -x[1])
        for value, count in sorted_medium[:10]:
            rev = revenue_by_medium.get(value, 0)
            print(f"  {value}: {count} orders, {rev:,.0f} UAH")

        print("\n" + "-"*40)
        print("UTM_CAMPAIGN (Top 15):")
        print("-"*40)
        sorted_campaigns = sorted(utm_stats['utm_campaign'].items(), key=lambda x: -x[1])
        for value, count in sorted_campaigns[:15]:
            rev = revenue_by_campaign.get(value, 0)
            print(f"  {value}: {count} orders, {rev:,.0f} UAH")

        print("\n" + "-"*40)
        print("UTM_LANG:")
        print("-"*40)
        for value, count in sorted(utm_stats['utm_lang'].items(), key=lambda x: -x[1]):
            print(f"  {value}: {count}")


if __name__ == "__main__":
    start_date = "2026-02-01"
    end_date = "2026-02-15"

    if len(sys.argv) > 2:
        start_date = sys.argv[1]
        end_date = sys.argv[2]

    asyncio.run(fetch_orders_with_utm(start_date, end_date))
