#!/usr/bin/env python3
"""
UTM analysis grouped by platform with absolute numbers.
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
    if not comment:
        return {}
    utm_data = {}
    utm_match = re.search(r'UTM:\s*(.+?)(?:\n\n|\n[A-Z]|$)', comment, re.DOTALL)
    if not utm_match:
        return {}
    utm_line = utm_match.group(1).strip()
    pairs = re.findall(r'(\w+):\s*([^;]+)', utm_line)
    for key, value in pairs:
        utm_data[key.strip()] = value.strip()
    return utm_data


import pytz

KYIV_TZ = pytz.timezone('Europe/Kyiv')
RETURN_STATUS_IDS = [19, 22, 21, 23]  # Excluded from revenue calculations

def parse_date(date_str: str) -> datetime:
    """Parse date and convert to Kyiv timezone."""
    if not date_str:
        return None
    try:
        utc_dt = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
        return utc_dt.astimezone(KYIV_TZ)
    except:
        return None


async def analyze_by_platform(start_date: str, end_date: str):
    # Use Kyiv timezone for date comparison (same as DuckDB)
    target_start = KYIV_TZ.localize(datetime.strptime(start_date, "%Y-%m-%d"))
    target_end = KYIV_TZ.localize(datetime.strptime(end_date, "%Y-%m-%d").replace(hour=23, minute=59, second=59))

    print(f"Platform Analysis: Shopify {start_date} to {end_date}")
    print("="*80)

    async with KeyCRMClient() as client:
        params = {
            "filter[source_id]": 4,
            "filter[created_between]": f"2026-01-30,2026-02-17",
            "include": "products,buyer",
            "limit": 50,
        }

        all_orders_raw = await client.fetch_all("order", params, max_pages=100)

        all_orders = []
        for order in all_orders_raw:
            ordered_at = parse_date(order.get('ordered_at'))
            status_id = order.get('status_id')
            if ordered_at and status_id not in RETURN_STATUS_IDS:
                if target_start <= ordered_at <= target_end:
                    all_orders.append(order)

        print(f"\nTotal Shopify orders: {len(all_orders)}")

        # Platform stats
        platforms = {
            'Facebook': {'paid_orders': 0, 'paid_revenue': 0, 'organic_orders': 0, 'organic_revenue': 0, 'pixel_only_orders': 0, 'pixel_only_revenue': 0},
            'TikTok': {'paid_orders': 0, 'paid_revenue': 0, 'organic_orders': 0, 'organic_revenue': 0, 'pixel_only_orders': 0, 'pixel_only_revenue': 0},
            'Instagram': {'paid_orders': 0, 'paid_revenue': 0, 'organic_orders': 0, 'organic_revenue': 0, 'pixel_only_orders': 0, 'pixel_only_revenue': 0},
            'Google': {'paid_orders': 0, 'paid_revenue': 0, 'organic_orders': 0, 'organic_revenue': 0, 'pixel_only_orders': 0, 'pixel_only_revenue': 0},
            'Email (Klaviyo)': {'paid_orders': 0, 'paid_revenue': 0, 'organic_orders': 0, 'organic_revenue': 0, 'pixel_only_orders': 0, 'pixel_only_revenue': 0},
            'Other': {'paid_orders': 0, 'paid_revenue': 0, 'organic_orders': 0, 'organic_revenue': 0, 'pixel_only_orders': 0, 'pixel_only_revenue': 0},
        }

        no_tracking = {'orders': 0, 'revenue': 0}

        # Campaign details
        fb_campaigns = defaultdict(lambda: {'orders': 0, 'revenue': 0})
        tt_campaigns = defaultdict(lambda: {'orders': 0, 'revenue': 0})
        google_campaigns = defaultdict(lambda: {'orders': 0, 'revenue': 0})

        for order in all_orders:
            grand_total = float(order.get('grand_total', 0))
            comment = order.get('manager_comment', '')
            utm_data = parse_utm_from_comment(comment)

            source = (utm_data.get('utm_source') or '').lower()
            medium = (utm_data.get('utm_medium') or '').lower()
            campaign = utm_data.get('utm_campaign') or ''

            has_fb_pixel = '_fbp' in utm_data or '_fbc' in utm_data
            has_tt_pixel = 'ttp' in utm_data
            has_fbclid = 'fbclid' in utm_data

            # Determine platform and paid/organic
            platform = None
            is_paid = False
            is_pixel_only = False

            # Facebook Ads
            if (source.startswith('fbads') or medium.startswith('fbads') or
                campaign.lower().startswith('fbads') or 'facebook_ua' in (utm_data.get('utm_content') or '').lower() or
                (has_fbclid and medium in ['paid', 'cpc'])):
                platform = 'Facebook'
                is_paid = True
                fb_campaigns[campaign]['orders'] += 1
                fb_campaigns[campaign]['revenue'] += grand_total

            # TikTok Ads
            elif source == 'tiktok' and medium in ['paid', 'cpc']:
                platform = 'TikTok'
                is_paid = True
                tt_campaigns[campaign]['orders'] += 1
                tt_campaigns[campaign]['revenue'] += grand_total

            elif any(x in campaign.lower() for x in ['tof', 'mof', 'bof', '| ss |', '| retarget']):
                platform = 'TikTok'
                is_paid = True
                tt_campaigns[campaign]['orders'] += 1
                tt_campaigns[campaign]['revenue'] += grand_total

            # Google Ads
            elif source == 'google' and (medium == 'cpc' or campaign.isdigit()):
                platform = 'Google'
                is_paid = True
                google_campaigns[campaign]['orders'] += 1
                google_campaigns[campaign]['revenue'] += grand_total

            # Instagram Organic
            elif source in ['ig', 'instagram'] and medium == 'social':
                platform = 'Instagram'
                is_paid = False

            # Facebook Organic
            elif source == 'facebook' and medium == 'social':
                platform = 'Facebook'
                is_paid = False

            # TikTok Organic
            elif source == 'tiktok' and medium in ['social', 'organic', '']:
                platform = 'TikTok'
                is_paid = False

            # Email
            elif source in ['klaviyo', 'email'] or medium in ['email', 'klaviyo']:
                platform = 'Email (Klaviyo)'
                is_paid = False

            # Pixel only (no UTM but has pixel)
            elif not source and not medium and (has_fb_pixel or has_tt_pixel):
                is_pixel_only = True
                if has_fb_pixel and has_tt_pixel:
                    # Count for both platforms
                    platforms['Facebook']['pixel_only_orders'] += 1
                    platforms['Facebook']['pixel_only_revenue'] += grand_total
                    platforms['TikTok']['pixel_only_orders'] += 1
                    platforms['TikTok']['pixel_only_revenue'] += grand_total
                    continue
                elif has_fb_pixel:
                    platform = 'Facebook'
                elif has_tt_pixel:
                    platform = 'TikTok'

            # No tracking at all
            elif not utm_data:
                no_tracking['orders'] += 1
                no_tracking['revenue'] += grand_total
                continue

            # Other
            else:
                platform = 'Other'

            if platform:
                if is_pixel_only:
                    platforms[platform]['pixel_only_orders'] += 1
                    platforms[platform]['pixel_only_revenue'] += grand_total
                elif is_paid:
                    platforms[platform]['paid_orders'] += 1
                    platforms[platform]['paid_revenue'] += grand_total
                else:
                    platforms[platform]['organic_orders'] += 1
                    platforms[platform]['organic_revenue'] += grand_total

        # ═══════════════════════════════════════════════════════════════════════
        # PRINT RESULTS
        # ═══════════════════════════════════════════════════════════════════════

        total_revenue = sum(float(o.get('grand_total', 0)) for o in all_orders)

        print("\n" + "="*80)
        print("SUMMARY BY PLATFORM")
        print("="*80)

        for platform_name in ['Facebook', 'TikTok', 'Instagram', 'Google', 'Email (Klaviyo)', 'Other']:
            p = platforms[platform_name]
            total_orders = p['paid_orders'] + p['organic_orders'] + p['pixel_only_orders']
            total_rev = p['paid_revenue'] + p['organic_revenue'] + p['pixel_only_revenue']

            if total_orders == 0:
                continue

            print(f"\n{'─'*40}")
            print(f"{platform_name.upper()}")
            print(f"{'─'*40}")
            print(f"  TOTAL:        {total_orders:>5} orders    {total_rev:>12,.0f} UAH")
            if p['paid_orders'] > 0:
                print(f"  ├─ Paid Ads:  {p['paid_orders']:>5} orders    {p['paid_revenue']:>12,.0f} UAH")
            if p['organic_orders'] > 0:
                print(f"  ├─ Organic:   {p['organic_orders']:>5} orders    {p['organic_revenue']:>12,.0f} UAH")
            if p['pixel_only_orders'] > 0:
                print(f"  └─ Pixel Only:{p['pixel_only_orders']:>5} orders    {p['pixel_only_revenue']:>12,.0f} UAH")

        print(f"\n{'─'*40}")
        print(f"NO TRACKING")
        print(f"{'─'*40}")
        print(f"  TOTAL:        {no_tracking['orders']:>5} orders    {no_tracking['revenue']:>12,.0f} UAH")

        # ═══════════════════════════════════════════════════════════════════════
        # TOP CAMPAIGNS
        # ═══════════════════════════════════════════════════════════════════════

        print("\n" + "="*80)
        print("TOP FACEBOOK ADS CAMPAIGNS")
        print("="*80)
        for campaign, data in sorted(fb_campaigns.items(), key=lambda x: -x[1]['revenue'])[:10]:
            name = campaign[:55] if campaign else "(no name)"
            print(f"  {name:<55} {data['orders']:>4} orders  {data['revenue']:>10,.0f} UAH")

        print("\n" + "="*80)
        print("TOP TIKTOK ADS CAMPAIGNS")
        print("="*80)
        for campaign, data in sorted(tt_campaigns.items(), key=lambda x: -x[1]['revenue'])[:10]:
            name = campaign[:55] if campaign else "(no name)"
            print(f"  {name:<55} {data['orders']:>4} orders  {data['revenue']:>10,.0f} UAH")

        print("\n" + "="*80)
        print("GOOGLE ADS CAMPAIGNS")
        print("="*80)
        for campaign, data in sorted(google_campaigns.items(), key=lambda x: -x[1]['revenue'])[:10]:
            name = campaign[:55] if campaign else "(no name)"
            print(f"  {name:<55} {data['orders']:>4} orders  {data['revenue']:>10,.0f} UAH")

        # ═══════════════════════════════════════════════════════════════════════
        # GRAND TOTALS
        # ═══════════════════════════════════════════════════════════════════════

        print("\n" + "="*80)
        print("GRAND TOTALS")
        print("="*80)

        total_paid_orders = sum(p['paid_orders'] for p in platforms.values())
        total_paid_revenue = sum(p['paid_revenue'] for p in platforms.values())
        total_organic_orders = sum(p['organic_orders'] for p in platforms.values())
        total_organic_revenue = sum(p['organic_revenue'] for p in platforms.values())
        total_pixel_orders = sum(p['pixel_only_orders'] for p in platforms.values())
        total_pixel_revenue = sum(p['pixel_only_revenue'] for p in platforms.values())

        # Note: pixel_only is counted for both FB and TT when both present, so we need unique count
        # Re-count pixel only properly
        pixel_both = 0
        pixel_both_rev = 0
        for order in all_orders:
            grand_total = float(order.get('grand_total', 0))
            comment = order.get('manager_comment', '')
            utm_data = parse_utm_from_comment(comment)
            source = (utm_data.get('utm_source') or '').lower()
            medium = (utm_data.get('utm_medium') or '').lower()
            has_fb = '_fbp' in utm_data or '_fbc' in utm_data
            has_tt = 'ttp' in utm_data
            if not source and not medium and has_fb and has_tt:
                pixel_both += 1
                pixel_both_rev += grand_total

        # Adjust for double counting
        actual_pixel_orders = total_pixel_orders - pixel_both
        actual_pixel_revenue = total_pixel_revenue - pixel_both_rev

        print(f"\n  {'PAID ADS':<20} {total_paid_orders:>5} orders    {total_paid_revenue:>12,.0f} UAH")
        print(f"  {'ORGANIC':<20} {total_organic_orders:>5} orders    {total_organic_revenue:>12,.0f} UAH")
        print(f"  {'PIXEL ONLY':<20} {actual_pixel_orders:>5} orders    {actual_pixel_revenue:>12,.0f} UAH")
        print(f"  {'NO TRACKING':<20} {no_tracking['orders']:>5} orders    {no_tracking['revenue']:>12,.0f} UAH")
        print(f"  {'─'*50}")
        print(f"  {'TOTAL':<20} {len(all_orders):>5} orders    {total_revenue:>12,.0f} UAH")


if __name__ == "__main__":
    asyncio.run(analyze_by_platform("2026-02-01", "2026-02-15"))
