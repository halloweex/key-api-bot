#!/usr/bin/env python3
"""
Deep UTM analysis - categorize organic vs paid traffic.
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
    utm_match = re.search(r'UTM:\s*(.+?)(?:\n\n|\n[A-Z]|$)', comment, re.DOTALL)
    if not utm_match:
        return {}

    utm_line = utm_match.group(1).strip()
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


def categorize_traffic(utm_data: dict) -> dict:
    """
    Categorize traffic source based on UTM parameters.
    Returns dict with category, subcategory, and details.
    """
    source = (utm_data.get('utm_source') or '').lower()
    medium = (utm_data.get('utm_medium') or '').lower()
    campaign = (utm_data.get('utm_campaign') or '').lower()
    content = (utm_data.get('utm_content') or '').lower()

    has_fb_pixel = '_fbp' in utm_data or '_fbc' in utm_data
    has_tiktok_pixel = 'ttp' in utm_data
    has_fbclid = 'fbclid' in utm_data

    result = {
        'category': 'Unknown',
        'subcategory': 'Unknown',
        'platform': None,
        'is_paid': False,
        'details': ''
    }

    # ═══════════════════════════════════════════════════════════════════════════
    # PAID ADS DETECTION
    # ═══════════════════════════════════════════════════════════════════════════

    # Facebook/Meta Ads
    if (source.startswith('fbads') or
        medium.startswith('fbads') or
        campaign.startswith('fbads') or
        'facebook_ua' in content or
        (has_fbclid and medium in ['paid', 'cpc', 'ppc'])):
        result['category'] = 'Paid Social'
        result['subcategory'] = 'Facebook Ads'
        result['platform'] = 'Facebook'
        result['is_paid'] = True
        # Extract campaign name
        for field in [campaign, source, medium]:
            if 'fbads_' in field:
                result['details'] = field
                break
        return result

    # TikTok Ads
    if (source == 'tiktok' and medium in ['paid', 'cpc', 'ppc']) or \
       ('tof' in campaign or 'mof' in campaign or 'bof' in campaign):
        result['category'] = 'Paid Social'
        result['subcategory'] = 'TikTok Ads'
        result['platform'] = 'TikTok'
        result['is_paid'] = True
        result['details'] = campaign
        return result

    # Google Ads (numeric campaign IDs or cpc medium)
    if source == 'google' and (medium == 'cpc' or campaign.isdigit()):
        result['category'] = 'Paid Search'
        result['subcategory'] = 'Google Ads'
        result['platform'] = 'Google'
        result['is_paid'] = True
        result['details'] = campaign
        return result

    # Generic paid detection
    if medium in ['paid', 'cpc', 'ppc', 'paidsocial', 'paid_social']:
        result['category'] = 'Paid'
        result['subcategory'] = f'{source.title()} Paid'
        result['is_paid'] = True
        result['details'] = campaign
        return result

    # ═══════════════════════════════════════════════════════════════════════════
    # ORGANIC SOCIAL
    # ═══════════════════════════════════════════════════════════════════════════

    # Instagram organic (link in bio, stories, etc.)
    if source in ['ig', 'instagram'] and medium == 'social':
        result['category'] = 'Organic Social'
        result['subcategory'] = 'Instagram'
        result['platform'] = 'Instagram'
        result['is_paid'] = False
        if 'link_in_bio' in content:
            result['details'] = 'Link in Bio'
        elif content:
            result['details'] = content
        return result

    # Facebook organic
    if source == 'facebook' and medium == 'social':
        result['category'] = 'Organic Social'
        result['subcategory'] = 'Facebook'
        result['platform'] = 'Facebook'
        result['is_paid'] = False
        result['details'] = content or campaign
        return result

    # TikTok organic
    if source == 'tiktok' and medium in ['social', 'organic']:
        result['category'] = 'Organic Social'
        result['subcategory'] = 'TikTok'
        result['platform'] = 'TikTok'
        result['is_paid'] = False
        result['details'] = campaign
        return result

    # Generic social
    if medium == 'social':
        result['category'] = 'Organic Social'
        result['subcategory'] = source.title() if source else 'Other'
        result['is_paid'] = False
        return result

    # ═══════════════════════════════════════════════════════════════════════════
    # EMAIL MARKETING
    # ═══════════════════════════════════════════════════════════════════════════

    if source in ['klaviyo', 'mailchimp', 'email'] or medium in ['email', 'klaviyo']:
        result['category'] = 'Email'
        result['subcategory'] = 'Klaviyo' if 'klaviyo' in (source + medium) else 'Email'
        result['is_paid'] = False  # Email is owned media, not paid
        result['details'] = campaign
        return result

    # ═══════════════════════════════════════════════════════════════════════════
    # ORGANIC SEARCH
    # ═══════════════════════════════════════════════════════════════════════════

    if source == 'google' and medium in ['organic', 'search', '']:
        result['category'] = 'Organic Search'
        result['subcategory'] = 'Google'
        result['platform'] = 'Google'
        result['is_paid'] = False
        return result

    # ═══════════════════════════════════════════════════════════════════════════
    # REFERRAL / AFFILIATE
    # ═══════════════════════════════════════════════════════════════════════════

    if medium in ['referral', 'affiliate'] or source in ['rivo']:
        result['category'] = 'Referral'
        result['subcategory'] = source.title() if source else 'Other'
        result['is_paid'] = False
        result['details'] = campaign
        return result

    # ═══════════════════════════════════════════════════════════════════════════
    # PRODUCT SYNC / MARKETPLACE
    # ═══════════════════════════════════════════════════════════════════════════

    if medium == 'product_sync' or 'sag_organic' in campaign:
        result['category'] = 'Marketplace'
        result['subcategory'] = 'Product Sync'
        result['is_paid'] = False
        result['details'] = campaign
        return result

    # ═══════════════════════════════════════════════════════════════════════════
    # PIXEL-ONLY (no explicit UTM but has tracking pixels)
    # ═══════════════════════════════════════════════════════════════════════════

    if not source and not medium:
        if has_fb_pixel and has_tiktok_pixel:
            result['category'] = 'Pixel Only'
            result['subcategory'] = 'FB + TikTok Pixels'
            result['details'] = 'No UTM, both pixels present'
        elif has_fb_pixel:
            result['category'] = 'Pixel Only'
            result['subcategory'] = 'Facebook Pixel'
            result['details'] = 'No UTM, FB pixel only'
        elif has_tiktok_pixel:
            result['category'] = 'Pixel Only'
            result['subcategory'] = 'TikTok Pixel'
            result['details'] = 'No UTM, TikTok pixel only'
        return result

    # Fallback
    result['details'] = f"source={source}, medium={medium}, campaign={campaign}"
    return result


async def analyze_utm_deep(start_date: str, end_date: str):
    """Deep UTM analysis."""

    target_start = datetime.strptime(start_date, "%Y-%m-%d").replace(tzinfo=None)
    target_end = datetime.strptime(end_date, "%Y-%m-%d").replace(hour=23, minute=59, second=59, tzinfo=None)

    print(f"Deep UTM Analysis: Shopify orders from {start_date} to {end_date}")
    print("="*80)

    async with KeyCRMClient() as client:
        params = {
            "filter[source_id]": 4,
            "filter[created_between]": f"2026-01-30,2026-02-17",
            "include": "products,buyer",
            "limit": 50,
        }

        all_orders_raw = await client.fetch_all("order", params, max_pages=100)

        # Filter by ordered_at
        all_orders = []
        for order in all_orders_raw:
            ordered_at = parse_date(order.get('ordered_at'))
            if ordered_at:
                ordered_at_naive = ordered_at.replace(tzinfo=None)
                if target_start <= ordered_at_naive <= target_end:
                    all_orders.append(order)

        print(f"\nTotal orders: {len(all_orders)}")

        # ═══════════════════════════════════════════════════════════════════════
        # CATEGORIZE ALL ORDERS
        # ═══════════════════════════════════════════════════════════════════════

        categories = defaultdict(lambda: {'orders': 0, 'revenue': 0, 'details': defaultdict(lambda: {'orders': 0, 'revenue': 0})})
        subcategories = defaultdict(lambda: {'orders': 0, 'revenue': 0})

        paid_total = {'orders': 0, 'revenue': 0}
        organic_total = {'orders': 0, 'revenue': 0}

        # Track campaigns within categories
        campaigns_by_category = defaultdict(lambda: defaultdict(lambda: {'orders': 0, 'revenue': 0}))

        # Orders without any UTM
        no_utm_orders = 0
        no_utm_revenue = 0

        for order in all_orders:
            grand_total = float(order.get('grand_total', 0))
            comment = order.get('manager_comment', '')
            utm_data = parse_utm_from_comment(comment)

            if not utm_data:
                no_utm_orders += 1
                no_utm_revenue += grand_total
                continue

            cat_info = categorize_traffic(utm_data)
            category = cat_info['category']
            subcategory = cat_info['subcategory']
            details = cat_info['details']

            categories[category]['orders'] += 1
            categories[category]['revenue'] += grand_total

            subcategories[f"{category} > {subcategory}"]['orders'] += 1
            subcategories[f"{category} > {subcategory}"]['revenue'] += grand_total

            if details:
                campaigns_by_category[f"{category} > {subcategory}"][details]['orders'] += 1
                campaigns_by_category[f"{category} > {subcategory}"][details]['revenue'] += grand_total

            if cat_info['is_paid']:
                paid_total['orders'] += 1
                paid_total['revenue'] += grand_total
            else:
                organic_total['orders'] += 1
                organic_total['revenue'] += grand_total

        # ═══════════════════════════════════════════════════════════════════════
        # PRINT RESULTS
        # ═══════════════════════════════════════════════════════════════════════

        total_orders = len(all_orders)
        total_revenue = sum(float(o.get('grand_total', 0)) for o in all_orders)

        print("\n" + "="*80)
        print("PAID vs ORGANIC OVERVIEW")
        print("="*80)

        print(f"\n{'Category':<25} {'Orders':>10} {'%':>8} {'Revenue':>15} {'%':>8}")
        print("-"*70)
        print(f"{'PAID (Ads)':<25} {paid_total['orders']:>10} {paid_total['orders']/total_orders*100:>7.1f}% {paid_total['revenue']:>14,.0f} {paid_total['revenue']/total_revenue*100:>7.1f}%")
        print(f"{'ORGANIC (Free)':<25} {organic_total['orders']:>10} {organic_total['orders']/total_orders*100:>7.1f}% {organic_total['revenue']:>14,.0f} {organic_total['revenue']/total_revenue*100:>7.1f}%")
        print(f"{'NO UTM DATA':<25} {no_utm_orders:>10} {no_utm_orders/total_orders*100:>7.1f}% {no_utm_revenue:>14,.0f} {no_utm_revenue/total_revenue*100:>7.1f}%")
        print("-"*70)
        print(f"{'TOTAL':<25} {total_orders:>10} {'100.0%':>8} {total_revenue:>14,.0f} {'100.0%':>8}")

        print("\n" + "="*80)
        print("BREAKDOWN BY CATEGORY")
        print("="*80)

        print(f"\n{'Category':<25} {'Orders':>10} {'%':>8} {'Revenue':>15} {'%':>8}")
        print("-"*70)
        for cat, data in sorted(categories.items(), key=lambda x: -x[1]['revenue']):
            print(f"{cat:<25} {data['orders']:>10} {data['orders']/total_orders*100:>7.1f}% {data['revenue']:>14,.0f} {data['revenue']/total_revenue*100:>7.1f}%")

        print("\n" + "="*80)
        print("BREAKDOWN BY SUBCATEGORY")
        print("="*80)

        print(f"\n{'Subcategory':<40} {'Orders':>8} {'Revenue':>15} {'AOV':>10}")
        print("-"*75)
        for subcat, data in sorted(subcategories.items(), key=lambda x: -x[1]['revenue']):
            aov = data['revenue'] / data['orders'] if data['orders'] > 0 else 0
            print(f"{subcat:<40} {data['orders']:>8} {data['revenue']:>14,.0f} {aov:>9,.0f}")

        # ═══════════════════════════════════════════════════════════════════════
        # DETAILED CAMPAIGN BREAKDOWN
        # ═══════════════════════════════════════════════════════════════════════

        print("\n" + "="*80)
        print("TOP CAMPAIGNS BY SUBCATEGORY")
        print("="*80)

        for subcat in sorted(campaigns_by_category.keys()):
            campaigns = campaigns_by_category[subcat]
            if not campaigns:
                continue

            print(f"\n{subcat}:")
            print("-"*60)
            sorted_campaigns = sorted(campaigns.items(), key=lambda x: -x[1]['revenue'])[:10]
            for campaign, data in sorted_campaigns:
                # Truncate long campaign names
                display_name = campaign[:50] + '...' if len(campaign) > 50 else campaign
                print(f"  {display_name:<52} {data['orders']:>5} orders, {data['revenue']:>10,.0f} UAH")

        # ═══════════════════════════════════════════════════════════════════════
        # PLATFORM PIXEL ANALYSIS
        # ═══════════════════════════════════════════════════════════════════════

        print("\n" + "="*80)
        print("PIXEL TRACKING ANALYSIS")
        print("="*80)

        pixel_stats = {
            'fb_only': {'orders': 0, 'revenue': 0},
            'tiktok_only': {'orders': 0, 'revenue': 0},
            'both': {'orders': 0, 'revenue': 0},
            'none': {'orders': 0, 'revenue': 0},
        }

        for order in all_orders:
            grand_total = float(order.get('grand_total', 0))
            comment = order.get('manager_comment', '')
            utm_data = parse_utm_from_comment(comment)

            has_fb = '_fbp' in utm_data or '_fbc' in utm_data
            has_tiktok = 'ttp' in utm_data

            if has_fb and has_tiktok:
                pixel_stats['both']['orders'] += 1
                pixel_stats['both']['revenue'] += grand_total
            elif has_fb:
                pixel_stats['fb_only']['orders'] += 1
                pixel_stats['fb_only']['revenue'] += grand_total
            elif has_tiktok:
                pixel_stats['tiktok_only']['orders'] += 1
                pixel_stats['tiktok_only']['revenue'] += grand_total
            else:
                pixel_stats['none']['orders'] += 1
                pixel_stats['none']['revenue'] += grand_total

        print(f"\n{'Pixel Status':<25} {'Orders':>10} {'%':>8} {'Revenue':>15} {'%':>8}")
        print("-"*70)
        labels = {
            'both': 'FB + TikTok Pixels',
            'fb_only': 'Facebook Pixel Only',
            'tiktok_only': 'TikTok Pixel Only',
            'none': 'No Pixels'
        }
        for key, label in labels.items():
            data = pixel_stats[key]
            print(f"{label:<25} {data['orders']:>10} {data['orders']/total_orders*100:>7.1f}% {data['revenue']:>14,.0f} {data['revenue']/total_revenue*100:>7.1f}%")


if __name__ == "__main__":
    start_date = "2026-02-01"
    end_date = "2026-02-15"

    if len(sys.argv) > 2:
        start_date = sys.argv[1]
        end_date = sys.argv[2]

    asyncio.run(analyze_utm_deep(start_date, end_date))
