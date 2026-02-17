"""DuckDBStore traffic analytics methods."""
from __future__ import annotations

import logging
import re
from typing import Optional, List, Dict, Any, Tuple

logger = logging.getLogger(__name__)


class TrafficMixin:

    @staticmethod
    def _parse_utm_from_comment(comment: str) -> Dict[str, Any]:
        """Parse UTM data from manager_comment field.

        The comment format is:
        UTM: utm_source: value; utm_medium: value; ...

        Also extracts pixel IDs like _fbp, _fbc, ttp, fbclid.
        """
        if not comment:
            return {}

        utm_data = {}

        # Find UTM block: "UTM: key: value; key: value; ..."
        utm_match = re.search(r'UTM:\s*(.+?)(?:\n\n|\n[A-Z]|$)', comment, re.DOTALL)
        if not utm_match:
            return {}

        utm_line = utm_match.group(1).strip()

        # Parse key: value pairs separated by semicolons
        pairs = re.findall(r'(\w+):\s*([^;]+)', utm_line)
        for key, value in pairs:
            utm_data[key.strip().lower()] = value.strip()

        return utm_data

    @staticmethod
    def _classify_traffic(utm_data: Dict[str, Any]) -> Tuple[str, str]:
        """Classify traffic based on UTM data.

        Returns:
            Tuple of (traffic_type, platform)

        Traffic types:
            - paid_confirmed: Strong evidence of paid ad (fbc, fbclid+medium=paid, campaign patterns)
            - paid_likely: Medium confidence paid (utm_medium=cpc/paid but no click tracking)
            - organic: Explicit organic/social medium with no ad indicators
            - pixel_only: Only pixel present, no UTM parameters
            - unknown: No tracking data at all
        """
        source = (utm_data.get('utm_source') or '').lower()
        medium = (utm_data.get('utm_medium') or '').lower()
        campaign = (utm_data.get('utm_campaign') or '').lower()
        content = (utm_data.get('utm_content') or '').lower()

        has_fbc = '_fbc' in utm_data  # Facebook click tracking = ad click
        has_fbp = '_fbp' in utm_data  # Facebook browser pixel
        has_ttp = 'ttp' in utm_data   # TikTok pixel
        has_fbclid = 'fbclid' in utm_data

        # Default values
        platform = 'other'
        traffic_type = 'unknown'

        # ─── Platform Detection ─────────────────────────────────────────────────

        # Facebook Ads - explicit patterns
        if (source.startswith('fbads') or medium.startswith('fbads') or
            campaign.startswith('fbads') or 'facebook_ua' in content):
            platform = 'facebook'
            traffic_type = 'paid_confirmed'

        # Facebook Ads - fbc (click tracking) indicates ad click
        elif has_fbc:
            platform = 'facebook'
            traffic_type = 'paid_confirmed'

        # Facebook paid via medium
        elif has_fbclid and medium in ['paid', 'cpc']:
            platform = 'facebook'
            traffic_type = 'paid_confirmed'

        # TikTok Ads - explicit campaign patterns (TOF/MOF/BOF = funnel stages)
        elif any(x in campaign for x in ['tof', 'mof', 'bof', '| ss |', '| retarget', '| dynamic']):
            platform = 'tiktok'
            traffic_type = 'paid_confirmed'

        # TikTok Ads - source + medium
        elif source == 'tiktok' and medium in ['paid', 'cpc']:
            platform = 'tiktok'
            traffic_type = 'paid_confirmed'

        # Google Ads
        elif source == 'google' and (medium == 'cpc' or campaign.isdigit()):
            platform = 'google'
            traffic_type = 'paid_confirmed'

        # Instagram Organic (social medium)
        elif source in ['ig', 'instagram'] and medium in ['social', 'organic', '']:
            platform = 'instagram'
            traffic_type = 'organic'

        # Facebook Organic (social medium)
        elif source == 'facebook' and medium in ['social', 'organic']:
            platform = 'facebook'
            traffic_type = 'organic'

        # TikTok Organic
        elif source == 'tiktok' and medium in ['social', 'organic', '']:
            platform = 'tiktok'
            traffic_type = 'organic'

        # Email (Klaviyo)
        elif source in ['klaviyo', 'email'] or medium in ['email', 'klaviyo']:
            platform = 'email'
            traffic_type = 'organic'  # Email is not "paid ads"

        # Pixel only - no UTM but has tracking pixel
        elif not source and not medium:
            if has_fbp or has_fbc:
                platform = 'facebook'
                traffic_type = 'pixel_only'
            elif has_ttp:
                platform = 'tiktok'
                traffic_type = 'pixel_only'
            else:
                platform = 'other'
                traffic_type = 'unknown'

        # Has some UTM but doesn't match known patterns
        elif source or medium:
            # Likely paid if medium indicates payment
            if medium in ['cpc', 'paid', 'ppc']:
                traffic_type = 'paid_likely'
            elif medium in ['social', 'organic', 'referral']:
                traffic_type = 'organic'
            else:
                traffic_type = 'unknown'

            # Try to infer platform from source
            if 'facebook' in source or 'fb' in source:
                platform = 'facebook'
            elif 'tiktok' in source or 'tt' in source:
                platform = 'tiktok'
            elif 'google' in source:
                platform = 'google'
            elif 'insta' in source or 'ig' in source:
                platform = 'instagram'

        return traffic_type, platform

    async def refresh_utm_silver_layer(self) -> int:
        """Parse UTM data from orders and populate silver_order_utm table.

        Returns:
            Number of orders processed
        """
        async with self.connection() as conn:
            # Get orders with manager_comment that haven't been parsed yet
            orders = conn.execute("""
                SELECT o.id, o.manager_comment
                FROM orders o
                WHERE o.manager_comment IS NOT NULL
                  AND o.manager_comment != ''
                  AND NOT EXISTS (SELECT 1 FROM silver_order_utm u WHERE u.order_id = o.id)
            """).fetchall()

            if not orders:
                return 0

            utm_rows = []
            for order_id, comment in orders:
                utm_data = self._parse_utm_from_comment(comment)

                # Skip if no UTM data parsed
                if not utm_data:
                    # Still insert a record with unknown type for tracking
                    utm_rows.append({
                        'order_id': order_id,
                        'traffic_type': 'unknown',
                        'platform': 'other',
                    })
                    continue

                traffic_type, platform = self._classify_traffic(utm_data)

                utm_rows.append({
                    'order_id': order_id,
                    'utm_source': utm_data.get('utm_source'),
                    'utm_medium': utm_data.get('utm_medium'),
                    'utm_campaign': utm_data.get('utm_campaign'),
                    'utm_content': utm_data.get('utm_content'),
                    'utm_term': utm_data.get('utm_term'),
                    'utm_lang': utm_data.get('utm_lang'),
                    'fbp': utm_data.get('_fbp'),
                    'fbc': utm_data.get('_fbc'),
                    'ttp': utm_data.get('ttp'),
                    'fbclid': utm_data.get('fbclid'),
                    'traffic_type': traffic_type,
                    'platform': platform,
                })

            # Batch insert
            if utm_rows:
                conn.executemany("""
                    INSERT OR REPLACE INTO silver_order_utm
                        (order_id, utm_source, utm_medium, utm_campaign, utm_content,
                         utm_term, utm_lang, fbp, fbc, ttp, fbclid, traffic_type, platform, parsed_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                """, [
                    (
                        r['order_id'], r.get('utm_source'), r.get('utm_medium'),
                        r.get('utm_campaign'), r.get('utm_content'), r.get('utm_term'),
                        r.get('utm_lang'), r.get('fbp'), r.get('fbc'), r.get('ttp'),
                        r.get('fbclid'), r['traffic_type'], r['platform']
                    )
                    for r in utm_rows
                ])

            logger.info(f"Parsed UTM data for {len(utm_rows)} orders")
            return len(utm_rows)

    async def refresh_traffic_gold_layer(self) -> int:
        """Rebuild gold_daily_traffic from silver layers.

        Aggregates traffic by date, source_id, sales_type, platform, and traffic_type.

        Returns:
            Number of rows in gold_daily_traffic
        """
        async with self.connection() as conn:
            conn.execute("""
                CREATE OR REPLACE TABLE gold_daily_traffic AS
                SELECT
                    s.order_date AS date,
                    s.source_id,
                    s.sales_type,
                    COALESCE(u.platform, 'other') AS platform,
                    COALESCE(u.traffic_type, 'unknown') AS traffic_type,
                    COUNT(DISTINCT s.id) AS orders_count,
                    COALESCE(SUM(s.grand_total), 0) AS revenue
                FROM silver_orders s
                LEFT JOIN silver_order_utm u ON s.id = u.order_id
                WHERE NOT s.is_return
                  AND s.is_active_source
                  AND s.order_date IS NOT NULL
                GROUP BY s.order_date, s.source_id, s.sales_type, u.platform, u.traffic_type
                ORDER BY s.order_date DESC, s.source_id
            """)

            # Recreate indexes
            conn.execute("CREATE INDEX IF NOT EXISTS idx_gold_traffic_date ON gold_daily_traffic(date)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_gold_traffic_platform ON gold_daily_traffic(platform)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_gold_traffic_sales_type ON gold_daily_traffic(sales_type)")

            row_count = conn.execute("SELECT COUNT(*) FROM gold_daily_traffic").fetchone()[0]
            logger.info(f"Refreshed gold_daily_traffic: {row_count} rows")
            return row_count

    async def get_traffic_analytics(
        self,
        start_date: date,
        end_date: date,
        sales_type: str = "all",
        source_id: Optional[int] = None,
    ) -> Dict[str, Any]:
        """Get traffic analytics for a date range.

        Returns breakdown by platform and traffic type.
        """
        # Build filters
        filters = [
            "g.date >= ?",
            "g.date <= ?",
        ]
        params: list = [start_date, end_date]

        if source_id:
            filters.append("g.source_id = ?")
            params.append(source_id)

        # Sales type filter (uses sales_type column in gold table)
        if sales_type != "all":
            filters.append("g.sales_type = ?")
            params.append(sales_type)

        where_clause = " AND ".join(filters)

        query = f"""
            SELECT
                g.platform,
                g.traffic_type,
                SUM(g.orders_count) AS orders,
                SUM(g.revenue) AS revenue
            FROM gold_daily_traffic g
            WHERE {where_clause}
            GROUP BY g.platform, g.traffic_type
            ORDER BY revenue DESC
        """

        rows = await self._fetch_all(query, params)

        # Aggregate by platform
        platforms = {}
        traffic_types = {}

        for row in rows:
            platform, traffic_type, orders, revenue = row

            if platform not in platforms:
                platforms[platform] = {'orders': 0, 'revenue': 0.0}
            platforms[platform]['orders'] += orders
            platforms[platform]['revenue'] += float(revenue)

            if traffic_type not in traffic_types:
                traffic_types[traffic_type] = {'orders': 0, 'revenue': 0.0}
            traffic_types[traffic_type]['orders'] += orders
            traffic_types[traffic_type]['revenue'] += float(revenue)

        # Calculate totals
        total_orders = sum(p['orders'] for p in platforms.values())
        total_revenue = sum(p['revenue'] for p in platforms.values())

        # Paid vs organic summary
        paid_orders = sum(
            traffic_types.get(t, {}).get('orders', 0)
            for t in ['paid_confirmed', 'paid_likely']
        )
        paid_revenue = sum(
            traffic_types.get(t, {}).get('revenue', 0)
            for t in ['paid_confirmed', 'paid_likely']
        )
        organic_orders = traffic_types.get('organic', {}).get('orders', 0)
        organic_revenue = traffic_types.get('organic', {}).get('revenue', 0)

        return {
            'period': {
                'start': start_date.isoformat(),
                'end': end_date.isoformat(),
            },
            'totals': {
                'orders': total_orders,
                'revenue': round(total_revenue, 2),
            },
            'summary': {
                'paid': {'orders': paid_orders, 'revenue': round(paid_revenue, 2)},
                'organic': {'orders': organic_orders, 'revenue': round(organic_revenue, 2)},
                'pixel_only': traffic_types.get('pixel_only', {'orders': 0, 'revenue': 0.0}),
                'unknown': traffic_types.get('unknown', {'orders': 0, 'revenue': 0.0}),
            },
            'by_platform': {
                k: {'orders': v['orders'], 'revenue': round(v['revenue'], 2)}
                for k, v in platforms.items()
            },
            'by_traffic_type': {
                k: {'orders': v['orders'], 'revenue': round(v['revenue'], 2)}
                for k, v in traffic_types.items()
            },
        }

    async def get_traffic_trend(
        self,
        start_date: date,
        end_date: date,
        sales_type: str = "all",
        source_id: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        """Get daily traffic trend with platform breakdown.

        Returns list of daily entries with paid/organic split.
        """
        filters = [
            "g.date >= ?",
            "g.date <= ?",
        ]
        params: list = [start_date, end_date]

        if source_id:
            filters.append("g.source_id = ?")
            params.append(source_id)

        if sales_type != "all":
            filters.append("g.sales_type = ?")
            params.append(sales_type)

        where_clause = " AND ".join(filters)

        query = f"""
            SELECT
                g.date,
                g.traffic_type,
                SUM(g.orders_count) AS orders,
                SUM(g.revenue) AS revenue
            FROM gold_daily_traffic g
            WHERE {where_clause}
            GROUP BY g.date, g.traffic_type
            ORDER BY g.date
        """

        rows = await self._fetch_all(query, params)

        # Group by date
        daily_data = {}
        for row in rows:
            d, traffic_type, orders, revenue = row
            date_str = d.isoformat() if hasattr(d, 'isoformat') else str(d)

            if date_str not in daily_data:
                daily_data[date_str] = {
                    'date': date_str,
                    'paid_orders': 0,
                    'paid_revenue': 0.0,
                    'organic_orders': 0,
                    'organic_revenue': 0.0,
                    'other_orders': 0,
                    'other_revenue': 0.0,
                }

            if traffic_type in ['paid_confirmed', 'paid_likely']:
                daily_data[date_str]['paid_orders'] += orders
                daily_data[date_str]['paid_revenue'] += float(revenue)
            elif traffic_type == 'organic':
                daily_data[date_str]['organic_orders'] += orders
                daily_data[date_str]['organic_revenue'] += float(revenue)
            else:
                daily_data[date_str]['other_orders'] += orders
                daily_data[date_str]['other_revenue'] += float(revenue)

        # Convert to list and round values
        result = []
        for d in sorted(daily_data.keys()):
            entry = daily_data[d]
            entry['paid_revenue'] = round(entry['paid_revenue'], 2)
            entry['organic_revenue'] = round(entry['organic_revenue'], 2)
            entry['other_revenue'] = round(entry['other_revenue'], 2)
            result.append(entry)

        return result

    async def get_traffic_transactions(
        self,
        start_date: date,
        end_date: date,
        sales_type: str = "all",
        source_id: Optional[int] = None,
        traffic_type: Optional[str] = None,
        limit: int = 50,
        offset: int = 0,
    ) -> Dict[str, Any]:
        """Get individual orders with traffic attribution details.

        Returns paginated list of orders joined with UTM data,
        plus evidence explaining each traffic classification.
        """
        filters = [
            "NOT s.is_return",
            "s.is_active_source",
            "s.order_date >= ?",
            "s.order_date <= ?",
        ]
        params: list = [start_date, end_date]

        if source_id:
            filters.append("s.source_id = ?")
            params.append(source_id)

        if sales_type != "all":
            filters.append("s.sales_type = ?")
            params.append(sales_type)

        if traffic_type:
            filters.append("COALESCE(u.traffic_type, 'unknown') = ?")
            params.append(traffic_type)

        where_clause = " AND ".join(filters)

        # Get total count
        count_query = f"""
            SELECT COUNT(*)
            FROM silver_orders s
            LEFT JOIN silver_order_utm u ON s.id = u.order_id
            WHERE {where_clause}
        """
        count_row = await self._fetch_one(count_query, params)
        total = count_row[0] if count_row else 0

        # Get paginated rows
        data_query = f"""
            SELECT
                s.id, s.order_date, s.grand_total, s.source_name,
                COALESCE(u.traffic_type, 'unknown') AS traffic_type,
                COALESCE(u.platform, 'other') AS platform,
                u.utm_source, u.utm_medium, u.utm_campaign, u.utm_content,
                u.fbp, u.fbc, u.ttp, u.fbclid
            FROM silver_orders s
            LEFT JOIN silver_order_utm u ON s.id = u.order_id
            WHERE {where_clause}
            ORDER BY s.order_date DESC, s.id DESC
            LIMIT ? OFFSET ?
        """
        data_params = params + [limit, offset]
        rows = await self._fetch_all(data_query, data_params)

        transactions = []
        for row in rows:
            (order_id, order_date, grand_total, source_name,
             tt, platform, utm_source, utm_medium, utm_campaign, utm_content,
             fbp, fbc, ttp, fbclid) = row

            evidence = self._build_evidence(
                utm_source, utm_medium, utm_campaign, utm_content,
                fbp, fbc, ttp, fbclid,
            )

            transactions.append({
                'id': order_id,
                'date': order_date.isoformat() if hasattr(order_date, 'isoformat') else str(order_date),
                'amount': float(grand_total),
                'source': source_name,
                'traffic_type': tt,
                'platform': platform,
                'evidence': evidence,
            })

        return {
            'transactions': transactions,
            'total': total,
            'limit': limit,
            'offset': offset,
        }

    @staticmethod
    def _build_evidence(
        utm_source: Optional[str],
        utm_medium: Optional[str],
        utm_campaign: Optional[str],
        utm_content: Optional[str],
        fbp: Optional[str],
        fbc: Optional[str],
        ttp: Optional[str],
        fbclid: Optional[str],
    ) -> List[Dict[str, str]]:
        """Build evidence list explaining WHY an order was classified.

        Returns list of {field, value, reason?} dicts, priority-ordered
        to match _classify_traffic() logic.
        """
        evidence = []

        # Priority 1: Ad click trackers (strongest signal)
        if fbc:
            evidence.append({'field': '_fbc', 'value': fbc, 'reason': 'Ad click tracked'})
        if fbclid:
            evidence.append({'field': 'fbclid', 'value': fbclid, 'reason': 'Facebook click ID'})

        # Priority 2: UTM parameters
        if utm_source:
            evidence.append({'field': 'utm_source', 'value': utm_source})
        if utm_medium:
            evidence.append({'field': 'utm_medium', 'value': utm_medium})
        if utm_campaign:
            evidence.append({'field': 'utm_campaign', 'value': utm_campaign})
        if utm_content:
            evidence.append({'field': 'utm_content', 'value': utm_content})

        # Priority 3: Pixel-only trackers
        if fbp and not fbc and not utm_source:
            evidence.append({'field': '_fbp', 'value': fbp, 'reason': 'Browser pixel only'})
        elif fbp and fbc:
            evidence.append({'field': '_fbp', 'value': fbp})
        if ttp:
            evidence.append({'field': 'ttp', 'value': ttp, 'reason': 'TikTok pixel'})

        return evidence

    # ─── Sync Methods ─────────────────────────────────────────────────────────
