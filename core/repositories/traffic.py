"""DuckDBStore traffic analytics methods."""
from __future__ import annotations

import logging
import re
from datetime import date
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
            - paid_confirmed: Strong evidence of paid ad (explicit UTM + click tracking)
            - paid_likely: Medium confidence paid (cookie/fbclid only, no explicit UTM)
            - manager: Sales manager driven (campaign starts with sales_manager_)
            - organic: Explicit organic/social medium with no ad indicators
            - pixel_only: Only pixel present, no UTM parameters
            - unknown: No tracking data at all

        Priority: explicit UTM params > cookies/pixels (cookies persist 90 days
        and don't indicate current session intent).
        """
        source = (utm_data.get('utm_source') or '').lower()
        medium = (utm_data.get('utm_medium') or '').lower()
        campaign = (utm_data.get('utm_campaign') or '').lower()
        content = (utm_data.get('utm_content') or '').lower()

        has_fbc = '_fbc' in utm_data  # Facebook click cookie (persists 90 days)
        has_fbp = '_fbp' in utm_data  # Facebook browser pixel
        has_ttp = 'ttp' in utm_data   # TikTok pixel
        has_fbclid = 'fbclid' in utm_data

        # ─── Explicit UTM rules (highest priority) ──────────────────────────────

        # 1. Sales manager tag (campaign starts with sales_manager_)
        if campaign.startswith('sales_manager_'):
            return 'manager', 'manager'

        # 2. Email / Klaviyo (source or medium indicates email)
        if source in ['klaviyo', 'email'] or medium in ['email', 'klaviyo']:
            return 'organic', 'email'

        # 3. Facebook Ads - explicit fbads patterns
        if (source.startswith('fbads') or medium.startswith('fbads') or
                campaign.startswith('fbads') or 'facebook_ua' in content):
            return 'paid_confirmed', 'facebook'

        # 4. Facebook Ads - explicit UTM (source=facebook, medium=paid/cpc)
        if source == 'facebook' and medium in ['paid', 'cpc']:
            return 'paid_confirmed', 'facebook'

        # 5. TikTok Ads - campaign patterns (TOF/MOF/BOF = funnel stages)
        if campaign and re.search(
            r'(?:^|[\s_|])(?:tof|mof|bof)(?:[\s_|]|$)|'
            r'\| ss \||\| retarget|\| dynamic',
            campaign
        ):
            return 'paid_confirmed', 'tiktok'

        # 6. TikTok Ads - source + medium
        if source == 'tiktok' and medium in ['paid', 'cpc']:
            return 'paid_confirmed', 'tiktok'

        # 7. Google Ads (source=google, medium=cpc OR campaign is numeric)
        if source == 'google' and (medium == 'cpc' or (campaign and campaign.isdigit())):
            return 'paid_confirmed', 'google'

        # 8. Google Shopping organic (source=google, medium=product_sync)
        if source == 'google' and medium == 'product_sync':
            return 'organic', 'google'

        # 9. Instagram organic (source=ig/instagram)
        if source in ['ig', 'instagram']:
            return 'organic', 'instagram'

        # 10. Facebook organic (source=facebook, medium=social/organic)
        if source == 'facebook' and medium in ['social', 'organic']:
            return 'organic', 'facebook'

        # 11. TikTok organic (source=tiktok, medium not paid/cpc)
        if source == 'tiktok' and medium in ['social', 'organic', '']:
            return 'organic', 'tiktok'

        # ─── Generic UTM fallback (has source/medium but no known pattern) ──────

        if source or medium:
            # Infer platform from source (token-based to avoid substring false positives)
            platform = 'other'
            source_tokens = set(re.split(r'[_\-\s]', source))
            if 'facebook' in source_tokens or source_tokens & {'fb', 'fbads'}:
                platform = 'facebook'
            elif 'tiktok' in source_tokens or source_tokens & {'tt', 'ttads'}:
                platform = 'tiktok'
            elif 'google' in source_tokens:
                platform = 'google'
            elif source_tokens & {'instagram', 'insta', 'ig'}:
                platform = 'instagram'

            if medium in ['cpc', 'paid', 'ppc']:
                return 'paid_likely', platform
            elif medium in ['social', 'organic', 'referral']:
                return 'organic', platform
            else:
                return 'unknown', platform

        # ─── Cookie/pixel fallback (no explicit UTM matched above) ──────────────

        # 12. _fbc only (no UTM) → paid_likely (cookie persists 90 days, not confirmed)
        if has_fbc:
            return 'paid_likely', 'facebook'

        # 13. fbclid only → paid_likely (URL click ID, no UTM params)
        if has_fbclid:
            return 'paid_likely', 'facebook'

        # 14. Pixel-only — passive tracking
        if has_fbp:
            return 'pixel_only', 'facebook'
        if has_ttp:
            return 'pixel_only', 'tiktok'

        # 15. No tracking data at all
        return 'unknown', 'other'

    async def refresh_utm_silver_layer(self) -> int:
        """Parse UTM data from orders and populate silver_order_utm table.

        Returns:
            Number of orders processed
        """
        async with self.connection() as conn:
            # Get orders that need UTM parsing:
            # - never parsed (no silver_order_utm row)
            # - stale (order updated after last parse)
            orders = conn.execute("""
                SELECT o.id, o.manager_comment
                FROM orders o
                LEFT JOIN silver_order_utm u ON u.order_id = o.id
                WHERE o.manager_comment IS NOT NULL
                  AND o.manager_comment != ''
                  AND (
                      u.order_id IS NULL
                      OR o.updated_at > u.parsed_at
                  )
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

    async def refresh_traffic_gold_layer(
        self, affected_dates: set[date] | None = None,
    ) -> int:
        """Rebuild gold_daily_traffic from silver layers.

        Uses transactional DELETE+INSERT (preserves indexes).
        When affected_dates is provided, only rebuilds those dates.

        Args:
            affected_dates: Dates to rebuild (None = full rebuild)

        Returns:
            Number of rows in gold_daily_traffic
        """
        _traffic_select = """
            SELECT
                s.order_date AS date,
                s.source_id,
                s.sales_type,
                COALESCE(u.platform,
                    CASE s.source_id WHEN 1 THEN 'instagram' WHEN 2 THEN 'telegram' ELSE 'other' END
                ) AS platform,
                COALESCE(u.traffic_type,
                    CASE WHEN s.source_id IN (1, 2) THEN 'organic' ELSE 'unknown' END
                ) AS traffic_type,
                COUNT(DISTINCT s.id) AS orders_count,
                COALESCE(SUM(s.grand_total), 0) AS revenue
            FROM silver_orders s
            LEFT JOIN silver_order_utm u ON s.id = u.order_id
            WHERE NOT s.is_return
              AND s.is_active_source
              AND s.order_date IS NOT NULL
        """

        async with self.connection() as conn:
            conn.execute("BEGIN TRANSACTION")
            try:
                if affected_dates:
                    date_params = list(affected_dates)
                    date_placeholders = ",".join("?" * len(date_params))
                    conn.execute(f"DELETE FROM gold_daily_traffic WHERE date IN ({date_placeholders})", date_params)
                    conn.execute(f"""
                        INSERT INTO gold_daily_traffic
                        {_traffic_select}
                          AND s.order_date IN ({date_placeholders})
                        GROUP BY s.order_date, s.source_id, s.sales_type, u.platform, u.traffic_type
                    """, date_params)
                else:
                    conn.execute("DELETE FROM gold_daily_traffic")
                    conn.execute(f"""
                        INSERT INTO gold_daily_traffic
                        {_traffic_select}
                        GROUP BY s.order_date, s.source_id, s.sales_type, u.platform, u.traffic_type
                    """)

                row_count = conn.execute("SELECT COUNT(*) FROM gold_daily_traffic").fetchone()[0]
                conn.execute("COMMIT")
            except Exception:
                conn.execute("ROLLBACK")
                raise

            logger.info(f"Refreshed gold_daily_traffic: {row_count} rows"
                        f"{f' (incremental: {len(affected_dates)} dates)' if affected_dates else ''}")
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
        paid_confirmed = traffic_types.get('paid_confirmed', {'orders': 0, 'revenue': 0.0})
        paid_likely = traffic_types.get('paid_likely', {'orders': 0, 'revenue': 0.0})
        paid_orders = paid_confirmed.get('orders', 0) + paid_likely.get('orders', 0)
        paid_revenue = paid_confirmed.get('revenue', 0) + paid_likely.get('revenue', 0)
        organic_orders = traffic_types.get('organic', {}).get('orders', 0)
        organic_revenue = traffic_types.get('organic', {}).get('revenue', 0)
        manager_data = traffic_types.get('manager', {'orders': 0, 'revenue': 0.0})

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
                'paid_confirmed': {'orders': paid_confirmed.get('orders', 0), 'revenue': round(paid_confirmed.get('revenue', 0), 2)},
                'paid_likely': {'orders': paid_likely.get('orders', 0), 'revenue': round(paid_likely.get('revenue', 0), 2)},
                'organic': {'orders': organic_orders, 'revenue': round(organic_revenue, 2)},
                'manager': {'orders': manager_data.get('orders', 0), 'revenue': round(manager_data.get('revenue', 0), 2)},
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
        platform: Optional[str] = None,
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
            filters.append("""
                COALESCE(u.traffic_type,
                    CASE WHEN s.source_id IN (1, 2) THEN 'organic' ELSE 'unknown' END
                ) = ?""")
            params.append(traffic_type)

        if platform:
            filters.append("""
                COALESCE(u.platform,
                    CASE s.source_id WHEN 1 THEN 'instagram' WHEN 2 THEN 'telegram' ELSE 'other' END
                ) = ?""")
            params.append(platform)

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
                COALESCE(u.traffic_type,
                    CASE WHEN s.source_id IN (1, 2) THEN 'organic' ELSE 'unknown' END
                ) AS traffic_type,
                COALESCE(u.platform,
                    CASE s.source_id WHEN 1 THEN 'instagram' WHEN 2 THEN 'telegram' ELSE 'other' END
                ) AS platform,
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

    # ─── ROAS Calculation ──────────────────────────────────────────────────────

    BONUS_TIERS = [
        (7.0, "+30%"),
        (6.0, "+20%"),
        (5.0, "+10%"),
        (4.0, "Base rate"),
        (0.0, "No bonus"),
    ]

    async def get_traffic_roas(
        self,
        start_date: date,
        end_date: date,
        sales_type: str = "all",
    ) -> Dict[str, Any]:
        """Calculate blended and per-platform ROAS.

        Combines:
        - Total revenue from gold_daily_revenue
        - Paid revenue per platform from gold_daily_traffic
        - Ad spend per platform from manual_expenses

        Returns dict with blended ROAS, per-platform breakdown, and bonus tier.
        """
        # 1. Total revenue (for blended ROAS)
        revenue_filters = ["date >= ?", "date <= ?"]
        revenue_params: list = [start_date, end_date]
        if sales_type != "all":
            revenue_filters.append("sales_type = ?")
            revenue_params.append(sales_type)
        revenue_where = " AND ".join(revenue_filters)

        total_rev_row = await self._fetch_one(
            f"SELECT COALESCE(SUM(revenue), 0) FROM gold_daily_revenue WHERE {revenue_where}",
            revenue_params,
        )
        total_revenue = float(total_rev_row[0]) if total_rev_row else 0.0

        # 2. Paid revenue per platform from gold_daily_traffic
        traffic_filters = [
            "g.date >= ?",
            "g.date <= ?",
            "g.traffic_type IN ('paid_confirmed', 'paid_likely')",
        ]
        traffic_params: list = [start_date, end_date]
        if sales_type != "all":
            traffic_filters.append("g.sales_type = ?")
            traffic_params.append(sales_type)
        traffic_where = " AND ".join(traffic_filters)

        paid_rows = await self._fetch_all(
            f"""SELECT g.platform, SUM(g.revenue) as paid_revenue
                FROM gold_daily_traffic g
                WHERE {traffic_where}
                GROUP BY g.platform""",
            traffic_params,
        )
        paid_by_platform = {row[0]: float(row[1]) for row in paid_rows}

        # 3. Ad spend per platform from manual_expenses
        spend_rows = await self._fetch_all(
            """SELECT platform, SUM(amount) as spend
               FROM manual_expenses
               WHERE expense_date BETWEEN ? AND ?
                 AND category = 'marketing'
                 AND platform IS NOT NULL
               GROUP BY platform""",
            [start_date, end_date],
        )
        spend_by_platform = {row[0]: float(row[1]) for row in spend_rows}
        total_spend = sum(spend_by_platform.values())

        # 4. Compute blended ROAS
        blended_roas = round(total_revenue / total_spend, 2) if total_spend > 0 else None

        # 5. Compute bonus tier
        bonus_tier = "No bonus"
        if blended_roas is not None:
            for threshold, tier in self.BONUS_TIERS:
                if blended_roas >= threshold:
                    bonus_tier = tier
                    break

        # 6. Per-platform ROAS
        all_platforms = set(list(spend_by_platform.keys()) + list(paid_by_platform.keys()))
        by_platform = {}
        for platform in sorted(all_platforms):
            spend = spend_by_platform.get(platform, 0)
            paid_rev = paid_by_platform.get(platform, 0)
            platform_roas = round(paid_rev / spend, 2) if spend > 0 else None
            by_platform[platform] = {
                "paid_revenue": round(paid_rev, 2),
                "spend": round(spend, 2),
                "roas": platform_roas,
            }

        return {
            "blended": {
                "revenue": round(total_revenue, 2),
                "spend": round(total_spend, 2),
                "roas": blended_roas,
            },
            "by_platform": by_platform,
            "bonus_tier": bonus_tier,
            "has_spend_data": total_spend > 0,
        }

    # ─── Sync Methods ─────────────────────────────────────────────────────────
