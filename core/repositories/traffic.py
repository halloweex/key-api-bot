"""DuckDBStore traffic analytics methods."""
from __future__ import annotations

import logging
from datetime import date
from typing import Optional, List, Dict, Any

from core.utm_classify import (
    classify_traffic,
    parse_utm_from_comment,
    tab_platform,
    utm_columns,
)

logger = logging.getLogger(__name__)


def _rounded(bucket: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    """One `{orders, revenue}` bucket, revenue to the kopeck, absent as zero."""
    bucket = bucket or {}
    return {
        'orders': int(bucket.get('orders', 0) or 0),
        'revenue': round(float(bucket.get('revenue', 0) or 0), 2),
    }


class TrafficMixin:

    async def _traffic_run(
        self, sql: str, params: "list | None" = None, *, mode: str = "all",
    ):
        """Run one traffic statement against whichever engine the flag names.

        The bodies below carry `{silver_orders}`, `{order_utm}`,
        `{gold_daily_revenue}`, `{gold_revenue_rollup}` and
        `{manual_expenses}` holes; `render_tables` fills them for the engine
        that is about to answer. One body, two engines — and the fill happens
        *inside* this method rather than at the call site, because a caller
        that picks the fragment from the flag renders the Postgres shape into
        the DuckDB fallback, which is how `/marketing` was broken for an hour.
        """
        from core.sql_dialect import DUCKDB, POSTGRES, render_tables

        from core import pg_traffic_read

        params = list(params or [])
        if pg_traffic_read.enabled() and pg_traffic_read.available():
            try:
                rows = await pg_traffic_read.fetch(
                    render_tables(sql, POSTGRES), params,
                )
                return (rows[0] if rows else None) if mode == "one" else rows
            except Exception as exc:  # noqa: BLE001
                logger.error(
                    "traffic: Postgres failed, falling back to DuckDB: %s",
                    exc, exc_info=True,
                )

        # `_fetch_one`/`_fetch_all`, not a bare `conn.execute` under
        # `self.connection()`. They take the store lock themselves — so this
        # must not be wrapped in one, the lock is not reentrant and a nested
        # acquisition hangs rather than raises — and they add the two things
        # this tab is the only repository to have ever had: the query runs in
        # a thread so the event loop keeps answering, and it is bounded by
        # DEFAULT_QUERY_TIMEOUT, on whose expiry they call `conn.interrupt()`
        # and wait for the thread to come off the connection before raising.
        #
        # The first cut of this router called `conn.execute` directly, which
        # is what the other four ported tabs do — but they never had the
        # offloading to lose, and this one did. Without it the fallback path
        # blocks the whole application for the length of a DuckDB query and
        # can hold the global store lock without bound, which is worst
        # exactly when it is reached: Postgres already being down.
        duck = render_tables(sql, DUCKDB)
        if mode == "one":
            return await self._fetch_one(duck, params)
        return await self._fetch_all(duck, params)

    # The two expressions that decide what a chart calls an order, repeated
    # verbatim in five places below because a GROUP BY cannot name the alias
    # (see `refresh_traffic_gold_layer` for what happens when it tries). They
    # are the fallback for an order the parser never classified: no UTM row at
    # all, which the LEFT JOIN renders as NULL.
    _PLATFORM_EXPR = """COALESCE(u.platform,
        CASE s.source_id WHEN 1 THEN 'instagram' WHEN 2 THEN 'telegram' ELSE 'unattributed' END)"""
    _TRAFFIC_TYPE_EXPR = """COALESCE(u.traffic_type,
        CASE WHEN s.source_id IN (1, 2) THEN 'organic' ELSE 'unknown' END)"""

    # The parser and the classifier live in `core.utm_classify` since DN-15:
    # they are pure, and the reclassify dry run and the Postgres parse need them
    # without a store in hand. Bound here as the same functions, not wrappers,
    # so `TrafficMixin._classify_traffic` still answers for existing callers and
    # a test that patches the method on `DuckDBStore` still reaches the parse
    # below, which looks them up through `self`.
    _parse_utm_from_comment = staticmethod(parse_utm_from_comment)
    _classify_traffic = staticmethod(classify_traffic)

    _UTM_BATCH_SIZE = 1000

    # How the last parse in this process ended: None when it finished, or when
    # none has run yet, and the error text when it raised. Read by
    # `core.pg_order_utm.ship_order_utm`, which will not copy a table that a
    # parse may have left half-written.
    #
    # A class default and not an `__init__` assignment, because this is a mixin
    # and `DuckDBStore.__init__` does not know about it; the parser sets it on
    # the instance, and the store is a process singleton, so the instance is
    # the process.
    last_utm_parse_error: Optional[str] = None

    async def refresh_utm_silver_layer(self) -> set[int]:
        """Parse UTM data from orders and populate silver_order_utm table.

        Processes in batches of _UTM_BATCH_SIZE, releasing the DB lock
        between batches so health checks and other queries aren't blocked.

        Returns:
            The ids of the orders parsed. Callers take ``len()`` for the
            count.

            They were once needed whole: `refresh_warehouse_layers` used them
            to work out which dates the traffic Gold had to be rebuilt for,
            and returning only a count is what forced a full rebuild of that
            table ~240 times a day. That layer is retired, so the ids are now
            just the honest return value of a parser.

        **Records its own outcome in `last_utm_parse_error`, here rather than
        in `refresh_warehouse_layers`.** The warehouse tick is one of five
        callers — the traffic refresh, the reclassify, the `manager_comment`
        backfill endpoint and `scripts/backfill_utm.py` are the others — and
        the tick swallows the error while the rest do not. A flag written only
        by the tick would miss a reclassify whose DELETE committed and whose
        re-parse raised, which is the most partial this table ever gets. It
        would also refuse wrongly the other way: a reclassify that finished
        after a failed tick has put every row back, and a flag only the tick
        can clear would keep refusing its ship until the next dirty tick.

        A finished parse is a complete table, which is what makes clearing
        the flag on success sound: the predicate below selects every order
        with a comment and no current row, so when it returns there is none
        left. The one thing it cannot see is a DELETE running beside it on
        another path, and that is what the shipper's row-count guard is for.
        """
        try:
            parsed = await self._parse_utm_into_silver()
        except BaseException as exc:
            # `BaseException`, so a cancelled parse counts. A shutdown or a
            # `wait_for` that cancels between two write batches leaves exactly
            # the partial table an exception does, and `CancelledError` is not
            # an `Exception`. Re-raised untouched either way.
            self.last_utm_parse_error = f"{type(exc).__name__}: {exc}"
            raise
        self.last_utm_parse_error = None
        return parsed

    async def _parse_utm_into_silver(self) -> set[int]:
        """The parse itself; `refresh_utm_silver_layer` is the entry point."""
        # Step 1: fetch IDs + comments that need parsing (short lock)
        async with self.connection() as conn:
            orders = conn.execute("""
                SELECT o.id, o.manager_comment, o.updated_at
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
            return set()

        # Step 2: parse UTM in Python — no lock held
        # `parsed_at` is the order's `updated_at` as it was when the comment
        # was read — not the wall clock at write time. The lock is released
        # between the read above and the write below, so a comment rewritten
        # by the sync in between used to be stamped with a *later* parse time
        # than its own `updated_at`, and the predicate above never picked it
        # up again. Stamped with the value it was parsed from, a later change
        # still compares greater.
        #
        # The row itself is `utm_columns`, the one place its shape is written —
        # including the NULLs for a comment with no tracking data, which let
        # the readers' COALESCE fall through to the source-based defaults. The
        # reclassify dry run builds the same row to compare against, and a row
        # built twice is two shapes the first time either is edited.
        utm_rows = []
        for order_id, comment, updated_at in orders:
            utm_rows.append((
                order_id,
                *utm_columns(
                    comment,
                    parse=self._parse_utm_from_comment,
                    classify=self._classify_traffic,
                ),
                updated_at,
            ))

        # Step 3: write in batches, releasing lock between each
        total = 0
        for i in range(0, len(utm_rows), self._UTM_BATCH_SIZE):
            batch = utm_rows[i : i + self._UTM_BATCH_SIZE]
            async with self.connection() as conn:
                conn.executemany("""
                    INSERT OR REPLACE INTO silver_order_utm
                        (order_id, utm_source, utm_medium, utm_campaign, utm_content,
                         utm_term, utm_lang, fbp, fbc, ttp, fbclid,
                         traffic_type, platform, parsed_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, COALESCE(?, CURRENT_TIMESTAMP))
                """, batch)
            total += len(batch)

        num_batches = (len(utm_rows) + self._UTM_BATCH_SIZE - 1) // self._UTM_BATCH_SIZE
        logger.info(f"Parsed UTM data for {total} orders ({num_batches} batches)")
        return {order_id for order_id, _, _ in orders}


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
        # `refresh_traffic_gold_layer`'s own predicate, because this reads the
        # rows that Gold aggregates rather than the Gold. Dropping one of the
        # three would not shift a cell, it would add rows the Gold never had.
        filters = [
            "NOT s.is_return",
            "s.is_active_source",
            "s.order_date IS NOT NULL",
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

        where_clause = " AND ".join(filters)

        query = f"""
            SELECT
                {self._PLATFORM_EXPR} AS platform,
                {self._TRAFFIC_TYPE_EXPR} AS traffic_type,
                COUNT(DISTINCT s.id) AS orders,
                COALESCE(SUM(s.grand_total), 0) AS revenue
            FROM {{silver_orders}} s
            LEFT JOIN {{order_utm}} u ON s.id = u.order_id
            WHERE {where_clause}
            GROUP BY {self._PLATFORM_EXPR}, {self._TRAFFIC_TYPE_EXPR}
            ORDER BY revenue DESC, platform, traffic_type
        """

        rows = await self._traffic_run(query, params)

        # Aggregate by platform
        platforms = {}
        traffic_types = {}

        for row in rows:
            platform, traffic_type, orders, revenue = row

            # Google split into ads and organic; `tab_platform` says why,
            # and is shared with the reclassify dry run.
            platform = tab_platform(platform, traffic_type)

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
                # Rounded like the five above them. These two used to be
                # handed straight out of the accumulator, so the payload
                # carried 1182863.0299999998 where every sibling carried two
                # decimals.
                'pixel_only': _rounded(traffic_types.get('pixel_only')),
                'unknown': _rounded(traffic_types.get('unknown')),
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
            "NOT s.is_return",
            "s.is_active_source",
            "s.order_date IS NOT NULL",
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

        where_clause = " AND ".join(filters)

        query = f"""
            SELECT
                s.order_date AS date,
                {self._TRAFFIC_TYPE_EXPR} AS traffic_type,
                COUNT(DISTINCT s.id) AS orders,
                COALESCE(SUM(s.grand_total), 0) AS revenue
            FROM {{silver_orders}} s
            LEFT JOIN {{order_utm}} u ON s.id = u.order_id
            WHERE {where_clause}
            GROUP BY s.order_date, {self._TRAFFIC_TYPE_EXPR}
            ORDER BY s.order_date, traffic_type
        """

        rows = await self._traffic_run(query, params)

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
                    CASE s.source_id WHEN 1 THEN 'instagram' WHEN 2 THEN 'telegram' ELSE 'unattributed' END
                ) = ?""")
            params.append(platform)

        where_clause = " AND ".join(filters)

        # Get total count
        count_query = f"""
            SELECT COUNT(*)
            FROM {{silver_orders}} s
            LEFT JOIN {{order_utm}} u ON s.id = u.order_id
            WHERE {where_clause}
        """
        count_row = await self._traffic_run(count_query, params, mode="one")
        total = count_row[0] if count_row else 0

        # Get paginated rows
        data_query = f"""
            SELECT
                s.id, s.order_date, s.grand_total, s.source_name,
                COALESCE(u.traffic_type,
                    CASE WHEN s.source_id IN (1, 2) THEN 'organic' ELSE 'unknown' END
                ) AS traffic_type,
                COALESCE(u.platform,
                    CASE s.source_id WHEN 1 THEN 'instagram' WHEN 2 THEN 'telegram' ELSE 'unattributed' END
                ) AS platform,
                u.utm_source, u.utm_medium, u.utm_campaign, u.utm_content,
                u.fbp, u.fbc, u.ttp, u.fbclid
            FROM {{silver_orders}} s
            LEFT JOIN {{order_utm}} u ON s.id = u.order_id
            WHERE {where_clause}
            ORDER BY s.order_date DESC, s.id DESC
            LIMIT ? OFFSET ?
        """
        data_params = params + [limit, offset]
        rows = await self._traffic_run(data_query, data_params)

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

    # Whitelist of sortable columns → SELECT aliases in the campaigns query
    _UTM_CAMPAIGN_SORT_COLUMNS = {
        'campaign', 'utm_source', 'platform', 'traffic_type', 'orders', 'revenue',
    }

    async def get_traffic_utm_campaigns(
        self,
        start_date: date,
        end_date: date,
        sales_type: str = "all",
        source_id: Optional[int] = None,
        traffic_type: Optional[str] = None,
        platform: Optional[str] = None,
        sort_by: str = "revenue",
        sort_dir: str = "desc",
        limit: int = 50,
        offset: int = 0,
    ) -> Dict[str, Any]:
        """Aggregate orders and revenue per UTM campaign.

        Groups by (utm_campaign, platform, traffic_type) so the same
        campaign name split across platforms (or paid vs organic, e.g.
        Google Ads numeric IDs vs product_sync) stays in separate rows.
        Orders without a UTM campaign fall into a single '' campaign row
        per platform/traffic_type (rendered as "no UTM" by the frontend).
        """
        if sort_by not in self._UTM_CAMPAIGN_SORT_COLUMNS:
            sort_by = 'revenue'
        sort_dir = 'ASC' if sort_dir.lower() == 'asc' else 'DESC'

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
                    CASE s.source_id WHEN 1 THEN 'instagram' WHEN 2 THEN 'telegram' ELSE 'unattributed' END
                ) = ?""")
            params.append(platform)

        where_clause = " AND ".join(filters)

        # GROUP BY must repeat the COALESCE expressions (see refresh_traffic_gold_layer)
        campaign_expr = "COALESCE(u.utm_campaign, '')"
        platform_expr = """COALESCE(u.platform,
            CASE s.source_id WHEN 1 THEN 'instagram' WHEN 2 THEN 'telegram' ELSE 'unattributed' END)"""
        traffic_type_expr = """COALESCE(u.traffic_type,
            CASE WHEN s.source_id IN (1, 2) THEN 'organic' ELSE 'unknown' END)"""

        # `AS groups` is not decoration: PostgreSQL requires a derived table to
        # be named, and DuckDB accepts the alias, so naming it is what makes
        # one body run on both.
        count_query = f"""
            SELECT COUNT(*) FROM (
                SELECT 1
                FROM {{silver_orders}} s
                LEFT JOIN {{order_utm}} u ON s.id = u.order_id
                WHERE {where_clause}
                GROUP BY {campaign_expr}, {platform_expr}, {traffic_type_expr}
            ) AS groups
        """
        count_row = await self._traffic_run(count_query, params, mode="one")
        total = count_row[0] if count_row else 0

        # `MIN`, not `any_value`: the campaign is grouped on but `utm_source`
        # is not, and "whichever row the engine reached first" is a different
        # row in each of them. The grouping keys settle the ORDER BY for the
        # same reason — this query is paginated, so two rows tied on the sort
        # column can otherwise appear on both page 1 and page 2, or on
        # neither.
        data_query = f"""
            SELECT
                {campaign_expr} AS campaign,
                MIN(u.utm_source) AS utm_source,
                {platform_expr} AS platform,
                {traffic_type_expr} AS traffic_type,
                COUNT(DISTINCT s.id) AS orders,
                COALESCE(SUM(s.grand_total), 0) AS revenue
            FROM {{silver_orders}} s
            LEFT JOIN {{order_utm}} u ON s.id = u.order_id
            WHERE {where_clause}
            GROUP BY {campaign_expr}, {platform_expr}, {traffic_type_expr}
            ORDER BY {sort_by} {sort_dir}, revenue DESC, campaign, platform, traffic_type
            LIMIT ? OFFSET ?
        """
        rows = await self._traffic_run(data_query, params + [limit, offset])

        campaigns = [
            {
                'campaign': campaign,
                'utm_source': utm_source,
                'platform': platform,
                'traffic_type': traffic_type,
                'orders': orders,
                'revenue': round(float(revenue), 2),
            }
            for campaign, utm_source, platform, traffic_type, orders, revenue in rows
        ]

        return {
            'campaigns': campaigns,
            'total': total,
            'limit': limit,
            'offset': offset,
        }

    # ─── ROAS Calculation ──────────────────────────────────────────────────────

    # Keys, not display strings. These used to be "+30%" … "No bonus", which
    # the frontend printed verbatim — so a Ukrainian dashboard said "Base
    # rate" — and then string-compared against its own *translated* label to
    # decide which row of the tier table to highlight, which therefore only
    # ever matched in English. A key is the thing both halves can agree on.
    BONUS_TIERS = [
        (7.0, "plus_30"),
        (6.0, "plus_20"),
        (5.0, "plus_10"),
        (4.0, "base"),
        (0.0, "none"),
    ]

    async def get_traffic_roas(
        self,
        start_date: date,
        end_date: date,
        sales_type: str = "all",
    ) -> Dict[str, Any]:
        """Calculate blended and per-platform ROAS.

        Combines:
        - Total revenue from the revenue Gold, roll-up rows only
        - Paid revenue per platform from `silver_orders` joined to the UTM
          level — not from the traffic Gold, which exists in one engine
        - Ad spend per platform from the manual expenses

        Returns dict with blended ROAS, per-platform breakdown, and bonus tier.
        """
        # 1. Total revenue (for blended ROAS)
        revenue_filters = ["date >= ?", "date <= ?"]
        revenue_params: list = [start_date, end_date]
        if sales_type != "all":
            revenue_filters.append("sales_type = ?")
            revenue_params.append(sales_type)
        revenue_where = " AND ".join(revenue_filters)

        # `{gold_revenue_rollup}` is `TRUE` in DuckDB and `source_id IS NULL`
        # in Postgres, whose Gold carries the source as a dimension with the
        # roll-up as a row. Without it this sum counts every order twice there
        # — 11,107,040.50 against a true 5,553,520.25, measured over 30 days —
        # and the blended ROAS reads exactly double.
        total_rev_row = await self._traffic_run(
            "SELECT COALESCE(SUM(revenue), 0) FROM {gold_daily_revenue} "
            f"WHERE {revenue_where} AND {{gold_revenue_rollup}}",
            revenue_params,
            mode="one",
        )
        total_revenue = float(total_rev_row[0]) if total_rev_row else 0.0

        # 2. Paid revenue per platform, from the same join the rest of the tab
        #    reads. `traffic_type` is a fallback expression rather than a
        #    column, so the paid filter has to repeat it — an order with no UTM
        #    row is `organic` or `unknown` and can never be paid, which is the
        #    answer the Gold gave too.
        traffic_filters = [
            "NOT s.is_return",
            "s.is_active_source",
            "s.order_date IS NOT NULL",
            "s.order_date >= ?",
            "s.order_date <= ?",
            f"{self._TRAFFIC_TYPE_EXPR} IN ('paid_confirmed', 'paid_likely')",
        ]
        traffic_params: list = [start_date, end_date]
        if sales_type != "all":
            traffic_filters.append("s.sales_type = ?")
            traffic_params.append(sales_type)
        traffic_where = " AND ".join(traffic_filters)

        paid_rows = await self._traffic_run(
            f"""SELECT {self._PLATFORM_EXPR} AS platform,
                       COALESCE(SUM(s.grand_total), 0) as paid_revenue
                FROM {{silver_orders}} s
                LEFT JOIN {{order_utm}} u ON s.id = u.order_id
                WHERE {traffic_where}
                GROUP BY {self._PLATFORM_EXPR}
                ORDER BY platform""",
            traffic_params,
        )
        paid_by_platform = {row[0]: float(row[1]) for row in paid_rows}

        # 3. Ad spend per platform from manual_expenses
        spend_rows = await self._traffic_run(
            """SELECT platform, SUM(amount) as spend
               FROM {manual_expenses}
               WHERE expense_date BETWEEN ? AND ?
                 AND category = 'marketing'
                 AND platform IS NOT NULL
               GROUP BY platform
               ORDER BY platform""",
            [start_date, end_date],
        )
        spend_by_platform = {row[0]: float(row[1]) for row in spend_rows}
        total_spend = sum(spend_by_platform.values())

        # 4. Compute blended ROAS
        blended_roas = round(total_revenue / total_spend, 2) if total_spend > 0 else None

        # 5. Compute bonus tier — `None` when there is no ROAS to place.
        #
        # It defaulted to the bottom tier, so a period with no ad spend
        # entered (which is every period: `manual_expenses` holds zero rows
        # in production) put "No bonus" on the card as though it had been
        # computed, and lit the "< 4.0x" row of the tier table as the one in
        # force. That is a verdict rendered out of missing data. Nothing
        # divided means nothing to report.
        bonus_tier = None
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
