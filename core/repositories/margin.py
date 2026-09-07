"""DuckDBStore margin analysis methods."""
from __future__ import annotations

import logging
from datetime import date
from typing import Dict, List, Any

from core.duckdb_constants import line_window_where

logger = logging.getLogger(__name__)


class MarginMixin:
    """Margin analysis queries using silver_orders + offer_stocks."""

    # ─── Which engine answers `/margin` ──────────────────────────────────
    #
    # Six methods, three tables, and all three are already in Postgres:
    # `silver.order_lines` (a view whose body is the same text DuckDB uses),
    # `bronze.offer_stocks` and `bronze.categories`. No migration, no
    # replication, no new hole in `Dialect`.
    #
    # The choice is made BEFORE any connection is taken — `/inventory`'s §34
    # invariant: a read bound for Postgres must not first queue behind
    # DuckDB's single writer, or the flag has moved the bottleneck rather than
    # left it behind.
    #
    # **Never call this from inside `self.connection()`.** The store lock is
    # not reentrant and the deadlock does not raise — it hangs.

    async def _margin_run(
        self, sql: str, params: "list | None" = None, *, mode: str = "all",
    ):
        """Run one margin statement against whichever engine the flag names."""
        from core.sql_dialect import DUCKDB, POSTGRES, render_tables

        from core import pg_margin_read

        params = list(params or [])
        if pg_margin_read.enabled() and pg_margin_read.available():
            try:
                rows = await pg_margin_read.fetch(
                    render_tables(sql, POSTGRES), params,
                )
                return (rows[0] if rows else None) if mode == "one" else rows
            except Exception as exc:  # noqa: BLE001
                logger.error(
                    "margin: Postgres failed, falling back to DuckDB: %s",
                    exc, exc_info=True,
                )

        async with self.connection() as conn:
            cursor = conn.execute(render_tables(sql, DUCKDB), params)
            return cursor.fetchone() if mode == "one" else cursor.fetchall()

    def _margin_base_where(self, sales_type: str, params: list) -> str:
        """The line predicate, which is not margin's to define — see
        `core.duckdb_store.line_window_where`."""
        return line_window_where(sales_type, params)

    async def get_margin_overview(
        self,
        start_date: date,
        end_date: date,
        sales_type: str = "retail",
    ) -> Dict[str, Any]:
        """Get overall margin KPIs."""
        params: list = [start_date, end_date]
        where = self._margin_base_where(sales_type, params)

        row = await self._margin_run(f"""
            SELECT
                COALESCE(SUM(l.line_amount), 0) as total_revenue,
                COALESCE(SUM(CASE WHEN os.purchased_price IS NOT NULL AND os.purchased_price > 0
                    THEN l.line_amount ELSE 0 END), 0) as costed_revenue,
                COALESCE(SUM(CASE WHEN os.purchased_price IS NOT NULL AND os.purchased_price > 0
                    THEN os.purchased_price * l.quantity ELSE 0 END), 0) as cogs,
                COUNT(DISTINCT CASE WHEN os.purchased_price IS NOT NULL AND os.purchased_price > 0
                    THEN l.sku END) as skus_with_cost,
                COUNT(DISTINCT l.sku) as total_skus,
                COALESCE(SUM(l.quantity), 0) as total_units
            FROM {{order_lines}} l
            LEFT JOIN {{offer_stocks}} os ON l.sku = os.sku
            WHERE {where}
        """, params, mode="one")

        total_revenue = float(row[0])
        costed_revenue = float(row[1])
        cogs = float(row[2])
        profit = costed_revenue - cogs
        margin_pct = round(100.0 * profit / costed_revenue, 1) if costed_revenue > 0 else 0
        coverage_pct = round(100.0 * costed_revenue / total_revenue, 1) if total_revenue > 0 else 0

        return {
            "total_revenue": round(total_revenue, 2),
            "costed_revenue": round(costed_revenue, 2),
            "cogs": round(cogs, 2),
            "profit": round(profit, 2),
            "margin_pct": margin_pct,
            "coverage_pct": coverage_pct,
            "skus_with_cost": row[3],
            "total_skus": row[4],
            "total_units": row[5],
        }

    async def get_margin_by_brand(
        self,
        start_date: date,
        end_date: date,
        sales_type: str = "retail",
        limit: int = 20,
    ) -> List[Dict[str, Any]]:
        """Get margin breakdown by brand, sorted by revenue."""
        params: list = [start_date, end_date]
        where = self._margin_base_where(sales_type, params)

        rows = await self._margin_run(f"""
            SELECT
                COALESCE(NULLIF(TRIM(l.brand), ''), 'Unknown') as brand,
                SUM(l.quantity) as total_units,
                ROUND(SUM(l.line_amount), 2) as total_revenue,
                SUM(CASE WHEN os.purchased_price IS NOT NULL AND os.purchased_price > 0
                    THEN l.quantity ELSE 0 END) as costed_units,
                ROUND(SUM(CASE WHEN os.purchased_price IS NOT NULL AND os.purchased_price > 0
                    THEN l.line_amount ELSE 0 END), 2) as costed_revenue,
                ROUND(SUM(CASE WHEN os.purchased_price IS NOT NULL AND os.purchased_price > 0
                    THEN os.purchased_price * l.quantity ELSE 0 END), 2) as cogs
            FROM {{order_lines}} l
            LEFT JOIN {{offer_stocks}} os ON l.sku = os.sku
            WHERE {where}
              AND TRIM(l.brand) != '' AND l.brand IS NOT NULL
            GROUP BY 1
            -- `1` is the grouping key, unique per output row. Without it
            -- two engines break a tie on revenue differently — and two of
            -- these carry a LIMIT, where that means different *rows*, not
            -- a different order. Found by the gate: two categories on
            -- ₴1450 each came back in opposite orders.
            ORDER BY total_revenue DESC, 1
            LIMIT ?
        """, params + [limit])

        result = []
        for r in rows:
            costed_rev = float(r[4])
            cogs = float(r[5])
            profit = costed_rev - cogs
            total_rev = float(r[2])
            result.append({
                "brand": r[0],
                "total_units": r[1],
                "total_revenue": total_rev,
                "costed_units": r[3],
                "costed_revenue": costed_rev,
                "cogs": cogs,
                "profit": round(profit, 2),
                "margin_pct": round(100.0 * profit / costed_rev, 1) if costed_rev > 0 else None,
                "coverage_pct": round(100.0 * costed_rev / total_rev, 1) if total_rev > 0 else 0,
            })
        return result

    async def get_margin_by_category(
        self,
        start_date: date,
        end_date: date,
        sales_type: str = "retail",
    ) -> List[Dict[str, Any]]:
        """Get margin breakdown by root category."""
        params: list = [start_date, end_date]
        where = self._margin_base_where(sales_type, params)

        rows = await self._margin_run(f"""
            WITH root_cat AS (
                SELECT c1.id,
                       COALESCE(c2.name, c1.name) as root_name
                FROM {{categories}} c1
                LEFT JOIN {{categories}} c2 ON c1.parent_id = c2.id
            )
            SELECT
                COALESCE(rc.root_name, 'Uncategorized') as category,
                SUM(l.quantity) as total_units,
                ROUND(SUM(l.line_amount), 2) as total_revenue,
                ROUND(SUM(CASE WHEN os.purchased_price IS NOT NULL AND os.purchased_price > 0
                    THEN l.line_amount ELSE 0 END), 2) as costed_revenue,
                ROUND(SUM(CASE WHEN os.purchased_price IS NOT NULL AND os.purchased_price > 0
                    THEN os.purchased_price * l.quantity ELSE 0 END), 2) as cogs,
                ROUND(100.0 * SUM(l.line_amount)
                    / SUM(SUM(l.line_amount)) OVER (), 1) as rev_share_pct
            FROM {{order_lines}} l
            LEFT JOIN {{offer_stocks}} os ON l.sku = os.sku
            LEFT JOIN root_cat rc ON l.category_id = rc.id
            WHERE {where}
            GROUP BY 1
            -- `1` is the grouping key, unique per output row. Without it
            -- two engines break a tie on revenue differently — and two of
            -- these carry a LIMIT, where that means different *rows*, not
            -- a different order. Found by the gate: two categories on
            -- ₴1450 each came back in opposite orders.
            ORDER BY total_revenue DESC, 1
        """, params)

        result = []
        for r in rows:
            costed_rev = float(r[3])
            cogs = float(r[4])
            profit = costed_rev - cogs
            total_rev = float(r[2])
            result.append({
                "category": r[0],
                "total_units": r[1],
                "total_revenue": total_rev,
                "costed_revenue": costed_rev,
                "cogs": cogs,
                "profit": round(profit, 2),
                "margin_pct": round(100.0 * profit / costed_rev, 1) if costed_rev > 0 else None,
                "coverage_pct": round(100.0 * costed_rev / total_rev, 1) if total_rev > 0 else 0,
                "rev_share_pct": float(r[5]) if r[5] else 0,
            })
        return result

    async def get_margin_trend(
        self,
        start_date: date,
        end_date: date,
        sales_type: str = "retail",
    ) -> List[Dict[str, Any]]:
        """Get monthly margin trend."""
        params: list = [start_date, end_date]
        where = self._margin_base_where(sales_type, params)

        rows = await self._margin_run(f"""
            SELECT
                -- `strftime` is DuckDB's alone: PostgreSQL rejects it, and
                -- because this tab falls back, that made the method answer
                -- from DuckDB forever while looking ported. Both engines
                -- render a DATE as 'YYYY-MM-DD' under ISO DateStyle (the
                -- server's, checked), so seven characters is the month.
                substr(CAST(l.order_date AS VARCHAR), 1, 7) as month,
                ROUND(SUM(CASE WHEN os.purchased_price IS NOT NULL AND os.purchased_price > 0
                    THEN l.line_amount ELSE 0 END), 2) as costed_revenue,
                ROUND(SUM(CASE WHEN os.purchased_price IS NOT NULL AND os.purchased_price > 0
                    THEN os.purchased_price * l.quantity ELSE 0 END), 2) as cogs,
                ROUND(SUM(l.line_amount), 2) as total_revenue
            FROM {{order_lines}} l
            LEFT JOIN {{offer_stocks}} os ON l.sku = os.sku
            WHERE {where}
            GROUP BY 1
            ORDER BY 1
        """, params)

        result = []
        for r in rows:
            costed_rev = float(r[1])
            cogs = float(r[2])
            profit = costed_rev - cogs
            total_rev = float(r[3])
            result.append({
                "month": r[0],
                "revenue": costed_rev,
                "cogs": cogs,
                "profit": round(profit, 2),
                "margin_pct": round(100.0 * profit / costed_rev, 1) if costed_rev > 0 else 0,
                "total_revenue": total_rev,
                "coverage_pct": round(100.0 * costed_rev / total_rev, 1) if total_rev > 0 else 0,
            })
        return result

    async def get_margin_brand_category(
        self,
        start_date: date,
        end_date: date,
        sales_type: str = "retail",
        min_revenue: float = 500,
    ) -> List[Dict[str, Any]]:
        """Get brand × category cross-tab with margin data."""
        params: list = [start_date, end_date]
        where = self._margin_base_where(sales_type, params)

        rows = await self._margin_run(f"""
            WITH root_cat AS (
                SELECT c1.id,
                       COALESCE(c2.name, c1.name) as root_name
                FROM {{categories}} c1
                LEFT JOIN {{categories}} c2 ON c1.parent_id = c2.id
            )
            SELECT
                COALESCE(NULLIF(TRIM(l.brand), ''), 'Unknown') as brand,
                COALESCE(rc.root_name, 'Uncategorized') as category,
                SUM(l.quantity) as total_units,
                ROUND(SUM(l.line_amount), 2) as total_revenue,
                ROUND(SUM(CASE WHEN os.purchased_price IS NOT NULL AND os.purchased_price > 0
                    THEN l.line_amount ELSE 0 END), 2) as costed_revenue,
                ROUND(SUM(CASE WHEN os.purchased_price IS NOT NULL AND os.purchased_price > 0
                    THEN os.purchased_price * l.quantity ELSE 0 END), 2) as cogs
            FROM {{order_lines}} l
            LEFT JOIN {{offer_stocks}} os ON l.sku = os.sku
            LEFT JOIN root_cat rc ON l.category_id = rc.id
            WHERE {where}
            GROUP BY 1, 2
            HAVING SUM(l.line_amount) > ?
            -- `1` is the grouping key, unique per output row. Without it
            -- two engines break a tie on revenue differently — and two of
            -- these carry a LIMIT, where that means different *rows*, not
            -- a different order. Found by the gate: two categories on
            -- ₴1450 each came back in opposite orders.
            ORDER BY brand, total_revenue DESC, 2
        """, params + [min_revenue])

        result = []
        for r in rows:
            costed_rev = float(r[4])
            cogs = float(r[5])
            profit = costed_rev - cogs
            total_rev = float(r[3])
            result.append({
                "brand": r[0],
                "category": r[1],
                "total_units": r[2],
                "total_revenue": total_rev,
                "costed_revenue": costed_rev,
                "cogs": cogs,
                "profit": round(profit, 2),
                "margin_pct": round(100.0 * profit / costed_rev, 1) if costed_rev > 0 else None,
                "coverage_pct": round(100.0 * costed_rev / total_rev, 1) if total_rev > 0 else 0,
            })
        return result

    async def get_margin_alerts(
        self,
        start_date: date,
        end_date: date,
        sales_type: str = "retail",
        margin_floor: float = 30.0,
        min_revenue: float = 50000,
    ) -> List[Dict[str, Any]]:
        """Get brands with margin below floor, sorted by revenue impact."""
        params: list = [start_date, end_date]
        where = self._margin_base_where(sales_type, params)

        rows = await self._margin_run(f"""
            SELECT
                COALESCE(NULLIF(TRIM(l.brand), ''), 'Unknown') as brand,
                SUM(l.quantity) as total_units,
                ROUND(SUM(l.line_amount), 2) as total_revenue,
                ROUND(SUM(CASE WHEN os.purchased_price IS NOT NULL AND os.purchased_price > 0
                    THEN l.line_amount ELSE 0 END), 2) as costed_revenue,
                ROUND(SUM(CASE WHEN os.purchased_price IS NOT NULL AND os.purchased_price > 0
                    THEN os.purchased_price * l.quantity ELSE 0 END), 2) as cogs
            FROM {{order_lines}} l
            LEFT JOIN {{offer_stocks}} os ON l.sku = os.sku
            WHERE {where}
            GROUP BY 1
            HAVING SUM(CASE WHEN os.purchased_price IS NOT NULL AND os.purchased_price > 0
                THEN l.line_amount ELSE 0 END) > ?
            -- `1` is the grouping key, unique per output row. Without it
            -- two engines break a tie on revenue differently — and two of
            -- these carry a LIMIT, where that means different *rows*, not
            -- a different order. Found by the gate: two categories on
            -- ₴1450 each came back in opposite orders.
            ORDER BY total_revenue DESC, 1
        """, params + [min_revenue])

        alerts = []
        for r in rows:
            costed_rev = float(r[3])
            cogs = float(r[4])
            profit = costed_rev - cogs
            margin = 100.0 * profit / costed_rev if costed_rev > 0 else 0
            if margin < margin_floor:
                # Impact = how much more profit we'd have at floor margin
                target_profit = costed_rev * margin_floor / 100.0
                impact = target_profit - profit
                alerts.append({
                    "brand": r[0],
                    "total_units": r[1],
                    "total_revenue": float(r[2]),
                    "costed_revenue": costed_rev,
                    "cogs": cogs,
                    "profit": round(profit, 2),
                    "margin_pct": round(margin, 1),
                    "margin_floor": margin_floor,
                    "impact": round(impact, 2),
                })
        alerts.sort(key=lambda x: x["impact"], reverse=True)
        return alerts
