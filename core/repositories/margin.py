"""DuckDBStore margin analysis methods."""
from __future__ import annotations

import logging
from datetime import date
from typing import Dict, List, Any, Optional

logger = logging.getLogger(__name__)


class MarginMixin:
    """Margin analysis queries using silver_orders + offer_stocks."""

    def _margin_base_where(self, sales_type: str, params: list) -> str:
        """Build common WHERE clause for margin queries."""
        clauses = [
            "s.order_date BETWEEN ? AND ?",
            "NOT s.is_return",
            "s.is_active_source",
        ]
        if sales_type != "all":
            clauses.append("s.sales_type = ?")
            params.append(sales_type)
        return " AND ".join(clauses)

    async def get_margin_overview(
        self,
        start_date: date,
        end_date: date,
        sales_type: str = "retail",
    ) -> Dict[str, Any]:
        """Get overall margin KPIs."""
        async with self.connection() as conn:
            params: list = [start_date, end_date]
            where = self._margin_base_where(sales_type, params)

            row = conn.execute(f"""
                SELECT
                    COALESCE(SUM(op.price_sold * op.quantity), 0) as total_revenue,
                    COALESCE(SUM(CASE WHEN os.purchased_price IS NOT NULL AND os.purchased_price > 0
                        THEN op.price_sold * op.quantity ELSE 0 END), 0) as costed_revenue,
                    COALESCE(SUM(CASE WHEN os.purchased_price IS NOT NULL AND os.purchased_price > 0
                        THEN os.purchased_price * op.quantity ELSE 0 END), 0) as cogs,
                    COUNT(DISTINCT CASE WHEN os.purchased_price IS NOT NULL AND os.purchased_price > 0
                        THEN p.sku END) as skus_with_cost,
                    COUNT(DISTINCT p.sku) as total_skus,
                    COALESCE(SUM(op.quantity), 0) as total_units
                FROM silver_orders s
                JOIN order_products op ON s.id = op.order_id
                LEFT JOIN products p ON op.product_id = p.id
                LEFT JOIN offer_stocks os ON p.sku = os.sku
                WHERE {where}
            """, params).fetchone()

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
        async with self.connection() as conn:
            params: list = [start_date, end_date]
            where = self._margin_base_where(sales_type, params)

            rows = conn.execute(f"""
                SELECT
                    COALESCE(NULLIF(TRIM(p.brand), ''), 'Unknown') as brand,
                    SUM(op.quantity) as total_units,
                    ROUND(SUM(op.price_sold * op.quantity), 2) as total_revenue,
                    SUM(CASE WHEN os.purchased_price IS NOT NULL AND os.purchased_price > 0
                        THEN op.quantity ELSE 0 END) as costed_units,
                    ROUND(SUM(CASE WHEN os.purchased_price IS NOT NULL AND os.purchased_price > 0
                        THEN op.price_sold * op.quantity ELSE 0 END), 2) as costed_revenue,
                    ROUND(SUM(CASE WHEN os.purchased_price IS NOT NULL AND os.purchased_price > 0
                        THEN os.purchased_price * op.quantity ELSE 0 END), 2) as cogs
                FROM silver_orders s
                JOIN order_products op ON s.id = op.order_id
                LEFT JOIN products p ON op.product_id = p.id
                LEFT JOIN offer_stocks os ON p.sku = os.sku
                WHERE {where}
                  AND TRIM(p.brand) != '' AND p.brand IS NOT NULL
                GROUP BY 1
                ORDER BY total_revenue DESC
                LIMIT ?
            """, params + [limit]).fetchall()

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
        async with self.connection() as conn:
            params: list = [start_date, end_date]
            where = self._margin_base_where(sales_type, params)

            rows = conn.execute(f"""
                WITH root_cat AS (
                    SELECT c1.id,
                           COALESCE(c2.name, c1.name) as root_name
                    FROM categories c1
                    LEFT JOIN categories c2 ON c1.parent_id = c2.id
                )
                SELECT
                    COALESCE(rc.root_name, 'Uncategorized') as category,
                    SUM(op.quantity) as total_units,
                    ROUND(SUM(op.price_sold * op.quantity), 2) as total_revenue,
                    ROUND(SUM(CASE WHEN os.purchased_price IS NOT NULL AND os.purchased_price > 0
                        THEN op.price_sold * op.quantity ELSE 0 END), 2) as costed_revenue,
                    ROUND(SUM(CASE WHEN os.purchased_price IS NOT NULL AND os.purchased_price > 0
                        THEN os.purchased_price * op.quantity ELSE 0 END), 2) as cogs,
                    ROUND(100.0 * SUM(op.price_sold * op.quantity)
                        / SUM(SUM(op.price_sold * op.quantity)) OVER (), 1) as rev_share_pct
                FROM silver_orders s
                JOIN order_products op ON s.id = op.order_id
                LEFT JOIN products p ON op.product_id = p.id
                LEFT JOIN offer_stocks os ON p.sku = os.sku
                LEFT JOIN root_cat rc ON p.category_id = rc.id
                WHERE {where}
                GROUP BY 1
                ORDER BY total_revenue DESC
            """, params).fetchall()

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
        async with self.connection() as conn:
            params: list = [start_date, end_date]
            where = self._margin_base_where(sales_type, params)

            rows = conn.execute(f"""
                SELECT
                    strftime(s.order_date, '%Y-%m') as month,
                    ROUND(SUM(CASE WHEN os.purchased_price IS NOT NULL AND os.purchased_price > 0
                        THEN op.price_sold * op.quantity ELSE 0 END), 2) as costed_revenue,
                    ROUND(SUM(CASE WHEN os.purchased_price IS NOT NULL AND os.purchased_price > 0
                        THEN os.purchased_price * op.quantity ELSE 0 END), 2) as cogs,
                    ROUND(SUM(op.price_sold * op.quantity), 2) as total_revenue
                FROM silver_orders s
                JOIN order_products op ON s.id = op.order_id
                LEFT JOIN products p ON op.product_id = p.id
                LEFT JOIN offer_stocks os ON p.sku = os.sku
                WHERE {where}
                GROUP BY 1
                ORDER BY 1
            """, params).fetchall()

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
        async with self.connection() as conn:
            params: list = [start_date, end_date]
            where = self._margin_base_where(sales_type, params)

            rows = conn.execute(f"""
                WITH root_cat AS (
                    SELECT c1.id,
                           COALESCE(c2.name, c1.name) as root_name
                    FROM categories c1
                    LEFT JOIN categories c2 ON c1.parent_id = c2.id
                )
                SELECT
                    COALESCE(NULLIF(TRIM(p.brand), ''), 'Unknown') as brand,
                    COALESCE(rc.root_name, 'Uncategorized') as category,
                    SUM(op.quantity) as total_units,
                    ROUND(SUM(op.price_sold * op.quantity), 2) as total_revenue,
                    ROUND(SUM(CASE WHEN os.purchased_price IS NOT NULL AND os.purchased_price > 0
                        THEN op.price_sold * op.quantity ELSE 0 END), 2) as costed_revenue,
                    ROUND(SUM(CASE WHEN os.purchased_price IS NOT NULL AND os.purchased_price > 0
                        THEN os.purchased_price * op.quantity ELSE 0 END), 2) as cogs
                FROM silver_orders s
                JOIN order_products op ON s.id = op.order_id
                LEFT JOIN products p ON op.product_id = p.id
                LEFT JOIN offer_stocks os ON p.sku = os.sku
                LEFT JOIN root_cat rc ON p.category_id = rc.id
                WHERE {where}
                GROUP BY 1, 2
                HAVING SUM(op.price_sold * op.quantity) > ?
                ORDER BY brand, total_revenue DESC
            """, params + [min_revenue]).fetchall()

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
        async with self.connection() as conn:
            params: list = [start_date, end_date]
            where = self._margin_base_where(sales_type, params)

            rows = conn.execute(f"""
                SELECT
                    COALESCE(NULLIF(TRIM(p.brand), ''), 'Unknown') as brand,
                    SUM(op.quantity) as total_units,
                    ROUND(SUM(op.price_sold * op.quantity), 2) as total_revenue,
                    ROUND(SUM(CASE WHEN os.purchased_price IS NOT NULL AND os.purchased_price > 0
                        THEN op.price_sold * op.quantity ELSE 0 END), 2) as costed_revenue,
                    ROUND(SUM(CASE WHEN os.purchased_price IS NOT NULL AND os.purchased_price > 0
                        THEN os.purchased_price * op.quantity ELSE 0 END), 2) as cogs
                FROM silver_orders s
                JOIN order_products op ON s.id = op.order_id
                LEFT JOIN products p ON op.product_id = p.id
                LEFT JOIN offer_stocks os ON p.sku = os.sku
                WHERE {where}
                GROUP BY 1
                HAVING SUM(CASE WHEN os.purchased_price IS NOT NULL AND os.purchased_price > 0
                    THEN op.price_sold * op.quantity ELSE 0 END) > ?
                ORDER BY total_revenue DESC
            """, params + [min_revenue]).fetchall()

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
