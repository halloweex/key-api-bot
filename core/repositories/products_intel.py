"""DuckDBStore product intelligence methods (basket analysis, pairs, momentum)."""
from __future__ import annotations

from datetime import date
from typing import Optional, Dict, Any, List

from core.duckdb_constants import line_window_where


class ProductsIntelMixin:

    # ─── Which engine answers the product-intelligence reads ─────────────
    #
    # Five of the six needed only table names; `get_product_momentum` needed
    # its Gold replaced by the line level (see `core/pg_products_intel_read`).
    #
    # The choice is made BEFORE any connection is taken — `/inventory`'s §34
    # invariant. **Never call this from inside `self.connection()`**: the store
    # lock is not reentrant and the deadlock does not raise, it hangs.

    async def _intel_run(
        self, sql: str, params: "list | None" = None, *, mode: str = "all",
    ):
        """Run one statement against whichever engine the flag names."""
        from core.sql_dialect import DUCKDB, POSTGRES, render_tables

        from core import pg_products_intel_read

        params = list(params or [])
        if pg_products_intel_read.enabled() and pg_products_intel_read.available():
            try:
                rows = await pg_products_intel_read.fetch(
                    render_tables(sql, POSTGRES), params,
                )
                return (rows[0] if rows else None) if mode == "one" else rows
            except Exception as exc:  # noqa: BLE001
                logger.error(
                    "products-intel: Postgres failed, falling back to DuckDB: %s",
                    exc, exc_info=True,
                )

        async with self.connection() as conn:
            cursor = conn.execute(render_tables(sql, DUCKDB), params)
            return cursor.fetchone() if mode == "one" else cursor.fetchall()

    async def get_basket_summary(
        self,
        start_date: date,
        end_date: date,
        sales_type: str = "retail",
    ) -> Dict[str, Any]:
        """Get basket KPIs: avg size, multi-item %, revenue uplift, top pair."""
        params: list = [start_date, end_date]
        where_sql = line_window_where(sales_type, params)

        result = await self._intel_run(f"""
            WITH order_sizes AS (
                SELECT l.order_id, l.order_grand_total AS grand_total,
                       COUNT(DISTINCT COALESCE(l.product_id, l.line_id)) AS item_count
                FROM {{order_lines}} l
                WHERE {where_sql}
                GROUP BY l.order_id, l.order_grand_total
            )
            SELECT
                COALESCE(AVG(item_count), 0) AS avg_basket_size,
                COUNT(*) AS total_orders,
                COUNT(CASE WHEN item_count >= 2 THEN 1 END) AS multi_item_orders,
                COALESCE(SUM(CASE WHEN item_count >= 2 THEN grand_total END), 0) AS multi_revenue,
                COALESCE(SUM(grand_total), 0) AS total_revenue,
                COALESCE(AVG(CASE WHEN item_count >= 2 THEN grand_total END), 0) AS multi_aov,
                COALESCE(AVG(CASE WHEN item_count = 1 THEN grand_total END), 0) AS single_aov
            FROM order_sizes
        """, params, mode="one")

        total_orders = int(result[1] or 0)
        multi_orders = int(result[2] or 0)
        multi_pct = round(multi_orders / total_orders * 100, 1) if total_orders > 0 else 0
        multi_aov = float(result[5] or 0)
        single_aov = float(result[6] or 0)
        uplift = round(multi_aov / single_aov, 1) if single_aov > 0 else 0

        # Top pair by co-occurrence (date-filtered)
        top_pair = await self._intel_run(f"""
            WITH oi AS (
                SELECT l.order_id,
                       COALESCE(l.product_id, l.line_id) AS product_id,
                       ANY_VALUE(l.product_name) AS product_name
                FROM {{order_lines}} l
                WHERE {where_sql}
                GROUP BY l.order_id, COALESCE(l.product_id, l.line_id)
            ),
            multi AS (
                SELECT order_id, product_id, product_name
                FROM oi WHERE order_id IN (
                    SELECT order_id FROM oi GROUP BY order_id HAVING COUNT(*) >= 2
                )
            )
            SELECT
                COALESCE(p_a.name, a.product_name) AS name_a,
                COALESCE(p_b.name, b.product_name) AS name_b,
                COUNT(DISTINCT a.order_id) AS co_occurrence
            FROM multi a
            JOIN multi b ON a.order_id = b.order_id AND a.product_id < b.product_id
            LEFT JOIN {{products}} p_a ON a.product_id = p_a.id
            LEFT JOIN {{products}} p_b ON b.product_id = p_b.id
            GROUP BY name_a, name_b
            -- Ties decide the cut, and two engines break them differently;
            -- under a LIMIT that means different rows, not a different
            -- order. `/margin` shipped one and the gate caught it on two
            -- categories tied at ₴1450.
            ORDER BY co_occurrence DESC, name_a, name_b
            LIMIT 1
        """, params, mode="one")

        top_pair_name = f"{top_pair[0]} + {top_pair[1]}" if top_pair else "N/A"
        top_pair_count = int(top_pair[2]) if top_pair else 0

        return {
            "avgBasketSize": round(float(result[0] or 0), 1),
            "multiItemPct": multi_pct,
            "multiItemOrders": multi_orders,
            "totalOrders": total_orders,
            "aovUplift": uplift,
            "multiAov": round(multi_aov, 0),
            "singleAov": round(single_aov, 0),
            "topPair": top_pair_name,
            "topPairCount": top_pair_count,
        }

    async def get_frequently_bought_together(
        self,
        start_date: date,
        end_date: date,
        sales_type: str = "retail",
        limit: int = 20,
        product_id: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        """Get top product pairs by co-occurrence within date range."""
        params: list = [start_date, end_date]
        where_sql = line_window_where(sales_type, params)

        # Dynamic threshold: >= 2 for 14+ day ranges, >= 1 for shorter
        days_span = (end_date - start_date).days + 1
        having_threshold = 2 if days_span >= 14 else 1

        product_filter = ""
        if product_id is not None:
            product_filter = "WHERE a_id = ? OR b_id = ?"
            params.extend([product_id, product_id])

        rows = await self._intel_run(f"""
            WITH order_items AS (
                SELECT l.order_id,
                       COALESCE(l.product_id, l.line_id) AS product_id,
                       ANY_VALUE(l.product_name) AS product_name
                FROM {{order_lines}} l
                WHERE {where_sql}
                GROUP BY l.order_id, COALESCE(l.product_id, l.line_id)
            ),
            multi_orders AS (
                SELECT order_id, product_id, product_name
                FROM order_items
                WHERE order_id IN (
                    SELECT order_id FROM order_items
                    GROUP BY order_id HAVING COUNT(*) >= 2
                )
            ),
            pair_counts AS (
                SELECT a.product_id AS a_id, b.product_id AS b_id,
                       COUNT(DISTINCT a.order_id) AS co_occurrence
                FROM multi_orders a
                JOIN multi_orders b ON a.order_id = b.order_id
                    AND a.product_id < b.product_id
                GROUP BY a.product_id, b.product_id
                HAVING co_occurrence >= {having_threshold}
            ),
            product_orders AS (
                SELECT product_id, COUNT(DISTINCT order_id) AS orders
                FROM order_items
                GROUP BY product_id
            ),
            total AS (
                SELECT COUNT(DISTINCT order_id) AS total_orders FROM order_items
            ),
            with_metrics AS (
                SELECT pc.a_id, pc.b_id, pc.co_occurrence,
                       COALESCE(p_a.name, oi_a.product_name, 'Unknown') AS a_name,
                       COALESCE(p_b.name, oi_b.product_name, 'Unknown') AS b_name,
                       po_a.orders AS a_orders,
                       po_b.orders AS b_orders,
                       t.total_orders,
                       pc.co_occurrence * 1.0 / t.total_orders AS support,
                       pc.co_occurrence * 1.0 / po_a.orders AS conf_a_to_b,
                       pc.co_occurrence * 1.0 / po_b.orders AS conf_b_to_a,
                       CASE WHEN po_a.orders * po_b.orders > 0
                           THEN (pc.co_occurrence * t.total_orders * 1.0) / (po_a.orders * po_b.orders)
                           ELSE 0 END AS lift
                FROM pair_counts pc
                LEFT JOIN {{products}} p_a ON pc.a_id = p_a.id
                LEFT JOIN {{products}} p_b ON pc.b_id = p_b.id
                LEFT JOIN (SELECT product_id, ANY_VALUE(product_name) AS product_name FROM multi_orders GROUP BY product_id) oi_a ON pc.a_id = oi_a.product_id
                LEFT JOIN (SELECT product_id, ANY_VALUE(product_name) AS product_name FROM multi_orders GROUP BY product_id) oi_b ON pc.b_id = oi_b.product_id
                LEFT JOIN product_orders po_a ON pc.a_id = po_a.product_id
                LEFT JOIN product_orders po_b ON pc.b_id = po_b.product_id
                CROSS JOIN total t
            )
            SELECT a_id, a_name, b_id, b_name,
                   co_occurrence, support, conf_a_to_b, conf_b_to_a, lift,
                   a_orders, b_orders, total_orders
            FROM with_metrics
            {product_filter}
            -- Ties decide the cut, and two engines break them differently;
            -- under a LIMIT that means different rows, not a different
            -- order. `/margin` shipped one and the gate caught it on two
            -- categories tied at ₴1450.
            ORDER BY co_occurrence DESC, a_id, b_id
            LIMIT ?
        """, params + [limit])

        return [
            {
                "productA": {"id": r[0], "name": r[1], "orders": int(r[9])},
                "productB": {"id": r[2], "name": r[3], "orders": int(r[10])},
                "coOccurrence": int(r[4]),
                "support": round(float(r[5]), 4),
                "confidenceAtoB": round(float(r[6]), 3),
                "confidenceBtoA": round(float(r[7]), 3),
                "lift": round(float(r[8]), 2),
                "totalOrders": int(r[11]),
            }
            for r in rows
        ]

    async def get_basket_distribution(
        self,
        start_date: date,
        end_date: date,
        sales_type: str = "retail",
    ) -> List[Dict[str, Any]]:
        """Get basket size distribution with AOV per bucket."""
        params: list = [start_date, end_date]
        where_sql = line_window_where(sales_type, params)

        rows = await self._intel_run(f"""
            WITH order_sizes AS (
                SELECT l.order_id, l.order_grand_total AS grand_total,
                       COUNT(DISTINCT COALESCE(l.product_id, l.line_id)) AS item_count
                FROM {{order_lines}} l
                WHERE {where_sql}
                GROUP BY l.order_id, l.order_grand_total
            ),
            bucketed AS (
                SELECT
                    CASE
                        WHEN item_count = 1 THEN '1 item'
                        WHEN item_count = 2 THEN '2 items'
                        WHEN item_count = 3 THEN '3 items'
                        WHEN item_count = 4 THEN '4 items'
                        WHEN item_count BETWEEN 5 AND 7 THEN '5-7 items'
                        ELSE '8+ items'
                    END AS bucket,
                    CASE
                        WHEN item_count = 1 THEN 1
                        WHEN item_count = 2 THEN 2
                        WHEN item_count = 3 THEN 3
                        WHEN item_count = 4 THEN 4
                        WHEN item_count BETWEEN 5 AND 7 THEN 5
                        ELSE 6
                    END AS sort_order,
                    grand_total
                FROM order_sizes
            )
            SELECT bucket, sort_order,
                   COUNT(*) AS orders,
                   COALESCE(SUM(grand_total), 0) AS revenue,
                   COALESCE(AVG(grand_total), 0) AS aov
            FROM bucketed
            GROUP BY bucket, sort_order
            ORDER BY sort_order
        """, params)

        return [
            {
                "bucket": r[0],
                "orders": int(r[2]),
                "revenue": round(float(r[3]), 0),
                "aov": round(float(r[4]), 0),
            }
            for r in rows
        ]

    async def get_category_combinations(
        self,
        start_date: date,
        end_date: date,
        sales_type: str = "retail",
        limit: int = 10,
    ) -> List[Dict[str, Any]]:
        """Get top category pair combinations from multi-item orders."""
        params: list = [start_date, end_date]
        where_sql = line_window_where(sales_type, params)

        rows = await self._intel_run(f"""
            WITH order_cats AS (
                SELECT DISTINCT l.order_id,
                       COALESCE(l.parent_category_name, l.category_name, 'Unknown') AS category_name,
                       COALESCE(l.parent_category_id, l.category_id) AS category_id
                FROM {{order_lines}} l
                WHERE {where_sql}
            ),
            cat_pairs AS (
                SELECT a.category_name AS cat_a, b.category_name AS cat_b,
                       COUNT(DISTINCT a.order_id) AS co_occurrence
                FROM order_cats a
                JOIN order_cats b ON a.order_id = b.order_id
                    AND a.category_name < b.category_name
                GROUP BY a.category_name, b.category_name
                HAVING co_occurrence >= 2
            )
            SELECT cat_a, cat_b, co_occurrence
            FROM cat_pairs
            -- Ties decide the cut, and two engines break them differently;
            -- under a LIMIT that means different rows, not a different
            -- order. `/margin` shipped one and the gate caught it on two
            -- categories tied at ₴1450.
            ORDER BY co_occurrence DESC, cat_a, cat_b
            LIMIT ?
        """, params + [limit])

        return [
            {
                "categoryA": r[0],
                "categoryB": r[1],
                "coOccurrence": int(r[2]),
            }
            for r in rows
        ]

    async def get_brand_affinity(
        self,
        start_date: date,
        end_date: date,
        sales_type: str = "retail",
        limit: int = 10,
    ) -> List[Dict[str, Any]]:
        """Get top brand pair co-purchases within date range."""
        params: list = [start_date, end_date]
        where_sql = line_window_where(sales_type, params)

        days_span = (end_date - start_date).days + 1
        having_threshold = 2 if days_span >= 14 else 1

        rows = await self._intel_run(f"""
            WITH order_brands AS (
                SELECT DISTINCT l.order_id, l.brand
                FROM {{order_lines}} l
                WHERE {where_sql}
                  AND l.brand IS NOT NULL AND l.brand != ''
            ),
            brand_pairs AS (
                SELECT a.brand AS brand_a, b.brand AS brand_b,
                       COUNT(DISTINCT a.order_id) AS co_occurrence,
                       0 AS product_pairs
                FROM order_brands a
                JOIN order_brands b ON a.order_id = b.order_id
                    AND a.brand < b.brand
                GROUP BY a.brand, b.brand
                HAVING co_occurrence >= {having_threshold}
            )
            SELECT brand_a, brand_b, co_occurrence, product_pairs
            FROM brand_pairs
            -- Ties decide the cut, and two engines break them differently;
            -- under a LIMIT that means different rows, not a different
            -- order. `/margin` shipped one and the gate caught it on two
            -- categories tied at ₴1450.
            ORDER BY co_occurrence DESC, brand_a, brand_b
            LIMIT ?
        """, params + [limit])

        return [
            {
                "brandA": r[0],
                "brandB": r[1],
                "coOccurrence": int(r[2]),
                "productPairs": int(r[3]),
            }
            for r in rows
        ]

    async def get_product_momentum(
        self,
        start_date: date,
        end_date: date,
        sales_type: str = "retail",
        limit: int = 5,
    ) -> Dict[str, List[Dict[str, Any]]]:
        """Get products with biggest revenue growth/decline vs previous period."""
        from datetime import timedelta
        # Calculate previous period of same length
        days = (end_date - start_date).days + 1
        prev_start = start_date - timedelta(days=days)
        prev_end = start_date - timedelta(days=1)

        sales_filter = ""
        params: list = []
        if sales_type != "all":
            sales_filter = "AND l.sales_type = ?"
            # current_period params: start, end, sales_type
            # prev_period params: prev_start, prev_end, sales_type
            params = [start_date, end_date, sales_type, prev_start, prev_end, sales_type]
        else:
            params = [start_date, end_date, prev_start, prev_end]

        rows = await self._intel_run(f"""
            -- The line level, not `gold_daily_products`: that table is
            -- DuckDB's alone, and the level reproduces it to the kopeck under
            -- Gold's own predicate (`core/duckdb_store.py`). Easier here than
            -- in the marketing brand table, because momentum never reads
            -- `order_count` — only the two sums — so no grain has to be
            -- rebuilt before summing.
            --
            -- `product_name` stays in the key: Gold's rows carry the name *as
            -- sold*, so a product sold under two names is two rows there, and
            -- dropping it would quietly merge them.
            WITH current_period AS (
                SELECT l.product_id, l.product_name,
                       SUM(l.line_amount) AS revenue,
                       SUM(l.quantity) AS qty
                FROM {{order_lines}} l
                WHERE NOT l.is_return AND l.is_active_source
                  AND l.order_date BETWEEN ? AND ? {sales_filter}
                GROUP BY l.product_id, l.product_name
                HAVING SUM(l.line_amount) > 0
            ),
            prev_period AS (
                SELECT l.product_id,
                       SUM(l.line_amount) AS revenue,
                       SUM(l.quantity) AS qty
                FROM {{order_lines}} l
                WHERE NOT l.is_return AND l.is_active_source
                  AND l.order_date BETWEEN ? AND ? {sales_filter}
                GROUP BY l.product_id
                HAVING SUM(l.line_amount) > 0
            ),
            momentum AS (
                SELECT c.product_id, c.product_name,
                       c.revenue AS current_revenue,
                       COALESCE(p.revenue, 0) AS prev_revenue,
                       c.qty AS current_qty,
                       COALESCE(p.qty, 0) AS prev_qty,
                       CASE
                           WHEN COALESCE(p.revenue, 0) > 0
                           THEN ((c.revenue - p.revenue) / p.revenue) * 100
                           ELSE NULL
                       END AS growth_pct
                FROM current_period c
                LEFT JOIN prev_period p ON c.product_id = p.product_id
                WHERE c.revenue >= 500
            )
            SELECT * FROM momentum
            WHERE growth_pct IS NOT NULL
            -- Ties decide the cut, and two engines break them differently;
            -- under a LIMIT that means different rows, not a different
            -- order. `/margin` shipped one and the gate caught it on two
            -- categories tied at ₴1450.
            ORDER BY growth_pct DESC, product_id, product_name
        """, params)

        gainers = []
        losers = []
        for r in rows:
            item = {
                "productId": r[0],
                "productName": r[1],
                "currentRevenue": round(float(r[2]), 0),
                "prevRevenue": round(float(r[3]), 0),
                "currentQty": int(r[4]),
                "prevQty": int(r[5]),
                "growthPct": round(float(r[6]), 1),
            }
            if r[6] > 0:
                gainers.append(item)
            elif r[6] < 0:
                losers.append(item)

        # Top gainers sorted by growth %, top losers sorted by decline
        gainers.sort(key=lambda x: x["growthPct"], reverse=True)
        losers.sort(key=lambda x: x["growthPct"])

        return {
            "gainers": gainers[:limit],
            "losers": losers[:limit],
        }
