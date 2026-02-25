"""DuckDBStore product intelligence methods (basket analysis, pairs, momentum)."""
from __future__ import annotations

from datetime import date
from typing import Optional, Dict, Any, List


class ProductsIntelMixin:

    async def get_basket_summary(
        self,
        start_date: date,
        end_date: date,
        sales_type: str = "retail",
    ) -> Dict[str, Any]:
        """Get basket KPIs: avg size, multi-item %, revenue uplift, top pair."""
        async with self.connection() as conn:
            params: list = [start_date, end_date]
            where = ["s.order_date BETWEEN ? AND ?", "NOT s.is_return", "s.is_active_source"]

            if sales_type != "all":
                where.append("s.sales_type = ?")
                params.append(sales_type)

            where_sql = " AND ".join(where)

            result = conn.execute(f"""
                WITH order_sizes AS (
                    SELECT s.id AS order_id, s.grand_total,
                           COUNT(DISTINCT COALESCE(op.product_id, op.id)) AS item_count
                    FROM silver_orders s
                    JOIN order_products op ON s.id = op.order_id
                    WHERE {where_sql}
                    GROUP BY s.id, s.grand_total
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
            """, params).fetchone()

            total_orders = int(result[1] or 0)
            multi_orders = int(result[2] or 0)
            multi_pct = round(multi_orders / total_orders * 100, 1) if total_orders > 0 else 0
            multi_aov = float(result[5] or 0)
            single_aov = float(result[6] or 0)
            uplift = round(multi_aov / single_aov, 1) if single_aov > 0 else 0

            # Top pair by co-occurrence
            st_param = sales_type if sales_type != "all" else None
            if st_param:
                top_pair = conn.execute("""
                    SELECT product_a_name, product_b_name, co_occurrence
                    FROM gold_product_pairs
                    WHERE sales_type = ?
                    ORDER BY co_occurrence DESC LIMIT 1
                """, [st_param]).fetchone()
            else:
                top_pair = conn.execute("""
                    SELECT product_a_name, product_b_name, co_occurrence
                    FROM gold_product_pairs
                    ORDER BY co_occurrence DESC LIMIT 1
                """).fetchone()

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
        sales_type: str = "retail",
        limit: int = 20,
        product_id: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        """Get top product pairs from pre-computed gold table."""
        async with self.connection() as conn:
            params: list = []
            where = []

            if sales_type != "all":
                where.append("sales_type = ?")
                params.append(sales_type)

            if product_id is not None:
                where.append("(product_a_id = ? OR product_b_id = ?)")
                params.extend([product_id, product_id])

            where_sql = " AND ".join(where) if where else "1=1"

            rows = conn.execute(f"""
                SELECT product_a_id, product_a_name, product_b_id, product_b_name,
                       co_occurrence, support, confidence_a_to_b, confidence_b_to_a, lift,
                       product_a_orders, product_b_orders, total_orders
                FROM gold_product_pairs
                WHERE {where_sql}
                ORDER BY co_occurrence DESC
                LIMIT ?
            """, params + [limit]).fetchall()

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
        async with self.connection() as conn:
            params: list = [start_date, end_date]
            where = ["s.order_date BETWEEN ? AND ?", "NOT s.is_return", "s.is_active_source"]

            if sales_type != "all":
                where.append("s.sales_type = ?")
                params.append(sales_type)

            where_sql = " AND ".join(where)

            rows = conn.execute(f"""
                WITH order_sizes AS (
                    SELECT s.id AS order_id, s.grand_total,
                           COUNT(DISTINCT COALESCE(op.product_id, op.id)) AS item_count
                    FROM silver_orders s
                    JOIN order_products op ON s.id = op.order_id
                    WHERE {where_sql}
                    GROUP BY s.id, s.grand_total
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
            """, params).fetchall()

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
        async with self.connection() as conn:
            params: list = [start_date, end_date]
            where = ["s.order_date BETWEEN ? AND ?", "NOT s.is_return", "s.is_active_source"]

            if sales_type != "all":
                where.append("s.sales_type = ?")
                params.append(sales_type)

            where_sql = " AND ".join(where)

            rows = conn.execute(f"""
                WITH order_cats AS (
                    SELECT DISTINCT s.id AS order_id,
                           COALESCE(parent_c.name, c.name, 'Unknown') AS category_name,
                           COALESCE(parent_c.id, c.id) AS category_id
                    FROM silver_orders s
                    JOIN order_products op ON s.id = op.order_id
                    LEFT JOIN products p ON op.product_id = p.id
                    LEFT JOIN categories c ON p.category_id = c.id
                    LEFT JOIN categories parent_c ON c.parent_id = parent_c.id
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
                ORDER BY co_occurrence DESC
                LIMIT ?
            """, params + [limit]).fetchall()

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
        sales_type: str = "retail",
        limit: int = 10,
    ) -> List[Dict[str, Any]]:
        """Get top brand pair co-purchases from gold_product_pairs + products."""
        async with self.connection() as conn:
            params: list = []
            where = []

            if sales_type != "all":
                where.append("gpp.sales_type = ?")
                params.append(sales_type)

            where_sql = " AND ".join(where) if where else "1=1"

            rows = conn.execute(f"""
                SELECT
                    COALESCE(pa.brand, 'Unknown') AS brand_a,
                    COALESCE(pb.brand, 'Unknown') AS brand_b,
                    SUM(gpp.co_occurrence) AS total_co_occurrence,
                    COUNT(*) AS product_pairs
                FROM gold_product_pairs gpp
                LEFT JOIN products pa ON gpp.product_a_id = pa.id
                LEFT JOIN products pb ON gpp.product_b_id = pb.id
                WHERE {where_sql}
                  AND pa.brand IS NOT NULL AND pb.brand IS NOT NULL
                  AND pa.brand != pb.brand
                GROUP BY COALESCE(pa.brand, 'Unknown'), COALESCE(pb.brand, 'Unknown')
                ORDER BY total_co_occurrence DESC
                LIMIT ?
            """, params + [limit]).fetchall()

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
        async with self.connection() as conn:
            from datetime import timedelta
            # Calculate previous period of same length
            days = (end_date - start_date).days + 1
            prev_start = start_date - timedelta(days=days)
            prev_end = start_date - timedelta(days=1)

            sales_filter = ""
            params: list = []
            if sales_type != "all":
                sales_filter = "AND sales_type = ?"
                # current_period params: start, end, sales_type
                # prev_period params: prev_start, prev_end, sales_type
                params = [start_date, end_date, sales_type, prev_start, prev_end, sales_type]
            else:
                params = [start_date, end_date, prev_start, prev_end]

            rows = conn.execute(f"""
                WITH current_period AS (
                    SELECT product_id, product_name,
                           SUM(product_revenue) AS revenue,
                           SUM(quantity_sold) AS qty
                    FROM gold_daily_products
                    WHERE date BETWEEN ? AND ? {sales_filter}
                    GROUP BY product_id, product_name
                    HAVING SUM(product_revenue) > 0
                ),
                prev_period AS (
                    SELECT product_id,
                           SUM(product_revenue) AS revenue,
                           SUM(quantity_sold) AS qty
                    FROM gold_daily_products
                    WHERE date BETWEEN ? AND ? {sales_filter}
                    GROUP BY product_id
                    HAVING SUM(product_revenue) > 0
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
                ORDER BY growth_pct DESC
            """, params).fetchall()

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
