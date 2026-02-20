"""DuckDBStore revenue and analytics methods."""
from __future__ import annotations

import re
from datetime import timedelta
from typing import Optional, List, Dict, Any, Tuple

from core.duckdb_constants import _date_in_kyiv
from core.models import OrderStatus


class RevenueMixin:

    async def get_summary_stats(
        self,
        start_date: date,
        end_date: date,
        source_id: Optional[int] = None,
        category_id: Optional[int] = None,
        brand: Optional[str] = None,
        sales_type: str = "retail"
    ) -> Dict[str, Any]:
        """Get summary statistics for a date range (from Gold/Silver layers)."""
        async with self.connection() as conn:
            if category_id or brand:
                # Use Silver layer with JOINs for correct distinct order counts
                # (gold_daily_products can't deduplicate orders with multiple matching products)
                params = [start_date, end_date]
                where_clauses = ["s.order_date BETWEEN ? AND ?", "NOT s.is_return", "s.is_active_source"]

                if sales_type != "all":
                    where_clauses.append("s.sales_type = ?")
                    params.append(sales_type)

                if source_id:
                    where_clauses.append("s.source_id = ?")
                    params.append(source_id)

                cat_ids = None
                if category_id:
                    cat_ids = await self._get_category_with_children(conn, category_id)
                    where_clauses.append(f"p.category_id IN ({','.join('?' * len(cat_ids))})")
                    params.extend(cat_ids)

                if brand:
                    where_clauses.append("LOWER(p.brand) = LOWER(?)")
                    params.append(brand)

                where_sql = " AND ".join(where_clauses)

                # Query Silver + order_products + products for correct distinct counts
                result = conn.execute(f"""
                    SELECT
                        COUNT(DISTINCT s.id) as total_orders,
                        COALESCE(SUM(op.price_sold * op.quantity), 0) as total_revenue
                    FROM silver_orders s
                    JOIN order_products op ON s.id = op.order_id
                    LEFT JOIN products p ON op.product_id = p.id
                    WHERE {where_sql}
                """, params).fetchone()

                total_orders = int(result[0] or 0)
                total_revenue = float(result[1] or 0)

                # Returns from silver_orders — consistent with the filtered orders query above
                # (Gold doesn't have category/brand/source breakdown for returns)
                ret_params = [start_date, end_date]
                ret_where = ["s.order_date BETWEEN ? AND ?", "s.is_return", "s.is_active_source"]
                if sales_type != "all":
                    ret_where.append("s.sales_type = ?")
                    ret_params.append(sales_type)
                if source_id:
                    ret_where.append("s.source_id = ?")
                    ret_params.append(source_id)
                ret_result = conn.execute(f"""
                    SELECT COUNT(DISTINCT s.id), COALESCE(SUM(s.grand_total), 0)
                    FROM silver_orders s
                    WHERE {" AND ".join(ret_where)}
                """, ret_params).fetchone()
                total_returns = int(ret_result[0])
                returns_revenue = float(ret_result[1])
            else:
                # Use gold_daily_revenue for non-product queries
                params = [start_date, end_date]
                where_clauses = ["date BETWEEN ? AND ?"]

                if sales_type != "all":
                    where_clauses.append("sales_type = ?")
                    params.append(sales_type)

                where_sql = " AND ".join(where_clauses)

                if source_id:
                    # Source-specific: sum per-source columns for orders/revenue
                    source_col_map = {1: "instagram", 2: "telegram", 4: "shopify"}
                    src_name = source_col_map.get(source_id)
                    if src_name:
                        result = conn.execute(f"""
                            SELECT
                                SUM({src_name}_orders) as total_orders,
                                SUM({src_name}_revenue) as total_revenue
                            FROM gold_daily_revenue
                            WHERE {where_sql}
                        """, params).fetchone()
                        # Gold doesn't have per-source return columns — query Silver
                        ret_params = [start_date, end_date, source_id]
                        sales_filter = self._build_sales_type_filter(sales_type, conn=conn)
                        ret_result = conn.execute(f"""
                            SELECT COUNT(DISTINCT id), COALESCE(SUM(grand_total), 0)
                            FROM silver_orders
                            WHERE order_date BETWEEN ? AND ?
                              AND is_return AND is_active_source AND source_id = ?
                              AND {sales_filter}
                        """, ret_params).fetchone()
                        result = (result[0], result[1], ret_result[0], ret_result[1])
                    else:
                        result = (0, 0, 0, 0)
                else:
                    result = conn.execute(f"""
                        SELECT
                            SUM(orders_count) as total_orders,
                            SUM(revenue) as total_revenue,
                            SUM(returns_count) as total_returns,
                            SUM(returns_revenue) as returns_revenue
                        FROM gold_daily_revenue
                        WHERE {where_sql}
                    """, params).fetchone()

                total_orders = int(result[0] or 0)
                total_revenue = float(result[1] or 0)
                total_returns = int(result[2] or 0)
                returns_revenue = float(result[3] or 0)

            avg_check = total_revenue / total_orders if total_orders > 0 else 0

            return {
                "totalOrders": total_orders,
                "totalRevenue": round(total_revenue, 2),
                "avgCheck": round(avg_check, 2),
                "totalReturns": total_returns,
                "returnsRevenue": round(returns_revenue, 2),
                "startDate": start_date.isoformat(),
                "endDate": end_date.isoformat()
            }

    async def get_return_orders(
        self,
        start_date: date,
        end_date: date,
        sales_type: str = "retail",
        limit: int = 50
    ) -> List[Dict[str, Any]]:
        """Get list of return orders for a date range.

        Returns list of orders with id, date, amount, status, source.
        """
        # Derive status names from OrderStatus enum (single source of truth)
        STATUS_NAMES = {
            s.value: s.name.replace("_", " ").title()
            for s in OrderStatus.return_statuses()
        }
        # Ensure we have a mapping (safety net)
        STATUS_NAMES.setdefault(19, "Returned")

        async with self.connection() as conn:
            params: list = [start_date, end_date]
            where_clauses = [
                "s.order_date BETWEEN ? AND ?",
                "s.is_return = TRUE",
                "s.is_active_source = TRUE"
            ]

            if sales_type != "all":
                where_clauses.append("s.sales_type = ?")
                params.append(sales_type)

            where_sql = " AND ".join(where_clauses)
            params.append(limit)

            result = conn.execute(f"""
                SELECT
                    s.id,
                    s.order_date,
                    s.grand_total,
                    s.status_id,
                    s.source_name,
                    s.buyer_id,
                    b.full_name AS buyer_name,
                    b.phone AS buyer_phone,
                    s.manager_id,
                    m.name AS manager_name
                FROM silver_orders s
                LEFT JOIN buyers b ON s.buyer_id = b.id
                LEFT JOIN managers m ON s.manager_id = m.id
                WHERE {where_sql}
                ORDER BY s.order_date DESC, s.id DESC
                LIMIT ?
            """, params).fetchall()

            return [
                {
                    "id": row[0],
                    "date": row[1].isoformat() if row[1] else None,
                    "amount": float(row[2] or 0),
                    "statusId": row[3],
                    "statusName": STATUS_NAMES.get(row[3], f"Status {row[3]}"),
                    "source": row[4],
                    "buyerId": row[5],
                    "buyerName": row[6],
                    "buyerPhone": row[7],
                    "managerId": row[8],
                    "managerName": row[9],
                }
                for row in result
            ]

    def _build_gold_revenue_query(
        self,
        start_date: date,
        end_date: date,
        sales_type: str = "retail",
        source_id: Optional[int] = None,
    ) -> Tuple[str, list]:
        """Build a query against gold_daily_revenue for a date range.

        Returns (sql, params) tuple that SELECTs day, revenue, order_count.
        """
        params = [start_date, end_date]
        where_clauses = ["date BETWEEN ? AND ?"]

        if sales_type != "all":
            where_clauses.append("sales_type = ?")
            params.append(sales_type)

        where_sql = " AND ".join(where_clauses)

        if source_id:
            source_col_map = {1: "instagram", 2: "telegram", 4: "shopify"}
            src = source_col_map.get(source_id)
            if src:
                sql = f"""
                    SELECT date AS day, {src}_revenue AS revenue, {src}_orders AS order_count
                    FROM gold_daily_revenue
                    WHERE {where_sql}
                    ORDER BY date
                """
            else:
                sql = f"SELECT NULL::DATE, 0, 0 WHERE FALSE"
        else:
            sql = f"""
                SELECT date AS day, revenue, orders_count AS order_count
                FROM gold_daily_revenue
                WHERE {where_sql}
                ORDER BY date
            """
        return sql, params

    def _build_silver_products_revenue_query(
        self,
        start_date: date,
        end_date: date,
        sales_type: str = "retail",
        source_id: Optional[int] = None,
        category_ids: Optional[List[int]] = None,
        brand: Optional[str] = None,
    ) -> Tuple[str, list]:
        """Build a query against Silver layer for revenue trend with product filters.

        Uses silver_orders + order_products JOIN for correct distinct order counts.
        Returns (sql, params) that SELECTs day, revenue, order_count.
        """
        params: list = [start_date, end_date]
        where_clauses = ["s.order_date BETWEEN ? AND ?", "NOT s.is_return", "s.is_active_source"]

        if sales_type != "all":
            where_clauses.append("s.sales_type = ?")
            params.append(sales_type)

        if source_id:
            where_clauses.append("s.source_id = ?")
            params.append(source_id)

        if category_ids:
            where_clauses.append(f"p.category_id IN ({','.join('?' * len(category_ids))})")
            params.extend(category_ids)

        if brand:
            where_clauses.append("LOWER(p.brand) = LOWER(?)")
            params.append(brand)

        where_sql = " AND ".join(where_clauses)
        sql = f"""
            SELECT s.order_date AS day,
                   COALESCE(SUM(op.price_sold * op.quantity), 0) AS revenue,
                   COUNT(DISTINCT s.id) AS order_count
            FROM silver_orders s
            JOIN order_products op ON s.id = op.order_id
            LEFT JOIN products p ON op.product_id = p.id
            WHERE {where_sql}
            GROUP BY s.order_date
            ORDER BY s.order_date
        """
        return sql, params

    async def get_revenue_trend(
        self,
        start_date: date,
        end_date: date,
        source_id: Optional[int] = None,
        category_id: Optional[int] = None,
        brand: Optional[str] = None,
        include_comparison: bool = True,
        sales_type: str = "retail",
        compare_type: str = "previous_period"
    ) -> Dict[str, Any]:
        """Get daily revenue trend for chart (from Gold layer)."""
        async with self.connection() as conn:
            cat_ids = None
            if category_id:
                cat_ids = await self._get_category_with_children(conn, category_id)

            use_products = bool(category_id or brand)

            if use_products:
                sql, params = self._build_silver_products_revenue_query(
                    start_date, end_date, sales_type, source_id, cat_ids, brand
                )
            else:
                sql, params = self._build_gold_revenue_query(
                    start_date, end_date, sales_type, source_id
                )

            results = conn.execute(sql, params).fetchall()
            daily_data = {row[0]: (float(row[1]), int(row[2])) for row in results}

            # Build labels and data
            labels = []
            data = []
            orders_data = []
            current = start_date
            while current <= end_date:
                labels.append(current.strftime("%d.%m"))
                revenue_val, orders_val = daily_data.get(current, (0, 0))
                data.append(round(revenue_val, 2))
                orders_data.append(orders_val)
                current += timedelta(days=1)

            datasets = [{
                "label": "This Period",
                "data": data,
                "borderColor": "#16A34A",
                "backgroundColor": "rgba(22, 163, 74, 0.1)",
                "fill": True,
                "tension": 0.3,
                "borderWidth": 2
            }]

            # Add previous period comparison
            if include_comparison:
                period_days = (end_date - start_date).days + 1

                if compare_type == "year_ago":
                    from dateutil.relativedelta import relativedelta
                    prev_start = start_date - relativedelta(years=1)
                    prev_end = end_date - relativedelta(years=1)
                elif compare_type == "month_ago":
                    from dateutil.relativedelta import relativedelta
                    prev_start = start_date - relativedelta(months=1)
                    prev_end = end_date - relativedelta(months=1)
                else:
                    prev_end = start_date - timedelta(days=1)
                    prev_start = prev_end - timedelta(days=period_days - 1)

                if use_products:
                    prev_sql, prev_params = self._build_silver_products_revenue_query(
                        prev_start, prev_end, sales_type, source_id, cat_ids, brand
                    )
                else:
                    prev_sql, prev_params = self._build_gold_revenue_query(
                        prev_start, prev_end, sales_type, source_id
                    )

                # Only need day + revenue for comparison
                prev_results = conn.execute(prev_sql, prev_params).fetchall()
                prev_daily = {row[0]: float(row[1]) for row in prev_results}

                prev_data = []
                prev_current = prev_start
                while prev_current <= prev_end:
                    prev_data.append(round(prev_daily.get(prev_current, 0), 2))
                    prev_current += timedelta(days=1)

                datasets.append({
                    "label": "Previous Period",
                    "data": prev_data,
                    "borderColor": "#9CA3AF",
                    "backgroundColor": "rgba(156, 163, 175, 0.1)",
                    "fill": False,
                    "tension": 0.3,
                    "borderWidth": 2,
                    "borderDash": [5, 5]
                })

            # Build comparison object for v2 frontend
            comparison = None
            if include_comparison and len(datasets) > 1:
                prev_dataset = datasets[1]
                current_total = sum(data)
                prev_total = sum(prev_dataset["data"])
                growth_percent = ((current_total - prev_total) / prev_total * 100) if prev_total > 0 else 0

                comparison = {
                    "labels": labels,
                    "revenue": prev_dataset["data"],
                    "orders": [],
                    "period": {
                        "start": prev_start.isoformat(),
                        "end": prev_end.isoformat(),
                        "type": compare_type
                    },
                    "totals": {
                        "current": round(current_total, 2),
                        "previous": round(prev_total, 2),
                        "growth_percent": round(growth_percent, 1)
                    }
                }

            result = {
                "labels": labels,
                "revenue": data,
                "orders": orders_data,
                "datasets": datasets
            }
            if comparison:
                result["comparison"] = comparison
            return result

    async def get_sales_by_source(
        self,
        start_date: date,
        end_date: date,
        category_id: Optional[int] = None,
        brand: Optional[str] = None,
        sales_type: str = "retail"
    ) -> Dict[str, Any]:
        """Get sales breakdown by source (from Gold/Silver layers)."""
        async with self.connection() as conn:
            source_names = {1: "Instagram", 2: "Telegram", 4: "Shopify"}
            source_colors = {1: "#7C3AED", 2: "#2563EB", 4: "#eb4200"}

            if category_id or brand:
                # Use Silver layer with JOINs for correct distinct order counts
                params = [start_date, end_date]
                where_clauses = ["s.order_date BETWEEN ? AND ?", "NOT s.is_return", "s.is_active_source"]

                if sales_type != "all":
                    where_clauses.append("s.sales_type = ?")
                    params.append(sales_type)

                if category_id:
                    cat_ids = await self._get_category_with_children(conn, category_id)
                    where_clauses.append(f"p.category_id IN ({','.join('?' * len(cat_ids))})")
                    params.extend(cat_ids)

                if brand:
                    where_clauses.append("LOWER(p.brand) = LOWER(?)")
                    params.append(brand)

                where_sql = " AND ".join(where_clauses)

                results = conn.execute(f"""
                    SELECT s.source_id,
                           COUNT(DISTINCT s.id) as orders,
                           COALESCE(SUM(op.price_sold * op.quantity), 0) as revenue
                    FROM silver_orders s
                    JOIN order_products op ON s.id = op.order_id
                    LEFT JOIN products p ON op.product_id = p.id
                    WHERE {where_sql}
                    GROUP BY s.source_id
                    ORDER BY revenue DESC
                """, params).fetchall()

                labels = []
                orders = []
                revenue = []
                colors = []
                for row in results:
                    sid = row[0]
                    if sid in source_names:
                        labels.append(source_names[sid])
                        orders.append(int(row[1]))
                        revenue.append(round(float(row[2]), 2))
                        colors.append(source_colors.get(sid, "#999999"))
            else:
                # Use gold_daily_revenue per-source columns
                params = [start_date, end_date]
                where_clauses = ["date BETWEEN ? AND ?"]

                if sales_type != "all":
                    where_clauses.append("sales_type = ?")
                    params.append(sales_type)

                where_sql = " AND ".join(where_clauses)

                result = conn.execute(f"""
                    SELECT
                        SUM(instagram_orders) as ig_orders, SUM(instagram_revenue) as ig_rev,
                        SUM(telegram_orders) as tg_orders, SUM(telegram_revenue) as tg_rev,
                        SUM(shopify_orders) as sh_orders, SUM(shopify_revenue) as sh_rev
                    FROM gold_daily_revenue
                    WHERE {where_sql}
                """, params).fetchone()

                # Build source list sorted by revenue desc
                source_data = [
                    (1, "Instagram", int(result[0] or 0), float(result[1] or 0)),
                    (2, "Telegram", int(result[2] or 0), float(result[3] or 0)),
                    (4, "Shopify", int(result[4] or 0), float(result[5] or 0)),
                ]
                source_data.sort(key=lambda x: x[3], reverse=True)

                labels = [s[1] for s in source_data if s[3] > 0 or s[2] > 0]
                orders = [s[2] for s in source_data if s[3] > 0 or s[2] > 0]
                revenue = [round(s[3], 2) for s in source_data if s[3] > 0 or s[2] > 0]
                colors = [source_colors[s[0]] for s in source_data if s[3] > 0 or s[2] > 0]

            return {
                "labels": labels,
                "orders": orders,
                "revenue": revenue,
                "backgroundColor": colors
            }

    async def get_top_products(
        self,
        start_date: date,
        end_date: date,
        source_id: Optional[int] = None,
        category_id: Optional[int] = None,
        brand: Optional[str] = None,
        limit: int = 10,
        sales_type: str = "retail"
    ) -> Dict[str, Any]:
        """Get top products by quantity (from Gold layer)."""
        async with self.connection() as conn:
            params = [start_date, end_date]
            where_clauses = ["g.date BETWEEN ? AND ?"]

            if sales_type != "all":
                where_clauses.append("g.sales_type = ?")
                params.append(sales_type)

            if source_id:
                where_clauses.append("g.source_id = ?")
                params.append(source_id)

            if category_id:
                cat_ids = await self._get_category_with_children(conn, category_id)
                where_clauses.append(f"g.category_id IN ({','.join('?' * len(cat_ids))})")
                params.extend(cat_ids)

            if brand:
                where_clauses.append("LOWER(g.brand) = LOWER(?)")
                params.append(brand)

            params.append(limit)
            where_sql = " AND ".join(where_clauses)

            results = conn.execute(f"""
                SELECT
                    ANY_VALUE(g.product_name) as product_name,
                    SUM(g.quantity_sold) as total_qty
                FROM gold_daily_products g
                WHERE {where_sql}
                GROUP BY COALESCE(CAST(g.product_id AS VARCHAR), g.product_name)
                ORDER BY total_qty DESC
                LIMIT ?
            """, params).fetchall()

            raw_labels = [row[0] or "Unknown" for row in results]
            labels = [self._wrap_label(row[0]) for row in results]
            data = [int(row[1]) for row in results]
            total = sum(data) if data else 1
            percentages = [round(d / total * 100, 1) for d in data]

            return {
                "labels": raw_labels,
                "wrappedLabels": labels,
                "data": data,
                "percentages": percentages,
                "backgroundColor": "#2563EB"
            }

    async def get_categories(self) -> List[Dict[str, Any]]:
        """Get root categories for filter dropdown."""
        async with self.connection() as conn:
            results = conn.execute("""
                SELECT id, name FROM categories
                WHERE parent_id IS NULL
                ORDER BY name
            """).fetchall()
            return [{"id": row[0], "name": row[1]} for row in results]

    async def get_child_categories(self, parent_id: int) -> List[Dict[str, Any]]:
        """Get child categories for a parent."""
        async with self.connection() as conn:
            results = conn.execute("""
                SELECT id, name FROM categories
                WHERE parent_id = ?
                ORDER BY name
            """, [parent_id]).fetchall()
            return [{"id": row[0], "name": row[1]} for row in results]

    async def get_brands(self) -> List[Dict[str, str]]:
        """Get all unique brands for filter dropdown."""
        async with self.connection() as conn:
            results = conn.execute("""
                SELECT DISTINCT brand FROM products
                WHERE brand IS NOT NULL AND brand != ''
                ORDER BY brand
            """).fetchall()
            return [{"name": row[0]} for row in results]

    # ─── Helper Methods ───────────────────────────────────────────────────────

    async def _get_category_with_children(
        self,
        conn: duckdb.DuckDBPyConnection,
        category_id: int
    ) -> List[int]:
        """Get category ID and all descendant IDs."""
        result = [category_id]

        # Recursive query to get all children
        children = conn.execute("""
            WITH RECURSIVE category_tree AS (
                SELECT id FROM categories WHERE id = ?
                UNION ALL
                SELECT c.id FROM categories c
                JOIN category_tree ct ON c.parent_id = ct.id
            )
            SELECT id FROM category_tree
        """, [category_id]).fetchall()

        return [row[0] for row in children]

    @staticmethod
    def _wrap_label(text: str, max_chars: int = 25) -> List[str]:
        """Wrap long text for chart labels."""
        if not text or len(text) <= max_chars:
            return [text] if text else ["Unknown"]

        words = text.split()
        lines = []
        current_line = ""

        for word in words:
            if len(current_line) + len(word) + 1 <= max_chars:
                current_line = f"{current_line} {word}".strip()
            else:
                if current_line:
                    lines.append(current_line)
                current_line = word

        if current_line:
            lines.append(current_line)

        if len(lines) > 2:
            lines = [lines[0], lines[1][:max_chars-3] + "..."]

        return lines

    # ─── Advanced Analytics Methods ──────────────────────────────────────────────

    async def get_product_performance(
        self,
        start_date: date,
        end_date: date,
        source_id: Optional[int] = None,
        brand: Optional[str] = None,
        sales_type: str = "retail"
    ) -> Dict[str, Any]:
        """Get product performance: top by revenue, category breakdown (from Gold layer)."""
        async with self.connection() as conn:
            params = [start_date, end_date]
            where_clauses = ["g.date BETWEEN ? AND ?"]

            if sales_type != "all":
                where_clauses.append("g.sales_type = ?")
                params.append(sales_type)

            if source_id:
                where_clauses.append("g.source_id = ?")
                params.append(source_id)

            if brand:
                where_clauses.append("LOWER(g.brand) = LOWER(?)")
                params.append(brand)

            where_sql = " AND ".join(where_clauses)

            # Top products by revenue
            top_results = conn.execute(f"""
                SELECT
                    g.product_name,
                    SUM(g.product_revenue) as revenue,
                    SUM(g.quantity_sold) as quantity
                FROM gold_daily_products g
                WHERE {where_sql}
                GROUP BY g.product_name
                ORDER BY revenue DESC
                LIMIT 10
            """, params).fetchall()

            top_by_revenue = {
                "labels": [row[0] or "Unknown" for row in top_results],
                "wrappedLabels": [self._wrap_label(row[0]) for row in top_results],
                "data": [round(float(row[1]), 2) for row in top_results],
                "quantities": [int(row[2]) for row in top_results],
                "backgroundColor": "#16A34A"
            }

            # Category breakdown (use parent_category_name, fall back to category_name)
            cat_results = conn.execute(f"""
                SELECT
                    COALESCE(g.parent_category_name, g.category_name, 'Other') as category_name,
                    SUM(g.product_revenue) as revenue,
                    SUM(g.quantity_sold) as quantity
                FROM gold_daily_products g
                WHERE {where_sql}
                GROUP BY COALESCE(g.parent_category_name, g.category_name, 'Other')
                ORDER BY revenue DESC
            """, params).fetchall()

            category_colors = ["#7C3AED", "#2563EB", "#16A34A", "#F59E0B", "#eb4200", "#EC4899", "#8B5CF6", "#06B6D4"]
            category_breakdown = {
                "labels": [row[0] for row in cat_results],
                "revenue": [round(float(row[1]), 2) for row in cat_results],
                "quantity": [int(row[2]) for row in cat_results],
                "backgroundColor": category_colors[:len(cat_results)]
            }

            total_revenue = sum(float(row[1]) for row in top_results) if top_results else 0
            total_quantity = sum(int(row[2]) for row in top_results) if top_results else 0

            return {
                "topByRevenue": top_by_revenue,
                "categoryBreakdown": category_breakdown,
                "metrics": {
                    "totalProducts": len(top_results),
                    "totalRevenue": round(total_revenue, 2),
                    "totalQuantity": total_quantity,
                    "avgProductRevenue": round(total_revenue / len(top_results), 2) if top_results else 0
                }
            }

    async def get_subcategory_breakdown(
        self,
        start_date: date,
        end_date: date,
        parent_category_name: str,
        source_id: Optional[int] = None,
        brand: Optional[str] = None,
        sales_type: str = "retail"
    ) -> Dict[str, Any]:
        """Get sales breakdown by subcategories for a given parent category."""
        async with self.connection() as conn:
            return_statuses = tuple(int(s) for s in OrderStatus.return_statuses())
            params = [start_date, end_date]
            where_clauses = [
                f"{_date_in_kyiv('o.ordered_at')} BETWEEN ? AND ?",
                f"o.status_id NOT IN {return_statuses}",
                "o.source_id IN (1, 2, 4)",  # Exclude Opencart (deprecated)
                self._build_sales_type_filter(sales_type, conn=conn)
            ]

            if source_id:
                where_clauses.append("o.source_id = ?")
                params.append(source_id)

            where_sql = " AND ".join(where_clauses)

            # Build brand filter
            brand_filter = ""
            brand_params = []
            if brand:
                brand_filter = "AND LOWER(p.brand) = LOWER(?)"
                brand_params.append(brand)

            # Get subcategories for the parent category
            subcategory_sql = f"""
                SELECT
                    c.name as subcategory_name,
                    SUM(op.price_sold * op.quantity) as revenue,
                    SUM(op.quantity) as quantity
                FROM orders o
                JOIN order_products op ON o.id = op.order_id
                LEFT JOIN products p ON op.product_id = p.id
                LEFT JOIN categories c ON p.category_id = c.id
                LEFT JOIN categories parent_c ON c.parent_id = parent_c.id
                WHERE {where_sql}
                    AND (parent_c.name = ? OR (c.name = ? AND c.parent_id IS NULL))
                    {brand_filter}
                GROUP BY c.name
                ORDER BY revenue DESC
            """
            # Build final params: base params + parent_category (twice) + brand
            final_params = params + [parent_category_name, parent_category_name] + brand_params

            results = conn.execute(subcategory_sql, final_params).fetchall()

            category_colors = ["#7C3AED", "#2563EB", "#16A34A", "#F59E0B", "#eb4200", "#EC4899", "#8B5CF6", "#06B6D4"]

            return {
                "parentCategory": parent_category_name,
                "labels": [row[0] for row in results],
                "revenue": [round(float(row[1]), 2) for row in results],
                "quantity": [row[2] for row in results],
                "backgroundColor": category_colors[:len(results)]
            }

    async def get_brand_analytics(
        self,
        start_date: date,
        end_date: date,
        source_id: Optional[int] = None,
        sales_type: str = "retail"
    ) -> Dict[str, Any]:
        """Get brand analytics: top brands by revenue and quantity (from Gold layer)."""
        async with self.connection() as conn:
            params = [start_date, end_date]
            where_clauses = ["g.date BETWEEN ? AND ?"]

            if sales_type != "all":
                where_clauses.append("g.sales_type = ?")
                params.append(sales_type)

            if source_id:
                where_clauses.append("g.source_id = ?")
                params.append(source_id)

            where_sql = " AND ".join(where_clauses)

            # Brand stats from gold_daily_products
            brand_results = conn.execute(f"""
                SELECT
                    COALESCE(g.brand, 'Unknown') as brand_name,
                    SUM(g.product_revenue) as revenue,
                    SUM(g.quantity_sold) as quantity,
                    SUM(g.order_count) as orders
                FROM gold_daily_products g
                WHERE {where_sql}
                GROUP BY COALESCE(g.brand, 'Unknown')
                ORDER BY revenue DESC
            """, params).fetchall()

            brand_colors = ["#7C3AED", "#2563EB", "#16A34A", "#F59E0B", "#eb4200", "#EC4899", "#8B5CF6", "#06B6D4", "#14B8A6", "#EF4444"]

            # Top 10 by revenue
            top_by_revenue = brand_results[:10]
            top_brands_revenue = {
                "labels": [row[0] for row in top_by_revenue],
                "data": [round(float(row[1]), 2) for row in top_by_revenue],
                "quantities": [int(row[2]) for row in top_by_revenue],
                "orders": [int(row[3]) for row in top_by_revenue],
                "backgroundColor": brand_colors[:len(top_by_revenue)]
            }

            # Top 10 by quantity
            sorted_by_qty = sorted(brand_results, key=lambda x: x[2], reverse=True)[:10]
            top_brands_quantity = {
                "labels": [row[0] for row in sorted_by_qty],
                "data": [int(row[2]) for row in sorted_by_qty],
                "revenue": [round(float(row[1]), 2) for row in sorted_by_qty],
                "backgroundColor": brand_colors[:len(sorted_by_qty)]
            }

            total_revenue = sum(float(row[1]) for row in brand_results)
            total_quantity = sum(int(row[2]) for row in brand_results)
            unique_brands = len([b for b in brand_results if b[0] != "Unknown"])

            top_brand = brand_results[0][0] if brand_results else "N/A"
            top_brand_revenue = float(brand_results[0][1]) if brand_results else 0
            top_brand_share = (top_brand_revenue / total_revenue * 100) if total_revenue > 0 else 0

            return {
                "topByRevenue": top_brands_revenue,
                "topByQuantity": top_brands_quantity,
                "metrics": {
                    "totalBrands": unique_brands,
                    "topBrand": top_brand,
                    "topBrandShare": round(top_brand_share, 1),
                    "totalRevenue": round(total_revenue, 2),
                    "totalQuantity": total_quantity,
                    "avgBrandRevenue": round(total_revenue / unique_brands, 2) if unique_brands > 0 else 0
                }
            }

    # ─── Expense Methods ─────────────────────────────────────────────────────
