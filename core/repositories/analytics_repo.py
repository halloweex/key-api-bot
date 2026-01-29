"""
Analytics repository for revenue, sales, and customer insights.

Handles all analytical queries for the dashboard.
"""
from datetime import date, timedelta
from typing import Optional, List, Dict, Any

from core.repositories.base import BaseRepository, _date_in_kyiv, B2B_MANAGER_ID, RETAIL_MANAGER_IDS
from core.models import OrderStatus
from core.observability import get_logger

logger = get_logger(__name__)


class AnalyticsRepository(BaseRepository):
    """Repository for analytics queries - revenue, sales, customers."""

    async def get_summary_stats(
        self,
        start_date: date,
        end_date: date,
        source_id: Optional[int] = None,
        category_id: Optional[int] = None,
        brand: Optional[str] = None,
        sales_type: str = "retail"
    ) -> Dict[str, Any]:
        """Get summary statistics for a date range."""
        async with self.connection() as conn:
            params = [start_date, end_date]
            where_clauses = [f"{_date_in_kyiv('o.ordered_at')} BETWEEN ? AND ?"]
            where_clauses.append(self._build_sales_type_filter(sales_type))

            return_statuses = tuple(int(s) for s in OrderStatus.return_statuses())

            if source_id:
                where_clauses.append("o.source_id = ?")
                params.append(source_id)

            joins = ""
            if category_id or brand:
                joins = "JOIN order_products op ON o.id = op.order_id JOIN products p ON op.product_id = p.id"
                if category_id:
                    cat_ids = await self._get_category_with_children(conn, category_id)
                    where_clauses.append(f"p.category_id IN ({','.join('?' * len(cat_ids))})")
                    params.extend(cat_ids)
                if brand:
                    where_clauses.append("LOWER(p.brand) = LOWER(?)")
                    params.append(brand)

            where_sql = " AND ".join(where_clauses)

            stats_sql = f"""
                SELECT
                    COUNT(DISTINCT CASE WHEN o.status_id NOT IN {return_statuses} THEN o.id END) as total_orders,
                    COALESCE(SUM(CASE WHEN o.status_id NOT IN {return_statuses} THEN o.grand_total END), 0) as total_revenue,
                    COUNT(DISTINCT CASE WHEN o.status_id IN {return_statuses} THEN o.id END) as total_returns,
                    COALESCE(SUM(CASE WHEN o.status_id IN {return_statuses} THEN o.grand_total END), 0) as returns_revenue
                FROM orders o
                {joins}
                WHERE {where_sql}
            """

            result = conn.execute(stats_sql, params).fetchone()

            total_orders = result[0] or 0
            total_revenue = float(result[1] or 0)
            total_returns = result[2] or 0
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

    async def get_revenue_trend(
        self,
        start_date: date,
        end_date: date,
        source_id: Optional[int] = None,
        category_id: Optional[int] = None,
        brand: Optional[str] = None,
        include_comparison: bool = True,
        sales_type: str = "retail"
    ) -> Dict[str, Any]:
        """Get daily revenue trend for chart."""
        async with self.connection() as conn:
            return_statuses = tuple(int(s) for s in OrderStatus.return_statuses())

            params = [start_date, end_date]
            where_clauses = [
                f"{_date_in_kyiv('o.ordered_at')} BETWEEN ? AND ?",
                f"o.status_id NOT IN {return_statuses}",
                self._build_sales_type_filter(sales_type)
            ]

            joins = ""
            cat_ids = []
            if source_id:
                where_clauses.append("o.source_id = ?")
                params.append(source_id)

            if category_id or brand:
                joins = "JOIN order_products op ON o.id = op.order_id JOIN products p ON op.product_id = p.id"
                if category_id:
                    cat_ids = await self._get_category_with_children(conn, category_id)
                    where_clauses.append(f"p.category_id IN ({','.join('?' * len(cat_ids))})")
                    params.extend(cat_ids)
                if brand:
                    where_clauses.append("LOWER(p.brand) = LOWER(?)")
                    params.append(brand)

            where_sql = " AND ".join(where_clauses)

            sql = f"""
                SELECT
                    {_date_in_kyiv('o.ordered_at')} as day,
                    SUM(o.grand_total) as revenue,
                    COUNT(DISTINCT o.id) as order_count
                FROM orders o
                {joins}
                WHERE {where_sql}
                GROUP BY {_date_in_kyiv('o.ordered_at')}
                ORDER BY day
            """

            results = conn.execute(sql, params).fetchall()
            daily_data = {row[0]: (float(row[1]), int(row[2])) for row in results}

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

            comparison = None
            if include_comparison:
                period_days = (end_date - start_date).days + 1
                prev_end = start_date - timedelta(days=1)
                prev_start = prev_end - timedelta(days=period_days - 1)

                prev_params = [prev_start, prev_end]
                prev_where = [
                    f"{_date_in_kyiv('o.ordered_at')} BETWEEN ? AND ?",
                    f"o.status_id NOT IN {return_statuses}",
                    self._build_sales_type_filter(sales_type)
                ]

                if source_id:
                    prev_where.append("o.source_id = ?")
                    prev_params.append(source_id)

                if category_id or brand:
                    if category_id:
                        prev_where.append(f"p.category_id IN ({','.join('?' * len(cat_ids))})")
                        prev_params.extend(cat_ids)
                    if brand:
                        prev_where.append("LOWER(p.brand) = LOWER(?)")
                        prev_params.append(brand)

                prev_sql = f"""
                    SELECT {_date_in_kyiv('o.ordered_at')} as day, SUM(o.grand_total) as revenue
                    FROM orders o {joins}
                    WHERE {" AND ".join(prev_where)}
                    GROUP BY {_date_in_kyiv('o.ordered_at')}
                    ORDER BY day
                """

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

                comparison = {
                    "labels": labels,
                    "revenue": prev_data,
                    "orders": []
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
        """Get sales breakdown by source."""
        async with self.connection() as conn:
            return_statuses = tuple(int(s) for s in OrderStatus.return_statuses())
            params = [start_date, end_date]
            where_clauses = [
                f"{_date_in_kyiv('o.ordered_at')} BETWEEN ? AND ?",
                f"o.status_id NOT IN {return_statuses}",
                self._build_sales_type_filter(sales_type)
            ]

            joins = ""
            if category_id or brand:
                joins = "JOIN order_products op ON o.id = op.order_id JOIN products p ON op.product_id = p.id"
                if category_id:
                    cat_ids = await self._get_category_with_children(conn, category_id)
                    where_clauses.append(f"p.category_id IN ({','.join('?' * len(cat_ids))})")
                    params.extend(cat_ids)
                if brand:
                    where_clauses.append("LOWER(p.brand) = LOWER(?)")
                    params.append(brand)

            sql = f"""
                SELECT
                    o.source_id,
                    COUNT(DISTINCT o.id) as orders,
                    SUM(o.grand_total) as revenue
                FROM orders o
                {joins}
                WHERE {" AND ".join(where_clauses)}
                GROUP BY o.source_id
                ORDER BY revenue DESC
            """

            results = conn.execute(sql, params).fetchall()

            source_names = {1: "Instagram", 2: "Telegram", 4: "Shopify"}
            source_colors = {1: "#7C3AED", 2: "#2563EB", 4: "#eb4200"}

            labels = []
            orders = []
            revenue = []
            colors = []

            for row in results:
                sid = row[0]
                if sid in source_names:
                    labels.append(source_names[sid])
                    orders.append(row[1])
                    revenue.append(round(float(row[2]), 2))
                    colors.append(source_colors.get(sid, "#999999"))

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
        """Get top products by quantity."""
        async with self.connection() as conn:
            return_statuses = tuple(int(s) for s in OrderStatus.return_statuses())
            params = [start_date, end_date]
            where_clauses = [
                f"{_date_in_kyiv('o.ordered_at')} BETWEEN ? AND ?",
                f"o.status_id NOT IN {return_statuses}",
                self._build_sales_type_filter(sales_type)
            ]

            if source_id:
                where_clauses.append("o.source_id = ?")
                params.append(source_id)

            joins = "JOIN order_products op ON o.id = op.order_id LEFT JOIN products p ON op.product_id = p.id"

            if category_id:
                cat_ids = await self._get_category_with_children(conn, category_id)
                where_clauses.append(f"p.category_id IN ({','.join('?' * len(cat_ids))})")
                params.extend(cat_ids)

            if brand:
                where_clauses.append("LOWER(p.brand) = LOWER(?)")
                params.append(brand)

            params.append(limit)

            sql = f"""
                SELECT
                    op.name,
                    SUM(op.quantity) as total_qty
                FROM orders o
                {joins}
                WHERE {" AND ".join(where_clauses)}
                GROUP BY op.name
                ORDER BY total_qty DESC
                LIMIT ?
            """

            results = conn.execute(sql, params).fetchall()

            raw_labels = [row[0] or "Unknown" for row in results]
            data = [row[1] for row in results]
            total = sum(data) if data else 1
            percentages = [round(d / total * 100, 1) for d in data]

            return {
                "labels": raw_labels,
                "data": data,
                "percentages": percentages,
                "backgroundColor": "#2563EB"
            }

    async def get_brand_analytics(
        self,
        start_date: date,
        end_date: date,
        source_id: Optional[int] = None,
        limit: int = 10,
        sales_type: str = "retail"
    ) -> Dict[str, Any]:
        """Get brand performance analytics."""
        async with self.connection() as conn:
            return_statuses = tuple(int(s) for s in OrderStatus.return_statuses())
            params = [start_date, end_date]
            where_clauses = [
                f"{_date_in_kyiv('o.ordered_at')} BETWEEN ? AND ?",
                f"o.status_id NOT IN {return_statuses}",
                self._build_sales_type_filter(sales_type)
            ]

            if source_id:
                where_clauses.append("o.source_id = ?")
                params.append(source_id)

            params.append(limit)

            sql = f"""
                SELECT
                    COALESCE(p.brand, 'Unknown') as brand,
                    SUM(op.price_sold * op.quantity) as revenue,
                    SUM(op.quantity) as quantity,
                    COUNT(DISTINCT o.id) as orders
                FROM orders o
                JOIN order_products op ON o.id = op.order_id
                LEFT JOIN products p ON op.product_id = p.id
                WHERE {" AND ".join(where_clauses)}
                GROUP BY COALESCE(p.brand, 'Unknown')
                ORDER BY revenue DESC
                LIMIT ?
            """

            results = conn.execute(sql, params).fetchall()

            return {
                "brands": [{
                    "name": row[0],
                    "revenue": round(float(row[1] or 0), 2),
                    "quantity": row[2],
                    "orders": row[3]
                } for row in results]
            }

    async def get_stats(self) -> Dict[str, Any]:
        """Get database statistics."""
        async with self.connection() as conn:
            orders = conn.execute("SELECT COUNT(*) FROM orders").fetchone()[0]
            products = conn.execute("SELECT COUNT(*) FROM products").fetchone()[0]
            categories = conn.execute("SELECT COUNT(*) FROM categories").fetchone()[0]
            managers = conn.execute("SELECT COUNT(*) FROM managers").fetchone()[0]

            # Get database file size
            import os
            db_size = os.path.getsize(self.db_path) if self.db_path.exists() else 0
            db_size_mb = round(db_size / (1024 * 1024), 2)

            return {
                "orders": orders,
                "products": products,
                "categories": categories,
                "managers": managers,
                "db_size_mb": db_size_mb
            }

    async def _get_category_with_children(
        self, conn, category_id: int
    ) -> List[int]:
        """Get category ID and all descendant IDs."""
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
