"""
DuckDB analytics store for KeyCRM dashboard.

Provides persistent storage for orders, products, and pre-aggregated statistics.
Uses incremental sync to minimize API calls and enable fast historical queries.
"""
import asyncio
import logging
from contextlib import asynccontextmanager
from datetime import datetime, date, timedelta
from pathlib import Path
from typing import Optional, List, Dict, Any, Tuple
from zoneinfo import ZoneInfo

import duckdb

from core.models import Order, SourceId, OrderStatus
from bot.config import DEFAULT_TIMEZONE, TELEGRAM_MANAGER_IDS

logger = logging.getLogger(__name__)

# Database configuration
DB_DIR = Path(__file__).parent.parent / "data"
DB_PATH = DB_DIR / "analytics.duckdb"
DEFAULT_TZ = ZoneInfo(DEFAULT_TIMEZONE)

# B2B (wholesale) manager ID - Olga D
B2B_MANAGER_ID = 15

# Retail manager IDs (exclude B2B/wholesale)
RETAIL_MANAGER_IDS = [22, 4, 16]


class DuckDBStore:
    """
    Async-compatible DuckDB store for analytics data.

    Features:
    - Persistent storage (survives restarts)
    - Incremental sync from KeyCRM API
    - Pre-aggregated daily statistics
    - Fast analytical queries
    """

    def __init__(self, db_path: Path = DB_PATH):
        self.db_path = db_path
        self._connection: Optional[duckdb.DuckDBPyConnection] = None
        self._lock = asyncio.Lock()

    async def connect(self) -> None:
        """Initialize database connection and schema."""
        DB_DIR.mkdir(parents=True, exist_ok=True)

        async with self._lock:
            if self._connection is None:
                self._connection = duckdb.connect(str(self.db_path))
                await self._init_schema()
                logger.info(f"DuckDB connected: {self.db_path}")

    async def close(self) -> None:
        """Close database connection."""
        async with self._lock:
            if self._connection:
                self._connection.close()
                self._connection = None
                logger.info("DuckDB connection closed")

    @asynccontextmanager
    async def connection(self):
        """Get database connection with automatic reconnection."""
        if self._connection is None:
            await self.connect()
        yield self._connection

    async def _init_schema(self) -> None:
        """Create database schema if not exists."""
        schema_sql = """
        -- Orders table
        CREATE TABLE IF NOT EXISTS orders (
            id INTEGER PRIMARY KEY,
            source_id INTEGER NOT NULL,
            status_id INTEGER NOT NULL,
            grand_total DECIMAL(12, 2) NOT NULL,
            ordered_at TIMESTAMP WITH TIME ZONE,
            created_at TIMESTAMP WITH TIME ZONE,
            buyer_id INTEGER,
            manager_id INTEGER,
            synced_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
        );

        -- Order products (line items)
        CREATE TABLE IF NOT EXISTS order_products (
            id INTEGER PRIMARY KEY,
            order_id INTEGER NOT NULL,
            product_id INTEGER,
            name VARCHAR NOT NULL,
            quantity INTEGER NOT NULL,
            price_sold DECIMAL(12, 2) NOT NULL,
            FOREIGN KEY (order_id) REFERENCES orders(id)
        );

        -- Products catalog
        CREATE TABLE IF NOT EXISTS products (
            id INTEGER PRIMARY KEY,
            name VARCHAR NOT NULL,
            category_id INTEGER,
            brand VARCHAR,
            sku VARCHAR,
            price DECIMAL(12, 2),
            synced_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
        );

        -- Categories
        CREATE TABLE IF NOT EXISTS categories (
            id INTEGER PRIMARY KEY,
            name VARCHAR NOT NULL,
            parent_id INTEGER,
            synced_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
        );

        -- Pre-aggregated daily statistics (materialized for speed)
        CREATE TABLE IF NOT EXISTS daily_stats (
            date DATE NOT NULL,
            source_id INTEGER NOT NULL,
            orders_count INTEGER DEFAULT 0,
            revenue DECIMAL(12, 2) DEFAULT 0,
            returns_count INTEGER DEFAULT 0,
            returns_revenue DECIMAL(12, 2) DEFAULT 0,
            unique_customers INTEGER DEFAULT 0,
            updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (date, source_id)
        );

        -- Expense types (delivery, taxes, advertising, etc.)
        CREATE TABLE IF NOT EXISTS expense_types (
            id INTEGER PRIMARY KEY,
            name VARCHAR NOT NULL,
            alias VARCHAR,
            is_active BOOLEAN DEFAULT TRUE,
            synced_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
        );

        -- Expenses (linked to orders)
        CREATE TABLE IF NOT EXISTS expenses (
            id INTEGER PRIMARY KEY,
            order_id INTEGER NOT NULL,
            expense_type_id INTEGER,
            amount DECIMAL(12, 2) NOT NULL,
            description VARCHAR,
            status VARCHAR,
            payment_date TIMESTAMP WITH TIME ZONE,
            created_at TIMESTAMP WITH TIME ZONE,
            synced_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (order_id) REFERENCES orders(id),
            FOREIGN KEY (expense_type_id) REFERENCES expense_types(id)
        );

        -- Sync metadata
        CREATE TABLE IF NOT EXISTS sync_metadata (
            key VARCHAR PRIMARY KEY,
            value VARCHAR,
            updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
        );

        -- Indexes for common queries
        CREATE INDEX IF NOT EXISTS idx_orders_ordered_at ON orders(ordered_at);
        CREATE INDEX IF NOT EXISTS idx_orders_source_id ON orders(source_id);
        CREATE INDEX IF NOT EXISTS idx_orders_status_id ON orders(status_id);
        CREATE INDEX IF NOT EXISTS idx_orders_manager_id ON orders(manager_id);
        CREATE INDEX IF NOT EXISTS idx_order_products_order_id ON order_products(order_id);
        CREATE INDEX IF NOT EXISTS idx_order_products_product_id ON order_products(product_id);
        CREATE INDEX IF NOT EXISTS idx_products_category_id ON products(category_id);
        CREATE INDEX IF NOT EXISTS idx_products_brand ON products(brand);
        CREATE INDEX IF NOT EXISTS idx_daily_stats_date ON daily_stats(date);
        CREATE INDEX IF NOT EXISTS idx_expenses_order_id ON expenses(order_id);
        CREATE INDEX IF NOT EXISTS idx_expenses_expense_type_id ON expenses(expense_type_id);
        CREATE INDEX IF NOT EXISTS idx_expenses_payment_date ON expenses(payment_date);
        """
        self._connection.execute(schema_sql)
        logger.info("DuckDB schema initialized")

    def _build_sales_type_filter(self, sales_type: str, table_alias: str = "o") -> str:
        """Build SQL clause for retail/b2b/all filtering based on manager_id.

        Args:
            sales_type: 'retail', 'b2b', or 'all'
            table_alias: Table alias for orders table (default 'o')

        Returns:
            SQL WHERE clause fragment
        """
        if sales_type == "all":
            # All = no manager filter
            return "1=1"
        elif sales_type == "b2b":
            # B2B = only Olga D (manager_id = 15)
            return f"{table_alias}.manager_id = {B2B_MANAGER_ID}"
        else:
            # Retail = specific managers (22, 4, 16) + Shopify orders (NULL manager)
            manager_list = ",".join(str(m) for m in RETAIL_MANAGER_IDS)
            return f"({table_alias}.manager_id IS NULL OR {table_alias}.manager_id IN ({manager_list}))"

    # ─── Sync Methods ─────────────────────────────────────────────────────────

    async def get_last_sync_time(self, key: str = "orders") -> Optional[datetime]:
        """Get last sync timestamp for incremental updates."""
        async with self.connection() as conn:
            result = conn.execute(
                "SELECT value FROM sync_metadata WHERE key = ?",
                [f"last_sync_{key}"]
            ).fetchone()
            if result and result[0]:
                return datetime.fromisoformat(result[0])
            return None

    async def set_last_sync_time(self, key: str = "orders", timestamp: datetime = None) -> None:
        """Update last sync timestamp."""
        timestamp = timestamp or datetime.now(DEFAULT_TZ)
        async with self.connection() as conn:
            conn.execute("""
                INSERT OR REPLACE INTO sync_metadata (key, value, updated_at)
                VALUES (?, ?, CURRENT_TIMESTAMP)
            """, [f"last_sync_{key}", timestamp.isoformat()])

    async def upsert_orders(self, orders: List[Dict[str, Any]]) -> int:
        """
        Insert or update orders from API response.

        Args:
            orders: List of order dicts from KeyCRM API

        Returns:
            Number of orders upserted
        """
        if not orders:
            return 0

        async with self.connection() as conn:
            count = 0
            for order_data in orders:
                order = Order.from_api(order_data)

                # Skip invalid orders
                if not order.ordered_at:
                    continue

                # Upsert order
                conn.execute("""
                    INSERT OR REPLACE INTO orders
                    (id, source_id, status_id, grand_total, ordered_at, created_at, buyer_id, manager_id, synced_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                """, [
                    order.id,
                    order.source_id,
                    order.status_id,
                    order.grand_total,
                    order.ordered_at.isoformat() if order.ordered_at else None,
                    order.created_at.isoformat() if order.created_at else None,
                    order.buyer.id if order.buyer else None,
                    order.manager.id if order.manager else None
                ])

                # Delete existing products for this order
                conn.execute("DELETE FROM order_products WHERE order_id = ?", [order.id])

                # Insert order products
                for i, prod in enumerate(order.products):
                    conn.execute("""
                        INSERT INTO order_products (id, order_id, product_id, name, quantity, price_sold)
                        VALUES (?, ?, ?, ?, ?, ?)
                    """, [
                        order.id * 10000 + i,  # Generate unique ID
                        order.id,
                        prod.product_id,
                        prod.name,
                        prod.quantity,
                        prod.price_sold
                    ])

                count += 1

            logger.info(f"Upserted {count} orders to DuckDB")
            return count

    async def upsert_products(self, products: List[Dict[str, Any]]) -> int:
        """Insert or update products from API response."""
        if not products:
            return 0

        async with self.connection() as conn:
            count = 0
            for prod_data in products:
                # Extract brand from custom_fields
                brand = None
                for cf in prod_data.get("custom_fields", []):
                    if cf.get("uuid") == "CT_1001" or cf.get("name") == "Brand":
                        values = cf.get("value", [])
                        if values and isinstance(values, list):
                            brand = values[0]
                        break

                conn.execute("""
                    INSERT OR REPLACE INTO products (id, name, category_id, brand, sku, price, synced_at)
                    VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                """, [
                    prod_data.get("id"),
                    prod_data.get("name", "Unknown"),
                    prod_data.get("category_id"),
                    brand,
                    prod_data.get("sku"),
                    prod_data.get("min_price") or prod_data.get("price")
                ])
                count += 1

            logger.info(f"Upserted {count} products to DuckDB")
            return count

    async def upsert_categories(self, categories: List[Dict[str, Any]]) -> int:
        """Insert or update categories from API response."""
        if not categories:
            return 0

        async with self.connection() as conn:
            count = 0
            for cat_data in categories:
                conn.execute("""
                    INSERT OR REPLACE INTO categories (id, name, parent_id, synced_at)
                    VALUES (?, ?, ?, CURRENT_TIMESTAMP)
                """, [
                    cat_data.get("id"),
                    cat_data.get("name", "Unknown"),
                    cat_data.get("parent_id")
                ])
                count += 1

            logger.info(f"Upserted {count} categories to DuckDB")
            return count

    async def upsert_expense_types(self, expense_types: List[Dict[str, Any]]) -> int:
        """Insert or update expense types from API response."""
        if not expense_types:
            return 0

        async with self.connection() as conn:
            count = 0
            for et in expense_types:
                name = et.get("name", "Unknown")
                alias = et.get("alias")

                # Clean up localization keys (e.g., "dictionaries.expense_types.delivery" -> "Delivery")
                if name.startswith("dictionaries.expense_types."):
                    # Use alias as display name, formatted nicely
                    if alias:
                        name = alias.replace("_", " ").title()
                    else:
                        name = name.replace("dictionaries.expense_types.", "").replace("_", " ").title()

                conn.execute("""
                    INSERT OR REPLACE INTO expense_types (id, name, alias, is_active, synced_at)
                    VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
                """, [
                    et.get("id"),
                    name,
                    alias,
                    et.get("is_active", True)
                ])
                count += 1

            logger.info(f"Upserted {count} expense types to DuckDB")
            return count

    async def upsert_expenses(self, order_id: int, expenses: List[Dict[str, Any]]) -> int:
        """Insert or update expenses for an order."""
        if not expenses:
            return 0

        async with self.connection() as conn:
            count = 0
            for exp in expenses:
                conn.execute("""
                    INSERT OR REPLACE INTO expenses
                    (id, order_id, expense_type_id, amount, description, status, payment_date, created_at, synced_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                """, [
                    exp.get("id"),
                    order_id,
                    exp.get("expense_type_id"),
                    exp.get("amount", 0),
                    exp.get("description"),
                    exp.get("status"),
                    exp.get("payment_date"),
                    exp.get("created_at")
                ])
                count += 1

            return count

    # ─── Query Methods ────────────────────────────────────────────────────────

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
            # Build query with filters
            params = [start_date, end_date]

            # Base query for valid orders
            where_clauses = ["DATE(o.ordered_at) BETWEEN ? AND ?"]

            # Add sales type filter (retail/b2b)
            where_clauses.append(self._build_sales_type_filter(sales_type))

            # Exclude returns for main stats (convert enums to int values)
            return_statuses = tuple(int(s) for s in OrderStatus.return_statuses())

            if source_id:
                where_clauses.append("o.source_id = ?")
                params.append(source_id)

            # Category/brand filtering requires join with order_products
            joins = ""
            if category_id or brand:
                joins = "JOIN order_products op ON o.id = op.order_id JOIN products p ON op.product_id = p.id"
                if category_id:
                    # Get category and children
                    cat_ids = await self._get_category_with_children(conn, category_id)
                    where_clauses.append(f"p.category_id IN ({','.join('?' * len(cat_ids))})")
                    params.extend(cat_ids)
                if brand:
                    where_clauses.append("LOWER(p.brand) = LOWER(?)")
                    params.append(brand)

            where_sql = " AND ".join(where_clauses)

            # Main stats query
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

            # Build filters
            params = [start_date, end_date]
            where_clauses = [
                "DATE(o.ordered_at) BETWEEN ? AND ?",
                f"o.status_id NOT IN {return_statuses}",
                self._build_sales_type_filter(sales_type)
            ]

            joins = ""
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

            # Query daily revenue
            sql = f"""
                SELECT
                    DATE(o.ordered_at) as day,
                    SUM(o.grand_total) as revenue
                FROM orders o
                {joins}
                WHERE {where_sql}
                GROUP BY DATE(o.ordered_at)
                ORDER BY day
            """

            results = conn.execute(sql, params).fetchall()
            daily_data = {row[0]: float(row[1]) for row in results}

            # Build labels and data
            labels = []
            data = []
            current = start_date
            while current <= end_date:
                labels.append(current.strftime("%d.%m"))
                data.append(round(daily_data.get(current, 0), 2))
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
                prev_end = start_date - timedelta(days=1)
                prev_start = prev_end - timedelta(days=period_days - 1)

                prev_params = [prev_start, prev_end]
                prev_where = [
                    "DATE(o.ordered_at) BETWEEN ? AND ?",
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
                    SELECT DATE(o.ordered_at) as day, SUM(o.grand_total) as revenue
                    FROM orders o {joins}
                    WHERE {" AND ".join(prev_where)}
                    GROUP BY DATE(o.ordered_at)
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

            return {"labels": labels, "datasets": datasets}

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
                "DATE(o.ordered_at) BETWEEN ? AND ?",
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
                if sid in source_names:  # Only include active sources
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
                "DATE(o.ordered_at) BETWEEN ? AND ?",
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

            labels = [self._wrap_label(row[0]) for row in results]
            data = [row[1] for row in results]
            total = sum(data) if data else 1
            percentages = [round(d / total * 100, 1) for d in data]

            return {
                "labels": labels,
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

    async def get_customer_insights(
        self,
        start_date: date,
        end_date: date,
        source_id: Optional[int] = None,
        brand: Optional[str] = None,
        sales_type: str = "retail"
    ) -> Dict[str, Any]:
        """Get customer insights: new vs returning, AOV trend."""
        async with self.connection() as conn:
            return_statuses = tuple(int(s) for s in OrderStatus.return_statuses())
            params = [start_date, end_date]
            where_clauses = [
                "DATE(o.ordered_at) BETWEEN ? AND ?",
                f"o.status_id NOT IN {return_statuses}",
                self._build_sales_type_filter(sales_type)
            ]

            joins = ""
            if source_id:
                where_clauses.append("o.source_id = ?")
                params.append(source_id)

            if brand:
                joins = "JOIN order_products op ON o.id = op.order_id JOIN products p ON op.product_id = p.id"
                where_clauses.append("LOWER(p.brand) = LOWER(?)")
                params.append(brand)

            where_sql = " AND ".join(where_clauses)

            # New vs returning customers (based on buyer creation date)
            customer_sql = f"""
                SELECT
                    COUNT(DISTINCT CASE WHEN DATE(o.created_at) >= ? THEN o.buyer_id END) as new_customers,
                    COUNT(DISTINCT CASE WHEN DATE(o.created_at) < ? THEN o.buyer_id END) as returning_customers,
                    COUNT(DISTINCT o.id) as total_orders
                FROM orders o
                {joins}
                WHERE {where_sql} AND o.buyer_id IS NOT NULL
            """
            customer_params = [start_date] + params + [start_date]
            # Simplified: use order created_at as proxy for customer newness
            customer_result = conn.execute(f"""
                SELECT
                    COUNT(DISTINCT o.buyer_id) as total_customers,
                    COUNT(DISTINCT o.id) as total_orders,
                    SUM(o.grand_total) as total_revenue
                FROM orders o
                {joins}
                WHERE {where_sql} AND o.buyer_id IS NOT NULL
            """, params).fetchone()

            total_customers = customer_result[0] or 0
            total_orders = customer_result[1] or 0
            total_revenue = float(customer_result[2] or 0)

            # Estimate new vs returning (buyers whose first order in DB is in this period)
            new_customers_result = conn.execute(f"""
                WITH first_orders AS (
                    SELECT buyer_id, MIN(DATE(ordered_at)) as first_order_date
                    FROM orders
                    WHERE buyer_id IS NOT NULL
                    GROUP BY buyer_id
                )
                SELECT COUNT(DISTINCT o.buyer_id)
                FROM orders o
                {joins}
                JOIN first_orders fo ON o.buyer_id = fo.buyer_id
                WHERE {where_sql} AND fo.first_order_date >= ?
            """, params + [start_date]).fetchone()
            new_customers = new_customers_result[0] or 0
            returning_customers = total_customers - new_customers

            # AOV trend by day
            aov_sql = f"""
                SELECT
                    DATE(o.ordered_at) as day,
                    AVG(o.grand_total) as avg_order_value,
                    COUNT(DISTINCT o.id) as orders
                FROM orders o
                {joins}
                WHERE {where_sql}
                GROUP BY DATE(o.ordered_at)
                ORDER BY day
            """
            aov_results = conn.execute(aov_sql, params).fetchall()
            aov_by_day = {row[0]: {"aov": float(row[1]), "orders": row[2]} for row in aov_results}

            # Build AOV trend data
            labels = []
            aov_data = []
            current = start_date
            while current <= end_date:
                labels.append(current.strftime("%d.%m"))
                day_data = aov_by_day.get(current, {"aov": 0, "orders": 0})
                aov_data.append(round(day_data["aov"], 2))
                current += timedelta(days=1)

            overall_aov = total_revenue / total_orders if total_orders > 0 else 0

            return {
                "newVsReturning": {
                    "labels": ["New Customers", "Returning Customers"],
                    "data": [new_customers, returning_customers],
                    "backgroundColor": ["#2563EB", "#16A34A"]
                },
                "aovTrend": {
                    "labels": labels,
                    "datasets": [{
                        "label": "AOV (UAH)",
                        "data": aov_data,
                        "borderColor": "#F59E0B",
                        "backgroundColor": "rgba(245, 158, 11, 0.1)",
                        "fill": True,
                        "tension": 0.3
                    }]
                },
                "metrics": {
                    "totalCustomers": total_customers,
                    "newCustomers": new_customers,
                    "returningCustomers": returning_customers,
                    "totalOrders": total_orders,
                    "repeatRate": round((returning_customers / total_customers * 100) if total_customers > 0 else 0, 1),
                    "averageOrderValue": round(overall_aov, 2)
                }
            }

    async def get_product_performance(
        self,
        start_date: date,
        end_date: date,
        source_id: Optional[int] = None,
        brand: Optional[str] = None,
        sales_type: str = "retail"
    ) -> Dict[str, Any]:
        """Get product performance: top by revenue, category breakdown."""
        async with self.connection() as conn:
            return_statuses = tuple(int(s) for s in OrderStatus.return_statuses())
            params = [start_date, end_date]
            where_clauses = [
                "DATE(o.ordered_at) BETWEEN ? AND ?",
                f"o.status_id NOT IN {return_statuses}",
                self._build_sales_type_filter(sales_type)
            ]

            if source_id:
                where_clauses.append("o.source_id = ?")
                params.append(source_id)

            brand_filter = ""
            if brand:
                brand_filter = "AND LOWER(p.brand) = LOWER(?)"
                params.append(brand)

            where_sql = " AND ".join(where_clauses)

            # Top products by revenue
            top_revenue_sql = f"""
                SELECT
                    op.name,
                    SUM(op.price_sold * op.quantity) as revenue,
                    SUM(op.quantity) as quantity
                FROM orders o
                JOIN order_products op ON o.id = op.order_id
                LEFT JOIN products p ON op.product_id = p.id
                WHERE {where_sql} {brand_filter}
                GROUP BY op.name
                ORDER BY revenue DESC
                LIMIT 10
            """
            top_results = conn.execute(top_revenue_sql, params).fetchall()

            top_by_revenue = {
                "labels": [self._wrap_label(row[0]) for row in top_results],
                "data": [round(float(row[1]), 2) for row in top_results],
                "quantities": [row[2] for row in top_results],
                "backgroundColor": "#16A34A"
            }

            # Category breakdown
            cat_params = [start_date, end_date]
            if source_id:
                cat_params.append(source_id)
            if brand:
                cat_params.append(brand)

            # Use parent category (root) for grouping, fall back to direct category if no parent
            category_sql = f"""
                SELECT
                    COALESCE(parent_c.name, c.name, 'Other') as category_name,
                    SUM(op.price_sold * op.quantity) as revenue,
                    SUM(op.quantity) as quantity
                FROM orders o
                JOIN order_products op ON o.id = op.order_id
                LEFT JOIN products p ON op.product_id = p.id
                LEFT JOIN categories c ON p.category_id = c.id
                LEFT JOIN categories parent_c ON c.parent_id = parent_c.id
                WHERE {where_sql} {brand_filter}
                GROUP BY COALESCE(parent_c.name, c.name, 'Other')
                ORDER BY revenue DESC
            """
            cat_results = conn.execute(category_sql, params).fetchall()

            category_colors = ["#7C3AED", "#2563EB", "#16A34A", "#F59E0B", "#eb4200", "#EC4899", "#8B5CF6", "#06B6D4"]
            category_breakdown = {
                "labels": [row[0] for row in cat_results],
                "revenue": [round(float(row[1]), 2) for row in cat_results],
                "quantity": [row[2] for row in cat_results],
                "backgroundColor": category_colors[:len(cat_results)]
            }

            total_revenue = sum(float(row[1]) for row in top_results) if top_results else 0
            total_quantity = sum(row[2] for row in top_results) if top_results else 0

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
                "DATE(o.ordered_at) BETWEEN ? AND ?",
                f"o.status_id NOT IN {return_statuses}",
                self._build_sales_type_filter(sales_type)
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
        """Get brand analytics: top brands by revenue and quantity."""
        async with self.connection() as conn:
            return_statuses = tuple(int(s) for s in OrderStatus.return_statuses())
            params = [start_date, end_date]
            where_clauses = [
                "DATE(o.ordered_at) BETWEEN ? AND ?",
                f"o.status_id NOT IN {return_statuses}",
                self._build_sales_type_filter(sales_type)
            ]

            if source_id:
                where_clauses.append("o.source_id = ?")
                params.append(source_id)

            where_sql = " AND ".join(where_clauses)

            # Brand stats
            brand_sql = f"""
                SELECT
                    COALESCE(p.brand, 'Unknown') as brand_name,
                    SUM(op.price_sold * op.quantity) as revenue,
                    SUM(op.quantity) as quantity,
                    COUNT(DISTINCT o.id) as orders
                FROM orders o
                JOIN order_products op ON o.id = op.order_id
                LEFT JOIN products p ON op.product_id = p.id
                WHERE {where_sql}
                GROUP BY COALESCE(p.brand, 'Unknown')
                ORDER BY revenue DESC
            """
            brand_results = conn.execute(brand_sql, params).fetchall()

            brand_colors = ["#7C3AED", "#2563EB", "#16A34A", "#F59E0B", "#eb4200", "#EC4899", "#8B5CF6", "#06B6D4", "#14B8A6", "#EF4444"]

            # Top 10 by revenue
            top_by_revenue = brand_results[:10]
            top_brands_revenue = {
                "labels": [row[0] for row in top_by_revenue],
                "data": [round(float(row[1]), 2) for row in top_by_revenue],
                "quantities": [row[2] for row in top_by_revenue],
                "orders": [row[3] for row in top_by_revenue],
                "backgroundColor": brand_colors[:len(top_by_revenue)]
            }

            # Top 10 by quantity
            sorted_by_qty = sorted(brand_results, key=lambda x: x[2], reverse=True)[:10]
            top_brands_quantity = {
                "labels": [row[0] for row in sorted_by_qty],
                "data": [row[2] for row in sorted_by_qty],
                "revenue": [round(float(row[1]), 2) for row in sorted_by_qty],
                "backgroundColor": brand_colors[:len(sorted_by_qty)]
            }

            total_revenue = sum(float(row[1]) for row in brand_results)
            total_quantity = sum(row[2] for row in brand_results)
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

    async def get_expense_types(self) -> List[Dict[str, Any]]:
        """Get all expense types for filter dropdown."""
        async with self.connection() as conn:
            results = conn.execute("""
                SELECT id, name, alias, is_active
                FROM expense_types
                WHERE is_active = TRUE
                ORDER BY name
            """).fetchall()
            return [{"id": row[0], "name": row[1], "alias": row[2]} for row in results]

    async def get_expense_summary(
        self,
        start_date: date,
        end_date: date,
        source_id: Optional[int] = None,
        expense_type_id: Optional[int] = None,
        sales_type: str = "retail"
    ) -> Dict[str, Any]:
        """Get expense summary for a date range."""
        async with self.connection() as conn:
            params = [start_date, end_date]
            where_clauses = ["DATE(o.ordered_at) BETWEEN ? AND ?"]

            # Add sales type filter
            where_clauses.append(self._build_sales_type_filter(sales_type))

            if source_id:
                where_clauses.append("o.source_id = ?")
                params.append(source_id)

            if expense_type_id:
                where_clauses.append("e.expense_type_id = ?")
                params.append(expense_type_id)

            where_sql = " AND ".join(where_clauses)

            # Total expenses by type
            by_type_sql = f"""
                SELECT
                    COALESCE(et.name, 'Other') as type_name,
                    et.id as type_id,
                    SUM(e.amount) as total_amount,
                    COUNT(e.id) as expense_count
                FROM expenses e
                JOIN orders o ON e.order_id = o.id
                LEFT JOIN expense_types et ON e.expense_type_id = et.id
                WHERE {where_sql}
                GROUP BY et.id, et.name
                ORDER BY total_amount DESC
            """
            by_type_results = conn.execute(by_type_sql, params).fetchall()

            # Total expenses summary
            total_sql = f"""
                SELECT
                    COALESCE(SUM(e.amount), 0) as total_expenses,
                    COUNT(e.id) as expense_count,
                    COUNT(DISTINCT e.order_id) as orders_with_expenses
                FROM expenses e
                JOIN orders o ON e.order_id = o.id
                WHERE {where_sql}
            """
            total_result = conn.execute(total_sql, params).fetchone()

            # Daily expense trend
            trend_sql = f"""
                SELECT
                    DATE(o.ordered_at) as day,
                    SUM(e.amount) as total_expenses
                FROM expenses e
                JOIN orders o ON e.order_id = o.id
                WHERE {where_sql}
                GROUP BY DATE(o.ordered_at)
                ORDER BY day
            """
            trend_results = conn.execute(trend_sql, params).fetchall()
            daily_data = {row[0]: float(row[1]) for row in trend_results}

            # Build trend labels and data
            labels = []
            data = []
            current = start_date
            while current <= end_date:
                labels.append(current.strftime("%d.%m"))
                data.append(round(daily_data.get(current, 0), 2))
                current += timedelta(days=1)

            # Chart colors
            expense_colors = ["#EF4444", "#F59E0B", "#8B5CF6", "#06B6D4", "#EC4899", "#14B8A6", "#7C3AED", "#2563EB"]

            # By type breakdown for pie chart
            by_type_data = {
                "labels": [row[0] for row in by_type_results],
                "data": [round(float(row[2]), 2) for row in by_type_results],
                "counts": [row[3] for row in by_type_results],
                "backgroundColor": expense_colors[:len(by_type_results)]
            }

            total_expenses = float(total_result[0] or 0)
            expense_count = total_result[1] or 0
            orders_with_expenses = total_result[2] or 0

            return {
                "byType": by_type_data,
                "trend": {
                    "labels": labels,
                    "datasets": [{
                        "label": "Expenses (UAH)",
                        "data": data,
                        "borderColor": "#EF4444",
                        "backgroundColor": "rgba(239, 68, 68, 0.1)",
                        "fill": True,
                        "tension": 0.3,
                        "borderWidth": 2
                    }]
                },
                "metrics": {
                    "totalExpenses": round(total_expenses, 2),
                    "expenseCount": expense_count,
                    "ordersWithExpenses": orders_with_expenses,
                    "avgExpensePerOrder": round(total_expenses / orders_with_expenses, 2) if orders_with_expenses > 0 else 0
                }
            }

    async def get_profit_analysis(
        self,
        start_date: date,
        end_date: date,
        source_id: Optional[int] = None,
        sales_type: str = "retail"
    ) -> Dict[str, Any]:
        """Get profit analysis: revenue vs expenses."""
        async with self.connection() as conn:
            return_statuses = tuple(int(s) for s in OrderStatus.return_statuses())
            params = [start_date, end_date]
            where_clauses = [
                "DATE(o.ordered_at) BETWEEN ? AND ?",
                f"o.status_id NOT IN {return_statuses}",
                self._build_sales_type_filter(sales_type)
            ]

            if source_id:
                where_clauses.append("o.source_id = ?")
                params.append(source_id)

            where_sql = " AND ".join(where_clauses)

            # Daily revenue and expenses
            daily_sql = f"""
                SELECT
                    DATE(o.ordered_at) as day,
                    SUM(o.grand_total) as revenue,
                    COALESCE(SUM(e.amount), 0) as expenses
                FROM orders o
                LEFT JOIN expenses e ON o.id = e.order_id
                WHERE {where_sql}
                GROUP BY DATE(o.ordered_at)
                ORDER BY day
            """
            results = conn.execute(daily_sql, params).fetchall()
            daily_data = {row[0]: {"revenue": float(row[1]), "expenses": float(row[2])} for row in results}

            # Build chart data
            labels = []
            revenue_data = []
            expenses_data = []
            profit_data = []
            current = start_date
            while current <= end_date:
                labels.append(current.strftime("%d.%m"))
                day_data = daily_data.get(current, {"revenue": 0, "expenses": 0})
                revenue = day_data["revenue"]
                expenses = day_data["expenses"]
                revenue_data.append(round(revenue, 2))
                expenses_data.append(round(expenses, 2))
                profit_data.append(round(revenue - expenses, 2))
                current += timedelta(days=1)

            total_revenue = sum(revenue_data)
            total_expenses = sum(expenses_data)
            total_profit = total_revenue - total_expenses
            profit_margin = (total_profit / total_revenue * 100) if total_revenue > 0 else 0

            return {
                "chart": {
                    "labels": labels,
                    "datasets": [
                        {
                            "label": "Revenue",
                            "data": revenue_data,
                            "borderColor": "#16A34A",
                            "backgroundColor": "rgba(22, 163, 74, 0.1)",
                            "fill": False,
                            "tension": 0.3
                        },
                        {
                            "label": "Expenses",
                            "data": expenses_data,
                            "borderColor": "#EF4444",
                            "backgroundColor": "rgba(239, 68, 68, 0.1)",
                            "fill": False,
                            "tension": 0.3
                        },
                        {
                            "label": "Gross Profit",
                            "data": profit_data,
                            "borderColor": "#2563EB",
                            "backgroundColor": "rgba(37, 99, 235, 0.2)",
                            "fill": True,
                            "tension": 0.3
                        }
                    ]
                },
                "metrics": {
                    "totalRevenue": round(total_revenue, 2),
                    "totalExpenses": round(total_expenses, 2),
                    "grossProfit": round(total_profit, 2),
                    "profitMargin": round(profit_margin, 1)
                }
            }

    # ─── Stats Methods ────────────────────────────────────────────────────────

    async def get_stats(self) -> Dict[str, Any]:
        """Get database statistics."""
        async with self.connection() as conn:
            orders_count = conn.execute("SELECT COUNT(*) FROM orders").fetchone()[0]
            products_count = conn.execute("SELECT COUNT(*) FROM products").fetchone()[0]
            categories_count = conn.execute("SELECT COUNT(*) FROM categories").fetchone()[0]
            expenses_count = conn.execute("SELECT COUNT(*) FROM expenses").fetchone()[0]
            expense_types_count = conn.execute("SELECT COUNT(*) FROM expense_types").fetchone()[0]

            min_date = conn.execute("SELECT MIN(DATE(ordered_at)) FROM orders").fetchone()[0]
            max_date = conn.execute("SELECT MAX(DATE(ordered_at)) FROM orders").fetchone()[0]

            return {
                "orders": orders_count,
                "products": products_count,
                "categories": categories_count,
                "expenses": expenses_count,
                "expense_types": expense_types_count,
                "date_range": {
                    "min": min_date.isoformat() if min_date else None,
                    "max": max_date.isoformat() if max_date else None
                },
                "db_size_mb": round(self.db_path.stat().st_size / 1024 / 1024, 2) if self.db_path.exists() else 0
            }


# ═══════════════════════════════════════════════════════════════════════════════
# SINGLETON INSTANCE
# ═══════════════════════════════════════════════════════════════════════════════

_store_instance: Optional[DuckDBStore] = None


async def get_store() -> DuckDBStore:
    """Get singleton DuckDB store instance."""
    global _store_instance
    if _store_instance is None:
        _store_instance = DuckDBStore()
        await _store_instance.connect()
    return _store_instance


async def close_store() -> None:
    """Close singleton store instance."""
    global _store_instance
    if _store_instance:
        await _store_instance.close()
        _store_instance = None
