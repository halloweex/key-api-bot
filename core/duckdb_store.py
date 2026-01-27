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
import pandas as pd

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
# Retail manager IDs (including historical managers who left: 8, 11, 17, 19)
RETAIL_MANAGER_IDS = [4, 8, 11, 16, 17, 19, 22]

# Timezone for date extraction - KeyCRM stores timestamps in +04:00 (server time)
# but UI displays in Kyiv timezone, so we convert for consistency
DISPLAY_TIMEZONE = 'Europe/Kyiv'


def _date_in_kyiv(column: str) -> str:
    """Generate SQL for extracting date in Kyiv timezone.

    KeyCRM API returns timestamps in +04:00 (server timezone), but the UI
    displays dates in Kyiv timezone. This ensures dashboard matches KeyCRM UI.

    Args:
        column: The timestamp column (e.g., 'o.ordered_at')

    Returns:
        SQL expression for date extraction in Kyiv timezone
    """
    return f"DATE(timezone('{DISPLAY_TIMEZONE}', {column}))"


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
            updated_at TIMESTAMP WITH TIME ZONE,  -- For idempotent sync
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
            price_sold DECIMAL(12, 2) NOT NULL
            -- FK removed due to DuckDB UPDATE/DELETE bug with foreign keys
            -- See: https://github.com/duckdb/duckdb/issues/4023
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
            synced_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
            -- FK removed due to DuckDB UPDATE/DELETE bug with foreign keys
            -- See: https://github.com/duckdb/duckdb/issues/4023
        );

        -- Offer stocks (inventory levels)
        CREATE TABLE IF NOT EXISTS offer_stocks (
            id INTEGER PRIMARY KEY,              -- offer_id from KeyCRM
            sku VARCHAR,
            price DECIMAL(12, 2),
            purchased_price DECIMAL(12, 2),
            quantity INTEGER DEFAULT 0,
            reserve INTEGER DEFAULT 0,
            synced_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
        );

        -- Inventory history (daily snapshots for average calculation)
        CREATE TABLE IF NOT EXISTS inventory_history (
            date DATE NOT NULL,
            total_quantity INTEGER NOT NULL,
            total_value DECIMAL(14, 2) NOT NULL,
            total_reserve INTEGER DEFAULT 0,
            sku_count INTEGER DEFAULT 0,
            recorded_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (date)
        );

        -- Sync metadata
        CREATE TABLE IF NOT EXISTS sync_metadata (
            key VARCHAR PRIMARY KEY,
            value VARCHAR,
            updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
        );

        -- Revenue goals (smart goal-setting system)
        CREATE TABLE IF NOT EXISTS revenue_goals (
            period_type VARCHAR(10) PRIMARY KEY,  -- 'daily', 'weekly', 'monthly'
            goal_amount DECIMAL(12, 2) NOT NULL,
            is_custom BOOLEAN DEFAULT FALSE,      -- TRUE = manual override, FALSE = auto-calculated
            calculated_goal DECIMAL(12, 2),       -- System-suggested goal (for reference)
            growth_factor DECIMAL(4, 2) DEFAULT 1.10,  -- Default 10% growth target
            updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
        );

        -- Seasonal indices (monthly seasonality factors)
        CREATE TABLE IF NOT EXISTS seasonal_indices (
            month INTEGER PRIMARY KEY,            -- 1-12 (January-December)
            seasonality_index DECIMAL(6, 4),      -- e.g., 0.85 means 15% below average
            sample_size INTEGER,                  -- Number of data points used
            avg_revenue DECIMAL(12, 2),           -- Average revenue for this month
            min_revenue DECIMAL(12, 2),           -- Min observed
            max_revenue DECIMAL(12, 2),           -- Max observed
            yoy_growth DECIMAL(6, 4),             -- Year-over-year growth for this month
            confidence VARCHAR(10),               -- 'high', 'medium', 'low'
            updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
        );

        -- Weekly patterns within month (how revenue distributes across weeks)
        CREATE TABLE IF NOT EXISTS weekly_patterns (
            month INTEGER NOT NULL,               -- 1-12
            week_of_month INTEGER NOT NULL,       -- 1-5
            weight DECIMAL(6, 4),                 -- e.g., 0.28 means 28% of monthly revenue
            sample_size INTEGER,
            updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (month, week_of_month)
        );

        -- Growth metrics (calculated growth rates)
        CREATE TABLE IF NOT EXISTS growth_metrics (
            metric_type VARCHAR(20) PRIMARY KEY,  -- 'yoy_overall', 'mom_avg', 'trend_slope'
            value DECIMAL(8, 4),
            period_start DATE,
            period_end DATE,
            sample_size INTEGER,
            updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
        );

        -- Managers/Users table (synced from KeyCRM)
        CREATE TABLE IF NOT EXISTS managers (
            id INTEGER PRIMARY KEY,
            name VARCHAR NOT NULL,
            email VARCHAR,
            status VARCHAR,                       -- 'active', 'blocked', 'pending'
            is_retail BOOLEAN DEFAULT FALSE,      -- TRUE for retail managers, FALSE for B2B
            first_order_date DATE,                -- Calculated from orders
            last_order_date DATE,                 -- Calculated from orders
            order_count INTEGER DEFAULT 0,        -- Total orders handled
            synced_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
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
        CREATE INDEX IF NOT EXISTS idx_managers_is_retail ON managers(is_retail);

        -- Composite indexes for common aggregate queries (performance optimization)
        CREATE INDEX IF NOT EXISTS idx_orders_source_date ON orders(source_id, ordered_at);
        CREATE INDEX IF NOT EXISTS idx_orders_status_date ON orders(status_id, ordered_at);
        CREATE INDEX IF NOT EXISTS idx_orders_manager_date ON orders(manager_id, ordered_at);
        CREATE INDEX IF NOT EXISTS idx_orders_buyer_date ON orders(buyer_id, ordered_at);
        """
        self._connection.execute(schema_sql)

        # Migration: add updated_at column to existing orders table
        await self._run_migrations()

        logger.info("DuckDB schema initialized")

    async def _run_migrations(self) -> None:
        """Run database migrations for schema changes."""
        # Migration 1: Add updated_at column to orders table (for idempotent sync)
        try:
            self._connection.execute(
                "ALTER TABLE orders ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP WITH TIME ZONE"
            )
            logger.debug("Migration: updated_at column added/verified")
        except Exception as e:
            # Column might already exist or ALTER TABLE not supported
            logger.debug(f"Migration note: {e}")

        # Migration 2: Recreate order_products without FK constraint (DuckDB FK bug workaround)
        try:
            # Check if FK constraint exists
            has_fk = False
            try:
                result = self._connection.execute("""
                    SELECT COUNT(*) FROM duckdb_constraints()
                    WHERE table_name = 'order_products' AND constraint_type = 'FOREIGN KEY'
                """).fetchone()
                has_fk = result[0] > 0 if result else False
            except Exception:
                # duckdb_constraints() might not be available, check by trying a test delete
                # If FK exists, we need to remove it
                logger.debug("duckdb_constraints() not available, checking FK via schema")
                # Alternative: check if REFERENCES exists in table definition
                try:
                    schema = self._connection.execute("""
                        SELECT sql FROM sqlite_master WHERE type='table' AND name='order_products'
                    """).fetchone()
                    if schema and 'REFERENCES' in str(schema[0]).upper():
                        has_fk = True
                except Exception:
                    # Try pragma approach
                    try:
                        fk_list = self._connection.execute("PRAGMA foreign_key_list('order_products')").fetchall()
                        has_fk = len(fk_list) > 0
                    except Exception:
                        pass

            if has_fk:
                logger.info("Migration: Removing FK constraint from order_products...")
                self._connection.execute("BEGIN TRANSACTION")
                try:
                    # Backup data
                    self._connection.execute("""
                        CREATE TABLE order_products_backup AS SELECT * FROM order_products
                    """)
                    # Drop old table
                    self._connection.execute("DROP TABLE order_products")
                    # Create new table without FK
                    self._connection.execute("""
                        CREATE TABLE order_products (
                            id INTEGER PRIMARY KEY,
                            order_id INTEGER NOT NULL,
                            product_id INTEGER,
                            name VARCHAR NOT NULL,
                            quantity INTEGER NOT NULL,
                            price_sold DECIMAL(12, 2) NOT NULL
                        )
                    """)
                    # Restore data
                    self._connection.execute("""
                        INSERT INTO order_products SELECT * FROM order_products_backup
                    """)
                    # Drop backup
                    self._connection.execute("DROP TABLE order_products_backup")
                    self._connection.execute("COMMIT")
                    logger.info("Migration: order_products FK constraint removed successfully")
                except Exception as e:
                    self._connection.execute("ROLLBACK")
                    logger.error(f"Migration failed (FK removal), rolling back: {e}")
                    raise
        except Exception as e:
            logger.error(f"Migration error (order_products FK removal): {e}")

        # Migration 3: Remove FK from expenses table (same DuckDB bug)
        try:
            has_fk = False
            try:
                result = self._connection.execute("""
                    SELECT COUNT(*) FROM duckdb_constraints()
                    WHERE table_name = 'expenses' AND constraint_type = 'FOREIGN KEY'
                """).fetchone()
                has_fk = result[0] > 0 if result else False
            except Exception:
                # Fallback: check via PRAGMA
                try:
                    fk_list = self._connection.execute("PRAGMA foreign_key_list('expenses')").fetchall()
                    has_fk = len(fk_list) > 0
                except Exception:
                    pass

            if has_fk:
                logger.info("Migration: Removing FK constraint from expenses...")
                self._connection.execute("BEGIN TRANSACTION")
                try:
                    self._connection.execute("""
                        CREATE TABLE expenses_backup AS SELECT * FROM expenses
                    """)
                    self._connection.execute("DROP TABLE expenses")
                    self._connection.execute("""
                        CREATE TABLE expenses (
                            id INTEGER PRIMARY KEY,
                            order_id INTEGER NOT NULL,
                            expense_type_id INTEGER,
                            amount DECIMAL(12, 2) NOT NULL,
                            description VARCHAR,
                            status VARCHAR,
                            payment_date TIMESTAMP WITH TIME ZONE,
                            created_at TIMESTAMP WITH TIME ZONE,
                            synced_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
                        )
                    """)
                    self._connection.execute("""
                        INSERT INTO expenses SELECT * FROM expenses_backup
                    """)
                    self._connection.execute("DROP TABLE expenses_backup")
                    self._connection.execute("COMMIT")
                    logger.info("Migration: expenses FK constraint removed successfully")
                except Exception as e:
                    self._connection.execute("ROLLBACK")
                    logger.error(f"Migration failed (expenses FK removal), rolling back: {e}")
                    raise
        except Exception as e:
            logger.error(f"Migration error (expenses FK removal): {e}")

    def _build_sales_type_filter(self, sales_type: str, table_alias: str = "o") -> str:
        """Build SQL clause for retail/b2b/all filtering based on manager_id.

        Uses managers table if populated, otherwise falls back to hardcoded RETAIL_MANAGER_IDS.

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
            # Retail = managers marked as retail + Shopify orders (NULL manager)
            # Uses managers table with fallback to hardcoded IDs if table is empty
            manager_list = ",".join(str(m) for m in RETAIL_MANAGER_IDS)
            return f"""(
                {table_alias}.manager_id IS NULL
                OR {table_alias}.manager_id IN (SELECT id FROM managers WHERE is_retail = TRUE)
                OR (NOT EXISTS (SELECT 1 FROM managers WHERE is_retail = TRUE)
                    AND {table_alias}.manager_id IN ({manager_list}))
            )"""

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
        Insert or update orders from API response (idempotent).

        Uses DataFrame bulk insert for performance (~10-100x faster than row-by-row).
        Only updates existing orders if the new updated_at is newer than
        the existing one. This prevents stale API responses from overwriting
        fresher data.

        Args:
            orders: List of order dicts from KeyCRM API

        Returns:
            Number of orders upserted
        """
        if not orders:
            return 0

        # Parse orders and build DataFrames
        order_rows = []
        product_rows = []

        for order_data in orders:
            order = Order.from_api(order_data)

            # Skip invalid orders
            if not order.ordered_at:
                continue

            order_rows.append({
                "id": order.id,
                "source_id": order.source_id,
                "status_id": order.status_id,
                "grand_total": float(order.grand_total),
                "ordered_at": order.ordered_at,  # Keep as datetime
                "created_at": order.created_at,  # Keep as datetime
                "updated_at": order.updated_at,  # Keep as datetime
                "buyer_id": order.buyer.id if order.buyer else None,
                "manager_id": order.manager.id if order.manager else None,
            })

            # Build product rows
            # ID generation: order_id * 1000 + position (supports up to 1000 products/order, order IDs up to ~2M)
            for i, prod in enumerate(order.products):
                product_rows.append({
                    "id": order.id * 1000 + i,
                    "order_id": order.id,
                    "product_id": prod.product_id,
                    "name": prod.name,
                    "quantity": prod.quantity,
                    "price_sold": float(prod.price_sold),
                })

        if not order_rows:
            return 0

        # Create DataFrame for orders (products use executemany for simplicity)
        orders_df = pd.DataFrame(order_rows)

        # Convert datetime columns to proper pandas datetime type for DuckDB
        for col in ["ordered_at", "created_at", "updated_at"]:
            orders_df[col] = pd.to_datetime(orders_df[col], utc=True)

        # Get order IDs for use in queries (avoids DuckDB FK bug with subqueries)
        order_ids = orders_df["id"].tolist()

        async with self.connection() as conn:
            conn.execute("BEGIN TRANSACTION")
            try:
                # Register DataFrame as view
                conn.register("stg_orders", orders_df)

                # 1. Delete existing products using explicit ID list
                if order_ids:
                    placeholders = ",".join("?" * len(order_ids))
                    conn.execute(f"DELETE FROM order_products WHERE order_id IN ({placeholders})", order_ids)

                # 2. Find orders that need to be updated (staging has newer data)
                orders_to_update = conn.execute("""
                    SELECT o.id FROM orders o
                    JOIN stg_orders stg ON o.id = stg.id
                    WHERE stg.updated_at > o.updated_at
                       OR o.updated_at IS NULL
                       OR stg.updated_at IS NULL
                """).fetchall()
                update_ids = [row[0] for row in orders_to_update]

                # 3. Delete orders that will be re-inserted with updated data
                if update_ids:
                    placeholders = ",".join("?" * len(update_ids))
                    conn.execute(f"DELETE FROM orders WHERE id IN ({placeholders})", update_ids)

                # 4. Insert all orders from staging (new ones + updated ones that were deleted)
                conn.execute("""
                    INSERT INTO orders (id, source_id, status_id, grand_total, ordered_at, created_at, updated_at, buyer_id, manager_id, synced_at)
                    SELECT id, source_id, status_id, grand_total, ordered_at, created_at, updated_at, buyer_id, manager_id, now()
                    FROM stg_orders stg
                    WHERE NOT EXISTS (SELECT 1 FROM orders WHERE orders.id = stg.id)
                """)

                # 5. Insert new products in batch (use OR REPLACE to handle ID collisions from schema change)
                if product_rows:
                    conn.executemany("""
                        INSERT OR REPLACE INTO order_products (id, order_id, product_id, name, quantity, price_sold)
                        VALUES (?, ?, ?, ?, ?, ?)
                    """, [
                        (p["id"], p["order_id"], p["product_id"], p["name"], p["quantity"], p["price_sold"])
                        for p in product_rows
                    ])

                conn.unregister("stg_orders")
                conn.execute("COMMIT")

                count = len(order_rows)
                logger.info(f"Upserted {count} orders to DuckDB (DataFrame bulk insert)")
                return count

            except Exception:
                conn.execute("ROLLBACK")
                raise

    async def upsert_products(self, products: List[Dict[str, Any]]) -> int:
        """Insert or update products from API response."""
        if not products:
            return 0

        async with self.connection() as conn:
            conn.execute("BEGIN TRANSACTION")
            try:
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

                conn.execute("COMMIT")
                logger.info(f"Upserted {count} products to DuckDB")
                return count

            except Exception:
                conn.execute("ROLLBACK")
                raise

    async def upsert_categories(self, categories: List[Dict[str, Any]]) -> int:
        """Insert or update categories from API response."""
        if not categories:
            return 0

        async with self.connection() as conn:
            conn.execute("BEGIN TRANSACTION")
            try:
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

                conn.execute("COMMIT")
                logger.info(f"Upserted {count} categories to DuckDB")
                return count

            except Exception:
                conn.execute("ROLLBACK")
                raise

    async def upsert_expense_types(self, expense_types: List[Dict[str, Any]]) -> int:
        """Insert or update expense types from API response."""
        if not expense_types:
            return 0

        async with self.connection() as conn:
            conn.execute("BEGIN TRANSACTION")
            try:
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

                conn.execute("COMMIT")
                logger.info(f"Upserted {count} expense types to DuckDB")
                return count

            except Exception:
                conn.execute("ROLLBACK")
                raise

    async def upsert_expenses(self, order_id: int, expenses: List[Dict[str, Any]]) -> int:
        """Insert or update expenses for an order."""
        if not expenses:
            return 0

        async with self.connection() as conn:
            conn.execute("BEGIN TRANSACTION")
            try:
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

                conn.execute("COMMIT")
                return count

            except Exception:
                conn.execute("ROLLBACK")
                raise

    async def upsert_managers(self, managers: List[Dict[str, Any]]) -> int:
        """Insert or update managers from KeyCRM API response.

        Args:
            managers: List of manager/user dicts from KeyCRM API

        Returns:
            Number of managers upserted
        """
        if not managers:
            return 0

        async with self.connection() as conn:
            conn.execute("BEGIN TRANSACTION")
            try:
                count = 0
                for mgr in managers:
                    conn.execute("""
                        INSERT OR REPLACE INTO managers
                        (id, name, email, status, is_retail, synced_at)
                        VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                    """, [
                        mgr.get("id"),
                        mgr.get("name") or mgr.get("full_name", "Unknown"),
                        mgr.get("email"),
                        mgr.get("status"),  # 'active', 'blocked', 'pending'
                        mgr.get("id") in RETAIL_MANAGER_IDS  # Set is_retail based on known IDs
                    ])
                    count += 1

                conn.execute("COMMIT")
                logger.info(f"Upserted {count} managers to DuckDB")
                return count

            except Exception:
                conn.execute("ROLLBACK")
                raise

    async def upsert_stocks(self, stocks: List[Dict[str, Any]]) -> int:
        """Insert or update offer stocks from KeyCRM API response.

        Args:
            stocks: List of stock dicts from KeyCRM API (offers/stocks endpoint)

        Returns:
            Number of stocks upserted
        """
        if not stocks:
            return 0

        async with self.connection() as conn:
            conn.execute("BEGIN TRANSACTION")
            try:
                count = 0
                for stock in stocks:
                    conn.execute("""
                        INSERT OR REPLACE INTO offer_stocks
                        (id, sku, price, purchased_price, quantity, reserve, synced_at)
                        VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                    """, [
                        stock.get("id"),
                        stock.get("sku"),
                        stock.get("price"),
                        stock.get("purchased_price"),
                        stock.get("quantity", 0),
                        stock.get("reserve", 0),
                    ])
                    count += 1

                conn.execute("COMMIT")
                logger.info(f"Upserted {count} offer stocks to DuckDB")
                return count

            except Exception:
                conn.execute("ROLLBACK")
                raise

    async def get_stock_summary(self, limit: int = 20) -> Dict[str, Any]:
        """Get stock summary for dashboard display.

        Returns:
            Dict with total stats and top items by quantity and low stock alerts
        """
        async with self.connection() as conn:
            # Overall stats (using sale/retail price)
            stats = conn.execute("""
                SELECT
                    COUNT(*) as total_offers,
                    COUNT(*) FILTER (WHERE quantity > 0) as in_stock_count,
                    COUNT(*) FILTER (WHERE quantity = 0) as out_of_stock_count,
                    COUNT(*) FILTER (WHERE quantity > 0 AND quantity <= 5) as low_stock_count,
                    SUM(quantity - reserve) as available_quantity,
                    SUM(reserve) as total_reserve,
                    SUM(quantity * price) as total_value,
                    SUM(reserve * price) as reserve_value
                FROM offer_stocks
            """).fetchone()

            # Top items by quantity (with product names)
            top_by_qty = conn.execute(f"""
                SELECT os.sku, os.quantity, os.reserve, os.price, p.name
                FROM offer_stocks os
                LEFT JOIN products p ON os.id = p.id
                WHERE os.quantity > 0
                ORDER BY os.quantity DESC
                LIMIT {limit}
            """).fetchall()

            # Low stock items (1-5 units, excluding 0)
            low_stock = conn.execute("""
                SELECT os.sku, os.quantity, os.reserve, os.price, p.name
                FROM offer_stocks os
                LEFT JOIN products p ON os.id = p.id
                WHERE os.quantity > 0 AND os.quantity <= 5
                ORDER BY os.quantity ASC
                LIMIT 20
            """).fetchall()

            # Out of stock items
            out_of_stock = conn.execute("""
                SELECT os.sku, os.price, p.name
                FROM offer_stocks os
                LEFT JOIN products p ON os.id = p.id
                WHERE os.quantity = 0
                ORDER BY os.price DESC
                LIMIT 20
            """).fetchall()

            # Last sync time
            last_sync = conn.execute("""
                SELECT value FROM sync_metadata WHERE key = 'stocks_last_sync'
            """).fetchone()

            # Get average inventory (30 days)
            avg_inv = conn.execute("""
                WITH period_data AS (
                    SELECT
                        total_quantity,
                        total_value,
                        ROW_NUMBER() OVER (ORDER BY date ASC) as rn_asc,
                        ROW_NUMBER() OVER (ORDER BY date DESC) as rn_desc
                    FROM inventory_history
                    WHERE date >= CURRENT_DATE - INTERVAL 30 DAY
                )
                SELECT
                    MAX(CASE WHEN rn_asc = 1 THEN total_quantity END) as beginning_qty,
                    MAX(CASE WHEN rn_asc = 1 THEN total_value END) as beginning_value,
                    MAX(CASE WHEN rn_desc = 1 THEN total_quantity END) as ending_qty,
                    MAX(CASE WHEN rn_desc = 1 THEN total_value END) as ending_value,
                    COUNT(*) as data_points
                FROM period_data
            """).fetchone()

            # Calculate average inventory
            if avg_inv and avg_inv[0] and avg_inv[2]:
                avg_quantity = (avg_inv[0] + avg_inv[2]) / 2
                avg_value = ((avg_inv[1] or 0) + (avg_inv[3] or 0)) / 2
                avg_data_points = avg_inv[4]
            else:
                avg_quantity = stats[4] or 0  # Use current as fallback
                avg_value = float(stats[6] or 0)
                avg_data_points = 0

            return {
                "summary": {
                    "totalOffers": stats[0] or 0,
                    "inStockCount": stats[1] or 0,
                    "outOfStockCount": stats[2] or 0,
                    "lowStockCount": stats[3] or 0,
                    "totalQuantity": stats[4] or 0,
                    "totalReserve": stats[5] or 0,
                    "totalValue": float(stats[6] or 0),
                    "reserveValue": float(stats[7] or 0),
                    "averageQuantity": round(avg_quantity),
                    "averageValue": round(avg_value, 2),
                    "avgDataPoints": avg_data_points,
                },
                "topByQuantity": [
                    {"sku": r[0], "quantity": r[1], "reserve": r[2], "price": float(r[3] or 0), "name": r[4]}
                    for r in top_by_qty
                ],
                "lowStock": [
                    {"sku": r[0], "quantity": r[1], "reserve": r[2], "price": float(r[3] or 0), "name": r[4]}
                    for r in low_stock
                ],
                "outOfStock": [
                    {"sku": r[0], "price": float(r[1] or 0), "name": r[2]}
                    for r in out_of_stock
                ],
                "lastSync": last_sync[0] if last_sync else None,
            }

    async def record_inventory_snapshot(self) -> bool:
        """Record daily inventory snapshot for average calculation.

        Only records one snapshot per day. Returns True if recorded, False if already exists.
        """
        async with self.connection() as conn:
            today = conn.execute("SELECT CURRENT_DATE").fetchone()[0]

            # Check if already recorded today
            exists = conn.execute(
                "SELECT 1 FROM inventory_history WHERE date = ?", [today]
            ).fetchone()

            if exists:
                return False

            # Record snapshot (using available quantity and sale/retail price)
            conn.execute("""
                INSERT INTO inventory_history (date, total_quantity, total_value, total_reserve, sku_count)
                SELECT
                    CURRENT_DATE,
                    COALESCE(SUM(quantity - reserve), 0),
                    COALESCE(SUM((quantity - reserve) * price), 0),
                    COALESCE(SUM(reserve), 0),
                    COUNT(*)
                FROM offer_stocks
            """)
            logger.info(f"Recorded inventory snapshot for {today}")
            return True

    async def get_average_inventory(self, days: int = 30) -> Dict[str, Any]:
        """Calculate average inventory over a period.

        Uses formula: (Beginning Inventory + Ending Inventory) / 2
        Also provides daily average if more data points available.

        Args:
            days: Number of days to look back (default 30)

        Returns:
            Dict with average inventory metrics
        """
        async with self.connection() as conn:
            # Get beginning and ending inventory for the period
            result = conn.execute("""
                WITH period_data AS (
                    SELECT
                        date,
                        total_quantity,
                        total_value,
                        ROW_NUMBER() OVER (ORDER BY date ASC) as rn_asc,
                        ROW_NUMBER() OVER (ORDER BY date DESC) as rn_desc
                    FROM inventory_history
                    WHERE date >= CURRENT_DATE - INTERVAL ? DAY
                )
                SELECT
                    -- Beginning inventory (oldest in period)
                    MAX(CASE WHEN rn_asc = 1 THEN total_quantity END) as beginning_qty,
                    MAX(CASE WHEN rn_asc = 1 THEN total_value END) as beginning_value,
                    MAX(CASE WHEN rn_asc = 1 THEN date END) as beginning_date,
                    -- Ending inventory (most recent)
                    MAX(CASE WHEN rn_desc = 1 THEN total_quantity END) as ending_qty,
                    MAX(CASE WHEN rn_desc = 1 THEN total_value END) as ending_value,
                    MAX(CASE WHEN rn_desc = 1 THEN date END) as ending_date,
                    -- Daily averages
                    AVG(total_quantity) as avg_daily_qty,
                    AVG(total_value) as avg_daily_value,
                    COUNT(*) as data_points
                FROM period_data
            """, [days]).fetchone()

            if not result or not result[0]:
                # No historical data, use current snapshot (sale/retail price)
                current = conn.execute("""
                    SELECT
                        COALESCE(SUM(quantity - reserve), 0),
                        COALESCE(SUM((quantity - reserve) * price), 0)
                    FROM offer_stocks
                """).fetchone()

                return {
                    "averageQuantity": current[0] or 0,
                    "averageValue": float(current[1] or 0),
                    "beginningQuantity": None,
                    "endingQuantity": current[0] or 0,
                    "beginningValue": None,
                    "endingValue": float(current[1] or 0),
                    "dataPoints": 0,
                    "periodDays": days,
                    "message": "No historical data yet. Average based on current snapshot.",
                }

            beginning_qty = result[0] or 0
            beginning_value = float(result[1] or 0)
            ending_qty = result[3] or 0
            ending_value = float(result[4] or 0)

            # Calculate averages using (Beginning + Ending) / 2
            avg_qty = (beginning_qty + ending_qty) / 2
            avg_value = (beginning_value + ending_value) / 2

            return {
                "averageQuantity": round(avg_qty),
                "averageValue": round(avg_value, 2),
                "beginningQuantity": beginning_qty,
                "beginningValue": beginning_value,
                "beginningDate": str(result[2]) if result[2] else None,
                "endingQuantity": ending_qty,
                "endingValue": ending_value,
                "endingDate": str(result[5]) if result[5] else None,
                "dailyAverageQuantity": round(float(result[6] or 0)),
                "dailyAverageValue": round(float(result[7] or 0), 2),
                "dataPoints": result[8] or 0,
                "periodDays": days,
            }

    async def get_inventory_trend(
        self,
        days: int = 90,
        granularity: str = "daily"
    ) -> Dict[str, Any]:
        """Get inventory trend over time for charting.

        Args:
            days: Number of days to look back (default 90)
            granularity: 'daily' or 'monthly'

        Returns:
            Dict with labels, values, quantities for trend chart
        """
        async with self.connection() as conn:
            if granularity == "monthly":
                # Monthly aggregation
                result = conn.execute("""
                    SELECT
                        DATE_TRUNC('month', date) as period,
                        AVG(total_quantity) as avg_quantity,
                        AVG(total_value) as avg_value,
                        AVG(total_reserve) as avg_reserve,
                        MIN(total_quantity) as min_quantity,
                        MAX(total_quantity) as max_quantity,
                        MIN(total_value) as min_value,
                        MAX(total_value) as max_value,
                        COUNT(*) as data_points
                    FROM inventory_history
                    WHERE date >= CURRENT_DATE - INTERVAL ? DAY
                    GROUP BY DATE_TRUNC('month', date)
                    ORDER BY period
                """, [days]).fetchall()

                labels = [row[0].strftime('%b %Y') for row in result]
                quantities = [round(row[1]) for row in result]
                values = [round(float(row[2]), 2) for row in result]
                reserves = [round(row[3]) for row in result]

                return {
                    "labels": labels,
                    "quantity": quantities,
                    "value": values,
                    "reserve": reserves,
                    "granularity": "monthly",
                    "periodDays": days,
                    "dataPoints": len(result),
                }
            else:
                # Daily data
                result = conn.execute("""
                    SELECT
                        date,
                        total_quantity,
                        total_value,
                        total_reserve,
                        sku_count
                    FROM inventory_history
                    WHERE date >= CURRENT_DATE - INTERVAL ? DAY
                    ORDER BY date
                """, [days]).fetchall()

                labels = [row[0].strftime('%d %b') for row in result]
                quantities = [row[1] for row in result]
                values = [float(row[2]) for row in result]
                reserves = [row[3] for row in result]
                sku_counts = [row[4] for row in result]

                # Calculate changes
                changes = []
                for i, val in enumerate(values):
                    if i == 0:
                        changes.append(0)
                    else:
                        changes.append(round(val - values[i - 1], 2))

                return {
                    "labels": labels,
                    "quantity": quantities,
                    "value": values,
                    "reserve": reserves,
                    "skuCount": sku_counts,
                    "valueChange": changes,
                    "granularity": "daily",
                    "periodDays": days,
                    "dataPoints": len(result),
                    "summary": {
                        "startValue": values[0] if values else 0,
                        "endValue": values[-1] if values else 0,
                        "change": round(values[-1] - values[0], 2) if len(values) > 1 else 0,
                        "changePercent": round((values[-1] - values[0]) / values[0] * 100, 1) if len(values) > 1 and values[0] > 0 else 0,
                        "minValue": min(values) if values else 0,
                        "maxValue": max(values) if values else 0,
                    } if values else None,
                }

    async def get_dead_stock_analysis(self) -> Dict[str, Any]:
        """Analyze dead stock using dynamic category-based thresholds.

        Methodology:
        1. Calculate sales velocity per category (median days between sales)
        2. Calculate dynamic threshold: median + 1.5×IQR per category
        3. Identify products not sold beyond their category threshold
        4. Fallback threshold of 180 days for categories with insufficient data

        Returns:
            Dict with dead stock analysis, category thresholds, and items at risk
        """
        async with self.connection() as conn:
            # Step 1: Calculate category-level sales velocity statistics
            # Using median and IQR for robust threshold calculation
            category_stats = conn.execute("""
                WITH product_sales AS (
                    -- Get all sales with dates per product
                    SELECT
                        p.id as product_id,
                        p.category_id,
                        c.name as category_name,
                        DATE(o.ordered_at AT TIME ZONE 'Europe/Kyiv') as sale_date
                    FROM order_products op
                    JOIN orders o ON op.order_id = o.id
                    JOIN products p ON op.product_id = p.id
                    LEFT JOIN categories c ON p.category_id = c.id
                    WHERE o.status_id NOT IN (19, 22, 21, 23)  -- Exclude returns
                ),
                product_last_sale AS (
                    -- Last sale date per product
                    SELECT
                        product_id,
                        category_id,
                        category_name,
                        MAX(sale_date) as last_sale_date,
                        CURRENT_DATE - MAX(sale_date) as days_since_sale
                    FROM product_sales
                    GROUP BY product_id, category_id, category_name
                ),
                category_velocity AS (
                    -- Calculate percentiles per category for threshold
                    SELECT
                        category_id,
                        category_name,
                        COUNT(*) as products_with_sales,
                        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY days_since_sale) as median_days,
                        PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY days_since_sale) as q1,
                        PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY days_since_sale) as q3,
                        AVG(days_since_sale) as avg_days,
                        MAX(days_since_sale) as max_days
                    FROM product_last_sale
                    GROUP BY category_id, category_name
                    HAVING COUNT(*) >= 5  -- Need minimum sample size
                )
                SELECT
                    category_id,
                    category_name,
                    products_with_sales,
                    ROUND(median_days) as median_days,
                    ROUND(q1) as q1_days,
                    ROUND(q3) as q3_days,
                    ROUND(avg_days) as avg_days,
                    -- Dynamic threshold: Q3 + 1.5×IQR, capped at 365 days max
                    ROUND(LEAST(GREATEST(q3 + 1.5 * (q3 - q1), 90), 365)) as threshold_days
                FROM category_velocity
                ORDER BY products_with_sales DESC
            """).fetchall()

            # Build category threshold lookup
            category_thresholds = {}
            for row in category_stats:
                category_thresholds[row[0]] = {
                    "categoryId": row[0],
                    "categoryName": row[1] or "Uncategorized",
                    "productsWithSales": row[2],
                    "medianDays": int(row[3]) if row[3] else 90,
                    "q1Days": int(row[4]) if row[4] else None,
                    "q3Days": int(row[5]) if row[5] else None,
                    "avgDays": int(row[6]) if row[6] else None,
                    "thresholdDays": int(row[7]) if row[7] else 180,
                }

            # Default threshold for categories without enough data
            DEFAULT_THRESHOLD = 180

            # Step 2: Identify dead stock items
            dead_stock_items = conn.execute("""
                WITH product_last_sale AS (
                    SELECT
                        op.product_id,
                        MAX(DATE(o.ordered_at AT TIME ZONE 'Europe/Kyiv')) as last_sale_date
                    FROM order_products op
                    JOIN orders o ON op.order_id = o.id
                    WHERE o.status_id NOT IN (19, 22, 21, 23)
                    GROUP BY op.product_id
                ),
                category_thresholds AS (
                    SELECT
                        category_id,
                        -- Q3 + 1.5×IQR threshold, capped at 365 days
                        LEAST(GREATEST(
                            PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY CURRENT_DATE - last_sale_date)
                            + 1.5 * (
                                PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY CURRENT_DATE - last_sale_date)
                                - PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY CURRENT_DATE - last_sale_date)
                            ),
                            90
                        ), 365) as threshold_days
                    FROM product_last_sale pls
                    JOIN products p ON pls.product_id = p.id
                    GROUP BY category_id
                    HAVING COUNT(*) >= 5
                ),
                stock_analysis AS (
                    SELECT
                        os.id,
                        os.sku,
                        os.quantity - os.reserve as available_qty,
                        os.price,
                        (os.quantity - os.reserve) * os.price as stock_value,
                        p.name as product_name,
                        p.brand as product_brand,
                        p.category_id,
                        c.name as category_name,
                        pls.last_sale_date,
                        CURRENT_DATE - pls.last_sale_date as days_since_sale,
                        COALESCE(ct.threshold_days, 180) as category_threshold
                    FROM offer_stocks os
                    LEFT JOIN products p ON os.id = p.id
                    LEFT JOIN categories c ON p.category_id = c.id
                    LEFT JOIN product_last_sale pls ON os.id = pls.product_id
                    LEFT JOIN category_thresholds ct ON p.category_id = ct.category_id
                    WHERE os.quantity - os.reserve > 0  -- Has available stock
                )
                SELECT
                    id,
                    sku,
                    available_qty,
                    price,
                    stock_value,
                    product_name,
                    product_brand,
                    category_id,
                    category_name,
                    last_sale_date,
                    days_since_sale,
                    category_threshold,
                    CASE
                        WHEN last_sale_date IS NULL THEN 'never_sold'
                        WHEN days_since_sale > category_threshold THEN 'dead_stock'
                        WHEN days_since_sale > category_threshold * 0.7 THEN 'at_risk'
                        ELSE 'healthy'
                    END as status
                FROM stock_analysis
                WHERE last_sale_date IS NULL
                   OR days_since_sale > category_threshold * 0.7
                ORDER BY
                    CASE WHEN last_sale_date IS NULL THEN 0 ELSE 1 END,
                    stock_value DESC
            """).fetchall()

            # Step 3: Calculate summary metrics
            summary = conn.execute("""
                WITH product_last_sale AS (
                    SELECT
                        op.product_id,
                        MAX(DATE(o.ordered_at AT TIME ZONE 'Europe/Kyiv')) as last_sale_date
                    FROM order_products op
                    JOIN orders o ON op.order_id = o.id
                    WHERE o.status_id NOT IN (19, 22, 21, 23)
                    GROUP BY op.product_id
                ),
                category_thresholds AS (
                    SELECT
                        category_id,
                        -- Q3 + 1.5×IQR threshold, capped at 365 days
                        LEAST(GREATEST(
                            PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY CURRENT_DATE - last_sale_date)
                            + 1.5 * (
                                PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY CURRENT_DATE - last_sale_date)
                                - PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY CURRENT_DATE - last_sale_date)
                            ),
                            90
                        ), 365) as threshold_days
                    FROM product_last_sale pls
                    JOIN products p ON pls.product_id = p.id
                    GROUP BY category_id
                    HAVING COUNT(*) >= 5
                ),
                stock_status AS (
                    SELECT
                        os.id,
                        os.quantity - os.reserve as available_qty,
                        (os.quantity - os.reserve) * os.price as stock_value,
                        CASE
                            WHEN pls.last_sale_date IS NULL THEN 'never_sold'
                            WHEN CURRENT_DATE - pls.last_sale_date > COALESCE(ct.threshold_days, 180) THEN 'dead_stock'
                            WHEN CURRENT_DATE - pls.last_sale_date > COALESCE(ct.threshold_days, 180) * 0.7 THEN 'at_risk'
                            ELSE 'healthy'
                        END as status
                    FROM offer_stocks os
                    LEFT JOIN products p ON os.id = p.id
                    LEFT JOIN product_last_sale pls ON os.id = pls.product_id
                    LEFT JOIN category_thresholds ct ON p.category_id = ct.category_id
                    WHERE os.quantity - os.reserve > 0
                )
                SELECT
                    COUNT(*) FILTER (WHERE status = 'healthy') as healthy_count,
                    SUM(available_qty) FILTER (WHERE status = 'healthy') as healthy_qty,
                    SUM(stock_value) FILTER (WHERE status = 'healthy') as healthy_value,
                    COUNT(*) FILTER (WHERE status = 'at_risk') as at_risk_count,
                    SUM(available_qty) FILTER (WHERE status = 'at_risk') as at_risk_qty,
                    SUM(stock_value) FILTER (WHERE status = 'at_risk') as at_risk_value,
                    COUNT(*) FILTER (WHERE status = 'dead_stock') as dead_count,
                    SUM(available_qty) FILTER (WHERE status = 'dead_stock') as dead_qty,
                    SUM(stock_value) FILTER (WHERE status = 'dead_stock') as dead_value,
                    COUNT(*) FILTER (WHERE status = 'never_sold') as never_sold_count,
                    SUM(available_qty) FILTER (WHERE status = 'never_sold') as never_sold_qty,
                    SUM(stock_value) FILTER (WHERE status = 'never_sold') as never_sold_value,
                    COUNT(*) as total_skus,
                    SUM(available_qty) as total_qty,
                    SUM(stock_value) as total_value
                FROM stock_status
            """).fetchone()

            total_value = float(summary[14] or 0)

            return {
                "summary": {
                    "healthy": {
                        "skuCount": summary[0] or 0,
                        "quantity": summary[1] or 0,
                        "value": float(summary[2] or 0),
                        "valuePercent": round(float(summary[2] or 0) / total_value * 100, 1) if total_value > 0 else 0,
                    },
                    "atRisk": {
                        "skuCount": summary[3] or 0,
                        "quantity": summary[4] or 0,
                        "value": float(summary[5] or 0),
                        "valuePercent": round(float(summary[5] or 0) / total_value * 100, 1) if total_value > 0 else 0,
                    },
                    "deadStock": {
                        "skuCount": summary[6] or 0,
                        "quantity": summary[7] or 0,
                        "value": float(summary[8] or 0),
                        "valuePercent": round(float(summary[8] or 0) / total_value * 100, 1) if total_value > 0 else 0,
                    },
                    "neverSold": {
                        "skuCount": summary[9] or 0,
                        "quantity": summary[10] or 0,
                        "value": float(summary[11] or 0),
                        "valuePercent": round(float(summary[11] or 0) / total_value * 100, 1) if total_value > 0 else 0,
                    },
                    "total": {
                        "skuCount": summary[12] or 0,
                        "quantity": summary[13] or 0,
                        "value": total_value,
                    },
                },
                "categoryThresholds": list(category_thresholds.values()),
                "items": [
                    {
                        "id": row[0],
                        "sku": row[1],
                        "quantity": row[2],
                        "price": float(row[3] or 0),
                        "value": float(row[4] or 0),
                        "name": row[5],
                        "brand": row[6],
                        "categoryId": row[7],
                        "categoryName": row[8],
                        "lastSaleDate": str(row[9]) if row[9] else None,
                        "daysSinceSale": row[10],
                        "categoryThreshold": int(row[11]) if row[11] else DEFAULT_THRESHOLD,
                        "status": row[12],
                    }
                    for row in dead_stock_items[:100]  # Limit to top 100
                ],
                "methodology": {
                    "description": "Dynamic thresholds calculated per category using Q3 + 1.5×IQR method",
                    "minimumThreshold": 90,
                    "defaultThreshold": DEFAULT_THRESHOLD,
                    "atRiskMultiplier": 0.7,
                    "minimumSampleSize": 5,
                },
            }

    async def update_manager_stats(self) -> int:
        """Update manager order statistics from orders table.

        Updates first_order_date, last_order_date, and order_count for all managers.

        Returns:
            Number of managers updated
        """
        async with self.connection() as conn:
            # Update stats for managers who have orders
            result = conn.execute("""
                UPDATE managers m
                SET
                    first_order_date = stats.first_order,
                    last_order_date = stats.last_order,
                    order_count = stats.order_cnt
                FROM (
                    SELECT
                        manager_id,
                        MIN(DATE(ordered_at)) as first_order,
                        MAX(DATE(ordered_at)) as last_order,
                        COUNT(*) as order_cnt
                    FROM orders
                    WHERE manager_id IS NOT NULL
                    GROUP BY manager_id
                ) stats
                WHERE m.id = stats.manager_id
            """)
            count = result.fetchone()
            logger.info(f"Updated manager statistics")
            return count[0] if count else 0

    async def get_retail_manager_ids(self) -> List[int]:
        """Get list of retail manager IDs from managers table.

        Returns:
            List of manager IDs where is_retail = TRUE
        """
        async with self.connection() as conn:
            result = conn.execute(
                "SELECT id FROM managers WHERE is_retail = TRUE"
            ).fetchall()
            return [row[0] for row in result]

    async def set_manager_retail_status(self, manager_id: int, is_retail: bool) -> None:
        """Update retail status for a specific manager.

        Args:
            manager_id: Manager ID to update
            is_retail: TRUE for retail, FALSE for B2B/other
        """
        async with self.connection() as conn:
            conn.execute(
                "UPDATE managers SET is_retail = ? WHERE id = ?",
                [is_retail, manager_id]
            )
            logger.info(f"Manager {manager_id} retail status set to {is_retail}")

    async def get_all_managers(self) -> List[Dict[str, Any]]:
        """Get all managers with their statistics.

        Returns:
            List of manager dicts with id, name, status, is_retail, order_count, etc.
        """
        async with self.connection() as conn:
            result = conn.execute("""
                SELECT
                    id, name, email, status, is_retail,
                    first_order_date, last_order_date, order_count, synced_at
                FROM managers
                ORDER BY order_count DESC NULLS LAST, name
            """).fetchall()
            return [
                {
                    "id": row[0],
                    "name": row[1],
                    "email": row[2],
                    "status": row[3],
                    "is_retail": row[4],
                    "first_order_date": row[5],
                    "last_order_date": row[6],
                    "order_count": row[7],
                    "synced_at": row[8],
                }
                for row in result
            ]

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
            where_clauses = [f"{_date_in_kyiv('o.ordered_at')} BETWEEN ? AND ?"]

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
                f"{_date_in_kyiv('o.ordered_at')} BETWEEN ? AND ?",
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

            # Query daily revenue and order counts
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

            # Build comparison object for v2 frontend
            comparison = None
            if include_comparison and len(datasets) > 1:
                prev_dataset = datasets[1]
                comparison = {
                    "labels": labels,  # Same labels, different time period
                    "revenue": prev_dataset["data"],
                    "orders": []  # Orders comparison not tracked separately
                }

            # Return both formats: 'datasets' for v1, 'revenue'/'orders'/'comparison' for v2
            result = {
                "labels": labels,
                "revenue": data,
                "orders": orders_data,
                "datasets": datasets  # Keep for backwards compatibility
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
            labels = [self._wrap_label(row[0]) for row in results]
            data = [row[1] for row in results]
            total = sum(data) if data else 1
            percentages = [round(d / total * 100, 1) for d in data]

            return {
                "labels": raw_labels,  # Plain strings for v2 React frontend
                "wrappedLabels": labels,  # Wrapped arrays for v1 Chart.js
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
                f"{_date_in_kyiv('o.ordered_at')} BETWEEN ? AND ?",
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
                    {_date_in_kyiv('o.ordered_at')} as day,
                    AVG(o.grand_total) as avg_order_value,
                    COUNT(DISTINCT o.id) as orders
                FROM orders o
                {joins}
                WHERE {where_sql}
                GROUP BY {_date_in_kyiv('o.ordered_at')}
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

            # CLV Metrics - Calculate Customer Lifetime Value
            # Using 250-day inactivity window (P95=243 days from production analysis)
            INACTIVITY_WINDOW_DAYS = 250

            # Get CLV data: lifespan, purchase frequency, and value for repeat customers
            clv_result = conn.execute(f"""
                WITH customer_stats AS (
                    SELECT
                        o.buyer_id,
                        COUNT(DISTINCT o.id) as order_count,
                        SUM(o.grand_total) as total_spent,
                        MIN(o.ordered_at) as first_order,
                        MAX(o.ordered_at) as last_order,
                        DATE_DIFF('day', MIN(o.ordered_at), MAX(o.ordered_at)) as lifespan_days
                    FROM orders o
                    {joins}
                    WHERE o.buyer_id IS NOT NULL
                      AND o.status_id NOT IN {return_statuses}
                      AND {self._build_sales_type_filter(sales_type)}
                    GROUP BY o.buyer_id
                    HAVING COUNT(DISTINCT o.id) > 1  -- Only repeat customers
                )
                SELECT
                    COUNT(*) as repeat_customer_count,
                    AVG(order_count) as avg_purchase_frequency,
                    AVG(lifespan_days) as avg_lifespan_days,
                    AVG(total_spent) as avg_customer_value,
                    SUM(total_spent) as total_repeat_revenue
                FROM customer_stats
            """).fetchone()

            repeat_customer_count = clv_result[0] or 0
            avg_purchase_frequency = float(clv_result[1] or 0)
            avg_lifespan_days = float(clv_result[2] or 0)
            avg_customer_value = float(clv_result[3] or 0)

            # Calculate CLV using: AOV × Purchase Frequency × (Lifespan in years)
            # Or simpler: Average total revenue per repeat customer
            clv = avg_customer_value if repeat_customer_count > 0 else 0

            # Purchase frequency for all customers in period
            purchase_frequency = total_orders / total_customers if total_customers > 0 else 0

            # All-time customer metrics (true repeat rate)
            alltime_result = conn.execute(f"""
                WITH customer_orders AS (
                    SELECT
                        o.buyer_id,
                        COUNT(DISTINCT o.id) as order_count
                    FROM orders o
                    WHERE o.buyer_id IS NOT NULL
                      AND o.status_id NOT IN {return_statuses}
                      AND {self._build_sales_type_filter(sales_type)}
                    GROUP BY o.buyer_id
                )
                SELECT
                    COUNT(*) as total_customers,
                    SUM(CASE WHEN order_count >= 2 THEN 1 ELSE 0 END) as repeat_customers,
                    AVG(order_count) as avg_orders_per_customer
                FROM customer_orders
            """).fetchone()

            alltime_total_customers = alltime_result[0] or 0
            alltime_repeat_customers = alltime_result[1] or 0
            alltime_avg_orders = float(alltime_result[2] or 0)
            true_repeat_rate = (alltime_repeat_customers / alltime_total_customers * 100) if alltime_total_customers > 0 else 0

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
                    "averageOrderValue": round(overall_aov, 2),
                    # CLV metrics
                    "customerLifetimeValue": round(clv, 2),
                    "avgPurchaseFrequency": round(avg_purchase_frequency, 2),
                    "avgCustomerLifespanDays": round(avg_lifespan_days, 0),
                    "purchaseFrequency": round(purchase_frequency, 2),
                    # All-time metrics
                    "totalCustomersAllTime": alltime_total_customers,
                    "repeatCustomersAllTime": alltime_repeat_customers,
                    "trueRepeatRate": round(true_repeat_rate, 1),
                    "avgOrdersPerCustomer": round(alltime_avg_orders, 2)
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
                f"{_date_in_kyiv('o.ordered_at')} BETWEEN ? AND ?",
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
                "labels": [row[0] or "Unknown" for row in top_results],  # Plain strings for v2
                "wrappedLabels": [self._wrap_label(row[0]) for row in top_results],  # For v1
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
                f"{_date_in_kyiv('o.ordered_at')} BETWEEN ? AND ?",
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
                f"{_date_in_kyiv('o.ordered_at')} BETWEEN ? AND ?",
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
            where_clauses = [f"{_date_in_kyiv('o.ordered_at')} BETWEEN ? AND ?"]

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
                    {_date_in_kyiv('o.ordered_at')} as day,
                    SUM(e.amount) as total_expenses
                FROM expenses e
                JOIN orders o ON e.order_id = o.id
                WHERE {where_sql}
                GROUP BY {_date_in_kyiv('o.ordered_at')}
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
                f"{_date_in_kyiv('o.ordered_at')} BETWEEN ? AND ?",
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
                    {_date_in_kyiv('o.ordered_at')} as day,
                    SUM(o.grand_total) as revenue,
                    COALESCE(SUM(e.amount), 0) as expenses
                FROM orders o
                LEFT JOIN expenses e ON o.id = e.order_id
                WHERE {where_sql}
                GROUP BY {_date_in_kyiv('o.ordered_at')}
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

    # ─── Goal Methods ─────────────────────────────────────────────────────────

    async def get_historical_revenue(
        self,
        period_type: str,
        weeks_back: int = 4,
        sales_type: str = "retail"
    ) -> Dict[str, Any]:
        """
        Get historical revenue data for goal calculation.

        Args:
            period_type: 'daily', 'weekly', or 'monthly'
            weeks_back: Number of weeks of history to analyze
            sales_type: 'retail', 'b2b', or 'all'

        Returns:
            Historical stats including average, min, max, and trend
        """
        async with self.connection() as conn:
            return_statuses = tuple(int(s) for s in OrderStatus.return_statuses())
            sales_filter = self._build_sales_type_filter(sales_type)

            if period_type == "daily":
                # Get daily averages for the same day of week over past N weeks
                sql = f"""
                    WITH daily_revenue AS (
                        SELECT
                            {_date_in_kyiv('o.ordered_at')} as day,
                            SUM(o.grand_total) as revenue
                        FROM orders o
                        WHERE {_date_in_kyiv('o.ordered_at')} >= CURRENT_DATE - INTERVAL '{weeks_back * 7} days'
                            AND {_date_in_kyiv('o.ordered_at')} < CURRENT_DATE
                            AND o.status_id NOT IN {return_statuses}
                            AND {sales_filter}
                        GROUP BY {_date_in_kyiv('o.ordered_at')}
                    )
                    SELECT
                        AVG(revenue) as avg_revenue,
                        MIN(revenue) as min_revenue,
                        MAX(revenue) as max_revenue,
                        COUNT(*) as days_count,
                        STDDEV(revenue) as std_dev
                    FROM daily_revenue
                """
            elif period_type == "weekly":
                # Get weekly totals for past N weeks
                sql = f"""
                    WITH weekly_revenue AS (
                        SELECT
                            DATE_TRUNC('week', {_date_in_kyiv('o.ordered_at')}) as week_start,
                            SUM(o.grand_total) as revenue
                        FROM orders o
                        WHERE {_date_in_kyiv('o.ordered_at')} >= CURRENT_DATE - INTERVAL '{weeks_back} weeks'
                            AND {_date_in_kyiv('o.ordered_at')} < DATE_TRUNC('week', CURRENT_DATE)
                            AND o.status_id NOT IN {return_statuses}
                            AND {sales_filter}
                        GROUP BY DATE_TRUNC('week', {_date_in_kyiv('o.ordered_at')})
                    )
                    SELECT
                        AVG(revenue) as avg_revenue,
                        MIN(revenue) as min_revenue,
                        MAX(revenue) as max_revenue,
                        COUNT(*) as weeks_count,
                        STDDEV(revenue) as std_dev
                    FROM weekly_revenue
                """
            else:  # monthly
                # Get monthly totals for past N months
                months_back = max(3, weeks_back // 4)
                sql = f"""
                    WITH monthly_revenue AS (
                        SELECT
                            DATE_TRUNC('month', {_date_in_kyiv('o.ordered_at')}) as month_start,
                            SUM(o.grand_total) as revenue
                        FROM orders o
                        WHERE {_date_in_kyiv('o.ordered_at')} >= CURRENT_DATE - INTERVAL '{months_back} months'
                            AND {_date_in_kyiv('o.ordered_at')} < DATE_TRUNC('month', CURRENT_DATE)
                            AND o.status_id NOT IN {return_statuses}
                            AND {sales_filter}
                        GROUP BY DATE_TRUNC('month', {_date_in_kyiv('o.ordered_at')})
                    )
                    SELECT
                        AVG(revenue) as avg_revenue,
                        MIN(revenue) as min_revenue,
                        MAX(revenue) as max_revenue,
                        COUNT(*) as months_count,
                        STDDEV(revenue) as std_dev
                    FROM monthly_revenue
                """

            result = conn.execute(sql).fetchone()

            avg_revenue = float(result[0] or 0)
            min_revenue = float(result[1] or 0)
            max_revenue = float(result[2] or 0)
            period_count = result[3] or 0
            std_dev = float(result[4] or 0)

            # Calculate trend (compare recent vs older periods)
            trend = 0.0
            if period_type == "weekly" and period_count >= 4:
                trend_sql = f"""
                    WITH weekly_revenue AS (
                        SELECT
                            DATE_TRUNC('week', {_date_in_kyiv('o.ordered_at')}) as week_start,
                            SUM(o.grand_total) as revenue,
                            ROW_NUMBER() OVER (ORDER BY DATE_TRUNC('week', {_date_in_kyiv('o.ordered_at')}) DESC) as week_num
                        FROM orders o
                        WHERE {_date_in_kyiv('o.ordered_at')} >= CURRENT_DATE - INTERVAL '{weeks_back} weeks'
                            AND {_date_in_kyiv('o.ordered_at')} < DATE_TRUNC('week', CURRENT_DATE)
                            AND o.status_id NOT IN {return_statuses}
                            AND {sales_filter}
                        GROUP BY DATE_TRUNC('week', {_date_in_kyiv('o.ordered_at')})
                    )
                    SELECT
                        AVG(CASE WHEN week_num <= 2 THEN revenue END) as recent_avg,
                        AVG(CASE WHEN week_num > 2 THEN revenue END) as older_avg
                    FROM weekly_revenue
                """
                trend_result = conn.execute(trend_sql).fetchone()
                recent = float(trend_result[0] or 0)
                older = float(trend_result[1] or 0)
                if older > 0:
                    trend = ((recent - older) / older) * 100

            return {
                "periodType": period_type,
                "average": round(avg_revenue, 2),
                "min": round(min_revenue, 2),
                "max": round(max_revenue, 2),
                "periodCount": period_count,
                "stdDev": round(std_dev, 2),
                "trend": round(trend, 1),  # % change
                "weeksAnalyzed": weeks_back
            }

    async def calculate_suggested_goals(
        self,
        sales_type: str = "retail",
        growth_factor: float = 1.10
    ) -> Dict[str, Dict[str, Any]]:
        """
        Calculate suggested goals based on historical performance.

        Uses average of past 4 weeks × growth factor.

        Args:
            sales_type: 'retail', 'b2b', or 'all'
            growth_factor: Target growth multiplier (1.10 = 10% growth)

        Returns:
            Suggested goals for daily, weekly, and monthly periods
        """
        suggestions = {}

        for period_type in ["daily", "weekly", "monthly"]:
            history = await self.get_historical_revenue(period_type, weeks_back=4, sales_type=sales_type)

            # Base suggestion on average + growth factor
            suggested = history["average"] * growth_factor

            # Round to nice numbers
            if period_type == "daily":
                # Round to nearest 10K
                suggested = round(suggested / 10000) * 10000
            elif period_type == "weekly":
                # Round to nearest 50K
                suggested = round(suggested / 50000) * 50000
            else:  # monthly
                # Round to nearest 100K
                suggested = round(suggested / 100000) * 100000

            # Ensure minimum reasonable goal
            min_goals = {"daily": 50000, "weekly": 300000, "monthly": 1000000}
            suggested = max(suggested, min_goals[period_type])

            suggestions[period_type] = {
                "suggested": suggested,
                "basedOnAverage": history["average"],
                "growthFactor": growth_factor,
                "trend": history["trend"],
                "confidence": "high" if history["periodCount"] >= 4 else "medium" if history["periodCount"] >= 2 else "low"
            }

        return suggestions

    async def get_goals(self, sales_type: str = "retail") -> Dict[str, Dict[str, Any]]:
        """
        Get current revenue goals.

        Returns stored goals if set, otherwise returns calculated suggestions.

        Args:
            sales_type: 'retail', 'b2b', or 'all'

        Returns:
            Goals for daily, weekly, and monthly periods
        """
        async with self.connection() as conn:
            # Get stored goals
            results = conn.execute("""
                SELECT period_type, goal_amount, is_custom, calculated_goal, growth_factor
                FROM revenue_goals
            """).fetchall()

            stored_goals = {
                row[0]: {
                    "amount": float(row[1]),
                    "isCustom": row[2],
                    "calculatedGoal": float(row[3]) if row[3] else None,
                    "growthFactor": float(row[4]) if row[4] else 1.10
                }
                for row in results
            }

        # Calculate current suggestions
        suggestions = await self.calculate_suggested_goals(sales_type)

        # Merge stored goals with suggestions
        goals = {}
        for period_type in ["daily", "weekly", "monthly"]:
            if period_type in stored_goals:
                stored = stored_goals[period_type]
                goals[period_type] = {
                    "amount": stored["amount"],
                    "isCustom": stored["isCustom"],
                    "suggestedAmount": suggestions[period_type]["suggested"],
                    "basedOnAverage": suggestions[period_type]["basedOnAverage"],
                    "trend": suggestions[period_type]["trend"],
                    "confidence": suggestions[period_type]["confidence"]
                }
            else:
                # No stored goal - use suggestion
                goals[period_type] = {
                    "amount": suggestions[period_type]["suggested"],
                    "isCustom": False,
                    "suggestedAmount": suggestions[period_type]["suggested"],
                    "basedOnAverage": suggestions[period_type]["basedOnAverage"],
                    "trend": suggestions[period_type]["trend"],
                    "confidence": suggestions[period_type]["confidence"]
                }

        return goals

    async def set_goal(
        self,
        period_type: str,
        amount: float,
        is_custom: bool = True,
        growth_factor: float = 1.10
    ) -> Dict[str, Any]:
        """
        Set a revenue goal.

        Args:
            period_type: 'daily', 'weekly', or 'monthly'
            amount: Goal amount in UAH
            is_custom: True if manually set, False if auto-calculated
            growth_factor: Growth factor used for calculation

        Returns:
            Updated goal data
        """
        if period_type not in ["daily", "weekly", "monthly"]:
            raise ValueError(f"Invalid period_type: {period_type}")

        # Calculate what the system would suggest (for reference)
        suggestions = await self.calculate_suggested_goals()
        calculated = suggestions[period_type]["suggested"]

        async with self.connection() as conn:
            now = datetime.now(DEFAULT_TZ)
            conn.execute("""
                INSERT INTO revenue_goals (period_type, goal_amount, is_custom, calculated_goal, growth_factor, updated_at)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT (period_type) DO UPDATE SET
                    goal_amount = excluded.goal_amount,
                    is_custom = excluded.is_custom,
                    calculated_goal = excluded.calculated_goal,
                    growth_factor = excluded.growth_factor,
                    updated_at = excluded.updated_at
            """, [period_type, amount, is_custom, calculated, growth_factor, now])

        return {
            "periodType": period_type,
            "amount": amount,
            "isCustom": is_custom,
            "calculatedGoal": calculated,
            "growthFactor": growth_factor
        }

    async def reset_goal_to_auto(self, period_type: str, sales_type: str = "retail") -> Dict[str, Any]:
        """
        Reset a goal to auto-calculated value.

        Args:
            period_type: 'daily', 'weekly', or 'monthly'
            sales_type: 'retail', 'b2b', or 'all'

        Returns:
            Updated goal data
        """
        suggestions = await self.calculate_suggested_goals(sales_type)
        suggested = suggestions[period_type]["suggested"]

        return await self.set_goal(
            period_type=period_type,
            amount=suggested,
            is_custom=False,
            growth_factor=1.10
        )

    # ─── Smart Seasonality Methods ─────────────────────────────────────────────

    async def calculate_seasonality_indices(self, sales_type: str = "retail") -> Dict[int, Dict[str, Any]]:
        """
        Calculate monthly seasonality indices from historical data.

        Analyzes all available historical data to determine how each month
        performs relative to the annual average.

        Args:
            sales_type: 'retail', 'b2b', or 'all'

        Returns:
            Dictionary mapping month (1-12) to seasonality data
        """
        async with self.connection() as conn:
            return_statuses = tuple(int(s) for s in OrderStatus.return_statuses())
            sales_filter = self._build_sales_type_filter(sales_type)

            # Get monthly revenue totals for all available history (only complete months with 25+ days)
            sql = f"""
                WITH monthly_data AS (
                    SELECT
                        EXTRACT(YEAR FROM {_date_in_kyiv('o.ordered_at')}) as year,
                        EXTRACT(MONTH FROM {_date_in_kyiv('o.ordered_at')}) as month,
                        SUM(o.grand_total) as revenue,
                        COUNT(DISTINCT DATE({_date_in_kyiv('o.ordered_at')})) as days_with_orders
                    FROM orders o
                    WHERE o.status_id NOT IN {return_statuses}
                        AND {sales_filter}
                    GROUP BY
                        EXTRACT(YEAR FROM {_date_in_kyiv('o.ordered_at')}),
                        EXTRACT(MONTH FROM {_date_in_kyiv('o.ordered_at')})
                    HAVING COUNT(DISTINCT DATE({_date_in_kyiv('o.ordered_at')})) >= 25
                ),
                monthly_stats AS (
                    SELECT
                        month,
                        AVG(revenue) as avg_revenue,
                        MIN(revenue) as min_revenue,
                        MAX(revenue) as max_revenue,
                        COUNT(*) as sample_size,
                        STDDEV(revenue) as std_dev
                    FROM monthly_data
                    GROUP BY month
                ),
                overall_avg AS (
                    SELECT AVG(avg_revenue) as grand_avg FROM monthly_stats
                )
                SELECT
                    ms.month,
                    ms.avg_revenue,
                    ms.min_revenue,
                    ms.max_revenue,
                    ms.sample_size,
                    ms.std_dev,
                    ms.avg_revenue / oa.grand_avg as seasonality_index
                FROM monthly_stats ms, overall_avg oa
                ORDER BY ms.month
            """

            results = conn.execute(sql).fetchall()

            indices = {}
            for row in results:
                month = int(row[0])
                sample_size = row[4] or 0

                # Determine confidence based on sample size
                if sample_size >= 3:
                    confidence = "high"
                elif sample_size >= 2:
                    confidence = "medium"
                else:
                    confidence = "low"

                indices[month] = {
                    "month": month,
                    "avg_revenue": round(float(row[1] or 0), 2),
                    "min_revenue": round(float(row[2] or 0), 2),
                    "max_revenue": round(float(row[3] or 0), 2),
                    "sample_size": sample_size,
                    "std_dev": round(float(row[5] or 0), 2),
                    "seasonality_index": round(float(row[6] or 1.0), 4),
                    "confidence": confidence
                }

            # Store in database for caching
            now = datetime.now(DEFAULT_TZ)
            for month, data in indices.items():
                conn.execute("""
                    INSERT INTO seasonal_indices
                    (month, seasonality_index, sample_size, avg_revenue, min_revenue, max_revenue, confidence, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT (month) DO UPDATE SET
                        seasonality_index = excluded.seasonality_index,
                        sample_size = excluded.sample_size,
                        avg_revenue = excluded.avg_revenue,
                        min_revenue = excluded.min_revenue,
                        max_revenue = excluded.max_revenue,
                        confidence = excluded.confidence,
                        updated_at = excluded.updated_at
                """, [
                    month,
                    data["seasonality_index"],
                    data["sample_size"],
                    data["avg_revenue"],
                    data["min_revenue"],
                    data["max_revenue"],
                    data["confidence"],
                    now
                ])

            logger.info(f"Calculated seasonality indices for {len(indices)} months")
            return indices

    async def calculate_yoy_growth(self, sales_type: str = "retail") -> Dict[str, Any]:
        """
        Calculate year-over-year growth rate.

        Compares revenue between consecutive years to determine growth trend.

        Args:
            sales_type: 'retail', 'b2b', or 'all'

        Returns:
            Growth metrics including overall YoY, monthly YoY, and trend slope
        """
        async with self.connection() as conn:
            return_statuses = tuple(int(s) for s in OrderStatus.return_statuses())
            sales_filter = self._build_sales_type_filter(sales_type)

            # Get yearly totals
            yearly_sql = f"""
                SELECT
                    EXTRACT(YEAR FROM {_date_in_kyiv('o.ordered_at')}) as year,
                    SUM(o.grand_total) as revenue
                FROM orders o
                WHERE o.status_id NOT IN {return_statuses}
                    AND {sales_filter}
                GROUP BY EXTRACT(YEAR FROM {_date_in_kyiv('o.ordered_at')})
                ORDER BY year
            """
            yearly_results = conn.execute(yearly_sql).fetchall()

            # Calculate YoY growth between consecutive years
            yoy_rates = []
            for i in range(1, len(yearly_results)):
                prev_year = yearly_results[i-1][1]
                curr_year = yearly_results[i][1]
                if prev_year > 0:
                    yoy_rate = (curr_year - prev_year) / prev_year
                    yoy_rates.append(yoy_rate)

            overall_yoy = sum(yoy_rates) / len(yoy_rates) if yoy_rates else 0.10

            # Calculate monthly YoY for each month
            monthly_yoy_sql = f"""
                WITH monthly_by_year AS (
                    SELECT
                        EXTRACT(YEAR FROM {_date_in_kyiv('o.ordered_at')}) as year,
                        EXTRACT(MONTH FROM {_date_in_kyiv('o.ordered_at')}) as month,
                        SUM(o.grand_total) as revenue
                    FROM orders o
                    WHERE o.status_id NOT IN {return_statuses}
                        AND {sales_filter}
                    GROUP BY
                        EXTRACT(YEAR FROM {_date_in_kyiv('o.ordered_at')}),
                        EXTRACT(MONTH FROM {_date_in_kyiv('o.ordered_at')})
                )
                SELECT
                    curr.month,
                    AVG((curr.revenue - prev.revenue) / NULLIF(prev.revenue, 0)) as avg_yoy_growth
                FROM monthly_by_year curr
                JOIN monthly_by_year prev
                    ON curr.month = prev.month
                    AND curr.year = prev.year + 1
                GROUP BY curr.month
                ORDER BY curr.month
            """
            monthly_yoy_results = conn.execute(monthly_yoy_sql).fetchall()

            monthly_yoy = {
                int(row[0]): round(float(row[1] or 0), 4)
                for row in monthly_yoy_results
            }

            # Store metrics
            min_date = conn.execute(f"SELECT MIN({_date_in_kyiv('ordered_at')}) FROM orders").fetchone()[0]
            max_date = conn.execute(f"SELECT MAX({_date_in_kyiv('ordered_at')}) FROM orders").fetchone()[0]
            now = datetime.now(DEFAULT_TZ)

            conn.execute("""
                INSERT INTO growth_metrics (metric_type, value, period_start, period_end, sample_size, updated_at)
                VALUES ('yoy_overall', ?, ?, ?, ?, ?)
                ON CONFLICT (metric_type) DO UPDATE SET
                    value = excluded.value,
                    period_start = excluded.period_start,
                    period_end = excluded.period_end,
                    sample_size = excluded.sample_size,
                    updated_at = excluded.updated_at
            """, [overall_yoy, min_date, max_date, len(yoy_rates), now])

            # Update seasonal_indices with monthly YoY
            for month, yoy in monthly_yoy.items():
                conn.execute("""
                    UPDATE seasonal_indices
                    SET yoy_growth = ?, updated_at = ?
                    WHERE month = ?
                """, [yoy, now, month])

            logger.info(f"Calculated YoY growth: {overall_yoy:.2%}")
            return {
                "overall_yoy": round(overall_yoy, 4),
                "monthly_yoy": monthly_yoy,
                "yearly_data": [
                    {"year": int(row[0]), "revenue": round(float(row[1]), 2)}
                    for row in yearly_results
                ],
                "sample_size": len(yoy_rates)
            }

    async def calculate_weekly_patterns(self, sales_type: str = "retail") -> Dict[int, Dict[int, float]]:
        """
        Calculate how revenue distributes across weeks within each month.

        Args:
            sales_type: 'retail', 'b2b', or 'all'

        Returns:
            Dictionary mapping month -> week_of_month -> weight (percentage)
        """
        async with self.connection() as conn:
            return_statuses = tuple(int(s) for s in OrderStatus.return_statuses())
            sales_filter = self._build_sales_type_filter(sales_type)

            # Calculate weekly revenue within each month instance
            sql = f"""
                WITH weekly_data AS (
                    SELECT
                        EXTRACT(YEAR FROM {_date_in_kyiv('o.ordered_at')}) as year,
                        EXTRACT(MONTH FROM {_date_in_kyiv('o.ordered_at')}) as month,
                        -- Week of month: 1-5 based on day of month
                        LEAST(5, CEIL(EXTRACT(DAY FROM {_date_in_kyiv('o.ordered_at')}) / 7.0)::int) as week_of_month,
                        SUM(o.grand_total) as revenue
                    FROM orders o
                    WHERE o.status_id NOT IN {return_statuses}
                        AND {sales_filter}
                    GROUP BY
                        EXTRACT(YEAR FROM {_date_in_kyiv('o.ordered_at')}),
                        EXTRACT(MONTH FROM {_date_in_kyiv('o.ordered_at')}),
                        LEAST(5, CEIL(EXTRACT(DAY FROM {_date_in_kyiv('o.ordered_at')}) / 7.0)::int)
                ),
                monthly_totals AS (
                    SELECT year, month, SUM(revenue) as month_total
                    FROM weekly_data
                    GROUP BY year, month
                ),
                weekly_weights AS (
                    SELECT
                        wd.month,
                        wd.week_of_month,
                        AVG(wd.revenue / NULLIF(mt.month_total, 0)) as avg_weight,
                        COUNT(*) as sample_size
                    FROM weekly_data wd
                    JOIN monthly_totals mt ON wd.year = mt.year AND wd.month = mt.month
                    GROUP BY wd.month, wd.week_of_month
                )
                SELECT month, week_of_month, avg_weight, sample_size
                FROM weekly_weights
                ORDER BY month, week_of_month
            """

            results = conn.execute(sql).fetchall()

            patterns = {}
            now = datetime.now(DEFAULT_TZ)
            for row in results:
                month = int(row[0])
                week = int(row[1])
                weight = float(row[2] or 0.25)  # Default to 25% if no data
                sample_size = int(row[3])

                if month not in patterns:
                    patterns[month] = {}
                patterns[month][week] = round(weight, 4)

                # Store in database
                conn.execute("""
                    INSERT INTO weekly_patterns (month, week_of_month, weight, sample_size, updated_at)
                    VALUES (?, ?, ?, ?, ?)
                    ON CONFLICT (month, week_of_month) DO UPDATE SET
                        weight = excluded.weight,
                        sample_size = excluded.sample_size,
                        updated_at = excluded.updated_at
                """, [month, week, weight, sample_size, now])

            # Ensure all months have all 5 weeks (fill missing with equal distribution)
            for month in range(1, 13):
                if month not in patterns:
                    patterns[month] = {}
                for week in range(1, 6):
                    if week not in patterns[month]:
                        # Default: slightly more revenue in weeks 1-4, less in week 5
                        default_weights = {1: 0.23, 2: 0.23, 3: 0.23, 4: 0.23, 5: 0.08}
                        patterns[month][week] = default_weights[week]

            logger.info(f"Calculated weekly patterns for {len(patterns)} months")
            return patterns

    async def generate_smart_goals(
        self,
        target_year: int,
        target_month: int,
        sales_type: str = "retail",
        recalculate: bool = False
    ) -> Dict[str, Any]:
        """
        Generate smart goals for a target month using seasonality and growth.

        Algorithm:
        1. Get last year's same month revenue as baseline
        2. Apply YoY growth rate
        3. Adjust using seasonality index
        4. Calculate weekly breakdown using weekly patterns

        Args:
            target_year: Year to generate goals for
            target_month: Month (1-12) to generate goals for
            sales_type: 'retail', 'b2b', or 'all'
            recalculate: Force recalculation of indices

        Returns:
            Smart goals with monthly total and weekly breakdown
        """
        async with self.connection() as conn:
            # Recalculate indices if needed or not cached
            indices_exist = conn.execute(
                "SELECT COUNT(*) FROM seasonal_indices"
            ).fetchone()[0]

            if recalculate or indices_exist < 12:
                await self.calculate_seasonality_indices(sales_type)
                await self.calculate_yoy_growth(sales_type)
                await self.calculate_weekly_patterns(sales_type)

            # Cap growth rate to reasonable maximum (35%)
            # Used as default when no historical data available
            MAX_GROWTH_RATE = 0.35

            # Get seasonality index for target month
            seasonality_result = conn.execute("""
                SELECT seasonality_index, avg_revenue, yoy_growth, confidence
                FROM seasonal_indices
                WHERE month = ?
            """, [target_month]).fetchone()

            if seasonality_result:
                seasonality_index = float(seasonality_result[0] or 1.0)
                historical_avg = float(seasonality_result[1] or 0)
                monthly_yoy = float(seasonality_result[2] or MAX_GROWTH_RATE)
                confidence = seasonality_result[3] or "low"
            else:
                seasonality_index = 1.0
                historical_avg = 0
                monthly_yoy = MAX_GROWTH_RATE
                confidence = "low"

            # Get overall YoY growth
            yoy_result = conn.execute("""
                SELECT value FROM growth_metrics WHERE metric_type = 'yoy_overall'
            """).fetchone()
            overall_yoy = float(yoy_result[0] or MAX_GROWTH_RATE) if yoy_result else MAX_GROWTH_RATE

            # Get last year's same month revenue
            return_statuses = tuple(int(s) for s in OrderStatus.return_statuses())
            sales_filter = self._build_sales_type_filter(sales_type)

            last_year_sql = f"""
                SELECT SUM(o.grand_total) as revenue
                FROM orders o
                WHERE EXTRACT(YEAR FROM {_date_in_kyiv('o.ordered_at')}) = ?
                    AND EXTRACT(MONTH FROM {_date_in_kyiv('o.ordered_at')}) = ?
                    AND o.status_id NOT IN {return_statuses}
                    AND {sales_filter}
            """
            last_year_result = conn.execute(last_year_sql, [target_year - 1, target_month]).fetchone()
            last_year_revenue = float(last_year_result[0] or 0) if last_year_result[0] else 0

            # Get recent 3-month average (last 3 complete months with at least 25 days of data)
            recent_avg_sql = f"""
                WITH monthly_revenue AS (
                    SELECT
                        EXTRACT(YEAR FROM {_date_in_kyiv('o.ordered_at')}) as year,
                        EXTRACT(MONTH FROM {_date_in_kyiv('o.ordered_at')}) as month,
                        SUM(o.grand_total) as revenue,
                        COUNT(DISTINCT DATE({_date_in_kyiv('o.ordered_at')})) as days_with_orders
                    FROM orders o
                    WHERE o.status_id NOT IN {return_statuses}
                        AND {sales_filter}
                        AND {_date_in_kyiv('o.ordered_at')} < DATE_TRUNC('month', CURRENT_DATE)
                    GROUP BY
                        EXTRACT(YEAR FROM {_date_in_kyiv('o.ordered_at')}),
                        EXTRACT(MONTH FROM {_date_in_kyiv('o.ordered_at')})
                    HAVING COUNT(DISTINCT DATE({_date_in_kyiv('o.ordered_at')})) >= 25
                    ORDER BY year DESC, month DESC
                    LIMIT 3
                )
                SELECT AVG(revenue) as avg_revenue FROM monthly_revenue
            """
            recent_avg_result = conn.execute(recent_avg_sql).fetchone()
            recent_3_month_avg = float(recent_avg_result[0] or 0) if recent_avg_result[0] else 0

            # Calculate goals using both methods
            yoy_goal = 0
            recent_goal = 0

            # Apply growth rate cap
            raw_growth_rate = monthly_yoy if monthly_yoy > 0 else overall_yoy
            growth_rate = min(raw_growth_rate, MAX_GROWTH_RATE)

            # Method 1: YoY growth (last year same month × growth)
            if last_year_revenue > 0:
                yoy_goal = last_year_revenue * (1 + growth_rate)

            # Method 2: Recent baseline adjusted for seasonality
            # recent_3_month_avg × seasonality_index
            # seasonality_index < 1 means this month is typically below average
            # seasonality_index > 1 means this month is typically above average
            if recent_3_month_avg > 0 and seasonality_index > 0:
                recent_goal = recent_3_month_avg * seasonality_index

            # Take the MAX of both methods (never set goal below recent performance)
            if yoy_goal > 0 and recent_goal > 0:
                monthly_goal = max(yoy_goal, recent_goal)
                calculation_method = "yoy_growth" if yoy_goal >= recent_goal else "recent_trend"
            elif recent_goal > 0:
                monthly_goal = recent_goal
                calculation_method = "recent_trend"
            elif yoy_goal > 0:
                monthly_goal = yoy_goal
                calculation_method = "yoy_growth"
            elif historical_avg > 0:
                monthly_goal = historical_avg * (1 + growth_rate)
                calculation_method = "historical_avg"
            else:
                monthly_goal = 3000000  # 3M UAH default
                growth_rate = MAX_GROWTH_RATE
                calculation_method = "fallback"

            # Round to nice number
            monthly_goal = round(monthly_goal / 100000) * 100000

            # Get weekly patterns for this month
            weekly_patterns = conn.execute("""
                SELECT week_of_month, weight
                FROM weekly_patterns
                WHERE month = ?
                ORDER BY week_of_month
            """, [target_month]).fetchall()

            if weekly_patterns:
                weekly_weights = {int(row[0]): float(row[1]) for row in weekly_patterns}
            else:
                # Default distribution
                weekly_weights = {1: 0.23, 2: 0.23, 3: 0.23, 4: 0.23, 5: 0.08}

            # Normalize weights to sum to 1
            total_weight = sum(weekly_weights.values())
            if total_weight > 0:
                weekly_weights = {k: v / total_weight for k, v in weekly_weights.items()}

            # Calculate weekly goals
            weekly_goals = {
                week: round(monthly_goal * weight / 10000) * 10000
                for week, weight in weekly_weights.items()
            }

            # Calculate daily goal (monthly / ~22 working days or 30 days)
            days_in_month = 30 if target_month in [4, 6, 9, 11] else 31 if target_month != 2 else 28
            daily_goal = round(monthly_goal / days_in_month / 10000) * 10000

            # Calculate weekly goal (monthly / 4.3 weeks on average)
            weekly_goal = round(monthly_goal / 4.3 / 50000) * 50000

            return {
                "targetYear": target_year,
                "targetMonth": target_month,
                "monthly": {
                    "goal": monthly_goal,
                    "lastYearRevenue": round(last_year_revenue, 2),
                    "recent3MonthAvg": round(recent_3_month_avg, 2),
                    "historicalAvg": round(historical_avg, 2),
                    "yoyGoal": round(yoy_goal, 2),
                    "recentGoal": round(recent_goal, 2),
                    "growthRate": round(growth_rate, 4),
                    "seasonalityIndex": seasonality_index,
                    "confidence": confidence,
                    "calculationMethod": calculation_method
                },
                "weekly": {
                    "goal": weekly_goal,  # Average weekly goal
                    "breakdown": weekly_goals,
                    "weights": weekly_weights
                },
                "daily": {
                    "goal": daily_goal,
                    "daysInMonth": days_in_month
                },
                "metadata": {
                    "overallYoY": round(overall_yoy, 4),
                    "monthlyYoY": round(monthly_yoy, 4),
                    "calculatedAt": datetime.now(DEFAULT_TZ).isoformat()
                }
            }

    async def get_smart_goals(self, sales_type: str = "retail") -> Dict[str, Dict[str, Any]]:
        """
        Get smart goals for the current period using seasonality.

        This is an enhanced version of get_goals() that uses the smart
        goal generation system with seasonality and growth factors.

        Args:
            sales_type: 'retail', 'b2b', or 'all'

        Returns:
            Goals for daily, weekly, and monthly periods with calculation details
        """
        now = datetime.now(DEFAULT_TZ)
        current_year = now.year
        current_month = now.month

        # Generate smart goals for current month
        smart = await self.generate_smart_goals(current_year, current_month, sales_type)

        # Check for custom overrides
        async with self.connection() as conn:
            stored_goals = conn.execute("""
                SELECT period_type, goal_amount, is_custom
                FROM revenue_goals
                WHERE is_custom = TRUE
            """).fetchall()
            custom_goals = {row[0]: float(row[1]) for row in stored_goals if row[2]}

        return {
            "daily": {
                "amount": custom_goals.get("daily", smart["daily"]["goal"]),
                "isCustom": "daily" in custom_goals,
                "suggestedAmount": smart["daily"]["goal"],
                "basedOnAverage": smart["monthly"]["historicalAvg"] / 30,
                "trend": smart["metadata"]["monthlyYoY"] * 100,
                "confidence": smart["monthly"]["confidence"]
            },
            "weekly": {
                "amount": custom_goals.get("weekly", smart["weekly"]["goal"]),
                "isCustom": "weekly" in custom_goals,
                "suggestedAmount": smart["weekly"]["goal"],
                "basedOnAverage": smart["monthly"]["historicalAvg"] / 4.3,
                "trend": smart["metadata"]["monthlyYoY"] * 100,
                "confidence": smart["monthly"]["confidence"],
                "weeklyBreakdown": smart["weekly"]["breakdown"]
            },
            "monthly": {
                "amount": custom_goals.get("monthly", smart["monthly"]["goal"]),
                "isCustom": "monthly" in custom_goals,
                "suggestedAmount": smart["monthly"]["goal"],
                "basedOnAverage": smart["monthly"]["historicalAvg"],
                "lastYearRevenue": smart["monthly"]["lastYearRevenue"],
                "growthRate": smart["monthly"]["growthRate"],
                "seasonalityIndex": smart["monthly"]["seasonalityIndex"],
                "trend": smart["metadata"]["monthlyYoY"] * 100,
                "confidence": smart["monthly"]["confidence"],
                "calculationMethod": smart["monthly"]["calculationMethod"]
            },
            "metadata": smart["metadata"]
        }

    # ─── Stats Methods ────────────────────────────────────────────────────────

    async def get_latest_order_time(self) -> Optional[datetime]:
        """Get the latest order updated_at timestamp for sync checkpoint."""
        async with self.connection() as conn:
            result = conn.execute("SELECT MAX(updated_at) FROM orders").fetchone()
            return result[0] if result and result[0] else None

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
