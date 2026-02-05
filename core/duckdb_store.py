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
from core.exceptions import QueryTimeoutError

logger = logging.getLogger(__name__)

# Query timeout settings
DEFAULT_QUERY_TIMEOUT = 30.0  # seconds
LONG_QUERY_TIMEOUT = 120.0   # for sync operations

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

    # ─── Query Execution with Timeout ────────────────────────────────────────

    async def _execute_with_timeout(
        self,
        query: str,
        params: list = None,
        timeout: float = DEFAULT_QUERY_TIMEOUT,
    ) -> None:
        """
        Execute a query with timeout (for INSERT/UPDATE/DELETE).

        Args:
            query: SQL query string
            params: Query parameters
            timeout: Timeout in seconds

        Raises:
            QueryTimeoutError: If query exceeds timeout
        """
        async with self.connection() as conn:
            try:
                await asyncio.wait_for(
                    asyncio.to_thread(conn.execute, query, params or []),
                    timeout=timeout
                )
            except asyncio.TimeoutError:
                raise QueryTimeoutError(query, timeout, "Execute failed")

    async def _fetch_one(
        self,
        query: str,
        params: list = None,
        timeout: float = DEFAULT_QUERY_TIMEOUT,
    ) -> Optional[tuple]:
        """
        Execute query and fetch one result with timeout.

        Args:
            query: SQL query string
            params: Query parameters
            timeout: Timeout in seconds

        Returns:
            Single row tuple or None

        Raises:
            QueryTimeoutError: If query exceeds timeout
        """
        async with self.connection() as conn:
            try:
                def _run():
                    return conn.execute(query, params or []).fetchone()

                return await asyncio.wait_for(
                    asyncio.to_thread(_run),
                    timeout=timeout
                )
            except asyncio.TimeoutError:
                raise QueryTimeoutError(query, timeout, "Fetch one failed")

    async def _fetch_all(
        self,
        query: str,
        params: list = None,
        timeout: float = DEFAULT_QUERY_TIMEOUT,
    ) -> List[tuple]:
        """
        Execute query and fetch all results with timeout.

        Args:
            query: SQL query string
            params: Query parameters
            timeout: Timeout in seconds

        Returns:
            List of row tuples

        Raises:
            QueryTimeoutError: If query exceeds timeout
        """
        async with self.connection() as conn:
            try:
                def _run():
                    return conn.execute(query, params or []).fetchall()

                return await asyncio.wait_for(
                    asyncio.to_thread(_run),
                    timeout=timeout
                )
            except asyncio.TimeoutError:
                raise QueryTimeoutError(query, timeout, "Fetch all failed")

    async def _fetch_df(
        self,
        query: str,
        params: list = None,
        timeout: float = DEFAULT_QUERY_TIMEOUT,
    ) -> "pd.DataFrame":
        """
        Execute query and return DataFrame with timeout.

        Args:
            query: SQL query string
            params: Query parameters
            timeout: Timeout in seconds

        Returns:
            pandas DataFrame

        Raises:
            QueryTimeoutError: If query exceeds timeout
        """
        async with self.connection() as conn:
            try:
                def _run():
                    return conn.execute(query, params or []).fetchdf()

                return await asyncio.wait_for(
                    asyncio.to_thread(_run),
                    timeout=timeout
                )
            except asyncio.TimeoutError:
                raise QueryTimeoutError(query, timeout, "Fetch DataFrame failed")

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

        -- Offers (product variations - links offer_id to product_id)
        CREATE TABLE IF NOT EXISTS offers (
            id INTEGER PRIMARY KEY,              -- offer_id from KeyCRM
            product_id INTEGER NOT NULL,         -- links to products.id
            sku VARCHAR,
            synced_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
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

        -- Inventory history (daily snapshots for average calculation) - DEPRECATED
        -- Kept for backwards compatibility, will be replaced by inventory_sku_history
        CREATE TABLE IF NOT EXISTS inventory_history (
            date DATE NOT NULL,
            total_quantity INTEGER NOT NULL,
            total_value DECIMAL(14, 2) NOT NULL,
            total_reserve INTEGER DEFAULT 0,
            sku_count INTEGER DEFAULT 0,
            recorded_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (date)
        );

        -- ═══════════════════════════════════════════════════════════════════════
        -- LAYER 1: SKU Inventory Status (current state per SKU)
        -- ═══════════════════════════════════════════════════════════════════════
        CREATE TABLE IF NOT EXISTS sku_inventory_status (
            offer_id INTEGER PRIMARY KEY,
            product_id INTEGER NOT NULL,
            sku VARCHAR NOT NULL,

            -- Product info (denormalized for query performance)
            name VARCHAR,
            brand VARCHAR,
            category_id INTEGER,

            -- Stock levels (from API)
            quantity INTEGER NOT NULL DEFAULT 0,
            reserve INTEGER NOT NULL DEFAULT 0,
            price DECIMAL(12, 2) NOT NULL DEFAULT 0,
            purchased_price DECIMAL(12, 2),

            -- Timestamps
            last_sale_date DATE,
            first_seen_at DATE NOT NULL DEFAULT CURRENT_DATE,
            updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
        );

        CREATE INDEX IF NOT EXISTS idx_sku_status_category
            ON sku_inventory_status(category_id);
        CREATE INDEX IF NOT EXISTS idx_sku_status_brand
            ON sku_inventory_status(brand);
        CREATE INDEX IF NOT EXISTS idx_sku_status_quantity
            ON sku_inventory_status(quantity);

        -- ═══════════════════════════════════════════════════════════════════════
        -- LAYER 2: SKU Inventory History (daily per-SKU snapshots)
        -- ═══════════════════════════════════════════════════════════════════════
        CREATE TABLE IF NOT EXISTS inventory_sku_history (
            date DATE NOT NULL,
            offer_id INTEGER NOT NULL,
            quantity INTEGER NOT NULL,
            reserve INTEGER NOT NULL,
            price DECIMAL(12, 2) NOT NULL,
            PRIMARY KEY (date, offer_id)
        );

        CREATE INDEX IF NOT EXISTS idx_sku_history_offer
            ON inventory_sku_history(offer_id, date DESC);

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

        -- Revenue predictions (ML forecast)
        CREATE TABLE IF NOT EXISTS revenue_predictions (
            prediction_date DATE NOT NULL,
            sales_type VARCHAR NOT NULL DEFAULT 'retail',
            predicted_revenue DECIMAL(12, 2),
            model_mae DECIMAL(10, 2),
            model_mape DECIMAL(6, 2),
            created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (prediction_date, sales_type)
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

        -- ═══════════════════════════════════════════════════════════════════════
        -- SILVER LAYER: Enriched orders (one row per order)
        -- ═══════════════════════════════════════════════════════════════════════
        CREATE TABLE IF NOT EXISTS silver_orders (
            id INTEGER PRIMARY KEY,
            source_id INTEGER NOT NULL,
            status_id INTEGER NOT NULL,
            grand_total DECIMAL(12, 2) NOT NULL,
            ordered_at TIMESTAMP WITH TIME ZONE,
            buyer_id INTEGER,
            manager_id INTEGER,
            order_date DATE NOT NULL,
            is_return BOOLEAN NOT NULL,
            sales_type VARCHAR NOT NULL,
            is_active_source BOOLEAN NOT NULL,
            source_name VARCHAR NOT NULL,
            is_new_customer BOOLEAN NOT NULL DEFAULT FALSE,
            buyer_first_order_date DATE
        );

        -- ═══════════════════════════════════════════════════════════════════════
        -- GOLD LAYER: Pre-aggregated daily revenue (one row per date+sales_type)
        -- ═══════════════════════════════════════════════════════════════════════
        CREATE TABLE IF NOT EXISTS gold_daily_revenue (
            date DATE NOT NULL,
            sales_type VARCHAR NOT NULL,
            revenue DECIMAL(14, 2) NOT NULL DEFAULT 0,
            orders_count INTEGER NOT NULL DEFAULT 0,
            unique_customers INTEGER NOT NULL DEFAULT 0,
            new_customers INTEGER NOT NULL DEFAULT 0,
            returning_customers INTEGER NOT NULL DEFAULT 0,
            instagram_revenue DECIMAL(14, 2) NOT NULL DEFAULT 0,
            telegram_revenue DECIMAL(14, 2) NOT NULL DEFAULT 0,
            shopify_revenue DECIMAL(14, 2) NOT NULL DEFAULT 0,
            instagram_orders INTEGER NOT NULL DEFAULT 0,
            telegram_orders INTEGER NOT NULL DEFAULT 0,
            shopify_orders INTEGER NOT NULL DEFAULT 0,
            returns_count INTEGER NOT NULL DEFAULT 0,
            returns_revenue DECIMAL(14, 2) NOT NULL DEFAULT 0,
            avg_order_value DECIMAL(12, 2) NOT NULL DEFAULT 0,
            PRIMARY KEY (date, sales_type)
        );

        -- ═══════════════════════════════════════════════════════════════════════
        -- GOLD LAYER: Pre-aggregated daily products
        -- ═══════════════════════════════════════════════════════════════════════
        CREATE TABLE IF NOT EXISTS gold_daily_products (
            date DATE NOT NULL,
            sales_type VARCHAR NOT NULL,
            source_id INTEGER NOT NULL,
            product_id INTEGER,
            product_name VARCHAR NOT NULL,
            brand VARCHAR,
            category_id INTEGER,
            category_name VARCHAR,
            parent_category_name VARCHAR,
            quantity_sold INTEGER NOT NULL DEFAULT 0,
            product_revenue DECIMAL(14, 2) NOT NULL DEFAULT 0,
            order_count INTEGER NOT NULL DEFAULT 0
        );

        -- ═══════════════════════════════════════════════════════════════════════
        -- WAREHOUSE REFRESH AUDIT LOG
        -- ═══════════════════════════════════════════════════════════════════════
        CREATE SEQUENCE IF NOT EXISTS warehouse_refresh_seq START 1;

        CREATE TABLE IF NOT EXISTS warehouse_refreshes (
            id INTEGER PRIMARY KEY DEFAULT (nextval('warehouse_refresh_seq')),
            refreshed_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
            trigger VARCHAR NOT NULL,
            duration_ms DECIMAL(10, 2),
            bronze_orders INTEGER,
            silver_rows INTEGER,
            gold_revenue_rows INTEGER,
            gold_products_rows INTEGER,
            silver_revenue_checksum DECIMAL(14, 2),
            gold_revenue_checksum DECIMAL(14, 2),
            checksum_match BOOLEAN,
            validation_passed BOOLEAN,
            error VARCHAR
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

        -- Silver/Gold layer indexes
        CREATE INDEX IF NOT EXISTS idx_silver_order_date ON silver_orders(order_date);
        CREATE INDEX IF NOT EXISTS idx_silver_sales_type ON silver_orders(sales_type, order_date);
        CREATE INDEX IF NOT EXISTS idx_silver_buyer ON silver_orders(buyer_id);
        CREATE INDEX IF NOT EXISTS idx_gold_rev_date ON gold_daily_revenue(date, sales_type);
        CREATE INDEX IF NOT EXISTS idx_gold_prod_date ON gold_daily_products(date, sales_type);
        CREATE INDEX IF NOT EXISTS idx_gold_prod_product ON gold_daily_products(product_id);
        CREATE INDEX IF NOT EXISTS idx_gold_prod_brand ON gold_daily_products(brand);
        CREATE INDEX IF NOT EXISTS idx_gold_prod_category ON gold_daily_products(category_id);
        CREATE INDEX IF NOT EXISTS idx_warehouse_refreshes_at ON warehouse_refreshes(refreshed_at);

        -- Composite indexes for drill-down queries (30-40% speedup)
        CREATE INDEX IF NOT EXISTS idx_silver_source_date_type ON silver_orders(source_id, order_date, sales_type);
        CREATE INDEX IF NOT EXISTS idx_silver_active_return ON silver_orders(is_active_source, is_return, order_date);
        CREATE INDEX IF NOT EXISTS idx_gold_prod_cat_date ON gold_daily_products(category_id, date, sales_type);
        CREATE INDEX IF NOT EXISTS idx_gold_prod_brand_date ON gold_daily_products(brand, date, sales_type);
        """
        self._connection.execute(schema_sql)

        # Create analytics views (Layer 3 & 4)
        await self._create_inventory_views()

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

    async def _create_inventory_views(self) -> None:
        """Create Layer 3 & 4 analytics views for inventory."""
        views_sql = """
        -- ═══════════════════════════════════════════════════════════════════════
        -- LAYER 3: Analytics Views
        -- ═══════════════════════════════════════════════════════════════════════

        -- View: Current SKU analysis (adds calculated fields)
        CREATE OR REPLACE VIEW v_sku_analysis AS
        SELECT
            s.*,
            c.name as category_name,
            s.quantity - s.reserve as available,
            s.quantity * s.price as stock_value,
            (s.quantity - s.reserve) * s.price as available_value,
            CURRENT_DATE - s.last_sale_date as days_since_sale,
            CURRENT_DATE - s.first_seen_at as days_in_stock
        FROM sku_inventory_status s
        LEFT JOIN categories c ON s.category_id = c.id;

        -- View: Category velocity (for dynamic thresholds)
        CREATE OR REPLACE VIEW v_category_velocity AS
        SELECT
            category_id,
            category_name,
            COUNT(*) as sample_size,
            PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY days_since_sale) as p50,
            PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY days_since_sale) as p75,
            PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY days_since_sale) as p90,
            LEAST(GREATEST(
                PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY days_since_sale),
                90
            ), 365) as threshold_days
        FROM v_sku_analysis
        WHERE last_sale_date IS NOT NULL AND quantity > 0
        GROUP BY category_id, category_name
        HAVING COUNT(*) >= 5;

        -- View: SKU status with dead stock classification
        CREATE OR REPLACE VIEW v_sku_status AS
        SELECT
            s.*,
            COALESCE(cv.threshold_days, 180) as threshold_days,
            CASE
                WHEN s.last_sale_date IS NULL THEN 'never_sold'
                WHEN s.days_since_sale > COALESCE(cv.threshold_days, 180) THEN 'dead_stock'
                WHEN s.days_since_sale > COALESCE(cv.threshold_days, 180) * 0.7 THEN 'at_risk'
                ELSE 'healthy'
            END as status
        FROM v_sku_analysis s
        LEFT JOIN v_category_velocity cv ON s.category_id = cv.category_id
        WHERE s.quantity > 0;

        -- View: Summary by status
        CREATE OR REPLACE VIEW v_inventory_summary AS
        SELECT
            status,
            COUNT(*) as sku_count,
            SUM(available) as total_units,
            SUM(available_value) as total_value,
            ROUND(100.0 * SUM(available_value) /
                NULLIF(SUM(SUM(available_value)) OVER (), 0), 1) as value_pct
        FROM v_sku_status
        GROUP BY status;

        -- View: Aging buckets
        CREATE OR REPLACE VIEW v_aging_buckets AS
        SELECT
            CASE
                WHEN days_since_sale IS NULL THEN '6. Never sold'
                WHEN days_since_sale <= 30 THEN '1. 0-30 days'
                WHEN days_since_sale <= 90 THEN '2. 31-90 days'
                WHEN days_since_sale <= 180 THEN '3. 91-180 days'
                WHEN days_since_sale <= 365 THEN '4. 181-365 days'
                ELSE '5. 365+ days'
            END as bucket,
            COUNT(*) as sku_count,
            SUM(available) as units,
            SUM(available_value) as value
        FROM v_sku_analysis
        WHERE quantity > 0
        GROUP BY bucket
        ORDER BY bucket;

        -- View: Daily inventory trend (from history)
        CREATE OR REPLACE VIEW v_inventory_trend AS
        SELECT
            date,
            COUNT(*) as sku_count,
            SUM(quantity) as total_quantity,
            SUM(quantity - reserve) as available,
            SUM(quantity * price) as total_value
        FROM inventory_sku_history
        GROUP BY date
        ORDER BY date;

        -- ═══════════════════════════════════════════════════════════════════════
        -- LAYER 4: Action Views
        -- ═══════════════════════════════════════════════════════════════════════

        -- View: Actionable recommendations
        CREATE OR REPLACE VIEW v_recommended_actions AS
        SELECT
            offer_id,
            sku,
            name,
            brand,
            category_name,
            available as units,
            available_value as value,
            days_since_sale,
            days_in_stock,
            status,
            CASE
                WHEN status = 'never_sold' AND days_in_stock > 180 THEN 'Return to supplier'
                WHEN status = 'never_sold' AND days_in_stock > 90 THEN 'Deep discount (70%+)'
                WHEN status = 'dead_stock' AND available_value > 10000 THEN 'Discount 50%'
                WHEN status = 'dead_stock' THEN 'Bundle with bestsellers'
                WHEN status = 'at_risk' THEN 'Promote / Feature'
                ELSE NULL
            END as action
        FROM v_sku_status
        WHERE status != 'healthy'
        ORDER BY available_value DESC;

        -- View: Low stock alerts
        CREATE OR REPLACE VIEW v_restock_alerts AS
        SELECT
            offer_id,
            sku,
            name,
            brand,
            available as units_left,
            days_since_sale,
            CASE
                WHEN available = 0 THEN 'OUT_OF_STOCK'
                WHEN available <= 3 THEN 'CRITICAL'
                WHEN available <= 10 THEN 'LOW'
            END as alert_level
        FROM v_sku_analysis
        WHERE available <= 10
          AND (days_since_sale IS NULL OR days_since_sale <= 90)
        ORDER BY available ASC;
        """
        self._connection.execute(views_sql)
        logger.info("Inventory analytics views created")

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

    # ─── Warehouse Layer Refresh ─────────────────────────────────────────────

    async def refresh_warehouse_layers(self, trigger: str = "manual") -> Dict[str, Any]:
        """Rebuild Silver and Gold warehouse layers from Bronze tables.

        Atomically rebuilds silver_orders, gold_daily_revenue, and gold_daily_products
        using CREATE OR REPLACE TABLE. Validates checksums and logs to warehouse_refreshes.

        Args:
            trigger: What triggered the refresh (incremental_sync, full_sync, sync_today, manual)

        Returns:
            Dict with refresh stats and validation results
        """
        import time
        start_time = time.perf_counter()
        error_msg = None

        try:
            async with self.connection() as conn:
                # Build the retail manager filter SQL for use inside CASE
                manager_list = ",".join(str(m) for m in RETAIL_MANAGER_IDS)
                retail_filter = f"""
                    WHEN o.manager_id IS NULL THEN 'retail'
                    WHEN o.manager_id = {B2B_MANAGER_ID} THEN 'b2b'
                    WHEN o.manager_id IN (SELECT id FROM managers WHERE is_retail = TRUE) THEN 'retail'
                    WHEN NOT EXISTS (SELECT 1 FROM managers WHERE is_retail = TRUE)
                         AND o.manager_id IN ({manager_list}) THEN 'retail'
                    ELSE 'other'
                """

                # ── Silver: one row per order with derived fields ──
                conn.execute(f"""
                    CREATE OR REPLACE TABLE silver_orders AS
                    SELECT
                        o.id,
                        o.source_id,
                        o.status_id,
                        o.grand_total,
                        o.ordered_at,
                        o.buyer_id,
                        o.manager_id,
                        {_date_in_kyiv('o.ordered_at')} AS order_date,
                        o.status_id IN {tuple(int(s) for s in OrderStatus.return_statuses())} AS is_return,
                        CASE {retail_filter} END AS sales_type,
                        o.source_id IN (1, 2, 4) AS is_active_source,
                        CASE o.source_id
                            WHEN 1 THEN 'Instagram'
                            WHEN 2 THEN 'Telegram'
                            WHEN 4 THEN 'Shopify'
                            ELSE 'Other'
                        END AS source_name,
                        CASE
                            WHEN o.buyer_id IS NOT NULL
                                 AND {_date_in_kyiv('o.ordered_at')} = fo.first_order_date
                            THEN TRUE ELSE FALSE
                        END AS is_new_customer,
                        fo.first_order_date AS buyer_first_order_date
                    FROM orders o
                    LEFT JOIN (
                        SELECT
                            buyer_id,
                            MIN({_date_in_kyiv('ordered_at')}) AS first_order_date
                        FROM orders
                        WHERE buyer_id IS NOT NULL
                          AND source_id IN (1, 2, 4)
                          AND status_id NOT IN {tuple(int(s) for s in OrderStatus.return_statuses())}
                        GROUP BY buyer_id
                    ) fo ON o.buyer_id = fo.buyer_id
                """)

                # ── Gold: daily revenue aggregated by (date, sales_type) ──
                conn.execute("""
                    CREATE OR REPLACE TABLE gold_daily_revenue AS
                    SELECT
                        order_date AS date,
                        sales_type,
                        COALESCE(SUM(CASE WHEN NOT is_return AND is_active_source THEN grand_total END), 0) AS revenue,
                        COUNT(DISTINCT CASE WHEN NOT is_return AND is_active_source THEN id END) AS orders_count,
                        COUNT(DISTINCT CASE WHEN NOT is_return AND is_active_source THEN buyer_id END) AS unique_customers,
                        COUNT(DISTINCT CASE WHEN NOT is_return AND is_active_source AND is_new_customer THEN buyer_id END) AS new_customers,
                        COUNT(DISTINCT CASE WHEN NOT is_return AND is_active_source AND NOT is_new_customer AND buyer_id IS NOT NULL THEN buyer_id END) AS returning_customers,
                        COALESCE(SUM(CASE WHEN NOT is_return AND source_id = 1 THEN grand_total END), 0) AS instagram_revenue,
                        COALESCE(SUM(CASE WHEN NOT is_return AND source_id = 2 THEN grand_total END), 0) AS telegram_revenue,
                        COALESCE(SUM(CASE WHEN NOT is_return AND source_id = 4 THEN grand_total END), 0) AS shopify_revenue,
                        COUNT(DISTINCT CASE WHEN NOT is_return AND source_id = 1 THEN id END) AS instagram_orders,
                        COUNT(DISTINCT CASE WHEN NOT is_return AND source_id = 2 THEN id END) AS telegram_orders,
                        COUNT(DISTINCT CASE WHEN NOT is_return AND source_id = 4 THEN id END) AS shopify_orders,
                        COUNT(DISTINCT CASE WHEN is_return AND is_active_source THEN id END) AS returns_count,
                        COALESCE(SUM(CASE WHEN is_return AND is_active_source THEN grand_total END), 0) AS returns_revenue,
                        CASE
                            WHEN COUNT(DISTINCT CASE WHEN NOT is_return AND is_active_source THEN id END) > 0
                            THEN COALESCE(SUM(CASE WHEN NOT is_return AND is_active_source THEN grand_total END), 0)
                                 / COUNT(DISTINCT CASE WHEN NOT is_return AND is_active_source THEN id END)
                            ELSE 0
                        END AS avg_order_value
                    FROM silver_orders
                    WHERE order_date IS NOT NULL
                    GROUP BY order_date, sales_type
                    ORDER BY order_date, sales_type
                """)

                # ── Gold: daily products aggregated by (date, sales_type, product, source) ──
                conn.execute(f"""
                    CREATE OR REPLACE TABLE gold_daily_products AS
                    SELECT
                        s.order_date AS date,
                        s.sales_type,
                        s.source_id,
                        op.product_id,
                        op.name AS product_name,
                        p.brand,
                        p.category_id,
                        c.name AS category_name,
                        parent_c.name AS parent_category_name,
                        SUM(op.quantity) AS quantity_sold,
                        SUM(op.price_sold * op.quantity) AS product_revenue,
                        COUNT(DISTINCT s.id) AS order_count
                    FROM silver_orders s
                    JOIN order_products op ON s.id = op.order_id
                    LEFT JOIN products p ON op.product_id = p.id
                    LEFT JOIN categories c ON p.category_id = c.id
                    LEFT JOIN categories parent_c ON c.parent_id = parent_c.id
                    WHERE NOT s.is_return
                      AND s.is_active_source
                      AND s.order_date IS NOT NULL
                    GROUP BY
                        s.order_date, s.sales_type, s.source_id,
                        op.product_id, op.name, p.brand, p.category_id,
                        c.name, parent_c.name
                """)

                # ── Recreate indexes (CREATE OR REPLACE TABLE drops them) ──
                # Basic indexes
                conn.execute("CREATE INDEX IF NOT EXISTS idx_silver_order_date ON silver_orders(order_date)")
                conn.execute("CREATE INDEX IF NOT EXISTS idx_silver_sales_type ON silver_orders(sales_type, order_date)")
                conn.execute("CREATE INDEX IF NOT EXISTS idx_silver_buyer ON silver_orders(buyer_id)")
                conn.execute("CREATE INDEX IF NOT EXISTS idx_gold_rev_date ON gold_daily_revenue(date, sales_type)")
                conn.execute("CREATE INDEX IF NOT EXISTS idx_gold_prod_date ON gold_daily_products(date, sales_type)")
                conn.execute("CREATE INDEX IF NOT EXISTS idx_gold_prod_product ON gold_daily_products(product_id)")
                conn.execute("CREATE INDEX IF NOT EXISTS idx_gold_prod_brand ON gold_daily_products(brand)")
                conn.execute("CREATE INDEX IF NOT EXISTS idx_gold_prod_category ON gold_daily_products(category_id)")
                # Composite indexes for drill-down query optimization
                conn.execute("CREATE INDEX IF NOT EXISTS idx_silver_source_date_type ON silver_orders(source_id, order_date, sales_type)")
                conn.execute("CREATE INDEX IF NOT EXISTS idx_silver_active_return ON silver_orders(is_active_source, is_return, order_date)")
                conn.execute("CREATE INDEX IF NOT EXISTS idx_gold_prod_cat_date ON gold_daily_products(category_id, date, sales_type)")
                conn.execute("CREATE INDEX IF NOT EXISTS idx_gold_prod_brand_date ON gold_daily_products(brand, date, sales_type)")

                # ── Validation checksums ──
                checksums = conn.execute("""
                    SELECT
                        (SELECT COUNT(*) FROM orders) AS bronze_orders,
                        (SELECT COUNT(*) FROM silver_orders) AS silver_rows,
                        (SELECT COUNT(*) FROM gold_daily_revenue) AS gold_revenue_rows,
                        (SELECT COUNT(*) FROM gold_daily_products) AS gold_products_rows,
                        (SELECT COALESCE(SUM(grand_total), 0) FROM silver_orders
                         WHERE NOT is_return AND is_active_source) AS silver_revenue,
                        (SELECT COALESCE(SUM(revenue), 0) FROM gold_daily_revenue) AS gold_revenue
                """).fetchone()

                bronze_orders = checksums[0]
                silver_rows = checksums[1]
                gold_revenue_rows = checksums[2]
                gold_products_rows = checksums[3]
                silver_revenue = float(checksums[4])
                gold_revenue = float(checksums[5])

                checksum_match = abs(silver_revenue - gold_revenue) < 0.01
                row_count_match = bronze_orders == silver_rows
                validation_passed = checksum_match and row_count_match

                if not validation_passed:
                    logger.warning(
                        f"Warehouse validation failed: "
                        f"rows={bronze_orders}→{silver_rows} (match={row_count_match}), "
                        f"revenue={silver_revenue:.2f}→{gold_revenue:.2f} (match={checksum_match})"
                    )

                duration_ms = (time.perf_counter() - start_time) * 1000

                # ── Audit log ──
                conn.execute("""
                    INSERT INTO warehouse_refreshes
                        (refreshed_at, trigger, duration_ms, bronze_orders, silver_rows,
                         gold_revenue_rows, gold_products_rows, silver_revenue_checksum,
                         gold_revenue_checksum, checksum_match, validation_passed, error)
                    VALUES (CURRENT_TIMESTAMP, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, [
                    trigger, round(duration_ms, 2),
                    bronze_orders, silver_rows, gold_revenue_rows, gold_products_rows,
                    round(silver_revenue, 2), round(gold_revenue, 2),
                    checksum_match, validation_passed, None
                ])

                logger.info(
                    f"Warehouse layers refreshed ({trigger}): "
                    f"silver={silver_rows}, gold_rev={gold_revenue_rows}, "
                    f"gold_prod={gold_products_rows}, "
                    f"duration={duration_ms:.0f}ms, valid={validation_passed}"
                )

                return {
                    "status": "success",
                    "trigger": trigger,
                    "duration_ms": round(duration_ms, 2),
                    "bronze_orders": bronze_orders,
                    "silver_rows": silver_rows,
                    "gold_revenue_rows": gold_revenue_rows,
                    "gold_products_rows": gold_products_rows,
                    "checksum_match": checksum_match,
                    "validation_passed": validation_passed,
                }

        except Exception as e:
            duration_ms = (time.perf_counter() - start_time) * 1000
            error_msg = str(e)
            logger.error(f"Warehouse refresh failed ({trigger}): {e}", exc_info=True)

            # Log failure to audit table
            try:
                async with self.connection() as conn:
                    conn.execute("""
                        INSERT INTO warehouse_refreshes
                            (refreshed_at, trigger, duration_ms, validation_passed, error)
                        VALUES (CURRENT_TIMESTAMP, ?, ?, FALSE, ?)
                    """, [trigger, round(duration_ms, 2), error_msg])
            except Exception:
                pass

            return {
                "status": "error",
                "trigger": trigger,
                "duration_ms": round(duration_ms, 2),
                "error": error_msg,
            }

    async def get_warehouse_status(self) -> Dict[str, Any]:
        """Get warehouse layer status for admin monitoring."""
        async with self.connection() as conn:
            # Last refresh info
            last = conn.execute("""
                SELECT refreshed_at, trigger, duration_ms, bronze_orders, silver_rows,
                       gold_revenue_rows, gold_products_rows, checksum_match, validation_passed
                FROM warehouse_refreshes
                ORDER BY id DESC
                LIMIT 1
            """).fetchone()

            # Count refreshes in last hour
            recent_count = conn.execute("""
                SELECT COUNT(*) FROM warehouse_refreshes
                WHERE refreshed_at > CURRENT_TIMESTAMP - INTERVAL '1 hour'
            """).fetchone()

            if last:
                return {
                    "last_refresh": last[0].isoformat() if last[0] else None,
                    "last_trigger": last[1],
                    "last_duration_ms": float(last[2]) if last[2] else None,
                    "bronze_orders": last[3],
                    "silver_rows": last[4],
                    "gold_revenue_rows": last[5],
                    "gold_products_rows": last[6],
                    "checksum_match": last[7],
                    "validation_passed": last[8],
                    "recent_refreshes": recent_count[0] if recent_count else 0,
                }
            else:
                return {
                    "last_refresh": None,
                    "last_trigger": None,
                    "last_duration_ms": None,
                    "bronze_orders": 0,
                    "silver_rows": 0,
                    "gold_revenue_rows": 0,
                    "gold_products_rows": 0,
                    "checksum_match": None,
                    "validation_passed": None,
                    "recent_refreshes": 0,
                }

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

    async def upsert_orders(self, orders: List[Dict[str, Any]], force_update: bool = False) -> int:
        """
        Insert or update orders from API response (idempotent).

        Uses DataFrame bulk insert for performance (~10-100x faster than row-by-row).
        Only updates existing orders if the new updated_at is newer than
        the existing one. This prevents stale API responses from overwriting
        fresher data.

        Args:
            orders: List of order dicts from KeyCRM API
            force_update: If True, update all orders regardless of updated_at timestamp.
                         Use this for status refresh since KeyCRM doesn't update
                         updated_at when status changes.

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

                # 2. Find orders that need to be updated
                if force_update:
                    # Force update: update ALL existing orders (for status refresh)
                    # KeyCRM doesn't update updated_at when status changes, so we must
                    # force-update to catch status changes like orders marked as returns
                    orders_to_update = conn.execute("""
                        SELECT o.id FROM orders o
                        JOIN stg_orders stg ON o.id = stg.id
                    """).fetchall()
                else:
                    # Normal update: only update if staging has newer data
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

    async def upsert_offers(self, offers: List[Dict[str, Any]]) -> int:
        """Insert or update offers from KeyCRM API response.

        Offers link offer_id to product_id, enabling proper joins between
        offer_stocks and products tables.

        Args:
            offers: List of offer dicts from KeyCRM API (/offers endpoint)

        Returns:
            Number of offers upserted
        """
        if not offers:
            return 0

        async with self.connection() as conn:
            conn.execute("BEGIN TRANSACTION")
            try:
                count = 0
                for offer in offers:
                    conn.execute("""
                        INSERT OR REPLACE INTO offers (id, product_id, sku, synced_at)
                        VALUES (?, ?, ?, CURRENT_TIMESTAMP)
                    """, [
                        offer.get("id"),
                        offer.get("product_id"),
                        offer.get("sku"),
                    ])
                    count += 1

                conn.execute("COMMIT")
                logger.info(f"Upserted {count} offers to DuckDB")
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

    async def refresh_sku_inventory_status(self) -> int:
        """Refresh Layer 1: sku_inventory_status from source tables.

        Combines data from offer_stocks, offers, products, and orders to create
        a denormalized view of current inventory with last sale dates.

        Returns:
            Number of SKUs in the refreshed table
        """
        async with self.connection() as conn:
            # Calculate last sale date per product (using offer_id from order_products)
            # Then merge with stock data
            conn.execute("""
                INSERT OR REPLACE INTO sku_inventory_status
                SELECT
                    os.id as offer_id,
                    COALESCE(o.product_id, 0) as product_id,
                    COALESCE(os.sku, CAST(os.id AS VARCHAR)) as sku,
                    p.name,
                    p.brand,
                    p.category_id,
                    os.quantity,
                    os.reserve,
                    COALESCE(os.price, 0) as price,
                    os.purchased_price,
                    pls.last_sale_date,
                    COALESCE(
                        (SELECT first_seen_at FROM sku_inventory_status WHERE offer_id = os.id),
                        CURRENT_DATE
                    ) as first_seen_at,
                    CURRENT_TIMESTAMP as updated_at
                FROM offer_stocks os
                LEFT JOIN offers o ON os.id = o.id
                LEFT JOIN products p ON o.product_id = p.id
                LEFT JOIN (
                    SELECT
                        op.product_id,
                        MAX(DATE(ord.ordered_at AT TIME ZONE 'Europe/Kyiv')) as last_sale_date
                    FROM order_products op
                    JOIN orders ord ON op.order_id = ord.id
                    WHERE ord.status_id NOT IN (19, 22, 21, 23)
                    GROUP BY op.product_id
                ) pls ON o.product_id = pls.product_id
            """)

            count = conn.execute("SELECT COUNT(*) FROM sku_inventory_status").fetchone()[0]
            logger.info(f"Refreshed sku_inventory_status: {count} SKUs")
            return count

    async def record_sku_inventory_snapshot(self) -> bool:
        """Record Layer 2: daily per-SKU snapshot.

        Only records one snapshot per day. Returns True if recorded, False if already exists.
        """
        async with self.connection() as conn:
            today = date.today()

            # Check if already recorded today
            exists = conn.execute(
                "SELECT 1 FROM inventory_sku_history WHERE date = ? LIMIT 1", [today]
            ).fetchone()

            if exists:
                return False

            # Record snapshot from current sku_inventory_status
            conn.execute("""
                INSERT INTO inventory_sku_history (date, offer_id, quantity, reserve, price)
                SELECT
                    CURRENT_DATE,
                    offer_id,
                    quantity,
                    reserve,
                    price
                FROM sku_inventory_status
                WHERE quantity > 0
            """)

            count = conn.execute(
                "SELECT COUNT(*) FROM inventory_sku_history WHERE date = ?", [today]
            ).fetchone()[0]
            logger.info(f"Recorded SKU inventory snapshot: {count} SKUs for {today}")
            return True

    async def get_stock_summary(self, limit: int = 20) -> Dict[str, Any]:
        """Get stock summary for dashboard display.

        Returns:
            Dict with total stats and top items by quantity and low stock alerts
        """
        async with self.connection() as conn:
            # Overall stats
            # Note: available = MAX(0, quantity - reserve) to match KeyCRM display
            stats = conn.execute("""
                SELECT
                    COUNT(*) as total_offers,
                    COUNT(*) FILTER (WHERE quantity > 0) as in_stock_count,
                    COUNT(*) FILTER (WHERE quantity = 0) as out_of_stock_count,
                    COUNT(*) FILTER (WHERE quantity > 0 AND quantity <= 5) as low_stock_count,
                    SUM(GREATEST(0, quantity - reserve)) as available_quantity,
                    SUM(reserve) as total_reserve,
                    SUM(GREATEST(0, quantity - reserve) * price) as available_value_sale,
                    SUM(reserve * price) as reserve_value_sale,
                    SUM(GREATEST(0, quantity - reserve) * COALESCE(purchased_price, 0)) as available_value_cost,
                    SUM(reserve * COALESCE(purchased_price, 0)) as reserve_value_cost
                FROM offer_stocks
            """).fetchone()

            # Top items by quantity (with product names via offers table)
            top_by_qty = conn.execute(f"""
                SELECT os.sku, os.quantity, os.reserve, os.price, p.name
                FROM offer_stocks os
                LEFT JOIN offers o ON os.id = o.id
                LEFT JOIN products p ON o.product_id = p.id
                WHERE os.quantity > 0
                ORDER BY os.quantity DESC
                LIMIT {limit}
            """).fetchall()

            # Low stock items (1-5 units, excluding 0)
            low_stock = conn.execute("""
                SELECT os.sku, os.quantity, os.reserve, os.price, p.name
                FROM offer_stocks os
                LEFT JOIN offers o ON os.id = o.id
                LEFT JOIN products p ON o.product_id = p.id
                WHERE os.quantity > 0 AND os.quantity <= 5
                ORDER BY os.quantity ASC
                LIMIT 20
            """).fetchall()

            # Out of stock items
            out_of_stock = conn.execute("""
                SELECT os.sku, os.price, p.name
                FROM offer_stocks os
                LEFT JOIN offers o ON os.id = o.id
                LEFT JOIN products p ON o.product_id = p.id
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
                    "totalValue": float(stats[6] or 0),  # Sale price
                    "reserveValue": float(stats[7] or 0),  # Sale price
                    "costValue": float(stats[8] or 0),  # Purchase/cost price
                    "reserveCostValue": float(stats[9] or 0),  # Purchase/cost price
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

    async def record_inventory_snapshot(self, force: bool = False) -> bool:
        """Record daily inventory snapshot for average calculation.

        Only records one snapshot per day. Returns True if recorded, False if already exists.

        Args:
            force: If True, delete existing snapshot and re-record
        """
        async with self.connection() as conn:
            today = conn.execute("SELECT CURRENT_DATE").fetchone()[0]

            # Check if already recorded today
            exists = conn.execute(
                "SELECT 1 FROM inventory_history WHERE date = ?", [today]
            ).fetchone()

            if exists:
                if force:
                    conn.execute("DELETE FROM inventory_history WHERE date = ?", [today])
                    logger.info(f"Deleted existing inventory snapshot for {today}")
                else:
                    return False

            # Record snapshot (using available quantity = quantity - reserve, same as KeyCRM)
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
                result = conn.execute(f"""
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
                    WHERE date >= CURRENT_DATE - INTERVAL '{days} days'
                    GROUP BY DATE_TRUNC('month', date)
                    ORDER BY period
                """).fetchall()

                labels = [row[0].strftime('%b %Y') for row in result if row[0]]
                quantities = [round(row[1] or 0) for row in result]
                values = [round(float(row[2] or 0), 2) for row in result]
                reserves = [round(row[3] or 0) for row in result]

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
                result = conn.execute(f"""
                    SELECT
                        date,
                        total_quantity,
                        total_value,
                        total_reserve,
                        sku_count
                    FROM inventory_history
                    WHERE date >= CURRENT_DATE - INTERVAL '{days} days'
                    ORDER BY date
                """).fetchall()

                labels = [row[0].strftime('%d %b') for row in result if row[0]]
                quantities = [row[1] or 0 for row in result]
                values = [float(row[2] or 0) for row in result]
                reserves = [row[3] or 0 for row in result]
                sku_counts = [row[4] or 0 for row in result]

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

                # Returns from gold_daily_revenue (returns aren't product-specific)
                ret_params = [start_date, end_date]
                ret_where = ["date BETWEEN ? AND ?"]
                if sales_type != "all":
                    ret_where.append("sales_type = ?")
                    ret_params.append(sales_type)
                ret_result = conn.execute(f"""
                    SELECT COALESCE(SUM(returns_count), 0), COALESCE(SUM(returns_revenue), 0)
                    FROM gold_daily_revenue
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
                    # Source-specific: sum per-source columns
                    source_col_map = {1: "instagram", 2: "telegram", 4: "shopify"}
                    src_name = source_col_map.get(source_id)
                    if src_name:
                        result = conn.execute(f"""
                            SELECT
                                SUM({src_name}_orders) as total_orders,
                                SUM({src_name}_revenue) as total_revenue,
                                SUM(returns_count) as total_returns,
                                SUM(returns_revenue) as returns_revenue
                            FROM gold_daily_revenue
                            WHERE {where_sql}
                        """, params).fetchone()
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
                    g.product_name,
                    SUM(g.quantity_sold) as total_qty
                FROM gold_daily_products g
                WHERE {where_sql}
                GROUP BY g.product_name
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

    async def get_customer_insights(
        self,
        start_date: date,
        end_date: date,
        source_id: Optional[int] = None,
        brand: Optional[str] = None,
        sales_type: str = "retail"
    ) -> Dict[str, Any]:
        """Get customer insights: new vs returning, AOV trend (from Gold/Silver layers)."""
        async with self.connection() as conn:
            # ── Base metrics from gold_daily_revenue ──
            params = [start_date, end_date]
            where_clauses = ["date BETWEEN ? AND ?"]

            if sales_type != "all":
                where_clauses.append("sales_type = ?")
                params.append(sales_type)

            where_sql = " AND ".join(where_clauses)

            gold_result = conn.execute(f"""
                SELECT
                    SUM(unique_customers) as total_customers,
                    SUM(orders_count) as total_orders,
                    SUM(revenue) as total_revenue,
                    SUM(new_customers) as new_customers,
                    SUM(returning_customers) as returning_customers
                FROM gold_daily_revenue
                WHERE {where_sql}
            """, params).fetchone()

            total_customers = int(gold_result[0] or 0)
            total_orders = int(gold_result[1] or 0)
            total_revenue = float(gold_result[2] or 0)
            new_customers = int(gold_result[3] or 0)
            returning_customers = int(gold_result[4] or 0)

            # AOV trend from gold_daily_revenue
            aov_results = conn.execute(f"""
                SELECT date,
                       CASE WHEN orders_count > 0 THEN revenue / orders_count ELSE 0 END as aov
                FROM gold_daily_revenue
                WHERE {where_sql}
                ORDER BY date
            """, params).fetchall()
            aov_by_day = {row[0]: float(row[1]) for row in aov_results}

            labels = []
            aov_data = []
            current = start_date
            while current <= end_date:
                labels.append(current.strftime("%d.%m"))
                aov_data.append(round(aov_by_day.get(current, 0), 2))
                current += timedelta(days=1)

            overall_aov = total_revenue / total_orders if total_orders > 0 else 0

            # ── CLV metrics from silver_orders (need per-buyer aggregation) ──
            sales_where = "s.sales_type = ?" if sales_type != "all" else "1=1"
            clv_params = [sales_type] if sales_type != "all" else []

            clv_result = conn.execute(f"""
                WITH customer_stats AS (
                    SELECT
                        s.buyer_id,
                        COUNT(DISTINCT s.id) as order_count,
                        SUM(s.grand_total) as total_spent,
                        DATE_DIFF('day', MIN(s.ordered_at), MAX(s.ordered_at)) as lifespan_days
                    FROM silver_orders s
                    WHERE s.buyer_id IS NOT NULL
                      AND NOT s.is_return
                      AND s.is_active_source
                      AND {sales_where}
                    GROUP BY s.buyer_id
                    HAVING COUNT(DISTINCT s.id) > 1
                )
                SELECT
                    COUNT(*) as repeat_customer_count,
                    AVG(order_count) as avg_purchase_frequency,
                    AVG(lifespan_days) as avg_lifespan_days,
                    AVG(total_spent) as avg_customer_value
                FROM customer_stats
            """, clv_params).fetchone()

            repeat_customer_count = clv_result[0] or 0
            avg_purchase_frequency = float(clv_result[1] or 0)
            avg_lifespan_days = float(clv_result[2] or 0)
            avg_customer_value = float(clv_result[3] or 0)
            clv = avg_customer_value if repeat_customer_count > 0 else 0
            purchase_frequency = total_orders / total_customers if total_customers > 0 else 0

            # All-time repeat rate from silver_orders
            alltime_result = conn.execute(f"""
                WITH customer_orders AS (
                    SELECT
                        s.buyer_id,
                        COUNT(DISTINCT s.id) as order_count
                    FROM silver_orders s
                    WHERE s.buyer_id IS NOT NULL
                      AND NOT s.is_return
                      AND s.is_active_source
                      AND {sales_where}
                    GROUP BY s.buyer_id
                )
                SELECT
                    COUNT(*) as total_customers,
                    SUM(CASE WHEN order_count >= 2 THEN 1 ELSE 0 END) as repeat_customers,
                    AVG(order_count) as avg_orders_per_customer
                FROM customer_orders
            """, clv_params).fetchone()

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
                    "customerLifetimeValue": round(clv, 2),
                    "avgPurchaseFrequency": round(avg_purchase_frequency, 2),
                    "avgCustomerLifespanDays": round(avg_lifespan_days, 0),
                    "purchaseFrequency": round(purchase_frequency, 2),
                    "totalCustomersAllTime": alltime_total_customers,
                    "repeatCustomersAllTime": alltime_repeat_customers,
                    "trueRepeatRate": round(true_repeat_rate, 1),
                    "avgOrdersPerCustomer": round(alltime_avg_orders, 2)
                }
            }

    async def get_cohort_retention(
        self,
        months_back: int = 12,
        retention_months: int = 6,
        sales_type: str = "retail"
    ) -> Dict[str, Any]:
        """
        Get cohort retention analysis.

        Shows what percentage of customers from each cohort (first purchase month)
        returned to make purchases in subsequent months.

        Args:
            months_back: How many months of cohorts to analyze
            retention_months: How many months of retention to track (M0 to Mn)
            sales_type: Filter by sales type (retail/b2b/all)

        Returns:
            Dict with cohorts, retention matrix, and summary metrics
        """
        async with self.connection() as conn:
            # Build sales type filter
            sales_type_filter = ""
            if sales_type == "retail":
                sales_type_filter = f"""
                    AND (o.manager_id IN ({','.join(map(str, RETAIL_MANAGER_IDS))})
                         OR (o.manager_id IS NULL AND o.source_id = 4))
                """
            elif sales_type == "b2b":
                sales_type_filter = f"AND o.manager_id = {B2B_MANAGER_ID}"

            query = f"""
            WITH customer_cohorts AS (
                -- Get each customer's first order month (their cohort)
                SELECT
                    o.buyer_id,
                    DATE_TRUNC('month', MIN(o.order_date)) AS cohort_month
                FROM silver_orders o
                WHERE o.buyer_id IS NOT NULL
                  AND NOT o.is_return
                  {sales_type_filter}
                GROUP BY o.buyer_id
            ),
            customer_orders AS (
                -- Get all order months per customer
                SELECT DISTINCT
                    o.buyer_id,
                    c.cohort_month,
                    DATEDIFF('month', c.cohort_month, DATE_TRUNC('month', o.order_date)) AS months_since
                FROM silver_orders o
                JOIN customer_cohorts c ON o.buyer_id = c.buyer_id
                WHERE NOT o.is_return
                  {sales_type_filter}
            ),
            cohort_sizes AS (
                SELECT cohort_month, COUNT(DISTINCT buyer_id) AS size
                FROM customer_cohorts
                GROUP BY cohort_month
            ),
            retention_data AS (
                SELECT
                    r.cohort_month,
                    r.months_since,
                    COUNT(DISTINCT r.buyer_id) AS retained_customers
                FROM customer_orders r
                WHERE r.months_since <= ?
                GROUP BY r.cohort_month, r.months_since
            )
            SELECT
                strftime(r.cohort_month, '%Y-%m') as cohort,
                s.size as cohort_size,
                r.months_since as month_number,
                r.retained_customers,
                ROUND(100.0 * r.retained_customers / s.size, 1) as retention_pct
            FROM retention_data r
            JOIN cohort_sizes s ON r.cohort_month = s.cohort_month
            WHERE r.cohort_month >= DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '{months_back} months'
            ORDER BY r.cohort_month DESC, r.months_since
            """

            rows = conn.execute(query, [retention_months]).fetchall()

            # Build cohort data structure
            cohorts = {}
            for cohort, size, month_num, retained, pct in rows:
                if cohort not in cohorts:
                    cohorts[cohort] = {
                        "size": size,
                        "retention": {}
                    }
                cohorts[cohort]["retention"][month_num] = {
                    "count": retained,
                    "percent": pct
                }

            # Calculate summary metrics
            total_cohort_size = sum(c["size"] for c in cohorts.values())

            # Average retention by month
            avg_retention = {}
            for m in range(retention_months + 1):
                values = [
                    c["retention"].get(m, {}).get("percent", 0)
                    for c in cohorts.values()
                    if m in c.get("retention", {})
                ]
                if values:
                    avg_retention[m] = round(sum(values) / len(values), 1)

            return {
                "cohorts": [
                    {
                        "month": cohort,
                        "size": data["size"],
                        "retention": [
                            data["retention"].get(m, {}).get("percent", None)
                            for m in range(retention_months + 1)
                        ]
                    }
                    for cohort, data in sorted(cohorts.items(), reverse=True)
                ],
                "retentionMonths": retention_months,
                "summary": {
                    "totalCohorts": len(cohorts),
                    "totalCustomers": total_cohort_size,
                    "avgRetention": avg_retention
                }
            }

    async def get_enhanced_cohort_retention(
        self,
        months_back: int = 12,
        retention_months: int = 6,
        sales_type: str = "retail",
        include_revenue: bool = True
    ) -> Dict[str, Any]:
        """
        Get enhanced cohort retention analysis with revenue tracking.

        Shows customer retention percentages AND revenue retention for each cohort.

        Args:
            months_back: How many months of cohorts to analyze
            retention_months: How many months of retention to track (M0 to Mn)
            sales_type: Filter by sales type (retail/b2b/all)
            include_revenue: Include revenue retention metrics

        Returns:
            Dict with cohorts, customer retention, revenue retention, and summary
        """
        async with self.connection() as conn:
            # Build sales type filter
            sales_type_filter = ""
            if sales_type == "retail":
                sales_type_filter = f"""
                    AND (o.manager_id IN ({','.join(map(str, RETAIL_MANAGER_IDS))})
                         OR (o.manager_id IS NULL AND o.source_id = 4))
                """
            elif sales_type == "b2b":
                sales_type_filter = f"AND o.manager_id = {B2B_MANAGER_ID}"

            query = f"""
            WITH customer_first_order AS (
                -- Get each customer's first order month (cohort)
                SELECT
                    o.buyer_id,
                    DATE_TRUNC('month', MIN(o.order_date)) AS cohort_month
                FROM silver_orders o
                WHERE o.buyer_id IS NOT NULL
                  AND NOT o.is_return
                  {sales_type_filter}
                GROUP BY o.buyer_id
            ),
            customer_cohorts AS (
                -- Add first month revenue per customer
                SELECT
                    c.buyer_id,
                    c.cohort_month,
                    COALESCE(SUM(o.grand_total), 0) AS first_month_revenue
                FROM customer_first_order c
                LEFT JOIN silver_orders o ON c.buyer_id = o.buyer_id
                    AND DATE_TRUNC('month', o.order_date) = c.cohort_month
                    AND NOT o.is_return
                GROUP BY c.buyer_id, c.cohort_month
            ),
            customer_orders AS (
                -- Get all order months per customer with revenue
                SELECT
                    o.buyer_id,
                    c.cohort_month,
                    DATEDIFF('month', c.cohort_month, DATE_TRUNC('month', o.order_date)) AS months_since,
                    o.grand_total AS revenue
                FROM silver_orders o
                JOIN customer_cohorts c ON o.buyer_id = c.buyer_id
                WHERE NOT o.is_return
                  {sales_type_filter}
            ),
            cohort_sizes AS (
                SELECT
                    cohort_month,
                    COUNT(DISTINCT buyer_id) AS size,
                    SUM(first_month_revenue) AS m0_revenue
                FROM customer_cohorts
                GROUP BY cohort_month
            ),
            retention_data AS (
                SELECT
                    r.cohort_month,
                    r.months_since,
                    COUNT(DISTINCT r.buyer_id) AS retained_customers,
                    SUM(r.revenue) AS period_revenue
                FROM customer_orders r
                WHERE r.months_since <= ?
                GROUP BY r.cohort_month, r.months_since
            )
            SELECT
                strftime(r.cohort_month, '%Y-%m') as cohort,
                s.size as cohort_size,
                s.m0_revenue,
                r.months_since as month_number,
                r.retained_customers,
                ROUND(100.0 * r.retained_customers / s.size, 1) as retention_pct,
                r.period_revenue,
                ROUND(100.0 * r.period_revenue / NULLIF(s.m0_revenue, 0), 1) as revenue_retention_pct
            FROM retention_data r
            JOIN cohort_sizes s ON r.cohort_month = s.cohort_month
            WHERE r.cohort_month >= DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '{months_back} months'
            ORDER BY r.cohort_month DESC, r.months_since
            """

            rows = conn.execute(query, [retention_months]).fetchall()

            # Build cohort data structure
            cohorts = {}
            for cohort, size, m0_rev, month_num, retained, pct, rev, rev_pct in rows:
                if cohort not in cohorts:
                    cohorts[cohort] = {
                        "size": size,
                        "m0_revenue": m0_rev or 0,
                        "retention": {},
                        "revenue_retention": {},
                        "revenue": {}
                    }
                cohorts[cohort]["retention"][month_num] = pct
                cohorts[cohort]["revenue_retention"][month_num] = rev_pct
                cohorts[cohort]["revenue"][month_num] = rev or 0

            # Calculate summary metrics
            total_cohort_size = sum(c["size"] for c in cohorts.values())
            total_revenue = sum(c["m0_revenue"] for c in cohorts.values())

            # Average retention by month (customer and revenue)
            avg_customer_retention = {}
            avg_revenue_retention = {}
            for m in range(retention_months + 1):
                cust_values = [
                    c["retention"].get(m)
                    for c in cohorts.values()
                    if c["retention"].get(m) is not None
                ]
                rev_values = [
                    c["revenue_retention"].get(m)
                    for c in cohorts.values()
                    if c["revenue_retention"].get(m) is not None
                ]
                if cust_values:
                    avg_customer_retention[m] = round(sum(cust_values) / len(cust_values), 1)
                if rev_values:
                    avg_revenue_retention[m] = round(sum(rev_values) / len(rev_values), 1)

            return {
                "cohorts": [
                    {
                        "month": cohort,
                        "size": data["size"],
                        "retention": [
                            data["retention"].get(m)
                            for m in range(retention_months + 1)
                        ],
                        "revenueRetention": [
                            data["revenue_retention"].get(m)
                            for m in range(retention_months + 1)
                        ] if include_revenue else None,
                        "revenue": [
                            round(data["revenue"].get(m, 0), 2)
                            for m in range(retention_months + 1)
                        ] if include_revenue else None
                    }
                    for cohort, data in sorted(cohorts.items(), reverse=True)
                ],
                "retentionMonths": retention_months,
                "summary": {
                    "totalCohorts": len(cohorts),
                    "totalCustomers": total_cohort_size,
                    "avgCustomerRetention": avg_customer_retention,
                    "avgRevenueRetention": avg_revenue_retention if include_revenue else None,
                    "totalRevenue": round(total_revenue, 2) if include_revenue else None
                }
            }

    async def get_days_to_second_purchase(
        self,
        months_back: int = 12,
        sales_type: str = "retail"
    ) -> Dict[str, Any]:
        """
        Analyze time between first and second purchase.

        Groups customers into buckets based on how many days it took them
        to make their second purchase. Useful for understanding repurchase cycles.

        Args:
            months_back: How many months of first-time customers to analyze
            sales_type: Filter by sales type (retail/b2b/all)

        Returns:
            Dict with buckets, customer counts, and summary statistics
        """
        async with self.connection() as conn:
            # Build sales type filter
            sales_type_filter = ""
            if sales_type == "retail":
                sales_type_filter = f"""
                    AND (o.manager_id IN ({','.join(map(str, RETAIL_MANAGER_IDS))})
                         OR (o.manager_id IS NULL AND o.source_id = 4))
                """
            elif sales_type == "b2b":
                sales_type_filter = f"AND o.manager_id = {B2B_MANAGER_ID}"

            query = f"""
            WITH customer_orders_ranked AS (
                SELECT
                    o.buyer_id,
                    o.order_date,
                    ROW_NUMBER() OVER (PARTITION BY o.buyer_id ORDER BY o.order_date) AS order_num
                FROM silver_orders o
                WHERE o.buyer_id IS NOT NULL
                  AND NOT o.is_return
                  {sales_type_filter}
            ),
            second_purchase AS (
                SELECT
                    c1.buyer_id,
                    c1.order_date AS first_order,
                    c2.order_date AS second_order,
                    DATEDIFF('day', c1.order_date, c2.order_date) AS days_to_second
                FROM customer_orders_ranked c1
                JOIN customer_orders_ranked c2
                    ON c1.buyer_id = c2.buyer_id
                    AND c1.order_num = 1
                    AND c2.order_num = 2
                WHERE c1.order_date >= CURRENT_DATE - INTERVAL '{months_back} months'
            ),
            bucketed AS (
                SELECT
                    days_to_second,
                    CASE
                        WHEN days_to_second <= 30 THEN '0-30'
                        WHEN days_to_second <= 60 THEN '31-60'
                        WHEN days_to_second <= 90 THEN '61-90'
                        WHEN days_to_second <= 120 THEN '91-120'
                        WHEN days_to_second <= 180 THEN '121-180'
                        ELSE '180+'
                    END AS bucket,
                    CASE
                        WHEN days_to_second <= 30 THEN 1
                        WHEN days_to_second <= 60 THEN 2
                        WHEN days_to_second <= 90 THEN 3
                        WHEN days_to_second <= 120 THEN 4
                        WHEN days_to_second <= 180 THEN 5
                        ELSE 6
                    END AS bucket_order
                FROM second_purchase
            )
            SELECT
                bucket,
                COUNT(*) AS customers,
                ROUND(AVG(days_to_second), 1) AS avg_days
            FROM bucketed
            GROUP BY bucket, bucket_order
            ORDER BY bucket_order
            """

            rows = conn.execute(query).fetchall()

            # Calculate totals and percentages
            total_repeat = sum(row[1] for row in rows)
            buckets = []
            for bucket, customers, avg_days in rows:
                buckets.append({
                    "bucket": bucket,
                    "customers": customers,
                    "avgDays": avg_days,
                    "percentage": round(100.0 * customers / total_repeat, 1) if total_repeat > 0 else 0
                })

            # Get median
            median_query = f"""
            WITH customer_orders_ranked AS (
                SELECT
                    o.buyer_id,
                    o.order_date,
                    ROW_NUMBER() OVER (PARTITION BY o.buyer_id ORDER BY o.order_date) AS order_num
                FROM silver_orders o
                WHERE o.buyer_id IS NOT NULL
                  AND NOT o.is_return
                  {sales_type_filter}
            ),
            second_purchase AS (
                SELECT
                    DATEDIFF('day', c1.order_date, c2.order_date) AS days_to_second
                FROM customer_orders_ranked c1
                JOIN customer_orders_ranked c2
                    ON c1.buyer_id = c2.buyer_id
                    AND c1.order_num = 1
                    AND c2.order_num = 2
                WHERE c1.order_date >= CURRENT_DATE - INTERVAL '{months_back} months'
            )
            SELECT
                MEDIAN(days_to_second) AS median_days,
                AVG(days_to_second) AS avg_days
            FROM second_purchase
            """
            stats = conn.execute(median_query).fetchone()
            median_days = stats[0] if stats else None
            avg_days_overall = stats[1] if stats else None

            return {
                "buckets": buckets,
                "summary": {
                    "totalRepeatCustomers": total_repeat,
                    "medianDays": round(median_days, 1) if median_days else None,
                    "avgDays": round(avg_days_overall, 1) if avg_days_overall else None
                }
            }

    async def get_cohort_ltv(
        self,
        months_back: int = 12,
        sales_type: str = "retail"
    ) -> Dict[str, Any]:
        """
        Get cumulative lifetime value by cohort.

        Shows how much revenue each cohort has generated over time,
        with cumulative totals per month since first purchase.

        Args:
            months_back: How many months of cohorts to analyze
            sales_type: Filter by sales type (retail/b2b/all)

        Returns:
            Dict with cohort LTV data and summary statistics
        """
        async with self.connection() as conn:
            # Build sales type filter
            sales_type_filter = ""
            if sales_type == "retail":
                sales_type_filter = f"""
                    AND (o.manager_id IN ({','.join(map(str, RETAIL_MANAGER_IDS))})
                         OR (o.manager_id IS NULL AND o.source_id = 4))
                """
            elif sales_type == "b2b":
                sales_type_filter = f"AND o.manager_id = {B2B_MANAGER_ID}"

            query = f"""
            WITH customer_cohorts AS (
                SELECT
                    o.buyer_id,
                    DATE_TRUNC('month', MIN(o.order_date)) AS cohort_month
                FROM silver_orders o
                WHERE o.buyer_id IS NOT NULL
                  AND NOT o.is_return
                  {sales_type_filter}
                GROUP BY o.buyer_id
            ),
            customer_revenue AS (
                SELECT
                    o.buyer_id,
                    c.cohort_month,
                    DATEDIFF('month', c.cohort_month, DATE_TRUNC('month', o.order_date)) AS months_since,
                    SUM(o.grand_total) AS revenue
                FROM silver_orders o
                JOIN customer_cohorts c ON o.buyer_id = c.buyer_id
                WHERE NOT o.is_return
                  {sales_type_filter}
                GROUP BY o.buyer_id, c.cohort_month, DATEDIFF('month', c.cohort_month, DATE_TRUNC('month', o.order_date))
            ),
            cohort_monthly AS (
                SELECT
                    cohort_month,
                    months_since,
                    SUM(revenue) AS total_revenue,
                    COUNT(DISTINCT buyer_id) AS active_customers
                FROM customer_revenue
                WHERE months_since <= 12
                GROUP BY cohort_month, months_since
            ),
            cohort_sizes AS (
                SELECT cohort_month, COUNT(DISTINCT buyer_id) AS cohort_size
                FROM customer_cohorts
                GROUP BY cohort_month
            )
            SELECT
                strftime(cm.cohort_month, '%Y-%m') AS cohort,
                cs.cohort_size,
                cm.months_since,
                cm.total_revenue,
                cm.active_customers
            FROM cohort_monthly cm
            JOIN cohort_sizes cs ON cm.cohort_month = cs.cohort_month
            WHERE cm.cohort_month >= DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '{months_back} months'
            ORDER BY cm.cohort_month DESC, cm.months_since
            """

            rows = conn.execute(query).fetchall()

            # Build cohort LTV structure with cumulative revenue
            cohorts = {}
            for cohort, size, months_since, revenue, active in rows:
                if cohort not in cohorts:
                    cohorts[cohort] = {
                        "size": size,
                        "monthly_revenue": {},
                        "cumulative": []
                    }
                cohorts[cohort]["monthly_revenue"][months_since] = revenue or 0

            # Calculate cumulative revenue for each cohort
            for cohort_data in cohorts.values():
                cumulative = 0
                cumulative_list = []
                for m in range(13):  # M0 to M12
                    cumulative += cohort_data["monthly_revenue"].get(m, 0)
                    cumulative_list.append(round(cumulative, 2))
                cohort_data["cumulative"] = cumulative_list

            # Calculate average LTV
            all_ltv = [
                c["cumulative"][-1] / c["size"] if c["size"] > 0 else 0
                for c in cohorts.values()
            ]
            avg_ltv = round(sum(all_ltv) / len(all_ltv), 2) if all_ltv else 0

            # Find best cohort
            best_cohort = max(
                cohorts.items(),
                key=lambda x: x[1]["cumulative"][-1] / x[1]["size"] if x[1]["size"] > 0 else 0,
                default=(None, {"cumulative": [0], "size": 1})
            )

            return {
                "cohorts": [
                    {
                        "month": cohort,
                        "customerCount": data["size"],
                        "cumulativeRevenue": data["cumulative"],
                        "avgLTV": round(data["cumulative"][-1] / data["size"], 2) if data["size"] > 0 else 0
                    }
                    for cohort, data in sorted(cohorts.items(), reverse=True)
                ],
                "summary": {
                    "avgLTV": avg_ltv,
                    "bestCohort": best_cohort[0],
                    "bestCohortLTV": round(best_cohort[1]["cumulative"][-1] / best_cohort[1]["size"], 2) if best_cohort[1]["size"] > 0 else 0
                }
            }

    async def get_at_risk_customers(
        self,
        days_threshold: int = 90,
        sales_type: str = "retail"
    ) -> Dict[str, Any]:
        """
        Identify at-risk customers who haven't purchased recently.

        Segments customers by their cohort and identifies those who haven't
        made a purchase in the specified number of days.

        Args:
            days_threshold: Days since last purchase to consider "at risk"
            sales_type: Filter by sales type (retail/b2b/all)

        Returns:
            Dict with at-risk counts by cohort and summary statistics
        """
        async with self.connection() as conn:
            # Build sales type filter
            sales_type_filter = ""
            if sales_type == "retail":
                sales_type_filter = f"""
                    AND (o.manager_id IN ({','.join(map(str, RETAIL_MANAGER_IDS))})
                         OR (o.manager_id IS NULL AND o.source_id = 4))
                """
            elif sales_type == "b2b":
                sales_type_filter = f"AND o.manager_id = {B2B_MANAGER_ID}"

            query = f"""
            WITH customer_activity AS (
                SELECT
                    o.buyer_id,
                    DATE_TRUNC('month', MIN(o.order_date)) AS cohort_month,
                    MAX(o.order_date) AS last_order_date,
                    DATEDIFF('day', MAX(o.order_date), CURRENT_DATE) AS days_since_last,
                    COUNT(*) AS total_orders,
                    SUM(o.grand_total) AS total_revenue
                FROM silver_orders o
                WHERE o.buyer_id IS NOT NULL
                  AND NOT o.is_return
                  {sales_type_filter}
                GROUP BY o.buyer_id
            )
            SELECT
                strftime(cohort_month, '%Y-%m') AS cohort,
                COUNT(*) AS total_customers,
                COUNT(*) FILTER (WHERE days_since_last > ?) AS at_risk_count,
                ROUND(100.0 * COUNT(*) FILTER (WHERE days_since_last > ?) / COUNT(*), 1) AS at_risk_pct,
                SUM(total_revenue) FILTER (WHERE days_since_last > ?) AS at_risk_revenue,
                AVG(total_orders) FILTER (WHERE days_since_last > ?) AS avg_orders_at_risk
            FROM customer_activity
            WHERE cohort_month >= DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '12 months'
            GROUP BY cohort_month
            ORDER BY cohort_month DESC
            """

            rows = conn.execute(query, [days_threshold, days_threshold, days_threshold, days_threshold]).fetchall()

            cohorts = []
            total_at_risk = 0
            total_customers = 0
            for cohort, total, at_risk, pct, revenue, avg_orders in rows:
                cohorts.append({
                    "cohort": cohort,
                    "totalCustomers": total,
                    "atRiskCount": at_risk,
                    "atRiskPct": pct,
                    "atRiskRevenue": round(revenue, 2) if revenue else 0,
                    "avgOrdersAtRisk": round(avg_orders, 1) if avg_orders else 0
                })
                total_at_risk += at_risk
                total_customers += total

            return {
                "cohorts": cohorts,
                "daysThreshold": days_threshold,
                "summary": {
                    "totalAtRisk": total_at_risk,
                    "totalCustomers": total_customers,
                    "overallAtRiskPct": round(100.0 * total_at_risk / total_customers, 1) if total_customers > 0 else 0
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

    # ═══════════════════════════════════════════════════════════════════════════
    # VIEW-BASED QUERIES (Layer 3 & 4)
    # ═══════════════════════════════════════════════════════════════════════════

    async def get_inventory_summary_v2(self) -> Dict[str, Any]:
        """Get inventory summary using Layer 3 views.

        Returns:
            Dict with summary by status, aging buckets, and category velocity
        """
        async with self.connection() as conn:
            # Summary by status
            summary = conn.execute("SELECT * FROM v_inventory_summary").fetchall()
            summary_dict = {}
            for row in summary:
                status, sku_count, units, value, pct = row
                summary_dict[status] = {
                    "skuCount": sku_count,
                    "quantity": units or 0,
                    "value": float(value or 0),
                    "valuePercent": float(pct or 0),
                }

            # Total
            total = conn.execute("""
                SELECT COUNT(*), SUM(available), SUM(available_value)
                FROM v_sku_status
            """).fetchone()

            # Aging buckets
            aging = conn.execute("SELECT * FROM v_aging_buckets").fetchall()
            aging_buckets = [
                {"bucket": row[0], "skuCount": row[1], "units": row[2], "value": float(row[3] or 0)}
                for row in aging
            ]

            # Category velocity
            velocity = conn.execute("SELECT * FROM v_category_velocity").fetchall()
            category_thresholds = [
                {
                    "categoryId": row[0],
                    "categoryName": row[1] or "Uncategorized",
                    "sampleSize": row[2],
                    "p50": int(row[3]) if row[3] else None,
                    "p75": int(row[4]) if row[4] else None,
                    "p90": int(row[5]) if row[5] else None,
                    "thresholdDays": int(row[6]) if row[6] else 180,
                }
                for row in velocity
            ]

            return {
                "summary": {
                    "healthy": summary_dict.get("healthy", {"skuCount": 0, "quantity": 0, "value": 0, "valuePercent": 0}),
                    "atRisk": summary_dict.get("at_risk", {"skuCount": 0, "quantity": 0, "value": 0, "valuePercent": 0}),
                    "deadStock": summary_dict.get("dead_stock", {"skuCount": 0, "quantity": 0, "value": 0, "valuePercent": 0}),
                    "neverSold": summary_dict.get("never_sold", {"skuCount": 0, "quantity": 0, "value": 0, "valuePercent": 0}),
                    "total": {
                        "skuCount": total[0] or 0,
                        "quantity": total[1] or 0,
                        "value": float(total[2] or 0),
                    },
                },
                "agingBuckets": aging_buckets,
                "categoryThresholds": category_thresholds,
            }

    async def get_dead_stock_items_v2(self, limit: int = 100) -> List[Dict[str, Any]]:
        """Get dead stock and at-risk items using Layer 3 views.

        Returns:
            List of items with status != healthy
        """
        async with self.connection() as conn:
            items = conn.execute(f"""
                SELECT
                    offer_id, sku, name, brand, category_name,
                    available, available_value, price,
                    days_since_sale, days_in_stock, threshold_days, status
                FROM v_sku_status
                WHERE status != 'healthy'
                ORDER BY available_value DESC
                LIMIT {limit}
            """).fetchall()

            return [
                {
                    "id": row[0],
                    "sku": row[1],
                    "name": row[2],
                    "brand": row[3],
                    "categoryName": row[4],
                    "quantity": row[5],
                    "value": float(row[6] or 0),
                    "price": float(row[7] or 0),
                    "daysSinceSale": row[8],
                    "daysInStock": row[9],
                    "thresholdDays": row[10],
                    "status": row[11],
                }
                for row in items
            ]

    async def get_recommended_actions(self, limit: int = 50) -> List[Dict[str, Any]]:
        """Get recommended actions for dead stock using Layer 4 view.

        Returns:
            List of items with recommended actions
        """
        async with self.connection() as conn:
            items = conn.execute(f"""
                SELECT * FROM v_recommended_actions
                WHERE action IS NOT NULL
                LIMIT {limit}
            """).fetchall()

            return [
                {
                    "offerId": row[0],
                    "sku": row[1],
                    "name": row[2],
                    "brand": row[3],
                    "categoryName": row[4],
                    "units": row[5],
                    "value": float(row[6] or 0),
                    "daysSinceSale": row[7],
                    "daysInStock": row[8],
                    "status": row[9],
                    "action": row[10],
                }
                for row in items
            ]

    async def get_restock_alerts(self, limit: int = 50) -> List[Dict[str, Any]]:
        """Get low stock alerts using Layer 4 view.

        Returns:
            List of items that need restocking
        """
        async with self.connection() as conn:
            items = conn.execute(f"""
                SELECT * FROM v_restock_alerts
                WHERE alert_level IS NOT NULL
                LIMIT {limit}
            """).fetchall()

            return [
                {
                    "offerId": row[0],
                    "sku": row[1],
                    "name": row[2],
                    "brand": row[3],
                    "unitsLeft": row[4],
                    "daysSinceSale": row[5],
                    "alertLevel": row[6],
                }
                for row in items
            ]


    # ─── Revenue Predictions ─────────────────────────────────────────────────

    async def store_predictions(
        self,
        predictions: List[Dict],
        sales_type: str = "retail",
        metrics: Optional[Dict[str, float]] = None,
    ) -> int:
        """Store revenue predictions in DuckDB.

        Args:
            predictions: List of dicts with 'date' and 'predicted_revenue' keys.
            sales_type: 'retail' or 'b2b'.
            metrics: Model metrics dict with 'mae' and 'mape'.

        Returns:
            Number of predictions stored.
        """
        if not predictions:
            return 0

        mae = metrics.get('mae', 0) if metrics else 0
        mape = metrics.get('mape', 0) if metrics else 0

        async with self.connection() as conn:
            # Delete existing predictions for this sales_type in the date range
            dates = [p['date'] for p in predictions]
            min_date = min(dates)
            max_date = max(dates)

            conn.execute(
                """DELETE FROM revenue_predictions
                   WHERE sales_type = ? AND prediction_date >= ? AND prediction_date <= ?""",
                [sales_type, min_date, max_date]
            )

            # Insert new predictions
            for pred in predictions:
                conn.execute(
                    """INSERT INTO revenue_predictions
                       (prediction_date, sales_type, predicted_revenue, model_mae, model_mape)
                       VALUES (?, ?, ?, ?, ?)""",
                    [pred['date'], sales_type, pred['predicted_revenue'], mae, mape]
                )

            logger.info(f"Stored {len(predictions)} predictions for {sales_type}")
            return len(predictions)

    async def get_predictions(
        self,
        start_date: date,
        end_date: date,
        sales_type: str = "retail",
    ) -> List[Dict]:
        """Get stored revenue predictions for a date range.

        Returns:
            List of dicts with date, predicted_revenue, model_mae, model_mape.
        """
        async with self.connection() as conn:
            rows = conn.execute(
                """SELECT prediction_date, predicted_revenue, model_mae, model_mape
                   FROM revenue_predictions
                   WHERE sales_type = ?
                     AND prediction_date >= ?
                     AND prediction_date <= ?
                   ORDER BY prediction_date""",
                [sales_type, start_date.isoformat(), end_date.isoformat()]
            ).fetchall()

        return [
            {
                'date': row[0].isoformat() if hasattr(row[0], 'isoformat') else str(row[0]),
                'predicted_revenue': float(row[1]),
                'model_mae': float(row[2]) if row[2] else 0,
                'model_mape': float(row[3]) if row[3] else 0,
            }
            for row in rows
        ]


    async def get_daily_revenue_for_dates(
        self,
        dates: list,
        sales_type: str = "retail",
    ) -> Dict[date, float]:
        """Get daily revenue for a list of specific dates.

        Returns dict mapping date -> revenue total.
        Used for extending comparison data to cover forecast dates.
        """
        if not dates:
            return {}

        sales_filter = self._build_sales_type_filter(sales_type)
        return_statuses = (19, 22, 21, 23)
        placeholders = ", ".join(["?"] * len(dates))
        date_strs = [d.isoformat() for d in dates]

        async with self.connection() as conn:
            rows = conn.execute(
                f"""SELECT {_date_in_kyiv('o.ordered_at')} as day,
                           SUM(o.grand_total) as revenue
                    FROM orders o
                    WHERE {_date_in_kyiv('o.ordered_at')} IN ({placeholders})
                      AND o.status_id NOT IN {return_statuses}
                      AND o.source_id IN (1, 2, 4)
                      AND {sales_filter}
                    GROUP BY day""",
                date_strs,
            ).fetchall()

        return {row[0]: float(row[1]) for row in rows}


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
