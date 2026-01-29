"""
Base repository with connection management and schema initialization.

All domain repositories inherit from this class.
"""
import asyncio
from contextlib import asynccontextmanager
from datetime import datetime
from pathlib import Path
from typing import Optional, Any
from zoneinfo import ZoneInfo

import duckdb

from core.observability import get_logger
from bot.config import DEFAULT_TIMEZONE

logger = get_logger(__name__)

# Database configuration
DB_DIR = Path(__file__).parent.parent.parent / "data"
DB_PATH = DB_DIR / "analytics.duckdb"
DEFAULT_TZ = ZoneInfo(DEFAULT_TIMEZONE)

# B2B (wholesale) manager ID - Olga D
B2B_MANAGER_ID = 15

# Retail manager IDs (including historical managers who left: 8, 11, 17, 19)
RETAIL_MANAGER_IDS = [4, 8, 11, 16, 17, 19, 22]

# Timezone for date extraction
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


class BaseRepository:
    """
    Base repository with DuckDB connection management.

    Usage:
        class OrdersRepository(BaseRepository):
            async def get_order(self, order_id: int):
                async with self.connection() as conn:
                    return conn.execute("SELECT * FROM orders WHERE id = ?", [order_id]).fetchone()
    """

    def __init__(self, db_path: Path = DB_PATH):
        self.db_path = db_path
        self._connection: Optional[duckdb.DuckDBPyConnection] = None
        self._lock = asyncio.Lock()
        self._schema_initialized = False

    async def connect(self) -> None:
        """Initialize database connection and schema."""
        DB_DIR.mkdir(parents=True, exist_ok=True)

        async with self._lock:
            if self._connection is None:
                self._connection = duckdb.connect(str(self.db_path))
                if not self._schema_initialized:
                    await self._init_schema()
                    await self._run_migrations()
                    await self._create_inventory_views()
                    self._schema_initialized = True
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

    async def execute(self, sql: str, params: list = None) -> Any:
        """Execute SQL query and return result."""
        async with self.connection() as conn:
            if params:
                return conn.execute(sql, params)
            return conn.execute(sql)

    async def fetchone(self, sql: str, params: list = None) -> Optional[tuple]:
        """Execute query and fetch one result."""
        result = await self.execute(sql, params)
        return result.fetchone()

    async def fetchall(self, sql: str, params: list = None) -> list:
        """Execute query and fetch all results."""
        result = await self.execute(sql, params)
        return result.fetchall()

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
            updated_at TIMESTAMP WITH TIME ZONE,
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

        -- Pre-aggregated daily statistics
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

        -- Expense types
        CREATE TABLE IF NOT EXISTS expense_types (
            id INTEGER PRIMARY KEY,
            name VARCHAR NOT NULL,
            alias VARCHAR,
            is_active BOOLEAN DEFAULT TRUE,
            synced_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
        );

        -- Expenses
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
        );

        -- Offers (product variations)
        CREATE TABLE IF NOT EXISTS offers (
            id INTEGER PRIMARY KEY,
            product_id INTEGER NOT NULL,
            sku VARCHAR,
            synced_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
        );

        -- Offer stocks
        CREATE TABLE IF NOT EXISTS offer_stocks (
            id INTEGER PRIMARY KEY,
            sku VARCHAR,
            price DECIMAL(12, 2),
            purchased_price DECIMAL(12, 2),
            quantity INTEGER DEFAULT 0,
            reserve INTEGER DEFAULT 0,
            synced_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
        );

        -- Inventory history (legacy, kept for backwards compatibility)
        CREATE TABLE IF NOT EXISTS inventory_history (
            date DATE NOT NULL,
            total_quantity INTEGER NOT NULL,
            total_value DECIMAL(14, 2) NOT NULL,
            total_reserve INTEGER DEFAULT 0,
            sku_count INTEGER DEFAULT 0,
            recorded_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (date)
        );

        -- Layer 1: SKU Inventory Status
        CREATE TABLE IF NOT EXISTS sku_inventory_status (
            offer_id INTEGER PRIMARY KEY,
            product_id INTEGER NOT NULL,
            sku VARCHAR NOT NULL,
            name VARCHAR,
            brand VARCHAR,
            category_id INTEGER,
            quantity INTEGER NOT NULL DEFAULT 0,
            reserve INTEGER NOT NULL DEFAULT 0,
            price DECIMAL(12, 2) NOT NULL DEFAULT 0,
            purchased_price DECIMAL(12, 2),
            last_sale_date DATE,
            first_seen_at DATE NOT NULL DEFAULT CURRENT_DATE,
            updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
        );

        CREATE INDEX IF NOT EXISTS idx_sku_status_category ON sku_inventory_status(category_id);
        CREATE INDEX IF NOT EXISTS idx_sku_status_brand ON sku_inventory_status(brand);
        CREATE INDEX IF NOT EXISTS idx_sku_status_quantity ON sku_inventory_status(quantity);

        -- Layer 2: SKU Inventory History
        CREATE TABLE IF NOT EXISTS inventory_sku_history (
            date DATE NOT NULL,
            offer_id INTEGER NOT NULL,
            quantity INTEGER NOT NULL,
            reserve INTEGER NOT NULL,
            price DECIMAL(12, 2) NOT NULL,
            PRIMARY KEY (date, offer_id)
        );

        CREATE INDEX IF NOT EXISTS idx_sku_history_offer ON inventory_sku_history(offer_id, date DESC);

        -- Sync metadata
        CREATE TABLE IF NOT EXISTS sync_metadata (
            key VARCHAR PRIMARY KEY,
            value VARCHAR,
            updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
        );

        -- Revenue goals
        CREATE TABLE IF NOT EXISTS revenue_goals (
            period_type VARCHAR(10) NOT NULL,
            sales_type VARCHAR(10) NOT NULL DEFAULT 'retail',
            goal_amount DECIMAL(12, 2) NOT NULL,
            is_custom BOOLEAN DEFAULT FALSE,
            calculated_goal DECIMAL(12, 2),
            growth_factor DECIMAL(4, 2) DEFAULT 1.10,
            updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (period_type, sales_type)
        );

        -- Seasonal indices
        CREATE TABLE IF NOT EXISTS seasonal_indices (
            month INTEGER NOT NULL,
            sales_type VARCHAR(10) NOT NULL DEFAULT 'retail',
            seasonality_index DECIMAL(6, 4),
            sample_size INTEGER,
            avg_revenue DECIMAL(12, 2),
            min_revenue DECIMAL(12, 2),
            max_revenue DECIMAL(12, 2),
            yoy_growth DECIMAL(6, 4),
            confidence VARCHAR(10),
            updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (month, sales_type)
        );

        -- Weekly patterns
        CREATE TABLE IF NOT EXISTS weekly_patterns (
            month INTEGER NOT NULL,
            week_of_month INTEGER NOT NULL,
            sales_type VARCHAR(10) NOT NULL DEFAULT 'retail',
            weight DECIMAL(6, 4),
            sample_size INTEGER,
            updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (month, week_of_month, sales_type)
        );

        -- Growth metrics
        CREATE TABLE IF NOT EXISTS growth_metrics (
            metric_type VARCHAR(20) NOT NULL,
            sales_type VARCHAR(10) NOT NULL DEFAULT 'retail',
            value DECIMAL(8, 4),
            period_start DATE,
            period_end DATE,
            sample_size INTEGER,
            updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (metric_type, sales_type)
        );

        -- Managers
        CREATE TABLE IF NOT EXISTS managers (
            id INTEGER PRIMARY KEY,
            name VARCHAR NOT NULL,
            email VARCHAR,
            status VARCHAR,
            is_retail BOOLEAN DEFAULT FALSE,
            first_order_date DATE,
            last_order_date DATE,
            order_count INTEGER DEFAULT 0,
            synced_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
        );

        -- Indexes
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
        CREATE INDEX IF NOT EXISTS idx_orders_source_date ON orders(source_id, ordered_at);
        CREATE INDEX IF NOT EXISTS idx_orders_status_date ON orders(status_id, ordered_at);
        CREATE INDEX IF NOT EXISTS idx_orders_manager_date ON orders(manager_id, ordered_at);
        CREATE INDEX IF NOT EXISTS idx_orders_buyer_date ON orders(buyer_id, ordered_at);
        """
        self._connection.execute(schema_sql)
        logger.info("DuckDB schema initialized")

    async def _run_migrations(self) -> None:
        """Run database migrations for schema changes."""
        # Migration 1: Add updated_at column to orders table
        try:
            self._connection.execute(
                "ALTER TABLE orders ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP WITH TIME ZONE"
            )
            logger.debug("Migration: updated_at column added/verified")
        except Exception as e:
            logger.debug(f"Migration note: {e}")

        # Migration 2: Add sales_type to revenue_goals table
        try:
            self._connection.execute(
                "ALTER TABLE revenue_goals ADD COLUMN IF NOT EXISTS sales_type VARCHAR(10) DEFAULT 'retail'"
            )
        except Exception:
            pass

    async def _create_inventory_views(self) -> None:
        """Create analytics views for inventory (Layer 3 & 4)."""
        views_sql = """
        -- Layer 3: SKU Analysis View
        CREATE OR REPLACE VIEW v_sku_analysis AS
        SELECT
            s.offer_id,
            s.product_id,
            s.sku,
            s.name,
            s.brand,
            s.category_id,
            c.name as category_name,
            s.quantity,
            s.reserve,
            s.quantity - s.reserve as available,
            s.price,
            s.purchased_price,
            s.quantity * s.price as stock_value,
            s.last_sale_date,
            CASE
                WHEN s.last_sale_date IS NULL THEN 999
                ELSE CURRENT_DATE - s.last_sale_date
            END as days_since_sale,
            s.first_seen_at,
            CURRENT_DATE - s.first_seen_at as days_in_stock
        FROM sku_inventory_status s
        LEFT JOIN categories c ON s.category_id = c.id
        WHERE s.quantity > 0;

        -- Layer 3: SKU Status View (with classification)
        CREATE OR REPLACE VIEW v_sku_status AS
        SELECT
            *,
            CASE
                WHEN days_since_sale > 180 THEN 'dead'
                WHEN days_since_sale > 90 THEN 'slow'
                WHEN days_since_sale > 30 THEN 'moderate'
                ELSE 'active'
            END as status,
            CASE
                WHEN days_since_sale > 180 THEN 4
                WHEN days_since_sale > 90 THEN 3
                WHEN days_since_sale > 30 THEN 2
                ELSE 1
            END as priority
        FROM v_sku_analysis;

        -- Layer 3: Inventory Summary View
        CREATE OR REPLACE VIEW v_inventory_summary AS
        SELECT
            status,
            COUNT(*) as sku_count,
            SUM(quantity) as total_units,
            SUM(stock_value) as total_value,
            AVG(days_since_sale) as avg_days_since_sale
        FROM v_sku_status
        GROUP BY status;

        -- Layer 3: Aging Buckets View
        CREATE OR REPLACE VIEW v_aging_buckets AS
        SELECT
            CASE
                WHEN days_since_sale <= 30 THEN '0-30 days'
                WHEN days_since_sale <= 60 THEN '31-60 days'
                WHEN days_since_sale <= 90 THEN '61-90 days'
                WHEN days_since_sale <= 180 THEN '91-180 days'
                ELSE '180+ days'
            END as bucket,
            CASE
                WHEN days_since_sale <= 30 THEN 1
                WHEN days_since_sale <= 60 THEN 2
                WHEN days_since_sale <= 90 THEN 3
                WHEN days_since_sale <= 180 THEN 4
                ELSE 5
            END as bucket_order,
            COUNT(*) as sku_count,
            SUM(quantity) as total_units,
            SUM(stock_value) as total_value
        FROM v_sku_status
        GROUP BY 1, 2
        ORDER BY bucket_order;

        -- Layer 4: Recommended Actions View
        CREATE OR REPLACE VIEW v_recommended_actions AS
        SELECT
            offer_id,
            sku,
            name,
            brand,
            category_name,
            quantity,
            stock_value,
            days_since_sale,
            status,
            CASE
                WHEN status = 'dead' THEN 'Consider liquidation or clearance sale'
                WHEN status = 'slow' THEN 'Apply promotional pricing'
                WHEN status = 'moderate' AND quantity > 10 THEN 'Monitor closely'
                ELSE 'No action needed'
            END as recommended_action,
            CASE
                WHEN status = 'dead' THEN stock_value * 0.3
                WHEN status = 'slow' THEN stock_value * 0.15
                ELSE 0
            END as potential_loss
        FROM v_sku_status
        WHERE status IN ('dead', 'slow')
        ORDER BY potential_loss DESC;

        -- Layer 4: Restock Alerts View
        CREATE OR REPLACE VIEW v_restock_alerts AS
        SELECT
            offer_id,
            sku,
            name,
            brand,
            category_name,
            quantity,
            reserve,
            available,
            days_since_sale,
            CASE
                WHEN available <= 0 THEN 'Out of Stock'
                WHEN available <= 2 THEN 'Critical Low'
                WHEN available <= 5 THEN 'Low Stock'
                ELSE 'OK'
            END as alert_level
        FROM v_sku_status
        WHERE available <= 5 AND status = 'active'
        ORDER BY available ASC, days_since_sale ASC;
        """
        self._connection.execute(views_sql)
        logger.debug("Inventory views created")

    def _build_sales_type_filter(self, sales_type: str, table_alias: str = "o") -> str:
        """Build SQL WHERE clause for sales type filtering.

        Args:
            sales_type: 'retail', 'b2b', or 'all'
            table_alias: Table alias (default 'o' for orders)

        Returns:
            SQL WHERE clause fragment
        """
        if sales_type == "b2b":
            return f"{table_alias}.manager_id = {B2B_MANAGER_ID}"
        elif sales_type == "retail":
            # Retail: specific manager IDs + NULL (Shopify orders)
            retail_ids = ",".join(str(id) for id in RETAIL_MANAGER_IDS)
            return f"""(
                {table_alias}.manager_id IN ({retail_ids})
                OR ({table_alias}.manager_id IS NULL AND {table_alias}.source_id = 4)
            )"""
        else:  # 'all'
            return "1=1"
