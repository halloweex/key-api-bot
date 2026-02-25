"""
DuckDB analytics store for KeyCRM dashboard.

Provides persistent storage for orders, products, and pre-aggregated statistics.
Uses incremental sync to minimize API calls and enable fast historical queries.

Domain-specific query methods are organized into repository mixins:
- UsersMixin: User management and permissions
- TrafficMixin: UTM parsing, traffic analytics
- CustomersMixin: Customer insights, cohort analysis
- GoalsMixin: Revenue goals, seasonality, forecasting
- InventoryMixin: Stock management and analysis
- ExpensesMixin: Expense tracking and profit analysis
- RevenueMixin: Revenue trends, sales analytics, products
"""
import asyncio
import logging
import re
from concurrent.futures import ThreadPoolExecutor
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
from core.duckdb_constants import (
    DB_DIR, DB_PATH, DEFAULT_TZ, DEFAULT_QUERY_TIMEOUT, LONG_QUERY_TIMEOUT,
    B2B_MANAGER_ID, RETAIL_MANAGER_IDS, DISPLAY_TIMEZONE, _date_in_kyiv,
)
from core.repositories import (
    UsersMixin, TrafficMixin, CustomersMixin, GoalsMixin,
    InventoryMixin, ExpensesMixin, RevenueMixin, ProductsIntelMixin,
)

logger = logging.getLogger(__name__)


class DuckDBStore(
    UsersMixin, TrafficMixin, CustomersMixin, GoalsMixin,
    InventoryMixin, ExpensesMixin, RevenueMixin, ProductsIntelMixin,
):
    """
    Async-compatible DuckDB store for analytics data.

    Features:
    - Persistent storage (survives restarts)
    - Incremental sync from KeyCRM API
    - Pre-aggregated daily statistics
    - Fast analytical queries
    - Thread offloading to avoid blocking asyncio event loop
    """

    def __init__(self, db_path: Path = DB_PATH):
        self.db_path = db_path
        self._connection: Optional[duckdb.DuckDBPyConnection] = None
        self._lock = asyncio.Lock()  # Serializes all database access

        # Thread pool for offloading blocking DB operations
        self._executor: Optional[ThreadPoolExecutor] = None

        # Stats for monitoring
        self._total_queries = 0

    async def connect(self) -> None:
        """Initialize database connection, schema, and thread pool."""
        DB_DIR.mkdir(parents=True, exist_ok=True)

        async with self._lock:
            if self._connection is None:
                self._connection = duckdb.connect(str(self.db_path))
                await self._init_schema()

                # Thread pool for offloading blocking operations
                self._executor = ThreadPoolExecutor(
                    max_workers=1,  # Single worker - DuckDB requires serialized access
                    thread_name_prefix="duckdb"
                )

                logger.info(f"DuckDB connected: {self.db_path}")

    async def close(self) -> None:
        """Close database connection and thread pool."""
        async with self._lock:
            # Shutdown thread pool (waits for in-flight queries to finish)
            if self._executor:
                self._executor.shutdown(wait=True)
                self._executor = None

            # Close main connection
            if self._connection:
                self._connection.close()
                self._connection = None
                logger.info("DuckDB connection closed")

    async def checkpoint(self) -> None:
        """
        Force WAL checkpoint to flush changes to main database file.

        DuckDB uses Write-Ahead Logging (WAL) for durability. The WAL file
        can grow over time with many writes. CHECKPOINT flushes all pending
        changes to the main database file and resets the WAL.
        """
        async with self._lock:
            if self._connection:
                self._connection.execute("CHECKPOINT")
                logger.info("DuckDB checkpoint completed")

    def get_connection_info(self) -> Dict[str, Any]:
        """Get connection info for monitoring."""
        return {
            "status": "active" if self._connection else "not_initialized",
            "total_queries": self._total_queries,
            "db_path": str(self.db_path),
        }

    @asynccontextmanager
    async def connection(self):
        """Get database connection with automatic reconnection.

        Acquires lock to ensure single-threaded DuckDB access.
        DuckDB connections are NOT thread-safe - only one thread can use
        a connection at a time.
        """
        if self._connection is None:
            await self.connect()
        async with self._lock:
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

        Offloads blocking DB work to thread pool to avoid blocking event loop.

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
            self._total_queries += 1
            try:
                loop = asyncio.get_event_loop()

                def _run():
                    return conn.execute(query, params or []).fetchone()

                return await asyncio.wait_for(
                    loop.run_in_executor(self._executor, _run),
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

        Offloads blocking DB work to thread pool to avoid blocking event loop.

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
            self._total_queries += 1
            try:
                loop = asyncio.get_event_loop()

                def _run():
                    return conn.execute(query, params or []).fetchall()

                return await asyncio.wait_for(
                    loop.run_in_executor(self._executor, _run),
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

        Offloads blocking DB work to thread pool to avoid blocking event loop.

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
            self._total_queries += 1
            try:
                loop = asyncio.get_event_loop()

                def _run():
                    return conn.execute(query, params or []).fetchdf()

                return await asyncio.wait_for(
                    loop.run_in_executor(self._executor, _run),
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
            manager_comment TEXT,  -- Contains UTM data for Shopify orders
            synced_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
        );

        -- Orders indexes for common queries
        CREATE INDEX IF NOT EXISTS idx_orders_ordered_at ON orders(ordered_at);
        CREATE INDEX IF NOT EXISTS idx_orders_status ON orders(status_id);
        CREATE INDEX IF NOT EXISTS idx_orders_buyer ON orders(buyer_id);
        CREATE INDEX IF NOT EXISTS idx_orders_source ON orders(source_id);
        CREATE INDEX IF NOT EXISTS idx_orders_manager ON orders(manager_id);

        -- Order products (line items)
        CREATE TABLE IF NOT EXISTS order_products (
            id BIGINT PRIMARY KEY,
            order_id INTEGER NOT NULL,
            product_id INTEGER,
            name VARCHAR NOT NULL,
            quantity INTEGER NOT NULL,
            price_sold DECIMAL(12, 2) NOT NULL
            -- FK removed due to DuckDB UPDATE/DELETE bug with foreign keys
            -- See: https://github.com/duckdb/duckdb/issues/4023
        );

        -- Order products indexes for joins
        CREATE INDEX IF NOT EXISTS idx_order_products_order ON order_products(order_id);
        CREATE INDEX IF NOT EXISTS idx_order_products_product ON order_products(product_id);

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

        -- Categories index for tree traversal
        CREATE INDEX IF NOT EXISTS idx_categories_parent ON categories(parent_id);

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

        -- Expenses index for order lookups
        CREATE INDEX IF NOT EXISTS idx_expenses_order ON expenses(order_id);

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
            updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
            last_stock_out_at DATE
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

        -- ═══════════════════════════════════════════════════════════════════════
        -- Stock Movements (delta detection from hourly sync)
        -- ═══════════════════════════════════════════════════════════════════════
        CREATE SEQUENCE IF NOT EXISTS seq_stock_movements_id START 1;

        CREATE TABLE IF NOT EXISTS stock_movements (
            id INTEGER PRIMARY KEY DEFAULT(nextval('seq_stock_movements_id')),
            offer_id INTEGER NOT NULL,
            product_id INTEGER,
            movement_type VARCHAR NOT NULL,
            quantity_before INTEGER NOT NULL,
            quantity_after INTEGER NOT NULL,
            delta INTEGER NOT NULL,
            reserve_before INTEGER NOT NULL,
            reserve_after INTEGER NOT NULL,
            recorded_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
            source VARCHAR DEFAULT 'sync'
        );

        CREATE INDEX IF NOT EXISTS idx_movements_offer
            ON stock_movements(offer_id, recorded_at DESC);
        CREATE INDEX IF NOT EXISTS idx_movements_product
            ON stock_movements(product_id, recorded_at DESC);
        CREATE INDEX IF NOT EXISTS idx_movements_date
            ON stock_movements(recorded_at);

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
            model_wape DECIMAL(6, 2),
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

        -- Buyers/Customers table (synced from KeyCRM)
        CREATE TABLE IF NOT EXISTS buyers (
            -- Core
            id INTEGER PRIMARY KEY,
            full_name VARCHAR NOT NULL,
            birthday DATE,
            note TEXT,

            -- Primary contact (indexed for quick lookup)
            phone VARCHAR,
            email VARCHAR,

            -- Relationships
            manager_id INTEGER,
            company_id INTEGER,
            company_name VARCHAR,

            -- Geographic
            city VARCHAR,
            region VARCHAR,

            -- Loyalty (denormalized)
            loyalty_program_name VARCHAR,
            loyalty_level_name VARCHAR,
            loyalty_discount DECIMAL(5,2) DEFAULT 0,
            loyalty_amount DECIMAL(12,2) DEFAULT 0,

            -- Timestamps
            created_at TIMESTAMP WITH TIME ZONE,
            updated_at TIMESTAMP WITH TIME ZONE,
            synced_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
        );

        CREATE INDEX IF NOT EXISTS idx_buyers_phone ON buyers(phone);
        CREATE INDEX IF NOT EXISTS idx_buyers_email ON buyers(email);
        CREATE INDEX IF NOT EXISTS idx_buyers_manager ON buyers(manager_id);
        CREATE INDEX IF NOT EXISTS idx_buyers_city ON buyers(city);

        -- Buyer contacts (normalized 1:N for all phones/emails)
        CREATE SEQUENCE IF NOT EXISTS seq_buyer_contacts_id START 1;
        CREATE TABLE IF NOT EXISTS buyer_contacts (
            id INTEGER PRIMARY KEY DEFAULT nextval('seq_buyer_contacts_id'),
            buyer_id INTEGER NOT NULL,
            contact_type VARCHAR NOT NULL,          -- 'phone' or 'email'
            value VARCHAR NOT NULL,
            is_primary BOOLEAN DEFAULT FALSE,
            UNIQUE(buyer_id, contact_type, value)
        );

        CREATE INDEX IF NOT EXISTS idx_buyer_contacts_buyer ON buyer_contacts(buyer_id);
        CREATE INDEX IF NOT EXISTS idx_buyer_contacts_value ON buyer_contacts(value);

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

        -- Silver orders indexes (defined in consolidated block below)

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
        -- GOLD LAYER: Pre-aggregated product pairs (association rules)
        -- ═══════════════════════════════════════════════════════════════════════
        CREATE TABLE IF NOT EXISTS gold_product_pairs (
            sales_type VARCHAR NOT NULL,
            product_a_id INTEGER,
            product_a_name VARCHAR NOT NULL,
            product_b_id INTEGER,
            product_b_name VARCHAR NOT NULL,
            co_occurrence INTEGER NOT NULL,
            product_a_orders INTEGER NOT NULL,
            product_b_orders INTEGER NOT NULL,
            total_orders INTEGER NOT NULL,
            support DOUBLE NOT NULL,
            confidence_a_to_b DOUBLE NOT NULL,
            confidence_b_to_a DOUBLE NOT NULL,
            lift DOUBLE NOT NULL
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

        -- Additional indexes (non-duplicate, supplementing per-table indexes above)
        CREATE INDEX IF NOT EXISTS idx_products_category_id ON products(category_id);
        CREATE INDEX IF NOT EXISTS idx_products_brand ON products(brand);
        CREATE INDEX IF NOT EXISTS idx_daily_stats_date ON daily_stats(date);
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
        CREATE INDEX IF NOT EXISTS idx_gold_pairs_co ON gold_product_pairs(sales_type, co_occurrence DESC);
        CREATE INDEX IF NOT EXISTS idx_gold_pairs_prod_a ON gold_product_pairs(sales_type, product_a_id);
        CREATE INDEX IF NOT EXISTS idx_gold_pairs_prod_b ON gold_product_pairs(sales_type, product_b_id);

        -- ═══════════════════════════════════════════════════════════════════════
        -- SILVER LAYER: UTM tracking data (parsed from manager_comment)
        -- ═══════════════════════════════════════════════════════════════════════
        CREATE TABLE IF NOT EXISTS silver_order_utm (
            order_id INTEGER PRIMARY KEY,

            -- Standard UTM parameters
            utm_source VARCHAR(100),      -- facebook, tiktok, google, instagram, klaviyo
            utm_medium VARCHAR(100),      -- paid, cpc, social, email, organic
            utm_campaign VARCHAR(255),
            utm_content VARCHAR(255),
            utm_term VARCHAR(255),
            utm_lang VARCHAR(10),

            -- Platform pixels (tracking cookies)
            fbp VARCHAR(100),             -- Facebook Browser ID (_fbp)
            fbc VARCHAR(100),             -- Facebook Click ID (_fbc) - indicates ad click
            ttp VARCHAR(100),             -- TikTok Pixel ID
            fbclid VARCHAR(100),          -- Facebook Click ID from URL

            -- Derived classification
            traffic_type VARCHAR(20),     -- paid_confirmed, paid_likely, organic, pixel_only, unknown
            platform VARCHAR(20),         -- facebook, tiktok, google, instagram, email, other

            parsed_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
        );

        CREATE INDEX IF NOT EXISTS idx_silver_utm_platform ON silver_order_utm(platform);
        CREATE INDEX IF NOT EXISTS idx_silver_utm_traffic_type ON silver_order_utm(traffic_type);

        -- ═══════════════════════════════════════════════════════════════════════
        -- GOLD LAYER: Daily traffic analytics
        -- ═══════════════════════════════════════════════════════════════════════
        CREATE TABLE IF NOT EXISTS gold_daily_traffic (
            date DATE NOT NULL,
            source_id INTEGER NOT NULL,
            sales_type VARCHAR NOT NULL,       -- retail, b2b, other
            platform VARCHAR(20) NOT NULL,     -- facebook, tiktok, google, instagram, email, other
            traffic_type VARCHAR(20) NOT NULL, -- paid_confirmed, paid_likely, organic, pixel_only, unknown

            orders_count INTEGER DEFAULT 0,
            revenue DECIMAL(12,2) DEFAULT 0,

            PRIMARY KEY (date, source_id, sales_type, platform, traffic_type)
        );

        CREATE INDEX IF NOT EXISTS idx_gold_traffic_date ON gold_daily_traffic(date);
        CREATE INDEX IF NOT EXISTS idx_gold_traffic_platform ON gold_daily_traffic(platform);
        CREATE INDEX IF NOT EXISTS idx_gold_traffic_sales_type ON gold_daily_traffic(sales_type);

        -- ═══════════════════════════════════════════════════════════════════════
        -- MANUAL EXPENSES (business expenses not in KeyCRM)
        -- ═══════════════════════════════════════════════════════════════════════
        CREATE SEQUENCE IF NOT EXISTS seq_manual_expenses_id START 1;
        CREATE TABLE IF NOT EXISTS manual_expenses (
            id INTEGER PRIMARY KEY DEFAULT nextval('seq_manual_expenses_id'),
            expense_date DATE NOT NULL,
            category VARCHAR NOT NULL,          -- marketing, salary, taxes, logistics, other
            expense_type VARCHAR NOT NULL,      -- Facebook Ads, Google Ads, Salary, etc.
            amount DECIMAL(12, 2) NOT NULL,
            currency VARCHAR DEFAULT 'UAH',
            note VARCHAR,
            created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP WITH TIME ZONE
        );

        CREATE INDEX IF NOT EXISTS idx_manual_expenses_date ON manual_expenses(expense_date);
        CREATE INDEX IF NOT EXISTS idx_manual_expenses_category ON manual_expenses(category);

        -- ═══════════════════════════════════════════════════════════════════════
        -- USER MANAGEMENT (migrated from SQLite bot.db)
        -- ═══════════════════════════════════════════════════════════════════════
        CREATE TABLE IF NOT EXISTS users (
            user_id BIGINT PRIMARY KEY,           -- Telegram user ID
            username VARCHAR,
            first_name VARCHAR,
            last_name VARCHAR,
            photo_url VARCHAR,
            role VARCHAR DEFAULT 'viewer',        -- admin, editor, viewer
            status VARCHAR DEFAULT 'pending',     -- pending, approved, denied, frozen
            requested_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
            reviewed_at TIMESTAMP WITH TIME ZONE,
            reviewed_by BIGINT,
            last_activity TIMESTAMP WITH TIME ZONE,
            denial_count INTEGER DEFAULT 0,
            created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
        );

        CREATE INDEX IF NOT EXISTS idx_users_status ON users(status);
        CREATE INDEX IF NOT EXISTS idx_users_role ON users(role);

        -- User preferences
        CREATE TABLE IF NOT EXISTS user_preferences (
            user_id BIGINT PRIMARY KEY,
            default_source VARCHAR,
            default_report_type VARCHAR DEFAULT 'summary',
            timezone VARCHAR DEFAULT 'Europe/Kyiv',
            default_date_range VARCHAR DEFAULT 'week',
            notifications_enabled BOOLEAN DEFAULT TRUE,
            created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP WITH TIME ZONE
        );

        -- Report history
        CREATE SEQUENCE IF NOT EXISTS seq_report_history_id START 1;
        CREATE TABLE IF NOT EXISTS report_history (
            id INTEGER PRIMARY KEY DEFAULT nextval('seq_report_history_id'),
            user_id BIGINT NOT NULL,
            report_type VARCHAR NOT NULL,
            start_date DATE NOT NULL,
            end_date DATE NOT NULL,
            source VARCHAR,
            created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
        );

        CREATE INDEX IF NOT EXISTS idx_report_history_user ON report_history(user_id, created_at DESC);

        -- Celebrated milestones
        CREATE TABLE IF NOT EXISTS celebrated_milestones (
            period_type VARCHAR NOT NULL,
            period_key VARCHAR NOT NULL,
            milestone_amount INTEGER NOT NULL,
            revenue DECIMAL(14, 2) NOT NULL,
            celebrated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (period_type, period_key, milestone_amount)
        );

        -- Role permissions (dynamic permissions matrix)
        CREATE TABLE IF NOT EXISTS role_permissions (
            role VARCHAR NOT NULL,              -- admin, editor, viewer
            feature VARCHAR NOT NULL,           -- dashboard, expenses, inventory, etc.
            can_view BOOLEAN DEFAULT FALSE,
            can_edit BOOLEAN DEFAULT FALSE,
            can_delete BOOLEAN DEFAULT FALSE,
            updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
            updated_by BIGINT,
            PRIMARY KEY (role, feature)
        );

        CREATE INDEX IF NOT EXISTS idx_role_permissions_role ON role_permissions(role);
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

        # Migration: Add manager_comment column to orders table (for UTM tracking)
        try:
            self._connection.execute(
                "ALTER TABLE orders ADD COLUMN IF NOT EXISTS manager_comment TEXT"
            )
            logger.debug("Migration: manager_comment column added/verified")
        except Exception as e:
            logger.debug(f"Migration note (manager_comment): {e}")

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

        # Migration: Add sales_type column to gold_daily_traffic
        try:
            self._connection.execute(
                "ALTER TABLE gold_daily_traffic ADD COLUMN IF NOT EXISTS sales_type VARCHAR NOT NULL DEFAULT 'retail'"
            )
            logger.debug("Migration: sales_type column added/verified on gold_daily_traffic")
        except Exception as e:
            logger.debug(f"Migration note (gold_daily_traffic sales_type): {e}")

        # Migration: Add platform column to manual_expenses (for ad spend tracking)
        try:
            self._connection.execute(
                "ALTER TABLE manual_expenses ADD COLUMN IF NOT EXISTS platform VARCHAR"
            )
            self._connection.execute(
                "CREATE INDEX IF NOT EXISTS idx_manual_expenses_platform ON manual_expenses(platform)"
            )
            # Backfill existing marketing rows
            self._connection.execute("""
                UPDATE manual_expenses SET platform = CASE
                    WHEN LOWER(expense_type) LIKE '%facebook%' OR LOWER(expense_type) LIKE '%fb %' THEN 'facebook'
                    WHEN LOWER(expense_type) LIKE '%tiktok%' THEN 'tiktok'
                    WHEN LOWER(expense_type) LIKE '%google%' THEN 'google'
                END WHERE category = 'marketing' AND platform IS NULL
            """)
            logger.debug("Migration: platform column added/verified on manual_expenses")
        except Exception as e:
            logger.debug(f"Migration note (manual_expenses platform): {e}")

        # Migration: order_products.id INTEGER → BIGINT (overflow safety)
        try:
            col_type = self._connection.execute("""
                SELECT data_type FROM information_schema.columns
                WHERE table_name = 'order_products' AND column_name = 'id'
            """).fetchone()
            if col_type and col_type[0] == 'INTEGER':
                logger.info("Migration: order_products.id INTEGER → BIGINT")
                self._connection.execute("BEGIN TRANSACTION")
                try:
                    self._connection.execute("CREATE TABLE order_products_new AS SELECT * FROM order_products")
                    self._connection.execute("DROP TABLE order_products")
                    self._connection.execute("""
                        CREATE TABLE order_products (
                            id BIGINT PRIMARY KEY,
                            order_id INTEGER NOT NULL,
                            product_id INTEGER,
                            name VARCHAR NOT NULL,
                            quantity INTEGER NOT NULL,
                            price_sold DECIMAL(12, 2) NOT NULL
                        )
                    """)
                    self._connection.execute("INSERT INTO order_products SELECT * FROM order_products_new")
                    self._connection.execute("DROP TABLE order_products_new")
                    self._connection.execute("CREATE INDEX IF NOT EXISTS idx_order_products_order ON order_products(order_id)")
                    self._connection.execute("CREATE INDEX IF NOT EXISTS idx_order_products_product ON order_products(product_id)")
                    self._connection.execute("COMMIT")
                    logger.info("Migration: order_products.id BIGINT migration complete")
                except Exception as e:
                    self._connection.execute("ROLLBACK")
                    logger.error(f"Migration failed (order_products BIGINT), rolling back: {e}")
                    raise
        except Exception as e:
            logger.debug(f"Migration note (order_products BIGINT): {e}")

        # Migration: Add last_stock_out_at column to sku_inventory_status
        try:
            self._connection.execute(
                "ALTER TABLE sku_inventory_status ADD COLUMN IF NOT EXISTS last_stock_out_at DATE"
            )
            logger.debug("Migration: last_stock_out_at column added/verified on sku_inventory_status")
        except Exception as e:
            logger.debug(f"Migration note (sku_inventory_status last_stock_out_at): {e}")

        # Migration: Add model_wape column to revenue_predictions
        try:
            self._connection.execute(
                "ALTER TABLE revenue_predictions ADD COLUMN IF NOT EXISTS model_wape DECIMAL(6, 2)"
            )
            logger.debug("Migration: model_wape column added/verified on revenue_predictions")
        except Exception as e:
            logger.debug(f"Migration note (revenue_predictions model_wape): {e}")

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

    async def refresh_warehouse_layers(
        self,
        trigger: str = "manual",
        changed_order_ids: list[int] | None = None,
    ) -> Dict[str, Any]:
        """Rebuild Silver and Gold warehouse layers from Bronze tables.

        Uses transactional DELETE+INSERT (preserves indexes and PKs).
        Split locking: releases the asyncio lock between Silver and each Gold
        rebuild so queued read queries can execute between steps.

        When changed_order_ids is provided, Gold layers are rebuilt only for
        affected dates (incremental). Otherwise full rebuild (startup, full_sync).

        Args:
            trigger: What triggered the refresh
            changed_order_ids: Order IDs that changed (for incremental Gold rebuild)

        Returns:
            Dict with refresh stats and validation results
        """
        import time
        start_time = time.perf_counter()
        error_msg = None

        # Pre-compute SQL fragments used across steps
        manager_list = ",".join(str(m) for m in RETAIL_MANAGER_IDS)
        retail_filter = f"""
            WHEN o.manager_id IS NULL THEN 'retail'
            WHEN o.manager_id = {B2B_MANAGER_ID} THEN 'b2b'
            WHEN o.manager_id IN (SELECT id FROM managers WHERE is_retail = TRUE) THEN 'retail'
            WHEN NOT EXISTS (SELECT 1 FROM managers WHERE is_retail = TRUE)
                 AND o.manager_id IN ({manager_list}) THEN 'retail'
            ELSE 'other'
        """
        return_statuses = tuple(int(s) for s in OrderStatus.return_statuses())

        try:
            # ── Step 1: Silver rebuild (always full — is_new_customer depends on global min) ──
            async with self.connection() as conn:
                conn.execute("BEGIN TRANSACTION")
                try:
                    conn.execute("DELETE FROM silver_orders")
                    conn.execute(f"""
                        INSERT INTO silver_orders
                        SELECT
                            o.id,
                            o.source_id,
                            o.status_id,
                            o.grand_total,
                            o.ordered_at,
                            o.buyer_id,
                            o.manager_id,
                            {_date_in_kyiv('o.ordered_at')} AS order_date,
                            o.status_id IN {return_statuses} AS is_return,
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
                              AND status_id NOT IN {return_statuses}
                            GROUP BY buyer_id
                        ) fo ON o.buyer_id = fo.buyer_id
                    """)
                    conn.execute("COMMIT")
                except Exception:
                    conn.execute("ROLLBACK")
                    raise

            # ── Determine affected dates for incremental Gold rebuild ──
            affected_dates: set[date] | None = None
            if changed_order_ids:
                async with self.connection() as conn:
                    # Get dates for changed orders + all dates for affected buyers
                    # (is_new_customer can cascade to other dates for the same buyer)
                    placeholders = ",".join("?" * len(changed_order_ids))
                    id_params = list(changed_order_ids)
                    rows = conn.execute(f"""
                        SELECT DISTINCT order_date FROM silver_orders
                        WHERE id IN ({placeholders})
                           OR buyer_id IN (
                               SELECT DISTINCT buyer_id FROM silver_orders
                               WHERE id IN ({placeholders}) AND buyer_id IS NOT NULL
                           )
                    """, id_params + id_params).fetchall()
                    affected_dates = {r[0] for r in rows if r[0] is not None}

                if not affected_dates:
                    affected_dates = None  # Fall back to full rebuild

            # ── Step 2: Gold daily revenue (lock acquired + released) ──
            gold_revenue_rows = 0
            async with self.connection() as conn:
                conn.execute("BEGIN TRANSACTION")
                try:
                    if affected_dates:
                        date_params = list(affected_dates)
                        date_placeholders = ",".join("?" * len(date_params))
                        conn.execute(f"DELETE FROM gold_daily_revenue WHERE date IN ({date_placeholders})", date_params)
                        conn.execute(f"""
                            INSERT INTO gold_daily_revenue
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
                            WHERE order_date IN ({date_placeholders})
                            GROUP BY order_date, sales_type
                        """, date_params)
                    else:
                        conn.execute("DELETE FROM gold_daily_revenue")
                        conn.execute("""
                            INSERT INTO gold_daily_revenue
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
                        """)
                    gold_revenue_rows = conn.execute("SELECT COUNT(*) FROM gold_daily_revenue").fetchone()[0]
                    conn.execute("COMMIT")
                except Exception:
                    conn.execute("ROLLBACK")
                    raise

            # ── Step 3: Gold daily products (lock acquired + released) ──
            gold_products_rows = 0
            async with self.connection() as conn:
                conn.execute("BEGIN TRANSACTION")
                try:
                    if affected_dates:
                        date_params = list(affected_dates)
                        date_placeholders = ",".join("?" * len(date_params))
                        conn.execute(f"DELETE FROM gold_daily_products WHERE date IN ({date_placeholders})", date_params)
                        conn.execute(f"""
                            INSERT INTO gold_daily_products
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
                              AND s.order_date IN ({date_placeholders})
                            GROUP BY
                                s.order_date, s.sales_type, s.source_id,
                                op.product_id, op.name, p.brand, p.category_id,
                                c.name, parent_c.name
                        """, date_params)
                    else:
                        conn.execute("DELETE FROM gold_daily_products")
                        conn.execute("""
                            INSERT INTO gold_daily_products
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
                    gold_products_rows = conn.execute("SELECT COUNT(*) FROM gold_daily_products").fetchone()[0]
                    conn.execute("COMMIT")
                except Exception:
                    conn.execute("ROLLBACK")
                    raise

            # ── Step 3.5: Gold product pairs (always full rebuild) ──
            # Uses staged temp tables to keep peak memory low (avoids OOM
            # in 1GB containers with 35K+ orders). Each step runs separately
            # so DuckDB can release intermediate memory between statements.
            gold_pairs_rows = 0
            async with self.connection() as conn:
                try:
                    # Stage 1: Minimal temp table — just IDs for the self-join
                    conn.execute("DROP TABLE IF EXISTS _tmp_order_items")
                    conn.execute("""
                        CREATE TEMP TABLE _tmp_order_items AS
                        SELECT s.id AS order_id, s.sales_type,
                               COALESCE(op.product_id, op.id) AS product_id
                        FROM silver_orders s
                        JOIN order_products op ON s.id = op.order_id
                        WHERE NOT s.is_return AND s.is_active_source
                          AND s.sales_type IN ('retail', 'b2b')
                    """)

                    # Stage 2: Filter to multi-item orders only
                    conn.execute("DROP TABLE IF EXISTS _tmp_multi")
                    conn.execute("""
                        CREATE TEMP TABLE _tmp_multi AS
                        SELECT oi.*
                        FROM _tmp_order_items oi
                        WHERE oi.order_id IN (
                            SELECT order_id FROM _tmp_order_items
                            GROUP BY order_id
                            HAVING COUNT(DISTINCT product_id) >= 2
                        )
                    """)

                    # Stage 3: Self-join for co-occurrence pairs (memory-intensive step)
                    conn.execute("DROP TABLE IF EXISTS _tmp_pairs")
                    conn.execute("""
                        CREATE TEMP TABLE _tmp_pairs AS
                        SELECT a.sales_type,
                               a.product_id AS pa_id,
                               b.product_id AS pb_id,
                               COUNT(DISTINCT a.order_id) AS co_occurrence
                        FROM _tmp_multi a
                        JOIN _tmp_multi b ON a.order_id = b.order_id
                            AND a.sales_type = b.sales_type AND a.product_id < b.product_id
                        GROUP BY a.sales_type, a.product_id, b.product_id
                        HAVING COUNT(DISTINCT a.order_id) >= 3
                    """)

                    # Stage 4: Final insert with metrics + product names
                    conn.execute("BEGIN TRANSACTION")
                    conn.execute("DELETE FROM gold_product_pairs")
                    conn.execute("""
                        INSERT INTO gold_product_pairs
                        SELECT p.sales_type,
                            p.pa_id,
                            COALESCE(na.name, opa.name, 'Unknown'),
                            p.pb_id,
                            COALESCE(nb.name, opb.name, 'Unknown'),
                            p.co_occurrence, ca.orders, cb.orders, t.total,
                            CAST(p.co_occurrence AS DOUBLE) / t.total,
                            CAST(p.co_occurrence AS DOUBLE) / ca.orders,
                            CAST(p.co_occurrence AS DOUBLE) / cb.orders,
                            (CAST(p.co_occurrence AS DOUBLE) * t.total) / (CAST(ca.orders AS DOUBLE) * cb.orders)
                        FROM _tmp_pairs p
                        JOIN (
                            SELECT sales_type, product_id, COUNT(DISTINCT order_id) AS orders
                            FROM _tmp_order_items GROUP BY sales_type, product_id
                        ) ca ON p.sales_type = ca.sales_type AND p.pa_id = ca.product_id
                        JOIN (
                            SELECT sales_type, product_id, COUNT(DISTINCT order_id) AS orders
                            FROM _tmp_order_items GROUP BY sales_type, product_id
                        ) cb ON p.sales_type = cb.sales_type AND p.pb_id = cb.product_id
                        JOIN (
                            SELECT sales_type, COUNT(DISTINCT order_id) AS total
                            FROM _tmp_multi GROUP BY sales_type
                        ) t ON p.sales_type = t.sales_type
                        LEFT JOIN products na ON p.pa_id = na.id
                        LEFT JOIN products nb ON p.pb_id = nb.id
                        LEFT JOIN (
                            SELECT COALESCE(product_id, id) AS pid, ANY_VALUE(name) AS name
                            FROM order_products GROUP BY COALESCE(product_id, id)
                        ) opa ON p.pa_id = opa.pid
                        LEFT JOIN (
                            SELECT COALESCE(product_id, id) AS pid, ANY_VALUE(name) AS name
                            FROM order_products GROUP BY COALESCE(product_id, id)
                        ) opb ON p.pb_id = opb.pid
                    """)
                    gold_pairs_rows = conn.execute("SELECT COUNT(*) FROM gold_product_pairs").fetchone()[0]
                    conn.execute("COMMIT")
                except Exception:
                    try:
                        conn.execute("ROLLBACK")
                    except Exception:
                        pass
                    raise
                finally:
                    # Always clean up temp tables
                    for t in ("_tmp_pairs", "_tmp_multi", "_tmp_order_items"):
                        try:
                            conn.execute(f"DROP TABLE IF EXISTS {t}")
                        except Exception:
                            pass

            # ── Step 4: Validation + audit log ──
            async with self.connection() as conn:
                checksums = conn.execute("""
                    SELECT
                        (SELECT COUNT(*) FROM orders) AS bronze_orders,
                        (SELECT COUNT(*) FROM silver_orders) AS silver_rows,
                        (SELECT COALESCE(SUM(grand_total), 0) FROM silver_orders
                         WHERE NOT is_return AND is_active_source) AS silver_revenue,
                        (SELECT COALESCE(SUM(revenue), 0) FROM gold_daily_revenue) AS gold_revenue,
                        (SELECT COALESCE(SUM(product_revenue), 0) FROM gold_daily_products) AS gold_product_revenue,
                        (SELECT COALESCE(SUM(op.price_sold * op.quantity), 0)
                         FROM order_products op
                         JOIN silver_orders s ON op.order_id = s.id
                         WHERE NOT s.is_return AND s.is_active_source) AS bronze_product_revenue
                """).fetchone()

                bronze_orders = checksums[0]
                silver_rows = checksums[1]
                silver_revenue = float(checksums[2])
                gold_revenue = float(checksums[3])
                gold_product_revenue = float(checksums[4])
                bronze_product_revenue = float(checksums[5])

                checksum_match = abs(silver_revenue - gold_revenue) < 0.01
                product_checksum_match = abs(gold_product_revenue - bronze_product_revenue) < 0.01
                row_count_match = bronze_orders == silver_rows
                validation_passed = checksum_match and row_count_match and product_checksum_match

                if not validation_passed:
                    logger.warning(
                        f"Warehouse validation failed: "
                        f"rows={bronze_orders}→{silver_rows} (match={row_count_match}), "
                        f"revenue={silver_revenue:.2f}→{gold_revenue:.2f} (match={checksum_match}), "
                        f"product_revenue={bronze_product_revenue:.2f}→{gold_product_revenue:.2f} (match={product_checksum_match})"
                    )

                duration_ms = (time.perf_counter() - start_time) * 1000

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

            incremental_info = ""
            if affected_dates:
                incremental_info = f", incremental={len(affected_dates)} dates"

            logger.info(
                f"Warehouse layers refreshed ({trigger}): "
                f"silver={silver_rows}, gold_rev={gold_revenue_rows}, "
                f"gold_prod={gold_products_rows}, gold_pairs={gold_pairs_rows}, "
                f"duration={duration_ms:.0f}ms, valid={validation_passed}"
                f"{incremental_info}"
            )

            # ── UTM/Traffic layers (after main refresh completes) ──
            utm_count = 0
            traffic_rows = 0
            try:
                utm_count = await self.refresh_utm_silver_layer()
                traffic_rows = await self.refresh_traffic_gold_layer(
                    affected_dates=affected_dates,
                )
            except Exception as utm_error:
                logger.warning(f"UTM layer refresh failed (non-critical): {utm_error}")

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
                "utm_orders_parsed": utm_count,
                "traffic_rows": traffic_rows,
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
                "manager_comment": order.manager_comment,
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

        # Ensure manager_comment is proper nullable string type for DuckDB
        orders_df["manager_comment"] = orders_df["manager_comment"].astype(pd.StringDtype())

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
                    INSERT INTO orders (id, source_id, status_id, grand_total, ordered_at, created_at, updated_at, buyer_id, manager_id, manager_comment, synced_at)
                    SELECT id, source_id, status_id, grand_total, ordered_at, created_at, updated_at, buyer_id, manager_id, manager_comment, now()
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

    async def upsert_buyers(self, buyers: List["Buyer"]) -> int:
        """Insert or update buyers from KeyCRM API.

        Args:
            buyers: List of Buyer objects

        Returns:
            Number of buyers upserted
        """
        from core.models import Buyer
        if not buyers:
            return 0

        async with self.connection() as conn:
            conn.execute("BEGIN TRANSACTION")
            try:
                count = 0
                contacts_count = 0

                for buyer in buyers:
                    # Upsert buyer record
                    conn.execute("""
                        INSERT OR REPLACE INTO buyers
                        (id, full_name, birthday, note, phone, email,
                         manager_id, company_id, company_name, city, region,
                         loyalty_program_name, loyalty_level_name, loyalty_discount, loyalty_amount,
                         created_at, updated_at, synced_at)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                    """, [
                        buyer.id,
                        buyer.full_name,
                        buyer.birthday,
                        buyer.note,
                        buyer.phone,
                        buyer.email,
                        buyer.manager_id,
                        buyer.company_id,
                        buyer.company_name,
                        buyer.city,
                        buyer.region,
                        buyer.loyalty_program_name,
                        buyer.loyalty_level_name,
                        buyer.loyalty_discount,
                        buyer.loyalty_amount,
                        buyer.created_at,
                        buyer.updated_at,
                    ])
                    count += 1

                    # Upsert contacts (phones and emails)
                    # First, remove existing contacts for this buyer
                    conn.execute("DELETE FROM buyer_contacts WHERE buyer_id = ?", [buyer.id])

                    # Insert all phones
                    if buyer.phones:
                        for i, phone in enumerate(buyer.phones):
                            if phone:  # Skip empty values
                                conn.execute("""
                                    INSERT INTO buyer_contacts (buyer_id, contact_type, value, is_primary)
                                    VALUES (?, 'phone', ?, ?)
                                    ON CONFLICT (buyer_id, contact_type, value) DO NOTHING
                                """, [buyer.id, phone, i == 0])
                                contacts_count += 1

                    # Insert all emails
                    if buyer.emails:
                        for i, email in enumerate(buyer.emails):
                            if email:  # Skip empty values
                                conn.execute("""
                                    INSERT INTO buyer_contacts (buyer_id, contact_type, value, is_primary)
                                    VALUES (?, 'email', ?, ?)
                                    ON CONFLICT (buyer_id, contact_type, value) DO NOTHING
                                """, [buyer.id, email, i == 0])
                                contacts_count += 1

                conn.execute("COMMIT")
                logger.info(f"Upserted {count} buyers, {contacts_count} contacts to DuckDB")
                return count

            except Exception:
                conn.execute("ROLLBACK")
                raise

    async def get_missing_buyer_ids(self, limit: int = 100) -> List[int]:
        """Get buyer IDs from orders that need syncing.

        Includes buyers that:
        - Are not in the buyers table at all
        - Are in the buyers table but have NULL full_name (incomplete sync)

        Prioritizes return orders to ensure return buyers are synced first.

        Args:
            limit: Maximum number of IDs to return

        Returns:
            List of buyer IDs that need to be synced
        """
        async with self.connection() as conn:
            # Use silver_orders which has is_return flag, prioritize returns
            # Also include buyers with NULL full_name (need re-sync)
            # Use subquery to properly handle DISTINCT with ORDER BY
            result = conn.execute("""
                SELECT buyer_id FROM (
                    SELECT s.buyer_id,
                           MAX(CASE WHEN s.is_return THEN 1 ELSE 0 END) as has_return,
                           MAX(s.order_date) as latest_order
                    FROM silver_orders s
                    LEFT JOIN buyers b ON s.buyer_id = b.id
                    WHERE s.buyer_id IS NOT NULL
                      AND (b.id IS NULL OR b.full_name IS NULL OR b.full_name = '')
                    GROUP BY s.buyer_id
                ) sub
                ORDER BY has_return DESC, latest_order DESC
                LIMIT ?
            """, [limit]).fetchall()
            return [row[0] for row in result]

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
            buyers_count = conn.execute("SELECT COUNT(*) FROM buyers").fetchone()[0]
            buyer_contacts_count = conn.execute("SELECT COUNT(*) FROM buyer_contacts").fetchone()[0]

            min_date = conn.execute("SELECT MIN(DATE(ordered_at)) FROM orders").fetchone()[0]
            max_date = conn.execute("SELECT MAX(DATE(ordered_at)) FROM orders").fetchone()[0]

            return {
                "orders": orders_count,
                "products": products_count,
                "categories": categories_count,
                "buyers": buyers_count,
                "buyer_contacts": buyer_contacts_count,
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
_store_lock = asyncio.Lock()


async def get_store() -> DuckDBStore:
    """Get singleton DuckDB store instance (coroutine-safe)."""
    global _store_instance
    async with _store_lock:
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

