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
import json
import logging
import os
import re
import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime, date
from pathlib import Path
from typing import Optional, List, Dict, Any, Tuple
from zoneinfo import ZoneInfo

import duckdb
import pandas as pd

from core.models import LOST_STATUS_GROUP_ID, Order, OrderStatus
from core.exceptions import QueryTimeoutError
from core.duckdb_constants import (
    DB_DIR, DB_PATH, DEFAULT_TZ, DEFAULT_QUERY_TIMEOUT, LONG_QUERY_TIMEOUT,
    B2B_MANAGER_ID, RETAIL_MANAGER_IDS, KNOWN_SALES_TYPES, DISPLAY_TIMEZONE, _date_in_kyiv,
    line_window_where,
    EXHIBITION_SOURCE_ID, REVENUE_SOURCE_IDS,
)
from core.repositories import (
    UsersMixin, TrafficMixin, CustomersMixin, GoalsMixin,
    InventoryMixin, ExpensesMixin, RevenueMixin, ProductsIntelMixin,
    MarginMixin,
)

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class UpsertResult:
    """What an order upsert actually did.

    `count` used to be the whole return, and it counts a row that was already
    in the desired state as a success — correct for "did this work", useless
    for "what changed". The sync loop consumed it as the latter and marked the
    entire fetched window dirty on every cycle, which is how a warehouse with
    no new data rebuilt 583 dates every two minutes. `changed_ids` is the set
    of rows actually written, and it is what callers should drive work from.
    """

    count: int                  # rows in the desired state (written or already correct)
    changed_ids: List[int]      # rows this call actually wrote
    skipped_unchanged: int      # rows already in the desired state
    failed: int                 # rows rejected by a constraint/transaction error

    def __len__(self) -> int:
        return self.count


# Warehouse self-heal bound: how many consecutive failed/errored refreshes we
# keep auto-retrying (mark dirty → next scheduler tick) before we STOP the loop
# and escalate loudly. Prevents both the old silent-idle dead-end (wrong data
# served until the weekly full_sync) and an unbounded retry spin on a
# deterministic data bug.
MAX_VALIDATION_RETRIES = 3

# Once the retry budget is spent we stop rebuilding on every scheduler tick, but
# "stop" used to mean forever — the Gold layer stayed wrong until a human acted
# or the weekly full_sync came round. On 2026-08-02 that was five days. A full
# rebuild every six hours is cheap enough to be worth the chance that the damage
# was transient, and rare enough not to spin on a deterministic data bug.
STUCK_REBUILD_COOLDOWN_SECONDS = 6 * 60 * 60

# DuckDB's own memory ceiling, independent of the container's mem_limit. Keep it
# below what the container allows: Python, pandas frames and the Meili sync all
# draw from the same budget, and a container OOM-kill is worse than a DuckDB
# spill to temp_directory.
DEFAULT_DUCKDB_MEMORY_LIMIT = "4GB"

# Above this many freshly-parsed UTM orders, resolving their dates costs more
# than the full traffic rebuild it would save, so we just rebuild everything.
# A parse this large is a backfill or a first run, not a steady-state tick.
UTM_DATE_LOOKUP_LIMIT = 5000


def _memory_limit() -> str:
    """Resolve the DuckDB memory limit from DUCKDB_MEMORY_LIMIT.

    Only DuckDB's own size syntax is accepted ("4GB", "512MB", …). Anything else
    falls back to the default rather than reaching the SET statement, so a typo
    in the environment cannot stop the store from connecting at all.
    """
    raw = (os.getenv("DUCKDB_MEMORY_LIMIT") or "").strip()
    if not raw:
        return DEFAULT_DUCKDB_MEMORY_LIMIT
    if re.fullmatch(r"\d+(\.\d+)?\s*(K|M|G|T)?i?B", raw, re.IGNORECASE):
        return raw
    logger.warning(
        "Ignoring malformed DUCKDB_MEMORY_LIMIT=%r; using %s",
        raw, DEFAULT_DUCKDB_MEMORY_LIMIT,
    )
    return DEFAULT_DUCKDB_MEMORY_LIMIT


# The one definition of what a Gold revenue cell contains. Both the rebuild and
# the per-cell audit in core/data_quality.py read it from here: an audit with
# its own copy of the projection checks that two hand-written queries agree,
# which is not the same question.
#
# `{date_filter}` is substituted by the caller — a date list for an incremental
# rebuild, `order_date IS NOT NULL` for everything.
SILVER_ORDERS_DDL = """CREATE TABLE IF NOT EXISTS silver_orders (
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
            buyer_first_order_date DATE,
            promocode VARCHAR
)"""



# ─── The one definition of an order line ─────────────────────────────────────
#
# Silver has one grain — the order — so anything asked about a *product* had to
# re-join `order_products` at the point of use. That join is written 49 times
# across the codebase, and 22 of those also carry `silver_orders`, which is the
# single largest obstacle to splitting this store in two: a line and its order
# would have to live in different databases.
#
# A view rather than a table, deliberately. Measured on the production backup
# (146,910 lines): the view is indistinguishable from writing the join out by
# hand — 21.0 ms vs 21.6 ms, 65.2 ms vs 66.5 ms on the heaviest shape — because
# DuckDB inlines it. Materialising it is 2–4× faster in relative terms and 48 ms
# in absolute ones, and it would cost 271 ms of lock time on every full refresh,
# 45 times a day, plus a fourth large table for the weekly compaction to carry.
# Nothing measured yet asks for that. Promotion to a table is a one-line change
# here and touches no call site, which is the point of naming the level now.
#
# Equivalence is pinned by a test, not by hope: under Gold's own predicate this
# reproduces `gold_daily_products` to the kopeck — ₴132,077,453.75 on both
# sides, 986 dates on both sides, zero disagreeing. The ₴11.2M it holds *above*
# Gold is returns (₴5.67M) and inactive sources (₴6.26M), which Gold excludes
# on purpose and this level leaves to the caller.
SILVER_ORDER_LINES_VIEW_SQL = """CREATE OR REPLACE VIEW silver_order_lines AS
        SELECT
            op.id                                        AS line_id,
            op.order_id,
            op.product_id,
            op.name                                      AS product_name,  -- as sold
            op.quantity,
            op.price_sold,
            CAST(op.quantity * op.price_sold AS DECIMAL(14, 2)) AS line_amount,
            -- the order, denormalised: every predicate a page applies to
            -- revenue applies here too, and re-deriving them was the bug
            s.order_date,
            s.ordered_at,
            s.sales_type,
            s.source_id,
            s.source_name,
            s.is_return,
            s.is_active_source,
            s.buyer_id,
            s.manager_id,
            s.is_new_customer,
            s.buyer_first_order_date,
            s.promocode,
            s.grand_total                                AS order_grand_total,
            -- the catalog, denormalised. 8.2 % of lines carry no product_id and
            -- 31 % no category; both stay NULL rather than being dropped, which
            -- is the difference between a level and a filter.
            p.name                                       AS catalog_product_name,
            p.brand,
            p.sku,
            p.category_id,
            c.name                                       AS category_name,
            c.parent_id                                  AS parent_category_id,
            parent_c.name                                AS parent_category_name
        FROM order_products op
        JOIN silver_orders s ON s.id = op.order_id
        LEFT JOIN products p ON p.id = op.product_id
        LEFT JOIN categories c ON c.id = p.category_id
        LEFT JOIN categories parent_c ON parent_c.id = c.parent_id
"""


# ─── The one definition of a Silver row ──────────────────────────────────────
#
# Two copies of this existed: here and in `web/routes/api/admin.py`'s
# `rebuild-silver` endpoint. On 2026-08-20 the exhibition source was added to
# one of them and not the other, by someone who knew about the duplicate and
# was checking for exactly this — so `rebuild-silver` would have silently
# reverted the fix and dropped 178 orders back out of revenue. The divergence
# took under a day.
#
# `GOLD_REVENUE_SELECT_SQL` below is the pattern done right: one home, imported
# by whoever needs it. These do the same for Silver. A test fails if a second
# copy appears.


def silver_sales_type_case() -> str:
    """`sales_type`, the one place it is decided.

    The exhibition branch is first on purpose: that fair was staffed by a
    retail manager, so a manager-based rule would quietly pull a one-off
    channel into the retail trend it would distort.

    **Retail is resolved as of the order's own date**, from
    `manager_classifications`, not from the manager's classification today.
    Reclassifying somebody used to rewrite their whole history on the next
    rebuild — the owner ruled that out on 2026-08-20. The two fallbacks below
    keep a manager nobody has ever classified behaving exactly as before: the
    first for a manager with no interval, the second for a database whose
    `managers` table has not synced yet.
    """
    manager_list = ",".join(str(m) for m in RETAIL_MANAGER_IDS)
    order_date = _date_in_kyiv("o.ordered_at")
    return f"""CASE
            WHEN o.source_id = {EXHIBITION_SOURCE_ID} THEN 'exhibition'
            WHEN o.manager_id IS NULL THEN 'retail'
            WHEN o.manager_id = {B2B_MANAGER_ID} THEN 'b2b'
            WHEN EXISTS (
                     SELECT 1 FROM manager_classifications mc
                     WHERE mc.manager_id = o.manager_id
                       AND mc.is_retail
                       AND {order_date} >= mc.valid_from
                       AND (mc.valid_to IS NULL OR {order_date} < mc.valid_to)
                 ) THEN 'retail'
            WHEN NOT EXISTS (SELECT 1 FROM manager_classifications mc
                             WHERE mc.manager_id = o.manager_id)
                 AND o.manager_id IN (SELECT id FROM managers WHERE is_retail = TRUE) THEN 'retail'
            WHEN NOT EXISTS (SELECT 1 FROM manager_classifications)
                 AND NOT EXISTS (SELECT 1 FROM managers WHERE is_retail = TRUE)
                 AND o.manager_id IN ({manager_list}) THEN 'retail'
            ELSE 'internal'
        END"""


def silver_select_sql() -> str:
    """Every column of a Silver row, selected `FROM orders o`."""
    # KeyCRM's own grouping decides when we have it; the id list covers rows
    # synced before the column existed. Verified equal for every status the
    # warehouse holds — see core/models.py.
    return_statuses = tuple(int(s) for s in OrderStatus.return_statuses())
    revenue_sources = ", ".join(str(s) for s in REVENUE_SOURCE_IDS)
    return f"""
            o.id, o.source_id, o.status_id, o.grand_total,
            o.ordered_at, o.buyer_id, o.manager_id,
            {_date_in_kyiv('o.ordered_at')} AS order_date,
            CASE
                WHEN o.status_group_id IS NOT NULL
                    THEN o.status_group_id = {LOST_STATUS_GROUP_ID}
                ELSE o.status_id IN {return_statuses}
            END AS is_return,
            {silver_sales_type_case()} AS sales_type,
            o.source_id IN ({revenue_sources}) AS is_active_source,
            CASE o.source_id
                WHEN 1 THEN 'Instagram'
                WHEN 2 THEN 'Telegram'
                WHEN 4 THEN 'Shopify'
                WHEN {EXHIBITION_SOURCE_ID} THEN 'Виставка'
                ELSE 'Other'
            END AS source_name,
            FALSE AS is_new_customer,
            NULL  AS buyer_first_order_date,
            o.promocode
    """


def silver_pass2_sql(buyer_filter: str = "") -> str:
    """Recompute `is_new_customer` from each buyer's MIN(order_date).

    Empty `buyer_filter` runs on every Silver row (full mode); otherwise it is
    scoped to the affected buyers, keeping the write set bounded.
    """
    inner_filter = "buyer_id IS NOT NULL" if not buyer_filter else f"buyer_id IN ({buyer_filter})"
    outer_filter = "" if not buyer_filter else f"AND silver_orders.buyer_id IN ({buyer_filter})"
    return f"""
                UPDATE silver_orders SET
                    buyer_first_order_date = fo.first_order_date,
                    is_new_customer = CASE
                        WHEN silver_orders.buyer_id IS NOT NULL
                             AND NOT silver_orders.is_return
                             AND silver_orders.is_active_source
                             AND silver_orders.order_date = fo.first_order_date
                        THEN TRUE ELSE FALSE
                    END
                FROM (
                    -- The baseline deliberately does NOT filter is_active_source.
                    -- A purchase on a retired channel is still a purchase: with
                    -- Opencart excluded here, 419 buyers whose first order was
                    -- placed there counted as brand new the next time they bought
                    -- on Instagram — 422 orders and ₴1,081,979.59 of repeat
                    -- business booked as acquisition, overstating new customers
                    -- by 3.9% across 2025.
                    -- Returns stay excluded: a cancelled first order is not a
                    -- purchase, so the next one genuinely is their first.
                    SELECT buyer_id, MIN(order_date) AS first_order_date
                    FROM silver_orders
                    WHERE {inner_filter}
                      AND NOT is_return
                    GROUP BY buyer_id
                ) fo
                WHERE silver_orders.buyer_id = fo.buyer_id
                  {outer_filter}
    """


GOLD_REVENUE_SELECT_SQL = """
SELECT
    order_date AS date,
    sales_type,
    COALESCE(SUM(CASE WHEN NOT is_return AND is_active_source THEN grand_total END), 0) AS revenue,
    COUNT(DISTINCT CASE WHEN NOT is_return AND is_active_source THEN id END) AS orders_count,
    COUNT(DISTINCT CASE WHEN NOT is_return AND is_active_source AND buyer_id IS NOT NULL THEN buyer_id END) AS unique_customers,
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
WHERE {date_filter}
GROUP BY order_date, sales_type
"""


class DuckDBStore(
    UsersMixin, TrafficMixin, CustomersMixin, GoalsMixin,
    InventoryMixin, ExpensesMixin, RevenueMixin, ProductsIntelMixin,
    MarginMixin,
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

    # Monotonic timestamp of the last full rebuild attempted after the per-tick
    # retry budget ran out. Class-level default so instances built without
    # __init__ still read cleanly.
    _last_stuck_rebuild: "float | None" = None

    def __init__(self, db_path: Optional[Path] = None):
        # Resolved here rather than bound as a default argument. A default is
        # evaluated once, when this function is defined, so `db_path=DB_PATH`
        # made the production path permanent and unpatchable: nothing a test
        # did to core.duckdb_store.DB_PATH afterwards could change where a
        # store built with no argument would open. Reading the module global at
        # call time is what lets tests/conftest.py redirect it — see the
        # `_never_the_production_database` fixture there.
        self.db_path = db_path if db_path is not None else DB_PATH
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
                # Prevent OOM in memory-limited containers (DuckDB defaults to 80% of system RAM).
                # 3GB verified safe for checkpoint on 19GB DB via compact_duckdb.py spike runs
                # (also exercises full export). 2GB OOMs WAL flush — keep 3GB as floor.
                #
                # 3GB was too tight: on 2026-08-02 seven consecutive warehouse refreshes died
                # at 2.7/2.7 GiB while the container sat at ~950 MiB of its 7g budget, and the
                # first refresh to complete afterwards left Gold truncated by 763 revenue rows.
                # The ceiling is now configurable so it can be raised without a code deploy.
                self._connection.execute(f"SET memory_limit='{_memory_limit()}'")
                # Reduce memory usage for bulk operations
                self._connection.execute("SET preserve_insertion_order=false")
                # Large WAL threshold; rely on the explicit 6h CHECKPOINT job.
                # 2MB caused checkpoint-during-write races on DuckDB 1.5.x
                # (corrupted in-memory column: row group rows mismatched column rows).
                self._connection.execute("SET wal_autocheckpoint='1GB'")
                # Enable disk spilling: DuckDB writes to disk instead of OOM crash
                tmp_dir = Path(self.db_path).parent / "duckdb_tmp"
                tmp_dir.mkdir(parents=True, exist_ok=True)
                self._connection.execute(f"SET temp_directory='{tmp_dir}'")

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
                loop = asyncio.get_running_loop()

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
                loop = asyncio.get_running_loop()

                def _run():
                    return conn.execute(query, params or []).fetchall()

                return await asyncio.wait_for(
                    loop.run_in_executor(self._executor, _run),
                    timeout=timeout
                )
            except asyncio.TimeoutError:
                raise QueryTimeoutError(query, timeout, "Fetch all failed")

    async def _init_schema(self) -> None:
        """Create database schema if not exists."""
        schema_sql = """
        -- Orders table
        CREATE TABLE IF NOT EXISTS orders (
            id INTEGER PRIMARY KEY,
            source_id INTEGER NOT NULL,
            status_id INTEGER NOT NULL,
            status_group_id INTEGER,       -- KeyCRM's own grouping; 6 = lost/cancel
            grand_total DECIMAL(12, 2) NOT NULL,
            ordered_at TIMESTAMP WITH TIME ZONE,
            created_at TIMESTAMP WITH TIME ZONE,
            updated_at TIMESTAMP WITH TIME ZONE,  -- For idempotent sync
            buyer_id INTEGER,
            manager_id INTEGER,
            manager_comment TEXT,  -- Contains UTM data for Shopify orders
            promocode VARCHAR,  -- Discount promo code applied to order
            synced_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
            first_seen_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
            update_count INTEGER DEFAULT 0
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
        -- daily_stats was declared here and never written to. It sat empty
        -- while presenting itself as pre-aggregated revenue by source, so any
        -- query that trusted it would have got zeros for every day the
        -- warehouse actually has data. gold_daily_revenue is the real table.
        DROP TABLE IF EXISTS daily_stats;

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

        -- ═══════════════════════════════════════════════════════════════════════
        -- SMS campaigns: the frozen roster of a send.
        --
        -- The eligible population moves every day (people buy, recency slides), so
        -- re-running the segmentation later returns a DIFFERENT set of people. Once
        -- a file has gone to the SMS provider, the only way to measure the campaign
        -- is to have recorded who was in it — target and holdout alike — at the
        -- moment of export. Without this table there is no control group to compare
        -- against and no measurable result.
        -- ═══════════════════════════════════════════════════════════════════════
        CREATE TABLE IF NOT EXISTS sms_campaigns (
            campaign VARCHAR PRIMARY KEY,
            ltv_basis VARCHAR NOT NULL,
            sales_type VARCHAR NOT NULL,
            holdout_pct INTEGER NOT NULL,
            criteria VARCHAR NOT NULL,            -- JSON snapshot of the thresholds
            promocode VARCHAR,                    -- optional, for direct attribution
            exported_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
            sent_at TIMESTAMP WITH TIME ZONE,     -- set once the file actually goes out
            notes VARCHAR,
            -- What the send cost, recorded as it happened. The price is kept
            -- per campaign, not read from config when the result is displayed,
            -- so changing the tariff cannot restate a past campaign.
            message_text VARCHAR,
            message_parts INTEGER,                -- billable segments per message
            recipients_sent INTEGER,              -- how many the gateway accepted
            price_per_part DECIMAL(10, 4),
            cost_total DECIMAL(14, 2)
        );

        CREATE TABLE IF NOT EXISTS sms_campaign_members (
            campaign VARCHAR NOT NULL,
            buyer_id INTEGER NOT NULL,
            phone VARCHAR NOT NULL,
            tier VARCHAR NOT NULL,
            assignment VARCHAR NOT NULL,          -- 'target' or 'holdout'
            -- State at export time: the campaign is measured against what these
            -- customers looked like when they were chosen, not what they became.
            orders_at_export INTEGER NOT NULL,
            revenue_ltv_at_export DECIMAL(14, 2),
            margin_ltv_at_export DECIMAL(14, 2),
            recency_at_export INTEGER,
            -- Delivery, filled in when the campaign is sent through the gateway.
            -- An undelivered number that stays counted as "messaged" drags the
            -- measured lift down, so results exclude anyone not delivered to.
            message_id VARCHAR,
            delivery_status VARCHAR,              -- gateway status, verbatim
            delivered BOOLEAN,                    -- NULL = operator hasn't reported
            delivered_at TIMESTAMP WITH TIME ZONE,
            PRIMARY KEY (campaign, buyer_id)
        );

        -- ═══════════════════════════════════════════════════════════════════════
        -- Marketing opt-outs.
        --
        -- Segmentation looks only at purchases, so without this table someone
        -- who opted out is re-selected by every future export. The provider's
        -- own stoplist prevents delivery but not re-selection, and it leaves
        -- them in the roster as a silent non-responder.
        -- ═══════════════════════════════════════════════════════════════════════
        CREATE TABLE IF NOT EXISTS marketing_optouts (
            buyer_id INTEGER NOT NULL,
            channel VARCHAR NOT NULL DEFAULT 'sms',
            phone VARCHAR,
            reason VARCHAR,                       -- 'stoplist', 'manual', 'complaint'
            source VARCHAR,                       -- who/what recorded it
            opted_out_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (buyer_id, channel)
        );

        CREATE INDEX IF NOT EXISTS idx_optouts_phone ON marketing_optouts(phone);

        CREATE INDEX IF NOT EXISTS idx_sms_members_campaign
            ON sms_campaign_members(campaign, assignment);
        CREATE INDEX IF NOT EXISTS idx_sms_members_buyer
            ON sms_campaign_members(buyer_id);
        -- idx_sms_members_message_id is created with the delivery-column
        -- migration instead: on a database predating the TurboSMS work the
        -- column does not exist yet at this point.

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

        -- Manager classification, effective-dated. `managers.is_retail` is the
        -- current answer; this is the answer *as of the order's date*.
        --
        -- Until 2026-08-20 a reclassification silently restated every report
        -- ever written: `sales_type` is materialised from the manager's state
        -- at rebuild time, so flipping one manager rewrote last year's numbers
        -- on the next refresh. Nobody chose that. The owner chose as-of-order-
        -- date: existing history is frozen as the first interval and a change
        -- applies forward only.
        --
        -- `valid_from` is inclusive, `valid_to` exclusive, NULL means open.
        -- The PRIMARY KEY costs nothing here despite the ART-index rule that
        -- governs the Gold tables: that cost is paid per DELETE+INSERT rebuild
        -- cycle, and this table is written by hand a few times a year.
        CREATE TABLE IF NOT EXISTS manager_classifications (
            manager_id INTEGER NOT NULL,
            is_retail BOOLEAN NOT NULL,
            valid_from DATE NOT NULL,
            valid_to DATE,
            set_by INTEGER,                       -- admin user id; NULL = seeded baseline
            set_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
            note VARCHAR,
            PRIMARY KEY (manager_id, valid_from)
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
        -- silver_orders itself is created from SILVER_ORDERS_DDL just before
        -- this script runs. It was declared here too, byte-for-byte identical,
        -- which is the same duplication that let the exhibition fix land in one
        -- copy of the Silver SELECT and not the other. Two identical copies are
        -- one edit away from two different ones.

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

        -- Order ids KeyCRM does not have. The gap-backfill walks the holes in
        -- our own id sequence, and a hole KeyCRM cannot fill is permanent —
        -- a deleted order, or one that never existed. Remembering them is what
        -- stops the job re-requesting the same 404s every run, forever.
        CREATE TABLE IF NOT EXISTS order_backfill_misses (
            order_id BIGINT PRIMARY KEY,
            checked_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
            reason VARCHAR
        );

        -- Additional indexes (non-duplicate, supplementing per-table indexes above)
        CREATE INDEX IF NOT EXISTS idx_products_category_id ON products(category_id);
        CREATE INDEX IF NOT EXISTS idx_products_brand ON products(brand);
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

        -- gold_daily_products deliberately carries NO index. In DuckDB 1.5.5 a
        -- single ART index — a PRIMARY KEY counts — disables row-group
        -- vacuuming for the whole table, and it is binary: one index costs
        -- exactly what six do. Measured on a clone of this table at its real
        -- size, a full DELETE+INSERT rebuild leaks 0.000 MB/cycle with no
        -- index and 1.171 MB with any number of them. At ~45 full rebuilds a
        -- day that was 53 MB/day — the largest single contributor to the file
        -- growth behind the weekly stop-the-world compaction.
        --
        -- What the six bought, measured against the real query shapes:
        --   top products for a month      1.18 ms -> 1.16 ms without
        --   revenue by brand for a month  0.65 ms -> 0.65 ms
        --   revenue by category           0.69 ms -> 0.63 ms
        --   brand breakdown for a year    0.84 ms -> 0.81 ms
        --   one product, all time         0.16 ms -> 0.26 ms
        -- Four of five unchanged; the fifth costs a tenth of a millisecond.
        -- 87 906 rows is nothing to a columnar scan, and the zone maps on
        -- `date` already do the work these indexes were added for.
        --
        -- Do not add one back without measuring both sides again.
        CREATE INDEX IF NOT EXISTS idx_warehouse_refreshes_at ON warehouse_refreshes(refreshed_at);

        -- Composite indexes for drill-down queries (30-40% speedup)
        CREATE INDEX IF NOT EXISTS idx_silver_source_date_type ON silver_orders(source_id, order_date, sales_type);
        CREATE INDEX IF NOT EXISTS idx_silver_active_return ON silver_orders(is_active_source, is_return, order_date);

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

        -- Weekly sales report: which weeks have already been delivered.
        -- The job fires daily and reports the last *complete* week, so this
        -- ledger is what keeps six of seven firings quiet. It also makes a
        -- missed Monday self-healing: a weekly CronTrigger that was not alive
        -- at its instant does not run late, it computes its next fire a week
        -- out and the week is simply never reported.
        CREATE TABLE IF NOT EXISTS weekly_report_sends (
            week_start DATE NOT NULL,
            sales_type VARCHAR NOT NULL,
            revenue DECIMAL(14, 2) NOT NULL,
            orders INTEGER NOT NULL,
            sent_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (week_start, sales_type)
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

        -- ═══════════════════════════════════════════════════════════════════════
        -- H3: Bronze order events (append-only audit log)
        -- Phase 1 — written alongside upsert_orders as shadow write.
        -- Phase 3 — becomes the only ingest path; promotion job reads this and
        -- produces orders in a single-writer batch. Rows are never deleted within
        -- the 7-day replay window, only tagged with processed_at.
        -- ═══════════════════════════════════════════════════════════════════════
        CREATE SEQUENCE IF NOT EXISTS seq_bronze_order_events_id START 1;

        CREATE TABLE IF NOT EXISTS bronze_order_events (
            id BIGINT PRIMARY KEY DEFAULT nextval('seq_bronze_order_events_id'),
            order_id INTEGER NOT NULL,
            payload JSON NOT NULL,
            source VARCHAR NOT NULL,
            event_ts TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
            processed_at TIMESTAMP WITH TIME ZONE
        );

        -- Promotion job scans unprocessed events ordered by time
        CREATE INDEX IF NOT EXISTS idx_bronze_processed_at
            ON bronze_order_events(processed_at);

        -- Replay and latest-per-order lookups
        CREATE INDEX IF NOT EXISTS idx_bronze_order
            ON bronze_order_events(order_id, event_ts DESC);

        -- Retention prune scans by event_ts
        CREATE INDEX IF NOT EXISTS idx_bronze_event_ts
            ON bronze_order_events(event_ts);

        """
        # Silver's shape has one home; the schema script above no longer
        # carries a copy of it.
        self._connection.execute(SILVER_ORDERS_DDL)
        self._connection.execute(schema_sql)

        # The order-line level. A view, so it is always exactly as fresh as
        # Silver and costs the file nothing — see SILVER_ORDER_LINES_VIEW_SQL.
        self._connection.execute(SILVER_ORDER_LINES_VIEW_SQL)

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

        # Migration: Add status_group_id to orders. KeyCRM's own grouping of the
        # status — 6 is lost/cancel — read off the order payload and preferred
        # over our hardcoded id list wherever it is known.
        #
        # No DEFAULT, deliberately: `ADD COLUMN ... DEFAULT x` rewrites every
        # row to materialise the value and has OOM-killed this container on a
        # multi-GB database before. Existing rows get NULL, which is exactly
        # what the fallback in the Silver CASE expects.
        try:
            self._connection.execute(
                "ALTER TABLE orders ADD COLUMN IF NOT EXISTS status_group_id INTEGER"
            )
            logger.debug("Migration: status_group_id column added/verified")
        except Exception as e:
            logger.debug(f"Migration note: {e}")

        # Migration: Add manager_comment column to orders table (for UTM tracking)
        try:
            self._connection.execute(
                "ALTER TABLE orders ADD COLUMN IF NOT EXISTS manager_comment TEXT"
            )
            logger.debug("Migration: manager_comment column added/verified")
        except Exception as e:
            logger.debug(f"Migration note (manager_comment): {e}")

        # Migration: record which mode a refresh actually ran in.
        #
        # warehouse_refreshes has 69k rows and is the best forensic trail in
        # the system, but it never recorded whether Silver was rebuilt whole
        # or incrementally — so the file growth that forced a weekly
        # stop-the-world compaction could not be attributed to a cause. Twice
        # now an estimate has been made by extrapolation and been wrong.
        #
        # No DEFAULT: on a table this size, materialising one rewrites every
        # row and has OOM-ed the container before. Existing rows get NULL,
        # which reads correctly as "we did not record this".
        # Migration: drop every index on gold_daily_products.
        #
        # They are gone from the schema above; this removes them from databases
        # that already have them. Existing dead row versions are NOT reclaimed
        # by the DROP — that needs a vacuum, which needs an exclusive lock a
        # two-minute refresh loop rarely yields. Growth stops immediately; the
        # ~197 MB already accumulated comes back at the next window that gets
        # the lock, or at the weekly compaction.
        for _idx in (
            "idx_gold_prod_date", "idx_gold_prod_product", "idx_gold_prod_brand",
            "idx_gold_prod_category", "idx_gold_prod_cat_date",
            "idx_gold_prod_brand_date",
        ):
            try:
                self._connection.execute(f"DROP INDEX IF EXISTS {_idx}")
            except Exception as e:
                logger.warning("Migration (drop %s) failed: %s", _idx, e)

        try:
            self._connection.execute(
                "ALTER TABLE warehouse_refreshes ADD COLUMN IF NOT EXISTS silver_mode VARCHAR"
            )
        except Exception as e:
            # WARNING, not DEBUG. A swallowed ALTER is exactly how the
            # 2026-08-09 incident began: the migration failed quietly, the
            # code read the column anyway, and the self-heal turned a one-off
            # into a rebuild every two minutes for hours.
            logger.warning("Migration (warehouse_refreshes.silver_mode) failed: %s", e)

        # Migration: freeze today's classification as each manager's first
        # interval. Runs every startup so a manager who syncs later still gets
        # a baseline; it never touches a manager who already has one, which is
        # what keeps a human's forward-dated answer from being overwritten by
        # the seed that used to fight it.
        #
        # 1970-01-01 rather than the manager's first order: the floor has to
        # precede every order the warehouse will ever hold, and the earliest is
        # 2023-12-02.
        try:
            self._connection.execute("""
                CREATE TABLE IF NOT EXISTS manager_classifications (
                    manager_id INTEGER NOT NULL,
                    is_retail BOOLEAN NOT NULL,
                    valid_from DATE NOT NULL,
                    valid_to DATE,
                    set_by INTEGER,
                    set_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                    note VARCHAR,
                    PRIMARY KEY (manager_id, valid_from)
                )
            """)
            seeded = self._connection.execute("""
                INSERT INTO manager_classifications
                    (manager_id, is_retail, valid_from, valid_to, set_by, note)
                SELECT m.id, COALESCE(m.is_retail, FALSE), DATE '1970-01-01',
                       NULL, NULL, 'baseline frozen from managers.is_retail'
                FROM managers m
                WHERE NOT EXISTS (
                    SELECT 1 FROM manager_classifications mc
                    WHERE mc.manager_id = m.id
                )
                RETURNING manager_id
            """).fetchall()
            if seeded:
                logger.info(
                    "Migration: seeded %d manager classification baselines",
                    len(seeded),
                )
        except Exception as e:
            # WARNING, not DEBUG: if this table is empty the Silver CASE falls
            # back to `managers.is_retail` and the as-of-date guarantee is
            # quietly gone. That is precisely the failure mode this project has
            # already paid for once.
            logger.warning("Migration (manager_classifications seed) failed: %s", e)

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

        # Migration: Recreate offer_stocks with PRIMARY KEY if missing
        # (CREATE TABLE IF NOT EXISTS doesn't alter existing tables, so old tables
        # may lack PK constraint. This enables INSERT OR REPLACE instead of DELETE+INSERT.)
        try:
            has_pk = False
            try:
                result = self._connection.execute("""
                    SELECT COUNT(*) FROM duckdb_constraints()
                    WHERE table_name = 'offer_stocks' AND constraint_type = 'PRIMARY KEY'
                """).fetchone()
                has_pk = result[0] > 0
            except Exception:
                pass

            if not has_pk:
                logger.info("Migration: Adding PRIMARY KEY to offer_stocks...")
                self._connection.execute("BEGIN TRANSACTION")
                try:
                    self._connection.execute("ALTER TABLE offer_stocks RENAME TO _offer_stocks_old")
                    self._connection.execute("""
                        CREATE TABLE offer_stocks (
                            id INTEGER PRIMARY KEY,
                            sku VARCHAR,
                            price DECIMAL(12, 2),
                            purchased_price DECIMAL(12, 2),
                            quantity INTEGER DEFAULT 0,
                            reserve INTEGER DEFAULT 0,
                            synced_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
                        )
                    """)
                    self._connection.execute("""
                        INSERT INTO offer_stocks SELECT * FROM _offer_stocks_old
                    """)
                    self._connection.execute("DROP TABLE _offer_stocks_old")
                    self._connection.execute("COMMIT")
                    logger.info("Migration: offer_stocks PRIMARY KEY migration complete")
                except Exception as e:
                    try:
                        self._connection.execute("ROLLBACK")
                    except Exception:
                        pass
                    logger.error(f"Migration failed (offer_stocks PK), rolling back: {e}")
        except Exception as e:
            logger.debug(f"Migration note (offer_stocks PK): {e}")

        # Migration: what a campaign cost to send.
        #
        # Without these the results page can report added margin but not whether
        # the campaign paid for itself — on the first real send that arithmetic
        # was done in someone's head against a figure from the provider's
        # invoice. The price is stored per campaign rather than read from config
        # at display time, so a tariff change does not silently restate history.
        #
        # No DEFAULT on any of them: ALTER ... ADD COLUMN ... DEFAULT rewrites
        # the whole table to materialise the value and has OOMed this database
        # before. Existing campaigns get NULL, which reads as "cost unknown".
        for col, ddl in (
            ("message_text", "VARCHAR"),
            ("message_parts", "INTEGER"),
            ("recipients_sent", "INTEGER"),
            ("price_per_part", "DECIMAL(10, 4)"),
            ("cost_total", "DECIMAL(14, 2)"),
        ):
            try:
                self._connection.execute(
                    f"ALTER TABLE sms_campaigns ADD COLUMN IF NOT EXISTS {col} {ddl}"
                )
            except Exception as e:
                logger.debug(f"Migration note (sms_campaigns {col}): {e}")
        logger.debug("Migration: sms_campaigns cost columns added/verified")

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

        # Migration: delivery columns on sms_campaign_members.
        #
        # The table ships from CREATE TABLE IF NOT EXISTS, which does nothing to
        # a table that already exists — so a database created before the
        # TurboSMS work has the roster but none of the delivery columns, and
        # every delivery report 500s. No DEFAULT on these: ALTER TABLE ... ADD
        # COLUMN ... DEFAULT rewrites the whole table and OOMs on a large DB.
        for column, ddl_type in (
            ("message_id", "VARCHAR"),
            ("delivery_status", "VARCHAR"),
            ("delivered", "BOOLEAN"),
            ("delivered_at", "TIMESTAMP WITH TIME ZONE"),
        ):
            try:
                self._connection.execute(
                    f"ALTER TABLE sms_campaign_members "
                    f"ADD COLUMN IF NOT EXISTS {column} {ddl_type}"
                )
                logger.debug(
                    "Migration: %s column added/verified on sms_campaign_members",
                    column,
                )
            except Exception as e:
                logger.debug(f"Migration note (sms_campaign_members {column}): {e}")

        # Index on message_id, created here rather than with the other indexes
        # because on a database that predates the TurboSMS work the column only
        # exists once the loop above has run. Every delivery report looks a
        # member up by message_id, and a send of 5 000 produces 5 000 of them
        # inside a minute; without this each one scans the whole table.
        try:
            self._connection.execute(
                "CREATE INDEX IF NOT EXISTS idx_sms_members_message_id "
                "ON sms_campaign_members(message_id)"
            )
        except Exception as e:
            logger.debug(f"Migration note (idx_sms_members_message_id): {e}")

        # Migration: Add language column to user_preferences
        try:
            self._connection.execute(
                "ALTER TABLE user_preferences ADD COLUMN IF NOT EXISTS language VARCHAR DEFAULT 'en'"
            )
            logger.debug("Migration: language column added/verified on user_preferences")
        except Exception as e:
            logger.debug(f"Migration note (user_preferences language): {e}")

        # Migration: Add promocode column to orders and silver_orders
        try:
            self._connection.execute(
                "ALTER TABLE orders ADD COLUMN IF NOT EXISTS promocode VARCHAR"
            )
            self._connection.execute(
                "ALTER TABLE silver_orders ADD COLUMN IF NOT EXISTS promocode VARCHAR"
            )
            logger.debug("Migration: promocode column added/verified on orders and silver_orders")
        except Exception as e:
            logger.debug(f"Migration note (promocode): {e}")

        # Migration: Add audit columns to orders (first_seen_at, update_count)
        # No DEFAULT in ALTER — avoids full table rewrite on 9GB+ DB (OOM).
        # CREATE TABLE schema has defaults for new rows; existing rows get NULL/NULL.
        try:
            self._connection.execute(
                "ALTER TABLE orders ADD COLUMN IF NOT EXISTS first_seen_at TIMESTAMP WITH TIME ZONE"
            )
            self._connection.execute(
                "ALTER TABLE orders ADD COLUMN IF NOT EXISTS update_count INTEGER"
            )
            logger.debug("Migration: audit columns (first_seen_at, update_count) added/verified on orders")
        except Exception as e:
            logger.debug(f"Migration note (audit columns): {e}")

        # Migration: Add reconciliation_log table
        try:
            self._connection.execute(
                "CREATE SEQUENCE IF NOT EXISTS reconciliation_seq START 1"
            )
            self._connection.execute("""
                CREATE TABLE IF NOT EXISTS reconciliation_log (
                    id INTEGER DEFAULT(nextval('reconciliation_seq')),
                    check_date DATE NOT NULL,
                    api_count INTEGER NOT NULL,
                    db_count INTEGER NOT NULL,
                    discrepancy INTEGER NOT NULL,
                    discrepancy_pct DECIMAL(6, 2) NOT NULL,
                    status VARCHAR NOT NULL,
                    checked_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
                )
            """)
            logger.debug("Migration: reconciliation_log table added/verified")
        except Exception as e:
            logger.debug(f"Migration note (reconciliation_log): {e}")

        # Migration: Data Quality framework (Layer 1+2) — run+diff schema.
        # Replaces single-row-per-check pattern with proper run/diff
        # parent-child for trend and audit. Old reconciliation_log stays.
        try:
            self._connection.execute(
                "CREATE SEQUENCE IF NOT EXISTS data_quality_run_seq START 1"
            )
            self._connection.execute("""
                CREATE TABLE IF NOT EXISTS data_quality_runs (
                    run_id BIGINT PRIMARY KEY DEFAULT(nextval('data_quality_run_seq')),
                    started_at TIMESTAMP WITH TIME ZONE NOT NULL,
                    ended_at TIMESTAMP WITH TIME ZONE,
                    as_of TIMESTAMP WITH TIME ZONE NOT NULL,
                    window_start DATE NOT NULL,
                    window_end DATE NOT NULL,
                    layer VARCHAR NOT NULL,          -- 'integrity' | 'reconciliation' | 'combined'
                    status VARCHAR NOT NULL,          -- 'PASS' | 'WARN' | 'CRITICAL' | 'FAILED'
                    integrity_issues_count INTEGER DEFAULT 0,
                    discrepancies_count INTEGER DEFAULT 0,
                    critical_count INTEGER DEFAULT 0,
                    warn_count INTEGER DEFAULT 0,
                    api_calls_used INTEGER DEFAULT 0,
                    duration_ms INTEGER,
                    error_message VARCHAR
                )
            """)
            self._connection.execute(
                "CREATE INDEX IF NOT EXISTS idx_dqr_started_at ON data_quality_runs(started_at DESC)"
            )
            self._connection.execute(
                "CREATE INDEX IF NOT EXISTS idx_dqr_layer ON data_quality_runs(layer)"
            )

            # Child table: layer-1 issues
            self._connection.execute("""
                CREATE TABLE IF NOT EXISTS data_quality_issues (
                    run_id BIGINT NOT NULL,
                    check_name VARCHAR NOT NULL,
                    table_name VARCHAR NOT NULL,
                    severity VARCHAR NOT NULL,
                    count INTEGER NOT NULL,
                    sample_ids VARCHAR,    -- JSON array
                    description VARCHAR
                )
            """)
            self._connection.execute(
                "CREATE INDEX IF NOT EXISTS idx_dqi_run ON data_quality_issues(run_id)"
            )

            # Child table: layer-2 discrepancies
            self._connection.execute("""
                CREATE TABLE IF NOT EXISTS data_quality_diffs (
                    run_id BIGINT NOT NULL,
                    month VARCHAR NOT NULL,        -- 'YYYY-MM'
                    source_id INTEGER NOT NULL,
                    diff_class VARCHAR NOT NULL,
                    field VARCHAR NOT NULL,
                    dk_value DOUBLE NOT NULL,
                    kc_value DOUBLE NOT NULL,
                    severity VARCHAR NOT NULL,
                    order_ids VARCHAR              -- JSON array, may be NULL
                )
            """)
            self._connection.execute(
                "CREATE INDEX IF NOT EXISTS idx_dqd_run ON data_quality_diffs(run_id)"
            )
            logger.debug("Migration: data_quality_* tables added/verified")
        except Exception as e:
            logger.debug(f"Migration note (data_quality_*): {e}")

        # Migration: disk samples for the growth watchdog.
        # Tiny table — one row per 6h sample, retained ~14 days = ~56 rows.
        # No sequence: composite of sampled_at is enough; nobody queries by id.
        try:
            self._connection.execute("""
                CREATE TABLE IF NOT EXISTS disk_samples (
                    sampled_at TIMESTAMP WITH TIME ZONE NOT NULL,
                    db_size_mb DOUBLE NOT NULL,
                    disk_pct_used DOUBLE NOT NULL,
                    disk_free_gb DOUBLE NOT NULL
                )
            """)
            self._connection.execute(
                "CREATE INDEX IF NOT EXISTS idx_disk_samples_at "
                "ON disk_samples(sampled_at DESC)"
            )
            logger.debug("Migration: disk_samples table added/verified")
        except Exception as e:
            logger.debug(f"Migration note (disk_samples): {e}")

        # Same shape, for memory. Persisted because the kernel's own counters
        # (memory.peak, memory.events oom_kill) reset when the container is
        # recreated — which is how a 5.3 GB reading in August 2026 became
        # impossible to decompose an hour later.
        try:
            self._connection.execute("""
                CREATE TABLE IF NOT EXISTS memory_samples (
                    sampled_at TIMESTAMP WITH TIME ZONE NOT NULL,
                    working_set_mb DOUBLE NOT NULL,
                    page_cache_mb DOUBLE NOT NULL,
                    limit_mb DOUBLE,
                    oom_kills INTEGER NOT NULL DEFAULT 0
                )
            """)
            self._connection.execute(
                "CREATE INDEX IF NOT EXISTS idx_memory_samples_at "
                "ON memory_samples(sampled_at DESC)"
            )
            logger.debug("Migration: memory_samples table added/verified")
        except Exception as e:
            logger.debug(f"Migration note (memory_samples): {e}")

        # One row per path group per sample. The growth detector differences
        # this at a 168h lag — the compact's own period — so everything
        # periodic cancels and only trend survives.
        try:
            self._connection.execute("""
                CREATE TABLE IF NOT EXISTS data_dir_samples (
                    sampled_at TIMESTAMP WITH TIME ZONE NOT NULL,
                    path_group VARCHAR NOT NULL,
                    bytes BIGINT NOT NULL
                )
            """)
            self._connection.execute(
                "CREATE INDEX IF NOT EXISTS idx_data_dir_samples_at "
                "ON data_dir_samples(sampled_at DESC)"
            )
            logger.debug("Migration: data_dir_samples table added/verified")
        except Exception as e:
            logger.debug(f"Migration note (data_dir_samples): {e}")

        # Migration: Fix sequences after EXPORT/IMPORT compaction.
        # DuckDB doesn't support ALTER SEQUENCE, and IMPORT resets sequences to
        # START value (1) even though tables already have rows. Fix by DROP+CREATE
        # with START = max(id) + 1.
        _seq_table_map = [
            ("warehouse_refresh_seq", "warehouse_refreshes", "id"),
            ("reconciliation_seq", "reconciliation_log", "id"),
            ("data_quality_run_seq", "data_quality_runs", "run_id"),
            ("seq_stock_movements_id", "stock_movements", "id"),
            ("seq_buyer_contacts_id", "buyer_contacts", "id"),
            ("seq_manual_expenses_id", "manual_expenses", "id"),
            ("seq_report_history_id", "report_history", "id"),
            ("seq_bronze_order_events_id", "bronze_order_events", "id"),
        ]
        for seq_name, table_name, col in _seq_table_map:
            try:
                row = self._connection.execute(
                    f"SELECT COALESCE(MAX({col}), 0) FROM {table_name}"
                ).fetchone()
                max_id = row[0] if row else 0
                if max_id > 0:
                    # Check current sequence value by peeking at nextval
                    cur = self._connection.execute(f"SELECT nextval('{seq_name}')").fetchone()[0]
                    if cur <= max_id:
                        new_start = max_id + 1
                        self._connection.execute(f"DROP SEQUENCE {seq_name}")
                        self._connection.execute(f"CREATE SEQUENCE {seq_name} START {new_start}")
                        logger.info(f"Sequence {seq_name} reset: {cur} → {new_start}")
            except Exception as e:
                logger.debug(f"Sequence fix note ({seq_name}): {e}")

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

        -- View: SKU status with dead stock + overstocked classification
        -- Uses 90-day velocity for days_of_supply (more stable than 30d)
        CREATE OR REPLACE VIEW v_sku_status AS
        SELECT
            s.*,
            COALESCE(cv.threshold_days, 180) as threshold_days,
            CASE WHEN COALESCE(vel.qty_sold_90d, 0) > 0
                 THEN ROUND((s.quantity - s.reserve) / (vel.qty_sold_90d / 90.0), 0)
                 ELSE NULL
            END as days_of_supply,
            CASE
                WHEN s.last_sale_date IS NULL THEN 'never_sold'
                WHEN s.days_since_sale > COALESCE(cv.threshold_days, 180) THEN 'dead_stock'
                WHEN s.days_since_sale > COALESCE(cv.threshold_days, 180) * 0.7 THEN 'at_risk'
                WHEN COALESCE(vel.qty_sold_90d, 0) > 0
                     AND ROUND((s.quantity - s.reserve) / (vel.qty_sold_90d / 90.0), 0) > 90
                    THEN 'overstocked'
                ELSE 'healthy'
            END as status
        FROM v_sku_analysis s
        LEFT JOIN v_category_velocity cv ON s.category_id = cv.category_id
        LEFT JOIN (
            SELECT product_id, SUM(quantity_sold) as qty_sold_90d
            FROM gold_daily_products
            WHERE date >= CURRENT_DATE - INTERVAL '90 days'
            GROUP BY product_id
        ) vel ON s.product_id = vel.product_id
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

        -- ═══════════════════════════════════════════════════════════════════════
        -- LAYER 3b: Turnover & ABC Analytics Views
        -- ═══════════════════════════════════════════════════════════════════════

        -- View: Per-SKU sell-through (joins inventory with actual sales from gold_daily_products)
        CREATE OR REPLACE VIEW v_sku_sell_through AS
        SELECT
            s.offer_id,
            s.product_id,
            s.sku,
            s.name,
            s.brand,
            s.category_id,
            s.category_name,
            s.available,
            s.available_value,
            s.price,
            s.purchased_price,
            s.days_since_sale,
            s.days_in_stock,
            COALESCE(g30.qty_sold_30d, 0) as qty_sold_30d,
            COALESCE(g30.revenue_30d, 0) as revenue_30d,
            COALESCE(g30.orders_30d, 0) as orders_30d,
            COALESCE(g90.qty_sold_90d, 0) as qty_sold_90d,
            COALESCE(g90.revenue_90d, 0) as revenue_90d,
            CASE WHEN (COALESCE(g30.qty_sold_30d, 0) + s.available) > 0
                 THEN ROUND(100.0 * COALESCE(g30.qty_sold_30d, 0) /
                      (COALESCE(g30.qty_sold_30d, 0) + s.available), 1)
                 ELSE 0
            END as sell_through_rate_30d,
            CASE WHEN COALESCE(g90.qty_sold_90d, 0) > 0
                 THEN ROUND(s.available / (g90.qty_sold_90d / 90.0), 0)
                 ELSE NULL
            END as days_of_supply,
            CASE WHEN COALESCE(g90.qty_sold_90d, 0) > 0
                 THEN ROUND(g90.qty_sold_90d / 90.0, 2)
                 ELSE 0
            END as avg_daily_sales
        FROM v_sku_analysis s
        LEFT JOIN (
            SELECT product_id,
                   SUM(quantity_sold) as qty_sold_30d,
                   SUM(product_revenue) as revenue_30d,
                   SUM(order_count) as orders_30d
            FROM gold_daily_products
            WHERE date >= CURRENT_DATE - INTERVAL '30 days'
            GROUP BY product_id
        ) g30 ON s.product_id = g30.product_id
        LEFT JOIN (
            SELECT product_id,
                   SUM(quantity_sold) as qty_sold_90d,
                   SUM(product_revenue) as revenue_90d
            FROM gold_daily_products
            WHERE date >= CURRENT_DATE - INTERVAL '90 days'
            GROUP BY product_id
        ) g90 ON s.product_id = g90.product_id
        WHERE s.quantity > 0;

        -- View: ABC classification by cumulative revenue (Pareto)
        CREATE OR REPLACE VIEW v_abc_classification AS
        WITH product_revenue AS (
            SELECT
                product_id,
                SUM(product_revenue) as total_revenue,
                SUM(quantity_sold) as total_qty_sold
            FROM gold_daily_products
            WHERE date >= CURRENT_DATE - INTERVAL '90 days'
            GROUP BY product_id
        ),
        ranked AS (
            SELECT
                s.offer_id,
                s.product_id,
                s.sku,
                s.name,
                s.brand,
                s.category_name,
                s.available,
                s.available_value,
                s.price,
                COALESCE(pr.total_revenue, 0) as revenue_90d,
                COALESCE(pr.total_qty_sold, 0) as qty_sold_90d,
                SUM(COALESCE(pr.total_revenue, 0)) OVER () as grand_total_revenue,
                SUM(COALESCE(pr.total_revenue, 0)) OVER (
                    ORDER BY COALESCE(pr.total_revenue, 0) DESC
                    ROWS UNBOUNDED PRECEDING
                ) as cumulative_revenue
            FROM v_sku_analysis s
            LEFT JOIN product_revenue pr ON s.product_id = pr.product_id
            WHERE s.quantity > 0
        )
        SELECT
            *,
            CASE WHEN grand_total_revenue > 0
                 THEN ROUND(100.0 * cumulative_revenue / grand_total_revenue, 1)
                 ELSE 0
            END as cumulative_pct,
            CASE
                WHEN grand_total_revenue > 0
                     AND cumulative_revenue - COALESCE(revenue_90d, 0) < grand_total_revenue * 0.8
                    THEN 'A'
                WHEN grand_total_revenue > 0
                     AND cumulative_revenue - COALESCE(revenue_90d, 0) < grand_total_revenue * 0.95
                    THEN 'B'
                ELSE 'C'
            END as abc_class
        FROM ranked;

        -- View: ABC summary (aggregated stats per class)
        CREATE OR REPLACE VIEW v_abc_summary AS
        SELECT
            abc_class,
            COUNT(*) as sku_count,
            SUM(available) as total_units,
            SUM(available_value) as stock_value,
            SUM(revenue_90d) as revenue,
            ROUND(100.0 * SUM(available_value) /
                NULLIF(SUM(SUM(available_value)) OVER (), 0), 1) as stock_value_pct,
            ROUND(100.0 * SUM(revenue_90d) /
                NULLIF(SUM(SUM(revenue_90d)) OVER (), 0), 1) as revenue_pct
        FROM v_abc_classification
        GROUP BY abc_class
        ORDER BY abc_class;

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

        -- ═══════════════════════════════════════════════════════════════════════
        -- LAYER 5: Dead Stock v2 — cost-basis ranking + velocity + ABC + decision
        -- ═══════════════════════════════════════════════════════════════════════

        -- View: per-SKU dead stock analysis with cost basis, GMROI inputs, and
        -- velocity tiers. NPV decision is computed in Python (so carrying rate
        -- and liquidation discount can be tuned without rebuilding the view).
        CREATE OR REPLACE VIEW v_sku_dead_stock_v2 AS
        WITH cost_ratio AS (
            -- Portfolio-wide cost-to-sale ratio for fallback when purchased_price is missing
            SELECT
                COALESCE(
                    SUM(quantity * NULLIF(purchased_price, 0)) /
                    NULLIF(SUM(quantity * NULLIF(price, 0)), 0),
                    0.5
                ) as ratio
            FROM offer_stocks
            WHERE quantity > 0
        ),
        sales_90 AS (
            SELECT product_id,
                   SUM(quantity_sold) as qty_sold_90d,
                   SUM(product_revenue) as revenue_90d
            FROM gold_daily_products
            WHERE date >= CURRENT_DATE - INTERVAL '90 days'
            GROUP BY product_id
        ),
        sales_30 AS (
            SELECT product_id,
                   SUM(quantity_sold) as qty_sold_30d,
                   SUM(product_revenue) as revenue_30d
            FROM gold_daily_products
            WHERE date >= CURRENT_DATE - INTERVAL '30 days'
            GROUP BY product_id
        ),
        base AS (
            SELECT
                s.offer_id,
                s.product_id,
                s.sku,
                s.name,
                s.brand,
                s.category_id,
                s.category_name,
                s.available,
                s.price,
                s.purchased_price,
                s.days_since_sale,
                s.days_in_stock,
                s.last_sale_date,
                COALESCE(
                    NULLIF(s.purchased_price, 0),
                    s.price * (SELECT ratio FROM cost_ratio)
                ) as effective_unit_cost,
                CASE
                    WHEN s.purchased_price IS NULL OR s.purchased_price = 0
                        THEN 'fallback'
                    ELSE 'actual'
                END as cost_quality,
                COALESCE(s90.qty_sold_90d, 0) as qty_sold_90d,
                COALESCE(s90.revenue_90d, 0) as revenue_90d,
                COALESCE(s30.qty_sold_30d, 0) as qty_sold_30d,
                COALESCE(s30.revenue_30d, 0) as revenue_30d,
                COALESCE(a.abc_class, 'C') as abc_class
            FROM v_sku_analysis s
            LEFT JOIN sales_90 s90 ON s.product_id = s90.product_id
            LEFT JOIN sales_30 s30 ON s.product_id = s30.product_id
            LEFT JOIN v_abc_classification a ON s.offer_id = a.offer_id
            WHERE s.quantity > 0
        )
        SELECT
            b.*,
            b.available * b.price as sale_value,
            b.available * b.effective_unit_cost as cost_basis,
            CASE WHEN b.qty_sold_90d > 0
                 THEN ROUND(b.available / (b.qty_sold_90d / 90.0), 0)
                 ELSE NULL
            END as days_of_supply,
            CASE WHEN b.qty_sold_90d > 0 THEN ROUND(b.qty_sold_90d / 90.0, 3) ELSE 0 END as avg_daily_sales_90d,
            CASE WHEN b.qty_sold_30d > 0 THEN ROUND(b.qty_sold_30d / 30.0, 3) ELSE 0 END as avg_daily_sales_30d,
            CASE
                WHEN b.qty_sold_90d = 0 THEN 'frozen'
                WHEN b.available / (b.qty_sold_90d / 90.0) > 365 THEN 'frozen'
                WHEN b.available / (b.qty_sold_90d / 90.0) > 180 THEN 'cold'
                WHEN b.available / (b.qty_sold_90d / 90.0) > 90 THEN 'warm'
                WHEN b.available / (b.qty_sold_90d / 90.0) > 30 THEN 'healthy'
                ELSE 'hot'
            END as velocity_tier,
            -- Velocity decay: 30d rate vs 90d rate. <0.7 = slowing, >1.3 = accelerating
            CASE WHEN b.qty_sold_90d > 0 AND (b.qty_sold_90d / 90.0) > 0
                 THEN ROUND((b.qty_sold_30d / 30.0) / (b.qty_sold_90d / 90.0), 2)
                 ELSE NULL
            END as velocity_ratio_30_90,
            -- Annualized gross profit per SKU (revenue × 4 × margin)
            CASE WHEN b.price > 0
                 THEN b.revenue_90d * 4.0 * ((b.price - b.effective_unit_cost) / b.price)
                 ELSE 0
            END as annual_gross_profit,
            -- GMROI annualized: gross profit / cost_basis (avg inventory proxy = current)
            CASE WHEN (b.available * b.effective_unit_cost) > 0 AND b.price > 0
                 THEN b.revenue_90d * 4.0 * ((b.price - b.effective_unit_cost) / b.price)
                      / (b.available * b.effective_unit_cost)
                 ELSE NULL
            END as gmroi
        FROM base b;
        """
        self._connection.execute(views_sql)
        logger.info("Inventory analytics views created")

    def _build_sales_type_filter(self, sales_type: str, table_alias: str = "o") -> str:
        """Build the SQL clause selecting one `sales_type`, for any orders-shaped table.

        This reads the answer Silver already stored; it does **not** re-derive
        it from `manager_id`. Deriving it a second time was a second definition
        in all but name, and the two had already parted company: #101 gave
        source 5 its own `sales_type`, and this filter — knowing only about
        managers — went on counting those 176 orders (₴267,416) as retail while
        Gold counted them as exhibition. Ten of its eleven call sites query raw
        `orders`, which is how a Gold-free corner of the dashboard ended up with
        its own opinion.

        Reading the column also makes the classification as-of-order-date for
        every consumer at once, which is the whole point of
        `manager_classifications`.

        Silver covers every order — 46,272 of 46,272, verified 2026-08-20 — so
        the EXISTS excludes nothing the old clause admitted.

        Args:
            sales_type: one of KNOWN_SALES_TYPES, or 'all' for no filter
            table_alias: alias of the orders-shaped table to constrain

        Returns:
            SQL WHERE clause fragment
        """
        if sales_type == "all":
            return "1=1"
        if sales_type not in KNOWN_SALES_TYPES:
            raise ValueError(
                f"unknown sales_type {sales_type!r}; expected one of "
                f"{', '.join(KNOWN_SALES_TYPES)} or 'all'"
            )
        return (
            f"EXISTS (SELECT 1 FROM silver_orders sv "
            f"WHERE sv.id = {table_alias}.id AND sv.sales_type = '{sales_type}')"
        )

    # ─── Warehouse Layer Refresh ─────────────────────────────────────────────

    async def refresh_warehouse_layers(
        self,
        trigger: str = "manual",
        changed_order_ids: list[int] | None = None,
    ) -> Dict[str, Any]:
        """Rebuild Silver and Gold warehouse layers from Bronze tables.

        Silver is always fully rebuilt (37K rows, <1s). Gold is rebuilt
        incrementally when changed_order_ids is provided (only affected dates),
        or fully otherwise.

        Args:
            trigger: What triggered the refresh
            changed_order_ids: Order IDs that changed (for incremental Gold rebuild)

        Returns:
            Dict with refresh stats and validation results
        """
        import time
        start_time = time.perf_counter()
        error_msg = None

        # Silver's shape lives at module level — see silver_select_sql above.
        # It used to be built here and copied into web/routes/api/admin.py,
        # which is how the two drifted apart in under a day.
        _silver_select_cols = silver_select_sql()
        _silver_pass2_sql = silver_pass2_sql

        try:
            # ── Step 1: Silver layer ──
            # Incremental rebuild scopes writes to changed orders + cascade
            # (other orders of the same buyers, since is_new_customer depends on
            # the buyer's MIN(order_date) across all their orders).
            #
            # Falls back to full rebuild when:
            #   (a) silver < 95% of orders — post-compact recovery, baseline rebuild
            #   (b) no changed_order_ids — manual trigger, startup, drift retry
            #   (c) cascade scope > 50% of orders — full is cheaper at that point
            silver_scope_ids: list[int] = []
            silver_affected_buyers: list[int] = []
            silver_mode = "full"

            async with self.connection() as conn:
                silver_count = conn.execute("SELECT COUNT(*) FROM silver_orders").fetchone()[0]
                orders_count = conn.execute("SELECT COUNT(*) FROM orders").fetchone()[0]
                silver_populated = orders_count > 0 and silver_count >= orders_count * 0.95

                if changed_order_ids and silver_populated:
                    ph = ",".join("?" * len(changed_order_ids))
                    ids = list(changed_order_ids)
                    # Affected buyers = NEW (orders) ∪ OLD (silver) — covers buyer reassignment
                    silver_affected_buyers = [r[0] for r in conn.execute(f"""
                        SELECT DISTINCT b FROM (
                            SELECT buyer_id AS b FROM orders
                            WHERE id IN ({ph}) AND buyer_id IS NOT NULL
                            UNION
                            SELECT buyer_id AS b FROM silver_orders
                            WHERE id IN ({ph}) AND buyer_id IS NOT NULL
                        )
                    """, ids + ids).fetchall()]
                    # Scope = changed_ids ∪ all orders of affected buyers (cascade).
                    # UNION over BOTH orders and silver_orders so DELETE catches
                    # orphan silver rows (orders deleted via admin/purge or H3
                    # bronze promotion). Without the silver-side branch, an
                    # orphan tied to an affected buyer would never get cleaned —
                    # full rebuild used to wipe these implicitly via DELETE *.
                    if silver_affected_buyers:
                        bph = ",".join("?" * len(silver_affected_buyers))
                        silver_scope_ids = [r[0] for r in conn.execute(f"""
                            SELECT id FROM orders        WHERE id IN ({ph})
                            UNION
                            SELECT id FROM orders        WHERE buyer_id IN ({bph})
                            UNION
                            SELECT id FROM silver_orders WHERE id IN ({ph})
                            UNION
                            SELECT id FROM silver_orders WHERE buyer_id IN ({bph})
                        """, ids + silver_affected_buyers + ids + silver_affected_buyers).fetchall()]
                    else:
                        # No buyer cascade. list(ids) covers both sides:
                        # DELETE removes silver rows for these ids (cleans orphans),
                        # INSERT only re-adds rows that exist in orders.
                        silver_scope_ids = list(ids)
                    # Guardrail: large scope → full rebuild is cheaper
                    if len(silver_scope_ids) > orders_count * 0.5:
                        silver_scope_ids = []
                        silver_affected_buyers = []
                    elif silver_scope_ids:
                        silver_mode = f"incremental_{len(silver_scope_ids)}"

                # Dates these rows occupy BEFORE they are rewritten. Gold is
                # rebuilt per date, and the dates a row leaves are not the dates
                # it arrives at: move an order from the 1st to the 5th and only
                # the 5th gets recomputed, so the 1st keeps the revenue too and
                # the money is counted twice. An order deleted upstream is worse
                # — after the DELETE its date is nowhere to be found, so Gold
                # keeps it until the next full rebuild.
                silver_old_dates: set = set()
                if silver_mode != "full" and silver_scope_ids:
                    sph = ",".join("?" * len(silver_scope_ids))
                    silver_old_dates = {
                        r[0] for r in conn.execute(
                            f"SELECT DISTINCT order_date FROM silver_orders "
                            f"WHERE id IN ({sph})",
                            silver_scope_ids,
                        ).fetchall()
                        if r[0] is not None
                    }

                conn.execute("BEGIN TRANSACTION")
                try:
                    if silver_mode != "full":
                        sph = ",".join("?" * len(silver_scope_ids))
                        conn.execute(
                            f"DELETE FROM silver_orders WHERE id IN ({sph})",
                            silver_scope_ids,
                        )
                        conn.execute(f"""
                            INSERT INTO silver_orders
                            SELECT {_silver_select_cols}
                            FROM orders o
                            WHERE o.id IN ({sph})
                        """, silver_scope_ids)
                        if silver_affected_buyers:
                            bph = ",".join("?" * len(silver_affected_buyers))
                            conn.execute(
                                _silver_pass2_sql(buyer_filter=bph),
                                silver_affected_buyers + silver_affected_buyers,
                            )
                    else:
                        conn.execute("DELETE FROM silver_orders")
                        conn.execute(f"""
                            INSERT INTO silver_orders
                            SELECT {_silver_select_cols}
                            FROM orders o
                        """)
                        conn.execute(_silver_pass2_sql())
                    conn.execute("COMMIT")
                except Exception:
                    try:
                        conn.execute("ROLLBACK")
                    except Exception:
                        pass
                    raise

            # ── Determine affected dates for incremental Gold rebuild ──
            # Gold follows Silver's decision. A full Silver rebuild with a
            # partial Gold one is how Gold ends up holding rows no Silver row
            # supports: the guardrail branch above and the post-compact
            # recovery path both rewrite every Silver row, and a Gold rebuild
            # scoped to the changed ids would leave every other date as it was.
            affected_dates: set[date] | None = None
            if silver_mode != "full" and silver_scope_ids:
                async with self.connection() as conn:
                    # Dates these rows occupy now. silver_scope_ids already
                    # carries the buyer cascade, so one scope drives Silver and
                    # Gold and the two cannot disagree about what changed.
                    sph = ",".join("?" * len(silver_scope_ids))
                    rows = conn.execute(f"""
                        SELECT DISTINCT order_date FROM silver_orders
                        WHERE id IN ({sph})
                    """, silver_scope_ids).fetchall()
                    new_dates = {r[0] for r in rows if r[0] is not None}

                # Where they were ∪ where they are.
                affected_dates = silver_old_dates | new_dates

                if not affected_dates:
                    affected_dates = None  # Fall back to full rebuild

            # gold_daily_products is the only rebuilt table that joins the
            # catalog, so a product or offer change widens that scope alone.
            # Everything else keeps whatever scope the orders gave it.
            catalog_dirty = await self._consume_catalog_dirty()
            gold_products_dates = None if catalog_dirty else affected_dates

            # ── Step 2: Gold daily revenue (lock acquired + released) ──
            # Single SQL template for both incremental and full rebuild
            _GOLD_REVENUE_SQL = "INSERT INTO gold_daily_revenue\n" + GOLD_REVENUE_SELECT_SQL

            gold_revenue_rows = 0
            async with self.connection() as conn:
                conn.execute("BEGIN TRANSACTION")
                try:
                    if affected_dates:
                        date_params = list(affected_dates)
                        date_placeholders = ",".join("?" * len(date_params))
                        conn.execute(f"DELETE FROM gold_daily_revenue WHERE date IN ({date_placeholders})", date_params)
                        conn.execute(_GOLD_REVENUE_SQL.format(date_filter=f"order_date IN ({date_placeholders})"), date_params)
                    else:
                        conn.execute("DELETE FROM gold_daily_revenue")
                        conn.execute(_GOLD_REVENUE_SQL.format(date_filter="order_date IS NOT NULL"))
                    gold_revenue_rows = conn.execute("SELECT COUNT(*) FROM gold_daily_revenue").fetchone()[0]
                    conn.execute("COMMIT")
                except Exception:
                    try:
                        conn.execute("ROLLBACK")
                    except Exception:
                        pass
                    raise

            # ── Step 3: Gold daily products (lock acquired + released) ──
            _GOLD_PRODUCTS_SQL = """
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
                  AND {date_filter}
                GROUP BY
                    s.order_date, s.sales_type, s.source_id,
                    op.product_id, op.name, p.brand, p.category_id,
                    c.name, parent_c.name
            """

            gold_products_rows = 0
            async with self.connection() as conn:
                conn.execute("BEGIN TRANSACTION")
                try:
                    if gold_products_dates:
                        date_params = list(gold_products_dates)
                        date_placeholders = ",".join("?" * len(date_params))
                        conn.execute(f"DELETE FROM gold_daily_products WHERE date IN ({date_placeholders})", date_params)
                        conn.execute(_GOLD_PRODUCTS_SQL.format(date_filter=f"s.order_date IN ({date_placeholders})"), date_params)
                    else:
                        conn.execute("DELETE FROM gold_daily_products")
                        conn.execute(_GOLD_PRODUCTS_SQL.format(date_filter="s.order_date IS NOT NULL"))
                    gold_products_rows = conn.execute("SELECT COUNT(*) FROM gold_daily_products").fetchone()[0]
                    conn.execute("COMMIT")
                except Exception:
                    try:
                        conn.execute("ROLLBACK")
                    except Exception:
                        pass
                    raise

            # ── Step 4: Validation + audit log ──
            needs_full_retry = False
            validation_alert: str | None = None
            # Names the condition for the throttle. The message text cannot:
            # it carries the checksums and the attempt number, so it differs
            # on every one of the 30 ticks an hour a standing failure produces.
            validation_alert_key: str | None = None
            partition_alert: str | None = None
            async with self.connection() as conn:
                # The known-types sum is written with a literal tuple because
                # DuckDB will not parameterise an IN list; the values come from
                # a module constant, never from a request.
                known_types_sql = ", ".join(f"'{t}'" for t in KNOWN_SALES_TYPES)
                checksums = conn.execute(f"""
                    SELECT
                        (SELECT COUNT(*) FROM orders) AS bronze_orders,
                        (SELECT COUNT(*) FROM silver_orders) AS silver_rows,
                        (SELECT COALESCE(SUM(grand_total), 0) FROM silver_orders
                         WHERE NOT is_return AND is_active_source) AS silver_revenue,
                        (SELECT COALESCE(SUM(revenue), 0) FROM gold_daily_revenue) AS gold_revenue,
                        (SELECT COALESCE(SUM(revenue), 0) FROM gold_daily_revenue
                         WHERE sales_type IN ({known_types_sql})) AS gold_revenue_known,
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
                gold_revenue_known = float(checksums[4])
                gold_product_revenue = float(checksums[5])
                bronze_product_revenue = float(checksums[6])

                checksum_match = abs(silver_revenue - gold_revenue) < 0.01
                product_checksum_match = abs(gold_product_revenue - bronze_product_revenue) < 0.01
                row_count_match = bronze_orders == silver_rows

                # ── Cell guard ──
                # Gold holds one row per (date, sales_type) and is built from
                # Silver by GROUP BY, so the two sets of cells must be equal.
                # The scalar checksums cannot see it when they are not: on the
                # three backups taken during the August incident there were
                # 100 → 90 → 84 mismatched cells and *zero* value mismatches —
                # every one was a cell Gold was missing, while the sums agreed.
                # Two anti-joins over an indexed pair of columns; ~7 ms.
                missing_cells = conn.execute("""
                    SELECT COUNT(*) FROM (
                        SELECT DISTINCT order_date AS d, sales_type AS t FROM silver_orders
                        EXCEPT
                        SELECT date, sales_type FROM gold_daily_revenue
                    )
                """).fetchone()[0]
                extra_cells = conn.execute("""
                    SELECT COUNT(*) FROM (
                        SELECT date AS d, sales_type AS t FROM gold_daily_revenue
                        EXCEPT
                        SELECT DISTINCT order_date, sales_type FROM silver_orders
                    )
                """).fetchone()[0]
                cells_match = (missing_cells == 0 and extra_cells == 0)
                if not cells_match:
                    logger.error(
                        f"Gold cell mismatch: {missing_cells} missing, "
                        f"{extra_cells} orphaned (silver_rows={silver_rows})"
                    )

                # A missing cell is a rebuild fault, and a rebuild is what
                # fixes it — so this joins the flag that drives the self-heal,
                # unlike the partition assertion below, which reports a
                # condition no rebuild can repair.
                validation_passed = (
                    checksum_match and row_count_match
                    and product_checksum_match and cells_match
                )

                # ── Partition assertion (P2-2) ──
                # The three checksums above sum every sales_type, while every
                # dashboard endpoint defaults to Query("retail"). A fourth
                # value — or a NULL — would therefore balance the checksums
                # perfectly and show up nowhere. Comparing the KNOWN types
                # against the Silver total is what notices; the subtraction
                # also catches NULL, which `NOT IN (...)` silently would not.
                #
                # Deliberately NOT part of validation_passed: that flag drives
                # mark_warehouse_dirty → a full rebuild every two minutes, and
                # a rebuild cannot fix a sales_type the code does not know
                # about. This reports; a human classifies.
                partition_exhaustive = abs(silver_revenue - gold_revenue_known) < 0.01
                if not partition_exhaustive:
                    unknown = conn.execute(f"""
                        SELECT sales_type, COALESCE(SUM(revenue), 0) AS revenue
                        FROM gold_daily_revenue
                        WHERE sales_type IS NULL
                           OR sales_type NOT IN ({known_types_sql})
                        GROUP BY sales_type
                        ORDER BY revenue DESC
                    """).fetchall()
                    detail = ", ".join(
                        f"{row[0] if row[0] is not None else 'NULL'}=₴{float(row[1]):,.2f}"
                        for row in unknown
                    ) or "no rows — Gold is short of Silver"
                    logger.error(
                        f"sales_type partition not exhaustive: silver={silver_revenue:.2f} "
                        f"known-types gold={gold_revenue_known:.2f}; {detail}"
                    )
                    partition_alert = (
                        "🚨 *Unknown `sales_type` in Gold*\n"
                        f"Silver revenue {silver_revenue:,.2f} vs "
                        f"{gold_revenue_known:,.2f} across "
                        f"{', '.join(KNOWN_SALES_TYPES)}.\n"
                        f"Outside the partition: {detail}\n\n"
                        "Revenue in an unknown sales_type reaches no page — "
                        "every endpoint defaults to retail."
                    )

                if not validation_passed:
                    # Count consecutive PRIOR failures (this run's row not yet
                    # written). Keep self-healing (mark dirty → full retry on the
                    # next scheduler tick) up to MAX_VALIDATION_RETRIES, then STOP
                    # the loop and escalate LOUDLY. The old code went silently idle
                    # on the 2nd consecutive failure, leaving the Gold layer serving
                    # wrong revenue until the weekly full_sync, with no alert.
                    #
                    # Errored rows are skipped rather than counted: an OOM says
                    # nothing about whether Gold reconciles, and on 2026-08-02 a
                    # storm of seven of them spent the entire budget before the
                    # truncated Gold it caused was ever seen — the self-heal was
                    # disarmed by the very event it exists to recover from.
                    prior_failures = conn.execute(
                        "SELECT validation_passed FROM warehouse_refreshes "
                        "WHERE error IS NULL "
                        "ORDER BY refreshed_at DESC LIMIT ?",
                        [MAX_VALIDATION_RETRIES + 1],
                    ).fetchall()
                    consecutive = 0
                    for (vp,) in prior_failures:
                        if vp is False:
                            consecutive += 1
                        else:
                            break

                    detail = (
                        f"rows={bronze_orders}→{silver_rows} (match={row_count_match}), "
                        f"cells: {missing_cells} missing/{extra_cells} orphaned, "
                        f"revenue={silver_revenue:.2f}→{gold_revenue:.2f} (match={checksum_match}), "
                        f"product_revenue={bronze_product_revenue:.2f}→{gold_product_revenue:.2f} "
                        f"(match={product_checksum_match})"
                    )

                    if consecutive < MAX_VALIDATION_RETRIES:
                        logger.warning(
                            f"Warehouse validation failed — scheduling full retry "
                            f"(consecutive={consecutive}): {detail}"
                        )
                        needs_full_retry = True
                        validation_alert = (
                            "⚠️ Warehouse validation failed — full retry scheduled "
                            f"(attempt {consecutive + 1}/{MAX_VALIDATION_RETRIES}).\n{detail}"
                        )
                        validation_alert_key = "warehouse:validation_retrying"
                    elif self._claim_stuck_rebuild_slot():
                        hours = STUCK_REBUILD_COOLDOWN_SECONDS // 3600
                        logger.error(
                            f"Warehouse validation failed {consecutive}x consecutively — "
                            f"per-tick retry stopped; attempting one full rebuild: {detail}"
                        )
                        needs_full_retry = True
                        validation_alert = (
                            f"🚨 CRITICAL: Warehouse validation failed {consecutive}x in a "
                            "row. The Gold layer may be serving WRONG revenue. Attempting a "
                            f"full rebuild; if this alert returns in {hours}h the cause is "
                            f"not transient and needs a human.\n{detail}"
                        )
                        validation_alert_key = "warehouse:validation_rebuilding"
                    else:
                        logger.error(
                            f"Warehouse validation failed {consecutive}x consecutively — "
                            f"full rebuild already attempted this period: {detail}"
                        )
                        validation_alert = (
                            f"🚨 CRITICAL: Warehouse validation failed {consecutive}x in a "
                            "row and a full rebuild did not fix it — the Gold layer may be "
                            f"serving WRONG revenue. Manual fix needed.\n{detail}"
                        )
                        validation_alert_key = "warehouse:validation_unfixed"

                duration_ms = (time.perf_counter() - start_time) * 1000

                _audit_values = [
                    trigger, round(duration_ms, 2),
                    bronze_orders, silver_rows, gold_revenue_rows, gold_products_rows,
                    round(silver_revenue, 2), round(gold_revenue, 2),
                    checksum_match, validation_passed, None,
                ]
                _audit_cols = (
                    "refreshed_at, trigger, duration_ms, bronze_orders, silver_rows, "
                    "gold_revenue_rows, gold_products_rows, silver_revenue_checksum, "
                    "gold_revenue_checksum, checksum_match, validation_passed, error"
                )
                try:
                    conn.execute(
                        f"INSERT INTO warehouse_refreshes ({_audit_cols}, silver_mode) "
                        f"VALUES (CURRENT_TIMESTAMP, {', '.join('?' * len(_audit_values))}, ?)",
                        _audit_values + [f"{silver_mode}{'+catalog' if catalog_dirty else ''}"],
                    )
                except Exception:
                    # The column may not exist yet: a deploy can reach this line
                    # before its migration lands, and losing the audit row is a
                    # worse outcome than losing one field of it. This is the
                    # 2026-08-09 failure mode written down as a fallback rather
                    # than left to be rediscovered.
                    conn.execute(
                        f"INSERT INTO warehouse_refreshes ({_audit_cols}) "
                        f"VALUES (CURRENT_TIMESTAMP, {', '.join('?' * len(_audit_values))})",
                        _audit_values,
                    )

            # Mark dirty OUTSIDE the connection block to avoid deadlock
            # (mark_warehouse_dirty also acquires self._lock via self.connection())
            if needs_full_retry:
                await self.mark_warehouse_dirty(None)
            if validation_alert:
                await self._send_warehouse_alert(validation_alert, validation_alert_key)
            if partition_alert:
                await self._send_warehouse_alert(
                    partition_alert, "warehouse:sales_type_partition",
                )

            incremental_info = ""
            if affected_dates:
                incremental_info = f", gold_dates={len(affected_dates)}"

            logger.info(
                f"Warehouse layers refreshed ({trigger}): "
                f"silver={silver_rows} ({silver_mode}), gold_rev={gold_revenue_rows}, "
                f"gold_prod={gold_products_rows}, "
                f"duration={duration_ms:.0f}ms, valid={validation_passed}"
                f"{incremental_info}"
            )

            # ── UTM/Traffic layers (after main refresh completes) ──
            utm_count = 0
            traffic_rows = 0
            try:
                utm_order_ids = await self.refresh_utm_silver_layer()
                utm_count = len(utm_order_ids)

                # Rebuild the dates that moved, not all 987 of them.
                #
                # This passed affected_dates=None unconditionally — a full
                # DELETE+INSERT of gold_daily_traffic on every one of ~240
                # refreshes a day. DuckDB cannot reclaim what that leaves
                # behind while a writer is live: vacuuming deletes needs an
                # exclusive lock a 2-minute refresh loop never yields, and
                # `vacuum_rebuild_indexes` is off by default so an indexed
                # table is skipped anyway. gold_daily_traffic reached 3.86M
                # stored rows behind 5 781 live ones — 667x amplification,
                # and the single largest contributor to the ~90 MB a day the
                # database file grew between weekly compactions.
                #
                # The old comment was right that UTM parsing can touch dates
                # outside affected_dates. The answer is to ask which ones
                # rather than to avoid the question: refresh_utm_silver_layer
                # now returns the ids it parsed, so the two sets union.
                traffic_dates = await self._traffic_rebuild_dates(
                    affected_dates, utm_order_ids,
                )
                if traffic_dates is None or traffic_dates:
                    traffic_rows = await self.refresh_traffic_gold_layer(
                        affected_dates=traffic_dates,
                    )
                else:
                    # Nothing moved and nothing parsed — the layer is already
                    # right. Report its size without rewriting it.
                    async with self.connection() as conn:
                        traffic_rows = conn.execute(
                            "SELECT COUNT(*) FROM gold_daily_traffic"
                        ).fetchone()[0]
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

            # A5-1 self-heal: an exception here can mean a DURABLE cross-layer
            # inconsistency on disk — e.g. the Gold-revenue transaction committed
            # (Step 2) but the Gold-products transaction (Step 3) threw, or the
            # process was OOM-killed between them. Each step is its own
            # BEGIN/COMMIT, so the layers are left from different points in time
            # and validation never ran. Mark dirty so the next scheduler tick does
            # a full rebuild and reconciles, bounded by the same consecutive-failure
            # escalation as validation failures (the audit row above is already
            # counted). Previously this path returned without marking dirty, so the
            # inconsistency persisted silently until the weekly full_sync.
            try:
                consecutive = await self._count_consecutive_refresh_failures()
                if consecutive <= MAX_VALIDATION_RETRIES:
                    await self.mark_warehouse_dirty(None)
                    await self._send_warehouse_alert(
                        f"⚠️ Warehouse refresh errored — full rebuild scheduled to "
                        f"self-heal (attempt {consecutive}/{MAX_VALIDATION_RETRIES}). "
                        f"Gold layers may be cross-inconsistent until then.\n{error_msg}",
                        "warehouse:refresh_errored",
                    )
                else:
                    await self._send_warehouse_alert(
                        f"🚨 CRITICAL: Warehouse refresh errored {consecutive}x in a row. "
                        f"Auto-retry stopped — Gold layers may be cross-inconsistent. "
                        f"Manual fix needed.\n{error_msg}",
                        "warehouse:refresh_errored_exhausted",
                    )
            except Exception as heal_err:
                logger.error(f"Failed to schedule warehouse self-heal: {heal_err}")

            return {
                "status": "error",
                "trigger": trigger,
                "duration_ms": round(duration_ms, 2),
                "error": error_msg,
            }

    async def _traffic_rebuild_dates(
        self,
        affected_dates: "set[date] | None",
        utm_order_ids: "set[int]",
    ) -> "set[date] | None":
        """Which dates gold_daily_traffic must be rebuilt for.

        `None` means every date. A non-empty set means exactly those. An
        **empty** set means none at all — and the caller must skip the rebuild
        rather than pass it on, because refresh_traffic_gold_layer reads a
        falsy value as "rebuild everything".
        """
        if affected_dates is None:
            # Silver was rebuilt whole, so Gold follows it whole. Same rule
            # the revenue and products layers already obey above.
            return None

        dates = set(affected_dates)
        if not utm_order_ids:
            return dates
        if len(utm_order_ids) > UTM_DATE_LOOKUP_LIMIT:
            return None

        async with self.connection() as conn:
            ids = list(utm_order_ids)
            ph = ",".join("?" * len(ids))
            rows = conn.execute(
                f"SELECT DISTINCT order_date FROM silver_orders WHERE id IN ({ph})",
                ids,
            ).fetchall()
        return dates | {r[0] for r in rows if r[0] is not None}

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

    async def mark_warehouse_dirty(self, changed_order_ids: list[int] | None = None) -> None:
        """Set dirty flag so the warehouse refresh job picks it up."""
        import json
        value = json.dumps(changed_order_ids) if changed_order_ids else "full"
        async with self.connection() as conn:
            # Merge with existing dirty state (another sync may have run before refresh)
            existing = conn.execute(
                "SELECT value FROM sync_metadata WHERE key = 'warehouse_dirty'"
            ).fetchone()
            if existing and existing[0]:
                old = existing[0]
                if old == "full" or value == "full":
                    value = "full"
                else:
                    # Merge ID lists
                    old_ids = json.loads(old)
                    new_ids = changed_order_ids or []
                    merged = list(set(old_ids + new_ids))
                    value = json.dumps(merged)
            conn.execute("""
                INSERT OR REPLACE INTO sync_metadata (key, value, updated_at)
                VALUES ('warehouse_dirty', ?, CURRENT_TIMESTAMP)
            """, [value])

    async def mark_catalog_dirty(self) -> None:
        """A product, offer or category changed — not an order.

        Kept apart from `mark_warehouse_dirty` because the two mean different
        things and only one of the four rebuilt tables cares:

            silver_orders        <- orders                     no catalog
            gold_daily_revenue   <- silver_orders              no catalog
            gold_daily_traffic   <- silver + silver_order_utm  no catalog
            gold_daily_products  <- silver + order_products
                                    + products + categories    YES

        A rename therefore has to widen exactly one scope. Marking the whole
        warehouse dirty instead rebuilt all four, and silver_orders — which has
        no product column at all — was the second-largest contributor to the
        file growth that forced a weekly stop-the-world compaction.
        """
        async with self.connection() as conn:
            conn.execute("""
                INSERT OR REPLACE INTO sync_metadata (key, value, updated_at)
                VALUES ('warehouse_catalog_dirty', '1', CURRENT_TIMESTAMP)
            """)

    async def _consume_catalog_dirty(self) -> bool:
        """Read and clear the catalog flag.

        Consumed inside `refresh_warehouse_layers` rather than alongside the
        order flag so that every caller honours it — the scheduler, a manual
        trigger and the admin endpoint alike — without each having to know it
        exists.
        """
        async with self.connection() as conn:
            row = conn.execute(
                "SELECT value FROM sync_metadata WHERE key = 'warehouse_catalog_dirty'"
            ).fetchone()
            if not row or not row[0]:
                return False
            conn.execute(
                "DELETE FROM sync_metadata WHERE key = 'warehouse_catalog_dirty'"
            )
            return True

    async def consume_warehouse_dirty(self) -> tuple[bool, list[int] | None]:
        """Atomically read and clear the dirty flag. Returns (is_dirty, changed_ids_or_None)."""
        import json
        async with self.connection() as conn:
            result = conn.execute(
                "SELECT value FROM sync_metadata WHERE key = 'warehouse_dirty'"
            ).fetchone()
            if not result or not result[0]:
                return False, None
            conn.execute("DELETE FROM sync_metadata WHERE key = 'warehouse_dirty'")
            value = result[0]
            if value == "full":
                return True, None
            return True, json.loads(value)

    async def find_order_id_gaps(self, limit: int = 200) -> List[int]:
        """Order ids missing from our copy, found without asking KeyCRM.

        KeyCRM issues order ids as a dense sequence, so every hole between our
        lowest and highest id is an order we never stored. That makes finding
        them free: 1 660 holes existed as of 2026-08, and a full-history
        comparison against the API confirmed 1 616 of them were real orders.

        Ids already known to be absent upstream are skipped, so the list drains
        to empty instead of cycling forever.
        """
        async with self.connection() as conn:
            rows = conn.execute("""
                WITH bounds AS (SELECT MIN(id) lo, MAX(id) hi FROM orders)
                SELECT g.id FROM bounds, generate_series(bounds.lo, bounds.hi) AS g(id)
                WHERE NOT EXISTS (SELECT 1 FROM orders o WHERE o.id = g.id)
                  AND NOT EXISTS (SELECT 1 FROM order_backfill_misses m
                                  WHERE m.order_id = g.id)
                ORDER BY g.id
                LIMIT ?
            """, [int(limit)]).fetchall()
        return [int(r[0]) for r in rows]

    async def record_backfill_misses(self, misses: "Dict[int, str]") -> int:
        """Remember ids KeyCRM could not supply, so they are not retried."""
        if not misses:
            return 0
        async with self.connection() as conn:
            conn.executemany(
                "INSERT OR REPLACE INTO order_backfill_misses "
                "(order_id, checked_at, reason) VALUES (?, CURRENT_TIMESTAMP, ?)",
                [(int(oid), str(reason)[:200]) for oid, reason in misses.items()],
            )
        return len(misses)

    def _claim_stuck_rebuild_slot(self) -> bool:
        """Take the one full-rebuild attempt allowed per cooldown, if it is free.

        Kept in process memory rather than in the database, so that a restart
        re-arms it. A deploy or a container restart is the moment someone is
        most likely to have just changed something that makes the rebuild work —
        raising the memory ceiling, for instance — and making them wait out the
        cooldown to find out would be perverse.
        """
        now = time.monotonic()
        last = self._last_stuck_rebuild
        if last is not None and (now - last) < STUCK_REBUILD_COOLDOWN_SECONDS:
            return False
        self._last_stuck_rebuild = now
        return True

    async def _count_consecutive_refresh_failures(self) -> int:
        """How many of the most-recent refreshes threw, in a row.

        Counts *errored* rows only. Validation failures are a separate budget
        with its own counter — conflating them let a burst of one exhaust the
        allowance meant for the other. Used to bound the self-heal retry loop so
        a deterministic failure doesn't spin a full rebuild every tick forever.
        """
        async with self.connection() as conn:
            rows = conn.execute(
                "SELECT error FROM warehouse_refreshes "
                "ORDER BY refreshed_at DESC LIMIT ?",
                [MAX_VALIDATION_RETRIES + 2],
            ).fetchall()
        consecutive = 0
        for (err,) in rows:
            if err is not None:
                consecutive += 1
            else:
                break
        return consecutive

    async def _send_warehouse_alert(
        self, message: str, key: "str | None" = None,
    ) -> None:
        """Push a warehouse-health alert to admins (best-effort, never raises).

        `key` names the condition so the throttle can recognise a repeat. Every
        message here embeds live checksums and an attempt counter, so without
        one no two are ever the same string and the throttle never fires.

        Mirrors scheduler._send_bronze_alert: lazy import to avoid a circular
        dependency on bot.main at module load.
        """
        try:
            from bot.main import send_admin_message
            await send_admin_message(message, key=key)
        except Exception as e:
            logger.warning(f"Failed to send warehouse alert: {e}")

    async def backup_database(
        self, dest_dir: "Path | str | None" = None, keep: int = 2,
    ) -> Dict[str, Any]:
        """Create a consistent on-disk backup of the DuckDB file (A9-1).

        DuckDB has no online/hot backup. We hold the store lock (serializing
        with every refresh/sync write), CHECKPOINT to fold the WAL into the main
        file, then copy the file while the lock is STILL held so no writer can
        run mid-copy — yielding a byte-consistent snapshot. The copy is offloaded
        to the executor so the event loop is not blocked. The copy is then
        validated read-only before older backups are pruned.

        Holds the global lock for the copy duration (tens of seconds on a multi-GB
        DB) → schedule at a low-traffic hour. For real DR the backup dir must be
        replicated OFF-HOST (rclone/S3) — see docs/backup_runbook.md.

        Protects data that is NOT recoverable from KeyCRM: revenue_goals,
        manual_expenses, users/roles, user_preferences, celebrated_milestones.
        """
        import os
        import shutil
        import time

        src = Path(self.db_path)
        dest = Path(dest_dir) if dest_dir else src.parent / "backups"
        dest.mkdir(parents=True, exist_ok=True)
        stamp = datetime.now(DEFAULT_TZ).strftime("%Y%m%d-%H%M%S")
        final_path = dest / f"{src.stem}-{stamp}.duckdb"
        tmp_path = dest / f".{src.stem}-{stamp}.duckdb.tmp"

        # Disk-space guard: need room for a full second copy (+10% margin).
        src_size = src.stat().st_size
        free = shutil.disk_usage(dest).free
        if free < src_size * 1.1:
            msg = (f"Backup aborted: only {free/1e9:.1f}GB free, need "
                   f"~{src_size*1.1/1e9:.1f}GB for {src.name}")
            logger.error(msg)
            await self._send_warehouse_alert(
                f"🚨 DB backup FAILED — {msg}", "warehouse:backup_failed",
            )
            return {"status": "error", "error": msg}

        t0 = time.perf_counter()
        try:
            async with self.connection() as conn:
                conn.execute("CHECKPOINT")
                loop = asyncio.get_running_loop()
                # Copy with the lock held → no concurrent writer → consistent.
                await loop.run_in_executor(
                    self._executor, shutil.copy2, str(src), str(tmp_path)
                )

            # Validate the copy read-only (outside the lock).
            def _validate() -> int:
                con = duckdb.connect(str(tmp_path), read_only=True)
                try:
                    return con.execute("SELECT COUNT(*) FROM orders").fetchone()[0]
                finally:
                    con.close()

            loop = asyncio.get_running_loop()
            orders_in_backup = await loop.run_in_executor(self._executor, _validate)
            if not orders_in_backup or orders_in_backup <= 0:
                raise ValueError(f"backup validation failed: orders={orders_in_backup}")

            os.replace(tmp_path, final_path)
            duration = time.perf_counter() - t0

            # Retain only the newest `keep` backups.
            backups = sorted(dest.glob(f"{src.stem}-*.duckdb"))
            removed = 0
            for old in backups[:-keep] if keep > 0 else []:
                try:
                    old.unlink()
                    removed += 1
                except OSError:
                    pass

            logger.info(
                f"DB backup OK: {final_path.name} ({src_size/1e9:.2f}GB, "
                f"orders={orders_in_backup}, {duration:.1f}s, pruned {removed})"
            )
            return {
                "status": "success",
                "path": str(final_path),
                "size_bytes": src_size,
                "orders": orders_in_backup,
                "duration_s": round(duration, 1),
                "pruned": removed,
            }
        except Exception as e:
            logger.error(f"DB backup failed: {e}", exc_info=True)
            try:
                if tmp_path.exists():
                    tmp_path.unlink()
            except OSError:
                pass
            await self._send_warehouse_alert(
                f"🚨 DB backup FAILED: {e}", "warehouse:backup_failed",
            )
            return {"status": "error", "error": str(e)}

    async def get_order_summaries_by_date(
        self, start_date: str, end_date: str,
    ) -> dict:
        """Get order ID → (status_id, grand_total) grouped by date (Kyiv TZ).

        Returns dict[date, dict[int, dict]] where outer key is date,
        inner key is order_id, inner value has status_id and grand_total.
        """
        from collections import defaultdict

        async with self.connection() as conn:
            rows = conn.execute(f"""
                SELECT {_date_in_kyiv('ordered_at')} AS d, id, status_id, grand_total
                FROM orders
                WHERE {_date_in_kyiv('ordered_at')} BETWEEN ? AND ?
            """, [start_date, end_date]).fetchall()

        result: dict = defaultdict(dict)
        for d, oid, status_id, grand_total in rows:
            result[d][oid] = {
                "status_id": status_id,
                "grand_total": float(grand_total),
            }
        return dict(result)

    async def log_reconciliation(self, check_date: str, api_count: int, db_count: int) -> dict:
        """Log reconciliation result and return the entry."""
        discrepancy = abs(api_count - db_count)
        discrepancy_pct = round((discrepancy / api_count * 100) if api_count > 0 else 0, 2)
        status = "ok" if discrepancy_pct <= 1.0 else "drift"

        async with self.connection() as conn:
            conn.execute("""
                INSERT INTO reconciliation_log (check_date, api_count, db_count, discrepancy, discrepancy_pct, status)
                VALUES (?, ?, ?, ?, ?, ?)
            """, [check_date, api_count, db_count, discrepancy, discrepancy_pct, status])

        return {
            "check_date": check_date,
            "api_count": api_count,
            "db_count": db_count,
            "discrepancy": discrepancy,
            "discrepancy_pct": discrepancy_pct,
            "status": status,
        }

    async def upsert_orders(
        self,
        orders: List[Dict[str, Any]],
        force_update: bool = False,
        skip_products: bool = False,
    ) -> "UpsertResult":
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
            skip_products: If True, skip product deletion/insertion.
                          Use this for status refresh where only order-level
                          fields (status_id, etc.) change — avoids OOM on
                          executemany for ~2000 orders worth of product rows.

        Returns:
            An UpsertResult. Read `.changed_ids` to find out what actually
            moved; `.count` includes rows that were already correct, so
            driving a rebuild from it rebuilds the world every cycle.
        """
        if not orders:
            return UpsertResult(count=0, changed_ids=[], skipped_unchanged=0, failed=0)

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
                "status_group_id": order.status_group_id,
                "grand_total": float(order.grand_total),
                "ordered_at": order.ordered_at,  # Keep as datetime
                "created_at": order.created_at,  # Keep as datetime
                "updated_at": order.updated_at,  # Keep as datetime
                "buyer_id": order.buyer.id if order.buyer else None,
                "manager_id": order.manager.id if order.manager else None,
                "manager_comment": order.manager_comment,
                "promocode": order.promocode,
            })

            # Build product rows (skip for status-only refresh to avoid OOM)
            if not skip_products:
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
            return UpsertResult(count=0, changed_ids=[], skipped_unchanged=0, failed=0)

        # Create DataFrame for orders (products use executemany for simplicity)
        # Deduplicate by id - API can return same order twice in paginated responses
        orders_df = pd.DataFrame(order_rows).drop_duplicates(subset=["id"], keep="last")

        # Convert datetime columns to proper pandas datetime type for DuckDB
        for col in ["ordered_at", "created_at", "updated_at"]:
            orders_df[col] = pd.to_datetime(orders_df[col], utc=True)

        # Ensure nullable integer columns use Int64 so None stays pd.NA, not float NaN
        # (DuckDB 1.5+ rejects float NaN → INT32 in executemany)
        for col in ["source_id", "status_id", "status_group_id", "buyer_id", "manager_id"]:
            orders_df[col] = orders_df[col].astype("Int64")

        # Ensure nullable string columns are proper type for DuckDB
        orders_df["manager_comment"] = orders_df["manager_comment"].astype(pd.StringDtype())
        orders_df["promocode"] = orders_df["promocode"].astype(pd.StringDtype())

        # Get order IDs for use in queries (avoids DuckDB FK bug with subqueries)
        order_ids = orders_df["id"].tolist()

        # Helper: pd.NA → None for DuckDB compatibility
        def _int_or_none(v):
            return None if pd.isna(v) else int(v)

        insert_rows = []
        for _, row in orders_df.iterrows():
            insert_rows.append((
                int(row["id"]), _int_or_none(row["source_id"]),
                _int_or_none(row["status_id"]),
                _int_or_none(row["status_group_id"]),
                float(row["grand_total"]),
                row["ordered_at"], row["created_at"], row["updated_at"],
                _int_or_none(row["buyer_id"]),
                _int_or_none(row["manager_id"]),
                row["manager_comment"],
                row["promocode"],
            ))

        async with self.connection() as conn:
            # 1. Upsert orders one-by-one in autocommit mode with row-level fault isolation.
            # DuckDB 1.5: ON CONFLICT inside explicit transactions is broken
            # (PK violation poisons transaction). Autocommit per-row works for most rows,
            # but occasional rows hit write-write conflicts. Skip them and continue —
            # the 24h sync_from buffer will retry on the next cycle.
            # P2-1: Explicit SELECT→UPDATE/INSERT instead of ON CONFLICT.
            # DuckDB 1.5.x has MVCC bugs in ON CONFLICT that cause write-write
            # conflicts on "poisoned" rows. Explicit check avoids that code path.
            insert_sql = """
                INSERT INTO orders (id, source_id, status_id, status_group_id,
                                   grand_total, ordered_at, created_at, updated_at,
                                   buyer_id, manager_id, manager_comment, promocode, synced_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, now())
            """
            # manager_comment carries UTM attribution data. COALESCE keeps the
            # stored value when the API payload has it as null — otherwise a
            # re-sync of an order whose payload omits the field would destroy
            # attribution that backfill restored (loss is unrecoverable: the
            # UTM silver layer parses from this column).
            update_sql = """
                UPDATE orders SET
                    source_id = ?, status_id = ?, status_group_id = ?, grand_total = ?,
                    ordered_at = ?, created_at = ?, updated_at = ?,
                    buyer_id = ?, manager_id = ?,
                    manager_comment = COALESCE(?, manager_comment),
                    promocode = ?, synced_at = now()
                WHERE id = ?
            """

            # Batch-fetch (id, updated_at) for every existing target row so the
            # skip-decider can compare timestamps without per-row SELECTs.
            # See core.upsert_decider.should_update_order for the contract.
            from core.upsert_decider import should_update_order

            all_ids = [int(p[0]) for p in insert_rows]
            placeholders = ",".join("?" * len(all_ids))
            existing_rows = conn.execute(
                f"SELECT id, updated_at FROM orders WHERE id IN ({placeholders})",
                all_ids,
            ).fetchall()
            existing: Dict[int, Any] = {int(r[0]): r[1] for r in existing_rows}

            success_ids: List[int] = []
            updated_ids: List[int] = []
            skipped_count = 0
            failed: List[tuple] = []  # (order_id, error_str)
            for params in insert_rows:
                order_id = int(params[0])
                incoming_updated_at = params[7]  # tuple index matches insert_sql
                existing_updated_at = existing.get(order_id) if order_id in existing else None

                try:
                    if order_id in existing:
                        if not should_update_order(
                            existing_updated_at, incoming_updated_at,
                            force=force_update,
                        ):
                            # Identity write — same updated_at, nothing to do.
                            # Counts toward success because the row IS in the
                            # desired state (just not freshly written).
                            success_ids.append(order_id)
                            skipped_count += 1
                            continue
                        conn.execute(update_sql, [
                            params[1], params[2], params[3], params[4], params[5],
                            params[6], params[7], params[8], params[9], params[10],
                            params[11],
                            order_id,
                        ])
                        updated_ids.append(order_id)
                    else:
                        conn.execute(insert_sql, list(params))
                        updated_ids.append(order_id)
                    success_ids.append(order_id)
                except (duckdb.TransactionException, duckdb.ConstraintException) as e:
                    failed.append((order_id, str(e)))
                    try:
                        conn.execute("ROLLBACK")
                    except Exception:
                        pass

            # 2. Delete stale products ONLY for rows we actually wrote.
            # Skipped rows (skip-if-unchanged) keep their existing products
            # untouched — their order_products are already correct because the
            # order itself didn't change. This was the bulk of the 1440x churn.
            # Failed rows likewise keep their existing products for consistency.
            if not skip_products and updated_ids:
                placeholders = ",".join("?" * len(updated_ids))
                conn.execute(
                    f"DELETE FROM order_products WHERE order_id IN ({placeholders})",
                    updated_ids,
                )

            # 3. Insert products for actually-updated orders only
            if not skip_products and product_rows and updated_ids:
                updated_set = set(updated_ids)
                products_to_insert = [p for p in product_rows if p["order_id"] in updated_set]
                if products_to_insert:
                    conn.execute("BEGIN TRANSACTION")
                    try:
                        conn.executemany("""
                            INSERT OR REPLACE INTO order_products (id, order_id, product_id, name, quantity, price_sold)
                            VALUES (?, ?, ?, ?, ?, ?)
                        """, [
                            (p["id"], p["order_id"], p["product_id"], p["name"], p["quantity"], p["price_sold"])
                            for p in products_to_insert
                        ])
                        conn.execute("COMMIT")
                    except Exception:
                        try:
                            conn.execute("ROLLBACK")
                        except Exception:
                            pass
                        raise

            if failed:
                sample = ", ".join(str(oid) for oid, _ in failed[:5])
                logger.error(
                    f"Skipped {len(failed)} poisoned orders (first 5: {sample}). "
                    f"First error: {failed[0][1]}"
                )

            count = len(success_ids)
            n_written = len(updated_ids)
            logger.info(
                f"Upserted {count}/{len(insert_rows)} orders to DuckDB "
                f"(written={n_written}, skipped_unchanged={skipped_count})"
            )
            return UpsertResult(
                count=count,
                changed_ids=updated_ids,
                skipped_unchanged=skipped_count,
                failed=len(failed),
            )

    # ─── H3: bronze order events (append-only audit log) ───────────────────────

    async def append_bronze_events(
        self,
        orders: List[Dict[str, Any]],
        source: str,
    ) -> int:
        """Append raw KeyCRM order payloads to bronze_order_events.

        Pure INSERT — by construction cannot write-write conflict on hot keys.
        Phase 1 shadow write; Phase 3 becomes the only ingest path.

        Args:
            orders: Raw KeyCRM order dicts. Payloads with no numeric `id`
                field are silently skipped.
            source: Origin tag, e.g. 'sync_delta', 'sync_status',
                'reconciliation', 'manual'. Used for audit and replay filtering.

        Returns:
            Number of events written.
        """
        if not orders:
            return 0

        rows = []
        for o in orders:
            oid = o.get("id")
            if oid is None:
                continue
            try:
                oid_int = int(oid)
            except (TypeError, ValueError):
                continue
            rows.append((oid_int, json.dumps(o, default=str, ensure_ascii=False), source))

        if not rows:
            return 0

        async with self.connection() as conn:
            conn.executemany(
                "INSERT INTO bronze_order_events (order_id, payload, source) "
                "VALUES (?, ?, ?)",
                rows,
            )
        return len(rows)

    async def get_bronze_stats(self) -> Dict[str, Any]:
        """Return health metrics for bronze_order_events.

        Fields:
            total: total rows ever written (within retention window).
            unprocessed: rows with processed_at IS NULL.
            oldest_unprocessed_age_s: seconds since the oldest unprocessed
                event was written (None if queue empty).
            latest_event_ts: ISO timestamp of newest event (None if empty).
        """
        async with self.connection() as conn:
            row = conn.execute("""
                SELECT
                    COUNT(*)                                                  AS total,
                    COUNT(*) FILTER (WHERE processed_at IS NULL)              AS unprocessed,
                    MIN(event_ts) FILTER (WHERE processed_at IS NULL)         AS oldest_unprocessed,
                    MAX(event_ts)                                             AS latest
                FROM bronze_order_events
            """).fetchone()

        total, unprocessed, oldest_unprocessed, latest = row or (0, 0, None, None)
        age_s: Optional[float] = None
        if oldest_unprocessed is not None:
            now = datetime.now(oldest_unprocessed.tzinfo) if oldest_unprocessed.tzinfo else datetime.utcnow()
            age_s = max(0.0, (now - oldest_unprocessed).total_seconds())

        return {
            "total": int(total or 0),
            "unprocessed": int(unprocessed or 0),
            "oldest_unprocessed_age_s": age_s,
            "latest_event_ts": latest.isoformat() if latest else None,
        }

    async def backfill_bronze_from_orders(self, batch_size: int = 5000) -> Dict[str, Any]:
        """Backfill bronze_order_events from existing orders table.

        One-time operation to populate bronze with all historical orders so
        that `orders` can be fully rebuilt from bronze alone. Skips order_ids
        that already have events.

        Returns:
            Dict with inserted count, skipped (already in bronze), total orders.
        """
        async with self.connection() as conn:
            # 1. Get all order IDs already in bronze to avoid duplicates
            existing = set(r[0] for r in conn.execute(
                "SELECT DISTINCT order_id FROM bronze_order_events WHERE source = 'backfill'"
            ).fetchall())

            # 2. Read all orders and build JSON payloads
            rows = conn.execute("""
                SELECT id, source_id, status_id, grand_total,
                       ordered_at, created_at, updated_at,
                       buyer_id, manager_id, manager_comment, promocode
                FROM orders
                ORDER BY id
            """).fetchall()

            total = len(rows)
            insert_rows = []
            skipped = 0

            for r in rows:
                oid = r[0]
                if oid in existing:
                    skipped += 1
                    continue

                payload = {
                    "id": oid,
                    "source_id": r[1],
                    "status_id": r[2],
                    "grand_total": float(r[3]) if r[3] is not None else 0,
                    "ordered_at": r[4].isoformat() if r[4] else None,
                    "created_at": r[5].isoformat() if r[5] else None,
                    "updated_at": r[6].isoformat() if r[6] else None,
                    "buyer_id": r[7],
                    "manager_id": r[8],
                    "manager_comment": r[9],
                    "promocode": r[10],
                }
                insert_rows.append((
                    oid,
                    json.dumps(payload, default=str, ensure_ascii=False),
                    "backfill",
                ))

            # 3. Batch insert
            if insert_rows:
                for i in range(0, len(insert_rows), batch_size):
                    chunk = insert_rows[i:i + batch_size]
                    conn.executemany(
                        "INSERT INTO bronze_order_events (order_id, payload, source) "
                        "VALUES (?, ?, ?)",
                        chunk,
                    )

            return {
                "total_orders": total,
                "inserted": len(insert_rows),
                "skipped_existing": skipped,
            }

    async def promote_bronze_to_orders(self, batch_size: int = 2000) -> Dict[str, Any]:
        """Promote unprocessed bronze events to the real orders table.

        The production promotion path, and since the H3 cutover the only one:
        the Phase 2 shadow table and its differ were removed once this took
        over writing to `orders` directly.

        Single-transaction batch: for each order_id takes the latest event,
        parses JSON payload, DELETE+INSERT into orders, marks processed_at.

        Returns:
            Dict with promoted count, skipped, errors.
        """
        async with self.connection() as conn:
            # 1. Grab unprocessed events, latest per order_id
            rows = conn.execute(f"""
                WITH ranked AS (
                    SELECT *, ROW_NUMBER() OVER (
                        PARTITION BY order_id ORDER BY event_ts DESC
                    ) AS rn
                    FROM bronze_order_events
                    WHERE processed_at IS NULL
                )
                SELECT id, order_id, payload
                FROM ranked WHERE rn = 1
                LIMIT {int(batch_size)}
            """).fetchall()

            if not rows:
                return {"promoted": 0, "skipped": 0, "batch_event_ids": 0}

            # 2. Parse payloads and build insert params
            insert_params = []
            event_ids_to_mark = []
            skipped = 0
            for event_id, order_id, payload_json in rows:
                try:
                    o = json.loads(payload_json) if isinstance(payload_json, str) else payload_json
                    insert_params.append((
                        int(o["id"]),
                        o.get("source_id"), o.get("status_id"),
                        float(o.get("grand_total", 0)),
                        o.get("ordered_at"), o.get("created_at"), o.get("updated_at"),
                        o.get("buyer", {}).get("id") if isinstance(o.get("buyer"), dict) else o.get("buyer_id"),
                        o.get("manager", {}).get("id") if isinstance(o.get("manager"), dict) else o.get("manager_id"),
                        o.get("manager_comment"),
                        o.get("promocode"),
                    ))
                except (KeyError, TypeError, ValueError):
                    skipped += 1
                    continue
                event_ids_to_mark.append(event_id)

            if not insert_params:
                return {"promoted": 0, "skipped": skipped, "batch_event_ids": 0}

            # 3. Collect ALL event IDs for these order_ids (not just latest)
            order_ids = [p[0] for p in insert_params]
            ph = ",".join("?" * len(order_ids))
            all_event_ids = [r[0] for r in conn.execute(
                f"SELECT id FROM bronze_order_events WHERE order_id IN ({ph}) AND processed_at IS NULL",
                order_ids,
            ).fetchall()]

            # 4. Batch DELETE+INSERT into orders in single transaction
            conn.execute("BEGIN TRANSACTION")
            try:
                conn.execute(
                    f"DELETE FROM orders WHERE id IN ({ph})", order_ids
                )
                conn.executemany("""
                    INSERT INTO orders (id, source_id, status_id, grand_total,
                        ordered_at, created_at, updated_at, buyer_id, manager_id,
                        manager_comment, promocode, synced_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, now())
                """, insert_params)

                # 5. Mark all events for these orders as processed
                eid_ph = ",".join("?" * len(all_event_ids))
                conn.execute(
                    f"UPDATE bronze_order_events SET processed_at = now() WHERE id IN ({eid_ph})",
                    all_event_ids,
                )
                conn.execute("COMMIT")
            except Exception:
                try:
                    conn.execute("ROLLBACK")
                except Exception:
                    pass
                raise

            return {
                "promoted": len(insert_params),
                "skipped": skipped,
                "batch_event_ids": len(all_event_ids),
            }

    async def prune_bronze_events(
        self,
        retention_days: int = 7,
        mode: Optional[str] = None,
    ) -> int:
        """Delete old bronze events. Mode-aware retention policy.

        Behaviour depends on the active sync mode:

        - **staging** mode: deletes only PROCESSED events older than the
          retention window. Unprocessed events are preserved because the
          promotion job still needs them. This is the original H3 design.

        - **legacy** mode: deletes events older than the retention window
          regardless of processed_at. In legacy mode the promotion job is
          inactive, so processed_at is permanently NULL — gating on it
          would prevent the table from ever shrinking. We learned this
          the expensive way on 2026-05-18 when bronze hit 4.4M rows and
          blocked weekly compaction.

        Args:
            retention_days: Events older than this many days are eligible
                for deletion.
            mode: 'legacy' or 'staging'. If None, read from
                core.config.config.sync.mode at call time. Passing
                explicitly is preferred for tests and ops scripts so the
                behaviour is reproducible regardless of env state.

        Returns:
            Number of rows deleted.
        """
        if mode is None:
            from core.config import config
            mode = config.sync.mode

        days = int(retention_days)
        if mode == "staging":
            where = "processed_at IS NOT NULL AND event_ts < now() - INTERVAL '{} days'".format(days)
        elif mode == "legacy":
            where = "event_ts < now() - INTERVAL '{} days'".format(days)
        else:
            raise ValueError(
                f"prune_bronze_events: unknown mode {mode!r}; expected 'legacy' or 'staging'"
            )

        async with self.connection() as conn:
            result = conn.execute(
                f"DELETE FROM bronze_order_events WHERE {where} RETURNING id"
            ).fetchall()
            return len(result)

    async def replay_bronze_events(self, since: Optional[datetime] = None,
                                   source: Optional[str] = None) -> int:
        """Reset processed_at to NULL for bronze events to trigger re-promotion.

        Args:
            since: Only replay events with event_ts >= since. Required.
            source: Optionally filter by source tag.

        Returns:
            Number of events marked for replay.
        """
        if since is None:
            raise ValueError("'since' parameter is required for replay")

        conditions = ["event_ts >= ?"]
        params: list = [since]
        if source:
            conditions.append("source = ?")
            params.append(source)

        where = " AND ".join(conditions)

        async with self.connection() as conn:
            result = conn.execute(
                f"UPDATE bronze_order_events SET processed_at = NULL WHERE {where} RETURNING id",
                params,
            ).fetchall()
            return len(result)

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
                try:
                    conn.execute("ROLLBACK")
                except Exception:
                    pass
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
                try:
                    conn.execute("ROLLBACK")
                except Exception:
                    pass
                raise

    async def upsert_managers(self, managers: List[Dict[str, Any]]) -> int:
        """Insert or update managers from KeyCRM API response.

        `is_retail` is seeded from RETAIL_MANAGER_IDS for managers we have
        never seen, and **left alone** for managers we already hold. It used
        to be recomputed from that constant on every sync, which made the
        column unfixable: KeyCRM does not know whether a manager is retail,
        wholesale, internal or a blogger, so the only source for that is a
        human — and whatever a human set was overwritten within the minute.
        The list has not grown since it was written; managers 3, 5, 6, 7, 10,
        28, 34 and 40 have been selling into `sales_type='other'` ever since,
        ₴3.1M of it, invisible on every page.

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
                        INSERT INTO managers
                        (id, name, email, status, is_retail, synced_at)
                        VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                        ON CONFLICT (id) DO UPDATE SET
                            name = EXCLUDED.name,
                            email = EXCLUDED.email,
                            status = EXCLUDED.status,
                            -- EXCLUDED, not CURRENT_TIMESTAMP: DuckDB binds a
                            -- bare name on this side as a column reference.
                            synced_at = EXCLUDED.synced_at
                    """, [
                        mgr.get("id"),
                        mgr.get("name") or mgr.get("full_name", "Unknown"),
                        mgr.get("email"),
                        mgr.get("status"),  # 'active', 'blocked', 'pending'
                        mgr.get("id") in RETAIL_MANAGER_IDS,  # seed for new rows only
                    ])
                    count += 1

                conn.execute("COMMIT")
                logger.info(f"Upserted {count} managers to DuckDB")
                return count

            except Exception:
                try:
                    conn.execute("ROLLBACK")
                except Exception:
                    pass
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
                try:
                    conn.execute("ROLLBACK")
                except Exception:
                    pass
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

    async def set_manager_retail_status(
        self,
        manager_id: int,
        is_retail: bool,
        effective_from: Optional[date] = None,
        set_by: Optional[int] = None,
        note: Optional[str] = None,
    ) -> None:
        """Update retail status for a specific manager.

        `sales_type` is materialised into silver_orders by one CASE at rebuild
        time, so a classification that is not followed by a rebuild changes
        nothing a reader can see. Marking the warehouse dirty here rather than
        in the route makes that true for every caller, not just the one that
        remembered — the route has always done it, and the method has always
        let anyone else forget.

        **Forward-dated, not retroactive.** The open interval is closed at
        `effective_from` and a new one opened, so orders before that date keep
        the answer they were given. This used to be a single UPDATE, which
        restated every report the manager had ever appeared in the moment the
        warehouse rebuilt — the owner ruled that out on 2026-08-20. Pass an
        explicit `effective_from` to correct a genuine misclassification
        backwards; that is now a deliberate act rather than the only behaviour.

        `managers.is_retail` is kept in step as the current answer, because the
        manager list and the seeding path both read it.

        Args:
            manager_id: Manager ID to update
            is_retail: TRUE for retail, FALSE for B2B/other
            effective_from: first order date the new answer applies to;
                defaults to today in the display timezone
            set_by: admin user id, recorded for the audit trail
            note: free-text reason, recorded alongside
        """
        if effective_from is None:
            effective_from = datetime.now(ZoneInfo(DISPLAY_TIMEZONE)).date()

        async with self.connection() as conn:
            # An interval already starting on this date is replaced, not
            # stacked: two rows with the same valid_from would make the
            # resolution ambiguous, and the PK would reject the second anyway.
            conn.execute(
                "DELETE FROM manager_classifications "
                "WHERE manager_id = ? AND valid_from = ?",
                [manager_id, effective_from],
            )
            conn.execute(
                "UPDATE manager_classifications SET valid_to = ? "
                "WHERE manager_id = ? AND valid_to IS NULL AND valid_from < ?",
                [effective_from, manager_id, effective_from],
            )
            conn.execute(
                "INSERT INTO manager_classifications "
                "(manager_id, is_retail, valid_from, valid_to, set_by, note) "
                "VALUES (?, ?, ?, NULL, ?, ?)",
                [manager_id, is_retail, effective_from, set_by, note],
            )
            conn.execute(
                "UPDATE managers SET is_retail = ? WHERE id = ?",
                [is_retail, manager_id]
            )
            logger.info(
                f"Manager {manager_id} retail status set to {is_retail} "
                f"from {effective_from}"
            )

        # Outside the connection block on purpose: asyncio.Lock is not
        # reentrant, and mark_warehouse_dirty takes it again. Calling it inside
        # deadlocked the validation retry path once already.
        await self.mark_warehouse_dirty(None)

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

            # Age of the last successful data-quality run per layer. Computed
            # here, inside the connection we already hold, so the health
            # endpoint's 60s cache covers it and the watchdog costs no extra
            # store-lock traffic. The verdict lives in bot/canary.py.
            try:
                from core.data_quality import fetch_last_success_ages
                data_quality = fetch_last_success_ages(conn)
            except Exception as e:
                # Never let the watchdog's own query break /api/health.
                logger.warning(f"data-quality freshness query failed: {e}")
                data_quality = None

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
                "db_size_mb": round(self.db_path.stat().st_size / 1024 / 1024, 2) if self.db_path.exists() else 0,
                "data_quality": data_quality,
            }


# ═══════════════════════════════════════════════════════════════════════════════
# BRONZE INVARIANT EVALUATOR
# ═══════════════════════════════════════════════════════════════════════════════

# Maximum bronze row count tolerated per (mode, shadow_enabled) combination.
# Upper bounds — exceeding them is a config-drift signal, not capacity.
# The actual normal state in legacy-no-shadow is 0 rows; the threshold gives
# one day of slop in case a prune cycle was missed.
BRONZE_INVARIANT_THRESHOLDS = {
    # (mode, shadow_enabled) : (max_total, max_unprocessed)
    ("legacy",  False): (10_000,     10_000),       # writes disabled; expect ~0
    ("legacy",  True):  (1_000_000,  1_000_000),    # opt-in shadow log; prune keeps bounded
    ("staging", False): (1_000_000,  100_000),      # promotion drains unprocessed
    ("staging", True):  (1_000_000,  100_000),      # shadow flag has no effect in staging
}


def evaluate_bronze_invariant(
    stats: Dict[str, Any],
    mode: str,
    shadow_enabled: bool,
) -> Tuple[bool, Optional[str]]:
    """Pure function. Return (healthy, reason).

    Invariants the system must hold:

    - **legacy + no shadow**: writes disabled (SyncConfig.should_write_bronze).
      bronze.total should be ~0. Millions → prune broken OR sync writing
      despite the flag OR mode/shadow drifted in env.

    - **legacy + shadow opt-in**: writes enabled, prune is mode-aware.
      Grows daily but 7-day retention keeps it bounded.

    - **staging**: writes always; promotion drains unprocessed. Growing
      unprocessed → promotion falling behind.

    Thresholds are deliberately generous (≈one day of slop) so the alert
    is a "something is wrong" signal rather than a noisy capacity gauge.
    """
    key = (mode, bool(shadow_enabled))
    if key not in BRONZE_INVARIANT_THRESHOLDS:
        return False, (
            f"unknown mode/shadow combination: "
            f"mode={mode!r}, shadow={shadow_enabled!r}"
        )

    max_total, max_unprocessed = BRONZE_INVARIANT_THRESHOLDS[key]
    total = stats.get("total", 0)
    unprocessed = stats.get("unprocessed", 0)

    if total > max_total:
        return False, (
            f"bronze.total={total:,} exceeds threshold {max_total:,} "
            f"for mode={mode}, shadow={shadow_enabled}"
        )
    if unprocessed > max_unprocessed:
        return False, (
            f"bronze.unprocessed={unprocessed:,} exceeds threshold "
            f"{max_unprocessed:,} for mode={mode}, shadow={shadow_enabled}"
        )
    return True, None


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

