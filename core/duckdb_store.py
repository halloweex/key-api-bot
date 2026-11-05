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
import contextlib
import json
import logging
import os
import re
import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import asynccontextmanager
from datetime import datetime, date, timezone
from pathlib import Path
from typing import Optional, List, Dict, Any, Tuple, TYPE_CHECKING
from zoneinfo import ZoneInfo

if TYPE_CHECKING:  # pragma: no cover — the annotation's name, no import cost
    from core.models import Buyer

import duckdb

from core import duckdb_switch
from core.models import LOST_STATUS_GROUP_ID, Order, OrderStatus
from core.exceptions import QueryTimeoutError
from core.observability import cut_row_dumps, without_row_dump
from core.duckdb_constants import (
    DB_DIR, DB_PATH, DEFAULT_TZ, DEFAULT_QUERY_TIMEOUT, LONG_QUERY_TIMEOUT,
    B2B_MANAGER_ID, RETAIL_MANAGER_IDS, KNOWN_SALES_TYPES, DISPLAY_TIMEZONE,
    line_window_where,
    EXHIBITION_SOURCE_ID, REVENUE_SOURCE_IDS,
)
from core.repositories import (
    UsersMixin, TrafficMixin, CustomersMixin, GoalsMixin,
    InventoryMixin, ExpensesMixin, RevenueMixin, ProductsIntelMixin,
    MarginMixin,
)
# What an order upsert did. It lives with its one remaining producer since
# step 14's S14-01; re-exported here as the same class for one release, for
# callers that have not moved yet. This module's own `upsert_orders`, its last
# user here, went with chain 3's DuckDB half in S14-09.
from core.pg_orders_write import UpsertResult  # noqa: F401 — the re-export

logger = logging.getLogger(__name__)


# DuckDB's own memory ceiling, independent of the container's mem_limit. Keep it
# below what the container allows: Python, pandas frames and the Meili sync all
# draw from the same budget, and a container OOM-kill is worse than a DuckDB
# spill to temp_directory.
DEFAULT_DUCKDB_MEMORY_LIMIT = "4GB"




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


def _invalidated(conn) -> "Optional[duckdb.FatalException]":
    """The FatalException a statement on `conn` raises once DuckDB has
    invalidated its instance, or None while it answers.

    A FatalException invalidates the whole instance, and from then on every
    statement on it raises FatalException "database has been invalidated
    because of a previous fatal error", whose text carries the original error
    (measured on 1.5.5). Asked of the instance rather than read off the
    exception in hand, because the exception in hand can mislead both ways: a
    caller may have swallowed the FATAL and raised something else, or wrapped
    it with no chain left to read; and a FatalException in the chain may have
    come from some other instance the caller opened.
    """
    try:
        conn.execute("SELECT 1").fetchall()
    except duckdb.FatalException as exc:
        return exc
    except Exception:  # noqa: BLE001 — e.g. an aborted transaction: still valid
        return None
    return None


# An index FATAL is damage in the FILE, and a restart forgets `_fatal`: so the
# first one is also written down beside the file, as the file's (device, inode)
# — which a compaction's swap replaces and a restart does not — and read back
# by `fatal_status()` while it still describes the file that is there. A
# restore that copies over the file in place keeps the inode: delete it then.
INDEX_SHORT_MARKER = ".duckdb_index_short.json"


def _fatal_kind(echo: BaseException) -> str:
    """`index` — an index short in the file, the damage a killed writer's WAL
    left before the open guard (`duckdb_switch.open_file`; the lever is
    rebuilding every CREATE INDEX index); `other` — anything else. Read off
    the "invalidated" echo, which quotes the original error."""
    if "delete all rows from index" in str(echo).lower():
        return "index"
    return "other"


class StoreClosedError(RuntimeError):
    """A use of a `DuckDBStore` that `close()` has closed.

    Shutdown closes the store while handlers past their 504 and jobs past
    their cancellation may still be queued on its lock; reopening it for them
    would run the schema and the migrations again during shutdown, write, and
    leave an instance nothing closes. `connect()` reopens one deliberately."""


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
# One body, two engines — the same contract the Silver projection has. The
# Postgres rendering is `silver.order_lines` (migration 0011); a test renders
# both from `core.sql_dialect.order_lines_select` and asserts they agree.
from core.sql_dialect import DUCKDB as _DUCKDB_DIALECT
from core.sql_dialect import inventory_view_selects as _inventory_view_selects
from core.sql_dialect import order_lines_select as _order_lines_select

SILVER_ORDER_LINES_VIEW_SQL = (
    "CREATE OR REPLACE VIEW silver_order_lines AS\n"
    + _order_lines_select(_DUCKDB_DIALECT)
)


# ─── Silver's projection and Gold's measures ─────────────────────────────────
#
# They live in `core/warehouse_sql.py` since step 14's S14-01, so that a
# Postgres derivation renders them without importing this module and the
# `duckdb` driver with it. Re-exported here as the same objects until this
# module goes (S14-18), for callers that have not moved yet; this module's own
# rebuild, their last reader here, went in S14-15.
from core.warehouse_sql import (
    GOLD_MEASURES,
    _GOLD_EARNING_ROWS,
    _GOLD_ORDERS_EXPR,
    _GOLD_RETURNED_ROWS,
    _GOLD_REVENUE_EXPR,
    silver_pass2_sql,
    silver_recompute_ctes,
    silver_sales_type_case,
    silver_select_sql,
)

# DuckDB's own Gold — `GOLD_SOURCE_MEASURES` (the six channel columns the
# `source_id` dimension replaced), `DUCKDB_GOLD_COLUMNS` (the table's DDL
# order, for a positional INSERT), `gold_select_items` and
# `GOLD_REVENUE_SELECT_SQL` — stood here until step 14's S14-15 deleted
# the DuckDB rebuild that rendered them. The table's DDL stays in
# `_init_schema` until the schema goes (S14-18): the compaction's phase 2
# and the inventory views are still built from it.


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

    # FatalExceptions this store has seen, for /api/health; None until one.
    # Class-level for the same reason.
    _fatal: "Optional[Dict[str, Any]]" = None

    # Set by close(), cleared by an explicit connect(): what tells a store
    # shut down from one a FATAL dropped (see `connection()`).
    _closed: bool = False

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

        # Migrations that failed on the last connect. Empty is the normal state;
        # non-empty is published on /api/health rather than left in a log line
        # nobody reads.
        self._failed_migrations: List[Dict[str, Any]] = []
        self._schema_status: Dict[str, Any] = {"status": "unknown", "reason": "not connected"}

    async def connect(self, *, reopen: bool = True) -> None:
        """Initialize database connection, schema, and thread pool.

        All or nothing: if anything here raises, the store is left with no
        connection, so the next use of it connects again from the start.

        `reopen=False` is `connection()`'s lazy open, and it refuses a store
        `close()` has closed (`StoreClosedError`) — decided under the lock,
        so a caller that was queued behind `close()` cannot slip in after it.
        A call with the default reopens a closed store on purpose.

        Under `KS_DUCKDB=off` the switch refuses here, as the first act,
        before anything touches the disk: `open_file` below refuses as well,
        but only after this method has created `data/` and `duckdb_tmp/`, so
        a refused connect used to leave two directories behind. Under `on`
        `guard()` does nothing.
        """
        duckdb_switch.guard()
        DB_DIR.mkdir(parents=True, exist_ok=True)

        async with self._lock:
            if self._closed and not reopen:
                raise StoreClosedError(
                    f"the DuckDB store is closed (shutdown): {self.db_path}")
            self._closed = False
            if self._connection is None:
                # Enable disk spilling: DuckDB writes to disk instead of OOM crash.
                tmp_dir = Path(self.db_path).parent / "duckdb_tmp"
                tmp_dir.mkdir(parents=True, exist_ok=True)
                # Through the one opener: it refuses under KS_DUCKDB=off
                # before the driver can open — or create — the file, and
                # otherwise CHECKPOINTs whatever WAL a killed writer left,
                # before the SETs below (core/duckdb_switch.py `open_file`).
                #
                # The memory limit and the spill directory are the instance's
                # from its first moment, not SETs after it. The open replays
                # that WAL and the opener checkpoints it before any SET could
                # run, so as SETs they bound neither: both ran under DuckDB's
                # default — 80% of the memory it detects, ~5.6 GB in web's 7 GB
                # container — after exactly the kill (an OOM kill, likeliest)
                # that leaves a large WAL (batch-E review).
                #
                # Prevent OOM in memory-limited containers (DuckDB defaults to 80% of system RAM).
                # 3GB verified safe for checkpoint on 19GB DB via compact_duckdb.py spike runs
                # (also exercises full export). 2GB OOMs WAL flush — keep 3GB as floor.
                #
                # 3GB was too tight: on 2026-08-02 seven consecutive warehouse refreshes died
                # at 2.7/2.7 GiB while the container sat at ~950 MiB of its 7g budget, and the
                # first refresh to complete afterwards left Gold truncated by 763 revenue rows.
                # The ceiling is now configurable so it can be raised without a code deploy.
                self._connection = duckdb_switch.open_file(
                    self.db_path, config={"memory_limit": _memory_limit(),
                                          "temp_directory": str(tmp_dir)})
                try:
                    # Reduce memory usage for bulk operations
                    self._connection.execute("SET preserve_insertion_order=false")
                    # Large WAL threshold; rely on the explicit 6h CHECKPOINT job.
                    # 2MB caused checkpoint-during-write races on DuckDB 1.5.x
                    # (corrupted in-memory column: row group rows mismatched column rows).
                    self._connection.execute("SET wal_autocheckpoint='1GB'")

                    await self._init_schema()

                    # Thread pool for offloading blocking operations
                    self._executor = ThreadPoolExecutor(
                        max_workers=1,  # Single worker - DuckDB requires serialized access
                        thread_name_prefix="duckdb"
                    )
                except BaseException as exc:
                    # The schema and the migrations run on `self._connection`,
                    # so it is set before they start — and a failure used to
                    # leave it set. `connection()` reconnects only when it is
                    # None, so every later caller took the half-built store as
                    # connected: views that failed to build meant not one
                    # migration ever ran, 0027 included, while the ledger
                    # stayed "unknown" — which /api/health does not count as
                    # degraded — and named no failure. The same held for
                    # anything above that raised, a spill directory that could
                    # not be created among them. Drop the connection instead,
                    # so the next use of the store runs all of this again.
                    #
                    # What still reaches here is what leaves no usable store:
                    # the connection settings, the table DDL, the ledger. A
                    # view no longer does — it is built after the migrations
                    # and a failure is published instead (`_build_view`),
                    # because it fails the same way on every attempt and
                    # would otherwise keep web from starting at all.
                    connection, self._connection = self._connection, None
                    with contextlib.suppress(Exception):
                        connection.close()
                    cut_row_dumps(exc)
                    raise

                logger.info(f"DuckDB connected: {self.db_path}")

    async def close(self) -> None:
        """Close database connection and thread pool.

        Final until an explicit `connect()`: a later `connection()` — the
        caller queued on the lock behind this one included — raises
        `StoreClosedError` instead of opening the file again."""
        async with self._lock:
            self._closed = True
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
            conn = self._connection
            if conn is None and duckdb_switch.is_off():
                # The store `get_store()` publishes under off is unconnected,
                # so there is nothing to checkpoint — said, so that the hourly
                # job's silence is not read as a checkpoint that ran.
                logger.info("DuckDB checkpoint skipped: KS_DUCKDB=off, the "
                            "store is not connected")
            if conn:
                try:
                    conn.execute("CHECKPOINT")
                except Exception as exc:
                    self._drop_if_invalidated()
                    cut_row_dumps(exc)
                    raise
                logger.info("DuckDB checkpoint completed")

    @asynccontextmanager
    async def connection(self):
        """Get database connection with automatic reconnection.

        Acquires lock to ensure single-threaded DuckDB access.
        DuckDB connections are NOT thread-safe - only one thread can use
        a connection at a time.

        A FatalException invalidates the DuckDB instance: every later
        statement on it raises "database has been invalidated". It used to
        stay in place — reconnection happened only when there was no
        connection — so one FATAL left web's DuckDB dead until a restart,
        the caller queued on the lock included. Now, whenever the block
        raises, the instance is asked whether it still answers
        (`_invalidated`), and one that does not is dropped, so the next use
        opens the file again. A FATAL a caller swallowed is dropped by the
        next use that raises, which is every use of an invalidated instance.

        What that does not do is heal: a FATAL from a short index (see
        `duckdb_switch.open_file`) comes back on the same write every time,
        because the index is short in the file. `fatal_status()` keeps
        /api/health degraded, and the page standing, until a restart.

        A store `close()` has closed is not reopened here: the open is
        `connect(reopen=False)`, which raises `StoreClosedError`. Otherwise a
        caller queued behind `close()` would take the missing connection for
        one a FATAL dropped and open the file again: the schema and the
        migrations during shutdown, a write after `close()`'s checkpoint, and
        an instance nothing would ever close.

        Whatever leaves the block leaves without DuckDB's dump of the rows a
        FATAL could not remove (`core.observability.cut_row_dumps`): every
        column of each, a buyer's name, phone and email among them, which a
        caller logging the exception with its traceback — the buyers step
        does — used to print into web's log on every retry.
        """
        while True:
            if self._connection is None:
                await self.connect(reopen=False)
            async with self._lock:
                conn = self._connection
                if conn is None:
                    # A FATAL dropped it, or close() closed it, while this
                    # caller waited for the lock: connect(reopen=False) opens
                    # the file again for the first and refuses the second.
                    continue
                try:
                    yield conn
                except Exception as exc:
                    # Not on a cancellation, which says nothing about the
                    # instance. (Nor would the probe be safe there if anything
                    # ever left a thread on `conn` past a cancellation: it
                    # runs on the event loop and would wait for that query.
                    # `_offload` does not, so today nothing does.)
                    self._drop_if_invalidated()
                    cut_row_dumps(exc)
                    raise
                return

    def _drop_if_invalidated(self) -> None:
        """Forget the store's connection if DuckDB invalidated its instance,
        so the next use opens the file again.

        Called with the lock held — by `connection()` and `checkpoint()`,
        after the statement that raised has come off the executor thread — so
        the connection asked is the one the failing block used: nothing else
        can replace it while the lock is held.

        The executor goes with it. `connect()` builds a new one, and the old
        one, left running, is a worker thread per FATAL waiting forever for
        work nothing will send it. Closing an invalidated instance writes
        nothing — the WAL stays, and the next open replays and checkpoints it
        through `duckdb_switch.open_file` — and a fresh `duckdb.connect` of
        the same file in the same process gets a new instance, even while a
        cursor of the old one is alive (both measured on 1.5.5).
        """
        conn = self._connection
        if conn is None:
            return
        echo = _invalidated(conn)
        if echo is None:
            return
        self._connection = None
        executor, self._executor = self._executor, None
        if executor:
            executor.shutdown(wait=False)
        with contextlib.suppress(Exception):
            conn.close()
        kind = _fatal_kind(echo)
        seen = self._fatal or {"count": 0, "kinds": {}}
        count = seen["count"] + 1
        kinds = {**seen["kinds"], kind: seen["kinds"].get(kind, 0) + 1}
        self._fatal = {
            "count": count,
            "kinds": kinds,
            "last_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        }
        if kind == "index":
            self._record_index_short(self._fatal["last_at"])
        lever = (
            " An index is short in the file, and the write that met it fails "
            "again until every CREATE INDEX index is rebuilt: the Sunday "
            "compaction, or scripts/weekly_compact.sh by hand. A restart does "
            "not heal it." if kind == "index" else ""
        )
        # Up to the chunk dump: an index FATAL prints the rows it could not
        # remove, and buyers' phone and email columns are indexed.
        error = " ".join(without_row_dump(str(echo)).split())[:400]
        logger.error(
            "DuckDB FATAL #%d (%s): the instance is dropped and the next use "
            "opens the file again.%s %s", count, kind, lever, error,
        )

    def fatal_status(self) -> "Optional[Dict[str, Any]]":
        """The invalidations this store has seen: how many, of which kind
        (`_fatal_kind`), and when the last one was — and `index_short`, the
        index damage an index FATAL found in the file that is there now
        (`since`, `last_at`, `count`), which outlives the process that saw it.

        None until one, or while neither. Published on /api/health, which
        reads degraded while it is set — without that, the reconnect above
        would let a dead write look healthy as soon as the next read
        answered, and a restart would announce a short index healed: it is
        not, until a compaction replaces the file (`INDEX_SHORT_MARKER`).
        Never raises."""
        index = self._read_index_short()
        if not self._fatal and not index:
            return None
        seen = self._fatal or {"count": 0, "kinds": {}, "last_at": None}
        return {**seen, "kinds": dict(seen["kinds"]), "index_short": index}

    def _index_marker(self) -> "Optional[Path]":
        # `getattr`: a store built without __init__ has no path (see `_fatal`).
        db_path = getattr(self, "db_path", None)
        return None if db_path is None else Path(db_path).parent / INDEX_SHORT_MARKER

    def _file_identity(self) -> "Optional[Tuple[int, int]]":
        db_path = getattr(self, "db_path", None)
        try:
            st = Path(db_path).stat() if db_path is not None else None
        except OSError:
            return None
        return None if st is None else (st.st_dev, st.st_ino)

    def _read_index_short(self) -> "Optional[Dict[str, Any]]":
        """The written-down index damage, if it describes the file that is
        there now; None otherwise. Never raises: a marker that cannot be read
        is logged and read as none, since inventing damage would hold the
        status degraded with nothing to clear it."""
        path = self._index_marker()
        if path is None:
            return None
        try:
            record = json.loads(path.read_text(encoding="utf-8"))
        except FileNotFoundError:
            return None
        except (OSError, ValueError) as exc:
            logger.warning("DuckDB index marker %s unreadable: %s", path, type(exc).__name__)
            return None
        if not isinstance(record, dict):
            return None
        identity = self._file_identity()
        if identity is None or [record.get("dev"), record.get("ino")] != list(identity):
            # Another file now — the compaction swapped one in, which rebuilt
            # every index — so it describes nothing that is there.
            return None
        return {"since": record.get("since"), "last_at": record.get("last_at"),
                "count": record.get("count")}

    def _record_index_short(self, at: str) -> None:
        """Write the index damage down beside the file. Never raises: the
        FATAL is already logged and counted in this process."""
        identity = self._file_identity()
        if identity is None:
            return
        prior = self._read_index_short() or {}
        record = {"file": Path(self.db_path).name, "dev": identity[0], "ino": identity[1],
                  "since": prior.get("since") or at, "last_at": at,
                  "count": int(prior.get("count") or 0) + 1}
        path = self._index_marker()
        if path is None:
            return
        tmp = path.with_name(path.name + ".tmp")
        try:
            tmp.write_text(json.dumps(record), encoding="utf-8")
            os.replace(tmp, path)
        except OSError as exc:
            logger.error("DuckDB index marker %s could not be written: %s: %s; the "
                         "damage is published until this process restarts only",
                         path, type(exc).__name__, exc)

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
                return await self._offload(
                    conn, lambda: conn.execute(query, params or []).fetchone(), timeout)
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
                return await self._offload(
                    conn, lambda: conn.execute(query, params or []).fetchall(), timeout)
            except asyncio.TimeoutError:
                raise QueryTimeoutError(query, timeout, "Fetch all failed")

    async def _offload(self, conn, run, timeout: float):
        """`run()` on the executor thread, under the lock the caller holds,
        and the thread off `conn` before this returns or raises — whatever
        ends the wait.

        The lock exists so that no two threads touch the one connection, and
        leaving the block releases it. After a timeout the thread is still
        inside `conn.execute()`, and after a **cancellation** too — the
        scheduler cancelling a job at shutdown, a caller giving up on a read.
        A timeout interrupted the query and waited; a cancellation used to
        leave at once, so the next holder of the lock shared `conn` with the
        leftover query, and the next block that raised made `connection()`
        probe the instance on the event loop — which waits for that query:
        every request stalled for seconds, measured. Both interrupt and wait
        now. A second cancellation during the wait is held until the thread
        is off, then raised: the interrupt is already sent, so what it costs
        is the rest of one statement.
        """
        future = asyncio.get_running_loop().run_in_executor(self._executor, run)
        try:
            return await asyncio.wait_for(asyncio.shield(future), timeout=timeout)
        except (asyncio.TimeoutError, asyncio.CancelledError):
            with contextlib.suppress(Exception):
                conn.interrupt()
            cancelled_again = False
            while not future.done():
                try:
                    await asyncio.wait([future])
                except asyncio.CancelledError:
                    cancelled_again = True
            if not future.cancelled():
                future.exception()   # retrieved: asyncio would log it otherwise
            if cancelled_again:
                raise asyncio.CancelledError()
            raise

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
        -- What each delivery-report event first said it was about.
        --
        -- TurboSMS signs SHA1(secret + id) and nothing else, so the signature on
        -- a callback proves the caller knew the secret but says nothing about
        -- which message the callback names. Without this table one captured
        -- (id, signature) pair was a permanent write into any recipient's
        -- delivery state — and delivery state is what a campaign's measured lift
        -- is computed from.
        --
        -- Durable rather than in-process because the captured pair is durable:
        -- the scheme carries no nonce and no timestamp, so a pair never expires,
        -- while the web container restarts on every deploy and is stopped weekly
        -- by the compact cron. A binding that forgets is a binding with a
        -- published expiry.
        --
        -- Append-only, and never pruned. Pruning would hand old pairs their
        -- window back, and DELETE against an ART-indexed table is the known
        -- file-growth mechanism in this database; insert-only never enters it.
        -- One row per event: a 5 000-recipient campaign is 5 000 rows.
        -- ═══════════════════════════════════════════════════════════════════════
        CREATE TABLE IF NOT EXISTS sms_dlr_events (
            event_id VARCHAR PRIMARY KEY,
            message_id VARCHAR NOT NULL,
            first_seen_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
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

        -- ═══════════════════════════════════════════════════════════════════════
        -- Saved audiences.
        --
        -- A campaign's audience is a set of filters, and the useful ones get
        -- reused: "everyone who bought this brand in the last quarter" is a
        -- question asked every time that brand runs a promotion. Without
        -- somewhere to keep it, the manager rebuilds it from memory each time
        -- and the second campaign is measured against a slightly different
        -- population than the first.
        --
        -- `criteria` is the wizard's own form state as JSON, and it is never
        -- executed: the page fills its controls from it and sends the values
        -- back through the same validated query parameters as a hand-built
        -- audience. A preset is therefore a saved answer, not a stored query,
        -- and cannot widen what the endpoints accept.
        -- ═══════════════════════════════════════════════════════════════════════
        CREATE TABLE IF NOT EXISTS sms_audience_presets (
            name VARCHAR PRIMARY KEY,
            criteria VARCHAR NOT NULL,
            created_by BIGINT,
            created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP WITH TIME ZONE
        );

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

        -- `gold_daily_products` stood here with a long note on why it carried
        -- no index. The measurement in it is still the rule for every rebuilt
        -- table: in DuckDB 1.5.5 a single ART index — a PRIMARY KEY counts —
        -- disables row-group vacuuming for the whole table, and a full
        -- DELETE+INSERT then leaked 1.171 MB a cycle against 0.000 MB with no
        -- index. That is why migration 0004 still drops those six on databases
        -- that have them, and why none of these tables carries one.
        --
        -- The layer itself is retired: its readers moved to the order-lines
        -- level, which reproduces it, and rebuilding it every two minutes for
        -- nobody was the largest thing left writing to this file.

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
            created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
            -- Which tabs this person may open, comma-separated, or NULL for
            -- "whatever the role gives". See core/dashboard_access.py; the
            -- Postgres twin is revision 0021.
            allowed_features VARCHAR
        );

        CREATE INDEX IF NOT EXISTS idx_users_status ON users(status);
        CREATE INDEX IF NOT EXISTS idx_users_role ON users(role);

        -- User preferences
        -- user_preferences, report_history and celebrated_milestones used to be
        -- declared here as well as in the bot's SQLite. The live rows were always
        -- in `data/bot.db` — the bot writes them, and it cannot write this file
        -- at all, because DuckDB takes a single writer and the web container
        -- holds it. These copies sat empty in production for their whole life
        -- while the backup routine's docstring named two of them as protected
        -- data, which is how `bot.db` stayed out of every backup unnoticed.
        --
        -- One home: `data/bot.db`. Read it through core/bot_prefs.py.
        -- Migration 0028 drops the leftovers, and only if they are empty.


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

        -- Weekly traffic report: the same ledger, for the other report.
        -- Its own table rather than a row in `weekly_report_sends` with
        -- "traffic" in the `sales_type` column: that column means a sales
        -- type, and a report is not one. Same shape, same reason — the job
        -- fires daily and reports the last complete week.
        CREATE TABLE IF NOT EXISTS traffic_report_sends (
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

        """
        # Silver's shape has one home; the schema script above no longer
        # carries a copy of it.
        self._connection.execute(SILVER_ORDERS_DDL)
        self._connection.execute(schema_sql)

        # Migrations BEFORE the views, and a view that fails does not take the
        # store down with it.
        #
        # The views used to come first, and a view is exactly the statement
        # that reads a column a later migration adds: `silver_order_lines`
        # reads `silver_orders.promocode`, which 0020 adds to a file created
        # before it. On such a file the view raised on every boot before a
        # single migration ran — 0020 and 0027, the only net under the weekly
        # compaction's restore, among them. Raising is also the wrong answer
        # for an all-or-nothing `connect()`: the same view fails the same way
        # on every attempt, so web would never start. No migration reads a
        # view (read, not assumed: none of core/migrations.py names one), and
        # every view is rebuilt on every boot, so this order costs nothing.
        await self._run_migrations()

        # The order-line level. A view, so it is always exactly as fresh as
        # Silver and costs the file nothing — see SILVER_ORDER_LINES_VIEW_SQL.
        await self._build_view("silver_order_lines", SILVER_ORDER_LINES_VIEW_SQL)

        # Create analytics views (Layer 3 & 4)
        await self._build_view("inventory", self._create_inventory_views)

        logger.info("DuckDB schema initialized")

    async def _build_view(self, name: str, build) -> None:
        """Build a view, and publish a failure rather than raise it.

        `build` is the statement, or the coroutine method that issues it. A
        view that cannot be built costs the readers of that view and nothing
        else — they read the definition the file last held, or fail if it
        held none — while the tables are there and every migration has run.
        Raising here would cost everything instead — `connect()` drops the
        connection on any failure, and a view whose SQL fails on this file
        fails the same way on the next attempt, so web would restart into it
        until a fix shipped.

        So it goes where a failed migration goes: into `_failed_migrations`,
        with the snapshot taken again, so `schema_status()` reads "failed",
        /api/health "degraded", and the canary pages. Not into
        `schema_migrations` — a view is rebuilt on every boot, and a "failed"
        row would outlive the boot that built it.
        """
        try:
            if callable(build):
                await build()
            else:
                self._connection.execute(build)
        except Exception as e:
            # Its text is published on /api/health, which is public: never
            # DuckDB's dump of row values (see `connection()`).
            cut_row_dumps(e)
            logger.error(
                "View %s could not be built; it keeps whatever definition the "
                "file last held, if any: %s", name, e, exc_info=True,
            )
            self._failed_migrations.append({
                "id": f"view:{name}",
                "error": f"{type(e).__name__}: {e}",
            })
            self._schema_status = self._read_schema_status()

    async def _run_migrations(self) -> None:
        """Apply every pending migration, in order, and record what happened.

        This used to be 647 lines of `try / except / logger.debug`: thirty-seven
        blocks re-run on every boot, any of which could fail leaving nothing
        above DEBUG behind. The 2026-08-09 incident began exactly there — an
        ALTER failed quietly, the code read the column anyway, and a one-off
        became a rebuild every two minutes for hours.

        The SQL did not change; it lives in `core/migrations.py` now, one named
        step at a time. What changed is that each step runs **once**, and its
        outcome is written down. A failure is logged at ERROR, recorded with its
        message, published on /api/health, and retried on the next boot — it is
        not recorded as applied, so it cannot be skipped past.

        A failed migration does not abort startup. Taking the dashboard and the
        bot down over one bad ALTER trades a silent failure for a total one; the
        point is that somebody finds out, and `schema_migrations` plus the health
        endpoint are how.
        """
        from core.migrations import MIGRATIONS, ONCE, SCHEMA_MIGRATIONS_DDL

        self._connection.execute(SCHEMA_MIGRATIONS_DDL)
        applied = {
            row[0] for row in self._connection.execute(
                "SELECT id FROM schema_migrations WHERE outcome = 'applied'"
            ).fetchall()
        }
        self._failed_migrations = []
        newly_applied = 0

        for migration in MIGRATIONS:
            if migration.mode == ONCE and migration.id in applied:
                continue
            started = time.perf_counter()
            try:
                migration.run(self)
            except Exception as e:
                # The text goes to /api/health, which is public: a migration
                # that met a short index — or ran on the instance a previous
                # one invalidated — would publish DuckDB's dump of the rows.
                cut_row_dumps(e)
                elapsed = (time.perf_counter() - started) * 1000
                logger.error(
                    "Migration %s FAILED after %.0f ms: %s",
                    migration.id, elapsed, e, exc_info=True,
                )
                self._failed_migrations.append({"id": migration.id, "error": str(e)})
                self._record_migration(migration.id, "failed", elapsed, str(e))
                continue

            elapsed = (time.perf_counter() - started) * 1000
            if migration.mode == ONCE:
                self._record_migration(migration.id, "applied", elapsed, None)
                newly_applied += 1
                logger.info("Migration %s applied in %.0f ms", migration.id, elapsed)

        if newly_applied:
            logger.info("Schema: %d migration(s) applied this start", newly_applied)
        if self._failed_migrations:
            logger.error(
                "Schema: %d migration(s) FAILED: %s",
                len(self._failed_migrations),
                ", ".join(m["id"] for m in self._failed_migrations),
            )

        self._schema_status = self._read_schema_status()

    def _record_migration(
        self, migration_id: str, outcome: str, duration_ms: float, error: Optional[str]
    ) -> None:
        """Write one row to the ledger. If *this* fails, say so loudly.

        A ledger that cannot be written is indistinguishable from a migration
        that never ran, and the next boot would replay the step. That is safe —
        every step is idempotent — but it is not something to discover later.
        """
        try:
            self._connection.execute(
                "DELETE FROM schema_migrations WHERE id = ?", [migration_id]
            )
            self._connection.execute(
                "INSERT INTO schema_migrations "
                "(id, applied_at, duration_ms, outcome, error_message) "
                "VALUES (?, CURRENT_TIMESTAMP, ?, ?, ?)",
                [migration_id, round(duration_ms, 2), outcome, error],
            )
        except Exception as e:
            logger.error("Could not record migration %s in the ledger: %s", migration_id, e)

    def schema_status(self) -> Dict[str, Any]:
        """What the ledger knew at connect time, for /api/health.

        Returns the snapshot taken at connect rather than querying — after the
        migrations, and again after any view that could not be built.
        The ledger only changes when migrations run, which is once per process,
        and `/api/health` is served from the event loop while a refresh may be
        using the one DuckDB connection from the executor — reading here would be
        two threads on a connection that is not thread-safe, for a table whose
        contents cannot have changed.
        """
        return dict(self._schema_status)

    def _read_schema_status(self) -> Dict[str, Any]:
        """Snapshot the ledger. Called at connect only: from `_run_migrations`,
        and from `_build_view` when a view adds a failure after them.

        `pending` and `failed` are kept apart on purpose: a step that has never
        run and a step that ran and blew up look identical from outside unless
        somebody says which is which.
        """
        from core.migrations import MIGRATIONS, ONCE

        try:
            rows = self._connection.execute(
                "SELECT id, outcome FROM schema_migrations"
            ).fetchall()
        except Exception as e:
            return {"status": "unknown", "error": without_row_dump(str(e))}
        applied = {r[0] for r in rows if r[1] == "applied"}
        once = [m.id for m in MIGRATIONS if m.mode == ONCE]
        return {
            "status": "failed" if self._failed_migrations else "ok",
            "applied": len(applied),
            "total": len(once),
            "pending": [m for m in once if m not in applied],
            "failed": list(self._failed_migrations),
        }


    async def _create_inventory_views(self) -> None:
        """Create Layer 3 & 4 analytics views for inventory.

        The bodies live in `core.sql_dialect.inventory_view_selects` — one text
        for both engines, table names the only difference, the same contract
        `silver_order_lines` has. Migration 0014 freezes the Postgres
        rendering and a test asserts the two agree.
        """
        views_sql = "\n\n".join(
            f"CREATE OR REPLACE VIEW {name} AS\n{select};"
            for name, select in _inventory_view_selects(_DUCKDB_DIALECT)
        )
        self._connection.execute(views_sql)
        logger.info("Inventory analytics views created")

    # ─── Warehouse Layer Refresh ─────────────────────────────────────────────
    #
    # `refresh_warehouse_layers` (DuckDB's Silver and Gold rebuild, its
    # validation and the cell guard) and `get_warehouse_status` stood here
    # until step 14's S14-15. DuckDB has derived nothing since step 13
    # (2026-10-09); Postgres alone derives (`core/pg_warehouse_derive`'s job
    # in the scheduler), and `/api/warehouse/status` reports that.

    async def get_last_sync_time(self, key: str = "orders") -> Optional[datetime]:
        """Get last sync timestamp for incremental updates — from
        `meta.chain_watermarks`, where every `last_sync_*` key lives.

        **Since step 14's S14-11 nothing is asked first.** Until then the
        getter and the setter asked the key's chain whether it wrote Postgres
        (`chain.writes_postgres()`, through the registry) and read DuckDB's
        `sync_metadata` otherwise; S14-00 made the answer a constant and
        S14-11 deleted it with the flags. A key absent from Postgres is None,
        which the sync reads as due: the first tick syncs, and its stamp is
        the first one there.

        Unless its chain declares `CHAIN_WATERMARK_INHERITS_DUCKDB` (chain 3,
        the only one left since S14-09): then an absent key is DuckDB's own
        value as Postgres last copied it (`app.sync_metadata`,
        `core.pg_frozen_metadata`), which stopped moving at the latch — the
        same stand-in `_freshness_check` judges. `last_sync_orders` is where
        the order step's window STARTS (the chain-3 review): read as None it
        became "an hour ago", the window began 25 h back, and every order
        KeyCRM updated before that was never fetched by the incremental sync
        at all. Present, Postgres's value wins. Production held every chain
        key on 2026-10-29 (the plan's H6), so the stand-in answers nothing
        there today (`tests/unit/test_absent_chain_keys.py` pins what a
        missing key costs).
        """
        full_key = f"last_sync_{key}"
        from core.pg_chain_watermarks import get_value
        from core.write_chains import chain_for_sync_key

        value = await get_value(full_key)
        if value is not None:
            return value
        chain = chain_for_sync_key(full_key)
        if chain is None or not getattr(chain, "CHAIN_WATERMARK_INHERITS_DUCKDB", False):
            return None
        from core.pg_frozen_metadata import frozen_stamp

        return await frozen_stamp(full_key)

    async def set_last_sync_time(self, key: str = "orders", timestamp: datetime = None) -> None:
        """Update last sync timestamp — in `meta.chain_watermarks`.

        One row per key. Stage 4 moved the nine `last_sync_*` watermarks
        chain by chain, routing each to the store its chain wrote (a watermark
        written to the store its chain no longer uses is a sync that repeats
        its whole window on the next tick, or never runs again); since step
        14's S14-11 every key goes to Postgres, asking nothing.
        """
        timestamp = timestamp or datetime.now(DEFAULT_TZ)
        full_key = f"last_sync_{key}"
        from core.pg_chain_watermarks import set_value

        await set_value(full_key, timestamp.isoformat())

    # `mark_warehouse_dirty`, `peek_warehouse_dirty`, `clear_warehouse_dirty`
    # and `consume_warehouse_dirty` — the flag DuckDB's warehouse tick
    # read — stood here until step 14's S14-15. A no-op since step 13: the
    # Postgres derivation is owed through `meta.derivation_signal`, which
    # the writers raise in their own transactions (`core.pg_derivation`).

    # Every repair job chooses its work by reading the order tables, and
    # chain 3 writes those to Postgres alone. Until step 14's S14-09 each of
    # these asked `_orders_in_postgres()` first and read DuckDB otherwise; that
    # answer had been a constant since S14-00, and S14-09 deleted it with the
    # DuckDB bodies. They are the store's methods still because their callers
    # — the scheduler's repair jobs, the 05:15 refresh, the boot — hold the
    # store; the statements are `core.pg_orders_read`'s.

    async def find_order_id_gaps(self, limit: int = 200) -> List[int]:
        """Order ids missing from our copy, found without asking KeyCRM.

        KeyCRM issues order ids as a dense sequence, so every hole between our
        lowest and highest id is an order we never stored. That makes finding
        them free: 1 660 holes existed as of 2026-08, and a full-history
        comparison against the API confirmed 1 616 of them were real orders.

        Ids already known to be absent upstream are skipped, so the list drains
        to empty instead of cycling forever. Asked of Postgres, where chain 3
        writes both the orders it repairs and the misses it records.
        """
        from core import pg_orders_read

        return await pg_orders_read.order_id_gaps(limit)

    async def find_backdated_order_ids(self, since: "date", limit: int = 200) -> List[int]:
        """Orders dated on or after `since` but created on or before it, by
        their Kyiv dates — the ones a `created_between` fetch starting at
        `since` does not return, although their order date is inside it.

        That is the backdated B2B order: KeyCRM lets `ordered_at` sit weeks
        after `created_at`. The 05:15 status refresh fetches by creation date,
        so without this such an order's status moved in KeyCRM, with
        `updated_at` untouched as ever, and nothing re-fetched it —
        `dq_reconciliation` filed STATUS_DRIFT every morning and repaired only
        what was missing. The legacy 06:00 job padded its fetch by 30 days for
        exactly this until OD-10 retired it.

        Created *on* `since` counts only when ordered on a later day: the
        refresh's window starts at midnight in KeyCRM's clock, not Kyiv's, and
        taking every order created that day would re-fetch fifty orders the
        fetch already returned to cover an hour at the edge. Measured on the
        2026-08-31 copy with a 30-day window: 156 ids over 183 daily runs
        (0.85 a day, at most 5), each one API call.
        """
        from core import pg_orders_read

        return await pg_orders_read.backdated_order_ids(since, limit)

    async def record_backfill_misses(self, misses: "Dict[int, str]") -> int:
        """Remember ids KeyCRM could not supply, so they are not retried.
        Written by chain 3: the ledger lives where the scans read it
        (`core.pg_orders_write`)."""
        if not misses:
            return 0
        from core import pg_orders_write

        return await pg_orders_write.record_backfill_misses(misses)

    async def find_halfwritten_orders(self, limit: int) -> List[int]:
        """Orders with revenue and no line items, not recorded as a miss in
        the last 30 days, largest first — the half-written repair's selection,
        moved here from `scheduler._run_halfwritten_repair`."""
        from core import pg_orders_read

        return await pg_orders_read.halfwritten_candidates(limit)

    async def halfwritten_among(self, ids: List[int]) -> List[int]:
        """Which of `ids` still carry revenue and no line items."""
        if not ids:
            return []
        from core import pg_orders_read

        return await pg_orders_read.halfwritten_among(ids)

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
            from core.alerting import raise_alert

            await raise_alert(
                message, conditions=[key] if key else [], bucket=key,
                group="warehouse",
            )
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
        manual_expenses, users and roles.

        It does NOT protect the bot's own database, and for a long time this
        docstring said otherwise. `user_preferences` and `celebrated_milestones`
        are declared in DuckDB *and* in `data/bot.db`, and the live rows are in
        the SQLite one — 24 approved users, 25 celebrated milestones, everybody's
        language — while these copies sat empty. Naming them here made a file
        that was in no backup at all look covered. It rides in the off-site
        bundle since 2026-08-20 (deploy/offsite_parquet.sh); this copy still
        does not include it, because this is a copy of the analytics file.

        Under `KS_DUCKDB=off` the switch refuses here, as the first act, for
        `connect()`'s reason: everything below up to `connection()` touches
        the disk beside the file — `backups/` made, a stale temp copy
        deleted, the file stat'ed — and a refusal caught by the `except`
        further down was a daily "DB backup FAILED" alert on top of the
        switch's own page. Under `on` `guard()` does nothing.
        """
        import os
        import shutil
        import time

        duckdb_switch.guard()
        src = Path(self.db_path)
        dest = Path(dest_dir) if dest_dir else src.parent / "backups"
        dest.mkdir(parents=True, exist_ok=True)
        stamp = datetime.now(DEFAULT_TZ).strftime("%Y%m%d-%H%M%S")
        final_path = dest / f"{src.stem}-{stamp}.duckdb"
        tmp_path = dest / f".{src.stem}-{stamp}.duckdb.tmp"

        # A copy interrupted by a shutdown cancellation, a kill or an OOM
        # leaves its temp file — a database-sized orphan on the volume the
        # backup protects, and nothing pruned them. A `finally` alone would
        # not do: SIGKILL skips it, and unlinking while copy2 still holds the
        # descriptor frees nothing until the thread ends.
        for stale in dest.glob(f".{src.stem}-*.duckdb.tmp"):
            try:
                stale.unlink()
                logger.warning("Removed stale backup temp file %s", stale.name)
            except OSError:
                pass

        # Disk-space guard: need room for a full second copy (+10% margin).
        src_size = src.stat().st_size
        free = shutil.disk_usage(dest).free
        if free < src_size * 1.1:
            msg = (f"Backup aborted: only {free/1e9:.1f}GB free, need "
                   f"~{src_size*1.1/1e9:.1f}GB for {src.name}")
            logger.error(msg)
            await self._send_warehouse_alert(
                f"🚨 DB backup FAILED — {msg}", "warehouse:backup_preflight",
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
                con = duckdb_switch.open_file(tmp_path, read_only=True)
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

    # `upsert_orders` stood here until step 14's S14-09: DuckDB's order writer
    # (a pandas frame, then the line items before the headers, then the
    # mirror to Postgres of exactly what it wrote). Chain 3 writes the orders
    # to Postgres alone (`core.pg_orders_write.upsert_orders_with_expenses`,
    # reached from `SyncService._upsert_orders_with_expenses`), and since
    # S14-00 this writer only refused (`ChainOwnsOrders`). Its decision is
    # pinned beside the chain's as a literal of what it answered
    # (`tests/unit/test_pg_orders_write.py`, `TestTheDecisionIsDuckDBs`).

    async def upsert_products(self, products: List[Dict[str, Any]]) -> int:
        """Insert or update products from API response.

        The payload — including pulling `brand` out of KeyCRM's custom fields —
        is read by `core.landing_rows`. Step 05 hands the same rows to
        Postgres, and brand is exactly the rule that must not acquire a second
        reading: it is typed by a human into a custom field, 6% of goods have
        none, and the dashboard's `Unknown` bucket is built on what this
        returns.
        """
        if not products:
            return 0

        from core.landing_rows import product_rows

        rows = product_rows(products)

        # Chain 6: the parsed rows are the Postgres write
        # (`core/pg_catalogue_write.py`) — after the parse, so a product is
        # read one way. DuckDB's copy stopped at the chain's flip, and its
        # writer went with step 14's S14-09.
        from core import pg_catalogue_write

        count = await pg_catalogue_write.upsert_products(rows)
        logger.info(f"Upserted {count} products to Postgres (chain 6)")
        return count

    async def upsert_categories(self, categories: List[Dict[str, Any]]) -> int:
        """Insert or update categories from API response.

        The payload is read by `core.landing_rows`, not here: step 05 hands the
        same rows to Postgres, and a store that reads the payload for itself is
        a second home for the rule (charter rule 1). `synced_at` stays local —
        it is this copy's bookkeeping, not a fact about the category.
        """
        if not categories:
            return 0

        from core.landing_rows import category_rows

        rows = category_rows(categories)

        from core import pg_catalogue_write

        count = await pg_catalogue_write.upsert_categories(rows)
        logger.info(f"Upserted {count} categories to Postgres (chain 6)")
        return count

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

        # One reading of the payload for both stores (`core.landing_rows`):
        # the name's fallbacks and the retail seed are spelled there once.
        from core.landing_rows import manager_rows

        rows = manager_rows(managers)

        # Chain 5: the managers land in Postgres with their baselines
        # (`core/pg_managers_write.py`). Since step 14's S14-11 nothing is
        # asked first: DuckDB's branch, and the copy of it `replicate_managers`
        # shipped after it, went with `KS_WRITE_MANAGERS`.
        from datetime import timezone

        from core import pg_managers_write

        return await pg_managers_write.upsert_managers(
            rows, set_at=datetime.now(timezone.utc))

    # How many buyers the admin's sync-all hands over per portion
    # (`web/routes/api/admin.py`, `_sync_all_buyers`), each under the
    # heavy-job lock. It was DuckDB's transaction size — 6 500 buyers
    # committed in 22 s under the production 4 GB limit and 8 000 ran out of
    # memory — and stays the portion the route takes the lock for; chain 4's
    # own writer splits each portion again (`pg_buyers_write.WRITE_PORTION`).
    BUYER_WRITE_PORTION = 1000

    async def upsert_buyers(self, buyers: List["Buyer"], *,
                            skipped_out: Optional[List[int]] = None) -> int:
        """Write buyers to Postgres — chain 4's writer
        (`core/pg_buyers_write.py`), which writes the buyers, their contacts
        and their gender verdicts in Postgres alone. `skipped_out` is filled
        with the buyers Postgres would refuse, which the chain skips rather
        than raising for.

        Until step 14's S14-11 this wrote DuckDB in portions and mirrored each
        portion to Postgres (`pg_buyers.mirror_buyers`) whenever the chain's
        flag said duckdb; S14-00 made the answer `postgres`, a constant, and
        S14-11 deleted the DuckDB tail (`_upsert_buyer_portion`) with the
        flag.

        Returns:
            Number of buyers upserted
        """
        from core import pg_buyers_write

        return await pg_buyers_write.upsert_buyers(
            buyers or [], skipped_out=skipped_out)

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
        from core import pg_buyer_sync_read
        from core.sql_dialect import POSTGRES, render_tables

        # Use silver_orders which has is_return flag, prioritize returns
        # Also include buyers with NULL full_name (need re-sync)
        # Use subquery to properly handle DISTINCT with ORDER BY
        #
        # `buyer_id DESC` is new: buyers level on both keys fell either side of
        # LIMIT in engine order, so the two engines could fetch different
        # buyers in the same hour.
        sql = """
                SELECT buyer_id FROM (
                    SELECT s.buyer_id,
                           MAX(CASE WHEN s.is_return THEN 1 ELSE 0 END) as has_return,
                           MAX(s.order_date) as latest_order
                    FROM {silver_orders} s
                    LEFT JOIN {buyers} b ON s.buyer_id = b.id
                    WHERE s.buyer_id IS NOT NULL
                      AND (b.id IS NULL OR b.full_name IS NULL OR b.full_name = '')
                    GROUP BY s.buyer_id
                ) sub
                ORDER BY has_return DESC, latest_order DESC, buyer_id DESC
                LIMIT ?
            """
        # Postgres, and nowhere else (step 14, S14-07). Chain 4 writes the
        # buyers there (`core/pg_buyers_write.py`), so DuckDB's copy has
        # stopped, and selecting from it would fetch the same buyers from
        # KeyCRM every hour for ever. Neither the chain's mode nor
        # `KS_READ_BUYER_SYNC` is asked any more: the first is `postgres`, a
        # constant since S14-00, and the second chose DuckDB before it. No
        # fallback — `core/pg_buyer_sync_read.py` says why, and where a
        # failure is contained instead.
        rows = await pg_buyer_sync_read.fetch(render_tables(sql, POSTGRES), [limit])
        return [row[0] for row in rows]

    async def update_manager_stats(self) -> int:
        """Update manager order statistics from the orders — chain 5's
        writer (`pg_managers_write.update_manager_stats`), against
        `bronze.orders`.

        Updates first_order_date, last_order_date, and order_count for all
        managers. Until step 14's S14-11 DuckDB's branch ran the same body
        against DuckDB's orders and replicated the result
        (`replicate_managers`), or left the stats alone while chain 3 owned
        the orders (`duckdb_orders_frozen`); S14-11 deleted both with the
        flag.

        Returns:
            Number of managers updated
        """
        from core import pg_managers_write

        return await pg_managers_write.update_manager_stats()

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
        nothing a reader can see. The writer raises the derivation signal in
        the decision's own transaction (`pg_managers_write`), so that is true
        for every caller, not just the one that remembered.

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

        # Chain 5: the decision is written where it is derived, in one
        # Postgres transaction that also raises the derivation signal. (The
        # DuckDB warehouse mark that followed it, a no-op since step 13, went
        # with the flag in step 14's S14-15.) A backdate behind the manager's latest change is
        # refused there (`BackdateBehindLatest`, OD-C5-1 (a)). DuckDB's own
        # path — one transaction closing the open interval and opening the
        # new one — went with the flag in step 14's S14-11.
        from datetime import timezone

        from core import pg_managers_write

        await pg_managers_write.set_manager_retail_status(
            manager_id, is_retail, effective_from, set_by, note,
            set_at=datetime.now(timezone.utc))

    async def get_all_managers(self) -> List[Dict[str, Any]]:
        """Get all managers with their statistics.

        Returns:
            List of manager dicts with id, name, status, is_retail, order_count, etc.
        """
        # Chain 5: from Postgres, with no fallback — DuckDB's copy is frozen
        # (`core/pg_managers_read.py`). DuckDB's read went with the flag in
        # step 14's S14-11.
        from core import pg_managers_read

        return await pg_managers_read.fetch_all_managers()

    async def get_latest_order_time(self) -> Optional[datetime]:
        """The latest order `updated_at` — the full sync's checkpoint. From
        Postgres, where chain 3 writes the orders (`core/pg_orders_read.py`):
        DuckDB's MAX is the moment of the flip, for ever."""
        from core import pg_orders_read

        return await pg_orders_read.latest_order_time()

    async def orders_held(self) -> int:
        """How many orders Postgres holds — the boot's "is there any history
        at all?". DuckDB's copy never grows since chain 3's flip, and a fresh
        DuckDB on a latched host would read as empty on every boot and pull
        730 days from KeyCRM each time. Raises what the read raises."""
        from core import pg_orders_read

        return await pg_orders_read.order_count()

    async def get_stats(self) -> Dict[str, Any]:
        """Get database statistics.

        No buyer counts: chain 4 writes the buyers in Postgres
        (`core/pg_buyers_write.py`), so DuckDB's copy has stopped, and a count
        that never moves on /health/detailed reads as a sync that stopped.
        Until step 14's S14-11 they were published while the chain's flag
        said duckdb; the live count is `bronze.buyers`
        (`pg_buyer_sync_read.count_buyers`).
        """
        async with self.connection() as conn:
            orders_count = conn.execute("SELECT COUNT(*) FROM orders").fetchone()[0]
            products_count = conn.execute("SELECT COUNT(*) FROM products").fetchone()[0]
            categories_count = conn.execute("SELECT COUNT(*) FROM categories").fetchone()[0]
            expenses_count = conn.execute("SELECT COUNT(*) FROM expenses").fetchone()[0]
            expense_types_count = conn.execute("SELECT COUNT(*) FROM expense_types").fetchone()[0]

            min_date = conn.execute("SELECT MIN(DATE(ordered_at)) FROM orders").fetchone()[0]
            max_date = conn.execute("SELECT MAX(DATE(ordered_at)) FROM orders").fetchone()[0]

            # The age of the last successful data-quality run per layer used
            # to ride here, read out of DuckDB's journal inside this
            # connection; `/api/health` replaced it with Postgres's since
            # chain 9 moved, and step 14's S14-10 deleted DuckDB's reader.

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
                "db_size_mb": round(self.db_path.stat().st_size / 1024 / 1024, 2) if self.db_path.exists() else 0,
            }


# ═══════════════════════════════════════════════════════════════════════════════
# SINGLETON INSTANCE
# ═══════════════════════════════════════════════════════════════════════════════

_store_instance: Optional[DuckDBStore] = None
_store_lock = asyncio.Lock()


async def get_store() -> DuckDBStore:
    """Get singleton DuckDB store instance (coroutine-safe).

    Under `KS_DUCKDB=off` the store is published **unconnected** (stage 5,
    PR-1). About 150 call sites reach `get_store()`, and most of them only
    hold the store as the object their router hangs off — `_users_run`,
    `_perms_run`, `_sms_run`, every `KS_READ_*` router, every Postgres
    writer — and decide where the read goes before they touch a connection.
    Refusing here refused all of them for what none of them was about to do.
    The constructor touches no file; `connection()` connects lazily, through
    `connect()`, whose first act is the switch, so the tripwire now stands
    where the file is actually reached and names that site.
    """
    global _store_instance
    async with _store_lock:
        if _store_instance is None:
            if duckdb_switch.is_off():
                _store_instance = DuckDBStore()
                return _store_instance
            # Published only once connected. It used to be assigned before
            # `connect()`, so a connect that raised still left a singleton
            # behind, and web's startup — which catches a failed first
            # sync and asks for the store again to serve stale data — was
            # handed that store instead of a second attempt at opening it.
            store = DuckDBStore()
            await store.connect()
            _store_instance = store
    return _store_instance


async def close_store() -> None:
    """Close singleton store instance."""
    global _store_instance
    if _store_instance:
        await _store_instance.close()
        _store_instance = None

