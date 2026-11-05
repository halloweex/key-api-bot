"""The analytics store: the repository mixins and the writers' doors, and no DuckDB.

Every read the dashboard serves and every write the sync makes goes through
this object, and none of them reaches DuckDB. The reads are the nine
repository mixins (`core/repositories/`), each a router over Postgres (or
ClickHouse for the cohorts) that refuses a failed engine with a 503 naming
its tab (`core.read_fallback.refuse`). The writes are the chains' writers
(`core/pg_*_write.py`), reached through the methods below because their
callers — the sync, the scheduler's jobs, the admin routes — hold the store.

Until step 14's S14-18 this was `DuckDBStore` in `core/duckdb_store.py`: the
same mixins on top of a DuckDB connection, its schema and its migrations, its
checkpoint and nightly backup, its FATAL bookkeeping and its counts. Web had
not opened the file since 2026-10-16 (`KS_DUCKDB=off`, a constant in web
since S14-00), so every one of those had no caller left, and S14-18 deleted
them. `core/duckdb_store.py` stays one release as a shim
(`DuckDBStore = AnalyticsStore`, `get_store`, `close_store`) for the callers
that still import it by that name; S14-21 renames them.

`connection()` stays one release too, as a refusal (`StoreRemoved`): a
caller nobody found would otherwise meet an `AttributeError` — a 500 naming
nothing — where it now gets the counted 503 naming `duckdb` that every read
refused gets, and `read_refused` pages it.

The file itself is not this module's: `data/analytics.duckdb`, its backups,
the export and the Arks stay on disk, untouched, until the final stage.
"""
import asyncio
import logging
from datetime import datetime, date
from typing import Optional, List, Dict, Any, NoReturn, TYPE_CHECKING
from zoneinfo import ZoneInfo

if TYPE_CHECKING:  # pragma: no cover — the annotation's name, no import cost
    from core.models import Buyer

from core import read_fallback
from core.duckdb_constants import DEFAULT_TZ, DISPLAY_TIMEZONE
from core.repositories import (
    UsersMixin, TrafficMixin, CustomersMixin, GoalsMixin,
    InventoryMixin, ExpensesMixin, RevenueMixin, ProductsIntelMixin,
    MarginMixin,
)

logger = logging.getLogger(__name__)


class StoreRemoved(read_fallback.ReadUnavailable):
    """A caller reached for the DuckDB connection this store no longer has.

    A `ReadUnavailable` naming `duckdb`, so `web/main.py` answers it as it
    answers every refused read: a 503 carrying the surface, never a 500. It
    is counted where it is raised (`read_fallback.refuse`, published as
    `read_fallback_mode.refused.duckdb` on `/api/health`) and the canary
    pages it as `read_refused`. Raised by `AnalyticsStore.connection()` and
    nothing else; it goes with that stub (S14-21)."""

    def __init__(self) -> None:
        super().__init__("duckdb")


class AnalyticsStore(
    UsersMixin, TrafficMixin, CustomersMixin, GoalsMixin,
    InventoryMixin, ExpensesMixin, RevenueMixin, ProductsIntelMixin,
    MarginMixin,
):
    """The repository mixins and the writers' doors, with no state of its own.

    It holds no connection: each router acquires its engine's pool when it
    reads, and each writer its transaction when it writes. So building one
    touches nothing, and one instance (`get_store()`) serves the process.
    """

    def connection(self) -> NoReturn:
        """Refused: the DuckDB connection was removed in step 14's S14-18.

        Kept one release so that a caller still written `async with
        store.connection()` — none is known; the stage 5 sweep found none
        before it was deleted — gets the counted 503 naming `duckdb` and the
        `read_refused` page, not an `AttributeError`. It depends on nothing
        DuckDB: not the switch, not the driver, not a handler of its own."""
        cause = RuntimeError(
            "the DuckDB half of the analytics store was removed in step 14's "
            "S14-18; this caller reached for its connection")
        read_fallback.refuse("duckdb", cause)
        raise StoreRemoved() from cause

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


_store_instance: Optional[AnalyticsStore] = None
_store_lock = asyncio.Lock()


async def get_store() -> AnalyticsStore:
    """The process's store (coroutine-safe). Building it opens nothing — it
    holds no connection — so this never fails on an engine being down; the
    read or write that needs one does, where it can say which."""
    global _store_instance
    async with _store_lock:
        if _store_instance is None:
            _store_instance = AnalyticsStore()
    return _store_instance


async def close_store() -> None:
    """Forget the process's store. There is nothing to close: the pools are
    `core.pg`'s and closed there. A later `get_store()` builds a new one, as
    a new lifespan and the tests expect."""
    global _store_instance
    _store_instance = None
