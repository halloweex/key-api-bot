"""
Sync service for keeping DuckDB in sync with KeyCRM API.

Handles incremental synchronization of orders, products, and categories.

Features:
- Full sync: Initial load of all historical data (in 90-day chunks)
- Incremental sync: Only fetch new/updated records
- Background sync: Periodic updates running in the background
- Observability: Correlation IDs and timing metrics
"""
import asyncio
import contextlib
from datetime import datetime, timedelta
from typing import Optional, Dict, Any
from zoneinfo import ZoneInfo

from core.keycrm import get_async_client
from core.duckdb_store import get_store, DuckDBStore
from core.exceptions import KeyCRMError, KeyCRMConnectionError, KeyCRMAPIError
from core.observability import get_logger, correlation_context
from core.events import (
    events,
    SyncEvent,
    emit_sync_started,
    emit_sync_completed,
    emit_sync_failed,
    emit_orders_synced,
)
from core.meilisearch_client import get_meili_client, init_meilisearch
from core.pg_landing import (
    mirror_categories,
    mirror_expenses,
    mirror_products,
)
from bot.config import DEFAULT_TIMEZONE

logger = get_logger(__name__)

DEFAULT_TZ = ZoneInfo(DEFAULT_TIMEZONE)


def _get_max_updated_at(orders: list) -> Optional[datetime]:
    """
    Extract max updated_at from a list of orders.

    Used for checkpoint - ensures we use SOURCE timestamp, not now().
    This prevents data loss when orders are updated during sync.
    """
    if not orders:
        return None

    max_updated = None
    for order in orders:
        updated_str = order.get("updated_at")
        if updated_str:
            try:
                updated = datetime.fromisoformat(updated_str.replace("Z", "+00:00"))
                if max_updated is None or updated > max_updated:
                    max_updated = updated
            except (ValueError, TypeError) as e:
                logger.warning(f"Skipping unparseable updated_at '{updated_str}': {e}")

    return max_updated


class SyncService:
    """
    Service for syncing KeyCRM data to DuckDB.

    Features:
    - Full sync: Initial load of all historical data
    - Incremental sync: Only fetch new/updated records
    - Background sync: Periodic updates in the background
    - Adaptive sync: Exponential backoff when no new orders
    """

    # Adaptive sync configuration
    BACKOFF_BASE_SECONDS = 60       # Base interval
    BACKOFF_MAX_SECONDS = 300       # Max interval (5 minutes)
    BACKOFF_MULTIPLIER = 2          # Exponential multiplier
    OFF_HOURS_START = 23            # 11 PM Kyiv
    OFF_HOURS_END = 7               # 7 AM Kyiv
    OFF_HOURS_INTERVAL = 300        # 5 min during off-hours

    def __init__(self, store: DuckDBStore):
        self.store = store
        self._sync_task: Optional[asyncio.Task] = None
        # Track if ordered_between filter is supported
        self._use_ordered_between: Optional[bool] = None

        # Adaptive sync state
        self._consecutive_empty_syncs = 0
        self._last_sync_time: Optional[datetime] = None
        self._last_orders_found = 0
        self._current_backoff_seconds = self.BACKOFF_BASE_SECONDS

    def _is_off_hours(self) -> bool:
        """Check if current time is during off-hours (low activity period)."""
        now = datetime.now(DEFAULT_TZ)
        hour = now.hour
        # Off-hours: 11 PM to 7 AM
        return hour >= self.OFF_HOURS_START or hour < self.OFF_HOURS_END

    def _should_skip_sync(self) -> tuple[bool, str]:
        """
        Determine if sync should be skipped based on adaptive backoff.

        Returns:
            Tuple of (should_skip, reason)
        """
        if self._last_sync_time is None:
            return False, ""

        now = datetime.now(DEFAULT_TZ)
        elapsed = (now - self._last_sync_time).total_seconds()

        # During off-hours, use longer interval
        if self._is_off_hours():
            if elapsed < self.OFF_HOURS_INTERVAL:
                remaining = int(self.OFF_HOURS_INTERVAL - elapsed)
                return True, f"off-hours mode, next sync in {remaining}s"

        # Use adaptive backoff based on consecutive empty syncs
        if elapsed < self._current_backoff_seconds:
            remaining = int(self._current_backoff_seconds - elapsed)
            return True, f"backoff active ({self._current_backoff_seconds}s interval), next in {remaining}s"

        return False, ""

    def _update_backoff(self, orders_found: int) -> None:
        """Update backoff state based on sync results."""
        if orders_found > 0:
            # Reset backoff when orders are found
            if self._consecutive_empty_syncs > 0:
                logger.info(f"Adaptive sync: backoff reset (found {orders_found} orders)")
            self._consecutive_empty_syncs = 0
            self._current_backoff_seconds = self.BACKOFF_BASE_SECONDS
        else:
            # Increase backoff exponentially
            self._consecutive_empty_syncs += 1
            new_backoff = min(
                self.BACKOFF_BASE_SECONDS * (self.BACKOFF_MULTIPLIER ** self._consecutive_empty_syncs),
                self.BACKOFF_MAX_SECONDS
            )
            if new_backoff != self._current_backoff_seconds:
                self._current_backoff_seconds = int(new_backoff)
                logger.info(
                    f"Adaptive sync: backoff increased to {self._current_backoff_seconds}s "
                    f"({self._consecutive_empty_syncs} empty syncs)"
                )

        self._last_orders_found = orders_found
        self._last_sync_time = datetime.now(DEFAULT_TZ)

    def get_sync_stats(self) -> Dict[str, Any]:
        """Get current adaptive sync statistics."""
        return {
            "consecutive_empty_syncs": self._consecutive_empty_syncs,
            "current_backoff_seconds": self._current_backoff_seconds,
            "last_orders_found": self._last_orders_found,
            "last_sync_time": self._last_sync_time.isoformat() if self._last_sync_time else None,
            "is_off_hours": self._is_off_hours(),
            "effective_interval": self.OFF_HOURS_INTERVAL if self._is_off_hours() else self._current_backoff_seconds,
        }

    async def _fetch_orders_with_date_filter(
        self,
        client,
        start_date: str,
        end_date: str,
        include: str = "products.offer,manager,buyer,expenses",
        include_updated: bool = True
    ) -> list:
        """
        Fetch orders with date filters.

        Uses BOTH created_between AND updated_between to catch:
        1. New orders (created_between)
        2. Orders with status changes (updated_between)

        Args:
            include_updated: If True, also fetch by updated_between (for incremental sync)
        """
        orders_by_id = {}

        # Fetch by created_between
        logger.debug(f"Fetching orders created between {start_date} and {end_date}")
        params = {
            "include": include,
            "filter[created_between]": f"{start_date}, {end_date}",
        }
        async for batch in client.paginate("order", params=params, page_size=50):
            for order in batch:
                orders_by_id[order["id"]] = order

        created_count = len(orders_by_id)

        # Also fetch by updated_between to catch status changes
        if include_updated:
            logger.debug(f"Fetching orders updated between {start_date} and {end_date}")
            params = {
                "include": include,
                "filter[updated_between]": f"{start_date}, {end_date}",
            }
            try:
                async for batch in client.paginate("order", params=params, page_size=50):
                    for order in batch:
                        orders_by_id[order["id"]] = order  # Overwrites with latest data
            except KeyCRMError as e:
                # filter[updated_between] may not be supported - fall back gracefully
                logger.warning(f"filter[updated_between] failed: {e}")

        updated_count = len(orders_by_id) - created_count
        if updated_count > 0:
            logger.info(f"Fetched {created_count} created + {updated_count} updated orders")

        return list(orders_by_id.values())

    async def _upsert_orders_with_expenses(
        self, orders: list, force_update: bool = False, skip_products: bool = False,
        changed_ids_out: "list[int] | None" = None,
    ) -> tuple:
        """Upsert orders and their expenses.

        Args:
            orders: List of order dicts from API
            force_update: If True, force update all orders regardless of updated_at.
                         Use for status refresh since KeyCRM doesn't update updated_at
                         when order status changes.
            skip_products: If True, skip product deletion/insertion to avoid OOM
                          during status refresh (~2000 orders worth of products).
            changed_ids_out: If given, is extended with the ids actually WRITTEN,
                          which is a subset of what was fetched. An out-list
                          rather than a wider return because six call sites and
                          a stack of mocks depend on the 2-tuple, and only the
                          incremental sync needs to know what moved.

        Returns:
            Tuple of (order_count, expense_count)
        """
        result = await self.store.upsert_orders(
            orders, force_update=force_update, skip_products=skip_products,
        )
        if changed_ids_out is not None:
            changed_ids_out.extend(result.changed_ids)

        # Batch upsert all expenses in a single transaction
        orders_with_expenses = [o for o in orders if o.get("expenses")]
        expense_count = await self.store.upsert_expenses_batch(orders_with_expenses)
        # And the same rows to Postgres. Never raises — a Postgres fault must
        # not be able to stop a sync — so a miss is carried by the backfill
        # and reported by the daily comparison, not by this line.
        await mirror_expenses(orders_with_expenses)

        return result.count, expense_count

    async def sync_managers(self) -> int:
        """
        Sync managers/users from KeyCRM API.

        Returns:
            Number of managers synced
        """
        logger.info("Syncing managers...")
        try:
            client = await get_async_client()
            managers = []

            async for batch in client.paginate("users", page_size=50):
                managers.extend(batch)

            count = await self.store.upsert_managers(managers)

            # Update manager order statistics
            await self.store.update_manager_stats()
            await self.store.set_last_sync_time("managers")

            logger.info(f"Synced {count} managers from KeyCRM")
            return count
        except KeyCRMConnectionError as e:
            logger.warning(f"Manager sync connection error (will retry): {e}")
            return 0
        except KeyCRMAPIError as e:
            logger.error(f"Manager sync API error: {e}")
            return 0
        except KeyCRMError as e:
            logger.error(f"Manager sync error: {e}")
            return 0

    async def sync_missing_buyers(self, limit: int = 500) -> int:
        """
        Sync buyers that are referenced in orders but not yet in buyers table.

        Fetches buyer details from KeyCRM API for orders that have buyer_id
        but no corresponding buyer record.

        Args:
            limit: Maximum number of buyers to sync per call

        Returns:
            Number of buyers synced
        """
        logger.info("Syncing missing buyers...")
        try:
            # Get buyer IDs from orders that don't have buyer records.
            #
            # A failed selection returns without moving the buyers watermark:
            # the next tick retries, `freshness_buyers` reports a stall that
            # lasts, and the offers and stocks syncs after this one in the same
            # tick still run — an unreadable selection must not cost the
            # inventory an hour. It used to propagate out of the whole tick.
            try:
                missing_ids = await self.store.get_missing_buyer_ids(limit)
            except Exception as e:  # noqa: BLE001 — logged, watermark held
                logger.error(
                    f"Buyer selection failed, buyers watermark not moved: {e}",
                    exc_info=True)
                return 0

            if not missing_ids:
                logger.info("No missing buyers to sync")
                await self.store.set_last_sync_time("buyers")
                return 0

            logger.info(f"Fetching {len(missing_ids)} missing buyers from KeyCRM...")
            client = await get_async_client()
            buyers = await client.fetch_buyers_by_ids(missing_ids)

            if buyers:
                count = await self.store.upsert_buyers(buyers)
                # Step 2 of «Одна бронза»: the same parsed batch, two stores —
                # `mirror_products`' shape. Never raises; failures land in
                # meta.mirror_state where the daily comparison reads them.
                from core.pg_buyers import mirror_buyers

                await mirror_buyers(buyers)
                await self.store.set_last_sync_time("buyers")
                logger.info(f"Synced {count} buyers from KeyCRM")
                return count

            await self.store.set_last_sync_time("buyers")
            return 0
        except KeyCRMConnectionError as e:
            logger.warning(f"Buyer sync connection error (will retry): {e}")
            return 0
        except KeyCRMAPIError as e:
            logger.error(f"Buyer sync API error: {e}")
            return 0
        except KeyCRMError as e:
            logger.error(f"Buyer sync error: {e}")
            return 0

    async def sync_offers(self) -> int:
        """
        Sync offers (product variations) from KeyCRM API.

        Offers link offer_id to product_id, enabling proper joins
        between offer_stocks and products tables.

        Returns:
            Number of offers synced
        """
        logger.info("Syncing offers...")
        try:
            client = await get_async_client()
            offers = await client.fetch_all_offers()

            count = await self.store.upsert_offers(offers)
            await self.store.set_last_sync_time("offers")

            logger.info(f"Synced {count} offers from KeyCRM")
            return count
        except KeyCRMConnectionError as e:
            logger.warning(f"Offers sync connection error (will retry): {e}")
            return 0
        except KeyCRMAPIError as e:
            logger.error(f"Offers sync API error: {e}")
            return 0
        except KeyCRMError as e:
            logger.error(f"Offers sync error: {e}")
            return 0

    async def sync_stocks(self) -> int:
        """
        Sync offer stocks from KeyCRM API.

        Returns:
            Number of stocks synced
        """
        logger.info("Syncing offer stocks...")
        try:
            client = await get_async_client()
            stocks = await client.fetch_all_stocks()

            count = await self.store.upsert_stocks(stocks)
            await self.store.set_last_sync_time("stocks")

            logger.info(f"Synced {count} offer stocks from KeyCRM")
            return count
        except KeyCRMConnectionError as e:
            logger.warning(f"Stock sync connection error (will retry): {e}")
            return 0
        except KeyCRMAPIError as e:
            logger.error(f"Stock sync API error: {e}")
            return 0
        except KeyCRMError as e:
            logger.error(f"Stock sync error: {e}")
            return 0

    async def sync_to_meilisearch(self) -> Dict[str, int]:
        """
        Sync buyers, orders, and products to Meilisearch.

        Incremental: uses the source rows' bookkeeping stamp as a watermark. On
        the 5-minute scheduler tick, only rows touched since the last successful
        Meili sync are pushed — a no-op tick costs one MAX() query, not a full
        37K re-index. First run (no watermark) falls back to a full sync.

        `KS_READ_SEARCH_INDEX` decides which store the index is built from.
        Postgres keeps a watermark key of its own, because its stamp is
        `mirrored_at` on a later clock than DuckDB's `synced_at` — see
        `core/pg_search_index_read.py`. Everything downstream of the frames
        (the `isoformat` calls, `to_dict`, the sanitiser, the index) is the same
        code for both engines, which is what keeps the documents identical.

        Returns:
            Dict with counts for each synced entity
        """
        from core import pg_search_index_read as pg_index

        stats = {"buyers": 0, "orders": 0, "products": 0}

        try:
            meili = get_meili_client()

            # The engine is chosen once: the watermark belongs to it. Only what
            # the index READS moves — the watermark is still `sync_metadata`
            # bookkeeping, written by DuckDB until that table's stage-4 chain.
            # Under KS_READ_FALLBACK=off a switch with no address is refused
            # rather than read from DuckDB; the handler below skips the step.
            from core import read_fallback
            read_fallback.no_address("search_index", pg_index)
            use_pg = pg_index.enabled() and pg_index.available()
            watermark_key = pg_index.WATERMARK_KEY if use_pg else "meilisearch"
            last_sync = await self.store.get_last_sync_time(watermark_key)

            # If nothing has been stamped since the watermark, skip entirely.
            high_watermark = (await pg_index.high_watermark() if use_pg
                              else await self._meili_high_watermark())
            if last_sync and high_watermark and high_watermark <= last_sync:
                logger.debug("Meilisearch sync: no changes since %s, skipping", last_sync)
                return stats

            full_sync = last_sync is None
            since = None if full_sync else last_sync
            logger.info(
                "Syncing data to Meilisearch... (%s, from %s)",
                "full" if full_sync else f"incremental since {last_sync.isoformat()}",
                "postgres" if use_pg else "duckdb",
            )

            # ── Buyers ─────────────────────────────────────────────────────
            # Incremental: only buyers whose row was re-synced (profile edit)
            # OR whose order activity was re-synced (order_count changed).
            buyers_df = (await pg_index.buyers_frame(since) if use_pg
                         else await self._meili_buyers_frame(since))
            if not buyers_df.empty:
                if 'created_at' in buyers_df.columns:
                    buyers_df['created_at'] = buyers_df['created_at'].apply(
                        lambda x: x.isoformat() if x else None
                    )
                stats["buyers"] = await meili.index_buyers(buyers_df.to_dict('records'))

            # ── Orders ─────────────────────────────────────────────────────
            MEILI_CHUNK = 10_000
            offset = 0
            while True:
                orders_df = (
                    await pg_index.orders_frame(since, MEILI_CHUNK, offset) if use_pg
                    else await self._meili_orders_frame(since, MEILI_CHUNK, offset))

                if orders_df.empty:
                    break

                if 'ordered_at' in orders_df.columns:
                    orders_df['ordered_at'] = orders_df['ordered_at'].apply(
                        lambda x: x.isoformat() if x else None
                    )
                if 'order_date' in orders_df.columns:
                    orders_df['order_date'] = orders_df['order_date'].apply(
                        lambda x: x.isoformat() if x else None
                    )
                stats["orders"] += await meili.index_orders(orders_df.to_dict('records'))
                offset += MEILI_CHUNK

                if len(orders_df) < MEILI_CHUNK:
                    break

            # ── Products ───────────────────────────────────────────────────
            products_df = (await pg_index.products_frame(since) if use_pg
                           else await self._meili_products_frame(since))
            if not products_df.empty:
                stats["products"] = await meili.index_products(products_df.to_dict('records'))

            logger.info(f"Meilisearch sync complete: {stats}")
            # Persist the watermark we actually covered, not wall-clock now().
            # Using now() would skip rows written between our MAX() and this line.
            if high_watermark:
                await self.store.set_last_sync_time(watermark_key, timestamp=high_watermark)
            else:
                await self.store.set_last_sync_time(watermark_key)
            return stats

        except Exception as e:
            logger.error(f"Meilisearch sync error: {e}")
            return stats

    # ── The DuckDB reads behind the Meilisearch index ────────────────────────
    # Moved here verbatim — the SQL is copied, not retyped — so that the index
    # method can choose an engine in one line per entity and the processing
    # after it stays identical for both. Their Postgres twins, and the reasons
    # the frames must match down to the dtype, are in
    # `core/pg_search_index_read.py`.

    async def _meili_high_watermark(self):
        async with self.store.connection() as conn:
            hw_row = conn.execute("""
                    SELECT MAX(ts) FROM (
                        SELECT MAX(synced_at) AS ts FROM orders
                        UNION ALL SELECT MAX(synced_at) FROM buyers
                        UNION ALL SELECT MAX(synced_at) FROM products
                    )
                """).fetchone()
        return hw_row[0] if hw_row else None

    async def _meili_buyers_frame(self, since):
        async with self.store.connection() as conn:
            if since is None:
                return conn.execute("""
                        SELECT
                            b.id, b.full_name, b.phone, b.email, b.city, b.note,
                            b.manager_id, b.created_at,
                            COUNT(DISTINCT o.id) as order_count
                        FROM buyers b
                        LEFT JOIN silver_orders o ON b.id = o.buyer_id AND NOT o.is_return
                        GROUP BY b.id, b.full_name, b.phone, b.email, b.city,
                                 b.note, b.manager_id, b.created_at
                    """).fetchdf()
            return conn.execute("""
                        WITH touched AS (
                            SELECT id FROM buyers WHERE synced_at > ?
                            UNION
                            SELECT DISTINCT buyer_id AS id FROM orders
                            WHERE synced_at > ? AND buyer_id IS NOT NULL
                        )
                        SELECT
                            b.id, b.full_name, b.phone, b.email, b.city, b.note,
                            b.manager_id, b.created_at,
                            COUNT(DISTINCT o.id) as order_count
                        FROM buyers b
                        JOIN touched t ON t.id = b.id
                        LEFT JOIN silver_orders o ON b.id = o.buyer_id AND NOT o.is_return
                        GROUP BY b.id, b.full_name, b.phone, b.email, b.city,
                                 b.note, b.manager_id, b.created_at
                    """, [since, since]).fetchdf()

    async def _meili_orders_frame(self, since, limit: int, offset: int):
        async with self.store.connection() as conn:
            if since is None:
                return conn.execute("""
                            SELECT
                                o.id, o.grand_total, o.ordered_at, o.status_id,
                                o.source_name, o.buyer_id, o.order_date,
                                b.full_name as buyer_name
                            FROM silver_orders o
                            LEFT JOIN buyers b ON o.buyer_id = b.id
                            ORDER BY o.ordered_at DESC
                            LIMIT ? OFFSET ?
                        """, [limit, offset]).fetchdf()
            return conn.execute("""
                            SELECT
                                o.id, o.grand_total, o.ordered_at, o.status_id,
                                o.source_name, o.buyer_id, o.order_date,
                                b.full_name as buyer_name
                            FROM silver_orders o
                            LEFT JOIN orders src ON src.id = o.id
                            LEFT JOIN buyers b ON o.buyer_id = b.id
                            WHERE src.synced_at > ? OR b.synced_at > ?
                            ORDER BY o.ordered_at DESC
                            LIMIT ? OFFSET ?
                        """, [since, since, limit, offset]).fetchdf()

    async def _meili_products_frame(self, since):
        async with self.store.connection() as conn:
            if since is None:
                return conn.execute("""
                        SELECT
                            p.id, p.name, p.sku, p.brand, p.price, p.category_id,
                            c.name as category_name
                        FROM products p
                        LEFT JOIN categories c ON p.category_id = c.id
                    """).fetchdf()
            return conn.execute("""
                        SELECT
                            p.id, p.name, p.sku, p.brand, p.price, p.category_id,
                            c.name as category_name
                        FROM products p
                        LEFT JOIN categories c ON p.category_id = c.id
                        WHERE p.synced_at > ?
                    """, [since]).fetchdf()

    async def full_sync(
        self, days_back: int = 730, force_update: bool = False,
    ) -> Dict[str, Any]:
        """
        Perform full sync of all data from KeyCRM.

        Args:
            days_back: Number of days of historical data to sync
            force_update: rewrite every order in the window even when its
                `updated_at` has not moved — what `force_resync` asks for,
                because KeyCRM does not bump `updated_at` on every change
                and a resync exists to overwrite what we hold.

        Returns:
            Dict with sync statistics
        """
        with correlation_context() as corr_id:
            logger.info(
                f"Starting full sync (last {days_back} days)",
                extra={"days_back": days_back, "sync_type": "full"}
            )

        stats = {"orders": 0, "products": 0, "categories": 0, "expense_types": 0, "expenses": 0, "managers": 0, "offers": 0, "stocks": 0}

        try:
            client = await get_async_client()

            # Sync managers first (needed for retail/b2b filtering)
            stats["managers"] = await self.sync_managers()

            # Fetch categories, expense_types, and products in parallel (independent data)
            logger.info("Syncing categories, expense types, and products in parallel...")

            async def fetch_categories():
                items = []
                async for batch in client.paginate("products/categories", page_size=50):
                    items.extend(batch)
                return items

            async def fetch_expense_types():
                items = []
                async for batch in client.paginate("order/expense-type", page_size=50):
                    items.extend(batch)
                return items

            async def fetch_products():
                items = []
                async for batch in client.paginate("products", params={"include": "custom_fields"}, page_size=50):
                    items.extend(batch)
                return items

            # Parallel fetch (3x faster than sequential)
            categories, expense_types, products = await asyncio.gather(
                fetch_categories(),
                fetch_expense_types(),
                fetch_products()
            )

            # Upsert to database (sequential due to DuckDB single-writer)
            stats["categories"] = await self.store.upsert_categories(categories)
            await self.store.set_last_sync_time("categories")
            # Step 05. Same payloads, read through the same `landing_rows`;
            # never raises, so a Postgres fault cannot stop a sync.
            await mirror_categories(categories)

            stats["expense_types"] = await self.store.upsert_expense_types(expense_types)
            await self.store.set_last_sync_time("expense_types")
            # Not mirrored from here. KeyCRM serves this dictionary only to the
            # weekly full sync, so a payload-fed mirror would leave Postgres
            # empty for up to a week — and `/expenses` renders the breakdown by
            # *name*, so that is a collapsed chart and an empty filter, not a
            # freshness detail. It rides the hourly `replicate_operational`
            # instead, out of DuckDB, which holds it every minute of that week.
            # `bronze.offer_stocks` sits in that family for the same reason.

            stats["products"] = await self.store.upsert_products(products)
            await self.store.set_last_sync_time("products")
            await mirror_products(products)

            # Sync orders with expenses - in chunks to avoid pagination limit (100 pages × 50 = 5000 orders max)
            # IMPORTANT: Save each chunk immediately to preserve progress on timeout/crash
            logger.info("Syncing orders...")
            final_end_date = datetime.now(DEFAULT_TZ) + timedelta(days=1)
            chunk_days = 30  # Sync in 1-month chunks to limit memory usage during bulk insert
            chunk_num = 0

            current_start = datetime.now(DEFAULT_TZ) - timedelta(days=days_back)

            while current_start < final_end_date:
                chunk_num += 1
                current_end = min(current_start + timedelta(days=chunk_days), final_end_date)
                logger.info(f"Chunk {chunk_num}: Fetching orders from {current_start.strftime('%Y-%m-%d')} to {current_end.strftime('%Y-%m-%d')}...")

                chunk_orders = await self._fetch_orders_with_date_filter(
                    client,
                    current_start.strftime('%Y-%m-%d'),
                    current_end.strftime('%Y-%m-%d')
                )
                logger.info(f"  Chunk {chunk_num}: Got {len(chunk_orders)} orders, saving to DB...")

                # Save chunk immediately to preserve progress
                if chunk_orders:
                    order_count, expense_count = await self._upsert_orders_with_expenses(
                        chunk_orders,
                        force_update=force_update,
                    )
                    stats["orders"] += order_count
                    stats["expenses"] += expense_count
                    logger.info(f"  Chunk {chunk_num}: Saved {order_count} orders, {expense_count} expenses")

                    # Force WAL checkpoint after each chunk to prevent WAL corruption
                    # on aarch64 (DuckDB 1.4.x bug with large WAL files)
                    await self.store.checkpoint()

                current_start = current_end + timedelta(days=1)

            logger.info(f"All chunks complete. Total: {stats['orders']} orders, {stats['expenses']} expenses")

            # Sync offers (needed for proper stock-to-product linking)
            stats["offers"] = await self.sync_offers()

            # Sync stocks
            stats["stocks"] = await self.sync_stocks()

            # Refresh Layer 1: sku_inventory_status
            await self.store.refresh_sku_inventory_status()

            # Record Layer 2: daily per-SKU snapshot
            await self.store.record_sku_inventory_snapshot()

            # Refresh warehouse layers (Silver → Gold)
            await self.store.refresh_warehouse_layers(trigger="full_sync")

            # Update sync checkpoint with latest order timestamp
            last_order_time = await self.store.get_latest_order_time()
            await self.store.set_last_sync_time("orders", last_order_time)
            logger.info(f"Full sync complete: {stats}, checkpoint: {last_order_time}")

        except KeyCRMConnectionError as e:
            logger.error(f"Full sync connection error: {e}", exc_info=True)
            raise
        except KeyCRMAPIError as e:
            logger.error(f"Full sync API error (status={e.status_code}): {e}", exc_info=True)
            raise
        except KeyCRMError as e:
            logger.error(f"Full sync error: {e}", exc_info=True)
            raise

        return stats

    async def incremental_sync(self) -> Dict[str, Any]:
        """
        Perform incremental sync - only fetch new/updated data since last sync.

        Uses adaptive backoff to reduce API calls during quiet periods:
        - Exponential backoff (60s → 120s → 240s → 300s) when no new orders
        - Extended intervals (5 min) during off-hours (11 PM - 7 AM Kyiv)
        - Instant reset to 60s when new orders are found

        Returns:
            Dict with sync statistics (includes "skipped" key if sync was skipped)
        """
        import time

        # Check if we should skip this sync cycle (adaptive backoff)
        should_skip, skip_reason = self._should_skip_sync()
        if should_skip:
            logger.info(f"Adaptive sync: skipped ({skip_reason})")
            return {"skipped": True, "reason": skip_reason}

        start_time = time.perf_counter()
        stats = {"orders": 0, "products": 0, "categories": 0, "expenses": 0, "managers": 0, "buyers": 0, "offers": 0, "stocks": 0}
        error_occurred = None

        # Emit sync started event
        await emit_sync_started("incremental")

        try:
            client = await get_async_client()

            # Get last sync times
            last_orders_sync = await self.store.get_last_sync_time("orders")
            last_products_sync = await self.store.get_last_sync_time("products")

            # Default to 1 hour ago if never synced
            if not last_orders_sync:
                last_orders_sync = datetime.now(DEFAULT_TZ) - timedelta(hours=1)

            # Add buffer for API delays - extended to 24 hours to catch backdated orders
            sync_from = last_orders_sync - timedelta(hours=24)
            sync_to = datetime.now(DEFAULT_TZ) + timedelta(minutes=5)

            # Sync new orders with expenses using the smart date filter helper
            # Explicitly localise to DEFAULT_TZ before formatting to survive DST transitions
            orders = await self._fetch_orders_with_date_filter(
                client,
                sync_from.astimezone(DEFAULT_TZ).strftime('%Y-%m-%d %H:%M:%S'),
                sync_to.astimezone(DEFAULT_TZ).strftime('%Y-%m-%d %H:%M:%S'),
            )

            # Ids actually WRITTEN, not ids fetched. The sync window is the
            # trailing 24h, so `orders` is ~200 rows on every run whether or not
            # anything about them moved; marking all of them dirty cascaded to
            # their buyers' full histories (~1300 orders) and 583 distinct dates,
            # and rebuilt Gold for all of it every two minutes around the clock.
            changed_ids: list[int] = []
            if orders:
                order_count, expense_count = await self._upsert_orders_with_expenses(
                    orders, changed_ids_out=changed_ids,
                )
                stats["orders"] = order_count
                stats["expenses"] = expense_count
                # Mark warehouse dirty only for orders that actually changed
                # (separate job handles refresh), and mark it HERE, adjacent to
                # the write. It used to happen at the end of the tick, after
                # the hourly catalogue/manager/buyer/stock branches — so a 429
                # on the products page skipped the mark and the orders just
                # written stayed out of Silver/Gold until the 05:15 refresh.
                #
                # This used to key off stats["orders"], which counts a row
                # already in the desired state as a success — so it was ~200
                # every cycle forever and the warehouse was permanently dirty.
                # Nothing changing now means nothing to rebuild.
                if changed_ids:
                    await self.store.mark_warehouse_dirty(changed_ids)
                # Use max(updated_at) from SOURCE data, not now()
                max_updated = _get_max_updated_at(orders)
                # Guard against checkpoint rollback — never go backward
                if max_updated and (not last_orders_sync or max_updated >= last_orders_sync):
                    await self.store.set_last_sync_time("orders", max_updated)
                else:
                    # Source returned stale timestamps; advance to sync_to instead
                    await self.store.set_last_sync_time("orders", sync_to)
                logger.info(f"Incremental sync: {stats['orders']} orders, {stats['expenses']} expenses, checkpoint: {max_updated}")

                # Emit orders synced event
                await events.emit(SyncEvent.ORDERS_SYNCED, {
                    "count": order_count,
                    "expenses": expense_count,
                    "checkpoint": max_updated.isoformat() if max_updated else None,
                })
            else:
                # No new orders — do NOT advance checkpoint.
                # The 24h buffer in sync_from already prevents re-scanning old data,
                # and advancing here risks skipping orders created during the window.
                logger.debug("No new orders in sync window, checkpoint unchanged")

            # Sync products less frequently (every hour)
            if not last_products_sync or (datetime.now(DEFAULT_TZ) - last_products_sync).total_seconds() > 3600:
                logger.info("Syncing products (hourly)...")
                products = []
                async for batch in client.paginate("products", params={"include": "custom_fields"}, page_size=50):
                    products.extend(batch)
                stats["products"] = await self.store.upsert_products(products)
                await self.store.set_last_sync_time("products")
                await mirror_products(products)

                # Emit products synced event
                await events.emit(SyncEvent.PRODUCTS_SYNCED, {"count": stats["products"]})

            # Sync managers daily (86400 seconds = 24 hours)
            last_managers_sync = await self.store.get_last_sync_time("managers")
            if not last_managers_sync or (datetime.now(DEFAULT_TZ) - last_managers_sync).total_seconds() > 86400:
                stats["managers"] = await self.sync_managers()

            # Sync missing buyers (fetch buyer details for orders that don't have them)
            last_buyers_sync = await self.store.get_last_sync_time("buyers")
            if not last_buyers_sync or (datetime.now(DEFAULT_TZ) - last_buyers_sync).total_seconds() > 3600:
                stats["buyers"] = await self.sync_missing_buyers()

            # Sync offers hourly (needed for proper stock-to-product linking)
            last_offers_sync = await self.store.get_last_sync_time("offers")
            if not last_offers_sync or (datetime.now(DEFAULT_TZ) - last_offers_sync).total_seconds() > 3600:
                stats["offers"] = await self.sync_offers()

            # Sync stocks hourly (same frequency as products)
            last_stocks_sync = await self.store.get_last_sync_time("stocks")
            if not last_stocks_sync or (datetime.now(DEFAULT_TZ) - last_stocks_sync).total_seconds() > 3600:
                stats["stocks"] = await self.sync_stocks()

                # Refresh Layer 1: sku_inventory_status (denormalized current state)
                await self.store.refresh_sku_inventory_status()

                # Record Layer 2: daily per-SKU snapshot
                await self.store.record_sku_inventory_snapshot()

                # Legacy: Record aggregated inventory snapshot
                await self.store.record_inventory_snapshot()

                # Emit inventory updated event
                await events.emit(SyncEvent.INVENTORY_UPDATED, {"stocks_count": stats["stocks"]})

            # Silver reads only `orders`, but Gold joins products and categories,
            # THE CATALOGUE NO LONGER HAS TO ASK FOR ITSELF
            #
            # A renamed product or a re-parented category used to change Gold
            # rows with no order involved, because `gold_daily_products` was
            # the only rebuilt table that joined the catalogue — so this raised
            # a dirty flag that widened *that* layer's scope to every date, and
            # nothing else's.
            #
            # That layer is retired: three tabs and the weekly report each read
            # the order-lines level instead, and the level takes the catalogue
            # name through the same joins at read time. A rename is therefore
            # visible on the next page load rather than on the next rebuild,
            # and there is nothing left for a dirty flag to schedule.

            # Adaptive backoff follows the same number. Keyed on the inflated
            # count it never saw a quiet period, so it sat on the 60s floor
            # around the clock instead of stepping out to 300s overnight.
            self._update_backoff(len(changed_ids))

        except KeyCRMConnectionError as e:
            error_occurred = str(e)
            # On connection error, don't increase backoff (might be temporary)
            self._last_sync_time = datetime.now(DEFAULT_TZ)
            logger.warning(f"Incremental sync connection error (will retry): {e}")
        except KeyCRMAPIError as e:
            error_occurred = str(e)
            logger.error(f"Incremental sync API error: {e}", exc_info=True)
        except KeyCRMError as e:
            error_occurred = str(e)
            logger.error(f"Incremental sync error: {e}", exc_info=True)
        finally:
            elapsed_ms = (time.perf_counter() - start_time) * 1000
            total_records = sum(stats.values())
            logger.info(
                "Incremental sync completed",
                extra={"duration_ms": round(elapsed_ms, 2), "stats": stats}
            )

            # Emit sync completed or failed event
            if error_occurred:
                await emit_sync_failed("incremental", error_occurred, stats=stats, duration_ms=elapsed_ms)
            else:
                await emit_sync_completed("incremental", elapsed_ms, total_records, stats=stats)

        return stats

    async def sync_today(self) -> Dict[str, Any]:
        """
        Sync only today's orders for real-time dashboard.

        Returns:
            Dict with sync statistics
        """
        stats = {"orders": 0, "expenses": 0}

        try:
            client = await get_async_client()
            today = datetime.now(DEFAULT_TZ).date()
            tomorrow = today + timedelta(days=1)

            # Sync today's orders using the smart date filter helper
            orders = await self._fetch_orders_with_date_filter(
                client,
                str(today),
                str(tomorrow)
            )

            if orders:
                order_count, expense_count = await self._upsert_orders_with_expenses(
                    orders,
                )
                stats["orders"] = order_count
                stats["expenses"] = expense_count
                logger.debug(f"Today sync: {stats['orders']} orders, {stats['expenses']} expenses")

                # Refresh warehouse layers (Silver → Gold)
                await self.store.refresh_warehouse_layers(trigger="sync_today")

        except KeyCRMConnectionError as e:
            logger.debug(f"Today sync connection error (will retry): {e}")
        except KeyCRMError as e:
            logger.error(f"Today sync error: {e}")

        return stats

    async def refresh_order_statuses(self, days_back: int = 30) -> Dict[str, Any]:
        """
        Re-fetch recent orders to catch status changes.

        KeyCRM does NOT update the `updated_at` field when order status changes,
        so the incremental sync (which relies on updated_between) misses these.
        This method re-fetches orders by created_between to refresh all statuses.

        Args:
            days_back: Number of days to look back (default 30)

        Returns:
            Dict with sync statistics
        """
        stats = {"orders": 0, "expenses": 0, "days_back": days_back}

        try:
            client = await get_async_client()
            start_date = datetime.now(DEFAULT_TZ) - timedelta(days=days_back)
            end_date = datetime.now(DEFAULT_TZ) + timedelta(days=1)

            logger.info(f"Refreshing order statuses for last {days_back} days...")

            # Fetch by created_between ONLY (not updated_between)
            # This ensures we get ALL orders regardless of their updated_at
            orders_by_id = {}
            params = {
                "include": "products.offer,manager,buyer,expenses",
                "filter[created_between]": f"{start_date.strftime('%Y-%m-%d')}, {end_date.strftime('%Y-%m-%d')}",
            }

            async for batch in client.paginate("order", params=params, page_size=50):
                for order in batch:
                    orders_by_id[order["id"]] = order

            orders = list(orders_by_id.values())

            if orders:
                order_ids = [o["id"] for o in orders if "id" in o]

                # Use force_update=True because KeyCRM doesn't update updated_at on status changes
                # Without this, orders with changed status but same updated_at won't be updated
                order_count, expense_count = await self._upsert_orders_with_expenses(
                    orders, force_update=True, skip_products=True,
                )
                stats["orders"] = order_count
                stats["expenses"] = expense_count
                logger.info(f"Status refresh: force-updated {order_count} orders, {expense_count} expenses")

                # Log return orders from API for diagnostics
                api_returns = {
                    o["id"]: o.get("status_id")
                    for o in orders
                    if o.get("status_id") in (19, 21, 22, 23)
                }
                if api_returns:
                    logger.info(
                        f"Status refresh: {len(api_returns)} return orders from API "
                        f"(sample: {dict(list(api_returns.items())[:5])})"
                    )

                # Incremental warehouse refresh — only rebuild Silver for changed
                # orders instead of DELETE+INSERT all 36K rows (prevents OOM)
                await self.store.refresh_warehouse_layers(
                    trigger="status_refresh",
                    changed_order_ids=order_ids,
                )
                # No read-back of Silver here. A sample of twenty return orders
                # used to be compared with DuckDB's `silver_orders`, and all it
                # produced was a log line. `dq_reconciliation` checks every
                # order's status against KeyCRM at 05:30 instead — Bronze in
                # both stores, Silver through the ClickHouse arm — and files a
                # moved status as STATUS_DRIFT where somebody reads it.

        except KeyCRMConnectionError as e:
            logger.warning(f"Status refresh connection error (will retry): {e}")
        except KeyCRMError as e:
            logger.error(f"Status refresh error: {e}")

        return stats

    # An order-by-order repair is one API call each, so a run is bounded and the
    # remainder is picked up on the next one rather than fired off in a burst
    # that trips the rate limiter.
    REPAIR_BATCH_LIMIT = 200

    async def repair_orders(
        self, order_ids, *, limit: int | None = None,
        lock: "asyncio.Lock | None" = None,
    ) -> Dict[str, Any]:
        """Re-fetch specific orders from KeyCRM and upsert them whole.

        The date-window syncs cannot reach these: an order absent from our copy
        is invisible to a delta sync keyed on updated_at, and one whose line
        items failed to write looks complete because the header is there. Both
        need the order pulled again by id.

        Only ever adds or corrects — nothing is deleted, so this is safe to run
        automatically against ids that a comparison against the source flagged.

        `lock` is the scheduler's heavy-job lock, held around the write and the
        dirty mark only — never around the per-id KeyCRM fetches, which would
        hold the sync up for a batch of HTTP calls. Without it a repair landing
        between a refresh's Silver commit and its validation read turned a
        correct rebuild into "validation failed" (a spurious alert and a full
        rebuild), and a repair's mirror call could overwrite a fresher order
        the sync had just written to Postgres. Not reentrant: a caller that
        already holds the lock must not pass it.
        """
        ids = list(dict.fromkeys(int(i) for i in order_ids))
        cap = self.REPAIR_BATCH_LIMIT if limit is None else limit
        attempted, fetched = ids[:cap], []
        failures: Dict[int, str] = {}

        if not attempted:
            return {"requested": 0, "attempted": 0, "repaired": 0,
                    "failed": 0, "remaining": 0, "failures": {}}

        client = await get_async_client()
        for order_id in attempted:
            try:
                order = await client.get_order(
                    order_id, include="products.offer,manager,buyer,expenses",
                )
                if order and order.get("id"):
                    fetched.append(order)
                else:
                    failures[order_id] = "not found in KeyCRM"
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                failures[order_id] = f"{type(exc).__name__}: {exc}"

        repaired = 0
        if fetched:
            held = lock if lock is not None else contextlib.nullcontext()
            async with held:
                # force_update: the whole point is to overwrite what we hold,
                # and KeyCRM's updated_at may well be older than our last touch.
                repaired, _ = await self._upsert_orders_with_expenses(
                    fetched, force_update=True,
                )
                await self.store.mark_warehouse_dirty([o["id"] for o in fetched])

        result = {
            "requested": len(ids),
            "attempted": len(attempted),
            "repaired": int(repaired),
            "failed": len(failures),
            "remaining": max(0, len(ids) - len(attempted)),
            # Every failure, not a sample. The caller decides which ids are
            # permanently absent and must never be asked for again; handing it
            # ten of thirty-seven meant the rest came round every hour forever.
            "failures": failures,
        }
        logger.info(
            "Order repair: %s",
            {**result, "failures": dict(list(failures.items())[:10])},
        )
        return result

    async def reconcile_with_api(
        self, days_back: int = 14, auto_resync: bool = True,
        *, lock: "asyncio.Lock | None" = None,
    ) -> list[dict]:
        """Compare orders between DuckDB and KeyCRM API per day.

        Fetches actual orders from API, groups by ordered_at in Kyiv TZ,
        and compares with DuckDB per-order (count, status, revenue).
        When auto_resync=True, re-fetches stale orders individually.
        Returns list of per-day results with status 'ok' or 'drift'.

        `lock` is the scheduler's heavy-job lock, held around the resync write
        and the dirty mark only — `repair_orders`' contract, for its reasons:
        a write landing between a derivation's Silver commit and its validation
        read reports a correct rebuild as failed, in DuckDB and — under
        KS_PG_DERIVE=own — in Postgres. The KeyCRM fetch stays outside it.
        """
        from collections import defaultdict
        from datetime import date

        KYIV_TZ = ZoneInfo("Europe/Kyiv")

        client = await get_async_client()
        results = []
        today = date.today()
        start_date = today - timedelta(days=days_back)

        # ── 1. Fetch all API orders for the window (one batch) ──
        # Pad start by 30 days to catch backdated B2B orders
        # (ordered_at may be weeks after created_at).
        # Include full data so we can upsert stale orders without refetching.
        api_start = (start_date - timedelta(days=30)).strftime("%Y-%m-%d 00:00:00")
        api_end = today.strftime("%Y-%m-%d 23:59:59")

        # api_by_date: summary for comparison; api_full: full order dict for resync
        api_by_date: dict[date, dict[int, dict]] = defaultdict(dict)
        api_full: dict[int, dict] = {}
        params = {
            "include": "products.offer,manager,buyer,expenses",
            "filter[created_between]": f"{api_start}, {api_end}",
        }
        total_fetched = 0
        async for batch in client.paginate("order", params=params, page_size=50):
            for order in batch:
                ordered_at_str = order.get("ordered_at")
                if not ordered_at_str:
                    continue
                try:
                    dt = datetime.fromisoformat(ordered_at_str)
                    if dt.tzinfo is None:
                        dt = dt.replace(tzinfo=ZoneInfo("Etc/GMT-4"))
                    d = dt.astimezone(KYIV_TZ).date()
                except (ValueError, TypeError):
                    continue
                if start_date <= d < today:
                    oid = order["id"]
                    api_by_date[d][oid] = {
                        "status_id": order.get("status_id"),
                        "grand_total": float(order.get("grand_total", 0)),
                    }
                    api_full[oid] = order
            total_fetched += len(batch)

        logger.info(f"Reconciliation: fetched {total_fetched} API orders for {days_back} days")

        # ── 2. Get DB order summaries for the whole window ──
        db_by_date = await self.store.get_order_summaries_by_date(
            start_date.isoformat(), (today - timedelta(days=1)).isoformat(),
        )

        # ── 3. Compare per day ──
        stale_order_ids: list[int] = []
        for offset in range(days_back):
            check_date = today - timedelta(days=offset + 1)
            if check_date < start_date:
                break
            date_str = check_date.isoformat()

            api_orders = api_by_date.get(check_date, {})
            db_orders = db_by_date.get(check_date, {})

            api_count = len(api_orders)
            db_count = len(db_orders)

            # Find order-level mismatches
            day_stale: list[int] = []
            # Orders in API but missing from DB
            for oid in api_orders.keys() - db_orders.keys():
                day_stale.append(oid)
            # Orders in both but with status or revenue drift
            for oid in api_orders.keys() & db_orders.keys():
                api_o = api_orders[oid]
                db_o = db_orders[oid]
                if (api_o["status_id"] != db_o["status_id"]
                        or abs(api_o["grand_total"] - db_o["grand_total"]) > 0.01):
                    day_stale.append(oid)

            stale_order_ids.extend(day_stale)

            entry = await self.store.log_reconciliation(date_str, api_count, db_count)
            results.append(entry)

            if entry["status"] == "drift" or day_stale:
                detail = f" ({len(day_stale)} stale orders)" if day_stale else ""
                logger.warning(
                    f"Reconciliation drift on {date_str}: API={api_count} DB={db_count}{detail}"
                )

        # ── 4. Auto-resync stale orders from already-fetched data ──
        # CRITICAL: force_update=True here. We arrived at this branch
        # specifically because reconciliation detected drift (status or
        # grand_total mismatch). KeyCRM does not bump updated_at on status
        # changes, so the skip-if-unchanged guard in upsert_orders would
        # otherwise refuse to write — defeating the whole point of
        # reconciliation.
        resync_count = 0
        if stale_order_ids and auto_resync:
            stale_orders = [api_full[oid] for oid in set(stale_order_ids) if oid in api_full]
            if stale_orders:
                held = lock if lock is not None else contextlib.nullcontext()
                async with held:
                    resync_count, _ = await self._upsert_orders_with_expenses(
                        stale_orders, force_update=True,
                    )
                    await self.store.mark_warehouse_dirty(None)
                logger.info(f"Resynced {resync_count} stale orders (force_update=True)")

        ok_count = sum(1 for r in results if r["status"] == "ok")
        drift_count = sum(1 for r in results if r["status"] == "drift")
        logger.info(
            f"Reconciliation complete: {ok_count} ok, {drift_count} drift, "
            f"{resync_count} orders resynced"
        )
        return results



# ═══════════════════════════════════════════════════════════════════════════════
# SINGLETON INSTANCE
# ═══════════════════════════════════════════════════════════════════════════════

_sync_service: Optional[SyncService] = None
_sync_service_lock = asyncio.Lock()


async def get_sync_service() -> SyncService:
    """Get singleton sync service instance (coroutine-safe)."""
    global _sync_service
    async with _sync_service_lock:
        if _sync_service is None:
            store = await get_store()
            _sync_service = SyncService(store)
    return _sync_service


async def init_and_sync(full_sync_days: int = 730) -> None:
    """
    Initialize store and perform initial sync if needed.

    Called on application startup.
    """
    store = await get_store()
    stats = await store.get_stats()

    # If no orders, do a full sync
    if stats["orders"] == 0:
        logger.info("No data in DuckDB, performing initial full sync...")
        sync_service = await get_sync_service()
        await sync_service.full_sync(days_back=full_sync_days)
    else:
        logger.info(f"DuckDB has {stats['orders']} orders, {stats['products']} products")
        # Do incremental sync
        sync_service = await get_sync_service()
        await sync_service.incremental_sync()

    # Ensure sku_inventory_status is populated (Layer 1)
    sku_count = await store.refresh_sku_inventory_status()
    if sku_count > 0:
        logger.info(f"Initialized sku_inventory_status: {sku_count} SKUs")

    # Warehouse layers (Silver/Gold) are refreshed inside incremental_sync()
    # or full_sync() when data changes. With DELETE+INSERT, tables persist across
    # restarts so existing data remains valid without a redundant second refresh.

    # Initialize Meilisearch for chat search
    try:
        if await init_meilisearch():
            sync_service = await get_sync_service()
            meili_stats = await sync_service.sync_to_meilisearch()
            logger.info(f"Meilisearch initialized: {meili_stats}")
        else:
            logger.warning("Meilisearch not available, chat search will be limited")
    except Exception as e:
        logger.warning(f"Meilisearch initialization failed: {e}")

    # Note: Background sync is now handled by APScheduler (core/scheduler.py)
    # The scheduler runs incremental_sync every 60 seconds, plus other jobs:
    # - full_sync_weekly: Sunday 2 AM
    # - inventory_snapshot: daily 1 AM
    # - manager_stats: daily 3 AM
    # - seasonality_calc: Monday 4 AM


async def force_resync(days_back: int = 730) -> dict:
    """
    Force a complete resync: re-fetch the window from KeyCRM and overwrite
    every order in it, in place.

    Use this when data discrepancies are detected between dashboard and KeyCRM.

    It used to DELETE `orders`, `order_products` and `expenses` first and refill
    them chunk by chunk. That made a 429 storm halfway through — the ordinary
    failure of a year-long walk over the API — leave the store missing months
    of facts, with nothing to restore them but the hourly gap crawl at 200 ids
    an hour, and left every DuckDB-fed page wrong for days. Rewriting in place
    with `force_update=True` reaches the same end state: each order is
    overwritten by the fetched payload, and an interrupted run can simply be
    run again. The one thing the DELETE bought — dropping orders KeyCRM has
    since deleted — is something the reconciliation deliberately refuses to do
    automatically and the purge endpoint does by id.

    Args:
        days_back: Number of days of historical data to sync

    Returns:
        Dict with sync statistics
    """
    logger.warning(f"Force resync requested - rewriting the last {days_back} days in place")

    sync_service = await get_sync_service()
    # Reset the filter detection flag
    sync_service._use_ordered_between = None
    stats = await sync_service.full_sync(days_back=days_back, force_update=True)

    logger.info(f"Force resync complete: {stats}")
    return stats
