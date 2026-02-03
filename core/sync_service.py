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
from datetime import datetime, timedelta
from typing import Optional, Dict, Any
from zoneinfo import ZoneInfo

from core.keycrm import get_async_client
from core.duckdb_store import get_store, DuckDBStore
from core.exceptions import KeyCRMError, KeyCRMConnectionError, KeyCRMAPIError
from core.observability import get_logger, Timer, correlation_context
from core.events import (
    events,
    SyncEvent,
    emit_sync_started,
    emit_sync_completed,
    emit_sync_failed,
    emit_orders_synced,
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
            except (ValueError, TypeError):
                pass

    return max_updated


class SyncService:
    """
    Service for syncing KeyCRM data to DuckDB.

    Features:
    - Full sync: Initial load of all historical data
    - Incremental sync: Only fetch new/updated records
    - Background sync: Periodic updates in the background
    """

    def __init__(self, store: DuckDBStore):
        self.store = store
        self._sync_task: Optional[asyncio.Task] = None
        self._stop_sync = False
        # Track if ordered_between filter is supported
        self._use_ordered_between: Optional[bool] = None

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

    async def _upsert_orders_with_expenses(self, orders: list) -> tuple:
        """Upsert orders and their expenses.

        Returns:
            Tuple of (order_count, expense_count)
        """
        order_count = await self.store.upsert_orders(orders)
        expense_count = 0

        for order in orders:
            expenses = order.get("expenses", [])
            if expenses:
                expense_count += await self.store.upsert_expenses(order["id"], expenses)

        return order_count, expense_count

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

    async def full_sync(self, days_back: int = 365) -> Dict[str, Any]:
        """
        Perform full sync of all data from KeyCRM.

        Args:
            days_back: Number of days of historical data to sync

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

            # Sync categories first
            logger.info("Syncing categories...")
            categories = []
            async for batch in client.paginate("products/categories", page_size=50):
                categories.extend(batch)
            stats["categories"] = await self.store.upsert_categories(categories)
            await self.store.set_last_sync_time("categories")

            # Sync expense types
            logger.info("Syncing expense types...")
            expense_types = []
            async for batch in client.paginate("order/expense-type", page_size=50):
                expense_types.extend(batch)
            stats["expense_types"] = await self.store.upsert_expense_types(expense_types)
            await self.store.set_last_sync_time("expense_types")

            # Sync products with custom_fields for brand extraction
            logger.info("Syncing products...")
            products = []
            async for batch in client.paginate("products", params={"include": "custom_fields"}, page_size=50):
                products.extend(batch)
            stats["products"] = await self.store.upsert_products(products)
            await self.store.set_last_sync_time("products")

            # Sync orders with expenses - in chunks to avoid pagination limit (100 pages × 50 = 5000 orders max)
            # IMPORTANT: Save each chunk immediately to preserve progress on timeout/crash
            logger.info("Syncing orders...")
            final_end_date = datetime.now(DEFAULT_TZ) + timedelta(days=1)
            chunk_days = 90  # Sync in 3-month chunks to stay under 5000 orders per chunk
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
                    order_count, expense_count = await self._upsert_orders_with_expenses(chunk_orders)
                    stats["orders"] += order_count
                    stats["expenses"] += expense_count
                    logger.info(f"  Chunk {chunk_num}: Saved {order_count} orders, {expense_count} expenses")

                current_start = current_end

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

        Returns:
            Dict with sync statistics
        """
        import time
        start_time = time.perf_counter()
        stats = {"orders": 0, "products": 0, "categories": 0, "expenses": 0, "managers": 0, "offers": 0, "stocks": 0}
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
            orders = await self._fetch_orders_with_date_filter(
                client,
                sync_from.strftime('%Y-%m-%d %H:%M:%S'),
                sync_to.strftime('%Y-%m-%d %H:%M:%S')
            )

            if orders:
                order_count, expense_count = await self._upsert_orders_with_expenses(orders)
                stats["orders"] = order_count
                stats["expenses"] = expense_count
                # Use max(updated_at) from SOURCE data, not now()
                max_updated = _get_max_updated_at(orders)
                await self.store.set_last_sync_time("orders", max_updated)
                logger.info(f"Incremental sync: {stats['orders']} orders, {stats['expenses']} expenses, checkpoint: {max_updated}")

                # Emit orders synced event
                await events.emit(SyncEvent.ORDERS_SYNCED, {
                    "count": order_count,
                    "expenses": expense_count,
                    "checkpoint": max_updated.isoformat() if max_updated else None,
                })

            # Sync products less frequently (every hour)
            if not last_products_sync or (datetime.now(DEFAULT_TZ) - last_products_sync).total_seconds() > 3600:
                logger.info("Syncing products (hourly)...")
                products = []
                async for batch in client.paginate("products", params={"include": "custom_fields"}, page_size=50):
                    products.extend(batch)
                stats["products"] = await self.store.upsert_products(products)
                await self.store.set_last_sync_time("products")

                # Emit products synced event
                await events.emit(SyncEvent.PRODUCTS_SYNCED, {"count": stats["products"]})

            # Sync managers daily (86400 seconds = 24 hours)
            last_managers_sync = await self.store.get_last_sync_time("managers")
            if not last_managers_sync or (datetime.now(DEFAULT_TZ) - last_managers_sync).total_seconds() > 86400:
                stats["managers"] = await self.sync_managers()

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

            # Refresh warehouse layers (Silver → Gold) after all syncs
            await self.store.refresh_warehouse_layers(trigger="incremental_sync")

        except KeyCRMConnectionError as e:
            error_occurred = str(e)
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
                order_count, expense_count = await self._upsert_orders_with_expenses(orders)
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
                order_count, expense_count = await self._upsert_orders_with_expenses(orders)
                stats["orders"] = order_count
                stats["expenses"] = expense_count
                logger.info(f"Status refresh: updated {order_count} orders, {expense_count} expenses")

                # Refresh warehouse layers (Silver → Gold)
                await self.store.refresh_warehouse_layers(trigger="status_refresh")

        except KeyCRMConnectionError as e:
            logger.warning(f"Status refresh connection error (will retry): {e}")
        except KeyCRMError as e:
            logger.error(f"Status refresh error: {e}")

        return stats

    async def start_background_sync(self, interval_seconds: int = 60) -> None:
        """
        Start background sync task.

        Args:
            interval_seconds: Seconds between sync cycles
        """
        self._stop_sync = False

        async def sync_loop():
            logger.info("Background sync started")
            while not self._stop_sync:
                try:
                    await self.sync_today()
                except KeyCRMConnectionError as e:
                    logger.debug(f"Background sync connection error (will retry): {e}")
                except KeyCRMError as e:
                    logger.warning(f"Background sync error: {e}")

                # Wait for next cycle
                for _ in range(interval_seconds):
                    if self._stop_sync:
                        break
                    await asyncio.sleep(1)

            logger.info("Background sync stopped")

        self._sync_task = asyncio.create_task(sync_loop())

    def stop_background_sync(self) -> None:
        """Stop background sync task."""
        self._stop_sync = True
        if self._sync_task and not self._sync_task.done():
            self._sync_task.cancel()


# ═══════════════════════════════════════════════════════════════════════════════
# SINGLETON INSTANCE
# ═══════════════════════════════════════════════════════════════════════════════

_sync_service: Optional[SyncService] = None


async def get_sync_service() -> SyncService:
    """Get singleton sync service instance."""
    global _sync_service
    if _sync_service is None:
        store = await get_store()
        _sync_service = SyncService(store)
    return _sync_service


async def init_and_sync(full_sync_days: int = 365) -> None:
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

    # Ensure warehouse layers (Silver/Gold) are populated
    wh_result = await store.refresh_warehouse_layers(trigger="startup")
    logger.info(f"Warehouse layers initialized: silver={wh_result.get('silver_rows', 0)}, gold_rev={wh_result.get('gold_revenue_rows', 0)}")

    # Note: Background sync is now handled by APScheduler (core/scheduler.py)
    # The scheduler runs incremental_sync every 60 seconds, plus other jobs:
    # - full_sync_weekly: Sunday 2 AM
    # - inventory_snapshot: daily 1 AM
    # - manager_stats: daily 3 AM
    # - seasonality_calc: Monday 4 AM


async def force_resync(days_back: int = 365) -> dict:
    """
    Force a complete resync by clearing orders and re-fetching from API.

    Use this when data discrepancies are detected between dashboard and KeyCRM.
    This clears the orders table and performs a fresh sync.

    Args:
        days_back: Number of days of historical data to sync

    Returns:
        Dict with sync statistics
    """
    logger.warning(f"Force resync requested - clearing orders and syncing last {days_back} days")

    store = await get_store()

    # Clear existing orders and related data
    async with store.connection() as conn:
        conn.execute("DELETE FROM expenses")
        conn.execute("DELETE FROM order_products")
        conn.execute("DELETE FROM orders")
        conn.execute("DELETE FROM sync_metadata WHERE key LIKE 'last_sync_orders%'")
        logger.info("Cleared orders, order_products, expenses tables")

    # Perform fresh full sync
    sync_service = await get_sync_service()
    # Reset the filter detection flag
    sync_service._use_ordered_between = None
    stats = await sync_service.full_sync(days_back=days_back)

    logger.info(f"Force resync complete: {stats}")
    return stats
