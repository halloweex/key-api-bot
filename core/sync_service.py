"""
Sync service for keeping DuckDB in sync with KeyCRM API.

Handles incremental synchronization of orders, products, and categories.
"""
import asyncio
import logging
from datetime import datetime, timedelta
from typing import Optional
from zoneinfo import ZoneInfo

from core.keycrm import get_async_client
from core.duckdb_store import get_store, DuckDBStore
from bot.config import DEFAULT_TIMEZONE

logger = logging.getLogger(__name__)

DEFAULT_TZ = ZoneInfo(DEFAULT_TIMEZONE)


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

    async def full_sync(self, days_back: int = 365) -> dict:
        """
        Perform full sync of all data from KeyCRM.

        Args:
            days_back: Number of days of historical data to sync

        Returns:
            Dict with sync statistics
        """
        logger.info(f"Starting full sync (last {days_back} days)...")
        stats = {"orders": 0, "products": 0, "categories": 0, "expense_types": 0, "expenses": 0}

        try:
            client = await get_async_client()

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

            # Sync orders with expenses
            logger.info("Syncing orders...")
            start_date = datetime.now(DEFAULT_TZ) - timedelta(days=days_back)
            end_date = datetime.now(DEFAULT_TZ) + timedelta(days=1)

            params = {
                "include": "products.offer,manager,buyer,expenses",
                "filter[created_between]": f"{start_date.strftime('%Y-%m-%d')}, {end_date.strftime('%Y-%m-%d')}",
            }

            orders = []
            async for batch in client.paginate("order", params=params, page_size=50):
                orders.extend(batch)
                # Batch insert every 500 orders to avoid memory issues
                if len(orders) >= 500:
                    order_count, expense_count = await self._upsert_orders_with_expenses(orders)
                    stats["orders"] += order_count
                    stats["expenses"] += expense_count
                    orders = []

            if orders:
                order_count, expense_count = await self._upsert_orders_with_expenses(orders)
                stats["orders"] += order_count
                stats["expenses"] += expense_count

            await self.store.set_last_sync_time("orders")
            logger.info(f"Full sync complete: {stats}")

        except Exception as e:
            logger.error(f"Full sync error: {e}", exc_info=True)
            raise

        return stats

    async def incremental_sync(self) -> dict:
        """
        Perform incremental sync - only fetch new/updated data since last sync.

        Returns:
            Dict with sync statistics
        """
        stats = {"orders": 0, "products": 0, "categories": 0, "expenses": 0}

        try:
            client = await get_async_client()

            # Get last sync times
            last_orders_sync = await self.store.get_last_sync_time("orders")
            last_products_sync = await self.store.get_last_sync_time("products")

            # Default to 1 hour ago if never synced
            if not last_orders_sync:
                last_orders_sync = datetime.now(DEFAULT_TZ) - timedelta(hours=1)

            # Add buffer for API delays
            sync_from = last_orders_sync - timedelta(minutes=10)
            sync_to = datetime.now(DEFAULT_TZ) + timedelta(minutes=5)

            # Sync new orders with expenses
            params = {
                "include": "products.offer,manager,buyer,expenses",
                "filter[created_between]": f"{sync_from.strftime('%Y-%m-%d %H:%M:%S')}, {sync_to.strftime('%Y-%m-%d %H:%M:%S')}",
            }

            orders = []
            async for batch in client.paginate("order", params=params, page_size=50):
                orders.extend(batch)

            if orders:
                order_count, expense_count = await self._upsert_orders_with_expenses(orders)
                stats["orders"] = order_count
                stats["expenses"] = expense_count
                await self.store.set_last_sync_time("orders")
                logger.info(f"Incremental sync: {stats['orders']} orders, {stats['expenses']} expenses updated")

            # Sync products less frequently (every hour)
            if not last_products_sync or (datetime.now(DEFAULT_TZ) - last_products_sync).total_seconds() > 3600:
                logger.info("Syncing products (hourly)...")
                products = []
                async for batch in client.paginate("products", params={"include": "custom_fields"}, page_size=50):
                    products.extend(batch)
                stats["products"] = await self.store.upsert_products(products)
                await self.store.set_last_sync_time("products")

        except Exception as e:
            logger.error(f"Incremental sync error: {e}", exc_info=True)

        return stats

    async def sync_today(self) -> dict:
        """
        Sync only today's orders for real-time dashboard.

        Returns:
            Dict with sync statistics
        """
        stats = {"orders": 0, "expenses": 0}

        try:
            client = await get_async_client()
            today = datetime.now(DEFAULT_TZ).date()

            params = {
                "include": "products.offer,manager,buyer,expenses",
                "filter[created_between]": f"{today}, {today + timedelta(days=1)}",
            }

            orders = []
            async for batch in client.paginate("order", params=params, page_size=50):
                orders.extend(batch)

            if orders:
                order_count, expense_count = await self._upsert_orders_with_expenses(orders)
                stats["orders"] = order_count
                stats["expenses"] = expense_count
                logger.debug(f"Today sync: {stats['orders']} orders, {stats['expenses']} expenses")

        except Exception as e:
            logger.error(f"Today sync error: {e}")

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
                except Exception as e:
                    logger.error(f"Background sync error: {e}")

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


async def init_and_sync(full_sync_days: int = 90) -> None:
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

    # Start background sync
    sync_service = await get_sync_service()
    await sync_service.start_background_sync(interval_seconds=60)
