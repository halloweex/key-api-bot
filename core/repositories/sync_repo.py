"""
Sync repository for data upsert operations.

Handles all data sync from KeyCRM API to DuckDB.
"""
from datetime import datetime
from typing import Optional, List, Dict, Any

import pandas as pd

from core.repositories.base import BaseRepository, _date_in_kyiv
from core.observability import get_logger

logger = get_logger(__name__)


class SyncRepository(BaseRepository):
    """Repository for sync operations - upsert data from KeyCRM API."""

    async def get_last_sync_time(self, key: str = "orders") -> Optional[datetime]:
        """Get the last sync timestamp for a given key."""
        async with self.connection() as conn:
            result = conn.execute(
                "SELECT value FROM sync_metadata WHERE key = ?", [key]
            ).fetchone()
            if result and result[0]:
                return datetime.fromisoformat(result[0])
            return None

    async def set_last_sync_time(
        self, key: str = "orders", timestamp: datetime = None
    ) -> None:
        """Set the last sync timestamp for a given key."""
        if timestamp is None:
            timestamp = datetime.now()

        async with self.connection() as conn:
            conn.execute("""
                INSERT INTO sync_metadata (key, value, updated_at)
                VALUES (?, ?, CURRENT_TIMESTAMP)
                ON CONFLICT (key) DO UPDATE SET
                    value = excluded.value,
                    updated_at = CURRENT_TIMESTAMP
            """, [key, timestamp.isoformat()])

    async def upsert_orders(self, orders: List[Dict[str, Any]]) -> int:
        """
        Upsert orders into the database.

        Uses updated_at timestamp for idempotent sync - only updates if
        the incoming data is newer than existing.

        Returns:
            Number of orders upserted
        """
        if not orders:
            return 0

        async with self.connection() as conn:
            # Prepare order data
            order_rows = []
            product_rows = []

            for order in orders:
                # Parse timestamps
                ordered_at = order.get("ordered_at")
                created_at = order.get("created_at")
                updated_at = order.get("updated_at")

                order_rows.append({
                    "id": order["id"],
                    "source_id": order.get("source_id", 0),
                    "status_id": order.get("status_id", 0),
                    "grand_total": float(order.get("grand_total", 0) or 0),
                    "ordered_at": ordered_at,
                    "created_at": created_at,
                    "updated_at": updated_at,
                    "buyer_id": order.get("buyer_id") or order.get("buyer", {}).get("id"),
                    "manager_id": order.get("manager_id") or order.get("manager", {}).get("id"),
                })

                # Extract products
                products = order.get("products", [])
                for i, prod in enumerate(products):
                    product_rows.append({
                        "id": order["id"] * 10000 + i,
                        "order_id": order["id"],
                        "product_id": prod.get("product_id") or prod.get("id"),
                        "name": prod.get("name", "Unknown"),
                        "quantity": int(prod.get("quantity", 1)),
                        "price_sold": float(prod.get("price", 0) or 0),
                    })

            # Bulk insert orders using pandas for performance
            orders_df = pd.DataFrame(order_rows)
            products_df = pd.DataFrame(product_rows) if product_rows else pd.DataFrame()

            # Delete existing products for these orders (to handle updates)
            order_ids = [o["id"] for o in order_rows]
            if order_ids:
                placeholders = ",".join(["?" for _ in order_ids])
                conn.execute(
                    f"DELETE FROM order_products WHERE order_id IN ({placeholders})",
                    order_ids
                )

            # Upsert orders with updated_at check
            conn.execute("""
                CREATE TEMP TABLE IF NOT EXISTS temp_orders AS
                SELECT * FROM orders WHERE 1=0
            """)
            conn.execute("DELETE FROM temp_orders")
            conn.register("temp_orders_df", orders_df)
            conn.execute("INSERT INTO temp_orders SELECT * FROM temp_orders_df")

            # Only update if newer
            conn.execute("""
                INSERT INTO orders
                SELECT * FROM temp_orders t
                ON CONFLICT (id) DO UPDATE SET
                    source_id = excluded.source_id,
                    status_id = excluded.status_id,
                    grand_total = excluded.grand_total,
                    ordered_at = excluded.ordered_at,
                    created_at = excluded.created_at,
                    updated_at = excluded.updated_at,
                    buyer_id = excluded.buyer_id,
                    manager_id = excluded.manager_id,
                    synced_at = CURRENT_TIMESTAMP
                WHERE orders.updated_at IS NULL
                   OR excluded.updated_at IS NULL
                   OR excluded.updated_at >= orders.updated_at
            """)

            # Insert products
            if not products_df.empty:
                conn.register("products_df", products_df)
                conn.execute("""
                    INSERT INTO order_products
                    SELECT * FROM products_df
                    ON CONFLICT (id) DO UPDATE SET
                        order_id = excluded.order_id,
                        product_id = excluded.product_id,
                        name = excluded.name,
                        quantity = excluded.quantity,
                        price_sold = excluded.price_sold
                """)

            logger.info(
                f"Upserted {len(order_rows)} orders, {len(product_rows)} products"
            )
            return len(order_rows)

    async def upsert_products(self, products: List[Dict[str, Any]]) -> int:
        """Upsert products into the database."""
        if not products:
            return 0

        async with self.connection() as conn:
            rows = []
            for product in products:
                # Extract brand from custom_fields
                brand = None
                custom_fields = product.get("custom_fields", [])
                if isinstance(custom_fields, list):
                    for field in custom_fields:
                        if isinstance(field, dict):
                            if field.get("uuid") == "CT_1002" or field.get("name") == "Бренд":
                                brand = field.get("value")
                                break

                rows.append({
                    "id": product["id"],
                    "name": product.get("name", "Unknown"),
                    "category_id": product.get("category_id"),
                    "brand": brand,
                    "sku": product.get("sku"),
                    "price": float(product.get("price", 0) or 0),
                })

            df = pd.DataFrame(rows)
            conn.register("products_batch", df)
            conn.execute("""
                INSERT INTO products
                SELECT id, name, category_id, brand, sku, price, CURRENT_TIMESTAMP
                FROM products_batch
                ON CONFLICT (id) DO UPDATE SET
                    name = excluded.name,
                    category_id = excluded.category_id,
                    brand = excluded.brand,
                    sku = excluded.sku,
                    price = excluded.price,
                    synced_at = CURRENT_TIMESTAMP
            """)

            logger.info(f"Upserted {len(rows)} products")
            return len(rows)

    async def upsert_categories(self, categories: List[Dict[str, Any]]) -> int:
        """Upsert categories into the database."""
        if not categories:
            return 0

        async with self.connection() as conn:
            rows = [{
                "id": cat["id"],
                "name": cat.get("name", "Unknown"),
                "parent_id": cat.get("parent_id"),
            } for cat in categories]

            df = pd.DataFrame(rows)
            conn.register("categories_batch", df)
            conn.execute("""
                INSERT INTO categories
                SELECT id, name, parent_id, CURRENT_TIMESTAMP
                FROM categories_batch
                ON CONFLICT (id) DO UPDATE SET
                    name = excluded.name,
                    parent_id = excluded.parent_id,
                    synced_at = CURRENT_TIMESTAMP
            """)

            logger.info(f"Upserted {len(rows)} categories")
            return len(rows)

    async def upsert_expense_types(self, expense_types: List[Dict[str, Any]]) -> int:
        """Upsert expense types into the database."""
        if not expense_types:
            return 0

        async with self.connection() as conn:
            rows = [{
                "id": et["id"],
                "name": et.get("name", "Unknown"),
                "alias": et.get("alias"),
                "is_active": et.get("is_active", True),
            } for et in expense_types]

            df = pd.DataFrame(rows)
            conn.register("expense_types_batch", df)
            conn.execute("""
                INSERT INTO expense_types
                SELECT id, name, alias, is_active, CURRENT_TIMESTAMP
                FROM expense_types_batch
                ON CONFLICT (id) DO UPDATE SET
                    name = excluded.name,
                    alias = excluded.alias,
                    is_active = excluded.is_active,
                    synced_at = CURRENT_TIMESTAMP
            """)

            logger.info(f"Upserted {len(rows)} expense types")
            return len(rows)

    async def upsert_expenses(
        self, order_id: int, expenses: List[Dict[str, Any]]
    ) -> int:
        """Upsert expenses for a specific order."""
        if not expenses:
            return 0

        async with self.connection() as conn:
            rows = [{
                "id": exp["id"],
                "order_id": order_id,
                "expense_type_id": exp.get("expense_type_id"),
                "amount": float(exp.get("amount", 0) or 0),
                "description": exp.get("description"),
                "status": exp.get("status"),
                "payment_date": exp.get("payment_date"),
                "created_at": exp.get("created_at"),
            } for exp in expenses]

            df = pd.DataFrame(rows)
            conn.register("expenses_batch", df)
            conn.execute("""
                INSERT INTO expenses
                SELECT id, order_id, expense_type_id, amount, description,
                       status, payment_date, created_at, CURRENT_TIMESTAMP
                FROM expenses_batch
                ON CONFLICT (id) DO UPDATE SET
                    order_id = excluded.order_id,
                    expense_type_id = excluded.expense_type_id,
                    amount = excluded.amount,
                    description = excluded.description,
                    status = excluded.status,
                    payment_date = excluded.payment_date,
                    created_at = excluded.created_at,
                    synced_at = CURRENT_TIMESTAMP
            """)

            return len(rows)

    async def upsert_managers(self, managers: List[Dict[str, Any]]) -> int:
        """Upsert managers/users into the database."""
        if not managers:
            return 0

        async with self.connection() as conn:
            rows = [{
                "id": m["id"],
                "name": m.get("name") or m.get("full_name", "Unknown"),
                "email": m.get("email"),
                "status": m.get("status", "active"),
            } for m in managers]

            df = pd.DataFrame(rows)
            conn.register("managers_batch", df)
            conn.execute("""
                INSERT INTO managers (id, name, email, status, synced_at)
                SELECT id, name, email, status, CURRENT_TIMESTAMP
                FROM managers_batch
                ON CONFLICT (id) DO UPDATE SET
                    name = excluded.name,
                    email = excluded.email,
                    status = excluded.status,
                    synced_at = CURRENT_TIMESTAMP
            """)

            logger.info(f"Upserted {len(rows)} managers")
            return len(rows)

    async def upsert_offers(self, offers: List[Dict[str, Any]]) -> int:
        """Upsert offers (product variations) into the database."""
        if not offers:
            return 0

        async with self.connection() as conn:
            rows = [{
                "id": o["id"],
                "product_id": o.get("product_id"),
                "sku": o.get("sku"),
            } for o in offers]

            df = pd.DataFrame(rows)
            conn.register("offers_batch", df)
            conn.execute("""
                INSERT INTO offers
                SELECT id, product_id, sku, CURRENT_TIMESTAMP
                FROM offers_batch
                ON CONFLICT (id) DO UPDATE SET
                    product_id = excluded.product_id,
                    sku = excluded.sku,
                    synced_at = CURRENT_TIMESTAMP
            """)

            logger.info(f"Upserted {len(rows)} offers")
            return len(rows)

    async def upsert_stocks(self, stocks: List[Dict[str, Any]]) -> int:
        """Upsert offer stocks into the database."""
        if not stocks:
            return 0

        async with self.connection() as conn:
            rows = [{
                "id": s.get("offer_id") or s.get("id"),
                "sku": s.get("sku"),
                "price": float(s.get("price", 0) or 0),
                "purchased_price": float(s.get("purchased_price", 0) or 0) if s.get("purchased_price") else None,
                "quantity": int(s.get("quantity", 0) or 0),
                "reserve": int(s.get("reserve", 0) or 0),
            } for s in stocks]

            df = pd.DataFrame(rows)
            conn.register("stocks_batch", df)
            conn.execute("""
                INSERT INTO offer_stocks
                SELECT id, sku, price, purchased_price, quantity, reserve, CURRENT_TIMESTAMP
                FROM stocks_batch
                ON CONFLICT (id) DO UPDATE SET
                    sku = excluded.sku,
                    price = excluded.price,
                    purchased_price = excluded.purchased_price,
                    quantity = excluded.quantity,
                    reserve = excluded.reserve,
                    synced_at = CURRENT_TIMESTAMP
            """)

            logger.info(f"Upserted {len(rows)} stocks")
            return len(rows)

    async def update_manager_stats(self) -> int:
        """Update manager statistics from order data."""
        async with self.connection() as conn:
            result = conn.execute(f"""
                UPDATE managers m SET
                    first_order_date = stats.first_order,
                    last_order_date = stats.last_order,
                    order_count = stats.order_count
                FROM (
                    SELECT
                        manager_id,
                        MIN({_date_in_kyiv('ordered_at')}) as first_order,
                        MAX({_date_in_kyiv('ordered_at')}) as last_order,
                        COUNT(*) as order_count
                    FROM orders
                    WHERE manager_id IS NOT NULL
                    GROUP BY manager_id
                ) stats
                WHERE m.id = stats.manager_id
            """)
            return result.rowcount if hasattr(result, 'rowcount') else 0

    async def get_retail_manager_ids(self) -> List[int]:
        """Get list of retail manager IDs."""
        async with self.connection() as conn:
            result = conn.execute(
                "SELECT id FROM managers WHERE is_retail = TRUE"
            ).fetchall()
            return [r[0] for r in result]

    async def set_manager_retail_status(
        self, manager_id: int, is_retail: bool
    ) -> None:
        """Set retail status for a manager."""
        async with self.connection() as conn:
            conn.execute(
                "UPDATE managers SET is_retail = ? WHERE id = ?",
                [is_retail, manager_id]
            )

    async def get_all_managers(self) -> List[Dict[str, Any]]:
        """Get all managers with their stats."""
        async with self.connection() as conn:
            result = conn.execute("""
                SELECT
                    id,
                    name,
                    email,
                    status,
                    is_retail,
                    first_order_date,
                    last_order_date,
                    order_count
                FROM managers
                ORDER BY order_count DESC
            """).fetchall()

            return [{
                "id": r[0],
                "name": r[1],
                "email": r[2],
                "status": r[3],
                "is_retail": r[4],
                "first_order_date": str(r[5]) if r[5] else None,
                "last_order_date": str(r[6]) if r[6] else None,
                "order_count": r[7],
            } for r in result]

    async def get_latest_order_time(self) -> Optional[datetime]:
        """Get the timestamp of the most recent order."""
        async with self.connection() as conn:
            result = conn.execute(
                "SELECT MAX(ordered_at) FROM orders"
            ).fetchone()
            return result[0] if result and result[0] else None
