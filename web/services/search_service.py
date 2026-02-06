"""
Search service for quick inline search using Meilisearch.

Provides fast, typo-tolerant search without LLM involvement.
"""
from typing import Optional, List, Dict, Any
from datetime import datetime

from core.meilisearch_client import get_meili_client
from core.duckdb_store import get_store
from core.observability import get_logger

logger = get_logger(__name__)


class SearchService:
    """Service for quick inline search."""

    def __init__(self):
        self.meili = get_meili_client()

    async def search(
        self,
        query: str,
        search_type: str = "all",
        limit: int = 10
    ) -> Dict[str, Any]:
        """
        Universal search across buyers, orders, and products.

        Args:
            query: Search query
            search_type: "buyers", "orders", "products", or "all"
            limit: Max results per type

        Returns:
            Dict with results organized by type
        """
        results = {
            "query": query,
            "buyers": [],
            "orders": [],
            "products": [],
            "total_hits": 0
        }

        if not query or len(query) < 2:
            return results

        try:
            if search_type in ("all", "buyers"):
                results["buyers"] = await self.meili.search_buyers(query, limit)

            if search_type in ("all", "orders"):
                results["orders"] = await self.meili.search_orders(query, limit)

            if search_type in ("all", "products"):
                results["products"] = await self.meili.search_products(query, limit)

            results["total_hits"] = (
                len(results["buyers"]) +
                len(results["orders"]) +
                len(results["products"])
            )

        except Exception as e:
            logger.error(f"Search failed: {e}")

        return results

    async def search_buyers(
        self,
        query: str,
        limit: int = 10,
        city: Optional[str] = None
    ) -> List[dict]:
        """
        Search buyers by name, phone, or email.

        Args:
            query: Search query
            limit: Max results
            city: Optional city filter

        Returns:
            List of matching buyers
        """
        return await self.meili.search_buyers(query, limit, city)

    async def search_orders(
        self,
        query: str,
        limit: int = 10
    ) -> List[dict]:
        """
        Search orders by ID or buyer name.

        Args:
            query: Search query (order ID or buyer name)
            limit: Max results

        Returns:
            List of matching orders
        """
        return await self.meili.search_orders(query, limit)

    async def search_products(
        self,
        query: str,
        limit: int = 10,
        brand: Optional[str] = None
    ) -> List[dict]:
        """
        Search products by name, SKU, or brand.

        Args:
            query: Search query
            limit: Max results
            brand: Optional brand filter

        Returns:
            List of matching products
        """
        return await self.meili.search_products(query, limit, brand)

    async def get_buyer_details(self, buyer_id: int) -> Optional[Dict[str, Any]]:
        """
        Get full buyer profile with order history.

        Args:
            buyer_id: Buyer ID

        Returns:
            Buyer details with orders or None
        """
        try:
            store = await get_store()

            async with store.connection() as conn:
                # Get buyer info
                buyer = conn.execute("""
                    SELECT
                        b.id,
                        b.full_name,
                        b.phone,
                        b.email,
                        b.city,
                        b.region,
                        b.note,
                        b.loyalty_program_name,
                        b.loyalty_level_name,
                        b.loyalty_discount,
                        b.created_at,
                        m.name as manager_name
                    FROM buyers b
                    LEFT JOIN managers m ON b.manager_id = m.id
                    WHERE b.id = ?
                """, [buyer_id]).fetchone()

                if not buyer:
                    return None

                buyer_dict = dict(buyer)

                # Get order history
                orders = conn.execute("""
                    SELECT
                        o.id,
                        o.grand_total,
                        o.ordered_at,
                        o.status_id,
                        o.source_name,
                        o.is_return
                    FROM silver_orders o
                    WHERE o.buyer_id = ?
                    ORDER BY o.ordered_at DESC
                    LIMIT 20
                """, [buyer_id]).fetchall()

                buyer_dict["orders"] = [dict(o) for o in orders]

                # Get aggregated stats
                stats = conn.execute("""
                    SELECT
                        COUNT(*) as total_orders,
                        SUM(CASE WHEN NOT is_return THEN grand_total ELSE 0 END) as total_spent,
                        AVG(CASE WHEN NOT is_return THEN grand_total ELSE NULL END) as avg_order_value,
                        MIN(ordered_at) as first_order,
                        MAX(ordered_at) as last_order
                    FROM silver_orders
                    WHERE buyer_id = ?
                """, [buyer_id]).fetchone()

                buyer_dict["stats"] = dict(stats) if stats else {}

                return buyer_dict

        except Exception as e:
            logger.error(f"Failed to get buyer details: {e}")
            return None

    async def get_order_details(self, order_id: int) -> Optional[Dict[str, Any]]:
        """
        Get full order details with products and buyer.

        Args:
            order_id: Order ID

        Returns:
            Order details or None
        """
        try:
            store = await get_store()

            async with store.connection() as conn:
                # Get order info
                order = conn.execute("""
                    SELECT
                        o.id,
                        o.grand_total,
                        o.ordered_at,
                        o.status_id,
                        o.source_name,
                        o.is_return,
                        o.sales_type,
                        b.id as buyer_id,
                        b.full_name as buyer_name,
                        b.phone as buyer_phone,
                        m.name as manager_name
                    FROM silver_orders o
                    LEFT JOIN buyers b ON o.buyer_id = b.id
                    LEFT JOIN managers m ON o.manager_id = m.id
                    WHERE o.id = ?
                """, [order_id]).fetchone()

                if not order:
                    return None

                order_dict = dict(order)

                # Get order products
                products = conn.execute("""
                    SELECT
                        op.name as product_name,
                        op.sku,
                        op.quantity,
                        op.price_sold,
                        op.quantity * op.price_sold as line_total
                    FROM order_products op
                    WHERE op.order_id = ?
                """, [order_id]).fetchall()

                order_dict["products"] = [dict(p) for p in products]

                return order_dict

        except Exception as e:
            logger.error(f"Failed to get order details: {e}")
            return None

    async def get_product_details(self, product_id: int) -> Optional[Dict[str, Any]]:
        """
        Get product details with sales stats.

        Args:
            product_id: Product ID

        Returns:
            Product details or None
        """
        try:
            store = await get_store()

            async with store.connection() as conn:
                # Get product info
                product = conn.execute("""
                    SELECT
                        p.id,
                        p.name,
                        p.sku,
                        p.brand,
                        p.price,
                        c.name as category_name
                    FROM products p
                    LEFT JOIN categories c ON p.category_id = c.id
                    WHERE p.id = ?
                """, [product_id]).fetchone()

                if not product:
                    return None

                product_dict = dict(product)

                # Get sales stats
                stats = conn.execute("""
                    SELECT
                        COUNT(DISTINCT op.order_id) as total_orders,
                        SUM(op.quantity) as total_quantity,
                        SUM(op.quantity * op.price_sold) as total_revenue,
                        AVG(op.price_sold) as avg_price
                    FROM order_products op
                    JOIN silver_orders o ON op.order_id = o.id
                    WHERE op.product_id = ? AND NOT o.is_return
                """, [product_id]).fetchone()

                product_dict["stats"] = dict(stats) if stats else {}

                return product_dict

        except Exception as e:
            logger.error(f"Failed to get product details: {e}")
            return None


# ═══════════════════════════════════════════════════════════════════════════════
# SINGLETON INSTANCE
# ═══════════════════════════════════════════════════════════════════════════════

_search_service: Optional[SearchService] = None


def get_search_service() -> SearchService:
    """Get singleton search service instance."""
    global _search_service
    if _search_service is None:
        _search_service = SearchService()
    return _search_service
