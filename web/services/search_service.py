"""
Search service for quick inline search using Meilisearch.

Provides fast, typo-tolerant search without LLM involvement.
"""
from typing import Optional, List, Dict, Any

from core.meilisearch_client import get_meili_client
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
