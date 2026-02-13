"""
Meilisearch async client wrapper for typo-tolerant search.

Provides fast, typo-tolerant search for buyers, orders, and products.
Syncs data from DuckDB to Meilisearch for optimal search performance.
"""
import asyncio
import math
from typing import Optional, List, Dict, Any
from datetime import datetime, date

import meilisearch
from meilisearch.errors import MeilisearchApiError

from core.config import config
from core.observability import get_logger

logger = get_logger(__name__)


def _sanitize_for_json(obj: Any) -> Any:
    """Sanitize a value for JSON serialization (handle NaN, Infinity, dates)."""
    if obj is None:
        return None
    if isinstance(obj, float):
        if math.isnan(obj) or math.isinf(obj):
            return None
        return obj
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    if isinstance(obj, dict):
        return {k: _sanitize_for_json(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_sanitize_for_json(v) for v in obj]
    return obj


def _sanitize_documents(docs: List[dict]) -> List[dict]:
    """Sanitize a list of documents for Meilisearch indexing."""
    return [_sanitize_for_json(doc) for doc in docs]


class MeiliClient:
    """Async wrapper for Meilisearch client."""

    def __init__(self, url: str, master_key: str):
        self.url = url
        self.master_key = master_key
        self._client: Optional[meilisearch.Client] = None
        self._initialized = False

    @property
    def client(self) -> meilisearch.Client:
        """Lazy-initialize Meilisearch client."""
        if self._client is None:
            self._client = meilisearch.Client(self.url, self.master_key)
        return self._client

    async def health_check(self) -> dict:
        """Check Meilisearch health status."""
        try:
            # Run sync operation in thread pool
            loop = asyncio.get_event_loop()
            result = await loop.run_in_executor(None, self.client.health)
            return result
        except Exception as e:
            logger.warning(f"Meilisearch health check failed: {e}")
            return {"status": "unavailable", "error": str(e)}

    async def initialize_indexes(self) -> bool:
        """Initialize required indexes with proper settings."""
        try:
            loop = asyncio.get_event_loop()

            # Buyers index - optimized for name/phone/email search
            await loop.run_in_executor(
                None,
                lambda: self.client.create_index("buyers", {"primaryKey": "id"})
            )
            buyers_index = self.client.index("buyers")
            await loop.run_in_executor(
                None,
                lambda: buyers_index.update_settings({
                    "searchableAttributes": [
                        "full_name",
                        "phone",
                        "email",
                        "city",
                        "note"
                    ],
                    "filterableAttributes": ["city", "order_count", "manager_id"],
                    "sortableAttributes": ["order_count", "created_at"],
                    "typoTolerance": {
                        "enabled": True,
                        "minWordSizeForTypos": {"oneTypo": 3, "twoTypos": 6}
                    }
                })
            )

            # Orders index - for order lookup
            await loop.run_in_executor(
                None,
                lambda: self.client.create_index("orders", {"primaryKey": "id"})
            )
            orders_index = self.client.index("orders")
            await loop.run_in_executor(
                None,
                lambda: orders_index.update_settings({
                    "searchableAttributes": [
                        "id",
                        "buyer_name",
                        "source_name"
                    ],
                    "filterableAttributes": ["status_id", "source_id", "order_date"],
                    "sortableAttributes": ["ordered_at", "grand_total"],
                })
            )

            # Products index - for product search
            await loop.run_in_executor(
                None,
                lambda: self.client.create_index("products", {"primaryKey": "id"})
            )
            products_index = self.client.index("products")
            await loop.run_in_executor(
                None,
                lambda: products_index.update_settings({
                    "searchableAttributes": [
                        "name",
                        "sku",
                        "brand"
                    ],
                    "filterableAttributes": ["category_id", "brand"],
                    "sortableAttributes": ["price", "name"],
                })
            )

            self._initialized = True
            logger.info("Meilisearch indexes initialized")
            return True

        except MeilisearchApiError as e:
            # Index already exists is OK
            if "index_already_exists" in str(e):
                self._initialized = True
                logger.info("Meilisearch indexes already exist")
                return True
            logger.error(f"Failed to initialize Meilisearch indexes: {e}")
            return False
        except Exception as e:
            logger.error(f"Meilisearch initialization error: {e}")
            return False

    async def search_buyers(
        self,
        query: str,
        limit: int = 10,
        city: Optional[str] = None
    ) -> List[dict]:
        """
        Typo-tolerant buyer search.

        Args:
            query: Search query (name, phone, email)
            limit: Max results
            city: Optional city filter

        Returns:
            List of matching buyers with highlights
        """
        try:
            loop = asyncio.get_event_loop()
            index = self.client.index("buyers")

            search_params = {
                "limit": limit,
                "attributesToRetrieve": [
                    "id", "full_name", "phone", "email", "city",
                    "order_count", "created_at"
                ],
                "attributesToHighlight": ["full_name", "phone", "email"],
                "highlightPreTag": "<mark>",
                "highlightPostTag": "</mark>",
            }

            if city:
                search_params["filter"] = f'city = "{city}"'

            result = await loop.run_in_executor(
                None,
                lambda: index.search(query, search_params)
            )
            return result.get("hits", [])

        except Exception as e:
            logger.warning(f"Buyer search failed: {e}")
            return []

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
        try:
            loop = asyncio.get_event_loop()
            index = self.client.index("orders")

            result = await loop.run_in_executor(
                None,
                lambda: index.search(query, {
                    "limit": limit,
                    "attributesToRetrieve": [
                        "id", "buyer_name", "buyer_id", "grand_total",
                        "ordered_at", "status_id", "source_name"
                    ],
                })
            )
            return result.get("hits", [])

        except Exception as e:
            logger.warning(f"Order search failed: {e}")
            return []

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
        try:
            loop = asyncio.get_event_loop()
            index = self.client.index("products")

            search_params = {
                "limit": limit,
                "attributesToRetrieve": [
                    "id", "name", "sku", "brand", "price", "category_id"
                ],
                "attributesToHighlight": ["name", "sku", "brand"],
                "highlightPreTag": "<mark>",
                "highlightPostTag": "</mark>",
            }

            if brand:
                search_params["filter"] = f'brand = "{brand}"'

            result = await loop.run_in_executor(
                None,
                lambda: index.search(query, search_params)
            )
            return result.get("hits", [])

        except Exception as e:
            logger.warning(f"Product search failed: {e}")
            return []

    async def index_buyers(self, buyers: List[dict]) -> int:
        """
        Index buyers for search.

        Args:
            buyers: List of buyer dicts with id, full_name, phone, etc.

        Returns:
            Number of documents indexed
        """
        if not buyers:
            return 0

        try:
            loop = asyncio.get_event_loop()
            index = self.client.index("buyers")

            # Sanitize data for JSON serialization
            buyers = _sanitize_documents(buyers)

            # Add documents in batches
            batch_size = 1000
            total = 0
            for i in range(0, len(buyers), batch_size):
                batch = buyers[i:i + batch_size]
                await loop.run_in_executor(
                    None,
                    lambda b=batch: index.add_documents(b)
                )
                total += len(batch)

            logger.info(f"Indexed {total} buyers to Meilisearch")
            return total

        except Exception as e:
            logger.error(f"Failed to index buyers: {e}")
            return 0

    async def index_orders(self, orders: List[dict]) -> int:
        """
        Index orders for search.

        Args:
            orders: List of order dicts

        Returns:
            Number of documents indexed
        """
        if not orders:
            return 0

        try:
            loop = asyncio.get_event_loop()
            index = self.client.index("orders")

            # Sanitize data for JSON serialization
            orders = _sanitize_documents(orders)

            batch_size = 1000
            total = 0
            for i in range(0, len(orders), batch_size):
                batch = orders[i:i + batch_size]
                await loop.run_in_executor(
                    None,
                    lambda b=batch: index.add_documents(b)
                )
                total += len(batch)

            logger.info(f"Indexed {total} orders to Meilisearch")
            return total

        except Exception as e:
            logger.error(f"Failed to index orders: {e}")
            return 0

    async def index_products(self, products: List[dict]) -> int:
        """
        Index products for search.

        Args:
            products: List of product dicts

        Returns:
            Number of documents indexed
        """
        if not products:
            return 0

        try:
            loop = asyncio.get_event_loop()
            index = self.client.index("products")

            # Sanitize data for JSON serialization
            products = _sanitize_documents(products)

            batch_size = 1000
            total = 0
            for i in range(0, len(products), batch_size):
                batch = products[i:i + batch_size]
                await loop.run_in_executor(
                    None,
                    lambda b=batch: index.add_documents(b)
                )
                total += len(batch)

            logger.info(f"Indexed {total} products to Meilisearch")
            return total

        except Exception as e:
            logger.error(f"Failed to index products: {e}")
            return 0

    async def get_stats(self) -> Dict[str, Any]:
        """Get index statistics."""
        try:
            loop = asyncio.get_event_loop()
            stats = {}

            for index_name in ["buyers", "orders", "products"]:
                try:
                    index = self.client.index(index_name)
                    index_stats = await loop.run_in_executor(
                        None,
                        lambda i=index: i.get_stats()
                    )
                    stats[index_name] = {
                        "documents": index_stats.get("numberOfDocuments", 0),
                        "is_indexing": index_stats.get("isIndexing", False)
                    }
                except Exception:
                    stats[index_name] = {"documents": 0, "is_indexing": False}

            return stats

        except Exception as e:
            logger.warning(f"Failed to get Meilisearch stats: {e}")
            return {}


# ═══════════════════════════════════════════════════════════════════════════════
# SINGLETON INSTANCE
# ═══════════════════════════════════════════════════════════════════════════════

_meili_client: Optional[MeiliClient] = None


def get_meili_client() -> MeiliClient:
    """Get singleton Meilisearch client instance."""
    global _meili_client
    if _meili_client is None:
        _meili_client = MeiliClient(
            url=config.meilisearch.url,
            master_key=config.meilisearch.master_key
        )
    return _meili_client


async def init_meilisearch() -> bool:
    """Initialize Meilisearch on startup."""
    client = get_meili_client()

    # Check health
    health = await client.health_check()
    if health.get("status") != "available":
        logger.warning(f"Meilisearch not available: {health}")
        return False

    # Initialize indexes
    return await client.initialize_indexes()
