"""
Brand service for fetching and caching product brands from custom_fields.
Fully async implementation using core.keycrm client.
"""
import asyncio
import logging
import time
from typing import Dict, List, Optional, Set

from core.keycrm import get_async_client

logger = logging.getLogger(__name__)

# Cache for brands
_brands_cache: Set[str] = set()
_product_brand_cache: Dict[int, str] = {}  # product_id -> brand name
_cache_lock = asyncio.Lock()
_load_lock = asyncio.Lock()  # Dedicated lock for loading to prevent concurrent fetches
_last_cache_update: float = 0
_brands_loaded = False
CACHE_TTL = 3600  # 1 hour

# Brand custom field ID (from KeyCRM)
BRAND_FIELD_UUID = "CT_1001"
BRAND_FIELD_NAME = "Brand"


async def fetch_all_products_with_brands() -> tuple:
    """
    Fetch all products with custom_fields to extract brands.
    Returns: (brands_set, product_brand_dict)
    """
    brands = set()
    product_brands = {}
    client = await get_async_client()

    logger.info("Starting async batch fetch of all products with brands...")

    try:
        async for batch in client.paginate("products", params={"include": "custom_fields"}, page_size=50):
            for product in batch:
                product_id = product.get("id")
                if not product_id:
                    continue

                # Extract brand from custom_fields (optimized: check uuid first as it's more reliable)
                custom_fields = product.get("custom_fields") or []
                for field in custom_fields:
                    # Check UUID first (faster string comparison, more reliable identifier)
                    if field.get("uuid") == BRAND_FIELD_UUID or field.get("name") == BRAND_FIELD_NAME:
                        values = field.get("value")
                        if values and isinstance(values, list) and len(values) > 0 and values[0]:
                            brand = values[0]
                            brands.add(brand)
                            product_brands[product_id] = brand
                        break
    except Exception as e:
        logger.error(f"Error fetching products with brands: {e}")

    logger.info(f"Found {len(brands)} unique brands across {len(product_brands)} products")
    return brands, product_brands


async def warm_brand_cache() -> None:
    """Pre-load all product brands into cache.

    Uses double-checked locking pattern with simplified lock structure.
    """
    global _brands_cache, _product_brand_cache, _brands_loaded, _last_cache_update

    # Quick check without lock (volatile read)
    if _brands_loaded and time.time() - _last_cache_update < CACHE_TTL:
        return

    # Acquire load lock to prevent concurrent fetches
    async with _load_lock:
        # Double-check after acquiring lock
        if _brands_loaded and time.time() - _last_cache_update < CACHE_TTL:
            return

        # Fetch outside cache lock
        brands, product_brands = await fetch_all_products_with_brands()

        # Update cache atomically
        async with _cache_lock:
            _brands_cache = brands
            _product_brand_cache = product_brands
            _brands_loaded = True
            _last_cache_update = time.time()


async def get_brands() -> List[str]:
    """Get list of all brands (sorted)."""
    global _brands_cache, _brands_loaded

    if not _brands_loaded:
        await warm_brand_cache()

    async with _cache_lock:
        return sorted(list(_brands_cache))


async def get_product_brand(product_id: int) -> Optional[str]:
    """Get brand name for a product."""
    global _product_brand_cache, _brands_loaded

    if not _brands_loaded:
        await warm_brand_cache()

    async with _cache_lock:
        return _product_brand_cache.get(product_id)


async def get_brands_for_api() -> List[Dict[str, str]]:
    """Get brands formatted for API response."""
    brands = await get_brands()
    return [{"name": brand} for brand in brands]


def get_product_brand_cache() -> Dict[int, str]:
    """Get the product brand cache for sync access."""
    return _product_brand_cache


def is_brands_loaded() -> bool:
    """Check if brands cache is loaded."""
    return _brands_loaded
