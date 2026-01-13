"""
Category service for fetching and caching product categories.
Fully async implementation using core.keycrm client.
"""
import asyncio
import logging
import time
from typing import Dict, List, Optional, Any

from core.keycrm import get_async_client

logger = logging.getLogger(__name__)

# Cache for categories and product mappings
_categories_cache: Dict[int, Dict[str, Any]] = {}
_product_category_cache: Dict[int, int] = {}  # product_id -> category_id
_cache_lock = asyncio.Lock()
_last_cache_update: float = 0
_last_products_update: float = 0
CACHE_TTL = 3600  # 1 hour
_products_loaded = False


async def fetch_all_categories() -> Dict[int, Dict[str, Any]]:
    """Fetch all categories from KeyCRM API."""
    categories = {}
    client = await get_async_client()

    try:
        # Use pagination to fetch all categories
        async for batch in client.paginate("product-category", page_size=50):
            for cat in batch:
                categories[cat['id']] = {
                    'id': cat['id'],
                    'name': cat['name'],
                    'parent_id': cat.get('parent_id')
                }
    except Exception as e:
        logger.error(f"Error fetching categories: {e}")

    return categories


async def fetch_all_products_categories() -> Dict[int, int]:
    """
    Batch fetch all products with their categories.
    Much faster than fetching individual products.
    """
    product_categories = {}
    client = await get_async_client()

    logger.info("Starting async batch fetch of all products...")

    try:
        async for batch in client.paginate("product", page_size=50):
            for product in batch:
                product_id = product.get("id")
                category_id = product.get("category_id")
                if product_id and category_id:
                    product_categories[product_id] = category_id
    except Exception as e:
        logger.error(f"Error fetching products: {e}")

    logger.info(f"Async batch fetched {len(product_categories)} products with categories")
    return product_categories


async def warm_product_cache() -> None:
    """Pre-load all product categories into cache."""
    global _product_category_cache, _products_loaded, _last_products_update

    async with _cache_lock:
        if _products_loaded and time.time() - _last_products_update < CACHE_TTL:
            return

    product_categories = await fetch_all_products_categories()

    async with _cache_lock:
        _product_category_cache.update(product_categories)
        _products_loaded = True
        _last_products_update = time.time()


async def get_categories() -> Dict[int, Dict[str, Any]]:
    """Get cached categories, refreshing if needed."""
    global _categories_cache, _last_cache_update

    async with _cache_lock:
        if time.time() - _last_cache_update > CACHE_TTL or not _categories_cache:
            logger.info("Refreshing categories cache (async)...")
            _categories_cache = await fetch_all_categories()
            _last_cache_update = time.time()
            logger.info(f"Cached {len(_categories_cache)} categories")

    return _categories_cache


async def get_root_categories() -> List[Dict[str, Any]]:
    """Get only root categories (parent_id is None)."""
    categories = await get_categories()
    return [
        cat for cat in categories.values()
        if cat.get('parent_id') is None
    ]


async def get_category_with_children(category_id: int) -> List[int]:
    """Get category ID and all its children IDs."""
    categories = await get_categories()
    result = [category_id]

    # Find all children recursively
    def find_children(parent_id: int):
        for cat in categories.values():
            if cat.get('parent_id') == parent_id:
                result.append(cat['id'])
                find_children(cat['id'])

    find_children(category_id)
    return result


async def get_product_category(product_id: int) -> Optional[int]:
    """Get category_id for a product (cached)."""
    global _product_category_cache, _products_loaded

    # First check if we have a cached value
    async with _cache_lock:
        if product_id in _product_category_cache:
            return _product_category_cache[product_id]

    # If batch cache not loaded yet, try to load it
    if not _products_loaded:
        await warm_product_cache()
        async with _cache_lock:
            if product_id in _product_category_cache:
                return _product_category_cache[product_id]

    # Still not found - return None (rare case for new products)
    return None


async def get_root_category_id(category_id: int) -> Optional[int]:
    """Get the root category ID for a given category."""
    categories = await get_categories()

    if category_id not in categories:
        return None

    cat = categories[category_id]
    while cat.get('parent_id') is not None:
        parent_id = cat['parent_id']
        if parent_id not in categories:
            break
        cat = categories[parent_id]

    return cat['id']


async def get_categories_for_api() -> List[Dict[str, Any]]:
    """Get categories formatted for API response."""
    root_cats = await get_root_categories()
    return sorted(
        [{'id': c['id'], 'name': c['name']} for c in root_cats],
        key=lambda x: x['name']
    )


async def get_child_categories(parent_id: int) -> List[Dict[str, Any]]:
    """Get child categories for a parent category."""
    categories = await get_categories()
    children = [
        {'id': c['id'], 'name': c['name']}
        for c in categories.values()
        if c.get('parent_id') == parent_id
    ]
    return sorted(children, key=lambda x: x['name'])


def get_product_category_cache() -> Dict[int, int]:
    """Get the product category cache for sync access."""
    return _product_category_cache


def is_products_loaded() -> bool:
    """Check if products cache is loaded."""
    return _products_loaded
