"""
Category service for fetching and caching product categories.
"""
import logging
import threading
import time
from typing import Dict, List, Optional, Any
import requests

from bot.config import KEYCRM_API_KEY, KEYCRM_BASE_URL

logger = logging.getLogger(__name__)

# Cache for categories and product mappings
_categories_cache: Dict[int, Dict[str, Any]] = {}
_product_category_cache: Dict[int, int] = {}  # product_id -> category_id
_cache_lock = threading.Lock()
_last_cache_update: float = 0
_last_products_update: float = 0
CACHE_TTL = 3600  # 1 hour
_products_loaded = False

# Singleton session for connection pooling
_session: Optional[requests.Session] = None


def _get_session() -> requests.Session:
    """Get singleton session with connection pooling."""
    global _session
    if _session is None:
        _session = requests.Session()
        _session.headers.update({
            "Authorization": f"Bearer {KEYCRM_API_KEY}",
            "Content-Type": "application/json"
        })
    return _session


def fetch_all_categories() -> Dict[int, Dict[str, Any]]:
    """Fetch all categories from KeyCRM API."""
    categories = {}
    page = 1
    session = _get_session()

    while True:
        try:
            url = f"{KEYCRM_BASE_URL}/products/categories"
            resp = session.get(
                url,
                params={"page": page, "limit": 50},
                timeout=30
            )
            resp.raise_for_status()
            data = resp.json()

            for cat in data.get("data", []):
                categories[cat['id']] = {
                    'id': cat['id'],
                    'name': cat['name'],
                    'parent_id': cat.get('parent_id')
                }

            if not data.get("next_page_url"):
                break
            page += 1

        except Exception as e:
            logger.error(f"Error fetching categories: {e}")
            break

    return categories


def fetch_product_category(product_id: int) -> Optional[int]:
    """Fetch category_id for a single product."""
    try:
        url = f"{KEYCRM_BASE_URL}/products/{product_id}"
        resp = _get_session().get(url, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        return data.get("category_id")
    except Exception as e:
        logger.error(f"Error fetching product {product_id}: {e}")
        return None


def fetch_all_products_categories() -> Dict[int, int]:
    """
    Batch fetch all products with their categories.
    Much faster than fetching individual products.
    """
    product_categories = {}
    page = 1
    session = _get_session()

    logger.info("Starting batch fetch of all products...")

    while True:
        try:
            url = f"{KEYCRM_BASE_URL}/products"
            resp = session.get(
                url,
                params={"page": page, "limit": 50},
                timeout=30
            )
            resp.raise_for_status()
            data = resp.json()

            products = data.get("data", [])
            for product in products:
                product_id = product.get("id")
                category_id = product.get("category_id")
                if product_id and category_id:
                    product_categories[product_id] = category_id

            if not data.get("next_page_url"):
                break
            page += 1

        except Exception as e:
            logger.error(f"Error fetching products page {page}: {e}")
            break

    logger.info(f"Batch fetched {len(product_categories)} products with categories")
    return product_categories


def warm_product_cache() -> None:
    """Pre-load all product categories into cache."""
    global _product_category_cache, _products_loaded, _last_products_update

    with _cache_lock:
        if _products_loaded and time.time() - _last_products_update < CACHE_TTL:
            return

    product_categories = fetch_all_products_categories()

    with _cache_lock:
        _product_category_cache.update(product_categories)
        _products_loaded = True
        _last_products_update = time.time()


def get_categories() -> Dict[int, Dict[str, Any]]:
    """Get cached categories, refreshing if needed."""
    global _categories_cache, _last_cache_update

    with _cache_lock:
        if time.time() - _last_cache_update > CACHE_TTL or not _categories_cache:
            logger.info("Refreshing categories cache...")
            _categories_cache = fetch_all_categories()
            _last_cache_update = time.time()
            logger.info(f"Cached {len(_categories_cache)} categories")

    return _categories_cache


def get_root_categories() -> List[Dict[str, Any]]:
    """Get only root categories (parent_id is None)."""
    categories = get_categories()
    return [
        cat for cat in categories.values()
        if cat.get('parent_id') is None
    ]


def get_category_with_children(category_id: int) -> List[int]:
    """Get category ID and all its children IDs."""
    categories = get_categories()
    result = [category_id]

    # Find all children recursively
    def find_children(parent_id: int):
        for cat in categories.values():
            if cat.get('parent_id') == parent_id:
                result.append(cat['id'])
                find_children(cat['id'])

    find_children(category_id)
    return result


def get_product_category(product_id: int) -> Optional[int]:
    """Get category_id for a product (cached)."""
    global _product_category_cache, _products_loaded

    # First check if we have a cached value
    with _cache_lock:
        if product_id in _product_category_cache:
            return _product_category_cache[product_id]

    # If batch cache not loaded yet, try to load it
    if not _products_loaded:
        warm_product_cache()
        with _cache_lock:
            if product_id in _product_category_cache:
                return _product_category_cache[product_id]

    # Still not found - fetch individual product (rare case for new products)
    category_id = fetch_product_category(product_id)

    if category_id is not None:
        with _cache_lock:
            _product_category_cache[product_id] = category_id

    return category_id


def get_root_category_id(category_id: int) -> Optional[int]:
    """Get the root category ID for a given category."""
    categories = get_categories()

    if category_id not in categories:
        return None

    cat = categories[category_id]
    while cat.get('parent_id') is not None:
        parent_id = cat['parent_id']
        if parent_id not in categories:
            break
        cat = categories[parent_id]

    return cat['id']


def get_categories_for_api() -> List[Dict[str, Any]]:
    """Get categories formatted for API response."""
    root_cats = get_root_categories()
    return sorted(
        [{'id': c['id'], 'name': c['name']} for c in root_cats],
        key=lambda x: x['name']
    )


def get_child_categories(parent_id: int) -> List[Dict[str, Any]]:
    """Get child categories for a parent category."""
    categories = get_categories()
    children = [
        {'id': c['id'], 'name': c['name']}
        for c in categories.values()
        if c.get('parent_id') == parent_id
    ]
    return sorted(children, key=lambda x: x['name'])
