"""
Brand service for fetching and caching product brands from custom_fields.
"""
import logging
import threading
import time
from typing import Dict, List, Optional, Set

from bot.config import KEYCRM_API_KEY, KEYCRM_BASE_URL
from web.services.category_service import _get_session

logger = logging.getLogger(__name__)

# Cache for brands
_brands_cache: Set[str] = set()
_product_brand_cache: Dict[int, str] = {}  # product_id -> brand name
_cache_lock = threading.Lock()
_last_cache_update: float = 0
_brands_loaded = False
CACHE_TTL = 3600  # 1 hour

# Brand custom field ID (from KeyCRM)
BRAND_FIELD_UUID = "CT_1001"
BRAND_FIELD_NAME = "Brand"


def fetch_all_products_with_brands() -> tuple:
    """
    Fetch all products with custom_fields to extract brands.
    Returns: (brands_set, product_brand_dict)
    """
    brands = set()
    product_brands = {}
    page = 1
    session = _get_session()

    logger.info("Starting batch fetch of all products with brands...")

    while True:
        try:
            url = f"{KEYCRM_BASE_URL}/products"
            resp = session.get(
                url,
                params={"page": page, "limit": 50, "include": "custom_fields"},
                timeout=30
            )
            resp.raise_for_status()
            data = resp.json()

            products = data.get("data", [])
            for product in products:
                product_id = product.get("id")
                if not product_id:
                    continue

                # Extract brand from custom_fields
                custom_fields = product.get("custom_fields", [])
                for field in custom_fields:
                    if field.get("name") == BRAND_FIELD_NAME or field.get("uuid") == BRAND_FIELD_UUID:
                        values = field.get("value", [])
                        if values and isinstance(values, list) and values[0]:
                            brand = values[0]
                            brands.add(brand)
                            product_brands[product_id] = brand
                        break

            if not data.get("next_page_url"):
                break
            page += 1

        except Exception as e:
            logger.error(f"Error fetching products page {page}: {e}")
            break

    logger.info(f"Found {len(brands)} unique brands across {len(product_brands)} products")
    return brands, product_brands


def warm_brand_cache() -> None:
    """Pre-load all product brands into cache."""
    global _brands_cache, _product_brand_cache, _brands_loaded, _last_cache_update

    with _cache_lock:
        if _brands_loaded and time.time() - _last_cache_update < CACHE_TTL:
            return

    brands, product_brands = fetch_all_products_with_brands()

    with _cache_lock:
        _brands_cache = brands
        _product_brand_cache = product_brands
        _brands_loaded = True
        _last_cache_update = time.time()


def get_brands() -> List[str]:
    """Get list of all brands (sorted)."""
    global _brands_cache, _brands_loaded

    if not _brands_loaded:
        warm_brand_cache()

    with _cache_lock:
        return sorted(list(_brands_cache))


def get_product_brand(product_id: int) -> Optional[str]:
    """Get brand name for a product."""
    global _product_brand_cache, _brands_loaded

    if not _brands_loaded:
        warm_brand_cache()

    with _cache_lock:
        return _product_brand_cache.get(product_id)


def get_brands_for_api() -> List[Dict[str, str]]:
    """Get brands formatted for API response."""
    brands = get_brands()
    return [{"name": brand} for brand in brands]


# Export cache for direct access (performance optimization)
def get_product_brand_cache() -> Dict[int, str]:
    """Get the product brand cache for bulk lookups."""
    global _product_brand_cache, _brands_loaded

    if not _brands_loaded:
        warm_brand_cache()

    return _product_brand_cache
