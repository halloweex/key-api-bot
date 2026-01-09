"""
Dashboard service for transforming sales data into chart-friendly formats.
"""
import logging
from datetime import date, datetime, timedelta
from typing import Dict, List, Optional, Any, Tuple, Union
import threading
import time
from zoneinfo import ZoneInfo

from bot.api_client import KeyCRMClient
from bot.services import ReportService
from bot.config import (
    KEYCRM_API_KEY,
    KEYCRM_BASE_URL,
    SOURCE_MAPPING,
    DEFAULT_TIMEZONE,
    RETURN_STATUS_IDS,
    TELEGRAM_MANAGER_IDS,
)

logger = logging.getLogger(__name__)


# ─── In-Memory Cache ─────────────────────────────────────────────────────────
CACHE_TTL_SECONDS = 300  # 5 minutes

_cache: Dict[str, Any] = {}
_cache_lock = threading.Lock()


def _get_cached(key: str) -> Optional[Any]:
    """Get value from cache if not expired."""
    with _cache_lock:
        if key in _cache:
            data, timestamp = _cache[key]
            if time.time() - timestamp < CACHE_TTL_SECONDS:
                return data
            del _cache[key]
    return None


def _set_cached(key: str, value: Any) -> None:
    """Store value in cache with timestamp."""
    with _cache_lock:
        _cache[key] = (value, time.time())


def _get_aggregated_data(start_date: str, end_date: str) -> Tuple[Dict, Dict, int, Dict, Dict]:
    """
    Get aggregated sales data with caching.
    Returns: (sales_dict, counts_dict, total_orders, revenue_dict, returns_dict)
    """
    cache_key = f"aggregated:{start_date}:{end_date}"
    cached = _get_cached(cache_key)
    if cached is not None:
        return cached

    service = get_report_service()
    try:
        result = service.aggregate_sales_data(
            target_date=(start_date, end_date),
            tz_name=DEFAULT_TIMEZONE
        )
        _set_cached(cache_key, result)
        return result
    except Exception as e:
        logger.error(f"Failed to aggregate sales data for {start_date} to {end_date}: {e}")
        return ({}, {}, 0, {}, {})


# ─── Background Cache Warming ────────────────────────────────────────────────
_warming_thread: Optional[threading.Thread] = None
_stop_warming = threading.Event()


def _warm_cache_for_period(period: str) -> None:
    """Pre-fetch data for a specific period."""
    try:
        start, end = parse_period(period, None, None)
        # Warm aggregated data
        _get_aggregated_data(start, end)
        # Warm revenue trend
        get_revenue_trend(start, end)
    except Exception as e:
        logger.warning(f"Cache warming failed for period '{period}': {e}")


def _cache_warming_loop() -> None:
    """Background loop that warms cache every 4 minutes."""
    import logging
    from concurrent.futures import ThreadPoolExecutor, as_completed
    logger = logging.getLogger(__name__)

    while not _stop_warming.is_set():
        try:
            logger.info("Cache warming: pre-fetching data (parallel)...")
            # Warm cache for all periods in parallel
            periods = ["today", "yesterday", "week", "last_week", "month", "last_month"]
            with ThreadPoolExecutor(max_workers=4) as executor:
                futures = {executor.submit(_warm_cache_for_period, p): p for p in periods}
                for future in as_completed(futures):
                    if _stop_warming.is_set():
                        break
                    period = futures[future]
                    try:
                        future.result()
                    except Exception as e:
                        logger.error(f"Cache warming error for {period}: {e}")
            logger.info("Cache warming: complete")
        except Exception as e:
            logger.error(f"Cache warming error: {e}")

        # Wait 4 minutes (less than 5-min TTL to keep cache fresh)
        _stop_warming.wait(240)


def start_cache_warming() -> None:
    """Start background cache warming thread."""
    global _warming_thread
    if _warming_thread is None or not _warming_thread.is_alive():
        _stop_warming.clear()
        _warming_thread = threading.Thread(target=_cache_warming_loop, daemon=True)
        _warming_thread.start()


def stop_cache_warming() -> None:
    """Stop background cache warming thread."""
    _stop_warming.set()
    if _warming_thread:
        _warming_thread.join(timeout=5)


# Source colors for charts
SOURCE_COLORS = {
    1: "#7C3AED",  # Instagram - purple (Accent)
    2: "#2563EB",  # Telegram - blue (Primary)
    3: "#F59E0B",  # Opencart - orange (Warning)
    4: "#eb4200",  # Shopify - orange-red
}

SOURCE_COLORS_LIST = ["#7C3AED", "#2563EB", "#F59E0B", "#eb4200"]


# Singleton instances for connection reuse
_client: Optional[KeyCRMClient] = None
_report_service: Optional[ReportService] = None


def _wrap_label(text: str, max_chars: int = 25) -> List[str]:
    """Wrap long text into multiple lines for Chart.js labels."""
    if len(text) <= max_chars:
        return [text]

    words = text.split()
    lines = []
    current_line = ""

    for word in words:
        if len(current_line) + len(word) + 1 <= max_chars:
            current_line = f"{current_line} {word}".strip()
        else:
            if current_line:
                lines.append(current_line)
            current_line = word

    if current_line:
        lines.append(current_line)

    # Limit to 2 lines max
    if len(lines) > 2:
        lines = [lines[0], lines[1][:max_chars-3] + "..."]

    return lines


def get_report_service() -> ReportService:
    """Get singleton ReportService instance (reuses connections)."""
    global _client, _report_service
    if _report_service is None:
        _client = KeyCRMClient(KEYCRM_API_KEY)
        _report_service = ReportService(_client)
    return _report_service


def parse_period(period: Optional[str], start_date: Optional[str], end_date: Optional[str]) -> Tuple[str, str]:
    """
    Parse period shortcut or dates into (start_date, end_date) tuple.

    Args:
        period: Shortcut like 'today', 'yesterday', 'week', 'month'
        start_date: Start date in YYYY-MM-DD format
        end_date: End date in YYYY-MM-DD format

    Returns:
        Tuple of (start_date, end_date) strings
    """
    today = datetime.now().date()

    if period:
        if period == "today":
            return (today.strftime("%Y-%m-%d"), today.strftime("%Y-%m-%d"))
        elif period == "yesterday":
            yesterday = today - timedelta(days=1)
            return (yesterday.strftime("%Y-%m-%d"), yesterday.strftime("%Y-%m-%d"))
        elif period == "week":
            # This week (Monday to today)
            start_of_week = today - timedelta(days=today.weekday())
            return (start_of_week.strftime("%Y-%m-%d"), today.strftime("%Y-%m-%d"))
        elif period == "last_week":
            # Last week (Monday to Sunday)
            start_of_this_week = today - timedelta(days=today.weekday())
            end_of_last_week = start_of_this_week - timedelta(days=1)
            start_of_last_week = end_of_last_week - timedelta(days=6)
            return (start_of_last_week.strftime("%Y-%m-%d"), end_of_last_week.strftime("%Y-%m-%d"))
        elif period == "month":
            # This month (1st to today)
            start_of_month = today.replace(day=1)
            return (start_of_month.strftime("%Y-%m-%d"), today.strftime("%Y-%m-%d"))
        elif period == "last_month":
            # Last month (1st to last day)
            first_of_this_month = today.replace(day=1)
            last_of_last_month = first_of_this_month - timedelta(days=1)
            first_of_last_month = last_of_last_month.replace(day=1)
            return (first_of_last_month.strftime("%Y-%m-%d"), last_of_last_month.strftime("%Y-%m-%d"))

    # Use provided dates or default to today
    if start_date and end_date:
        return (start_date, end_date)

    return (today.strftime("%Y-%m-%d"), today.strftime("%Y-%m-%d"))


# ─── Date/Time Utilities ──────────────────────────────────────────────────────

def _parse_date_range(
    start_date: str,
    end_date: str,
    tz_name: str = DEFAULT_TIMEZONE
) -> Tuple[date, date, datetime, datetime, datetime, datetime, ZoneInfo]:
    """
    Parse date strings into local and UTC boundaries with timezone info.

    Args:
        start_date: Start date (YYYY-MM-DD)
        end_date: End date (YYYY-MM-DD)
        tz_name: Timezone name (default: Europe/Kyiv)

    Returns:
        Tuple of (start_date, end_date, local_start, local_end, utc_start, utc_end_with_buffer, tz)
        - start_date, end_date: date objects
        - local_start, local_end: datetime with local timezone
        - utc_start: start of day in UTC
        - utc_end_with_buffer: end of day in UTC + 24h buffer (for API filtering)
        - tz: ZoneInfo object
    """
    start = datetime.strptime(start_date, "%Y-%m-%d").date()
    end = datetime.strptime(end_date, "%Y-%m-%d").date()

    tz = ZoneInfo(tz_name)
    local_start = datetime(start.year, start.month, start.day, 0, 0, 0, tzinfo=tz)
    local_end = datetime(end.year, end.month, end.day, 23, 59, 59, tzinfo=tz)
    utc_start = local_start.astimezone(ZoneInfo("UTC"))
    utc_end_with_buffer = local_end.astimezone(ZoneInfo("UTC")) + timedelta(hours=24)

    return start, end, local_start, local_end, utc_start, utc_end_with_buffer, tz


def _init_daily_dict(
    start: Union[date, datetime],
    end: Union[date, datetime],
    default_value: Any = 0.0
) -> Dict[str, Any]:
    """
    Initialize a dictionary with date keys for the given range.

    Args:
        start: Start date
        end: End date
        default_value: Default value for each day (0.0 or {"revenue": 0.0, "orders": 0})

    Returns:
        Dict with date string keys initialized to default_value
    """
    daily_dict = {}
    current = start
    while current <= end:
        if isinstance(default_value, dict):
            daily_dict[current.strftime("%Y-%m-%d")] = default_value.copy()
        else:
            daily_dict[current.strftime("%Y-%m-%d")] = default_value
        current += timedelta(days=1)
    return daily_dict


def _is_valid_order(
    order: Dict[str, Any],
    utc_start: datetime,
    local_end: datetime,
    return_status_ids: set
) -> Tuple[bool, Optional[datetime]]:
    """
    Check if order is valid (within date range and not a return).

    Args:
        order: Order dict from API
        utc_start: Start boundary in UTC
        local_end: End boundary with local timezone (will be converted to UTC)
        return_status_ids: Set of return/cancel status IDs to exclude

    Returns:
        Tuple of (is_valid, ordered_at_datetime or None)
    """
    ordered_at_str = order.get("ordered_at")
    if not ordered_at_str:
        return False, None

    try:
        ordered_at = datetime.fromisoformat(ordered_at_str.replace("Z", "+00:00"))
    except (ValueError, AttributeError) as e:
        logger.debug(f"Invalid ordered_at format: {ordered_at_str}, error: {e}")
        return False, None

    # Convert local_end to UTC for comparison
    utc_end = local_end.astimezone(ZoneInfo("UTC"))
    if not (utc_start <= ordered_at <= utc_end):
        return False, None

    if order.get("status_id") in return_status_ids:
        return False, None

    # Filter Telegram orders by manager
    source_id = order.get("source_id")
    if source_id == 2:
        manager = order.get("manager")
        manager_id = str(manager.get("id")) if manager else None
        if manager_id not in TELEGRAM_MANAGER_IDS:
            return False, None

    return True, ordered_at


def get_revenue_trend(
    start_date: str,
    end_date: str,
    granularity: str = "daily"
) -> Dict[str, Any]:
    """
    Get revenue data over time for line chart.
    Optimized: fetches all data in ONE API call, then groups by day.

    Args:
        start_date: Start date (YYYY-MM-DD)
        end_date: End date (YYYY-MM-DD)
        granularity: 'daily', 'weekly', or 'monthly'

    Returns:
        Chart.js compatible data structure
    """
    cache_key = f"revenue_trend:{start_date}:{end_date}:{granularity}"
    cached = _get_cached(cache_key)
    if cached is not None:
        return cached

    # Parse dates and initialize daily revenue using utilities
    start, end, local_start, local_end, utc_start, utc_end_buffer, tz = _parse_date_range(
        start_date, end_date
    )
    daily_revenue = _init_daily_dict(start, end, 0.0)

    try:
        # Use singleton client for connection reuse
        if _client is None:
            get_report_service()
        client = _client

        params = {
            "include": "products,manager",
            "limit": 50,
            "filter[created_between]": f"{utc_start.strftime('%Y-%m-%d %H:%M:%S')}, {utc_end_buffer.strftime('%Y-%m-%d %H:%M:%S')}",
        }

        page = 1
        return_status_ids = set(RETURN_STATUS_IDS)

        while True:
            params["page"] = page
            resp = client.get_orders(params)

            if isinstance(resp, dict) and resp.get("error"):
                logger.error(f"API error fetching orders: {resp.get('error')}")
                break

            batch = resp.get("data", [])
            if not batch:
                break

            for order in batch:
                # Use utility for order validation
                is_valid, ordered_at = _is_valid_order(
                    order, utc_start, local_end, return_status_ids
                )
                if not is_valid:
                    continue

                # Get local date and add revenue
                local_ordered = ordered_at.astimezone(tz)
                date_key = local_ordered.strftime("%Y-%m-%d")
                if date_key in daily_revenue:
                    daily_revenue[date_key] += float(order.get("grand_total", 0))

            if len(batch) < params["limit"]:
                break
            page += 1

    except Exception as e:
        logger.error(f"Error fetching revenue trend: {e}")

    # Build response
    labels = []
    data = []
    current = start
    while current <= end:
        date_key = current.strftime("%Y-%m-%d")
        labels.append(current.strftime("%d.%m"))
        data.append(round(daily_revenue.get(date_key, 0), 2))
        current += timedelta(days=1)

    result = {
        "labels": labels,
        "datasets": [{
            "label": "Revenue (UAH)",
            "data": data,
            "borderColor": "#16A34A",
            "backgroundColor": "rgba(22, 163, 74, 0.1)",
            "fill": True,
            "tension": 0.3
        }]
    }
    _set_cached(cache_key, result)
    return result


def get_sales_by_source(
    start_date: str,
    end_date: str,
    category_id: Optional[int] = None,
    brand: Optional[str] = None
) -> Dict[str, Any]:
    """
    Get sales data aggregated by source for bar/pie chart.

    Args:
        start_date: Start date (YYYY-MM-DD)
        end_date: End date (YYYY-MM-DD)
        category_id: Optional category filter
        brand: Optional brand filter

    Returns:
        Chart.js compatible data structure
    """
    if category_id or brand:
        # Use filtered data
        return _get_sales_by_source_with_filter(start_date, end_date, category_id, brand)

    _, counts_dict, _, revenue_dict, _ = _get_aggregated_data(start_date, end_date)

    # Build data for each source
    source_data = []
    for source_id in [1, 2, 4]:  # Instagram, Telegram, Shopify (Opencart excluded)
        orders = counts_dict.get(source_id, counts_dict.get(str(source_id), 0))
        revenue = round(revenue_dict.get(source_id, revenue_dict.get(str(source_id), 0)), 2)
        source_data.append({
            'source_id': source_id,
            'name': SOURCE_MAPPING.get(source_id, f"Source {source_id}"),
            'orders': orders,
            'revenue': revenue,
            'color': SOURCE_COLORS.get(source_id, "#999999")
        })

    # Sort by orders descending
    source_data.sort(key=lambda x: x['orders'], reverse=True)

    return {
        "labels": [s['name'] for s in source_data],
        "orders": [s['orders'] for s in source_data],
        "revenue": [s['revenue'] for s in source_data],
        "backgroundColor": [s['color'] for s in source_data]
    }


def get_top_products(
    start_date: str,
    end_date: str,
    source_id: Optional[int] = None,
    limit: int = 10,
    category_id: Optional[int] = None,
    brand: Optional[str] = None
) -> Dict[str, Any]:
    """
    Get top products for horizontal bar chart.

    Args:
        start_date: Start date (YYYY-MM-DD)
        end_date: End date (YYYY-MM-DD)
        source_id: Optional source filter (1=Instagram, 2=Telegram, etc.)
        limit: Number of products to return
        category_id: Optional category filter
        brand: Optional brand filter

    Returns:
        Chart.js compatible data structure
    """
    if category_id or brand:
        return _get_top_products_with_filter(start_date, end_date, source_id, limit, category_id, brand)

    sales_dict, _, _, _, _ = _get_aggregated_data(start_date, end_date)

    # Aggregate products across sources or filter by source
    products = {}
    if source_id:
        # Handle both int and string keys
        products = sales_dict.get(source_id, sales_dict.get(str(source_id), {}))
    else:
        for src_products in sales_dict.values():
            for product_name, qty in src_products.items():
                products[product_name] = products.get(product_name, 0) + qty

    # Sort and limit (descending - highest at top)
    sorted_products = sorted(products.items(), key=lambda x: x[1], reverse=True)[:limit]

    labels = [_wrap_label(p[0]) for p in sorted_products]
    data = [p[1] for p in sorted_products]

    # Calculate percentages
    total = sum(data) if data else 1
    percentages = [round(d / total * 100, 1) for d in data]

    return {
        "labels": labels,
        "data": data,
        "percentages": percentages,
        "backgroundColor": "#2563EB"
    }


def get_summary_stats(
    start_date: str,
    end_date: str,
    category_id: Optional[int] = None,
    brand: Optional[str] = None
) -> Dict[str, Any]:
    """
    Get summary statistics for dashboard cards.

    Args:
        start_date: Start date (YYYY-MM-DD)
        end_date: End date (YYYY-MM-DD)
        category_id: Optional category filter
        brand: Optional brand filter

    Returns:
        Summary statistics dictionary
    """
    if category_id or brand:
        return _get_summary_stats_with_filter(start_date, end_date, category_id, brand)

    _, counts_dict, total_orders, revenue_dict, returns_dict = _get_aggregated_data(start_date, end_date)

    total_revenue = sum(revenue_dict.values())
    avg_check = total_revenue / total_orders if total_orders > 0 else 0

    total_returns = sum(r.get("count", 0) for r in returns_dict.values())
    returns_revenue = sum(r.get("revenue", 0) for r in returns_dict.values())

    return {
        "totalOrders": total_orders,
        "totalRevenue": round(total_revenue, 2),
        "avgCheck": round(avg_check, 2),
        "totalReturns": total_returns,
        "returnsRevenue": round(returns_revenue, 2),
        "startDate": start_date,
        "endDate": end_date
    }


# ─── Unified Filter Functions (Category + Brand) ─────────────────────────────

def _fetch_orders_with_filter(
    start_date: str,
    end_date: str,
    category_id: Optional[int] = None,
    brand: Optional[str] = None
) -> Tuple[Dict[int, int], Dict[int, float], Dict[str, int], Dict[str, float]]:
    """
    Fetch orders and filter by category and/or brand.

    Returns:
        Tuple of (source_orders, source_revenue, products, daily_revenue)
    """
    from web.services.category_service import (
        get_category_with_children,
        warm_product_cache,
        _product_category_cache,
        _products_loaded,
        _get_session
    )
    from web.services.brand_service import get_product_brand_cache

    # Pre-load caches
    if not _products_loaded:
        warm_product_cache()

    product_brand_cache = get_product_brand_cache()

    # Get all category IDs to match (including children)
    valid_category_ids = None
    if category_id:
        valid_category_ids = set(get_category_with_children(category_id))

    # Parse dates using utility
    start, end, local_start, local_end, utc_start, utc_end_buffer, tz = _parse_date_range(
        start_date, end_date
    )

    session = _get_session()
    return_status_ids = set(RETURN_STATUS_IDS)

    # Aggregation dicts
    source_orders: Dict[int, int] = {1: 0, 2: 0, 4: 0}
    source_revenue: Dict[int, float] = {1: 0.0, 2: 0.0, 4: 0.0}
    products: Dict[str, int] = {}
    daily_revenue = _init_daily_dict(start, end, 0.0)

    page = 1
    while True:
        params = {
            "include": "products.offer,manager",
            "limit": 50,
            "page": page,
            "filter[created_between]": f"{utc_start.strftime('%Y-%m-%d %H:%M:%S')}, {utc_end_buffer.strftime('%Y-%m-%d %H:%M:%S')}",
        }

        try:
            resp = session.get(f"{KEYCRM_BASE_URL}/order", params=params, timeout=30)
            if resp.status_code != 200:
                logger.error(f"API returned status {resp.status_code} fetching orders")
                break

            data = resp.json()
            batch = data.get("data", [])
            if not batch:
                break

            for order in batch:
                # Use utility for order validation
                is_valid, ordered_at = _is_valid_order(
                    order, utc_start, local_end, return_status_ids
                )
                if not is_valid:
                    continue

                source_id = order.get("source_id")

                # Check if any product matches the filters
                order_has_matching_product = False
                order_matching_revenue = 0.0

                for product in order.get("products", []):
                    offer = product.get("offer", {})
                    product_id = offer.get("product_id") if offer else None

                    if product_id:
                        # Check category filter
                        category_match = True
                        if valid_category_ids:
                            prod_category_id = _product_category_cache.get(product_id)
                            category_match = prod_category_id and prod_category_id in valid_category_ids

                        # Check brand filter
                        brand_match = True
                        if brand:
                            prod_brand = product_brand_cache.get(product_id)
                            brand_match = prod_brand and prod_brand == brand

                        # Both filters must match
                        if category_match and brand_match:
                            order_has_matching_product = True
                            product_revenue = float(product.get("price_sold", 0)) * int(product.get("quantity", 1))
                            order_matching_revenue += product_revenue

                            # Add to products dict
                            product_name = product.get("name", "Unknown")
                            qty = int(product.get("quantity", 1))
                            products[product_name] = products.get(product_name, 0) + qty

                if order_has_matching_product and source_id in source_orders:
                    source_orders[source_id] += 1
                    source_revenue[source_id] += order_matching_revenue

                    # Add to daily revenue
                    local_ordered = ordered_at.astimezone(tz)
                    date_key = local_ordered.strftime("%Y-%m-%d")
                    if date_key in daily_revenue:
                        daily_revenue[date_key] += order_matching_revenue

            if len(batch) < 50:
                break
            page += 1

        except Exception as e:
            logger.error(f"Error fetching orders with filter: {e}")
            break

    return source_orders, source_revenue, products, daily_revenue


def _get_sales_by_source_with_filter(
    start_date: str,
    end_date: str,
    category_id: Optional[int] = None,
    brand: Optional[str] = None
) -> Dict[str, Any]:
    """Get sales by source filtered by category and/or brand."""
    cache_key = f"sales_by_source_filter:{start_date}:{end_date}:{category_id}:{brand}"
    cached = _get_cached(cache_key)
    if cached is not None:
        return cached

    source_orders, source_revenue, _, _ = _fetch_orders_with_filter(
        start_date, end_date, category_id, brand
    )

    # Build data for each source
    source_data = []
    for source_id in [1, 2, 4]:
        source_data.append({
            'name': SOURCE_MAPPING.get(source_id, f"Source {source_id}"),
            'orders': source_orders.get(source_id, 0),
            'revenue': round(source_revenue.get(source_id, 0), 2),
            'color': SOURCE_COLORS.get(source_id, "#999999")
        })

    # Sort by orders descending
    source_data.sort(key=lambda x: x['orders'], reverse=True)

    result = {
        "labels": [s['name'] for s in source_data],
        "orders": [s['orders'] for s in source_data],
        "revenue": [s['revenue'] for s in source_data],
        "backgroundColor": [s['color'] for s in source_data]
    }
    _set_cached(cache_key, result)
    return result


def _get_top_products_with_filter(
    start_date: str,
    end_date: str,
    source_id: Optional[int],
    limit: int,
    category_id: Optional[int] = None,
    brand: Optional[str] = None
) -> Dict[str, Any]:
    """Get top products filtered by category and/or brand."""
    cache_key = f"top_products_filter:{start_date}:{end_date}:{source_id}:{limit}:{category_id}:{brand}"
    cached = _get_cached(cache_key)
    if cached is not None:
        return cached

    _, _, products, _ = _fetch_orders_with_filter(
        start_date, end_date, category_id, brand
    )

    # Sort and limit (descending - highest at top)
    sorted_products = sorted(products.items(), key=lambda x: x[1], reverse=True)[:limit]

    labels = [_wrap_label(p[0]) for p in sorted_products]
    data = [p[1] for p in sorted_products]

    total = sum(data) if data else 1
    percentages = [round(d / total * 100, 1) for d in data]

    result = {
        "labels": labels,
        "data": data,
        "percentages": percentages,
        "backgroundColor": "#2563EB"
    }
    _set_cached(cache_key, result)
    return result


def _get_summary_stats_with_filter(
    start_date: str,
    end_date: str,
    category_id: Optional[int] = None,
    brand: Optional[str] = None
) -> Dict[str, Any]:
    """Get summary stats filtered by category and/or brand."""
    cache_key = f"summary_filter:{start_date}:{end_date}:{category_id}:{brand}"
    cached = _get_cached(cache_key)
    if cached is not None:
        return cached

    source_orders, source_revenue, _, _ = _fetch_orders_with_filter(
        start_date, end_date, category_id, brand
    )

    total_orders = sum(source_orders.values())
    total_revenue = sum(source_revenue.values())
    avg_check = total_revenue / total_orders if total_orders > 0 else 0

    result = {
        "totalOrders": total_orders,
        "totalRevenue": round(total_revenue, 2),
        "avgCheck": round(avg_check, 2),
        "totalReturns": 0,  # Returns not filtered for now
        "returnsRevenue": 0,
        "startDate": start_date,
        "endDate": end_date
    }
    _set_cached(cache_key, result)
    return result


# ─── Async Functions ─────────────────────────────────────────────────────────

# ═══════════════════════════════════════════════════════════════════════════
# CUSTOMER INSIGHTS
# ═══════════════════════════════════════════════════════════════════════════

def get_customer_insights(start_date: str, end_date: str, brand: Optional[str] = None) -> Dict[str, Any]:
    """
    Get customer insights: new vs returning, AOV trend, repeat rate.

    Returns:
        Dict with customer analytics data
    """
    cache_key = f"customer_insights:{start_date}:{end_date}:{brand}"
    cached = _get_cached(cache_key)
    if cached is not None:
        return cached

    from web.services.brand_service import get_product_brand_cache
    from web.services.category_service import _get_session

    product_brand_cache = get_product_brand_cache() if brand else {}

    # Parse dates using utility
    start, end, local_start, local_end, utc_start, utc_end_buffer, tz = _parse_date_range(
        start_date, end_date
    )

    session = _get_session()
    return_status_ids = set(RETURN_STATUS_IDS)

    # Track customers and daily AOV
    # Store buyer_id -> (buyer_created_at, order_count)
    customer_data: Dict[int, Dict[str, Any]] = {}
    daily_aov = _init_daily_dict(start, end, {"revenue": 0.0, "orders": 0})

    page = 1
    while True:
        params = {
            "include": "buyer,manager,products.offer" if brand else "buyer,manager",
            "limit": 50,
            "page": page,
            "filter[created_between]": f"{utc_start.strftime('%Y-%m-%d %H:%M:%S')}, {utc_end_buffer.strftime('%Y-%m-%d %H:%M:%S')}",
        }

        try:
            resp = session.get(f"{KEYCRM_BASE_URL}/order", params=params, timeout=30)
            if resp.status_code != 200:
                logger.error(f"API returned status {resp.status_code} fetching customer insights")
                break

            data = resp.json()
            batch = data.get("data", [])
            if not batch:
                break

            for order in batch:
                # Use utility for order validation
                is_valid, ordered_at = _is_valid_order(
                    order, utc_start, local_end, return_status_ids
                )
                if not is_valid:
                    continue

                # If brand filter, check if order has matching product
                if brand:
                    has_brand_product = False
                    brand_revenue = 0.0
                    for product in order.get("products", []):
                        offer = product.get("offer", {})
                        product_id = offer.get("product_id") if offer else None
                        if product_id:
                            prod_brand = product_brand_cache.get(product_id)
                            if prod_brand == brand:
                                has_brand_product = True
                                brand_revenue += float(product.get("price_sold", 0)) * int(product.get("quantity", 1))
                    if not has_brand_product:
                        continue
                    order_revenue = brand_revenue
                else:
                    order_revenue = float(order.get("grand_total", 0))

                # Track customer with their created_at for new vs returning analysis
                buyer = order.get("buyer", {})
                buyer_id = buyer.get("id") if buyer else None
                if buyer_id:
                    if buyer_id not in customer_data:
                        # Parse buyer's created_at to determine if they're new or returning
                        buyer_created_str = buyer.get("created_at")
                        buyer_created_at = None
                        if buyer_created_str:
                            try:
                                buyer_created_at = datetime.fromisoformat(
                                    buyer_created_str.replace("Z", "+00:00")
                                )
                            except (ValueError, AttributeError):
                                pass
                        customer_data[buyer_id] = {
                            "created_at": buyer_created_at,
                            "order_count": 0
                        }
                    customer_data[buyer_id]["order_count"] += 1

                # Track daily AOV
                local_ordered = ordered_at.astimezone(tz)
                date_key = local_ordered.strftime("%Y-%m-%d")
                if date_key in daily_aov:
                    daily_aov[date_key]["revenue"] += order_revenue
                    daily_aov[date_key]["orders"] += 1

            if len(batch) < 50:
                break
            page += 1

        except Exception as e:
            logger.error(f"Error fetching customer insights: {e}")
            break

    # Calculate metrics using buyer's created_at for true new vs returning analysis
    # New customer: registered during this period (created_at >= period start)
    # Returning customer: registered before this period (created_at < period start)
    new_customers = 0
    returning_customers = 0
    repeat_purchasers = 0  # Customers who made multiple orders in this period

    for data in customer_data.values():
        buyer_created_at = data.get("created_at")
        order_count = data.get("order_count", 0)

        if buyer_created_at:
            # Compare buyer creation time with period start (both in UTC)
            if buyer_created_at >= utc_start:
                new_customers += 1
            else:
                returning_customers += 1
        else:
            # If no created_at, count as returning (existing customer)
            returning_customers += 1

        # Track repeat purchasers (made >1 order in this period)
        if order_count > 1:
            repeat_purchasers += 1

    total_customers = len(customer_data)

    # Repeat purchase rate: % of customers who made multiple orders in this period
    repeat_rate = (repeat_purchasers / total_customers * 100) if total_customers > 0 else 0

    # Build AOV trend data
    aov_labels = []
    aov_data = []
    current = start
    while current <= end:
        date_key = current.strftime("%Y-%m-%d")
        aov_labels.append(current.strftime("%d.%m"))
        day_data = daily_aov.get(date_key, {"revenue": 0, "orders": 0})
        aov = day_data["revenue"] / day_data["orders"] if day_data["orders"] > 0 else 0
        aov_data.append(round(aov, 2))
        current += timedelta(days=1)

    # Calculate overall AOV
    total_revenue = sum(d["revenue"] for d in daily_aov.values())
    total_orders = sum(d["orders"] for d in daily_aov.values())
    overall_aov = total_revenue / total_orders if total_orders > 0 else 0

    result = {
        "newVsReturning": {
            "labels": ["New Customers", "Returning Customers"],
            "data": [new_customers, returning_customers],
            "backgroundColor": ["#2563EB", "#16A34A"]
        },
        "aovTrend": {
            "labels": aov_labels,
            "datasets": [{
                "label": "AOV (UAH)",
                "data": aov_data,
                "borderColor": "#F59E0B",
                "backgroundColor": "rgba(245, 158, 11, 0.1)",
                "fill": True,
                "tension": 0.3
            }]
        },
        "metrics": {
            "totalCustomers": total_customers,
            "newCustomers": new_customers,
            "returningCustomers": returning_customers,
            "repeatPurchasers": repeat_purchasers,
            "repeatRate": round(repeat_rate, 1),
            "averageOrderValue": round(overall_aov, 2)
        }
    }

    _set_cached(cache_key, result)
    return result


# ═══════════════════════════════════════════════════════════════════════════
# PRODUCT PERFORMANCE
# ═══════════════════════════════════════════════════════════════════════════

def get_product_performance(start_date: str, end_date: str, brand: Optional[str] = None) -> Dict[str, Any]:
    """
    Get product performance: top by revenue, category breakdown, brands.

    Returns:
        Dict with product analytics data
    """
    cache_key = f"product_performance:{start_date}:{end_date}:{brand}"
    cached = _get_cached(cache_key)
    if cached is not None:
        return cached

    from web.services.category_service import (
        get_categories,
        _product_category_cache,
        warm_product_cache,
        _products_loaded,
        _get_session
    )
    from web.services.brand_service import get_product_brand_cache

    # Pre-load product categories
    if not _products_loaded:
        warm_product_cache()

    product_brand_cache = get_product_brand_cache() if brand else {}

    # Parse dates using utility
    start, end, local_start, local_end, utc_start, utc_end_buffer, tz = _parse_date_range(
        start_date, end_date
    )

    session = _get_session()
    return_status_ids = set(RETURN_STATUS_IDS)

    # Track products by revenue
    product_revenue: Dict[str, float] = {}
    product_quantity: Dict[str, int] = {}
    category_revenue: Dict[str, float] = {}
    category_quantity: Dict[str, int] = {}

    categories = get_categories()

    page = 1
    while True:
        params = {
            "include": "products.offer,manager",
            "limit": 50,
            "page": page,
            "filter[created_between]": f"{utc_start.strftime('%Y-%m-%d %H:%M:%S')}, {utc_end_buffer.strftime('%Y-%m-%d %H:%M:%S')}",
        }

        try:
            resp = session.get(f"{KEYCRM_BASE_URL}/order", params=params, timeout=30)
            if resp.status_code != 200:
                logger.error(f"API returned status {resp.status_code} fetching product performance")
                break

            data = resp.json()
            batch = data.get("data", [])
            if not batch:
                break

            for order in batch:
                # Use utility for order validation
                is_valid, ordered_at = _is_valid_order(
                    order, utc_start, local_end, return_status_ids
                )
                if not is_valid:
                    continue

                # Process products
                for product in order.get("products", []):
                    offer = product.get("offer", {})
                    product_id = offer.get("product_id") if offer else None

                    # Check brand filter
                    if brand and product_id:
                        prod_brand = product_brand_cache.get(product_id)
                        if prod_brand != brand:
                            continue

                    product_name = product.get("name", "Unknown")
                    qty = int(product.get("quantity", 1))
                    revenue = float(product.get("price_sold", 0)) * qty

                    # Track product revenue and quantity
                    product_revenue[product_name] = product_revenue.get(product_name, 0) + revenue
                    product_quantity[product_name] = product_quantity.get(product_name, 0) + qty

                    # Get category from product
                    if product_id:
                        cat_id = _product_category_cache.get(product_id)
                        if cat_id and cat_id in categories:
                            cat = categories[cat_id]
                            while cat.get('parent_id') and cat['parent_id'] in categories:
                                cat = categories[cat['parent_id']]
                            cat_name = cat.get('name', 'Other')
                        else:
                            cat_name = 'Other'
                    else:
                        cat_name = 'Other'

                    category_revenue[cat_name] = category_revenue.get(cat_name, 0) + revenue
                    category_quantity[cat_name] = category_quantity.get(cat_name, 0) + qty

            if len(batch) < 50:
                break
            page += 1

        except Exception as e:
            logger.error(f"Error fetching product performance: {e}")
            break

    # Build top products by revenue (top 10)
    sorted_by_revenue = sorted(product_revenue.items(), key=lambda x: x[1], reverse=True)[:10]

    top_by_revenue = {
        "labels": [_wrap_label(p[0]) for p in sorted_by_revenue],
        "data": [round(p[1], 2) for p in sorted_by_revenue],
        "quantities": [product_quantity.get(p[0], 0) for p in sorted_by_revenue],
        "backgroundColor": "#16A34A"
    }

    # Build category breakdown
    sorted_categories = sorted(category_revenue.items(), key=lambda x: x[1], reverse=True)

    # Color palette for categories
    category_colors = ["#7C3AED", "#2563EB", "#16A34A", "#F59E0B", "#eb4200", "#EC4899", "#8B5CF6", "#06B6D4"]

    category_breakdown = {
        "labels": [c[0] for c in sorted_categories],
        "revenue": [round(c[1], 2) for c in sorted_categories],
        "quantity": [category_quantity.get(c[0], 0) for c in sorted_categories],
        "backgroundColor": category_colors[:len(sorted_categories)]
    }

    # Calculate totals
    total_revenue = sum(product_revenue.values())
    total_quantity = sum(product_quantity.values())

    result = {
        "topByRevenue": top_by_revenue,
        "categoryBreakdown": category_breakdown,
        "metrics": {
            "totalProducts": len(product_revenue),
            "totalRevenue": round(total_revenue, 2),
            "totalQuantity": total_quantity,
            "avgProductRevenue": round(total_revenue / len(product_revenue), 2) if product_revenue else 0
        }
    }

    _set_cached(cache_key, result)
    return result


# ═══════════════════════════════════════════════════════════════════════════
# BRAND ANALYTICS
# ═══════════════════════════════════════════════════════════════════════════

def get_brand_analytics(start_date: str, end_date: str) -> Dict[str, Any]:
    """
    Get brand analytics: top brands by revenue, orders, quantity.

    Returns:
        Dict with brand analytics data
    """
    cache_key = f"brand_analytics:{start_date}:{end_date}"
    cached = _get_cached(cache_key)
    if cached is not None:
        return cached

    from web.services.brand_service import get_product_brand_cache
    from web.services.category_service import _get_session

    product_brand_cache = get_product_brand_cache()

    # Parse dates using utility
    start, end, local_start, local_end, utc_start, utc_end_buffer, tz = _parse_date_range(
        start_date, end_date
    )

    session = _get_session()
    return_status_ids = set(RETURN_STATUS_IDS)

    # Track brand metrics
    brand_revenue: Dict[str, float] = {}
    brand_quantity: Dict[str, int] = {}
    brand_orders: Dict[str, set] = {}  # Track unique orders per brand

    page = 1
    while True:
        params = {
            "include": "products.offer,manager",
            "limit": 50,
            "page": page,
            "filter[created_between]": f"{utc_start.strftime('%Y-%m-%d %H:%M:%S')}, {utc_end_buffer.strftime('%Y-%m-%d %H:%M:%S')}",
        }

        try:
            resp = session.get(f"{KEYCRM_BASE_URL}/order", params=params, timeout=30)
            if resp.status_code != 200:
                logger.error(f"API returned status {resp.status_code} fetching brand analytics")
                break

            data = resp.json()
            batch = data.get("data", [])
            if not batch:
                break

            for order in batch:
                # Use utility for order validation
                is_valid, ordered_at = _is_valid_order(
                    order, utc_start, local_end, return_status_ids
                )
                if not is_valid:
                    continue

                order_id = order.get("id")

                # Process products
                for product in order.get("products", []):
                    offer = product.get("offer", {})
                    product_id = offer.get("product_id") if offer else None

                    if not product_id:
                        continue

                    brand = product_brand_cache.get(product_id)
                    if not brand:
                        brand = "Unknown"

                    qty = int(product.get("quantity", 1))
                    revenue = float(product.get("price_sold", 0)) * qty

                    # Track brand metrics
                    brand_revenue[brand] = brand_revenue.get(brand, 0) + revenue
                    brand_quantity[brand] = brand_quantity.get(brand, 0) + qty

                    # Track unique orders for this brand
                    if brand not in brand_orders:
                        brand_orders[brand] = set()
                    brand_orders[brand].add(order_id)

            if len(batch) < 50:
                break
            page += 1

        except Exception as e:
            logger.error(f"Error fetching brand analytics: {e}")
            break

    # Convert order sets to counts
    brand_order_counts = {brand: len(orders) for brand, orders in brand_orders.items()}

    # Build top brands by revenue (top 10)
    sorted_by_revenue = sorted(brand_revenue.items(), key=lambda x: x[1], reverse=True)[:10]

    # Color palette for brands
    brand_colors = ["#7C3AED", "#2563EB", "#16A34A", "#F59E0B", "#eb4200", "#EC4899", "#8B5CF6", "#06B6D4", "#14B8A6", "#EF4444"]

    top_brands_revenue = {
        "labels": [b[0] for b in sorted_by_revenue],
        "data": [round(b[1], 2) for b in sorted_by_revenue],
        "quantities": [brand_quantity.get(b[0], 0) for b in sorted_by_revenue],
        "orders": [brand_order_counts.get(b[0], 0) for b in sorted_by_revenue],
        "backgroundColor": brand_colors[:len(sorted_by_revenue)]
    }

    # Build top brands by quantity (top 10)
    sorted_by_quantity = sorted(brand_quantity.items(), key=lambda x: x[1], reverse=True)[:10]

    top_brands_quantity = {
        "labels": [b[0] for b in sorted_by_quantity],
        "data": [b[1] for b in sorted_by_quantity],
        "revenue": [round(brand_revenue.get(b[0], 0), 2) for b in sorted_by_quantity],
        "backgroundColor": brand_colors[:len(sorted_by_quantity)]
    }

    # Calculate totals
    total_revenue = sum(brand_revenue.values())
    total_quantity = sum(brand_quantity.values())
    total_orders = sum(brand_order_counts.values())
    unique_brands = len([b for b in brand_revenue.keys() if b != "Unknown"])

    # Top brand info
    top_brand = sorted_by_revenue[0][0] if sorted_by_revenue else "N/A"
    top_brand_revenue = sorted_by_revenue[0][1] if sorted_by_revenue else 0
    top_brand_share = (top_brand_revenue / total_revenue * 100) if total_revenue > 0 else 0

    result = {
        "topByRevenue": top_brands_revenue,
        "topByQuantity": top_brands_quantity,
        "metrics": {
            "totalBrands": unique_brands,
            "topBrand": top_brand,
            "topBrandShare": round(top_brand_share, 1),
            "totalRevenue": round(total_revenue, 2),
            "totalQuantity": total_quantity,
            "avgBrandRevenue": round(total_revenue / unique_brands, 2) if unique_brands > 0 else 0
        }
    }

    _set_cached(cache_key, result)
    return result


async def async_get_revenue_trend(
    start_date: str,
    end_date: str,
    category_id: Optional[int] = None,
    granularity: str = "daily",
    brand: Optional[str] = None
) -> Dict[str, Any]:
    """
    Async version of get_revenue_trend.
    Uses httpx for non-blocking HTTP requests with parallel pagination.
    """
    # If category or brand filter, use sync version with filter
    if category_id or brand:
        _, _, _, daily_revenue = _fetch_orders_with_filter(
            start_date, end_date, category_id, brand
        )
        start, end, _, _, _, _, _ = _parse_date_range(start_date, end_date)

        labels = []
        data = []
        current = start
        while current <= end:
            date_key = current.strftime("%Y-%m-%d")
            labels.append(current.strftime("%d.%m"))
            data.append(round(daily_revenue.get(date_key, 0), 2))
            current += timedelta(days=1)

        return {
            "labels": labels,
            "datasets": [{
                "label": "Revenue (UAH)",
                "data": data,
                "borderColor": "#16A34A",
                "backgroundColor": "rgba(22, 163, 74, 0.1)",
                "fill": True,
                "tension": 0.3
            }]
        }

    cache_key = f"revenue_trend:{start_date}:{end_date}:{granularity}"
    cached = _get_cached(cache_key)
    if cached is not None:
        return cached

    from web.services.async_client import AsyncKeyCRMClient

    # Parse dates using utility
    start, end, local_start, local_end, utc_start, utc_end_buffer, tz = _parse_date_range(
        start_date, end_date
    )
    daily_revenue = _init_daily_dict(start, end, 0.0)

    try:
        client = AsyncKeyCRMClient()

        params = {
            "include": "products,manager",
            "limit": 50,
            "filter[created_between]": f"{utc_start.strftime('%Y-%m-%d %H:%M:%S')}, {utc_end_buffer.strftime('%Y-%m-%d %H:%M:%S')}",
        }

        all_orders = await client.fetch_all_orders(params)
        return_status_ids = set(RETURN_STATUS_IDS)

        for order in all_orders:
            # Use utility for order validation
            is_valid, ordered_at = _is_valid_order(
                order, utc_start, local_end, return_status_ids
            )
            if not is_valid:
                continue

            # Get local date and add revenue
            local_ordered = ordered_at.astimezone(tz)
            date_key = local_ordered.strftime("%Y-%m-%d")
            if date_key in daily_revenue:
                daily_revenue[date_key] += float(order.get("grand_total", 0))

    except Exception as e:
        logger.error(f"Error in async_get_revenue_trend: {e}")

    # Build response
    labels = []
    data = []
    current = start
    while current <= end:
        date_key = current.strftime("%Y-%m-%d")
        labels.append(current.strftime("%d.%m"))
        data.append(round(daily_revenue.get(date_key, 0), 2))
        current += timedelta(days=1)

    result = {
        "labels": labels,
        "datasets": [{
            "label": "Revenue (UAH)",
            "data": data,
            "borderColor": "#16A34A",
            "backgroundColor": "rgba(22, 163, 74, 0.1)",
            "fill": True,
            "tension": 0.3
        }]
    }
    _set_cached(cache_key, result)
    return result
