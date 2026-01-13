"""
Dashboard service for transforming sales data into chart-friendly formats.
Fully async implementation using core.keycrm client.
"""
import asyncio
import logging
from datetime import date, datetime, timedelta
from typing import Dict, List, Optional, Any, Tuple
import time
from zoneinfo import ZoneInfo

from bot.config import (
    SOURCE_MAPPING,
    DEFAULT_TIMEZONE,
    RETURN_STATUS_IDS,
    TELEGRAM_MANAGER_IDS,
)
from core.keycrm import get_async_client
from core.models import Order, SourceId, OrderStatus

logger = logging.getLogger(__name__)


# ─── In-Memory Cache ─────────────────────────────────────────────────────────
CACHE_TTL_SECONDS = 300  # 5 minutes

_cache: Dict[str, Any] = {}
_cache_lock = asyncio.Lock()


async def _get_cached(key: str) -> Optional[Any]:
    """Get value from cache if not expired."""
    async with _cache_lock:
        if key in _cache:
            data, timestamp = _cache[key]
            if time.time() - timestamp < CACHE_TTL_SECONDS:
                return data
            del _cache[key]
    return None


async def _set_cached(key: str, value: Any) -> None:
    """Store value in cache with timestamp."""
    async with _cache_lock:
        _cache[key] = (value, time.time())


# Source colors for charts
SOURCE_COLORS = {
    1: "#7C3AED",  # Instagram - purple (Accent)
    2: "#2563EB",  # Telegram - blue (Primary)
    3: "#F59E0B",  # Opencart - orange (Warning)
    4: "#eb4200",  # Shopify - orange-red
}

SOURCE_COLORS_LIST = ["#7C3AED", "#2563EB", "#F59E0B", "#eb4200"]


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


def parse_period(period: Optional[str], start_date: Optional[str], end_date: Optional[str]) -> Tuple[str, str]:
    """
    Parse period shortcut or dates into (start_date, end_date) tuple.
    """
    today = datetime.now().date()

    if period:
        if period == "today":
            return (today.strftime("%Y-%m-%d"), today.strftime("%Y-%m-%d"))
        elif period == "yesterday":
            yesterday = today - timedelta(days=1)
            return (yesterday.strftime("%Y-%m-%d"), yesterday.strftime("%Y-%m-%d"))
        elif period == "week":
            start_of_week = today - timedelta(days=today.weekday())
            return (start_of_week.strftime("%Y-%m-%d"), today.strftime("%Y-%m-%d"))
        elif period == "last_week":
            start_of_this_week = today - timedelta(days=today.weekday())
            end_of_last_week = start_of_this_week - timedelta(days=1)
            start_of_last_week = end_of_last_week - timedelta(days=6)
            return (start_of_last_week.strftime("%Y-%m-%d"), end_of_last_week.strftime("%Y-%m-%d"))
        elif period == "month":
            start_of_month = today.replace(day=1)
            return (start_of_month.strftime("%Y-%m-%d"), today.strftime("%Y-%m-%d"))
        elif period == "last_month":
            first_of_this_month = today.replace(day=1)
            last_of_last_month = first_of_this_month - timedelta(days=1)
            first_of_last_month = last_of_last_month.replace(day=1)
            return (first_of_last_month.strftime("%Y-%m-%d"), last_of_last_month.strftime("%Y-%m-%d"))

    if start_date and end_date:
        return (start_date, end_date)

    return (today.strftime("%Y-%m-%d"), today.strftime("%Y-%m-%d"))


# ─── Date/Time Utilities ──────────────────────────────────────────────────────

def _parse_date_range(
    start_date: str,
    end_date: str,
    tz_name: str = DEFAULT_TIMEZONE
) -> Tuple[date, date, datetime, datetime, datetime, datetime, ZoneInfo]:
    """Parse date strings into local and UTC boundaries with timezone info."""
    start = datetime.strptime(start_date, "%Y-%m-%d").date()
    end = datetime.strptime(end_date, "%Y-%m-%d").date()

    tz = ZoneInfo(tz_name)
    local_start = datetime(start.year, start.month, start.day, 0, 0, 0, tzinfo=tz)
    local_end = datetime(end.year, end.month, end.day, 23, 59, 59, tzinfo=tz)
    utc_start = local_start.astimezone(ZoneInfo("UTC"))
    utc_end_with_buffer = local_end.astimezone(ZoneInfo("UTC")) + timedelta(hours=24)

    return start, end, local_start, local_end, utc_start, utc_end_with_buffer, tz


def _init_daily_dict(start, end, default_value: Any = 0.0) -> Dict[str, Any]:
    """Initialize a dictionary with date keys for the given range."""
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
    order_data: Dict[str, Any],
    utc_start: datetime,
    local_end: datetime,
) -> Tuple[bool, Optional[datetime]]:
    """Check if order is valid (within date range and not a return)."""
    order = Order.from_api(order_data)

    if not order.ordered_at:
        return False, None

    utc_end = local_end.astimezone(ZoneInfo("UTC"))
    if not order.is_within_period(utc_start, utc_end):
        return False, None

    if order.is_return:
        return False, None

    if order.source == SourceId.TELEGRAM:
        if not order.matches_manager(TELEGRAM_MANAGER_IDS):
            return False, None

    return True, order.ordered_at


# ─── Async Data Fetching ──────────────────────────────────────────────────────

async def _fetch_orders_async(
    start_date: str,
    end_date: str,
    include: str = "products,manager"
) -> List[Dict[str, Any]]:
    """Fetch all orders for a date range using async client."""
    start, end, local_start, local_end, utc_start, utc_end_buffer, tz = _parse_date_range(
        start_date, end_date
    )

    client = await get_async_client()
    params = {
        "include": include,
        "filter[created_between]": f"{utc_start.strftime('%Y-%m-%d %H:%M:%S')}, {utc_end_buffer.strftime('%Y-%m-%d %H:%M:%S')}",
    }

    all_orders = []
    try:
        async for batch in client.paginate("order", params=params, page_size=50):
            all_orders.extend(batch)
    except Exception as e:
        logger.error(f"Error fetching orders: {e}")

    return all_orders


async def get_revenue_trend(
    start_date: str,
    end_date: str,
    granularity: str = "daily",
    include_comparison: bool = True,
    category_id: Optional[int] = None,
    brand: Optional[str] = None,
    source_id: Optional[int] = None
) -> Dict[str, Any]:
    """Get revenue data over time for line chart."""
    cache_key = f"revenue_trend:{start_date}:{end_date}:{granularity}:{category_id}:{brand}:{source_id}"
    cached = await _get_cached(cache_key)
    if cached is not None:
        return cached

    start, end, local_start, local_end, utc_start, utc_end_buffer, tz = _parse_date_range(
        start_date, end_date
    )
    daily_revenue = _init_daily_dict(start, end, 0.0)

    # Fetch orders
    orders = await _fetch_orders_async(start_date, end_date, "products.offer,manager")

    # Import for filtering
    from web.services.category_service import get_category_with_children, get_product_category_cache, is_products_loaded
    from web.services.brand_service import get_product_brand_cache, is_brands_loaded

    # Get filter caches if needed
    valid_category_ids = None
    if category_id:
        valid_category_ids = set(await get_category_with_children(category_id))

    product_brand_cache = get_product_brand_cache() if brand else {}
    product_category_cache = get_product_category_cache()

    for order_data in orders:
        is_valid, ordered_at = _is_valid_order(order_data, utc_start, local_end)
        if not is_valid:
            continue

        order = Order.from_api(order_data)

        # Apply source filter
        if source_id and order.source_id != source_id:
            continue

        # Calculate revenue (with filters)
        order_revenue = 0.0
        if category_id or brand:
            for prod in order.products:
                prod_id = prod.product_id
                if prod_id:
                    category_match = True
                    if valid_category_ids:
                        prod_cat = product_category_cache.get(prod_id)
                        category_match = prod_cat and prod_cat in valid_category_ids

                    brand_match = True
                    if brand:
                        prod_brand = product_brand_cache.get(prod_id)
                        brand_match = prod_brand and prod_brand.lower() == brand.lower()

                    if category_match and brand_match:
                        order_revenue += prod.total
        else:
            order_revenue = order.grand_total

        if order_revenue > 0:
            local_ordered = ordered_at.astimezone(tz)
            date_key = local_ordered.strftime("%Y-%m-%d")
            if date_key in daily_revenue:
                daily_revenue[date_key] += order_revenue

    # Build chart data
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
            "label": "This Period",
            "data": data,
            "borderColor": "#16A34A",
            "backgroundColor": "rgba(22, 163, 74, 0.1)",
            "fill": True,
            "tension": 0.3,
            "borderWidth": 2
        }]
    }

    await _set_cached(cache_key, result)
    return result


async def get_sales_by_source(
    start_date: str,
    end_date: str,
    category_id: Optional[int] = None,
    brand: Optional[str] = None,
    source_id: Optional[int] = None
) -> Dict[str, Any]:
    """Get sales data aggregated by source for bar/pie chart."""
    cache_key = f"sales_by_source:{start_date}:{end_date}:{category_id}:{brand}:{source_id}"
    cached = await _get_cached(cache_key)
    if cached is not None:
        return cached

    start, end, local_start, local_end, utc_start, utc_end_buffer, tz = _parse_date_range(
        start_date, end_date
    )

    orders = await _fetch_orders_async(start_date, end_date, "products.offer,manager")

    from web.services.category_service import get_category_with_children, get_product_category_cache
    from web.services.brand_service import get_product_brand_cache

    valid_category_ids = None
    if category_id:
        valid_category_ids = set(await get_category_with_children(category_id))

    product_brand_cache = get_product_brand_cache() if brand else {}
    product_category_cache = get_product_category_cache()

    # Aggregate by source
    active_source_ids = [s.value for s in SourceId.active_sources()]
    source_orders: Dict[int, int] = {sid: 0 for sid in active_source_ids}
    source_revenue: Dict[int, float] = {sid: 0.0 for sid in active_source_ids}

    for order_data in orders:
        is_valid, ordered_at = _is_valid_order(order_data, utc_start, local_end)
        if not is_valid:
            continue

        order = Order.from_api(order_data)

        if source_id and order.source_id != source_id:
            continue

        # Check filters
        if category_id or brand:
            order_has_match = False
            order_revenue = 0.0
            for prod in order.products:
                prod_id = prod.product_id
                if prod_id:
                    category_match = True
                    if valid_category_ids:
                        prod_cat = product_category_cache.get(prod_id)
                        category_match = prod_cat and prod_cat in valid_category_ids

                    brand_match = True
                    if brand:
                        prod_brand = product_brand_cache.get(prod_id)
                        brand_match = prod_brand and prod_brand.lower() == brand.lower()

                    if category_match and brand_match:
                        order_has_match = True
                        order_revenue += prod.total

            if order_has_match and order.source_id in source_orders:
                source_orders[order.source_id] += 1
                source_revenue[order.source_id] += order_revenue
        else:
            if order.source_id in source_orders:
                source_orders[order.source_id] += 1
                source_revenue[order.source_id] += order.grand_total

    # Build response
    source_data = []
    for sid in [1, 2, 4]:  # Instagram, Telegram, Shopify
        source_data.append({
            'name': SOURCE_MAPPING.get(sid, f"Source {sid}"),
            'orders': source_orders.get(sid, 0),
            'revenue': round(source_revenue.get(sid, 0), 2),
            'color': SOURCE_COLORS.get(sid, "#999999")
        })

    source_data.sort(key=lambda x: x['orders'], reverse=True)

    result = {
        "labels": [s['name'] for s in source_data],
        "orders": [s['orders'] for s in source_data],
        "revenue": [s['revenue'] for s in source_data],
        "backgroundColor": [s['color'] for s in source_data]
    }

    await _set_cached(cache_key, result)
    return result


async def get_top_products(
    start_date: str,
    end_date: str,
    source_id: Optional[int] = None,
    limit: int = 10,
    category_id: Optional[int] = None,
    brand: Optional[str] = None
) -> Dict[str, Any]:
    """Get top products for horizontal bar chart."""
    cache_key = f"top_products:{start_date}:{end_date}:{source_id}:{limit}:{category_id}:{brand}"
    cached = await _get_cached(cache_key)
    if cached is not None:
        return cached

    start, end, local_start, local_end, utc_start, utc_end_buffer, tz = _parse_date_range(
        start_date, end_date
    )

    orders = await _fetch_orders_async(start_date, end_date, "products.offer,manager")

    from web.services.category_service import get_category_with_children, get_product_category_cache
    from web.services.brand_service import get_product_brand_cache

    valid_category_ids = None
    if category_id:
        valid_category_ids = set(await get_category_with_children(category_id))

    product_brand_cache = get_product_brand_cache() if brand else {}
    product_category_cache = get_product_category_cache()

    products: Dict[str, int] = {}

    for order_data in orders:
        is_valid, ordered_at = _is_valid_order(order_data, utc_start, local_end)
        if not is_valid:
            continue

        order = Order.from_api(order_data)

        if source_id and order.source_id != source_id:
            continue

        for prod in order.products:
            prod_id = prod.product_id

            # Check filters
            if category_id or brand:
                if prod_id:
                    category_match = True
                    if valid_category_ids:
                        prod_cat = product_category_cache.get(prod_id)
                        category_match = prod_cat and prod_cat in valid_category_ids

                    brand_match = True
                    if brand:
                        prod_brand = product_brand_cache.get(prod_id)
                        brand_match = prod_brand and prod_brand.lower() == brand.lower()

                    if category_match and brand_match:
                        products[prod.name] = products.get(prod.name, 0) + prod.quantity
            else:
                products[prod.name] = products.get(prod.name, 0) + prod.quantity

    # Sort and limit
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

    await _set_cached(cache_key, result)
    return result


async def get_summary_stats(
    start_date: str,
    end_date: str,
    category_id: Optional[int] = None,
    brand: Optional[str] = None,
    source_id: Optional[int] = None
) -> Dict[str, Any]:
    """Get summary statistics for dashboard cards."""
    cache_key = f"summary:{start_date}:{end_date}:{category_id}:{brand}:{source_id}"
    cached = await _get_cached(cache_key)
    if cached is not None:
        return cached

    start, end, local_start, local_end, utc_start, utc_end_buffer, tz = _parse_date_range(
        start_date, end_date
    )

    orders = await _fetch_orders_async(start_date, end_date, "products.offer,manager")

    from web.services.category_service import get_category_with_children, get_product_category_cache
    from web.services.brand_service import get_product_brand_cache

    valid_category_ids = None
    if category_id:
        valid_category_ids = set(await get_category_with_children(category_id))

    product_brand_cache = get_product_brand_cache() if brand else {}
    product_category_cache = get_product_category_cache()

    total_orders = 0
    total_revenue = 0.0
    total_returns = 0
    returns_revenue = 0.0

    for order_data in orders:
        order = Order.from_api(order_data)

        if not order.ordered_at:
            continue

        utc_end = local_end.astimezone(ZoneInfo("UTC"))
        if not order.is_within_period(utc_start, utc_end):
            continue

        if source_id and order.source_id != source_id:
            continue

        # Handle returns separately
        if order.is_return:
            total_returns += 1
            returns_revenue += order.grand_total
            continue

        # Filter Telegram orders
        if order.source == SourceId.TELEGRAM:
            if not order.matches_manager(TELEGRAM_MANAGER_IDS):
                continue

        # Apply category/brand filters
        if category_id or brand:
            order_has_match = False
            order_revenue = 0.0
            for prod in order.products:
                prod_id = prod.product_id
                if prod_id:
                    category_match = True
                    if valid_category_ids:
                        prod_cat = product_category_cache.get(prod_id)
                        category_match = prod_cat and prod_cat in valid_category_ids

                    brand_match = True
                    if brand:
                        prod_brand = product_brand_cache.get(prod_id)
                        brand_match = prod_brand and prod_brand.lower() == brand.lower()

                    if category_match and brand_match:
                        order_has_match = True
                        order_revenue += prod.total

            if order_has_match:
                total_orders += 1
                total_revenue += order_revenue
        else:
            total_orders += 1
            total_revenue += order.grand_total

    avg_check = total_revenue / total_orders if total_orders > 0 else 0

    result = {
        "totalOrders": total_orders,
        "totalRevenue": round(total_revenue, 2),
        "avgCheck": round(avg_check, 2),
        "totalReturns": total_returns,
        "returnsRevenue": round(returns_revenue, 2),
        "startDate": start_date,
        "endDate": end_date
    }

    await _set_cached(cache_key, result)
    return result


async def get_customer_insights(
    start_date: str,
    end_date: str,
    brand: Optional[str] = None,
    source_id: Optional[int] = None
) -> Dict[str, Any]:
    """Get customer insights: new vs returning, AOV trend, repeat rate."""
    cache_key = f"customer_insights:{start_date}:{end_date}:{brand}:{source_id}"
    cached = await _get_cached(cache_key)
    if cached is not None:
        return cached

    start, end, local_start, local_end, utc_start, utc_end_buffer, tz = _parse_date_range(
        start_date, end_date
    )

    include = "buyer,manager,products.offer" if brand else "buyer,manager"
    orders = await _fetch_orders_async(start_date, end_date, include)

    from web.services.brand_service import get_product_brand_cache
    product_brand_cache = get_product_brand_cache() if brand else {}

    customer_data: Dict[int, Dict[str, Any]] = {}
    daily_aov = _init_daily_dict(start, end, {"revenue": 0.0, "orders": 0})
    total_orders_count = 0
    orders_from_returning = 0

    for order_data in orders:
        is_valid, ordered_at = _is_valid_order(order_data, utc_start, local_end)
        if not is_valid:
            continue

        order = Order.from_api(order_data)

        if source_id and order.source_id != source_id:
            continue

        # Brand filter
        if brand:
            has_brand_product = False
            brand_revenue = 0.0
            for prod in order.products:
                prod_id = prod.product_id
                if prod_id:
                    prod_brand = product_brand_cache.get(prod_id)
                    if prod_brand and prod_brand.lower() == brand.lower():
                        has_brand_product = True
                        brand_revenue += prod.total
            if not has_brand_product:
                continue
            order_revenue = brand_revenue
        else:
            order_revenue = order.grand_total

        buyer = order.buyer
        buyer_id = buyer.id if buyer else None

        total_orders_count += 1
        if buyer and buyer.is_returning(utc_start):
            orders_from_returning += 1

        if buyer_id:
            if buyer_id not in customer_data:
                customer_data[buyer_id] = {
                    "created_at": buyer.created_at if buyer else None,
                    "order_count": 0
                }
            customer_data[buyer_id]["order_count"] += 1

        local_ordered = ordered_at.astimezone(tz)
        date_key = local_ordered.strftime("%Y-%m-%d")
        if date_key in daily_aov:
            daily_aov[date_key]["revenue"] += order_revenue
            daily_aov[date_key]["orders"] += 1

    # Calculate metrics
    new_customers = 0
    returning_customers = 0

    for cust_data in customer_data.values():
        buyer_created_at = cust_data.get("created_at")
        if buyer_created_at and buyer_created_at >= utc_start:
            new_customers += 1
        else:
            returning_customers += 1

    repeat_rate = (orders_from_returning / total_orders_count * 100) if total_orders_count > 0 else 0

    # Build AOV trend
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
            "totalCustomers": len(customer_data),
            "newCustomers": new_customers,
            "returningCustomers": returning_customers,
            "totalOrders": total_orders_count,
            "ordersFromReturning": orders_from_returning,
            "repeatRate": round(repeat_rate, 1),
            "averageOrderValue": round(overall_aov, 2)
        }
    }

    await _set_cached(cache_key, result)
    return result


async def get_product_performance(
    start_date: str,
    end_date: str,
    brand: Optional[str] = None,
    source_id: Optional[int] = None
) -> Dict[str, Any]:
    """Get product performance: top by revenue, category breakdown."""
    cache_key = f"product_performance:{start_date}:{end_date}:{brand}:{source_id}"
    cached = await _get_cached(cache_key)
    if cached is not None:
        return cached

    start, end, local_start, local_end, utc_start, utc_end_buffer, tz = _parse_date_range(
        start_date, end_date
    )

    orders = await _fetch_orders_async(start_date, end_date, "products.offer,manager")

    from web.services.category_service import get_categories, get_product_category_cache
    from web.services.brand_service import get_product_brand_cache

    product_brand_cache = get_product_brand_cache() if brand else {}
    product_category_cache = get_product_category_cache()
    categories = await get_categories()

    product_revenue: Dict[str, float] = {}
    product_quantity: Dict[str, int] = {}
    category_revenue: Dict[str, float] = {}
    category_quantity: Dict[str, int] = {}

    for order_data in orders:
        is_valid, ordered_at = _is_valid_order(order_data, utc_start, local_end)
        if not is_valid:
            continue

        order = Order.from_api(order_data)

        if source_id and order.source_id != source_id:
            continue

        for prod in order.products:
            prod_id = prod.product_id

            # Brand filter
            if brand and prod_id:
                prod_brand = product_brand_cache.get(prod_id)
                if not prod_brand or prod_brand.lower() != brand.lower():
                    continue

            qty = prod.quantity
            revenue = prod.total

            product_revenue[prod.name] = product_revenue.get(prod.name, 0) + revenue
            product_quantity[prod.name] = product_quantity.get(prod.name, 0) + qty

            # Get category
            if prod_id:
                cat_id = product_category_cache.get(prod_id)
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

    # Build response
    sorted_by_revenue = sorted(product_revenue.items(), key=lambda x: x[1], reverse=True)[:10]

    top_by_revenue = {
        "labels": [_wrap_label(p[0]) for p in sorted_by_revenue],
        "data": [round(p[1], 2) for p in sorted_by_revenue],
        "quantities": [product_quantity.get(p[0], 0) for p in sorted_by_revenue],
        "backgroundColor": "#16A34A"
    }

    sorted_categories = sorted(category_revenue.items(), key=lambda x: x[1], reverse=True)
    category_colors = ["#7C3AED", "#2563EB", "#16A34A", "#F59E0B", "#eb4200", "#EC4899", "#8B5CF6", "#06B6D4"]

    category_breakdown = {
        "labels": [c[0] for c in sorted_categories],
        "revenue": [round(c[1], 2) for c in sorted_categories],
        "quantity": [category_quantity.get(c[0], 0) for c in sorted_categories],
        "backgroundColor": category_colors[:len(sorted_categories)]
    }

    result = {
        "topByRevenue": top_by_revenue,
        "categoryBreakdown": category_breakdown,
        "metrics": {
            "totalProducts": len(product_revenue),
            "totalRevenue": round(sum(product_revenue.values()), 2),
            "totalQuantity": sum(product_quantity.values()),
            "avgProductRevenue": round(sum(product_revenue.values()) / len(product_revenue), 2) if product_revenue else 0
        }
    }

    await _set_cached(cache_key, result)
    return result


async def get_brand_analytics(start_date: str, end_date: str) -> Dict[str, Any]:
    """Get brand analytics: top brands by revenue and quantity."""
    cache_key = f"brand_analytics:{start_date}:{end_date}"
    cached = await _get_cached(cache_key)
    if cached is not None:
        return cached

    start, end, local_start, local_end, utc_start, utc_end_buffer, tz = _parse_date_range(
        start_date, end_date
    )

    orders = await _fetch_orders_async(start_date, end_date, "products.offer,manager")

    from web.services.brand_service import get_product_brand_cache
    product_brand_cache = get_product_brand_cache()

    brand_revenue: Dict[str, float] = {}
    brand_quantity: Dict[str, int] = {}
    brand_orders: Dict[str, set] = {}

    for order_data in orders:
        is_valid, ordered_at = _is_valid_order(order_data, utc_start, local_end)
        if not is_valid:
            continue

        order = Order.from_api(order_data)
        order_id = order.id

        for prod in order.products:
            prod_id = prod.product_id
            if not prod_id:
                continue

            brand_name = product_brand_cache.get(prod_id, "Unknown")
            qty = prod.quantity
            revenue = prod.total

            brand_revenue[brand_name] = brand_revenue.get(brand_name, 0) + revenue
            brand_quantity[brand_name] = brand_quantity.get(brand_name, 0) + qty

            if brand_name not in brand_orders:
                brand_orders[brand_name] = set()
            brand_orders[brand_name].add(order_id)

    # Build response
    brand_order_counts = {b: len(o) for b, o in brand_orders.items()}

    sorted_by_revenue = sorted(brand_revenue.items(), key=lambda x: x[1], reverse=True)[:10]
    brand_colors = ["#7C3AED", "#2563EB", "#16A34A", "#F59E0B", "#eb4200", "#EC4899", "#8B5CF6", "#06B6D4", "#14B8A6", "#EF4444"]

    top_brands_revenue = {
        "labels": [b[0] for b in sorted_by_revenue],
        "data": [round(b[1], 2) for b in sorted_by_revenue],
        "quantities": [brand_quantity.get(b[0], 0) for b in sorted_by_revenue],
        "orders": [brand_order_counts.get(b[0], 0) for b in sorted_by_revenue],
        "backgroundColor": brand_colors[:len(sorted_by_revenue)]
    }

    sorted_by_quantity = sorted(brand_quantity.items(), key=lambda x: x[1], reverse=True)[:10]

    top_brands_quantity = {
        "labels": [b[0] for b in sorted_by_quantity],
        "data": [b[1] for b in sorted_by_quantity],
        "revenue": [round(brand_revenue.get(b[0], 0), 2) for b in sorted_by_quantity],
        "backgroundColor": brand_colors[:len(sorted_by_quantity)]
    }

    total_revenue = sum(brand_revenue.values())
    unique_brands = len([b for b in brand_revenue.keys() if b != "Unknown"])

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
            "totalQuantity": sum(brand_quantity.values()),
            "avgBrandRevenue": round(total_revenue / unique_brands, 2) if unique_brands > 0 else 0
        }
    }

    await _set_cached(cache_key, result)
    return result


# Alias for backwards compatibility
async def async_get_revenue_trend(
    start_date: str,
    end_date: str,
    category_id: Optional[int] = None,
    granularity: str = "daily",
    brand: Optional[str] = None,
    source_id: Optional[int] = None
) -> Dict[str, Any]:
    """Alias for get_revenue_trend (backwards compatibility)."""
    return await get_revenue_trend(
        start_date, end_date, granularity,
        category_id=category_id, brand=brand, source_id=source_id
    )


# ─── Cache Warming ───────────────────────────────────────────────────────────

_cache_warming_task: Optional[asyncio.Task] = None
_cache_warming_stop = False


async def _warm_cache_for_periods():
    """Pre-fetch data for common periods to warm the cache."""
    tz = ZoneInfo(DEFAULT_TIMEZONE)
    now = datetime.now(tz)
    today = now.strftime("%Y-%m-%d")

    periods = [
        # Today
        (today, today),
        # Yesterday
        ((now - timedelta(days=1)).strftime("%Y-%m-%d"),
         (now - timedelta(days=1)).strftime("%Y-%m-%d")),
        # This week
        ((now - timedelta(days=now.weekday())).strftime("%Y-%m-%d"), today),
        # Last week
        ((now - timedelta(days=now.weekday() + 7)).strftime("%Y-%m-%d"),
         (now - timedelta(days=now.weekday() + 1)).strftime("%Y-%m-%d")),
        # This month
        (now.replace(day=1).strftime("%Y-%m-%d"), today),
        # Last month
        ((now.replace(day=1) - timedelta(days=1)).replace(day=1).strftime("%Y-%m-%d"),
         (now.replace(day=1) - timedelta(days=1)).strftime("%Y-%m-%d")),
    ]

    for start, end in periods:
        if _cache_warming_stop:
            break
        try:
            logger.info(f"Cache warming: {start} to {end}")
            await get_summary_stats(start, end)
            await get_revenue_trend(start, end)
            await get_sales_by_source(start, end)
            await get_top_products(start, end)
        except Exception as e:
            logger.error(f"Cache warming error for {start}-{end}: {e}")


async def _cache_warming_loop():
    """Background loop that warms cache periodically."""
    global _cache_warming_stop
    logger.info("Cache warming loop started")

    while not _cache_warming_stop:
        try:
            await _warm_cache_for_periods()
        except Exception as e:
            logger.error(f"Cache warming loop error: {e}")

        # Wait 4 minutes before next warming cycle
        for _ in range(240):  # 240 seconds = 4 minutes
            if _cache_warming_stop:
                break
            await asyncio.sleep(1)

    logger.info("Cache warming loop stopped")


def start_cache_warming():
    """Start the background cache warming task."""
    global _cache_warming_task, _cache_warming_stop
    _cache_warming_stop = False

    try:
        loop = asyncio.get_running_loop()
        _cache_warming_task = loop.create_task(_cache_warming_loop())
        logger.info("Background cache warming started")
    except RuntimeError:
        # No running loop yet - will be started when app starts
        logger.info("Cache warming will start when event loop is running")


def stop_cache_warming():
    """Stop the background cache warming task."""
    global _cache_warming_task, _cache_warming_stop
    _cache_warming_stop = True

    if _cache_warming_task and not _cache_warming_task.done():
        _cache_warming_task.cancel()
        logger.info("Background cache warming stopped")
