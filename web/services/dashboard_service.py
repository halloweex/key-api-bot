"""
Dashboard service for transforming sales data into chart-friendly formats.
"""
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any

from bot.api_client import KeyCRMClient
from bot.services import ReportService
from bot.config import (
    KEYCRM_API_KEY,
    SOURCE_MAPPING,
    DEFAULT_TIMEZONE,
)


# Source colors for charts
SOURCE_COLORS = {
    1: "#7C3AED",  # Instagram - purple (Accent)
    2: "#2563EB",  # Telegram - blue (Primary)
    3: "#F59E0B",  # Opencart - orange (Warning)
    4: "#16A34A",  # Shopify - green (Success)
}

SOURCE_COLORS_LIST = ["#7C3AED", "#2563EB", "#F59E0B", "#16A34A"]


def get_report_service() -> ReportService:
    """Create and return a ReportService instance."""
    client = KeyCRMClient(KEYCRM_API_KEY)
    return ReportService(client)


def parse_period(period: Optional[str], start_date: Optional[str], end_date: Optional[str]) -> tuple:
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
        elif period == "month":
            # This month (1st to today)
            start_of_month = today.replace(day=1)
            return (start_of_month.strftime("%Y-%m-%d"), today.strftime("%Y-%m-%d"))

    # Use provided dates or default to today
    if start_date and end_date:
        return (start_date, end_date)

    return (today.strftime("%Y-%m-%d"), today.strftime("%Y-%m-%d"))


def get_revenue_trend(
    start_date: str,
    end_date: str,
    granularity: str = "daily"
) -> Dict[str, Any]:
    """
    Get revenue data over time for line chart.

    Args:
        start_date: Start date (YYYY-MM-DD)
        end_date: End date (YYYY-MM-DD)
        granularity: 'daily', 'weekly', or 'monthly'

    Returns:
        Chart.js compatible data structure
    """
    service = get_report_service()

    start = datetime.strptime(start_date, "%Y-%m-%d").date()
    end = datetime.strptime(end_date, "%Y-%m-%d").date()

    labels = []
    data = []

    current = start
    while current <= end:
        date_str = current.strftime("%Y-%m-%d")

        try:
            _, _, _, revenue_dict, _ = service.aggregate_sales_data(
                target_date=date_str,
                tz_name=DEFAULT_TIMEZONE
            )
            total_revenue = sum(revenue_dict.values())
        except Exception:
            total_revenue = 0

        labels.append(current.strftime("%d.%m"))
        data.append(round(total_revenue, 2))

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


def get_sales_by_source(start_date: str, end_date: str) -> Dict[str, Any]:
    """
    Get sales data aggregated by source for bar/pie chart.

    Args:
        start_date: Start date (YYYY-MM-DD)
        end_date: End date (YYYY-MM-DD)

    Returns:
        Chart.js compatible data structure
    """
    service = get_report_service()

    try:
        _, counts_dict, _, revenue_dict, _ = service.aggregate_sales_data(
            target_date=(start_date, end_date),
            tz_name=DEFAULT_TIMEZONE
        )
    except Exception:
        counts_dict = {}
        revenue_dict = {}

    labels = []
    orders_data = []
    revenue_data = []
    colors = []

    for source_id in [1, 2, 3, 4]:  # Instagram, Telegram, Opencart, Shopify
        source_name = SOURCE_MAPPING.get(source_id, f"Source {source_id}")
        labels.append(source_name)
        # Handle both int and string keys from API
        orders_data.append(counts_dict.get(source_id, counts_dict.get(str(source_id), 0)))
        revenue_data.append(round(revenue_dict.get(source_id, revenue_dict.get(str(source_id), 0)), 2))
        colors.append(SOURCE_COLORS.get(source_id, "#999999"))

    return {
        "labels": labels,
        "orders": orders_data,
        "revenue": revenue_data,
        "backgroundColor": colors
    }


def get_top_products(
    start_date: str,
    end_date: str,
    source_id: Optional[int] = None,
    limit: int = 10
) -> Dict[str, Any]:
    """
    Get top products for horizontal bar chart.

    Args:
        start_date: Start date (YYYY-MM-DD)
        end_date: End date (YYYY-MM-DD)
        source_id: Optional source filter (1=Instagram, 2=Telegram, etc.)
        limit: Number of products to return

    Returns:
        Chart.js compatible data structure
    """
    service = get_report_service()

    try:
        sales_dict, _, _, _, _ = service.aggregate_sales_data(
            target_date=(start_date, end_date),
            tz_name=DEFAULT_TIMEZONE
        )
    except Exception:
        sales_dict = {}

    # Aggregate products across sources or filter by source
    products = {}
    if source_id:
        # Handle both int and string keys
        products = sales_dict.get(source_id, sales_dict.get(str(source_id), {}))
    else:
        for src_products in sales_dict.values():
            for product_name, qty in src_products.items():
                products[product_name] = products.get(product_name, 0) + qty

    # Sort and limit
    sorted_products = sorted(products.items(), key=lambda x: x[1], reverse=True)[:limit]

    # Reverse for horizontal bar (highest at top)
    sorted_products = list(reversed(sorted_products))

    labels = [p[0][:30] + "..." if len(p[0]) > 30 else p[0] for p in sorted_products]
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


def get_summary_stats(start_date: str, end_date: str) -> Dict[str, Any]:
    """
    Get summary statistics for dashboard cards.

    Args:
        start_date: Start date (YYYY-MM-DD)
        end_date: End date (YYYY-MM-DD)

    Returns:
        Summary statistics dictionary
    """
    service = get_report_service()

    try:
        _, counts_dict, total_orders, revenue_dict, returns_dict = service.aggregate_sales_data(
            target_date=(start_date, end_date),
            tz_name=DEFAULT_TIMEZONE
        )
    except Exception:
        counts_dict = {}
        total_orders = 0
        revenue_dict = {}
        returns_dict = {}

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
