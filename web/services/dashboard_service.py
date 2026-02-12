"""
Dashboard service for analytics data.

Uses DuckDB as the primary data source for fast, persistent queries.
No in-memory caching needed - DuckDB queries are <10ms.
"""
import logging
from datetime import date, datetime
from typing import Dict, List, Optional, Any, Tuple

from core.duckdb_store import get_store
from core.filters import parse_period as _parse_period_core

logger = logging.getLogger(__name__)


def parse_period(period: Optional[str], start_date: Optional[str], end_date: Optional[str]) -> Tuple[str, str]:
    """Parse period shortcut or dates into (start_date, end_date) tuple."""
    return _parse_period_core(period, start_date, end_date).as_str_tuple()


def _parse_dates(start_date: str, end_date: str) -> Tuple[date, date]:
    """Parse date strings to date objects."""
    start = datetime.strptime(start_date, "%Y-%m-%d").date()
    end = datetime.strptime(end_date, "%Y-%m-%d").date()
    return start, end


# ─── Dashboard Query Functions ────────────────────────────────────────────────


async def get_revenue_trend(
    start_date: str,
    end_date: str,
    granularity: str = "daily",
    include_comparison: bool = True,
    category_id: Optional[int] = None,
    brand: Optional[str] = None,
    source_id: Optional[int] = None,
    sales_type: str = "retail",
    compare_type: str = "previous_period"
) -> Dict[str, Any]:
    """Get revenue data over time for line chart."""
    start, end = _parse_dates(start_date, end_date)
    store = await get_store()
    return await store.get_revenue_trend(
        start, end,
        source_id=source_id,
        category_id=category_id,
        brand=brand,
        include_comparison=include_comparison,
        sales_type=sales_type,
        compare_type=compare_type
    )


async def get_forecast_data(sales_type: str = "retail") -> Optional[Dict[str, Any]]:
    """Get ML revenue forecast for the current month."""
    try:
        from core.prediction_service import get_prediction_service
        service = get_prediction_service()
        return await service.get_forecast(sales_type)
    except Exception as e:
        logger.warning(f"Forecast data unavailable: {e}")
        return None


async def get_comparison_for_dates(
    dates: List[date],
    compare_type: str = "year_ago",
    sales_type: str = "retail",
) -> Dict[date, float]:
    """Get comparison revenue for specific dates.

    Returns dict mapping each date to its comparison-period revenue.
    E.g. for year_ago, date 2026-02-01 maps to revenue on 2025-02-01.
    """
    from dateutil.relativedelta import relativedelta

    if not dates:
        return {}

    # Compute comparison dates
    comp_dates = []
    date_map = {}  # comp_date -> original_date
    for d in dates:
        if compare_type == "year_ago":
            cd = d - relativedelta(years=1)
        elif compare_type == "month_ago":
            cd = d - relativedelta(months=1)
        else:
            # previous_period: shift by len(dates) days back
            cd = d - relativedelta(days=len(dates))
        comp_dates.append(cd)
        date_map[cd] = d

    if not comp_dates:
        return {}

    store = await get_store()
    revenue_by_date = await store.get_daily_revenue_for_dates(comp_dates, sales_type)

    # Map back to original dates
    return {date_map[cd]: revenue_by_date.get(cd, 0) for cd in comp_dates}


async def get_sales_by_source(
    start_date: str,
    end_date: str,
    category_id: Optional[int] = None,
    brand: Optional[str] = None,
    source_id: Optional[int] = None,
    sales_type: str = "retail"
) -> Dict[str, Any]:
    """Get sales data aggregated by source for bar/pie chart."""
    start, end = _parse_dates(start_date, end_date)
    store = await get_store()
    return await store.get_sales_by_source(
        start, end,
        category_id=category_id,
        brand=brand,
        sales_type=sales_type
    )


async def get_top_products(
    start_date: str,
    end_date: str,
    source_id: Optional[int] = None,
    limit: int = 10,
    category_id: Optional[int] = None,
    brand: Optional[str] = None,
    sales_type: str = "retail"
) -> Dict[str, Any]:
    """Get top products for horizontal bar chart."""
    start, end = _parse_dates(start_date, end_date)
    store = await get_store()
    return await store.get_top_products(
        start, end,
        source_id=source_id,
        category_id=category_id,
        brand=brand,
        limit=limit,
        sales_type=sales_type
    )


async def get_summary_stats(
    start_date: str,
    end_date: str,
    category_id: Optional[int] = None,
    brand: Optional[str] = None,
    source_id: Optional[int] = None,
    sales_type: str = "retail"
) -> Dict[str, Any]:
    """Get summary statistics for dashboard cards."""
    start, end = _parse_dates(start_date, end_date)
    store = await get_store()
    return await store.get_summary_stats(
        start, end,
        source_id=source_id,
        category_id=category_id,
        brand=brand,
        sales_type=sales_type
    )


async def get_customer_insights(
    start_date: str,
    end_date: str,
    brand: Optional[str] = None,
    source_id: Optional[int] = None,
    sales_type: str = "retail"
) -> Dict[str, Any]:
    """Get customer insights: new vs returning, AOV trend, repeat rate."""
    start, end = _parse_dates(start_date, end_date)
    store = await get_store()
    return await store.get_customer_insights(
        start, end,
        source_id=source_id,
        brand=brand,
        sales_type=sales_type
    )


async def get_product_performance(
    start_date: str,
    end_date: str,
    brand: Optional[str] = None,
    source_id: Optional[int] = None,
    sales_type: str = "retail"
) -> Dict[str, Any]:
    """Get product performance: top by revenue, category breakdown."""
    start, end = _parse_dates(start_date, end_date)
    store = await get_store()
    return await store.get_product_performance(
        start, end,
        source_id=source_id,
        brand=brand,
        sales_type=sales_type
    )


async def get_brand_analytics(start_date: str, end_date: str, sales_type: str = "retail") -> Dict[str, Any]:
    """Get brand analytics: top brands by revenue and quantity."""
    start, end = _parse_dates(start_date, end_date)
    store = await get_store()
    return await store.get_brand_analytics(start, end, sales_type=sales_type)


async def get_subcategory_breakdown(
    start_date: str,
    end_date: str,
    parent_category: str,
    source_id: Optional[int] = None,
    brand: Optional[str] = None,
    sales_type: str = "retail"
) -> Dict[str, Any]:
    """Get sales breakdown by subcategories for a given parent category."""
    start, end = _parse_dates(start_date, end_date)
    store = await get_store()
    return await store.get_subcategory_breakdown(
        start, end,
        parent_category_name=parent_category,
        source_id=source_id,
        brand=brand,
        sales_type=sales_type
    )


async def get_expense_types() -> List[Dict[str, Any]]:
    """Get list of expense types for filter dropdown."""
    store = await get_store()
    return await store.get_expense_types()


async def get_expense_summary(
    start_date: str,
    end_date: str,
    source_id: Optional[int] = None,
    expense_type_id: Optional[int] = None,
    sales_type: str = "retail"
) -> Dict[str, Any]:
    """Get expense summary: breakdown by type, daily trend."""
    start, end = _parse_dates(start_date, end_date)
    store = await get_store()
    return await store.get_expense_summary(
        start, end,
        source_id=source_id,
        expense_type_id=expense_type_id,
        sales_type=sales_type
    )


async def get_profit_analysis(
    start_date: str,
    end_date: str,
    source_id: Optional[int] = None,
    sales_type: str = "retail"
) -> Dict[str, Any]:
    """Get profit analysis: revenue vs expenses."""
    start, end = _parse_dates(start_date, end_date)
    store = await get_store()
    return await store.get_profit_analysis(
        start, end,
        source_id=source_id,
        sales_type=sales_type
    )


# ─── Backwards Compatibility ──────────────────────────────────────────────────


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


# ─── No-op Cache Functions (for backwards compatibility) ─────────────────────


def start_cache_warming():
    """No-op: DuckDB replaces in-memory cache warming."""
    logger.info("Cache warming disabled - using DuckDB for queries")


def stop_cache_warming():
    """No-op: DuckDB replaces in-memory cache warming."""
    pass
