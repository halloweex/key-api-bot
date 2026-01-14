"""
Dashboard service for analytics data.

Uses DuckDB as the primary data source for fast, persistent queries.
No in-memory caching needed - DuckDB queries are <10ms.
"""
import logging
from datetime import date, datetime
from typing import Dict, Optional, Any, Tuple

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
    source_id: Optional[int] = None
) -> Dict[str, Any]:
    """Get revenue data over time for line chart."""
    start, end = _parse_dates(start_date, end_date)
    store = await get_store()
    return await store.get_revenue_trend(
        start, end,
        source_id=source_id,
        category_id=category_id,
        brand=brand,
        include_comparison=include_comparison
    )


async def get_sales_by_source(
    start_date: str,
    end_date: str,
    category_id: Optional[int] = None,
    brand: Optional[str] = None,
    source_id: Optional[int] = None
) -> Dict[str, Any]:
    """Get sales data aggregated by source for bar/pie chart."""
    start, end = _parse_dates(start_date, end_date)
    store = await get_store()
    return await store.get_sales_by_source(
        start, end,
        category_id=category_id,
        brand=brand
    )


async def get_top_products(
    start_date: str,
    end_date: str,
    source_id: Optional[int] = None,
    limit: int = 10,
    category_id: Optional[int] = None,
    brand: Optional[str] = None
) -> Dict[str, Any]:
    """Get top products for horizontal bar chart."""
    start, end = _parse_dates(start_date, end_date)
    store = await get_store()
    return await store.get_top_products(
        start, end,
        source_id=source_id,
        category_id=category_id,
        brand=brand,
        limit=limit
    )


async def get_summary_stats(
    start_date: str,
    end_date: str,
    category_id: Optional[int] = None,
    brand: Optional[str] = None,
    source_id: Optional[int] = None
) -> Dict[str, Any]:
    """Get summary statistics for dashboard cards."""
    start, end = _parse_dates(start_date, end_date)
    store = await get_store()
    return await store.get_summary_stats(
        start, end,
        source_id=source_id,
        category_id=category_id,
        brand=brand
    )


async def get_customer_insights(
    start_date: str,
    end_date: str,
    brand: Optional[str] = None,
    source_id: Optional[int] = None
) -> Dict[str, Any]:
    """Get customer insights: new vs returning, AOV trend, repeat rate."""
    start, end = _parse_dates(start_date, end_date)
    store = await get_store()
    return await store.get_customer_insights(
        start, end,
        source_id=source_id,
        brand=brand
    )


async def get_product_performance(
    start_date: str,
    end_date: str,
    brand: Optional[str] = None,
    source_id: Optional[int] = None
) -> Dict[str, Any]:
    """Get product performance: top by revenue, category breakdown."""
    start, end = _parse_dates(start_date, end_date)
    store = await get_store()
    return await store.get_product_performance(
        start, end,
        source_id=source_id,
        brand=brand
    )


async def get_brand_analytics(start_date: str, end_date: str) -> Dict[str, Any]:
    """Get brand analytics: top brands by revenue and quantity."""
    start, end = _parse_dates(start_date, end_date)
    store = await get_store()
    return await store.get_brand_analytics(start, end)


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
