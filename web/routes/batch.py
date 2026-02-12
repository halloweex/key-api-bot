"""
Batch API endpoint for fetching multiple dashboard sections in a single request.

Reduces round-trips and improves dashboard load time by parallelizing data fetches.
"""
import asyncio
import logging
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Query, Request
from pydantic import BaseModel, Field
from slowapi import Limiter
from slowapi.util import get_remote_address

from web.services import dashboard_service
from core.duckdb_store import get_store
from core.validators import (
    validate_period,
    validate_source_id,
    validate_category_id,
    validate_brand_name,
    validate_sales_type,
)
from core.exceptions import ValidationError

router = APIRouter(tags=["batch"])
limiter = Limiter(key_func=get_remote_address)
logger = logging.getLogger(__name__)


class BatchRequest(BaseModel):
    """Request body for batch dashboard endpoint."""

    sections: List[str] = Field(
        ...,
        description="Sections to fetch: summary, revenue, sales, products, customers, brands, goals",
        min_length=1,
        max_length=10,
    )
    # Filter parameters (optional)
    period: Optional[str] = Field(None, description="Period shortcut: today, yesterday, week, month")
    start_date: Optional[str] = Field(None, description="Start date (YYYY-MM-DD)")
    end_date: Optional[str] = Field(None, description="End date (YYYY-MM-DD)")
    source_id: Optional[int] = Field(None, description="Filter by source ID")
    category_id: Optional[int] = Field(None, description="Filter by category ID")
    brand: Optional[str] = Field(None, description="Filter by brand name")
    sales_type: Optional[str] = Field("retail", description="Sales type: retail, b2b, or all")
    compare_type: Optional[str] = Field("previous_period", description="Comparison type")
    include_forecast: Optional[bool] = Field(False, description="Include ML forecast")


class BatchResponse(BaseModel):
    """Response containing multiple dashboard sections."""

    summary: Optional[Dict[str, Any]] = None
    revenue: Optional[Dict[str, Any]] = None
    sales: Optional[Dict[str, Any]] = None
    products: Optional[Dict[str, Any]] = None
    customers: Optional[Dict[str, Any]] = None
    brands: Optional[Dict[str, Any]] = None
    goals: Optional[Dict[str, Any]] = None
    errors: Optional[Dict[str, str]] = None
    meta: Dict[str, Any] = Field(default_factory=dict)


# Map section names to fetch functions
SECTION_FETCHERS = {
    "summary": "_fetch_summary",
    "revenue": "_fetch_revenue",
    "sales": "_fetch_sales",
    "products": "_fetch_products",
    "customers": "_fetch_customers",
    "brands": "_fetch_brands",
    "goals": "_fetch_goals",
}


async def _fetch_summary(
    start: str, end: str, category_id: Optional[int], brand: Optional[str],
    source_id: Optional[int], sales_type: str, **kwargs
) -> Dict[str, Any]:
    """Fetch summary statistics."""
    return await dashboard_service.get_summary_stats(
        start, end, category_id, brand=brand, source_id=source_id, sales_type=sales_type
    )


async def _fetch_revenue(
    start: str, end: str, category_id: Optional[int], brand: Optional[str],
    source_id: Optional[int], sales_type: str, compare_type: str,
    include_forecast: bool, period: Optional[str], **kwargs
) -> Dict[str, Any]:
    """Fetch revenue trend data."""
    result = await dashboard_service.get_revenue_trend(
        start, end,
        category_id=category_id,
        brand=brand,
        source_id=source_id,
        sales_type=sales_type,
        compare_type=compare_type
    )

    # Include forecast when requested and appropriate
    if include_forecast and period in ("month", "week") and not category_id and not brand and not source_id:
        try:
            forecast = await dashboard_service.get_forecast_data(sales_type)
            if forecast and forecast.get("daily_predictions"):
                result["forecast"] = forecast
        except Exception as e:
            logger.warning(f"Forecast unavailable in batch: {e}")

    return result


async def _fetch_sales(
    start: str, end: str, category_id: Optional[int], brand: Optional[str],
    source_id: Optional[int], sales_type: str, **kwargs
) -> Dict[str, Any]:
    """Fetch sales by source data."""
    return await dashboard_service.get_sales_by_source(
        start, end, category_id, brand=brand, source_id=source_id, sales_type=sales_type
    )


async def _fetch_products(
    start: str, end: str, category_id: Optional[int], brand: Optional[str],
    source_id: Optional[int], sales_type: str, **kwargs
) -> Dict[str, Any]:
    """Fetch top products."""
    return await dashboard_service.get_top_products(
        start, end, source_id, limit=10, category_id=category_id, brand=brand, sales_type=sales_type
    )


async def _fetch_customers(
    start: str, end: str, brand: Optional[str],
    source_id: Optional[int], sales_type: str, **kwargs
) -> Dict[str, Any]:
    """Fetch customer insights."""
    return await dashboard_service.get_customer_insights(
        start, end, brand=brand, source_id=source_id, sales_type=sales_type
    )


async def _fetch_brands(
    start: str, end: str, sales_type: str, **kwargs
) -> Dict[str, Any]:
    """Fetch brand analytics."""
    return await dashboard_service.get_brand_analytics(start, end, sales_type=sales_type)


async def _fetch_goals(sales_type: str, **kwargs) -> Dict[str, Any]:
    """Fetch revenue goals."""
    store = await get_store()
    return await store.get_goals(sales_type)


@router.post("/dashboard/batch", response_model=BatchResponse)
@limiter.limit("30/minute")
async def get_dashboard_batch(request: Request, batch: BatchRequest):
    """
    Fetch multiple dashboard sections in a single request.

    Reduces latency by parallelizing data fetches for the dashboard.
    Each section is fetched independently, so one failure won't affect others.

    Available sections:
    - summary: Summary statistics (total revenue, orders, customers)
    - revenue: Revenue trend with daily breakdown
    - sales: Sales breakdown by source
    - products: Top 10 products by quantity
    - customers: Customer insights (new vs returning)
    - brands: Brand analytics
    - goals: Revenue goals

    Returns:
        BatchResponse with requested sections and any errors that occurred.
    """
    import time
    start_time = time.perf_counter()

    # Validate parameters
    try:
        validate_period(batch.period)
        validate_source_id(batch.source_id)
        validate_category_id(batch.category_id)
        brand = validate_brand_name(batch.brand)
        sales_type = validate_sales_type(batch.sales_type)
        compare_type = batch.compare_type or "previous_period"
        if compare_type not in ("previous_period", "year_ago", "month_ago"):
            compare_type = "previous_period"
    except ValidationError as e:
        return BatchResponse(
            errors={"validation": str(e)},
            meta={"duration_ms": 0, "sections_requested": batch.sections}
        )

    # Parse period
    start, end = dashboard_service.parse_period(batch.period, batch.start_date, batch.end_date)

    # Prepare fetch parameters
    fetch_params = {
        "start": start,
        "end": end,
        "category_id": batch.category_id,
        "brand": brand,
        "source_id": batch.source_id,
        "sales_type": sales_type,
        "compare_type": compare_type,
        "include_forecast": batch.include_forecast,
        "period": batch.period,
    }

    # Create fetch tasks for requested sections
    tasks = {}
    for section in batch.sections:
        if section in SECTION_FETCHERS:
            fetcher = globals()[SECTION_FETCHERS[section]]
            tasks[section] = asyncio.create_task(
                _safe_fetch(section, fetcher, fetch_params)
            )
        else:
            logger.warning(f"Unknown batch section requested: {section}")

    # Execute all tasks in parallel
    results = {}
    errors = {}

    if tasks:
        completed = await asyncio.gather(*tasks.values(), return_exceptions=True)
        for section, result in zip(tasks.keys(), completed):
            if isinstance(result, Exception):
                errors[section] = str(result)
                logger.error(f"Batch fetch failed for {section}: {result}")
            elif isinstance(result, dict) and "error" in result:
                errors[section] = result["error"]
            else:
                results[section] = result

    duration_ms = (time.perf_counter() - start_time) * 1000

    return BatchResponse(
        summary=results.get("summary"),
        revenue=results.get("revenue"),
        sales=results.get("sales"),
        products=results.get("products"),
        customers=results.get("customers"),
        brands=results.get("brands"),
        goals=results.get("goals"),
        errors=errors if errors else None,
        meta={
            "duration_ms": round(duration_ms, 2),
            "sections_requested": batch.sections,
            "sections_fetched": list(results.keys()),
            "start_date": start,
            "end_date": end,
        }
    )


async def _safe_fetch(section: str, fetcher, params: Dict[str, Any]) -> Dict[str, Any]:
    """Safely execute a fetch function with timeout and error handling."""
    try:
        return await asyncio.wait_for(fetcher(**params), timeout=30.0)
    except asyncio.TimeoutError:
        return {"error": f"Timeout fetching {section}"}
    except Exception as e:
        logger.error(f"Error fetching {section}: {e}")
        return {"error": str(e)}
