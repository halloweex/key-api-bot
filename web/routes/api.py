"""
API routes for chart data.
Fully async implementation.

All endpoints are rate-limited to prevent abuse:
- Lightweight endpoints (categories, brands, health): 60 requests/minute
- Data-heavy endpoints (revenue, summary, etc.): 30 requests/minute
"""
import time
from fastapi import APIRouter, Query, Request, HTTPException
from typing import Optional

from slowapi import Limiter
from slowapi.util import get_remote_address

from web.services import dashboard_service
from core.duckdb_store import get_store
from web.config import VERSION
from core.validators import (
    validate_period,
    validate_source_id,
    validate_category_id,
    validate_brand_name,
    validate_limit,
    validate_sales_type
)
from core.exceptions import ValidationError

router = APIRouter(tags=["api"])

# Track startup time for uptime calculation
_start_time = time.time()

# Rate limiter (uses app.state.limiter from main.py)
limiter = Limiter(key_func=get_remote_address)


# ─── Health Check ────────────────────────────────────────────────────────────

@router.get("/health")
@limiter.limit("60/minute")
async def health_check(request: Request):
    """Health check endpoint for Docker/load balancer monitoring."""
    uptime_seconds = int(time.time() - _start_time)

    # Get DuckDB stats
    try:
        store = await get_store()
        duckdb_stats = await store.get_stats()
        duckdb_status = "connected"
    except Exception as e:
        duckdb_stats = None
        duckdb_status = f"error: {e}"

    return {
        "status": "healthy" if duckdb_stats else "degraded",
        "version": VERSION,
        "uptime_seconds": uptime_seconds,
        "duckdb": {
            "status": duckdb_status,
            **(duckdb_stats or {})
        }
    }


@router.get("/duckdb/stats")
@limiter.limit("60/minute")
async def get_duckdb_stats(request: Request):
    """Get DuckDB analytics store statistics."""
    try:
        store = await get_store()
        stats = await store.get_stats()
        return {
            "status": "connected",
            **stats
        }
    except Exception as e:
        return {
            "status": "error",
            "error": str(e)
        }


@router.post("/duckdb/resync")
@limiter.limit("1/minute")
async def trigger_resync(request: Request, days: int = 365):
    """
    Force a complete resync of orders from KeyCRM API.

    Use this when data discrepancies are detected between dashboard and KeyCRM.
    This clears all order data and performs a fresh sync.

    WARNING: This operation can take several minutes for large datasets.
    """
    from core.sync_service import force_resync

    try:
        stats = await force_resync(days_back=days)
        return {
            "status": "success",
            "message": f"Resync complete - synced last {days} days",
            "stats": stats
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Resync failed: {str(e)}")


# ─── Lightweight Endpoints (60/minute) ─────────────────────────────────────────

@router.get("/categories")
@limiter.limit("60/minute")
async def get_categories(request: Request):
    """Get list of root categories for filter dropdown."""
    store = await get_store()
    return await store.get_categories()


@router.get("/categories/{parent_id}/children")
@limiter.limit("60/minute")
async def get_child_categories(request: Request, parent_id: int):
    """Get child categories for a parent category."""
    try:
        validate_category_id(parent_id, allow_none=False)
    except ValidationError as e:
        raise HTTPException(status_code=400, detail=str(e))
    store = await get_store()
    return await store.get_child_categories(parent_id)


@router.get("/brands")
@limiter.limit("60/minute")
async def get_brands(request: Request):
    """Get list of brands for filter dropdown."""
    store = await get_store()
    return await store.get_brands()


# ─── Data-Heavy Endpoints (30/minute) ──────────────────────────────────────────

@router.get("/revenue/trend")
@limiter.limit("30/minute")
async def get_revenue_trend(
    request: Request,
    period: Optional[str] = Query(None, description="Shortcut: today, yesterday, week, month"),
    start_date: Optional[str] = Query(None, description="Start date (YYYY-MM-DD)"),
    end_date: Optional[str] = Query(None, description="End date (YYYY-MM-DD)"),
    source_id: Optional[int] = Query(None, description="Filter by source ID"),
    category_id: Optional[int] = Query(None, description="Filter by category ID"),
    brand: Optional[str] = Query(None, description="Filter by brand name"),
    sales_type: Optional[str] = Query("retail", description="Sales type: retail, b2b, or all")
):
    """Get revenue trend data for line chart."""
    try:
        validate_period(period)
        validate_source_id(source_id)
        validate_category_id(category_id)
        brand = validate_brand_name(brand)
        sales_type = validate_sales_type(sales_type)
    except ValidationError as e:
        raise HTTPException(status_code=400, detail=str(e))

    start, end = dashboard_service.parse_period(period, start_date, end_date)
    return await dashboard_service.get_revenue_trend(
        start, end, category_id=category_id, brand=brand, source_id=source_id, sales_type=sales_type
    )


@router.get("/sales/by-source")
@limiter.limit("30/minute")
async def get_sales_by_source(
    request: Request,
    period: Optional[str] = Query(None, description="Shortcut: today, yesterday, week, month"),
    start_date: Optional[str] = Query(None, description="Start date (YYYY-MM-DD)"),
    end_date: Optional[str] = Query(None, description="End date (YYYY-MM-DD)"),
    source_id: Optional[int] = Query(None, description="Filter by source ID"),
    category_id: Optional[int] = Query(None, description="Filter by category ID"),
    brand: Optional[str] = Query(None, description="Filter by brand name"),
    sales_type: Optional[str] = Query("retail", description="Sales type: retail, b2b, or all")
):
    """Get sales data by source for bar/pie chart."""
    try:
        validate_period(period)
        validate_source_id(source_id)
        validate_category_id(category_id)
        brand = validate_brand_name(brand)
        sales_type = validate_sales_type(sales_type)
    except ValidationError as e:
        raise HTTPException(status_code=400, detail=str(e))

    start, end = dashboard_service.parse_period(period, start_date, end_date)
    return await dashboard_service.get_sales_by_source(start, end, category_id, brand=brand, source_id=source_id, sales_type=sales_type)


@router.get("/products/top")
@limiter.limit("30/minute")
async def get_top_products(
    request: Request,
    period: Optional[str] = Query(None, description="Shortcut: today, yesterday, week, month"),
    start_date: Optional[str] = Query(None, description="Start date (YYYY-MM-DD)"),
    end_date: Optional[str] = Query(None, description="End date (YYYY-MM-DD)"),
    source_id: Optional[int] = Query(None, description="Filter by source ID"),
    category_id: Optional[int] = Query(None, description="Filter by category ID"),
    brand: Optional[str] = Query(None, description="Filter by brand name"),
    limit: int = Query(10, description="Number of products to return"),
    sales_type: Optional[str] = Query("retail", description="Sales type: retail, b2b, or all")
):
    """Get top products for horizontal bar chart."""
    try:
        validate_period(period)
        validate_source_id(source_id)
        validate_category_id(category_id)
        brand = validate_brand_name(brand)
        limit = validate_limit(limit, max_value=50)
        sales_type = validate_sales_type(sales_type)
    except ValidationError as e:
        raise HTTPException(status_code=400, detail=str(e))

    start, end = dashboard_service.parse_period(period, start_date, end_date)
    return await dashboard_service.get_top_products(start, end, source_id, limit, category_id, brand=brand, sales_type=sales_type)


@router.get("/summary")
@limiter.limit("30/minute")
async def get_summary(
    request: Request,
    period: Optional[str] = Query(None, description="Shortcut: today, yesterday, week, month"),
    start_date: Optional[str] = Query(None, description="Start date (YYYY-MM-DD)"),
    end_date: Optional[str] = Query(None, description="End date (YYYY-MM-DD)"),
    source_id: Optional[int] = Query(None, description="Filter by source ID"),
    category_id: Optional[int] = Query(None, description="Filter by category ID"),
    brand: Optional[str] = Query(None, description="Filter by brand name"),
    sales_type: Optional[str] = Query("retail", description="Sales type: retail, b2b, or all")
):
    """Get summary statistics for dashboard cards."""
    try:
        validate_period(period)
        validate_source_id(source_id)
        validate_category_id(category_id)
        brand = validate_brand_name(brand)
        sales_type = validate_sales_type(sales_type)
    except ValidationError as e:
        raise HTTPException(status_code=400, detail=str(e))

    start, end = dashboard_service.parse_period(period, start_date, end_date)
    return await dashboard_service.get_summary_stats(start, end, category_id, brand=brand, source_id=source_id, sales_type=sales_type)


@router.get("/customers/insights")
@limiter.limit("30/minute")
async def get_customer_insights(
    request: Request,
    period: Optional[str] = Query(None, description="Shortcut: today, yesterday, week, month"),
    start_date: Optional[str] = Query(None, description="Start date (YYYY-MM-DD)"),
    end_date: Optional[str] = Query(None, description="End date (YYYY-MM-DD)"),
    source_id: Optional[int] = Query(None, description="Filter by source ID"),
    brand: Optional[str] = Query(None, description="Filter by brand name"),
    sales_type: Optional[str] = Query("retail", description="Sales type: retail, b2b, or all")
):
    """Get customer insights: new vs returning, AOV trend, repeat rate."""
    try:
        validate_period(period)
        validate_source_id(source_id)
        brand = validate_brand_name(brand)
        sales_type = validate_sales_type(sales_type)
    except ValidationError as e:
        raise HTTPException(status_code=400, detail=str(e))

    start, end = dashboard_service.parse_period(period, start_date, end_date)
    return await dashboard_service.get_customer_insights(start, end, brand=brand, source_id=source_id, sales_type=sales_type)


@router.get("/products/performance")
@limiter.limit("30/minute")
async def get_product_performance(
    request: Request,
    period: Optional[str] = Query(None, description="Shortcut: today, yesterday, week, month"),
    start_date: Optional[str] = Query(None, description="Start date (YYYY-MM-DD)"),
    end_date: Optional[str] = Query(None, description="End date (YYYY-MM-DD)"),
    source_id: Optional[int] = Query(None, description="Filter by source ID"),
    brand: Optional[str] = Query(None, description="Filter by brand name"),
    sales_type: Optional[str] = Query("retail", description="Sales type: retail, b2b, or all")
):
    """Get product performance: top by revenue, category breakdown."""
    try:
        validate_period(period)
        validate_source_id(source_id)
        brand = validate_brand_name(brand)
        sales_type = validate_sales_type(sales_type)
    except ValidationError as e:
        raise HTTPException(status_code=400, detail=str(e))

    start, end = dashboard_service.parse_period(period, start_date, end_date)
    return await dashboard_service.get_product_performance(start, end, brand=brand, source_id=source_id, sales_type=sales_type)


@router.get("/brands/analytics")
@limiter.limit("30/minute")
async def get_brand_analytics(
    request: Request,
    period: Optional[str] = Query(None, description="Shortcut: today, yesterday, week, month"),
    start_date: Optional[str] = Query(None, description="Start date (YYYY-MM-DD)"),
    end_date: Optional[str] = Query(None, description="End date (YYYY-MM-DD)"),
    sales_type: Optional[str] = Query("retail", description="Sales type: retail, b2b, or all")
):
    """Get brand analytics: top brands by revenue and quantity."""
    try:
        validate_period(period)
        sales_type = validate_sales_type(sales_type)
    except ValidationError as e:
        raise HTTPException(status_code=400, detail=str(e))

    start, end = dashboard_service.parse_period(period, start_date, end_date)
    return await dashboard_service.get_brand_analytics(start, end, sales_type=sales_type)


@router.get("/categories/breakdown")
@limiter.limit("30/minute")
async def get_subcategory_breakdown(
    request: Request,
    parent_category: str = Query(..., description="Parent category name to drill down into"),
    period: Optional[str] = Query(None, description="Shortcut: today, yesterday, week, month"),
    start_date: Optional[str] = Query(None, description="Start date (YYYY-MM-DD)"),
    end_date: Optional[str] = Query(None, description="End date (YYYY-MM-DD)"),
    source_id: Optional[int] = Query(None, description="Filter by source ID"),
    brand: Optional[str] = Query(None, description="Filter by brand name"),
    sales_type: Optional[str] = Query("retail", description="Sales type: retail, b2b, or all")
):
    """Get sales breakdown by subcategories for a given parent category (drill-down)."""
    try:
        validate_period(period)
        validate_source_id(source_id)
        brand = validate_brand_name(brand)
        sales_type = validate_sales_type(sales_type)
    except ValidationError as e:
        raise HTTPException(status_code=400, detail=str(e))

    start, end = dashboard_service.parse_period(period, start_date, end_date)
    return await dashboard_service.get_subcategory_breakdown(
        start, end,
        parent_category=parent_category,
        source_id=source_id,
        brand=brand,
        sales_type=sales_type
    )


# ─── Expense Endpoints ─────────────────────────────────────────────────────────


@router.get("/expense-types")
@limiter.limit("60/minute")
async def get_expense_types(request: Request):
    """Get list of expense types for filter dropdown."""
    return await dashboard_service.get_expense_types()


@router.get("/expenses/summary")
@limiter.limit("30/minute")
async def get_expense_summary(
    request: Request,
    period: Optional[str] = Query(None, description="Shortcut: today, yesterday, week, month"),
    start_date: Optional[str] = Query(None, description="Start date (YYYY-MM-DD)"),
    end_date: Optional[str] = Query(None, description="End date (YYYY-MM-DD)"),
    source_id: Optional[int] = Query(None, description="Filter by source ID"),
    expense_type_id: Optional[int] = Query(None, description="Filter by expense type ID"),
    sales_type: Optional[str] = Query("retail", description="Sales type: retail, b2b, or all")
):
    """Get expense summary: breakdown by type, daily trend."""
    try:
        validate_period(period)
        validate_source_id(source_id)
        sales_type = validate_sales_type(sales_type)
    except ValidationError as e:
        raise HTTPException(status_code=400, detail=str(e))

    start, end = dashboard_service.parse_period(period, start_date, end_date)
    return await dashboard_service.get_expense_summary(
        start, end,
        source_id=source_id,
        expense_type_id=expense_type_id,
        sales_type=sales_type
    )


@router.get("/expenses/profit")
@limiter.limit("30/minute")
async def get_profit_analysis(
    request: Request,
    period: Optional[str] = Query(None, description="Shortcut: today, yesterday, week, month"),
    start_date: Optional[str] = Query(None, description="Start date (YYYY-MM-DD)"),
    end_date: Optional[str] = Query(None, description="End date (YYYY-MM-DD)"),
    source_id: Optional[int] = Query(None, description="Filter by source ID"),
    sales_type: Optional[str] = Query("retail", description="Sales type: retail, b2b, or all")
):
    """Get profit analysis: revenue vs expenses comparison."""
    try:
        validate_period(period)
        validate_source_id(source_id)
        sales_type = validate_sales_type(sales_type)
    except ValidationError as e:
        raise HTTPException(status_code=400, detail=str(e))

    start, end = dashboard_service.parse_period(period, start_date, end_date)
    return await dashboard_service.get_profit_analysis(
        start, end,
        source_id=source_id,
        sales_type=sales_type
    )
