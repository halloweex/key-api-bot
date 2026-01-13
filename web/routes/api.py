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
from web.services import category_service
from web.services import brand_service
from web.config import VERSION
from core.validators import (
    validate_period,
    validate_source_id,
    validate_category_id,
    validate_brand_name,
    validate_limit
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
    return {
        "status": "healthy",
        "version": VERSION,
        "uptime_seconds": uptime_seconds,
        "services": {
            "categories_loaded": category_service.is_products_loaded(),
            "brands_loaded": brand_service.is_brands_loaded()
        }
    }


# ─── Lightweight Endpoints (60/minute) ─────────────────────────────────────────

@router.get("/categories")
@limiter.limit("60/minute")
async def get_categories(request: Request):
    """Get list of root categories for filter dropdown."""
    return await category_service.get_categories_for_api()


@router.get("/categories/{parent_id}/children")
@limiter.limit("60/minute")
async def get_child_categories(request: Request, parent_id: int):
    """Get child categories for a parent category."""
    try:
        validate_category_id(parent_id, allow_none=False)
    except ValidationError as e:
        raise HTTPException(status_code=400, detail=str(e))
    return await category_service.get_child_categories(parent_id)


@router.get("/brands")
@limiter.limit("60/minute")
async def get_brands(request: Request):
    """Get list of brands for filter dropdown."""
    return await brand_service.get_brands_for_api()


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
    brand: Optional[str] = Query(None, description="Filter by brand name")
):
    """Get revenue trend data for line chart."""
    try:
        validate_period(period)
        validate_source_id(source_id)
        validate_category_id(category_id)
        brand = validate_brand_name(brand)
    except ValidationError as e:
        raise HTTPException(status_code=400, detail=str(e))

    start, end = dashboard_service.parse_period(period, start_date, end_date)
    return await dashboard_service.get_revenue_trend(
        start, end, category_id=category_id, brand=brand, source_id=source_id
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
    brand: Optional[str] = Query(None, description="Filter by brand name")
):
    """Get sales data by source for bar/pie chart."""
    try:
        validate_period(period)
        validate_source_id(source_id)
        validate_category_id(category_id)
        brand = validate_brand_name(brand)
    except ValidationError as e:
        raise HTTPException(status_code=400, detail=str(e))

    start, end = dashboard_service.parse_period(period, start_date, end_date)
    return await dashboard_service.get_sales_by_source(start, end, category_id, brand=brand, source_id=source_id)


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
    limit: int = Query(10, description="Number of products to return")
):
    """Get top products for horizontal bar chart."""
    try:
        validate_period(period)
        validate_source_id(source_id)
        validate_category_id(category_id)
        brand = validate_brand_name(brand)
        limit = validate_limit(limit, max_value=50)
    except ValidationError as e:
        raise HTTPException(status_code=400, detail=str(e))

    start, end = dashboard_service.parse_period(period, start_date, end_date)
    return await dashboard_service.get_top_products(start, end, source_id, limit, category_id, brand=brand)


@router.get("/summary")
@limiter.limit("30/minute")
async def get_summary(
    request: Request,
    period: Optional[str] = Query(None, description="Shortcut: today, yesterday, week, month"),
    start_date: Optional[str] = Query(None, description="Start date (YYYY-MM-DD)"),
    end_date: Optional[str] = Query(None, description="End date (YYYY-MM-DD)"),
    source_id: Optional[int] = Query(None, description="Filter by source ID"),
    category_id: Optional[int] = Query(None, description="Filter by category ID"),
    brand: Optional[str] = Query(None, description="Filter by brand name")
):
    """Get summary statistics for dashboard cards."""
    try:
        validate_period(period)
        validate_source_id(source_id)
        validate_category_id(category_id)
        brand = validate_brand_name(brand)
    except ValidationError as e:
        raise HTTPException(status_code=400, detail=str(e))

    start, end = dashboard_service.parse_period(period, start_date, end_date)
    return await dashboard_service.get_summary_stats(start, end, category_id, brand=brand, source_id=source_id)


@router.get("/customers/insights")
@limiter.limit("30/minute")
async def get_customer_insights(
    request: Request,
    period: Optional[str] = Query(None, description="Shortcut: today, yesterday, week, month"),
    start_date: Optional[str] = Query(None, description="Start date (YYYY-MM-DD)"),
    end_date: Optional[str] = Query(None, description="End date (YYYY-MM-DD)"),
    source_id: Optional[int] = Query(None, description="Filter by source ID"),
    brand: Optional[str] = Query(None, description="Filter by brand name")
):
    """Get customer insights: new vs returning, AOV trend, repeat rate."""
    try:
        validate_period(period)
        validate_source_id(source_id)
        brand = validate_brand_name(brand)
    except ValidationError as e:
        raise HTTPException(status_code=400, detail=str(e))

    start, end = dashboard_service.parse_period(period, start_date, end_date)
    return await dashboard_service.get_customer_insights(start, end, brand=brand, source_id=source_id)


@router.get("/products/performance")
@limiter.limit("30/minute")
async def get_product_performance(
    request: Request,
    period: Optional[str] = Query(None, description="Shortcut: today, yesterday, week, month"),
    start_date: Optional[str] = Query(None, description="Start date (YYYY-MM-DD)"),
    end_date: Optional[str] = Query(None, description="End date (YYYY-MM-DD)"),
    source_id: Optional[int] = Query(None, description="Filter by source ID"),
    brand: Optional[str] = Query(None, description="Filter by brand name")
):
    """Get product performance: top by revenue, category breakdown."""
    try:
        validate_period(period)
        validate_source_id(source_id)
        brand = validate_brand_name(brand)
    except ValidationError as e:
        raise HTTPException(status_code=400, detail=str(e))

    start, end = dashboard_service.parse_period(period, start_date, end_date)
    return await dashboard_service.get_product_performance(start, end, brand=brand, source_id=source_id)


@router.get("/brands/analytics")
@limiter.limit("30/minute")
async def get_brand_analytics(
    request: Request,
    period: Optional[str] = Query(None, description="Shortcut: today, yesterday, week, month"),
    start_date: Optional[str] = Query(None, description="Start date (YYYY-MM-DD)"),
    end_date: Optional[str] = Query(None, description="End date (YYYY-MM-DD)")
):
    """Get brand analytics: top brands by revenue and quantity."""
    try:
        validate_period(period)
    except ValidationError as e:
        raise HTTPException(status_code=400, detail=str(e))

    start, end = dashboard_service.parse_period(period, start_date, end_date)
    return await dashboard_service.get_brand_analytics(start, end)
