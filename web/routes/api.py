"""
API routes for chart data.
"""
from fastapi import APIRouter, Query
from typing import Optional, List

from web.services import dashboard_service
from web.services import category_service
from web.services import brand_service

router = APIRouter(tags=["api"])


@router.get("/categories")
async def get_categories():
    """Get list of root categories for filter dropdown."""
    return category_service.get_categories_for_api()


@router.get("/categories/{parent_id}/children")
async def get_child_categories(parent_id: int):
    """Get child categories for a parent category."""
    return category_service.get_child_categories(parent_id)


@router.get("/brands")
async def get_brands():
    """Get list of brands for filter dropdown."""
    return brand_service.get_brands_for_api()


@router.get("/revenue/trend")
async def get_revenue_trend(
    period: Optional[str] = Query(None, description="Shortcut: today, yesterday, week, month"),
    start_date: Optional[str] = Query(None, description="Start date (YYYY-MM-DD)"),
    end_date: Optional[str] = Query(None, description="End date (YYYY-MM-DD)"),
    category_id: Optional[int] = Query(None, description="Filter by category ID"),
    brand: Optional[str] = Query(None, description="Filter by brand name")
):
    """Get revenue trend data for line chart."""
    start, end = dashboard_service.parse_period(period, start_date, end_date)
    return await dashboard_service.async_get_revenue_trend(start, end, category_id, brand=brand)


@router.get("/sales/by-source")
async def get_sales_by_source(
    period: Optional[str] = Query(None, description="Shortcut: today, yesterday, week, month"),
    start_date: Optional[str] = Query(None, description="Start date (YYYY-MM-DD)"),
    end_date: Optional[str] = Query(None, description="End date (YYYY-MM-DD)"),
    category_id: Optional[int] = Query(None, description="Filter by category ID"),
    brand: Optional[str] = Query(None, description="Filter by brand name")
):
    """Get sales data by source for bar/pie chart."""
    start, end = dashboard_service.parse_period(period, start_date, end_date)
    return dashboard_service.get_sales_by_source(start, end, category_id, brand=brand)


@router.get("/products/top")
async def get_top_products(
    period: Optional[str] = Query(None, description="Shortcut: today, yesterday, week, month"),
    start_date: Optional[str] = Query(None, description="Start date (YYYY-MM-DD)"),
    end_date: Optional[str] = Query(None, description="End date (YYYY-MM-DD)"),
    source_id: Optional[int] = Query(None, description="Filter by source ID"),
    category_id: Optional[int] = Query(None, description="Filter by category ID"),
    brand: Optional[str] = Query(None, description="Filter by brand name"),
    limit: int = Query(10, description="Number of products to return")
):
    """Get top products for horizontal bar chart."""
    start, end = dashboard_service.parse_period(period, start_date, end_date)
    return dashboard_service.get_top_products(start, end, source_id, limit, category_id, brand=brand)


@router.get("/summary")
async def get_summary(
    period: Optional[str] = Query(None, description="Shortcut: today, yesterday, week, month"),
    start_date: Optional[str] = Query(None, description="Start date (YYYY-MM-DD)"),
    end_date: Optional[str] = Query(None, description="End date (YYYY-MM-DD)"),
    category_id: Optional[int] = Query(None, description="Filter by category ID"),
    brand: Optional[str] = Query(None, description="Filter by brand name")
):
    """Get summary statistics for dashboard cards."""
    start, end = dashboard_service.parse_period(period, start_date, end_date)
    return dashboard_service.get_summary_stats(start, end, category_id, brand=brand)


@router.get("/customers/insights")
async def get_customer_insights(
    period: Optional[str] = Query(None, description="Shortcut: today, yesterday, week, month"),
    start_date: Optional[str] = Query(None, description="Start date (YYYY-MM-DD)"),
    end_date: Optional[str] = Query(None, description="End date (YYYY-MM-DD)"),
    brand: Optional[str] = Query(None, description="Filter by brand name")
):
    """Get customer insights: new vs returning, AOV trend, repeat rate."""
    start, end = dashboard_service.parse_period(period, start_date, end_date)
    return dashboard_service.get_customer_insights(start, end, brand=brand)


@router.get("/products/performance")
async def get_product_performance(
    period: Optional[str] = Query(None, description="Shortcut: today, yesterday, week, month"),
    start_date: Optional[str] = Query(None, description="Start date (YYYY-MM-DD)"),
    end_date: Optional[str] = Query(None, description="End date (YYYY-MM-DD)"),
    brand: Optional[str] = Query(None, description="Filter by brand name")
):
    """Get product performance: top by revenue, category breakdown."""
    start, end = dashboard_service.parse_period(period, start_date, end_date)
    return dashboard_service.get_product_performance(start, end, brand=brand)
