"""
API routes for chart data.
"""
from fastapi import APIRouter, Query
from typing import Optional

from web.services import dashboard_service

router = APIRouter(tags=["api"])


@router.get("/revenue/trend")
async def get_revenue_trend(
    period: Optional[str] = Query(None, description="Shortcut: today, yesterday, week, month"),
    start_date: Optional[str] = Query(None, description="Start date (YYYY-MM-DD)"),
    end_date: Optional[str] = Query(None, description="End date (YYYY-MM-DD)")
):
    """Get revenue trend data for line chart."""
    start, end = dashboard_service.parse_period(period, start_date, end_date)
    # Use async version for better performance
    return await dashboard_service.async_get_revenue_trend(start, end)


@router.get("/sales/by-source")
async def get_sales_by_source(
    period: Optional[str] = Query(None, description="Shortcut: today, yesterday, week, month"),
    start_date: Optional[str] = Query(None, description="Start date (YYYY-MM-DD)"),
    end_date: Optional[str] = Query(None, description="End date (YYYY-MM-DD)")
):
    """Get sales data by source for bar/pie chart."""
    start, end = dashboard_service.parse_period(period, start_date, end_date)
    return dashboard_service.get_sales_by_source(start, end)


@router.get("/products/top")
async def get_top_products(
    period: Optional[str] = Query(None, description="Shortcut: today, yesterday, week, month"),
    start_date: Optional[str] = Query(None, description="Start date (YYYY-MM-DD)"),
    end_date: Optional[str] = Query(None, description="End date (YYYY-MM-DD)"),
    source_id: Optional[int] = Query(None, description="Filter by source ID"),
    limit: int = Query(10, description="Number of products to return")
):
    """Get top products for horizontal bar chart."""
    start, end = dashboard_service.parse_period(period, start_date, end_date)
    return dashboard_service.get_top_products(start, end, source_id, limit)


@router.get("/summary")
async def get_summary(
    period: Optional[str] = Query(None, description="Shortcut: today, yesterday, week, month"),
    start_date: Optional[str] = Query(None, description="Start date (YYYY-MM-DD)"),
    end_date: Optional[str] = Query(None, description="End date (YYYY-MM-DD)")
):
    """Get summary statistics for dashboard cards."""
    start, end = dashboard_service.parse_period(period, start_date, end_date)
    return dashboard_service.get_summary_stats(start, end)
