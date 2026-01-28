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


# ─── Goal Endpoints ─────────────────────────────────────────────────────────


@router.get("/goals")
@limiter.limit("60/minute")
async def get_goals(
    request: Request,
    sales_type: Optional[str] = Query("retail", description="Sales type: retail, b2b, or all")
):
    """
    Get revenue goals for daily, weekly, and monthly periods.

    Returns both custom goals (if set) and auto-calculated suggestions based on
    historical performance (average of last 4 weeks × 10% growth factor).

    Response includes:
    - amount: Current goal (custom or auto-calculated)
    - isCustom: Whether goal was manually set
    - suggestedAmount: System-calculated suggestion
    - basedOnAverage: Historical average used for calculation
    - trend: Recent performance trend (% change)
    - confidence: Confidence level (high/medium/low) based on data availability
    """
    try:
        sales_type = validate_sales_type(sales_type)
    except ValidationError as e:
        raise HTTPException(status_code=400, detail=str(e))

    store = await get_store()
    return await store.get_goals(sales_type)


@router.get("/goals/history")
@limiter.limit("30/minute")
async def get_goal_history(
    request: Request,
    period_type: str = Query(..., description="Period type: daily, weekly, or monthly"),
    weeks_back: int = Query(4, ge=1, le=12, description="Number of weeks of history to analyze"),
    sales_type: Optional[str] = Query("retail", description="Sales type: retail, b2b, or all")
):
    """
    Get historical revenue data used for goal calculations.

    Returns statistics for the specified period type including:
    - average: Average revenue per period
    - min/max: Range of values
    - trend: Recent vs older performance change (%)
    - stdDev: Standard deviation (volatility)
    """
    if period_type not in ["daily", "weekly", "monthly"]:
        raise HTTPException(status_code=400, detail="period_type must be: daily, weekly, or monthly")

    try:
        sales_type = validate_sales_type(sales_type)
    except ValidationError as e:
        raise HTTPException(status_code=400, detail=str(e))

    store = await get_store()
    return await store.get_historical_revenue(period_type, weeks_back, sales_type)


@router.post("/goals")
@limiter.limit("10/minute")
async def set_goal(
    request: Request,
    period_type: str = Query(..., description="Period type: daily, weekly, or monthly"),
    amount: float = Query(..., gt=0, description="Goal amount in UAH"),
    growth_factor: float = Query(1.10, ge=1.0, le=2.0, description="Growth factor for future calculations")
):
    """
    Set a custom revenue goal.

    The goal will be marked as custom (manually set). To revert to auto-calculated
    goals, use DELETE /api/goals/{period_type}.
    """
    if period_type not in ["daily", "weekly", "monthly"]:
        raise HTTPException(status_code=400, detail="period_type must be: daily, weekly, or monthly")

    store = await get_store()
    return await store.set_goal(period_type, amount, is_custom=True, growth_factor=growth_factor)


@router.delete("/goals/{period_type}")
@limiter.limit("10/minute")
async def reset_goal(
    request: Request,
    period_type: str,
    sales_type: Optional[str] = Query("retail", description="Sales type: retail, b2b, or all")
):
    """
    Reset a goal to auto-calculated value.

    Removes the custom goal and reverts to using the system-calculated suggestion
    based on historical performance.
    """
    if period_type not in ["daily", "weekly", "monthly"]:
        raise HTTPException(status_code=400, detail="period_type must be: daily, weekly, or monthly")

    try:
        sales_type = validate_sales_type(sales_type)
    except ValidationError as e:
        raise HTTPException(status_code=400, detail=str(e))

    store = await get_store()
    return await store.reset_goal_to_auto(period_type, sales_type)


@router.get("/goals/smart")
@limiter.limit("30/minute")
async def get_smart_goals(
    request: Request,
    sales_type: Optional[str] = Query("retail", description="Sales type: retail, b2b, or all")
):
    """
    Get smart revenue goals using seasonality and YoY growth.

    This is an enhanced goal calculation that considers:
    - Same month last year as baseline
    - Year-over-year growth rate
    - Monthly seasonality patterns
    - Weekly distribution within the month

    Response includes:
    - amount: Current goal (custom or smart-calculated)
    - isCustom: Whether goal was manually set
    - suggestedAmount: Smart-calculated suggestion
    - lastYearRevenue: Same month last year revenue
    - growthRate: Applied growth rate
    - seasonalityIndex: Month's seasonality factor
    - weeklyBreakdown: How goal distributes across weeks (for monthly)
    - confidence: Calculation confidence (high/medium/low)
    - calculationMethod: How the goal was calculated
    """
    try:
        sales_type = validate_sales_type(sales_type)
    except ValidationError as e:
        raise HTTPException(status_code=400, detail=str(e))

    store = await get_store()
    return await store.get_smart_goals(sales_type)


@router.get("/goals/seasonality")
@limiter.limit("30/minute")
async def get_seasonality_data(
    request: Request,
    sales_type: Optional[str] = Query("retail", description="Sales type: retail, b2b, or all")
):
    """
    Get monthly seasonality indices.

    Returns how each month performs relative to the annual average.
    A seasonality index of 1.2 means the month is typically 20% above average.
    """
    try:
        sales_type = validate_sales_type(sales_type)
    except ValidationError as e:
        raise HTTPException(status_code=400, detail=str(e))

    store = await get_store()
    return await store.calculate_seasonality_indices(sales_type)


@router.get("/goals/growth")
@limiter.limit("30/minute")
async def get_growth_data(
    request: Request,
    sales_type: Optional[str] = Query("retail", description="Sales type: retail, b2b, or all")
):
    """
    Get year-over-year growth metrics.

    Returns:
    - overall_yoy: Average YoY growth rate
    - monthly_yoy: YoY growth for each month
    - yearly_data: Revenue totals by year
    """
    try:
        sales_type = validate_sales_type(sales_type)
    except ValidationError as e:
        raise HTTPException(status_code=400, detail=str(e))

    store = await get_store()
    return await store.calculate_yoy_growth(sales_type)


@router.get("/goals/weekly-patterns")
@limiter.limit("30/minute")
async def get_weekly_patterns(
    request: Request,
    sales_type: Optional[str] = Query("retail", description="Sales type: retail, b2b, or all")
):
    """
    Get weekly distribution patterns within months.

    Returns how revenue typically distributes across weeks 1-5 of each month.
    For example, week 1 might typically have 25% of monthly revenue.
    """
    try:
        sales_type = validate_sales_type(sales_type)
    except ValidationError as e:
        raise HTTPException(status_code=400, detail=str(e))

    store = await get_store()
    return await store.calculate_weekly_patterns(sales_type)


@router.post("/goals/recalculate")
@limiter.limit("5/minute")
async def recalculate_seasonality(
    request: Request,
    sales_type: Optional[str] = Query("retail", description="Sales type: retail, b2b, or all")
):
    """
    Force recalculation of seasonality indices and growth metrics.

    Use this after significant data changes or to update calculations
    with the latest data.
    """
    try:
        sales_type = validate_sales_type(sales_type)
    except ValidationError as e:
        raise HTTPException(status_code=400, detail=str(e))

    store = await get_store()

    # Recalculate all indices
    seasonality = await store.calculate_seasonality_indices(sales_type)
    growth = await store.calculate_yoy_growth(sales_type)
    weekly = await store.calculate_weekly_patterns(sales_type)

    return {
        "status": "success",
        "message": "Seasonality indices and growth metrics recalculated",
        "summary": {
            "monthsCalculated": len(seasonality),
            "overallYoY": growth.get("overall_yoy", 0),
            "yearsAnalyzed": len(growth.get("yearly_data", []))
        }
    }


@router.get("/goals/forecast")
@limiter.limit("30/minute")
async def get_goal_forecast(
    request: Request,
    year: int = Query(..., ge=2020, le=2030, description="Target year"),
    month: int = Query(..., ge=1, le=12, description="Target month (1-12)"),
    sales_type: Optional[str] = Query("retail", description="Sales type: retail, b2b, or all"),
    recalculate: bool = Query(False, description="Force recalculation of indices")
):
    """
    Generate smart goals for a specific future month.

    Uses historical data, seasonality, and growth patterns to predict
    optimal revenue goals for the target period.
    """
    try:
        sales_type = validate_sales_type(sales_type)
    except ValidationError as e:
        raise HTTPException(status_code=400, detail=str(e))

    store = await get_store()
    return await store.generate_smart_goals(year, month, sales_type, recalculate)


# ─── Stock Endpoints ──────────────────────────────────────────────────────────


@router.get("/stocks/summary")
@limiter.limit("30/minute")
async def get_stock_summary(
    request: Request,
    limit: int = Query(20, ge=5, le=50, description="Number of items to return in lists")
):
    """
    Get stock summary for dashboard display.
    
    Returns:
    - summary: Overall stock statistics (totals, counts)
    - topByQuantity: Top items by stock quantity
    - lowStock: Items with low stock (1-5 units)
    - outOfStock: Items currently out of stock
    """
    store = await get_store()
    return await store.get_stock_summary(limit)



@router.get("/stocks/average")
@limiter.limit("30/minute")
async def get_average_inventory(
    request: Request,
    days: int = Query(30, ge=7, le=365, description="Number of days to calculate average over")
):
    """
    Get average inventory using formula: (Beginning + Ending) / 2
    
    Returns beginning and ending inventory values for the period,
    calculated average, and daily averages if multiple data points exist.
    """
    store = await get_store()
    return await store.get_average_inventory(days)


@router.get("/stocks/trend")
@limiter.limit("30/minute")
async def get_inventory_trend(
    request: Request,
    days: int = Query(90, ge=7, le=365, description="Number of days to look back"),
    granularity: str = Query("daily", description="Data granularity: daily or monthly")
):
    """
    Get inventory trend over time for charting stock changes.

    Shows how stock value and quantity change daily or monthly.
    Useful for tracking inventory health and identifying trends.
    """
    import logging
    logger = logging.getLogger(__name__)

    # Validate granularity
    if granularity not in ("daily", "monthly"):
        granularity = "daily"

    try:
        store = await get_store()
        return await store.get_inventory_trend(days, granularity)
    except Exception as e:
        logger.error(f"Inventory trend error: {e}", exc_info=True)
        raise


# ─── V2 Endpoints (View-based) ────────────────────────────────────────────────

@router.get("/stocks/analysis")
@limiter.limit("30/minute")
async def get_inventory_analysis(request: Request):
    """
    Get comprehensive inventory analysis using Layer 3 views.

    Uses pre-computed sku_inventory_status table for fast queries.
    Returns summary by status, aging distribution, and category thresholds.
    """
    store = await get_store()
    summary = await store.get_inventory_summary_v2()
    items = await store.get_dead_stock_items_v2(limit=100)

    return {
        **summary,
        "items": items,
        "methodology": {
            "description": "Dynamic thresholds per category using P75, minimum 90 days, maximum 365 days",
            "minimumThreshold": 90,
            "defaultThreshold": 180,
            "atRiskMultiplier": 0.7,
        },
    }


@router.get("/stocks/actions")
@limiter.limit("30/minute")
async def get_stock_actions(request: Request):
    """
    Get recommended actions for dead stock items.

    Uses Layer 4 view to provide actionable recommendations:
    - Return to supplier
    - Deep discount
    - Bundle with bestsellers
    - Promote / Feature
    """
    store = await get_store()
    return await store.get_recommended_actions(limit=50)


@router.get("/stocks/alerts")
@limiter.limit("30/minute")
async def get_stock_alerts(request: Request):
    """
    Get low stock alerts for items that need restocking.

    Returns items with:
    - OUT_OF_STOCK: 0 units
    - CRITICAL: 1-3 units
    - LOW: 4-10 units

    Only includes healthy/fast-moving items (not dead stock).
    """
    store = await get_store()
    return await store.get_restock_alerts(limit=50)
