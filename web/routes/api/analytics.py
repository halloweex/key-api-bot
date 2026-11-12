"""Revenue, summary, returns, sales-by-source, products, brands, categories endpoints."""
import logging
from datetime import date as _date, datetime as _datetime
from zoneinfo import ZoneInfo

_KYIV_TZ = ZoneInfo("Europe/Kyiv")

from fastapi import APIRouter, Query, Request, HTTPException, Depends
from typing import Optional, List

from core.filters import has_comparison
from web.routes.auth import require_admin, require_any_permission, require_permission
from web.services import dashboard_service
from web.schemas import CategoryResponse, BrandResponse
from ._deps import (
    limiter, get_store,
    validate_period, validate_source_id, validate_category_id,
    validate_brand_name, validate_limit, validate_sales_type,
    validate_promocode,
    ValidationError,
)

router = APIRouter()
logger = logging.getLogger(__name__)


# ─── Lightweight Endpoints ─────────────────────────────────────────────────────

@router.get("/categories", response_model=List[CategoryResponse])
@limiter.limit("60/minute")
async def get_categories(request: Request):
    """Get list of root categories for filter dropdown."""
    store = await get_store()
    return await store.get_categories()


@router.get("/categories/{parent_id}/children", response_model=List[CategoryResponse])
@limiter.limit("60/minute")
async def get_child_categories(request: Request, parent_id: int):
    """Get child categories for a parent category."""
    try:
        validate_category_id(parent_id, allow_none=False)
    except ValidationError as e:
        raise HTTPException(status_code=400, detail=str(e))
    store = await get_store()
    return await store.get_child_categories(parent_id)


@router.get("/brands", response_model=List[BrandResponse])
@limiter.limit("60/minute")
async def get_brands(request: Request):
    """Get list of brands for filter dropdown."""
    store = await get_store()
    return await store.get_brands()


@router.get("/promocodes")
@limiter.limit("60/minute")
async def get_promocodes(request: Request):
    """Get list of unique promocodes for filter dropdown."""
    store = await get_store()
    return await store.get_promocodes()


# ─── Revenue ───────────────────────────────────────────────────────────────────

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
    promocode: Optional[str] = Query(None, description="Filter by promocode"),
    sales_type: Optional[str] = Query("retail", description="Sales type: retail, b2b, or all"),
    compare_type: Optional[str] = Query("previous_period", description="Comparison type: previous_period, year_ago, month_ago"),
    include_forecast: Optional[bool] = Query(False, description="Include ML forecast for remaining month days"),
    granularity: str = Query("daily", description="daily, or monthly: one point a calendar month, no comparison"),
    _gate=Depends(require_permission("dashboard")),
):
    """Get revenue trend data for line chart."""
    try:
        validate_period(period)
        if granularity not in ("daily", "monthly"):
            raise ValidationError("granularity", "Must be one of: daily, monthly", granularity)
        validate_source_id(source_id)
        validate_category_id(category_id)
        brand = validate_brand_name(brand)
        promocode = validate_promocode(promocode)
        sales_type = validate_sales_type(sales_type)
        if compare_type not in ("previous_period", "year_ago", "month_ago"):
            compare_type = "previous_period"
        start, end = dashboard_service.parse_period(period, start_date, end_date)
    except ValidationError as e:
        raise HTTPException(status_code=400, detail=str(e))

    first, last = _date.fromisoformat(start), _date.fromisoformat(end)
    today = _datetime.now(_KYIV_TZ).date()
    # This month and this week end today, and the chart draws them to their
    # last day: the comparison is the whole period's, day by day, and the
    # growth badge counts the days through today on both sides (owner's
    # decision, 2026-11-12). The whole history has nothing before it to be
    # compared with (`has_comparison`).
    period_end = dashboard_service.last_day_of_period(period, first, last)
    result = await dashboard_service.get_revenue_trend(
        start, end, category_id=category_id, brand=brand, source_id=source_id,
        sales_type=sales_type, compare_type=compare_type, promocode=promocode,
        include_comparison=has_comparison(period), granularity=granularity,
        period_end=period_end, through=min(last, today),
    )

    # Attach forecast data when requested (no filters applied)
    has_future = last > today
    is_current_period = period in ("month", "week")
    allow_forecast = (is_current_period or has_future) and granularity == "daily"
    forecast_days: List[_date] = []
    if include_forecast and allow_forecast and not category_id and not brand and not promocode and not source_id:
        try:
            # The stored forecast for the period's days from today on, read
            # one way for the month, the week and a range: the model never
            # runs on a request, and no actuals are read for it — the badge
            # is stacked on the series below. The month's went through
            # `get_forecast_data` until 2026-11-12, which read the month's
            # actuals only for them to be replaced here.
            from core.prediction_service import get_prediction_service
            forecast = await get_prediction_service().get_range_forecast(
                first, period_end, sales_type)
            if forecast and forecast.get("daily_predictions"):
                # The badge is what the bars add up to, from the series the
                # chart draws: taken before the future days are appended.
                forecast.update(dashboard_service.stack_forecast_on_actuals(
                    first, result["revenue"], forecast["daily_predictions"]))
                result["forecast"] = forecast
                forecast_days = [
                    day for day in (_date.fromisoformat(p["date"])
                                    for p in forecast["daily_predictions"])
                    if day > last]
        except Exception as e:
            logger.warning(f"Forecast unavailable: {e}")

    dashboard_service.fit_trend(result, first, forecast_days)
    return result


@router.get("/revenue/forecast")
@limiter.limit("30/minute")
async def get_revenue_forecast(
    request: Request,
    sales_type: Optional[str] = Query("retail", description="Sales type: retail or b2b"),
    _gate=Depends(require_permission("dashboard")),
):
    """Get ML revenue forecast for the current month."""
    try:
        sales_type = validate_sales_type(sales_type)
    except ValidationError as e:
        raise HTTPException(status_code=400, detail=str(e))

    forecast = await dashboard_service.get_forecast_data(sales_type)
    if not forecast:
        return {"status": "unavailable", "message": "Forecast not available yet"}
    return forecast


@router.post("/revenue/forecast/train")
@limiter.limit("5/hour")
async def train_revenue_forecast(
    request: Request,
    sales_type: Optional[str] = Query("retail", description="Sales type: retail or b2b"),
    admin: dict = Depends(require_admin),
):
    """Manually trigger revenue prediction model training. Requires admin."""
    try:
        sales_type = validate_sales_type(sales_type)
    except ValidationError as e:
        raise HTTPException(status_code=400, detail=str(e))

    from core.prediction_service import get_prediction_service
    service = get_prediction_service()
    return await service.train(sales_type)


@router.post("/revenue/forecast/tune")
@limiter.limit("2/hour")
async def tune_revenue_forecast(
    request: Request,
    sales_type: Optional[str] = Query("retail", description="Sales type: retail or b2b"),
    admin: dict = Depends(require_admin),
):
    """Run hyperparameter grid search for LightGBM parameters. Requires admin."""
    try:
        sales_type = validate_sales_type(sales_type)
    except ValidationError as e:
        raise HTTPException(status_code=400, detail=str(e))

    from core.prediction_service import get_prediction_service
    service = get_prediction_service()
    return await service.tune(sales_type)


@router.get("/revenue/forecast/evaluate")
@limiter.limit("5/hour")
async def evaluate_revenue_forecast(
    request: Request,
    sales_type: Optional[str] = Query("retail", description="Sales type: retail or b2b"),
    admin: dict = Depends(require_admin),
):
    """Run walk-forward cross-validation to evaluate the model. Requires admin.

    It holds the single ML thread for as long as train and tune do, and was
    gated on the dashboard tab until 2026-11-04, so any viewer could start
    one five times an hour. The page never calls it.
    """
    try:
        sales_type = validate_sales_type(sales_type)
    except ValidationError as e:
        raise HTTPException(status_code=400, detail=str(e))

    from core.prediction_service import get_prediction_service
    service = get_prediction_service()
    return await service.evaluate(sales_type)


# ─── Sales & Products ──────────────────────────────────────────────────────────

@router.get("/sales/by-source")
@limiter.limit("30/minute")
async def get_sales_by_source(
    request: Request,
    period: Optional[str] = Query(None),
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
    source_id: Optional[int] = Query(None),
    category_id: Optional[int] = Query(None),
    brand: Optional[str] = Query(None),
    promocode: Optional[str] = Query(None),
    sales_type: Optional[str] = Query("retail"),
    _gate=Depends(require_permission("dashboard")),
):
    """Get sales data by source for bar/pie chart."""
    try:
        validate_period(period)
        validate_source_id(source_id)
        validate_category_id(category_id)
        brand = validate_brand_name(brand)
        promocode = validate_promocode(promocode)
        sales_type = validate_sales_type(sales_type)
        start, end = dashboard_service.parse_period(period, start_date, end_date)
    except ValidationError as e:
        raise HTTPException(status_code=400, detail=str(e))

    return await dashboard_service.get_sales_by_source(
        start, end, category_id, brand=brand, source_id=source_id, sales_type=sales_type,
        promocode=promocode,
    )


@router.get("/products/top")
@limiter.limit("30/minute")
async def get_top_products(
    request: Request,
    period: Optional[str] = Query(None),
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
    source_id: Optional[int] = Query(None),
    category_id: Optional[int] = Query(None),
    brand: Optional[str] = Query(None),
    promocode: Optional[str] = Query(None),
    limit: int = Query(10, description="Number of products to return"),
    sales_type: Optional[str] = Query("retail"),
    _gate=Depends(require_permission("dashboard")),
):
    """Get top products for horizontal bar chart."""
    try:
        validate_period(period)
        validate_source_id(source_id)
        validate_category_id(category_id)
        brand = validate_brand_name(brand)
        promocode = validate_promocode(promocode)
        limit = validate_limit(limit, max_value=50)
        sales_type = validate_sales_type(sales_type)
        start, end = dashboard_service.parse_period(period, start_date, end_date)
    except ValidationError as e:
        raise HTTPException(status_code=400, detail=str(e))

    return await dashboard_service.get_top_products(
        start, end, source_id, limit, category_id, brand=brand, sales_type=sales_type,
        promocode=promocode,
    )


@router.get("/summary")
@limiter.limit("30/minute")
async def get_summary(
    request: Request,
    period: Optional[str] = Query(None),
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
    source_id: Optional[int] = Query(None),
    category_id: Optional[int] = Query(None),
    brand: Optional[str] = Query(None),
    promocode: Optional[str] = Query(None),
    sales_type: Optional[str] = Query("retail"),
    _gate=Depends(require_any_permission(["dashboard", "marketing", "traffic"])),
):
    """Get summary statistics for dashboard cards."""
    try:
        validate_period(period)
        validate_source_id(source_id)
        validate_category_id(category_id)
        brand = validate_brand_name(brand)
        promocode = validate_promocode(promocode)
        sales_type = validate_sales_type(sales_type)
        start, end = dashboard_service.parse_period(period, start_date, end_date)
    except ValidationError as e:
        raise HTTPException(status_code=400, detail=str(e))

    return await dashboard_service.get_summary_stats(
        start, end, category_id, brand=brand, source_id=source_id, sales_type=sales_type,
        promocode=promocode,
    )


@router.get("/returns")
@limiter.limit("30/minute")
async def get_returns(
    request: Request,
    period: Optional[str] = Query(None),
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
    sales_type: Optional[str] = Query("retail"),
    limit: int = Query(50, ge=1, le=100),
    _gate=Depends(require_permission("dashboard")),
):
    """Get list of return orders for a date range."""
    try:
        validate_period(period)
        sales_type = validate_sales_type(sales_type)
        start, end = dashboard_service.parse_period(period, start_date, end_date)
    except ValidationError as e:
        raise HTTPException(status_code=400, detail=str(e))

    store = await get_store()
    start_dt = _datetime.strptime(start, "%Y-%m-%d").date()
    end_dt = _datetime.strptime(end, "%Y-%m-%d").date()
    returns = await store.get_return_orders(start_dt, end_dt, sales_type, limit)

    return {"returns": returns, "count": len(returns), "startDate": start, "endDate": end}


@router.get("/products/performance")
@limiter.limit("30/minute")
async def get_product_performance(
    request: Request,
    period: Optional[str] = Query(None),
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
    source_id: Optional[int] = Query(None),
    brand: Optional[str] = Query(None),
    promocode: Optional[str] = Query(None),
    sales_type: Optional[str] = Query("retail"),
    _gate=Depends(require_permission("dashboard")),
):
    """Get product performance: top by revenue, category breakdown."""
    try:
        validate_period(period)
        validate_source_id(source_id)
        brand = validate_brand_name(brand)
        promocode = validate_promocode(promocode)
        sales_type = validate_sales_type(sales_type)
        start, end = dashboard_service.parse_period(period, start_date, end_date)
    except ValidationError as e:
        raise HTTPException(status_code=400, detail=str(e))

    return await dashboard_service.get_product_performance(
        start, end, brand=brand, source_id=source_id, sales_type=sales_type,
        promocode=promocode,
    )


@router.get("/brands/analytics")
@limiter.limit("30/minute")
async def get_brand_analytics(
    request: Request,
    period: Optional[str] = Query(None),
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
    sales_type: Optional[str] = Query("retail"),
    _gate=Depends(require_any_permission(["dashboard", "marketing"])),
):
    """Get brand analytics: top brands by revenue and quantity."""
    try:
        validate_period(period)
        sales_type = validate_sales_type(sales_type)
        start, end = dashboard_service.parse_period(period, start_date, end_date)
    except ValidationError as e:
        raise HTTPException(status_code=400, detail=str(e))

    return await dashboard_service.get_brand_analytics(start, end, sales_type=sales_type)


@router.get("/promocodes/analytics")
@limiter.limit("30/minute")
async def get_promocode_analytics(
    request: Request,
    period: Optional[str] = Query(None),
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
    sales_type: Optional[str] = Query("retail"),
    _gate=Depends(require_any_permission(["dashboard", "marketing"])),
):
    """Get promocode performance overview."""
    try:
        validate_period(period)
        sales_type = validate_sales_type(sales_type)
        start, end = dashboard_service.parse_period(period, start_date, end_date)
    except ValidationError as e:
        raise HTTPException(status_code=400, detail=str(e))

    return await dashboard_service.get_promocode_analytics(start, end, sales_type=sales_type)


@router.get("/categories/breakdown")
@limiter.limit("30/minute")
async def get_subcategory_breakdown(
    request: Request,
    parent_category: str = Query(..., description="Parent category name to drill down into"),
    period: Optional[str] = Query(None),
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
    source_id: Optional[int] = Query(None),
    brand: Optional[str] = Query(None),
    promocode: Optional[str] = Query(None),
    sales_type: Optional[str] = Query("retail"),
    _gate=Depends(require_permission("dashboard")),
):
    """Get sales breakdown by subcategories for a given parent category."""
    try:
        validate_period(period)
        validate_source_id(source_id)
        brand = validate_brand_name(brand)
        promocode = validate_promocode(promocode)
        sales_type = validate_sales_type(sales_type)
        start, end = dashboard_service.parse_period(period, start_date, end_date)
    except ValidationError as e:
        raise HTTPException(status_code=400, detail=str(e))

    return await dashboard_service.get_subcategory_breakdown(
        start, end,
        parent_category=parent_category,
        source_id=source_id,
        brand=brand,
        sales_type=sales_type,
        promocode=promocode,
    )
