"""Revenue, summary, returns, sales-by-source, products, brands, categories endpoints."""
import logging
from datetime import date as _date, datetime as _datetime, timedelta as _timedelta

from fastapi import APIRouter, Query, Request, HTTPException
from typing import Optional, List

from web.services import dashboard_service
from web.schemas import (
    SummaryStatsResponse,
    RevenueTrendResponse,
    SalesBySourceResponse,
    TopProductsResponse,
    BrandAnalyticsResponse,
    CategoryResponse,
    BrandResponse,
)
from ._deps import (
    limiter, get_store,
    validate_period, validate_source_id, validate_category_id,
    validate_brand_name, validate_limit, validate_sales_type,
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
    sales_type: Optional[str] = Query("retail", description="Sales type: retail, b2b, or all"),
    compare_type: Optional[str] = Query("previous_period", description="Comparison type: previous_period, year_ago, month_ago"),
    include_forecast: Optional[bool] = Query(False, description="Include ML forecast for remaining month days"),
):
    """Get revenue trend data for line chart."""
    try:
        validate_period(period)
        validate_source_id(source_id)
        validate_category_id(category_id)
        brand = validate_brand_name(brand)
        sales_type = validate_sales_type(sales_type)
        if compare_type not in ("previous_period", "year_ago", "month_ago"):
            compare_type = "previous_period"
    except ValidationError as e:
        raise HTTPException(status_code=400, detail=str(e))

    start, end = dashboard_service.parse_period(period, start_date, end_date)
    result = await dashboard_service.get_revenue_trend(
        start, end, category_id=category_id, brand=brand, source_id=source_id,
        sales_type=sales_type, compare_type=compare_type,
    )

    # Attach forecast data when requested (no filters applied)
    today = _date.today()
    has_future = end > today.isoformat()
    is_current_period = period in ("month", "week")
    allow_forecast = is_current_period or has_future
    if include_forecast and allow_forecast and not category_id and not brand and not source_id:
        try:
            if period == "month":
                forecast = await dashboard_service.get_forecast_data(sales_type)
                if forecast and forecast.get("daily_predictions"):
                    month_end = forecast.get("month_end", "")
                    forecast["daily_predictions"] = [
                        p for p in forecast["daily_predictions"]
                        if p["date"] <= month_end
                    ]
                    forecast["predicted_remaining"] = sum(
                        p["predicted_revenue"] for p in forecast["daily_predictions"]
                    )
                    forecast["predicted_total"] = forecast.get("actual_to_date", 0) + forecast["predicted_remaining"]
            elif period == "week":
                from core.prediction_service import get_prediction_service
                service = get_prediction_service()
                end_of_week = today + _timedelta(days=6 - today.weekday())
                forecast = await service.predict_range(today, end_of_week, sales_type)
            else:
                from core.prediction_service import get_prediction_service
                service = get_prediction_service()
                forecast = await service.predict_range(
                    _datetime.strptime(start, "%Y-%m-%d").date(),
                    _datetime.strptime(end, "%Y-%m-%d").date(),
                    sales_type,
                )
            if forecast and forecast.get("daily_predictions"):
                result["forecast"] = forecast

                preds = forecast.get("daily_predictions", [])
                comparison = result.get("comparison")
                if preds and comparison:
                    existing_labels = set(result.get("labels", []))
                    extra_dates = []
                    for p in preds:
                        parts = p["date"].split("-")
                        label = f"{parts[2]}.{parts[1]}"
                        if label not in existing_labels:
                            extra_dates.append(_datetime.strptime(p["date"], "%Y-%m-%d").date())

                    if extra_dates:
                        comp_type = comparison.get("period", {}).get("type", compare_type)
                        extra_comp = await dashboard_service.get_comparison_for_dates(
                            extra_dates, comp_type, sales_type=sales_type,
                        )
                        for d in extra_dates:
                            label = d.strftime("%d.%m")
                            result["labels"].append(label)
                            result["revenue"].append(0)
                            result["orders"].append(0)
                            comparison["revenue"].append(round(extra_comp.get(d, 0), 2))
        except Exception as e:
            logger.warning(f"Forecast unavailable: {e}")

    return result


@router.get("/revenue/forecast")
@limiter.limit("30/minute")
async def get_revenue_forecast(
    request: Request,
    sales_type: Optional[str] = Query("retail", description="Sales type: retail or b2b"),
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
):
    """Manually trigger revenue prediction model training."""
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
):
    """Run hyperparameter grid search for LightGBM parameters."""
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
):
    """Run walk-forward cross-validation to evaluate the model."""
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
    sales_type: Optional[str] = Query("retail"),
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
    return await dashboard_service.get_sales_by_source(
        start, end, category_id, brand=brand, source_id=source_id, sales_type=sales_type,
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
    limit: int = Query(10, description="Number of products to return"),
    sales_type: Optional[str] = Query("retail"),
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
    return await dashboard_service.get_top_products(
        start, end, source_id, limit, category_id, brand=brand, sales_type=sales_type,
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
    sales_type: Optional[str] = Query("retail"),
):
    """Get summary statistics for dashboard cards."""
    from core.cache import cache

    try:
        validate_period(period)
        validate_source_id(source_id)
        validate_category_id(category_id)
        brand = validate_brand_name(brand)
        sales_type = validate_sales_type(sales_type)
    except ValidationError as e:
        raise HTTPException(status_code=400, detail=str(e))

    start, end = dashboard_service.parse_period(period, start_date, end_date)
    cache_key = f"summary:{start}:{end}:{source_id or ''}:{category_id or ''}:{brand or ''}:{sales_type}"

    try:
        cached = await cache.get(cache_key)
        if cached is not None:
            return cached
    except Exception as e:
        logger.warning(f"Cache get failed for {cache_key}: {e}")

    result = await dashboard_service.get_summary_stats(
        start, end, category_id, brand=brand, source_id=source_id, sales_type=sales_type,
    )

    try:
        await cache.set(cache_key, result, ttl=60)
    except Exception as e:
        logger.warning(f"Cache set failed for {cache_key}: {e}")

    return result


@router.get("/returns")
@limiter.limit("30/minute")
async def get_returns(
    request: Request,
    period: Optional[str] = Query(None),
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
    sales_type: Optional[str] = Query("retail"),
    limit: int = Query(50, ge=1, le=100),
):
    """Get list of return orders for a date range."""
    try:
        validate_period(period)
        sales_type = validate_sales_type(sales_type)
    except ValidationError as e:
        raise HTTPException(status_code=400, detail=str(e))

    start, end = dashboard_service.parse_period(period, start_date, end_date)

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
    sales_type: Optional[str] = Query("retail"),
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
    return await dashboard_service.get_product_performance(
        start, end, brand=brand, source_id=source_id, sales_type=sales_type,
    )


@router.get("/brands/analytics")
@limiter.limit("30/minute")
async def get_brand_analytics(
    request: Request,
    period: Optional[str] = Query(None),
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
    sales_type: Optional[str] = Query("retail"),
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
    period: Optional[str] = Query(None),
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
    source_id: Optional[int] = Query(None),
    brand: Optional[str] = Query(None),
    sales_type: Optional[str] = Query("retail"),
):
    """Get sales breakdown by subcategories for a given parent category."""
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
        sales_type=sales_type,
    )
