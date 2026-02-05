"""
API routes for chart data.
Fully async implementation.

All endpoints are rate-limited to prevent abuse:
- Lightweight endpoints (categories, brands, health): 60 requests/minute
- Data-heavy endpoints (revenue, summary, etc.): 30 requests/minute
"""
import time
from fastapi import APIRouter, Query, Request, HTTPException, Depends
from typing import Optional, List

from slowapi import Limiter
from slowapi.util import get_remote_address

from web.services import dashboard_service
from web.routes.auth import require_admin
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
from core.observability import get_correlation_id, metrics, Timer
from web.schemas import (
    HealthResponse,
    SummaryStatsResponse,
    RevenueTrendResponse,
    SalesBySourceResponse,
    TopProductsResponse,
    BrandAnalyticsResponse,
    CustomerInsightsResponse,
    CategoryResponse,
    BrandResponse,
    MetricsResponse,
    JobsResponse,
)

router = APIRouter(tags=["api"])

# Track startup time for uptime calculation
_start_time = time.time()

# Rate limiter (uses app.state.limiter from main.py)
limiter = Limiter(key_func=get_remote_address)


# ─── Health Check ────────────────────────────────────────────────────────────

@router.get("/health", response_model=HealthResponse)
@limiter.limit("60/minute")
async def health_check(request: Request):
    """Health check endpoint for Docker/load balancer monitoring."""
    uptime_seconds = int(time.time() - _start_time)

    # Get DuckDB stats with latency measurement
    db_latency_ms = None
    try:
        with Timer("health_check_db") as timer:
            store = await get_store()
            duckdb_stats = await store.get_stats()
        duckdb_status = "connected"
        db_latency_ms = round(timer.elapsed_ms, 2)
    except Exception as e:
        duckdb_stats = None
        duckdb_status = f"error: {e}"

    return {
        "status": "healthy" if duckdb_stats else "degraded",
        "version": VERSION,
        "uptime_seconds": uptime_seconds,
        "correlation_id": get_correlation_id(),
        "duckdb": {
            "status": duckdb_status,
            "latency_ms": db_latency_ms,
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
async def trigger_resync(
    request: Request,
    days: int = 365,
    admin: dict = Depends(require_admin)
):
    """
    Force a complete resync of orders from KeyCRM API.

    Use this when data discrepancies are detected between dashboard and KeyCRM.
    This clears all order data and performs a fresh sync.

    WARNING: This operation can take several minutes for large datasets.
    Requires admin authentication.
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


@router.post("/duckdb/refresh-statuses")
@limiter.limit("10/hour")
async def refresh_order_statuses(
    request: Request,
    days: int = Query(30, ge=1, le=90, description="Days to look back for status changes"),
    background: bool = Query(True, description="Run in background (recommended)"),
):
    """
    Refresh order statuses from KeyCRM API.

    KeyCRM does NOT update `updated_at` when order status changes, so incremental
    sync misses status updates (like orders marked as returns). This endpoint
    re-fetches recent orders to catch these changes.

    Use this when you notice discrepancies in order counts or revenue.
    """
    import asyncio
    from core.sync_service import get_sync_service

    async def run_refresh():
        sync_service = await get_sync_service()
        return await sync_service.refresh_order_statuses(days_back=days)

    if background:
        # Run in background task
        asyncio.create_task(run_refresh())
        return {
            "status": "started",
            "message": f"Status refresh started in background - checking last {days} days",
            "note": "Check /api/jobs for progress or wait ~60 seconds and verify data"
        }

    # Synchronous mode (may timeout for large datasets)
    try:
        stats = await run_refresh()
        return {
            "status": "success",
            "message": f"Status refresh complete - checked last {days} days",
            "stats": stats
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Status refresh failed: {str(e)}")


@router.get("/warehouse/status")
@limiter.limit("60/minute")
async def get_warehouse_status(request: Request):
    """Get warehouse layer (Silver/Gold) status and last refresh info."""
    store = await get_store()
    return await store.get_warehouse_status()


@router.post("/warehouse/refresh")
@limiter.limit("5/minute")
async def refresh_warehouse(
    request: Request,
    admin: dict = Depends(require_admin)
):
    """Manually trigger warehouse layer refresh (Silver → Gold).

    Requires admin authentication.
    """
    store = await get_store()
    result = await store.refresh_warehouse_layers(trigger="manual")
    return result


@router.post("/duckdb/sync-buyers")
@limiter.limit("120/minute")
async def sync_buyers(
    request: Request,
    limit: int = Query(100, ge=1, le=500, description="Maximum buyers to sync"),
):
    """
    Manually sync missing buyers from KeyCRM.

    Fetches buyer details for orders that have buyer_id but no buyer record.
    """
    from core.sync_service import get_sync_service

    try:
        sync_service = await get_sync_service()
        count = await sync_service.sync_missing_buyers(limit=limit)
        return {
            "status": "success",
            "message": f"Synced {count} buyers from KeyCRM",
            "buyers_synced": count
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Buyer sync failed: {str(e)}")


@router.get("/metrics", response_model=MetricsResponse)
@limiter.limit("60/minute")
async def get_metrics(request: Request):
    """
    Get application metrics.

    Returns request counts, error counts, and timing statistics.
    """
    return {
        "uptime_seconds": int(time.time() - _start_time),
        "correlation_id": get_correlation_id(),
        **metrics.get_stats()
    }


@router.get("/buyers/stats")
@limiter.limit("60/minute")
async def get_buyer_stats(request: Request):
    """Get buyer sync statistics."""
    store = await get_store()
    async with store.connection() as conn:
        orders_buyers = conn.execute("""
            SELECT COUNT(DISTINCT buyer_id) FROM orders WHERE buyer_id IS NOT NULL
        """).fetchone()[0]

        silver_buyers = conn.execute("""
            SELECT COUNT(DISTINCT buyer_id) FROM silver_orders WHERE buyer_id IS NOT NULL
        """).fetchone()[0]

        synced = conn.execute("SELECT COUNT(*) FROM buyers").fetchone()[0]

        missing = conn.execute("""
            SELECT COUNT(DISTINCT s.buyer_id)
            FROM silver_orders s
            LEFT JOIN buyers b ON s.buyer_id = b.id
            WHERE s.buyer_id IS NOT NULL AND b.id IS NULL
        """).fetchone()[0]

        return {
            "unique_in_orders": orders_buyers,
            "unique_in_silver_orders": silver_buyers,
            "synced_to_buyers_table": synced,
            "missing": missing
        }


@router.get("/cache/stats")
@limiter.limit("60/minute")
async def get_cache_stats(request: Request):
    """
    Get Redis cache statistics.

    Returns cache hits, misses, hit rate, and connection status.
    """
    from core.cache import cache
    return cache.get_stats()


@router.post("/cache/invalidate")
@limiter.limit("10/minute")
async def invalidate_cache(
    request: Request,
    pattern: str = Query(..., description="Cache key pattern to invalidate (e.g., 'summary:*')"),
    admin: dict = Depends(require_admin)
):
    """
    Manually invalidate cache by pattern.

    Requires admin authentication.
    """
    from core.cache import cache

    if not cache.is_connected:
        raise HTTPException(status_code=503, detail="Cache not connected")

    deleted = await cache.invalidate_pattern(pattern)
    return {
        "status": "success",
        "pattern": pattern,
        "keys_deleted": deleted,
    }


@router.get("/jobs", response_model=JobsResponse)
@limiter.limit("60/minute")
async def get_jobs(request: Request):
    """
    Get background job scheduler status.

    Returns list of registered jobs with their schedules, last run times,
    and execution history.
    """
    from core.scheduler import get_scheduler

    scheduler = get_scheduler()
    if scheduler is None:
        return {
            "status": "not_running",
            "jobs": [],
            "history": []
        }

    # Collect recent history from all jobs
    all_history = []
    jobs = scheduler.get_jobs()
    job_names = {j["id"]: j["name"] for j in jobs}

    for job in jobs:
        job_history = scheduler.get_job_history(job["id"], limit=5)
        for h in job_history:
            all_history.append({
                "job_id": job["id"],
                "job_name": job_names.get(job["id"], job["id"]),
                "started_at": h.get("started_at") or "",
                "completed_at": h.get("finished_at"),
                "duration_ms": h.get("duration_ms"),
                "status": h.get("status", "unknown"),
                "error": h.get("error"),
                "result": None,  # Not storing result in history
            })
    # Sort by started_at descending
    all_history.sort(key=lambda x: x.get("started_at") or "", reverse=True)

    # Get adaptive sync stats
    from core.sync_service import get_sync_service
    try:
        sync_service = await get_sync_service()
        sync_stats = sync_service.get_sync_stats()
    except Exception:
        sync_stats = None

    return {
        "status": "running",
        "jobs": jobs,
        "history": all_history[:20],
        "adaptive_sync": sync_stats,
    }


@router.get("/sync/stats")
@limiter.limit("60/minute")
async def get_sync_stats(request: Request):
    """
    Get adaptive sync statistics.

    Shows current backoff state, consecutive empty syncs, and effective interval.
    Useful for monitoring sync efficiency.
    """
    from core.sync_service import get_sync_service

    try:
        sync_service = await get_sync_service()
        stats = sync_service.get_sync_stats()
        return {
            "status": "ok",
            **stats,
            "config": {
                "base_interval_seconds": sync_service.BACKOFF_BASE_SECONDS,
                "max_interval_seconds": sync_service.BACKOFF_MAX_SECONDS,
                "backoff_multiplier": sync_service.BACKOFF_MULTIPLIER,
                "off_hours": f"{sync_service.OFF_HOURS_START}:00 - {sync_service.OFF_HOURS_END}:00",
                "off_hours_interval_seconds": sync_service.OFF_HOURS_INTERVAL,
            }
        }
    except Exception as e:
        return {"status": "error", "error": str(e)}


@router.get("/events")
@limiter.limit("60/minute")
async def get_events(
    request: Request,
    event_type: Optional[str] = Query(None, description="Filter by event type"),
    limit: int = Query(20, ge=1, le=100, description="Number of events to return"),
):
    """
    Get recent event history.

    Returns list of sync events with timestamps and data.
    Useful for debugging and monitoring sync operations.
    """
    from core.events import events, SyncEvent

    # Convert string to SyncEvent if provided
    filter_type = None
    if event_type:
        try:
            filter_type = SyncEvent(event_type)
        except ValueError:
            # Try matching by name
            for et in SyncEvent:
                if et.name.lower() == event_type.lower():
                    filter_type = et
                    break

    return {
        "events": events.get_history(event_type=filter_type, limit=limit),
        "handlers": events.get_handlers(),
    }


@router.post("/jobs/{job_id}/trigger")
@limiter.limit("5/minute")
async def trigger_job(
    request: Request,
    job_id: str,
    admin: dict = Depends(require_admin)
):
    """
    Manually trigger a background job.

    Requires admin authentication.
    """
    from core.scheduler import get_scheduler

    scheduler = get_scheduler()
    if scheduler is None:
        raise HTTPException(status_code=503, detail="Scheduler not running")

    result = await scheduler.trigger_job(job_id)
    if result is None:
        raise HTTPException(status_code=404, detail=f"Job '{job_id}' not found")

    return {
        "status": "triggered",
        "job_id": job_id,
        "result": result
    }


# ─── Lightweight Endpoints (60/minute) ─────────────────────────────────────────

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
        # Validate compare_type
        if compare_type not in ("previous_period", "year_ago", "month_ago"):
            compare_type = "previous_period"
    except ValidationError as e:
        raise HTTPException(status_code=400, detail=str(e))

    start, end = dashboard_service.parse_period(period, start_date, end_date)
    result = await dashboard_service.get_revenue_trend(
        start, end, category_id=category_id, brand=brand, source_id=source_id,
        sales_type=sales_type, compare_type=compare_type
    )

    # Attach forecast data when requested (no filters applied)
    from datetime import date as _date, datetime as _datetime, timedelta as _timedelta
    today = _date.today()
    has_future = end > today.isoformat()
    is_current_period = period in ("month", "week")
    allow_forecast = is_current_period or has_future
    if include_forecast and allow_forecast and not category_id and not brand and not source_id:
        try:
            if period == "month":
                forecast = await dashboard_service.get_forecast_data(sales_type)
                # Cap predictions to current month only
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
                # Predict remaining days of the week (tomorrow..Sunday)
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

                # Extend comparison data to cover forecast dates
                # so the tooltip can show last year's revenue for predicted days
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
        except Exception:
            pass  # Graceful degradation

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
    result = await service.train(sales_type)
    return result


@router.post("/revenue/forecast/tune")
@limiter.limit("2/hour")
async def tune_revenue_forecast(
    request: Request,
    sales_type: Optional[str] = Query("retail", description="Sales type: retail or b2b"),
):
    """Run hyperparameter grid search to find optimal LightGBM parameters.

    Tests 72 parameter combinations across walk-forward CV folds.
    Saves best params to data/lgbm_best_params.json for use by train/evaluate.
    """
    try:
        sales_type = validate_sales_type(sales_type)
    except ValidationError as e:
        raise HTTPException(status_code=400, detail=str(e))

    from core.prediction_service import get_prediction_service
    service = get_prediction_service()
    result = await service.tune(sales_type)
    return result


@router.get("/revenue/forecast/evaluate")
@limiter.limit("5/hour")
async def evaluate_revenue_forecast(
    request: Request,
    sales_type: Optional[str] = Query("retail", description="Sales type: retail or b2b"),
):
    """Run walk-forward cross-validation to evaluate the revenue prediction model.

    Returns detailed metrics including WAPE, MAE, R², directional accuracy,
    comparison with 4 baseline models, feature importance, and residual analysis
    by day-of-week and month.
    """
    try:
        sales_type = validate_sales_type(sales_type)
    except ValidationError as e:
        raise HTTPException(status_code=400, detail=str(e))

    from core.prediction_service import get_prediction_service
    service = get_prediction_service()
    result = await service.evaluate(sales_type)
    return result


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

    # Build cache key from parameters
    cache_key = f"summary:{start}:{end}:{source_id}:{category_id}:{brand}:{sales_type}"

    # Try cache first (60 second TTL for summary data)
    cached = await cache.get(cache_key)
    if cached is not None:
        return cached

    # Fetch from database
    result = await dashboard_service.get_summary_stats(
        start, end, category_id, brand=brand, source_id=source_id, sales_type=sales_type
    )

    # Cache the result
    await cache.set(cache_key, result, ttl=60)

    return result


@router.get("/returns")
@limiter.limit("30/minute")
async def get_returns(
    request: Request,
    period: Optional[str] = Query(None, description="Shortcut: today, yesterday, week, month"),
    start_date: Optional[str] = Query(None, description="Start date (YYYY-MM-DD)"),
    end_date: Optional[str] = Query(None, description="End date (YYYY-MM-DD)"),
    sales_type: Optional[str] = Query("retail", description="Sales type: retail, b2b, or all"),
    limit: int = Query(50, ge=1, le=100, description="Maximum number of returns to fetch")
):
    """Get list of return orders for a date range."""
    from core.duckdb_store import get_store

    try:
        validate_period(period)
        sales_type = validate_sales_type(sales_type)
    except ValidationError as e:
        raise HTTPException(status_code=400, detail=str(e))

    start, end = dashboard_service.parse_period(period, start_date, end_date)

    store = await get_store()
    # Convert strings to date objects for the store method
    from datetime import datetime as _dt
    start_dt = _dt.strptime(start, "%Y-%m-%d").date()
    end_dt = _dt.strptime(end, "%Y-%m-%d").date()
    returns = await store.get_return_orders(start_dt, end_dt, sales_type, limit)

    return {
        "returns": returns,
        "count": len(returns),
        "startDate": start,
        "endDate": end
    }


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


@router.get("/customers/cohort-retention")
@limiter.limit("30/minute")
async def get_cohort_retention(
    request: Request,
    months_back: int = Query(12, ge=3, le=24, description="Number of months of cohorts to analyze"),
    retention_months: int = Query(6, ge=1, le=12, description="Number of retention months to track"),
    sales_type: Optional[str] = Query("retail", description="Sales type: retail, b2b, or all"),
    include_revenue: bool = Query(True, description="Include revenue retention metrics")
):
    """
    Get cohort retention analysis.

    Shows what percentage of customers from each monthly cohort
    returned to make purchases in subsequent months.

    When include_revenue=True (default), also includes:
    - Revenue retention % per month
    - Absolute revenue per cohort per month
    - Average revenue retention summary
    """
    try:
        sales_type = validate_sales_type(sales_type)
    except ValidationError as e:
        raise HTTPException(status_code=400, detail=str(e))

    store = await get_store()
    if include_revenue:
        return await store.get_enhanced_cohort_retention(
            months_back=months_back,
            retention_months=retention_months,
            sales_type=sales_type,
            include_revenue=True
        )
    return await store.get_cohort_retention(
        months_back=months_back,
        retention_months=retention_months,
        sales_type=sales_type
    )


@router.get("/customers/purchase-timing")
@limiter.limit("30/minute")
async def get_purchase_timing(
    request: Request,
    months_back: int = Query(12, ge=3, le=24, description="Number of months of first-time customers to analyze"),
    sales_type: Optional[str] = Query("retail", description="Sales type: retail, b2b, or all")
):
    """
    Get days-to-second-purchase analysis.

    Analyzes how long it takes customers to make their second purchase
    after their first order. Groups into time buckets (0-30 days, 31-60 days, etc.).

    Useful for:
    - Understanding product repurchase cycles
    - Timing re-engagement campaigns
    - Setting up email automation triggers
    """
    try:
        sales_type = validate_sales_type(sales_type)
    except ValidationError as e:
        raise HTTPException(status_code=400, detail=str(e))

    store = await get_store()
    return await store.get_days_to_second_purchase(
        months_back=months_back,
        sales_type=sales_type
    )


@router.get("/customers/cohort-ltv")
@limiter.limit("30/minute")
async def get_cohort_ltv(
    request: Request,
    months_back: int = Query(12, ge=3, le=24, description="Number of months of cohorts to analyze"),
    sales_type: Optional[str] = Query("retail", description="Sales type: retail, b2b, or all")
):
    """
    Get cumulative lifetime value by cohort.

    Shows how much revenue each cohort has generated over time,
    with cumulative totals per month since first purchase.

    Returns:
    - cumulativeRevenue: Array of cumulative revenue from M0 to M12
    - avgLTV: Average lifetime value per customer in cohort
    - Summary with best performing cohort
    """
    try:
        sales_type = validate_sales_type(sales_type)
    except ValidationError as e:
        raise HTTPException(status_code=400, detail=str(e))

    store = await get_store()
    return await store.get_cohort_ltv(
        months_back=months_back,
        sales_type=sales_type
    )


@router.get("/customers/at-risk")
@limiter.limit("30/minute")
async def get_at_risk_customers(
    request: Request,
    days_threshold: int = Query(90, ge=30, le=365, description="Days since last purchase to consider at-risk"),
    sales_type: Optional[str] = Query("retail", description="Sales type: retail, b2b, or all")
):
    """
    Get at-risk customers by cohort.

    Identifies customers who haven't purchased in the specified number of days,
    grouped by their acquisition cohort.

    Useful for:
    - Identifying churn risk
    - Targeting re-engagement campaigns
    - Understanding which cohorts have higher churn rates
    """
    try:
        sales_type = validate_sales_type(sales_type)
    except ValidationError as e:
        raise HTTPException(status_code=400, detail=str(e))

    store = await get_store()
    return await store.get_at_risk_customers(
        days_threshold=days_threshold,
        sales_type=sales_type
    )


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
    growth_factor: float = Query(1.10, ge=1.0, le=2.0, description="Growth factor for future calculations"),
    admin: dict = Depends(require_admin)
):
    """
    Set a custom revenue goal.

    The goal will be marked as custom (manually set). To revert to auto-calculated
    goals, use DELETE /api/goals/{period_type}.
    Requires admin authentication.
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
    sales_type: Optional[str] = Query("retail", description="Sales type: retail, b2b, or all"),
    admin: dict = Depends(require_admin)
):
    """
    Reset a goal to auto-calculated value.

    Removes the custom goal and reverts to using the system-calculated suggestion
    based on historical performance.
    Requires admin authentication.
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
    sales_type: Optional[str] = Query("retail", description="Sales type: retail, b2b, or all"),
    admin: dict = Depends(require_admin)
):
    """
    Force recalculation of seasonality indices and growth metrics.

    Use this after significant data changes or to update calculations
    with the latest data.
    Requires admin authentication.
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


@router.post("/stocks/snapshot/refresh")
@limiter.limit("5/minute")
async def refresh_inventory_snapshot(request: Request):
    """
    Force refresh today's inventory snapshot.

    Deletes existing snapshot for today and re-records with current data.
    Use this after fixing snapshot formula or to update with latest stock data.
    """
    store = await get_store()
    result = await store.record_inventory_snapshot(force=True)
    return {"success": result, "message": "Inventory snapshot refreshed" if result else "Failed to refresh"}


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
