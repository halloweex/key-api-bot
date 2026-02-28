"""Stock summary, average, trend, analysis, actions, alerts endpoints."""
import logging

from fastapi import APIRouter, Query, Request

from ._deps import limiter, get_store

router = APIRouter()
logger = logging.getLogger(__name__)


@router.get("/stocks/summary")
@limiter.limit("30/minute")
async def get_stock_summary(
    request: Request,
    limit: int = Query(20, ge=5, le=50),
):
    """Get stock summary for dashboard display."""
    store = await get_store()
    return await store.get_stock_summary(limit)


@router.get("/stocks/average")
@limiter.limit("30/minute")
async def get_average_inventory(
    request: Request,
    days: int = Query(30, ge=7, le=365),
):
    """Get average inventory using formula: (Beginning + Ending) / 2."""
    store = await get_store()
    return await store.get_average_inventory(days)


@router.get("/stocks/trend")
@limiter.limit("30/minute")
async def get_inventory_trend(
    request: Request,
    days: int = Query(90, ge=7, le=365),
    granularity: str = Query("daily", description="Data granularity: daily or monthly"),
):
    """Get inventory trend over time for charting stock changes."""
    if granularity not in ("daily", "monthly"):
        granularity = "daily"

    try:
        store = await get_store()
        return await store.get_inventory_trend(days, granularity)
    except Exception as e:
        logger.error(f"Inventory trend error: {e}", exc_info=True)
        raise


@router.get("/stocks/analysis")
@limiter.limit("30/minute")
async def get_inventory_analysis(request: Request):
    """Get comprehensive inventory analysis using Layer 3 views."""
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
    """Force refresh today's inventory snapshot."""
    store = await get_store()
    result = await store.record_inventory_snapshot(force=True)
    return {
        "success": result,
        "message": "Inventory snapshot refreshed" if result else "Failed to refresh",
    }


@router.get("/stocks/turnover")
@limiter.limit("30/minute")
async def get_inventory_turnover(
    request: Request,
    days: int = Query(30, ge=7, le=90),
):
    """Get inventory turnover KPIs, ABC analysis, and excess stock metrics."""
    store = await get_store()
    return await store.get_inventory_turnover(days)


@router.get("/stocks/actions")
@limiter.limit("30/minute")
async def get_stock_actions(request: Request):
    """Get recommended actions for dead stock items."""
    store = await get_store()
    return await store.get_recommended_actions(limit=50)


@router.get("/stocks/alerts")
@limiter.limit("30/minute")
async def get_stock_alerts(request: Request):
    """Get low stock alerts for items that need restocking."""
    store = await get_store()
    return await store.get_restock_alerts(limit=50)
