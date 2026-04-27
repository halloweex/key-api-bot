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
async def get_inventory_analysis(
    request: Request,
    carrying_rate: float = Query(0.25, ge=0.05, le=0.50,
                                 description="Annual inventory carrying cost (fraction of cost basis)"),
    liquidation_discount: float = Query(0.50, ge=0.20, le=0.80,
                                        description="Discount used in liquidation NPV calculation"),
):
    """Comprehensive inventory analysis with cost basis, GMROI, NPV decision."""
    store = await get_store()
    summary = await store.get_inventory_summary_v2()
    deep = await store.get_dead_stock_deep(
        limit=100,
        carrying_rate=carrying_rate,
        liquidation_discount=liquidation_discount,
    )

    return {
        **summary,
        "items": deep["items"],
        "quadrantMatrix": deep["quadrantMatrix"],
        "concentration": deep["concentration"],
        "costQuality": deep["costQuality"],
        "gmroiDistribution": deep["gmroiDistribution"],
        "liquidationSummary": deep["liquidationSummary"],
        "params": deep["params"],
        "methodology": {
            "description": "Dynamic thresholds per category using P75, minimum 90 days, maximum 365 days",
            "minimumThreshold": 90,
            "defaultThreshold": 180,
            "atRiskMultiplier": 0.7,
            "velocityTiers": {
                "hot": "DOS ≤ 30",
                "healthy": "30 < DOS ≤ 90",
                "warm": "90 < DOS ≤ 180",
                "cold": "180 < DOS ≤ 365",
                "frozen": "DOS > 365 or no sales in 90d",
            },
            "optimalDays": deep["params"]["optimalDays"],
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
    lead_time: int = Query(14, ge=1, le=90, description="Supplier lead time in days"),
    safety_multiplier: float = Query(1.5, ge=1.0, le=3.0, description="Safety stock multiplier on lead time"),
    buffer_days: int = Query(5, ge=0, le=30, description="Extra buffer days (customs, logistics)"),
    max_acceptable_days: int = Query(60, ge=30, le=365, description="Max acceptable stock days (yellow/red boundary)"),
):
    """Get inventory turnover KPIs, ABC analysis, and excess stock metrics."""
    store = await get_store()
    return await store.get_inventory_turnover(
        days,
        lead_time_days=lead_time,
        safety_multiplier=safety_multiplier,
        buffer_days=buffer_days,
        max_acceptable_days=max_acceptable_days,
    )


@router.get("/stocks/skus")
@limiter.limit("30/minute")
async def get_all_skus(
    request: Request,
    carrying_rate: float = Query(0.25, ge=0.05, le=0.50),
    liquidation_discount: float = Query(0.50, ge=0.20, le=0.80),
):
    """All SKUs with cost basis, GMROI, NPV decision — for SKU rotation table.

    Client-side filters/sorts. Same params as /stocks/analysis.
    """
    store = await get_store()
    return await store.get_all_skus_deep(
        carrying_rate=carrying_rate,
        liquidation_discount=liquidation_discount,
    )


@router.get("/stocks/brand-rotation")
@limiter.limit("30/minute")
async def get_brand_rotation(
    request: Request,
    min_skus: int = Query(1, ge=1, le=20, description="Minimum SKUs per brand to include"),
):
    """Per-brand rotation scorecard: rotation_days, GMROI, frozen capital share."""
    store = await get_store()
    return await store.get_brand_rotation(min_skus=min_skus)


@router.get("/stocks/abc/{abc_class}")
@limiter.limit("30/minute")
async def get_abc_skus(
    request: Request,
    abc_class: str,
    limit: int = Query(50, ge=1, le=200),
):
    """Get SKUs for a specific ABC class."""
    abc_class = abc_class.upper()
    if abc_class not in ("A", "B", "C"):
        from fastapi import HTTPException
        raise HTTPException(400, "abc_class must be A, B, or C")
    store = await get_store()
    return await store.get_abc_skus(abc_class, limit)


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
