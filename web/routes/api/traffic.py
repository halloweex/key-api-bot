"""Traffic analytics, trend, transactions, refresh endpoints."""
import logging
from datetime import datetime as _dt

from fastapi import APIRouter, Query, Request, HTTPException, Depends
from typing import Optional

from web.services import dashboard_service
from web.routes.auth import require_admin
from ._deps import (
    limiter, get_store,
    validate_period, validate_source_id, validate_sales_type,
    ValidationError,
)

router = APIRouter()
logger = logging.getLogger(__name__)


@router.get("/traffic/analytics")
@limiter.limit("30/minute")
async def get_traffic_analytics(
    request: Request,
    period: Optional[str] = Query(None),
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
    source_id: Optional[int] = Query(None),
    sales_type: Optional[str] = Query("retail"),
):
    """Get traffic analytics with platform and traffic type breakdown."""
    try:
        validate_period(period)
        validate_source_id(source_id)
        sales_type = validate_sales_type(sales_type)
    except ValidationError as e:
        raise HTTPException(status_code=400, detail=str(e))

    start, end = dashboard_service.parse_period(period, start_date, end_date)
    start_dt = _dt.strptime(start, "%Y-%m-%d").date()
    end_dt = _dt.strptime(end, "%Y-%m-%d").date()

    store = await get_store()
    return await store.get_traffic_analytics(
        start_date=start_dt, end_date=end_dt, sales_type=sales_type, source_id=source_id,
    )


@router.get("/traffic/trend")
@limiter.limit("30/minute")
async def get_traffic_trend(
    request: Request,
    period: Optional[str] = Query(None),
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
    source_id: Optional[int] = Query(None),
    sales_type: Optional[str] = Query("retail"),
):
    """Get daily traffic trend with paid/organic breakdown."""
    try:
        validate_period(period)
        validate_source_id(source_id)
        sales_type = validate_sales_type(sales_type)
    except ValidationError as e:
        raise HTTPException(status_code=400, detail=str(e))

    start, end = dashboard_service.parse_period(period, start_date, end_date)
    start_dt = _dt.strptime(start, "%Y-%m-%d").date()
    end_dt = _dt.strptime(end, "%Y-%m-%d").date()

    store = await get_store()
    result = await store.get_traffic_trend(
        start_date=start_dt, end_date=end_dt, sales_type=sales_type, source_id=source_id,
    )
    return {"trend": result}


@router.get("/traffic/transactions")
@limiter.limit("30/minute")
async def get_traffic_transactions(
    request: Request,
    period: Optional[str] = Query(None),
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
    source_id: Optional[int] = Query(None),
    sales_type: Optional[str] = Query("retail"),
    traffic_type: Optional[str] = Query(None),
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
):
    """Get individual orders with traffic attribution details."""
    try:
        validate_period(period)
        validate_source_id(source_id)
        sales_type = validate_sales_type(sales_type)
    except ValidationError as e:
        raise HTTPException(status_code=400, detail=str(e))

    if traffic_type and traffic_type not in (
        "paid_confirmed", "paid_likely", "organic", "pixel_only", "unknown",
    ):
        raise HTTPException(status_code=400, detail=f"Invalid traffic_type: {traffic_type}")

    start, end = dashboard_service.parse_period(period, start_date, end_date)
    start_dt = _dt.strptime(start, "%Y-%m-%d").date()
    end_dt = _dt.strptime(end, "%Y-%m-%d").date()

    store = await get_store()
    return await store.get_traffic_transactions(
        start_date=start_dt, end_date=end_dt, sales_type=sales_type,
        source_id=source_id, traffic_type=traffic_type, limit=limit, offset=offset,
    )


@router.post("/traffic/refresh")
@limiter.limit("5/minute")
async def refresh_traffic_data(
    request: Request,
    user: dict = Depends(require_admin),
):
    """Force refresh UTM and traffic layers (admin only)."""
    store = await get_store()

    try:
        utm_count = await store.refresh_utm_silver_layer()
        traffic_rows = await store.refresh_traffic_gold_layer()

        return {
            "success": True,
            "utm_orders_parsed": utm_count,
            "traffic_rows": traffic_rows,
        }
    except Exception as e:
        logger.error(f"Traffic refresh failed: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Refresh failed: {e}")
