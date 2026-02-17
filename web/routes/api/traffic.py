"""Traffic analytics, trend, transactions, refresh endpoints."""
import asyncio
import logging
from datetime import datetime as _dt, timedelta
from zoneinfo import ZoneInfo

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


@router.get("/traffic/roas")
@limiter.limit("30/minute")
async def get_traffic_roas(
    request: Request,
    period: Optional[str] = Query(None),
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
    sales_type: Optional[str] = Query("retail"),
):
    """Get blended and per-platform ROAS with bonus tier."""
    try:
        validate_period(period)
        sales_type = validate_sales_type(sales_type)
    except ValidationError as e:
        raise HTTPException(status_code=400, detail=str(e))

    start, end = dashboard_service.parse_period(period, start_date, end_date)
    start_dt = _dt.strptime(start, "%Y-%m-%d").date()
    end_dt = _dt.strptime(end, "%Y-%m-%d").date()

    store = await get_store()
    return await store.get_traffic_roas(
        start_date=start_dt, end_date=end_dt, sales_type=sales_type,
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


_backfill_status: dict = {"running": False, "result": None}


async def _run_backfill(days: int):
    """Background task: backfill manager_comment from KeyCRM API."""
    from core.keycrm import get_async_client

    store = await get_store()
    client = await get_async_client()
    tz = ZoneInfo("Europe/Kyiv")

    try:
        async with store.connection() as conn:
            null_count = conn.execute(
                "SELECT COUNT(*) FROM orders WHERE manager_comment IS NULL"
            ).fetchone()[0]
            total = conn.execute("SELECT COUNT(*) FROM orders").fetchone()[0]

        if null_count == 0:
            _backfill_status.update(running=False, result={
                "status": "skip", "message": "All orders already have manager_comment",
            })
            return

        logger.info(f"UTM backfill: {null_count}/{total} orders missing manager_comment")

        final_end = _dt.now(tz) + timedelta(days=1)
        chunk_days = 90
        current_start = _dt.now(tz) - timedelta(days=days)
        updated_total = 0
        chunks_processed = 0

        while current_start < final_end:
            current_end = min(current_start + timedelta(days=chunk_days), final_end)
            start_str = current_start.strftime('%Y-%m-%d')
            end_str = current_end.strftime('%Y-%m-%d')
            chunks_processed += 1

            orders_by_id = {}
            try:
                params = {
                    "filter[created_between]": f"{start_str}, {end_str}",
                    "limit": 50,
                }
                async for batch in client.paginate("order", params=params, page_size=50):
                    for order in batch:
                        mc = order.get("manager_comment")
                        if mc:
                            orders_by_id[order["id"]] = mc
            except Exception as e:
                logger.warning(f"UTM backfill chunk {start_str}-{end_str} failed: {e}")
                current_start = current_end
                continue

            if orders_by_id:
                async with store.connection() as conn:
                    conn.execute("BEGIN TRANSACTION")
                    try:
                        for order_id, comment in orders_by_id.items():
                            conn.execute(
                                "UPDATE orders SET manager_comment = ? WHERE id = ? AND manager_comment IS NULL",
                                [comment, order_id]
                            )
                        conn.execute("COMMIT")
                        updated_total += len(orders_by_id)
                    except Exception as e:
                        conn.execute("ROLLBACK")
                        logger.error(f"UTM backfill DB update failed: {e}")

            _backfill_status["result"] = {
                "status": "in_progress",
                "chunks_processed": chunks_processed,
                "orders_updated": updated_total,
            }
            current_start = current_end

        logger.info(f"UTM backfill: updated {updated_total} orders across {chunks_processed} chunks")

        async with store.connection() as conn:
            conn.execute("DELETE FROM silver_order_utm")

        utm_count = await store.refresh_utm_silver_layer()
        traffic_rows = await store.refresh_traffic_gold_layer()

        logger.info(f"UTM backfill complete: {utm_count} UTM records, {traffic_rows} traffic rows")

        _backfill_status.update(running=False, result={
            "status": "success",
            "orders_missing_before": null_count,
            "orders_updated": updated_total,
            "chunks_processed": chunks_processed,
            "utm_records_parsed": utm_count,
            "traffic_gold_rows": traffic_rows,
        })
    except Exception as e:
        logger.error(f"UTM backfill failed: {e}", exc_info=True)
        _backfill_status.update(running=False, result={
            "status": "error", "error": str(e),
        })


@router.post("/traffic/backfill-utm")
@limiter.limit("1/minute")
async def backfill_utm_data(
    request: Request,
    days: int = Query(730, ge=30, le=1000),
):
    """Start UTM backfill as background task. Check status via GET."""
    if _backfill_status["running"]:
        return {"status": "already_running", "progress": _backfill_status["result"]}

    _backfill_status.update(running=True, result={"status": "started"})
    asyncio.create_task(_run_backfill(days))
    return {"status": "started", "message": "Backfill started in background. GET /traffic/backfill-utm/status to check."}


@router.get("/traffic/backfill-utm/status")
@limiter.limit("30/minute")
async def backfill_utm_status(request: Request):
    """Check status of UTM backfill background task."""
    return {
        "running": _backfill_status["running"],
        "result": _backfill_status["result"],
    }
