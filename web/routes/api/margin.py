"""Margin analysis API endpoints.

Admin-only. These return cost, margin, GMROI and low-margin alerts — the same
class of sensitive data as expenses. The frontend gates the whole page behind
``<AdminGuard>`` (web/frontend/src/App.tsx); every endpoint here stacks
``Depends(require_admin)`` on top of the inherited ``api_gate`` so the server
enforces the same intent, not merely a valid session.
"""
import logging
from fastapi import APIRouter, Query, Request, HTTPException, Depends
from typing import Optional

from web.routes.auth import require_admin
from web.services import margin_service
from ._deps import limiter, validate_period, validate_sales_type, ValidationError

router = APIRouter()
logger = logging.getLogger(__name__)


@router.get("/margin/overview")
@limiter.limit("30/minute")
async def get_margin_overview(
    request: Request,
    period: Optional[str] = Query(None),
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
    sales_type: Optional[str] = Query("retail"),
    admin: dict = Depends(require_admin),
):
    """Get overall margin KPIs."""
    try:
        validate_period(period)
        sales_type = validate_sales_type(sales_type)
    except ValidationError as e:
        raise HTTPException(status_code=400, detail=str(e))

    start, end = margin_service.parse_period(period, start_date, end_date)
    return await margin_service.get_margin_overview(start, end, sales_type=sales_type)


@router.get("/margin/by-brand")
@limiter.limit("30/minute")
async def get_margin_by_brand(
    request: Request,
    period: Optional[str] = Query(None),
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
    sales_type: Optional[str] = Query("retail"),
    limit: int = Query(20, ge=1, le=50),
    admin: dict = Depends(require_admin),
):
    """Get margin breakdown by brand."""
    try:
        validate_period(period)
        sales_type = validate_sales_type(sales_type)
    except ValidationError as e:
        raise HTTPException(status_code=400, detail=str(e))

    start, end = margin_service.parse_period(period, start_date, end_date)
    return await margin_service.get_margin_by_brand(start, end, sales_type=sales_type, limit=limit)


@router.get("/margin/by-category")
@limiter.limit("30/minute")
async def get_margin_by_category(
    request: Request,
    period: Optional[str] = Query(None),
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
    sales_type: Optional[str] = Query("retail"),
    admin: dict = Depends(require_admin),
):
    """Get margin breakdown by category."""
    try:
        validate_period(period)
        sales_type = validate_sales_type(sales_type)
    except ValidationError as e:
        raise HTTPException(status_code=400, detail=str(e))

    start, end = margin_service.parse_period(period, start_date, end_date)
    return await margin_service.get_margin_by_category(start, end, sales_type=sales_type)


@router.get("/margin/trend")
@limiter.limit("30/minute")
async def get_margin_trend(
    request: Request,
    period: Optional[str] = Query(None),
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
    sales_type: Optional[str] = Query("retail"),
    admin: dict = Depends(require_admin),
):
    """Get monthly margin trend."""
    try:
        validate_period(period)
        sales_type = validate_sales_type(sales_type)
    except ValidationError as e:
        raise HTTPException(status_code=400, detail=str(e))

    start, end = margin_service.parse_period(period, start_date, end_date)
    return await margin_service.get_margin_trend(start, end, sales_type=sales_type)


@router.get("/margin/brand-category")
@limiter.limit("30/minute")
async def get_margin_brand_category(
    request: Request,
    period: Optional[str] = Query(None),
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
    sales_type: Optional[str] = Query("retail"),
    min_revenue: float = Query(500, ge=0),
    admin: dict = Depends(require_admin),
):
    """Get brand × category margin cross-tab."""
    try:
        validate_period(period)
        sales_type = validate_sales_type(sales_type)
    except ValidationError as e:
        raise HTTPException(status_code=400, detail=str(e))

    start, end = margin_service.parse_period(period, start_date, end_date)
    return await margin_service.get_margin_brand_category(start, end, sales_type=sales_type, min_revenue=min_revenue)


@router.get("/margin/alerts")
@limiter.limit("30/minute")
async def get_margin_alerts(
    request: Request,
    period: Optional[str] = Query(None),
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
    sales_type: Optional[str] = Query("retail"),
    margin_floor: float = Query(30.0, ge=0, le=100),
    admin: dict = Depends(require_admin),
):
    """Get low-margin brand alerts."""
    try:
        validate_period(period)
        sales_type = validate_sales_type(sales_type)
    except ValidationError as e:
        raise HTTPException(status_code=400, detail=str(e))

    start, end = margin_service.parse_period(period, start_date, end_date)
    return await margin_service.get_margin_alerts(start, end, sales_type=sales_type, margin_floor=margin_floor)
