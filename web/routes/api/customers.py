"""Customer insights, cohort retention, purchase timing, LTV, at-risk endpoints."""
from fastapi import APIRouter, Query, Request, HTTPException
from typing import Optional

from web.services import dashboard_service
from ._deps import (
    limiter, get_store,
    validate_period, validate_source_id, validate_brand_name, validate_sales_type,
    ValidationError,
)

router = APIRouter()


@router.get("/customers/insights")
@limiter.limit("30/minute")
async def get_customer_insights(
    request: Request,
    period: Optional[str] = Query(None),
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
    source_id: Optional[int] = Query(None),
    brand: Optional[str] = Query(None),
    sales_type: Optional[str] = Query("retail"),
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
    return await dashboard_service.get_customer_insights(
        start, end, brand=brand, source_id=source_id, sales_type=sales_type,
    )


@router.get("/customers/cohort-retention")
@limiter.limit("30/minute")
async def get_cohort_retention(
    request: Request,
    months_back: int = Query(12, ge=3, le=24),
    retention_months: int = Query(6, ge=1, le=12),
    sales_type: Optional[str] = Query("retail"),
    include_revenue: bool = Query(True),
):
    """Get cohort retention analysis with optional revenue retention."""
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
            include_revenue=True,
        )
    return await store.get_cohort_retention(
        months_back=months_back,
        retention_months=retention_months,
        sales_type=sales_type,
    )


@router.get("/customers/purchase-timing")
@limiter.limit("30/minute")
async def get_purchase_timing(
    request: Request,
    months_back: int = Query(12, ge=3, le=24),
    sales_type: Optional[str] = Query("retail"),
):
    """Get days-to-second-purchase analysis."""
    try:
        sales_type = validate_sales_type(sales_type)
    except ValidationError as e:
        raise HTTPException(status_code=400, detail=str(e))

    store = await get_store()
    return await store.get_days_to_second_purchase(
        months_back=months_back, sales_type=sales_type,
    )


@router.get("/customers/cohort-ltv")
@limiter.limit("30/minute")
async def get_cohort_ltv(
    request: Request,
    months_back: int = Query(12, ge=3, le=24),
    retention_months: int = Query(12, ge=1, le=24),
    sales_type: Optional[str] = Query("retail"),
):
    """Get cumulative lifetime value by cohort."""
    try:
        sales_type = validate_sales_type(sales_type)
    except ValidationError as e:
        raise HTTPException(status_code=400, detail=str(e))

    store = await get_store()
    return await store.get_cohort_ltv(
        months_back=months_back, retention_months=retention_months, sales_type=sales_type,
    )


@router.get("/customers/at-risk")
@limiter.limit("30/minute")
async def get_at_risk_customers(
    request: Request,
    days_threshold: int = Query(90, ge=30, le=365),
    months_back: int = Query(12, ge=3, le=24),
    sales_type: Optional[str] = Query("retail"),
):
    """Get at-risk customers by cohort (haven't purchased in N days)."""
    try:
        sales_type = validate_sales_type(sales_type)
    except ValidationError as e:
        raise HTTPException(status_code=400, detail=str(e))

    store = await get_store()
    return await store.get_at_risk_customers(
        days_threshold=days_threshold, months_back=months_back, sales_type=sales_type,
    )
