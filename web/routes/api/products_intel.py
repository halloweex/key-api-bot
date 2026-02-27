"""Product intelligence endpoints: basket analysis, pairs, momentum."""
import logging
from datetime import datetime as _dt

from fastapi import APIRouter, Query, Request, HTTPException
from typing import Optional

from web.services import dashboard_service
from ._deps import (
    limiter, get_store,
    validate_period, validate_sales_type, validate_limit,
    ValidationError,
)

router = APIRouter()
logger = logging.getLogger(__name__)


@router.get("/products/intel/summary")
@limiter.limit("30/minute")
async def get_basket_summary(
    request: Request,
    period: Optional[str] = Query(None),
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
    sales_type: Optional[str] = Query("retail"),
):
    """Get basket KPIs: avg size, multi-item %, uplift, top pair."""
    try:
        validate_period(period)
        sales_type = validate_sales_type(sales_type)
    except ValidationError as e:
        raise HTTPException(status_code=400, detail=str(e))

    start, end = dashboard_service.parse_period(period, start_date, end_date)
    start_dt = _dt.strptime(start, "%Y-%m-%d").date()
    end_dt = _dt.strptime(end, "%Y-%m-%d").date()

    store = await get_store()
    return await store.get_basket_summary(
        start_date=start_dt, end_date=end_dt, sales_type=sales_type,
    )


@router.get("/products/intel/pairs")
@limiter.limit("30/minute")
async def get_product_pairs(
    request: Request,
    period: Optional[str] = Query(None),
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
    sales_type: Optional[str] = Query("retail"),
    limit: Optional[int] = Query(20),
    product_id: Optional[int] = Query(None),
):
    """Get frequently bought together product pairs."""
    try:
        validate_period(period)
        sales_type = validate_sales_type(sales_type)
        limit = validate_limit(limit, max_value=50)
    except ValidationError as e:
        raise HTTPException(status_code=400, detail=str(e))

    start, end = dashboard_service.parse_period(period, start_date, end_date)
    start_dt = _dt.strptime(start, "%Y-%m-%d").date()
    end_dt = _dt.strptime(end, "%Y-%m-%d").date()

    store = await get_store()
    return await store.get_frequently_bought_together(
        start_date=start_dt, end_date=end_dt,
        sales_type=sales_type, limit=limit, product_id=product_id,
    )


@router.get("/products/intel/basket-distribution")
@limiter.limit("30/minute")
async def get_basket_distribution(
    request: Request,
    period: Optional[str] = Query(None),
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
    sales_type: Optional[str] = Query("retail"),
):
    """Get basket size distribution with AOV per bucket."""
    try:
        validate_period(period)
        sales_type = validate_sales_type(sales_type)
    except ValidationError as e:
        raise HTTPException(status_code=400, detail=str(e))

    start, end = dashboard_service.parse_period(period, start_date, end_date)
    start_dt = _dt.strptime(start, "%Y-%m-%d").date()
    end_dt = _dt.strptime(end, "%Y-%m-%d").date()

    store = await get_store()
    return await store.get_basket_distribution(
        start_date=start_dt, end_date=end_dt, sales_type=sales_type,
    )


@router.get("/products/intel/category-combos")
@limiter.limit("30/minute")
async def get_category_combos(
    request: Request,
    period: Optional[str] = Query(None),
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
    sales_type: Optional[str] = Query("retail"),
    limit: Optional[int] = Query(10),
):
    """Get top category pair combinations."""
    try:
        validate_period(period)
        sales_type = validate_sales_type(sales_type)
        limit = validate_limit(limit, max_value=30)
    except ValidationError as e:
        raise HTTPException(status_code=400, detail=str(e))

    start, end = dashboard_service.parse_period(period, start_date, end_date)
    start_dt = _dt.strptime(start, "%Y-%m-%d").date()
    end_dt = _dt.strptime(end, "%Y-%m-%d").date()

    store = await get_store()
    return await store.get_category_combinations(
        start_date=start_dt, end_date=end_dt, sales_type=sales_type, limit=limit,
    )


@router.get("/products/intel/brand-affinity")
@limiter.limit("30/minute")
async def get_brand_affinity(
    request: Request,
    period: Optional[str] = Query(None),
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
    sales_type: Optional[str] = Query("retail"),
    limit: Optional[int] = Query(10),
):
    """Get top brand pair co-purchases."""
    try:
        validate_period(period)
        sales_type = validate_sales_type(sales_type)
        limit = validate_limit(limit, max_value=30)
    except ValidationError as e:
        raise HTTPException(status_code=400, detail=str(e))

    start, end = dashboard_service.parse_period(period, start_date, end_date)
    start_dt = _dt.strptime(start, "%Y-%m-%d").date()
    end_dt = _dt.strptime(end, "%Y-%m-%d").date()

    store = await get_store()
    return await store.get_brand_affinity(
        start_date=start_dt, end_date=end_dt,
        sales_type=sales_type, limit=limit,
    )


@router.get("/products/intel/momentum")
@limiter.limit("30/minute")
async def get_product_momentum(
    request: Request,
    period: Optional[str] = Query(None),
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
    sales_type: Optional[str] = Query("retail"),
    limit: Optional[int] = Query(5),
):
    """Get products with biggest revenue growth/decline vs previous period."""
    try:
        validate_period(period)
        sales_type = validate_sales_type(sales_type)
        limit = validate_limit(limit, max_value=20)
    except ValidationError as e:
        raise HTTPException(status_code=400, detail=str(e))

    start, end = dashboard_service.parse_period(period, start_date, end_date)
    start_dt = _dt.strptime(start, "%Y-%m-%d").date()
    end_dt = _dt.strptime(end, "%Y-%m-%d").date()

    store = await get_store()
    return await store.get_product_momentum(
        start_date=start_dt, end_date=end_dt, sales_type=sales_type, limit=limit,
    )
