"""Revenue goals, smart goals, seasonality, growth, weekly patterns endpoints."""
from fastapi import APIRouter, Query, Request, HTTPException, Depends
from typing import Optional

from web.routes.auth import require_admin
from ._deps import limiter, get_store, validate_sales_type, ValidationError

router = APIRouter()


@router.get("/goals")
@limiter.limit("60/minute")
async def get_goals(
    request: Request,
    sales_type: Optional[str] = Query("retail"),
):
    """Get revenue goals for daily, weekly, and monthly periods."""
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
    weeks_back: int = Query(4, ge=1, le=12),
    sales_type: Optional[str] = Query("retail"),
):
    """Get historical revenue data used for goal calculations."""
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
    period_type: str = Query(...),
    amount: float = Query(..., gt=0),
    growth_factor: float = Query(1.10, ge=1.0, le=2.0),
    admin: dict = Depends(require_admin),
):
    """Set a custom revenue goal. Requires admin."""
    if period_type not in ["daily", "weekly", "monthly"]:
        raise HTTPException(status_code=400, detail="period_type must be: daily, weekly, or monthly")

    store = await get_store()
    return await store.set_goal(period_type, amount, is_custom=True, growth_factor=growth_factor)


@router.delete("/goals/{period_type}")
@limiter.limit("10/minute")
async def reset_goal(
    request: Request,
    period_type: str,
    sales_type: Optional[str] = Query("retail"),
    admin: dict = Depends(require_admin),
):
    """Reset a goal to auto-calculated value. Requires admin."""
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
    sales_type: Optional[str] = Query("retail"),
):
    """Get smart revenue goals using seasonality and YoY growth."""
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
    sales_type: Optional[str] = Query("retail"),
):
    """Get monthly seasonality indices."""
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
    sales_type: Optional[str] = Query("retail"),
):
    """Get year-over-year growth metrics."""
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
    sales_type: Optional[str] = Query("retail"),
):
    """Get weekly distribution patterns within months."""
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
    sales_type: Optional[str] = Query("retail"),
    admin: dict = Depends(require_admin),
):
    """Force recalculation of seasonality indices and growth metrics. Requires admin."""
    try:
        sales_type = validate_sales_type(sales_type)
    except ValidationError as e:
        raise HTTPException(status_code=400, detail=str(e))

    store = await get_store()
    seasonality = await store.calculate_seasonality_indices(sales_type)
    growth = await store.calculate_yoy_growth(sales_type)
    await store.calculate_weekly_patterns(sales_type)

    return {
        "status": "success",
        "message": "Seasonality indices and growth metrics recalculated",
        "summary": {
            "monthsCalculated": len(seasonality),
            "overallYoY": growth.get("overall_yoy", 0),
            "yearsAnalyzed": len(growth.get("yearly_data", [])),
        },
    }


@router.get("/goals/forecast")
@limiter.limit("30/minute")
async def get_goal_forecast(
    request: Request,
    year: int = Query(..., ge=2020, le=2030),
    month: int = Query(..., ge=1, le=12),
    sales_type: Optional[str] = Query("retail"),
    recalculate: bool = Query(False),
):
    """Generate smart goals for a specific future month."""
    try:
        sales_type = validate_sales_type(sales_type)
    except ValidationError as e:
        raise HTTPException(status_code=400, detail=str(e))

    store = await get_store()
    return await store.generate_smart_goals(year, month, sales_type, recalculate)
