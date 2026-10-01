"""Revenue goals, smart goals, seasonality, growth, weekly patterns endpoints."""
from fastapi import APIRouter, Query, Request, HTTPException, Depends
from typing import Optional

from web.routes.auth import require_admin, require_permission
from core.repositories.goals import GOAL_TABLES_SALES_TYPE
from ._deps import limiter, get_store, validate_sales_type, ValidationError

router = APIRouter()


@router.get("/goals")
@limiter.limit("60/minute")
async def get_goals(
    request: Request,
    sales_type: Optional[str] = Query("retail"),
    _gate=Depends(require_permission("dashboard")),
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
    _gate=Depends(require_permission("dashboard")),
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
    year: Optional[int] = Query(None, ge=2020, le=2030),
    month: Optional[int] = Query(None, ge=1, le=12),
    _gate=Depends(require_permission("dashboard")),
):
    """Get smart revenue goals using seasonality and YoY growth.

    Optionally pass year/month to get goals for a specific month
    (e.g. for 'last_month' period). Defaults to current month.
    """
    try:
        sales_type = validate_sales_type(sales_type)
    except ValidationError as e:
        raise HTTPException(status_code=400, detail=str(e))

    store = await get_store()
    return await store.get_smart_goals(sales_type, year=year, month=month)


@router.get("/goals/seasonality")
@limiter.limit("30/minute")
async def get_seasonality_data(
    request: Request,
    sales_type: Optional[str] = Query("retail"),
    _gate=Depends(require_permission("dashboard")),
):
    """Get monthly seasonality indices."""
    try:
        sales_type = validate_sales_type(sales_type)
    except ValidationError as e:
        raise HTTPException(status_code=400, detail=str(e))

    store = await get_store()
    # Read path: computed for the caller, stored by nobody — the calculators
    # cannot store at all (OD-14 (i)). The tables have no sales_type in their
    # keys, so a GET that stored its result replaced what every user's retail
    # goal is built from, for any viewer, with one query parameter. Writing is
    # `POST /goals/recalculate` and the Monday job.
    return await store.calculate_seasonality_indices(sales_type)


@router.get("/goals/growth")
@limiter.limit("30/minute")
async def get_growth_data(
    request: Request,
    sales_type: Optional[str] = Query("retail"),
    _gate=Depends(require_permission("dashboard")),
):
    """Get year-over-year growth metrics."""
    try:
        sales_type = validate_sales_type(sales_type)
    except ValidationError as e:
        raise HTTPException(status_code=400, detail=str(e))

    store = await get_store()
    # Read path: computed for the caller, stored by nobody — the calculators
    # cannot store at all (OD-14 (i)). The tables have no sales_type in their
    # keys, so a GET that stored its result replaced what every user's retail
    # goal is built from, for any viewer, with one query parameter. Writing is
    # `POST /goals/recalculate` and the Monday job.
    return await store.calculate_yoy_growth(sales_type)


@router.get("/goals/weekly-patterns")
@limiter.limit("30/minute")
async def get_weekly_patterns(
    request: Request,
    sales_type: Optional[str] = Query("retail"),
    _gate=Depends(require_permission("dashboard")),
):
    """Get weekly distribution patterns within months."""
    try:
        sales_type = validate_sales_type(sales_type)
    except ValidationError as e:
        raise HTTPException(status_code=400, detail=str(e))

    store = await get_store()
    # Read path: computed for the caller, stored by nobody — the calculators
    # cannot store at all (OD-14 (i)). The tables have no sales_type in their
    # keys, so a GET that stored its result replaced what every user's retail
    # goal is built from, for any viewer, with one query parameter. Writing is
    # `POST /goals/recalculate` and the Monday job.
    return await store.calculate_weekly_patterns(sales_type)


@router.post("/goals/recalculate")
@limiter.limit("5/minute")
async def recalculate_seasonality(
    request: Request,
    sales_type: Optional[str] = Query("retail"),
    admin: dict = Depends(require_admin),
):
    """Recompute and store the seasonality indices, growth metrics and weekly
    patterns. Requires admin.

    Retail only. The three tables carry no sales_type in their keys, so the
    rows mean retail — the Monday job's answer since it stopped writing a b2b
    pass over them. This route used to store the caller's sales_type there,
    and every user's retail goal was then built from it.
    """
    try:
        sales_type = validate_sales_type(sales_type)
    except ValidationError as e:
        raise HTTPException(status_code=400, detail=str(e))
    if sales_type != GOAL_TABLES_SALES_TYPE:
        raise HTTPException(
            status_code=400,
            detail=(f"sales_type must be {GOAL_TABLES_SALES_TYPE!r}: the "
                    "seasonality tables are shared by every sales type and "
                    "hold retail's numbers"))

    store = await get_store()
    tables = await store.recalculate_goal_tables(include_weekly=True)
    growth = tables["yoy"]

    return {
        "status": "success",
        "message": "Seasonality indices and growth metrics recalculated",
        "summary": {
            "monthsCalculated": len(tables["seasonal"]),
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
    _gate=Depends(require_permission("dashboard")),
):
    """Generate smart goals for a specific future month. Reads only.

    `recalculate=true` used to rewrite the shared seasonality tables from a
    GET (OD-14 (i): a GET never writes). It is refused, for an admin too, and
    the answer names the route that does it.
    """
    try:
        sales_type = validate_sales_type(sales_type)
    except ValidationError as e:
        raise HTTPException(status_code=400, detail=str(e))
    if recalculate:
        raise HTTPException(
            status_code=400,
            detail=("recalculate is not accepted on a GET: POST "
                    "/api/goals/recalculate recomputes and stores the "
                    "seasonality tables (admin)"))

    store = await get_store()
    return await store.generate_smart_goals(year, month, sales_type)
