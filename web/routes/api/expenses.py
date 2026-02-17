"""Expense types, expense summary, profit analysis, manual expenses endpoints."""
from datetime import date as date_type
from fastapi import APIRouter, Query, Request, HTTPException
from pydantic import BaseModel, Field
from typing import Optional

from web.services import dashboard_service
from ._deps import (
    limiter, get_store,
    validate_period, validate_source_id, validate_sales_type,
    ValidationError,
)


class CreateExpenseRequest(BaseModel):
    expense_date: str = Field(..., pattern=r'^\d{4}-\d{2}-\d{2}$')
    category: str
    expense_type: str
    amount: float = Field(..., gt=0)
    currency: str = "UAH"
    note: Optional[str] = None
    platform: Optional[str] = None

router = APIRouter()


@router.get("/expense-types")
@limiter.limit("60/minute")
async def get_expense_types(request: Request):
    """Get list of expense types for filter dropdown."""
    return await dashboard_service.get_expense_types()


@router.get("/expenses/summary")
@limiter.limit("30/minute")
async def get_expense_summary(
    request: Request,
    period: Optional[str] = Query(None),
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
    source_id: Optional[int] = Query(None),
    expense_type_id: Optional[int] = Query(None),
    sales_type: Optional[str] = Query("retail"),
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
        start, end, source_id=source_id, expense_type_id=expense_type_id, sales_type=sales_type,
    )


@router.get("/expenses/profit")
@limiter.limit("30/minute")
async def get_profit_analysis(
    request: Request,
    period: Optional[str] = Query(None),
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
    source_id: Optional[int] = Query(None),
    sales_type: Optional[str] = Query("retail"),
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
        start, end, source_id=source_id, sales_type=sales_type,
    )


@router.get("/expenses")
@limiter.limit("30/minute")
async def get_expenses(
    request: Request,
    period: Optional[str] = Query(None),
    category: Optional[str] = Query(None),
    limit: int = Query(50, ge=1, le=200),
):
    """Get manual expenses list with optional filters."""
    from datetime import date, timedelta, datetime
    from zoneinfo import ZoneInfo

    store = await get_store()

    start_date = None
    end_date = None

    if period:
        tz = ZoneInfo("Europe/Kyiv")
        today = datetime.now(tz).date()

        if period == "today":
            start_date = end_date = today
        elif period == "yesterday":
            start_date = end_date = today - timedelta(days=1)
        elif period == "week":
            start_date = today - timedelta(days=today.weekday())
            end_date = today
        elif period == "last_week":
            end_date = today - timedelta(days=today.weekday() + 1)
            start_date = end_date - timedelta(days=6)
        elif period == "month":
            start_date = today.replace(day=1)
            end_date = today
        elif period == "last_month":
            first_of_month = today.replace(day=1)
            end_date = first_of_month - timedelta(days=1)
            start_date = end_date.replace(day=1)

    expenses = await store.list_expenses(
        start_date=start_date, end_date=end_date, category=category, limit=limit,
    )
    summary = await store.get_expenses_summary(start_date=start_date, end_date=end_date)

    return {"expenses": expenses, "summary": summary, "period": period, "category": category}


@router.post("/expenses")
@limiter.limit("30/minute")
async def create_expense(request: Request, body: CreateExpenseRequest):
    """Create a manual expense."""
    from datetime import datetime

    store = await get_store()

    # Validate platform
    valid_platforms = {None, "facebook", "tiktok", "google"}
    if body.platform and body.platform not in valid_platforms:
        raise HTTPException(status_code=400, detail=f"Invalid platform: {body.platform}")

    expense_date = datetime.strptime(body.expense_date, "%Y-%m-%d").date()
    result = await store.add_expense(
        expense_date=expense_date,
        category=body.category,
        expense_type=body.expense_type,
        amount=body.amount,
        currency=body.currency,
        note=body.note,
        platform=body.platform,
    )
    return result


@router.delete("/expenses/{expense_id}")
@limiter.limit("30/minute")
async def delete_expense(request: Request, expense_id: int):
    """Delete a manual expense."""
    store = await get_store()
    deleted = await store.delete_expense(expense_id)
    if not deleted:
        raise HTTPException(status_code=404, detail="Expense not found")
    return {"success": True, "id": expense_id}
