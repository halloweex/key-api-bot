"""Expense types, expense summary, profit analysis, manual expenses endpoints."""
import math

from fastapi import APIRouter, Query, Request, HTTPException, Depends
from pydantic import BaseModel, Field, field_validator
from typing import Optional

from core.filters import EARLIEST_DATE, LATEST_DATE, parse_period
from web.services import dashboard_service
from web.routes.auth import require_permission
from ._deps import (
    limiter, get_store,
    validate_period, validate_source_id, validate_sales_type, validate_db_int,
    ValidationError,
)


# `app.manual_expenses.amount` is NUMERIC(12,2). Past it, or a NaN/inf
# (`1e400` parses as inf), asyncpg refused the INSERT and the route answered
# 500 (L3-02).
MAX_EXPENSE_AMOUNT = 9_999_999_999.99


class CreateExpenseRequest(BaseModel):
    expense_date: str = Field(..., pattern=r'^\d{4}-\d{2}-\d{2}$')
    category: str = Field(..., max_length=200)
    expense_type: str = Field(..., max_length=200)
    amount: float = Field(..., gt=0, le=MAX_EXPENSE_AMOUNT, allow_inf_nan=False)
    currency: str = Field("UAH", max_length=10)
    note: Optional[str] = Field(None, max_length=2000)
    platform: Optional[str] = Field(None, max_length=50)

    @field_validator("amount", mode="before")
    @classmethod
    def _non_finite_as_text(cls, value):
        # `1e400` and `NaN` arrive from the JSON parser as a float inf/NaN.
        # Refused as such, the 422 carries the input back and cannot itself be
        # written as JSON — a 500 again. As text it is refused the same way
        # (`allow_inf_nan=False`) and the answer renders.
        if isinstance(value, float) and not math.isfinite(value):
            return repr(value)
        return value

    @field_validator("category", "expense_type", "currency", "note", "platform")
    @classmethod
    def _no_nul(cls, value: Optional[str]) -> Optional[str]:
        # Postgres refuses NUL in text; the 500 it caused is a 422 here.
        if value is not None and "\x00" in value:
            raise ValueError("Must not contain a NUL character")
        return value

router = APIRouter()


@router.get("/expense-types")
@limiter.limit("60/minute")
async def get_expense_types(
    request: Request,
    _=Depends(require_permission("expenses", "view")),
):
    """Get list of expense types for filter dropdown. Requires expenses:view."""
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
    _=Depends(require_permission("expenses", "view")),
):
    """Get expense summary: breakdown by type, daily trend. Requires expenses:view."""
    try:
        validate_period(period)
        validate_source_id(source_id)
        validate_db_int(expense_type_id, "expense_type_id")
        sales_type = validate_sales_type(sales_type)
        start, end = dashboard_service.parse_period(period, start_date, end_date)
    except ValidationError as e:
        raise HTTPException(status_code=400, detail=str(e))

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
    _=Depends(require_permission("expenses", "view")),
):
    """Get profit analysis: revenue vs expenses comparison. Requires expenses:view."""
    try:
        validate_period(period)
        validate_source_id(source_id)
        sales_type = validate_sales_type(sales_type)
        start, end = dashboard_service.parse_period(period, start_date, end_date)
    except ValidationError as e:
        raise HTTPException(status_code=400, detail=str(e))

    return await dashboard_service.get_profit_analysis(
        start, end, source_id=source_id, sales_type=sales_type,
    )


@router.get("/expenses")
@limiter.limit("30/minute")
async def get_expenses(
    request: Request,
    period: Optional[str] = Query(None),
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
    category: Optional[str] = Query(None),
    limit: int = Query(50, ge=1, le=200),
    _=Depends(require_permission("expenses", "view")),
):
    """Get manual expenses list with optional filters. Requires expenses:view.

    The window is `parse_period`'s, as on every other route. This route
    carried a copy of its own that knew six of the eight periods and took no
    dates, so `last_7_days`, `last_28_days` and a custom range listed, and
    summed, every expense ever typed. With no period and no pair of dates it
    is the whole list, as it always was. `custom` is what a page loaded
    before this fix sends for a custom range: the dates decide, and without
    them it is the whole list too.
    """
    shortcut = None if period == "custom" else period
    start = end = None
    if shortcut or (start_date and end_date):
        try:
            validate_period(shortcut)
            window = parse_period(shortcut, start_date, end_date)
        except ValidationError as e:
            raise HTTPException(status_code=400, detail=str(e))
        start, end = window.start, window.end

    store = await get_store()
    expenses = await store.list_expenses(
        start_date=start, end_date=end, category=category, limit=limit,
    )
    summary = await store.get_expenses_summary(start_date=start, end_date=end)

    return {"expenses": expenses, "summary": summary, "period": period, "category": category}


@router.post("/expenses")
@limiter.limit("30/minute")
async def create_expense(
    request: Request,
    body: CreateExpenseRequest,
    _=Depends(require_permission("expenses", "edit")),
):
    """Create a manual expense. Requires expenses:edit permission."""
    from datetime import datetime

    store = await get_store()

    # Validate platform
    valid_platforms = {None, "facebook", "tiktok", "google"}
    if body.platform and body.platform not in valid_platforms:
        raise HTTPException(status_code=400, detail=f"Invalid platform: {body.platform}")

    # The pattern lets 2026-02-30 and 0000-01-01 through, and `strptime` then
    # raised out of the handler as a 500 (L3-02). The dashboard's window, as
    # every date parameter here (`core.filters`).
    try:
        expense_date = datetime.strptime(body.expense_date, "%Y-%m-%d").date()
    except ValueError:
        raise HTTPException(status_code=400,
                            detail="expense_date: Not a valid date (YYYY-MM-DD)")
    if not EARLIEST_DATE <= expense_date <= LATEST_DATE:
        raise HTTPException(
            status_code=400,
            detail=f"expense_date: Date must be between {EARLIEST_DATE} and {LATEST_DATE}")
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
async def delete_expense(
    request: Request,
    expense_id: int,
    _=Depends(require_permission("expenses", "delete")),
):
    """Delete a manual expense. Requires expenses:delete permission."""
    validate_db_int(expense_id, "expense_id", allow_none=False)
    store = await get_store()
    deleted = await store.delete_expense(expense_id)
    if not deleted:
        raise HTTPException(status_code=404, detail="Expense not found")
    return {"success": True, "id": expense_id}
