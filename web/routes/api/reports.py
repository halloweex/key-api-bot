"""Reports endpoints: summary, top products, CSV export."""
import csv
import io
import logging
from datetime import datetime as _datetime
from typing import Optional

from fastapi import APIRouter, Query, Request, HTTPException
from fastapi.responses import StreamingResponse

from web.services import dashboard_service
from ._deps import (
    limiter, get_store,
    validate_period, validate_source_id, validate_category_id,
    validate_brand_name, validate_limit, validate_sales_type,
    ValidationError,
)

router = APIRouter(prefix="/reports", tags=["reports"])
logger = logging.getLogger(__name__)


def _parse_common_params(
    period, start_date, end_date, source_id, category_id, brand, sales_type,
):
    """Validate and parse common report parameters."""
    validate_period(period)
    validate_source_id(source_id)
    validate_category_id(category_id)
    brand = validate_brand_name(brand)
    sales_type = validate_sales_type(sales_type)
    start, end = dashboard_service.parse_period(period, start_date, end_date)
    start_dt = _datetime.strptime(start, "%Y-%m-%d").date()
    end_dt = _datetime.strptime(end, "%Y-%m-%d").date()
    return start_dt, end_dt, source_id, category_id, brand, sales_type


@router.get("/marketing-summary")
@limiter.limit("30/minute")
async def get_marketing_summary(
    request: Request,
    period: Optional[str] = Query(None),
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
    sales_type: Optional[str] = Query("retail"),
):
    """Get marketing report for any date range with previous period and YoY comparison."""
    try:
        st = validate_sales_type(sales_type)
        if period:
            validate_period(period)
    except ValidationError as ex:
        raise HTTPException(status_code=400, detail=str(ex))

    start, end = dashboard_service.parse_period(period, start_date, end_date)
    start_dt = _datetime.strptime(start, "%Y-%m-%d").date()
    end_dt = _datetime.strptime(end, "%Y-%m-%d").date()

    store = await get_store()
    return await store.get_marketing_report_by_dates(start_dt, end_dt, st)


# ─── Ukrainian month names for CSV export ────────────────────────────────

_MONTH_NAMES_UK = [
    "СІЧЕНЬ", "ЛЮТИЙ", "БЕРЕЗЕНЬ", "КВІТЕНЬ", "ТРАВЕНЬ", "ЧЕРВЕНЬ",
    "ЛИПЕНЬ", "СЕРПЕНЬ", "ВЕРЕСЕНЬ", "ЖОВТЕНЬ", "ЛИСТОПАД", "ГРУДЕНЬ",
]


def _fmt_currency(v: float) -> str:
    return f"₴{v:,.0f}"


def _fmt_pct_change(cur: float, prev: float) -> str:
    if prev == 0:
        return "+100%" if cur > 0 else "0%"
    return f"{(cur - prev) / prev * 100:.2f}%"


def _fmt_goal(v) -> str:
    if v is None:
        return ""
    if v >= 1_000_000:
        m = v / 1_000_000
        return f"₴{m:.1f} млн" if m % 1 else f"₴{m:.0f} млн"
    return _fmt_currency(v)


@router.get("/marketing-summary/export/csv")
@limiter.limit("10/minute")
async def export_marketing_csv(
    request: Request,
    period: Optional[str] = Query(None),
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
    sales_type: Optional[str] = Query("retail"),
):
    """Export marketing report as CSV for the selected period."""
    try:
        st = validate_sales_type(sales_type)
        if period:
            validate_period(period)
    except ValidationError as ex:
        raise HTTPException(status_code=400, detail=str(ex))

    start, end = dashboard_service.parse_period(period, start_date, end_date)
    start_dt = _datetime.strptime(start, "%Y-%m-%d").date()
    end_dt = _datetime.strptime(end, "%Y-%m-%d").date()

    store = await get_store()
    report = await store.get_marketing_report_by_dates(start_dt, end_dt, st)

    def _fmt_range(sd: str, ed: str) -> str:
        if sd == ed:
            return sd
        return f"{sd} — {ed}"

    cur_label = _fmt_range(report["start_date"], report["end_date"])
    prev_label = _fmt_range(report["prev_start_date"], report["prev_end_date"])
    yoy_label = _fmt_range(report["yoy_start_date"], report["yoy_end_date"])

    output = io.StringIO()
    writer = csv.writer(output)

    # Header
    writer.writerow([f"Маркетинговий звіт: {cur_label}"])
    writer.writerow([])

    # Section 1: General Sales
    writer.writerow(["1. ЗАГАЛЬНІ ПРОДАЖІ"])
    has_goal = report["general_sales"]["monthly_goal"] is not None
    header = ["Показник", cur_label, prev_label, "Зміна, %", yoy_label, "Зміна YoY"]
    if has_goal:
        header.append("Ціль місяця")
    writer.writerow(header)

    general_rows = [
        ("Виручка (грн)", "revenue", _fmt_currency, True),
        ("К-сть замовлень", "orders", lambda v: f"{v:,}", False),
        ("Середній чек (грн)", "avg_check", _fmt_currency, False),
        ("К-сть клієнтів", "customers", lambda v: f"{v:,}", False),
        ("Нові клієнти", "new_customers", lambda v: f"{v:,}", False),
        ("Клієнти, що повернулись", "returning_customers", lambda v: f"{v:,}", False),
        ("% повернення", "return_rate", lambda v: f"{v:.0f}%", False),
    ]
    for label, key, fmt, show_goal in general_rows:
        cur = report["general_sales"]["current"][key]
        prev = report["general_sales"]["previous"][key]
        yoy = report["general_sales"]["year_ago"][key]
        row = [label, fmt(cur), fmt(prev), _fmt_pct_change(cur, prev), fmt(yoy), _fmt_pct_change(cur, yoy)]
        if has_goal:
            row.append(_fmt_goal(report["general_sales"]["monthly_goal"]) if show_goal else "")
        writer.writerow(row)

    writer.writerow([])

    # Section 2: Brands
    writer.writerow(["2. ПРОДАЖІ ПО БРЕНДАХ"])
    writer.writerow(["Бренд", "Виручка (грн)", "К-сть замовлень", "Ср. чек (грн)", "% від загального"])
    for b in report["brands"]:
        writer.writerow([b["brand"], _fmt_currency(b["revenue"]), f"{b['orders']:,}", _fmt_currency(b["avg_check"]), f"{b['share_pct']}%"])

    writer.writerow([])

    # Section 3: Sources
    writer.writerow(["3. КАНАЛИ / ДЖЕРЕЛА"])
    writer.writerow(["Канал", "К-сть замовлень", "Виручка (грн)", "% замовлень", "% виручки"])
    for s in report["sources"]:
        writer.writerow([s["source_name"], f"{s['orders']:,}", _fmt_currency(s["revenue"]), f"{s['orders_pct']}%", f"{s['revenue_pct']}%"])

    output.seek(0)
    filename = f"marketing_report_{start}_{end}.csv"
    return StreamingResponse(
        iter([output.getvalue().encode("utf-8-sig")]),
        media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": f"attachment; filename={filename}"},
    )


@router.get("/summary")
@limiter.limit("30/minute")
async def get_report_summary(
    request: Request,
    period: Optional[str] = Query(None),
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
    source_id: Optional[int] = Query(None),
    category_id: Optional[int] = Query(None),
    brand: Optional[str] = Query(None),
    sales_type: Optional[str] = Query("retail"),
):
    """Get report summary with per-source breakdown."""
    try:
        s, e, src, cat, br, st = _parse_common_params(
            period, start_date, end_date, source_id, category_id, brand, sales_type,
        )
    except ValidationError as ex:
        raise HTTPException(status_code=400, detail=str(ex))

    store = await get_store()
    return await store.get_report_summary(s, e, st, src, cat, br)


@router.get("/top-products")
@limiter.limit("30/minute")
async def get_report_top_products(
    request: Request,
    period: Optional[str] = Query(None),
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
    source_id: Optional[int] = Query(None),
    category_id: Optional[int] = Query(None),
    brand: Optional[str] = Query(None),
    sales_type: Optional[str] = Query("retail"),
    limit: int = Query(10),
):
    """Get top products ranked by quantity."""
    try:
        s, e, src, cat, br, st = _parse_common_params(
            period, start_date, end_date, source_id, category_id, brand, sales_type,
        )
        limit = validate_limit(limit, max_value=50)
    except ValidationError as ex:
        raise HTTPException(status_code=400, detail=str(ex))

    store = await get_store()
    return await store.get_report_top_products(s, e, st, src, cat, br, limit)


@router.get("/all-products")
@limiter.limit("30/minute")
async def get_report_all_products(
    request: Request,
    period: Optional[str] = Query(None),
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
    source_id: Optional[int] = Query(None),
    category_id: Optional[int] = Query(None),
    brand: Optional[str] = Query(None),
    sales_type: Optional[str] = Query("retail"),
):
    """Get all products (no limit) for full breakdown report."""
    try:
        s, e, src, cat, br, st = _parse_common_params(
            period, start_date, end_date, source_id, category_id, brand, sales_type,
        )
    except ValidationError as ex:
        raise HTTPException(status_code=400, detail=str(ex))

    store = await get_store()
    return await store.get_report_top_products(s, e, st, src, cat, br, limit=5000)


@router.get("/export/csv")
@limiter.limit("10/minute")
async def export_report_csv(
    request: Request,
    type: str = Query(..., description="Report type: summary or top_products"),
    period: Optional[str] = Query(None),
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
    source_id: Optional[int] = Query(None),
    category_id: Optional[int] = Query(None),
    brand: Optional[str] = Query(None),
    sales_type: Optional[str] = Query("retail"),
    limit: int = Query(10),
):
    """Export report data as CSV file."""
    if type not in ("summary", "top_products"):
        raise HTTPException(status_code=400, detail="type must be 'summary' or 'top_products'")

    try:
        s, e, src, cat, br, st = _parse_common_params(
            period, start_date, end_date, source_id, category_id, brand, sales_type,
        )
        if type == "top_products":
            limit = validate_limit(limit, max_value=5000)
    except ValidationError as ex:
        raise HTTPException(status_code=400, detail=str(ex))

    store = await get_store()
    output = io.StringIO()
    writer = csv.writer(output)

    source_names = {1: "Instagram", 2: "Telegram", 4: "Shopify"}

    if type == "summary":
        data = await store.get_report_summary(s, e, st, src, cat, br)
        products_by_source = await store.get_report_products_by_source(s, e, st)

        # Header matching bot Excel format
        display_date = f"{s} to {e}" if s != e else str(s)
        writer.writerow([f"Sales Summary for {display_date} (Timezone: Europe/Kyiv)"])
        writer.writerow([f"Total Orders: {data['totals']['orders_count']}"])
        writer.writerow([])

        # Per-source sections with product tables
        for src_row in data["sources"]:
            sid = src_row["source_id"]
            writer.writerow([f"Source: {src_row['source_name']}"])
            writer.writerow([f"Total Orders: {src_row['orders_count']}"])
            writer.writerow([f"Average Check: {src_row['avg_check']:.2f} UAH"])
            writer.writerow([f"Returns: {src_row['returns_count']} ({src_row['return_rate']}%)"])
            writer.writerow(["Product", "Quantity"])
            for product in products_by_source.get(sid, []):
                writer.writerow([product["product_name"], product["quantity"]])
            writer.writerow([])

        filename = f"sales_report_{s}_{e}.csv"
    else:
        products = await store.get_report_top_products(s, e, st, src, cat, br, limit)
        writer.writerow(["#", "Product", "SKU", "Qty", "%", "Revenue", "Orders"])
        for p in products:
            writer.writerow([
                p["rank"],
                p["product_name"],
                p["sku"],
                p["quantity"],
                p["percentage"],
                p["revenue"],
                p["orders_count"],
            ])
        filename = f"top_products_{s}_{e}.csv"

    output.seek(0)
    return StreamingResponse(
        iter([output.getvalue().encode("utf-8-sig")]),
        media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": f"attachment; filename={filename}"},
    )
