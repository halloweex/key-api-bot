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
    year: Optional[int] = Query(None),
    month: Optional[int] = Query(None),
    sales_type: Optional[str] = Query("retail"),
):
    """Get monthly marketing report: general sales, brands, sources."""
    from datetime import date as _d
    from zoneinfo import ZoneInfo
    now = _datetime.now(ZoneInfo("Europe/Kyiv"))
    y = year or now.year
    m = month or now.month
    if not (1 <= m <= 12):
        raise HTTPException(status_code=400, detail="month must be 1-12")
    if not (2020 <= y <= 2100):
        raise HTTPException(status_code=400, detail="year out of range")

    try:
        st = validate_sales_type(sales_type)
    except ValidationError as ex:
        raise HTTPException(status_code=400, detail=str(ex))

    store = await get_store()
    return await store.get_marketing_report(y, m, st)


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
    year: Optional[int] = Query(None),
    month: Optional[int] = Query(None),
    months: int = Query(3, ge=1, le=6),
    sales_type: Optional[str] = Query("retail"),
):
    """Export marketing report as CSV — multiple months side by side."""
    from zoneinfo import ZoneInfo
    now = _datetime.now(ZoneInfo("Europe/Kyiv"))
    start_year = year or now.year
    start_month = month or now.month

    try:
        st = validate_sales_type(sales_type)
    except ValidationError as ex:
        raise HTTPException(status_code=400, detail=str(ex))

    store = await get_store()

    # Fetch data for each month
    reports = []
    y, m = start_year, start_month
    for _ in range(months):
        reports.append(await store.get_marketing_report(y, m, st))
        m += 1
        if m > 12:
            m = 1
            y += 1

    # Build CSV — months side by side, separated by empty column
    output = io.StringIO()
    writer = csv.writer(output)
    cols_per_month = 7  # 7 data columns per month
    gap = 1  # 1 empty column between months

    def _row(*month_cells):
        """Build a row with month blocks side by side."""
        row = []
        for i, cells in enumerate(month_cells):
            row.extend(cells)
            # Pad to cols_per_month
            row.extend([""] * (cols_per_month - len(cells)))
            if i < len(month_cells) - 1:
                row.extend([""] * gap)
        writer.writerow(row)

    # Row 1: Month names
    _row(*[[_MONTH_NAMES_UK[r["month"] - 1]] for r in reports])

    # Section 1: General Sales
    _row(*[["1. ЗАГАЛЬНІ ПРОДАЖІ"] for _ in reports])
    _row(*[["Показник", "Поточний місяць", "Попередній місяць", "Зміна, %", "Рік тому", "Зміна YoY", "Ціль місяця"] for _ in reports])

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
        cells_per_month = []
        for r in reports:
            cur = r["general_sales"]["current"][key]
            prev = r["general_sales"]["previous"][key]
            yoy = r["general_sales"]["year_ago"][key]
            goal_str = _fmt_goal(r["general_sales"]["monthly_goal"]) if show_goal else ""
            cells_per_month.append([label, fmt(cur), fmt(prev), _fmt_pct_change(cur, prev), fmt(yoy), _fmt_pct_change(cur, yoy), goal_str])
        _row(*cells_per_month)

    # Empty row
    writer.writerow([])

    # Section 2: Brands
    _row(*[["2. ПРОДАЖІ ПО БРЕНДАХ"] for _ in reports])
    _row(*[["Бренд", "Виручка (грн)", "К-сть замовлень", "Ср. чек (грн)", "% від загального"] for _ in reports])

    max_brands = max(len(r["brands"]) for r in reports) if reports else 0
    for i in range(max_brands):
        cells_per_month = []
        for r in reports:
            if i < len(r["brands"]):
                b = r["brands"][i]
                cells_per_month.append([b["brand"], _fmt_currency(b["revenue"]), f"{b['orders']:,}", _fmt_currency(b["avg_check"]), f"{b['share_pct']}%"])
            else:
                cells_per_month.append(["", "", "", "", ""])
        _row(*cells_per_month)

    # Empty row
    writer.writerow([])

    # Section 3: Sources
    _row(*[["3. КАНАЛИ / ДЖЕРЕЛА"] for _ in reports])
    _row(*[["Канал", "К-сть замовлень", "Виручка (грн)", "% замовлень", "% виручки"] for _ in reports])

    max_sources = max(len(r["sources"]) for r in reports) if reports else 0
    for i in range(max_sources):
        cells_per_month = []
        for r in reports:
            if i < len(r["sources"]):
                s = r["sources"][i]
                cells_per_month.append([s["source_name"], f"{s['orders']:,}", _fmt_currency(s["revenue"]), f"{s['orders_pct']}%", f"{s['revenue_pct']}%"])
            else:
                cells_per_month.append(["", "", "", "", ""])
        _row(*cells_per_month)

    output.seek(0)
    m_start = _MONTH_NAMES_UK[start_month - 1].lower()
    filename = f"marketing_report_{m_start}_{start_year}.csv"
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
