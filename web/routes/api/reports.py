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
            limit = validate_limit(limit, max_value=50)
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
        iter([output.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename={filename}"},
    )
