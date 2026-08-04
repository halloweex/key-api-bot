"""Customer insights, cohort retention, purchase timing, LTV, at-risk endpoints."""
import csv
import io
import logging
from datetime import date as _date

from fastapi import APIRouter, Query, Request, HTTPException, Depends
from fastapi.responses import StreamingResponse
from typing import Optional

from core.repositories.customers import SMS_LTV_BASES, SMS_TIER_DEFAULTS
from web.routes.auth import require_admin
from web.services import dashboard_service
from ._deps import (
    limiter, get_store,
    validate_period, validate_source_id, validate_brand_name, validate_sales_type,
    validate_promocode,
    ValidationError,
)

router = APIRouter()
logger = logging.getLogger(__name__)


@router.get("/customers/insights")
@limiter.limit("30/minute")
async def get_customer_insights(
    request: Request,
    period: Optional[str] = Query(None),
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
    source_id: Optional[int] = Query(None),
    brand: Optional[str] = Query(None),
    promocode: Optional[str] = Query(None),
    sales_type: Optional[str] = Query("retail"),
):
    """Get customer insights: new vs returning, AOV trend, repeat rate."""
    try:
        validate_period(period)
        validate_source_id(source_id)
        brand = validate_brand_name(brand)
        promocode = validate_promocode(promocode)
        sales_type = validate_sales_type(sales_type)
    except ValidationError as e:
        raise HTTPException(status_code=400, detail=str(e))

    start, end = dashboard_service.parse_period(period, start_date, end_date)
    return await dashboard_service.get_customer_insights(
        start, end, brand=brand, source_id=source_id, sales_type=sales_type,
        promocode=promocode,
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


# ─── SMS campaign segments ───────────────────────────────────────────────
# These expose customer names and phone numbers, so both endpoints stack
# require_admin on top of the api_gate session check.

_SMS_TIERS = ("VIP", "CORE", "REACTIVATION")

_CAMPAIGN_PATTERN = r"^[A-Za-z0-9][A-Za-z0-9_-]{0,39}$"


def _sms_segment_params(
    max_recency_days: int = Query(270, ge=30, le=730),
    ltv_basis: str = Query(
        "revenue",
        description="revenue or margin — which lifetime value drives tier assignment",
    ),
    vip_ltv: Optional[float] = Query(
        None, ge=0, le=10_000_000,
        description="VIP cut-off; defaults to 10000 (revenue) / 5500 (margin)",
    ),
    core_ltv: Optional[float] = Query(
        None, ge=0, le=10_000_000,
        description="CORE cut-off; defaults to 5000 (revenue) / 2750 (margin)",
    ),
    core_min_orders: int = Query(2, ge=2, le=50),
    reactivation_max_recency: int = Query(120, ge=7, le=730),
    sales_type: Optional[str] = Query("retail"),
    holdout_pct: int = Query(10, ge=0, le=50),
    campaign: str = Query("default", pattern=_CAMPAIGN_PATTERN),
    tier: Optional[str] = Query(None),
) -> dict:
    """Validate and normalise the segmentation criteria shared by both endpoints."""
    try:
        sales_type = validate_sales_type(sales_type)
    except ValidationError as e:
        raise HTTPException(status_code=400, detail=str(e))

    if ltv_basis not in SMS_LTV_BASES:
        raise HTTPException(
            status_code=400,
            detail=f"ltv_basis must be one of {', '.join(SMS_LTV_BASES)}",
        )

    if tier is not None:
        tier = tier.upper()
        if tier not in _SMS_TIERS:
            raise HTTPException(
                status_code=400, detail=f"tier must be one of {', '.join(_SMS_TIERS)}",
            )

    # Thresholds are basis-specific, so resolve defaults before comparing them.
    defaults = SMS_TIER_DEFAULTS[ltv_basis]
    if vip_ltv is None:
        vip_ltv = defaults["vip"]
    if core_ltv is None:
        core_ltv = defaults["core"]

    if core_ltv > vip_ltv:
        raise HTTPException(
            status_code=400, detail="core_ltv must not exceed vip_ltv",
        )
    if reactivation_max_recency > max_recency_days:
        raise HTTPException(
            status_code=400,
            detail="reactivation_max_recency must not exceed max_recency_days",
        )

    return {
        "max_recency_days": max_recency_days,
        "ltv_basis": ltv_basis,
        "vip_ltv": vip_ltv,
        "core_ltv": core_ltv,
        "core_min_orders": core_min_orders,
        "reactivation_max_recency": reactivation_max_recency,
        "sales_type": sales_type,
        "holdout_pct": holdout_pct,
        "campaign": campaign,
        "tier": tier,
    }


@router.get("/customers/sms-segments")
@limiter.limit("20/minute")
async def get_sms_segments(
    request: Request,
    criteria: dict = Depends(_sms_segment_params),
    include_customers: bool = Query(False),
    limit: int = Query(20000, ge=1, le=100000),
    admin: dict = Depends(require_admin),
):
    """
    RFM segments for an SMS campaign, split into VIP / CORE / REACTIVATION.

    `ltv_basis=margin` ranks customers by contribution margin instead of
    revenue, which stops low-margin buyers from soaking up campaign budget.
    Both figures come back either way, so the two bases can be compared on the
    same people.

    Returns per-tier sizes by default. Pass `include_customers=true` for the
    rows themselves (names and phone numbers), or use the `/export/csv`
    variant to download them.
    """
    store = await get_store()
    return await store.get_sms_segments(
        include_customers=include_customers, limit=limit, **criteria,
    )


@router.get("/customers/sms-segments/export/csv")
@limiter.limit("5/minute")
async def export_sms_segments_csv(
    request: Request,
    criteria: dict = Depends(_sms_segment_params),
    include_holdout: bool = Query(False),
    limit: int = Query(50000, ge=1, le=100000),
    admin: dict = Depends(require_admin),
):
    """
    Export the SMS campaign list as CSV.

    By default only the `target` group is exported — the holdout must stay
    unmessaged for the campaign uplift to be measurable. Pass
    `include_holdout=true` to get both groups (e.g. to archive the split).
    """
    store = await get_store()
    data = await store.get_sms_segments(include_customers=True, limit=limit, **criteria)

    rows = data["customers"]
    if not include_holdout:
        rows = [c for c in rows if c["assignment"] == "target"]

    # Exports carry customer PII — record who pulled which list.
    logger.info(
        "SMS segment export: user=%s campaign=%s tier=%s rows=%d holdout=%s",
        admin.get("user_id"), criteria["campaign"], criteria["tier"] or "all",
        len(rows), include_holdout,
    )

    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow([
        "buyer_id", "full_name", "phone", "city", "tier", "assignment",
        "orders", "ltv", "ltv_basis", "avg_order_value",
        # Both bases travel with the file so the list can be re-ranked in a
        # spreadsheet without another export.
        "revenue_ltv", "margin_ltv", "margin_pct", "cost_coverage_pct",
        "recency_days", "last_order_date", "first_order_date",
    ])
    for c in rows:
        writer.writerow([
            c["buyerId"],
            c["fullName"],
            # E.164 — what SMS gateways expect, and Excel keeps it as text
            # instead of mangling a 12-digit number into scientific notation.
            f"+{c['phone']}",
            c["city"] or "",
            c["tier"],
            c["assignment"],
            c["orders"],
            c["ltv"],
            criteria["ltv_basis"],
            c["avgOrderValue"],
            c["revenueLtv"],
            c["marginLtv"],
            "" if c["marginPct"] is None else c["marginPct"],
            c["costCoverage"],
            c["recencyDays"],
            c["lastOrderDate"] or "",
            c["firstOrderDate"] or "",
        ])

    tier_part = (criteria["tier"] or "all").lower()
    filename = (
        f"sms_{criteria['campaign']}_{tier_part}"
        f"_{criteria['ltv_basis']}_{_date.today()}.csv"
    )

    output.seek(0)
    return StreamingResponse(
        iter([output.getvalue().encode("utf-8-sig")]),
        media_type="text/csv; charset=utf-8",
        headers={
            "Content-Disposition": f"attachment; filename={filename}",
            "X-Segment-Rows": str(len(rows)),
            "X-Segment-Truncated": str(data["truncated"]).lower(),
        },
    )
