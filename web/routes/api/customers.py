"""Customer insights, cohort retention, purchase timing, LTV, at-risk endpoints."""
import csv
import io
import logging
from datetime import date as _date, datetime as _datetime

from fastapi import APIRouter, Query, Request, HTTPException, Depends
from fastapi.responses import StreamingResponse
from typing import Optional

from core.repositories.customers import SMS_LTV_BASES, SMS_TIER_DEFAULTS
from core.turbosms import TurboSmsClient, TurboSmsError
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
    freeze: bool = Query(
        False,
        description="Record this roster as the campaign's control group. Set it on "
                    "the export you actually send.",
    ),
    overwrite: bool = Query(False, description="Replace an existing frozen roster"),
    promocode: Optional[str] = Query(
        None, max_length=40,
        description="Code carried by this campaign, for direct attribution",
    ),
    admin: dict = Depends(require_admin),
):
    """
    Export the SMS campaign list as CSV.

    By default only the `target` group is exported — the holdout must stay
    unmessaged for the campaign uplift to be measurable. Pass
    `include_holdout=true` to get both groups (e.g. to archive the split).

    Pass `freeze=true` on the export you actually send. The eligible population
    shifts daily, so a roster that is not recorded now cannot be reconstructed
    later — and without it there is no control group to measure against.
    """
    store = await get_store()
    data = await store.get_sms_segments(include_customers=True, limit=limit, **criteria)

    # Freezing must happen before the file leaves — the roster recorded here is
    # the only control group that will exist when results are measured.
    frozen = None
    if freeze:
        if data["truncated"]:
            raise HTTPException(
                status_code=400,
                detail="refusing to freeze a truncated roster — raise `limit` so the "
                       "whole segment is recorded",
            )
        try:
            frozen = await store.freeze_sms_campaign(
                campaign=criteria["campaign"],
                customers=data["customers"],
                criteria=data["criteria"],
                ltv_basis=criteria["ltv_basis"],
                sales_type=criteria["sales_type"],
                holdout_pct=criteria["holdout_pct"],
                promocode=promocode,
                overwrite=overwrite,
            )
        except ValueError as e:
            raise HTTPException(status_code=409, detail=str(e))

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
        # What they bought last — the hook the message is written around.
        "last_order_id", "last_order_total", "last_order_item_count", "last_order_items",
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
            c["lastOrderId"] or "",
            c["lastOrderTotal"],
            c["lastOrderItemCount"],
            c["lastOrderItems"] or "",
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
            "X-Campaign-Frozen": str(bool(frozen)).lower(),
            "X-Campaign-Holdout": str(frozen["totals"]["holdout"]) if frozen else "0",
        },
    )


@router.post("/customers/sms-campaigns/{campaign}/sent")
@limiter.limit("20/minute")
async def mark_sms_campaign_sent(
    request: Request,
    campaign: str,
    sent_at: Optional[str] = Query(
        None, description="ISO timestamp; defaults to now",
    ),
    admin: dict = Depends(require_admin),
):
    """
    Record when the campaign file actually went to the SMS provider.

    Results are measured from this moment, not from the export — the two can be
    days apart, and measuring from the wrong one invents an effect that isn't there.
    """
    parsed = None
    if sent_at:
        try:
            parsed = _datetime.fromisoformat(sent_at)
        except ValueError:
            raise HTTPException(
                status_code=400, detail="sent_at must be an ISO timestamp",
            )

    store = await get_store()
    try:
        result = await store.mark_sms_campaign_sent(campaign, parsed)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))

    logger.info(
        "SMS campaign marked sent: user=%s campaign=%s at=%s",
        admin.get("user_id"), campaign, result["sentAt"],
    )
    return result


@router.post("/customers/sms-campaigns/{campaign}/send")
@limiter.limit("3/minute")
async def send_sms_campaign(
    request: Request,
    campaign: str,
    text: str = Query(..., min_length=1, max_length=600),
    admin: dict = Depends(require_admin),
):
    """
    Send the campaign's target group through TurboSMS.

    Only the target arm is sent — the control must stay unmessaged for the
    result to mean anything. The gateway's per-recipient answer is recorded:
    message ids for tracking delivery, and stoplist refusals as opt-outs, so
    those people are never selected again.

    Sending twice is refused: the campaign is stamped sent on the first pass.
    """
    store = await get_store()
    try:
        targets = await store.get_sms_campaign_targets(campaign)
    except ValueError as e:
        status = 404 if "not frozen" in str(e) else 409
        raise HTTPException(status_code=status, detail=str(e))

    if not targets:
        raise HTTPException(status_code=409, detail="campaign has no target recipients")

    by_phone = {t["phone"]: t["buyerId"] for t in targets}

    try:
        async with TurboSmsClient() as client:
            results = await client.send(list(by_phone), text)
    except TurboSmsError as e:
        # Nothing is stamped sent — the campaign can be retried once fixed.
        logger.error("TurboSMS send failed: campaign=%s error=%s", campaign, e)
        raise HTTPException(status_code=502, detail=str(e))

    accepted, failed, stoplisted = {}, {}, []
    for r in results:
        buyer_id = by_phone.get(r.phone)
        if buyer_id is None:
            continue
        if r.accepted:
            accepted[buyer_id] = r.message_id
        elif r.stoplisted:
            stoplisted.append(buyer_id)
        else:
            failed[buyer_id] = r.status or f"code {r.code}"

    summary = await store.record_sms_send(campaign, accepted, stoplisted, failed)

    logger.info(
        "SMS campaign sent: user=%s campaign=%s accepted=%d stoplisted=%d failed=%d",
        admin.get("user_id"), campaign,
        summary["accepted"], summary["stoplisted"], summary["failed"],
    )
    return summary


@router.post("/customers/sms-campaigns/optout")
@limiter.limit("30/minute")
async def add_marketing_optout(
    request: Request,
    buyer_id: int = Query(..., ge=1),
    phone: Optional[str] = Query(None, max_length=20),
    reason: str = Query("manual", max_length=40),
    admin: dict = Depends(require_admin),
):
    """Record that a customer asked not to receive marketing SMS."""
    store = await get_store()
    result = await store.add_marketing_optout(
        buyer_id=buyer_id, phone=phone, reason=reason,
        source=str(admin.get("user_id") or "dashboard"),
    )
    logger.info("Marketing opt-out: user=%s buyer=%s reason=%s",
                admin.get("user_id"), buyer_id, reason)
    return result


@router.get("/customers/sms-campaigns/{campaign}/results")
@limiter.limit("30/minute")
async def get_sms_campaign_results(
    request: Request,
    campaign: str,
    window_days: int = Query(30, ge=1, le=180),
    delivered_only: bool = Query(
        False,
        description="Restrict the target arm to confirmed deliveries. Optimistic "
                    "bound, not a clean randomised comparison — see the docs.",
    ),
    admin: dict = Depends(require_admin),
):
    """
    Measure a campaign: the messaged group against the control.

    The target group's own conversion is not a result — most of it would have
    happened anyway. What this returns is the *difference*, with a 95% interval
    and a p-value, per tier and overall. When the interval spans zero the
    campaign has not been shown to have done anything, whatever the raw rates
    look like.

    Requires the campaign to be marked sent; without a send date there is no
    window to measure over.
    """
    store = await get_store()
    try:
        return await store.get_sms_campaign_results(
            campaign, window_days=window_days, delivered_only=delivered_only,
        )
    except ValueError as e:
        # Unknown campaign is 404; frozen-but-unsent is a state problem, not a
        # missing resource, so it answers 409.
        status = 404 if "not frozen" in str(e) else 409
        raise HTTPException(status_code=status, detail=str(e))


@router.get("/customers/sms-campaigns")
@limiter.limit("30/minute")
async def list_sms_campaigns(
    request: Request,
    admin: dict = Depends(require_admin),
):
    """List frozen campaigns with their roster sizes and send dates."""
    store = await get_store()
    return {"campaigns": await store.list_sms_campaigns()}
