"""Customer insights, cohort retention, purchase timing, LTV, at-risk endpoints."""
import csv
import json
import io
import logging
import re
from datetime import date as _date, datetime as _datetime

from fastapi import APIRouter, Path, Query, Request, HTTPException, Depends
from fastapi.responses import JSONResponse, StreamingResponse
from pydantic import BaseModel, ConfigDict, Field, TypeAdapter
from pydantic import ValidationError as PydanticValidationError
from typing import Annotated, List, Optional

from core.gender import CONFIDENCE_LADDER, GENDERS
from core.repositories.customers import (
    BUILTIN_AUDIENCE_PRESETS, DEFAULT_WINDOW_DAYS, MAX_AUDIENCE_GROUPS,
    SMS_GROUPINGS, SMS_LTV_BASES, SMS_RECENT_CAMPAIGN_HOURS, SMS_TIER_DEFAULTS,
    SmsAudienceFilters, SmsAudienceGroup,
)
from core.turbosms import (
    PartialSendError, TurboSmsClient, TurboSmsConfig, TurboSmsError,
    ViberMessage, count_segments,
)
from web.routes.auth import has_permission, require_permission
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
    _gate=Depends(require_permission("dashboard")),
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
    _gate=Depends(require_permission("dashboard")),
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
    _gate=Depends(require_permission("dashboard")),
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
    _gate=Depends(require_permission("dashboard")),
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
    _gate=Depends(require_permission("dashboard")),
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
# These expose customer names and phone numbers and spend real money, so every
# one of them stacks the `sms` permission on top of the api_gate session check.
# The split between the two actions is deliberate:
#
#   view — roster sizes and past results, figures about people;
#   edit — the CSV of names and phone numbers, and anything that reaches a
#          customer's handset or the gateway's balance.
#
# It used to be require_admin throughout, which meant the only way to let
# somebody run a campaign was to hand over user management, expenses, margin
# and the internal sales_type with it. `marketer` is that grant without the
# rest; admins keep it through the same matrix.

_SMS_TIERS = ("VIP", "CORE", "REACTIVATION")

_CAMPAIGN_PATTERN = r"^[A-Za-z0-9][A-Za-z0-9_-]{0,39}$"


def _int_list(raw: Optional[str], label: str) -> list:
    """Parse a comma-separated list of ids from a query parameter."""
    if not raw:
        return []
    out = []
    for piece in raw.split(","):
        piece = piece.strip()
        if not piece:
            continue
        try:
            out.append(int(piece))
        except ValueError:
            raise HTTPException(
                status_code=400, detail=f"{label} must be a comma-separated list of ids",
            )
    return out


def _text_list(raw: Optional[str]) -> list:
    """Parse a comma-separated list of names, dropping blanks and duplicates."""
    if not raw:
        return []
    seen, out = set(), []
    for piece in raw.split(","):
        piece = piece.strip()
        if piece and piece.lower() not in seen:
            seen.add(piece.lower())
            out.append(piece)
    return out


# Every parameter that decides who is in an audience. A follow-up's audience is
# decided by the campaign it follows and nothing else, so any of these beside
# `follow_up_of` is refused rather than ignored: a filter that was silently
# dropped reads, to whoever typed it, as a filter that worked.
_AUDIENCE_SHAPING_PARAMS = frozenset({
    "max_recency_days", "ltv_basis", "vip_ltv", "core_ltv", "core_min_orders",
    "reactivation_max_recency", "tier", "grouping",
    "recency_min", "recency_max", "orders_min", "orders_max", "ltv_min", "ltv_max",
    "aov_min", "aov_max", "first_order_from", "first_order_to", "city", "gender",
    "gender_min_confidence", "brand", "category_id", "source_id", "promocode_used",
    "bought_within_days", "period_orders_min", "period_orders_max", "period_from",
    "period_to", "period_days", "or_groups",
})


# ─── Audience groups ──────────────────────────────────────────────────────
# An audience is one or more groups, a customer being in it if they match ANY
# of them. The first group is the flat parameters below, exactly as before
# groups existed — so every saved link, preset and past campaign still names
# one group and selects what it selected. Any further group travels in ONE
# query parameter, `or_groups`, as a JSON array of objects whose keys are the
# flat parameters' names. Still the query string, because the CSV is a link
# the browser follows and `api_gate` reads `sales_type` from it — which is why
# `sales_type` and everything else common to the campaign stays flat and is
# not a group's to set.

# A group's filters in the shape `or_groups` carries them. Every bound is the
# flat parameter's own (`tests/integration/test_sms_audience_groups_api.py`
# compares the two), and `extra="forbid"` because a key nobody reads is a
# condition somebody thinks is applied.
_Text = Annotated[str, Field(max_length=200)]

# The widest base window a group may ask for. Ten years reaches past the first
# order the warehouse holds, so it is how the page says "all time" for a group
# with no time condition of its own: every group has a window, and one wider
# than the whole history is the honest spelling of none. It was 730, which
# made "everybody who ever bought" unsayable once the history passed two
# years — and a group whose only condition is a period ("nothing since
# January") needs every customer in reach to count over.
_MAX_WINDOW_DAYS = 3650


class _AudienceGroupParams(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    max_recency_days: int = Field(DEFAULT_WINDOW_DAYS, ge=30, le=_MAX_WINDOW_DAYS)
    recency_min: Optional[int] = Field(None, ge=0, le=3650)
    recency_max: Optional[int] = Field(None, ge=0, le=3650)
    orders_min: Optional[int] = Field(None, ge=0, le=1000)
    orders_max: Optional[int] = Field(None, ge=0, le=1000)
    ltv_min: Optional[float] = Field(None, ge=-1_000_000, le=100_000_000)
    ltv_max: Optional[float] = Field(None, ge=-1_000_000, le=100_000_000)
    aov_min: Optional[float] = Field(None, ge=0, le=100_000_000)
    aov_max: Optional[float] = Field(None, ge=0, le=100_000_000)
    first_order_from: Optional[_date] = None
    first_order_to: Optional[_date] = None
    city: Optional[List[_Text]] = Field(None, max_length=50)
    gender: Optional[List[_Text]] = Field(None, max_length=2)
    gender_min_confidence: Optional[str] = Field(None, max_length=10)
    brand: Optional[List[_Text]] = Field(None, max_length=100)
    category_id: Optional[List[int]] = Field(None, max_length=100)
    source_id: Optional[List[int]] = Field(None, max_length=20)
    promocode_used: Optional[str] = Field(None, max_length=40)
    bought_within_days: Optional[int] = Field(None, ge=1, le=3650)
    period_orders_min: Optional[int] = Field(None, ge=1, le=1000)
    period_orders_max: Optional[int] = Field(None, ge=0, le=1000)
    period_from: Optional[_date] = None
    period_to: Optional[_date] = None
    period_days: Optional[int] = Field(None, ge=1, le=3650)


_EXTRA_GROUPS = TypeAdapter(List[_AudienceGroupParams])

# The longest `or_groups` accepted. Four groups of every filter at its widest
# would not reach it; it exists so a URL cannot carry a megabyte of JSON into
# a parser.
_OR_GROUPS_MAX_CHARS = 8_000


def _clean_texts(values) -> list:
    """Strip, drop blanks and case-insensitive duplicates, keep the order."""
    seen, out = set(), []
    for value in values or ():
        piece = value.strip()
        if piece and piece.lower() not in seen:
            seen.add(piece.lower())
            out.append(piece)
    return out


def _audience_group(values: dict, label: str) -> SmsAudienceGroup:
    """One group's validated conditions, from either transport.

    `values` holds the flat parameters' names, typed and with lists already
    split, so the first group and every `or_groups` entry pass the same rules
    and refuse in the same words — with the group named when there are
    several, because "orders_min must not exceed orders_max" is no help when
    four groups each have one. A lone group (`label=""`) refuses exactly as
    the audience did before groups existed.
    """
    def refuse(detail: str):
        raise HTTPException(
            status_code=400, detail=f"{label}: {detail}" if label else detail,
        )

    genders = tuple(g.lower() for g in _clean_texts(values.get("gender")))
    for g in genders:
        if g not in GENDERS:
            refuse(f"gender must be one of {', '.join(GENDERS)}")
    # Refused rows carry NULL, and NULL never satisfies `IN`. So there is no
    # spelling of this parameter that selects them, and offering one would be a
    # campaign addressed by gender to people whose gender nobody could decide.
    confidence = values.get("gender_min_confidence")
    if confidence is not None:
        confidence = confidence.strip().lower()
        if confidence not in CONFIDENCE_LADDER:
            refuse(f"gender_min_confidence must be one of {', '.join(CONFIDENCE_LADDER)}")

    # A window whose edges are the wrong way round returns nothing and looks
    # like a data problem, so it is refused with the reason instead.
    for lo, hi, name in (
        ("recency_min", "recency_max", "recency"),
        ("orders_min", "orders_max", "orders"),
        ("ltv_min", "ltv_max", "ltv"),
        ("aov_min", "aov_max", "aov"),
        ("first_order_from", "first_order_to", "first_order"),
        ("period_orders_min", "period_orders_max", "period_orders"),
    ):
        if values.get(lo) is not None and values.get(hi) is not None \
                and values[lo] > values[hi]:
            refuse(f"{name}_min must not exceed {name}_max")

    # Orders in a period: a date range (from, to, or both), or within N days —
    # one of the two shapes, and a count without either says nothing about
    # when. `period_days` ends the period today, so a `period_to` beside it
    # would be a second end.
    period_from, period_to = values.get("period_from"), values.get("period_to")
    period_days = values.get("period_days")
    if period_from is not None and period_days is not None:
        refuse("period_from and period_days are two ways to say one period — pick one")
    if period_to is not None and period_days is not None:
        refuse("period_to and period_days are two ways to end one period — pick one")
    if period_from is not None and period_to is not None and period_from > period_to:
        refuse("period_from must not exceed period_to")
    has_period = period_from is not None or period_to is not None \
        or period_days is not None
    for bound in ("period_orders_min", "period_orders_max"):
        if values.get(bound) is not None and not has_period:
            refuse(f"{bound} needs a period: period_from, period_to or period_days")

    filters = SmsAudienceFilters(
        recency_min_days=values.get("recency_min"),
        recency_max_days=values.get("recency_max"),
        orders_min=values.get("orders_min"),
        orders_max=values.get("orders_max"),
        ltv_min=values.get("ltv_min"),
        ltv_max=values.get("ltv_max"),
        aov_min=values.get("aov_min"),
        aov_max=values.get("aov_max"),
        first_order_from=values.get("first_order_from"),
        first_order_to=values.get("first_order_to"),
        cities=tuple(_clean_texts(values.get("city"))),
        genders=genders,
        gender_min_confidence=confidence,
        brands=tuple(_clean_texts(values.get("brand"))),
        category_ids=tuple(dict.fromkeys(values.get("category_id") or ())),
        source_ids=tuple(dict.fromkeys(values.get("source_id") or ())),
        promocode=(values.get("promocode_used") or "").strip() or None,
        bought_within_days=values.get("bought_within_days"),
        period_orders_min=values.get("period_orders_min"),
        period_orders_max=values.get("period_orders_max"),
        period_from=period_from,
        period_to=period_to,
        period_days=period_days,
    )
    return SmsAudienceGroup(
        max_recency_days=values.get("max_recency_days", DEFAULT_WINDOW_DAYS),
        filters=filters,
    )


def _extra_audience_groups(raw: Optional[str]) -> List[SmsAudienceGroup]:
    """Groups 2..N out of `or_groups`, or a 400 saying what is wrong with it."""
    if raw is None:
        return []
    if len(raw) > _OR_GROUPS_MAX_CHARS:
        raise HTTPException(status_code=400, detail="or_groups is too long")
    try:
        parsed = _EXTRA_GROUPS.validate_json(raw)
    except PydanticValidationError as e:
        first = e.errors()[0]
        loc = list(first.get("loc", ()))
        where = "or_groups"
        if loc and isinstance(loc[0], int):
            where = f"group {loc[0] + 2}"
            loc = loc[1:]
        field_name = ".".join(str(part) for part in loc)
        raise HTTPException(
            status_code=400,
            detail=(f"{where}: {field_name + ': ' if field_name else ''}"
                    f"{first.get('msg', 'invalid')}"),
        )
    if not parsed:
        raise HTTPException(
            status_code=400,
            detail="or_groups must hold at least one group — leave it out for one group",
        )
    if len(parsed) + 1 > MAX_AUDIENCE_GROUPS:
        raise HTTPException(
            status_code=400,
            detail=f"an audience holds at most {MAX_AUDIENCE_GROUPS} groups",
        )
    return [
        _audience_group(group.model_dump(), f"group {n}")
        for n, group in enumerate(parsed, start=2)
    ]


def _sms_segment_params(
    request: Request,
    max_recency_days: int = Query(270, ge=30, le=_MAX_WINDOW_DAYS),
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
    # Optional so a window narrower than the default does not become an error:
    # only a value somebody actually typed can contradict the window.
    #
    # Still capped at two years when the window goes to ten: the level is
    # one-order buyers recent enough to win back, and a single order three
    # years ago is not that. Unset it stays 120 whatever the window, so an
    # "all time" group does not put every one-order buyer of years past into
    # the reactivation level.
    reactivation_max_recency: Optional[int] = Query(None, ge=7, le=730),
    sales_type: Optional[str] = Query("retail"),
    holdout_pct: int = Query(10, ge=0, le=50),
    campaign: str = Query("default", pattern=_CAMPAIGN_PATTERN),
    tier: Optional[str] = Query(None),
    # ─── Audience filters ────────────────────────────────────────────────
    # Flat query parameters rather than a JSON body, and deliberately so: the
    # CSV download is a plain link the browser follows, and `api_gate` reads
    # `sales_type` from the query string. A body would have broken both.
    grouping: str = Query(
        "rfm",
        description="rfm — three value tiers; single — one arm of everyone the "
                    "filters kept",
    ),
    recency_min: Optional[int] = Query(None, ge=0, le=3650),
    recency_max: Optional[int] = Query(None, ge=0, le=3650),
    orders_min: Optional[int] = Query(None, ge=0, le=1000),
    orders_max: Optional[int] = Query(None, ge=0, le=1000),
    ltv_min: Optional[float] = Query(None, ge=-1_000_000, le=100_000_000),
    ltv_max: Optional[float] = Query(None, ge=-1_000_000, le=100_000_000),
    aov_min: Optional[float] = Query(None, ge=0, le=100_000_000),
    aov_max: Optional[float] = Query(None, ge=0, le=100_000_000),
    first_order_from: Optional[_date] = Query(None),
    first_order_to: Optional[_date] = Query(None),
    city: Optional[str] = Query(None, max_length=500),
    gender: Optional[str] = Query(None, max_length=20),
    gender_min_confidence: Optional[str] = Query(None, max_length=10),
    brand: Optional[str] = Query(None, max_length=500),
    category_id: Optional[str] = Query(None, max_length=500),
    source_id: Optional[str] = Query(None, max_length=200),
    promocode_used: Optional[str] = Query(None, max_length=40),
    bought_within_days: Optional[int] = Query(None, ge=1, le=3650),
    # Orders in a period — between N and M orders from a date, up to a date,
    # between two dates (both edges inclusive), or within the last N days.
    # `orders_min` counts the whole history and cannot say "two purchases this
    # year". `period_orders_max=0` alone is "bought, but not in the period".
    period_orders_min: Optional[int] = Query(None, ge=1, le=1000),
    period_orders_max: Optional[int] = Query(None, ge=0, le=1000),
    period_from: Optional[_date] = Query(None),
    period_to: Optional[_date] = Query(None),
    period_days: Optional[int] = Query(None, ge=1, le=3650),
    # ─── More groups, ORed with the one above ────────────────────────────
    or_groups: Optional[str] = Query(
        None,
        description="JSON array of further audience groups, each an object with "
                    "the per-group parameters above (max_recency_days and the "
                    "filters); a customer is in the audience if they match any "
                    "group",
    ),
    # ─── A follow-up ─────────────────────────────────────────────────────
    follow_up_of: Optional[str] = Query(
        None, pattern=_CAMPAIGN_PATTERN,
        description="A sent campaign to follow up: the audience becomes exactly its "
                    "accepted target arm, with no control of its own",
    ),
) -> dict:
    """Validate and normalise the segmentation criteria shared by both endpoints."""
    try:
        sales_type = validate_sales_type(sales_type)
    except ValidationError as e:
        raise HTTPException(status_code=400, detail=str(e))

    if follow_up_of is not None:
        shaping = sorted(k for k in request.query_params if k in _AUDIENCE_SHAPING_PARAMS)
        if shaping:
            raise HTTPException(
                status_code=400,
                detail=(
                    f"a follow-up goes to exactly the people {follow_up_of!r} reached; "
                    f"{', '.join(shaping)} cannot narrow it — leave them out, or "
                    f"build a new audience instead"
                ),
            )
        # Forced, and refused when asked for: a follow-up withholding people of
        # its own would draw a second control out of the first one's targets,
        # and the first one's control is the only baseline either has.
        if "holdout_pct" in request.query_params and holdout_pct != 0:
            raise HTTPException(
                status_code=400,
                detail="a follow-up has no control group of its own — it is measured "
                       f"on {follow_up_of!r}'s control, so holdout_pct must be 0",
            )
        if follow_up_of == campaign:
            raise HTTPException(
                status_code=400, detail="a campaign cannot follow itself up",
            )
        holdout_pct = 0

    if ltv_basis not in SMS_LTV_BASES:
        raise HTTPException(
            status_code=400,
            detail=f"ltv_basis must be one of {', '.join(SMS_LTV_BASES)}",
        )

    # A comma-separated list, because the tiers are not always addressed one at
    # a time: a discount suits Core and Reactivation and cannibalises VIP, and
    # that is one campaign with one text, not two campaigns to reconcile later.
    tiers: Optional[list] = None
    if tier:
        tiers = []
        for raw in tier.split(","):
            name = raw.strip().upper()
            if not name:
                continue
            if name not in _SMS_TIERS:
                raise HTTPException(
                    status_code=400,
                    detail=f"tier must be one of {', '.join(_SMS_TIERS)}, got {name!r}",
                )
            if name not in tiers:
                tiers.append(name)
        if not tiers:
            tiers = None

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

    if grouping not in SMS_GROUPINGS:
        raise HTTPException(
            status_code=400,
            detail=f"grouping must be one of {', '.join(SMS_GROUPINGS)}",
        )

    # The first group is the flat parameters — the audience as it was before
    # groups, refused in the words it was refused in then.
    first = _audience_group({
        "max_recency_days": max_recency_days,
        "recency_min": recency_min, "recency_max": recency_max,
        "orders_min": orders_min, "orders_max": orders_max,
        "ltv_min": ltv_min, "ltv_max": ltv_max,
        "aov_min": aov_min, "aov_max": aov_max,
        "first_order_from": first_order_from, "first_order_to": first_order_to,
        "city": _text_list(city),
        "gender": _text_list(gender),
        "gender_min_confidence": gender_min_confidence,
        "brand": _text_list(brand),
        "category_id": _int_list(category_id, "category_id"),
        "source_id": _int_list(source_id, "source_id"),
        "promocode_used": promocode_used,
        "bought_within_days": bought_within_days,
        "period_orders_min": period_orders_min,
        "period_orders_max": period_orders_max,
        "period_from": period_from, "period_to": period_to,
        "period_days": period_days,
    }, "group 1" if or_groups is not None else "")
    groups = [first] + _extra_audience_groups(or_groups)
    widest = max(g.max_recency_days for g in groups)

    if reactivation_max_recency is None:
        # The reactivation window cannot outlast the base window it lives in.
        # Pinning it at 120 made "last 60 days" a 400 rather than a narrower
        # audience, which is the opposite of what the control is for. With
        # groups the base window is the widest of theirs.
        reactivation_max_recency = min(120, widest)

    if reactivation_max_recency > widest:
        raise HTTPException(
            status_code=400,
            detail="reactivation_max_recency must not exceed max_recency_days",
        )

    return {
        "groups": groups,
        "ltv_basis": ltv_basis,
        "vip_ltv": vip_ltv,
        "core_ltv": core_ltv,
        "core_min_orders": core_min_orders,
        "reactivation_max_recency": reactivation_max_recency,
        "sales_type": sales_type,
        "holdout_pct": holdout_pct,
        "campaign": campaign,
        "tier": tiers,
        "grouping": grouping,
        "follow_up_of": follow_up_of,
    }


async def _sms_audience(store, criteria: dict, *, include_customers: bool, limit: int) -> dict:
    """The audience `criteria` names — the one call the preview and the freeze share.

    A follow-up is read off the campaign it follows; everything else is
    segmented. Both answers have one shape, and both routes go through here,
    so the roster frozen is the roster that was on screen.
    """
    root = criteria["follow_up_of"]
    if root is None:
        segment_args = {k: v for k, v in criteria.items()
                        if k not in ("follow_up_of", "groups")}
        groups = criteria["groups"]
        if len(groups) == 1:
            # One group is the audience as it was before groups existed, and
            # it is asked for the way it was asked for then.
            segment_args.update(max_recency_days=groups[0].max_recency_days,
                                filters=groups[0].filters)
        else:
            segment_args["groups"] = groups
        return await store.get_sms_segments(
            include_customers=include_customers, limit=limit, **segment_args,
        )

    try:
        data = await store.get_sms_follow_up_audience(
            root, campaign=criteria["campaign"],
            include_customers=include_customers, limit=limit,
        )
    except ValueError as e:
        status = 404 if "not frozen" in str(e) else 409
        raise HTTPException(status_code=status, detail=str(e))

    # `api_gate` checked the sales type in this request's query string, and only
    # that one. Inheriting a different one from the root would let a follow-up
    # reach a roster the caller was never cleared to see.
    if data["salesType"] != criteria["sales_type"]:
        raise HTTPException(
            status_code=409,
            detail=(
                f"campaign {root!r} was built for sales_type={data['salesType']}; "
                f"a follow-up inherits it — pass sales_type={data['salesType']}"
            ),
        )
    return data


@router.get("/customers/sms-segments")
# The wizard previews the audience live, so a manager adjusting filters spends
# these quickly even with the typing debounced. It is an admin-only read of
# aggregates, and the cost of refusing one is a page that stops counting
# mid-campaign.
@limiter.limit("60/minute")
async def get_sms_segments(
    request: Request,
    criteria: dict = Depends(_sms_segment_params),
    include_customers: bool = Query(False),
    limit: int = Query(20000, ge=1, le=100000),
    user: dict = Depends(require_permission("sms", "view")),
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

    `view` covers the sizes; the rows themselves need `edit`, the same
    permission the CSV download needs, because they are the same data.

    `follow_up_of=X` previews a follow-up instead: X's accepted target arm,
    less anyone opted out since, in the same shape — the same function the
    freeze calls, so what is frozen is what this showed.
    """
    if include_customers and not await has_permission(user, "sms", "edit"):
        raise HTTPException(
            status_code=403,
            detail="Customer rows require edit access to SMS campaigns",
        )
    store = await get_store()
    return await _sms_audience(
        store, criteria, include_customers=include_customers, limit=limit,
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
    user: dict = Depends(require_permission("sms", "edit")),
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
    if criteria["follow_up_of"] is not None:
        # The file and its freeze describe a segmented audience: tiers, LTV,
        # city. A follow-up is somebody else's roster sent twice, and it is
        # created where nothing leaves as a file.
        raise HTTPException(
            status_code=400,
            detail="a follow-up is created with POST /customers/sms-campaigns, "
                   "not exported",
        )
    store = await get_store()
    data = await _sms_audience(store, criteria, include_customers=True, limit=limit)

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
        "SMS segment export: user=%s campaign=%s tier=%s groups=%d rows=%d holdout=%s",
        user.get("user_id"), criteria["campaign"], criteria["tier"] or "all",
        len(criteria["groups"]), len(rows), include_holdout,
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

    # Name the file after what is in it: several tiers join with a dash, so a
    # Core+Reactivation export is not mistaken for the whole base on disk.
    tier_part = "-".join(t.lower() for t in criteria["tier"]) if criteria["tier"] else "all"
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


# ─── Saved audiences ──────────────────────────────────────────────────────
# A preset is the wizard's form state under a name. It is never executed: the
# page reads it, fills its controls, and sends the values back through
# `_sms_segment_params` like any hand-built audience. So a preset cannot widen
# what the segmentation accepts, however it was stored.

_PRESET_NAME_MAX = 60
_PRESET_CRITERIA_MAX = 8_000

# Criteria are handed back to every reader of the listing, so what cannot be
# rendered must not be stored — and size is not the whole of shape. `json.loads`
# accepts hundreds of levels of nesting where the response serializer refuses
# past 254, and the row is written before the response is rendered: 1 787
# characters, well inside the cap above, wrote a preset and then made
# `GET /sms-audience-presets` fail for every reader until somebody deleted it by
# a name the dead listing no longer showed. Bounded here rather than at the
# serializer's own limit because the listing wraps `criteria` three levels
# deeper than the PUT response, so a body that renders on the way in can still
# be unrenderable on the way out. The page's form state is two levels — three
# where a filter holds a list — so this leaves it room it will never use.
_PRESET_CRITERIA_MAX_DEPTH = 20

# A preset name is shown to people, so it is deliberately softer than a campaign
# id (`_CAMPAIGN_PATTERN`): letters of any script — a Ukrainian team names an
# audience in Cyrillic — digits, spaces and a small readable punctuation set are
# allowed. What is refused is anything outside that: control characters (a
# newline in the name forges a second audit-log line), markup and quotes, path
# and format metacharacters. `\w` already covers underscore and every script's
# letters/digits. Anchoring on the character class rather than `^…$` sidesteps
# the Python gotcha where `$` matches before a trailing newline.
_PRESET_NAME_BAD = re.compile(r"[^\w .,'()&+%\-]", re.UNICODE)


def _clean_preset_name(name: str) -> str:
    """Validate and normalise the `{name}` path parameter for PUT and DELETE.

    Trims edge whitespace, then enforces the same rule for both verbs so a name
    that cannot be created cannot be addressed for deletion either — the two
    used to disagree, DELETE being a bare `.strip()` that accepted names PUT
    would reject.
    """
    name = name.strip()
    if not name or len(name) > _PRESET_NAME_MAX:
        raise HTTPException(
            status_code=400,
            detail=f"name must be 1..{_PRESET_NAME_MAX} characters",
        )
    if _PRESET_NAME_BAD.search(name):
        raise HTTPException(
            status_code=400,
            detail="name may contain only letters, digits, spaces and . , ' ( ) & + % - _",
        )
    return name


def _nested_deeper_than(value, limit: int) -> bool:
    """Does `value` nest containers more than `limit` levels deep?

    Walked with an explicit stack, not by recursion: the body is already parsed
    by the time this runs, and a recursive walk would exhaust the interpreter's
    own stack on exactly the input it is here to refuse. It stops at the first
    level past the limit rather than measuring how deep the thing really goes.
    """
    stack = [(value, 1)]
    while stack:
        node, depth = stack.pop()
        if isinstance(node, dict):
            children = node.values()
        elif isinstance(node, list):
            children = node
        else:
            continue
        if depth > limit:
            return True
        stack.extend((child, depth + 1) for child in children)
    return False


@router.get("/customers/sms-audience-presets")
@limiter.limit("30/minute")
async def list_sms_audience_presets(
    request: Request,
    user: dict = Depends(require_permission("sms", "view")),
):
    """Saved audiences, built-ins first."""
    store = await get_store()
    return {"presets": await store.list_sms_audience_presets()}


@router.put("/customers/sms-audience-presets/{name}")
@limiter.limit("20/minute")
async def save_sms_audience_preset(
    request: Request,
    name: str,
    user: dict = Depends(require_permission("sms", "edit")),
):
    """Store or replace a saved audience under `name`.

    The body is the form state as JSON. It is stored verbatim and handed back
    to the page, which is why the only checks here are on size and shape: this
    endpoint decides what a manager can save, not what the segmentation runs.
    """
    name = _clean_preset_name(name)

    try:
        criteria = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="expected a JSON body")
    if not isinstance(criteria, dict):
        raise HTTPException(status_code=400, detail="criteria must be a JSON object")
    if len(json.dumps(criteria, ensure_ascii=False, default=str)) > _PRESET_CRITERIA_MAX:
        raise HTTPException(status_code=400, detail="criteria is too large")
    if _nested_deeper_than(criteria, _PRESET_CRITERIA_MAX_DEPTH):
        raise HTTPException(status_code=400, detail="criteria is nested too deeply")

    store = await get_store()
    try:
        saved = await store.save_sms_audience_preset(
            name, criteria, created_by=user.get("user_id"),
        )
    except ValueError as e:
        raise HTTPException(status_code=409, detail=str(e))

    logger.info("SMS audience preset saved: user=%s name=%s",
                user.get("user_id"), name)
    return saved


@router.delete("/customers/sms-audience-presets/{name}")
@limiter.limit("20/minute")
async def delete_sms_audience_preset(
    request: Request,
    name: str,
    user: dict = Depends(require_permission("sms", "edit")),
):
    """Remove a saved audience. Built-ins refuse."""
    name = _clean_preset_name(name)
    store = await get_store()
    try:
        removed = await store.delete_sms_audience_preset(name)
    except ValueError as e:
        raise HTTPException(status_code=409, detail=str(e))
    if not removed:
        raise HTTPException(status_code=404, detail=f"no audience named {name!r}")
    # Presets are a shared resource — any sms:edit user can remove another's —
    # so a deletion leaves the same audit trail a save does.
    logger.info("SMS audience preset deleted: user=%s name=%s",
                user.get("user_id"), name)
    return {"deleted": name}


@router.post("/customers/sms-campaigns")
@limiter.limit("10/minute")
async def create_sms_campaign(
    request: Request,
    criteria: dict = Depends(_sms_segment_params),
    limit: int = Query(50000, ge=1, le=100000),
    overwrite: bool = Query(False, description="Replace an existing frozen roster"),
    promocode: Optional[str] = Query(
        None, max_length=40,
        description="Code carried by this campaign, for direct attribution",
    ),
    user: dict = Depends(require_permission("sms", "edit")),
):
    """Freeze this audience as a campaign, without downloading anything.

    Freezing used to be a side effect of the CSV export, so creating a campaign
    meant taking a file of phone numbers whether or not anybody wanted one —
    the send goes through the gateway, and the file was pure ceremony. The
    roster still has to be recorded at this instant for the campaign to be
    measurable at all; that is what this endpoint is for.

    `follow_up_of=X` freezes a follow-up: exactly X's target members whose
    message the gateway accepted, less anyone opted out since, with no
    control of its own. X must be sent and must not itself be a follow-up.
    The link is kept in the campaign's criteria, where the send's overlap
    check and the results read it.
    """
    if criteria["campaign"] == "default":
        raise HTTPException(
            status_code=400,
            detail="name the campaign — 'default' is the placeholder the preview uses",
        )

    store = await get_store()
    data = await _sms_audience(store, criteria, include_customers=True, limit=limit)

    if data["truncated"]:
        raise HTTPException(
            status_code=400,
            detail="refusing to freeze a truncated roster — raise `limit` so the "
                   "whole audience is recorded",
        )
    if not data["customers"]:
        raise HTTPException(
            status_code=400,
            detail="this audience is empty — nothing to freeze",
        )

    # A follow-up carries its root's basis; the query's own default would
    # misdescribe the figures frozen with every member.
    ltv_basis = (data["ltvBasis"] if criteria["follow_up_of"]
                 else criteria["ltv_basis"])
    try:
        frozen = await store.freeze_sms_campaign(
            campaign=criteria["campaign"],
            customers=data["customers"],
            criteria=data["criteria"],
            ltv_basis=ltv_basis,
            sales_type=criteria["sales_type"],
            holdout_pct=criteria["holdout_pct"],
            promocode=promocode,
            overwrite=overwrite,
        )
    except ValueError as e:
        raise HTTPException(status_code=409, detail=str(e))

    logger.info(
        "SMS campaign created: user=%s campaign=%s grouping=%s groups=%d "
        "follow_up_of=%s target=%d holdout=%d",
        user.get("user_id"), criteria["campaign"], criteria["grouping"],
        len(criteria["groups"]), criteria["follow_up_of"],
        data["totals"]["target"], data["totals"]["holdout"],
    )
    return {
        "campaign": criteria["campaign"],
        "frozen": frozen,
        "segments": data["segments"],
        "totals": data["totals"],
        "funnel": data["funnel"],
        "criteria": data["criteria"],
    }


@router.post("/customers/sms-campaigns/{campaign}/sent")
@limiter.limit("20/minute")
async def mark_sms_campaign_sent(
    request: Request,
    campaign: str = Path(..., pattern=_CAMPAIGN_PATTERN),
    sent_at: Optional[str] = Query(
        None, description="ISO timestamp; defaults to now",
    ),
    user: dict = Depends(require_permission("sms", "edit")),
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
        status = 409 if "through the gateway" in str(e) else 404
        raise HTTPException(status_code=status, detail=str(e))

    logger.info(
        "SMS campaign marked sent: user=%s campaign=%s at=%s",
        user.get("user_id"), campaign, result["sentAt"],
    )
    return result


# What the balance has to cover, over the configured tariff.
#
# The contract rate in config (1.28 ₴) holds for top-ups of 10 000 ₴ and more;
# a smaller top-up bills 1.29. Two percent over the configured figure covers
# that and rounding, so a send this guard lets through is not then cut short
# at the gateway — which bills batches it then fails to dispatch, and took the
# balance to −675 ₴ the one time it ran dry mid-roster.
SMS_BALANCE_MARGIN = 1.02

# How long the page waits for the balance before showing none. The send reads
# it on the client's own timeout: there, an answer is worth waiting for.
_BALANCE_READ_TIMEOUT_S = 5.0


def _refusal(detail: str, reason: str, **extra) -> JSONResponse:
    """A 409 that says why in `detail`, as every refusal here does, and carries
    the figures behind it beside the sentence for a page that wants them."""
    return JSONResponse(
        status_code=409, content={"detail": detail, "reason": reason, **extra},
    )


async def _refuse_before_sending(
    store, campaign: str, recipients: int, text: str,
) -> Optional[JSONResponse]:
    """Why this claimed campaign must not go to the gateway, or None.

    Asked after the claim and before a single message leaves, so a refusal
    hands the campaign back intact. Both questions are about money and about
    the measurement, in that order of cheapness: the database first, the
    gateway second.
    """
    conflicts = await store.find_sms_send_conflicts(campaign)
    if conflicts:
        named = "; ".join(
            f"{c['people']} in the {c['arm']} arm of {c['campaign']!r} "
            f"(sent {c['sentAt']})"
            for c in conflicts
        )
        why = [f"refused: this roster overlaps campaigns sent in the last "
               f"{SMS_RECENT_CAMPAIGN_HOURS} h — {named}."]
        if any(c["arm"] == "control" for c in conflicts):
            why.append("Messaging another campaign's control group destroys its "
                       "measurement, and nothing overrides that.")
        if any(c["arm"] == "target" for c in conflicts):
            why.append("A second message to another campaign's recipients has to "
                       "be a declared follow-up of it (follow_up_of).")
        why.append("Nothing was sent; the campaign is unsent.")
        return _refusal(" ".join(why), "recent_campaign", conflicts=conflicts)

    parts = count_segments(text).parts
    price = TurboSmsConfig().price_per_part
    # Priced as SMS for everyone, Viber included: the hybrid falls back to SMS
    # for whoever Viber cannot reach, and that is not known until after.
    cost = round(recipients * parts * price * SMS_BALANCE_MARGIN, 2)
    figures = {"recipients": recipients, "parts": parts, "pricePerPart": price,
               "margin": SMS_BALANCE_MARGIN, "cost": cost}

    balance, why = None, None
    try:
        async with TurboSmsClient() as client:
            balance = await client.balance()
    except TurboSmsError as e:
        why = str(e)

    if balance is None:
        return _refusal(
            f"could not read the TurboSMS balance ({why or 'the gateway gave no figure'}) "
            f"— this send costs up to {cost:.2f} ₴ and is not started without "
            f"knowing it is covered. Nothing was sent; the campaign is unsent.",
            "balance_unreadable",
            balance=None, shortfall=None, **figures,
        )
    if balance < cost:
        shortfall = round(cost - balance, 2)
        return _refusal(
            f"the TurboSMS balance is {balance:.2f} ₴ and this send costs up to "
            f"{cost:.2f} ₴ ({recipients} recipients × {parts} part(s) × {price} ₴, "
            f"+{round((SMS_BALANCE_MARGIN - 1) * 100)}%) — {shortfall:.2f} ₴ short. "
            f"Top up and send again; nothing was sent and the campaign is unsent.",
            "balance_short",
            balance=balance, shortfall=shortfall, **figures,
        )
    return None


@router.post("/customers/sms-campaigns/{campaign}/send")
@limiter.limit("3/minute")
async def send_sms_campaign(
    request: Request,
    campaign: str = Path(..., pattern=_CAMPAIGN_PATTERN),
    text: str = Query(..., min_length=1, max_length=600),
    channel: str = Query("sms", pattern="^(sms|viber_sms)$"),
    viber_text: Optional[str] = Query(None, max_length=1000),
    button_caption: Optional[str] = Query(None, max_length=30),
    button_url: Optional[str] = Query(None, max_length=300),
    # Optional in the signature only so that leaving it out is a 409 with the
    # reason, like a wrong one, rather than FastAPI's 422 about a field.
    confirm_campaign: Optional[str] = Query(
        None, max_length=200,
        description="The campaign name, typed back by whoever is sending",
    ),
    user: dict = Depends(require_permission("sms", "edit")),
):
    """
    Send the campaign's target group through TurboSMS.

    Only the target arm is sent — the control must stay unmessaged for the
    result to mean anything. The gateway's per-recipient answer is recorded:
    message ids for tracking delivery, and stoplist refusals as opt-outs, so
    those people are never selected again.

    `channel=viber_sms` sends over Viber first and falls back to SMS only for
    recipients Viber could not reach. The measurement is unaffected — one
    message id per recipient either way — but the copy is not: the Viber arm
    can carry a button, and `text` is what the SMS fallback shows, so it has
    to stand on its own with the link spelled out.

    Sending twice is refused: the campaign is stamped sent on the first pass.

    Refused before the claim when `confirm_campaign` is missing or is not the
    campaign's name. Refused after it — handed back, nothing sent — when the
    roster overlaps a campaign sent in the last day (another's control at
    all, another's targets unless this is its declared follow-up), when the
    TurboSMS balance cannot be read, or when it does not cover the send.
    """
    # The name typed back is checked here and not only in the dialog: a
    # replayed request, a script or a second tab would otherwise skip the one
    # deliberate act this send asks for. Before the claim, so it costs nothing.
    if (confirm_campaign or "").strip() != campaign:
        raise HTTPException(
            status_code=409,
            detail=f"type the campaign name {campaign!r} to confirm the send — "
                   f"confirm_campaign is missing or does not match",
        )

    # Assembled before the claim, because it is built from query parameters
    # alone and can be rejected. Taking the claim first meant a caption with no
    # URL answered 400 with the campaign stamped sent — and nothing clears that
    # stamp again, so a typo cost the roster, which is the only control group
    # the campaign will ever have.
    viber = _build_viber(channel, text, viber_text, button_caption, button_url)

    store = await get_store()
    try:
        targets = await store.get_sms_campaign_targets(campaign)
    except ValueError as e:
        status = 404 if "not frozen" in str(e) else 409
        raise HTTPException(status_code=status, detail=str(e))

    if not targets:
        # Only knowable after the claim, so it has to be handed back here.
        await store.release_sms_campaign(campaign)
        raise HTTPException(status_code=409, detail="campaign has no target recipients")

    by_phone = {t["phone"]: t["buyerId"] for t in targets}

    try:
        refusal = await _refuse_before_sending(store, campaign, len(by_phone), text)
    except Exception:
        # Nothing has left yet. A guard that could not answer must not leave
        # the campaign claimed, or it is stuck unsendable with its roster unsent.
        try:
            await store.release_sms_campaign(campaign)
        except Exception:  # noqa: BLE001 — the original failure is the one to raise
            logger.exception("SMS claim could not be released: campaign=%s", campaign)
        raise
    if refusal is not None:
        await store.release_sms_campaign(campaign)
        logger.warning(
            "SMS campaign send refused before the gateway: user=%s campaign=%s "
            "reason=%s",
            user.get("user_id"), campaign, json.loads(refusal.body)["reason"],
        )
        return refusal

    # A roster past the gateway's per-request limit is split, so a later batch
    # can fail with earlier ones already delivered. Those have to be recorded
    # anyway: a retry that did not know about them would message those people
    # twice, spend the budget twice, and destroy the comparison the campaign
    # exists to produce.
    partial: Optional[PartialSendError] = None
    try:
        async with TurboSmsClient() as client:
            results = await client.send(list(by_phone), text, viber=viber)
    except PartialSendError as e:
        logger.error(
            "TurboSMS send partially failed: campaign=%s sent=%d unsent=%d error=%s",
            campaign, e.sent, e.unsent, e,
        )
        results, partial = e.results, e
    except TurboSmsError as e:
        if e.unsent:
            # The gateway provably never took the request — refused connection,
            # upload never finished, a 4xx or an outright rejection — so hand
            # the campaign back; it can be retried once whatever the gateway
            # objected to is fixed. Safe only on this branch: PartialSendError
            # above keeps the claim, because messages did leave.
            await store.release_sms_campaign(campaign)
            logger.error("TurboSMS send failed: campaign=%s error=%s", campaign, e)
            raise HTTPException(status_code=502, detail=str(e))
        # A read timeout, a 5xx or an unreadable body is an unknown answer, not
        # a negative one: the whole batch was uploaded and the gateway may be
        # delivering it right now. Releasing here is how the roster gets sent
        # twice — the operator sees a 502, presses send again, and the claim
        # no longer stops them. Keep it; being wrong this way costs one manual
        # release after checking the panel, the other way costs a double send.
        logger.error(
            "TurboSMS send outcome unknown, claim kept: campaign=%s error=%s",
            campaign, e,
        )
        raise HTTPException(
            status_code=502,
            detail=(
                f"{e}. The gateway may have accepted the batch, so the campaign "
                f"stays claimed — check the TurboSMS panel before releasing it."
            ),
        )

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

    # What it cost, from the message that went and the count the gateway took.
    billing = count_segments(text)
    summary = await store.record_sms_send(
        campaign, accepted, stoplisted, failed,
        message_text=text,
        message_parts=billing.parts,
        price_per_part=TurboSmsConfig().price_per_part,
    )

    logger.info(
        "SMS campaign sent: user=%s campaign=%s channel=%s accepted=%d "
        "stoplisted=%d failed=%d unsent=%d",
        user.get("user_id"), campaign, channel,
        summary["accepted"], summary["stoplisted"], summary["failed"],
        partial.unsent if partial else 0,
    )
    return {
        **summary,
        "channel": channel,
        # Non-zero means the roster is only partly messaged and cannot be
        # resent — the campaign is stamped, so say so rather than reporting a
        # clean success.
        "unsent": partial.unsent if partial else 0,
        "partialError": str(partial) if partial else None,
    }


def _build_viber(
    channel: str,
    text: str,
    viber_text: Optional[str],
    button_caption: Optional[str],
    button_url: Optional[str],
) -> Optional[ViberMessage]:
    """
    Assemble the Viber half of a send, or None for SMS only.

    Viber is not a nicer SMS: it carries a button, which is the only way a
    campaign link ever gets readable anchor text. That is also why the two
    texts are separate — the SMS fallback has no button, so it has to spell
    the URL out, while the Viber copy stays clean.
    """
    if channel != "viber_sms":
        return None
    try:
        return ViberMessage(
            text=(viber_text or text).strip(),
            caption=(button_caption or None),
            action=(button_url or None),
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/customers/sms/channels")
@limiter.limit("30/minute")
async def get_sms_channels(
    request: Request,
    include_balance: bool = Query(
        False, description="Also ask the gateway for the account balance",
    ),
    user: dict = Depends(require_permission("sms", "view")),
):
    """
    Which channels this deployment can actually send on.

    Viber sender names are registered separately from SMS alpha names, so a
    working SMS setup says nothing about Viber. The dashboard needs to know
    before it offers the choice, rather than finding out from a 502.

    `balance` is the TurboSMS account balance in ₴, asked of the gateway only
    with `include_balance=true` — the send dialog asks, every other reader of
    this configuration does not, and a configuration read should not depend on
    the gateway answering. Null when not asked, not configured, or unreadable.
    The send checks the balance again itself; this figure is for the page.
    """
    config = TurboSmsConfig()
    balance = None
    if include_balance and config.configured:
        config.timeout = _BALANCE_READ_TIMEOUT_S
        try:
            async with TurboSmsClient(config) as client:
                balance = await client.balance()
        except TurboSmsError as e:
            logger.warning("TurboSMS balance unreadable: %s", e)
    return {
        "balance": balance,
        "sms": config.configured,
        "viber": config.viber_configured,
        "smsSender": config.sender or None,
        "viberSender": config.viber_sender or None,
        # The tariff the send will be billed at, so the page can price a
        # campaign before it goes out. Read from config rather than restated in
        # the frontend: the two drifting apart would put one number on screen
        # and charge another.
        "pricePerPart": config.price_per_part,
    }


def _require_ua_phone(phone: str) -> str:
    """Reduce a phone to the canonical 380+9-digit form, or reject it.

    Segmentation only ever compares against this exact shape
    (``length(phone) = 12 AND phone LIKE '380%'``), and the opt-out exclusion
    matches ``o.phone = scored.phone`` — so a phone stored in any other format
    can never suppress by phone. Normalising here is what makes a recorded
    opt-out actually match, and it keeps the stoplist to numbers a campaign
    could contain.
    """
    digits = "".join(c for c in phone if c.isdigit())
    if len(digits) != 12 or not digits.startswith("380"):
        raise HTTPException(
            status_code=400,
            detail="phone must be a full Ukrainian number: 380 followed by 9 digits",
        )
    return digits


@router.post("/customers/sms/test-send")
@limiter.limit("10/minute")
async def send_test_sms(
    request: Request,
    phone: str = Query(..., min_length=10, max_length=20),
    text: str = Query(..., min_length=1, max_length=600),
    channel: str = Query("sms", pattern="^(sms|viber_sms)$"),
    viber_text: Optional[str] = Query(None, max_length=1000),
    button_caption: Optional[str] = Query(None, max_length=30),
    button_url: Optional[str] = Query(None, max_length=300),
    user: dict = Depends(require_permission("sms", "edit")),
):
    """
    Send one message to one number, to check the creative before a campaign.

    Until this existed the only way to see a message as a customer sees it was
    to send a real campaign, which stamps the roster sent and cannot be undone.

    Deliberately writes nothing: no campaign, no roster, no opt-out. A stoplist
    refusal is reported in the response instead of being recorded, because this
    path is a rehearsal and must not move the data a campaign is measured on.
    The billed cost comes back too — Cyrillic drops the limit from 160
    characters to 70, which is invisible while writing the text.

    `channel=viber_sms` rehearses the hybrid send: Viber first, SMS only for
    what Viber could not deliver. Both arms are worth testing, because they do
    not look alike — the Viber one carries a button, the SMS one cannot.
    """
    # Same rule the segmentation applies, so a number that passes here is one
    # that could actually appear in a campaign.
    digits = _require_ua_phone(phone)

    cost = count_segments(text)
    viber = _build_viber(channel, text, viber_text, button_caption, button_url)

    try:
        async with TurboSmsClient() as client:
            results = await client.send([digits], text, viber=viber)
    except TurboSmsError as e:
        logger.error("Test SMS failed: user=%s error=%s", user.get("user_id"), e)
        raise HTTPException(status_code=502, detail=str(e))

    if not results:
        raise HTTPException(status_code=502, detail="gateway returned no result")

    result = results[0]
    logger.info(
        "Test SMS: user=%s phone=%s channel=%s accepted=%s code=%s parts=%d",
        user.get("user_id"), digits, channel, result.accepted, result.code,
        cost.parts,
    )
    return {
        "phone": digits,
        "channel": channel,
        "accepted": result.accepted,
        "stoplisted": result.stoplisted,
        "messageId": result.message_id,
        "code": result.code,
        "status": result.status,
        "cost": {
            "encoding": cost.encoding,
            "characters": cost.characters,
            "parts": cost.parts,
        },
    }


@router.post("/customers/sms-campaigns/optout")
@limiter.limit("30/minute")
async def add_marketing_optout(
    request: Request,
    buyer_id: int = Query(..., ge=1),
    phone: Optional[str] = Query(None, max_length=20),
    reason: str = Query("manual", max_length=40),
    user: dict = Depends(require_permission("sms", "edit")),
):
    """Record that a customer asked not to receive marketing SMS.

    A phone, when given, is normalised to the canonical 380+9-digit form the
    segmentation matches against. Stored in any other shape it would sit in the
    stoplist and never suppress the number it names — the phone column exists
    precisely to catch the same number under a second buyer record.
    """
    normalised_phone = _require_ua_phone(phone) if phone is not None else None
    store = await get_store()
    result = await store.add_marketing_optout(
        buyer_id=buyer_id, phone=normalised_phone, reason=reason,
        source=str(user.get("user_id") or "dashboard"),
    )
    logger.info("Marketing opt-out: user=%s buyer=%s reason=%s",
                user.get("user_id"), buyer_id, reason)
    return result


@router.get("/customers/sms-campaigns/{campaign}/results")
@limiter.limit("30/minute")
async def get_sms_campaign_results(
    request: Request,
    campaign: str = Path(..., pattern=_CAMPAIGN_PATTERN),
    window_days: int = Query(30, ge=1, le=180),
    delivered_only: bool = Query(
        False,
        description="Restrict the target arm to confirmed deliveries. Optimistic "
                    "bound, not a clean randomised comparison — see the docs.",
    ),
    user: dict = Depends(require_permission("sms", "view")),
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
    user: dict = Depends(require_permission("sms", "view")),
):
    """List frozen campaigns with their roster sizes and send dates."""
    store = await get_store()
    return {"campaigns": await store.list_sms_campaigns()}
