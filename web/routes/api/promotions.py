"""The promotions calendar: list, history, and the saves (revision 0039).

The section `PromotionCalendar` on /marketing reads and writes these, and the
month's forecast badge reads what is planned through
`/api/revenue/forecast`'s `planned_promotions`. `core/promotions.py` is the
table's one writer and holds every rule; this module turns requests into its
calls and its answers into the contract's JSON.

- **The tab is `marketing`**: `view` for the two GETs, `edit` for the four
  saves, through `require_permission` on each endpoint — the two levels
  differ, so the gate is not at the include.
- **Every invalid field is a 400 `{"detail": "<field>: <reason>"}`**, in
  English, the codebase's convention: the body is read and checked here, not
  by a pydantic model whose refusal would be a 422. An unknown id is a 404;
  a save the promotion's status refuses — an edit or a cancel of a cancelled
  promotion, a restore of a planned one — a 409.
- **A read whose Postgres fails is refused**, a 503 naming `promotions`, as
  every tab's read is (`core.read_fallback.refuse`). A save that fails is the
  500 it is.
- **Who saved a version is not served.** The history says `recorded_by_me`
  and nothing more: a person's id is personal data, and the page needs only
  "you" or "not you".
- **The SMS link is the `sms` tab's** (`core.promotions.sms_link`). Every
  answer carries `sms_campaign` only for a caller with `sms` view, null for
  anybody else; a save of theirs that changes the link is a 403 ("No view
  access to sms"), and the null they send back keeps the stored campaign.

Writes are limited as `/api/expenses`' are, 30 a minute.
"""
from __future__ import annotations

import json
import logging
from datetime import date
from typing import Any, Dict, Optional

from fastapi import APIRouter, Depends, HTTPException, Query, Request

from core import promotions, read_fallback
from core.exceptions import ValidationError
from web.routes.auth import has_permission, require_permission

from ._deps import limiter, validate_db_int

router = APIRouter()
logger = logging.getLogger(__name__)


def _refused(exc: ValidationError) -> HTTPException:
    return HTTPException(status_code=400, detail=f"{exc.field}: {exc.message}")


def _window_day(value: Optional[str], field: str) -> Optional[date]:
    if value is None or value == "":
        return None
    try:
        return promotions.parse_day(value, field)
    except ValidationError as exc:
        raise _refused(exc) from None


def _flag(value: Optional[str], field: str) -> bool:
    if value is None or value == "":
        return False
    lowered = value.strip().lower()
    if lowered in ("true", "1", "yes"):
        return True
    if lowered in ("false", "0", "no"):
        return False
    raise HTTPException(status_code=400, detail=f"{field}: Must be true or false")


def _promotion_id(promotion_id: int) -> int:
    try:
        validate_db_int(promotion_id, "promotion_id", bits=64)
    except ValidationError as exc:
        raise _refused(exc) from None
    return promotion_id


async def _body(request: Request) -> promotions.PromotionIn:
    """The request's JSON, parsed and checked, or the 400 naming the field.
    A body nested deeper than the decoder recurses is a RecursionError, not
    a ValueError, and it was a 500."""
    raw = await request.body()
    try:
        payload: Any = json.loads(raw) if raw else None
    except (ValueError, UnicodeDecodeError, RecursionError):
        raise HTTPException(status_code=400, detail="body: Must be a JSON object") from None
    try:
        return promotions.parse(payload)
    except ValidationError as exc:
        raise _refused(exc) from None


def _user_id(user: Dict[str, Any]) -> Optional[int]:
    value = user.get("user_id")
    try:
        return int(value) if value is not None else None
    except (TypeError, ValueError):
        return None


async def _sms_view(user: Dict[str, Any]) -> bool:
    """Whether the caller may open the `sms` tab, as `require_permission`
    would answer it: only they see a promotion's SMS campaign or change it."""
    return await has_permission(user, "sms", "view")


def _shown(out: Dict[str, Any], sms_view: bool) -> Dict[str, Any]:
    """A promotion's JSON as this caller may read it: the SMS campaign it
    names is the `sms` tab's, null for anybody who may not open that."""
    if not sms_view:
        out["sms_campaign"] = None
    return out


@router.get("/promotions")
@limiter.limit("60/minute")
async def list_promotions(
    request: Request,
    window_from: Optional[str] = Query(None, alias="from"),
    window_to: Optional[str] = Query(None, alias="to"),
    include_cancelled: Optional[str] = Query(None),
    user=Depends(require_permission("marketing", "view")),
):
    """The promotions whose days overlap the window, as they stand now: by
    first day, then id. Without a window, 90 days back to a year ahead (Kyiv);
    the cancelled only with `include_cancelled=true`."""
    first = _window_day(window_from, "from")
    last = _window_day(window_to, "to")
    default_first, default_last = promotions.default_window()
    first = first or default_first
    last = last or default_last
    if last < first:
        raise HTTPException(status_code=400, detail="to: Must not be before from")
    with_cancelled = _flag(include_cancelled, "include_cancelled")
    try:
        found = await promotions.current(first, last, include_cancelled=with_cancelled)
    except Exception as exc:
        raise read_fallback.refuse("promotions", exc)
    sms_view = await _sms_view(user)
    return {"promotions": [_shown(promotions.to_json(p), sms_view) for p in found]}


@router.get("/promotions/{promotion_id}/versions")
@limiter.limit("60/minute")
async def promotion_versions(
    request: Request,
    promotion_id: int,
    user=Depends(require_permission("marketing", "view")),
):
    """Every version of one promotion, newest first, each as the state it
    held, when it was recorded, and whether the caller recorded it."""
    _promotion_id(promotion_id)
    try:
        found = await promotions.versions(promotion_id)
    except Exception as exc:
        raise read_fallback.refuse("promotions", exc)
    if not found:
        raise HTTPException(status_code=404, detail=f"No promotion {promotion_id}")
    viewer = _user_id(user)
    sms_view = await _sms_view(user)
    return {"versions": [_shown(promotions.version_json(p, viewer), sms_view)
                         for p in found]}


async def _saved(save, request: Request, user: Dict[str, Any],
                 sms_view: bool) -> Dict[str, Any]:
    """Run one save; its refusals as the contract's 404, 409 and 403."""
    try:
        return _shown(promotions.to_json(await save), sms_view)
    except promotions.PromotionNotFound as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from None
    except promotions.PromotionConflict as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from None
    except promotions.PromotionForbidden as exc:
        # In `web.routes.auth._refused`'s shape, as every refusal is logged.
        logger.info("Authorization refused: user=%s role=%s wanted=%s path=%s",
                    user.get("user_id"), user.get("role"),
                    "view on sms (the promotion's SMS campaign)", request.url.path)
        raise HTTPException(status_code=403, detail=str(exc)) from None


@router.post("/promotions", status_code=201)
@limiter.limit("30/minute")
async def create_promotion(
    request: Request,
    user=Depends(require_permission("marketing", "edit")),
):
    """A new promotion, planned: version 1. 403 when it names an SMS
    campaign and the caller may not open the `sms` tab."""
    data = await _body(request)
    sms_view = await _sms_view(user)
    return await _saved(promotions.create(data, by=_user_id(user), sms_view=sms_view),
                        request, user, sms_view)


@router.put("/promotions/{promotion_id}")
@limiter.limit("30/minute")
async def update_promotion(
    request: Request,
    promotion_id: int,
    user=Depends(require_permission("marketing", "edit")),
):
    """The promotion with the body as its content, in a new version — none
    when nothing changed. 404 for an unknown id, 409 when it is cancelled,
    403 when it changes the SMS link and the caller may not open `sms`."""
    _promotion_id(promotion_id)
    data = await _body(request)
    sms_view = await _sms_view(user)
    return await _saved(
        promotions.update(promotion_id, data, by=_user_id(user), sms_view=sms_view),
        request, user, sms_view)


@router.post("/promotions/{promotion_id}/cancel")
@limiter.limit("30/minute")
async def cancel_promotion(
    request: Request,
    promotion_id: int,
    user=Depends(require_permission("marketing", "edit")),
):
    """A version saying it will not happen. 409 when it is cancelled already."""
    _promotion_id(promotion_id)
    sms_view = await _sms_view(user)
    return await _saved(promotions.cancel(promotion_id, by=_user_id(user)),
                        request, user, sms_view)


@router.post("/promotions/{promotion_id}/restore")
@limiter.limit("30/minute")
async def restore_promotion(
    request: Request,
    promotion_id: int,
    user=Depends(require_permission("marketing", "edit")),
):
    """A version saying a cancelled promotion is planned again. 409 when it
    is not cancelled."""
    _promotion_id(promotion_id)
    sms_view = await _sms_view(user)
    return await _saved(promotions.restore(promotion_id, by=_user_id(user)),
                        request, user, sms_view)
