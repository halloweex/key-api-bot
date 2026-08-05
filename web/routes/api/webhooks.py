"""Inbound webhooks from external services.

These endpoints cannot require a session — the caller is a machine on someone
else's infrastructure — so each one authenticates itself instead. The path is
listed in PUBLIC_API_PATHS; that list is the audit surface for anything
reachable without a login.
"""
import logging
from datetime import datetime
from typing import Any, Dict

from fastapi import APIRouter, Request, HTTPException

from core.turbosms import TurboSmsConfig, classify_dlr, verify_webhook_signature
from ._deps import limiter, get_store

router = APIRouter(prefix="/webhooks", tags=["webhooks"])
logger = logging.getLogger(__name__)

# TurboSMS timestamps arrive as Kyiv local time in this shape.
_DLR_TIME_FORMAT = "%Y-%m-%d %H:%M:%S"


def _parse_dlr_time(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.strptime(str(value), _DLR_TIME_FORMAT)
    except ValueError:
        return None


@router.post("/turbosms")
@limiter.limit("120/minute")
async def turbosms_delivery_report(request: Request):
    """
    Delivery reports from TurboSMS.

    Without the signature check this endpoint would be an open write into
    campaign results: anyone could post fabricated deliveries and move the
    measured lift. So an unsigned or wrongly-signed call is rejected before the
    payload is looked at, and a missing local secret fails closed.

    Returns 200 on anything it has genuinely finished with — including reports
    for message ids we do not know — because TurboSMS retries for 4.5 hours on
    any other status, and retrying an unknown id would never succeed.
    """
    config = TurboSmsConfig()
    if not config.webhook_secret:
        # Fail closed: an unconfigured secret must not mean "accept anything".
        logger.error("TurboSMS webhook hit but TURBOSMS_WEBHOOK_SECRET is unset")
        raise HTTPException(status_code=503, detail="webhook not configured")

    try:
        payload: Dict[str, Any] = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="expected a JSON body")

    event_id = str(payload.get("id") or "")
    signature = str(payload.get("signature") or "")
    if not verify_webhook_signature(event_id, signature, config.webhook_secret):
        logger.warning("TurboSMS webhook rejected: bad signature, id=%s", event_id)
        raise HTTPException(status_code=401, detail="bad signature")

    data = payload.get("data") or {}
    message_id = str(data.get("message_id") or "")
    status = str(data.get("status") or "")
    if not message_id:
        raise HTTPException(status_code=400, detail="missing message_id")

    store = await get_store()
    known = await store.record_sms_delivery(
        message_id=message_id,
        status=status,
        delivered=classify_dlr(status),
        delivered_at=_parse_dlr_time(data.get("dlr_date")),
    )

    if not known:
        # Acknowledge anyway — retrying will not make the id appear.
        logger.info("TurboSMS DLR for unknown message_id=%s status=%s",
                    message_id, status)

    return {"ok": True, "matched": known}
