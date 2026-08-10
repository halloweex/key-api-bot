"""Inbound webhooks from external services.

These endpoints cannot require a session — the caller is a machine on someone
else's infrastructure — so each one authenticates itself instead. The path is
listed in PUBLIC_API_PATHS; that list is the audit surface for anything
reachable without a login.
"""
import logging
import os
from collections import Counter
from datetime import datetime
from typing import Any, Dict

from fastapi import APIRouter, Request, HTTPException

from core.turbosms import TurboSmsConfig, classify_dlr, match_webhook_signature
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


# A send of N recipients produces up to N delivery reports, ~98% of them inside
# the same minute. At 120/minute the gateway's own retry schedule (1, 3, 5, 10,
# 15, 20, 30, 60, 120 min) was the only thing delivering receipts at all, and on
# 2026-08-05 it delivered none: 13 211 × 429 against 3 655 × 401, zero 200s.
# 600/minute absorbs a 5 000-recipient send inside the retry window while still
# capping what an unauthenticated caller can cost us.
_DLR_RATE_LIMIT = "600/minute"

# Rejections are counted per condition so a wrong secret cannot be silent again.
# The gateway retries each event nine times over 4.5 hours, so a handful of
# rejections is normal noise and a flood is a misconfiguration.
_dlr_counts: Counter = Counter()
_ALERT_AT = 25          # first alert once a burst is clearly not noise
_ALERT_EVERY = 500      # then a reminder, throttled by condition key downstream


async def _note_rejection(kind: str, **fields: Any) -> None:
    """Record a rejected callback and tell a human when it stops looking like noise.

    `kind` names the condition, which is what the alert throttle keys on — the
    message text carries live counters and would defeat it. Nothing here can
    raise: a webhook that fails to complain must still return its own error.
    """
    _dlr_counts[kind] += 1
    count = _dlr_counts[kind]
    logger.warning(
        "TurboSMS webhook rejected (%s), %d so far | %s", kind, count, fields,
    )
    if count != _ALERT_AT and count % _ALERT_EVERY != 0:
        return
    try:
        from bot.main import send_admin_message
        await send_admin_message(
            f"⚠️ TurboSMS delivery reports are being rejected: <b>{kind}</b>\n"
            f"{count} so far, accepted: {_dlr_counts['accepted']}.\n"
            f"Every rejected report is a delivery result lost for good — the "
            f"gateway gives up after 4.5 hours. If this is "
            f"<code>bad_signature</code>, compare TURBOSMS_WEBHOOK_SECRET with "
            f"the secret key set beside the callback URL in the TurboSMS panel "
            f"(<code>scripts/check_turbosms_signature.py</code> settles it).",
            key=f"turbosms:webhook:{kind}",
        )
    except Exception as e:  # noqa: BLE001 — alerting must never break the endpoint
        logger.warning("Failed to send TurboSMS webhook alert: %s", e)


@router.post("/turbosms")
@limiter.limit(_DLR_RATE_LIMIT)
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

    Every rejection is counted and, past a threshold, alerted on. A callback
    this endpoint refuses is a delivery result nobody can recover: the gateway
    exposes no way to replay one, and it stops trying after 4.5 hours. That is
    how the 2026-08-05 campaign lost every receipt it had.
    """
    config = TurboSmsConfig()
    if not config.webhook_secret:
        # Fail closed: an unconfigured secret must not mean "accept anything".
        await _note_rejection("secret_unset")
        raise HTTPException(status_code=503, detail="webhook not configured")

    try:
        payload: Dict[str, Any] = await request.json()
    except Exception:
        await _note_rejection("malformed_body")
        raise HTTPException(status_code=400, detail="expected a JSON body")

    event_id = str(payload.get("id") or "")
    signature = str(payload.get("signature") or "")
    scheme = match_webhook_signature(event_id, signature, config.webhook_secret)
    if scheme is None:
        # Name the branch that failed. "Bad signature" alone cost four days of
        # guessing once: a wrong shared secret and a payload in a shape we do
        # not parse are the same 401, and they need opposite fixes. The
        # signature's length is the tell — 40 hex chars is the documented
        # SHA1(secret + id), anything else is a different scheme. The signature
        # itself is never logged; it is a secret-derived value.
        if not event_id:
            kind = "no_event_id"
        elif not signature:
            kind = "no_signature"
        else:
            kind = "bad_signature"
        await _note_rejection(
            kind,
            event_id=event_id or None,
            event_type=payload.get("type"),
            attempt=payload.get("try"),
            signature_len=len(signature),
            signature_is_sha1_hex=(
                len(signature) == 40
                and all(c in "0123456789abcdefABCDEF" for c in signature)
            ),
        )
        if os.getenv("TURBOSMS_WEBHOOK_DEBUG", "").lower() in ("1", "true", "yes"):
            # Opt-in, off by default, and worth switching on for exactly one
            # test callback from the panel: the (id, signature) pair it prints
            # is what scripts/check_turbosms_signature.py needs to say which
            # secret the gateway is actually signing with. The signature is
            # derived from the shared secret — turn this back off afterwards.
            logger.warning("TurboSMS webhook debug payload: %s", payload)
        raise HTTPException(status_code=401, detail="bad signature")

    data = payload.get("data") or {}
    message_id = str(data.get("message_id") or "")
    status = str(data.get("status") or "")
    if not message_id:
        await _note_rejection("no_message_id", event_type=payload.get("type"))
        raise HTTPException(status_code=400, detail="missing message_id")

    # Record which concatenation the gateway used. The docs do not say, this
    # code guessed, and the guess is the leading suspect for the campaign that
    # lost every receipt — so the first accepted callback settles it in writing.
    if _dlr_counts[scheme] == 0:
        logger.info("TurboSMS webhook signature scheme confirmed: %s", scheme)
    _dlr_counts[scheme] += 1
    _dlr_counts["accepted"] += 1
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
