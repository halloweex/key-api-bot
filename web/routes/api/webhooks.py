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
from starlette.requests import ClientDisconnect

from core.repositories.customers import DlrEventRebound
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
# 600/minute was still three times too small, measured on the 2026-08-26 send:
# 4 199 recipients produced 2 028 × 429 and 1 130 × 499 (the gateway giving up
# mid-request) inside fifteen minutes. A delivery report refused is a result
# nobody can recover — the gateway stops trying after 4.5 hours and offers no
# replay — so the ceiling has to clear a whole send arriving at once, with the
# gateway's nine retries stacked on top of it.
#
# The endpoint is not free to abuse at this rate: every request must carry a
# SHA1 over a shared secret, and an unsigned one is refused before the payload
# is read.
_DLR_RATE_LIMIT = "12000/minute"

# Rejections are counted per condition so a wrong secret cannot be silent again.
# The gateway retries each event nine times over 4.5 hours, so a handful of
# rejections is normal noise and a flood is a misconfiguration.
_dlr_counts: Counter = Counter()
_ALERT_AT = 25          # first alert once a burst is clearly not noise
_ALERT_EVERY = 500      # then a reminder, throttled by condition key downstream

# What to actually do about each condition. This used to be one paragraph that
# named `bad_signature` whatever the kind was, so a burst of
# `client_disconnected` — a condition in which the secret is provably fine —
# sent its reader off to compare secrets. An alert that misnames its own cause
# costs more than no alert.
_GUIDANCE: Dict[str, str] = {
    "bad_signature": (
        "Compare TURBOSMS_WEBHOOK_SECRET with the secret key set beside the "
        "callback URL in the TurboSMS panel "
        "(<code>scripts/check_turbosms_signature.py</code> settles it)."
    ),
    "secret_unset": (
        "TURBOSMS_WEBHOOK_SECRET is not set in this container. Note that "
        "<code>docker compose up -d</code> does not pick up a changed .env — "
        "that needs <code>--force-recreate</code>, which is how this hid once "
        "already."
    ),
    "event_rebound": (
        "A callback reused an event id that had already reported on a "
        "different message_id. The gateway signs the id and nothing else, so "
        "this is what stops one captured (id, signature) pair from being "
        "re-pointed at another recipient's message; its own retries carry the "
        "same data and cannot trip it. Before assuming a replay, compare "
        "data.message_id across two rejected callbacks — the other reading is "
        "that TurboSMS has started reusing ids across messages, and that is a "
        "revert, not a secret to rotate."
    ),
    "client_disconnected": (
        "Nothing here is misconfigured — the signature was never even read. "
        "The gateway hung up mid-request, which it does when we answer too "
        "slowly, so this counts capacity and not format. A send delivers "
        "~1000 callbacks in ~7s against ~100/s of capacity; on 2026-08-27 that "
        "lost ~48% of a burst. The lever that works without knowing the cause "
        "is asking TurboSMS to spread the callbacks out."
    ),
}
_GUIDANCE_DEFAULT = (
    "The payload did not have the shape this endpoint parses. Compare a raw "
    "callback against the TurboSMS docs before changing anything here."
)


async def _note_rejection(kind: str, **fields: Any) -> None:
    """Record a callback we could not record and tell a human when it stops
    looking like noise.

    `kind` names the condition, which is what the alert throttle keys on — the
    message text carries live counters and would defeat it. Nothing here can
    raise: a webhook that fails to complain must still return its own error.
    """
    _dlr_counts[kind] += 1
    count = _dlr_counts[kind]
    logger.warning(
        "TurboSMS webhook not recorded (%s), %d so far | %s", kind, count, fields,
    )
    if count != _ALERT_AT and count % _ALERT_EVERY != 0:
        return
    # We rejected it, or it never finished arriving. Saying "rejected" for the
    # second one is what made the old message point at the wrong thing.
    headline = (
        "are being dropped before we can read them"
        if kind == "client_disconnected"
        else "are being rejected"
    )
    try:
        from core.alerting import raise_alert

        condition = f"turbosms:webhook:{kind}"
        await raise_alert(
            f"⚠️ TurboSMS delivery reports {headline}: <b>{kind}</b>\n"
            f"{count} so far, accepted: {_dlr_counts['accepted']}.\n"
            f"Each one is a delivery result lost for good — the gateway gives "
            f"up after 4.5 hours and offers no replay.\n"
            f"{_GUIDANCE.get(kind, _GUIDANCE_DEFAULT)}",
            conditions=[condition], bucket=condition,
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

    The signature is not on its own an authorisation to write, because the
    gateway signs the event id and nothing else: the message_id being written
    is outside the proof. So the store binds each event id to the message it
    first reported on, and a callback re-pointing a known id at a different
    message is refused — see ``record_sms_delivery``.

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
    except ClientDisconnect:
        # Not a malformed payload — the gateway stopped sending it. Counted
        # apart from `malformed_body` because the two need opposite responses:
        # one is a format or secret problem to debug here, the other is us
        # being too slow, and lumping them together is how 25 abandoned
        # requests read as 25 bad payloads on 2026-08-27. nginx records these
        # as 499 and never as 400, which is the tell.
        await _note_rejection("client_disconnected")
        raise HTTPException(status_code=408, detail="client disconnected")
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
        # itself is not logged here (it is secret-derived); the opt-in debug
        # branch below is the only place it appears, and only when explicitly
        # enabled.
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
            # test callback from the panel: the (id, signature) pair below is
            # what scripts/check_turbosms_signature.py needs to say which secret
            # the gateway is actually signing with. Only these diagnostic fields
            # are logged, never the whole payload — it is attacker-controlled, so
            # dumping it verbatim would let a caller inject log lines. The
            # signature is derived from the shared secret — turn this back off
            # afterwards.
            logger.warning(
                "TurboSMS webhook debug: id=%s signature=%s type=%s try=%s",
                event_id, signature, payload.get("type"), payload.get("try"),
            )
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

    store = await get_store()
    try:
        known = await store.record_sms_delivery(
            message_id=message_id,
            status=status,
            delivered=classify_dlr(status),
            delivered_at=_parse_dlr_time(data.get("dlr_date")),
            event_id=event_id,
        )
    except DlrEventRebound as e:
        # The signature proved the caller knew the secret; it did not prove
        # anything about `message_id`, which is the object being written. The
        # store binds each event id to the message it first named, and this is
        # that binding refusing a re-pointed pair.
        #
        # 409 rather than 200 on purpose. A forger does not retry, so refusing
        # costs nothing there; the gateway does, nine times over 4.5 hours, so
        # if this ever fires on legitimate traffic the alert this counts toward
        # has that long to reach a human and the reports survive a revert.
        # Swallowing it with a 200 would consume them instead.
        await _note_rejection(
            "event_rebound",
            event_id=event_id,
            first_reported_on=e.bound_message_id,
            now_claims=message_id,
        )
        raise HTTPException(
            status_code=409, detail="event id already reported on another message",
        )

    _dlr_counts["accepted"] += 1

    if not known:
        # Acknowledge anyway — retrying will not make the id appear.
        logger.info("TurboSMS DLR for unknown message_id=%s status=%s",
                    message_id, status)

    return {"ok": True, "matched": known}
