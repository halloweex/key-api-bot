"""Telegram delivery that does not depend on a running bot Application.

`bot.main.send_admin_message` can only send while the python-telegram-bot
`Application` is live, which is true in the bot container and false everywhere
else. The scheduler, the warehouse validator and the prediction service all run
inside the *web* container, so every alert they raised was logged as
"called before bot init; dropping" and never left the host.

The Telegram Bot API is plain HTTPS and needs nothing but the token, so the web
container can deliver on its own. This module is that transport; `bot.main`
falls back to it when no Application is available.
"""
import asyncio
import logging
import os
import socket
import time
from typing import Iterable

import httpx

logger = logging.getLogger(__name__)

TELEGRAM_API = "https://api.telegram.org"

# Alerts are diagnostics, not user traffic — a slow Telegram must never stall a
# warehouse refresh or a scheduler job.
_TIMEOUT_SECONDS = 10.0

# How long an identical alert stays silent after being sent. The warehouse
# validator runs every two minutes and re-raises the same CRITICAL for as long
# as the condition holds, so an unthrottled channel delivers 30 copies an hour
# of a message whose whole point is "a human needs to act". Repetition does not
# make it more actionable; it makes the channel ignorable.
DEFAULT_COOLDOWN_SECONDS = 1800.0


class AlertThrottle:
    """Suppresses repeats of the same alert within a cooldown, counting them.

    Callers should pass an explicit `key` naming the *condition* — e.g.
    `warehouse:validation_failed`. Keying on message text does not work for
    the alerts that most need throttling: the warehouse validator puts live
    checksums and an attempt counter in every body, so 3 119 failures produced
    404 distinct "identical" messages and 124 deliveries on the worst day.
    Text remains the fallback key for callers that have no stable identity.

    State is per-process and resets on restart — deliberately: after a restart
    the first alert of each kind should always land.
    """

    def __init__(self, cooldown_seconds: float = DEFAULT_COOLDOWN_SECONDS):
        self.cooldown_seconds = cooldown_seconds
        self._last_sent: dict[str, float] = {}
        self._suppressed: dict[str, int] = {}

    def check(
        self, text: str, *, key: str | None = None, now: float | None = None,
    ) -> "tuple[bool, int]":
        """Return (should_send, suppressed_since_last_send).

        Calling this records the decision, so call it exactly once per attempt.
        """
        now = time.monotonic() if now is None else now
        bucket = key if key is not None else text
        last = self._last_sent.get(bucket)
        if last is not None and (now - last) < self.cooldown_seconds:
            self._suppressed[bucket] = self._suppressed.get(bucket, 0) + 1
            return False, self._suppressed[bucket]
        swallowed = self._suppressed.pop(bucket, 0)
        self._last_sent[bucket] = now
        return True, swallowed


_throttle = AlertThrottle()


def throttle_check(text: str, key: str | None = None) -> "tuple[bool, str]":
    """Decide whether `text` should go out, and what exactly to send.

    `key` names the condition; pass one whenever the body carries live numbers.
    Without it the text itself is the key, which only throttles alerts that
    repeat verbatim.

    Returns (should_send, text_to_send). When an alert has been muted, the copy
    that finally lands says how many it stood in for — silence about the
    suppression would understate how long the condition has been shouting.
    """
    allow, swallowed = _throttle.check(text, key=key)
    if not allow:
        logger.debug("Admin alert suppressed (repeat #%d within cooldown)", swallowed)
        return False, text
    if swallowed:
        minutes = int(_throttle.cooldown_seconds // 60)
        return True, f"{text}\n(repeat ×{swallowed} in {minutes} min)"
    return True, text


def reset_throttle() -> None:
    """Drop all throttle state. For tests and for a deliberate re-arm."""
    _throttle._last_sent.clear()
    _throttle._suppressed.clear()


# The kill switch for every outbound Telegram this codebase produces. Born
# of two phantoms in two days: a developer laptop running on a copy of the
# production backup sent the admins a disk CRITICAL and a data-quality digest,
# both describing a machine that was not production. A dev instance must be
# able to run the whole app — schedulers, watchdogs, digests — without ever
# reaching a real human's phone. Explicit env, not inference: alerts fail
# CLOSED to sending (production sets nothing), and dev declares itself.
DISABLE_ENV = "KS_ALERTS_DISABLED"


def alerts_disabled() -> bool:
    return os.getenv(DISABLE_ENV, "").strip().lower() in {"1", "true", "yes"}


# ─── Who is speaking ────────────────────────────────────────────────────────
#
# Every outbound message says which instance produced it. The kill switch above
# stops a dev instance from reaching anyone; this answers the question that
# comes *after* a message has already arrived — "which machine is telling me
# this?" — which took a backup-restore and two phantom alerts to become a
# question worth answering. `KS_ALERTS_DISABLED` is opt-in and therefore
# forgettable; the signature is not opt-in and costs one line.
#
# The default is the hostname rather than "unknown": inside a container that is
# the short container id, which is ugly but true and traceable, and `docker
# compose` sets `KS_INSTANCE` on both services so production reads `prod-vps`.
INSTANCE_ENV = "KS_INSTANCE"

# Rendered with no markup at all. Two parse modes are in use on this channel —
# HTML from the canary and the bot, Markdown from the data-quality formatter —
# and a signature appended by the transport cannot know which one it landed in.
# Plain text is the only thing that renders correctly under both.
_SIGNATURE_PREFIX = "· "


def instance_name() -> str:
    """The name this instance signs its messages with."""
    configured = os.getenv(INSTANCE_ENV, "").strip()
    if configured:
        # One line, always: a multi-line value would look like message body.
        return " ".join(configured.split())
    try:
        return socket.gethostname() or "unknown"
    except Exception:  # pragma: no cover - gethostname does not fail in practice
        return "unknown"


def sign(text: str) -> str:
    """Append the instance signature to `text`.

    Applied by the transports, so every path is covered by one call site each
    and nothing that merely *builds* a message has to remember. Idempotent
    against itself: a text already carrying this instance's signature is
    returned unchanged, which is what keeps the bot's Application path and its
    HTTP fallback from signing the same string twice.
    """
    line = _SIGNATURE_PREFIX + instance_name()
    if text.rstrip().endswith(line):
        return text
    return f"{text}\n\n{line}" if text else line


# ─── The alerting watching itself ───────────────────────────────────────────
#
# Every transport failure used to be a logger.warning and nothing else, which
# is how the certificate alert stayed undeliverable for months. This counter
# is read by /api/health (web) and judged by the canary from the other
# container — the same shape as every other dead-man's switch here. Only real
# attempts count: a kill-switched or empty-recipients send is configuration,
# not a transport failure.
_consecutive_transport_failures = 0
_last_delivery_at: "float | None" = None


def record_transport_outcome(delivered: int, attempted: int) -> None:
    global _consecutive_transport_failures, _last_delivery_at
    if attempted <= 0:
        return
    if delivered > 0:
        _consecutive_transport_failures = 0
        _last_delivery_at = time.time()
    else:
        _consecutive_transport_failures += 1


def transport_health() -> dict:
    return {
        "consecutive_transport_failures": _consecutive_transport_failures,
        "last_delivery_at": _last_delivery_at,
    }


def reset_transport_health() -> None:
    """For tests."""
    global _consecutive_transport_failures, _last_delivery_at
    _consecutive_transport_failures = 0
    _last_delivery_at = None


def _log_suppressed(what: str, text: str) -> None:
    logger.info(
        "%s suppressed (%s): %.80s", what, DISABLE_ENV, text.replace("\n", " ")
    )


async def send_admin_message_http(
    text: str,
    parse_mode: str = "HTML",
    *,
    token: str | None = None,
    chat_ids: Iterable[int] | None = None,
) -> int:
    """Send `text` to every admin over the HTTP Bot API. Never raises.

    Returns the number of admins the message actually reached, so callers can
    tell "delivered" from "silently dropped" — the distinction this whole module
    exists to restore.
    """
    from core.config import ADMIN_USER_IDS, BOT_TOKEN

    if alerts_disabled():
        _log_suppressed("admin message", text)
        return 0

    # Signed here rather than at the ~dozen places that build a message: one
    # call site per transport is the only version of this that cannot be
    # forgotten by the next alert somebody adds. Clamped before signing so the
    # signature always survives the cut.
    text = sign(clamp_message(text, reserve=_signature_reserve()))

    token = token if token is not None else BOT_TOKEN
    recipients = list(chat_ids if chat_ids is not None else ADMIN_USER_IDS)

    if not token:
        logger.warning("Cannot send admin alert: BOT_TOKEN is not configured")
        return 0
    if not recipients:
        logger.warning("Cannot send admin alert: ADMIN_USER_IDS is empty")
        return 0

    url = f"{TELEGRAM_API}/bot{token}/sendMessage"
    delivered = 0
    try:
        async with httpx.AsyncClient(timeout=_TIMEOUT_SECONDS) as client:
            for admin_id in recipients:
                try:
                    response = await client.post(url, json={
                        "chat_id": admin_id,
                        "text": text,
                        "parse_mode": parse_mode,
                        "disable_web_page_preview": True,
                    })
                    if _is_parse_rejection(
                        response.status_code, response.text,
                    ) and parse_mode:
                        # Broken markup degrades to plain text instead of
                        # dying: delivery outranks typography, and this net is
                        # what makes every builder's forgotten escape a
                        # cosmetic bug instead of a silent one.
                        logger.warning(
                            "Admin alert to %s rejected as unparseable %s; "
                            "resending as plain text", admin_id, parse_mode,
                        )
                        response = await client.post(url, json={
                            "chat_id": admin_id,
                            "text": text,
                            "disable_web_page_preview": True,
                        })
                    response.raise_for_status()
                    delivered += 1
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    logger.warning("HTTP admin alert to %s failed: %s", admin_id, exc)
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        logger.warning("HTTP admin alert transport failed: %s", exc)
        return delivered

    if delivered:
        logger.info("Admin alert delivered over HTTP to %d/%d admins",
                    delivered, len(recipients))
    record_transport_outcome(delivered, len(recipients))
    return delivered


# Telegram counts a caption in UTF-16 code units *after* the parse mode has
# been applied, so markup does not spend the budget — but the text does, and a
# caption over the limit fails the whole send rather than being trimmed.
TELEGRAM_CAPTION_LIMIT = 1024

# The message limit, in the same UTF-16 units. Nothing guarded this until
# 29.08: an incident-morning digest renders ~10 000 characters, so the digest
# failed hardest exactly when it had the most to say — and then advanced its
# beat and went quiet for a week.
TELEGRAM_MESSAGE_LIMIT = 4096


def _utf16_units(text: str) -> int:
    """Length the way Telegram measures it: UTF-16 code units."""
    return len(text.encode("utf-16-le")) // 2


_TRUNCATION_MARK = "\n… (truncated)"


def clamp_message(text: str, *, reserve: int = 0) -> str:
    """Cut `text` to fit Telegram's message limit, minus `reserve` units.

    Truncation may split an HTML tag; that is deliberately tolerated because
    the transports fall back to a plain-text resend on any parse rejection —
    a clipped tag costs formatting, never delivery.
    """
    budget = TELEGRAM_MESSAGE_LIMIT - reserve
    if _utf16_units(text) <= budget:
        return text
    budget -= _utf16_units(_TRUNCATION_MARK)
    # Cut by UTF-16 units, not Python chars: an emoji is two units.
    encoded = text.encode("utf-16-le")[: budget * 2]
    clipped = encoded.decode("utf-16-le", errors="ignore")
    return clipped + _TRUNCATION_MARK


def _signature_reserve() -> int:
    """UTF-16 units the transport will append: two newlines plus the line."""
    return _utf16_units("\n\n" + sign(""))


def _is_parse_rejection(status_code: int, body: str) -> bool:
    """Telegram's 400 for markup it cannot parse — e.g. an unescaped `<`.

    This is the failure that silently killed the canary's certificate alert:
    `cert expires in 13d (<14)` under parse_mode=HTML is an "unsupported
    start tag". The check is deliberately narrow — other 400s (bad chat id,
    message too long) must not trigger a plain-text retry that would fail
    identically.
    """
    return status_code == 400 and "can't parse entities" in body.lower()


async def send_admin_photo_http(
    photo: bytes,
    caption: str = "",
    *,
    filename: str = "report.png",
    parse_mode: str = "HTML",
    token: str | None = None,
    chat_ids: Iterable[int] | None = None,
) -> int:
    """Send an image with a caption to every admin. Never raises.

    Returns the number of admins reached, which is what lets a caller fall back
    to plain text on a partial or total failure instead of assuming a picture
    got through.
    """
    if alerts_disabled():
        _log_suppressed("photo", caption or "<photo>")
        return 0

    # Before the limit check below, not after: a signature that pushed the
    # caption over Telegram's budget would cost the picture silently, and the
    # budget has to be measured against what is actually sent.
    caption = sign(caption)

    from core.config import ADMIN_USER_IDS, BOT_TOKEN

    token = token if token is not None else BOT_TOKEN
    recipients = list(chat_ids if chat_ids is not None else ADMIN_USER_IDS)

    if not token:
        logger.warning("Cannot send admin photo: BOT_TOKEN is not configured")
        return 0
    if not recipients:
        logger.warning("Cannot send admin photo: ADMIN_USER_IDS is empty")
        return 0
    if len(caption) > TELEGRAM_CAPTION_LIMIT:
        logger.warning("Caption is %d chars, over Telegram's %d limit",
                       len(caption), TELEGRAM_CAPTION_LIMIT)
        return 0

    url = f"{TELEGRAM_API}/bot{token}/sendPhoto"
    delivered = 0
    try:
        # A generous timeout: this is an upload, repeated once per recipient
        # because Telegram has no multi-chat send.
        async with httpx.AsyncClient(timeout=_TIMEOUT_SECONDS * 3) as client:
            for admin_id in recipients:
                try:
                    response = await client.post(
                        url,
                        data={
                            "chat_id": str(admin_id),
                            "caption": caption,
                            "parse_mode": parse_mode,
                        },
                        files={"photo": (filename, photo, "image/png")},
                    )
                    response.raise_for_status()
                    delivered += 1
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    logger.warning("HTTP admin photo to %s failed: %s",
                                   admin_id, exc)
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        logger.warning("HTTP admin photo transport failed: %s", exc)
        return delivered

    if delivered:
        logger.info("Admin photo delivered over HTTP to %d/%d admins",
                    delivered, len(recipients))
    record_transport_outcome(delivered, len(recipients))
    return delivered
