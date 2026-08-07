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
    """Suppresses identical alerts within a cooldown, counting what it swallowed.

    Keyed on exact message text, so distinct alerts never mask one another and
    a changing metric in the body still gets through. State is per-process and
    resets on restart — deliberately: after a restart the first alert of each
    kind should always land.
    """

    def __init__(self, cooldown_seconds: float = DEFAULT_COOLDOWN_SECONDS):
        self.cooldown_seconds = cooldown_seconds
        self._last_sent: dict[str, float] = {}
        self._suppressed: dict[str, int] = {}

    def check(self, text: str, *, now: float | None = None) -> "tuple[bool, int]":
        """Return (should_send, suppressed_since_last_send).

        Calling this records the decision, so call it exactly once per attempt.
        """
        now = time.monotonic() if now is None else now
        last = self._last_sent.get(text)
        if last is not None and (now - last) < self.cooldown_seconds:
            self._suppressed[text] = self._suppressed.get(text, 0) + 1
            return False, self._suppressed[text]
        swallowed = self._suppressed.pop(text, 0)
        self._last_sent[text] = now
        return True, swallowed


_throttle = AlertThrottle()


def throttle_check(text: str) -> "tuple[bool, str]":
    """Decide whether `text` should go out, and what exactly to send.

    Returns (should_send, text_to_send). When an alert has been muted, the copy
    that finally lands says how many it stood in for — silence about the
    suppression would understate how long the condition has been shouting.
    """
    allow, swallowed = _throttle.check(text)
    if not allow:
        logger.debug("Admin alert suppressed (repeat #%d within cooldown)", swallowed)
        return False, text
    if swallowed:
        minutes = int(_throttle.cooldown_seconds // 60)
        return True, (
            f"{text}\n\n(unchanged, and repeated {swallowed}× "
            f"in the last {minutes} min)"
        )
    return True, text


def reset_throttle() -> None:
    """Drop all throttle state. For tests and for a deliberate re-arm."""
    _throttle._last_sent.clear()
    _throttle._suppressed.clear()


async def send_admin_message_http(
    text: str,
    parse_mode: str = "HTML",
    *,
    token: str | None = None,
    admin_ids: Iterable[int] | None = None,
) -> int:
    """Send `text` to every admin over the HTTP Bot API. Never raises.

    Returns the number of admins the message actually reached, so callers can
    tell "delivered" from "silently dropped" — the distinction this whole module
    exists to restore.
    """
    from core.config import ADMIN_USER_IDS, BOT_TOKEN

    token = token if token is not None else BOT_TOKEN
    recipients = list(admin_ids if admin_ids is not None else ADMIN_USER_IDS)

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
    return delivered
