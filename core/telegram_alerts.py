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
from typing import Iterable

import httpx

logger = logging.getLogger(__name__)

TELEGRAM_API = "https://api.telegram.org"

# Alerts are diagnostics, not user traffic — a slow Telegram must never stall a
# warehouse refresh or a scheduler job.
_TIMEOUT_SECONDS = 10.0


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
