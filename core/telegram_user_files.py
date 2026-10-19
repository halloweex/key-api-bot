"""Send a file a user asked for to that user's own chat with the bot.

The dashboard opens inside Telegram as a Mini App, and Telegram's WebView does
not save a `blob:` link — an `<a download>` click there does nothing at all.
So an export made on the page goes to the person who made it, through the bot,
as a document. That is a reply to one's own action, the audience
`tests/unit/test_alert_recipients.py` allows: the recipient is the session's
own Telegram id and nobody else, never a list.

Honours `KS_ALERTS_DISABLED` like every other outgoing Telegram path.
"""
import asyncio
import logging

import httpx

from core.telegram_alerts import TELEGRAM_API, alerts_disabled

logger = logging.getLogger(__name__)

_TIMEOUT_SECONDS = 30


async def send_user_document_http(
    chat_id: int,
    content: bytes,
    filename: str,
    caption: str = "",
    *,
    mime_type: str = "text/csv",
) -> bool:
    """Send one document to one chat. Never raises; True when delivered."""
    if alerts_disabled():
        logger.info("Telegram disabled (KS_ALERTS_DISABLED): document %s not sent",
                    filename)
        return False

    from core.config import BOT_TOKEN

    if not BOT_TOKEN:
        logger.warning("Cannot send document: BOT_TOKEN is not configured")
        return False

    url = f"{TELEGRAM_API}/bot{BOT_TOKEN}/sendDocument"
    data = {"chat_id": str(chat_id)}
    if caption:
        data["caption"] = caption
    try:
        async with httpx.AsyncClient(timeout=_TIMEOUT_SECONDS) as client:
            response = await client.post(
                url, data=data, files={"document": (filename, content, mime_type)},
            )
            response.raise_for_status()
        return True
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        # The text of an httpx error carries the URL, and the URL carries the
        # token; log the class and status only.
        status = getattr(getattr(exc, "response", None), "status_code", None)
        logger.warning("Document %s to %s failed: %s (status %s)",
                       filename, chat_id, type(exc).__name__, status)
        return False
