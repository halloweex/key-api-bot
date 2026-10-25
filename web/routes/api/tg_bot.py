"""The customer Telegram bot's activity, for the admin page /tg-bot.

Admin-only, gated once at the include in ``web/routes/api/__init__.py``. Not a
tab in ``core/permissions.py``: the numbers carry no customer data, but nobody
has asked to grant them to anyone, and a tab is the access model's to add when
someone does. The data is the bot's own readout — see
``web/services/tg_bot_service.py`` for how it is fetched and why it is cached.
"""
from fastapi import APIRouter, HTTPException, Query, Request

from web.services import tg_bot_service
from ._deps import get_logger, limiter

router = APIRouter()
logger = get_logger(__name__)


@router.get("/tg-bot/activity")
@limiter.limit("30/minute")
async def get_tg_bot_activity(
    request: Request,
    days: int = Query(tg_bot_service.DEFAULT_WINDOW),
):
    """Daily and weekly activity of the customer bot, and its totals."""
    if days not in tg_bot_service.WINDOWS:
        raise HTTPException(
            status_code=400,
            detail=f"days must be one of {list(tg_bot_service.WINDOWS)}")
    try:
        return await tg_bot_service.get_activity(days)
    except tg_bot_service.BotUnavailable as exc:
        logger.warning(f"Telegram bot activity unavailable: {exc}")
        raise HTTPException(status_code=503, detail=str(exc))
