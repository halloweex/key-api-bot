"""Current-user endpoints: /api/me, /api/me/preferences.

Previously these lived on ``auth.router`` with absolute paths and self-gated
via inline ``get_current_user`` checks. Moved here so they sit under the
``api_gate`` umbrella — single audit point for /api/* auth.
"""
import logging
import re

from fastapi import APIRouter, Request, HTTPException, Depends
from pydantic import BaseModel, Field

from web.routes.auth import effective_permissions, require_user
from core.bot_prefs import default_language_for, read_language, write_language
from core.config import ADMIN_USER_IDS
from core.permissions import is_hardcoded_admin
from ._deps import limiter

logger = logging.getLogger(__name__)

router = APIRouter()

SUPPORTED_LANGUAGES = {"en", "uk", "ru"}


@router.get("/me")
async def get_current_user_info(user: dict = Depends(require_user)):
    """Return current user identity, role, permissions and preferences.

    api_gate enforces session presence at the router-include level;
    ``require_user`` here injects the fresh user dict (role re-read from DB).
    """
    user_id = user.get("user_id")
    role = user.get("role", "viewer")

    # The role's matrix narrowed to this person's tab set. A hardcoded admin
    # short-circuits every gate on the server (`is_hardcoded_admin`), so the
    # page must be told the same thing — otherwise the sidebar hides tabs the
    # API would happily serve, which reads as a broken dashboard rather than
    # as a permission.
    permissions = await effective_permissions(user)
    allowed_features = user.get("allowed_features")
    if is_hardcoded_admin(user_id):
        from core.permissions import Feature

        permissions = {
            feature.value: {"view": True, "edit": True, "delete": True}
            for feature in Feature
        }
        allowed_features = None

    # The bot's SQLite, not DuckDB. Both databases declare a `user_preferences`
    # table with the same columns, and until 2026-08-20 this endpoint used the
    # DuckDB one while the bot, the settings screen and the weekly report all
    # used the other. A language chosen here reached nothing; a language chosen
    # in the bot never showed here. The DuckDB copy held zero rows in production
    # the whole time, which is how it went unnoticed.
    preferences = {"language": default_language_for(user_id, ADMIN_USER_IDS)}
    try:
        preferences["language"] = read_language(
            user_id, default=preferences["language"]
        )
    except Exception as e:
        logger.debug(f"Failed to load preferences for user {user_id}: {e}")

    return {
        "user": {
            "id": user_id,
            "username": user.get("username", ""),
            "first_name": user.get("first_name", ""),
            "last_name": user.get("last_name", ""),
            "photo_url": user.get("photo_url", ""),
            "role": role,
        },
        "permissions": permissions,
        # What the admin ticked, or null for "as the role". The page shows it
        # nowhere; it is here so the nav can say *why* a tab is missing and so
        # the admin screens do not need a second round trip to find out.
        "allowed_features": allowed_features,
        "preferences": preferences,
    }


@router.patch("/me/preferences")
async def update_preferences(request: Request, user: dict = Depends(require_user)):
    """Update current user's preferences (language, etc.)."""
    user_id = user.get("user_id")

    try:
        body = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON body")

    language = body.get("language")
    if language and language not in SUPPORTED_LANGUAGES:
        raise HTTPException(
            status_code=400,
            detail=f"Unsupported language: {language}. Supported: {', '.join(sorted(SUPPORTED_LANGUAGES))}",
        )

    fallback = default_language_for(user_id, ADMIN_USER_IDS)
    if language:
        try:
            # Raises rather than swallowing: a settings screen that reports
            # success while storing nothing somewhere else is the bug this
            # replaced.
            return {"language": write_language(user_id, language)}
        except Exception as e:
            logger.error("Could not store language for user %s: %s", user_id, e)
            raise HTTPException(
                status_code=503,
                detail="Preferences are temporarily unavailable; nothing was changed",
            )

    return {"language": read_language(user_id, default=fallback)}


# ─── a file to my own chat ───────────────────────────────────────────────────

_FILENAME = re.compile(r"^[A-Za-z0-9._-]{0,80}[A-Za-z0-9_-]\.csv$")
_FILE_MAX_CHARS = 5_000_000


class SendFile(BaseModel):
    filename: str
    content: str = Field(..., max_length=_FILE_MAX_CHARS)


@router.post("/me/send-file")
@limiter.limit("10/minute")
async def send_file_to_my_chat(
    request: Request,
    body: SendFile,
    user: dict = Depends(require_user),
):
    """Send a CSV the page already holds to the requester's own bot chat.

    The dashboard opens as a Telegram Mini App, whose WebView can neither save
    a `blob:` download nor follow a download link with the session — Telegram
    hands `window.open` to the external browser, which is not signed in. So
    inside Telegram every export button fetches its file as it would anyway,
    under this user's own permissions, and posts it here. Nothing is read on
    the server and nothing widens: the content is what this user was just
    served, and the recipient is this user's own Telegram id, never a list.
    """
    from core.telegram_user_files import send_user_document_http

    if not _FILENAME.match(body.filename):
        raise HTTPException(status_code=400, detail="Invalid filename")
    user_id = user.get("user_id")
    if not user_id:
        raise HTTPException(status_code=401, detail="Authentication required")

    # UTF-8 BOM, as the downloads write it, so Excel reads Cyrillic.
    data = ("\ufeff" + body.content.lstrip("\ufeff")).encode("utf-8")
    if not await send_user_document_http(int(user_id), data, body.filename):
        raise HTTPException(status_code=502, detail="Could not send the file via Telegram")
    return {"sent": True}
