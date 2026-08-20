"""Current-user endpoints: /api/me, /api/me/preferences.

Previously these lived on ``auth.router`` with absolute paths and self-gated
via inline ``get_current_user`` checks. Moved here so they sit under the
``api_gate`` umbrella — single audit point for /api/* auth.
"""
import logging
from fastapi import APIRouter, Request, HTTPException, Depends

from web.routes.auth import require_user
from core.bot_prefs import default_language_for, read_language, write_language
from core.config import ADMIN_USER_IDS
from core.permissions import get_permissions_for_role_async

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

    permissions = await get_permissions_for_role_async(role)

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
