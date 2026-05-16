"""Current-user endpoints: /api/me, /api/me/preferences.

Previously these lived on ``auth.router`` with absolute paths and self-gated
via inline ``get_current_user`` checks. Moved here so they sit under the
``api_gate`` umbrella — single audit point for /api/* auth.
"""
import logging
from fastapi import APIRouter, Request, HTTPException, Depends

from web.routes.auth import require_user
from core.duckdb_store import get_store
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

    preferences = {"language": "en"}
    try:
        store = await get_store()
        async with store.connection() as conn:
            result = conn.execute(
                "SELECT language FROM user_preferences WHERE user_id = ?",
                [user_id],
            ).fetchone()
            if result:
                preferences["language"] = result[0] or "en"
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

    store = await get_store()
    async with store.connection() as conn:
        if language:
            conn.execute(
                """
                INSERT INTO user_preferences (user_id, language, updated_at)
                VALUES (?, ?, CURRENT_TIMESTAMP)
                ON CONFLICT (user_id)
                DO UPDATE SET language = excluded.language, updated_at = CURRENT_TIMESTAMP
                """,
                [user_id, language],
            )

        result = conn.execute(
            "SELECT language FROM user_preferences WHERE user_id = ?",
            [user_id],
        ).fetchone()

    return {"language": result[0] if result else "en"}
