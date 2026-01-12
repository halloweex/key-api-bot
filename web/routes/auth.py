"""
Authentication routes for Telegram Login.
"""
import os
import logging
from fastapi import APIRouter, Request, Response
from fastapi.responses import RedirectResponse, HTMLResponse
from fastapi.templating import Jinja2Templates
from itsdangerous import URLSafeTimedSerializer, BadSignature, SignatureExpired

from web.services.auth_service import (
    verify_telegram_auth,
    check_user_access,
    create_session_data
)

logger = logging.getLogger(__name__)

# Secret key for signing sessions (use BOT_TOKEN as secret)
SECRET_KEY = os.getenv("BOT_TOKEN", "fallback-secret-key-change-me")
session_serializer = URLSafeTimedSerializer(SECRET_KEY)

router = APIRouter(tags=["auth"])

# Templates
templates = Jinja2Templates(directory="web/templates")

# Bot username from environment (without @)
BOT_USERNAME = os.getenv("BOT_USERNAME", "ksorderbot")

# Session cookie name
SESSION_COOKIE = "dashboard_session"

# Session duration (7 days)
SESSION_MAX_AGE = 7 * 24 * 60 * 60

# Use secure cookies in production (HTTPS)
COOKIE_SECURE = os.getenv("COOKIE_SECURE", "false").lower() == "true"


@router.get("/login")
async def login_page(request: Request, error: str = None, status: str = None):
    """Show login page with Telegram Login Widget."""
    # Check if already logged in
    session = request.cookies.get(SESSION_COOKIE)
    if session:
        # Verify session is still valid (with signature)
        try:
            session_data = session_serializer.loads(session, max_age=SESSION_MAX_AGE)
            user_id = session_data.get('user_id')
            if user_id:
                access = check_user_access(user_id)
                if access['authorized']:
                    return RedirectResponse(url="/", status_code=302)
        except (BadSignature, SignatureExpired):
            pass

    # Build callback URL
    callback_url = str(request.url_for('telegram_callback'))

    return templates.TemplateResponse("login.html", {
        "request": request,
        "bot_username": BOT_USERNAME,
        "callback_url": callback_url,
        "error": error,
        "status": status
    })


@router.get("/auth/telegram/callback")
async def telegram_callback(request: Request):
    """
    Handle Telegram Login callback.

    Telegram sends auth data as query parameters:
    id, first_name, last_name, username, photo_url, auth_date, hash
    """
    # Get all query params as auth data
    auth_data = dict(request.query_params)

    if not auth_data:
        logger.warning("Empty auth data received")
        return RedirectResponse(url="/login?error=No+authentication+data+received", status_code=302)

    # Verify the auth data
    if not verify_telegram_auth(auth_data):
        logger.warning(f"Invalid auth data: {auth_data.get('id', 'unknown')}")
        return RedirectResponse(url="/login?error=Invalid+authentication+data", status_code=302)

    # Check if user has access
    user_id = int(auth_data['id'])
    access = check_user_access(user_id)

    if not access['authorized']:
        status = access['status']
        logger.info(f"User {user_id} login denied - status: {status}")
        return RedirectResponse(url=f"/login?status={status}", status_code=302)

    # Create session
    session_data = create_session_data(auth_data)

    # Sign session data and set cookie
    signed_session = session_serializer.dumps(session_data)
    response = RedirectResponse(url="/", status_code=302)
    response.set_cookie(
        key=SESSION_COOKIE,
        value=signed_session,
        max_age=SESSION_MAX_AGE,
        httponly=True,
        samesite="lax",
        secure=COOKIE_SECURE
    )

    logger.info(f"User {user_id} (@{auth_data.get('username', 'unknown')}) logged in successfully")
    return response


@router.get("/logout")
async def logout(response: Response):
    """Log out user by clearing session cookie."""
    response = RedirectResponse(url="/login", status_code=302)
    response.delete_cookie(SESSION_COOKIE)
    return response


def get_current_user(request: Request) -> dict | None:
    """
    Get current user from session cookie.

    Returns user data dict or None if not authenticated.
    """
    session = request.cookies.get(SESSION_COOKIE)
    if not session:
        return None

    try:
        # Verify signature and check expiration
        session_data = session_serializer.loads(session, max_age=SESSION_MAX_AGE)
        user_id = session_data.get('user_id')
        if not user_id:
            return None

        # Verify user is still authorized
        access = check_user_access(user_id)
        if not access['authorized']:
            return None

        return session_data
    except (BadSignature, SignatureExpired):
        logger.warning("Invalid or expired session signature")
        return None


def require_auth(request: Request) -> RedirectResponse | None:
    """
    Check if user is authenticated.

    Returns RedirectResponse to login if not authenticated, None if OK.
    """
    user = get_current_user(request)
    if not user:
        return RedirectResponse(url="/login", status_code=302)
    return None
