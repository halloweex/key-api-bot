"""
Authentication routes for Telegram Login.
"""
import os
import logging
from fastapi import APIRouter, Request, Response, HTTPException, Depends
from fastapi.responses import RedirectResponse, HTMLResponse
from fastapi.templating import Jinja2Templates
from itsdangerous import URLSafeTimedSerializer, BadSignature, SignatureExpired

from core.config import config
from web.services.auth_service import (
    verify_telegram_auth,
    verify_webapp_auth,
    check_user_access,
    create_session_data
)

logger = logging.getLogger(__name__)

# Secret key for signing sessions (prefer DASHBOARD_SECRET_KEY, fallback to BOT_TOKEN)
SECRET_KEY = config.web.secret_key or config.bot.token
if not SECRET_KEY:
    raise RuntimeError("DASHBOARD_SECRET_KEY or BOT_TOKEN must be set")
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

# Use secure cookies in production (HTTPS) - auto-detect from DASHBOARD_URL
DASHBOARD_URL = os.getenv("DASHBOARD_URL", "")
COOKIE_SECURE = DASHBOARD_URL.startswith("https://") or os.getenv("COOKIE_SECURE", "false").lower() == "true"


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


@router.post("/auth/webapp")
async def webapp_auth(request: Request):
    """
    Handle Telegram WebApp authentication.

    Receives initData from Telegram.WebApp.initData and verifies it.
    Used when dashboard is opened via MenuButtonWebApp.
    """
    try:
        body = await request.json()
        init_data = body.get('initData', '')
    except Exception:
        return {"success": False, "error": "Invalid request body"}

    if not init_data:
        return {"success": False, "error": "No initData provided"}

    # Verify the WebApp initData
    user_data = verify_webapp_auth(init_data)
    if not user_data:
        return {"success": False, "error": "Invalid WebApp data"}

    # Check if user has access
    user_id = int(user_data['id'])
    access = check_user_access(user_id)

    if not access['authorized']:
        status = access['status']
        logger.info(f"WebApp user {user_id} denied - status: {status}")
        return {"success": False, "error": "Not authorized", "status": status}

    # Create session data (convert WebApp format to standard format)
    auth_data = {
        'id': str(user_data['id']),
        'first_name': user_data.get('first_name', ''),
        'last_name': user_data.get('last_name', ''),
        'username': user_data.get('username', ''),
        'photo_url': user_data.get('photo_url', ''),
        'auth_date': str(user_data['auth_date'])
    }
    session_data = create_session_data(auth_data)

    # Sign session data
    signed_session = session_serializer.dumps(session_data)

    logger.info(f"WebApp user {user_id} (@{user_data.get('username', 'unknown')}) authenticated")

    # Return success with session cookie value (client will set it)
    response = {"success": True, "session": signed_session}
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


async def require_admin(request: Request) -> dict:
    """
    FastAPI dependency for admin-only endpoints.

    Returns user data if authenticated and admin, raises HTTPException otherwise.
    """
    user = get_current_user(request)
    if not user:
        raise HTTPException(status_code=401, detail="Authentication required")

    if not config.bot.is_admin(user.get('user_id')):
        raise HTTPException(status_code=403, detail="Admin access required")

    return user
