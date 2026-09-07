"""
Authentication routes for Telegram Login.
"""
import os
import logging
from typing import Sequence

from fastapi import APIRouter, Request, Response, HTTPException, Depends
from fastapi.responses import RedirectResponse
from fastapi.templating import Jinja2Templates
from itsdangerous import URLSafeTimedSerializer, BadSignature, SignatureExpired

from core.config import config, is_production_url
from core.permissions import is_hardcoded_admin
from web.config import TEMPLATES_DIR
from web.services.auth_service import (
    verify_telegram_auth,
    verify_webapp_auth,
    check_user_access,
    check_user_access_async,
    create_session_data,
    get_user_role,
)

logger = logging.getLogger(__name__)

# Secret key for signing sessions (prefer DASHBOARD_SECRET_KEY, fallback to BOT_TOKEN)
SECRET_KEY = config.web.secret_key or config.bot.token
if not SECRET_KEY:
    raise RuntimeError("DASHBOARD_SECRET_KEY or BOT_TOKEN must be set")
if not config.web.secret_key:
    # Falling back to BOT_TOKEN: the bot token is used in many places (bot code,
    # logs, Telegram HMAC) — a leak would let an attacker forge dashboard sessions.
    logger.critical(
        "DASHBOARD_SECRET_KEY is not set — using BOT_TOKEN as the session signing "
        "key. Set a dedicated, random DASHBOARD_SECRET_KEY in production."
    )
session_serializer = URLSafeTimedSerializer(SECRET_KEY)

router = APIRouter(tags=["auth"])

# Templates. TEMPLATES_DIR is absolute, derived from this package's location;
# the literal "web/templates" that used to be here resolved against the working
# directory, so the login page rendered only when the process happened to start
# at the repository root.
templates = Jinja2Templates(directory=str(TEMPLATES_DIR))

# Bot username from environment (without @)
BOT_USERNAME = os.getenv("BOT_USERNAME", "ksorderbot")

# Session cookie name
SESSION_COOKIE = "dashboard_session"

# Session duration (7 days)
SESSION_MAX_AGE = 7 * 24 * 60 * 60

# Use secure cookies in production (HTTPS) — case-insensitive auto-detect.
DASHBOARD_URL = os.getenv("DASHBOARD_URL", "")
COOKIE_SECURE = is_production_url(DASHBOARD_URL) or os.getenv("COOKIE_SECURE", "false").lower() == "true"


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

    return templates.TemplateResponse(
        request,
        "login.html",
        {
            "bot_username": BOT_USERNAME,
            "callback_url": callback_url,
            "error": error,
            "status": status,
        },
    )


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

    # Check if user has access (pass auth_data to create/update user record)
    user_id = int(auth_data['id'])
    access = await check_user_access_async(user_id, auth_data)

    if not access['authorized']:
        status = access['status']
        logger.info(f"User {user_id} login denied - status: {status}")
        return RedirectResponse(url=f"/login?status={status}", status_code=302)

    # Get user role
    role = access.get('role', 'viewer')

    # Create session with role
    session_data = create_session_data(auth_data, role=role)

    # Sign session data and set cookie
    signed_session = session_serializer.dumps(session_data)

    # Build welcome redirect URL with user's name
    first_name = auth_data.get('first_name', '')
    from urllib.parse import quote
    welcome_param = f"?welcome={quote(first_name)}" if first_name else ""

    response = RedirectResponse(url=f"/{welcome_param}", status_code=302)
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

    The session cookie is set server-side with HttpOnly so JS can never read
    or exfiltrate it (was previously returned in the JSON body and set via
    `document.cookie`, defeating HttpOnly).
    """
    from fastapi.responses import JSONResponse

    try:
        body = await request.json()
        init_data = body.get('initData', '')
    except Exception:
        return JSONResponse({"success": False, "error": "Invalid request body"}, status_code=400)

    if not init_data:
        return JSONResponse({"success": False, "error": "No initData provided"}, status_code=400)

    # Verify the WebApp initData
    user_data = verify_webapp_auth(init_data)
    if not user_data:
        return JSONResponse({"success": False, "error": "Invalid WebApp data"}, status_code=401)

    # Check if user has access (pass user_data to create/update user record)
    user_id = int(user_data['id'])
    # Convert to auth_data format for user creation
    webapp_auth_data = {
        'id': str(user_data['id']),
        'username': user_data.get('username'),
        'first_name': user_data.get('first_name'),
        'last_name': user_data.get('last_name'),
        'photo_url': user_data.get('photo_url'),
    }
    access = await check_user_access_async(user_id, webapp_auth_data)

    if not access['authorized']:
        status = access['status']
        logger.info(f"WebApp user {user_id} denied - status: {status}")
        return JSONResponse(
            {"success": False, "error": "Not authorized", "status": status},
            status_code=403,
        )

    # Get user role
    role = access.get('role', 'viewer')

    # Create session data (convert WebApp format to standard format)
    auth_data = {
        'id': str(user_data['id']),
        'first_name': user_data.get('first_name', ''),
        'last_name': user_data.get('last_name', ''),
        'username': user_data.get('username', ''),
        'photo_url': user_data.get('photo_url', ''),
        'auth_date': str(user_data['auth_date'])
    }
    session_data = create_session_data(auth_data, role=role)
    signed_session = session_serializer.dumps(session_data)

    logger.info(f"WebApp user {user_id} (@{user_data.get('username', 'unknown')}) authenticated")

    # Set the cookie server-side with HttpOnly — the JSON body returns
    # nothing sensitive, just a success flag, so client can simply redirect.
    response = JSONResponse({"success": True})
    response.set_cookie(
        key=SESSION_COOKIE,
        value=signed_session,
        max_age=SESSION_MAX_AGE,
        httponly=True,
        samesite="lax",
        secure=COOKIE_SECURE,
    )
    return response


@router.get("/logout")
async def logout(response: Response):
    """Log out user by clearing session cookie."""
    response = RedirectResponse(url="/login", status_code=302)
    response.delete_cookie(SESSION_COOKIE)
    return response


async def _resolve_session(session: str | None) -> dict | None:
    """
    Validate a signed session string and return fresh user data, or None.

    Re-reads role/status from DuckDB so admin changes (role updates, freezes,
    denials) take effect immediately on the next request. Shared by the HTTP
    and WebSocket entry points.
    """
    if not session:
        return None

    try:
        # Verify signature and check expiration
        session_data = session_serializer.loads(session, max_age=SESSION_MAX_AGE)
        user_id = session_data.get('user_id')
        if not user_id:
            return None

        # Hardcoded admins always authorized
        if is_hardcoded_admin(user_id):
            session_data['role'] = 'admin'
            return session_data

        # Verify user is still authorized via DuckDB (primary)
        try:
            from core.duckdb_store import get_store
            store = await get_store()
            user = await store.get_user(user_id)
            if user:
                if user.get('status') != 'approved':
                    return None
                # Always use fresh role from DB, not stale session cookie
                session_data['role'] = user.get('role', 'viewer')
                # And the tab set beside it, from the same row and the same
                # read. A second query for it would put the permission check
                # on the request path this read exists to keep short; a cache
                # would make a revoked tab outlive the click that revoked it.
                session_data['allowed_features'] = user.get('allowed_features')
                return session_data
        except Exception as e:
            logger.warning(f"DuckDB user check failed, falling back to SQLite: {e}")

        # Fallback to SQLite. Reached either when DuckDB raised above OR when
        # the user is not in the DuckDB `users` table (migration period).
        # In either case we can't trust a fresh role from DuckDB, so downgrade
        # to 'viewer' rather than honouring the role baked into the cookie
        # — a demoted admin must not retain admin via stale cookie data.
        access = check_user_access(user_id)
        if not access['authorized']:
            return None
        session_data['role'] = 'viewer'
        # No row means no override to read, so the role decides — which is
        # `viewer` here for the reason above. Not an empty list: that would
        # mean "no tabs at all" and lock out somebody the fallback exists to
        # keep working.
        session_data['allowed_features'] = None
        return session_data
    except (BadSignature, SignatureExpired):
        logger.warning("Invalid or expired session signature")
        return None


async def get_current_user(request: Request) -> dict | None:
    """Get current user from the HTTP session cookie (None if not authenticated)."""
    return await _resolve_session(request.cookies.get(SESSION_COOKIE))


async def get_current_user_ws(websocket) -> dict | None:
    """Get current user from the session cookie sent on the WebSocket handshake."""
    return await _resolve_session(websocket.cookies.get(SESSION_COOKIE))


async def require_auth(request: Request) -> RedirectResponse | None:
    """
    Check if user is authenticated.

    Returns RedirectResponse to login if not authenticated, None if OK.
    """
    user = await get_current_user(request)
    if not user:
        return RedirectResponse(url="/login", status_code=302)
    return None


async def require_user(request: Request) -> dict:
    """
    FastAPI dependency for any authenticated, approved dashboard user.

    Returns user data if authenticated, raises 401 otherwise.
    """
    user = await get_current_user(request)
    if not user:
        raise HTTPException(status_code=401, detail="Authentication required")
    return user


# Paths under /api that are intentionally reachable without a session.
# Keep this set tiny — every entry here is a deliberate, audited exception.
# /api/health is polled by Docker, nginx and external uptime monitors.
# /api/health is polled by Docker, nginx and external uptime monitors.
# /api/webhooks/turbosms is called by the SMS gateway, which cannot hold a
# session; it authenticates itself with a SHA1 signature over a shared secret
# and rejects anything unsigned. See web/routes/api/webhooks.py.
PUBLIC_API_PATHS: set[str] = {"/api/health", "/api/webhooks/turbosms"}


async def api_gate(request: Request) -> None:
    """
    Single authentication gate for the entire ``/api`` surface.

    Applied at the router-include level in ``web/main.py`` so every API
    endpoint inherits it without per-router or per-endpoint repetition. The
    only way to expose an endpoint without a session is to add its path to
    ``PUBLIC_API_PATHS`` — one place to audit.

    Admin-only routers stack their own ``Depends(require_admin)`` on top of
    this (both run; require_admin is stricter).
    """
    if request.url.path in PUBLIC_API_PATHS:
        return
    user = await require_user(request)
    require_admin_for_internal(user, request.query_params.get("sales_type"))


def require_admin_for_internal(user: dict, sales_type: str | None) -> None:
    """`sales_type=internal` is admin-only.

    It is staff activity outside retail and wholesale — one manager's own
    sales, another's shipments to bloggers — and who did what is not for
    every dashboard viewer. Enforced here rather than in `validate_sales_type`
    because that function never sees who is asking, and once per gate rather
    than per endpoint because there are dozens of endpoints and one gate.

    Note `sales_type=all` still spans every category, as it always has; it
    reveals a total, not who is inside it.

    The value is normalised the same way ``validate_sales_type`` normalises it
    (``.lower().strip()``) *before* the comparison. The gate reads the raw query
    string, but the endpoint downstream folds ``Internal``/`` internal `` into
    ``internal`` — so without matching that folding here, casing or surrounding
    whitespace would slip an internal request past the gate only to be run as
    internal. ``None`` and the empty string stay non-``internal`` and pass.
    """
    normalized = sales_type.lower().strip() if isinstance(sales_type, str) else sales_type
    if normalized != "internal":
        return
    user_id = user.get("user_id")
    if user.get("role") != "admin" and not is_hardcoded_admin(user_id):
        raise HTTPException(
            status_code=403, detail="sales_type=internal is admin-only",
        )


async def require_admin(request: Request) -> dict:
    """
    FastAPI dependency for admin-only endpoints.

    Returns user data if authenticated and admin, raises HTTPException otherwise.
    """
    user = await get_current_user(request)
    if not user:
        raise HTTPException(status_code=401, detail="Authentication required")

    user_id = user.get('user_id')
    role = user.get('role', 'viewer')

    # Check role from session (already refreshed from DB by get_current_user)
    if role != 'admin' and not is_hardcoded_admin(user_id):
        raise HTTPException(status_code=403, detail="Admin access required")

    return user


async def effective_permissions(user: dict) -> dict:
    """What this session may do: the role's matrix, narrowed to its tab set.

    The two halves come from different places on purpose. The matrix is per
    *role*, stored, editable from the admin page and cached in-process. The tab
    set is per *person*, carried in the session dict by ``_resolve_session``
    from the row it already read. `core.permissions.apply_feature_override`
    holds the rule that combines them, and is pure, so both this and the bot
    can state the same answer without sharing a database.
    """
    from core.permissions import apply_feature_override, get_permissions_for_role_async

    permissions = await get_permissions_for_role_async(user.get("role", "viewer"))
    return apply_feature_override(permissions, user.get("allowed_features"))


async def has_permission(user: dict, feature: str, action: str = "view") -> bool:
    """Does this user hold `action` on `feature`? Never raises.

    The counterpart to ``require_permission`` for the cases a dependency
    cannot express — one endpoint whose *arguments* decide how much access it
    needs, such as an SMS roster that returns sizes to a viewer and names and
    phone numbers to whoever may send.
    """
    if is_hardcoded_admin(user.get("user_id")):
        return True
    try:
        permissions = await effective_permissions(user)
    except Exception:  # noqa: BLE001 — an unreadable matrix must deny, not crash
        return False
    return bool(permissions.get(feature, {}).get(action, False))


def require_permission(feature: str, action: str = "view"):
    """
    FastAPI dependency factory for permission-based access control.

    Uses DB-backed permissions (falls back to hardcoded if DB unavailable).

    Usage:
        @router.get("/expenses")
        async def get_expenses(user = Depends(require_permission("expenses", "view"))):
            ...
    """
    async def check_permission(request: Request) -> dict:
        user = await get_current_user(request)
        if not user:
            raise HTTPException(status_code=401, detail="Authentication required")

        # Hardcoded admins have all permissions
        if is_hardcoded_admin(user.get('user_id')):
            return user

        permissions = await effective_permissions(user)
        feature_perms = permissions.get(feature, {})
        if not feature_perms.get(action, False):
            raise HTTPException(
                status_code=403,
                detail=f"No {action} access to {feature}"
            )

        return user

    return check_permission


def require_any_permission(features: Sequence[str], action: str = "view"):
    """Dependency for an endpoint that more than one tab legitimately reads.

    `/api/summary` is the case that forced it: the revenue totals are on the
    dashboard, and they are also what `ROASSection` divides ad spend by on
    /traffic and what `ROICalculator` reads on /marketing. Gating it on
    `dashboard` alone would have made "traffic only" a tab that renders empty
    cards, which is the failure mode a per-tab gate is supposed to prevent.

    Passing a single feature here is the same as ``require_permission``; the
    list is a statement that the endpoint is genuinely shared, and each entry
    is one page that would break without it.
    """

    async def check_any(request: Request) -> dict:
        user = await get_current_user(request)
        if not user:
            raise HTTPException(status_code=401, detail="Authentication required")

        if is_hardcoded_admin(user.get('user_id')):
            return user

        permissions = await effective_permissions(user)
        if any(permissions.get(f, {}).get(action, False) for f in features):
            return user

        raise HTTPException(
            status_code=403,
            detail=f"No {action} access to {' or '.join(features)}",
        )

    return check_any


# /api/me and /api/me/preferences moved to web/routes/api/me.py so they sit
# under the api_gate audit umbrella (was previously self-gated here, outside
# the structural invariant).
