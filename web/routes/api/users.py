"""Admin user management and permissions endpoints."""
import logging

from fastapi import APIRouter, Query, Request, HTTPException, Depends
from typing import Optional

from core.permissions import is_hardcoded_admin
from web.routes.auth import require_admin
from ._deps import limiter, get_store

router = APIRouter()
logger = logging.getLogger(__name__)


def _refuse_if_ineffective(user_id: int, field: str, value: str, enforced: str) -> None:
    """Refuse a change to an account whose access is pinned in source.

    ``_resolve_session`` short-circuits on ``is_hardcoded_admin`` before it
    reads either column (web/routes/auth.py), so for those ids the stored role
    and status decide nothing. Denying one used to answer 200 having changed
    no access at all, and the admin page then showed "denied" beside an account
    that still had everything — a control reporting a revocation it did not
    perform is worse than one that is absent.

    Refused rather than enforced: the hardcoded set is the way back in when the
    users table or the permissions system is unusable, so honouring a stored
    status there would put the only recovery path behind one click of a
    dropdown, undoable only by editing source and redeploying. Writing the
    value the code already enforces stays allowed, so a row this once corrupted
    can be corrected back into agreement.
    """
    if not is_hardcoded_admin(user_id) or value == enforced:
        return
    raise HTTPException(
        status_code=409,
        detail=(
            f"This account's admin access is pinned in source; its {field} is "
            f"always '{enforced}' whatever is stored. Changing it here would "
            f"report a change that does not happen."
        ),
    )


# ─── User Management ──────────────────────────────────────────────────────────

@router.get("/admin/users")
@limiter.limit("30/minute")
async def list_users(
    request: Request,
    status: Optional[str] = Query(None),
    role: Optional[str] = Query(None),
    limit: int = Query(100, ge=1, le=500),
    offset: int = Query(0, ge=0),
    user: dict = Depends(require_admin),
):
    """List all users (admin only)."""
    store = await get_store()
    users = await store.list_users(status=status, role=role, limit=limit, offset=offset)
    return {"users": users, "count": len(users)}


@router.get("/admin/users/{user_id}")
@limiter.limit("30/minute")
async def get_user(
    request: Request,
    user_id: int,
    user: dict = Depends(require_admin),
):
    """Get a specific user by ID (admin only)."""
    store = await get_store()
    target_user = await store.get_user(user_id)
    if not target_user:
        raise HTTPException(status_code=404, detail="User not found")
    return {"user": target_user}


@router.patch("/admin/users/{user_id}/role")
@limiter.limit("10/minute")
async def update_user_role(
    request: Request,
    user_id: int,
    role: str = Query(..., description="New role: admin, editor, marketer, viewer"),
    user: dict = Depends(require_admin),
):
    """Update user role (admin only)."""
    # The list of roles lives in core.permissions; restating it here is how a
    # role becomes settable everywhere except through the admin page.
    from core.permissions import Role

    if role not in {r.value for r in Role}:
        raise HTTPException(status_code=400, detail="Invalid role")

    _refuse_if_ineffective(user_id, "role", role, Role.ADMIN.value)

    store = await get_store()
    admin_id = user.get("user_id")
    success = await store.update_user_role(user_id, role, changed_by=admin_id)
    if not success:
        raise HTTPException(status_code=404, detail="User not found")

    logger.info(f"Admin {admin_id} changed user {user_id} role to {role}")
    return {"success": True, "user_id": user_id, "role": role}


@router.patch("/admin/users/{user_id}/status")
@limiter.limit("10/minute")
async def update_user_status(
    request: Request,
    user_id: int,
    status: str = Query(..., description="New status: approved, denied, frozen, pending"),
    user: dict = Depends(require_admin),
):
    """Update user status (admin only)."""
    if status not in ("approved", "denied", "frozen", "pending"):
        raise HTTPException(status_code=400, detail="Invalid status")

    _refuse_if_ineffective(user_id, "status", status, "approved")

    store = await get_store()
    admin_id = user.get("user_id")
    success = await store.update_user_status(user_id, status, reviewed_by=admin_id)
    if not success:
        raise HTTPException(status_code=404, detail="User not found")

    logger.info(f"Admin {admin_id} changed user {user_id} status to {status}")
    return {"success": True, "user_id": user_id, "status": status}


@router.patch("/admin/users/{user_id}/features")
@limiter.limit("20/minute")
async def update_user_features(
    request: Request,
    user_id: int,
    user: dict = Depends(require_admin),
):
    """Set which tabs one account may open (admin only).

    The body is either ``{"preset": "traffic_only"}`` — one of the code-defined
    bundles in `core.permissions.ACCESS_PRESETS` — or ``{"features": [...]}``
    with the keys ticked in the checklist. ``{"features": null}`` clears the
    override and the account goes back to whatever its role shows, which is
    what every account meant before this existed.

    A body rather than query parameters, unlike its neighbours here: this is a
    list, and the alternative — repeating ``?feature=a&feature=b`` — makes the
    empty set (a real, reachable state: somebody ticked nothing) indistinguishable
    from "the parameter was omitted", which is the one distinction the column
    carries.
    """
    from core.permissions import (
        ACCESS_PRESETS, TAB_FEATURE_KEYS, normalize_features, preset_features,
    )

    try:
        body = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON body")

    if not isinstance(body, dict):
        raise HTTPException(status_code=400, detail="Body must be an object")

    if "preset" in body:
        preset = body.get("preset")
        features = preset_features(preset) if isinstance(preset, str) else None
        if features is None:
            raise HTTPException(
                status_code=400,
                detail=f"Unknown preset. Must be one of: {sorted(ACCESS_PRESETS)}",
            )
    elif "features" in body:
        raw = body.get("features")
        if raw is None:
            features = None
        elif isinstance(raw, list) and all(isinstance(v, str) for v in raw):
            unknown = sorted(set(raw) - set(TAB_FEATURE_KEYS))
            if unknown:
                # Refused, not dropped. `normalize_features` drops silently
                # because it also reads stored rows, where an unknown key means
                # a tab that was removed. Here it means the caller and the
                # server disagree about what a tab is, and an admin who ticked
                # something must not be told it was saved when it was not.
                raise HTTPException(
                    status_code=400,
                    detail=f"Not tabs: {unknown}. Known: {list(TAB_FEATURE_KEYS)}",
                )
            features = normalize_features(raw)
        else:
            raise HTTPException(
                status_code=400, detail="features must be a list of strings, or null",
            )
    else:
        raise HTTPException(
            status_code=400, detail="Pass either 'features' or 'preset'",
        )

    # A hardcoded admin short-circuits every gate on the server, so a tab set
    # stored against one decides nothing — `_refuse_if_ineffective`'s reason,
    # and the same failure it exists to prevent: a control reporting a change
    # that does not happen. Clearing the override is still allowed, so a row
    # that already carries one can be put back into agreement.
    if features is not None and is_hardcoded_admin(user_id):
        raise HTTPException(
            status_code=409,
            detail=(
                "This account's admin access is pinned in source; it opens "
                "every tab whatever is stored. Setting tabs here would report "
                "a change that does not happen."
            ),
        )

    store = await get_store()
    admin_id = user.get("user_id")
    if not await store.set_user_features(user_id, features, changed_by=admin_id):
        raise HTTPException(status_code=404, detail="User not found")

    logger.info("Admin %s set tabs for user %s to %s", admin_id, user_id, features)
    return {"success": True, "user_id": user_id, "allowed_features": features}


# ─── Incoming access requests ─────────────────────────────────────────────────
#
# A person asking for access lands in the **bot's** list (`app.authorized_users`)
# as `pending`, because the only door is Telegram. The dashboard's list gains a
# row when somebody approves them — so until this existed, an incoming request
# was invisible on the admin page and the decision could only be taken from a
# phone. That is the half of "manage access in the UI" that was missing.
#
# The two lists still are not merged: this reads the bot's queue and writes the
# same two rows the bot's own Approve button writes, through the same port and
# the same statement. Nothing here decides that the lists are one.


def _request_row(row: dict) -> dict:
    """One pending request, in the shape the admin page renders."""
    return {
        "user_id": row.get("user_id"),
        "username": row.get("username"),
        "first_name": row.get("first_name"),
        "last_name": row.get("last_name"),
        "requested_at": row.get("requested_at"),
        "denial_count": row.get("denial_count") or 0,
    }


@router.get("/admin/access-requests")
@limiter.limit("30/minute")
async def list_access_requests(
    request: Request,
    user: dict = Depends(require_admin),
):
    """Who is waiting for access (admin only)."""
    from bot import database

    try:
        pending = database.get_pending_requests()
    except Exception as e:  # noqa: BLE001 — an unreachable bot store must not
        # take the whole admin page down; the rest of it reads another store.
        logger.error("Could not read pending access requests: %s", e, exc_info=True)
        raise HTTPException(
            status_code=503,
            detail="The access-request queue is unavailable right now",
        )

    return {"requests": [_request_row(row) for row in pending], "count": len(pending)}


async def _tell_the_person(user_id: int, key: str) -> None:
    """Say what was decided, in their language. Never raises.

    The bot says this from its own Application; from here it goes over the same
    HTTP transport the weekly report uses, which means it honours
    `KS_ALERTS_DISABLED` and — through `sign_for` — does not put an instance
    signature under a message to somebody who is not an admin.

    A failure here is logged and swallowed: the access decision is already
    written, and refusing the admin's click because a Telegram delivery failed
    would leave the two disagreeing.
    """
    from bot import database
    from core.i18n import t
    from core.telegram_alerts import send_admin_message_http

    try:
        language = database.get_user_language(user_id)
        await send_admin_message_http(t(key, language), chat_ids=[user_id])
    except Exception as e:  # noqa: BLE001
        logger.warning("Could not tell user %s about the decision: %s", user_id, e)


@router.post("/admin/access-requests/{user_id}/approve")
@limiter.limit("20/minute")
async def approve_access_request(
    request: Request,
    user_id: int,
    user: dict = Depends(require_admin),
):
    """Approve a pending request and set the tabs it opens (admin only).

    The body is `{"preset": ...}` or `{"features": [...]}`, exactly as
    `/admin/users/{id}/features` takes them, and omitting it grants the
    default preset — which is what the bot's Approve button grants, and what a
    viewer could reach before per-user tabs existed.
    """
    from bot import database
    from core.permissions import (
        ACCESS_PRESETS, DEFAULT_PRESET, TAB_FEATURE_KEYS,
        normalize_features, preset_features,
    )

    try:
        body = await request.json()
    except Exception:
        body = {}
    if not isinstance(body, dict):
        raise HTTPException(status_code=400, detail="Body must be an object")

    if "preset" in body:
        features = preset_features(body.get("preset")) if isinstance(
            body.get("preset"), str) else None
        if features is None:
            raise HTTPException(
                status_code=400,
                detail=f"Unknown preset. Must be one of: {sorted(ACCESS_PRESETS)}",
            )
    elif "features" in body:
        raw = body.get("features")
        if not isinstance(raw, list) or not all(isinstance(v, str) for v in raw):
            raise HTTPException(
                status_code=400, detail="features must be a list of strings",
            )
        unknown = sorted(set(raw) - set(TAB_FEATURE_KEYS))
        if unknown:
            raise HTTPException(
                status_code=400,
                detail=f"Not tabs: {unknown}. Known: {list(TAB_FEATURE_KEYS)}",
            )
        features = normalize_features(raw)
    else:
        features = preset_features(DEFAULT_PRESET)

    admin_id = user.get("user_id")
    profile = database.get_user_auth_status(user_id) or {}

    # `expected_status="pending"` for the bot's reason: two admins tapping
    # Approve and Deny on the same request in the same second used to be
    # last-write-wins, with the person told both.
    if not database.approve_user(user_id, admin_id, expected_status="pending"):
        raise HTTPException(
            status_code=409,
            detail="This request has already been decided by somebody else",
        )

    store = await get_store()
    await store.grant_access(
        user_id,
        reviewed_by=admin_id,
        features=features,
        username=profile.get("username"),
        first_name=profile.get("first_name"),
        last_name=profile.get("last_name"),
    )
    await _tell_the_person(user_id, "access.granted")

    logger.info(
        "Admin %s approved access request %s with tabs %s",
        admin_id, user_id, features,
    )
    return {"success": True, "user_id": user_id, "allowed_features": features}


@router.post("/admin/access-requests/{user_id}/deny")
@limiter.limit("20/minute")
async def deny_access_request(
    request: Request,
    user_id: int,
    user: dict = Depends(require_admin),
):
    """Refuse a pending request (admin only).

    No dashboard row is written — a refusal is not a decision about somebody
    who is not there. The count is what freezes, not the verdict: five
    refusals and the person is frozen out for thirty days.
    """
    from bot import database

    admin_id = user.get("user_id")
    written, frozen = database.deny_user(user_id, admin_id, expected_status="pending")
    if not written:
        raise HTTPException(
            status_code=409,
            detail="This request has already been decided by somebody else",
        )

    await _tell_the_person(
        user_id, "access.frozen" if frozen else "access.denied",
    )
    logger.info("Admin %s denied access request %s (frozen=%s)",
                admin_id, user_id, frozen)
    return {"success": True, "user_id": user_id, "frozen": frozen}


# ─── Permissions ───────────────────────────────────────────────────────────────

@router.get("/admin/permissions")
@limiter.limit("30/minute")
async def get_all_permissions(
    request: Request,
    user: dict = Depends(require_admin),
):
    """Get all permissions for all roles (admin only)."""
    from core.permissions import (
        get_all_features, get_all_permissions_async, get_all_presets, get_all_roles,
    )

    permissions = await get_all_permissions_async()
    features = get_all_features()
    roles = get_all_roles()

    # The tab checklist and its preset buttons are drawn from the same two
    # lists the bot draws its keyboard from, so a tab added in
    # `core/permissions.py` appears in both without either being edited.
    from core.permissions import DEFAULT_TABS

    return {
        "permissions": permissions,
        "features": features,
        "roles": roles,
        "tabs": [f["key"] for f in features if f.get("tab")],
        "presets": get_all_presets(),
        # What "no tab set" means, per level. **The page cannot derive this
        # any more and must not try.** It used to read the matrix and take
        # every feature with `view` — which was right while the matrix carried
        # areas, and became "all nine tabs for everybody" the moment it became
        # uniform depth. An admin opening an inheriting row then saw nine lit
        # chips instead of six, and one click would have written an explicit
        # eight-tab set: margin, expenses and the SMS roster granted by
        # touching an unrelated tab.
        #
        # `null` for a level means not narrowed at all — the admin.
        "default_tabs": {
            role: (list(tabs) if tabs is not None else None)
            for role, tabs in DEFAULT_TABS.items()
        },
    }


@router.patch("/admin/permissions")
@limiter.limit("10/minute")
async def update_permission(
    request: Request,
    role: str = Query(...),
    feature: str = Query(...),
    can_view: Optional[bool] = Query(None),
    can_edit: Optional[bool] = Query(None),
    can_delete: Optional[bool] = Query(None),
    user: dict = Depends(require_admin),
):
    """Update a permission (admin only).

    Column-level: only the flags given are written. The page used to send all
    three from its cached copy of the matrix, so a click made from a snapshot
    up to a minute old — or before the previous click's refetch had landed —
    put the other two columns back to what the snapshot held, undoing another
    admin's grant or the caller's own previous toggle.
    """
    from core.permissions import set_permission_async, Role, Feature

    valid_roles = [r.value for r in Role]
    if role not in valid_roles:
        raise HTTPException(status_code=400, detail=f"Invalid role. Must be one of: {valid_roles}")

    valid_features = [f.value for f in Feature]
    if feature not in valid_features:
        raise HTTPException(status_code=400, detail=f"Invalid feature. Must be one of: {valid_features}")

    if can_view is None and can_edit is None and can_delete is None:
        raise HTTPException(status_code=400, detail="Nothing to change: pass at least one of can_view, can_edit, can_delete")

    admin_id = user.get("user_id")
    success = await set_permission_async(role, feature, can_view, can_edit, can_delete, admin_id)

    if not success:
        raise HTTPException(status_code=500, detail="Failed to update permission")

    logger.info(
        f"Admin {admin_id} updated permission: {role}/{feature} -> "
        f"view={can_view}, edit={can_edit}, delete={can_delete}"
    )
    return {
        "success": True,
        "role": role,
        "feature": feature,
        "can_view": can_view,
        "can_edit": can_edit,
        "can_delete": can_delete,
    }
