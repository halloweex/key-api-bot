"""Admin user management and permissions endpoints."""
import logging

from fastapi import APIRouter, Query, Request, HTTPException, Depends
from typing import Optional

from web.routes.auth import require_admin
from ._deps import limiter, get_store

router = APIRouter()
logger = logging.getLogger(__name__)


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
    role: str = Query(..., description="New role: admin, editor, viewer"),
    user: dict = Depends(require_admin),
):
    """Update user role (admin only)."""
    if role not in ("admin", "editor", "viewer"):
        raise HTTPException(status_code=400, detail="Invalid role")

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

    store = await get_store()
    admin_id = user.get("user_id")
    success = await store.update_user_status(user_id, status, reviewed_by=admin_id)
    if not success:
        raise HTTPException(status_code=404, detail="User not found")

    logger.info(f"Admin {admin_id} changed user {user_id} status to {status}")
    return {"success": True, "user_id": user_id, "status": status}


# ─── Permissions ───────────────────────────────────────────────────────────────

@router.get("/admin/permissions")
@limiter.limit("30/minute")
async def get_all_permissions(
    request: Request,
    user: dict = Depends(require_admin),
):
    """Get all permissions for all roles (admin only)."""
    from core.permissions import get_all_permissions_async, get_all_features, get_all_roles

    permissions = await get_all_permissions_async()
    features = get_all_features()
    roles = get_all_roles()

    return {"permissions": permissions, "features": features, "roles": roles}


@router.patch("/admin/permissions")
@limiter.limit("10/minute")
async def update_permission(
    request: Request,
    role: str = Query(...),
    feature: str = Query(...),
    can_view: bool = Query(...),
    can_edit: bool = Query(...),
    can_delete: bool = Query(...),
    user: dict = Depends(require_admin),
):
    """Update a permission (admin only)."""
    from core.permissions import set_permission_async, Role, Feature

    valid_roles = [r.value for r in Role]
    if role not in valid_roles:
        raise HTTPException(status_code=400, detail=f"Invalid role. Must be one of: {valid_roles}")

    valid_features = [f.value for f in Feature]
    if feature not in valid_features:
        raise HTTPException(status_code=400, detail=f"Invalid feature. Must be one of: {valid_features}")

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
