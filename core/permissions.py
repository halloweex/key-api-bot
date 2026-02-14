"""
Role-based permissions system.

Supports both hardcoded defaults and dynamic DB-stored permissions.
DB permissions take precedence when available.
"""
import logging
from enum import Enum
from typing import Dict, Set, Optional

logger = logging.getLogger(__name__)


class Role(str, Enum):
    """User roles."""
    ADMIN = "admin"
    EDITOR = "editor"
    VIEWER = "viewer"


class Feature(str, Enum):
    """Protected features."""
    DASHBOARD = "dashboard"
    EXPENSES = "expenses"
    INVENTORY = "inventory"
    ANALYTICS = "analytics"
    CUSTOMERS = "customers"
    REPORTS = "reports"
    USER_MANAGEMENT = "user_management"


class Action(str, Enum):
    """Permission actions."""
    VIEW = "view"
    EDIT = "edit"
    DELETE = "delete"


# ═══════════════════════════════════════════════════════════════════════════════
# ROLE PERMISSIONS MATRIX
# ═══════════════════════════════════════════════════════════════════════════════

ROLE_PERMISSIONS: Dict[str, Dict[str, Set[str]]] = {
    Role.ADMIN: {
        Feature.DASHBOARD: {Action.VIEW},
        Feature.EXPENSES: {Action.VIEW, Action.EDIT, Action.DELETE},
        Feature.INVENTORY: {Action.VIEW, Action.EDIT, Action.DELETE},
        Feature.ANALYTICS: {Action.VIEW},
        Feature.CUSTOMERS: {Action.VIEW},
        Feature.REPORTS: {Action.VIEW, Action.EDIT},
        Feature.USER_MANAGEMENT: {Action.VIEW, Action.EDIT, Action.DELETE},
    },
    Role.EDITOR: {
        Feature.DASHBOARD: {Action.VIEW},
        Feature.EXPENSES: {Action.VIEW, Action.EDIT},
        Feature.INVENTORY: {Action.VIEW, Action.EDIT},
        Feature.ANALYTICS: {Action.VIEW},
        Feature.CUSTOMERS: {Action.VIEW},
        Feature.REPORTS: {Action.VIEW},
    },
    Role.VIEWER: {
        Feature.DASHBOARD: {Action.VIEW},
        Feature.INVENTORY: {Action.VIEW},
        Feature.ANALYTICS: {Action.VIEW},
        Feature.CUSTOMERS: {Action.VIEW},
        Feature.REPORTS: {Action.VIEW},
    },
}


# ═══════════════════════════════════════════════════════════════════════════════
# PERMISSION HELPERS
# ═══════════════════════════════════════════════════════════════════════════════

def can(role: str, feature: str, action: str = "view") -> bool:
    """
    Check if a role has permission to perform an action on a feature.

    Args:
        role: User role (admin, editor, viewer)
        feature: Feature key (dashboard, expenses, etc.)
        action: Action (view, edit, delete)

    Returns:
        True if permitted, False otherwise
    """
    role_perms = ROLE_PERMISSIONS.get(role, {})
    feature_perms = role_perms.get(feature, set())
    return action in feature_perms


def get_permissions_for_role(role: str) -> Dict[str, Dict[str, bool]]:
    """
    Get all permissions for a role in frontend-friendly format.

    Args:
        role: User role

    Returns:
        Dict of feature -> {view: bool, edit: bool, delete: bool}
    """
    role_perms = ROLE_PERMISSIONS.get(role, {})

    result = {}
    for feature in Feature:
        feature_actions = role_perms.get(feature, set())
        result[feature.value] = {
            "view": Action.VIEW in feature_actions,
            "edit": Action.EDIT in feature_actions,
            "delete": Action.DELETE in feature_actions,
        }

    return result


def get_all_features() -> list:
    """Get list of all features with metadata."""
    return [
        {"key": Feature.DASHBOARD.value, "name": "Dashboard", "description": "Main dashboard view"},
        {"key": Feature.EXPENSES.value, "name": "Manual Expenses", "description": "View and manage expenses"},
        {"key": Feature.INVENTORY.value, "name": "Inventory", "description": "Stock management"},
        {"key": Feature.ANALYTICS.value, "name": "Analytics", "description": "Advanced analytics"},
        {"key": Feature.CUSTOMERS.value, "name": "Customer Insights", "description": "Customer data"},
        {"key": Feature.REPORTS.value, "name": "Reports", "description": "Export reports"},
        {"key": Feature.USER_MANAGEMENT.value, "name": "User Management", "description": "Manage users"},
    ]


def get_all_roles() -> list:
    """Get list of all roles with metadata."""
    return [
        {"key": Role.ADMIN.value, "name": "Admin", "description": "Full access to all features"},
        {"key": Role.EDITOR.value, "name": "Editor", "description": "Can view and edit most features"},
        {"key": Role.VIEWER.value, "name": "Viewer", "description": "View-only access"},
    ]


# ═══════════════════════════════════════════════════════════════════════════════
# ASYNC PERMISSIONS (Database-backed)
# ═══════════════════════════════════════════════════════════════════════════════

# Cache for DB permissions (refreshed on updates)
_permissions_cache: Optional[Dict[str, Dict[str, Dict[str, bool]]]] = None


async def get_permissions_for_role_async(role: str) -> Dict[str, Dict[str, bool]]:
    """
    Get permissions for a role from database.

    Falls back to hardcoded permissions if DB unavailable.
    """
    global _permissions_cache

    try:
        from core.duckdb_store import get_store
        store = await get_store()

        # Try cache first
        if _permissions_cache is not None and role in _permissions_cache:
            return _permissions_cache[role]

        # Ensure defaults are seeded
        await store.seed_default_permissions()

        # Load from DB
        db_perms = await store.get_role_permissions(role)

        if db_perms:
            # Fill in any missing features with defaults
            result = {}
            for feature in Feature:
                if feature.value in db_perms:
                    result[feature.value] = db_perms[feature.value]
                else:
                    result[feature.value] = {"view": False, "edit": False, "delete": False}
            return result

    except Exception as e:
        logger.warning(f"Failed to load permissions from DB: {e}, using hardcoded")

    # Fallback to hardcoded
    return get_permissions_for_role(role)


async def get_all_permissions_async() -> Dict[str, Dict[str, Dict[str, bool]]]:
    """Get all permissions for all roles from database."""
    global _permissions_cache

    try:
        from core.duckdb_store import get_store
        store = await get_store()

        # Ensure defaults are seeded
        await store.seed_default_permissions()

        # Load all from DB
        _permissions_cache = await store.get_all_permissions()
        return _permissions_cache

    except Exception as e:
        logger.warning(f"Failed to load all permissions from DB: {e}")
        return {}


async def set_permission_async(
    role: str,
    feature: str,
    can_view: bool,
    can_edit: bool,
    can_delete: bool,
    updated_by: int
) -> bool:
    """Set a permission in the database."""
    global _permissions_cache

    try:
        from core.duckdb_store import get_store
        store = await get_store()
        result = await store.set_permission(role, feature, can_view, can_edit, can_delete, updated_by)

        # Invalidate cache
        _permissions_cache = None

        return result
    except Exception as e:
        logger.error(f"Failed to set permission: {e}")
        return False


def invalidate_permissions_cache():
    """Invalidate the permissions cache."""
    global _permissions_cache
    _permissions_cache = None


# ═══════════════════════════════════════════════════════════════════════════════
# ADMIN FALLBACK
# ═══════════════════════════════════════════════════════════════════════════════

# Hardcoded admin user IDs as fallback (if permissions system fails)
ADMIN_USER_IDS = {183618567, 129462784}


def is_hardcoded_admin(user_id: int) -> bool:
    """Check if user is a hardcoded admin (fallback)."""
    return user_id in ADMIN_USER_IDS
