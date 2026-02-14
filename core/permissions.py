"""
Role-based permissions system.

Simple hardcoded permissions per role. Easy to modify in code,
no database needed. Add dynamic permissions later if needed.
"""
from enum import Enum
from typing import Dict, Set


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
        {"key": Feature.DASHBOARD, "name": "Dashboard", "description": "Main dashboard view"},
        {"key": Feature.EXPENSES, "name": "Manual Expenses", "description": "View and manage expenses"},
        {"key": Feature.INVENTORY, "name": "Inventory", "description": "Stock management"},
        {"key": Feature.ANALYTICS, "name": "Analytics", "description": "Advanced analytics"},
        {"key": Feature.CUSTOMERS, "name": "Customer Insights", "description": "Customer data"},
        {"key": Feature.REPORTS, "name": "Reports", "description": "Export reports"},
        {"key": Feature.USER_MANAGEMENT, "name": "User Management", "description": "Manage users"},
    ]


# ═══════════════════════════════════════════════════════════════════════════════
# ADMIN FALLBACK
# ═══════════════════════════════════════════════════════════════════════════════

# Hardcoded admin user IDs as fallback (if permissions system fails)
ADMIN_USER_IDS = {183618567, 129462784}


def is_hardcoded_admin(user_id: int) -> bool:
    """Check if user is a hardcoded admin (fallback)."""
    return user_id in ADMIN_USER_IDS
