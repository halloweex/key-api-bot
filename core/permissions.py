"""
Role-based permissions system.

Supports both hardcoded defaults and dynamic DB-stored permissions.
DB permissions take precedence when available.
"""
import logging
from enum import Enum
from typing import Dict, Iterable, List, Optional, Sequence, Set

logger = logging.getLogger(__name__)


class Role(str, Enum):
    """How deep an account may go — **not** what it may see.

    These are levels, and the two questions are independent: *how deep* is the
    role, *where* is the tab set (`dashboard_users.allowed_features`). One
    person is "viewer over the marketing tabs", another is "editor over the
    same tabs"; the pair expresses both without inventing a role for each area.

    `marketer` used to be a member here and was the mistake this enum now
    avoids: it was an *area* wearing a level's clothes — a viewer's depth plus
    `sms` edit — so "a marketer who may only look" could not be said at all.
    It is a tab **preset** now (`ACCESS_PRESETS["marketer"]`), and revision
    0023 moved the one account carrying it to `editor` with that set.

    A stored `marketer` therefore no longer resolves to anything here. It is
    left out of the enum deliberately rather than kept as a quiet alias:
    `update_user_role` validates against these members, so a row that somehow
    still said `marketer` would be refused loudly at the next edit instead of
    resolving to a level nobody chose.
    """
    ADMIN = "admin"
    EDITOR = "editor"
    VIEWER = "viewer"


class Feature(str, Enum):
    """Protected features.

    Most of these are **tabs** — one page in the sidebar, one entry in
    ``TAB_FEATURES``, one gate on the endpoints behind it. Until 2026-09-07
    only ``expenses`` and ``sms`` were actually enforced on the server and the
    other pages were reachable by any approved user; ``traffic``, ``products``,
    ``marketing`` and ``margin`` did not exist as features at all, which is why
    "open only the traffic tab to this person" could not be expressed.

    Three members are **not** tabs and are marked so in ``TAB_FEATURES``'s
    absence: ``analytics`` and ``customers`` gate nothing today and are kept
    because turning off a stored permission row is a decision somebody made,
    and ``user_management`` describes the admin pages, which are gated on the
    admin *role* — a checkbox must never be the way somebody gets the keys to
    the access system itself.
    """
    DASHBOARD = "dashboard"
    EXPENSES = "expenses"
    INVENTORY = "inventory"
    ANALYTICS = "analytics"
    CUSTOMERS = "customers"
    REPORTS = "reports"
    USER_MANAGEMENT = "user_management"
    # The /sms page and every /api/customers/sms-* endpoint behind it. `view`
    # is the roster sizes and past results; `edit` is what leaves the building
    # — the CSV of names and phone numbers, and the send itself.
    SMS = "sms"
    # The four pages that had no feature of their own. Their role defaults
    # below reproduce exactly what each role could reach the day before this
    # existed: everybody saw traffic, products and marketing, and /margin was
    # behind the admin role.
    TRAFFIC = "traffic"
    PRODUCTS = "products"
    MARKETING = "marketing"
    MARGIN = "margin"


class Action(str, Enum):
    """Permission actions."""
    VIEW = "view"
    EDIT = "edit"
    DELETE = "delete"


# ═══════════════════════════════════════════════════════════════════════════════
# ROLE PERMISSIONS MATRIX
# ═══════════════════════════════════════════════════════════════════════════════

ROLE_PERMISSIONS: Dict[str, Dict[str, Set[str]]] = {
    # **Depth, not area.** Each level grants the same actions everywhere,
    # because *where* is the tab set's question and asking it twice is what
    # produced `marketer` — an area that had to be a role because the matrix
    # was the only place areas could be expressed.
    #
    # So a viewer looks at whatever tabs they hold, an editor changes things in
    # whatever tabs they hold, and an admin also manages access. "Viewer over
    # the marketing tabs" and "editor over the marketing tabs" are now one
    # sentence each instead of two roles.
    #
    # `user_management` is the one feature that is not a tab and stays with the
    # level: it is the access system itself, and a checkbox must never be the
    # way somebody reaches it.
    Role.ADMIN: {
        **{feature: {Action.VIEW, Action.EDIT, Action.DELETE} for feature in Feature},
    },
    Role.EDITOR: {
        **{feature: {Action.VIEW, Action.EDIT} for feature in Feature},
        Feature.USER_MANAGEMENT: set(),
    },
    Role.VIEWER: {
        **{feature: {Action.VIEW} for feature in Feature},
        Feature.USER_MANAGEMENT: set(),
    },
}


# What "no tab set" means, per level.
#
# **This is load-bearing and it is new.** While the matrix carried areas, an
# account with no override saw whatever its role happened to grant, and a
# viewer's row simply did not grant `margin`, `expenses` or `sms`. With depth
# uniform, "no override" would hand every one of the sixteen viewers the margin
# tab, the expenses block and the SMS roster the moment this shipped. So the
# default is written down instead of falling out of the matrix.
#
# `standard` is what a viewer could reach the day before per-user tabs existed,
# which keeps that promise exactly. An editor adds `expenses`, which is what
# the editor row granted before. An admin holds everything.
DEFAULT_TABS: Dict[str, tuple] = {
    Role.ADMIN.value: None,     # every tab; None means "no narrowing at all"
    Role.EDITOR.value: (
        Feature.DASHBOARD.value, Feature.PRODUCTS.value, Feature.TRAFFIC.value,
        Feature.INVENTORY.value, Feature.REPORTS.value, Feature.MARKETING.value,
        Feature.EXPENSES.value,
    ),
    Role.VIEWER.value: (
        Feature.DASHBOARD.value, Feature.PRODUCTS.value, Feature.TRAFFIC.value,
        Feature.INVENTORY.value, Feature.REPORTS.value, Feature.MARKETING.value,
    ),
}


def default_tabs(role: str):
    """The tab set an account of this level holds when nobody set one.

    `None` means "not narrowed" — the admin case, and the answer for a level
    this code does not recognise, which must not silently become "sees
    nothing".
    """
    return DEFAULT_TABS.get(role, None)

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


# ═══════════════════════════════════════════════════════════════════════════════
# TABS, AND THE SET ONE PERSON MAY SEE
# ═══════════════════════════════════════════════════════════════════════════════

# The features that are a page in the sidebar, in the order they appear there.
# Everything about per-user access — the checklist in the admin page, the
# keyboard the bot draws on approval, the override applied to a session — reads
# this tuple, so a tab added to one of those three is added to all of them by
# adding it here.
#
# `user_management` is deliberately absent: the admin pages are gated on the
# admin *role*, and a checkbox that hands somebody the access system itself
# would make every other rule here advisory. `analytics` and `customers` are
# absent because they gate nothing — they are stored matrix rows, not pages.
TAB_FEATURES: tuple = (
    Feature.DASHBOARD,
    Feature.PRODUCTS,
    Feature.TRAFFIC,
    Feature.INVENTORY,
    Feature.REPORTS,
    Feature.MARKETING,
    Feature.MARGIN,
    Feature.EXPENSES,
    Feature.SMS,
)

TAB_FEATURE_KEYS: tuple = tuple(f.value for f in TAB_FEATURES)

# Ready-made bundles the admin picks with one tap — in the bot on approval, and
# as buttons above the checklist in the admin page. They live **in code**, like
# `BUILTIN_AUDIENCE_PRESETS`, and for the same reason: a preset stored in a
# table can be edited into something that no longer means what it meant when
# somebody was granted it, and the bot would then need a second store to read
# before it could draw a keyboard.
#
# `standard` is what a viewer could reach the day before per-user access
# existed, and it is what an approval grants when the admin taps nothing. That
# is the whole reason it is spelled out rather than derived: the default has to
# be a decision that survives somebody changing the viewer role.
ACCESS_PRESETS: Dict[str, tuple] = {
    "full": TAB_FEATURE_KEYS,
    "standard": (
        Feature.DASHBOARD.value,
        Feature.PRODUCTS.value,
        Feature.TRAFFIC.value,
        Feature.INVENTORY.value,
        Feature.REPORTS.value,
        Feature.MARKETING.value,
    ),
    "traffic_only": (Feature.TRAFFIC.value,),
    # What the `marketer` *role* used to grant, now expressible as an area:
    # everything a viewer saw, plus the SMS tab. Paired with `viewer` it is
    # somebody who reads campaign results; paired with `editor` it is somebody
    # who sends them. That pair is what the role could not say.
    "marketer": (
        Feature.DASHBOARD.value, Feature.PRODUCTS.value, Feature.TRAFFIC.value,
        Feature.INVENTORY.value, Feature.REPORTS.value, Feature.MARKETING.value,
        Feature.SMS.value,
    ),
    "marketing": (
        Feature.MARKETING.value,
        Feature.TRAFFIC.value,
        Feature.REPORTS.value,
    ),
}

DEFAULT_PRESET = "standard"


def preset_features(name: str) -> Optional[List[str]]:
    """The tab set a preset names, or None if there is no such preset."""
    values = ACCESS_PRESETS.get(name)
    return list(values) if values is not None else None


def normalize_features(values: Optional[Iterable[str]]) -> Optional[List[str]]:
    """Keep the known tab keys, in `TAB_FEATURES` order, without duplicates.

    ``None`` means "this person has no override" and is passed through
    untouched — it is the difference between *inherit the role* and *see
    nothing*, and an empty list is a real state an admin can create.

    Unknown keys are dropped rather than refused: the caller is a checklist
    drawn from `TAB_FEATURE_KEYS`, and a key that is no longer a tab is a tab
    that was removed, not an attack. What must never happen is a stored value
    outside this set, because the override is read on every request and
    compared by key.
    """
    if values is None:
        return None
    wanted = {str(v).strip().lower() for v in values}
    return [key for key in TAB_FEATURE_KEYS if key in wanted]


def parse_features(raw) -> Optional[List[str]]:
    """Read the stored column: a comma-separated list, or NULL for "inherit"."""
    if raw is None:
        return None
    if isinstance(raw, (list, tuple)):
        return normalize_features(raw)
    text = str(raw).strip()
    if not text:
        # An empty string is a stored, deliberate "no tabs at all" — see
        # `serialize_features`, which writes one rather than a NULL.
        return []
    return normalize_features(text.split(","))


def serialize_features(values: Optional[Iterable[str]]) -> Optional[str]:
    """The column value for a tab set. ``None`` stays NULL — "as the role"."""
    features = normalize_features(values)
    if features is None:
        return None
    return ",".join(features)


def apply_feature_override(
    permissions: Dict[str, Dict[str, bool]],
    allowed: Optional[Iterable[str]],
    role: Optional[str] = None,
) -> Dict[str, Dict[str, bool]]:
    """Narrow (or widen) a role's permissions to one person's tab set.

    The rule, in one sentence: **the checklist decides which tabs are visible,
    the role decides what can be done inside them.**

    So a tab that is ticked becomes viewable even if the role would not show it
    — that is how "give this person traffic only" works without inventing a
    role — while `edit` and `delete` still come from the role, and are dropped
    along with a tab that is not ticked. The consequence worth stating out
    loud: ticking `sms` for a viewer grants the roster *sizes*, never the CSV
    of names and phone numbers nor the send, because those are `sms` **edit**
    and no override grants an action.

    Features that are not tabs are returned untouched.

    ``allowed=None`` means nobody has set tabs for this person, and the answer
    is then the **level's default set** (`DEFAULT_TABS`) rather than the whole
    matrix. That indirection is what keeps the two questions apart: since the
    matrix became uniform depth, "no override" without a default would hand
    every viewer the margin tab, the expenses block and the SMS roster. A level
    whose default is `None` — the admin — is not narrowed at all.
    """
    if allowed is None:
        allowed = default_tabs(role) if role else None
        if allowed is None:
            return permissions

    granted = set(normalize_features(allowed) or ())
    result = {}
    for feature, actions in permissions.items():
        if feature not in TAB_FEATURE_KEYS:
            result[feature] = dict(actions)
            continue
        visible = feature in granted
        result[feature] = {
            "view": visible,
            "edit": visible and bool(actions.get("edit")),
            "delete": visible and bool(actions.get("delete")),
        }
    return result


def get_all_features() -> list:
    """Get list of all features with metadata.

    ``tab`` says whether the feature is a page somebody can be given or denied
    on its own — the admin checklist and the bot's keyboard are built from the
    entries that carry it.
    """
    return [
        {"key": Feature.DASHBOARD.value, "name": "Dashboard", "description": "Main dashboard view", "tab": True, "path": "/"},
        {"key": Feature.PRODUCTS.value, "name": "Product Intelligence", "description": "Baskets, pairs, momentum", "tab": True, "path": "/products"},
        {"key": Feature.TRAFFIC.value, "name": "Traffic", "description": "UTM traffic, campaigns and ROAS", "tab": True, "path": "/traffic"},
        {"key": Feature.INVENTORY.value, "name": "Inventory", "description": "Stock management", "tab": True, "path": "/inventory"},
        {"key": Feature.REPORTS.value, "name": "Reports", "description": "Export reports", "tab": True, "path": "/reports"},
        {"key": Feature.MARKETING.value, "name": "Marketing", "description": "Monthly report, promocodes, ROI", "tab": True, "path": "/marketing"},
        {"key": Feature.MARGIN.value, "name": "Margin", "description": "Cost price and profit", "tab": True, "path": "/margin"},
        {"key": Feature.EXPENSES.value, "name": "Manual Expenses", "description": "View and manage expenses", "tab": True, "path": None},
        {"key": Feature.SMS.value, "name": "SMS Campaigns", "description": "Segment, export and send SMS campaigns", "tab": True, "path": "/sms"},
        {"key": Feature.ANALYTICS.value, "name": "Analytics", "description": "Advanced analytics", "tab": False, "path": None},
        {"key": Feature.CUSTOMERS.value, "name": "Customer Insights", "description": "Customer data", "tab": False, "path": None},
        {"key": Feature.USER_MANAGEMENT.value, "name": "User Management", "description": "Manage users", "tab": False, "path": "/admin/users"},
    ]


# ═══════════════════════════════════════════════════════════════════════════════
# WHAT THE BOT HANDS OUT, AND WHICH TAB IT BELONGS TO
# ═══════════════════════════════════════════════════════════════════════════════

# The bot is a second door to the same numbers: a summary report *is* the
# dashboard's revenue in a Telegram message. So each thing it produces names
# the tab that thing lives on, and somebody who does not hold that tab does not
# receive it — whichever door they came through.
#
# **One table, not an argument per handler.** The mapping is the decision; if
# it were spelled at fifteen call sites, "marketing sees marketing" and
# "traffic sees traffic" would drift apart the first time somebody added a
# report. A test walks the handlers and fails on one that produces data
# without an entry here.
#
# `search` is the entry that is not obvious and is deliberately conservative:
# the web's `/api/search*` is admin-only, while the bot has offered search to
# every approved person since long before tabs existed. Gating it on the
# dashboard tab keeps that promise for everybody who is not narrowed rather
# than silently taking it away; whether the two doors should agree on *admin*
# is a separate decision and is not taken here.
BOT_SURFACES: Dict[str, str] = {
    # /report → the sales summary: revenue, orders, the split by source.
    "summary": Feature.DASHBOARD.value,
    # /report → TOP-10 products, which is the dashboard's own panel.
    "top10": Feature.DASHBOARD.value,
    # /report → the Excel export.
    "excel": Feature.REPORTS.value,
    # /search over orders, products and buyers.
    "search": Feature.DASHBOARD.value,
    # The weekly push: last week's revenue against the week before.
    "weekly_report": Feature.DASHBOARD.value,
}


def surface_feature(surface: str) -> Optional[str]:
    """Which tab a bot surface belongs to, or None if it is not data."""
    return BOT_SURFACES.get(surface)


def get_all_presets() -> list:
    """The code-defined tab bundles, for the admin page and the bot."""
    return [
        {"key": name, "features": list(features)}
        for name, features in ACCESS_PRESETS.items()
    ]


def get_all_roles() -> list:
    """Get list of all roles with metadata."""
    return [
        {"key": Role.ADMIN.value, "name": "Admin", "description": "Everything, including access itself"},
        {"key": Role.EDITOR.value, "name": "Editor", "description": "May change things in the tabs they hold"},
        {"key": Role.VIEWER.value, "name": "Viewer", "description": "May look at the tabs they hold"},
    ]


# ═══════════════════════════════════════════════════════════════════════════════
# ASYNC PERMISSIONS (Database-backed)
# ═══════════════════════════════════════════════════════════════════════════════

# Cache for DB permissions (refreshed on updates)
_permissions_cache: Optional[Dict[str, Dict[str, Dict[str, bool]]]] = None


def _remember(role: str, permissions: Dict[str, Dict[str, bool]]) -> None:
    """Keep one role's answer until somebody changes the matrix."""
    global _permissions_cache
    if _permissions_cache is None:
        _permissions_cache = {}
    _permissions_cache[role] = permissions


async def get_permissions_for_role_async(role: str) -> Dict[str, Dict[str, bool]]:
    """
    Get permissions for a role from database.

    Falls back to hardcoded permissions if DB unavailable.
    """
    global _permissions_cache

    # The cache is checked before the store is even fetched. It used to be
    # checked after, which cost nothing — until per-user tabs put a permission
    # dependency on roughly 120 of the 140 endpoints instead of the ~20 that
    # were gated before, and every miss here is two acquisitions of DuckDB's
    # process-wide store lock: one to seed, one to read.
    if _permissions_cache is not None and role in _permissions_cache:
        return _permissions_cache[role]

    try:
        from core.duckdb_store import get_store
        store = await get_store()

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

            _remember(role, result)
            return result

        # An empty table after a seed is an **answer**, not a failure: nothing
        # is stored for this role and the hardcoded matrix is therefore the
        # matrix. Cached for that reason — without it every single request
        # re-ran the seed and the read, taking the store lock twice, forever.
        #
        # It is also an anomaly worth a line in the log, because the seed
        # immediately above should have written those rows: a table that is
        # still empty means the write went nowhere.
        logger.warning(
            "role_permissions is empty for %r after seeding — serving the "
            "hardcoded matrix. The seed wrote nothing; check the store.", role,
        )
        result = get_permissions_for_role(role)
        _remember(role, result)
        return result

    except Exception as e:
        logger.warning(f"Failed to load permissions from DB: {e}, using hardcoded")

    # Deliberately **not** cached: an error is not an answer, and the next
    # request may get one. The cost of retrying is bounded by the store being
    # broken, which is a louder problem than this one.
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
    can_view: Optional[bool],
    can_edit: Optional[bool],
    can_delete: Optional[bool],
    updated_by: int
) -> bool:
    """Set a permission in the database. A flag left None is not touched."""
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
