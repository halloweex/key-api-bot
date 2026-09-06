"""The bot's state, as forty-six call sites already know how to ask for it.

This was 717 lines of `sqlite3`. The SQL now lives in `bot/store_sqlite.py`
behind the port in `core/bot_store.py`, and what is left here is the shape the
callers use: module-level functions, synchronous, same names, same signatures.

WHY A FACADE RATHER THAN MOVING THE CALLERS

Because the goal is a seam, and the seam is already here once these functions
delegate. Rewriting 46 call sites to say `get_bot_store().access.approve(...)`
would change every one of them to buy nothing the engine switch needs — and
`bot/handlers_legacy.py` holds 38 of them, in code with no tests of its own.

The layering cost is real and worth naming: callers see this module, not the
port, so nothing stops a future one from importing `bot.store_sqlite` directly
and going round the seam. There is no import linter here to forbid it. The
test that watches for it is `tests/unit/test_bot_store.py`.

WHAT IS HERE AND NOT IN THE PORT

Two things, both because they are rules rather than storage:

* `get_user_language` reads a preference and then applies a decision about this
  company — Ukrainian for everyone, English for admins, a stored choice wins
  for good. An adapter should not have to know who the admins are.
* `generate_cache_key` touches nothing at all.

And one thing that is here because it is a surprise: `revoke_user` is
`deny_user`. Revoking somebody's access increments their denial count, and five
revocations freeze them out for thirty days. It is deliberately not a port
method — see `core/bot_store.py` — so that changing that decision adds a method
rather than quietly redefining one.
"""
from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

from core.bot_store import get_bot_store

logger = logging.getLogger(__name__)

# Re-exported: `bot/handlers_legacy.py` reads three of these to build its admin
# screens, and `MAX_DENIAL_COUNT` reaches the message a denied person is shown.
from bot.store_sqlite import (  # noqa: E402  (after the port import, on purpose)
    FREEZE_DURATION_DAYS,
    MAX_DENIAL_COUNT,
    STATUS_APPROVED,
    STATUS_DENIED,
    STATUS_FROZEN,
    STATUS_PENDING,
)

__all__ = [
    "STATUS_PENDING", "STATUS_APPROVED", "STATUS_DENIED", "STATUS_FROZEN",
    "MAX_DENIAL_COUNT", "FREEZE_DURATION_DAYS",
    "init_database", "get_database_stats",
    "get_user_auth_status", "is_user_authorized", "has_pending_request",
    "is_user_frozen", "unfreeze_user", "request_access", "approve_user",
    "deny_user", "revoke_user", "reset_user_to_pending", "update_last_activity",
    "revoke_inactive_users", "get_pending_requests", "get_all_authorized_users",
    "get_frozen_users",
    "get_user_preferences", "get_user_language", "save_user_preferences",
    "update_user_preference",
    "cache_get", "cache_set", "cache_delete", "cache_cleanup",
    "generate_cache_key",
    "is_milestone_celebrated", "mark_milestone_celebrated",
    "get_celebrated_milestones",
]


def init_database() -> None:
    """Initialize database tables."""
    get_bot_store().initialise()


def get_database_stats() -> Dict[str, int]:
    """Get database statistics."""
    return get_bot_store().stats()


# ═══════════════════════════════════════════════════════════════════════════
# USER AUTHORIZATION
# ═══════════════════════════════════════════════════════════════════════════


def get_user_auth_status(user_id: int) -> Optional[Dict[str, Any]]:
    """Get user authorization status."""
    return get_bot_store().access.status(user_id)


def is_user_authorized(user_id: int) -> bool:
    """Check if user is authorized (approved status)."""
    return get_bot_store().access.is_approved(user_id)


def has_pending_request(user_id: int) -> bool:
    """Check if user has a pending access request."""
    status = get_bot_store().access.status(user_id)
    return bool(status) and status["status"] == STATUS_PENDING


def is_user_frozen(user_id: int) -> bool:
    """Check if user is frozen. Auto-unfreezes after FREEZE_DURATION_DAYS."""
    return get_bot_store().access.is_frozen(user_id)


def unfreeze_user(user_id: int, admin_id: int) -> bool:
    """Admin unfreezes a user. Resets denial count. True if successful."""
    return get_bot_store().access.unfreeze(user_id, admin_id)


def request_access(
    user_id: int,
    username: str = None,
    first_name: str = None,
    last_name: str = None,
) -> bool:
    """Request access. True if a new request was created, False if one exists."""
    return get_bot_store().access.request(
        user_id, username, first_name, last_name,
    )


def approve_user(
    user_id: int, admin_id: int, *, expected_status: str | None = None,
) -> bool:
    """Approve user access. Resets denial count. True if successful.

    `expected_status="pending"` from the admin's Approve button: the verdict
    is for the request, not for whatever state a faster admin left behind.
    """
    return get_bot_store().access.approve(
        user_id, admin_id, expected_status=expected_status,
    )


def deny_user(
    user_id: int, admin_id: int, *, expected_status: str | None = None,
) -> tuple[bool, bool]:
    """Deny user access. Increments denial count. Returns (success, is_frozen)."""
    return get_bot_store().access.deny(
        user_id, admin_id, expected_status=expected_status,
    )


def revoke_user(user_id: int, admin_id: int) -> bool:
    """Revoke user access.

    A denial, and therefore counted as one: five revocations freeze somebody
    out for thirty days. Pinned by `tests/unit/test_bot_database.py`, which
    calls it surprising rather than correct — if that is ever changed, it
    becomes its own port method rather than a different meaning for `deny`.
    """
    success, _ = get_bot_store().access.deny(user_id, admin_id)
    return success


def reset_user_to_pending(user_id: int) -> tuple[bool, bool]:
    """Reset to pending for a re-request. Returns (success, was_frozen)."""
    return get_bot_store().access.reset_to_pending(user_id)


def update_last_activity(user_id: int) -> None:
    """Update user's last activity timestamp."""
    get_bot_store().access.touch(user_id)


def revoke_inactive_users(days: int = 45) -> int:
    """Revoke access for users inactive for X days. Returns count revoked."""
    return get_bot_store().access.sweep_inactive(days)


def get_pending_requests() -> List[Dict[str, Any]]:
    """Get all pending access requests."""
    return get_bot_store().access.pending()


def get_all_authorized_users() -> List[Dict[str, Any]]:
    """Get all approved users."""
    return get_bot_store().access.approved()


def get_frozen_users() -> List[Dict[str, Any]]:
    """Get all frozen users."""
    return get_bot_store().access.frozen()


# ═══════════════════════════════════════════════════════════════════════════
# USER PREFERENCES
# ═══════════════════════════════════════════════════════════════════════════


def get_user_preferences(user_id: int) -> Optional[Dict[str, Any]]:
    """Get user preferences."""
    return get_bot_store().preferences.get(user_id)


def get_user_language(user_id: int) -> str:
    """The language this user picked, or the default for who they are.

    Ukrainian for everyone, English for admins — until they choose otherwise,
    at which point the stored choice wins for good, here and in the weekly
    report alike. Telegram's own `language_code` is deliberately not consulted:
    the default is a decision about this company, not about a phone's locale.

    Stays out of the port because of that second paragraph: it is a rule about
    this company, and an adapter has no business knowing who the admins are.
    """
    from bot.config import ADMIN_USER_IDS
    from core.bot_prefs import default_language_for
    from core.i18n import normalize

    prefs = get_bot_store().preferences.get(user_id) or {}
    stored = prefs.get("language")
    if stored:
        return normalize(stored)
    return default_language_for(user_id, ADMIN_USER_IDS)


def save_user_preferences(
    user_id: int,
    default_source: str = None,
    default_report_type: str = "summary",
    timezone: str = "Europe/Kyiv",
    default_date_range: str = "week",
    notifications_enabled: bool = True,
) -> None:
    """Save or update user preferences."""
    get_bot_store().preferences.save(
        user_id,
        default_source=default_source,
        default_report_type=default_report_type,
        timezone=timezone,
        default_date_range=default_date_range,
        notifications_enabled=notifications_enabled,
    )


def update_user_preference(user_id: int, key: str, value: Any) -> None:
    """Update a single user preference."""
    get_bot_store().preferences.set(user_id, key, value)


# ═══════════════════════════════════════════════════════════════════════════
# CACHE
# ═══════════════════════════════════════════════════════════════════════════


def cache_get(key: str) -> Optional[Any]:
    """Get value from cache if not expired."""
    return get_bot_store().cache.get(key)


def cache_set(key: str, value: Any, ttl_minutes: int = 10) -> None:
    """Set value in cache with TTL."""
    get_bot_store().cache.set(key, value, ttl_minutes)


def cache_delete(key: str) -> None:
    """Delete a cache entry."""
    get_bot_store().cache.delete(key)


def cache_cleanup() -> int:
    """Remove all expired cache entries. Returns count deleted."""
    return get_bot_store().cache.sweep()


def generate_cache_key(prefix: str, **kwargs) -> str:
    """Generate a cache key from prefix and parameters.

    Pure, and therefore not a port method: it touches no store.
    """
    parts = [prefix]
    for k, v in sorted(kwargs.items()):
        parts.append(f"{k}={v}")
    return ":".join(parts)


# ═══════════════════════════════════════════════════════════════════════════
# MILESTONE TRACKING
# ═══════════════════════════════════════════════════════════════════════════


def is_milestone_celebrated(
    period_type: str, period_key: str, milestone_amount: int,
) -> bool:
    """Check if a milestone has already been celebrated for a given period."""
    return get_bot_store().milestones.already_celebrated(
        period_type, period_key, milestone_amount,
    )


def mark_milestone_celebrated(
    period_type: str, period_key: str, milestone_amount: int, revenue: float,
) -> bool:
    """Mark a milestone celebrated. True if newly inserted, False if it existed."""
    return get_bot_store().milestones.celebrate(
        period_type, period_key, milestone_amount, revenue,
    )


def get_celebrated_milestones(
    period_type: str = None, limit: int = 10,
) -> List[Dict[str, Any]]:
    """Get recent celebrated milestones."""
    return get_bot_store().milestones.recent(period_type, limit)
