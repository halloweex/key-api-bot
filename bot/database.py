"""
SQLite database module for persistent storage.

Handles:
- User preferences (default filters, settings)
- Report history (last reports per user)
- Cache (API responses with TTL)
"""
import sqlite3
import json
import logging
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, List
from pathlib import Path

logger = logging.getLogger(__name__)

# Database file path
DB_PATH = Path(__file__).parent.parent / "data" / "bot.db"


def get_connection() -> sqlite3.Connection:
    """Get database connection with row factory."""
    # Ensure data directory exists
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    return conn


def init_database():
    """Initialize database tables."""
    conn = get_connection()
    cursor = conn.cursor()

    # User preferences table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS user_preferences (
            user_id INTEGER PRIMARY KEY,
            default_source TEXT DEFAULT NULL,
            default_report_type TEXT DEFAULT 'summary',
            timezone TEXT DEFAULT 'Europe/Kyiv',
            default_date_range TEXT DEFAULT 'week',
            notifications_enabled INTEGER DEFAULT 1,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # Add new columns if they don't exist (migration for existing DBs)
    try:
        cursor.execute("ALTER TABLE user_preferences ADD COLUMN default_date_range TEXT DEFAULT 'week'")
    except sqlite3.OperationalError:
        pass  # Column already exists

    try:
        cursor.execute("ALTER TABLE user_preferences ADD COLUMN notifications_enabled INTEGER DEFAULT 1")
    except sqlite3.OperationalError:
        pass  # Column already exists

    # Report history table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS report_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            report_type TEXT NOT NULL,
            start_date TEXT NOT NULL,
            end_date TEXT NOT NULL,
            source TEXT DEFAULT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (user_id) REFERENCES user_preferences(user_id)
        )
    """)

    # Create index for faster lookups
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_report_history_user
        ON report_history(user_id, created_at DESC)
    """)

    # Cache table with TTL
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS cache (
            cache_key TEXT PRIMARY KEY,
            value TEXT NOT NULL,
            expires_at TIMESTAMP NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # Authorized users table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS authorized_users (
            user_id INTEGER PRIMARY KEY,
            username TEXT,
            first_name TEXT,
            last_name TEXT,
            status TEXT DEFAULT 'pending',
            requested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            reviewed_at TIMESTAMP,
            reviewed_by INTEGER,
            last_activity TIMESTAMP
        )
    """)

    # Add last_activity column if it doesn't exist (migration for existing DBs)
    try:
        cursor.execute("ALTER TABLE authorized_users ADD COLUMN last_activity TIMESTAMP")
    except sqlite3.OperationalError:
        pass  # Column already exists

    # Add denial_count column if it doesn't exist (migration for existing DBs)
    try:
        cursor.execute("ALTER TABLE authorized_users ADD COLUMN denial_count INTEGER DEFAULT 0")
    except sqlite3.OperationalError:
        pass  # Column already exists

    # Create index for status lookups
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_authorized_users_status
        ON authorized_users(status)
    """)

    # Celebrated milestones table (to avoid duplicate notifications)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS celebrated_milestones (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            period_type TEXT NOT NULL,
            period_key TEXT NOT NULL,
            milestone_amount INTEGER NOT NULL,
            revenue REAL NOT NULL,
            celebrated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(period_type, period_key, milestone_amount)
        )
    """)

    conn.commit()
    conn.close()
    logger.info(f"Database initialized at {DB_PATH}")


# ═══════════════════════════════════════════════════════════════════════════
# USER AUTHORIZATION
# ═══════════════════════════════════════════════════════════════════════════

# Status constants
STATUS_PENDING = 'pending'
STATUS_APPROVED = 'approved'
STATUS_DENIED = 'denied'
STATUS_FROZEN = 'frozen'

# Max denial count before freezing
MAX_DENIAL_COUNT = 5

# Freeze duration in days
FREEZE_DURATION_DAYS = 30


def get_user_auth_status(user_id: int) -> Optional[Dict[str, Any]]:
    """Get user authorization status."""
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute(
        "SELECT * FROM authorized_users WHERE user_id = ?",
        (user_id,)
    )
    row = cursor.fetchone()
    conn.close()

    if row:
        return dict(row)
    return None


def is_user_authorized(user_id: int) -> bool:
    """Check if user is authorized (approved status)."""
    status = get_user_auth_status(user_id)
    if not status:
        return False
    return status['status'] == STATUS_APPROVED


def has_pending_request(user_id: int) -> bool:
    """Check if user has a pending access request."""
    status = get_user_auth_status(user_id)
    if not status:
        return False
    return status['status'] == STATUS_PENDING


def is_user_frozen(user_id: int) -> bool:
    """
    Check if user is frozen (too many denials).
    Auto-unfreezes after FREEZE_DURATION_DAYS.
    """
    status = get_user_auth_status(user_id)
    if not status:
        return False

    if status['status'] != STATUS_FROZEN:
        return False

    # Check if freeze period has expired
    reviewed_at = status.get('reviewed_at')
    if reviewed_at:
        try:
            frozen_date = datetime.fromisoformat(reviewed_at)
            if datetime.now() - frozen_date > timedelta(days=FREEZE_DURATION_DAYS):
                # Auto-unfreeze: reset to denied so user can request again
                _auto_unfreeze_user(user_id)
                logger.info(f"User {user_id} auto-unfrozen after {FREEZE_DURATION_DAYS} days")
                return False
        except (ValueError, TypeError):
            pass

    return True


def _auto_unfreeze_user(user_id: int) -> None:
    """Internal: Reset frozen user to denied status."""
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("""
        UPDATE authorized_users
        SET status = ?, denial_count = 0
        WHERE user_id = ? AND status = ?
    """, (STATUS_DENIED, user_id, STATUS_FROZEN))

    conn.commit()
    conn.close()


def unfreeze_user(user_id: int, admin_id: int) -> bool:
    """Admin unfreezes a user. Resets denial count. Returns True if successful."""
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("""
        UPDATE authorized_users
        SET status = ?, denial_count = 0, reviewed_at = CURRENT_TIMESTAMP, reviewed_by = ?
        WHERE user_id = ? AND status = ?
    """, (STATUS_DENIED, admin_id, user_id, STATUS_FROZEN))

    success = cursor.rowcount > 0
    conn.commit()
    conn.close()

    if success:
        logger.info(f"User {user_id} unfrozen by admin {admin_id}")
    return success


def request_access(
    user_id: int,
    username: str = None,
    first_name: str = None,
    last_name: str = None
) -> bool:
    """
    Request access to the bot.
    Returns True if new request created, False if already exists.
    """
    conn = get_connection()
    cursor = conn.cursor()

    # Check if already exists
    cursor.execute(
        "SELECT status FROM authorized_users WHERE user_id = ?",
        (user_id,)
    )
    existing = cursor.fetchone()

    if existing:
        conn.close()
        return False  # Already has a record

    # Create new request
    cursor.execute("""
        INSERT INTO authorized_users (user_id, username, first_name, last_name, status)
        VALUES (?, ?, ?, ?, ?)
    """, (user_id, username, first_name, last_name, STATUS_PENDING))

    conn.commit()
    conn.close()
    logger.info(f"Access request created for user {user_id} (@{username})")
    return True


def approve_user(user_id: int, admin_id: int) -> bool:
    """Approve user access. Resets denial count. Returns True if successful."""
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("""
        UPDATE authorized_users
        SET status = ?, reviewed_at = CURRENT_TIMESTAMP, reviewed_by = ?, denial_count = 0
        WHERE user_id = ?
    """, (STATUS_APPROVED, admin_id, user_id))

    success = cursor.rowcount > 0
    conn.commit()
    conn.close()

    if success:
        logger.info(f"User {user_id} approved by admin {admin_id}")
    return success


def deny_user(user_id: int, admin_id: int) -> tuple[bool, bool]:
    """
    Deny user access. Increments denial count.
    Returns (success, is_frozen) tuple.
    """
    conn = get_connection()
    cursor = conn.cursor()

    # Get current denial count
    cursor.execute("SELECT denial_count FROM authorized_users WHERE user_id = ?", (user_id,))
    row = cursor.fetchone()
    current_count = (row[0] or 0) if row else 0
    new_count = current_count + 1

    # Determine new status
    if new_count >= MAX_DENIAL_COUNT:
        new_status = STATUS_FROZEN
        is_frozen = True
    else:
        new_status = STATUS_DENIED
        is_frozen = False

    cursor.execute("""
        UPDATE authorized_users
        SET status = ?, reviewed_at = CURRENT_TIMESTAMP, reviewed_by = ?, denial_count = ?
        WHERE user_id = ?
    """, (new_status, admin_id, new_count, user_id))

    success = cursor.rowcount > 0
    conn.commit()
    conn.close()

    if success:
        if is_frozen:
            logger.info(f"User {user_id} FROZEN by admin {admin_id} (denied {new_count} times)")
        else:
            logger.info(f"User {user_id} denied by admin {admin_id} (count: {new_count}/{MAX_DENIAL_COUNT})")
    return success, is_frozen


def get_pending_requests() -> List[Dict[str, Any]]:
    """Get all pending access requests."""
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT * FROM authorized_users
        WHERE status = ?
        ORDER BY requested_at ASC
    """, (STATUS_PENDING,))

    rows = cursor.fetchall()
    conn.close()

    return [dict(row) for row in rows]


def get_all_authorized_users() -> List[Dict[str, Any]]:
    """Get all approved users."""
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT * FROM authorized_users
        WHERE status = ?
        ORDER BY reviewed_at DESC
    """, (STATUS_APPROVED,))

    rows = cursor.fetchall()
    conn.close()

    return [dict(row) for row in rows]


def get_frozen_users() -> List[Dict[str, Any]]:
    """Get all frozen users."""
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT * FROM authorized_users
        WHERE status = ?
        ORDER BY reviewed_at DESC
    """, (STATUS_FROZEN,))

    rows = cursor.fetchall()
    conn.close()

    return [dict(row) for row in rows]


def revoke_user(user_id: int, admin_id: int) -> bool:
    """Revoke user access (set to denied). Returns True if successful."""
    success, _ = deny_user(user_id, admin_id)
    return success


def reset_user_to_pending(user_id: int) -> tuple[bool, bool]:
    """
    Reset user status to pending (for re-requesting access).
    Returns (success, was_frozen) tuple.
    Frozen users cannot reset.
    """
    # Check if frozen first
    if is_user_frozen(user_id):
        logger.warning(f"Frozen user {user_id} attempted to re-request access")
        return False, True

    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("""
        UPDATE authorized_users
        SET status = ?, requested_at = CURRENT_TIMESTAMP, reviewed_at = NULL, reviewed_by = NULL
        WHERE user_id = ? AND status != ?
    """, (STATUS_PENDING, user_id, STATUS_FROZEN))

    success = cursor.rowcount > 0
    conn.commit()
    conn.close()

    if success:
        logger.info(f"User {user_id} reset to pending status")
    return success, False


def update_last_activity(user_id: int) -> None:
    """Update user's last activity timestamp."""
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("""
        UPDATE authorized_users
        SET last_activity = CURRENT_TIMESTAMP
        WHERE user_id = ? AND status = ?
    """, (user_id, STATUS_APPROVED))

    conn.commit()
    conn.close()


def revoke_inactive_users(days: int = 45) -> int:
    """
    Revoke access for users inactive for X days.
    Returns count of revoked users.
    """
    conn = get_connection()
    cursor = conn.cursor()

    cutoff = datetime.now() - timedelta(days=days)

    # Get users to revoke (for logging)
    cursor.execute("""
        SELECT user_id, username FROM authorized_users
        WHERE status = ? AND (
            last_activity < ? OR
            (last_activity IS NULL AND reviewed_at < ?)
        )
    """, (STATUS_APPROVED, cutoff.isoformat(), cutoff.isoformat()))

    users_to_revoke = cursor.fetchall()

    if users_to_revoke:
        # Revoke inactive users
        cursor.execute("""
            UPDATE authorized_users
            SET status = ?
            WHERE status = ? AND (
                last_activity < ? OR
                (last_activity IS NULL AND reviewed_at < ?)
            )
        """, (STATUS_DENIED, STATUS_APPROVED, cutoff.isoformat(), cutoff.isoformat()))

        revoked_count = cursor.rowcount
        conn.commit()

        for user in users_to_revoke:
            logger.info(f"Revoked inactive user {user[0]} (@{user[1]}) - no activity for {days}+ days")
    else:
        revoked_count = 0

    conn.close()
    return revoked_count


# ═══════════════════════════════════════════════════════════════════════════
# USER PREFERENCES
# ═══════════════════════════════════════════════════════════════════════════

def get_user_preferences(user_id: int) -> Optional[Dict[str, Any]]:
    """Get user preferences."""
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute(
        "SELECT * FROM user_preferences WHERE user_id = ?",
        (user_id,)
    )
    row = cursor.fetchone()
    conn.close()

    if row:
        return dict(row)
    return None


def save_user_preferences(
    user_id: int,
    default_source: str = None,
    default_report_type: str = "summary",
    timezone: str = "Europe/Kyiv",
    default_date_range: str = "week",
    notifications_enabled: bool = True
) -> None:
    """Save or update user preferences."""
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("""
        INSERT INTO user_preferences (user_id, default_source, default_report_type, timezone, default_date_range, notifications_enabled, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        ON CONFLICT(user_id) DO UPDATE SET
            default_source = excluded.default_source,
            default_report_type = excluded.default_report_type,
            timezone = excluded.timezone,
            default_date_range = excluded.default_date_range,
            notifications_enabled = excluded.notifications_enabled,
            updated_at = CURRENT_TIMESTAMP
    """, (user_id, default_source, default_report_type, timezone, default_date_range, 1 if notifications_enabled else 0))

    conn.commit()
    conn.close()
    logger.debug(f"Saved preferences for user {user_id}")


def update_user_preference(user_id: int, key: str, value: Any) -> None:
    """Update a single user preference."""
    # Ensure user exists first
    if not get_user_preferences(user_id):
        save_user_preferences(user_id)

    conn = get_connection()
    cursor = conn.cursor()

    # Only allow specific keys
    allowed_keys = {'default_source', 'default_report_type', 'timezone', 'default_date_range', 'notifications_enabled'}
    if key not in allowed_keys:
        logger.warning(f"Attempted to update invalid preference key: {key}")
        return

    cursor.execute(f"""
        UPDATE user_preferences
        SET {key} = ?, updated_at = CURRENT_TIMESTAMP
        WHERE user_id = ?
    """, (value, user_id))

    conn.commit()
    conn.close()


# ═══════════════════════════════════════════════════════════════════════════
# REPORT HISTORY
# ═══════════════════════════════════════════════════════════════════════════

def save_report_history(
    user_id: int,
    report_type: str,
    start_date: str,
    end_date: str,
    source: str = None
) -> None:
    """Save a report to history."""
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("""
        INSERT INTO report_history (user_id, report_type, start_date, end_date, source)
        VALUES (?, ?, ?, ?, ?)
    """, (user_id, report_type, start_date, end_date, source))

    conn.commit()
    conn.close()
    logger.debug(f"Saved report history for user {user_id}")


def get_last_report(user_id: int) -> Optional[Dict[str, Any]]:
    """Get user's most recent report."""
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT * FROM report_history
        WHERE user_id = ?
        ORDER BY created_at DESC
        LIMIT 1
    """, (user_id,))

    row = cursor.fetchone()
    conn.close()

    if row:
        return dict(row)
    return None


def get_report_history(user_id: int, limit: int = 10) -> List[Dict[str, Any]]:
    """Get user's report history."""
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT * FROM report_history
        WHERE user_id = ?
        ORDER BY created_at DESC
        LIMIT ?
    """, (user_id, limit))

    rows = cursor.fetchall()
    conn.close()

    return [dict(row) for row in rows]


def cleanup_old_history(days: int = 30) -> int:
    """Delete report history older than X days. Returns count deleted."""
    conn = get_connection()
    cursor = conn.cursor()

    cutoff = datetime.now() - timedelta(days=days)

    cursor.execute("""
        DELETE FROM report_history
        WHERE created_at < ?
    """, (cutoff.isoformat(),))

    deleted = cursor.rowcount
    conn.commit()
    conn.close()

    if deleted > 0:
        logger.info(f"Cleaned up {deleted} old history records")

    return deleted


# ═══════════════════════════════════════════════════════════════════════════
# CACHE
# ═══════════════════════════════════════════════════════════════════════════

def cache_get(key: str) -> Optional[Any]:
    """Get value from cache if not expired."""
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT value, expires_at FROM cache
        WHERE cache_key = ?
    """, (key,))

    row = cursor.fetchone()
    conn.close()

    if not row:
        return None

    # Check if expired
    expires_at = datetime.fromisoformat(row['expires_at'])
    if datetime.now() > expires_at:
        cache_delete(key)
        return None

    try:
        return json.loads(row['value'])
    except json.JSONDecodeError:
        return row['value']


def cache_set(key: str, value: Any, ttl_minutes: int = 10) -> None:
    """Set value in cache with TTL."""
    conn = get_connection()
    cursor = conn.cursor()

    expires_at = datetime.now() + timedelta(minutes=ttl_minutes)

    # Serialize value
    if isinstance(value, (dict, list)):
        value_str = json.dumps(value)
    else:
        value_str = str(value)

    cursor.execute("""
        INSERT INTO cache (cache_key, value, expires_at)
        VALUES (?, ?, ?)
        ON CONFLICT(cache_key) DO UPDATE SET
            value = excluded.value,
            expires_at = excluded.expires_at,
            created_at = CURRENT_TIMESTAMP
    """, (key, value_str, expires_at.isoformat()))

    conn.commit()
    conn.close()


def cache_delete(key: str) -> None:
    """Delete a cache entry."""
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("DELETE FROM cache WHERE cache_key = ?", (key,))

    conn.commit()
    conn.close()


def cache_cleanup() -> int:
    """Remove all expired cache entries. Returns count deleted."""
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("""
        DELETE FROM cache
        WHERE expires_at < ?
    """, (datetime.now().isoformat(),))

    deleted = cursor.rowcount
    conn.commit()
    conn.close()

    if deleted > 0:
        logger.debug(f"Cleaned up {deleted} expired cache entries")

    return deleted


# ═══════════════════════════════════════════════════════════════════════════
# UTILITY FUNCTIONS
# ═══════════════════════════════════════════════════════════════════════════

def generate_cache_key(prefix: str, **kwargs) -> str:
    """Generate a cache key from prefix and parameters."""
    parts = [prefix]
    for k, v in sorted(kwargs.items()):
        parts.append(f"{k}={v}")
    return ":".join(parts)


def get_database_stats() -> Dict[str, int]:
    """Get database statistics."""
    conn = get_connection()
    cursor = conn.cursor()

    stats = {}

    cursor.execute("SELECT COUNT(*) FROM user_preferences")
    stats['users'] = cursor.fetchone()[0]

    cursor.execute("SELECT COUNT(*) FROM report_history")
    stats['reports'] = cursor.fetchone()[0]

    cursor.execute("SELECT COUNT(*) FROM cache WHERE expires_at > ?",
                   (datetime.now().isoformat(),))
    stats['active_cache'] = cursor.fetchone()[0]

    conn.close()
    return stats


# ═══════════════════════════════════════════════════════════════════════════
# MILESTONE TRACKING
# ═══════════════════════════════════════════════════════════════════════════

def is_milestone_celebrated(period_type: str, period_key: str, milestone_amount: int) -> bool:
    """Check if a milestone has already been celebrated for a given period."""
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT 1 FROM celebrated_milestones
        WHERE period_type = ? AND period_key = ? AND milestone_amount = ?
    """, (period_type, period_key, milestone_amount))

    result = cursor.fetchone() is not None
    conn.close()
    return result


def mark_milestone_celebrated(period_type: str, period_key: str, milestone_amount: int, revenue: float) -> bool:
    """Mark a milestone as celebrated. Returns True if newly inserted, False if already exists."""
    conn = get_connection()
    cursor = conn.cursor()

    try:
        cursor.execute("""
            INSERT INTO celebrated_milestones (period_type, period_key, milestone_amount, revenue)
            VALUES (?, ?, ?, ?)
        """, (period_type, period_key, milestone_amount, revenue))
        conn.commit()
        result = True
    except sqlite3.IntegrityError:
        # Already celebrated
        result = False

    conn.close()
    return result


def get_celebrated_milestones(period_type: str = None, limit: int = 10) -> List[Dict[str, Any]]:
    """Get recent celebrated milestones."""
    conn = get_connection()
    cursor = conn.cursor()

    if period_type:
        cursor.execute("""
            SELECT * FROM celebrated_milestones
            WHERE period_type = ?
            ORDER BY celebrated_at DESC
            LIMIT ?
        """, (period_type, limit))
    else:
        cursor.execute("""
            SELECT * FROM celebrated_milestones
            ORDER BY celebrated_at DESC
            LIMIT ?
        """, (limit,))

    rows = cursor.fetchall()
    conn.close()
    return [dict(row) for row in rows]
