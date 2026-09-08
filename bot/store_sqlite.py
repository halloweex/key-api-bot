"""The bot's state in SQLite — the adapter behind `core/bot_store.py`.

Everything here was `bot/database.py` until the port existed. The SQL is
unchanged, statement for statement, because the 44 tests in
`tests/unit/test_bot_database.py` were written against it the day before it
moved and are what says the move preserved behaviour. A "tidy-up while moving"
would have made them prove less.

WHERE THE PATH LIVES

`DB_PATH` is read at call time, not captured at construction, so a test can
redirect it by rebinding this module's global — the same arrangement
`core/duckdb_store.py` has, and for the same reason: a value bound once is a
production path nothing can patch.

TWO CLOCKS, AND THE ADAPTER OWNS BOTH

`authorized_users`, `user_preferences` and `celebrated_milestones` carry
timestamps written by SQLite's `CURRENT_TIMESTAMP`, which is **UTC** in
`'YYYY-MM-DD HH:MM:SS'`. Anything comparing against them must use `_utc_now()`
and `_sqlite_stamp()`, which is what closed two defects on the inactivity
sweep — see the commit that added the tests.

The cache is the exception and stays the exception: it writes `expires_at`
itself with Python's local `datetime.now().isoformat()` and reads it back the
same way. Self-consistent, different convention, and unifying the two would
expire the cache by the wrong clock.
"""
from __future__ import annotations

import json
import logging
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Generator, List, Optional, Sequence, Tuple

logger = logging.getLogger(__name__)

DB_PATH = Path(__file__).parent.parent / "data" / "bot.db"

# Status constants
STATUS_PENDING = "pending"
STATUS_APPROVED = "approved"
STATUS_DENIED = "denied"
STATUS_FROZEN = "frozen"

# Max denial count before freezing
MAX_DENIAL_COUNT = 5

# Freeze duration in days
FREEZE_DURATION_DAYS = 30

# The settings `set` will write. The key reaches the SQL by name, so this is
# the only thing between a caller and an injection.
ALLOWED_PREFERENCES = frozenset({
    "default_source", "default_report_type", "timezone",
    "default_date_range", "notifications_enabled", "language",
})


def _utc_now() -> datetime:
    """Naive UTC — the shape SQLite stores, so the two are comparable."""
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _sqlite_stamp(moment: datetime) -> str:
    """A moment in the exact text `CURRENT_TIMESTAMP` produces."""
    return moment.strftime("%Y-%m-%d %H:%M:%S")


def get_connection() -> sqlite3.Connection:
    """Get database connection with row factory."""
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    return conn


@contextmanager
def db_connection() -> Generator[sqlite3.Connection, None, None]:
    """Context manager for database connections with automatic cleanup."""
    conn = get_connection()
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


class SqliteAccessControl:
    """`authorized_users`."""

    def status(self, user_id: int) -> Optional[Dict[str, Any]]:
        with db_connection() as conn:
            row = conn.execute(
                "SELECT * FROM authorized_users WHERE user_id = ?", (user_id,)
            ).fetchone()
            return dict(row) if row else None

    def is_approved(self, user_id: int) -> bool:
        status = self.status(user_id)
        return bool(status) and status["status"] == STATUS_APPROVED

    def is_frozen(self, user_id: int) -> bool:
        status = self.status(user_id)
        if not status or status["status"] != STATUS_FROZEN:
            return False

        reviewed_at = status.get("reviewed_at")
        if reviewed_at:
            try:
                frozen_date = datetime.fromisoformat(reviewed_at)
                if _utc_now() - frozen_date > timedelta(days=FREEZE_DURATION_DAYS):
                    self._auto_unfreeze(user_id)
                    logger.info(
                        f"User {user_id} auto-unfrozen after "
                        f"{FREEZE_DURATION_DAYS} days"
                    )
                    return False
            except (ValueError, TypeError):
                # Fail closed: a value nobody can read is not a licence to
                # come back.
                pass

        return True

    def _auto_unfreeze(self, user_id: int) -> None:
        with db_connection() as conn:
            conn.execute(
                """
                UPDATE authorized_users
                SET status = ?, denial_count = 0
                WHERE user_id = ? AND status = ?
                """,
                (STATUS_DENIED, user_id, STATUS_FROZEN),
            )

    def request(
        self, user_id: int, username: Optional[str] = None,
        first_name: Optional[str] = None, last_name: Optional[str] = None,
    ) -> bool:
        with db_connection() as conn:
            existing = conn.execute(
                "SELECT status FROM authorized_users WHERE user_id = ?",
                (user_id,),
            ).fetchone()
            if existing:
                return False

            conn.execute(
                """
                INSERT INTO authorized_users
                       (user_id, username, first_name, last_name, status)
                VALUES (?, ?, ?, ?, ?)
                """,
                (user_id, username, first_name, last_name, STATUS_PENDING),
            )

        logger.info(f"Access request created for user {user_id} (@{username})")
        return True

    def approve(
        self, user_id: int, admin_id: int, *, expected_status: Optional[str] = None,
    ) -> bool:
        with db_connection() as conn:
            cursor = conn.execute(
                """
                UPDATE authorized_users
                SET status = ?, reviewed_at = CURRENT_TIMESTAMP,
                    reviewed_by = ?, denial_count = 0
                WHERE user_id = ? AND (? IS NULL OR status = ?)
                """,
                (STATUS_APPROVED, admin_id, user_id, expected_status, expected_status),
            )
            success = cursor.rowcount > 0

        if success:
            logger.info(f"User {user_id} approved by admin {admin_id}")
        return success

    def deny(
        self, user_id: int, admin_id: int, *, expected_status: Optional[str] = None,
    ) -> Tuple[bool, bool]:
        with db_connection() as conn:
            row = conn.execute(
                "SELECT denial_count, status FROM authorized_users WHERE user_id = ?",
                (user_id,),
            ).fetchone()
            if expected_status is not None and (row is None or row[1] != expected_status):
                return False, False
            current_count = (row[0] or 0) if row else 0
            new_count = current_count + 1

            if new_count >= MAX_DENIAL_COUNT:
                new_status, is_frozen = STATUS_FROZEN, True
            else:
                new_status, is_frozen = STATUS_DENIED, False

            cursor = conn.execute(
                """
                UPDATE authorized_users
                SET status = ?, reviewed_at = CURRENT_TIMESTAMP,
                    reviewed_by = ?, denial_count = ?
                WHERE user_id = ?
                """,
                (new_status, admin_id, new_count, user_id),
            )
            success = cursor.rowcount > 0

        if success:
            if is_frozen:
                logger.info(
                    f"User {user_id} FROZEN by admin {admin_id} "
                    f"(denied {new_count} times)"
                )
            else:
                logger.info(
                    f"User {user_id} denied by admin {admin_id} "
                    f"(count: {new_count}/{MAX_DENIAL_COUNT})"
                )
        return success, is_frozen

    def unfreeze(self, user_id: int, admin_id: int) -> bool:
        with db_connection() as conn:
            cursor = conn.execute(
                """
                UPDATE authorized_users
                SET status = ?, denial_count = 0,
                    reviewed_at = CURRENT_TIMESTAMP, reviewed_by = ?
                WHERE user_id = ? AND status = ?
                """,
                (STATUS_DENIED, admin_id, user_id, STATUS_FROZEN),
            )
            success = cursor.rowcount > 0

        if success:
            logger.info(f"User {user_id} unfrozen by admin {admin_id}")
        return success

    def reset_to_pending(self, user_id: int) -> Tuple[bool, bool]:
        if self.is_frozen(user_id):
            logger.warning(
                f"Frozen user {user_id} attempted to re-request access"
            )
            return False, True

        with db_connection() as conn:
            cursor = conn.execute(
                """
                UPDATE authorized_users
                SET status = ?, requested_at = CURRENT_TIMESTAMP,
                    reviewed_at = NULL, reviewed_by = NULL
                WHERE user_id = ? AND status = ?
                """,
                (STATUS_PENDING, user_id, STATUS_DENIED),
            )
            success = cursor.rowcount > 0

        if success:
            logger.info(f"User {user_id} reset to pending status")
        return success, False

    def touch(self, user_id: int) -> None:
        with db_connection() as conn:
            conn.execute(
                """
                UPDATE authorized_users
                SET last_activity = CURRENT_TIMESTAMP
                WHERE user_id = ? AND status = ?
                """,
                (user_id, STATUS_APPROVED),
            )

    def sweep_inactive(self, days: int = 45) -> int:
        cutoff = _sqlite_stamp(_utc_now() - timedelta(days=days))

        with db_connection() as conn:
            users_to_revoke = conn.execute(
                """
                SELECT user_id, username FROM authorized_users
                WHERE status = ? AND (
                    last_activity < ? OR
                    (last_activity IS NULL AND reviewed_at < ?)
                )
                """,
                (STATUS_APPROVED, cutoff, cutoff),
            ).fetchall()

            if not users_to_revoke:
                return 0

            cursor = conn.execute(
                """
                UPDATE authorized_users
                SET status = ?
                WHERE status = ? AND (
                    last_activity < ? OR
                    (last_activity IS NULL AND reviewed_at < ?)
                )
                """,
                (STATUS_DENIED, STATUS_APPROVED, cutoff, cutoff),
            )
            revoked_count = cursor.rowcount

            for user in users_to_revoke:
                logger.info(
                    f"Revoked inactive user {user[0]} (@{user[1]}) - "
                    f"no activity for {days}+ days"
                )

        return revoked_count

    def _by_status(self, status: str, order: str) -> List[Dict[str, Any]]:
        with db_connection() as conn:
            rows = conn.execute(
                f"SELECT * FROM authorized_users WHERE status = ? ORDER BY {order}",
                (status,),
            ).fetchall()
            return [dict(row) for row in rows]

    def approved(self) -> List[Dict[str, Any]]:
        return self._by_status(STATUS_APPROVED, "reviewed_at DESC")

    def pending(self) -> List[Dict[str, Any]]:
        return self._by_status(STATUS_PENDING, "requested_at ASC")

    def frozen(self) -> List[Dict[str, Any]]:
        return self._by_status(STATUS_FROZEN, "reviewed_at DESC")


class SqlitePreferences:
    """`user_preferences`."""

    def get(self, user_id: int) -> Optional[Dict[str, Any]]:
        with db_connection() as conn:
            row = conn.execute(
                "SELECT * FROM user_preferences WHERE user_id = ?", (user_id,)
            ).fetchone()
            return dict(row) if row else None

    def save(
        self, user_id: int, default_source: Optional[str] = None,
        default_report_type: str = "summary", timezone: str = "Europe/Kyiv",
        default_date_range: str = "week", notifications_enabled: bool = True,
    ) -> None:
        # `language` is written as NULL on insert and never touched on
        # conflict. The column's DEFAULT 'en' used to fill it the first time
        # somebody changed a timezone or muted notifications, and the reader
        # treats a stored value as a *choice* — so that first action silently
        # switched a Ukrainian-default user to English, in the bot and in the
        # weekly report.
        with db_connection() as conn:
            conn.execute(
                """
                INSERT INTO user_preferences
                       (user_id, default_source, default_report_type, timezone,
                        default_date_range, notifications_enabled, updated_at,
                        language)
                VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, NULL)
                ON CONFLICT(user_id) DO UPDATE SET
                    default_source = excluded.default_source,
                    default_report_type = excluded.default_report_type,
                    timezone = excluded.timezone,
                    default_date_range = excluded.default_date_range,
                    notifications_enabled = excluded.notifications_enabled,
                    updated_at = CURRENT_TIMESTAMP
                """,
                (user_id, default_source, default_report_type, timezone,
                 default_date_range, 1 if notifications_enabled else 0),
            )

        logger.debug(f"Saved preferences for user {user_id}")

    def set(self, user_id: int, key: str, value: Any) -> None:
        if not self.get(user_id):
            self.save(user_id)

        if key not in ALLOWED_PREFERENCES:
            logger.warning(f"Attempted to update invalid preference key: {key}")
            return

        with db_connection() as conn:
            # The column name is interpolated; `ALLOWED_PREFERENCES` above is
            # what makes that safe, and it is checked before this line runs.
            conn.execute(
                f"""
                UPDATE user_preferences
                SET {key} = ?, updated_at = CURRENT_TIMESTAMP
                WHERE user_id = ?
                """,
                (value, user_id),
            )


class SqliteMilestones:
    """`celebrated_milestones`."""

    def already_celebrated(
        self, period_type: str, period_key: str, amount: int,
    ) -> bool:
        with db_connection() as conn:
            return conn.execute(
                """
                SELECT 1 FROM celebrated_milestones
                WHERE period_type = ? AND period_key = ? AND milestone_amount = ?
                """,
                (period_type, period_key, amount),
            ).fetchone() is not None

    def celebrate(
        self, period_type: str, period_key: str, amount: int, revenue: float,
    ) -> bool:
        try:
            with db_connection() as conn:
                conn.execute(
                    """
                    INSERT INTO celebrated_milestones
                           (period_type, period_key, milestone_amount, revenue)
                    VALUES (?, ?, ?, ?)
                    """,
                    (period_type, period_key, amount, revenue),
                )
            return True
        except sqlite3.IntegrityError:
            # The UNIQUE constraint is the mechanism: already announced.
            return False

    def recent(
        self, period_type: Optional[str] = None, limit: int = 10,
    ) -> List[Dict[str, Any]]:
        with db_connection() as conn:
            if period_type:
                rows = conn.execute(
                    """
                    SELECT * FROM celebrated_milestones
                    WHERE period_type = ?
                    ORDER BY celebrated_at DESC
                    LIMIT ?
                    """,
                    (period_type, limit),
                ).fetchall()
            else:
                rows = conn.execute(
                    """
                    SELECT * FROM celebrated_milestones
                    ORDER BY celebrated_at DESC
                    LIMIT ?
                    """,
                    (limit,),
                ).fetchall()
            return [dict(row) for row in rows]


class SqliteCache:
    """`cache`. The one aggregate on Python's local clock — see the module
    docstring; it writes `expires_at` and reads it back itself."""

    def get(self, key: str) -> Optional[Any]:
        with db_connection() as conn:
            row = conn.execute(
                "SELECT value, expires_at FROM cache WHERE cache_key = ?",
                (key,),
            ).fetchone()

        if not row:
            return None

        expires_at = datetime.fromisoformat(row["expires_at"])
        if datetime.now() > expires_at:
            self.delete(key)
            return None

        try:
            return json.loads(row["value"])
        except json.JSONDecodeError:
            return row["value"]

    def set(self, key: str, value: Any, ttl_minutes: int = 10) -> None:
        expires_at = datetime.now() + timedelta(minutes=ttl_minutes)

        if isinstance(value, (dict, list)):
            value_str = json.dumps(value)
        else:
            value_str = str(value)

        with db_connection() as conn:
            conn.execute(
                """
                INSERT INTO cache (cache_key, value, expires_at)
                VALUES (?, ?, ?)
                ON CONFLICT(cache_key) DO UPDATE SET
                    value = excluded.value,
                    expires_at = excluded.expires_at,
                    created_at = CURRENT_TIMESTAMP
                """,
                (key, value_str, expires_at.isoformat()),
            )

    def delete(self, key: str) -> None:
        with db_connection() as conn:
            conn.execute("DELETE FROM cache WHERE cache_key = ?", (key,))

    def sweep(self) -> int:
        with db_connection() as conn:
            cursor = conn.execute(
                "DELETE FROM cache WHERE expires_at < ?",
                (datetime.now().isoformat(),),
            )
            deleted = cursor.rowcount

        if deleted > 0:
            logger.debug(f"Cleaned up {deleted} expired cache entries")

        return deleted


class NoDashboardTabs:
    """`dashboard`, when there is no way to reach it.

    The dashboard's user list lives in DuckDB under `KS_USER_STORE=duckdb`, and
    DuckDB takes a single writer that the web container holds — a bot process
    cannot open that file at all. This is the same wall `core/bot_prefs.py`
    documents from the other side.

    So the answer is `available() -> False` and nothing else pretends. The
    approval the admin taps still grants **bot** access, the person is still
    told, and the tabs are set from the admin page — which the bot says in the
    message rather than leaving the admin to wonder why no checklist appeared.
    """

    def available(self) -> bool:
        return False

    def get(self, user_id: int) -> Optional[Dict[str, Any]]:
        return None

    def grant(
        self, user_id: int, admin_id: int,
        features: Optional[Sequence[str]] = None,
        username: Optional[str] = None,
        first_name: Optional[str] = None,
        last_name: Optional[str] = None,
    ) -> bool:
        return False

    def set_features(
        self, user_id: int, features: Optional[Sequence[str]], admin_id: int,
    ) -> bool:
        return False

    def permissions(self, user_id: int) -> Optional[Dict[str, Dict[str, bool]]]:
        # Nothing to narrow: this store cannot see the dashboard's list at all,
        # so the bot behaves as it did before tabs existed.
        return None


class SqliteBotStore:
    """The four aggregates against one SQLite file, plus a fifth that is not
    reachable from here at all — see `NoDashboardTabs`."""

    def __init__(self) -> None:
        self.access = SqliteAccessControl()
        self.preferences = SqlitePreferences()
        self.milestones = SqliteMilestones()
        self.cache = SqliteCache()
        self.dashboard = NoDashboardTabs()

    def initialise(self) -> None:
        """Create every table, and add every column added since.

        A verbatim move, statement for statement and **in the original order**.
        The order is load-bearing in a way that is easy to miss: each
        `ALTER TABLE` follows the `CREATE TABLE IF NOT EXISTS` it belongs to,
        because on a fresh database the ALTER must fail (no such table) and be
        swallowed, while on production's database the CREATE is the no-op and
        the ALTER is the whole point. Reordering them looks like tidying and is
        a schema change.

        `denial_count` is likewise absent from the CREATE and added by ALTER,
        which is how production got it. Left alone for the same reason.
        """
        with db_connection() as conn:
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
                    language TEXT DEFAULT 'en',
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

            # The interface language, and the language the weekly report arrives
            # in. Read from the *web* container too — see core/bot_prefs.py —
            # because the report is built where DuckDB lives and this file is
            # the only place a user's choice is recorded.
            try:
                cursor.execute("ALTER TABLE user_preferences ADD COLUMN language TEXT DEFAULT 'en'")
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

        logger.info(f"Database initialized at {DB_PATH}")

    def stats(self) -> Dict[str, int]:
        with db_connection() as conn:
            return {
                "users": conn.execute(
                    "SELECT COUNT(*) FROM user_preferences"
                ).fetchone()[0],
                "reports": conn.execute(
                    "SELECT COUNT(*) FROM report_history"
                ).fetchone()[0],
                "active_cache": conn.execute(
                    "SELECT COUNT(*) FROM cache WHERE expires_at > ?",
                    (datetime.now().isoformat(),),
                ).fetchone()[0],
            }
