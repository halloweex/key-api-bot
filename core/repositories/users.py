"""The dashboard's user list, on whichever store owns it.

`KS_USER_STORE` (duckdb | postgres, a typo raises) decides. The bodies below
are one text with two holes — `{users}` for the table and `{self}` for the name
`ON CONFLICT DO UPDATE` may use to reach the existing row, which Postgres
insists be unqualified. Everything else is spelled portably, so the table names
stay the only difference and `core/pg_dashboard_users.py` carries the reason
the writer moved rather than a replica being read.

The permissions matrix at the bottom of this file has **not** moved. It is read
once per role and cached in-process (`core/permissions._permissions_cache`), so
it is not on the request path the switch exists to clear.
"""
from __future__ import annotations

import logging
from typing import Optional, List, Dict, Any, Sequence, Tuple

from core.permissions import parse_features

logger = logging.getLogger(__name__)

MAX_DENIAL_COUNT = 5


class UsersMixin:

    # ─── which store answers ──────────────────────────────────────────────
    #
    # Chosen before any connection is taken, `/inventory`'s §34 invariant: a
    # read bound for Postgres must not first queue behind DuckDB's single
    # writer, or the switch has moved the bottleneck instead of leaving it.
    #
    # **Never call these from inside `self.connection()`.** The store lock is
    # not reentrant and the deadlock does not raise — it simply never returns.

    async def _users_run(self, sql: str, params: Optional[Sequence[Any]] = None,
                         *, mode: str = "all"):
        """Run one statement against the store that owns the user list."""
        from core.pg_dashboard_users import user_store_is_postgres

        params = list(params or [])
        if user_store_is_postgres():
            from core.pg_dashboard_users import (
                TABLE, execute, fetch_row, fetch_rows,
            )
            from core.sql_dialect import numbered

            rendered = numbered(
                sql.format(users=TABLE, self=TABLE.split(".")[-1])
            )
            if mode == "all":
                return await fetch_rows(rendered, params)
            if mode == "one":
                return await fetch_row(rendered, params)
            await execute(rendered, params)
            return None

        rendered = sql.format(users="users", self="users")
        async with self.connection() as conn:
            cursor = conn.execute(rendered, params)
            if mode == "all":
                return cursor.fetchall()
            if mode == "one":
                return cursor.fetchone()
            return None

    @staticmethod
    def _stamp(value) -> Optional[str]:
        """A timestamp on its way out as text, pinned to UTC first.

        DuckDB renders a TIMESTAMPTZ in the session's timezone —
        `Europe/Kyiv` in the web container — and asyncpg hands back UTC. The
        same stored instant would leave as two different strings depending on
        which store answered, which is the defect the `/inventory` port hit
        with `lastSync` and the cohort tab hit a week earlier.
        """
        if value is None:
            return None
        from datetime import timezone

        if getattr(value, "tzinfo", None) is None:
            value = value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc).isoformat()

    async def get_user(self, user_id: int) -> Optional[Dict[str, Any]]:
        """Get user by ID. The read `_resolve_session` makes on every request."""
        row = await self._users_run("""
            SELECT user_id, username, first_name, last_name, photo_url,
                   role, status, requested_at, reviewed_at, reviewed_by,
                   last_activity, denial_count, created_at, allowed_features
            FROM {users} WHERE user_id = ?
        """, [user_id], mode="one")

        if not row:
            return None

        return {
            "user_id": row[0],
            "username": row[1],
            "first_name": row[2],
            "last_name": row[3],
            "photo_url": row[4],
            "role": row[5],
            "status": row[6],
            "requested_at": self._stamp(row[7]),
            "reviewed_at": self._stamp(row[8]),
            "reviewed_by": row[9],
            "last_activity": self._stamp(row[10]),
            "denial_count": row[11],
            "created_at": self._stamp(row[12]),
            # A list of tab keys, or None for "as the role". Read on every
            # request — `_resolve_session` carries it into the session so the
            # override costs no second query.
            "allowed_features": parse_features(row[13]),
        }

    async def get_user_by_status(self, status: str) -> List[Dict[str, Any]]:
        """Get all users with a given status."""
        rows = await self._users_run("""
            SELECT user_id, username, first_name, last_name, photo_url,
                   role, status, requested_at, reviewed_at, last_activity,
                   allowed_features
            FROM {users} WHERE status = ?
            -- `user_id` closes the sort: `requested_at` can tie, and two
            -- engines break a tie differently.
            ORDER BY requested_at DESC, user_id
        """, [status])

        return [
            {
                "user_id": row[0],
                "username": row[1],
                "first_name": row[2],
                "last_name": row[3],
                "photo_url": row[4],
                "role": row[5],
                "status": row[6],
                "requested_at": self._stamp(row[7]),
                "reviewed_at": self._stamp(row[8]),
                "last_activity": self._stamp(row[9]),
                "allowed_features": parse_features(row[10]),
            }
            for row in rows
        ]

    async def list_users(
        self,
        status: Optional[str] = None,
        role: Optional[str] = None,
        limit: int = 100,
        offset: int = 0
    ) -> List[Dict[str, Any]]:
        """List users with optional filters."""
        conditions = []
        params = []

        if status:
            conditions.append("status = ?")
            params.append(status)
        if role:
            conditions.append("role = ?")
            params.append(role)

        where_clause = f"WHERE {' AND '.join(conditions)}" if conditions else ""
        params.extend([limit, offset])

        rows = await self._users_run(f"""
            SELECT user_id, username, first_name, last_name, photo_url,
                   role, status, requested_at, reviewed_at, last_activity,
                   allowed_features
            FROM {{users}}
            {where_clause}
            ORDER BY
                CASE status
                    WHEN 'pending' THEN 1
                    WHEN 'approved' THEN 2
                    WHEN 'denied' THEN 3
                    WHEN 'frozen' THEN 4
                END,
                requested_at DESC,
                -- The page is paged, so a tie under LIMIT/OFFSET would let the
                -- two engines return different people on the same page.
                user_id
            LIMIT ? OFFSET ?
        """, params)

        return [
            {
                "user_id": row[0],
                "username": row[1],
                "first_name": row[2],
                "last_name": row[3],
                "photo_url": row[4],
                "role": row[5],
                "status": row[6],
                "requested_at": self._stamp(row[7]),
                "reviewed_at": self._stamp(row[8]),
                "last_activity": self._stamp(row[9]),
                "allowed_features": parse_features(row[10]),
            }
            for row in rows
        ]

    async def create_user(
        self,
        user_id: int,
        username: Optional[str] = None,
        first_name: Optional[str] = None,
        last_name: Optional[str] = None,
        photo_url: Optional[str] = None,
        status: str = "pending",
        role: str = "viewer"
    ) -> Dict[str, Any]:
        """Create a new user (access request)."""
        # `{self}` and not `{users}`: PostgreSQL requires the *unqualified*
        # relation name to reach the existing row in `DO UPDATE`, and
        # `app.dashboard_users.username` is rejected there.
        #
        # The COALESCE is why a repeat login cannot clear a name Telegram
        # stopped sending, and why it cannot reset a status or a role either —
        # those two are absent from the update list on purpose.
        await self._users_run("""
            INSERT INTO {users}
                (user_id, username, first_name, last_name, photo_url, status, role)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (user_id) DO UPDATE SET
                username = COALESCE(excluded.username, {self}.username),
                first_name = COALESCE(excluded.first_name, {self}.first_name),
                last_name = COALESCE(excluded.last_name, {self}.last_name),
                photo_url = COALESCE(excluded.photo_url, {self}.photo_url)
        """, [user_id, username, first_name, last_name, photo_url, status, role],
            mode="none")

        return await self.get_user(user_id)

    async def update_user_role(
        self,
        user_id: int,
        role: str,
        changed_by: int
    ) -> bool:
        """Update user role. Returns True if updated."""
        from core.permissions import Role

        if role not in {r.value for r in Role}:
            raise ValueError(f"Invalid role: {role}")

        result = await self._users_run("""
            UPDATE {users}
            SET role = ?, reviewed_at = CURRENT_TIMESTAMP, reviewed_by = ?
            WHERE user_id = ?
            RETURNING user_id
        """, [role, changed_by, user_id], mode="one")

        return result is not None

    async def update_user_status(
        self,
        user_id: int,
        status: str,
        reviewed_by: int
    ) -> bool:
        """Update user status (approve, deny, etc). Returns True if updated."""
        if status not in ("pending", "approved", "denied", "frozen"):
            raise ValueError(f"Invalid status: {status}")

        # Approving clears the denial count, so five past refusals do not
        # leave somebody one refusal from a thirty-day freeze.
        if status == "approved":
            result = await self._users_run("""
                UPDATE {users}
                SET status = ?, reviewed_at = CURRENT_TIMESTAMP,
                    reviewed_by = ?, denial_count = 0
                WHERE user_id = ?
                RETURNING user_id
            """, [status, reviewed_by, user_id], mode="one")
        else:
            result = await self._users_run("""
                UPDATE {users}
                SET status = ?, reviewed_at = CURRENT_TIMESTAMP, reviewed_by = ?
                WHERE user_id = ?
                RETURNING user_id
            """, [status, reviewed_by, user_id], mode="one")

        return result is not None

    async def grant_access(
        self,
        user_id: int,
        reviewed_by: int,
        features: Optional[Sequence[str]] = None,
        username: Optional[str] = None,
        first_name: Optional[str] = None,
        last_name: Optional[str] = None,
        role: Optional[str] = None,
    ) -> bool:
        """Approve dashboard access and set the tab set, in one statement.

        The web container's half of the decision the bot makes from a Telegram
        keyboard. Same statement — `core/dashboard_access.GRANT_ACCESS` — run
        through whichever store `KS_USER_STORE` names, because a grant written
        two ways drifts the first time one of them learns something.

        Not `create_user` followed by `update_user_status`: between two
        statements the person is approved carrying the previous tab set, and
        the approval is the moment they are told they have access.
        """
        from core.dashboard_access import GRANT_ACCESS
        from core.permissions import Role, serialize_features

        await self._users_run(
            GRANT_ACCESS,
            [
                user_id, username, first_name, last_name,
                # The role a *new* row starts at. An existing row keeps its
                # own: the statement leaves `role` out of its update list, so
                # approving cannot demote an admin whose request came back.
                role or Role.VIEWER.value,
                serialize_features(features),
                reviewed_by,
            ],
            mode="none",
        )
        return True

    async def set_user_features(
        self,
        user_id: int,
        features: Optional[Sequence[str]],
        changed_by: int,
    ) -> bool:
        """Set which tabs one person may open. Returns True if the row existed.

        ``features=None`` clears the override — the person goes back to seeing
        whatever their role shows, which is what every account meant before
        this column existed. An empty list is *not* the same thing and is
        stored as one: somebody ticked nothing, deliberately.

        The statement lives in `core/dashboard_access.py` because the bot
        writes the same column at the moment it approves a request, through a
        different engine and a different container.
        """
        from core.dashboard_access import SET_FEATURES
        from core.permissions import serialize_features

        result = await self._users_run(
            SET_FEATURES,
            [serialize_features(features), changed_by, user_id],
            mode="one",
        )
        return result is not None

    async def update_user_activity(self, user_id: int) -> bool:
        """Update user's last activity timestamp. Returns True if updated."""
        result = await self._users_run("""
            UPDATE {users}
            SET last_activity = CURRENT_TIMESTAMP
            WHERE user_id = ?
            RETURNING user_id
        """, [user_id], mode="one")
        return result is not None

    async def deny_user(self, user_id: int, admin_id: int) -> Tuple[bool, bool]:
        """Deny access, counting the refusal. Returns (found, is_frozen).

        One statement, where it used to be a SELECT and then an UPDATE. Under
        DuckDB those two ran inside one acquisition of the store lock, so
        nothing could interleave; on a Postgres pool they are two round trips
        and two admins refusing the same person at once would both read the
        same count, write the same count, and lose a refusal on the way to the
        freeze. The increment happens in the database now, and the new value
        comes back rather than being computed here.
        """
        row = await self._users_run("""
            UPDATE {users}
            SET denial_count = COALESCE(denial_count, 0) + 1,
                status = CASE
                    WHEN COALESCE(denial_count, 0) + 1 >= ? THEN 'frozen'
                    ELSE 'denied'
                END,
                reviewed_at = CURRENT_TIMESTAMP,
                reviewed_by = ?
            WHERE user_id = ?
            RETURNING denial_count
        """, [MAX_DENIAL_COUNT, admin_id, user_id], mode="one")

        if row is None:
            return False, False
        return True, (row[0] or 0) >= MAX_DENIAL_COUNT

    async def update_last_activity(self, user_id: int) -> None:
        """Update user's last activity timestamp."""
        await self._users_run("""
            UPDATE {users}
            SET last_activity = CURRENT_TIMESTAMP
            WHERE user_id = ? AND status = 'approved'
        """, [user_id], mode="none")

    async def is_user_authorized(self, user_id: int) -> bool:
        """Check if user is authorized (approved status)."""
        row = await self._users_run(
            "SELECT status FROM {users} WHERE user_id = ?", [user_id], mode="one",
        )
        return row is not None and row[0] == "approved"

    async def get_pending_users(self) -> List[Dict[str, Any]]:
        """Get all users with pending status."""
        return await self.get_user_by_status("pending")

    async def get_approved_users(self) -> List[Dict[str, Any]]:
        """Get all approved users."""
        return await self.get_user_by_status("approved")

    # ─── Role Permissions ─────────────────────────────────────────────────────

    async def get_role_permissions(self, role: str) -> Dict[str, Dict[str, bool]]:
        """
        Get all permissions for a role.

        Returns dict of feature -> {view: bool, edit: bool, delete: bool}
        """
        async with self.connection() as conn:
            rows = conn.execute("""
                SELECT feature, can_view, can_edit, can_delete
                FROM role_permissions
                WHERE role = ?
            """, [role]).fetchall()

            return {
                row[0]: {
                    "view": bool(row[1]),
                    "edit": bool(row[2]),
                    "delete": bool(row[3]),
                }
                for row in rows
            }

    async def get_all_permissions(self) -> Dict[str, Dict[str, Dict[str, bool]]]:
        """
        Get permissions for all roles.

        Returns dict of role -> feature -> {view: bool, edit: bool, delete: bool}
        """
        async with self.connection() as conn:
            rows = conn.execute("""
                SELECT role, feature, can_view, can_edit, can_delete
                FROM role_permissions
                ORDER BY role, feature
            """).fetchall()

            result: Dict[str, Dict[str, Dict[str, bool]]] = {}
            for row in rows:
                role, feature = row[0], row[1]
                if role not in result:
                    result[role] = {}
                result[role][feature] = {
                    "view": bool(row[2]),
                    "edit": bool(row[3]),
                    "delete": bool(row[4]),
                }
            return result

    async def set_permission(
        self,
        role: str,
        feature: str,
        can_view: Optional[bool],
        can_edit: Optional[bool],
        can_delete: Optional[bool],
        updated_by: int
    ) -> bool:
        """Set permission for a role/feature combination.

        Column-level, the shape `create_user` uses for names: a flag passed as
        None keeps the stored value, so two admins toggling different columns
        of one cell from stale copies of the matrix both land instead of the
        later one putting the earlier one back.
        """
        async with self.connection() as conn:
            conn.execute("""
                INSERT INTO role_permissions (role, feature, can_view, can_edit, can_delete, updated_at, updated_by)
                VALUES (?, ?, COALESCE(?, FALSE), COALESCE(?, FALSE), COALESCE(?, FALSE), CURRENT_TIMESTAMP, ?)
                ON CONFLICT (role, feature) DO UPDATE SET
                    can_view = COALESCE(?, role_permissions.can_view),
                    can_edit = COALESCE(?, role_permissions.can_edit),
                    can_delete = COALESCE(?, role_permissions.can_delete),
                    updated_at = excluded.updated_at,
                    updated_by = excluded.updated_by
            """, [role, feature, can_view, can_edit, can_delete, updated_by,
                  can_view, can_edit, can_delete])
            return True

    async def seed_default_permissions(self) -> None:
        """Insert any (role, feature) pair the table does not carry yet.

        Rows are derived from ``ROLE_PERMISSIONS`` rather than restated here:
        the copy that used to live in this function drifted the moment a
        feature was added, and a missing row reads as "denied" — a new feature
        would have been invisible to every DB-backed role, admins included.

        Missing rows are filled in on every call, not only into an empty
        table. Turning a permission off stores ``false``; it never deletes the
        row, so re-seeding cannot resurrect a decision somebody made.
        """
        from core.permissions import ROLE_PERMISSIONS, Action

        async with self.connection() as conn:
            existing = {
                (row[0], row[1])
                for row in conn.execute(
                    "SELECT role, feature FROM role_permissions"
                ).fetchall()
            }

            inserted = 0
            for role, features in ROLE_PERMISSIONS.items():
                for feature, actions in features.items():
                    key = (str(role.value), str(feature.value))
                    if key in existing:
                        continue
                    conn.execute("""
                        INSERT INTO role_permissions (role, feature, can_view, can_edit, can_delete)
                        VALUES (?, ?, ?, ?, ?)
                        ON CONFLICT (role, feature) DO NOTHING
                    """, [
                        key[0], key[1],
                        Action.VIEW in actions,
                        Action.EDIT in actions,
                        Action.DELETE in actions,
                    ])
                    inserted += 1

            if inserted:
                logger.info("Seeded %d missing role permissions", inserted)


# ═══════════════════════════════════════════════════════════════════════════════
# SINGLETON INSTANCE
# ═══════════════════════════════════════════════════════════════════════════════

_store_instance: Optional[DuckDBStore] = None


async def get_store() -> DuckDBStore:
    """Get singleton DuckDB store instance."""
    global _store_instance
    if _store_instance is None:
        _store_instance = DuckDBStore()
        await _store_instance.connect()
    return _store_instance


async def close_store() -> None:
    """Close singleton store instance."""
    global _store_instance
    if _store_instance:
        await _store_instance.close()
        _store_instance = None
