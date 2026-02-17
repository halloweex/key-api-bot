"""DuckDBStore user management methods."""
from __future__ import annotations

import logging
from typing import Optional, List, Dict, Any, Tuple

logger = logging.getLogger(__name__)


class UsersMixin:

    async def get_user(self, user_id: int) -> Optional[Dict[str, Any]]:
        """Get user by ID."""
        async with self.connection() as conn:
            row = conn.execute("""
                SELECT user_id, username, first_name, last_name, photo_url,
                       role, status, requested_at, reviewed_at, reviewed_by,
                       last_activity, denial_count, created_at
                FROM users WHERE user_id = ?
            """, [user_id]).fetchone()

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
                "requested_at": row[7].isoformat() if row[7] else None,
                "reviewed_at": row[8].isoformat() if row[8] else None,
                "reviewed_by": row[9],
                "last_activity": row[10].isoformat() if row[10] else None,
                "denial_count": row[11],
                "created_at": row[12].isoformat() if row[12] else None,
            }

    async def get_user_by_status(self, status: str) -> List[Dict[str, Any]]:
        """Get all users with a given status."""
        async with self.connection() as conn:
            rows = conn.execute("""
                SELECT user_id, username, first_name, last_name, photo_url,
                       role, status, requested_at, reviewed_at, last_activity
                FROM users WHERE status = ?
                ORDER BY requested_at DESC
            """, [status]).fetchall()

            return [
                {
                    "user_id": row[0],
                    "username": row[1],
                    "first_name": row[2],
                    "last_name": row[3],
                    "photo_url": row[4],
                    "role": row[5],
                    "status": row[6],
                    "requested_at": row[7].isoformat() if row[7] else None,
                    "reviewed_at": row[8].isoformat() if row[8] else None,
                    "last_activity": row[9].isoformat() if row[9] else None,
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

        async with self.connection() as conn:
            rows = conn.execute(f"""
                SELECT user_id, username, first_name, last_name, photo_url,
                       role, status, requested_at, reviewed_at, last_activity
                FROM users
                {where_clause}
                ORDER BY
                    CASE status
                        WHEN 'pending' THEN 1
                        WHEN 'approved' THEN 2
                        WHEN 'denied' THEN 3
                        WHEN 'frozen' THEN 4
                    END,
                    requested_at DESC
                LIMIT ? OFFSET ?
            """, params).fetchall()

            return [
                {
                    "user_id": row[0],
                    "username": row[1],
                    "first_name": row[2],
                    "last_name": row[3],
                    "photo_url": row[4],
                    "role": row[5],
                    "status": row[6],
                    "requested_at": row[7].isoformat() if row[7] else None,
                    "reviewed_at": row[8].isoformat() if row[8] else None,
                    "last_activity": row[9].isoformat() if row[9] else None,
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
        async with self.connection() as conn:
            conn.execute("""
                INSERT INTO users (user_id, username, first_name, last_name, photo_url, status, role)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (user_id) DO UPDATE SET
                    username = COALESCE(excluded.username, users.username),
                    first_name = COALESCE(excluded.first_name, users.first_name),
                    last_name = COALESCE(excluded.last_name, users.last_name),
                    photo_url = COALESCE(excluded.photo_url, users.photo_url)
            """, [user_id, username, first_name, last_name, photo_url, status, role])

        return await self.get_user(user_id)

    async def update_user_role(
        self,
        user_id: int,
        role: str,
        changed_by: int
    ) -> bool:
        """Update user role. Returns True if updated."""
        if role not in ("admin", "editor", "viewer"):
            raise ValueError(f"Invalid role: {role}")

        async with self.connection() as conn:
            result = conn.execute("""
                UPDATE users
                SET role = ?, reviewed_at = CURRENT_TIMESTAMP, reviewed_by = ?
                WHERE user_id = ?
                RETURNING user_id
            """, [role, changed_by, user_id]).fetchone()

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

        async with self.connection() as conn:
            # If approving, reset denial count
            if status == "approved":
                result = conn.execute("""
                    UPDATE users
                    SET status = ?, reviewed_at = CURRENT_TIMESTAMP,
                        reviewed_by = ?, denial_count = 0
                    WHERE user_id = ?
                    RETURNING user_id
                """, [status, reviewed_by, user_id]).fetchone()
            else:
                result = conn.execute("""
                    UPDATE users
                    SET status = ?, reviewed_at = CURRENT_TIMESTAMP, reviewed_by = ?
                    WHERE user_id = ?
                    RETURNING user_id
                """, [status, reviewed_by, user_id]).fetchone()

            return result is not None

    async def update_user_activity(self, user_id: int) -> bool:
        """Update user's last activity timestamp. Returns True if updated."""
        async with self.connection() as conn:
            result = conn.execute("""
                UPDATE users
                SET last_activity = CURRENT_TIMESTAMP
                WHERE user_id = ?
                RETURNING user_id
            """, [user_id]).fetchone()
            return result is not None

    async def deny_user(self, user_id: int, admin_id: int) -> Tuple[bool, bool]:
        """
        Deny user access. Increments denial count.
        Returns (success, is_frozen) tuple.
        """
        MAX_DENIAL_COUNT = 5

        async with self.connection() as conn:
            # Get current denial count
            row = conn.execute(
                "SELECT denial_count FROM users WHERE user_id = ?", [user_id]
            ).fetchone()

            if not row:
                return False, False

            new_count = (row[0] or 0) + 1
            is_frozen = new_count >= MAX_DENIAL_COUNT
            new_status = "frozen" if is_frozen else "denied"

            conn.execute("""
                UPDATE users
                SET status = ?, denial_count = ?,
                    reviewed_at = CURRENT_TIMESTAMP, reviewed_by = ?
                WHERE user_id = ?
            """, [new_status, new_count, admin_id, user_id])

            return True, is_frozen

    async def update_last_activity(self, user_id: int) -> None:
        """Update user's last activity timestamp."""
        async with self.connection() as conn:
            conn.execute("""
                UPDATE users
                SET last_activity = CURRENT_TIMESTAMP
                WHERE user_id = ? AND status = 'approved'
            """, [user_id])

    async def is_user_authorized(self, user_id: int) -> bool:
        """Check if user is authorized (approved status)."""
        async with self.connection() as conn:
            row = conn.execute(
                "SELECT status FROM users WHERE user_id = ?", [user_id]
            ).fetchone()

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
        can_view: bool,
        can_edit: bool,
        can_delete: bool,
        updated_by: int
    ) -> bool:
        """Set permission for a role/feature combination."""
        async with self.connection() as conn:
            conn.execute("""
                INSERT INTO role_permissions (role, feature, can_view, can_edit, can_delete, updated_at, updated_by)
                VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP, ?)
                ON CONFLICT (role, feature) DO UPDATE SET
                    can_view = excluded.can_view,
                    can_edit = excluded.can_edit,
                    can_delete = excluded.can_delete,
                    updated_at = excluded.updated_at,
                    updated_by = excluded.updated_by
            """, [role, feature, can_view, can_edit, can_delete, updated_by])
            return True

    async def seed_default_permissions(self) -> None:
        """Seed default permissions if table is empty."""
        async with self.connection() as conn:
            count = conn.execute("SELECT COUNT(*) FROM role_permissions").fetchone()[0]
            if count > 0:
                return  # Already seeded

            # Default permissions matrix
            defaults = [
                # Admin - full access
                ("admin", "dashboard", True, True, False),
                ("admin", "expenses", True, True, True),
                ("admin", "inventory", True, True, True),
                ("admin", "analytics", True, True, False),
                ("admin", "customers", True, True, False),
                ("admin", "reports", True, True, False),
                ("admin", "user_management", True, True, True),
                # Editor - view + edit most things
                ("editor", "dashboard", True, True, False),
                ("editor", "expenses", True, True, False),
                ("editor", "inventory", True, True, False),
                ("editor", "analytics", True, False, False),
                ("editor", "customers", True, False, False),
                ("editor", "reports", True, False, False),
                ("editor", "user_management", False, False, False),
                # Viewer - view only, no expenses
                ("viewer", "dashboard", True, False, False),
                ("viewer", "expenses", False, False, False),
                ("viewer", "inventory", True, False, False),
                ("viewer", "analytics", True, False, False),
                ("viewer", "customers", True, False, False),
                ("viewer", "reports", True, False, False),
                ("viewer", "user_management", False, False, False),
            ]

            for role, feature, can_view, can_edit, can_delete in defaults:
                conn.execute("""
                    INSERT INTO role_permissions (role, feature, can_view, can_edit, can_delete)
                    VALUES (?, ?, ?, ?, ?)
                    ON CONFLICT (role, feature) DO NOTHING
                """, [role, feature, can_view, can_edit, can_delete])

            logger.info("Default permissions seeded")


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
