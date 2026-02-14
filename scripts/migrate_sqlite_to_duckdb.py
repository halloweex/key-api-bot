#!/usr/bin/env python3
"""
Migrate user data from SQLite (bot.db) to DuckDB (analytics.duckdb).

Usage:
    python scripts/migrate_sqlite_to_duckdb.py           # Migrate all data
    python scripts/migrate_sqlite_to_duckdb.py --dry-run # Show what would be migrated
"""
import argparse
import asyncio
import sqlite3
import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from core.duckdb_store import get_store, close_store

# Paths
SQLITE_PATH = Path(__file__).parent.parent / "data" / "bot.db"

# Admin user IDs (will be set to admin role)
ADMIN_USER_IDS = {183618567, 129462784}


def get_sqlite_connection():
    """Get SQLite connection."""
    if not SQLITE_PATH.exists():
        print(f"SQLite database not found: {SQLITE_PATH}")
        return None
    conn = sqlite3.connect(str(SQLITE_PATH))
    conn.row_factory = sqlite3.Row
    return conn


def read_sqlite_users(conn) -> list:
    """Read all users from SQLite."""
    cursor = conn.cursor()
    cursor.execute("""
        SELECT user_id, username, first_name, last_name, status,
               requested_at, reviewed_at, reviewed_by, last_activity, denial_count
        FROM authorized_users
    """)
    return [dict(row) for row in cursor.fetchall()]


def read_sqlite_preferences(conn) -> list:
    """Read all user preferences from SQLite."""
    cursor = conn.cursor()
    cursor.execute("""
        SELECT user_id, default_source, default_report_type, timezone,
               default_date_range, notifications_enabled, created_at, updated_at
        FROM user_preferences
    """)
    return [dict(row) for row in cursor.fetchall()]


def read_sqlite_milestones(conn) -> list:
    """Read all celebrated milestones from SQLite."""
    cursor = conn.cursor()
    cursor.execute("""
        SELECT period_type, period_key, milestone_amount, revenue, celebrated_at
        FROM celebrated_milestones
    """)
    return [dict(row) for row in cursor.fetchall()]


def read_sqlite_report_history(conn) -> list:
    """Read report history from SQLite."""
    cursor = conn.cursor()
    cursor.execute("""
        SELECT user_id, report_type, start_date, end_date, source, created_at
        FROM report_history
        ORDER BY created_at DESC
        LIMIT 1000
    """)
    return [dict(row) for row in cursor.fetchall()]


async def migrate_users(store, users: list, dry_run: bool = False) -> int:
    """Migrate users to DuckDB."""
    count = 0
    for user in users:
        user_id = user['user_id']

        # Determine role
        role = 'admin' if user_id in ADMIN_USER_IDS else 'viewer'

        if dry_run:
            print(f"  Would migrate user {user_id} (@{user.get('username', 'N/A')}) "
                  f"status={user.get('status')} role={role}")
        else:
            async with store.connection() as conn:
                conn.execute("""
                    INSERT INTO users (user_id, username, first_name, last_name,
                                       role, status, requested_at, reviewed_at,
                                       reviewed_by, last_activity, denial_count)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT (user_id) DO UPDATE SET
                        username = excluded.username,
                        first_name = excluded.first_name,
                        last_name = excluded.last_name,
                        role = excluded.role,
                        status = excluded.status
                """, [
                    user_id,
                    user.get('username'),
                    user.get('first_name'),
                    user.get('last_name'),
                    role,
                    user.get('status', 'pending'),
                    user.get('requested_at'),
                    user.get('reviewed_at'),
                    user.get('reviewed_by'),
                    user.get('last_activity'),
                    user.get('denial_count', 0),
                ])
        count += 1

    return count


async def migrate_preferences(store, preferences: list, dry_run: bool = False) -> int:
    """Migrate user preferences to DuckDB."""
    count = 0
    for pref in preferences:
        if dry_run:
            print(f"  Would migrate preferences for user {pref['user_id']}")
        else:
            async with store.connection() as conn:
                conn.execute("""
                    INSERT INTO user_preferences (user_id, default_source, default_report_type,
                                                  timezone, default_date_range, notifications_enabled,
                                                  created_at, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT (user_id) DO UPDATE SET
                        default_source = excluded.default_source,
                        default_report_type = excluded.default_report_type,
                        timezone = excluded.timezone,
                        default_date_range = excluded.default_date_range,
                        notifications_enabled = excluded.notifications_enabled,
                        updated_at = excluded.updated_at
                """, [
                    pref['user_id'],
                    pref.get('default_source'),
                    pref.get('default_report_type', 'summary'),
                    pref.get('timezone', 'Europe/Kyiv'),
                    pref.get('default_date_range', 'week'),
                    bool(pref.get('notifications_enabled', True)),
                    pref.get('created_at'),
                    pref.get('updated_at'),
                ])
        count += 1

    return count


async def migrate_milestones(store, milestones: list, dry_run: bool = False) -> int:
    """Migrate celebrated milestones to DuckDB."""
    count = 0
    for m in milestones:
        if dry_run:
            print(f"  Would migrate milestone: {m['period_type']}/{m['period_key']} "
                  f"amount={m['milestone_amount']}")
        else:
            async with store.connection() as conn:
                try:
                    conn.execute("""
                        INSERT INTO celebrated_milestones (period_type, period_key,
                                                           milestone_amount, revenue, celebrated_at)
                        VALUES (?, ?, ?, ?, ?)
                    """, [
                        m['period_type'],
                        m['period_key'],
                        m['milestone_amount'],
                        m['revenue'],
                        m.get('celebrated_at'),
                    ])
                    count += 1
                except Exception:
                    # Duplicate - already exists
                    pass

    return count


async def migrate_report_history(store, history: list, dry_run: bool = False) -> int:
    """Migrate report history to DuckDB."""
    count = 0
    for h in history:
        if dry_run:
            print(f"  Would migrate report history for user {h['user_id']}: "
                  f"{h['report_type']} {h['start_date']}-{h['end_date']}")
        else:
            async with store.connection() as conn:
                conn.execute("""
                    INSERT INTO report_history (user_id, report_type, start_date,
                                                end_date, source, created_at)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, [
                    h['user_id'],
                    h['report_type'],
                    h['start_date'],
                    h['end_date'],
                    h.get('source'),
                    h.get('created_at'),
                ])
        count += 1

    return count


async def main():
    parser = argparse.ArgumentParser(description="Migrate SQLite to DuckDB")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be migrated")
    args = parser.parse_args()

    print("=" * 60)
    print("SQLite to DuckDB Migration")
    print("=" * 60)

    if args.dry_run:
        print("\n*** DRY RUN - No changes will be made ***\n")

    # Connect to SQLite
    sqlite_conn = get_sqlite_connection()
    if not sqlite_conn:
        print("No SQLite database found - nothing to migrate")
        return

    # Read data from SQLite
    print("\nReading from SQLite...")
    users = read_sqlite_users(sqlite_conn)
    preferences = read_sqlite_preferences(sqlite_conn)
    milestones = read_sqlite_milestones(sqlite_conn)
    history = read_sqlite_report_history(sqlite_conn)

    print(f"  Found {len(users)} users")
    print(f"  Found {len(preferences)} user preferences")
    print(f"  Found {len(milestones)} celebrated milestones")
    print(f"  Found {len(history)} report history entries")

    sqlite_conn.close()

    # Connect to DuckDB
    print("\nConnecting to DuckDB...")
    try:
        store = await get_store()
    except Exception as e:
        if "lock" in str(e).lower():
            print("\n" + "=" * 60)
            print("ERROR: DuckDB database is locked by another process!")
            print("=" * 60)
            print("\nThe web server is likely running and holding the database lock.")
            print("Please stop it first:")
            print()
            print("  # If running locally:")
            print("  pkill -f 'uvicorn web.main'")
            print()
            print("  # If running in Docker:")
            print("  docker-compose stop web")
            print()
            print("Then re-run this migration script.")
            return
        raise

    # Migrate data
    print("\nMigrating users...")
    users_migrated = await migrate_users(store, users, args.dry_run)

    print("\nMigrating preferences...")
    prefs_migrated = await migrate_preferences(store, preferences, args.dry_run)

    print("\nMigrating milestones...")
    milestones_migrated = await migrate_milestones(store, milestones, args.dry_run)

    print("\nMigrating report history...")
    history_migrated = await migrate_report_history(store, history, args.dry_run)

    # Summary
    print("\n" + "=" * 60)
    print("Migration Summary")
    print("=" * 60)
    print(f"  Users migrated: {users_migrated}")
    print(f"  Preferences migrated: {prefs_migrated}")
    print(f"  Milestones migrated: {milestones_migrated}")
    print(f"  Report history migrated: {history_migrated}")

    # Admin users
    admin_users = [u for u in users if u['user_id'] in ADMIN_USER_IDS]
    print(f"\n  Admin users set: {len(admin_users)}")
    for u in admin_users:
        print(f"    - {u['user_id']} (@{u.get('username', 'N/A')})")

    if args.dry_run:
        print("\n*** DRY RUN complete - no changes made ***")
    else:
        print("\nMigration complete!")
        print("\nNext steps:")
        print("  1. Verify data: SELECT * FROM users LIMIT 10;")
        print("  2. Test login with admin user")
        print("  3. Test /api/me endpoint")

    await close_store()


if __name__ == "__main__":
    asyncio.run(main())
