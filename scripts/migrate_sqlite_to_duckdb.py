#!/usr/bin/env python3
"""
Copy the bot's approved users into DuckDB's `users`, which the dashboard reads.

One direction only, and only this table. The script used to carry
user_preferences, celebrated_milestones and report_history across as well —
that plan is dead, and 0028 dropped the DuckDB tables it targeted. The bot
cannot write analytics.duckdb at all: DuckDB allows one writer and the web
container holds it, which is why those three live in `data/bot.db` and are read
from there through core/bot_prefs.py.

`users` is different. It is the dashboard's own table, carrying roles the bot
knows nothing about, and this is how somebody the bot approved gets a seat in
the dashboard.

Usage:
    python scripts/migrate_sqlite_to_duckdb.py           # Copy users across
    python scripts/migrate_sqlite_to_duckdb.py --dry-run # Show what would change
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

    print(f"  Found {len(users)} users")

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

    # Summary
    print("\n" + "=" * 60)
    print("Migration Summary")
    print("=" * 60)
    print(f"  Users migrated: {users_migrated}")

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

    await close_store()


if __name__ == "__main__":
    asyncio.run(main())
