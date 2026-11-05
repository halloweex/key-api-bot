"""A real signed dashboard session, for the tests that call the app as a
visitor would: `_resolve_session` reads it as it reads any visitor's.

They lived in the stage 5 sweep's door module (`duckdb_off_sweep_doors.py`)
until step 14's S14-18 deleted the sweep with the last opener of the DuckDB
file; the two route tests that borrowed them import them from here.
"""
from __future__ import annotations

import time


def session_cookie(user_id: int, role: str) -> str:
    """`<cookie>=<value>`, as `/auth/telegram/callback` sets it."""
    from web.routes.auth import SESSION_COOKIE, create_session_data, session_serializer

    data = create_session_data({"id": str(user_id), "first_name": "Sweep",
                                "last_name": "", "username": "sweep",
                                "auth_date": str(int(time.time()))}, role=role)
    return f"{SESSION_COOKIE}={session_serializer.dumps(data)}"


def hardcoded_admin() -> int:
    from core.permissions import ADMIN_USER_IDS

    return sorted(ADMIN_USER_IDS)[0]
