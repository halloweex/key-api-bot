"""A permission toggle writes the column it toggled, not the whole cell.

The page sent all three flags from its cached copy of the matrix, so a click
made from a snapshot up to a minute old — or before the previous click's
refetch had landed — put the other two columns back to what the snapshot
held: another admin's grant undone, or the caller's own previous toggle.

The matrix is `app.role_permissions`, the only store the router knows since
step 14's S14-08 (these ran on DuckDB until then;
`tests/integration/pg_sms_seed.py`); skipped without `KS_PG_DSN`.
"""
import inspect
import re
from pathlib import Path

import pytest
import pytest_asyncio

from tests.integration import pg_sms_seed
from tests.integration.pg_sms_seed import ACCESS_TABLES, as_tuple, pg_conn

REPO = Path(__file__).resolve().parents[2]


@pytest_asyncio.fixture(autouse=True)
async def _postgres(request, monkeypatch):
    async with pg_sms_seed.for_async_tests(request, monkeypatch, ACCESS_TABLES,
                                           what="the access path"):
        yield


async def _cell(store, role, feature):
    async with pg_conn() as conn:
        return as_tuple(await conn.fetchrow(
            "SELECT can_view, can_edit, can_delete FROM app.role_permissions "
            "WHERE role = $1 AND feature = $2", role, feature,
        ))


@pytest.mark.asyncio
async def test_a_flag_left_none_keeps_its_stored_value(tmp_path):
    store = pg_sms_seed.store(tmp_path, "perm.duckdb")
    try:
        await store.set_permission("editor", "expenses", True, False, False, 1)
        # Admin B grants edit; admin A, from a stale copy, grants delete.
        await store.set_permission("editor", "expenses", None, True, None, 2)
        await store.set_permission("editor", "expenses", None, None, True, 1)
        assert await _cell(store, "editor", "expenses") == (True, True, True)
        # Revoking one column leaves the others alone.
        await store.set_permission("editor", "expenses", None, False, None, 2)
        assert await _cell(store, "editor", "expenses") == (True, False, True)
    finally:
        await store.close()


@pytest.mark.asyncio
async def test_a_new_pair_defaults_the_flags_it_was_not_given(tmp_path):
    store = pg_sms_seed.store(tmp_path, "perm.duckdb")
    try:
        async with pg_conn() as conn:
            await conn.execute("DELETE FROM app.role_permissions"
                               " WHERE role = 'marketer' AND feature = 'traffic'")
        await store.set_permission("marketer", "traffic", True, None, None, 1)
        assert await _cell(store, "marketer", "traffic") == (True, False, False)
    finally:
        await store.close()


def test_the_route_accepts_a_partial_and_refuses_an_empty_one():
    from web.routes.api import users

    src = inspect.getsource(users.update_permission)
    assert "Optional[bool] = Query(None)" in src
    assert "Nothing to change" in src


def test_the_page_sends_only_what_the_click_changed():
    page = (REPO / "web/frontend/src/components/AdminPermissionsPage.tsx").read_text()
    client = (REPO / "web/frontend/src/api/client.ts").read_text()
    assert "updatedPerms[key] !== currentPerms[key]" in page
    assert re.search(r"return refetched", page), "the invalidate promise must be awaited"
    assert "can_view=${canView}" not in client, "the client no longer sends all three"


def test_boot_no_longer_demotes_admins():
    """The repair that undid every promotion, and the function that carried it.

    A "one-time repair" (070af9a) set `role='viewer'` on every boot for any
    admin outside `ADMIN_USER_IDS`, so a promotion made through the admin page
    lasted until the next restart. The repair went first; the function that
    held it — `_migrate_sqlite_users_to_duckdb` — went on 2026-09-07, once it
    was measured to be a no-op (the frozen `data/bot.db` and the DuckDB table
    held the same 24 people) and once Postgres became the writer it would have
    been writing behind.

    So the assertion is now about the whole boot path rather than one
    function: nothing there may write a role at all.
    """
    from web import main

    src = inspect.getsource(main)
    assert "_migrate_sqlite_users_to_duckdb" not in src.replace(
        "# `_migrate_sqlite_users_to_duckdb` lived here until 2026-09-07.", "",
    ), "the boot-time user migration is back"
    assert "SET role" not in src, "startup writes a role"
    assert "role=role" not in src, "startup creates users with a role"
