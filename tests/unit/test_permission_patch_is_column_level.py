"""A permission toggle writes the column it toggled, not the whole cell.

The page sent all three flags from its cached copy of the matrix, so a click
made from a snapshot up to a minute old — or before the previous click's
refetch had landed — put the other two columns back to what the snapshot
held: another admin's grant undone, or the caller's own previous toggle.

The matrix is `app.role_permissions`, the only store the router knows since
step 14's S14-08 (these ran on DuckDB until then;
`tests/integration/pg_sms_seed.py`); skipped without `KS_PG_DSN`.
"""
import pytest
import pytest_asyncio

from tests.integration import pg_sms_seed
from tests.helpers.app_client import signed_in
from tests.integration.pg_sms_seed import ACCESS_TABLES, as_tuple, pg_conn

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
    await store.set_permission("editor", "expenses", True, False, False, 1)
    # Admin B grants edit; admin A, from a stale copy, grants delete.
    await store.set_permission("editor", "expenses", None, True, None, 2)
    await store.set_permission("editor", "expenses", None, None, True, 1)
    assert await _cell(store, "editor", "expenses") == (True, True, True)
    # Revoking one column leaves the others alone.
    await store.set_permission("editor", "expenses", None, False, None, 2)
    assert await _cell(store, "editor", "expenses") == (True, False, True)


@pytest.mark.asyncio
async def test_a_new_pair_defaults_the_flags_it_was_not_given(tmp_path):
    store = pg_sms_seed.store(tmp_path, "perm.duckdb")
    async with pg_conn() as conn:
        await conn.execute("DELETE FROM app.role_permissions"
                           " WHERE role = 'marketer' AND feature = 'traffic'")
    await store.set_permission("marketer", "traffic", True, None, None, 1)
    assert await _cell(store, "marketer", "traffic") == (True, False, False)


@pytest.mark.asyncio
async def test_the_route_writes_only_the_flag_it_was_given():
    """`PATCH /api/admin/permissions` with one flag leaves the cell's other two
    columns as they were; with none it is a 400 and writes nothing."""
    async with pg_conn() as conn:
        await conn.execute(
            "INSERT INTO app.role_permissions (role, feature, can_view, can_edit, can_delete) "
            "VALUES ('editor', 'expenses', true, false, true)")
    cell = {"role": "editor", "feature": "expenses"}
    async with signed_in("admin") as admin:
        one = await admin.patch("/api/admin/permissions", params={**cell, "can_edit": "true"})
        assert one.status_code == 200
        assert await _cell(None, "editor", "expenses") == (True, True, True)

        none = await admin.patch("/api/admin/permissions", params=cell)
        assert none.status_code == 400
        assert await _cell(None, "editor", "expenses") == (True, True, True)


@pytest.mark.asyncio
async def test_a_restart_keeps_a_promotion(monkeypatch, tmp_path):
    """The repair that undid every promotion (070af9a) ran on every boot: it
    set `role='viewer'` for any admin outside `ADMIN_USER_IDS`, so a promotion
    made through the admin page lasted until the next restart.

    web's real `startup_event` over the test Postgres, with only the work that
    reaches outside the process stubbed — the boot sync (KeyCRM), the
    scheduler, the event subscriptions (process-wide) and the forecast model —
    must leave the dashboard's list exactly as it found it."""
    from types import SimpleNamespace
    from unittest.mock import AsyncMock

    import web.main as main
    from bot import store_sqlite
    from core.bot_store import use_bot_store
    from core.permissions import ADMIN_USER_IDS
    from core.store import close_store

    promoted, editor = 7_000_000_801, 7_000_000_802
    assert promoted not in ADMIN_USER_IDS

    async def _users():
        async with pg_conn() as conn:
            return [dict(r) for r in await conn.fetch(
                "SELECT * FROM app.dashboard_users ORDER BY user_id")]

    async with pg_conn() as conn:
        await conn.execute(
            "INSERT INTO app.dashboard_users (user_id, first_name, role, status) "
            "VALUES ($1, 'Promoted', 'admin', 'approved'), ($2, 'Editor', 'editor', 'approved')",
            promoted, editor)
    before = await _users()

    # The bot's local store, as a start initialises it, in tmp_path.
    monkeypatch.delenv("KS_BOT_STORE", raising=False)
    monkeypatch.setattr(store_sqlite, "DB_PATH", tmp_path / "bot.db")
    monkeypatch.setattr(main, "init_and_sync", AsyncMock())
    monkeypatch.setattr(main, "start_scheduler", AsyncMock())
    monkeypatch.setattr(main, "_register_event_handlers", lambda: None)
    monkeypatch.setattr("core.prediction_service.get_prediction_service",
                        lambda: SimpleNamespace(is_ready=True))
    previous = use_bot_store(None)
    try:
        await main.startup_event()
    finally:
        use_bot_store(previous)
        await close_store()

    assert await _users() == before
