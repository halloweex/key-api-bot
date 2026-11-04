"""`seed_default_permissions` against a real store.

The behaviour under test is the one that broke: seeding stopped at the first
existing row, so a feature added later never reached the table, and every role
whose permissions come from the database was denied it silently.

The store is Postgres — `app.role_permissions` and `app.dashboard_users`,
where access has lived since 2026-09-07 and the only store the routers know
since step 14's S14-08 (these ran on DuckDB until then;
`tests/integration/pg_sms_seed.py`). Skipped without `KS_PG_DSN`.
"""
from pathlib import Path

import pytest
import pytest_asyncio

from core.duckdb_store import DuckDBStore
from core.permissions import Feature, ROLE_PERMISSIONS, Role
from tests.integration import pg_sms_seed
from tests.integration.pg_sms_seed import ACCESS_TABLES, needs_postgres, pg_conn

pytestmark = needs_postgres


@pytest_asyncio.fixture(autouse=True)
async def _postgres(monkeypatch):
    async with pg_sms_seed.on_postgres(monkeypatch, ACCESS_TABLES,
                                       what="the access path"):
        yield


async def _store(tmp_path: Path) -> DuckDBStore:
    return pg_sms_seed.store(tmp_path, "perms.duckdb")


@pytest.mark.asyncio
async def test_seeds_every_role_and_feature(tmp_path):
    store = await _store(tmp_path)
    try:
        await store.seed_default_permissions()
        stored = await store.get_all_permissions()

        assert set(stored) == {r.value for r in Role}
        for role, features in ROLE_PERMISSIONS.items():
            assert set(stored[role.value]) == {f.value for f in features}
    finally:
        await store.close()


@pytest.mark.asyncio
async def test_a_new_feature_reaches_an_already_seeded_table(tmp_path):
    """The regression: a populated table used to end the function outright."""
    store = await _store(tmp_path)
    try:
        await store.seed_default_permissions()
        async with pg_conn() as conn:
            await conn.execute(
                "DELETE FROM app.role_permissions WHERE feature = $1", Feature.SMS.value,
            )

        await store.seed_default_permissions()

        stored = await store.get_all_permissions()
        # Depth, not area: every level may look, and editing starts at editor.
        assert stored[Role.EDITOR.value][Feature.SMS.value] == {
            "view": True, "edit": True, "delete": False,
        }
        assert stored[Role.VIEWER.value][Feature.SMS.value] == {
            "view": True, "edit": False, "delete": False,
        }
    finally:
        await store.close()


@pytest.mark.asyncio
async def test_a_permission_turned_off_stays_off(tmp_path):
    """Re-seeding must not undo somebody's decision on the permissions page."""
    store = await _store(tmp_path)
    try:
        await store.seed_default_permissions()
        await store.set_permission(
            Role.EDITOR.value, Feature.SMS.value,
            can_view=False, can_edit=False, can_delete=False, updated_by=1,
        )

        await store.seed_default_permissions()

        stored = await store.get_role_permissions(Role.EDITOR.value)
        assert stored[Feature.SMS.value]["view"] is False
        assert stored[Feature.SMS.value]["edit"] is False
    finally:
        await store.close()


@pytest.mark.asyncio
async def test_only_levels_are_settable_roles(tmp_path):
    """`marketer` was settable here and is not a level — it was an area, and
    is a tab preset now. A row that still carried it is refused rather than
    quietly resolving to a depth nobody chose."""
    store = await _store(tmp_path)
    try:
        await store.create_user(
            user_id=4242, username="somebody", first_name="S", last_name=None,
            photo_url=None, status="approved",
        )
        assert await store.update_user_role(4242, Role.EDITOR.value, changed_by=1)
        assert (await store.get_user(4242))["role"] == Role.EDITOR.value

        for not_a_level in ("sysadmin", "marketer"):
            with pytest.raises(ValueError):
                await store.update_user_role(4242, not_a_level, changed_by=1)
    finally:
        await store.close()
