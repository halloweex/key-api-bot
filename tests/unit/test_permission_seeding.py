"""`seed_default_permissions` against a real store.

The behaviour under test is the one that broke: seeding stopped at the first
existing row, so a feature added later never reached the table, and every role
whose permissions come from the database was denied it silently.
"""
from pathlib import Path

import pytest

from core.duckdb_store import DuckDBStore
from core.permissions import Feature, ROLE_PERMISSIONS, Role


async def _store(tmp_path: Path) -> DuckDBStore:
    store = DuckDBStore(db_path=tmp_path / "perms.duckdb")
    await store.connect()
    return store


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
        async with store.connection() as conn:
            conn.execute(
                "DELETE FROM role_permissions WHERE feature = ?", [Feature.SMS.value],
            )

        await store.seed_default_permissions()

        stored = await store.get_all_permissions()
        assert stored[Role.MARKETER.value][Feature.SMS.value] == {
            "view": True, "edit": True, "delete": False,
        }
        assert stored[Role.VIEWER.value][Feature.SMS.value]["view"] is False
    finally:
        await store.close()


@pytest.mark.asyncio
async def test_a_permission_turned_off_stays_off(tmp_path):
    """Re-seeding must not undo somebody's decision on the permissions page."""
    store = await _store(tmp_path)
    try:
        await store.seed_default_permissions()
        await store.set_permission(
            Role.MARKETER.value, Feature.SMS.value,
            can_view=False, can_edit=False, can_delete=False, updated_by=1,
        )

        await store.seed_default_permissions()

        stored = await store.get_role_permissions(Role.MARKETER.value)
        assert stored[Feature.SMS.value]["view"] is False
        assert stored[Feature.SMS.value]["edit"] is False
    finally:
        await store.close()


@pytest.mark.asyncio
async def test_marketer_is_a_settable_role(tmp_path):
    store = await _store(tmp_path)
    try:
        await store.create_user(
            user_id=4242, username="marketer", first_name="M", last_name=None,
            photo_url=None, status="approved",
        )
        assert await store.update_user_role(4242, Role.MARKETER.value, changed_by=1)
        assert (await store.get_user(4242))["role"] == Role.MARKETER.value

        with pytest.raises(ValueError):
            await store.update_user_role(4242, "sysadmin", changed_by=1)
    finally:
        await store.close()
