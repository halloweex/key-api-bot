"""Who may open the dashboard, and which store is asked.

WHAT THIS IS PROTECTING

`_resolve_session` re-reads status and role on **every** request, and that read
is what revocation actually is here: the session cookie is a stateless signed
token that cannot be withdrawn, so taking access away means the next request
finds the account no longer approved. Until 2026-09-07 the read went to DuckDB,
which meant every authenticated request to every tab acquired the process-wide
store lock — the same lock a warehouse rebuild holds for seconds at a time.

Moving it is therefore worth doing and dangerous to get wrong, and the two
failure directions are not symmetric: reading the wrong store could lock
everyone out (loud, recoverable) or hand somebody access they no longer have
(silent, not). The tests below lean on the second.

WHY THE OBVIOUS SHORTCUT WAS NOT TAKEN

`app.authorized_users` already exists in Postgres and holds users. It is the
*bot's* list, it has no `role` column, and on the day of the move it disagreed
with the dashboard's list about **twelve of sixteen** approved people —
`revoke_user` in the bot store is implemented as `deny`, and taking somebody's
bot access away was never a statement about their dashboard access. Pointing
the session at it would have locked twelve people out, one of them an owner who
survives only because their id is hardcoded. Hence a second table, and hence
`TestTheTwoListsAreNotConfusedForEachOther`.
"""
from __future__ import annotations

import asyncio
from pathlib import Path

import pytest

from tests.helpers.access_lists import (
    access_lists_empty, bot_store_on_postgres, dashboard_row, execute,
)
from tests.helpers.app_client import signed_in
from tests.integration.pg_sms_seed import needs_postgres

REPO = Path(__file__).resolve().parents[2]
MIGRATION = REPO / "migrations" / "versions" / "0016_dashboard_users.py"


# ─── the switch ──────────────────────────────────────────────────────────────


# ─── the copy, and when it must not run ──────────────────────────────────────


# `TestTheReplicationStandsDown` held `replicate_dashboard_users` to nothing
# under the switch and without a DSN, until step 14's S14-13 deleted it.


# ─── the routed bodies ───────────────────────────────────────────────────────


@needs_postgres
class TestRefusalsAtOnceAreAllCounted:
    """`deny_user` counts a refusal, and the fifth freezes the person. It was
    a SELECT and then an UPDATE, safe only because DuckDB's lock made them one
    acquisition; on a pool, two admins refusing the same person at once would
    read the same count and lose a refusal on the way to the freeze."""

    PERSON, ADMIN = 7_000_000_701, 7_000_000_702

    @pytest.mark.asyncio
    async def test_concurrent_refusals_reach_the_freeze(self, monkeypatch):
        from core.repositories.users import MAX_DENIAL_COUNT
        from core.store import AnalyticsStore

        at_once = 3
        async with access_lists_empty(monkeypatch):
            await execute(
                "INSERT INTO app.dashboard_users (user_id, role, status, denial_count) "
                "VALUES ($1, 'viewer', 'denied', $2)",
                self.PERSON, MAX_DENIAL_COUNT - at_once)
            store = AnalyticsStore()
            answers = await asyncio.gather(
                *(store.deny_user(self.PERSON, self.ADMIN) for _ in range(at_once)))
            row = await dashboard_row(self.PERSON)

        assert (row["denial_count"], row["status"]) == (MAX_DENIAL_COUNT, "frozen")
        # Each refusal was found, and exactly one of them froze the person.
        assert sorted(answers) == [(True, False)] * (at_once - 1) + [(True, True)]


@needs_postgres
class TestTheTwoListsAreNotConfusedForEachOther:
    """Twelve of sixteen approved dashboard users were `denied` in
    `app.authorized_users` on the day the session moved off DuckDB. Reading
    the bot's list there would have locked them out, and the one it would not
    have locked out is an owner whose id is hardcoded.

    Over the test Postgres, with the bot's store there as production runs it:
    each person is approved in one list and denied in the other, and the
    session follows the dashboard's."""

    APPROVED_HERE, DENIED_HERE = 7_000_000_601, 7_000_000_602

    @pytest.mark.asyncio
    async def test_the_session_follows_the_dashboards_list(self, monkeypatch, tmp_path):
        async with bot_store_on_postgres(monkeypatch, tmp_path):
            await execute(
                "INSERT INTO app.authorized_users (user_id, status) "
                "VALUES ($1, 'denied'), ($2, 'approved')",
                self.APPROVED_HERE, self.DENIED_HERE)
            async with signed_in("viewer", user_id=self.APPROVED_HERE) as visitor:
                approved_here = await visitor.get("/api/me")
            async with signed_in("viewer", user_id=self.DENIED_HERE) as visitor:
                await execute("UPDATE app.dashboard_users SET status = 'denied' "
                              "WHERE user_id = $1", self.DENIED_HERE)
                denied_here = await visitor.get("/api/me")

        assert approved_here.status_code == 200
        assert approved_here.json()["user"]["id"] == self.APPROVED_HERE
        assert denied_here.status_code == 401


# ─── the matrix followed the list ────────────────────────────────────────────


class TestTheRoleMatrixLeftDuckDB:
    """`KS_USER_STORE` decides where *all* of access lives, not half of it.

    Revision 0016 moved who may open the dashboard and left the matrix saying
    what each role may do behind, cached per role and read out of DuckDB. Per
    user tabs made that untenable: a permission dependency sits on roughly 120
    of the 140 endpoints now, so every cache miss took DuckDB's process-wide
    store lock on an authorisation path — the lock a warehouse rebuild holds
    every two minutes.
    """

    @pytest.mark.asyncio
    async def test_seeding_is_one_statement(self, tmp_path, monkeypatch):
        """It runs on the request path of the first authorization check after
        every restart, and there is a pair per (level, feature).

        A statement per row meant 48 acquisitions of DuckDB's store lock where
        the single-connection version it replaced took one — and under Postgres
        48 × (require_revision + insert). Measured with two concurrent callers
        on a cold table before this was fixed: 93 statements for 48 rows. On
        Postgres, the matrix's only store since step 14's S14-08 (this ran on
        DuckDB until then); skipped without `KS_PG_DSN`.
        """
        from tests.integration import pg_sms_seed

        if not pg_sms_seed.DSN:
            pytest.skip("needs a live PostgreSQL at KS_PG_DSN")
        async with pg_sms_seed.on_postgres(
                monkeypatch, pg_sms_seed.ACCESS_TABLES, what="the access path"):
            store = pg_sms_seed.store(tmp_path, "seed.duckdb")
            calls = []
            original = store._perms_run

            async def counting(sql, params=None, *, mode="all"):
                calls.append(sql)
                return await original(sql, params, mode=mode)

            store._perms_run = counting
            await store.seed_default_permissions()

            inserts = [sql for sql in calls if "INSERT" in sql.upper()]
            assert len(inserts) == 1, f"{len(inserts)} inserts for one seed"

            from core.permissions import ROLE_PERMISSIONS

            matrix = await store.get_all_permissions()
            pairs = sum(len(features) for features in matrix.values())
            expected = sum(len(f) for f in ROLE_PERMISSIONS.values())
            assert pairs == expected, (pairs, expected)

            # And it stays idempotent, which is what lets every read path seed.
            calls.clear()
            await store.seed_default_permissions()
            assert not [sql for sql in calls if "INSERT" in sql.upper()]

    def test_the_migration_and_the_code_agree_on_the_table(self):
        migration = (MIGRATION.parent / "0022_role_permissions.py").read_text(
            encoding="utf-8")
        from core.repositories.users import PERMISSIONS_TABLE

        assert PERMISSIONS_TABLE == "app.role_permissions"
        assert "CREATE TABLE IF NOT EXISTS app.role_permissions" in migration
        for column in ("role", "feature", "can_view", "can_edit", "can_delete",
                       "updated_at", "updated_by"):
            assert column in migration, column
