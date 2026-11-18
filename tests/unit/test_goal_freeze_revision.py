"""Revision 0037, the month's automatic goal frozen on its 1st: one table, and
a downgrade that keeps it.

Read by replaying the revision against a recorder (`tests/migration_replay.py`),
so what is asserted is the SQL Alembic would hand the server. The server's
half — the columns and the key as the catalogue reports them, applied twice,
the downgrade on a real database, `ks_readonly`'s rights — is
`tests/integration/test_goal_freeze_pg.py`. Each test names the mutation it
kills.
"""
from __future__ import annotations

import re

from tests import migration_replay as replay

REV_ID = "0037_frozen_month_goals"
REV = replay.VERSIONS / f"{REV_ID}.py"
TABLE = "app.frozen_month_goals"

_CREATED = re.compile(r"CREATE\s+TABLE\s+(IF\s+NOT\s+EXISTS\s+)?([\w.]+)", re.I)


def _module():
    return replay.load(REV)


def _up():
    return replay.statements(_module(), "upgrade")


class TestTheRevision:
    def test_it_follows_0036_and_is_in_the_chain_the_code_requires(self):
        """Kills: 0037 hung off anything but the analysts' captures, or dropped
        from the line the code requires. It was the head until 0038 (the
        discount columns) descended from it; `test_pg_plumbing` owns the head
        pin."""
        from core import pg

        rev = _module()
        assert rev.revision == REV_ID
        assert rev.down_revision == "0036_analytics_captures"
        assert REV_ID in [m.revision for m in replay.lineage(pg.REQUIRED_REVISION)]

    def test_it_creates_exactly_the_one_table(self):
        """Kills: a table left out or added, one created outside `app`, or an
        index beside the primary key, which is the reader's whole access path."""
        sql = re.sub(r"--[^\n]*", "", "\n".join(_up()))
        assert {m.group(2) for m in _CREATED.finditer(sql)} == {TABLE}
        assert _module().TABLES == (TABLE,)
        assert not re.search(r"\bCREATE\s+(UNIQUE\s+)?INDEX\b", sql, re.I)

    def test_every_create_is_idempotent(self):
        """IF NOT EXISTS, so the upgrade after a keep-the-table downgrade is a
        no-op. Kills: the clause dropped — the re-upgrade fails on the table."""
        for m in _CREATED.finditer("\n".join(_up())):
            assert m.group(1), m.group(0)

    def test_it_writes_nothing_but_ddl_and_one_comment(self):
        """No schema (rule 11), no grant (the default privileges in
        `postgres/initdb/30-app.sql` give ks_readonly SELECT), no data, no
        change to an existing table. Read without the SQL's own `--` comments
        and the table comment's text, which speak of the writer
        ("insert-only")."""
        sql = re.sub(r"--[^\n]*", "", "\n".join(_up()))
        sql = re.sub(r"'(?:[^']|'')*'", "''", sql).upper()
        for word in ("CREATE SCHEMA", "GRANT", "REVOKE", "INSERT", "UPDATE",
                     "DELETE", "ALTER", "DROP", "TRUNCATE"):
            assert not re.search(rf"\b{word}\b", sql), word
        state = replay.apply({}, _up())
        assert set(state) == {TABLE}
        assert state[TABLE].startswith("Each month's automatic goal per sales type")

    def test_the_replay_sees_the_table_at_the_head_and_not_before(self):
        at_head = replay.comments_at(REV_ID)
        at_0036 = replay.comments_at("0036_analytics_captures")
        assert TABLE in at_head
        assert TABLE not in at_0036
        assert set(at_head) - set(at_0036) == {TABLE}


class TestTheDowngradeKeepsTheTable:
    def test_it_executes_nothing(self):
        """A frozen goal cannot be recomputed — a month computed later is
        another number — so the downgrade moves the stamp and nothing else.
        Kills: any statement in the downgrade, a DROP above all."""
        assert replay.statements(_module(), "downgrade") == []

    def test_after_the_downgrade_the_replay_still_holds_it(self):
        rev = _module()
        up = replay.apply(replay.comments_at("0036_analytics_captures"), _up())
        down = replay.apply(up, replay.statements(rev, "downgrade"))
        assert TABLE in down
        assert down == up
