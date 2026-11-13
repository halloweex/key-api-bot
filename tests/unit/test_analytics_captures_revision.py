"""Revision 0036, the analysts' captures: eleven tables, and a downgrade that
keeps them (owner decision D2).

Read by replaying the revision against a recorder (`tests/migration_replay.py`),
so what is asserted is the SQL Alembic would hand the server. The server's
half — applied twice, the downgrade on a real database — is
`tests/integration/test_analytics_captures_pg.py`. Each test names the
mutation it kills.
"""
from __future__ import annotations

import re

from tests import migration_replay as replay

REV_ID = "0036_analytics_captures"
REV = replay.VERSIONS / f"{REV_ID}.py"

# The design's §3.6, as the owner approved it on 2026-11-13. Frozen, like the
# revision: once applied it is never re-run, so this does not follow the code.
TABLES = {
    "app.analytics_buyer_key", "app.analytics_person_key",
    "app.analytics_phone_excluded", "app.analytics_person_merges",
    "app.offer_cost_daily", "app.catalogue_versions",
    "app.revenue_prediction_vintages", "app.revenue_goal_history",
    "app.order_channel_verdicts", "app.expense_deletions", "app.ga4_funnel",
}
INDEXES = {"idx_revenue_prediction_vintages", "idx_order_channel_verdicts_order"}

_CREATED = re.compile(r"CREATE\s+TABLE\s+(IF\s+NOT\s+EXISTS\s+)?([\w.]+)", re.I)
_INDEX = re.compile(r"CREATE\s+INDEX\s+(IF\s+NOT\s+EXISTS\s+)?(\w+)", re.I)


def _module():
    return replay.load(REV)


def _up():
    return replay.statements(_module(), "upgrade")


class TestTheRevision:
    def test_it_follows_0035_and_is_the_head_the_code_requires(self):
        """Kills: 0036 hung off anything but the batch-E lock, or the pin left
        at 0035 — web would refuse a database at 0036."""
        from core import pg

        rev = _module()
        assert rev.revision == REV_ID
        assert rev.down_revision == "0035_batch_e_chains"
        assert pg.REQUIRED_REVISION == REV_ID

    def test_it_creates_exactly_the_eleven_tables_and_two_indexes(self):
        """Kills: a table left out or added, or one created outside `app`."""
        sql = "\n".join(_up())
        assert {m.group(2) for m in _CREATED.finditer(sql)} == TABLES
        assert {m.group(2) for m in _INDEX.finditer(sql)} == INDEXES
        assert set(_module().TABLES) == TABLES

    def test_every_statement_is_idempotent(self):
        """Every CREATE carries IF NOT EXISTS, so the upgrade after a
        keep-the-tables downgrade is a no-op. Kills: an IF NOT EXISTS dropped,
        which would fail the re-upgrade on the first table that is there."""
        sql = "\n".join(_up())
        for pattern in (_CREATED, _INDEX):
            for m in pattern.finditer(sql):
                assert m.group(1), m.group(0)

    def test_it_writes_nothing_but_ddl_and_one_comment(self):
        """No schema (rule 11), no grant (the default privileges in
        `postgres/initdb/30-app.sql` already give ks_readonly SELECT), no
        data, no change to an existing table. The SQL's own `--` comments say
        DELETE and INSERT about the writers, so they are read without them."""
        sql = re.sub(r"--[^\n]*", "", "\n".join(_up())).upper()
        for word in ("CREATE SCHEMA", "GRANT", "REVOKE", "INSERT", "UPDATE",
                     "DELETE", "ALTER", "DROP", "TRUNCATE"):
            assert not re.search(rf"\b{word}\b", sql), word
        state = replay.apply({}, _up())
        assert {t for t, text in state.items() if text is not None} == {
            "app.analytics_buyer_key"}

    def test_the_replay_sees_the_tables_at_the_head(self):
        """What the other revision tests read: every table appears at 0036
        and none at 0035."""
        at_head = replay.comments_at(REV_ID)
        at_0035 = replay.comments_at("0035_batch_e_chains")
        assert TABLES <= set(at_head)
        assert not TABLES & set(at_0035)
        assert at_head["app.analytics_buyer_key"].startswith("buyer_id -> random 63-bit key")


class TestTheDowngradeKeepsTheTables:
    def test_it_executes_nothing(self):
        """Owner decision D2: until Z3 `rollback-step14` requires 0035 by the
        stamp alone, so the downgrade moves the stamp and nothing else.
        Kills: any statement in the downgrade — a DROP would destroy the only
        record of seven histories."""
        assert replay.statements(_module(), "downgrade") == []

    def test_after_the_downgrade_the_replay_still_holds_them(self):
        """Up, then down, from 0035's state: the eleven tables are still
        there. Kills: a DROP TABLE, which the replay follows."""
        rev = _module()
        up = replay.apply(replay.comments_at("0035_batch_e_chains"), _up())
        down = replay.apply(up, replay.statements(rev, "downgrade"))
        assert TABLES <= set(down)
        assert down == up
