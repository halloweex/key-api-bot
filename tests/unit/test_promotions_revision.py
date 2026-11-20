"""Revision 0039, the promotions calendar: three tables and a sequence in
`app`, and a downgrade that keeps them.

Read by replaying the revision against a recorder (`tests/migration_replay.py`),
so what is asserted is the SQL Alembic would hand the server. The server's
half — the columns and the CHECKs as the catalogue reports them, applied
twice, the downgrade on a real database, `ks_readonly`'s rights — is
`tests/integration/test_promotions_pg.py`. Each test names the mutation it
kills.
"""
from __future__ import annotations

import re

from tests import migration_replay as replay

REV_ID = "0039_promotions"
REV = replay.VERSIONS / f"{REV_ID}.py"
TABLES = {"app.promotion_versions", "app.promotion_checkins", "app.promotion_asks"}

_CREATED = re.compile(r"CREATE\s+TABLE\s+(IF\s+NOT\s+EXISTS\s+)?([\w.]+)", re.I)
_SEQUENCE = re.compile(r"CREATE\s+SEQUENCE\s+(IF\s+NOT\s+EXISTS\s+)?([\w.]+)", re.I)


def _module():
    return replay.load(REV)


def _up():
    return replay.statements(_module(), "upgrade")


def _sql_without_comments() -> str:
    return re.sub(r"--[^\n]*", "", "\n".join(_up()))


class TestTheRevision:
    def test_it_follows_0038_and_is_the_head_the_code_requires(self):
        """Kills: 0039 hung off anything but the discount columns, or the pin
        left at 0038 — web would read tables a database at 0038 does not
        have. `test_pg_plumbing` holds the pin to the directory's head."""
        from core import pg

        rev = _module()
        assert rev.revision == REV_ID
        assert rev.down_revision == "0038_order_discounts"
        assert pg.REQUIRED_REVISION == REV_ID
        assert REV_ID in [m.revision for m in replay.lineage(pg.REQUIRED_REVISION)]

    def test_it_creates_exactly_the_three_tables_and_the_sequence(self):
        """Kills: a table left out or added, one outside `app`, or an index
        beside the keys — the reads are of a few hundred rows."""
        sql = _sql_without_comments()
        assert {m.group(2) for m in _CREATED.finditer(sql)} == TABLES
        assert {m.group(2) for m in _SEQUENCE.finditer(sql)} == {"app.promotion_ids"}
        assert set(_module().TABLES) == TABLES
        assert _module().SEQUENCES == ("app.promotion_ids",)
        assert not re.search(r"\bCREATE\s+(UNIQUE\s+)?INDEX\b", sql, re.I)

    def test_every_create_is_idempotent(self):
        """IF NOT EXISTS, so the upgrade after a keep-the-tables downgrade is
        a no-op. Kills: the clause dropped from any of the four."""
        sql = "\n".join(_up())
        for m in list(_CREATED.finditer(sql)) + list(_SEQUENCE.finditer(sql)):
            assert m.group(1), m.group(0)

    def test_it_writes_nothing_but_ddl_and_comments(self):
        """No schema (rule 11), no grant (the default privileges in
        `postgres/initdb/30-app.sql` give ks_readonly SELECT), no data, no
        change to an existing table. Read without the SQL's own comments and
        the comments' texts, which speak of writers."""
        sql = _sql_without_comments()
        sql = re.sub(r"'(?:[^']|'')*'", "''", sql).upper()
        for word in ("CREATE SCHEMA", "GRANT", "REVOKE", "INSERT", "UPDATE",
                     "DELETE", "ALTER", "DROP", "TRUNCATE"):
            assert not re.search(rf"\b{word}\b", sql), word

    def test_every_table_carries_a_comment(self):
        """Kills: a table created bare — a reader of the catalogue would have
        nothing to go on."""
        state = replay.apply({}, _up())
        assert set(state) == TABLES
        assert all(state[table] for table in TABLES), state
        assert "Append-only" in state["app.promotion_versions"]
        assert "core.promotions" in state["app.promotion_versions"]

    def test_the_replay_sees_the_tables_at_the_head_and_not_before(self):
        at_head = replay.comments_at(REV_ID)
        at_0038 = replay.comments_at("0038_order_discounts")
        assert set(at_head) - set(at_0038) == TABLES

    def test_the_versions_table_holds_the_contracts_vocabulary(self):
        """The CHECKs name exactly what `core.promotions` accepts. Kills: a
        kind, scope, channel, source or status added on one side only."""
        from core import promotions

        sql = _sql_without_comments()

        def listed(column):
            m = re.search(rf"{column}\s+TEXT\s+NOT NULL CHECK \({column} IN \(([^)]*)\)\)", sql)
            assert m, column
            return tuple(v.strip().strip("'") for v in m.group(1).split(","))

        assert listed("source") == promotions.SOURCES
        assert listed("status") == (promotions.PLANNED, promotions.CANCELLED)
        assert listed("kind") == promotions.KINDS
        assert listed("scope") == promotions.SCOPES
        channels = re.search(r"channels <@ ARRAY\[([^\]]*)\]", sql)
        assert channels
        assert tuple(v.strip().strip("'") for v in channels.group(1).split(",")) == \
            promotions.CHANNELS

    def test_the_bounds_are_the_writers(self):
        """Kills: a bound moved on one side only — the API would accept what
        the table refuses (a 500) or refuse what it would hold."""
        from core import promotions

        sql = _sql_without_comments()
        assert f"char_length(name) BETWEEN 1 AND {promotions.NAME_MAX}" in sql
        assert f"ends_on - starts_on < {promotions.SPAN_MAX_DAYS}" in sql
        assert f"discount_pct BETWEEN 0 AND {promotions.DISCOUNT_MAX}" in sql
        assert f"cardinality(scope_items) <= {promotions.ITEMS_MAX}" in sql
        assert f"char_length(note) <= {promotions.NOTE_MAX}" in sql
        assert "UNIQUE (promotion_id, version)" in sql


class TestTheDowngradeKeepsTheTables:
    def test_it_executes_nothing(self):
        """What a person typed about a promotion cannot be recomputed, so the
        downgrade moves the stamp and nothing else. Kills: any statement in
        the downgrade, a DROP above all."""
        assert replay.statements(_module(), "downgrade") == []

    def test_after_the_downgrade_the_replay_still_holds_them(self):
        rev = _module()
        up = replay.apply(replay.comments_at("0038_order_discounts"), _up())
        down = replay.apply(up, replay.statements(rev, "downgrade"))
        assert TABLES <= set(down)
        assert down == up
