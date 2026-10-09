"""Revision 0035 against a real PostgreSQL: the refusal it exists for, its
statements as a server parses them, and the lock they take.

The suite's database is at the head — CI runs `alembic upgrade head` before
the suite, the gate does the same. Nothing here moves it: the downgrade's and
the upgrade's statements run inside one transaction that is always rolled
back, and `COMMENT` is transactional. The expected texts are not restated;
they come from replaying the migrations (`tests/migration_replay.py`).
"""
from __future__ import annotations

import os

import asyncpg
import pytest

from core import pg
from tests import migration_replay as replay

DSN = os.getenv("KS_PG_DSN")
pytestmark = pytest.mark.skipif(not DSN, reason="needs a live PostgreSQL at KS_PG_DSN")

REV = replay.load(replay.VERSIONS / "0035_batch_e_chains.py")


def _tables():
    return sorted(replay.single_comment(s)[0]
                  for s in replay.statements(REV, "upgrade"))


async def _comments(conn, tables):
    rows = await conn.fetch(
        "SELECT t, obj_description(t::regclass, 'pg_class') AS c "
        "FROM unnest($1::text[]) AS t", tables)
    return {r["t"]: r["c"] for r in rows}


class TestTheRefusal:
    @pytest.mark.asyncio
    async def test_an_image_at_0034_refuses_this_database_and_this_code_does_not(self):
        """What the revision is for, asked of the server rather than of a fake
        connection. Kills: the suite's database left below the head, or a pin
        that no longer refuses the image before it."""
        conn = await asyncpg.connect(DSN)
        try:
            await pg.require_revision(conn=conn)
            with pytest.raises(pg.SchemaVersionError, match="0034_buyer_chain"):
                await pg.require_revision("0034_buyer_chain", conn=conn)
        finally:
            await conn.close()


class TestTheRoundTripOnAServer:
    @pytest.mark.asyncio
    async def test_down_then_up_lands_where_the_replay_says_and_locks_no_reader(self):
        """Down and then up, as statements a server parses and applies, read
        back with `obj_description`. Kills: a quote the literal does not
        double (revision 0009 died on a real server over one), a downgrade
        text that differs from what the old revisions actually stored, and a
        statement that takes more than SHARE UPDATE EXCLUSIVE — the lock the
        docstring promises reads and writes carry on beside."""
        tables = _tables()
        at_head = replay.comments_at(pg.REQUIRED_REVISION)
        at_0034 = replay.comments_at("0034_buyer_chain")
        at_0035 = replay.comments_at("0035_batch_e_chains")
        conn = await asyncpg.connect(DSN)
        transaction = conn.transaction()
        await transaction.start()
        try:
            assert await _comments(conn, tables) == {t: at_head[t] for t in tables}

            for sql in replay.statements(REV, "downgrade"):
                await conn.execute(sql)
            assert await _comments(conn, tables) == {t: at_0034[t] for t in tables}

            for sql in replay.statements(REV, "upgrade"):
                await conn.execute(sql)
            assert await _comments(conn, tables) == {t: at_0035[t] for t in tables}

            held = await conn.fetch(
                "SELECT relation::regclass::text AS t, mode FROM pg_locks "
                "WHERE pid = pg_backend_pid() AND locktype = 'relation' "
                "AND relation = ANY($1::regclass[])", tables)
            modes = {}
            for row in held:
                modes.setdefault(row["t"], set()).add(row["mode"])
            assert modes == {t: {"ShareUpdateExclusiveLock"} for t in tables}
        finally:
            await transaction.rollback()
            await conn.close()
