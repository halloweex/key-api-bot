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

DSN = os.getenv("KS_PG_DSN")
pytestmark = pytest.mark.skipif(not DSN, reason="needs a live PostgreSQL at KS_PG_DSN")


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
