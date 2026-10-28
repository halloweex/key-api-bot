"""Where DN-06's owner rows meet the failure stamp of the serialised copy.

`replicate_operational` learns which tables Postgres owns in two steps: the
local markers and flags before the first round trip, then the owner rows in
`meta.chain_watermarks` once it holds a pool. A run that fails stamps every
table it was copying. These pin which set a failure is written against on each
side of the owner-row read.

Since step 14's S14-00 every chain writes Postgres, a constant, so the local
answer already stands every chain's table down before the owner rows are
read, and what a failure stamps on either side of that read no longer
differs: those tests went with it. The copy itself goes in S14-13; the lock
it holds stays pinned here until then.
"""
from __future__ import annotations

import asyncio
from unittest.mock import patch

import pytest

from core import pg_operational
from core.pg_operational import replicate_operational
from core.write_chains import WRITE_CHAINS


@pytest.fixture
def no_flags(monkeypatch):
    """No KS_WRITE_* set, and a fresh lock for the copy."""
    for chain in WRITE_CHAINS:
        monkeypatch.delenv(chain.WRITE_ENV, raising=False)
    monkeypatch.setattr(pg_operational, "OPERATIONAL_REPLICATION_LOCK", asyncio.Lock())
    return monkeypatch


@pytest.mark.asyncio
async def test_a_cancelled_copy_releases_the_lock(no_flags):
    """CancelledError is not an Exception, so it skips the stamp — the lock
    must still come back, or every later copy (and every form submit) waits
    on a run that is gone."""
    entered = asyncio.Event()

    async def hang():
        entered.set()
        await asyncio.Event().wait()

    with patch("core.mirror_reconciliation.configured", return_value=True), \
         patch("core.pg.get_pool", new=hang):
        task = asyncio.create_task(replicate_operational(object()))
        await asyncio.wait_for(entered.wait(), 2)
        assert pg_operational.OPERATIONAL_REPLICATION_LOCK.locked()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
    assert not pg_operational.OPERATIONAL_REPLICATION_LOCK.locked()
