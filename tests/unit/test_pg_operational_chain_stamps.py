"""A chain's disagreement is stamped only on the tables this job ships.

`replicate_operational` stamps a write chain's tables failing when the chain
is latched against its flag, when its owner rows outlived their marker, and
when its flag cannot be read — so a watermark says so instead of ageing
quietly. Chains 1, 8, 7a and 6a own only tables this job ships, which is why
it stamped every `CHAIN_TABLES` entry. Chain 4 is the first to own tables
another shipper carries: `bronze.buyers` and `bronze.buyer_contacts` are the
buyers mirror's. A failure stamped there is a count only the mirror's next
non-empty success can clear, so after a rollback the daily comparison would
page `mirror_failing` on two stores that agree (chain 4 plan, step 8e).

These register a fake chain shaped like chain 4 — two mirrored bronze tables
and `app.buyer_gender`, which this job does ship — and put it in each of the
three states.
"""
from __future__ import annotations

import types
from unittest.mock import AsyncMock, patch

import pytest

from core import chain_latch, write_chains
from core.duckdb_store import DuckDBStore
from core.pg_operational import _tables_to_ship
from tests.unit.test_pg_operational_merged_failure import (  # noqa: F401 — fixture
    STAMP, _Conn, _Pool, _run, no_flags,
)

BUYERS = "bronze.buyers"
CONTACTS = "bronze.buyer_contacts"
GENDER = "app.buyer_gender"
FAKE = "pg_buyers_stamp_fake"


def _register(monkeypatch, env):
    fake = types.ModuleType(f"core.{FAKE}")
    fake.WRITE_ENV = "KS_WRITE_BUYERS_STAMP_FAKE"
    fake.CHAIN_TABLES = (BUYERS, CONTACTS, GENDER)
    fake.env_writes_postgres = env
    monkeypatch.setattr(write_chains, "WRITE_CHAINS",
                        write_chains.WRITE_CHAINS + (fake,))
    return fake


def _off():
    return False


def _typo():
    raise RuntimeError("KS_WRITE_BUYERS_STAMP_FAKE='postgrse' is not understood")


def test_the_shape_this_depends_on():
    """The fake is only chain 4's shape if these hold: the job ships the
    gender table and neither bronze buyer table."""
    ships = set(_tables_to_ship(frozenset()))
    assert GENDER in ships
    assert not {BUYERS, CONTACTS} & ships


@pytest.mark.asyncio
@pytest.mark.parametrize("state", ["latched_against_flag", "owner_rows_no_marker",
                                   "flag_not_understood"])
async def test_only_the_shipped_table_is_stamped(no_flags, tmp_path, state):
    owners = {}
    real_latched_at = chain_latch.latched_at
    if state == "flag_not_understood":
        _register(no_flags, _typo)
    else:
        _register(no_flags, _off)
    if state == "latched_against_flag":
        no_flags.setattr(chain_latch, "latched_at",
                         lambda name: STAMP if name == FAKE else real_latched_at(name))
    if state == "owner_rows_no_marker":
        owners = {t: STAMP for t in (BUYERS, CONTACTS, GENDER)}

    store = DuckDBStore(db_path=tmp_path / "ops.duckdb")
    await store.connect()
    stamps = AsyncMock(return_value=True)
    try:
        result = await _run(store, _Pool(_Conn(fail_at=None)), owners, stamps)
    finally:
        await store.close()

    assert "error" not in result, result
    stamped = {c.args[0] for c in stamps.await_args_list}
    assert stamped == {GENDER}, (state, stamped)
    # Each state says its own thing on the one table it charges.
    note = stamps.await_args_list[0].args[1]
    assert {"latched_against_flag": "owned by Postgres",
            "owner_rows_no_marker": "no local marker",
            "flag_not_understood": "not shipped"}[state] in note, note
