"""The standing watch on a moved chain's tables, against a real Postgres.

DN-07. Once a chain latches, the hourly shipper and the daily comparison both
stand down for its tables and the comparison does not even file a finding — so
these invariants are the only thing left watching them. A test that proved them
against a mock would be proving the mock.

The shape is the one `reconcile_operational` was verified with: build a healthy
state, assert silence, then break **one** thing per test and assert exactly the
finding it should produce and no other. Silence on the healthy state is the
half that matters most here — a check nobody trusts is a check nobody reads.
"""
from __future__ import annotations

import os
from datetime import date, datetime, timedelta, timezone
from unittest.mock import AsyncMock, patch

import asyncpg
import pytest
import pytest_asyncio

from core import chain_latch, pg_chain_invariants as inv
from core.duckdb_constants import DEFAULT_TZ
from core.pg_chain_invariants import check_chain_invariants, read_facts

DSN = os.getenv("KS_PG_DSN")
pytestmark = pytest.mark.skipif(not DSN, reason="needs a live PostgreSQL at KS_PG_DSN")

OFFERS = 10
# Three days back, so the per-day window covers a handful of days and every
# "since the handover" invariant has something to judge.
LATCH_DAYS_AGO = 3
# How far before the handover the replicated history reaches. Production's
# goes back to 2026-01-27; what matters is that it exists, because the
# first-seen check reads an offer's earliest photograph.
HISTORY_DAYS_BEFORE = 7


def _kyiv_day(stamp: datetime) -> date:
    """The handover as the warehouse dates it. The latch stamp is UTC and every
    date these tables carry is Kyiv's, so `stamp.date()` is the wrong day from
    21:00 or 22:00 UTC — the fixtures would disagree with the module three
    hours a day and pass or fail by the clock."""
    return stamp.astimezone(DEFAULT_TZ).date()


async def _clean(pool):
    async with pool.acquire() as conn:
        for table in ("app.manual_expenses", "app.stock_movements",
                      "app.inventory_sku_history", "app.inventory_history",
                      "app.sku_inventory_status", "bronze.offer_stocks",
                      "bronze.offers", "meta.chain_watermarks"):
            await conn.execute(f"DELETE FROM {table}")


async def _seed_healthy(conn, latched_at: datetime, today: date) -> None:
    """Everything the invariants expect to find, and nothing wrong with it."""
    latch_day = _kyiv_day(latched_at)
    yesterday = today - timedelta(days=1)
    history_from = latch_day - timedelta(days=HISTORY_DAYS_BEFORE)

    await conn.executemany(
        "INSERT INTO bronze.offers (id, product_id, sku, synced_at) "
        "VALUES ($1, $2, $3, now())",
        [(i, i, f"SKU-{i}") for i in range(1, OFFERS + 1)])
    await conn.executemany(
        "INSERT INTO bronze.offer_stocks "
        "(id, sku, price, purchased_price, quantity, reserve, mirrored_at) "
        "VALUES ($1, $2, 100, 50, 5, 0, now())",
        [(i, f"SKU-{i}") for i in range(1, OFFERS + 1)])
    # `first_seen_at` well before the handover: that is the carry-forward
    # working, and it is what the reset check measures against.
    await conn.executemany(
        "INSERT INTO app.sku_inventory_status "
        "(offer_id, product_id, sku, quantity, reserve, price, first_seen_at) "
        "VALUES ($1, $1, $2, 5, 0, 100, $3)",
        [(i, f"SKU-{i}", latch_day - timedelta(days=90))
         for i in range(1, OFFERS + 1)])

    # The snapshots begin before the handover, as production's do: DuckDB took
    # them and the hourly replication carried them across.
    days = []
    day = history_from
    while day <= yesterday:
        days.append(day)
        day += timedelta(days=1)
    await conn.executemany(
        "INSERT INTO app.inventory_history "
        "(date, total_quantity, total_value, total_reserve, sku_count, recorded_at) "
        "VALUES ($1, 50, 5000, 0, $2, now())",
        [(d, OFFERS) for d in days])
    await conn.executemany(
        "INSERT INTO app.inventory_sku_history (date, offer_id, quantity, reserve, price) "
        "VALUES ($1, $2, 5, 0, 100)",
        [(d, i) for d in days for i in range(1, OFFERS + 1)])

    # DuckDB's own first sync, long before the handover and replicated across:
    # the whole catalogue `initial`, as every store's first sync is. Then the
    # first sync after the handover, which has a delta base — the replication
    # delivered `bronze.offer_stocks` — and so records two ordinary changes.
    await conn.executemany(
        "INSERT INTO app.stock_movements "
        "(id, offer_id, product_id, movement_type, quantity_before, quantity_after, "
        " delta, reserve_before, reserve_after, recorded_at, source) "
        "VALUES ($1, $2, $2, $3, $4, $5, $6, 0, 0, $7, 'sync')",
        [(i, i, "initial", 0, 5, 5, latched_at - timedelta(days=30))
         for i in range(1, OFFERS + 1)]
        + [(OFFERS + i, i, "stock_out", 5, 4, -1, latched_at + timedelta(minutes=1))
           for i in (1, 2)])
    await conn.execute(
        "SELECT setval('app.stock_movements_id_seq', "
        "COALESCE((SELECT MAX(id) FROM app.stock_movements), 0) + 1, false)")

    await conn.executemany(
        "INSERT INTO app.manual_expenses "
        "(id, expense_date, category, expense_type, amount, currency, created_at) "
        "VALUES ($1, $2, 'ads', 'facebook', 100, 'UAH', now())",
        [(i, today) for i in (1, 2)])
    await conn.execute(
        "SELECT setval('app.manual_expenses_id_seq', "
        "COALESCE((SELECT MAX(id) FROM app.manual_expenses), 0) + 1, false)")

    fresh = datetime.now(timezone.utc).isoformat()
    await conn.executemany(
        "INSERT INTO meta.chain_watermarks (key, value, updated_at) "
        "VALUES ($1, $2, now())",
        [("last_sync_offers", fresh), ("last_sync_stocks", fresh)])


@pytest_asyncio.fixture
async def latched(monkeypatch):
    """Both chains latched, a healthy Postgres behind them, and `get_pool`
    pointed at it — the state the invariants exist for."""
    monkeypatch.setenv("KS_PG_DSN", DSN)
    for env in ("KS_WRITE_INVENTORY", "KS_WRITE_EXPENSES"):
        monkeypatch.delenv(env, raising=False)

    pool = await asyncpg.create_pool(DSN, min_size=1, max_size=3)
    await _clean(pool)
    async with pool.acquire() as conn:
        today = await conn.fetchval("SELECT (now() AT TIME ZONE 'Europe/Kyiv')::date")
    latched_at = datetime.now(timezone.utc) - timedelta(days=LATCH_DAYS_AGO)

    # The marker carries the stamp, and `chain_latch.latch` would use *now*;
    # these invariants are all dated from the handover, so the fixture writes
    # the marker itself, exactly as the module reads it back.
    for chain in ("pg_expenses_write", "pg_inventory_write"):
        chain_latch.MARKER_DIR.mkdir(parents=True, exist_ok=True)
        (chain_latch.MARKER_DIR / chain).write_text(
            f'{{"chain": "{chain}", "latched_at": "{latched_at.isoformat()}"}}',
            encoding="utf-8")
    chain_latch.load()

    async with pool.acquire() as conn:
        await _seed_healthy(conn, latched_at, today)

    with patch("core.pg.get_pool", new=AsyncMock(return_value=pool)), \
         patch("core.pg.require_revision", new=AsyncMock()):
        yield pool, latched_at, today
    await _clean(pool)
    await pool.close()


async def _findings(names_only: bool = True):
    issues = check_chain_invariants(await read_facts())
    return sorted(i.check_name for i in issues) if names_only else issues


def _relatch(ago: timedelta) -> datetime:
    """Move the inventory handover to `ago` before now.

    Inside the day, the per-day invariants have no complete day to judge,
    which is what isolates the burst check in the tests that use this.
    """
    return _relatch_at(datetime.now(timezone.utc) - ago)


def _relatch_at(stamp: datetime) -> datetime:
    """Move the inventory handover to exactly `stamp`."""
    (chain_latch.MARKER_DIR / "pg_inventory_write").write_text(
        f'{{"chain": "pg_inventory_write", "latched_at": "{stamp.isoformat()}"}}',
        encoding="utf-8")
    chain_latch.load()
    return stamp


async def _initial_batch(pool, at) -> None:
    """The whole catalogue labelled `initial` in one sync, stamped `at`."""
    async with pool.acquire() as conn:
        await conn.executemany(
            "INSERT INTO app.stock_movements "
            "(offer_id, product_id, movement_type, quantity_before, quantity_after,"
            " delta, reserve_before, reserve_after, recorded_at, source) "
            "VALUES ($1, $1, 'initial', 0, 5, 5, 0, 0, $2, 'sync')",
            [(i, at) for i in range(1, OFFERS + 1)])


class TestTheHealthyState:
    @pytest.mark.asyncio
    async def test_a_correct_chain_is_silent(self, latched):
        assert await _findings() == []

    @pytest.mark.asyncio
    async def test_catalogue_growth_over_sixty_days_never_fires(self, latched):
        """Fifty new offers arriving one a day after a handover two months old
        — more than 5% of the catalogue dated after it, which the first form of
        the first-seen check turned into a CRITICAL that paged for ever."""
        pool, _, today = latched
        await _seed_sixty_days(pool, today)
        assert await _findings() == []


async def _seed_sixty_days(pool, today: date) -> date:
    """A handover sixty days ago and a catalogue that kept growing after it.

    840 offers first seen well before the handover and photographed every day
    of the replicated history; then 50 more, one a day from the day after the
    handover, each dated the day it arrived and photographed from that day on
    — what `rebuild_sku_inventory_status` and `record_sku_inventory_snapshot`
    write for an offer KeyCRM starts serving. Returns the handover day.
    """
    latch_day = _kyiv_day(_relatch(timedelta(days=60)))
    old, new = 840, 50
    history_from = latch_day - timedelta(days=30)
    yesterday = today - timedelta(days=1)
    async with pool.acquire() as conn:
        for table in ("app.sku_inventory_status", "app.inventory_sku_history",
                      "app.inventory_history", "bronze.offer_stocks", "bronze.offers"):
            await conn.execute(f"DELETE FROM {table}")
        await conn.execute(
            "INSERT INTO bronze.offers (id, product_id, sku, synced_at) "
            "SELECT i, i, 'SKU-' || i, now() FROM generate_series(1, $1) i",
            old + new)
        await conn.execute(
            "INSERT INTO bronze.offer_stocks "
            "(id, sku, price, purchased_price, quantity, reserve, mirrored_at) "
            "SELECT i, 'SKU-' || i, 100, 50, 5, 0, now() "
            "FROM generate_series(1, $1) i", old + new)
        # An offer's arrival day: before the history for the old ones, one a
        # day from the day after the handover for the new ones.
        arrival = ("CASE WHEN i <= $1 THEN $2::date - 90 "
                   "ELSE $3::date + (i - $1) END")
        await conn.execute(
            "INSERT INTO app.sku_inventory_status "
            "(offer_id, product_id, sku, quantity, reserve, price, first_seen_at) "
            f"SELECT i, i, 'SKU-' || i, 5, 0, 100, {arrival} "
            "FROM generate_series(1, $4) i",
            old, latch_day, latch_day, old + new)
        await conn.execute(
            "INSERT INTO app.inventory_sku_history "
            "(date, offer_id, quantity, reserve, price) "
            "SELECT d::date, i, 5, 0, 100 "
            "FROM generate_series($5::date, $6::date, interval '1 day') d, "
            "     generate_series(1, $4) i "
            f"WHERE d::date >= GREATEST({arrival}, $5::date)",
            old, latch_day, latch_day, old + new, history_from, yesterday)
        await conn.execute(
            "INSERT INTO app.inventory_history "
            "(date, total_quantity, total_value, total_reserve, sku_count, recorded_at) "
            "SELECT d::date, 50, 5000, 0, $3, now() "
            "FROM generate_series($1::date, $2::date, interval '1 day') d",
            history_from, yesterday, old + new)
        grown = await conn.fetchval(
            "SELECT count(*) FROM app.sku_inventory_status WHERE first_seen_at > $1",
            latch_day)
    assert grown == new and grown / (old + new) > 0.05
    return latch_day


class TestOneMutationEach:
    @pytest.mark.asyncio
    async def test_an_expense_allocator_below_the_table_collides(self, latched):
        pool, _, _ = latched
        async with pool.acquire() as conn:
            await conn.execute(
                "SELECT setval('app.manual_expenses_id_seq', 1, true)")
        issues = await _findings(names_only=False)
        found = [i for i in issues if i.check_name == inv.SEQUENCE_BEHIND]
        assert [i.table_name for i in found] == ["app.manual_expenses"]
        assert "already holds id 2" in found[0].description

    @pytest.mark.asyncio
    async def test_a_movement_allocator_below_the_table_renames_a_movement(self, latched):
        pool, _, _ = latched
        async with pool.acquire() as conn:
            await conn.execute(
                "SELECT setval('app.stock_movements_id_seq', 5, true)")
        found = [i for i in await _findings(names_only=False)
                 if i.check_name == inv.SEQUENCE_BEHIND]
        assert [i.table_name for i in found] == ["app.stock_movements"]

    @pytest.mark.asyncio
    @pytest.mark.parametrize("table, sequence", [
        ("app.manual_expenses", "app.manual_expenses_id_seq"),
        ("app.stock_movements", "app.stock_movements_id_seq"),
    ])
    async def test_a_sequence_set_to_max_not_called_hands_out_max(
        self, latched, table, sequence,
    ):
        """`setval(seq, MAX(id), false)`: the next `nextval` returns MAX(id)
        itself — the most direct collision there is, and the one that reads as
        healthy if `is_called` is ignored or read backwards."""
        pool, _, _ = latched
        async with pool.acquire() as conn:
            await conn.execute(
                f"SELECT setval('{sequence}', (SELECT MAX(id) FROM {table}), false)")
        found = [i for i in await _findings(names_only=False)
                 if i.check_name == inv.SEQUENCE_BEHIND]
        assert [i.table_name for i in found] == [table]
        assert found[0].count == 1

    @pytest.mark.asyncio
    @pytest.mark.parametrize("table, sequence", [
        ("app.manual_expenses", "app.manual_expenses_id_seq"),
        ("app.stock_movements", "app.stock_movements_id_seq"),
    ])
    async def test_a_sequence_that_last_handed_out_max_is_healthy(
        self, latched, table, sequence,
    ):
        """`setval(seq, MAX(id), true)` — what a `nextval` that returned MAX(id)
        leaves behind. The next id is MAX + 1, and nothing is wrong."""
        pool, _, _ = latched
        async with pool.acquire() as conn:
            await conn.execute(
                f"SELECT setval('{sequence}', (SELECT MAX(id) FROM {table}), true)")
        assert inv.SEQUENCE_BEHIND not in await _findings()

    @pytest.mark.asyncio
    async def test_a_null_created_at_names_the_column(self, latched):
        pool, _, _ = latched
        async with pool.acquire() as conn:
            await conn.execute(
                "UPDATE app.manual_expenses SET created_at = NULL WHERE id = 1")
        found = [i for i in await _findings(names_only=False)
                 if i.check_name == inv.COLUMN_NULL]
        assert len(found) == 1 and found[0].table_name == "app.manual_expenses"
        assert "created_at in 1 row(s)" in found[0].description

    @pytest.mark.asyncio
    async def test_a_null_recorded_at_and_source_are_one_finding(self, latched):
        pool, _, _ = latched
        async with pool.acquire() as conn:
            await conn.execute(
                "UPDATE app.stock_movements SET recorded_at = NULL, source = NULL "
                "WHERE id <= 2")
        found = [i for i in await _findings(names_only=False)
                 if i.check_name == inv.COLUMN_NULL]
        assert len(found) == 1 and found[0].count == 4
        assert "recorded_at in 2 row(s)" in found[0].description
        assert "source in 2 row(s)" in found[0].description

    @pytest.mark.asyncio
    async def test_a_burst_after_quiet_syncs_is_reported(self, latched):
        """The reviewer's case against the old exemption: the syncs after the
        handover changed nothing and wrote no movement, then the base was lost.
        "The earliest batch since the latch" was this one, and it was excused."""
        pool, _, _ = latched
        _relatch(timedelta(hours=3))
        await _initial_batch(pool, datetime.now(timezone.utc))
        found = [i for i in await _findings(names_only=False)
                 if i.check_name == inv.INITIAL_BURST]
        assert len(found) == 1 and found[0].count == OFFERS

    @pytest.mark.asyncio
    async def test_a_burst_in_the_first_sync_after_the_handover_is_reported(
        self, latched,
    ):
        """No exemption: the replication delivered the delta base before the
        latch, so a first sync labelling the catalogue is a base that never
        arrived — the finding's own failure."""
        pool, _, _ = latched
        stamp = _relatch(timedelta(hours=1))
        await _initial_batch(pool, stamp + timedelta(minutes=1))
        found = [i for i in await _findings(names_only=False)
                 if i.check_name == inv.INITIAL_BURST]
        assert len(found) == 1 and found[0].count == OFFERS
        assert "not excused" in found[0].description

    @pytest.mark.asyncio
    async def test_a_burst_duckdb_recorded_before_the_handover_is_not_this_writers(
        self, latched,
    ):
        """Inside the 24 hours and before the latch: DuckDB's movement, carried
        across by the replication. The chain that took over did not write it."""
        pool, _, _ = latched
        stamp = _relatch(timedelta(hours=1))
        await _initial_batch(pool, stamp - timedelta(hours=1))
        assert inv.INITIAL_BURST not in await _findings()

    @pytest.mark.asyncio
    async def test_first_seen_resetting_to_the_handover_is_reported(self, latched):
        pool, latched_at, _ = latched
        async with pool.acquire() as conn:
            await conn.execute(
                "UPDATE app.sku_inventory_status SET first_seen_at = $1 "
                "WHERE offer_id <= 8", _kyiv_day(latched_at))
        found = [i for i in await _findings(names_only=False)
                 if i.check_name == inv.FIRST_SEEN_RESET]
        assert len(found) == 1 and found[0].count == 8
        assert found[0].sample_ids == tuple(range(1, 9))
        assert "8 of 10" in found[0].description

    @pytest.mark.asyncio
    async def test_a_reset_through_the_real_rebuild_is_reported(self, latched):
        """The failure itself rather than its imitation: the status table empty
        under `rebuild_sku_inventory_status`, so the carry-forward has nothing
        to carry and the rebuild dates every offer again — today, here, since
        none has sold. Every one of them was photographed before today."""
        from core import pg_inventory_write

        pool, _, today = latched
        await _seed_sixty_days(pool, today)
        async with pool.acquire() as conn:
            await conn.execute("DELETE FROM app.sku_inventory_status")
        assert await pg_inventory_write.rebuild_sku_inventory_status() == 890
        async with pool.acquire() as conn:
            assert await conn.fetchval(
                "SELECT count(DISTINCT first_seen_at) FROM app.sku_inventory_status") == 1
        found = [i for i in await _findings(names_only=False)
                 if i.check_name == inv.FIRST_SEEN_RESET]
        assert len(found) == 1 and found[0].count == 890

    @pytest.mark.asyncio
    async def test_a_date_duckdb_moved_before_the_handover_is_not_judged(
        self, latched,
    ):
        """A first-seen date later than an earlier photograph, but set before
        the handover: DuckDB's, and indistinguishable here from a correct one
        moved in March. Judging it would fire from the latch about the store
        this chain replaced."""
        pool, latched_at, _ = latched
        async with pool.acquire() as conn:
            await conn.execute(
                "UPDATE app.sku_inventory_status SET first_seen_at = $1 "
                "WHERE offer_id = 1", _kyiv_day(latched_at) - timedelta(days=2))
        assert inv.FIRST_SEEN_RESET not in await _findings()

    @pytest.mark.asyncio
    async def test_the_handover_is_dated_in_kyiv_not_in_utc(self, latched):
        """A latch at 22:30 UTC is the next morning in Kyiv, where every date in
        these tables is kept. Offer 1 is dated the UTC day — the Kyiv day
        before the handover, DuckDB's — and offer 2 the Kyiv day of it. Bound
        on `latched_at.date()`, the check judged both."""
        pool, latched_at, _ = latched
        utc_day = latched_at.date()
        stamp = _relatch_at(datetime(utc_day.year, utc_day.month, utc_day.day,
                                     22, 30, tzinfo=timezone.utc))
        assert _kyiv_day(stamp) == utc_day + timedelta(days=1)
        async with pool.acquire() as conn:
            await conn.execute(
                "UPDATE app.sku_inventory_status SET first_seen_at = $1 "
                "WHERE offer_id = 1", utc_day)
            await conn.execute(
                "UPDATE app.sku_inventory_status SET first_seen_at = $1 "
                "WHERE offer_id = 2", _kyiv_day(stamp))
        found = [i for i in await _findings(names_only=False)
                 if i.check_name == inv.FIRST_SEEN_RESET]
        assert len(found) == 1 and found[0].sample_ids == (2,)
        assert f"handover ({_kyiv_day(stamp).isoformat()})" in found[0].description

    @pytest.mark.asyncio
    async def test_a_missing_rolled_up_snapshot_day_is_reported(self, latched):
        pool, _, today = latched
        gone = today - timedelta(days=1)
        async with pool.acquire() as conn:
            await conn.execute("DELETE FROM app.inventory_history WHERE date = $1", gone)
        found = [i for i in await _findings(names_only=False)
                 if i.check_name == inv.ROLLUP_MISSING]
        assert len(found) == 1 and found[0].count == 1
        assert gone.isoformat() in found[0].description

    @pytest.mark.asyncio
    async def test_a_short_per_sku_snapshot_day_is_reported(self, latched):
        """A day that was photographed while the status table was half
        rebuilt. A day with no rows at all is the continuity check's finding,
        not this one."""
        pool, _, today = latched
        short = today - timedelta(days=1)
        async with pool.acquire() as conn:
            await conn.execute(
                "DELETE FROM app.inventory_sku_history WHERE date = $1 AND offer_id > 2",
                short)
        found = [i for i in await _findings(names_only=False)
                 if i.check_name == inv.SNAPSHOT_SHORT]
        assert len(found) == 1
        assert f"{short.isoformat()} (2)" in found[0].description

    @pytest.mark.asyncio
    async def test_a_stalled_chain_watermark_is_reported(self, latched):
        pool, _, _ = latched
        stale = (datetime.now(timezone.utc) - timedelta(hours=4)).isoformat()
        async with pool.acquire() as conn:
            await conn.execute(
                "UPDATE meta.chain_watermarks SET value = $1 WHERE key = $2",
                stale, "last_sync_stocks")
        found = [i for i in await _findings(names_only=False)
                 if i.check_name == inv.WATERMARK_STALE]
        assert [i.table_name for i in found] == ["last_sync_stocks"]
        assert "pg_inventory_write" in found[0].description


class TestWhatItWillNotSay:
    @pytest.mark.asyncio
    async def test_a_watermark_that_was_never_written_is_the_freshness_check(
        self, latched,
    ):
        """Absent is "this chain has not synced since the handover", which
        `_freshness_check` already reports under its own name. Two names for
        one fact reads as two problems."""
        pool, _, _ = latched
        async with pool.acquire() as conn:
            await conn.execute("DELETE FROM meta.chain_watermarks")
        assert inv.WATERMARK_STALE not in await _findings()

    @pytest.mark.asyncio
    async def test_an_empty_status_table_has_lost_nothing(self, latched):
        pool, _, _ = latched
        async with pool.acquire() as conn:
            await conn.execute("DELETE FROM app.sku_inventory_status")
        assert inv.FIRST_SEEN_RESET not in await _findings()

    @pytest.mark.asyncio
    async def test_every_row_recent_is_the_worst_case_and_is_not_excused(self, latched):
        """An early draft excused this as a bootstrap, and it is the opposite:
        the table is full-replaced from DuckDB every hour, so at the handover
        it already carries historical dates, and the replicated snapshots
        photographed every offer before it. Every row dated after them means
        the rebuild replaced them all."""
        pool, latched_at, _ = latched
        async with pool.acquire() as conn:
            await conn.execute(
                "UPDATE app.sku_inventory_status SET first_seen_at = $1",
                _kyiv_day(latched_at))
        found = [i for i in await _findings(names_only=False)
                 if i.check_name == inv.FIRST_SEEN_RESET]
        assert len(found) == 1 and found[0].count == OFFERS
        assert f"{OFFERS} of {OFFERS}" in found[0].description


class TestOneUnreadableTable:
    """The per-group savepoint in `read_facts`, on the engine whose rule it
    exists for: in Postgres a failed statement aborts the transaction it ran
    in, and every statement after it fails until that transaction — or the
    savepoint around the statement — is rolled back.

    Chain 8's allocator is made unreadable by renaming its sequence away, and
    chain 1's allocator is left at MAX(id) not called, a real collision. The
    groups are read in name order, so the expense read fails first. Without the
    savepoint, chain 1's read and the watermarks' fail behind it and the run
    files three unwatched findings and no CRITICAL; with the failure re-raised
    rather than recorded, the whole run goes blind. Either way the collision is
    never filed, and that is what this pins."""

    @pytest.mark.asyncio
    async def test_the_other_chains_verdict_is_still_read_and_judged(self, latched):
        pool, _, _ = latched
        async with pool.acquire() as conn:
            await conn.execute(
                "SELECT setval('app.stock_movements_id_seq', "
                "(SELECT MAX(id) FROM app.stock_movements), false)")
            await conn.execute(
                "ALTER SEQUENCE app.manual_expenses_id_seq "
                "RENAME TO manual_expenses_id_seq_away")
        try:
            facts = await read_facts()
        finally:
            async with pool.acquire() as conn:
                await conn.execute(
                    "ALTER SEQUENCE app.manual_expenses_id_seq_away "
                    "RENAME TO manual_expenses_id_seq")

        assert facts.whole is None
        assert isinstance(facts.expenses, inv.Unwatched)
        assert "manual_expenses_id_seq" in facts.expenses.reason
        assert isinstance(facts.inventory, inv.Inventory)
        assert facts.watermarks_unread is None and facts.watermarks
        issues = check_chain_invariants(facts)
        assert sorted((i.check_name, i.table_name) for i in issues) == sorted([
            (inv.UNWATCHED, "(write chains)"),
            (inv.SEQUENCE_BEHIND, "app.stock_movements")])
        (unwatched,) = [i for i in issues if i.check_name == inv.UNWATCHED]
        assert "pg_expenses_write" in unwatched.description
        assert "pg_inventory_write" not in unwatched.description


class TestTheJobWhenTheDuckDBHalfFails:
    """The integrity job with the real read, a real (empty) DuckDB, and the
    DuckDB scan made to raise. These are Postgres facts about tables DuckDB no
    longer writes, so the scan failing must neither lose the chain finding nor
    hold back its page — the twins' arrangement, which they now share."""

    @pytest_asyncio.fixture
    async def job(self, latched, tmp_path, monkeypatch):
        from core.duckdb_store import DuckDBStore
        from core.scheduler import BackgroundScheduler

        monkeypatch.delenv("KS_DQ_PG_WAREHOUSE", raising=False)
        store = DuckDBStore(db_path=tmp_path / "integrity.duckdb")
        await store.connect()
        scheduler = BackgroundScheduler()
        sent = AsyncMock(return_value=True)
        monkeypatch.setattr(scheduler, "_send_dq_alert_throttled", sent)
        resolve = AsyncMock(return_value=0)
        with patch("core.duckdb_store.get_store", new=AsyncMock(return_value=store)), \
             patch("core.alerting.resolve_group", resolve):
            yield latched[0], scheduler, store, sent, resolve
        await store.close()

    @pytest.mark.asyncio
    async def test_a_movement_allocator_at_max_is_persisted_and_pages(self, job):
        from core.data_quality import fetch_run_issues

        pool, scheduler, store, sent, resolve = job
        async with pool.acquire() as conn:
            await conn.execute(
                "SELECT setval('app.stock_movements_id_seq', "
                "(SELECT MAX(id) FROM app.stock_movements), false)")

        with patch("core.data_quality.check_internal_integrity",
                   side_effect=RuntimeError("DuckDB file will not open")):
            result = await scheduler._run_dq_integrity()

        assert "DuckDB file will not open" in result["error"]
        async with store.connection() as conn:
            persisted = {r["check_name"]: r
                         for r in fetch_run_issues(conn, result["run_id"])}
        assert persisted[inv.SEQUENCE_BEHIND]["severity"] == "CRITICAL"
        assert persisted[inv.SEQUENCE_BEHIND]["table_name"] == "app.stock_movements"
        assert sent.await_count == 1
        assert sent.await_args.kwargs["conditions"] == [inv.SEQUENCE_BEHIND]
        (call,) = [c for c in resolve.await_args_list
                   if c.kwargs.get("only_prefix") == inv.PREFIX]
        assert inv.SEQUENCE_BEHIND in call.kwargs["still_firing"]
