"""`freshness_orders` judges when the order step last wrote, not KeyCRM's clock.

The defect this pins: `last_sync_orders` holds KeyCRM's newest `updated_at`
(`SyncService._orders_step`), because that is where the next window starts.
The freshness check judged that value at 6 h, so every night on which nobody
created or edited an order for six hours before the 07:00 Kyiv integrity run
paged "orders not arriving" CRITICAL while the sync landed a batch every
minute. Measured on production on 2026-10-29: five pages from 2026-09-21 to
10-29, and on four of them the age it reported was the gap since the
evening's last order activity (`bronze.orders`, `app.order_versions`) to the
tenth of an hour — on the fifth, 10-12, an evening edit since overwritten
hides 0.5 h of it. On the 54 other mornings since 09-01 that gap was at most
6.4 h and nothing paged. On 10-29 the row was being rewritten at 07:52 UTC,
failures 0.

The order step rewrites the row on every tick that lands a batch
(`meta.chain_watermarks.updated_at`), so that stamp is what a stall stops.
"""
from __future__ import annotations

from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone

import pytest

from core import data_quality as dq
from core.data_quality import Severity, _judge_freshness
from tests.helpers.alerts_journal_pg import alert_ledger
from tests.helpers.dq_journal_pg import pool  # noqa: F401 — the journal, emptied
from tests.integration.pg_tab_seed import needs_postgres

UTC = timezone.utc

# (the 07:00 Kyiv run that paged, KeyCRM's newest updated_at it judged) —
# each value is the run time minus the age the finding reported.
PAGED_MORNINGS = [
    (datetime(2026, 9, 21, 4, 0, tzinfo=UTC), datetime(2026, 9, 20, 21, 0, tzinfo=UTC)),
    (datetime(2026, 9, 25, 4, 0, tzinfo=UTC), datetime(2026, 9, 24, 20, 18, tzinfo=UTC)),
    (datetime(2026, 10, 12, 4, 0, tzinfo=UTC), datetime(2026, 10, 11, 21, 0, tzinfo=UTC)),
    (datetime(2026, 10, 26, 5, 0, tzinfo=UTC), datetime(2026, 10, 25, 21, 48, tzinfo=UTC)),
    (datetime(2026, 10, 29, 5, 0, tzinfo=UTC), datetime(2026, 10, 28, 20, 24, tzinfo=UTC)),
]
MORNING_IDS = [now.strftime("%m-%d") for now, _ in PAGED_MORNINGS]


@pytest.fixture(autouse=True)
def _flags(monkeypatch):
    for env in ("KS_WRITE_INVENTORY", "KS_WRITE_EXPENSES", "KS_WRITE_GOALS"):
        monkeypatch.delenv(env, raising=False)


def _moved(now, **values):
    """Every moved watermark, written 5 min ago with a value 5 min old, but
    for the ones given as `entity=(value, stamp)` — as `{key: value}` and
    `{key: stamp}`."""
    from core.write_chains import sync_keys

    fresh = (now - timedelta(minutes=5)).isoformat()
    watermarks = {key: fresh for key in sync_keys()}
    stamps = dict(watermarks)
    for entity, (value, stamp) in values.items():
        watermarks[f"last_sync_{entity}"] = value.isoformat()
        stamps[f"last_sync_{entity}"] = stamp.isoformat()
    return watermarks, stamps


def _judge(now, watermarks, stamps):
    # Any DuckDB row: past the fresh-install gate. Orders are moved, so the
    # row itself is not judged.
    rows = {"last_sync_orders": (now - timedelta(days=30)).isoformat()}
    return _judge_freshness(rows, now, chain_watermarks=watermarks,
                            chain_stamps=stamps)


def _orders(issues):
    return [i for i in issues if i.check_name == "freshness_orders"]


class TestAQuietNightIsNotAStall:
    @pytest.mark.parametrize("now,value", PAGED_MORNINGS, ids=MORNING_IDS)
    def test_the_value_alone_pages_as_production_did(self, now, value):
        """The false page, reproduced: with no stamp in hand the value is
        judged, and the age is the one each morning reported."""
        watermarks, _ = _moved(now, orders=(value, now - timedelta(minutes=1)))
        (page,) = _orders(_judge(now, watermarks, None))
        assert page.severity == Severity.CRITICAL
        assert f"{(now - value).total_seconds() / 3600:.1f}h ago" in page.description

    @pytest.mark.parametrize("now,value", PAGED_MORNINGS, ids=MORNING_IDS)
    def test_with_the_row_rewritten_a_minute_ago_nothing_pages(self, now, value):
        """Mutation: judge the value whatever the stamp says."""
        watermarks, stamps = _moved(now, orders=(value, now - timedelta(minutes=1)))
        assert _orders(_judge(now, watermarks, stamps)) == []


class TestAStalledSyncStillPages:
    def test_a_row_not_written_for_seven_hours_is_critical(self):
        """The order step stopped at 22:00 UTC: KeyCRM's newest update it saw
        was 20:24, and nothing has rewritten the row since. Mutation: never
        page an entity judged by its stamp."""
        now = datetime(2026, 10, 29, 5, 0, tzinfo=UTC)
        stopped = now - timedelta(hours=7)
        watermarks, stamps = _moved(
            now, orders=(datetime(2026, 10, 28, 20, 24, tzinfo=UTC), stopped))
        (page,) = _orders(_judge(now, watermarks, stamps))
        assert page.severity == Severity.CRITICAL
        assert page.count == 7
        assert "7.0h ago" in page.description
        assert "meta.chain_watermarks" in page.description
        assert "2026-10-28T20:24:00+00:00" in page.description

    def test_the_stamp_decides_even_beside_a_fresh_value(self):
        """Mutation: judge whichever of the two clocks is newer. A value that
        looks fresh — KeyCRM's clock running ahead, the `sync_to` branch —
        does not excuse a row nobody has written for seven hours."""
        now = datetime(2026, 10, 29, 12, 0, tzinfo=UTC)
        watermarks, stamps = _moved(
            now, orders=(now - timedelta(minutes=10), now - timedelta(hours=7)))
        assert [p.count for p in _orders(_judge(now, watermarks, stamps))] == [7]

    def test_under_six_hours_since_the_last_write_is_quiet(self):
        now = datetime(2026, 10, 29, 12, 0, tzinfo=UTC)
        watermarks, stamps = _moved(
            now, orders=(now - timedelta(hours=9),
                         now - timedelta(hours=5, minutes=50)))
        assert _orders(_judge(now, watermarks, stamps)) == []


class TestOnlyTheSourceClockMoves:
    def test_other_entities_are_still_judged_by_their_value(self):
        """Their value is the sync's own `now()`; a stamp beside it changes
        nothing. Mutation: judge every entity by its stamp."""
        now = datetime(2026, 10, 29, 12, 0, tzinfo=UTC)
        watermarks, stamps = _moved(
            now, offers=(now - timedelta(hours=60), now - timedelta(minutes=1)))
        assert sorted(i.check_name for i in _judge(now, watermarks, stamps)) == [
            "freshness_offers"]

    def test_the_set_is_orders_alone(self):
        assert dq.JUDGED_BY_LAST_WRITE == frozenset({"orders"})
        assert dq.JUDGED_BY_LAST_WRITE <= set(dq.FRESHNESS_THRESHOLDS)

    def test_the_inherited_stand_in_is_judged_by_its_value(self):
        """No row in Postgres yet: DuckDB's frozen stamp stands in, frozen on
        both clocks, and is judged as it always was."""
        now = datetime(2026, 10, 29, 12, 0, tzinfo=UTC)
        watermarks, stamps = _moved(now)
        watermarks.pop("last_sync_orders")
        stamps.pop("last_sync_orders")
        rows = {"last_sync_orders": (now - timedelta(hours=7)).isoformat()}
        (page,) = _orders(_judge_freshness(rows, now, chain_watermarks=watermarks,
                                           chain_stamps=stamps))
        assert "DuckDB's stamp from before the switch" in page.description


@asynccontextmanager
async def _watermarks(pool, rows):
    """`meta.chain_watermarks` holding `rows` — `{key: (value, updated_at)}` —
    for the block, and as it was after it."""
    keys = list(rows)
    async with pool.acquire() as conn:
        before = await conn.fetch(
            "SELECT key, value, updated_at FROM meta.chain_watermarks "
            "WHERE key = ANY($1)", keys)
        await conn.execute("DELETE FROM meta.chain_watermarks WHERE key = ANY($1)", keys)
        await conn.executemany(
            "INSERT INTO meta.chain_watermarks (key, value, updated_at) VALUES ($1, $2, $3)",
            [(key, value.isoformat(), stamp) for key, (value, stamp) in rows.items()])
    try:
        yield
    finally:
        async with pool.acquire() as conn:
            await conn.execute("DELETE FROM meta.chain_watermarks WHERE key = ANY($1)", keys)
            await conn.executemany(
                "INSERT INTO meta.chain_watermarks (key, value, updated_at) "
                "VALUES ($1, $2, $3)", [tuple(r) for r in before])


class TestTheWiring:
    @needs_postgres
    @pytest.mark.asyncio
    @pytest.mark.parametrize("unwritten_h, filed", [
        (0, []), (9, [("CRITICAL", 9)])], ids=["a_quiet_night", "a_stalled_step"])
    async def test_the_integrity_job_judges_the_order_step_by_its_last_write(
        self, pool, monkeypatch, unwritten_h, filed,
    ):
        """The integrity job over the test Postgres. KeyCRM's newest update in
        `last_sync_orders` is nine hours old; every other sync wrote five
        minutes ago. Rewritten a minute ago, the row is a quiet night and the
        run files no `freshness_orders`; not rewritten for nine hours, the
        order step stalled, CRITICAL at 9 h. Mutation: read the values alone,
        or drop `chain_stamps=` from the job's call — the quiet night pages."""
        from core.scheduler import BackgroundScheduler
        from core.write_chains import sync_keys

        now = datetime.now(timezone.utc)
        rows = {key: (now - timedelta(minutes=5), now - timedelta(minutes=5))
                for key in sync_keys()}
        rows["last_sync_orders"] = (now - timedelta(hours=9),
                                    now - timedelta(hours=unwritten_h, minutes=1))
        async with _watermarks(pool, rows), alert_ledger(monkeypatch):
            result = await BackgroundScheduler()._run_dq_integrity()
            async with pool.acquire() as conn:
                found = await conn.fetch(
                    "SELECT severity, count FROM app.data_quality_issues "
                    "WHERE run_id = $1 AND check_name = 'freshness_orders'",
                    result["run_id"])

        assert result["run_id"] is not None
        assert [tuple(r) for r in found] == filed

    def test_the_scan_judges_the_order_step_by_its_last_write(self):
        """The integrity scan itself, on a quiet night: KeyCRM's newest update
        nine hours old, the row rewritten a minute ago. Without the stamps the
        same night pages, as production did. Mutation: drop `chain_stamps=`
        from the scan's call of the freshness check."""
        now = datetime.now(UTC)
        watermarks, stamps = _moved(
            now, orders=(now - timedelta(hours=9), now - timedelta(minutes=1)))
        assert _orders(dq.check_internal_integrity(
            chain_watermarks=watermarks, chain_stamps=stamps)) == []
        (page,) = _orders(dq.check_internal_integrity(chain_watermarks=watermarks))
        assert (page.severity, page.count) == (Severity.CRITICAL, 9)
