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

import ast
import inspect
import textwrap
from datetime import datetime, timedelta, timezone

import pytest

from core import data_quality as dq
from core.data_quality import Severity, _judge_freshness

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
    from core.write_chains import stood_down_sync_keys

    fresh = (now - timedelta(minutes=5)).isoformat()
    watermarks = {key: fresh for key in stood_down_sync_keys()}
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


class TestTheWiring:
    def test_the_integrity_job_reads_the_stamps_and_passes_them(self):
        """One statement for both, so a value and its stamp are one row's.
        Mutation: read the values alone, or drop `chain_stamps=` — the check
        goes back to judging KeyCRM's clock."""
        from core.scheduler import BackgroundScheduler

        tree = ast.parse(textwrap.dedent(
            inspect.getsource(BackgroundScheduler._run_dq_integrity)))
        calls = [n for n in ast.walk(tree) if isinstance(n, ast.Call)]
        called = {c.func.id for c in calls if isinstance(c.func, ast.Name)}
        assert "read_values_and_stamps" in called
        scan = [c for c in calls if isinstance(c.func, ast.Name)
                and c.func.id == "check_internal_integrity"]
        assert scan and all(
            {"chain_watermarks", "chain_stamps"} <= {k.arg for k in c.keywords}
            for c in scan)

    @pytest.mark.asyncio
    async def test_the_scan_hands_them_to_the_check(self, tmp_path, monkeypatch):
        from core.duckdb_store import DuckDBStore

        seen = {}

        def spy(conn, now=None, **kwargs):
            seen.update(kwargs)
            return []

        monkeypatch.setattr(dq, "_freshness_check", spy)
        store = DuckDBStore(db_path=tmp_path / "scan.duckdb")
        await store.connect()
        stamps = {"last_sync_orders": "2026-10-29T04:59:00+00:00"}
        try:
            async with store.connection() as conn:
                dq.check_internal_integrity(conn, chain_watermarks={},
                                            chain_stamps=stamps)
        finally:
            await store.close()
        assert seen["chain_stamps"] is stamps
