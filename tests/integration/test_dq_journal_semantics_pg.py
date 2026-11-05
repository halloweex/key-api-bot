"""The data-quality journal's statements, on the one engine that runs them.

`core.data_quality` holds the journal's texts — the run's status and counts
(`run_values`), the latest run, the previous one, the digest's baseline, the
layer ages, the findings worst first — and chain 9 runs them on Postgres
(`core.pg_dq_journal_write`). Until step 14's S14-10 their semantics were
proved on DuckDB (`tests/unit/test_data_quality_persistence.py` and
`test_dq_digest.py`'s `TestFetchPreviousRun`), through DuckDB's `persist_run`
and `fetch_*` readers, which S14-10 deleted. The texts are the same; these are
those tests' cases, written through the journal's door and read through the
chain's readers, against a real Postgres.
"""
from __future__ import annotations

import json
import os
from datetime import date, datetime, timedelta, timezone
from unittest.mock import AsyncMock, patch

import asyncpg
import pytest
import pytest_asyncio

from core import dq_journal, pg_dq_journal_write as chain
from core.data_quality import (
    WATCHED_LAYERS, Discrepancy, DiscrepancyClass, IntegrityIssue, Severity,
)

DSN = os.getenv("KS_PG_DSN")
pytestmark = pytest.mark.skipif(not DSN, reason="needs a live PostgreSQL at KS_PG_DSN")


async def _clean(pool):
    """The journal and the chain's owner rows — the throwaway database's."""
    async with pool.acquire() as conn:
        for t in reversed(chain.CHAIN_TABLES):
            await conn.execute(f"DELETE FROM {t}")
        await conn.execute("DELETE FROM meta.chain_watermarks WHERE key LIKE 'owner:%'")


@pytest_asyncio.fixture
async def pool(monkeypatch):
    monkeypatch.setenv("KS_PG_DSN", DSN)
    pool = await asyncpg.create_pool(DSN, min_size=1, max_size=3)
    await _clean(pool)
    with patch("core.pg.get_pool", new=AsyncMock(return_value=pool)), \
         patch("core.pg.require_revision", new=AsyncMock()):
        yield pool
    await _clean(pool)
    await pool.close()


def _now_pair():
    started = datetime.now(timezone.utc)
    return started, started + timedelta(seconds=3)


async def _run(started, *, layer="combined", ended=None, issues=(), diffs=(),
               error_message=None):
    return await dq_journal.journal_run(
        started_at=started, ended_at=ended or started + timedelta(seconds=1),
        as_of=started, window_start=date(2026, 1, 1), window_end=date(2026, 5, 1),
        layer=layer, issues=list(issues), discrepancies=list(diffs),
        error_message=error_message)


async def _latest(layer=None):
    async with chain.reading() as conn:
        return await chain.latest_run(conn, layer)


class TestARun:
    @pytest.mark.asyncio
    async def test_an_empty_run_passes(self, pool):
        """A run with no issues and no discrepancies → status PASS."""
        started, ended = _now_pair()
        run_id = await _run(started, ended=ended)
        assert run_id > 0
        row = await _latest()
        assert row["run_id"] == run_id
        assert row["status"] == "PASS"
        assert (row["integrity_issues_count"], row["discrepancies_count"],
                row["critical_count"]) == (0, 0, 0)
        assert row["layer"] == "combined"

    @pytest.mark.asyncio
    async def test_a_critical_severity_is_the_runs_status(self, pool):
        started, ended = _now_pair()
        run_id = await _run(
            started, ended=ended,
            issues=[IntegrityIssue(check_name="fk_orphan_order_products_order_id",
                                   table_name="order_products", severity=Severity.CRITICAL,
                                   count=3, sample_ids=(1, 2, 3), description="3 orphan rows")],
            diffs=[Discrepancy(month="2026-04", source_id=1,
                               diff_class=DiscrepancyClass.MISSING_IN_DK, field="orders",
                               dk_value=0, kc_value=5, severity=Severity.CRITICAL)])
        row = await _latest()
        assert row["status"] == "CRITICAL" and row["critical_count"] == 2
        async with pool.acquire() as conn:
            assert await conn.fetchval(
                "SELECT COUNT(*) FROM app.data_quality_issues WHERE run_id = $1", run_id) == 1
            assert await conn.fetchval(
                "SELECT COUNT(*) FROM app.data_quality_diffs WHERE run_id = $1", run_id) == 1

    @pytest.mark.asyncio
    async def test_an_error_makes_the_run_failed_whatever_it_found(self, pool):
        started, ended = _now_pair()
        await _run(started, ended=ended, layer="reconciliation",
                   error_message="KeyCRM API 500")
        row = await _latest()
        assert row["status"] == "FAILED" and row["error_message"] == "KeyCRM API 500"

    @pytest.mark.asyncio
    async def test_sample_ids_are_stored_as_json(self, pool):
        started, ended = _now_pair()
        run_id = await _run(
            started, ended=ended, layer="integrity",
            issues=[IntegrityIssue(check_name="pk_uniqueness_orders", table_name="orders",
                                   severity=Severity.CRITICAL, count=2,
                                   sample_ids=(101, 202), description="2 dupes")])
        async with pool.acquire() as conn:
            stored = await conn.fetchval(
                "SELECT sample_ids FROM app.data_quality_issues WHERE run_id = $1", run_id)
        assert json.loads(stored) == [101, 202]

    @pytest.mark.asyncio
    async def test_the_duration_is_computed(self, pool):
        started = datetime.now(timezone.utc)
        await _run(started, ended=started + timedelta(milliseconds=2_500))
        assert 2000 <= (await _latest())["duration_ms"] <= 3000


class TestTheLatestRun:
    @pytest.mark.asyncio
    async def test_none_when_empty(self, pool):
        assert await _latest() is None

    @pytest.mark.asyncio
    async def test_the_most_recent(self, pool):
        await _run(datetime.now(timezone.utc) - timedelta(hours=2))
        new_id = await _run(datetime.now(timezone.utc))
        assert (await _latest())["run_id"] == new_id

    @pytest.mark.asyncio
    async def test_by_layer(self, pool):
        base = datetime.now(timezone.utc)
        integrity_id = await _run(base, layer="integrity")
        reconciliation_id = await _run(base + timedelta(minutes=1), layer="reconciliation")
        assert (await _latest("integrity"))["run_id"] == integrity_id
        assert (await _latest("reconciliation"))["run_id"] == reconciliation_id


class TestThePreviousRun:
    """`test_dq_digest.py`'s `TestFetchPreviousRun`, on Postgres."""

    @staticmethod
    async def _previous(layer, before):
        async with chain.reading() as conn:
            return await chain.previous_run(conn, layer, before)

    @staticmethod
    def _issue(count):
        return IntegrityIssue(check_name="headline_vs_line_items",
                              table_name="silver_orders", severity=Severity.WARN,
                              count=count, sample_ids=(1, 2, 3))

    @pytest.mark.asyncio
    async def test_failed_runs_are_skipped(self, pool):
        """A failed run's zero counts would read as "fixed, then broke
        again", turning every 429 into a fake recovery."""
        t0 = datetime.now(timezone.utc) - timedelta(days=2)
        first = await _run(t0, ended=t0, layer="integrity", issues=[self._issue(1182)])
        await _run(t0 + timedelta(days=1), ended=t0 + timedelta(days=1), layer="integrity",
                   error_message="KeyCRMAPIError: 429")
        current = await _run(t0 + timedelta(days=2), ended=t0 + timedelta(days=2),
                             layer="integrity", issues=[self._issue(1184)])
        previous = await self._previous("integrity", current)
        assert previous["run_id"] == first
        async with chain.reading() as conn:
            issues = await chain.run_issues(conn, previous["run_id"])
        assert issues[0]["count"] == 1182

    @pytest.mark.asyncio
    async def test_none_when_there_is_no_earlier_run(self, pool):
        now = datetime.now(timezone.utc)
        run_id = await _run(now, ended=now, layer="integrity")
        assert await self._previous("integrity", run_id) is None

    @pytest.mark.asyncio
    async def test_layers_are_not_crossed(self, pool):
        now = datetime.now(timezone.utc)
        await _run(now, ended=now, layer="reconciliation")
        integrity_run = await _run(now, ended=now, layer="integrity")
        assert await self._previous("integrity", integrity_run) is None


class TestTheLayerAges:
    """Freshness for the run-age watchdog — keyed on success, not on rows."""

    @pytest.mark.asyncio
    async def test_failed_runs_do_not_count_as_freshness(self, pool):
        """The bug this exists for: 57 failed runs in a row read as 'ran recently'."""
        old = datetime.now(timezone.utc) - timedelta(days=3)
        recent = datetime.now(timezone.utc) - timedelta(minutes=5)
        await _run(old, layer="reconciliation_pg")
        await _run(recent, layer="reconciliation_pg", error_message="429 Too Many Attempts")
        recon = (await dq_journal.last_success_ages_pg())["reconciliation_pg"]
        assert recon["age_seconds"] > 2 * 86400
        returned = datetime.fromisoformat(recon["last_success_at"])
        assert abs((returned - old).total_seconds()) < 1

    @pytest.mark.asyncio
    async def test_a_layer_never_run_reports_a_null_age(self, pool):
        """Every watched layer has a key, a layer with no success a null age."""
        await _run(datetime.now(timezone.utc), layer="integrity")
        ages = await dq_journal.last_success_ages_pg()
        assert set(ages) == set(WATCHED_LAYERS)
        assert ages["integrity"]["age_seconds"] is not None
        assert ages["integrity"]["age_seconds"] < 60
        assert ages["reconciliation_pg"] == {"last_success_at": None, "age_seconds": None}
        # The 05:30 job's DuckDB layer left the watched ones in step 14's S14-12.
        assert "reconciliation" not in ages

    @pytest.mark.asyncio
    async def test_warn_and_critical_runs_still_count_as_fresh(self, pool):
        """A check that found problems still ran — only an errored run did not."""
        await _run(datetime.now(timezone.utc), layer="integrity",
                   issues=[IntegrityIssue(check_name="orphan_products",
                                          table_name="order_products",
                                          severity=Severity.CRITICAL, count=3,
                                          sample_ids=(1, 2, 3), description="orphans")])
        assert (await dq_journal.last_success_ages_pg())["integrity"]["age_seconds"] is not None


class TestTheDigestBaselineIsWhatYouLastRead:
    """`(=)` must mean "unchanged since the last digest", not "since the last
    run". The integrity layer runs four times a day and the digest goes out
    once, so the old comparison covered six hours of a twenty-four hour
    reporting cycle — and a finding that moved and settled inside that gap was
    reported unchanged on both sides of it.
    """

    # The integrity layer's real slots. `dq_integrity_check` is scheduled at
    # 01/07/13/19 **Kyiv**, which is 22/04/10/16 UTC, and the digest goes out
    # at 09:00 Kyiv = 06:00 UTC. Writing the Kyiv numbers as UTC here would
    # put the digest between the wrong pair of runs and prove nothing.
    SLOTS_UTC = [
        datetime(2026, 9, 13, 22, 0, tzinfo=timezone.utc),
        datetime(2026, 9, 14, 4, 0, tzinfo=timezone.utc),
        datetime(2026, 9, 14, 10, 0, tzinfo=timezone.utc),
        datetime(2026, 9, 14, 16, 0, tzinfo=timezone.utc),
        datetime(2026, 9, 14, 22, 0, tzinfo=timezone.utc),
        datetime(2026, 9, 15, 4, 0, tzinfo=timezone.utc),
    ]
    # 09:00 Kyiv on 2026-09-14.
    SENT_AT = datetime(2026, 9, 14, 6, 0, tzinfo=timezone.utc)

    @classmethod
    async def _layer_with_runs(cls, counts, layer="integrity"):
        """One run per count, in the layer's real slots. Returns run ids."""
        ids = []
        for i, count in enumerate(counts):
            started = cls.SLOTS_UTC[i]
            ids.append(await _run(
                started, ended=started + timedelta(seconds=3), layer=layer,
                issues=[IntegrityIssue(check_name="goods_shipped_without_sale",
                                       table_name="orders", severity=Severity.INFO,
                                       count=count,
                                       description="shipments without a sale")]))
        return ids

    @staticmethod
    async def _both(layer, today, sent_at):
        async with chain.reading() as conn:
            stale = await chain.previous_run(conn, layer, today)
            fresh = await chain.baseline_run(conn, layer, sent_at=sent_at,
                                             before_run_id=today)
        return stale, fresh

    @pytest.mark.asyncio
    async def test_a_step_between_two_digests_is_not_reported_unchanged(self, pool):
        """The 2026-09-14 case: 809 at 01:00 and 07:00 Kyiv, then 812 from
        13:00 onward. The digest goes out at 09:00, so it reports the 07:00
        run; next morning the run before the 07:00 one is also 812, and the
        old baseline printed "="."""
        ids = await self._layer_with_runs([809, 809, 812, 812, 812, 812])
        stale, fresh = await self._both("integrity", ids[-1], self.SENT_AT)
        assert stale["run_id"] == ids[-2]
        assert fresh["run_id"] == ids[1]

    @pytest.mark.asyncio
    async def test_a_layer_that_runs_once_a_day_is_unaffected(self, pool):
        """`mirror_landing` and `reconciliation_pg` fire once daily, so their
        previous run already *is* the previous digest."""
        base = datetime(2026, 9, 13, 4, 30, tzinfo=timezone.utc)   # 07:30 Kyiv
        ids = [await _run(base + timedelta(days=i), layer="mirror_landing",
                          ended=base + timedelta(days=i, seconds=3)) for i in range(3)]
        stale, fresh = await self._both("mirror_landing", ids[-1], self.SENT_AT)
        assert stale["run_id"] == fresh["run_id"] == ids[1]

    @pytest.mark.asyncio
    async def test_no_marker_falls_back_to_the_previous_run(self, pool):
        """The first digest with no marker to derive from reads as an
        ordinary morning, not every finding `new`."""
        ids = await self._layer_with_runs([809, 809, 812])
        stale, fresh = await self._both("integrity", ids[-1], None)
        assert fresh["run_id"] == stale["run_id"]

    @pytest.mark.asyncio
    async def test_a_marker_older_than_every_run_falls_back(self, pool):
        """A layer younger than the marker has no run that finished before
        it; returning None would print `new` against every finding."""
        ids = await self._layer_with_runs([809, 812])
        stale, fresh = await self._both("integrity", ids[-1],
                                        datetime(2026, 1, 1, tzinfo=timezone.utc))
        assert fresh["run_id"] == stale["run_id"]

    @pytest.mark.asyncio
    async def test_a_run_still_in_flight_at_send_is_not_the_baseline(self, pool):
        """The baseline keys on `ended_at`: a run that had begun but not
        finished when the digest went out cannot have been in it."""
        early = datetime(2026, 9, 14, 1, 0, tzinfo=timezone.utc)
        finished = await _run(early, ended=early + timedelta(seconds=3), layer="integrity")
        straddling = await _run(early + timedelta(hours=4), ended=early + timedelta(hours=8),
                                layer="integrity")
        today = await _run(early + timedelta(hours=30),
                           ended=early + timedelta(hours=30, seconds=3), layer="integrity")
        _, fresh = await self._both("integrity", today, early + timedelta(hours=6))
        assert fresh["run_id"] == finished != straddling

    def test_the_two_rankings_ask_the_same_question(self):
        """The baseline bounds on `run_id`; the latest run ranks the same rows.
        Ranking by one key while bounding with another is right only while
        the two never disagree — verified identical on all 350 production
        rows, and made structural here."""
        import inspect

        import core.data_quality as dq

        src = inspect.getsource(chain.latest_run)
        for name in ("LATEST_RUN_SQL", "LATEST_RUN_OF_LAYER_SQL"):
            assert name in src, f"latest_run no longer reads {name}"
            assert "ORDER BY run_id DESC" in getattr(dq, name), name


class TestTheWorstComesFirst:
    """`severity` is text, and `ORDER BY severity DESC` ranked it WARN, INFO,
    CRITICAL — the worst finding last in the digest, and the first to fall
    past the digest's LIMIT 20 or into its "…+N more"."""

    @staticmethod
    async def _read():
        started, ended = _now_pair()
        # Names chosen so that alphabetical order would not rescue it either.
        issues = [IntegrityIssue(check_name=name, table_name="orders", severity=sev,
                                 count=1)
                  for name, sev in (("a_info", Severity.INFO), ("b_warn", Severity.WARN),
                                    ("z_critical", Severity.CRITICAL))]
        diffs = [Discrepancy(month="2026-09", source_id=src,
                             diff_class=DiscrepancyClass.VALUE_MISMATCH, field="revenue",
                             dk_value=1, kc_value=2, severity=sev)
                 for src, sev in ((1, Severity.INFO), (2, Severity.WARN),
                                  (4, Severity.CRITICAL))]
        run_id = await _run(started, ended=ended, layer="mirror_landing",
                            issues=issues, diffs=diffs)
        async with chain.reading() as conn:
            return (
                [r["severity"] for r in await chain.run_issues(conn, run_id)],
                [r["severity"] for r in await chain.run_issues(conn, run_id, 1)],
                [r["severity"] for r in await chain.run_diffs(conn, run_id)],
                [r["severity"] for r in await chain.run_diffs(conn, run_id, 1)],
            )

    @pytest.mark.asyncio
    async def test_issues_are_read_worst_first(self, pool):
        every, first, _, _ = await self._read()
        assert every == ["CRITICAL", "WARN", "INFO"] and first == ["CRITICAL"]

    @pytest.mark.asyncio
    async def test_diffs_are_read_worst_first(self, pool):
        _, _, every, first = await self._read()
        assert every == ["CRITICAL", "WARN", "INFO"] and first == ["CRITICAL"]
