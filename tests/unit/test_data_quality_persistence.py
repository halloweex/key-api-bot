"""Tests for persist_run + fetch_latest_run.

These exercise the new data_quality_runs / _issues / _diffs schema and
verify the round-trip: insert run with issues/diffs, then read it back.
"""
from __future__ import annotations

import json
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pytest

from core.data_quality import (
    WATCHED_LAYERS,
    Discrepancy,
    DiscrepancyClass,
    IntegrityIssue,
    Severity,
    fetch_baseline_run,
    fetch_last_success_ages,
    fetch_latest_run,
    fetch_previous_run,
    persist_run,
)
from core.duckdb_store import DuckDBStore


async def _make_store(tmp_path: Path) -> DuckDBStore:
    s = DuckDBStore(db_path=tmp_path / "test.duckdb")
    await s.connect()
    return s


def _now_pair():
    started = datetime.now(timezone.utc)
    return started, started + timedelta(seconds=3)


class TestPersistRun:
    @pytest.mark.asyncio
    async def test_empty_run_pass(self, tmp_path):
        """A run with no issues and no discrepancies → status PASS."""
        store = await _make_store(tmp_path)
        try:
            started, ended = _now_pair()
            async with store.connection() as conn:
                run_id = persist_run(
                    conn,
                    started_at=started, ended_at=ended,
                    as_of=started,
                    window_start=date(2026, 1, 1), window_end=date(2026, 5, 1),
                    layer="combined",
                    issues=[], discrepancies=[],
                )
                assert run_id > 0
                row = fetch_latest_run(conn)
            assert row is not None
            assert row["run_id"] == run_id
            assert row["status"] == "PASS"
            assert row["integrity_issues_count"] == 0
            assert row["discrepancies_count"] == 0
            assert row["critical_count"] == 0
            assert row["layer"] == "combined"
        finally:
            await store.close()

    @pytest.mark.asyncio
    async def test_critical_severity_propagated_to_status(self, tmp_path):
        store = await _make_store(tmp_path)
        try:
            started, ended = _now_pair()
            issues = [IntegrityIssue(
                check_name="fk_orphan_order_products_order_id",
                table_name="order_products",
                severity=Severity.CRITICAL,
                count=3, sample_ids=(1, 2, 3),
                description="3 orphan rows",
            )]
            discrepancies = [Discrepancy(
                month="2026-04", source_id=1,
                diff_class=DiscrepancyClass.MISSING_IN_DK,
                field="orders", dk_value=0, kc_value=5,
                severity=Severity.CRITICAL,
            )]
            async with store.connection() as conn:
                run_id = persist_run(
                    conn,
                    started_at=started, ended_at=ended,
                    as_of=started,
                    window_start=date(2026, 1, 1), window_end=date(2026, 5, 1),
                    layer="combined",
                    issues=issues, discrepancies=discrepancies,
                )
                row = fetch_latest_run(conn)

                # Children persisted?
                n_issues = conn.execute(
                    "SELECT COUNT(*) FROM data_quality_issues WHERE run_id = ?",
                    [run_id],
                ).fetchone()[0]
                n_diffs = conn.execute(
                    "SELECT COUNT(*) FROM data_quality_diffs WHERE run_id = ?",
                    [run_id],
                ).fetchone()[0]

            assert row["status"] == "CRITICAL"
            assert row["critical_count"] == 2
            assert n_issues == 1
            assert n_diffs == 1
        finally:
            await store.close()

    @pytest.mark.asyncio
    async def test_failed_run_status_overrides_severity(self, tmp_path):
        """When error_message is set, status is FAILED regardless of issues."""
        store = await _make_store(tmp_path)
        try:
            started, ended = _now_pair()
            async with store.connection() as conn:
                persist_run(
                    conn,
                    started_at=started, ended_at=ended,
                    as_of=started,
                    window_start=date(2026, 1, 1), window_end=date(2026, 5, 1),
                    layer="reconciliation",
                    issues=[], discrepancies=[],
                    error_message="KeyCRM API 500",
                )
                row = fetch_latest_run(conn)
            assert row["status"] == "FAILED"
            assert row["error_message"] == "KeyCRM API 500"
        finally:
            await store.close()

    @pytest.mark.asyncio
    async def test_sample_ids_persisted_as_json(self, tmp_path):
        store = await _make_store(tmp_path)
        try:
            started, ended = _now_pair()
            issues = [IntegrityIssue(
                check_name="pk_uniqueness_orders",
                table_name="orders",
                severity=Severity.CRITICAL,
                count=2, sample_ids=(101, 202),
                description="2 dupes",
            )]
            async with store.connection() as conn:
                run_id = persist_run(
                    conn,
                    started_at=started, ended_at=ended,
                    as_of=started,
                    window_start=date(2026, 1, 1), window_end=date(2026, 5, 1),
                    layer="integrity",
                    issues=issues, discrepancies=[],
                )
                stored = conn.execute(
                    "SELECT sample_ids FROM data_quality_issues WHERE run_id = ?",
                    [run_id],
                ).fetchone()[0]
            assert json.loads(stored) == [101, 202]
        finally:
            await store.close()

    @pytest.mark.asyncio
    async def test_duration_ms_computed(self, tmp_path):
        store = await _make_store(tmp_path)
        try:
            started = datetime.now(timezone.utc)
            ended = started + timedelta(milliseconds=2_500)
            async with store.connection() as conn:
                persist_run(
                    conn,
                    started_at=started, ended_at=ended,
                    as_of=started,
                    window_start=date(2026, 1, 1), window_end=date(2026, 5, 1),
                    layer="combined",
                    issues=[], discrepancies=[],
                )
                row = fetch_latest_run(conn)
            assert 2000 <= row["duration_ms"] <= 3000
        finally:
            await store.close()


class TestFetchLatestRun:
    @pytest.mark.asyncio
    async def test_returns_none_when_empty(self, tmp_path):
        store = await _make_store(tmp_path)
        try:
            async with store.connection() as conn:
                assert fetch_latest_run(conn) is None
        finally:
            await store.close()

    @pytest.mark.asyncio
    async def test_returns_most_recent(self, tmp_path):
        store = await _make_store(tmp_path)
        try:
            t1 = datetime.now(timezone.utc) - timedelta(hours=2)
            t2 = datetime.now(timezone.utc)
            async with store.connection() as conn:
                persist_run(
                    conn, started_at=t1, ended_at=t1 + timedelta(seconds=1),
                    as_of=t1, window_start=date(2026, 1, 1), window_end=date(2026, 5, 1),
                    layer="combined", issues=[], discrepancies=[],
                )
                new_id = persist_run(
                    conn, started_at=t2, ended_at=t2 + timedelta(seconds=1),
                    as_of=t2, window_start=date(2026, 1, 1), window_end=date(2026, 5, 1),
                    layer="combined", issues=[], discrepancies=[],
                )
                row = fetch_latest_run(conn)
            assert row["run_id"] == new_id
        finally:
            await store.close()

    @pytest.mark.asyncio
    async def test_filter_by_layer(self, tmp_path):
        store = await _make_store(tmp_path)
        try:
            base = datetime.now(timezone.utc)
            async with store.connection() as conn:
                integrity_id = persist_run(
                    conn, started_at=base, ended_at=base + timedelta(seconds=1),
                    as_of=base, window_start=date(2026, 1, 1), window_end=date(2026, 5, 1),
                    layer="integrity", issues=[], discrepancies=[],
                )
                reconciliation_id = persist_run(
                    conn, started_at=base + timedelta(minutes=1),
                    ended_at=base + timedelta(minutes=1, seconds=1),
                    as_of=base, window_start=date(2026, 1, 1), window_end=date(2026, 5, 1),
                    layer="reconciliation", issues=[], discrepancies=[],
                )
                latest_integrity = fetch_latest_run(conn, layer="integrity")
                latest_recon = fetch_latest_run(conn, layer="reconciliation")

            assert latest_integrity["run_id"] == integrity_id
            assert latest_recon["run_id"] == reconciliation_id
        finally:
            await store.close()


class TestFetchLastSuccessAges:
    """Freshness for the run-age watchdog — keyed on success, not on rows."""

    @pytest.mark.asyncio
    async def test_failed_runs_do_not_count_as_freshness(self, tmp_path):
        """The bug this exists for: 57 failed runs in a row read as 'ran recently'."""
        store = await _make_store(tmp_path)
        try:
            old = datetime.now(timezone.utc) - timedelta(days=3)
            recent = datetime.now(timezone.utc) - timedelta(minutes=5)
            async with store.connection() as conn:
                # A real verdict, three days ago.
                persist_run(
                    conn, started_at=old, ended_at=old + timedelta(seconds=1),
                    as_of=old, window_start=date(2026, 1, 1), window_end=date(2026, 5, 1),
                    layer="reconciliation", issues=[], discrepancies=[],
                )
                # …then nothing but crashes, the most recent five minutes ago.
                persist_run(
                    conn, started_at=recent, ended_at=recent + timedelta(seconds=1),
                    as_of=recent, window_start=date(2026, 1, 1), window_end=date(2026, 5, 1),
                    layer="reconciliation", issues=[], discrepancies=[],
                    error_message="429 Too Many Attempts",
                )
                ages = fetch_last_success_ages(conn)

            recon = ages["reconciliation"]
            # Must reflect the 3-day-old success, not the 5-minute-old failure.
            assert recon["age_seconds"] > 2 * 86400
            # Compare instants, not rendered dates: DuckDB returns TIMESTAMPTZ
            # in the session timezone, whose calendar day can differ from UTC's.
            returned = datetime.fromisoformat(recon["last_success_at"])
            assert abs((returned - old).total_seconds()) < 1
        finally:
            await store.close()

    @pytest.mark.asyncio
    async def test_never_run_layer_reports_null_age(self, tmp_path):
        """A layer with no successes gets a key with a null age, not a missing key."""
        store = await _make_store(tmp_path)
        try:
            base = datetime.now(timezone.utc)
            async with store.connection() as conn:
                persist_run(
                    conn, started_at=base, ended_at=base + timedelta(seconds=1),
                    as_of=base, window_start=date(2026, 1, 1), window_end=date(2026, 5, 1),
                    layer="integrity", issues=[], discrepancies=[],
                )
                ages = fetch_last_success_ages(conn)

            # Every watched layer gets a key, including ones added later.
            assert set(ages) == set(WATCHED_LAYERS)
            assert ages["integrity"]["age_seconds"] is not None
            assert ages["integrity"]["age_seconds"] < 60
            assert ages["reconciliation"] == {
                "last_success_at": None, "age_seconds": None,
            }
        finally:
            await store.close()

    @pytest.mark.asyncio
    async def test_warn_and_critical_runs_still_count_as_fresh(self, tmp_path):
        """A check that found problems still ran — only an errored run did not."""
        store = await _make_store(tmp_path)
        try:
            base = datetime.now(timezone.utc)
            issue = IntegrityIssue(
                check_name="orphan_products", table_name="order_products",
                severity=Severity.CRITICAL, count=3, sample_ids=[1, 2, 3],
                description="orphans",
            )
            async with store.connection() as conn:
                persist_run(
                    conn, started_at=base, ended_at=base + timedelta(seconds=1),
                    as_of=base, window_start=date(2026, 1, 1), window_end=date(2026, 5, 1),
                    layer="integrity", issues=[issue], discrepancies=[],
                )
                ages = fetch_last_success_ages(conn)

            assert ages["integrity"]["age_seconds"] is not None
        finally:
            await store.close()


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
    # put the digest between the wrong pair of runs and prove nothing — this
    # repo has a standing note about fixtures that think in the wrong zone.
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
    async def _layer_with_runs(cls, store, counts, layer="integrity"):
        """One run per count, in the layer's real slots. Returns run ids."""
        ids = []
        async with store.connection() as conn:
            for i, count in enumerate(counts):
                started = cls.SLOTS_UTC[i]
                ids.append(persist_run(
                    conn,
                    started_at=started, ended_at=started + timedelta(seconds=3),
                    as_of=started,
                    window_start=date(2026, 1, 1), window_end=date(2026, 5, 1),
                    layer=layer,
                    issues=[IntegrityIssue(
                        check_name="goods_shipped_without_sale",
                        table_name="orders",
                        severity=Severity.INFO,
                        count=count,
                        description="shipments without a sale",
                    )],
                    discrepancies=[],
                ))
        return ids

    @pytest.mark.asyncio
    async def test_a_step_between_two_digests_is_not_reported_unchanged(
            self, tmp_path):
        """The 2026-09-14 case, reproduced.

        809 at 01:00 and 07:00, then 812 from 13:00 onward. The digest goes
        out at 09:00, so it reports the 07:00 run. Next morning it reports the
        following 07:00 run, by which time the count is 812 — and the run
        before *that* is also 812, so the old baseline printed "=".
        """
        store = await _make_store(tmp_path)
        try:
            # 809 through the 07:00 Kyiv run, 812 from 13:00 Kyiv onward.
            ids = await self._layer_with_runs(
                store, [809, 809, 812, 812, 812, 812])
            sent_at = self.SENT_AT
            today = ids[-1]

            async with store.connection() as conn:
                stale = fetch_previous_run(conn, "integrity", today)
                fresh = fetch_baseline_run(
                    conn, "integrity", sent_at=sent_at, before_run_id=today)

            # What it used to compare against: the 22:00 UTC run, already
            # 812 — so the morning printed "=" over a count that had moved.
            assert stale["run_id"] == ids[-2]
            # What the reader was actually last shown: the run the previous
            # digest reported, still 809. The step becomes visible.
            assert fresh["run_id"] == ids[1]
            assert fresh["run_id"] != stale["run_id"]
        finally:
            await store.close()

    @pytest.mark.asyncio
    async def test_a_layer_that_runs_once_a_day_is_unaffected(self, tmp_path):
        """`mirror_landing` and `reconciliation` fire once daily, so their
        previous run already *is* the previous digest. The change must be a
        no-op there, or it would move two layers nobody asked to move."""
        store = await _make_store(tmp_path)
        try:
            # 07:30 Kyiv = 04:30 UTC, once a day.
            base = datetime(2026, 9, 13, 4, 30, tzinfo=timezone.utc)
            ids = []
            async with store.connection() as conn:
                for i in range(3):
                    started = base + timedelta(days=i)
                    ids.append(persist_run(
                        conn,
                        started_at=started,
                        ended_at=started + timedelta(seconds=3),
                        as_of=started,
                        window_start=date(2026, 1, 1),
                        window_end=date(2026, 5, 1),
                        layer="mirror_landing",
                        issues=[], discrepancies=[],
                    ))
                sent_at = datetime(2026, 9, 14, 6, 0, tzinfo=timezone.utc)
                stale = fetch_previous_run(conn, "mirror_landing", ids[-1])
                fresh = fetch_baseline_run(
                    conn, "mirror_landing",
                    sent_at=sent_at, before_run_id=ids[-1])

            assert stale["run_id"] == fresh["run_id"] == ids[1]
        finally:
            await store.close()

    @pytest.mark.asyncio
    async def test_no_marker_falls_back_to_todays_behaviour(self, tmp_path):
        """The first digest after this ships has no marker to derive from. It
        must read as an ordinary morning, not mark everything `new`: a digest
        that cries wolf once is a digest people learn to skim."""
        store = await _make_store(tmp_path)
        try:
            ids = await self._layer_with_runs(store, [809, 809, 812])
            async with store.connection() as conn:
                stale = fetch_previous_run(conn, "integrity", ids[-1])
                fresh = fetch_baseline_run(
                    conn, "integrity", sent_at=None, before_run_id=ids[-1])
            assert fresh["run_id"] == stale["run_id"]
        finally:
            await store.close()

    @pytest.mark.asyncio
    async def test_a_marker_older_than_every_run_falls_back(self, tmp_path):
        """A layer younger than the marker has no run that finished before it.
        Falling back is right; returning None would print `new` against every
        finding the layer has ever had."""
        store = await _make_store(tmp_path)
        try:
            ids = await self._layer_with_runs(store, [809, 812])
            ancient = datetime(2026, 1, 1, tzinfo=timezone.utc)
            async with store.connection() as conn:
                fresh = fetch_baseline_run(
                    conn, "integrity", sent_at=ancient, before_run_id=ids[-1])
                stale = fetch_previous_run(conn, "integrity", ids[-1])
            assert fresh["run_id"] == stale["run_id"]
        finally:
            await store.close()

    @pytest.mark.asyncio
    async def test_a_run_still_in_flight_at_send_is_not_the_baseline(
            self, tmp_path):
        """The baseline keys on `ended_at`, not `started_at`: a run that had
        begun but not finished when the digest went out cannot have been in
        it, and using it would compare against numbers nobody was shown."""
        store = await _make_store(tmp_path)
        try:
            early = datetime(2026, 9, 14, 1, 0, tzinfo=timezone.utc)
            async with store.connection() as conn:
                finished = persist_run(
                    conn,
                    started_at=early, ended_at=early + timedelta(seconds=3),
                    as_of=early,
                    window_start=date(2026, 1, 1), window_end=date(2026, 5, 1),
                    layer="integrity", issues=[], discrepancies=[],
                )
                # Starts before the marker, ends after it.
                straddling = persist_run(
                    conn,
                    started_at=early + timedelta(hours=4),
                    ended_at=early + timedelta(hours=8),
                    as_of=early,
                    window_start=date(2026, 1, 1), window_end=date(2026, 5, 1),
                    layer="integrity", issues=[], discrepancies=[],
                )
                today = persist_run(
                    conn,
                    started_at=early + timedelta(hours=30),
                    ended_at=early + timedelta(hours=30, seconds=3),
                    as_of=early,
                    window_start=date(2026, 1, 1), window_end=date(2026, 5, 1),
                    layer="integrity", issues=[], discrepancies=[],
                )
                sent_at = early + timedelta(hours=6)
                fresh = fetch_baseline_run(
                    conn, "integrity",
                    sent_at=sent_at, before_run_id=today)

            assert fresh["run_id"] == finished
            assert fresh["run_id"] != straddling
        finally:
            await store.close()

    @pytest.mark.asyncio
    async def test_the_two_rankings_ask_the_same_question(self, tmp_path):
        """`fetch_baseline_run` bounds on `run_id`; `fetch_latest_run` ranks
        the same rows. Ranking by one key while bounding with another is right
        only while the two never disagree — verified identical on all 350
        production rows, and made structural here."""
        import inspect

        import core.data_quality as dq

        src = inspect.getsource(dq.fetch_latest_run)
        assert "ORDER BY run_id DESC" in src, (
            "fetch_latest_run no longer ranks by run_id, so the digest "
            "baseline reconstruction is only coincidentally correct"
        )


class TestTheWorstComesFirst:
    """`severity` is text, and `ORDER BY severity DESC` ranked it WARN, INFO,
    CRITICAL — the worst finding last in the digest, and the first to fall
    past the digest's LIMIT 20 or into its "…+N more"."""

    async def _persist(self, tmp_path):
        from core.data_quality import fetch_run_diffs, fetch_run_issues

        store = await _make_store(tmp_path)
        started, ended = _now_pair()
        # Names chosen so that alphabetical order would not rescue it either.
        issues = [
            IntegrityIssue(check_name=name, table_name="orders",
                           severity=sev, count=1)
            for name, sev in (("a_info", Severity.INFO),
                              ("b_warn", Severity.WARN),
                              ("z_critical", Severity.CRITICAL))
        ]
        diffs = [
            Discrepancy(month="2026-09", source_id=src,
                        diff_class=DiscrepancyClass.VALUE_MISMATCH,
                        field="revenue", dk_value=1, kc_value=2, severity=sev)
            for src, sev in ((1, Severity.INFO), (2, Severity.WARN),
                             (4, Severity.CRITICAL))
        ]
        try:
            async with store.connection() as conn:
                run_id = persist_run(
                    conn, started_at=started, ended_at=ended, as_of=started,
                    window_start=date(2026, 6, 1), window_end=date(2026, 9, 1),
                    layer="mirror_landing", issues=issues, discrepancies=diffs,
                )
                return (
                    [r["severity"] for r in fetch_run_issues(conn, run_id)],
                    [r["severity"] for r in fetch_run_issues(conn, run_id, limit=1)],
                    [r["severity"] for r in fetch_run_diffs(conn, run_id)],
                    [r["severity"] for r in fetch_run_diffs(conn, run_id, limit=1)],
                )
        finally:
            await store.close()

    @pytest.mark.asyncio
    async def test_issues_are_read_worst_first(self, tmp_path):
        every, first, _, _ = await self._persist(tmp_path)
        assert every == ["CRITICAL", "WARN", "INFO"]
        assert first == ["CRITICAL"]

    @pytest.mark.asyncio
    async def test_diffs_are_read_worst_first(self, tmp_path):
        _, _, every, first = await self._persist(tmp_path)
        assert every == ["CRITICAL", "WARN", "INFO"]
        assert first == ["CRITICAL"]
