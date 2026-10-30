"""Tests for evaluate_memory, the cgroup reader, and the job that pages on them.

`memory_samples`' own statements — the last sample across a recreate, the
peak over a window, the prune — were proved here on DuckDB until step 14's
S14-10 deleted DuckDB's writer and readers of them; the same cases run on
Postgres in `tests/integration/test_watchdog_samples_pg.py`
(`TestTheMemoryStatements`).

Fixtures are production readings with the date they were taken, for the reason
spelled out in test_disk_monitor.py: this suite exists because a previous one
stayed green while asserting a world that had ended.

Readings below were taken from keycrm-web on 2026-08-12 with the container
under normal load: anon 650 MB, file 677 MB, memory.current 1.34 GB, limit
7 GB, oom_kill 0. Page cache was 50.3% of the number the old alert judged.
"""
from __future__ import annotations

import math

from pathlib import Path
from types import SimpleNamespace

import pytest

from core.data_quality import Severity
from core.memory_monitor import (
    CRITICAL_PCT,
    WARN_PCT,
    evaluate_memory,
    read_cgroup_memory,
)

MB = 1024 * 1024
GB = 1024 * MB

# keycrm-web, 2026-08-12, normal load.
LIVE_ANON = 650 * MB
LIVE_CACHE = 677 * MB
LIVE_LIMIT = 7 * GB


class TestPageCacheIsNotUsage:
    def test_the_live_reading_is_quiet(self):
        """anon 650 MB of a 7 GB limit is 9%. Nothing to say."""
        assert evaluate_memory(
            working_set_bytes=LIVE_ANON,
            page_cache_bytes=LIVE_CACHE,
            limit_bytes=LIVE_LIMIT,
            oom_kills=0,
            previous_oom_kills=0,
        ) is None

    def test_cache_alone_never_triggers(self):
        """A container streaming a large file fills the page cache to the brim.
        The kernel reclaims it on demand; it is not pressure, and the whole
        point of this module is that it is no longer counted."""
        assert evaluate_memory(
            working_set_bytes=600 * MB,
            page_cache_bytes=6 * GB,     # would be 94% under the old maths
            limit_bytes=LIVE_LIMIT,
            oom_kills=0,
            previous_oom_kills=0,
        ) is None

    def test_the_same_bytes_as_working_set_do_fire(self):
        """The distinction is the whole module: 6 GB of cache is a Tuesday,
        6 GB of anon is 86% of the cap and worth saying out loud."""
        alert = evaluate_memory(
            working_set_bytes=6 * GB,
            page_cache_bytes=100 * MB,
            limit_bytes=LIVE_LIMIT,
            oom_kills=0,
            previous_oom_kills=0,
        )
        assert alert is not None
        assert alert.severity == Severity.WARN
        assert "page cache" in alert.reason


class TestThresholds:
    def test_duckdbs_own_ceiling_is_not_an_alert(self):
        """DUCKDB_MEMORY_LIMIT is 4 GB against a 7 GB cap. An engine using its
        whole configured allowance is behaving, not failing."""
        assert evaluate_memory(
            working_set_bytes=4 * GB,
            page_cache_bytes=500 * MB,
            limit_bytes=LIVE_LIMIT,
            oom_kills=0,
            previous_oom_kills=0,
        ) is None

    def test_warn_at_the_threshold(self):
        alert = evaluate_memory(
            working_set_bytes=math.ceil(WARN_PCT * LIVE_LIMIT),
            page_cache_bytes=0,
            limit_bytes=LIVE_LIMIT,
            oom_kills=0,
            previous_oom_kills=0,
        )
        assert alert is not None
        assert alert.severity == Severity.WARN

    def test_just_below_warn_is_silent(self):
        assert evaluate_memory(
            working_set_bytes=math.ceil(WARN_PCT * LIVE_LIMIT) - MB,
            page_cache_bytes=0,
            limit_bytes=LIVE_LIMIT,
            oom_kills=0,
            previous_oom_kills=0,
        ) is None

    def test_critical_at_the_threshold(self):
        alert = evaluate_memory(
            working_set_bytes=math.ceil(CRITICAL_PCT * LIVE_LIMIT),
            page_cache_bytes=0,
            limit_bytes=LIVE_LIMIT,
            oom_kills=0,
            previous_oom_kills=0,
        )
        assert alert is not None
        assert alert.severity == Severity.CRITICAL

    def test_headroom_is_what_is_left_now(self):
        """Not limit minus a monotonic peak, which only ever shrinks."""
        alert = evaluate_memory(
            working_set_bytes=6 * GB,
            page_cache_bytes=0,
            limit_bytes=7 * GB,
            oom_kills=0,
            previous_oom_kills=0,
        )
        assert alert.headroom_mb == pytest.approx(1024, abs=1)

    def test_no_limit_means_no_percentage(self):
        assert evaluate_memory(
            working_set_bytes=50 * GB,
            page_cache_bytes=0,
            limit_bytes=None,
            oom_kills=0,
            previous_oom_kills=0,
        ) is None


class TestOomKills:
    """The only unambiguous signal: not a prediction that memory might run out,
    a record that it did and the kernel picked a victim."""

    def test_a_kill_outranks_a_healthy_reading(self):
        alert = evaluate_memory(
            working_set_bytes=500 * MB,   # 7% — nothing wrong right now
            page_cache_bytes=0,
            limit_bytes=LIVE_LIMIT,
            oom_kills=1,
            previous_oom_kills=0,
        )
        assert alert is not None
        assert alert.severity == Severity.CRITICAL
        assert alert.oom_kills_delta == 1

    def test_a_kill_is_reported_once_not_forever(self):
        """Second sample, same counter: already reported."""
        assert evaluate_memory(
            working_set_bytes=500 * MB,
            page_cache_bytes=0,
            limit_bytes=LIVE_LIMIT,
            oom_kills=1,
            previous_oom_kills=1,
        ) is None

    def test_a_counter_reset_is_a_restart_not_a_kill(self):
        """memory.events resets when the container is recreated. Reading that
        as three deaths would be the same class of error as reading page cache
        as usage."""
        assert evaluate_memory(
            working_set_bytes=500 * MB,
            page_cache_bytes=0,
            limit_bytes=LIVE_LIMIT,
            oom_kills=0,
            previous_oom_kills=3,
        ) is None

    def test_a_kill_before_the_first_sample_still_counts(self):
        alert = evaluate_memory(
            working_set_bytes=500 * MB,
            page_cache_bytes=0,
            limit_bytes=LIVE_LIMIT,
            oom_kills=2,
            previous_oom_kills=None,
        )
        assert alert is not None
        assert alert.oom_kills_delta == 2

    def test_a_kill_fires_without_a_limit(self):
        alert = evaluate_memory(
            working_set_bytes=500 * MB,
            page_cache_bytes=0,
            limit_bytes=None,
            oom_kills=1,
            previous_oom_kills=0,
        )
        assert alert is not None
        assert alert.severity == Severity.CRITICAL


class TestCgroupReader:
    def _write(self, root: Path, **files):
        root.mkdir(parents=True, exist_ok=True)
        for name, body in files.items():
            (root / name.replace("__", ".")).write_text(body)

    def test_reads_the_working_set_not_the_total(self, tmp_path):
        self._write(
            tmp_path,
            memory__current=str(1_341_079_552),
            memory__max=str(LIVE_LIMIT),
            memory__stat=f"anon {LIVE_ANON}\nfile {LIVE_CACHE}\nsock 0\nslab_unreclaimable {10 * MB}\n",
            memory__events="low 0\nhigh 0\nmax 0\noom 0\noom_kill 0\n",
        )
        mem = read_cgroup_memory(str(tmp_path))
        assert mem["working_set"] == LIVE_ANON + 10 * MB
        assert mem["page_cache"] == LIVE_CACHE
        assert mem["current"] == 1_341_079_552
        assert mem["limit"] == LIVE_LIMIT
        assert mem["oom_kills"] == 0

    def test_max_means_unlimited(self, tmp_path):
        self._write(
            tmp_path,
            memory__current="1000",
            memory__max="max",
            memory__stat="anon 800\nfile 200\n",
        )
        assert read_cgroup_memory(str(tmp_path))["limit"] is None

    def test_without_memory_stat_it_over_reports_rather_than_under(self, tmp_path):
        """A missing stat file must not yield a zero working set: that would
        read as perfectly healthy, which is the worst way for this to fail."""
        self._write(tmp_path, memory__current="5000", memory__max="10000")
        mem = read_cgroup_memory(str(tmp_path))
        assert mem["working_set"] == 5000
        assert mem["page_cache"] == 0

    def test_not_in_a_container_returns_none(self, tmp_path):
        assert read_cgroup_memory(str(tmp_path / "nope")) is None

    def test_counts_oom_kills(self, tmp_path):
        self._write(
            tmp_path,
            memory__current="1000",
            memory__max="10000",
            memory__stat="anon 800\nfile 200\n",
            memory__events="low 0\nhigh 2\nmax 5\noom 3\noom_kill 2\n",
        )
        assert read_cgroup_memory(str(tmp_path))["oom_kills"] == 2


def _sample(working_mb: float, cache_mb: float = 100, oom: int = 0) -> dict:
    return {
        "working_set": int(working_mb * MB),
        "page_cache": int(cache_mb * MB),
        "limit": LIVE_LIMIT,
        "oom_kills": oom,
    }


class TestTheWebWatchdogSendsWhatItJudges:
    """The scheduler's half: a verdict of `evaluate_memory` reaching a human.

    Everything above tests the verdict, and the one test of the job itself
    replaces the evaluation so the alert path is never reached. On 2026-09-14
    the retirement of the H3 bronze subsystem (afaaab19) deleted the two class
    attributes that path reads — the per-level cooldowns and the clock of the
    last alert — together with the bronze alert above them. From then on every
    WARN, CRITICAL and OOM kill of the web container raised AttributeError in
    a job nobody is paged for, and the watchdog said nothing at exactly the
    moments it exists for. Found by the type checker, not by a test.
    """

    @pytest.fixture
    def watch(self, monkeypatch):
        from unittest.mock import AsyncMock

        from core import alerting, memory_monitor, watchdog_samples
        from core.scheduler import BackgroundScheduler

        reading = {"mem": _sample(650)}
        monkeypatch.setattr(memory_monitor, "read_cgroup_memory", lambda: reading["mem"])
        # The previous stored sample: no kill recorded yet.
        monkeypatch.setattr(watchdog_samples, "memory_tick", AsyncMock(
            return_value={"last": {"oom_kills": 0}, "peak_24h": None}))
        sent = AsyncMock(return_value=True)
        monkeypatch.setattr(alerting, "raise_alert", sent)
        monkeypatch.setattr(alerting, "resolve_group", AsyncMock())
        monkeypatch.setattr(BackgroundScheduler, "_get_db_size_mb", staticmethod(lambda: None))

        def at(working_mb, oom=0):
            reading["mem"] = _sample(working_mb, oom=oom)

        return SimpleNamespace(scheduler=BackgroundScheduler(), at=at, sent=sent)

    @pytest.mark.asyncio
    async def test_a_critical_reading_is_sent(self, watch):
        """Mutation: delete the cooldown table again — AttributeError, nothing sent."""
        watch.at(0.95 * LIVE_LIMIT / MB)
        result = await watch.scheduler._run_memory_monitor()

        assert result["alert_sent"] == "CRITICAL", result
        watch.sent.assert_awaited_once()
        assert watch.sent.await_args.kwargs["conditions"] == ["memory:web:CRITICAL"]

    @pytest.mark.asyncio
    async def test_the_same_level_inside_its_cooldown_is_held(self, watch):
        """A WARN waits 24 h and a CRITICAL 6 h before it is sent again —
        the documented pre-OOM exception to the Gate's daily decay."""
        watch.at(0.85 * LIVE_LIMIT / MB)
        first = await watch.scheduler._run_memory_monitor()
        second = await watch.scheduler._run_memory_monitor()

        assert first["alert_sent"] == "WARN", first
        assert second.get("alert_suppressed") == "WARN", second
        watch.sent.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_an_oom_kill_is_never_held(self, watch):
        """A kill is a fact about the past: it goes out inside a CRITICAL's
        cooldown, unkeyed, and spools for the diagnostician."""
        watch.at(0.95 * LIVE_LIMIT / MB)
        await watch.scheduler._run_memory_monitor()
        watch.at(0.95 * LIVE_LIMIT / MB, oom=1)
        result = await watch.scheduler._run_memory_monitor()

        assert result["alert_sent"] == "CRITICAL", result
        assert watch.sent.await_count == 2
        assert watch.sent.await_args.kwargs["conditions"] == []
        assert watch.sent.await_args.kwargs["spool_as"] == "memory:web:oom"
