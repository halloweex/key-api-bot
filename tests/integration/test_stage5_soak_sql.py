"""Stage 5's soak check on the DuckDB file against a real Postgres: P1
(OD-17 (a)).

P1 reads the file's hash record the hourly host cron keeps
(deploy/duckdb_silence_check.sh) and must say FAIL on the breach it exists to
catch, PASS next to it, and UNKNOWN where nobody can say the day was watched.
The harness is `test_stage4_soak_sql.py`'s: one rolled back transaction per
scenario, a read-only check, a fixed Wednesday noon in Kyiv in 2030.

P2, P3 and P4 — the rollback levers, the 14-day parallel period and the week
of silence — and F1, the read-fallback week, were retired with their tests at
step 14's S14-02: the owner closed both clocks on 2026-10-20. P1 stays until
the file is deleted.
"""
from __future__ import annotations

import re
from datetime import datetime, timezone

import pytest

from tests.integration.test_stage4_soak_sql import (  # noqa: F401  (pool is a fixture)
    NOW, SQL_DIR, _uncommented, ago, check, needs_pg, pool, scenario, verdict,
)

P1 = "50_p1_duckdb_file.sql"


def z(dt: datetime) -> str:
    """The silence record's own spelling of an instant."""
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def record(*, last="UNCHANGED", since=None, reason="baseline", checked=None,
           missing=None) -> dict:
    """psql variables as deploy/stage4_soak.sh hands over a file record."""
    return {"duckdb_file_last": last,
            "duckdb_file_since": z(since or ago(days=8)),
            "duckdb_file_since_reason": reason,
            "duckdb_file_checked_at": z(checked or ago(minutes=20)),
            "duckdb_file_missing_at": z(missing) if missing else ""}


# ── the file against the code it reads ────────────────────────────────────────

class TestTheFileReadsWhatTheCodeWrites:
    @staticmethod
    def code(name):
        return _uncommented((SQL_DIR / name).read_text(encoding="utf-8"))

    def test_the_clocks(self):
        """A day, and an hourly cron's record allowed three hours."""
        code = self.code(P1)
        assert re.findall(r"interval\s+'(\d+)\s+hours'\s+AS\s+span", code) == ["24"]
        assert re.findall(r"interval\s+'([^']+)'\s+AS\s+record_max_age\b", code) == ["3 hours"]

    def test_the_record_shape_is_the_scripts(self):
        """P1 parses the instants deploy/duckdb_silence_check.sh writes."""
        script = (SQL_DIR.parent / "duckdb_silence_check.sh").read_text(encoding="utf-8")
        assert "+%Y-%m-%dT%H:%M:%SZ" in script
        assert "'^\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}Z$'" in self.code(P1)


# ── P1: the file ──────────────────────────────────────────────────────────────

@needs_pg
class TestP1DuckDBFile:
    @pytest.mark.asyncio
    @pytest.mark.parametrize("off, last, expected, words", [
        ("0", "none", "PASS", "not applicable"),
        ("0", "UNCHANGED", "PASS", "not applicable"),
        ("invalid", "CHANGED", "PASS", "not applicable"),
        ("unknown", "UNCHANGED", "UNKNOWN", "web is not running"),
        ("1", "none", "UNKNOWN", "no record of the file's hash"),
        ("1", "error", "UNKNOWN", "could not be read"),
    ])
    async def test_the_mode_and_the_record(self, pool, off, last, expected, words):
        async with scenario(pool) as conn:
            v, detail = await verdict(conn, P1, duckdb_off=off, **record(last=last))
        assert (v, words in detail) == (expected, True), detail

    @pytest.mark.asyncio
    @pytest.mark.parametrize("off", ["0", "1", "unknown"])
    async def test_a_missing_file_fails_whatever_the_switch(self, pool, off):
        """Deleting the file is a DROP (OD-11 (a))."""
        async with scenario(pool) as conn:
            v, detail = await verdict(conn, P1, duckdb_off=off, **record(last="MISSING"))
        assert v == "FAIL" and "OD-11" in detail, detail

    @pytest.mark.asyncio
    @pytest.mark.parametrize("off", ["0", "1", "unknown"])
    async def test_a_missing_episode_fails_the_day_it_ends_in(self, pool, off):
        """The file moved away for two checks and back byte for byte: the
        record reads UNCHANGED since its baseline, and only `missing_at` says
        it was gone (review of 02.10). Mutation: drop `fail_was_missing`."""
        async with scenario(pool) as conn:
            v, detail = await verdict(conn, P1, duckdb_off=off,
                                      **record(missing=ago(hours=3)))
        assert v == "FAIL", detail
        assert "was missing at a check inside the day (last 05.06 09:00 Kyiv)" in detail
        assert "OD-11" in detail

    @pytest.mark.asyncio
    async def test_a_missing_episode_over_a_day_ago_is_the_days_before(self, pool):
        """P1 is the day's question: an episode over a day ago failed the day
        it ended in, and does not fail this one."""
        async with scenario(pool) as conn:
            v, detail = await verdict(conn, P1, duckdb_off="1",
                                      **record(missing=ago(hours=25)))
        assert v == "PASS" and "the same bytes since" in detail, detail

    @pytest.mark.asyncio
    async def test_the_same_bytes_for_over_a_day_pass(self, pool):
        async with scenario(pool) as conn:
            v, detail = await verdict(conn, P1, duckdb_off="1", **record())
        assert v == "PASS", detail
        assert "the same bytes since 28.05 12:00 Kyiv (192 h)" in detail, detail

    @pytest.mark.asyncio
    async def test_a_change_fails_at_the_check_and_through_the_day(self, pool):
        async with scenario(pool) as conn:
            v, _ = await verdict(conn, P1, duckdb_off="1",
                                 **record(last="CHANGED", since=ago(minutes=20), reason="change"))
        assert v == "FAIL"
        async with scenario(pool) as conn:
            v, detail = await verdict(conn, P1, duckdb_off="1",
                                      **record(since=ago(hours=5), reason="change"))
        assert v == "FAIL" and "first seen at 05.06 07:00 Kyiv" in detail, detail
        async with scenario(pool) as conn:
            v, _ = await verdict(conn, P1, duckdb_off="1",
                                 **record(since=ago(hours=25), reason="change"))
        assert v == "PASS"

    @pytest.mark.asyncio
    async def test_a_record_younger_than_the_day_is_unknown(self, pool):
        async with scenario(pool) as conn:
            v, detail = await verdict(conn, P1, duckdb_off="1",
                                      **record(since=ago(hours=5)))
        assert v == "UNKNOWN" and "recorded only since 05.06 07:00 Kyiv" in detail, detail
        async with scenario(pool) as conn:
            v, detail = await verdict(conn, P1, duckdb_off="1",
                                      **record(last="BASELINE", since=ago(minutes=20)))
        assert v == "UNKNOWN" and "recorded only since" in detail, detail

    @pytest.mark.asyncio
    async def test_the_staleness_limit_is_exact(self, pool):
        """The cron runs hourly; three hours to the second is alive."""
        async with scenario(pool) as conn:
            v, _ = await verdict(conn, P1, duckdb_off="1", **record(checked=ago(hours=3)))
        assert v == "PASS"
        async with scenario(pool) as conn:
            v, detail = await verdict(conn, P1, duckdb_off="1",
                                      **record(checked=ago(hours=3, seconds=1)))
        assert v == "UNKNOWN" and "the hourly cron is not running" in detail, detail


# ── every variant runs, read-only, one row ────────────────────────────────────

@needs_pg
class TestEveryVariantRuns:
    VARIANTS = (
        {"duckdb_off": "1", **record()},
        {"duckdb_off": "invalid", "duckdb_file_last": "error"},
        {"duckdb_off": "unknown", "duckdb_file_last": "none"},
        {"duckdb_off": "1", "duckdb_file_last": "MISSING", "duckdb_file_since": "garbage",
         "duckdb_file_checked_at": "", "duckdb_file_since_reason": ""},
    )

    @pytest.mark.asyncio
    @pytest.mark.parametrize("clock", ["fixed", "real"])
    async def test_one_verdict_row(self, pool, clock):
        for variables in self.VARIANTS:
            async with scenario(pool) as conn:
                rows = await check(conn, P1, now=NOW if clock == "fixed" else None,
                                   **variables)
            [row] = rows
            assert row["verdict"] in {"PASS", "FAIL", "UNKNOWN"}, row
            assert row["detail"] and "\n" not in row["detail"], row
