"""Stage 5's soak checks against a real Postgres: P1–P4 (OD-17 (a)).

The 30-day parallel period and the 7-day week of silence are counted by four
files under `deploy/stage4_soak/` — the DuckDB file's hash record (P1), the
rollback levers (P2), and the two clocks (P3, P4). Each must say FAIL on the
breach it exists to catch, PASS next to it, and UNKNOWN where nobody can say
the day was watched. The harness is `test_stage4_soak_sql.py`'s: one rolled
back transaction per scenario, a read-only check, a fixed Wednesday noon in
Kyiv in 2030.

What every name and number shared with the code is held to the code: the
lever rows to `core/lever_journal.py`, the pages and watches to
`bot/canary.py`, the gaps to the canary's, and the read-fallback half to
F1's own lists.
"""
from __future__ import annotations

import json
import re
from datetime import date, datetime, timedelta, timezone

import pytest

from tests.integration.test_stage4_soak_sql import (  # noqa: F401  (pool is a fixture)
    NOW, SQL_DIR, _uncommented, ago, check, needs_pg, pool, scenario, verdict,
)

P1 = "50_p1_duckdb_file.sql"
P2 = "51_p2_rollback_levers.sql"
P3 = "52_p3_parallel_period.sql"
P4 = "53_p4_week_of_silence.sql"
F1 = "22_f1_read_fallbacks.sql"

TRIPWIRE = "duckdb_opened_while_off"
LEVER_PAGES = ("write_chain_flag_mismatch", "warehouse_hold_stuck",
               "warehouse_preconditions_unmet")
FALLBACK_PAGES = ("read_fallback_used", "read_routed_to_duckdb")
WATCHES = ("watch:duckdb_switch", "watch:read_fallbacks")


def z(dt: datetime) -> str:
    """The silence record's own spelling of an instant."""
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def record(*, last="UNCHANGED", since=None, reason="baseline", checked=None) -> dict:
    """psql variables as deploy/stage4_soak.sh hands over a file record."""
    return {"duckdb_file_last": last,
            "duckdb_file_since": z(since or ago(days=8)),
            "duckdb_file_since_reason": reason,
            "duckdb_file_checked_at": z(checked or ago(minutes=20))}


async def clear(conn):
    keys = [TRIPWIRE, *LEVER_PAGES, *FALLBACK_PAGES]
    await conn.execute(
        "DELETE FROM app.alert_events WHERE condition_key = ANY($1::text[]) "
        "OR left(condition_key, 6) = 'lever:'", keys)
    await conn.execute(
        "DELETE FROM app.alert_series WHERE condition_key = ANY($1::text[])",
        [*keys, *WATCHES])
    await conn.execute("DELETE FROM meta.chain_watermarks WHERE left(key, 6) = 'owner:'")
    await conn.execute("DELETE FROM app.sync_metadata WHERE key = 'warehouse_writer'")
    await conn.execute("DELETE FROM app.weekly_report_sends")


async def lever(conn, *, at, subject="pg_inventory_write", outcome="released"):
    await conn.execute(
        "INSERT INTO app.alert_events (condition_key, event_type, at, instance, message, "
        "context) VALUES ('lever:chain_copy_back', 'lever_used', $1, 'web', 'x', $2::jsonb)",
        at, json.dumps({"subject": subject, "outcome": outcome}))


async def page(conn, key, *, at, event_type="fired"):
    await conn.execute(
        "INSERT INTO app.alert_events (condition_key, event_type, at, instance, "
        "delivered_to, message) VALUES ($1, $2, $3, 'bot', 1, 'x')", key, event_type, at)


async def series(conn, key, *, state, first, resolved=None):
    await conn.execute(
        "INSERT INTO app.alert_series (condition_key, kind, state, first_fired_at, "
        "last_fired_at, fired_count, resolved_at, instance) "
        "VALUES ($1, 'condition', $2, $3, $3, 1, $4, 'bot')", key, state, first, resolved)


async def watch(conn, key, *, since, last=None, probes=100):
    await conn.execute(
        "INSERT INTO app.alert_series (condition_key, kind, state, first_fired_at, "
        "last_fired_at, fired_count, instance) VALUES ($1, 'event', 'event', $2, $3, $4, 'bot')",
        key, since, last or ago(minutes=10), probes)


async def writer(conn, value: str):
    await conn.execute(
        "INSERT INTO app.sync_metadata (key, value, updated_at) VALUES "
        "('warehouse_writer', $1, $2)", value, ago(hours=1))


async def owner(conn, table, *, at):
    await conn.execute(
        "INSERT INTO meta.chain_watermarks (key, value, updated_at) VALUES ($1, 'x', $2)",
        f"owner:{table}", at)


async def report(conn, *, sent_at):
    await conn.execute(
        "INSERT INTO app.weekly_report_sends (week_start, sales_type, revenue, orders, sent_at) "
        "VALUES ($1, 'retail', 1, 1, $2)", date(2030, 5, 27), sent_at)


# ── the files against the code they read ──────────────────────────────────────

class TestTheFilesReadWhatTheCodeWrites:
    @staticmethod
    def code(name):
        return _uncommented((SQL_DIR / name).read_text(encoding="utf-8"))

    def test_the_lever_rows_are_core_lever_journals(self):
        """Mutation: rename `LEVER_USED` or `LEVER_PREFIX`."""
        from core import lever_journal

        prefix = lever_journal.LEVER_PREFIX
        for name in (P2, P3, P4):
            code = self.code(name)
            assert f"left(e.condition_key, {len(prefix)}) = '{prefix}'" in code, name
            assert f"e.event_type = '{lever_journal.LEVER_USED}'" in code, name
            assert f"substr(e.condition_key, {len(prefix) + 1})" in code, name

    def test_every_page_key_is_a_registered_condition(self):
        from core.alerting import REGISTRY

        for name in (P2, P3, P4):
            [block] = re.findall(r"VALUES (.*?)\)\s*AS v \(", self.code(name), flags=re.S)
            keys = re.findall(r"\('([a-z_]+)',", block)
            assert len(keys) >= 3, (name, keys)
            for key in keys:
                assert key in REGISTRY, (name, key)

    def test_the_lever_pages_are_the_same_three_everywhere(self):
        for name in (P2, P3, P4):
            found = set(re.findall(r"\('((?:write_chain|warehouse)_[a-z_]+)'", self.code(name)))
            assert found == set(LEVER_PAGES), (name, found)

    def test_the_fallback_half_is_f1s(self):
        """P3 and P4 count a read served from DuckDB exactly as F1 does: the
        same page keys, the same watch, the same gap. Mutation: drop
        `read_routed_to_duckdb` from either clock."""
        from bot import canary

        f1 = self.code(F1)
        f1_keys = set(re.findall(r"'(read_[a-z_]+)'", re.search(
            r"condition_key IN \(([^)]*)\)", f1).group(1)))
        assert f1_keys == set(FALLBACK_PAGES)
        for name in (P3, P4):
            code = self.code(name)
            assert set(re.findall(r"\('(read_[a-z_]+)'", code)) == f1_keys, name
            assert "'watch:read_fallbacks'" in code, name
            gap = re.findall(r"interval\s+'(\d+)\s+minutes'\s+AS\s+watch_gap", code)
            assert [int(m) * 60 for m in gap] == [canary.READ_FALLBACK_WATCH_GAP_S], name

    def test_the_tripwire_is_the_canarys(self):
        """Mutation: rename the page key or the watch key in bot/canary.py."""
        from bot import canary

        [(key, _)] = canary.check_duckdb_switch(
            {"duckdb_switch": {"opened_while_off": {"a:b": {"count": 1}}}})
        code = self.code(P4)
        assert f"('{key}', 'web opened DuckDB')" in code
        assert f"'{canary.DUCKDB_SWITCH_WATCH_KEY}'" in code
        assert canary.DUCKDB_SWITCH_WATCH_GAP_S == canary.READ_FALLBACK_WATCH_GAP_S

    @pytest.mark.parametrize("name, span, extra", [
        (P1, 24, {"record_max_age": "3 hours"}),
        (P2, 24, {}),
        (P3, 24, {"period": "720 hours"}),
        (P4, 24, {"week": "168 hours", "record_max_age": "3 hours"}),
    ])
    def test_the_clocks(self, name, span, extra):
        """A day, the owner's 30 days (OD-17 (a)), the week of silence, and an
        hourly cron's record allowed three hours."""
        code = self.code(name)
        assert re.findall(r"interval\s+'(\d+)\s+hours'\s+AS\s+span", code) == [str(span)]
        for alias, value in extra.items():
            assert re.findall(rf"interval\s+'([^']+)'\s+AS\s+{alias}\b", code) == [value]

    def test_the_record_shape_is_the_scripts(self):
        """P1 and P4 parse the instants deploy/duckdb_silence_check.sh writes."""
        script = (SQL_DIR.parent / "duckdb_silence_check.sh").read_text(encoding="utf-8")
        assert "+%Y-%m-%dT%H:%M:%SZ" in script
        for name in (P1, P4):
            assert "'^\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}Z$'" in self.code(name), name


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


# ── P2: levers ────────────────────────────────────────────────────────────────

@needs_pg
class TestP2RollbackLevers:
    @pytest.mark.asyncio
    async def test_nothing_recorded_passes(self, pool):
        async with scenario(pool) as conn:
            await clear(conn)
            v, detail = await verdict(conn, P2)
        assert v == "PASS" and "last recorded: none" in detail, detail

    @pytest.mark.asyncio
    async def test_a_copy_back_in_the_day_fails_and_names_itself(self, pool):
        async with scenario(pool) as conn:
            await clear(conn)
            await lever(conn, at=ago(hours=2))
            v, detail = await verdict(conn, P2)
        assert v == "FAIL", detail
        assert "chain_copy_back pg_inventory_write released at 05.06 10:00 Kyiv" in detail
        assert "start again" in detail

    @pytest.mark.asyncio
    async def test_one_before_the_day_is_the_last_recorded(self, pool):
        """Mutation: drop `r.at > win.starts` from the count."""
        async with scenario(pool) as conn:
            await clear(conn)
            await lever(conn, at=ago(hours=25), outcome="committed_not_released")
            v, detail = await verdict(conn, P2)
        assert v == "PASS", detail
        assert "pg_inventory_write committed_not_released at 04.06 11:00 Kyiv" in detail

    @pytest.mark.asyncio
    async def test_a_flag_put_back_on_a_latched_chain_is_a_lever(self, pool):
        async with scenario(pool) as conn:
            await clear(conn)
            await page(conn, "write_chain_flag_mismatch", at=ago(hours=3))
            v, detail = await verdict(conn, P2)
        assert v == "FAIL" and "write_chain_flag_mismatch" in detail, detail
        async with scenario(pool) as conn:
            await clear(conn)
            await series(conn, "write_chain_flag_mismatch", state="firing", first=ago(days=3))
            v, detail = await verdict(conn, P2)
        assert v == "FAIL" and "still paging" in detail, detail

    @pytest.mark.asyncio
    async def test_held_back_counts_only_once_a_period_runs(self, pool):
        """Before step 13's flip `warehouse_preconditions_unmet` is a first
        flip held back; after it, the way back. Mutation: make the key count
        unconditionally."""
        for variables, expected in (
                ({}, "PASS"),
                ({"parallel_from": "2030-05-01 10:00+03"}, "FAIL"),
                ({"duckdb_off": "1"}, "FAIL")):
            async with scenario(pool) as conn:
                await clear(conn)
                await page(conn, "warehouse_preconditions_unmet", at=ago(hours=3))
                v, detail = await verdict(conn, P2, **variables)
            assert v == expected, (variables, detail)

    @pytest.mark.asyncio
    async def test_step_13_given_back(self, pool):
        async with scenario(pool) as conn:
            await clear(conn)
            await writer(conn, json.dumps({"writer": "duckdb",
                                           "since": ago(hours=3).isoformat()}))
            v, detail = await verdict(conn, P2)
        assert v == "FAIL" and "step 13 was given back to DuckDB at 05.06 09:00" in detail
        async with scenario(pool) as conn:
            await clear(conn)
            await writer(conn, json.dumps({"writer": "duckdb",
                                           "since": ago(days=3).isoformat()}))
            v, detail = await verdict(conn, P2)
        assert v == "PASS" and "back on DuckDB since 02.06" in detail, detail

    @pytest.mark.asyncio
    async def test_a_record_it_cannot_parse_is_not_an_error(self, pool):
        async with scenario(pool) as conn:
            await clear(conn)
            await writer(conn, '{"writer": "postgres", "since": "yesterday"}')
            v, _ = await verdict(conn, P2)
        assert v == "PASS"

    @pytest.mark.asyncio
    async def test_the_writers_row_is_what_it_reads(self, pool):
        """`core.lever_journal.record` itself, on the real clock."""
        from core import lever_journal

        async with scenario(pool) as conn:
            await clear(conn)
            await lever_journal.record(conn, lever_journal.CHAIN_COPY_BACK,
                                       subject="pg_goals_write",
                                       outcome=lever_journal.RELEASED)
            v, detail = await verdict(conn, P2, now=None)
        assert v == "FAIL" and "chain_copy_back pg_goals_write released" in detail, detail


# ── P3: the parallel period ───────────────────────────────────────────────────

DECLARED = "2030-05-26 12:00+03"  # ten days before NOW


@needs_pg
class TestP3ParallelPeriod:
    async def watched(self, conn, *, since=None, last=None, probes=1000):
        await watch(conn, "watch:read_fallbacks", since=since or ago(days=40),
                    last=last, probes=probes)

    @pytest.mark.asyncio
    async def test_undeclared_is_not_applicable(self, pool):
        async with scenario(pool) as conn:
            await clear(conn)
            v, detail = await verdict(conn, P3)
        assert v == "PASS" and detail.startswith("not applicable"), detail

    @pytest.mark.asyncio
    async def test_it_counts_from_the_declared_flip(self, pool):
        async with scenario(pool) as conn:
            await clear(conn)
            await self.watched(conn)
            await owner(conn, "app.manual_expenses", at=ago(days=20))
            v, detail = await verdict(conn, P3, parallel_from=DECLARED)
        assert v == "PASS", detail
        assert "clean since 26.05 12:00 Kyiv (the declared last write flag): 10 d 0 h" in detail

    @pytest.mark.asyncio
    async def test_thirty_days_are_covered(self, pool):
        async with scenario(pool) as conn:
            await clear(conn)
            await self.watched(conn)
            v, detail = await verdict(conn, P3, parallel_from="2030-05-06 12:00+03")
        assert v == "PASS" and "the 30 days of the parallel period are covered" in detail
        async with scenario(pool) as conn:
            await clear(conn)
            await self.watched(conn)
            v, detail = await verdict(conn, P3, parallel_from="2030-05-06 12:00:01+03")
        assert v == "PASS" and "29 d 23 h of the 30 days" in detail, detail

    @pytest.mark.asyncio
    async def test_a_later_latch_moves_the_start(self, pool):
        async with scenario(pool) as conn:
            await clear(conn)
            await self.watched(conn)
            await owner(conn, "app.stock_movements", at=ago(days=4))
            v, detail = await verdict(conn, P3, parallel_from=DECLARED)
        assert v == "PASS" and "(a chain latched): 4 d 0 h" in detail, detail

    @pytest.mark.asyncio
    async def test_a_lever_restarts_it(self, pool):
        async with scenario(pool) as conn:
            await clear(conn)
            await self.watched(conn)
            await lever(conn, at=ago(days=3))
            v, detail = await verdict(conn, P3, parallel_from=DECLARED)
        assert v == "PASS", detail
        assert "(a rollback lever: chain_copy_back pg_inventory_write released): 3 d" in detail
        async with scenario(pool) as conn:
            await clear(conn)
            await self.watched(conn)
            await lever(conn, at=ago(hours=2))
            v, detail = await verdict(conn, P3, parallel_from=DECLARED)
        assert v == "FAIL" and "the 30 days start again" in detail, detail

    @pytest.mark.asyncio
    @pytest.mark.parametrize("key", FALLBACK_PAGES)
    async def test_a_read_served_from_duckdb_restarts_it(self, pool, key):
        async with scenario(pool) as conn:
            await clear(conn)
            await self.watched(conn)
            await page(conn, key, at=ago(hours=2))
            v, detail = await verdict(conn, P3, parallel_from=DECLARED)
        assert v == "FAIL" and "a read served from DuckDB" in detail, detail
        async with scenario(pool) as conn:
            await clear(conn)
            await self.watched(conn)
            await series(conn, key, state="resolved", first=ago(days=6), resolved=ago(days=5))
            v, detail = await verdict(conn, P3, parallel_from=DECLARED)
        assert v == "PASS" and "resolved)): 5 d 0 h" in detail, detail

    @pytest.mark.asyncio
    async def test_a_standing_page_fails_however_old(self, pool):
        async with scenario(pool) as conn:
            await clear(conn)
            await self.watched(conn)
            await series(conn, "read_fallback_used", state="firing", first=ago(days=12))
            v, detail = await verdict(conn, P3, parallel_from=DECLARED)
        assert v == "FAIL" and "still paging: read_fallback_used" in detail, detail

    @pytest.mark.asyncio
    async def test_step_13_on_duckdb_fails(self, pool):
        async with scenario(pool) as conn:
            await clear(conn)
            await self.watched(conn)
            await writer(conn, json.dumps({"writer": "duckdb",
                                           "since": ago(days=9).isoformat()}))
            v, detail = await verdict(conn, P3, parallel_from=DECLARED)
        assert v == "FAIL" and "step 13 is on DuckDB" in detail, detail

    @pytest.mark.asyncio
    async def test_an_unwatched_day_cannot_be_counted(self, pool):
        """No watch, a stale one, one younger than the day — and a dirty
        latest probe, which is a fallback."""
        cases = (
            ({}, "UNKNOWN", "no watch:read_fallbacks row"),
            ({"last": ago(minutes=36)}, "UNKNOWN", "min ago (limit 35)"),
            ({"since": ago(hours=5)}, "UNKNOWN", "watched clean only since"),
            ({"since": ago(minutes=10), "probes": 0}, "FAIL", "found a read served"),
        )
        for kw, expected, words in cases:
            async with scenario(pool) as conn:
                await clear(conn)
                if kw:
                    await self.watched(conn, **kw)
                v, detail = await verdict(conn, P3, parallel_from=DECLARED)
            assert (v, words in detail) == (expected, True), (kw, detail)


# ── P4: the week of silence ───────────────────────────────────────────────────

@needs_pg
class TestP4WeekOfSilence:
    async def silent(self, conn, *, days=8):
        """A week under off that nothing broke: both watches, the file."""
        await watch(conn, "watch:duckdb_switch", since=ago(days=days))
        await watch(conn, "watch:read_fallbacks", since=ago(days=40))

    @pytest.mark.asyncio
    @pytest.mark.parametrize("off, expected, words", [
        ("0", "PASS", "not applicable"),
        ("invalid", "FAIL", "does not understand"),
        ("unknown", "UNKNOWN", "web is not running"),
    ])
    async def test_the_switch(self, pool, off, expected, words):
        async with scenario(pool) as conn:
            await clear(conn)
            v, detail = await verdict(conn, P4, duckdb_off=off)
        assert (v, words in detail) == (expected, True), detail

    @pytest.mark.asyncio
    async def test_a_clean_week_with_a_monday_report_is_covered(self, pool):
        async with scenario(pool) as conn:
            await clear(conn)
            await self.silent(conn)
            await report(conn, sent_at=ago(days=2))
            v, detail = await verdict(conn, P4, duckdb_off="1", **record())
        assert v == "PASS", detail
        assert "silent since 28.05 12:00 Kyiv" in detail
        assert "the 168 h are covered, a weekly report delivered 03.06 12:00" in detail

    @pytest.mark.asyncio
    async def test_no_report_inside_the_week_is_not_covered(self, pool):
        """168 h hold a Monday by the clock; the report proves the Monday path
        ran without DuckDB. Mutation: drop `report_inside` from the covered
        branch."""
        for sent in (None, ago(days=9)):
            async with scenario(pool) as conn:
                await clear(conn)
                await self.silent(conn)
                if sent:
                    await report(conn, sent_at=sent)
                v, detail = await verdict(conn, P4, duckdb_off="1", **record())
            assert v == "PASS" and "not covered until one is" in detail, (sent, detail)

    @pytest.mark.asyncio
    async def test_three_days_in(self, pool):
        async with scenario(pool) as conn:
            await clear(conn)
            await self.silent(conn, days=3)
            v, detail = await verdict(conn, P4, duckdb_off="1", **record())
        assert v == "PASS" and "(web watched under KS_DUCKDB=off" in detail
        assert "72 h of the 168" in detail, detail

    @pytest.mark.asyncio
    async def test_web_opening_duckdb_breaks_it(self, pool):
        """The tripwire's page, inside the day or standing, and a dirty probe
        the Gate did not deliver. Mutation: drop the tripwire from
        `breach_keys`."""
        async with scenario(pool) as conn:
            await clear(conn)
            await self.silent(conn)
            await page(conn, TRIPWIRE, at=ago(hours=2))
            v, detail = await verdict(conn, P4, duckdb_off="1", **record())
        assert v == "FAIL" and "web opened DuckDB" in detail, detail
        async with scenario(pool) as conn:
            await clear(conn)
            await self.silent(conn)
            await series(conn, TRIPWIRE, state="firing", first=ago(days=2))
            v, detail = await verdict(conn, P4, duckdb_off="1", **record())
        assert v == "FAIL" and f"still paging: {TRIPWIRE}" in detail, detail
        async with scenario(pool) as conn:
            await clear(conn)
            await watch(conn, "watch:duckdb_switch", since=ago(minutes=10), probes=0)
            await watch(conn, "watch:read_fallbacks", since=ago(days=40))
            v, detail = await verdict(conn, P4, duckdb_off="1", **record())
        assert v == "FAIL" and "latest probe found an open" in detail, detail

    @pytest.mark.asyncio
    async def test_the_file_breaks_it(self, pool):
        for rec in (record(last="CHANGED", since=ago(minutes=20), reason="change"),
                    record(last="MISSING"),
                    record(since=ago(hours=4), reason="change")):
            async with scenario(pool) as conn:
                await clear(conn)
                await self.silent(conn)
                v, detail = await verdict(conn, P4, duckdb_off="1", **rec)
            assert v == "FAIL", (rec, detail)

    @pytest.mark.asyncio
    async def test_a_change_days_ago_is_the_start_and_named(self, pool):
        async with scenario(pool) as conn:
            await clear(conn)
            await self.silent(conn)
            v, detail = await verdict(conn, P4, duckdb_off="1",
                                      **record(since=ago(days=2), reason="change"))
        assert v == "PASS" and "(the DuckDB file changed): 48 h of the 168" in detail, detail

    @pytest.mark.asyncio
    async def test_levers_and_fallbacks_break_it_too(self, pool):
        for seed in (lambda c: lever(c, at=ago(hours=1)),
                     lambda c: page(c, "warehouse_preconditions_unmet", at=ago(hours=1)),
                     lambda c: page(c, "read_fallback_used", at=ago(hours=1))):
            async with scenario(pool) as conn:
                await clear(conn)
                await self.silent(conn)
                await seed(conn)
                v, detail = await verdict(conn, P4, duckdb_off="1", **record())
            assert v == "FAIL" and "the week starts again" in detail, detail

    @pytest.mark.asyncio
    async def test_what_it_cannot_see_is_unknown(self, pool):
        cases = (
            ("no tripwire watch", None, record()),
            ("no file record", "silent", {"duckdb_file_last": "none"}),
            ("a stale file record", "silent", record(checked=ago(hours=4))),
            ("a record younger than the day", "silent", record(since=ago(hours=5))),
            ("a tripwire watch younger than the day", "short", record()),
        )
        for label, seed, rec in cases:
            async with scenario(pool) as conn:
                await clear(conn)
                if seed == "silent":
                    await self.silent(conn)
                elif seed == "short":
                    await self.silent(conn, days=0.2)
                else:
                    await watch(conn, "watch:read_fallbacks", since=ago(days=40))
                v, detail = await verdict(conn, P4, duckdb_off="1", **rec)
            assert v == "UNKNOWN", (label, detail)


# ── every variant runs, read-only, one row ────────────────────────────────────

@needs_pg
class TestEveryVariantRuns:
    VARIANTS = (
        {"duckdb_off": "1", "parallel_from": DECLARED, **record()},
        {"duckdb_off": "invalid", "duckdb_file_last": "error"},
        {"duckdb_off": "unknown", "duckdb_file_last": "none"},
        {"duckdb_off": "1", "duckdb_file_last": "MISSING", "duckdb_file_since": "garbage",
         "duckdb_file_checked_at": "", "duckdb_file_since_reason": ""},
    )

    @pytest.mark.asyncio
    @pytest.mark.parametrize("name", [P1, P2, P3, P4])
    @pytest.mark.parametrize("clock", ["fixed", "real"])
    async def test_one_verdict_row(self, pool, name, clock):
        for variables in self.VARIANTS:
            async with scenario(pool) as conn:
                rows = await check(conn, name, now=NOW if clock == "fixed" else None,
                                   **variables)
            [row] = rows
            assert row["verdict"] in {"PASS", "FAIL", "UNKNOWN"}, row
            assert row["detail"] and "\n" not in row["detail"], row
