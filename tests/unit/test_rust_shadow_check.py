"""The nightly judge of the Rust shadow (core/ch_rust_shadow.py), without stores.

What is decided here is decided by pure functions over a snapshot, so each
rule is held on its own: when two copies are comparable, what a difference
is, what waits for the next attempt, and when a night is journalled. The
harness (tests/integration/test_rust_k1_ch_sync.py) runs the same judge
against a real Postgres and ClickHouse.
"""
from __future__ import annotations

import asyncio
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from typing import Any, Dict, List

import pytest

from core import ch_rust_shadow as shadow
from core.ch_rust_shadow import (
    GOLD_SHIP, GOLD_STAMP, HISTORY_SHIP, PREFIX, SILVER_SHIP, SILVER_STAMP, Snapshot, State,
    Verdict,
)
from core.data_quality import Severity
from core.pg_gold import GOLD_COLUMNS

T0 = datetime(2026, 11, 28, 0, 0, tzinfo=timezone.utc)


def _at(minutes: float) -> datetime:
    return T0 + timedelta(minutes=minutes)


def _states(**ok: float) -> Dict[str, State]:
    """`silver=-60, rust_silver=10, …` → state rows stamped at those minutes."""
    names = {
        "silver": SILVER_STAMP, "gold": GOLD_STAMP,
        "py_silver": SILVER_SHIP, "py_gold": GOLD_SHIP, "py_history": HISTORY_SHIP,
        "rust_silver": PREFIX + SILVER_SHIP, "rust_gold": PREFIX + GOLD_SHIP,
        "rust_history": PREFIX + HISTORY_SHIP,
    }
    return {names[k]: State(_at(v)) for k, v in ok.items()}


ALL_FRESH = dict(silver=-60, gold=-59, py_silver=10, py_gold=10.1, py_history=10.2,
                 rust_silver=20, rust_gold=20.1, rust_history=20.2)


def _silver(n: int = 3, bump: int = 0) -> List[tuple]:
    return [(i, 1, 4, Decimal("100.00") + bump * (i == 2)) for i in range(1, n + 1)]


def _gold_cell(day: date, revenue: str, source=None) -> tuple:
    values: Dict[str, Any] = dict(
        date=day, sales_type="retail", source_id=source, revenue=Decimal(revenue),
        orders_count=1, unique_customers=1, new_customers=1, returning_customers=0,
        returns_count=0, returns_revenue=Decimal("0"), avg_order_value=Decimal(revenue))
    return tuple(values[c] for c in GOLD_COLUMNS)


GOLD = [_gold_cell(date(2026, 11, 27), "100.00"), _gold_cell(date(2026, 11, 27), "100.00", 4)]


def _snap(states=None, silver=None, gold=None, buckets=None, cut=None, switch=None):
    states = _states(**ALL_FRESH) if states is None else states
    return Snapshot(now=_at(30), states=states, switch=switch,
                    silver=_silver() if silver is None else silver,
                    gold=GOLD if gold is None else gold,
                    history_cut=cut if cut is not None else _at(15),
                    history_buckets={0: 10, 1: 5} if buckets is None else buckets)


# ─── Which jobs the shadow covers ───────────────────────────────────────────

@pytest.mark.parametrize("value, expected", [
    ("ch_sync", True), (" ch_sync ", True), ("other,ch_sync", True),
    ("other , ch_sync ,", True), ("", False), ("CH_SYNC", False),
    ("ch_sync:shadow", False), ("ch_syncs", False),
])
def test_the_shadow_is_on_exactly_when_the_worker_would_run_it(monkeypatch, value, expected):
    monkeypatch.setenv(shadow.SHADOW_ENV, value)
    assert shadow.shadow_configured() is expected


def test_unset_is_off(monkeypatch):
    monkeypatch.delenv(shadow.SHADOW_ENV, raising=False)
    assert shadow.shadow_configured() is False


# ─── When two copies are comparable ─────────────────────────────────────────

def test_a_ship_past_the_margin_after_the_stamp_read_that_silver():
    assert shadow.comparable(_at(0), _at(5.01))
    assert not shadow.comparable(_at(0), _at(5))
    assert not shadow.comparable(_at(10), _at(9))
    assert not shadow.comparable(None, _at(10))
    assert not shadow.comparable(_at(0), None)


def test_the_archive_is_cut_a_margin_before_the_shadows_ship():
    assert shadow.history_cut(_states(rust_history=20)) == _at(15)
    assert shadow.history_cut({}) is None


# ─── Silver and Gold against Postgres ───────────────────────────────────────

def test_an_equal_silver_compares_whatever_the_stamps():
    """The shadow equals Postgres: right, however long before the latest
    stamp it shipped — the hourly heartbeat moves the stamp, not the rows."""
    states = _states(**{**ALL_FRESH, "silver": 25, "gold": 25.1})
    verdict = Verdict()
    assert shadow.judge_silver_and_gold(_snap(states=states), _silver(), GOLD, verdict) is True
    assert verdict.compared == ["silver", "gold"]
    assert verdict.issues == [] and verdict.unwatched == []


def test_a_difference_shipped_after_the_stamp_is_the_shadows():
    verdict = Verdict()
    assert shadow.judge_silver_and_gold(_snap(), _silver(bump=1), GOLD, verdict) is False
    [issue] = verdict.issues
    assert issue.check_name == "rust_shadow_silver_differs"
    assert issue.severity == Severity.WARN
    assert issue.sample_ids == (2,)
    assert "0 lost, 0 invented, 1 changed" in issue.description
    assert verdict.compared == ["silver"] and verdict.differs


def test_a_difference_shipped_before_the_stamp_waits():
    states = _states(**{**ALL_FRESH, "silver": 18, "gold": 18.5})
    verdict = Verdict()
    assert shadow.judge_silver_and_gold(_snap(states=states), _silver(bump=1), GOLD,
                                        verdict) is False
    assert verdict.issues == [] and verdict.compared == []
    assert verdict.unwatched and verdict.unwatched[0].startswith("Silver:")


def test_lost_and_invented_rows_are_differences():
    verdict = Verdict()
    shadow.judge_silver_and_gold(_snap(), _silver(n=2) + [(9, 1, 4, Decimal("1"))], GOLD,
                                 verdict)
    [issue] = verdict.issues
    assert issue.sample_ids == (3, 9)
    assert "1 lost, 1 invented, 0 changed" in issue.description


def test_gold_compares_only_when_both_sides_derived_it_from_their_silver():
    # The shadow's Gold is older than its last Silver ship.
    verdict = Verdict()
    states = _states(**{**ALL_FRESH, "rust_gold": 19})
    assert shadow.judge_silver_and_gold(_snap(states=states), _silver(), GOLD, verdict)
    assert verdict.compared == ["silver"]
    assert any(r.startswith("Gold:") for r in verdict.unwatched)
    # Postgres' Gold is older than its Silver.
    verdict = Verdict()
    states = _states(**{**ALL_FRESH, "gold": -61})
    shadow.judge_silver_and_gold(_snap(states=states), _silver(), GOLD, verdict)
    assert verdict.compared == ["silver"]
    assert any("older than its Silver" in r for r in verdict.unwatched)


def test_a_gold_that_differs_is_one_warn():
    verdict = Verdict()
    other = [_gold_cell(date(2026, 11, 27), "101.00"), GOLD[1]]
    shadow.judge_silver_and_gold(_snap(), _silver(), other, verdict)
    assert verdict.compared == ["silver", "gold"]
    [issue] = verdict.issues
    assert issue.check_name == "rust_shadow_gold_differs"
    assert issue.severity == Severity.WARN
    assert issue.sample_ids == (20261127,)


def test_a_kopeck_on_the_average_is_not_a_difference_against_postgres():
    verdict = Verdict()
    cell = list(GOLD[0])
    cell[GOLD_COLUMNS.index("avg_order_value")] = Decimal("100.01")
    shadow.judge_silver_and_gold(_snap(), _silver(), [tuple(cell), GOLD[1]], verdict)
    assert verdict.issues == []


# ─── The archive against Postgres ───────────────────────────────────────────

def test_equal_buckets_compare_clean():
    verdict = Verdict()
    shadow.judge_history(_snap(), {0: 10, 1: 5}, verdict)
    assert verdict.compared == ["archive"] and verdict.issues == []


def test_a_short_bucket_is_a_difference():
    verdict = Verdict()
    shadow.judge_history(_snap(), {0: 9, 1: 5, 4: 1}, verdict)
    [issue] = verdict.issues
    assert issue.check_name == "rust_shadow_history_differs"
    assert issue.sample_ids == (0, 4)
    assert "holds 15 version(s)" in issue.description and "Postgres 15" in issue.description


def test_an_archive_never_shipped_waits():
    verdict = Verdict()
    snap = _snap()
    snap.history_cut, snap.history_buckets = None, None
    shadow.judge_history(snap, {}, verdict)
    assert verdict.compared == [] and verdict.unwatched


# ─── The shadow's own state rows ────────────────────────────────────────────

def test_a_shadow_with_no_rows_is_stale():
    [issue] = shadow.liveness({}, _at(30))
    assert issue.check_name == "rust_shadow_stale" and issue.count == 3


def test_a_fresh_shadow_says_nothing():
    assert shadow.liveness(_states(**ALL_FRESH), _at(30)) == []


def test_an_old_or_failing_shadow_says_so():
    states = _states(**ALL_FRESH)
    states[PREFIX + GOLD_SHIP] = State(_at(-200), failures=2, last_error="Code: 497.\nmore")
    found = {i.check_name: i for i in shadow.liveness(states, _at(30))}
    assert set(found) == {"rust_shadow_stale", "rust_shadow_failing"}
    assert "3.8 h ago" in found["rust_shadow_stale"].description
    assert "Code: 497." in found["rust_shadow_failing"].description
    assert "more" not in found["rust_shadow_failing"].description


# ─── ClickHouse's answers ───────────────────────────────────────────────────

def test_a_comparisons_answer_parses():
    assert shadow.parse_difference("2\t1\t[5,7,9]\n") == (2, 1, [5, 7, 9])
    assert shadow.parse_difference("0\t0\t[]\n") == (0, 0, [])


def test_the_comparison_counts_multiplicities_over_every_column():
    sql = shadow.difference_sql("t_a", "t_b", columns=("id", "x"), key="id",
                                where=" WHERE id < 9")
    assert "EXCEPT" not in sql
    assert "SELECT id, x, 1 AS side FROM t_a WHERE id < 9 UNION ALL " in sql
    assert "SELECT id, x, -1 AS side FROM t_b WHERE id < 9" in sql
    assert "GROUP BY id, x HAVING d != 0" in sql


def test_a_cut_is_written_as_clickhouse_reads_it():
    at = datetime(2026, 11, 28, 3, 2, 1, 456789, tzinfo=timezone(timedelta(hours=2)))
    assert shadow.ts_literal(at) == "toDateTime64('2026-11-28 01:02:01.456789', 6, 'UTC')"


def test_a_ship_during_the_reads_is_named():
    snap = _snap()
    stamps = {n: s.last_ok_at for n, s in snap.states.items()}
    assert shadow.moved_ships(snap, stamps) == []
    stamps[PREFIX + SILVER_SHIP] = _at(31)
    assert shadow.moved_ships(snap, stamps) == [PREFIX + SILVER_SHIP]


# ─── The night ──────────────────────────────────────────────────────────────

def test_kyiv_midnight_is_the_local_day():
    # 00:30 UTC on the 28th is 02:30 in Kyiv (UTC+2 in November).
    assert shadow.kyiv_midnight(datetime(2026, 11, 28, 0, 30, tzinfo=timezone.utc)) == \
        datetime(2026, 11, 27, 22, 0, tzinfo=timezone.utc)


def test_when_a_night_is_journalled():
    complete = Verdict(compared=["silver", "gold", "archive"])
    partial = Verdict(compared=["silver", "archive"], unwatched=["Gold: …"])
    differs = Verdict(compared=["silver"], issues=[shadow._issue(
        check_name="rust_shadow_silver_differs", table="t", count=1, description="x")])
    stale_only = Verdict(issues=shadow.liveness({}, _at(30)), unwatched=["…"])
    assert shadow.should_journal(complete, final=False)
    assert not shadow.should_journal(partial, final=False)
    assert shadow.should_journal(partial, final=True)
    assert shadow.should_journal(differs, final=False)
    assert not shadow.should_journal(stale_only, final=False)


class _Journal:
    def __init__(self):
        self.runs: List[Dict[str, Any]] = []

    async def journal_run(self, **run):
        self.runs.append(run)
        return len(self.runs)


@pytest.fixture
def night(monkeypatch):
    """`run_nightly` with the shadow on, ClickHouse configured, the journal
    recorded and the verdict chosen by the test."""
    from core import dq_journal

    monkeypatch.setenv(shadow.SHADOW_ENV, "ch_sync")
    monkeypatch.setenv("KS_CH_URL", "http://clickhouse.invalid:8123")
    journal = _Journal()
    monkeypatch.setattr(dq_journal, "journal_run", journal.journal_run)
    state: Dict[str, Any] = {"judged": False, "verdict": Verdict()}

    async def judged_since(at):
        return state["judged"]

    async def judge(*, margin=shadow.MARGIN):
        if isinstance(state["verdict"], Exception):
            raise state["verdict"]
        return state["verdict"]

    monkeypatch.setattr(shadow, "judged_since", judged_since)
    monkeypatch.setattr(shadow, "judge", judge)
    return journal, state


@pytest.mark.asyncio
async def test_off_without_the_shadow(monkeypatch, night):
    journal, _ = night
    monkeypatch.setenv(shadow.SHADOW_ENV, "")
    assert "skipped" in await shadow.run_nightly(final=True)
    assert journal.runs == []


@pytest.mark.asyncio
async def test_off_without_clickhouse(monkeypatch, night):
    journal, _ = night
    monkeypatch.delenv("KS_CH_URL", raising=False)
    assert "skipped" in await shadow.run_nightly(final=True)
    assert journal.runs == []


@pytest.mark.asyncio
async def test_a_night_already_judged_stands_down(night):
    journal, state = night
    state["judged"] = True
    assert (await shadow.run_nightly(final=True))["skipped"].startswith("tonight's")
    assert journal.runs == []


@pytest.mark.asyncio
async def test_a_partial_attempt_waits_and_the_last_one_says_what_it_missed(night):
    journal, state = night
    state["verdict"] = Verdict(compared=["silver", "archive"], unwatched=["Gold: x"])
    assert "pending" in await shadow.run_nightly(final=False)
    assert journal.runs == []

    result = await shadow.run_nightly(final=True)
    [run] = journal.runs
    assert run["layer"] == "rust_shadow"
    assert [i.check_name for i in run["issues"]] == ["rust_shadow_unwatched"]
    assert run["issues"][0].severity == Severity.WARN
    assert run["error_message"] is None  # it compared Silver and the archive
    assert result["findings"] == ["rust_shadow_unwatched"]


@pytest.mark.asyncio
async def test_a_night_that_compared_nothing_moves_no_age(night):
    journal, state = night
    state["verdict"] = Verdict(issues=shadow.liveness({}, _at(30)),
                               unwatched=["the shadow could not be read: x"])
    await shadow.run_nightly(final=True)
    [run] = journal.runs
    assert run["error_message"].startswith("compared nothing")
    assert {i.check_name for i in run["issues"]} == {"rust_shadow_stale", "rust_shadow_unwatched"}


@pytest.mark.asyncio
async def test_a_difference_is_journalled_at_once(night):
    journal, state = night
    state["verdict"] = Verdict(compared=["silver"], unwatched=["Gold: waits"], issues=[
        shadow._issue(check_name="rust_shadow_silver_differs", table="silver.orders_rust",
                      count=2, description="x")])
    await shadow.run_nightly(final=False)
    [run] = journal.runs
    assert [i.check_name for i in run["issues"]] == [
        "rust_shadow_silver_differs", "rust_shadow_unwatched"]


@pytest.mark.asyncio
async def test_a_clean_night_journals_no_findings(night):
    journal, state = night
    state["verdict"] = Verdict(compared=["silver", "gold", "live silver", "live gold",
                                         "archive", "live archive"])
    await shadow.run_nightly(final=False)
    [run] = journal.runs
    assert run["issues"] == [] and run["error_message"] is None


@pytest.mark.asyncio
async def test_rust_owning_the_job_stands_the_judge_down(night):
    journal, state = night
    state["verdict"] = Verdict(skipped="executor:ch_sync is rust")
    assert "skipped" in await shadow.run_nightly(final=True)
    assert journal.runs == []


@pytest.mark.asyncio
async def test_a_failure_is_an_answer_not_a_raise(night):
    journal, state = night
    state["verdict"] = RuntimeError("Postgres went away")
    assert await shadow.run_nightly(final=True) == {"error": "RuntimeError: Postgres went away"}
    assert journal.runs == []


# ─── The digest reads the layer only while there is a shadow ────────────────

@pytest.mark.asyncio
@pytest.mark.parametrize("value, included", [("ch_sync", True), ("", False)])
async def test_the_digest_asks_for_the_layer_only_with_a_shadow(monkeypatch, value, included):
    from core import dq_journal
    from core.scheduler import BackgroundScheduler

    monkeypatch.setenv(shadow.SHADOW_ENV, value)
    asked: List[List[str]] = []

    async def digest_inputs(layers, now):
        asked.append(list(layers))
        return None, []

    monkeypatch.setattr(dq_journal, "digest_inputs", digest_inputs)
    await BackgroundScheduler()._run_dq_digest()
    assert (shadow.LAYER in asked[0]) is included


def test_the_digest_gives_the_layer_a_night_and_grace():
    from core.data_quality import DIGEST_MAX_AGE_HOURS

    assert DIGEST_MAX_AGE_HOURS[shadow.LAYER] == 30


# ─── The schedule ───────────────────────────────────────────────────────────

def test_the_job_is_registered_at_night():
    """01:20 to 04:20: twenty minutes clear of the 01:00 integrity check, and
    04:20, the last, exists once on both clock-change days (03:xx is skipped
    in March and repeated in October; the journal keeps a repeat quiet)."""
    from apscheduler.schedulers.asyncio import AsyncIOScheduler
    from apscheduler.triggers.cron import CronTrigger

    from core.scheduler import SCHEDULER_TIMEZONE, BackgroundScheduler

    scheduler = BackgroundScheduler()
    scheduler._scheduler = AsyncIOScheduler(timezone=SCHEDULER_TIMEZONE)
    asyncio.run(scheduler._register_jobs())
    job = scheduler._scheduler.get_job("rust_shadow_check")
    assert job is not None
    assert isinstance(job.trigger, CronTrigger)
    fields = {f.name: str(f) for f in job.trigger.fields}
    assert (fields["hour"], fields["minute"]) == (shadow.ATTEMPT_HOURS, "20")
    assert shadow.FINAL_HOUR == 4
