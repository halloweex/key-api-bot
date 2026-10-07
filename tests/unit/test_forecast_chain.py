"""Chain 7b-3 without a database: the goal and forecast tables' writer.

`core/pg_forecast_write.py` moves the writes of `app.seasonal_indices`,
`app.growth_metrics`, `app.weekly_patterns` and `app.revenue_predictions` to
Postgres behind `KS_WRITE_FORECAST`. What is proved here needs no server: the
flag and its four preconditions, the refusals taken before the latch, the
repository's routing, the one-statement read of the three goal tables, and the
scheduler slots the standing watch judges staleness by. The SQL itself is
proved against a real Postgres in `tests/integration/test_forecast_writer.py`.
"""
from __future__ import annotations

import asyncio

from apscheduler.schedulers.asyncio import AsyncIOScheduler


def _registered():
    """A scheduler with every production job registered and none started."""
    from core.scheduler import BackgroundScheduler, SCHEDULER_TIMEZONE

    s = BackgroundScheduler()
    s._scheduler = AsyncIOScheduler(timezone=SCHEDULER_TIMEZONE)
    asyncio.run(s._register_jobs())
    return s


def _fields(trigger) -> dict:
    return {f.name: str(f) for f in trigger.fields}


class TestTheSlotsAreTheSchedulers:
    """The standing watch judges the four tables stale against the last slot
    each writer was due at. Those slots are read from the constants the
    scheduler registers its jobs with, so the watch cannot judge by a time the
    scheduler no longer fires at."""

    def test_training_fires_on_the_exported_schedule(self):
        from apscheduler.triggers.cron import CronTrigger
        from core.scheduler import REVENUE_TRAIN_SCHEDULE, SCHEDULER_TIMEZONE

        job = _registered()._scheduler.get_job("revenue_prediction_train")
        assert _fields(job.trigger) == _fields(
            CronTrigger(**REVENUE_TRAIN_SCHEDULE, timezone=SCHEDULER_TIMEZONE))

    def test_the_monday_job_fires_on_the_exported_schedule(self):
        from apscheduler.triggers.cron import CronTrigger
        from core.scheduler import SCHEDULER_TIMEZONE, SEASONALITY_SCHEDULE

        job = _registered()._scheduler.get_job("seasonality_calc")
        assert _fields(job.trigger) == _fields(
            CronTrigger(**SEASONALITY_SCHEDULE, timezone=SCHEDULER_TIMEZONE))

    def test_training_is_twice_a_week_not_daily(self):
        """CLAUDE.md said "retrains daily at 3:30" for months. A watch built on
        that sentence would page every Tuesday, Wednesday, Friday, Saturday
        and Sunday."""
        from core.scheduler import REVENUE_TRAIN_SCHEDULE

        assert REVENUE_TRAIN_SCHEDULE["day_of_week"] == "mon,thu"
        assert (REVENUE_TRAIN_SCHEDULE["hour"],
                REVENUE_TRAIN_SCHEDULE["minute"]) == (3, 30)


# ─── The flag and its four preconditions ────────────────────────────────────

from datetime import date, datetime, timedelta, timezone  # noqa: E402
from unittest.mock import AsyncMock, patch  # noqa: E402
from zoneinfo import ZoneInfo  # noqa: E402

import pytest  # noqa: E402

from core import chain_latch, pg_forecast_write as chain, read_fallback  # noqa: E402
from core import write_chains  # noqa: E402

KYIV = ZoneInfo("Europe/Kyiv")
DSN = "postgresql://nobody@127.0.0.1:1/none"


@pytest.fixture
def ready(monkeypatch):
    """Every precondition met, the chain's own flag unset. A test that sets
    KS_WRITE_FORECAST=postgres means it; one that takes a clause away says
    which. `KS_READ_FALLBACK` is the configured mode, set the way
    `configure_modes()` leaves it — the environment alone is not it."""
    monkeypatch.delenv(chain.WRITE_ENV, raising=False)
    monkeypatch.setenv("KS_GOALS_HISTORY", "silver")
    monkeypatch.setenv("KS_READ_GOALS", "postgres")
    monkeypatch.setenv("KS_READ_FORECAST_INPUT", "postgres")
    monkeypatch.setenv("KS_READ_FALLBACK", "off")
    monkeypatch.setenv("KS_PG_DSN", DSN)
    monkeypatch.setattr(read_fallback, "_mode", read_fallback.OFF)
    monkeypatch.setattr(read_fallback, "_mode_error", None)
    return monkeypatch


def _state():
    return write_chains.chain_modes()[chain.CHAIN]


class TestTheFlag:
    @pytest.mark.parametrize("value, expected", [
        (None, False), ("duckdb", False), ("postgres", True), (" Postgres ", True)])
    def test_what_it_says(self, ready, value, expected):
        if value is not None:
            ready.setenv(chain.WRITE_ENV, value)
        assert chain.env_writes_postgres() is expected
        assert chain.writes_postgres() is expected

    def test_a_value_nobody_can_read_stops_the_writers_and_stands_the_tables_down(
            self, ready):
        ready.setenv(chain.WRITE_ENV, "postgre")
        with pytest.raises(RuntimeError, match=chain.WRITE_ENV):
            chain.writes_postgres()
        # The reads hand the choice back to today's engines (7a's rule) …
        assert chain.reads_postgres() is False
        # … and the registry stands the four tables down, so neither copy moves.
        state = _state()
        assert state["mode"] is None and chain.WRITE_ENV in state["error"]
        tables, errors = write_chains.stood_down_tables_checked()
        assert set(chain.CHAIN_TABLES) <= tables and chain.CHAIN in errors

    def test_off_by_default_moves_nothing(self, ready):
        assert _state()["mode"] == "duckdb"
        tables, _ = write_chains.stood_down_tables_checked()
        assert not set(chain.CHAIN_TABLES) & tables


class TestThePreconditions:
    """`unmet_precondition()` — chain 6a's arrangement. Unmet and unlatched,
    the chain runs as duckdb for every consumer, whatever the flag says."""

    def test_all_four_met_moves_the_writes(self, ready):
        ready.setenv(chain.WRITE_ENV, "postgres")
        assert chain.unmet_precondition() is None
        assert chain.writes_postgres() is True and chain.reads_postgres() is True
        state = _state()
        assert state["mode"] == "postgres" and state["unmet_precondition"] is None
        tables, _ = write_chains.stood_down_tables_checked()
        assert set(chain.CHAIN_TABLES) <= tables

    @pytest.mark.parametrize("take_away, names", [
        # 1. The history must be Silver, not the DN-12 bridge — `== silver`,
        # not `!= bridge`: a typo is unmet too.
        (lambda m: m.setenv("KS_GOALS_HISTORY", "bridge"), "KS_GOALS_HISTORY"),
        (lambda m: m.delenv("KS_GOALS_HISTORY"), "KS_GOALS_HISTORY"),
        (lambda m: m.setenv("KS_GOALS_HISTORY", "silvr"), "KS_GOALS_HISTORY"),
        # 2. …and Postgres' Silver.
        (lambda m: m.setenv("KS_READ_GOALS", "duckdb"), "KS_READ_GOALS"),
        (lambda m: m.setenv("KS_READ_GOALS", "postgress"), "KS_READ_GOALS"),
        # 3. The configured fallback mode, not the environment.
        (lambda m: m.setattr(read_fallback, "_mode", read_fallback.DUCKDB),
         "KS_READ_FALLBACK"),
        (lambda m: m.setattr(read_fallback, "_mode_error", "nonsense"),
         "KS_READ_FALLBACK"),
        # 4. The training frame, beyond the brief (OQ-1 of the design).
        (lambda m: m.setenv("KS_READ_FORECAST_INPUT", "duckdb"),
         "KS_READ_FORECAST_INPUT"),
        (lambda m: m.setenv("KS_READ_FORECAST_INPUT", "ch"),
         "KS_READ_FORECAST_INPUT"),
    ])
    def test_each_clause_alone_holds_the_chain_on_duckdb(self, ready, take_away, names):
        ready.setenv(chain.WRITE_ENV, "postgres")
        take_away(ready)
        unmet = chain.unmet_precondition()
        assert unmet and names in unmet
        others = {"KS_GOALS_HISTORY", "KS_READ_GOALS", "KS_READ_FALLBACK",
                  "KS_READ_FORECAST_INPUT"} - {names}
        for other in others:
            assert other not in unmet, (other, unmet)
        assert chain.writes_postgres() is False
        assert chain.reads_postgres() is False
        state = _state()
        assert state["mode"] == "duckdb" and state["unmet_precondition"] == unmet
        tables, _ = write_chains.stood_down_tables_checked()
        assert not set(chain.CHAIN_TABLES) & tables

    def test_the_environment_saying_off_is_not_the_configured_mode(self, ready):
        """The process refuses by the mode it was configured with. Reading the
        live variable here would call a process that still falls back
        'ready'."""
        ready.setattr(read_fallback, "_mode", None)          # never configured
        assert "KS_READ_FALLBACK" in chain.unmet_precondition()

    def test_no_dsn_is_unmet_twice(self, ready):
        ready.delenv("KS_PG_DSN")
        unmet = chain.unmet_precondition()
        assert "KS_READ_GOALS=postgres but KS_PG_DSN" in unmet
        assert "KS_READ_FORECAST_INPUT=postgres but KS_PG_DSN" in unmet

    def test_unmet_says_names_only(self, ready):
        """The block is public: it names variables, never a value or a DSN."""
        ready.delenv("KS_PG_DSN")
        assert DSN not in (chain.unmet_precondition() or "")

    def test_a_check_that_raises_is_unmet_not_a_raise(self, ready):
        ready.setenv(chain.WRITE_ENV, "postgres")
        with patch.object(chain, "_unmet_clauses", side_effect=ImportError("gone")):
            assert "could not be read" in chain.unmet_precondition()
            assert chain.writes_postgres() is False


class TestTheLatchOutranks:
    def test_every_precondition_unmet_and_the_latch_still_writes_postgres(self, ready):
        for env in ("KS_GOALS_HISTORY", "KS_READ_GOALS", "KS_READ_FORECAST_INPUT"):
            ready.setenv(env, "duckdb" if env != "KS_GOALS_HISTORY" else "bridge")
        chain_latch.latch(chain.CHAIN, chain.WRITE_ENV)
        assert chain.writes_postgres() is True and chain.reads_postgres() is True
        state = _state()
        assert state["mode"] == "postgres" and state["latched"] is True
        assert state["mismatch"] is True            # the flag says duckdb
        assert state["unmet_precondition"]          # and says why it worries

    @pytest.mark.parametrize("value", ["duckdb", "postgre", None])
    def test_any_flag_once_latched(self, ready, value):
        if value is not None:
            ready.setenv(chain.WRITE_ENV, value)
        chain_latch.latch(chain.CHAIN, chain.WRITE_ENV)
        assert chain.writes_postgres() is True and chain.reads_postgres() is True


class TestReadsTheChain:
    OTHER = ("SELECT goal_amount FROM {revenue_goals}",
             "SELECT SUM(revenue) FROM {gold_daily_revenue}",
             "SELECT * FROM seasonal_indices")

    @pytest.mark.parametrize("hole", chain.TABLE_HOLES)
    def test_each_hole_follows_once_latched(self, ready, hole):
        chain_latch.latch(chain.CHAIN, chain.WRITE_ENV)
        assert chain.reads_the_chain(f"SELECT * FROM {hole}") is True

    @pytest.mark.parametrize("sql", OTHER)
    def test_nothing_else_follows(self, ready, sql):
        chain_latch.latch(chain.CHAIN, chain.WRITE_ENV)
        assert chain.reads_the_chain(sql) is False

    @pytest.mark.parametrize("hole", chain.TABLE_HOLES)
    def test_flagged_but_held_keeps_todays_engine(self, ready, hole):
        """7a's `reads_postgres` copied verbatim would send these to a Postgres
        the hourly copy is still filling while the writes go to DuckDB."""
        ready.setenv(chain.WRITE_ENV, "postgres")
        ready.setenv("KS_GOALS_HISTORY", "bridge")
        assert chain.reads_the_chain(f"SELECT * FROM {hole}") is False

    @pytest.mark.parametrize("hole", chain.TABLE_HOLES)
    def test_off_keeps_todays_engine(self, ready, hole):
        assert chain.reads_the_chain(f"SELECT * FROM {hole}") is False

    def test_the_holes_are_the_dialects(self):
        """Every hole is a field `render_tables` fills, in both engines."""
        from core.sql_dialect import DUCKDB, POSTGRES, render_tables

        for hole in chain.TABLE_HOLES:
            duck = render_tables(f"SELECT 1 FROM {hole}", DUCKDB)
            pg = render_tables(f"SELECT 1 FROM {hole}", POSTGRES)
            name = hole.strip("{}")
            assert duck == f"SELECT 1 FROM {name}"
            assert pg == f"SELECT 1 FROM app.{name}"
            assert f"app.{name}" in chain.CHAIN_TABLES


# ─── Refused before the latch ───────────────────────────────────────────────


def _aware(*a):
    return datetime(*a, tzinfo=timezone.utc)


NOW = _aware(2026, 10, 7, 9, 0)
SEASONAL = [(1, 0.95, 3, 1000.0, 800.0, 1200.0, "high")]
YOY = (0.5019, date(2023, 12, 2), date(2026, 10, 6), 1)


@pytest.fixture
def no_postgres(ready):
    """The pool is never asked for: every refusal below is taken first."""
    pool = AsyncMock(side_effect=AssertionError("reached the pool"))
    ready.setattr(chain, "_pool", pool)
    return pool


class TestRefusedBeforeTheLatch:
    """The latch is permanent. A value Postgres would refuse must be refused
    before it, or the chain is latched for good with no owner row behind it.
    DuckDB refuses NaN in DECIMAL(6,2); Postgres stores it — so the rule is
    `core.pg_numeric.refusal`'s, which refuses NaN on purpose."""

    @pytest.mark.parametrize("metrics", [
        {"mae": 1, "mape": 12345.67, "wape": 1},
        {"mae": 1, "mape": float("nan"), "wape": 1},
        {"mae": 1, "mape": float("inf"), "wape": 1},
        {"mae": 1, "mape": 9999.995, "wape": 1},
        {"mae": 1e30, "mape": 1, "wape": 1},
        {"mae": 1, "mape": 1, "wape": "12"},
    ])
    def test_a_metric(self, no_postgres, metrics):
        with pytest.raises(ValueError):
            asyncio.run(chain.store_predictions(
                [{"date": "2026-10-07", "predicted_revenue": 1.0}],
                "retail", metrics, NOW))
        assert not no_postgres.await_count
        assert not chain_latch.latched(chain.CHAIN)

    @pytest.mark.parametrize("prediction, sales_type, created_at", [
        ({"date": "not-a-date", "predicted_revenue": 1.0}, "retail", NOW),
        ({"date": "2026-10-07", "predicted_revenue": 1e30}, "retail", NOW),
        ({"date": "2026-10-07", "predicted_revenue": float("nan")}, "retail", NOW),
        ({"date": None, "predicted_revenue": 1.0}, "retail", NOW),
        ({"date": "2026-10-07", "predicted_revenue": 1.0}, "", NOW),
        ({"date": "2026-10-07", "predicted_revenue": 1.0}, "x" * 33, NOW),
        ({"date": "2026-10-07", "predicted_revenue": 1.0}, "retail", None),
        ({"date": "2026-10-07", "predicted_revenue": 1.0}, "retail",
         datetime(2026, 10, 7, 9, 0)),                      # naive
    ])
    def test_a_prediction(self, no_postgres, prediction, sales_type, created_at):
        with pytest.raises(ValueError):
            asyncio.run(chain.store_predictions(
                [prediction], sales_type, {"mae": 1, "mape": 1, "wape": 1},
                created_at))
        assert not no_postgres.await_count
        assert not chain_latch.latched(chain.CHAIN)

    @pytest.mark.parametrize("seasonal, yoy, monthly, weekly, now", [
        ([(1, 100.0, 3, 1.0, 1.0, 1.0, "high")], YOY, {}, [], NOW),   # (6,4)
        ([(13, 1.0, 3, 1.0, 1.0, 1.0, "high")], YOY, {}, [], NOW),
        ([(1, 1.0, 3, 1e12, 1.0, 1.0, "high")], YOY, {}, [], NOW),
        ([(1, 1.0, 3, 1.0, 1.0, 1.0, "very-high-x")], YOY, {}, [], NOW),
        (SEASONAL, (float("nan"), None, None, 1), {}, [], NOW),
        (SEASONAL, (0.5, "2023-13-01", None, 1), {}, [], NOW),
        (SEASONAL, (0.5, None, None, None), {}, [], NOW),
        (SEASONAL, YOY, {1: float("nan")}, [], NOW),
        (SEASONAL, YOY, {1: None}, [], NOW),
        (SEASONAL, YOY, {0: 0.1}, [], NOW),
        (SEASONAL, YOY, {}, [(1, 6, 0.1, 3)], NOW),
        (SEASONAL, YOY, {}, [(1, 1, 1.5e2, 3)], NOW),
        (SEASONAL, YOY, {}, [], None),
        (SEASONAL, YOY, {}, [], datetime(2026, 10, 7, 9, 0)),          # naive
    ])
    def test_a_goal_table_value(self, no_postgres, seasonal, yoy, monthly, weekly, now):
        with pytest.raises(ValueError):
            asyncio.run(chain.persist_goal_tables(seasonal, yoy, monthly, weekly, now))
        assert not no_postgres.await_count
        assert not chain_latch.latched(chain.CHAIN)

    def test_the_models_iso_dates_become_dates(self):
        """`_predict_future` hands dates over as strings; asyncpg refuses a
        string where a DATE goes (measured: DataError, 'toordinal')."""
        rows = chain._prediction_rows(
            [{"date": "2026-10-07", "predicted_revenue": 1.5},
             {"date": date(2026, 10, 8), "predicted_revenue": 2.5},
             {"date": datetime(2026, 10, 9, 0, 0), "predicted_revenue": 3.5}],
            "retail", {"mae": 1, "mape": 2, "wape": 3}, NOW)
        assert [r[0] for r in rows] == [date(2026, 10, 7), date(2026, 10, 8),
                                        date(2026, 10, 9)]
        assert all(r[-1] is NOW for r in rows)

    def test_the_largest_value_postgres_keeps_is_kept(self):
        rows = chain._prediction_rows(
            [{"date": "2026-10-07", "predicted_revenue": 9999999999.99}],
            "retail", {"mae": 1, "mape": 9999.99, "wape": 9999.994}, NOW)
        assert rows[0][4] == 9999.99

    def test_metrics_absent_are_zero_as_in_duckdb(self):
        rows = chain._prediction_rows(
            [{"date": "2026-10-07", "predicted_revenue": 1.0}], "retail", None, NOW)
        assert rows[0][3:6] == (0, 0, 0)

    def test_nothing_to_store_asks_postgres_nothing(self, no_postgres):
        assert asyncio.run(chain.store_predictions([], "retail", None, NOW)) == 0
        assert not no_postgres.await_count
        assert not chain_latch.latched(chain.CHAIN)


# ─── The standing watch ─────────────────────────────────────────────────────

from core import pg_chain_invariants as inv  # noqa: E402


def _kyiv(*a):
    return datetime(*a, tzinfo=KYIV)


# 2026-10-05 is a Monday, 2026-10-08 a Thursday, 2026-10-12 the next Monday.
MON = _kyiv(2026, 10, 5, 4, 1)          # the Monday job stored
MON_TRAIN = _kyiv(2026, 10, 5, 3, 31)   # Monday's training stored
THU_TRAIN = _kyiv(2026, 10, 8, 3, 31)   # Thursday's


def _healthy(**over) -> inv.Forecast:
    base = dict(latched_at=None, months=12, yoy_null=0, seasonal_at=MON,
                seasonal_unstamped=0, yoy_rows=1, yoy_overall=0.5019,
                yoy_sample=1, yoy_from=date(2023, 12, 2),
                forecast_at=THU_TRAIN, horizon=date(2026, 12, 7),
                forecast_unstamped=0)
    base.update(over)
    return inv.Forecast(**base)


def _judge(forecast, now):
    facts = inv.Facts(watched=(chain.CHAIN,), now=now, forecast=forecast)
    return inv.check_chain_invariants(facts)


def _names(issues):
    return sorted(i.check_name for i in issues)


class TestTheWatchIsComplete:
    NOW = _kyiv(2026, 10, 8, 13, 0)     # Thursday afternoon, all fresh

    def test_a_healthy_set_is_quiet(self):
        assert _judge(_healthy(), self.NOW) == []

    @pytest.mark.parametrize("over", [
        dict(months=11), dict(months=0, seasonal_at=None), dict(yoy_null=1),
        dict(yoy_rows=0, yoy_overall=None, yoy_sample=None, yoy_from=None),
        dict(yoy_overall=None),
    ])
    def test_a_short_set_is_incomplete(self, over):
        issues = _judge(_healthy(**over), self.NOW)
        assert inv.GOAL_TABLES_INCOMPLETE in _names(issues)
        (issue,) = [i for i in issues if i.check_name == inv.GOAL_TABLES_INCOMPLETE]
        assert issue.severity.value == "WARN"
        assert "generate_smart_goals falls back" in issue.description

    def test_the_placeholder_over_two_full_years_is_incomplete(self):
        """The 2026-08-31 backup's exact state: 0.10, sample 0, a history from
        2023-12-02 — which by 2026 holds 2024 and 2025 whole."""
        issues = _judge(_healthy(yoy_overall=0.10, yoy_sample=0), self.NOW)
        (issue,) = issues
        assert issue.check_name == inv.GOAL_TABLES_INCOMPLETE
        assert "0.10 placeholder" in issue.description

    def test_the_placeholder_is_honest_without_two_full_years(self):
        assert _judge(_healthy(yoy_overall=0.10, yoy_sample=0,
                               yoy_from=date(2025, 6, 1)), self.NOW) == []
        # In 2025 a history from 2023-12-02 held 2024 alone.
        assert _judge(_healthy(yoy_overall=0.10, yoy_sample=0,
                               seasonal_at=_kyiv(2025, 10, 6, 4, 1),
                               forecast_at=_kyiv(2025, 10, 6, 3, 31)),
                      _kyiv(2025, 10, 6, 13, 0)) == []

    def test_before_the_first_write_it_says_whose_state_it_is(self):
        (issue,) = _judge(_healthy(months=11), self.NOW)
        assert "has not written these tables yet" in issue.description
        (issue,) = _judge(_healthy(months=11, latched_at=self.NOW), self.NOW)
        assert "pg_forecast_write writes these tables" in issue.description


class TestTheWatchJudgesTheSlot:
    """Training is Mon and Thu 03:30 (not daily), the Monday job Mon 04:00;
    each is judged at the last slot `FORECAST_SLOT_GRACE` before now."""

    def test_monday_seven_with_mondays_stamps_is_quiet(self):
        now = _kyiv(2026, 10, 12, 7, 0)
        f = _healthy(seasonal_at=_kyiv(2026, 10, 12, 4, 1),
                     forecast_at=_kyiv(2026, 10, 12, 3, 31))
        assert _judge(f, now) == []

    def test_monday_seven_with_thursdays_forecast_is_stale(self):
        now = _kyiv(2026, 10, 12, 7, 0)
        f = _healthy(seasonal_at=_kyiv(2026, 10, 12, 4, 1), forecast_at=THU_TRAIN)
        (issue,) = _judge(f, now)
        assert issue.check_name == inv.FORECAST_STALE
        assert issue.severity.value == "WARN"
        assert "2026-10-12T03:30:00+03:00" in issue.description

    def test_monday_seven_with_last_weeks_indices_is_stale(self):
        now = _kyiv(2026, 10, 12, 7, 0)
        f = _healthy(seasonal_at=MON, forecast_at=_kyiv(2026, 10, 12, 3, 31))
        (issue,) = _judge(f, now)
        assert issue.check_name == inv.GOAL_TABLES_STALE
        assert "2026-10-12T04:00:00+03:00" in issue.description

    def test_thursday_seven_with_mondays_forecast_is_stale(self):
        now = _kyiv(2026, 10, 8, 7, 0)
        assert _names(_judge(_healthy(forecast_at=MON_TRAIN), now)) == [
            inv.FORECAST_STALE]
        assert _judge(_healthy(forecast_at=THU_TRAIN), now) == []

    def test_the_healthy_maximum_is_quiet(self):
        """Monday 01:00: Thursday's forecast is 93 h old and Monday's indices a
        week — the oldest a healthy set ever gets. A flat 24 h limit (the
        brief's 'daily') pages here, and so does a flat 72 h one."""
        now = _kyiv(2026, 10, 12, 1, 0)
        assert _judge(_healthy(), now) == []

    def test_inside_the_grace_the_previous_slot_is_judged(self):
        now = _kyiv(2026, 10, 12, 5, 30)       # Monday's slots under 2 h 30 ago
        assert _judge(_healthy(), now) == []

    def test_the_grace_ends_before_the_seven_oclock_run(self):
        assert inv.FORECAST_SLOT_GRACE <= timedelta(hours=3)        # 03:30 + < 07:00
        assert inv.FORECAST_SLOT_GRACE >= timedelta(hours=2)

    def test_the_week_the_clocks_go_back(self):
        """Kyiv leaves summer time at 04:00 on Sunday 2026-10-25. Monday's
        03:30 is then 01:30 UTC, not 00:30 — the trigger's own arithmetic."""
        now = _kyiv(2026, 10, 26, 7, 0)
        fresh = _healthy(seasonal_at=_aware(2026, 10, 26, 2, 1),
                         forecast_at=_aware(2026, 10, 26, 1, 31))
        assert _judge(fresh, now) == []
        early = _healthy(seasonal_at=_aware(2026, 10, 26, 2, 1),
                         forecast_at=_aware(2026, 10, 26, 0, 31))
        assert _names(_judge(early, now)) == [inv.FORECAST_STALE]

    def test_every_row_unstamped_is_stale(self):
        now = _kyiv(2026, 10, 8, 13, 0)
        (issue,) = _judge(_healthy(seasonal_at=None, seasonal_unstamped=12), now)
        assert issue.check_name == inv.GOAL_TABLES_STALE
        assert "no updated_at" in issue.description

    def test_no_retail_forecast_at_all_is_stale(self):
        now = _kyiv(2026, 10, 8, 13, 0)
        (issue,) = _judge(_healthy(forecast_at=None, horizon=None), now)
        assert issue.check_name == inv.FORECAST_STALE
        assert "holds no retail row" in issue.description

    def test_unstamped_forecast_rows_ride_in_the_staleness(self):
        now = _kyiv(2026, 10, 8, 13, 0)
        (issue,) = _judge(_healthy(forecast_at=MON_TRAIN,
                                   forecast_unstamped=4), now)
        assert "4 row(s) from today on carry no created_at" in issue.description

    def test_without_a_clock_only_completeness_is_judged(self):
        assert _judge(_healthy(forecast_at=None), None) == []

    def test_the_slot_walk_uses_the_schedulers_constants(self, monkeypatch):
        """Move the scheduler's training to Wednesday and the watch follows."""
        from core import scheduler

        monkeypatch.setattr(scheduler, "REVENUE_TRAIN_SCHEDULE",
                            {"day_of_week": "wed", "hour": 3, "minute": 30})
        now = _kyiv(2026, 10, 8, 7, 0)              # Thursday
        assert _judge(_healthy(forecast_at=_kyiv(2026, 10, 7, 3, 31)), now) == []


class TestTheWatchIsRegistered:
    def test_the_chain_has_a_reader_and_three_conditions(self):
        assert inv._reader_groups()[chain.CHAIN] == "forecast"
        for name in (inv.GOAL_TABLES_INCOMPLETE, inv.GOAL_TABLES_STALE,
                     inv.FORECAST_STALE):
            assert name in inv.CONDITIONS and name.startswith(inv.PREFIX)

    def test_each_condition_is_in_the_registry_with_a_lever(self):
        from core.alerting import REGISTRY
        from core.data_quality import DEFAULT_REMEDIATION, remediation_for

        for name in (inv.GOAL_TABLES_INCOMPLETE, inv.GOAL_TABLES_STALE,
                     inv.FORECAST_STALE):
            assert name in REGISTRY, name
            assert remediation_for([name]) != [DEFAULT_REMEDIATION], name

    def test_an_unreadable_group_is_blindness(self):
        facts = inv.Facts(watched=(chain.CHAIN,), now=NOW,
                          forecast=inv.Unwatched("relation does not exist"))
        issues = inv.check_chain_invariants(facts)
        assert _names(issues) == [inv.UNWATCHED]
        assert chain.CHAIN in issues[0].description
        assert inv.unverified_conditions(issues) == sorted(inv.CONDITIONS)

    def test_flagged_with_its_preconditions_it_is_watched(self, ready):
        ready.setenv(chain.WRITE_ENV, "postgres")
        assert inv.watched_chains() == {chain.CHAIN: None}

    def test_flagged_but_held_it_is_not(self, ready):
        ready.setenv(chain.WRITE_ENV, "postgres")
        ready.setenv("KS_GOALS_HISTORY", "bridge")
        assert inv.watched_chains() == {}

    def test_read_facts_reads_the_four_tables_with_the_stamp(self, ready):
        """One statement, today's Kyiv date as its parameter, and the latch
        stamp carried to the verdict."""
        seen = []

        class Conn:
            def transaction(self, **_kw):
                return _Tx()

            async def execute(self, sql, *a):
                seen.append(sql)

            async def fetchval(self, sql, *a):
                seen.append(sql)
                if "AT TIME ZONE" in sql:
                    return date(2026, 10, 8)
                return _aware(2026, 10, 8, 10, 0)

            async def fetchrow(self, sql, *a):
                seen.append((sql, a))
                return {"months": 12, "yoy_null": 0, "seasonal_at": MON,
                        "seasonal_unstamped": 0, "yoy_rows": 1,
                        "yoy_overall": __import__("decimal").Decimal("0.5019"),
                        "yoy_sample": 1, "yoy_from": date(2023, 12, 2),
                        "forecast_at": THU_TRAIN, "horizon": date(2026, 12, 7),
                        "forecast_unstamped": 0}

            async def fetch(self, sql, *a):
                seen.append(sql)
                return []

        class _Tx:
            async def __aenter__(self):
                return None

            async def __aexit__(self, *exc):
                return False

        class Pool:
            def acquire(self, timeout=None):
                conn = Conn()

                class Ctx:
                    async def __aenter__(self):
                        return conn

                    async def __aexit__(self, *exc):
                        return False
                return Ctx()

        stamp = _aware(2026, 10, 8, 9, 0)
        ready.setenv(chain.WRITE_ENV, "postgres")
        chain_latch.latch(chain.CHAIN, chain.WRITE_ENV)
        ready.setattr(chain_latch, "latched_at", lambda name: stamp.isoformat()
                      if name == chain.CHAIN else None)
        with patch("core.pg.require_revision", new=AsyncMock()):
            facts = asyncio.run(inv.read_facts(pool=Pool()))
        assert facts.whole is None and facts.watched == (chain.CHAIN,)
        assert isinstance(facts.forecast, inv.Forecast)
        assert facts.forecast.latched_at == stamp
        assert facts.forecast.yoy_overall == 0.5019
        (sql, params), = [s for s in seen if isinstance(s, tuple)]
        assert params == (date(2026, 10, 8),)
        for table in chain.CHAIN_TABLES:
            if table != "app.weekly_patterns":   # read by nothing it judges
                assert table in sql, table
        assert inv.check_chain_invariants(facts) == []
