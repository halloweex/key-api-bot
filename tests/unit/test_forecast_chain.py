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

    @pytest.mark.parametrize("table", chain.CHAIN_TABLES)
    def test_each_table_the_chain_owns_follows_once_latched(self, ready, table):
        """Parametrised over `CHAIN_TABLES`, not `TABLE_HOLES`, so a hole
        dropped from the second is a failure here rather than a case that
        quietly disappears. Mutation: `{growth_metrics}` removed from
        `TABLE_HOLES` — a read of that table would go by `KS_READ_GOALS`, with
        a fallback, to DuckDB's frozen copy."""
        chain_latch.latch(chain.CHAIN, chain.WRITE_ENV)
        hole = "{%s}" % table.split(".", 1)[1]
        assert chain.reads_the_chain(f"SELECT * FROM {hole}") is True

    def test_the_holes_are_the_tables_the_chain_owns(self):
        assert {"app." + h.strip("{}") for h in chain.TABLE_HOLES} == \
            set(chain.CHAIN_TABLES)
        assert len(chain.TABLE_HOLES) == len(chain.CHAIN_TABLES)

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


# ─── The repository routes ──────────────────────────────────────────────────

from core.duckdb_store import DuckDBStore  # noqa: E402
from core.repositories.goals import GoalsMixin  # noqa: E402

TABLES = {
    "seasonal": {
        1: {"month": 1, "seasonality_index": 0.95, "sample_size": 3,
            "avg_revenue": 1000.0, "min_revenue": 800.0, "max_revenue": 1200.0,
            "std_dev": 1.0, "confidence": "high"},
        2: {"month": 2, "seasonality_index": 1.05, "sample_size": 2,
            "avg_revenue": 1100.0, "min_revenue": 900.0, "max_revenue": 1300.0,
            "std_dev": 1.0, "confidence": "medium"},
    },
    "yoy": {"overall_yoy": 0.5019, "monthly_yoy": {1: 0.25, 2: 0.3},
            "yearly_data": [], "sample_size": 1},
    "yoy_overall": 0.50186,
    "history_bounds": (date(2023, 12, 2), date(2026, 10, 6)),
    "weekly_rows": [[1, 1, 0.25, 3], [1, 2, 0.24, 3]],
}


def _goal_rows(store_path):
    async def read():
        store = DuckDBStore(db_path=store_path)
        await store.connect()
        try:
            async with store.connection() as conn:
                return {t: conn.execute(f"SELECT * FROM {t} ORDER BY 1").fetchall()
                        for t in ("seasonal_indices", "growth_metrics",
                                  "weekly_patterns", "revenue_predictions")}
        finally:
            await store.close()
    return asyncio.run(read())


def _with_store(path, step):
    async def run():
        store = DuckDBStore(db_path=path)
        await store.connect()
        try:
            return await step(store)
        finally:
            await store.close()
    return asyncio.run(run())


class TestTheRepositoryRoutes:
    """Routed in the repository, where the Monday job and the POST both
    arrive (and `predict_month` for the forecast) — never in a route, where
    one caller would be missed."""

    def test_under_the_chain_the_goal_tables_go_to_postgres_and_duckdb_is_untouched(
            self, ready, tmp_path):
        ready.setenv(chain.WRITE_ENV, "postgres")
        path = tmp_path / "g.duckdb"
        before = _goal_rows(path)
        writer = AsyncMock(return_value={})
        with patch.object(chain, "persist_goal_tables", new=writer):
            _with_store(path, lambda s: s._persist_goal_tables(TABLES, NOW))
        (seasonal, yoy, monthly, weekly, now), _ = writer.await_args
        assert [list(r) for r in seasonal] == [
            [1, 0.95, 3, 1000.0, 800.0, 1200.0, "high"],
            [2, 1.05, 2, 1100.0, 900.0, 1300.0, "medium"]]
        assert tuple(yoy) == (0.50186, date(2023, 12, 2), date(2026, 10, 6), 1)
        assert monthly == {1: 0.25, 2: 0.3}
        assert [list(r) for r in weekly] == [[1, 1, 0.25, 3], [1, 2, 0.24, 3]]
        assert now is NOW
        assert _goal_rows(path) == before

    def test_off_the_writer_is_not_called_and_duckdb_is_written(self, ready, tmp_path):
        path = tmp_path / "g.duckdb"
        writer = AsyncMock(side_effect=AssertionError("routed while off"))
        with patch.object(chain, "persist_goal_tables", new=writer):
            _with_store(path, lambda s: s._persist_goal_tables(TABLES, NOW))
        rows = _goal_rows(path)
        assert [r[0] for r in rows["seasonal_indices"]] == [1, 2]
        assert len(rows["weekly_patterns"]) == 2 and rows["growth_metrics"]

    def test_flagged_but_held_writes_duckdb(self, ready, tmp_path):
        ready.setenv(chain.WRITE_ENV, "postgres")
        ready.setattr(read_fallback, "_mode", read_fallback.DUCKDB)
        path = tmp_path / "g.duckdb"
        writer = AsyncMock(side_effect=AssertionError("routed while held"))
        with patch.object(chain, "persist_goal_tables", new=writer):
            _with_store(path, lambda s: s._persist_goal_tables(TABLES, NOW))
        assert len(_goal_rows(path)["seasonal_indices"]) == 2

    def test_a_flag_nobody_can_read_writes_neither_store(self, ready, tmp_path):
        ready.setenv(chain.WRITE_ENV, "postgre")
        path = tmp_path / "g.duckdb"
        before = _goal_rows(path)
        with pytest.raises(RuntimeError, match=chain.WRITE_ENV):
            _with_store(path, lambda s: s._persist_goal_tables(TABLES, NOW))
        assert _goal_rows(path) == before

    PREDICTIONS = [{"date": "2026-10-07", "predicted_revenue": 1000.0},
                   {"date": "2026-10-08", "predicted_revenue": 1100.0}]
    METRICS = {"mae": 1.0, "mape": 2.0, "wape": 3.0}

    def test_under_the_chain_the_forecast_goes_to_postgres_stamped_now(
            self, ready, tmp_path):
        ready.setenv(chain.WRITE_ENV, "postgres")
        path = tmp_path / "p.duckdb"
        writer = AsyncMock(return_value=2)
        before = datetime.now(timezone.utc)
        with patch.object(chain, "store_predictions", new=writer):
            n = _with_store(path, lambda s: s.store_predictions(
                self.PREDICTIONS, "retail", self.METRICS))
        assert n == 2
        (preds, sales_type, metrics, created_at), _ = writer.await_args
        assert preds == self.PREDICTIONS and sales_type == "retail"
        assert metrics == self.METRICS
        assert created_at.tzinfo is not None and created_at >= before
        assert _goal_rows(path)["revenue_predictions"] == []

    def test_off_the_forecast_goes_to_duckdb(self, ready, tmp_path):
        path = tmp_path / "p.duckdb"
        writer = AsyncMock(side_effect=AssertionError("routed while off"))
        with patch.object(chain, "store_predictions", new=writer):
            _with_store(path, lambda s: s.store_predictions(
                self.PREDICTIONS, "retail", self.METRICS))
        assert len(_goal_rows(path)["revenue_predictions"]) == 2

    def test_nothing_to_store_asks_no_chain(self, ready, tmp_path):
        ready.setenv(chain.WRITE_ENV, "postgre")      # would raise if asked
        assert _with_store(tmp_path / "p.duckdb",
                           lambda s: s.store_predictions([], "retail")) == 0


class TestPredictionsStored:
    """`predict_month` swallows a failed store — policy unchanged — and
    `_train_impl` now says whether it landed, for /api/jobs and the POST."""

    @staticmethod
    def _service(monkeypatch, store):
        from core import prediction_service as ps
        from tests.unit.test_audit_fixes import _valid_training_df

        svc = ps.PredictionService()

        async def fake_get_store():
            return store

        monkeypatch.setattr("core.duckdb_store.get_store", fake_get_store)
        monkeypatch.setattr(svc, "_query_daily_revenue",
                            AsyncMock(return_value=_valid_training_df()))
        monkeypatch.setattr(ps, "_train_model", lambda df: (
            object(), {"wape": 27.66, "mape": 30.0, "mae": 1.0}, {}, 1.0))
        monkeypatch.setattr(svc, "_save_model", lambda: None)
        monkeypatch.setattr(ps, "_predict_future", lambda *a, **k: [
            {"date": "2026-10-07", "predicted_revenue": 1000.0}])
        return svc

    def test_a_store_that_fails_is_reported(self, monkeypatch):
        store = AsyncMock()
        store.store_predictions.side_effect = RuntimeError("Postgres refused")
        svc = self._service(monkeypatch, store)
        result = asyncio.run(svc._train_impl("retail"))
        assert result["status"] == "success"
        assert result["predictions_stored"] is False

    def test_a_store_that_lands_is_reported(self, monkeypatch):
        store = AsyncMock()
        svc = self._service(monkeypatch, store)
        result = asyncio.run(svc._train_impl("retail"))
        assert result["predictions_stored"] is True
        store.store_predictions.assert_awaited_once()

    def test_a_second_run_does_not_inherit_the_first_verdict(self, monkeypatch):
        store = AsyncMock()
        svc = self._service(monkeypatch, store)
        assert asyncio.run(svc._train_impl("retail"))["predictions_stored"] is True
        store.store_predictions.side_effect = RuntimeError("down")
        assert asyncio.run(svc._train_impl("retail"))["predictions_stored"] is False


# ─── The smart goal reads one set ───────────────────────────────────────────


class _Smart(GoalsMixin):
    """The history reads answered, the shared tables from `rows`."""

    def __init__(self, rows):
        self.rows = rows
        self.asked = []

    async def _get_ml_forecast_total(self, *a, **k):
        return 0.0

    async def _dynamic_growth_cap(self, *a, **k):
        return 0.35

    async def _last_year_month_revenue(self, *a, **k):
        return 3_000_000.0

    async def _recent_three_month_average(self, *a, **k):
        return 3_200_000.0

    async def _goal_tables_run(self, sql, params=None):
        self.asked.append((sql, params))
        return list(self.rows)


SHARED = [("seasonal", None, 1.0, 1000.0, 0.2, "high"),
          ("growth", None, 0.25, None, None, None),
          ("weekly", 1, 0.3333, None, None, None),
          ("weekly", 2, 0.3333, None, None, None),
          ("weekly", 3, 0.3333, None, None, None)]


class TestTheSmartGoalReadsOneSet:
    def test_one_statement_both_params_the_month(self):
        smart = _Smart(SHARED)
        asyncio.run(smart.generate_smart_goals(2026, 10, "retail"))
        ((sql, params),) = smart.asked
        for hole in ("{seasonal_indices}", "{growth_metrics}", "{weekly_patterns}"):
            assert hole in sql
        assert params == [10, 10]

    def test_the_week_order_is_not_the_engines(self):
        """Three equal weights and a residual of ₴10 000: `max` breaks the tie
        by insertion order, so a breakdown built in the engine's row order
        would hand the residual to week 3 for one engine and week 1 for the
        other."""
        straight = asyncio.run(_Smart(SHARED).generate_smart_goals(2026, 10))
        flipped = asyncio.run(_Smart(
            SHARED[:2] + SHARED[2:][::-1]).generate_smart_goals(2026, 10))
        assert list(straight["weekly"]["breakdown"].items()) == \
            list(flipped["weekly"]["breakdown"].items())
        breakdown = straight["weekly"]["breakdown"]
        assert sum(breakdown.values()) == straight["monthly"]["goal"]
        assert breakdown[1] > breakdown[3], "the residual landed on week 1"

    def test_the_values_are_read_as_before(self):
        result = asyncio.run(_Smart(SHARED).generate_smart_goals(2026, 10))
        assert result["monthly"]["seasonalityIndex"] == 1.0
        assert result["monthly"]["growthRate"] == 0.2
        assert result["monthly"]["confidence"] == "high"

    def test_a_month_that_shrank_takes_the_overall_rate(self):
        """A month whose own YoY is negative takes the overall rate — the
        `growth` row of the statement, never the `seasonal` row beside it.
        Every other fixture here has a positive monthly rate, where the overall
        one is never read. Mutation: `yoy_result` taken from the `seasonal`
        part, whose third column is the index (1.0) — the cap, 0.35, instead
        of 0.25."""
        rows = [("seasonal", None, 1.0, 1000.0, -0.1, "high"),
                ("growth", None, 0.25, None, None, None)] + SHARED[2:]
        result = asyncio.run(_Smart(rows).generate_smart_goals(2026, 10))
        assert result["monthly"]["growthRate"] == 0.25
        assert result["metadata"]["overallYoY"] == 0.25
        assert result["metadata"]["monthlyYoY"] == -0.1

    def test_an_empty_set_falls_back_as_it_always_did(self):
        result = asyncio.run(_Smart([]).generate_smart_goals(2026, 10))
        assert result["monthly"]["confidence"] == "low"
        assert result["monthly"]["seasonalityIndex"] == 1.0
        assert set(result["weekly"]["breakdown"]) == {1, 2, 3, 4, 5}


# ─── A failed read under the chain is a refusal, never a quiet answer ───────

from tests.unit.test_read_fallback_sites import (  # noqa: E402,F401 — fixtures
    admin_client,
    fresh_read_fallback,
)

def _failing_on(table):
    """`pg_goals_read.fetch` failing only for a statement naming `table` — a
    statement timeout or a lock wait on one table while the rest of Postgres
    answers, which is what lets the Gold read before it succeed."""
    async def fetch(sql, params=()):
        if table in sql:
            raise ConnectionError(f"statement timeout on app.{table}")
        return [(1000.0,)]
    return fetch


class _NoDuckDB(GoalsMixin):
    def connection(self):
        raise AssertionError("DuckDB was read; under the chain its copy is frozen")


@pytest.fixture
def latched(ready):
    ready.setenv(chain.WRITE_ENV, "postgres")
    chain_latch.latch(chain.CHAIN, chain.WRITE_ENV)
    read_fallback.reset_counts()
    yield ready
    read_fallback.reset_counts()


def _forecast(fetch):
    from web.services import dashboard_service

    with patch("core.duckdb_store.get_store",
               new=AsyncMock(return_value=_NoDuckDB())), \
            patch("core.pg_goals_read.fetch", new=fetch):
        return asyncio.run(dashboard_service.get_forecast_data("retail"))


class TestAFailedChainReadIsRefused:
    """Before the flip, under the precondition's `KS_READ_FALLBACK=off`, a
    failed forecast read was `fall_back`'s: a counted refusal, a 503 naming
    `goals`. The flip must not turn it into a raw exception, which the two
    handlers built to let `ReadUnavailable` through and contain everything
    else then swallow, counting nothing. Mutation: the chain branch of
    `_goals_run` (or `_goal_tables_run`) without its `try` —
    `get_forecast_data` returns None, the route's 200 "Forecast not available
    yet", and `_get_ml_forecast_total` 0.0, a goal without its ML signal."""

    def test_the_forecast_is_refused_not_unavailable(self, latched):
        with pytest.raises(read_fallback.ReadUnavailable) as raised:
            _forecast(_failing_on("revenue_predictions"))
        assert raised.value.surface == "goals"
        assert read_fallback.refusals()["goals"]["count"] == 1
        assert read_fallback.counts() == {}, "nothing was answered from DuckDB"

    def test_the_smart_goal_is_not_computed_without_its_ml_signal(self, latched):
        now = datetime.now(KYIV)
        with patch("core.pg_goals_read.fetch",
                   new=_failing_on("revenue_predictions")):
            with pytest.raises(read_fallback.ReadUnavailable):
                asyncio.run(_NoDuckDB()._get_ml_forecast_total(
                    now.year, now.month, "retail"))
        assert read_fallback.refusals()["goals"]["count"] == 1

    def test_the_goal_tables_read_is_refused_too(self, latched):
        from core.repositories.goals import _SHARED_GOAL_TABLES_SQL

        with patch("core.pg_goals_read.fetch", new=_failing_on("growth_metrics")):
            with pytest.raises(read_fallback.ReadUnavailable) as raised:
                asyncio.run(_NoDuckDB()._goal_tables_run(
                    _SHARED_GOAL_TABLES_SQL, [10, 10]))
        assert raised.value.surface == "goals"
        assert read_fallback.refusals()["goals"]["count"] == 1

    def test_a_read_that_answers_is_not_touched(self, latched):
        """Non-vacuity: with Postgres answering, the forecast is read from it."""
        async def fetch(sql, params=()):
            if "revenue_predictions" in sql:
                return [(date.today(), 1234.0, 1.0, 2.0, 3.0)]
            return [(1000.0,)]

        result = _forecast(fetch)
        assert result and result["predicted_remaining"] == 1234.0
        assert read_fallback.refusals() == {}

    def test_under_duckdb_a_latched_chain_still_refuses_and_says_so(self, latched):
        """A latched chain whose precondition has lapsed — a restart with
        `KS_READ_FALLBACK` back at duckdb. DuckDB has nothing to answer with but
        the pre-flip copy, so the read is refused all the same, and
        /api/health publishes the refusal under duckdb too, where the block
        used to carry none (the canary's `read_refused` reads it there)."""
        from web.routes.api.health import _read_fallback_mode

        latched.setattr(read_fallback, "_mode", read_fallback.DUCKDB)
        assert _read_fallback_mode().get("refused") is None
        with pytest.raises(read_fallback.ReadUnavailable):
            _forecast(_failing_on("revenue_predictions"))
        assert read_fallback.counts() == {}
        block = _read_fallback_mode()
        assert block["mode"] == "duckdb"
        assert block["refused"]["goals"]["count"] == 1


class TestTheForecastRouteUnderTheChain:
    """The same failure through the real app: 503 naming `goals`, never 200
    "Forecast not available yet"."""

    def test_a_failed_forecast_read_is_a_503(self, admin_client, fresh_read_fallback,
                                            monkeypatch):
        monkeypatch.setenv("KS_GOALS_HISTORY", "silver")
        monkeypatch.setenv("KS_READ_GOALS", "postgres")
        monkeypatch.setenv("KS_READ_FORECAST_INPUT", "postgres")
        monkeypatch.setenv("KS_PG_DSN", DSN)
        monkeypatch.setenv("KS_READ_FALLBACK", "off")
        monkeypatch.setenv(chain.WRITE_ENV, "postgres")
        fresh_read_fallback.configure_mode()
        chain_latch.latch(chain.CHAIN, chain.WRITE_ENV)
        monkeypatch.setattr("core.pg_goals_read.fetch",
                            _failing_on("revenue_predictions"))

        response = admin_client.get("/api/revenue/forecast")

        assert response.status_code == 503, response.text
        assert response.json()["surface"] == "goals"
        assert "statement timeout" not in response.text
        assert fresh_read_fallback.refusals()["goals"]["count"] == 1
        health = admin_client.get("/api/health").json()
        assert health["read_fallbacks"] == {}
        assert health["read_fallback_mode"]["refused"]["goals"]["count"] == 1
