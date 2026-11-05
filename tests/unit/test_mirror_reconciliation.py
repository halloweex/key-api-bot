"""`compare_table`, the comparison the `mirror_landing` layer still runs.

It was Reconciliation A's — Postgres landing against DuckDB landing — and
step 14's S14-12 deleted those comparisons; `reconcile_bot_state` (SQLite
against `app.*`) still runs it, so its rules are pinned here on the catalogue's
specs, which `core.pg_landing.carry_retired_catalogue` still reads.

What follows is the rule as it was written for the landing mirror.

The check exists because step 05 closes on it — zero findings, tolerance zero
— and the hard part was never the comparison. It was telling the one
difference that is not a defect from the one that is, and both look like "one
row short" from a row count.

Production, the morning the mirror first ran: DuckDB 1,004 products, Postgres
1,003. The extra is id 1055, last synced 2026-06-13; KeyCRM stopped serving it
and `upsert_products` never deletes, so a payload-fed mirror can never learn of
it again. `meta.mirror_state.last_ok_at` is what separates that from a lost
row, because a successful mirror ships the whole catalogue — so anything DuckDB
wrote *before* the last success and Postgres still lacks is a row KeyCRM has
retired, and anything written *after* it is either in flight or lost.

Those three cases, and the suppression that keeps an unshipped table from
reporting every one of its rows as missing, are what these tests pin.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest

from core.data_quality import (
    DIGEST_MAX_AGE_HOURS,
    WATCHED_LAYERS,
    Severity,
)
from core.landing_rows import CATEGORY_COLUMNS, PRODUCT_COLUMNS
from core.mirror_reconciliation import (
    MIRROR_LAYER,
    MirroredTable,
    compare_table,
    configured,
)

NOW = datetime(2026, 8, 24, 12, 0, tzinfo=timezone.utc)
LAST_OK = NOW - timedelta(hours=4)

# The catalogue's two specs, as `core.mirror_reconciliation` declared them
# until step 14's S14-13 deleted them with their last reader (the catalogue
# carry). `compare_table` stays — `reconcile_bot_state` runs it — and these
# are the shapes its tests were written against.
PRODUCTS = MirroredTable(
    pg_table="bronze.products",
    dk_table="products",
    columns=tuple(PRODUCT_COLUMNS),
    numeric=("price",),
)
CATEGORIES = MirroredTable(
    pg_table="bronze.categories",
    dk_table="categories",
    columns=tuple(CATEGORY_COLUMNS),
)


def _watermark(last_ok_at=LAST_OK, failures=0, last_error=None):
    return {
        "table_name": "bronze.products",
        "last_attempted_at": last_ok_at,
        "last_ok_at": last_ok_at,
        "failures_since_ok": failures,
        "last_error": last_error,
        "last_rows": 3,
    }


def _row(id_, name="Toner", cat=11, brand="Wellage", sku="1678", price="700.00"):
    return (id_, name, cat, brand, sku, Decimal(price))


def _sides(ids=(1, 2, 3), synced=None):
    """Three identical products on both sides, all synced before the mirror ran."""
    synced = synced or (LAST_OK - timedelta(days=1))
    dk = {i: _row(i) for i in ids}
    return dk, {i: synced for i in ids}, {i: _row(i) for i in ids}


def _names(issues):
    return {i.check_name for i in issues}


def _by_name(issues, name):
    return next(i for i in issues if i.check_name == name)


class TestAgreement:
    def test_identical_sides_report_nothing(self):
        dk, synced, pg = _sides()
        assert compare_table(PRODUCTS, dk, synced, pg, _watermark(), now=NOW) == []

    def test_decimal_and_float_price_are_the_same_price(self):
        """`0.00` against `0.0` is a repr, not a discrepancy.

        Both drivers return Decimal for these columns today. The normalisation
        is for the day one of them stops, and this pins that a difference in
        type never reaches the report.
        """
        dk, synced, pg = _sides(ids=(1,))
        dk[1] = (1, "Toner", 11, "Wellage", "1678", 700.0)
        pg[1] = (1, "Toner", 11, "Wellage", "1678", Decimal("700.00"))
        # Both sides go through the same normaliser in the real read path.
        from core.mirror_reconciliation import _normalise_row
        dk[1] = _normalise_row(dk[1], PRODUCTS.columns, PRODUCTS.numeric)
        pg[1] = _normalise_row(pg[1], PRODUCTS.columns, PRODUCTS.numeric)
        assert compare_table(PRODUCTS, dk, synced, pg, _watermark(), now=NOW) == []


class TestTheRowKeyCRMForgot:
    """The 1004/1003 case. INFO, counted, never a defect."""

    def test_row_written_before_the_last_success_is_retired_not_lost(self):
        dk, synced, pg = _sides()
        dk[1055] = _row(1055)
        synced[1055] = datetime(2026, 6, 13, 9, 46, tzinfo=timezone.utc)
        issues = compare_table(PRODUCTS, dk, synced, pg, _watermark(), now=NOW)

        assert _names(issues) == {"mirror_retired_rows"}
        found = _by_name(issues, "mirror_retired_rows")
        assert found.severity is Severity.INFO
        assert found.count == 1
        assert found.sample_ids == (1055,)

    def test_retired_rows_never_summon_a_critical(self):
        """An INFO finding is content; it must not make the run CRITICAL."""
        from core.data_quality import overall_severity

        dk, synced, pg = _sides()
        dk[1055] = _row(1055)
        synced[1055] = LAST_OK - timedelta(days=70)
        issues = compare_table(PRODUCTS, dk, synced, pg, _watermark(), now=NOW)
        assert overall_severity(issues, []) is Severity.INFO


class TestTheRowTheMirrorLost:
    def test_written_after_the_last_success_and_past_grace_is_critical(self):
        dk, synced, pg = _sides()
        dk[9001] = _row(9001)
        synced[9001] = LAST_OK + timedelta(minutes=30)   # after the mirror ran
        issues = compare_table(
            PRODUCTS, dk, synced, pg, _watermark(), now=NOW, grace_minutes=15,
        )

        found = _by_name(issues, "mirror_missing_rows")
        assert found.severity is Severity.CRITICAL
        assert found.sample_ids == (9001,)
        assert "mirror_retired_rows" not in _names(issues)

    def test_written_inside_the_grace_window_is_in_flight(self):
        """The two writes are consecutive statements; a sync mid-flight is not a fault."""
        dk, synced, pg = _sides()
        dk[9001] = _row(9001)
        synced[9001] = NOW - timedelta(minutes=5)
        issues = compare_table(
            PRODUCTS, dk, synced, pg, _watermark(), now=NOW, grace_minutes=15,
        )
        assert issues == []

    def test_a_null_synced_at_is_treated_as_old(self):
        """Unknown provenance must not be reported as a mirror failure."""
        dk, synced, pg = _sides()
        dk[9001] = _row(9001)
        synced[9001] = None
        issues = compare_table(PRODUCTS, dk, synced, pg, _watermark(), now=NOW)
        assert _names(issues) == {"mirror_retired_rows"}


class TestTheOtherDirections:
    def test_row_only_in_postgres_is_an_orphan(self):
        dk, synced, pg = _sides()
        pg[4242] = _row(4242)
        found = _by_name(
            compare_table(PRODUCTS, dk, synced, pg, _watermark(), now=NOW),
            "mirror_orphan_rows",
        )
        assert found.severity is Severity.WARN
        assert found.sample_ids == (4242,)

    def test_a_value_disagreement_is_critical_and_names_the_column(self):
        """No arithmetic sits between the two sides, so drift is impossible."""
        dk, synced, pg = _sides()
        pg[2] = _row(2, brand="Wellagе")   # Cyrillic е — the classic
        pg[3] = _row(3, price="701.00")
        issues = compare_table(PRODUCTS, dk, synced, pg, _watermark(), now=NOW)

        found = _by_name(issues, "mirror_row_values")
        assert found.severity is Severity.CRITICAL
        assert found.count == 2
        assert found.sample_ids == (2, 3)
        assert "brand (1)" in found.description
        assert "price (1)" in found.description

    def test_null_on_one_side_only_is_a_disagreement(self):
        """423 of 1,004 products have no brand; a NULL that appears is still news."""
        dk, synced, pg = _sides()
        pg[2] = _row(2, brand=None)
        found = _by_name(
            compare_table(PRODUCTS, dk, synced, pg, _watermark(), now=NOW),
            "mirror_row_values",
        )
        assert found.sample_ids == (2,)


class TestTheWatermarkItself:
    def test_never_shipped_reports_once_and_suppresses_the_rows(self):
        """`bronze.categories` was empty for a schedule, not a fault.

        Without the suppression the check's first act would be 28 categories
        reported missing every morning until the weekly full sync runs.
        """
        dk = {i: (i, f"cat {i}", None) for i in range(1, 29)}
        synced = {i: LAST_OK - timedelta(days=3) for i in dk}
        issues = compare_table(CATEGORIES, dk, synced, {}, None, now=NOW)

        assert _names(issues) == {"mirror_never_shipped"}
        found = issues[0]
        assert found.severity is Severity.WARN
        assert found.count == 28

    def test_a_watermark_with_no_success_counts_as_never_shipped(self):
        dk, synced, pg = _sides()
        issues = compare_table(
            PRODUCTS, dk, synced, pg, _watermark(last_ok_at=None), now=NOW,
        )
        assert _names(issues) == {"mirror_never_shipped"}

    def test_a_failing_mirror_is_critical_and_carries_its_error(self):
        """The state `/api/health` was documented to publish and does not."""
        dk, synced, pg = _sides()
        issues = compare_table(
            PRODUCTS, dk, synced, pg,
            _watermark(failures=7, last_error="NotNullViolationError: name"),
            now=NOW,
        )
        found = _by_name(issues, "mirror_failing")
        assert found.severity is Severity.CRITICAL
        assert found.count == 7
        assert "NotNullViolationError" in found.description

    def test_failing_and_never_shipped_are_both_reported(self):
        dk, synced, pg = _sides()
        issues = compare_table(
            PRODUCTS, dk, synced, pg,
            _watermark(last_ok_at=None, failures=3, last_error="boom"),
            now=NOW,
        )
        assert _names(issues) == {"mirror_failing", "mirror_never_shipped"}

    def test_a_naive_timestamp_does_not_crash_the_comparison(self):
        """asyncpg returns tz-aware; a fixture or a driver change may not."""
        dk, synced, pg = _sides()
        dk[1055] = _row(1055)
        synced[1055] = (LAST_OK - timedelta(days=1)).replace(tzinfo=None)
        naive = _watermark(last_ok_at=LAST_OK.replace(tzinfo=None))
        assert _names(compare_table(PRODUCTS, dk, synced, pg, naive, now=NOW)) == {
            "mirror_retired_rows"
        }


class TestTheKillSwitch:
    def test_configured_is_false_without_a_dsn(self, monkeypatch):
        monkeypatch.delenv("KS_PG_DSN", raising=False)
        assert configured() is False
        monkeypatch.setenv("KS_PG_DSN", "postgresql://x@y/z")
        assert configured() is True


class TestThePostgresAloneChecksAskForAPostgres:
    """Step 14's S14-13 re-gated the four checks that asked
    `pg_landing.enabled()` — the landing mirror, deleted there — on
    `configured()`: they read Postgres alone, so a Postgres is what they
    need. Without `KS_PG_DSN` each answers nothing and asks nothing (a
    morning's run on a laptop must not persist a failed layer); with it, each
    reaches Postgres whatever `KS_MIRROR_LANDING` says. Mutation: put
    `if not pg_landing.enabled()` back in any of them, and the first test
    fails for it."""

    CHECKS = (
        "reconcile_order_utm_completeness",
        "reconcile_buyer_completeness",
        "reconcile_bot_state",
        "reconcile_order_versions",
    )

    @staticmethod
    def _call(name, tmp_path):
        import core.mirror_reconciliation as mr

        if name == "reconcile_bot_state":
            db = tmp_path / "bot.db"
            db.touch()
            return getattr(mr, name)(db)
        return getattr(mr, name)()

    @pytest.mark.asyncio
    @pytest.mark.parametrize("name", CHECKS)
    async def test_without_a_postgres_it_asks_nothing(self, name, monkeypatch, tmp_path):
        from unittest.mock import AsyncMock

        monkeypatch.delenv("KS_PG_DSN", raising=False)
        monkeypatch.setenv("KS_BOT_STORE", "sqlite")
        asked = AsyncMock(side_effect=AssertionError("reached Postgres"))
        monkeypatch.setattr("core.pg.get_pool", asked)
        assert await self._call(name, tmp_path) == []
        asked.assert_not_awaited()

    @pytest.mark.asyncio
    @pytest.mark.parametrize("mirror", ["0", "off", "garbage", ""])
    @pytest.mark.parametrize("name", CHECKS)
    async def test_with_one_it_asks_whatever_the_mirror_flag_says(
            self, name, mirror, monkeypatch, tmp_path):
        from unittest.mock import AsyncMock

        monkeypatch.setenv("KS_PG_DSN", "postgresql://nobody@127.0.0.1:1/none")
        monkeypatch.setenv("KS_MIRROR_LANDING", mirror)
        monkeypatch.setenv("KS_BOT_STORE", "sqlite")
        asked = AsyncMock(side_effect=RuntimeError("reached Postgres"))
        monkeypatch.setattr("core.pg.get_pool", asked)
        with pytest.raises(RuntimeError, match="reached Postgres"):
            await self._call(name, tmp_path)
        asked.assert_awaited_once()


class TestWiring:
    def test_the_layer_is_watched(self):
        """Absent from WATCHED_LAYERS, the catch-up re-queues it every restart."""
        assert MIRROR_LAYER in WATCHED_LAYERS
        assert MIRROR_LAYER in DIGEST_MAX_AGE_HOURS

    def test_the_job_is_registered_daily_and_clear_of_the_sunday_cron(self):
        from core.scheduler import CATCHUP_CHECKS

        assert CATCHUP_CHECKS["dq_mirror_landing"][0] == MIRROR_LAYER

class TestIgnoredBookkeepingColumns:
    """A column that records when the copy was taken, not what it holds.

    `app.sku_inventory_status.updated_at` is stamped on every row by every
    DuckDB rebuild, and the replica is copied minutes before the next one — so
    the two stores disagree on it for 891 of 891 rows, permanently. It fired
    CRITICAL for days from 2026-09-01 while hiding the three rows that really
    did differ.
    """

    def _spec(self, **kw):
        from core.mirror_reconciliation import MirroredTable
        base = dict(
            pg_table="app.sku_inventory_status",
            dk_table="sku_inventory_status",
            columns=("offer_id", "quantity", "reserve", "updated_at"),
            key_columns=("offer_id",),
            synced_column="updated_at",
            full_replace=True,
        )
        base.update(kw)
        return MirroredTable(**base)

    def test_the_stamp_alone_no_longer_makes_rows_differ(self):
        from core.mirror_reconciliation import _normalise_row
        spec = self._spec(ignore_columns=("updated_at",))
        dk = _normalise_row((7, 5, 2, "2026-09-03T04:14"), spec.columns,
                            spec.numeric, spec.ignore_columns)
        pg = _normalise_row((7, 5, 2, "2026-09-03T03:13"), spec.columns,
                            spec.numeric, spec.ignore_columns)
        assert dk == pg

    def test_a_real_difference_beside_the_stamp_still_shows(self):
        """The three `reserve` rows are the signal the 891 was burying."""
        from core.mirror_reconciliation import _normalise_row
        spec = self._spec(ignore_columns=("updated_at",))
        dk = _normalise_row((7, 5, 9, "2026-09-03T04:14"), spec.columns,
                            spec.numeric, spec.ignore_columns)
        pg = _normalise_row((7, 5, 2, "2026-09-03T03:13"), spec.columns,
                            spec.numeric, spec.ignore_columns)
        assert dk != pg

    def test_without_the_setting_the_stamp_still_differs(self):
        """Guards the default: this is opt-in per table, not a blanket rule."""
        from core.mirror_reconciliation import _normalise_row
        spec = self._spec()
        dk = _normalise_row((7, 5, 2, "2026-09-03T04:14"), spec.columns,
                            spec.numeric, spec.ignore_columns)
        pg = _normalise_row((7, 5, 2, "2026-09-03T03:13"), spec.columns,
                            spec.numeric, spec.ignore_columns)
        assert dk != pg

    def test_masking_does_not_move_the_key(self):
        """`_row_key` and `sample_index` address by position, so the ignored
        column is substituted rather than dropped."""
        from core.mirror_reconciliation import _normalise_row, _row_key
        spec = self._spec(ignore_columns=("updated_at",))
        row = (7, 5, 2, "whenever")
        assert len(_normalise_row(row, spec.columns, spec.numeric,
                                  spec.ignore_columns)) == len(spec.columns)
        assert _row_key(spec, row) == 7
        assert spec.sample_index == 0

class TestSyncedColumnDualRole:
    """A stamp may be both the clock and a compared value — on one condition.

    `synced_column` normally sits outside `columns` (landing's `synced_at`), and
    `fetch_duckdb_rows` promises it is "read alongside but never compared". When
    a spec lists it in `columns` too, that promise is only kept by the grace
    window, and the grace window is per row.

    So the rule is: **a dual-role stamp is safe exactly when the writer stamps
    one row at a time.**

      * `order_backfill_misses.checked_at` — moves when that id is re-recorded;
      * `inventory_history.recorded_at`    — `DELETE ... WHERE date = ?` then
        re-insert, so only today's row is ever restamped;
      * `manager_classifications.set_at`   — a human's decision, carried
        through the full replace.

    `sku_inventory_status.updated_at` is the one that broke it: the refresh is
    a whole-table DELETE+INSERT writing CURRENT_TIMESTAMP to all 891 rows, so
    the grace stopped being per-row and became all-or-nothing — every row
    forgiven, or every row reported. It reported 891 for days.

    This test exists so the next spec with a dual-role stamp has to say which
    kind it is, instead of finding out in production.
    """

    # Reviewed 2026-09-03, 2026-09-07 and 2026-09-11. Adding a spec here is a
    # claim that its writer stamps one row at a time; if it rewrites the
    # table, it belongs in `ignore_columns` instead. Step 14's S14-12 deleted
    # the comparisons against DuckDB and their specs, so only the bot state's
    # three dual-role stamps are left to classify — all event times written
    # when that row's event happened, so each dates its own row.
    PER_ROW_STAMPS = {
        "app.celebrated_milestones": "celebrated_at",
        "app.report_history": "created_at",
        "app.user_preferences": "updated_at",
    }
    # None left: `app.sku_inventory_status`, `bronze.offers` and the forecast
    # group, whose stamps were whole-table, were specs of the deleted
    # comparisons.
    WHOLE_TABLE_STAMPS: dict = {}

    def _dual_role(self):
        """Every spec in the module, not the two tuples this first scanned.

        Until 2026-09-11 it read `MIRRORED_TABLES` and `OPERATIONAL_TABLES`
        only, and so covered four of eleven dual-role stamps: a guard whose
        whole job is "the next one has to declare itself" was not looking at
        `BOT_STATE_TABLES`, `SMS_TABLES`, `DASHBOARD_USER_TABLES`, or the two
        standalone specs. `silver.order_utm.parsed_at` fired in production
        while this test was green.

        Walks the module instead, so a sixth group cannot reopen the blind
        spot by existing.
        """
        import core.mirror_reconciliation as m
        from core.mirror_reconciliation import MirroredTable

        found = {}
        for name in dir(m):
            value = getattr(m, name)
            if isinstance(value, MirroredTable):
                specs = [value]
            elif (isinstance(value, tuple) and value
                  and all(isinstance(x, MirroredTable) for x in value)):
                specs = list(value)
            else:
                continue
            for spec in specs:
                if spec.synced_column and spec.synced_column in spec.columns:
                    found[spec.pg_table] = spec
        return found

    def test_every_dual_role_spec_has_been_classified(self):
        """The guard: a new one fails here until somebody decides which it is."""
        known = set(self.PER_ROW_STAMPS) | set(self.WHOLE_TABLE_STAMPS)
        found = set(self._dual_role())
        assert found == known, (
            "a spec now lists its synced_column in columns and is not "
            f"classified: {found ^ known}. Decide whether its writer stamps "
            "one row at a time (leave it compared) or the whole table (add it "
            "to ignore_columns), then record the answer here."
        )

    def test_per_row_stamps_stay_compared(self):
        """Not swept up by the fix: on these the comparison is meaningful."""
        specs = self._dual_role()
        for table, column in self.PER_ROW_STAMPS.items():
            assert specs[table].synced_column == column
            assert column not in specs[table].ignore_columns, table

    def test_whole_table_stamps_are_never_compared(self):
        specs = self._dual_role()
        for table, column in self.WHOLE_TABLE_STAMPS.items():
            assert specs[table].synced_column == column
            assert column in specs[table].ignore_columns, table
            assert column in specs[table].columns, "still shipped, still the clock"


class TestGraceOnDifferingValues:
    """A row written between the copy and the check legitimately differs.

    `compare_bucket` has always asked whether such a row was in flight.
    `compare_table` — the path every small table uses — did not, until
    2026-09-11. The cost was a CRITICAL every morning naming three rows of
    `reserve` that had moved minutes earlier: nine firings over ten days, with
    the sample ids rotating each time, which is the signature of a race and not
    of a stuck row.
    """

    def _spec(self, **kw):
        from core.mirror_reconciliation import MirroredTable
        base = dict(
            pg_table="app.sku_inventory_status",
            dk_table="sku_inventory_status",
            columns=("offer_id", "reserve", "updated_at"),
            key_columns=("offer_id",),
            synced_column="updated_at",
            full_replace=True,
        )
        base.update(kw)
        return MirroredTable(**base)

    def _sides(self, dk_reserve, pg_reserve, stamp):
        dk = {470: (470, dk_reserve, stamp)}
        pg = {470: (470, pg_reserve, stamp)}
        return dk, {470: stamp}, pg

    def test_a_row_written_minutes_ago_is_forgiven(self):
        """The production case: `reserve` moved eight minutes before the
        check, on a table copied hourly."""
        from core.mirror_reconciliation import compare_table
        spec = self._spec()
        dk, synced, pg = self._sides(4, 7, NOW - timedelta(minutes=8))
        assert compare_table(spec, dk, synced, pg, _watermark(), now=NOW) == []

    def test_a_row_older_than_the_window_still_reports(self):
        """The leniency is a window, not a tolerance."""
        from core.mirror_reconciliation import compare_table
        spec = self._spec()
        dk, synced, pg = self._sides(4, 7, NOW - timedelta(hours=6))
        issues = compare_table(spec, dk, synced, pg, _watermark(), now=NOW)
        assert _names(issues) == {"mirror_row_values"}
        assert _by_name(issues, "mirror_row_values").count == 1

    def test_a_whole_table_stamp_forgives_nothing(self):
        """The safety property. `sku_inventory_status.updated_at` is restamped
        on all 891 rows by every rebuild, so a fresh stamp says nothing about
        any particular row — granting grace on it would forgive the whole
        table every morning the refresh happened to be recent, which is how a
        real defect would disappear."""
        from core.mirror_reconciliation import compare_table
        spec = self._spec(ignore_columns=("updated_at",))
        assert spec.stamp_is_per_row is False
        dk, synced, pg = self._sides(4, 7, NOW - timedelta(minutes=1))
        issues = compare_table(spec, dk, synced, pg, _watermark(), now=NOW)
        assert _names(issues) == {"mirror_row_values"}

    def test_a_row_with_no_stamp_is_never_forgiven(self):
        """Absent is not recent."""
        from core.mirror_reconciliation import compare_table
        spec = self._spec()
        dk = {470: (470, 4, None)}
        pg = {470: (470, 7, None)}
        issues = compare_table(spec, dk, {470: None}, pg, _watermark(), now=NOW)
        assert _names(issues) == {"mirror_row_values"}

    def test_a_naive_stamp_is_read_as_utc(self):
        """DuckDB hands some columns back without a tzinfo; comparing those to
        an aware cutoff raises rather than forgiving."""
        from core.mirror_reconciliation import compare_table
        spec = self._spec()
        naive = (NOW - timedelta(minutes=8)).replace(tzinfo=None)
        dk, synced, pg = self._sides(4, 7, naive)
        assert compare_table(spec, dk, synced, pg, _watermark(), now=NOW) == []

class TestStampIsPerRow:
    """Spelled with specs of their own since step 14's S14-12 deleted the
    comparisons whose specs these read (`app.sku_inventory_status`,
    `app.order_backfill_misses`, `app.sms_campaign_members`)."""

    def test_it_is_derived_from_ignore_columns_not_declared_twice(self):
        from core.mirror_reconciliation import MirroredTable
        whole = MirroredTable(
            pg_table="app.sku_inventory_status", dk_table="sku_inventory_status",
            columns=("offer_id", "updated_at"), key_columns=("offer_id",),
            synced_column="updated_at", ignore_columns=("updated_at",))
        per_row = MirroredTable(
            pg_table="app.order_backfill_misses", dk_table="order_backfill_misses",
            columns=("id", "checked_at"), synced_column="checked_at")
        assert whole.stamp_is_per_row is False
        assert per_row.stamp_is_per_row is True

    def test_an_expression_stamp_is_per_row(self):
        """`COALESCE(delivered_at, ...)` is still a function of the row."""
        from core.mirror_reconciliation import MirroredTable
        members = MirroredTable(
            pg_table="app.sms_campaign_members", dk_table="sms_campaign_members",
            columns=("id",), synced_column="COALESCE(delivered_at, sent_at)")
        assert members.synced_column.startswith("COALESCE")
        assert members.stamp_is_per_row is True

    def test_no_stamp_at_all_is_not_per_row(self):
        from core.mirror_reconciliation import MirroredTable
        spec = MirroredTable(
            pg_table="t", dk_table="t", columns=("id",), synced_column=None,
        )
        assert spec.stamp_is_per_row is False
