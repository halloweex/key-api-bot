"""Reconciliation A: Postgres landing against DuckDB landing.

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
from pathlib import Path
from unittest.mock import patch

import pytest

from core.data_quality import (
    DIGEST_MAX_AGE_HOURS,
    WATCHED_LAYERS,
    Severity,
)
from core.duckdb_store import DuckDBStore
from core.mirror_reconciliation import (
    MIRROR_LAYER,
    MIRRORED_TABLES,
    MirroredTable,
    compare_table,
    configured,
    fetch_duckdb_rows,
    read_duckdb_side,
    reconcile_mirror,
)

NOW = datetime(2026, 8, 24, 12, 0, tzinfo=timezone.utc)
LAST_OK = NOW - timedelta(hours=4)

PRODUCTS = MIRRORED_TABLES[0]
CATEGORIES = MIRRORED_TABLES[1]


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
    @pytest.mark.asyncio
    async def test_mirror_off_reports_that_and_compares_nothing(self):
        """"1,004 rows missing" would be true and useless."""
        with patch("core.pg_landing.enabled", return_value=False):
            issues = await reconcile_mirror({})
        assert _names(issues) == {"mirror_disabled"}
        assert issues[0].severity is Severity.WARN

    def test_configured_is_false_without_a_dsn(self, monkeypatch):
        monkeypatch.delenv("KS_PG_DSN", raising=False)
        assert configured() is False
        monkeypatch.setenv("KS_PG_DSN", "postgresql://x@y/z")
        assert configured() is True


class TestAgainstARealDuckDB:
    """The read half, against the schema it actually runs on."""

    async def _store(self, tmp_path: Path) -> DuckDBStore:
        store = DuckDBStore(db_path=tmp_path / "mirror.duckdb")
        await store.connect()
        return store

    @pytest.mark.asyncio
    async def test_reads_the_shared_contract_and_synced_at(self, tmp_path):
        store = await self._store(tmp_path)
        async with store.connection() as conn:
            conn.execute(
                "INSERT INTO products (id, name, category_id, brand, sku, price) "
                "VALUES (7, 'Тонер', 11, 'Wellage', '1678', 700.00)"
            )
            conn.execute(
                "INSERT INTO categories (id, name, parent_id) VALUES (11, 'Face', NULL)"
            )
            rows, synced = fetch_duckdb_rows(conn, PRODUCTS)
            side = read_duckdb_side(conn)

        assert rows == {7: (7, "Тонер", 11, "Wellage", "1678", Decimal("700.00"))}
        assert synced[7] is not None
        assert {"bronze.products", "bronze.categories"} <= set(side)
        assert side["bronze.categories"][0] == {11: (11, "Face", None)}

    @pytest.mark.asyncio
    async def test_a_real_read_compares_clean_against_an_identical_mirror(self, tmp_path):
        """End to end over the read path, with Postgres played by a dict."""
        store = await self._store(tmp_path)
        async with store.connection() as conn:
            conn.execute(
                "INSERT INTO products (id, name, category_id, brand, sku, price) "
                "VALUES (7, 'Тонер', 11, NULL, '1678', 0.00)"
            )
            dk_rows, synced = fetch_duckdb_rows(conn, PRODUCTS)

        assert compare_table(
            PRODUCTS, dk_rows, synced, dict(dk_rows), _watermark(), now=NOW,
        ) == []


class TestWiring:
    def test_the_layer_is_watched(self):
        """Absent from WATCHED_LAYERS, the catch-up re-queues it every restart."""
        assert MIRROR_LAYER in WATCHED_LAYERS
        assert MIRROR_LAYER in DIGEST_MAX_AGE_HOURS

    def test_the_job_is_registered_daily_and_clear_of_the_sunday_cron(self):
        from core.scheduler import CATCHUP_CHECKS

        assert CATCHUP_CHECKS["dq_mirror_landing"][0] == MIRROR_LAYER

    def test_every_pulled_whole_table_is_covered(self):
        assert {s.pg_table for s in MIRRORED_TABLES} == {
            "bronze.products", "bronze.categories",
            "bronze.managers", "app.manager_classifications",
        }
        for spec in MIRRORED_TABLES:
            assert isinstance(spec, MirroredTable)
            # Every key column is a real column, or `_row_key` indexes into
            # nothing and the comparison keys on garbage.
            for column in spec.key_columns:
                assert column in spec.columns

    def test_the_replicated_tables_have_no_retired_category(self):
        """A full replace writes every row it holds, so a row missing from
        Postgres cannot mean 'KeyCRM retired it' — that reading belongs to the
        payload-fed mirrors and would excuse a real loss here."""
        replicated = {"bronze.managers", "app.manager_classifications"}
        for spec in MIRRORED_TABLES:
            assert spec.full_replace is (spec.pg_table in replicated)

    def test_the_effective_dated_table_is_keyed_on_its_interval(self):
        spec = next(
            s for s in MIRRORED_TABLES
            if s.pg_table == "app.manager_classifications"
        )
        assert spec.key_columns == ("manager_id", "valid_from")
        assert spec.synced_column == "set_at"


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

    def test_the_live_spec_ignores_the_stamp_and_nothing_else(self):
        from core.mirror_reconciliation import OPERATIONAL_TABLES
        by_table = {s.pg_table: s for s in OPERATIONAL_TABLES}
        sku = by_table["app.sku_inventory_status"]
        assert sku.ignore_columns == ("updated_at",)
        assert "updated_at" in sku.columns, "still shipped and still the clock"
        assert sku.synced_column == "updated_at"
        # checked_at is deliberately both clock and compared value.
        assert by_table["app.order_backfill_misses"].ignore_columns == ()
        assert by_table["app.inventory_history"].ignore_columns == ()


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

    # Reviewed 2026-09-03, and again 2026-09-07 for `app.revenue_goals`.
    # Adding a spec here is a claim that its writer stamps one row at a time;
    # if it rewrites the table, it belongs in `ignore_columns` instead.
    PER_ROW_STAMPS = {
        "app.manager_classifications": "set_at",
        "app.order_backfill_misses": "checked_at",
        "app.inventory_history": "recorded_at",
        # `set_goal` writes one `period_type` under `ON CONFLICT (period_type)`
        # and stamps `updated_at` from Python, so the value dates that goal's
        # own change. Three rows at most, each with its own history.
        "app.revenue_goals": "updated_at",
        # Added 2026-09-11 with the widened scan below. All seven are event times
        # written when that row's event happened, so each dates its own row.
        "app.celebrated_milestones": "celebrated_at",
        "app.marketing_optouts": "opted_out_at",
        "app.report_history": "created_at",
        "app.sms_dlr_events": "first_seen_at",
        "app.user_preferences": "updated_at",
        # `parsed_at` is a column DEFAULT, so only the rows a reparse rewrites
        # take a new value. This is the one that fired while the test was green.
        "silver.order_utm": "parsed_at",
    }
    WHOLE_TABLE_STAMPS = {
        "app.sku_inventory_status": "updated_at",
    }

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

    def test_the_two_paths_now_agree(self):
        """`compare_bucket` and `compare_table` answered the same
        question differently, and that disagreement was the defect."""
        import inspect
        from core.mirror_reconciliation import compare_bucket, compare_table
        for fn in (compare_bucket, compare_table):
            src = inspect.getsource(fn)
            body = src.split("dk_rows.keys() & pg_rows.keys()")[1]
            assert "in_flight" in body, f"{fn.__name__} skips the grace window"


class TestStampIsPerRow:
    def test_it_is_derived_from_ignore_columns_not_declared_twice(self):
        from core.mirror_reconciliation import OPERATIONAL_TABLES
        by_table = {s.pg_table: s for s in OPERATIONAL_TABLES}
        assert by_table["app.sku_inventory_status"].stamp_is_per_row is False
        assert by_table["app.order_backfill_misses"].stamp_is_per_row is True

    def test_an_expression_stamp_is_per_row(self):
        """`COALESCE(delivered_at, ...)` is still a function of the row."""
        from core.mirror_reconciliation import SMS_TABLES
        members = next(s for s in SMS_TABLES
                       if s.pg_table == "app.sms_campaign_members")
        assert members.synced_column.startswith("COALESCE")
        assert members.stamp_is_per_row is True

    def test_no_stamp_at_all_is_not_per_row(self):
        from core.mirror_reconciliation import MirroredTable
        spec = MirroredTable(
            pg_table="t", dk_table="t", columns=("id",), synced_column=None,
        )
        assert spec.stamp_is_per_row is False
