"""The landing→Silver arc had nothing on it.

PK, FK, NULL and domain checks read `orders` in isolation. The cell guard and
`_gold_cell_values_check` read Silver→Gold. Between them sat the one arc with
no instrument, and two facts made it the vulnerable one: `silver_mode` has
never once been written as `full`, so no periodic full rebuild was covering
the gap, and an incremental rebuild only touches rows inside its scope — so a
row that fell out of scope stayed wrong indefinitely while every check in the
file reported clean.

The tests below corrupt Silver in the three ways it can be wrong and assert
that the arc sees each one, that a fresh warehouse is silent, and that an
order still in flight is not reported as missing.
"""
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from core.data_quality import (
    Severity,
    _silver_arc_check,
    check_internal_integrity,
)
from core.duckdb_store import DuckDBStore, silver_select_sql


async def _make_store(tmp_path: Path) -> DuckDBStore:
    store = DuckDBStore(db_path=tmp_path / "test.duckdb")
    await store.connect()
    return store


def _insert_order(conn, oid, buyer_id, when, total="1000.00"):
    conn.execute(
        """
        INSERT INTO orders (
            id, source_id, status_id, grand_total, ordered_at, created_at,
            updated_at, buyer_id, manager_id, manager_comment, promocode
        ) VALUES (?, 4, 12, ?, ?, ?, ?, ?, NULL, NULL, NULL)
        """,
        [oid, total, when, when, when, buyer_id],
    )


async def _seed(store, n=4):
    """Four orders, midday UTC so the Kyiv calendar date cannot drift.

    Silver derives `order_date` as the Kyiv date; anchoring at midday keeps
    the seeded instant and the derived date on the same day at every offset
    Kyiv has ever used. The neighbouring Gold test was broken three hours out
    of twenty-four for exactly this reason.
    """
    when = datetime.now(timezone.utc).replace(
        hour=12, minute=0, second=0, microsecond=0
    ) - timedelta(days=1)
    async with store.connection() as conn:
        for oid in range(1, n + 1):
            _insert_order(conn, oid, 10 + oid, when)
    await store.refresh_warehouse_layers(trigger="manual")
    return when


def _age_landing(conn):
    """Push every `synced_at` outside the grace window.

    A freshly seeded order is genuinely in flight, and the check is right to
    stay quiet about it. Every test that wants a finding has to first say the
    warehouse has had its chance.
    """
    conn.execute("UPDATE orders SET synced_at = now() - INTERVAL '1 hour'")


def _findings(conn):
    return {i.check_name: i for i in _silver_arc_check(conn)}


class TestOneDefinitionOfASilverRow:
    def test_the_rebuild_and_the_check_read_the_same_sql(self):
        """A check with its own copy of the projection asks a different
        question: whether two hand-written queries agree. Rule 1 of the
        charter, as a test."""
        import inspect
        from core import data_quality, duckdb_store

        assert "silver_select_sql" in inspect.getsource(
            duckdb_store.DuckDBStore.refresh_warehouse_layers
        )
        assert "silver_select_sql" in inspect.getsource(
            data_quality._silver_arc_check
        )

    @pytest.mark.asyncio
    async def test_every_silver_column_is_compared(self, tmp_path):
        """The check names Silver's columns a second time, and a second list
        is the thing this codebase keeps getting hurt by.

        A column added to `silver_orders` and not to `_SILVER_ROW_COLUMNS`
        would be a column the arc silently never compares — the check would
        stay green while the new value drifted. This fails instead.
        """
        from core.data_quality import _SILVER_ROW_COLUMNS

        store = await _make_store(tmp_path)
        try:
            async with store.connection() as conn:
                stored = {
                    r[1] for r in
                    conn.execute("PRAGMA table_info('silver_orders')").fetchall()
                }
        finally:
            await store.close()

        compared = {c for c, _ in _SILVER_ROW_COLUMNS} | {"id"}
        assert stored == compared, (
            "silver_orders and the arc check disagree about what a Silver "
            "row is: " + str(stored ^ compared)
        )


class TestAFreshWarehouseIsSilent:
    @pytest.mark.asyncio
    async def test_nothing_is_reported_after_a_clean_rebuild(self, tmp_path):
        store = await _make_store(tmp_path)
        try:
            await _seed(store)
            async with store.connection() as conn:
                _age_landing(conn)
                assert _findings(conn) == {}
        finally:
            await store.close()

    @pytest.mark.asyncio
    async def test_an_empty_database_is_silent(self, tmp_path):
        store = await _make_store(tmp_path)
        try:
            async with store.connection() as conn:
                assert _findings(conn) == {}
        finally:
            await store.close()


class TestARowLandingHasAndSilverDoesNot:
    @pytest.mark.asyncio
    async def test_a_missing_row_is_critical_and_carries_its_money(self, tmp_path):
        store = await _make_store(tmp_path)
        try:
            await _seed(store)
            async with store.connection() as conn:
                _age_landing(conn)
                conn.execute("DELETE FROM silver_orders WHERE id = 2")
                found = _findings(conn)["silver_missing_rows"]

            assert found.severity == Severity.CRITICAL
            assert found.count == 1
            assert found.sample_ids == (2,)
            assert "1,000.00" in found.description
        finally:
            await store.close()

    @pytest.mark.asyncio
    async def test_an_order_still_in_flight_is_not_missing(self, tmp_path):
        """The refresh runs every two minutes. Without the grace window this
        check fires on every sync, which is how an instrument becomes noise
        nobody reads."""
        store = await _make_store(tmp_path)
        try:
            when = await _seed(store)
            async with store.connection() as conn:
                _age_landing(conn)
                # A brand-new order: in landing, not yet in Silver.
                _insert_order(conn, 99, 99, when)
                conn.execute("UPDATE orders SET synced_at = now() WHERE id = 99")

                assert "silver_missing_rows" not in _findings(conn)
                assert "silver_missing_rows" in {
                    i.check_name for i in _silver_arc_check(conn, grace_minutes=0)
                }
        finally:
            await store.close()

    @pytest.mark.asyncio
    async def test_the_row_count_guard_already_repairs_a_plain_deletion(self, tmp_path):
        """Written expecting the opposite, and it found the guard instead.

        `row_count_match` joins `validation_passed`, so a Silver row simply
        going missing fails validation, marks the warehouse dirty and is
        rebuilt. The arc check does not replace that and should not claim to.
        What it adds here is the ids — the count's own answer is a full
        rebuild of everything.
        """
        store = await _make_store(tmp_path)
        try:
            await _seed(store)
            async with store.connection() as conn:
                conn.execute("DELETE FROM silver_orders WHERE id = 2")

            await store.refresh_warehouse_layers(
                trigger="dirty_flag", changed_order_ids=[1],
            )

            async with store.connection() as conn:
                restored = conn.execute(
                    "SELECT COUNT(*) FROM silver_orders WHERE id = 2"
                ).fetchone()[0]
            assert restored == 1, "the count guard drove the self-heal"
        finally:
            await store.close()

    @pytest.mark.asyncio
    async def test_a_ghost_and_a_missing_row_cancel_out_in_the_count(self, tmp_path):
        """The count's blind spot: two faults that keep it equal.

        One row gone and one row that should not exist leaves
        `COUNT(*) FROM orders == COUNT(*) FROM silver_orders` true, so the
        guard is silent and the self-heal never starts. Both faults are still
        there, one shorting the numbers and one inflating them.
        """
        store = await _make_store(tmp_path)
        try:
            await _seed(store)
            async with store.connection() as conn:
                _age_landing(conn)
                conn.execute("DELETE FROM silver_orders WHERE id = 2")
                conn.execute("""
                    INSERT INTO silver_orders
                    SELECT * REPLACE (999 AS id) FROM silver_orders WHERE id = 1
                """)

                landing, silver = conn.execute("""
                    SELECT (SELECT COUNT(*) FROM orders),
                           (SELECT COUNT(*) FROM silver_orders)
                """).fetchone()
                assert landing == silver, "the count guard sees nothing here"

                names = set(_findings(conn))
            assert {"silver_missing_rows", "silver_orphan_rows"} <= names
        finally:
            await store.close()


class TestARowSilverHasAndLandingDoesNot:
    @pytest.mark.asyncio
    async def test_a_ghost_row_is_reported(self, tmp_path):
        store = await _make_store(tmp_path)
        try:
            await _seed(store)
            async with store.connection() as conn:
                _age_landing(conn)
                conn.execute("DELETE FROM orders WHERE id = 3")
                found = _findings(conn)["silver_orphan_rows"]

            assert found.count == 1
            assert found.sample_ids == (3,)
        finally:
            await store.close()

    @pytest.mark.asyncio
    async def test_a_ghost_needs_no_grace_window(self, tmp_path):
        """Silver is only ever written from landing, so a row with nothing
        behind it was never in flight."""
        store = await _make_store(tmp_path)
        try:
            await _seed(store)
            async with store.connection() as conn:
                conn.execute("DELETE FROM orders WHERE id = 3")
                assert "silver_orphan_rows" in _findings(conn)
        finally:
            await store.close()


class TestBothHaveTheRowAndDisagree:
    @pytest.mark.asyncio
    async def test_a_row_that_fell_out_of_scope_keeps_stale_values(self, tmp_path):
        """The production mechanism, and the reason the arc needs more than a
        count.

        Landing moves — an order becomes a return — and the next refresh is
        scoped to a different order, so Silver is never asked to rebuild that
        row. Counts still match, the revenue checksum still matches because
        Gold is rebuilt from the same stale Silver, and the cell guard sees
        the same cells. Validation reports success while Silver disagrees
        with landing about whether an order is money.
        """
        store = await _make_store(tmp_path)
        try:
            await _seed(store)
            async with store.connection() as conn:
                conn.execute("UPDATE orders SET status_id = 19 WHERE id = 1")

            res = await store.refresh_warehouse_layers(
                trigger="dirty_flag", changed_order_ids=[2],
            )
            assert res["validation_passed"] is True, (
                "no scalar and no cell guard compares a Silver row's values "
                "against landing"
            )

            async with store.connection() as conn:
                _age_landing(conn)
                found = _findings(conn)["silver_row_values"]

            assert 1 in found.sample_ids
            assert "is_return" in found.description
        finally:
            await store.close()

    @pytest.mark.asyncio
    async def test_a_changed_amount_is_caught(self, tmp_path):
        store = await _make_store(tmp_path)
        try:
            await _seed(store)
            async with store.connection() as conn:
                _age_landing(conn)
                conn.execute("UPDATE silver_orders SET grand_total = 7.77 WHERE id = 1")
                found = _findings(conn)["silver_row_values"]

            assert found.severity == Severity.WARN
            assert "grand_total" in found.description
            assert 1 in found.sample_ids
        finally:
            await store.close()

    @pytest.mark.asyncio
    async def test_a_cent_of_slack_on_money(self, tmp_path):
        store = await _make_store(tmp_path)
        try:
            await _seed(store)
            async with store.connection() as conn:
                _age_landing(conn)
                conn.execute(
                    "UPDATE silver_orders SET grand_total = grand_total + 0.005"
                )
                assert "silver_row_values" not in _findings(conn), (
                    "DECIMAL(12,2) rounding is not a fault"
                )
        finally:
            await store.close()

    @pytest.mark.asyncio
    async def test_a_flipped_flag_is_caught(self, tmp_path):
        """The August incident's shape: money right, classification wrong."""
        store = await _make_store(tmp_path)
        try:
            await _seed(store)
            async with store.connection() as conn:
                _age_landing(conn)
                conn.execute("UPDATE silver_orders SET is_return = TRUE WHERE id = 1")
                assert "is_return" in _findings(conn)["silver_row_values"].description
        finally:
            await store.close()

    @pytest.mark.asyncio
    async def test_a_wrong_sales_type_is_caught(self, tmp_path):
        """`sales_type` partitions Gold and gates every endpoint, and it is
        the column the migration's own reconciliation depends on."""
        store = await _make_store(tmp_path)
        try:
            await _seed(store)
            async with store.connection() as conn:
                _age_landing(conn)
                conn.execute("UPDATE silver_orders SET sales_type = 'b2b' WHERE id = 1")
                assert "sales_type" in _findings(conn)["silver_row_values"].description
        finally:
            await store.close()

    @pytest.mark.asyncio
    async def test_a_null_that_became_a_value_is_caught(self, tmp_path):
        """`<>` answers NULL here and the row would pass. `IS DISTINCT FROM`
        is why the column list carries a comparison mode at all."""
        store = await _make_store(tmp_path)
        try:
            await _seed(store)
            async with store.connection() as conn:
                _age_landing(conn)
                conn.execute("UPDATE silver_orders SET promocode = 'GHOST' WHERE id = 1")
                assert "promocode" in _findings(conn)["silver_row_values"].description
        finally:
            await store.close()

    @pytest.mark.asyncio
    async def test_a_stale_pass_two_is_caught(self, tmp_path):
        """Pass 2 takes its baseline from stored Silver, so a stale baseline
        reproduces itself. The check recomputes it from landing instead."""
        store = await _make_store(tmp_path)
        try:
            await _seed(store)
            async with store.connection() as conn:
                _age_landing(conn)
                conn.execute(
                    "UPDATE silver_orders SET is_new_customer = FALSE WHERE id = 1"
                )
                assert (
                    "is_new_customer"
                    in _findings(conn)["silver_row_values"].description
                )
        finally:
            await store.close()


class TestItCannotDriveARebuild:
    def test_the_refresh_never_calls_the_arc_check(self):
        """Report-only has to be structural, not a convention.

        Wiring this into `validation_passed` would mark the warehouse dirty
        on every finding, and three findings across ~46 000 rows failing
        every two minutes is the rebuild loop the design debate vetoed one
        layer up. The proof that it stays report-only is that the refresh
        cannot reach it.

        `test_a_row_that_fell_out_of_scope_keeps_stale_values` is the other
        half: a live finding, and validation still green.
        """
        import inspect
        from core import duckdb_store

        assert "_silver_arc_check" not in inspect.getsource(
            duckdb_store.DuckDBStore.refresh_warehouse_layers
        )


class TestItIsRegistered:
    @pytest.mark.asyncio
    async def test_the_scan_runs_the_arc(self, tmp_path):
        """A check nobody calls is rule 13 in miniature."""
        store = await _make_store(tmp_path)
        try:
            await _seed(store)
            async with store.connection() as conn:
                _age_landing(conn)
                conn.execute("DELETE FROM silver_orders WHERE id = 2")
                names = {i.check_name for i in check_internal_integrity(conn)}

            assert "silver_missing_rows" in names
        finally:
            await store.close()
