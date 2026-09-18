"""The copy-back's rules that need no database: the handover, the chunking, the flags.

DN-08's review found the first draft releasing a latch on a comparison of what
its own DELETE had just written. The answer is a question asked of the two
stores before anything is written — `classify_handover` — and it is pure, so
every branch of "a subset by key, and equal or newer" is pinned here without a
Postgres. The end-to-end cases, where the real repository methods produce the
states these rules name, are in `tests/integration/test_chain_copy_back.py`.
"""
from __future__ import annotations

import json
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from unittest.mock import AsyncMock, patch

import pytest

from core import chain_transfer, pg_expenses_write, pg_inventory_write
from core.chain_transfer import classify_handover

T0 = datetime(2026, 9, 15, 10, tzinfo=timezone.utc)


def _spec(chain, table):
    (spec,) = [s for s in chain_transfer.chain_specs(chain) if s.pg_table == table]
    return spec


def _severities(issues):
    return {(i.check_name, i.severity.value) for i in issues}


def _row(spec, **values):
    """A row as `fetch_duckdb_rows` returns it: in the COMPARISON's column
    order, which is not always the shipping order — `inventory_sku_history`
    compares offer first, and `stock_movements` reads `recorded_at` last,
    after the daily spec's own columns, because that spec leaves it out."""
    return tuple(values.get(c) for c in spec.compare.columns)


class TestTheClockIsDerived:
    def test_only_where_both_stores_carry_it_as_a_value(self):
        """`synced_column` from the daily spec, kept only when every column it
        names is shipped and compared. Anything else is one store's
        bookkeeping and orders nothing across the two."""
        clocks = {s.pg_table: s.clock
                  for chain in (pg_inventory_write, pg_expenses_write)
                  for s in chain_transfer.chain_specs(chain)}
        assert clocks == {
            "bronze.offers": (),                   # shipped, never compared
            "bronze.offer_stocks": (),             # synced_at vs mirrored_at
            "app.stock_movements": (),             # append: written once
            "app.sku_inventory_status": (),        # restamped by every rebuild
            "app.inventory_sku_history": (),       # append: written once
            "app.inventory_history": ("recorded_at",),
            "app.manual_expenses": ("updated_at", "created_at"),
        }


class TestEveryWrittenColumnIsCompared:
    """The comparison that releases a latch reads back everything it wrote.

    `app.stock_movements.recorded_at` is the column that forced this. The
    daily spec leaves it out because it is that spec's clock; the copy-back
    gives append tables no clock at all, so the column was covered neither
    way, and a review shifted every copied value by an hour and watched the
    latch released on a clean comparison.
    """

    def test_every_chain_every_table(self):
        """Walks `WRITE_CHAINS` rather than naming two: a guard that names
        its subjects only guards the ones already thought of."""
        from core.write_chains import WRITE_CHAINS

        for chain in WRITE_CHAINS:
            for spec in chain_transfer.chain_specs(chain):
                assert set(spec.columns) <= set(spec.compare.columns), (
                    spec.pg_table,
                    sorted(set(spec.columns) - set(spec.compare.columns)))

    def test_recorded_at_is_a_value_here_and_still_the_clock_daily(self):
        from core.mirror_reconciliation import APPEND_ONLY_TABLES

        spec = _spec(pg_inventory_write, "app.stock_movements")
        assert "recorded_at" in spec.compare.columns
        assert "recorded_at" not in spec.compare.ignore_columns
        # Reversed for this direction only: the daily spec keeps its clock.
        (daily,) = [s for s in APPEND_ONLY_TABLES
                    if s.pg_table == "app.stock_movements"]
        assert "recorded_at" not in daily.columns

    def test_what_it_forgives_is_the_daily_bookkeeping_and_nothing_else(self):
        """Read back and still not compared: a column in the daily spec's
        `ignore_columns` is read and then masked, here as there.

        The documentation says the latch-releasing comparison covers every
        written column EXCEPT these two, and names them — so this computes the
        set rather than trusting the sentence. Both are one stamp across the
        whole table per sync or rebuild, restamped wholesale by the writer's
        next run; a third, or a per-row fact joining them, must fail here
        before it can be forgiven in the one comparison that releases a latch.
        And the mask is the daily spec's own list, not a second opinion.
        """
        from core.mirror_reconciliation import OPERATIONAL_TABLES
        from core.write_chains import WRITE_CHAINS

        daily = {s.pg_table: s for s in OPERATIONAL_TABLES}
        forgiven = set()
        for chain in WRITE_CHAINS:
            for spec in chain_transfer.chain_specs(chain):
                masked = set(spec.columns) & set(spec.compare.ignore_columns)
                forgiven |= {(spec.pg_table, c) for c in masked}
                if spec.pg_table in daily:
                    assert spec.compare.ignore_columns == \
                        daily[spec.pg_table].ignore_columns, spec.pg_table
        assert forgiven == {("bronze.offers", "synced_at"),
                            ("app.sku_inventory_status", "updated_at")}


class TestAKeyOnlyDuckdbHolds:
    @pytest.mark.parametrize("moved_on", [False, True])
    def test_is_critical_on_either_side_of_the_latch(self, moved_on):
        """Before a flip it is stranded by it; after one a full-replace copy
        would delete it. The reviewer's reproduction was offer 2."""
        spec = _spec(pg_inventory_write, "bronze.offers")
        one = _row(spec, id=1, product_id=101, sku="S-1")
        two = _row(spec, id=2, product_id=102, sku="S-2")
        issues = classify_handover(spec, {1: one, 2: two}, {1: one},
                                   moved_on=moved_on)
        (missing,) = issues
        assert (missing.check_name, missing.severity.value) == (
            "handover_rows_missing", "CRITICAL")
        assert missing.sample_ids == (2,)


class TestAppendTablesHaveNoNewer:
    @staticmethod
    def _movement(spec, id, after=37, at=T0):
        return _row(spec, id=id, offer_id=1, product_id=101,
                    movement_type="stock_out", quantity_before=40,
                    quantity_after=after, delta=after - 40, reserve_before=2,
                    reserve_after=2, recorded_at=at, source="sync")

    def test_the_same_movement_at_another_time_is_another_event(self):
        """Identical in every column but `recorded_at`. The daily spec cannot
        see this pair; the handover, reading every written column, can."""
        spec = _spec(pg_inventory_write, "app.stock_movements")
        issues = classify_handover(
            spec, {2: self._movement(spec, 2)},
            {2: self._movement(spec, 2, at=T0 + timedelta(hours=1))},
            moved_on=True)
        assert _severities(issues) == {("handover_rows_differ", "CRITICAL")}

    @pytest.mark.parametrize("moved_on", [False, True])
    def test_two_rows_under_one_id_are_two_events(self, moved_on):
        """The collision the review traced: DuckDB wrote movement 2 after the
        last shipment, and Postgres — flooring on its own MAX(id) — issued 2
        again after the flip. Not an older and a newer; refused either way."""
        spec = _spec(pg_inventory_write, "app.stock_movements")
        issues = classify_handover(
            spec, {2: self._movement(spec, 2, after=37)},
            {2: self._movement(spec, 2, after=30)}, moved_on=moved_on)
        assert _severities(issues) == {("handover_rows_differ", "CRITICAL")}

    def test_a_postgres_row_below_the_watermark_can_never_come_back(self):
        spec = _spec(pg_inventory_write, "app.stock_movements")
        row = lambda i: self._movement(spec, i)  # noqa: E731
        issues = classify_handover(
            spec, {1: row(1), 5: row(5)},
            {1: row(1), 3: row(3), 5: row(5), 9: row(9)}, moved_on=True)
        by_name = {i.check_name: i for i in issues}
        assert by_name["handover_rows_behind_watermark"].severity.value == "CRITICAL"
        assert by_name["handover_rows_behind_watermark"].sample_ids == (3,)
        # Above the MAX is exactly what the copy carries.
        assert by_name["handover_rows_ahead"].severity.value == "INFO"
        assert by_name["handover_rows_ahead"].sample_ids == (9,)

    def test_an_inclusive_watermark_carries_its_own_day(self):
        """`inventory_sku_history` re-reads DuckDB's MAX(date) with `>=`, so a
        Postgres row on that day is carried, not stranded; the day before is
        not."""
        spec = _spec(pg_inventory_write, "app.inventory_sku_history")
        day = date(2026, 9, 17)

        def snap(d, offer):
            return (d, offer), _row(spec, date=d, offer_id=offer, quantity=5,
                                    reserve=0, price=Decimal("90.00"))

        dk = dict([snap(day, 1)])
        pg = dict([snap(day, 1), snap(day, 2), snap(day - timedelta(days=1), 2)])
        by_name = {i.check_name: i for i in classify_handover(
            spec, dk, pg, moved_on=True)}
        assert by_name["handover_rows_behind_watermark"].count == 1
        assert by_name["handover_rows_ahead"].count == 1


class TestMutableTablesAreEqualOrNewer:
    def _expense(self, amount, updated_at):
        spec = _spec(pg_expenses_write, "app.manual_expenses")
        return _row(spec, id=7, expense_date=date(2026, 9, 15),
                    category="marketing", expense_type="Facebook Ads",
                    amount=Decimal(amount), currency="UAH", created_at=T0,
                    updated_at=updated_at)

    def test_before_a_flip_any_difference_is_refused(self):
        """Postgres has no writer yet but the copy of DuckDB, so it cannot be
        newer: every difference is the copy being broken."""
        spec = _spec(pg_expenses_write, "app.manual_expenses")
        issues = classify_handover(
            spec, {7: self._expense("10.00", None)},
            {7: self._expense("20.00", T0 + timedelta(hours=1))}, moved_on=False)
        assert _severities(issues) == {("handover_rows_differ", "CRITICAL")}

    def test_after_the_latch_a_later_postgres_edit_is_the_copy_s_work(self):
        spec = _spec(pg_expenses_write, "app.manual_expenses")
        issues = classify_handover(
            spec, {7: self._expense("10.00", None)},
            {7: self._expense("20.00", T0 + timedelta(hours=1))}, moved_on=True)
        assert _severities(issues) == {("handover_rows_differ", "INFO")}

    def test_after_the_latch_a_later_duckdb_edit_is_refused(self):
        """An edit DuckDB made after the last shipment, which a copy-back
        would overwrite with the older Postgres value."""
        spec = _spec(pg_expenses_write, "app.manual_expenses")
        issues = classify_handover(
            spec, {7: self._expense("99.00", T0 + timedelta(hours=2))},
            {7: self._expense("20.00", T0 + timedelta(hours=1))}, moved_on=True)
        assert _severities(issues) == {("handover_rows_newer_in_duckdb", "CRITICAL")}

    def test_without_a_shared_clock_it_says_what_it_is_assuming(self):
        spec = _spec(pg_inventory_write, "bronze.offer_stocks")
        stock = lambda q: _row(spec, id=1, sku="S-1", price=Decimal("500"),  # noqa: E731
                               purchased_price=Decimal("250"), quantity=q,
                               reserve=2)
        (diff,) = classify_handover(spec, {1: stock(40)}, {1: stock(37)},
                                    moved_on=True)
        assert (diff.check_name, diff.severity.value) == ("handover_rows_differ", "INFO")
        assert "--handover" in diff.description and "taken as" in diff.description


class TestTheMarkerOnlyRefusal:
    """A marker with no owner rows: the steps that clear it, in their order."""

    def test_handover_comes_before_the_marker_is_touched(self):
        """The review's reproduction: an expense DuckDB held and never shipped,
        the marker taken, no owner rows. The steps used to offer "leave the
        flag at postgres, delete the marker, up -d" with nothing before them,
        and the next Postgres write would latch the chain over that expense.
        With no owner rows `--handover` applies the pre-flip rule — exactly
        the question — so it is step 1, and keeping postgres is conditional
        on it."""
        said = chain_transfer._marker_only(
            pg_expenses_write, "pg_expenses_write", "2026-09-18T06:00:00+00:00")
        handover = said.index("--handover")
        assert handover < said.index("delete the marker")
        assert handover < said.index("Leave it at postgres only if step 1")
        assert "exited 0" in said
        assert "Nothing was written" in said


class TestWhatAFailureAfterTheCommitSays:
    """`_after_commit` reads the latch it is handed and names one next step.

    Pure, so every state is pinned here; the integration tests produce the
    first two from real failures after a real COMMIT.
    """

    PLAN = {"rows": {"app.manual_expenses": 3}}

    def _said(self, stage="release", **latch):
        state = {"marker": None, "owned_since": None, "owners_error": None}
        state.update(latch)
        return chain_transfer._after_commit(
            pg_expenses_write, "pg_expenses_write", stage,
            ConnectionError("gone"), self.PLAN, state)

    def test_every_state_says_duckdb_has_changed(self):
        for latch in ({"marker": "t0", "owned_since": "t0"},
                      {"marker": "t0"}, {}, {"owners_error": "OSError: x"}):
            said = self._said(**latch)
            assert "COMMITTED to DuckDB" in said and "NOT as it was" in said
            assert "app.manual_expenses" in said

    def test_owner_rows_held_means_execute_again(self):
        said = self._said(marker="t0", owned_since="t0")
        assert "--execute again" in said and "--handover" not in said
        assert "Bringing web back up instead is safe" in said

    def test_owner_rows_held_without_a_marker_must_not_bring_web_up(self):
        """The copy-back runs on the owner rows alone — a lost `./data` — and
        then web up would route the writers by the flag again."""
        said = self._said(owned_since="t0")
        assert "--execute again" in said
        assert "Do not bring web back up first" in said

    def test_unreadable_owner_rows_wait_for_postgres(self):
        said = self._said(marker="t0", owners_error="ConnectionError: gone")
        assert "UNREADABLE" in said and "once Postgres answers" in said

    def test_a_marker_alone_prints_the_refusal_s_steps_handover_first(self):
        said = self._said(marker="t0")
        assert "between its two deletes" in said
        assert said.index("--handover") < said.index("delete the marker")

    def test_both_gone_is_released_and_checks_the_unlink_landed(self):
        said = self._said()
        assert "chain is released" in said
        assert "KS_WRITE_EXPENSES=duckdb" in said
        assert said.index("still absent") < said.index("docker compose up -d")

    def test_only_a_failed_checkpoint_speaks_of_the_wal(self):
        assert "WAL" in self._said(stage="checkpoint", marker="t0", owned_since="t0")
        assert "WAL" not in self._said(marker="t0", owned_since="t0")


class _Recorder:
    """A DuckDB connection that only remembers what it was asked."""

    def __init__(self):
        self.calls = []

    def execute(self, sql, params=None):
        self.calls.append((sql, params))


class TestTheWriteIsChunked:
    def test_no_statement_carries_more_than_a_chunk(self, monkeypatch):
        """`executemany` runs once per row in DuckDB's client — about eight
        minutes for chain 1's 164,000 history rows. Chunked VALUES is the
        guard, and this is its shape: every row once, in order, no statement
        over the chunk."""
        monkeypatch.setattr(chain_transfer, "DUCKDB_CHUNK", 1000)
        spec = _spec(pg_inventory_write, "app.inventory_sku_history")
        rows = [(date(2026, 1, 1) + timedelta(days=i // 900), i % 900, 1, 0,
                 Decimal("1.00")) for i in range(2500)]
        conn = _Recorder()

        chain_transfer._write_duckdb(conn, spec, rows)

        inserts = [(sql, params) for sql, params in conn.calls if "INSERT" in sql]
        width = len(spec.columns)
        assert [len(params) // width for _sql, params in inserts] == [1000, 1000, 500]
        assert [v for _s, params in inserts for v in params] == [
            v for row in rows for v in row]

    def test_the_chunk_stays_where_it_was_measured(self):
        """1,000 rows a statement moved 163,000 in 3.8 s; 5,000 was no faster.
        A larger chunk only grows the statement text and its parameter list."""
        assert 100 <= chain_transfer.DUCKDB_CHUNK <= 5000

    def test_a_full_replace_deletes_first_and_an_empty_table_inserts_nothing(self):
        spec = _spec(pg_inventory_write, "bronze.offers")
        conn = _Recorder()
        chain_transfer._write_duckdb(conn, spec, [])
        assert [sql for sql, _p in conn.calls] == ["DELETE FROM offers"]


def _script(argv, copy_back, close=None):
    """The real `main`, with no DuckDB file and `copy_back` standing in."""
    from scripts import chain_copy_back as script

    with patch("core.duckdb_store.DuckDBStore.connect", new=AsyncMock()), \
         patch("core.duckdb_store.DuckDBStore.close", new=close or AsyncMock()), \
         patch("core.chain_transfer.copy_back", new=copy_back):
        return script.main(argv)


class TestTheFlags:
    """`--dry-run` is the default by name too, and never beside `--execute`."""

    def _run(self, argv, copy_back):
        return _script(argv, copy_back)

    def test_dry_run_by_name_is_the_default(self, capsys):
        plan = {"chain": "pg_expenses_write", "latched_at": None,
                "owned_since": "x", "rows": {}, "executed": False, "runbook": []}
        copy_back = AsyncMock(return_value=plan)

        assert self._run(["expenses", "--dry-run"], copy_back) == 0
        assert copy_back.await_args.kwargs["dry_run"] is True

    @pytest.mark.parametrize("argv", [
        ["expenses", "--dry-run", "--execute"],
        ["expenses", "--handover", "--dry-run"],
        ["expenses", "--handover", "--execute"],
    ])
    def test_two_asks_at_once_are_refused_before_anything_opens(self, argv, capsys):
        copy_back = AsyncMock()
        assert self._run(argv, copy_back) == 2
        copy_back.assert_not_awaited()
        assert "REFUSED" in capsys.readouterr().err


class TestTheExitCodes:
    """0 released, 1 not committed, 2 refused, 3 committed and not released.

    3 exists because 1 used to be two things: an exception after the COMMIT
    left through the interpreter's own exit 1 — the code that says "rolled
    back, DuckDB as it was" — with the copy in DuckDB. The integration tests
    prove `copy_back` raises `CommittedNotReleased` for a real failure after
    a real COMMIT; these prove the script turns each outcome into its code.
    """

    PLAN = {"chain": "pg_expenses_write", "latched_at": "t0", "owned_since": "t0",
            "rows": {"app.manual_expenses": 3}, "runbook": []}

    @pytest.mark.parametrize("outcome, code", [
        ({"executed": True, "committed": True, "released": True, "findings": []}, 0),
        ({"executed": True, "committed": False, "released": False,
          "findings": [{"check": "mirror_row_values", "table": "app.manual_expenses",
                        "severity": "CRITICAL", "count": 1, "samples": [7],
                        "description": "a value differs"}]}, 1),
    ])
    def test_a_release_is_0_and_a_rollback_is_1(self, outcome, code, capsys):
        copy_back = AsyncMock(return_value={**self.PLAN, **outcome})
        assert _script(["expenses", "--execute"], copy_back) == code

    def test_a_refusal_is_2(self, capsys):
        refused = AsyncMock(side_effect=chain_transfer.CopyBackRefused("nope"))
        assert _script(["expenses", "--execute"], refused) == 2
        assert "REFUSED" in capsys.readouterr().err

    @pytest.mark.parametrize("close_fails", [False, True])
    def test_a_failure_after_the_commit_is_3_and_says_what_was_committed(
        self, close_fails, capsys,
    ):
        """And a close that raises cannot turn it back into 1. The checkpoint's
        likeliest failure is a full disk, and closing a DuckDB file
        checkpoints it again."""
        latch = {"marker": "t0", "owned_since": "t0", "owners_error": None}
        failed = chain_transfer.CommittedNotReleased(
            "the copy-back of pg_expenses_write COMMITTED to DuckDB and the "
            "checkpoint after it failed (OSError: disk full).",
            {**self.PLAN, "executed": True, "committed": True, "released": False,
             "latch": latch},
            latch,
        )
        close = AsyncMock(side_effect=OSError(28, "No space left on device")
                          if close_fails else None)

        assert _script(["expenses", "--execute", "--json"],
                       AsyncMock(side_effect=failed), close) == 3
        out, err = capsys.readouterr()
        assert "COMMITTED, NOT RELEASED" in err and "COMMITTED to DuckDB" in err
        printed = json.loads(out)
        assert printed["committed"] is True and printed["released"] is False
        assert printed["latch"] == latch
