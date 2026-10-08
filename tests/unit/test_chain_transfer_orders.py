"""The copy-back learns chain 3: the orders, their line items, their expenses
and the backfill-miss ledger (G15).

Three of the four are landing tables the sync's mirrors ship from the same
parse as DuckDB — mirrored, chain 4's third source — and the misses ledger is
replaced whole by `replicate_operational`. What is new is that a mirrored
table's rewrite clock is no longer always `bronze.buyers`: an order dates its
own rows (`bronze.orders.mirrored_at`), its line items their own
(`bronze.order_products.mirrored_at`, grouped by order), an expense its own,
and the handover's sentences name chain 3's levers. Against a real Postgres
the whole tool is proved in `tests/integration/test_chain_copy_back_orders.py`.

Each test names the mutation it exists to fail on.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal
from unittest.mock import AsyncMock

import pytest
import pytest_asyncio

from core import chain_transfer, pg_orders_write
from core.chain_transfer import chain_sequences, chain_specs, classify_handover
from core.mirror_reconciliation import _normalise_row

ORDERS, LINES, EXPENSES, MISSES = pg_orders_write.CHAIN_TABLES
T0 = datetime(2026, 9, 20, 10, 0, tzinfo=timezone.utc)


@pytest.fixture
def specs():
    return {s.pg_table: s for s in chain_specs(pg_orders_write)}


def _row(spec, **values):
    raw = tuple(values.get(c) for c in spec.compare.columns)
    return _normalise_row(raw, spec.compare.columns, spec.compare.numeric,
                          spec.compare.ignore_columns)


def _order(spec, oid, *, updated=T0, status=12):
    return _row(spec, id=oid, source_id=1, status_id=status, status_group_id=4,
                grand_total=Decimal("100.00"), ordered_at=T0, created_at=T0,
                updated_at=updated, buyer_id=7, manager_id=4)


def _line(spec, oid, pos, qty=1):
    return _row(spec, id=oid * 1000 + pos, order_id=oid, product_id=700 + pos,
                name="Товар", quantity=qty, price_sold=Decimal("50.00"))


def _names(issues):
    return {(i.check_name, i.severity.value) for i in issues}


class TestTheSpecs:
    def test_four_specs_in_chain_order(self, specs):
        assert list(specs) == [ORDERS, LINES, EXPENSES, MISSES]
        assert [specs[t].kind for t in specs] == [
            "mirrored", "mirrored", "mirrored", "operational"]

    def test_each_row_is_dated_by_the_table_its_writer_rewrites(self, specs):
        """A line item belongs to its order's landing (its words, its
        shipper), but what dates it is ITS OWN `mirrored_at`, grouped by its
        order: the order's header stamp moves on header-only writes too — the
        05:15 refresh and the comment restore — and dated by it a line only
        DuckDB holds under any refreshed order read as a basket the chain
        shrank (the chain-3 review). Mutation: date line items by their
        order's header."""
        assert (specs[ORDERS].rewrite_clock, specs[ORDERS].rewritten_by) == (ORDERS, "id")
        assert (specs[LINES].rewrite_clock, specs[LINES].rewritten_by) == (ORDERS, "order_id")
        assert (specs[EXPENSES].rewrite_clock, specs[EXPENSES].rewritten_by) == (EXPENSES, "id")
        assert specs[MISSES].rewrite_clock is None
        assert specs[ORDERS].rewrite_stamp == (ORDERS, "id")
        assert specs[LINES].rewrite_stamp == (LINES, "order_id")
        assert specs[EXPENSES].rewrite_stamp == (EXPENSES, "id")
        assert specs[MISSES].rewrite_stamp is None

    def test_an_order_is_ordered_by_keycrm_s_own_stamp(self, specs):
        """Mutation: drop the source clock — a DuckDB write after the latch
        would be read as the chain's and overwritten by the copy-back."""
        assert specs[ORDERS].clock == ("updated_at",)
        assert specs[LINES].clock == () and specs[EXPENSES].clock == ()
        assert specs[MISSES].clock == ("checked_at",)

    def test_the_comparison_is_whole_and_forgives_nothing(self, specs):
        """The fingerprint's blind spot — a text rewritten to the same length —
        is not affordable in the comparison that releases a latch, and a key
        on one side only is a failed copy whatever the daily check calls it."""
        for table in (ORDERS, LINES, EXPENSES, MISSES):
            assert specs[table].compare.synced_column is None
            assert specs[table].compare.full_replace is True
        assert "manager_comment" in specs[ORDERS].compare.columns
        assert specs[ORDERS].compare.numeric == ("grand_total",)

    def test_there_is_no_allocator_to_carry(self):
        """Ids are KeyCRM's, positional or the order's own."""
        assert chain_sequences(pg_orders_write) == ()

    def test_a_clock_table_outside_the_chain_raises(self, monkeypatch):
        clocks = dict(chain_transfer._REWRITE_CLOCK, **{LINES: "bronze.buyers"})
        monkeypatch.setattr(chain_transfer, "_REWRITE_CLOCK", clocks)
        with pytest.raises(LookupError, match="does not own"):
            chain_specs(pg_orders_write)

    def test_a_stamp_table_outside_the_chain_raises(self, monkeypatch):
        """Its owner row is what the stamp is compared against."""
        stamps = dict(chain_transfer._REWRITE_STAMP, **{LINES: ("bronze.buyers", "id")})
        monkeypatch.setattr(chain_transfer, "_REWRITE_STAMP", stamps)
        with pytest.raises(LookupError, match="does not own"):
            chain_specs(pg_orders_write)

    def test_a_mirrored_table_with_no_stamp_raises(self, monkeypatch):
        stamps = {k: v for k, v in chain_transfer._REWRITE_STAMP.items() if k != LINES}
        monkeypatch.setattr(chain_transfer, "_REWRITE_STAMP", stamps)
        with pytest.raises(LookupError, match="_REWRITE_STAMP"):
            chain_specs(pg_orders_write)

    def test_every_clock_table_has_its_words(self):
        """A landing with no sentences would raise at the handover, when an
        operator is reading it, rather than here."""
        assert set(chain_transfer._REWRITE_CLOCK.values()) <= set(chain_transfer._WORDS)


class TestBeforeTheFlip:
    def test_an_order_only_in_duckdb_names_the_backfill(self, specs):
        s = specs[ORDERS]
        issues = classify_handover(s, {1: _order(s, 1)}, {}, moved_on=False)
        assert _names(issues) == {("handover_rows_missing", "CRITICAL")}
        assert "/api/mirror/backfill/orders" in issues[0].description
        assert "buyer" not in issues[0].description

    def test_an_order_held_differently_names_the_resync(self, specs):
        s = specs[ORDERS]
        issues = classify_handover(s, {1: _order(s, 1, status=12)},
                                   {1: _order(s, 1, status=19)}, moved_on=False)
        assert _names(issues) == {("handover_rows_differ", "CRITICAL")}
        assert "/api/duckdb/resync" in issues[0].description

    def test_a_line_item_only_postgres_holds_is_refused(self, specs):
        s = specs[LINES]
        issues = classify_handover(s, {}, {1002: _line(s, 1, 2)}, moved_on=False)
        assert _names(issues) == {("handover_rows_ahead", "CRITICAL")}
        assert "line item" in issues[0].description


class TestAfterTheLatch:
    def test_a_shrunk_basket_is_the_chain_s_work(self, specs):
        """Order 1's line items were replaced since the latch with one fewer:
        the one only DuckDB holds is a line it dropped. `rewritten` is the set
        of orders whose Postgres line items carry a stamp at or after the
        latch. Mutation: treat a missing line item as stranded whatever its
        order — every shrunk basket would refuse the copy-back."""
        s = specs[LINES]
        dk = {1001: _line(s, 1, 1), 1002: _line(s, 1, 2)}
        pg = {1001: _line(s, 1, 1)}
        issues = classify_handover(s, dk, pg, moved_on=True, rewritten=frozenset({1}))
        assert _names(issues) == {("handover_rows_missing", "INFO")}
        assert "basket" in issues[0].description

    def test_a_line_item_nothing_explains_is_stranded(self, specs):
        s = specs[LINES]
        issues = classify_handover(s, {1002: _line(s, 1, 2)}, {}, moved_on=True,
                                   rewritten=frozenset())
        assert _names(issues) == {("handover_rows_missing", "CRITICAL")}
        assert "/api/duckdb/resync" in issues[0].description

    def test_an_order_duckdb_dates_later_refuses(self, specs):
        s = specs[ORDERS]
        issues = classify_handover(
            s, {1: _order(s, 1, updated=T0 + timedelta(hours=1), status=19)},
            {1: _order(s, 1, updated=T0)}, moved_on=True, rewritten=frozenset({1}))
        assert _names(issues) == {("handover_rows_newer_in_duckdb", "CRITICAL")}
        assert "re-fetch these orders" in issues[0].description

    def test_an_equal_stamp_proves_nothing_and_the_rewrite_decides(self, specs):
        """KeyCRM does not bump `updated_at` on a status change, so an equal
        stamp is the common case for an order the chain rewrote."""
        s = specs[ORDERS]
        dk, pg = {1: _order(s, 1, status=12)}, {1: _order(s, 1, status=20)}
        assert _names(classify_handover(s, dk, pg, moved_on=True,
                                        rewritten=frozenset({1}))) == {
            ("handover_rows_differ", "INFO")}
        assert _names(classify_handover(s, dk, pg, moved_on=True,
                                        rewritten=frozenset())) == {
            ("handover_rows_differ", "CRITICAL")}

    def test_an_order_only_duckdb_holds_always_refuses(self, specs):
        """The chain's writer never deletes an order."""
        s = specs[ORDERS]
        issues = classify_handover(s, {5: _order(s, 5)}, {}, moved_on=True,
                                   rewritten=frozenset({5}))
        assert ("handover_rows_missing", "CRITICAL") in _names(issues)

    def test_an_order_the_chain_wrote_is_the_size_of_the_copy(self, specs):
        s = specs[ORDERS]
        issues = classify_handover(s, {}, {9: _order(s, 9)}, moved_on=True,
                                   rewritten=frozenset({9}))
        assert _names(issues) == {("handover_rows_ahead", "INFO")}


class TestTheHandoverReadsEachClockOnce:
    @pytest.mark.asyncio
    async def test_owners_first_and_each_table_judged_by_its_own_clock(
            self, specs, monkeypatch):
        """The orders are classified before their line items, a later DuckDB
        order is taken out of the orders' rewritten set before the line items
        are judged by it, and the expenses are judged by their own."""
        s_orders, s_lines, s_exp = specs[ORDERS], specs[LINES], specs[EXPENSES]
        later = _order(s_orders, 1, updated=T0 + timedelta(hours=1))
        sides = {
            ORDERS: ({1: later, 2: _order(s_orders, 2)},
                     {1: _order(s_orders, 1), 2: _order(s_orders, 2)}),
            LINES: ({1001: _line(s_lines, 1, 1), 2001: _line(s_lines, 2, 1, qty=2)},
                    {1001: _line(s_lines, 1, 1, qty=3), 2001: _line(s_lines, 2, 1)}),
            EXPENSES: ({}, {}),
            MISSES: ({}, {}),
        }
        asked, seen = [], {}

        async def rewritten(_pool, table, column):
            asked.append((table, column))
            return {(ORDERS, "id"): frozenset({1, 2}),
                    (LINES, "order_id"): frozenset({1, 2})}.get((table, column),
                                                             frozenset())

        def classify(spec, dk, pg, **kw):
            seen[spec.pg_table] = kw["rewritten"]
            return []

        monkeypatch.setattr(chain_transfer, "_rewritten_since_latch", rewritten)
        monkeypatch.setattr(chain_transfer, "_fetch_dk",
                            lambda conn, spec: sides[spec.pg_table][0])
        monkeypatch.setattr(chain_transfer, "_fetch_pg",
                            AsyncMock(side_effect=lambda pool, spec: sides[spec.pg_table][1]))
        monkeypatch.setattr(chain_transfer, "_not_null_columns", lambda conn, spec: ())
        monkeypatch.setattr(chain_transfer, "classify_handover", classify)

        class _Store:
            def connection(self):
                class _C:
                    async def __aenter__(s):
                        return object()

                    async def __aexit__(s, *a):
                        return False
                return _C()

        await chain_transfer._handover_issues(
            _Store(), object(), list(specs.values()), moved_on=True, max_samples=5)
        assert sorted(asked) == [(EXPENSES, "id"), (LINES, "order_id"), (ORDERS, "id")]
        assert list(seen)[:2] == [ORDERS, EXPENSES]          # owners first
        assert seen[ORDERS] == frozenset({1, 2})
        assert seen[LINES] == frozenset({2})                 # 1 is DuckDB's later version
        assert seen[EXPENSES] == frozenset()
        assert seen[MISSES] is None                          # operational: no rewrite clock

    @pytest.mark.asyncio
    async def test_only_a_rewrite_stamp_is_ever_read(self):
        for table, column in (("bronze.order_products", "id"),
                              ("bronze.orders", "order_id"),
                              ("bronze.order_products; DROP", "order_id")):
            with pytest.raises(ValueError):
                await chain_transfer._rewritten_since_latch(object(), table, column)


@pytest_asyncio.fixture
async def duck(tmp_path, monkeypatch):
    from core.duckdb_store import DuckDBStore

    monkeypatch.delenv("KS_WRITE_ORDERS", raising=False)
    monkeypatch.setenv("KS_MIRROR_LANDING", "0")
    store = DuckDBStore(db_path=tmp_path / "copy-back.duckdb")
    await store.connect()
    yield store
    await store.close()


def _payload(oid, *, status=12, products=2):
    return {
        "id": oid, "source_id": 1, "status_id": status, "status_group_id": 4,
        "grand_total": "100.00", "ordered_at": T0.isoformat(),
        "created_at": T0.isoformat(), "updated_at": T0.isoformat(),
        "buyer": {"id": 7}, "manager": {"id": 4}, "manager_comment": "utm",
        "promocode": None,
        "products": [{"name": f"Товар {i}", "quantity": 1, "price_sold": "50.00",
                      "offer": {"product_id": 700 + i}} for i in range(products)],
        "expenses": [{"id": 900 + oid, "expense_type_id": 1, "amount": 10.5,
                      "status": "paid"}],
    }


class TestTheDuckDBWriteOfTheSameKeys:
    """The copy-back writes every chain-3 table into DuckDB as DELETE then
    INSERT of the keys it already holds, in one transaction — and `orders`
    carries four secondary indexes besides its key. Proven on DuckDB itself
    rather than assumed (design §6): chains 1 and 4 only proved it on tables
    with fewer indexes."""

    @pytest.mark.asyncio
    async def test_each_table_round_trips_in_one_transaction(self, duck, specs):
        await duck.upsert_orders([_payload(1), _payload(2)])
        await duck.upsert_expenses_batch([_payload(1), _payload(2)])
        rewritten = {
            ORDERS: [tuple(r) for r in (
                (1, 1, 19, 6, Decimal("90.00"), T0, T0, T0, 7, 4, "utm2", None),
                (2, 1, 12, 4, Decimal("100.00"), T0, T0, T0, 7, 4, "utm", None))],
            LINES: [(1000, 1, 700, "Товар 0", 3, Decimal("50.00")),
                    (2000, 2, 700, "Товар 0", 1, Decimal("50.00"))],
            EXPENSES: [(901, 1, 1, Decimal("11.00"), None, "paid", None, None),
                       (902, 2, 1, Decimal("10.50"), None, "paid", None, None)],
        }
        async with duck.connection() as conn:
            conn.execute("BEGIN TRANSACTION")
            for table in (ORDERS, LINES, EXPENSES):
                chain_transfer._write_duckdb(conn, specs[table], rewritten[table])
            conn.execute("COMMIT")
            assert conn.execute(
                "SELECT status_id, grand_total, manager_comment FROM orders "
                "WHERE id = 1").fetchone() == (19, Decimal("90.00"), "utm2")
            # The basket shrank to one line per order: 1001 and 2001 are gone.
            assert [r[0] for r in conn.execute(
                "SELECT id FROM order_products ORDER BY id").fetchall()] == [1000, 2000]
            assert conn.execute(
                "SELECT amount FROM expenses WHERE id = 901").fetchone()[0] == Decimal("11.00")


class TestTheRunbookNamesChain3sShippers:
    def test_the_order_tables_are_sent_to_their_mirrors(self):
        said = " ".join(chain_transfer._runbook(pg_orders_write, executed=True,
                                                released=True))
        assert "order mirror" in said and "expense mirror" in said
        assert "/api/mirror/backfill/orders" in said
        assert "buyers mirror" not in said
        assert MISSES in said and "replicate_operational" in said

    def test_a_marker_without_owner_rows_names_chain_3_s_levers(self):
        said = " ".join(chain_transfer._marker_steps(pg_orders_write, pg_orders_write.CHAIN))
        assert "order mirror" in said and "/api/duckdb/resync" in said
        assert "buyers mirror" not in said
