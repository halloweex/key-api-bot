"""What is left of step 4 of «Одна бронза»: ClickHouse Gold's shape and codec.

The copy mode — `ship_gold`, `reconcile_ch_gold` and their five `ch_gold_*`
conditions — was deleted by the owner's decision O11 (a) (2026-11-11); step 5's
derivation (`core/ch_silver.py`) had superseded it and nothing ran it. What
stays is the table's DDL and the TSV codec typed to it, which the derivation
and the engines' comparison import, so their tests stay: the wire format gets a
round-trip test because it is the one place where every column's type is
decided by hand.
"""
from __future__ import annotations

from datetime import date
from decimal import Decimal

from core.ch_gold import (
    COLUMNS,
    _TABLE_DDL,
    parse_tsv,
    render_tsv,
)
from tests.helpers.recon_chains_ddl import declared_columns


def _row(**overrides):
    """One gold cell with plausible defaults, positionally ordered."""
    base = {
        "date": date(2026, 8, 27),
        "sales_type": "retail",
        "source_id": 1,
        "revenue": Decimal("1834807.24"),
        "orders_count": 52,
        "unique_customers": 48,
        "new_customers": 7,
        "returning_customers": 41,
        "returns_count": 2,
        "returns_revenue": Decimal("1200.00"),
        "avg_order_value": Decimal("35284.75"),
    }
    base.update(overrides)
    return tuple(base[c] for c in COLUMNS)


class TestTheWireFormat:
    def test_round_trip_is_identity(self):
        rows = [
            _row(),
            _row(sales_type="b2b", source_id=None, revenue=Decimal("0.00")),
            _row(date=date(2024, 1, 1), sales_type="exhibition", source_id=5),
        ]
        assert parse_tsv(render_tsv(rows).decode("utf-8")) == rows

    def test_null_source_id_is_backslash_n(self):
        line = render_tsv([_row(source_id=None)]).decode("utf-8")
        assert "\\N" in line.split("\t")[2]

    def test_decimals_survive_as_decimals(self):
        (row,) = parse_tsv(render_tsv([_row()]).decode("utf-8"))
        assert isinstance(row[COLUMNS.index("revenue")], Decimal)
        assert row[COLUMNS.index("avg_order_value")] == Decimal("35284.75")

    def test_escaping_is_written_even_for_data_that_never_needs_it(self):
        hostile = _row(sales_type="re\ttail\\n")
        (back,) = parse_tsv(render_tsv([hostile]).decode("utf-8"))
        assert back == hostile

    def test_empty_set_renders_empty_body(self):
        assert render_tsv([]) == b""
        assert parse_tsv("") == []


class TestTheDDLAndTheColumnListAgree:
    """The derivation inserts COLUMNS by name into the table this DDL creates,
    live and staging, and the comparison reads them back by name: a column
    the DDL lacks fails every hourly derivation, one only the DDL has is
    written by nothing. Compared as a set, so the statement's layout, types
    and column order stay its own. On a real ClickHouse the whole path runs
    in `tests/integration/test_own_entity_not_revenue.py::TestClickHouse`."""

    def test_the_table_declares_exactly_the_shipped_columns(self):
        assert declared_columns(_TABLE_DDL.format(name="gold.daily_revenue")) == set(COLUMNS)
