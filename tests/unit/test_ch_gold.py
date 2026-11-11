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

import ast
import inspect
import re
from datetime import date
from decimal import Decimal
from pathlib import Path

from core import ch_gold
from core.ch_gold import (
    COLUMNS,
    _TABLE_DDL,
    parse_tsv,
    render_tsv,
)

ROOT = Path(__file__).resolve().parents[2]

# The copy mode's conditions. Deleted with it (O11 a) and never reused: a page
# under one of these names would read as the retired copy's.
COPY_MODE_CONDITIONS = frozenset({
    "ch_gold_unreachable", "ch_gold_ship_failed", "ch_gold_row_missing",
    "ch_gold_row_extra", "ch_gold_value_mismatch",
})


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
    """Structure, not prose: the DDL is parsed, not grepped for a comment."""

    def test_ddl_declares_exactly_the_shipped_columns_in_order(self):
        declared = re.findall(
            r"^\s{4}(\w+)\s+(?:Date|LowCardinality|Nullable|Decimal|Int32)",
            _TABLE_DDL, re.MULTILINE,
        )
        assert tuple(declared) == COLUMNS


class TestTheCopyModeIsGone:
    def test_the_module_holds_no_shipper_and_no_comparison(self):
        """O11 (a): nothing here ships, reads back or compares any more —
        the module is a DDL and a codec. Mutation: put back `ship_gold` (or
        any coroutine) in `core/ch_gold.py`."""
        coroutines = [name for name, fn in vars(ch_gold).items()
                      if inspect.iscoroutinefunction(fn)]
        assert coroutines == []
        for gone in ("ship_gold", "reconcile_ch_gold", "compare_rows",
                     "ensure_schema", "STAGING"):
            assert not hasattr(ch_gold, gone), gone

    def test_its_condition_names_are_registered_and_emitted_by_nothing(self):
        """A deleted condition name is never reused. Mutation: register or
        emit any of the five again (`check_name="ch_gold_ship_failed"`)."""
        from core.alerting import REGISTRY
        from core.data_quality import HUMAN_CHECK_NAMES, REMEDIATION

        emitted = set()
        for tree in ("core", "web", "bot", "scripts", "deploy"):
            for path in sorted((ROOT / tree).rglob("*.py")):
                for node in ast.walk(ast.parse(path.read_text(encoding="utf-8"))):
                    if isinstance(node, ast.Constant) and node.value in COPY_MODE_CONDITIONS:
                        emitted.add((str(path.relative_to(ROOT)), node.value))
        assert emitted == set()
        assert not COPY_MODE_CONDITIONS & set(REGISTRY)
        assert not COPY_MODE_CONDITIONS & set(HUMAN_CHECK_NAMES)
        assert not [p for p, _ in REMEDIATION if p.startswith("ch_gold")]
