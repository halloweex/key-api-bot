"""The two engines' Gold, compared when a day differs in both grains (L2-01).

Gold holds a fine row per source and a roll-up per day (`source_id` NULL), so
a real divergence between Postgres's and ClickHouse's aggregations touches a
day's fine rows and its roll-up together. `compare_gold_cells` sorted the
cell keys `(date, sales_type, source_id)` as they came, and Python cannot
order `None` against an `int`: that shape — the one the comparison exists
for — raised `TypeError`, `reconcile_clickhouse` raised with it, and the job
filed `gold_values_unwatched` ("the comparison raised", naming `KS_CH_URL`)
in place of the finding, losing the round-trip finding built beside it.

Mutation: sort the raw keys again (drop `key=_cell_order`) and every test
here that compares a day in both grains fails.
"""
from __future__ import annotations

from datetime import date
from decimal import Decimal
from unittest.mock import AsyncMock

import pytest

from core import ch_silver
from core.ch_silver import compare_gold_cells
from core.pg_gold import GOLD_COLUMNS

DAY = date(2026, 10, 1)


def _cell(source, revenue: str, day: date = DAY):
    row = [day, "retail", source] + [0] * (len(GOLD_COLUMNS) - 3)
    row[GOLD_COLUMNS.index("revenue")] = Decimal(revenue)
    row[GOLD_COLUMNS.index("avg_order_value")] = Decimal("0.00")
    return tuple(row)


def _names(issues):
    return [i.check_name for i in issues]


class TestBothGrainsAtOnce:
    def test_a_fine_row_and_its_roll_up_differing_is_a_mismatch(self):
        pg = [_cell(4, "1000"), _cell(None, "1000")]
        ch = [_cell(4, "900"), _cell(None, "900")]
        issues = compare_gold_cells(pg, ch)
        assert _names(issues) == ["ch_engines_gold_mismatch"]
        assert issues[0].count == 2

    def test_a_day_missing_in_both_grains_is_missing(self):
        pg = [_cell(4, "1000"), _cell(1, "50"), _cell(None, "1050")]
        issues = compare_gold_cells(pg, [])
        assert _names(issues) == ["ch_engines_gold_missing"]
        assert issues[0].count == 3

    def test_a_day_only_clickhouse_holds_in_both_grains_is_extra(self):
        ch = [_cell(4, "1000"), _cell(None, "1000")]
        issues = compare_gold_cells([], ch)
        assert _names(issues) == ["ch_engines_gold_extra"]

    def test_the_roll_up_sorts_first_in_its_day(self):
        assert sorted([(DAY, "retail", 4), (DAY, "retail", None), (DAY, "b2b", 1)],
                      key=ch_silver._cell_order) == [
            (DAY, "b2b", 1), (DAY, "retail", None), (DAY, "retail", 4)]

    def test_the_cent_of_tolerance_on_the_average_stays(self):
        a, b = list(_cell(None, "10")), list(_cell(None, "10"))
        i = GOLD_COLUMNS.index("avg_order_value")
        a[i], b[i] = Decimal("3.33"), Decimal("3.34")
        assert compare_gold_cells([tuple(a)], [tuple(b)]) == []


class TestTheJobGetsTheFindingNotTheBlindness:
    @pytest.mark.asyncio
    async def test_reconcile_clickhouse_returns_value_and_round_trip_findings(
        self, monkeypatch,
    ):
        """The shape that raised, through the whole verdict: the value
        finding and the round-trip finding built before it both come back,
        and nothing says the comparison did not run."""
        monkeypatch.setattr(ch_silver, "configured", lambda: True)
        monkeypatch.setattr(ch_silver, "_fetch_pg_silver",
                            AsyncMock(return_value=[(1, "shipped")]))
        monkeypatch.setattr(ch_silver, "_ship_silver_rows", AsyncMock())
        monkeypatch.setattr(ch_silver, "_mark_ok", AsyncMock())
        monkeypatch.setattr(ch_silver, "_mark_failed", AsyncMock())
        monkeypatch.setattr(ch_silver, "_derive_gold", AsyncMock(return_value=2))
        # The read-back of silver comes back empty: one row lost.
        monkeypatch.setattr(ch_silver, "_execute", AsyncMock(return_value=""))
        monkeypatch.setattr(ch_silver, "_fetch_pg_gold_cells", AsyncMock(
            return_value=[_cell(4, "1000"), _cell(None, "1000")]))
        monkeypatch.setattr(ch_silver, "_fetch_ch_gold_cells", AsyncMock(
            return_value=[_cell(4, "900"), _cell(None, "900")]))

        issues = await ch_silver.reconcile_clickhouse()

        assert sorted(_names(issues)) == ["ch_engines_gold_mismatch",
                                          "ch_silver_roundtrip"]
        assert ch_silver.GOLD_UNWATCHED not in _names(issues)
        assert ch_silver.gold_unverified_conditions(issues) == []
