"""`calculate_yoy_growth` with more than one pair of full years.

Production's first order is 2023-12-02, so until now there have been two full
years and one pair of them — and with one pair the recency weighting is
skipped. With two pairs it ran, and the yearly revenues are DuckDB DECIMAL sums
(`orders.grand_total` is DECIMAL(12,2)) multiplied by float weights: TypeError,
on the Monday job, `POST /goals/recalculate`, the YoY GET and every smart goal
recomputed from scratch.

The second pair was due on 2026-11-01, and not from a year that had ended: the
current year counted as "full" once it had 11 active months, so it would have
been compared, two months short, against a whole one. The current year is now
excluded by the calendar.

Years are counted back from the current year in Kyiv, not written as dates, so
the cases mean the same thing whenever the suite runs. Seeded orders may lie in
the future; nothing here reads the clock but the exclusion.
"""
from __future__ import annotations

from datetime import date, datetime, time, timedelta
from decimal import ROUND_HALF_UP, Decimal
from zoneinfo import ZoneInfo

import pytest
import pytest_asyncio

from core.duckdb_store import DuckDBStore

KYIV = ZoneInfo("Europe/Kyiv")


def _current_year() -> int:
    return datetime.now(KYIV).year


def _daily_total(d: date, first_year: int) -> Decimal:
    """A yearly trend and a seasonal shape, so every pair of years has its own
    rate and the weighting has something to weigh."""
    growth = Decimal("1") + Decimal("0.15") * (d.year - first_year)
    season = (Decimal("1.4") if d.month in (11, 12)
              else Decimal("0.8") if d.month in (6, 7) else Decimal("1"))
    return (Decimal(1000) * growth * season + (d.day % 7) * 13).quantize(Decimal("0.01"))


async def _seed(store: DuckDBStore, start: date, end: date) -> dict[int, Decimal]:
    """One retail order a day (source 1, no manager), at noon Kyiv. Returns
    the revenue per year, computed here and not read back."""
    rows, per_year, oid, d = [], {}, 1, start
    while d <= end:
        total = _daily_total(d, start.year)
        rows.append((oid, total, datetime.combine(d, time(12), KYIV)))
        per_year[d.year] = per_year.get(d.year, Decimal(0)) + total
        oid += 1
        d += timedelta(days=1)
    async with store.connection() as conn:
        conn.executemany(
            "INSERT INTO orders (id, source_id, status_id, grand_total, "
            "ordered_at, buyer_id, manager_id) VALUES (?, 1, 1, ?, ?, 1, NULL)",
            rows)
        conn.execute("DELETE FROM growth_metrics")
    await store.refresh_warehouse_layers(trigger="manual")
    return per_year


def _rate(per_year: dict[int, Decimal], year: int) -> Decimal:
    return (per_year[year] - per_year[year - 1]) / per_year[year - 1]


def _four_places(value: Decimal) -> Decimal:
    """`growth_metrics.value` is DECIMAL(8,4)."""
    return value.quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)


async def _stored(store: DuckDBStore):
    async with store.connection() as conn:
        return conn.execute(
            "SELECT value, sample_size FROM growth_metrics "
            "WHERE metric_type = 'yoy_overall'").fetchone()


@pytest_asyncio.fixture
async def store(tmp_path):
    s = DuckDBStore(db_path=tmp_path / "yoy.duckdb")
    await s.connect()
    try:
        yield s
    finally:
        await s.close()


class TestTwoPairsOfFullYears:
    @pytest.mark.asyncio
    async def test_the_recency_weighted_rate_is_computed_and_stored(self, store):
        y = _current_year()
        per_year = await _seed(store, date(y - 3, 1, 1), date(y - 1, 12, 31))

        out = await store.calculate_yoy_growth("retail")

        # Oldest pair weighs 1.0, newest 2.0.
        older, newer = _rate(per_year, y - 2), _rate(per_year, y - 1)
        assert abs(older - newer) > Decimal("0.01"), \
            "the fixture lost its trend: both pairs have one rate, nothing to weigh"
        expected = (older * 1 + newer * 2) / 3
        assert [r["year"] for r in out["yearly_data"]] == [y - 3, y - 2, y - 1]
        assert out["sample_size"] == 2
        assert isinstance(out["overall_yoy"], float)
        assert out["overall_yoy"] == round(float(expected), 4)

        value, sample_size = await _stored(store)
        assert value == _four_places(expected)
        assert sample_size == 2

    @pytest.mark.asyncio
    async def test_the_read_path_answers_the_same(self, store):
        y = _current_year()
        await _seed(store, date(y - 3, 1, 1), date(y - 1, 12, 31))

        read = await store.calculate_yoy_growth("retail", persist=False)
        written = await store.calculate_yoy_growth("retail")

        assert read == written


class TestTheCurrentYearIsNeverFull:
    """Production's shape on 2 November: the first order on 2 December three
    years back, and the current year at 11 active months."""

    @pytest.mark.asyncio
    async def test_eleven_active_months_of_this_year_are_not_compared(self, store):
        y = _current_year()
        per_year = await _seed(store, date(y - 3, 12, 2), date(y, 11, 2))

        out = await store.calculate_yoy_growth("retail")

        assert [r["year"] for r in out["yearly_data"]] == [y - 2, y - 1]
        assert out["sample_size"] == 1

        # One pair: no weighting, and the value is what it was before the
        # conversion to float, rounded the same way into the same column.
        exact = _rate(per_year, y - 1)
        assert isinstance(out["overall_yoy"], float)
        assert out["overall_yoy"] == round(float(exact), 4)
        value, sample_size = await _stored(store)
        assert value == _four_places(exact)
        assert sample_size == 1
