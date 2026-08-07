"""The KeyCRM side must count exactly what the DuckDB side counts.

Every discrepancy the daily Layer-2 job reported on 2026-08-04 decomposed into
three defects in the comparator rather than any drift in the warehouse:

  * the KeyCRM rollup counted whole calendar months while DuckDB was clipped to
    the window — 410 orders / ₴1,075,169.68 of phantom "missing" data in May;
  * `seen` was stamped before the month check, so the 1st and 2nd of each month
    vanished from the KeyCRM side — 102 orders across June and July;
  * the watermark cut a wider set on the live side than on the synced copy —
    the remaining 22 orders, all in the current month.
"""
from datetime import date, datetime, timezone

import pytest

from core.reconciliation_io import _process_batch

WATERMARK = datetime(2026, 8, 4, 0, 0, tzinfo=timezone.utc)
WINDOW_START = date(2026, 5, 6)
WINDOW_END = date(2026, 8, 4)


def order(oid, ordered_at, *, source_id=1, status_id=12, grand_total=100.0,
          updated_at="2026-01-01T00:00:00+00:00", quantity=1):
    return {
        "id": oid,
        "source_id": source_id,
        "status_id": status_id,
        "ordered_at": ordered_at,
        "updated_at": updated_at,
        "grand_total": grand_total,
        "products": [{"quantity": quantity}],
    }


def run(batches, months):
    """Feed batches through the month loop the way the real fetcher does."""
    rollup, seen, inflight = {}, set(), set()
    from collections import defaultdict
    rollup = defaultdict(lambda: {"orders": 0, "qty": 0, "revenue": 0.0,
                                  "returns_count": 0, "returns_revenue": 0.0})
    for month in months:
        for batch in batches:
            _process_batch(batch, month, rollup, seen, WATERMARK,
                           WINDOW_START, WINDOW_END, inflight)
    return dict(rollup), inflight


class TestWindowClipping:
    def test_orders_before_window_start_are_not_counted(self):
        """May 1-5 sits in the May calendar month but outside the window."""
        batch = [order(1, "2026-05-03T10:00:00+00:00"),
                 order(2, "2026-05-07T10:00:00+00:00")]
        rollup, _ = run([batch], ["2026-05"])

        assert rollup[("2026-05", 1)]["orders"] == 1
        assert rollup[("2026-05", 1)]["revenue"] == 100.0

    def test_orders_after_window_end_are_not_counted(self):
        batch = [order(1, "2026-08-03T10:00:00+00:00"),
                 order(2, "2026-08-06T10:00:00+00:00")]
        rollup, _ = run([batch], ["2026-08"])

        assert rollup[("2026-08", 1)]["orders"] == 1

    def test_window_bounds_are_inclusive(self):
        batch = [order(1, "2026-05-06T10:00:00+00:00"),
                 order(2, "2026-08-04T10:00:00+00:00")]
        rollup, _ = run([batch], ["2026-05", "2026-08"])

        assert rollup[("2026-05", 1)]["orders"] == 1
        assert rollup[("2026-08", 1)]["orders"] == 1


class TestMonthBoundaryDedup:
    def test_next_month_order_survives_the_widening(self):
        """June 1st is pulled into May's fetch by the +2 day widening."""
        mays_page = [order(1, "2026-06-01T10:00:00+00:00")]
        junes_page = [order(1, "2026-06-01T10:00:00+00:00")]
        rollup, _ = run([mays_page], ["2026-05"])
        assert rollup.get(("2026-06", 1), {"orders": 0})["orders"] == 0

        rollup, _ = run([mays_page, junes_page], ["2026-05", "2026-06"])
        assert rollup[("2026-06", 1)]["orders"] == 1, (
            "order was consumed by May's pass and never counted in June"
        )

    def test_no_double_count_when_both_passes_return_it(self):
        page = [order(1, "2026-06-10T10:00:00+00:00")]
        rollup, _ = run([page, page, page], ["2026-05", "2026-06", "2026-07"])

        assert rollup[("2026-06", 1)]["orders"] == 1

    def test_previous_month_order_is_left_for_nobody_but_still_not_counted(self):
        """April 30 appears in May's -2 day widening; it is outside the run."""
        batch = [order(1, "2026-04-30T10:00:00+00:00")]
        rollup, _ = run([batch], ["2026-05"])

        assert rollup == {}


class TestWatermarkSymmetry:
    def test_inflight_ids_are_reported(self):
        batch = [
            order(1, "2026-07-10T10:00:00+00:00", updated_at="2026-08-04T06:00:00+00:00"),
            order(2, "2026-07-10T10:00:00+00:00", updated_at="2026-07-01T06:00:00+00:00"),
        ]
        rollup, inflight = run([batch], ["2026-07"])

        assert inflight == {1}
        assert rollup[("2026-07", 1)]["orders"] == 1

    def test_inflight_orders_are_not_counted(self):
        batch = [order(1, "2026-07-10T10:00:00+00:00",
                       updated_at="2026-08-04T06:00:00+00:00")]
        rollup, inflight = run([batch], ["2026-07"])

        assert rollup == {}
        assert inflight == {1}

    def test_inflight_order_is_not_reconsidered_in_a_later_month(self):
        batch = [order(1, "2026-07-10T10:00:00+00:00",
                       updated_at="2026-08-04T06:00:00+00:00")]
        _, inflight = run([batch, batch], ["2026-07", "2026-08"])

        assert inflight == {1}


class TestStillCountsWhatItShould:
    def test_returns_go_to_the_returns_bucket(self):
        batch = [order(1, "2026-06-10T10:00:00+00:00", status_id=19,
                       grand_total=250.0)]
        rollup, _ = run([batch], ["2026-06"])

        assert rollup[("2026-06", 1)]["returns_count"] == 1
        assert rollup[("2026-06", 1)]["returns_revenue"] == 250.0
        assert rollup[("2026-06", 1)]["orders"] == 0

    def test_inactive_sources_are_skipped(self):
        batch = [order(1, "2026-06-10T10:00:00+00:00", source_id=99)]
        rollup, _ = run([batch], ["2026-06"])

        assert rollup == {}

    def test_quantity_and_revenue_accumulate(self):
        batch = [order(1, "2026-06-10T10:00:00+00:00", grand_total=100.0, quantity=3),
                 order(2, "2026-06-11T10:00:00+00:00", grand_total=50.0, quantity=2)]
        rollup, _ = run([batch], ["2026-06"])

        assert rollup[("2026-06", 1)] == {
            "orders": 2, "qty": 5, "revenue": 150.0,
            "returns_count": 0, "returns_revenue": 0.0,
        }


class TestDuckDbExclusion:
    """The DuckDB rollup must honour the ids KeyCRM held back."""

    @pytest.mark.asyncio
    async def test_excluded_ids_are_dropped(self, tmp_path):
        import duckdb
        from core.reconciliation_io import duckdb_monthly_source_rollup

        conn = duckdb.connect(str(tmp_path / "t.duckdb"))
        conn.execute("""
            CREATE TABLE orders (id BIGINT, source_id INTEGER, status_id INTEGER,
                                 grand_total DOUBLE, ordered_at TIMESTAMPTZ,
                                 updated_at TIMESTAMPTZ)
        """)
        conn.execute("CREATE TABLE order_products (order_id BIGINT, quantity INTEGER)")
        conn.execute("""
            INSERT INTO orders VALUES
              (1, 1, 12, 100.0, '2026-06-10 10:00:00+00', '2026-06-10 10:00:00+00'),
              (2, 1, 12, 250.0, '2026-06-11 10:00:00+00', '2026-06-11 10:00:00+00')
        """)

        both = duckdb_monthly_source_rollup(
            conn, date(2026, 6, 1), date(2026, 6, 30), watermark=WATERMARK,
        )
        assert both[("2026-06", 1)]["orders"] == 2
        assert both[("2026-06", 1)]["revenue"] == 350.0

        one = duckdb_monthly_source_rollup(
            conn, date(2026, 6, 1), date(2026, 6, 30), watermark=WATERMARK,
            exclude_ids={2},
        )
        assert one[("2026-06", 1)]["orders"] == 1
        assert one[("2026-06", 1)]["revenue"] == 100.0
        conn.close()
