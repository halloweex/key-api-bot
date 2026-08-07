"""Per-order reconciliation: what monthly totals cannot see.

A rollup nets out anything that offsets — a status wrong on one order and right
on another, a line item lost here and gained there, a manager reassigned. The
monthly comparison can be perfectly green while individual orders disagree.
"""
from datetime import date, datetime, timedelta, timezone

import duckdb
import pytest

from core.data_quality import DiscrepancyClass, Severity, classify_order_discrepancies
from core.reconciliation_io import (
    _process_batch,
    duckdb_orders_in_window,
    rollup_from_orders,
)

WATERMARK = datetime(2026, 8, 4, tzinfo=timezone.utc)
WINDOW_START = date(2026, 6, 1)
WINDOW_END = date(2026, 6, 30)


def facts(**over):
    base = {
        "status_id": 12, "source_id": 1, "manager_id": 4, "buyer_id": 900,
        "grand_total": 100.0, "order_date": date(2026, 6, 10),
        "n_lines": 1, "qty": 2, "line_amount": 100.0,
    }
    base.update(over)
    return base


class TestRollupFromOrders:
    def test_groups_by_month_and_source(self):
        orders = {
            1: facts(),
            2: facts(order_date=date(2026, 6, 20), grand_total=50.0, qty=1),
            3: facts(source_id=2, grand_total=25.0, qty=3),
        }
        rollup = rollup_from_orders(orders)

        assert rollup[("2026-06", 1)] == {
            "orders": 2, "qty": 3, "revenue": 150.0,
            "returns_count": 0, "returns_revenue": 0.0,
        }
        assert rollup[("2026-06", 2)]["revenue"] == 25.0

    def test_returns_go_to_their_own_buckets(self):
        rollup = rollup_from_orders({1: facts(status_id=19, grand_total=80.0)})

        cell = rollup[("2026-06", 1)]
        assert cell["returns_count"] == 1
        assert cell["returns_revenue"] == 80.0
        assert cell["orders"] == 0
        assert cell["qty"] == 0

    def test_both_sides_of_identical_facts_produce_identical_rollups(self):
        """The point of a single rollup function."""
        orders = {1: facts(), 2: facts(status_id=19), 3: facts(source_id=4)}

        assert rollup_from_orders(dict(orders)) == rollup_from_orders(dict(orders))


class TestClassifyOrderDiscrepancies:
    def test_identical_sides_produce_nothing(self):
        orders = {1: facts(), 2: facts(source_id=2)}

        assert classify_order_discrepancies(dict(orders), dict(orders)) == []

    def test_order_missing_from_the_warehouse(self):
        out = classify_order_discrepancies({}, {77: facts()})

        assert len(out) == 1
        assert out[0].diff_class == DiscrepancyClass.MISSING_IN_DK
        assert out[0].severity == Severity.CRITICAL
        assert out[0].order_ids == (77,)
        assert out[0].month == "2026-06"

    def test_order_the_source_does_not_have(self):
        out = classify_order_discrepancies({88: facts()}, {})

        assert out[0].diff_class == DiscrepancyClass.MISSING_IN_KC
        assert out[0].severity == Severity.CRITICAL
        assert out[0].order_ids == (88,)

    def test_status_drift_is_its_own_class_and_critical(self):
        out = classify_order_discrepancies({1: facts(status_id=12)},
                                           {1: facts(status_id=19)})

        assert len(out) == 1
        assert out[0].field == "status_id"
        assert out[0].diff_class == DiscrepancyClass.STATUS_DRIFT
        assert out[0].severity == Severity.CRITICAL

    @pytest.mark.parametrize("field,dk_val,kc_val", [
        ("manager_id", 4, 15),
        ("buyer_id", 900, 901),
        ("source_id", 1, 2),
        ("n_lines", 1, 3),
        ("qty", 2, 5),
        ("order_date", date(2026, 6, 10), date(2026, 6, 11)),
    ])
    def test_every_compared_field_is_reported(self, field, dk_val, kc_val):
        out = classify_order_discrepancies({1: facts(**{field: dk_val})},
                                           {1: facts(**{field: kc_val})})

        assert [d.field for d in out] == [field]
        assert out[0].severity == Severity.WARN

    def test_money_differences_below_a_cent_are_not_reported(self):
        out = classify_order_discrepancies({1: facts(grand_total=100.004)},
                                           {1: facts(grand_total=100.0)})

        assert out == []

    def test_money_differences_above_a_cent_are_reported(self):
        out = classify_order_discrepancies({1: facts(grand_total=100.5)},
                                           {1: facts(grand_total=100.0)})

        assert [d.field for d in out] == ["grand_total"]

    def test_offsetting_errors_a_rollup_would_hide(self):
        """Two orders swap their totals: monthly revenue is unchanged."""
        dk = {1: facts(grand_total=100.0), 2: facts(grand_total=200.0)}
        kc = {1: facts(grand_total=200.0), 2: facts(grand_total=100.0)}

        assert rollup_from_orders(dk) == rollup_from_orders(kc), "totals agree"
        out = classify_order_discrepancies(dk, kc)
        assert len(out) == 1
        assert out[0].dk_value == 2.0
        assert set(out[0].order_ids) == {1, 2}

    def test_findings_are_grouped_by_month_source_and_field(self):
        dk = {1: facts(status_id=12), 2: facts(status_id=12, source_id=2)}
        kc = {1: facts(status_id=19), 2: facts(status_id=19, source_id=2)}
        out = classify_order_discrepancies(dk, kc)

        assert len(out) == 2
        assert {d.source_id for d in out} == {1, 2}
        assert all(d.dk_value == 1.0 for d in out)

    def test_id_list_is_capped(self):
        dk = {i: facts(status_id=12) for i in range(100)}
        kc = {i: facts(status_id=19) for i in range(100)}
        out = classify_order_discrepancies(dk, kc, max_ids=10)

        assert out[0].dk_value == 100.0
        assert len(out[0].order_ids) == 10


class TestDuckDbExtractor:
    @pytest.fixture
    def conn(self):
        c = duckdb.connect(":memory:")
        c.execute("""
            CREATE TABLE orders (id BIGINT, source_id INTEGER, status_id INTEGER,
                                 manager_id INTEGER, buyer_id INTEGER,
                                 grand_total DECIMAL(12,2), ordered_at TIMESTAMPTZ,
                                 updated_at TIMESTAMPTZ);
            CREATE TABLE order_products (order_id BIGINT, price_sold DECIMAL(12,2),
                                         quantity INTEGER);
            INSERT INTO orders VALUES
              (1, 1, 12, 4, 900, 100.00, '2026-06-10 09:00:00+00', '2026-06-10 09:00:00+00'),
              (2, 2, 19, 15, 901, 50.00, '2026-06-11 09:00:00+00', '2026-06-11 09:00:00+00');
            INSERT INTO order_products VALUES (1, 50.00, 2), (2, 25.00, 2);
        """)
        yield c
        c.close()

    def test_extracts_the_compared_fields(self, conn):
        out = duckdb_orders_in_window(conn, WINDOW_START, WINDOW_END, watermark=WATERMARK)

        assert out[1] == {
            "status_id": 12, "source_id": 1, "manager_id": 4, "buyer_id": 900,
            "grand_total": 100.0, "order_date": date(2026, 6, 10),
            "n_lines": 1, "qty": 2, "line_amount": 100.0,
        }
        assert out[2]["status_id"] == 19

    def test_an_order_with_no_line_items_still_appears(self, conn):
        conn.execute("""INSERT INTO orders VALUES
            (3, 1, 12, 4, 902, 30.00, '2026-06-12 09:00:00+00', '2026-06-12 09:00:00+00')""")
        out = duckdb_orders_in_window(conn, WINDOW_START, WINDOW_END, watermark=WATERMARK)

        assert out[3]["n_lines"] == 0
        assert out[3]["qty"] == 0
        assert out[3]["line_amount"] == 0.0

    def test_excluded_ids_are_dropped(self, conn):
        out = duckdb_orders_in_window(conn, WINDOW_START, WINDOW_END,
                                      watermark=WATERMARK, exclude_ids={2})

        assert set(out) == {1}

    def test_watermark_holds_back_recent_updates(self, conn):
        out = duckdb_orders_in_window(conn, WINDOW_START, WINDOW_END,
                                      watermark=datetime(2026, 6, 10, 12,
                                                         tzinfo=timezone.utc))
        assert set(out) == {1}


class TestKeyCrmExtractor:
    def _run(self, batch):
        orders, inflight = {}, set()
        _process_batch(batch, orders, WATERMARK, WINDOW_START, WINDOW_END, inflight)
        return orders, inflight

    def test_maps_a_keycrm_payload(self):
        orders, _ = self._run([{
            "id": 5, "source_id": 1, "status_id": 12,
            "ordered_at": "2026-06-10T09:00:00+00:00",
            "updated_at": "2026-06-10T09:00:00+00:00",
            "grand_total": "100.00",
            "manager": {"id": 4}, "buyer": {"id": 900},
            "products": [{"price_sold": "50.00", "quantity": 2}],
        }])

        assert orders[5] == {
            "status_id": 12, "source_id": 1, "manager_id": 4, "buyer_id": 900,
            "grand_total": 100.0, "order_date": date(2026, 6, 10),
            "n_lines": 1, "qty": 2, "line_amount": 100.0,
        }

    def test_status_from_the_nested_object(self):
        orders, _ = self._run([{
            "id": 6, "source_id": 1, "status": {"id": 19},
            "ordered_at": "2026-06-10T09:00:00+00:00",
            "updated_at": "2026-06-10T09:00:00+00:00",
            "grand_total": 0, "products": [],
        }])

        assert orders[6]["status_id"] == 19

    def test_missing_manager_and_buyer_are_none_not_a_crash(self):
        orders, _ = self._run([{
            "id": 7, "source_id": 1, "status_id": 12,
            "ordered_at": "2026-06-10T09:00:00+00:00",
            "updated_at": "2026-06-10T09:00:00+00:00",
            "grand_total": 10, "products": [],
        }])

        assert orders[7]["manager_id"] is None
        assert orders[7]["buyer_id"] is None

    def test_in_flight_orders_are_held_back_by_id(self):
        orders, inflight = self._run([{
            "id": 8, "source_id": 1, "status_id": 12,
            "ordered_at": "2026-06-10T09:00:00+00:00",
            "updated_at": "2026-08-05T09:00:00+00:00",
            "grand_total": 10, "products": [],
        }])

        assert orders == {}
        assert inflight == {8}

    def test_the_same_order_twice_is_recorded_once(self):
        payload = {
            "id": 9, "source_id": 1, "status_id": 12,
            "ordered_at": "2026-06-10T09:00:00+00:00",
            "updated_at": "2026-06-10T09:00:00+00:00",
            "grand_total": 10, "products": [{"price_sold": "5.00", "quantity": 2}],
        }
        orders, _ = self._run([payload, dict(payload)])

        assert list(orders) == [9]
        assert orders[9]["qty"] == 2
