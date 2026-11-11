"""The store's side of `dq_reconciliation`: `postgres_orders_in_window`, run.

Until step 14's S14-12 `tests/unit/test_per_order_reconciliation.py` ran these
four cases against DuckDB's extractor (`duckdb_orders_in_window`), and the
Postgres one was held to it only by its SQL text
(`test_reconciliation_postgres.py`). DuckDB's went with the job's DuckDB arm;
the same fixture and the same expected facts — DuckDB's answers, literally —
are asked of the extractor that stays, against a real server.
"""
from __future__ import annotations

from datetime import date, datetime, timezone

import pytest

from tests.integration.pg_tab_seed import DSN, needs_postgres, seed, tab_in_postgres

pytestmark = [needs_postgres, pytest.mark.asyncio]

WATERMARK = datetime(2026, 8, 4, tzinfo=timezone.utc)
WINDOW_START = date(2026, 6, 1)
WINDOW_END = date(2026, 6, 30)


async def _populate(conn, pool):
    await seed(
        conn,
        orders=[(1, 1, 12, 100.00, datetime(2026, 6, 10, 9, tzinfo=timezone.utc), 900, 4, None),
                (2, 2, 19, 50.00, datetime(2026, 6, 11, 9, tzinfo=timezone.utc), 901, 15, None),
                (3, 1, 12, 30.00, datetime(2026, 6, 12, 9, tzinfo=timezone.utc), 902, 4, None)],
        lines=[(1001, 1, None, "a", 2, 50.00), (2001, 2, None, "b", 2, 25.00)],
    )
    # The extractor's watermark reads `updated_at`; DuckDB's fixture set it to
    # the order's own instant.
    await conn.execute("UPDATE bronze.orders SET updated_at = ordered_at")


async def _extract(pool, **kw):
    from core.reconciliation_io import postgres_orders_in_window

    return await postgres_orders_in_window(
        pool, WINDOW_START, WINDOW_END, watermark=kw.pop("watermark", WATERMARK), **kw)


async def test_extracts_the_compared_fields(monkeypatch):
    async with tab_in_postgres(DSN, monkeypatch, _populate) as pool:
        out = await _extract(pool)

    assert out[1] == {
        "status_id": 12, "source_id": 1, "manager_id": 4, "buyer_id": 900,
        "grand_total": 100.0, "order_date": date(2026, 6, 10),
        "n_lines": 1, "qty": 2, "line_amount": 100.0,
    }
    assert out[2]["status_id"] == 19


async def test_an_order_with_no_line_items_still_appears(monkeypatch):
    async with tab_in_postgres(DSN, monkeypatch, _populate) as pool:
        out = await _extract(pool)

    assert out[3]["n_lines"] == 0
    assert out[3]["qty"] == 0
    assert out[3]["line_amount"] == 0.0


async def test_excluded_ids_are_dropped(monkeypatch):
    async with tab_in_postgres(DSN, monkeypatch, _populate) as pool:
        out = await _extract(pool, exclude_ids={2, 3})

    assert set(out) == {1}


async def test_watermark_holds_back_recent_updates(monkeypatch):
    async with tab_in_postgres(DSN, monkeypatch, _populate) as pool:
        out = await _extract(pool, watermark=datetime(2026, 6, 10, 12, tzinfo=timezone.utc))

    assert set(out) == {1}
