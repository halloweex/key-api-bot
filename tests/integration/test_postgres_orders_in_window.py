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


async def test_an_order_never_updated_passes_the_watermark(monkeypatch):
    """The watermark holds back orders touched after it; one whose
    `updated_at` is NULL was never touched since it landed and is compared.
    Mutation: compare `updated_at < watermark` alone and order 4 vanishes."""
    async def populate(conn, pool):
        await _populate(conn, pool)
        await seed(conn, orders=[(4, 1, 12, 20.00,
                                  datetime(2026, 6, 13, 9, tzinfo=timezone.utc),
                                  903, 4, None)])
        await conn.execute("UPDATE bronze.orders SET updated_at = NULL WHERE id = 4")

    async with tab_in_postgres(DSN, monkeypatch, populate) as pool:
        out = await _extract(pool, watermark=datetime(2026, 6, 10, 12, tzinfo=timezone.utc))

    assert set(out) == {1, 4}


# ─── What KeyCRM's side asks, asked of the store's ──────────────────────────
#
# KeyCRM's side files an order under its Kyiv date and keeps the sources this
# system syncs. A day's difference moves an order between months and invents
# a whole rollup cell; a source compared on one side only is a discrepancy
# every morning.

async def _populate_edges(conn, pool):
    await seed(conn, orders=[
        # 1 July in Kyiv, 30 June in UTC — and the other way round.
        (21, 1, 12, 30.00, datetime(2026, 6, 30, 22, 30, tzinfo=timezone.utc), 910, 4, None),
        (22, 1, 12, 60.00, datetime(2026, 5, 31, 21, 30, tzinfo=timezone.utc), 911, 4, None),
        # Opencart: synced and reconciled, though never revenue.
        (23, 3, 12, 40.00, datetime(2026, 6, 12, 9, tzinfo=timezone.utc), 912, None, None),
        # A source this system does not sync.
        (24, 6, 12, 70.00, datetime(2026, 6, 12, 9, tzinfo=timezone.utc), 913, None, None),
    ])
    await conn.execute("UPDATE bronze.orders SET updated_at = ordered_at")


async def test_an_order_is_in_the_month_of_its_kyiv_date(monkeypatch):
    """Mutation: take the date, or bound the window, in UTC."""
    from core.reconciliation_io import postgres_orders_in_window

    async with tab_in_postgres(DSN, monkeypatch, _populate_edges) as pool:
        june = await _extract(pool)
        july = await postgres_orders_in_window(
            pool, date(2026, 7, 1), date(2026, 7, 31), watermark=WATERMARK)

    assert june[22]["order_date"] == date(2026, 6, 1)
    assert 21 not in june
    assert {oid: f["order_date"] for oid, f in july.items()} == {21: date(2026, 7, 1)}


async def test_a_source_it_does_not_sync_is_not_compared(monkeypatch):
    """Mutation: drop the source filter — order 24 is compared."""
    async with tab_in_postgres(DSN, monkeypatch, _populate_edges) as pool:
        out = await _extract(pool)

    assert set(out) == {22, 23}
    assert out[23]["source_id"] == 3
