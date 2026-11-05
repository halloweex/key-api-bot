"""Silver from DuckDB's landing by the shared projection, for the tests that
prove what the projection means.

Silver's projection (`core.warehouse_sql.silver_select_sql` and its second
pass) is one text rendered for two engines; `tests/unit/test_silver_dialects.py`
holds the two renderings to their two known differences. The tests that pin
what it *says* — who is a new customer, which status is a return, which
manager sells retail — ran it in DuckDB through `refresh_warehouse_layers`,
DuckDB's warehouse rebuild, until step 14's S14-15 deleted that rebuild.
They run the same statements here: exactly the rebuild's full branch, which
is what the admin door `rebuild-silver` ran too — DELETE, the projection over
`orders`, the second pass, in one transaction.

Gold is the Postgres projection (`core.pg_gold.select_sql`) rendered for
DuckDB over that Silver, as `tests/unit/test_pg_gold.py` runs it: DuckDB's
own Gold went with the rebuild. Gone with the store in S14-18.
"""
from __future__ import annotations

from typing import Any, Dict, Tuple


async def derive_silver(store) -> int:
    """Rebuild DuckDB's `silver_orders` whole from its `orders`; the row count."""
    from core.sql_dialect import DUCKDB
    from core.warehouse_sql import silver_pass2_sql, silver_select_sql

    async with store.connection() as conn:
        conn.execute("BEGIN TRANSACTION")
        try:
            conn.execute("DELETE FROM silver_orders")
            conn.execute(
                f"INSERT INTO silver_orders SELECT {silver_select_sql(DUCKDB)} FROM orders o")
            conn.execute(silver_pass2_sql(dialect=DUCKDB))
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            raise
        return conn.execute("SELECT COUNT(*) FROM silver_orders").fetchone()[0]


def gold_cells(conn, *, source_id: Any = None) -> Dict[Tuple[Any, str], Dict[str, Any]]:
    """`{(date, sales_type): {measure: value}}` — the Postgres Gold projection
    over DuckDB's Silver, at the roll-up grain (`source_id IS NULL`) unless a
    source is named."""
    from core.pg_gold import GOLD_MEASURE_COLUMNS, select_sql
    from core.sql_dialect import DUCKDB

    out = {}
    for row in conn.execute(select_sql(DUCKDB)).fetchall():
        if row[2] == source_id:
            out[(row[0], row[1])] = dict(zip(GOLD_MEASURE_COLUMNS, row[3:]))
    return out
