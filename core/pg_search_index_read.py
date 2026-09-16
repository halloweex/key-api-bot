"""The Meilisearch index, built from Postgres.

`SyncService.sync_to_meilisearch` reads DuckDB `buyers`, `silver_orders` and
`products` and writes nothing back to DuckDB — its only output is the external
search index. That makes it a genuine read, and the last one stage 1's "16 of
16" did not count.

THE RENDERING IS THE VALUE HERE

Everywhere else in this migration the rule is to compare values, not renderings.
This is the exception, and it is why the frames below are built so carefully:
the downstream code calls `.isoformat()` and `.to_dict('records')`, and the
resulting STRINGS are what the index stores and what a person searching sees.

Measured on the production catalogue, not assumed:

- `ordered_at` and `created_at` come out of DuckDB timezone-aware in the
  SESSION's zone, which follows the container. Production runs `TZ=Europe/Kyiv`
  and indexes `...+02:00` / `...+03:00` across DST; the gate and CI images leave
  `TZ` unset, `/etc/localtime` points at `Etc/UTC`, and DuckDB renders `+00:00`
  there. asyncpg returns UTC regardless, so an unconverted port would show every
  search result in UTC in production. Hardcoding Kyiv would be no better: it
  matches production by coincidence of deployment and diverges from DuckDB
  everywhere else. So `local_zone()` resolves the zone DuckDB uses — `TZ` first,
  then `/etc/localtime` — as an IANA name, never a fixed offset, because a
  fixed offset renders a winter order with a summer offset.
- `order_date` comes out as naive `datetime64[us]` and indexes as
  `2026-09-01T00:00:00`. A Postgres `date` would index as `2026-09-01`.
- Money is `float64`. asyncpg returns `Decimal`, and
  `meilisearch_client._sanitize_for_json` passes a `Decimal` through untouched.

So each frame is rebuilt in DuckDB's dtypes, and everything after it — the
`isoformat` calls, `to_dict`, the sanitiser, the index — runs unchanged. The
verification compares the finished, sanitised documents, because that is the
contract.

ITS OWN WATERMARK, SO THE TWO CLOCKS NEVER MEET

The incremental sync is self-referential: it takes `MAX(synced_at)` from the
data, indexes rows above the stored watermark, and stores that maximum back —
deliberately not wall-clock `now()`. That scheme is only consistent inside one
clock. Postgres has no `synced_at`; its bookkeeping column is `mirrored_at`, on
a later clock, and CLAUDE.md is explicit that the two are not shared.

Reusing the `meilisearch` watermark would work going forward — a mirror lands
after the write, so `mirrored_at` is always the later of the two — but a
rollback would compare DuckDB's `synced_at` against a stored Postgres value and
silently skip every row stamped between them. So the Postgres path keeps
`WATERMARK_KEY` of its own. Its first run finds none, rebuilds the index in full,
and a rollback returns to a `meilisearch` watermark nothing has touched.
"""
from __future__ import annotations

import logging
import os
from datetime import datetime
from typing import Optional

logger = logging.getLogger(__name__)

ENV = "KS_READ_SEARCH_INDEX"
_VALID = ("duckdb", "postgres")
WATERMARK_KEY = "meilisearch_pg"


def local_zone() -> str:
    """The IANA zone DuckDB renders TIMESTAMPTZ in, resolved the way it does.

    `TZ` wins when it is set — production sets `Europe/Kyiv` while its
    `/etc/localtime` still points at `Etc/UTC`, and DuckDB renders Kyiv. When it
    is unset, `/etc/localtime` decides, which is how the gate image renders
    `Etc/UTC`. Both measured, in both images, before this was written.
    """
    tz = (os.environ.get("TZ") or "").strip().lstrip(":")
    if tz:
        return tz
    try:
        from pathlib import Path
        resolved = str(Path("/etc/localtime").resolve())
        if "zoneinfo/" in resolved:
            return resolved.split("zoneinfo/", 1)[1]
    except OSError:
        pass
    return "UTC"

# DuckDB's dtypes for each frame, measured with fetchdf() on production.
_DTYPES = {
    "buyers": {
        "id": "int32", "full_name": "str", "phone": "str", "email": "str",
        "city": "object", "note": "str", "manager_id": "Int32",
        "created_at": "local", "order_count": "int64",
    },
    "orders": {
        "id": "int32", "grand_total": "float64", "ordered_at": "local",
        "status_id": "int32", "source_name": "str", "buyer_id": "Int32",
        "order_date": "naive", "buyer_name": "str",
    },
    "products": {
        "id": "int32", "name": "str", "sku": "str", "brand": "str",
        "price": "float64", "category_id": "Int32", "category_name": "str",
    },
}


def enabled() -> bool:
    """Whether the search index should be built from Postgres.

    An unknown value raises — `KS_BOT_STORE`'s rule.
    """
    value = os.getenv(ENV, "duckdb").strip().lower() or "duckdb"
    if value not in _VALID:
        raise ValueError(
            f"{ENV}={value!r} — unknown engine. Expected one of {_VALID}; "
            f"a typo here must stop the read, not point it at the other store."
        )
    return value == "postgres"


def available() -> bool:
    return bool(os.getenv("KS_PG_DSN", "").strip())


def normalise(rows, kind: str):
    """Plain rows in, the DataFrame DuckDB's `fetchdf()` produced out."""
    import pandas as pd

    spec = _DTYPES[kind]
    df = pd.DataFrame([tuple(r) for r in rows], columns=list(spec))
    for col, dtype in spec.items():
        if dtype == "local":
            zone = local_zone()
            df[col] = (pd.to_datetime(df[col], utc=True)
                       .dt.tz_convert(zone).astype(f"datetime64[us, {zone}]"))
        elif dtype == "naive":
            df[col] = pd.to_datetime(df[col]).astype("datetime64[us]")
        elif dtype in ("float64",):
            df[col] = df[col].astype("float64")
        elif dtype == "object":
            df[col] = df[col].astype("object")
        elif dtype == "str":
            # pandas infers its own string dtype from the values; forcing it
            # would change how an all-NULL column renders.
            pass
        else:
            df[col] = df[col].astype(dtype)
    return df


async def _fetch(sql: str, params=()):
    from core.pg import get_pool, require_revision
    from core.sql_dialect import numbered

    pool = await get_pool()
    await require_revision()
    async with pool.acquire() as conn:
        return await conn.fetch(numbered(sql), *params)


async def high_watermark() -> Optional[datetime]:
    """The newest `mirrored_at` across the three tables the index is built from."""
    rows = await _fetch("""
        SELECT MAX(ts) FROM (
            SELECT MAX(mirrored_at) AS ts FROM bronze.orders
            UNION ALL SELECT MAX(mirrored_at) FROM bronze.buyers
            UNION ALL SELECT MAX(mirrored_at) FROM bronze.products
        ) t
    """)
    return rows[0][0] if rows else None


async def buyers_frame(last_sync: Optional[datetime]):
    base = """
        SELECT b.id, b.full_name, b.phone, b.email, b.city, b.note,
               b.manager_id, b.created_at, COUNT(DISTINCT o.id) AS order_count
        FROM bronze.buyers b
        {join}
        LEFT JOIN silver.orders o ON b.id = o.buyer_id AND NOT o.is_return
        GROUP BY b.id, b.full_name, b.phone, b.email, b.city, b.note,
                 b.manager_id, b.created_at
    """
    if last_sync is None:
        rows = await _fetch(base.format(join=""))
    else:
        rows = await _fetch(base.format(join="""
            JOIN (
                SELECT id FROM bronze.buyers WHERE mirrored_at > ?
                UNION
                SELECT DISTINCT buyer_id AS id FROM bronze.orders
                WHERE mirrored_at > ? AND buyer_id IS NOT NULL
            ) t ON t.id = b.id"""), [last_sync, last_sync])
    return normalise(rows, "buyers")


async def orders_frame(last_sync: Optional[datetime], limit: int, offset: int):
    # `o.id` breaks ties under LIMIT/OFFSET. DuckDB's `ORDER BY ordered_at DESC`
    # alone leaves equal timestamps unordered, so a page boundary can land a
    # row on both pages or neither — the pagination defect already found once
    # in the /traffic port. Harmless for the index (upserts, whole set covered
    # on a full run), but defined order costs nothing.
    if last_sync is None:
        rows = await _fetch("""
            SELECT o.id, o.grand_total, o.ordered_at, o.status_id, o.source_name,
                   o.buyer_id, o.order_date, b.full_name AS buyer_name
            FROM silver.orders o
            LEFT JOIN bronze.buyers b ON o.buyer_id = b.id
            ORDER BY o.ordered_at DESC, o.id DESC
            LIMIT ? OFFSET ?
        """, [limit, offset])
    else:
        rows = await _fetch("""
            SELECT o.id, o.grand_total, o.ordered_at, o.status_id, o.source_name,
                   o.buyer_id, o.order_date, b.full_name AS buyer_name
            FROM silver.orders o
            LEFT JOIN bronze.orders src ON src.id = o.id
            LEFT JOIN bronze.buyers b ON o.buyer_id = b.id
            WHERE src.mirrored_at > ? OR b.mirrored_at > ?
            ORDER BY o.ordered_at DESC, o.id DESC
            LIMIT ? OFFSET ?
        """, [last_sync, last_sync, limit, offset])
    return normalise(rows, "orders")


async def products_frame(last_sync: Optional[datetime]):
    sql = """
        SELECT p.id, p.name, p.sku, p.brand, p.price, p.category_id,
               c.name AS category_name
        FROM bronze.products p
        LEFT JOIN bronze.categories c ON p.category_id = c.id
    """
    if last_sync is None:
        rows = await _fetch(sql)
    else:
        rows = await _fetch(sql + " WHERE p.mirrored_at > ?", [last_sync])
    return normalise(rows, "products")
