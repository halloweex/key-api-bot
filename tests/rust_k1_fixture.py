"""The data К1's harness ships: production's own Silver and an archive of
versions, adversarial wherever a copy can go wrong.

The orders are the trend pilot's (`tests/rust_pilot_fixture.build_orders`:
~41 000 from December 2023 to January 2027, every source and sales type,
orders a second either side of the Kyiv midnights that matter), with what
the trend never needed: a promocode on one order in five, drawn from strings
that need ClickHouse's escapes, and a sub-second `ordered_at` on one in
twenty, which the copy must truncate as Python does. Silver and Gold are
derived by production's `rebuild_silver` and `rebuild_gold`. The archive is
a baseline per order and a change on one in ten, NULLs where the columns
allow them, comments with tabs, newlines and three thousand characters, and
an id skipped every 997 — the gaps a rolled-back BIGSERIAL insert leaves,
which the harness later fills out of order.

Only for a throwaway database: it empties the tables it writes.
"""
from __future__ import annotations

import random
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Tuple
from unittest.mock import AsyncMock, patch

from tests.rust_pilot_fixture import COLUMNS as PILOT_COLUMNS
from tests.rust_pilot_fixture import build_orders

ORDER_COLUMNS = PILOT_COLUMNS + ("promocode",)
VERSION_COLUMNS = (
    "id", "order_id", "captured_at", "kind", "source_id", "status_id",
    "status_group_id", "grand_total", "ordered_at", "buyer_id", "manager_id",
    "manager_comment", "promocode", "updated_at",
)
TABLES = ("gold.daily_revenue", "silver.orders", "bronze.orders", "app.order_versions")

PROMOCODES = (
    "SPRING10", "", "tab\tinside", "line\nbreak", "cr\rreturn", "back\\slash",
    "\\N", "bell\x08form\x0cfeed", "кириллиця", "подарунок 🎁", "it's",
    " padded ", "ends\\", "\\t literally",
)
COMMENTS = (
    None, None, "utm_source=instagram\nutm_medium=stories",
    "UTM:\tsource=fb\tcampaign=black_friday", "fbp:fb.1.123\nfbc:fb.1.456",
    "менеджер: «уточнити адресу»\r\n", "\\N", "x" * 3000, "доставка 🚚", "",
)
GAP_EVERY = 997


def build(seed: int = 20261125) -> Tuple[List[tuple], List[tuple]]:
    rng = random.Random(seed)
    orders: List[tuple] = []
    for row in build_orders():
        at = row[5]
        if rng.random() < 0.05:
            # Within the second: never across a Kyiv midnight.
            at = at + timedelta(microseconds=rng.randrange(1, 1_000_000))
        promocode = rng.choice(PROMOCODES) if rng.random() < 0.2 else None
        orders.append(row[:5] + (at,) + row[6:] + (promocode,))

    versions: List[tuple] = []
    version_id = 0
    first_capture = datetime(2026, 8, 27, 3, 0, tzinfo=timezone.utc)
    for (order_id, source, status, group, total, at, _created, updated,
         buyer, manager, promocode) in orders:
        kinds = ("baseline", "change") if rng.random() < 0.1 else ("baseline",)
        for kind in kinds:
            version_id += 1
            if version_id % GAP_EVERY == 0:
                version_id += 1
            captured = first_capture + timedelta(
                seconds=version_id, microseconds=rng.randrange(1_000_000))
            blank = rng.random() < 0.05
            versions.append((
                version_id, order_id, captured, kind,
                None if blank else source, status, None if blank else group,
                None if blank else total, at, buyer, manager,
                rng.choice(COMMENTS), promocode, None if blank else updated,
            ))
    return orders, versions


def gaps(versions: List[tuple]) -> List[int]:
    """The ids the archive skipped, below its top: where a late commit lands."""
    held = {v[0] for v in versions}
    return [i for i in range(1, max(held)) if i not in held]


async def seed(pool) -> Dict[str, int]:
    """Empty the four tables, land orders and versions, derive Silver and Gold."""
    from core.pg_gold import rebuild_gold
    from core.pg_silver import rebuild_silver

    orders, versions = build()
    async with pool.acquire() as conn:
        for table in TABLES:
            await conn.execute(f"DELETE FROM {table}")
        await conn.copy_records_to_table(
            "orders", schema_name="bronze", columns=ORDER_COLUMNS, records=orders)
        await conn.copy_records_to_table(
            "order_versions", schema_name="app", columns=VERSION_COLUMNS, records=versions)
    with patch("core.pg.require_revision", new=AsyncMock()):
        silver = await rebuild_silver(pool=pool)
        gold = await rebuild_gold(pool=pool)
    return {"orders": len(orders), "versions": len(versions),
            "silver": silver["rows"], "gold": gold.get("rows", 0)}
