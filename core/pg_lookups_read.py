"""Reading the filter bar's four dropdowns from Postgres.

Root categories, a parent's children, brands and promocodes. They are not a
tab — they are the header, drawn on every page except `/traffic`, whose
filters were narrowed. That is why they share one flag: they are one surface,
they fail together, and rolling one back without the others would leave a
filter bar half on each engine with nothing on screen saying so.

WHY THIS IS THE FIRST THING TO MOVE, AND NOT THE LAST

Every other port took a tab off DuckDB and left these behind, so a page could
be entirely on Postgres and still reach into DuckDB the moment somebody opened
a dropdown. They are also the most frequently called reads in the system —
once per page load, on every tab — and each one takes DuckDB's **process-wide
store lock**, the same lock a warehouse rebuild holds every two minutes. There
is no cache above them: `web/routes/api/analytics.py` calls the store
directly.

WHY THIS FALLS BACK

Nothing diverges. Both engines read the same catalogue — `bronze.categories`
and `bronze.products` are the same parsed tuple `core/landing_rows.py` handed
to both stores — and the tab writes nothing. So a fault costs the engine
rather than the page, which is `core/pg_gold_read.py`'s answer and the
opposite of `core/pg_sms_read.py`, where the two stores genuinely differ after
the switch.

The case for falling back is stronger here than anywhere else it has been
made: a dark dropdown is not one tab going down, it is the filter bar on all
nine.

THE COLLATION, MEASURED RATHER THAN ASSUMED

Three of the four end in `ORDER BY` on a text column, and the order reaches
the user as the order of a dropdown. Two engines can sort text differently:
PostgreSQL sorts by the database collation, DuckDB by its own default. Checked
on production 2026-09-12 rather than reasoned about — this database is
`datcollate = C`, DuckDB 1.5.5 defaults to binary, and both return byte order.
Verified on the case that separates byte order from locale order (mixed case,
an apostrophe, a diaeresis and Cyrillic in one list): identical, element for
element.

Worth re-checking if the database is ever restored under a different locale.
`BrandFilter` re-sorts by label on the client, so brands would survive it;
categories and promocodes would not.

WHAT THE FLAG DOES NOT COVER

Only these four. The category *tree* walk (`_CATEGORY_TREE_SQL`) is a filter
predicate rather than a dropdown, and rides whichever router its caller uses.

THE NINTH COPY OF `fetch`

`core/pg_reports_read.py` filed this duplication as worth extracting when it
was the fourth. It is the ninth now, and the argument it made still holds —
twelve lines of pool handling with no rule in them, so charter rule 1 does not
bite — but so does the reason it was filed rather than done: extracting it
means touching eight live tabs. Adding the ninth copy in the middle of a
migration step is the smaller blast radius; the extraction is its own change.
"""
from __future__ import annotations

import logging
import os
from typing import Any, List, Sequence, Tuple

logger = logging.getLogger(__name__)

ENV = "KS_READ_LOOKUPS"
_VALID = ("duckdb", "postgres")


def enabled() -> bool:
    """Whether the filter lookups should ask Postgres.

    An unknown value raises — `KS_BOT_STORE`'s rule. A typo in the variable
    deciding which engine answers should stop the read loudly rather than
    quietly serve the other store.
    """
    value = os.getenv(ENV, "duckdb").strip().lower() or "duckdb"
    if value not in _VALID:
        raise ValueError(
            f"{ENV}={value!r} — unknown engine. Expected one of {_VALID}; "
            f"a typo here must stop the read, not point it at the other store."
        )
    return value == "postgres"


def available() -> bool:
    """Postgres is configured at all. Without a DSN there is nothing to ask,
    and the flag alone must not turn a working filter bar into an error."""
    return bool(os.getenv("KS_PG_DSN", "").strip())


async def fetch(sql: str, params: Sequence[Any] = ()) -> List[Tuple]:
    """Run one lookup query against Postgres and return plain tuples.

    `sql` arrives with `?` markers, the shared bodies' convention, and is
    renumbered for asyncpg here rather than by every caller.
    """
    from core.pg import get_pool, require_revision
    from core.sql_dialect import numbered

    pool = await get_pool()
    await require_revision()
    async with pool.acquire() as conn:
        rows = await conn.fetch(numbered(sql), *params)
    return [tuple(r) for r in rows]
