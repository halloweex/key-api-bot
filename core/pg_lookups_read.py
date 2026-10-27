"""Reading the filter bar's four dropdowns from Postgres.

Root categories, a parent's children, brands and promocodes. They are not a
tab — they are the header, drawn on every page except `/traffic`, whose
filters were narrowed. That is why they share one flag: they are one surface,
they fail together, and rolling one back without the others would leave a
filter bar half on each engine with nothing on screen saying so.

NO FALLBACK SINCE STEP 14 (S14-05)

The four dropdowns read Postgres. The flag that chose the engine
(`KS_READ_LOOKUPS`) chooses nothing now: the router asks Postgres and nothing
else, and a failure — a lost DSN line included — is refused as a 503 naming
`lookups` (`read_fallback.refuse`), which is what `KS_READ_FALLBACK=off` made
of it since 2026-10-09. The paragraphs below are the history of the port.

WHY THIS IS THE FIRST THING TO MOVE, AND NOT THE LAST

Every other port took a tab off DuckDB and left these behind, so a page could
be entirely on Postgres and still reach into DuckDB the moment somebody opened
a dropdown. They are also the most frequently called reads in the system —
once per page load, on every tab — and each one takes DuckDB's **process-wide
store lock**, the same lock a warehouse rebuild holds every two minutes. There
is no cache above them: `web/routes/api/analytics.py` calls the store
directly.

WHY IT FELL BACK, UNTIL S14-05

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
from typing import Any, List, Sequence, Tuple

logger = logging.getLogger(__name__)


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
