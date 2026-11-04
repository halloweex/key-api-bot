"""Reading the cohort tab from ClickHouse.

ONE ENGINE SINCE STEP 14 (S14-07)

The cohort tab reads ClickHouse and nothing else: `CustomersMixin._analytics_rows`
asks this module, and a failure — `KS_CH_URL` unset included — is refused as a
503 naming `cohorts` (`read_fallback.refuse`), which is what
`KS_READ_FALLBACK=off` made of it since 2026-10-09. `KS_READ_COHORTS` chooses
nothing now. The paragraphs below are the history of the port.

The five customer-analytics queries are the natural ClickHouse candidate and
the only one in this dashboard: they are order-level, read-only, and
`silver.orders` is already shipped there hourly by `core/ch_silver.py`. Nothing
new has to be replicated for them to run.

WHY IT FELL BACK, UNTIL S14-07

`core/pg_sms_read.py` refuses to fall back, because after that switch the two
stores diverge and answering from the stale one would look perfectly ordinary.
Nothing here diverges: ClickHouse holds a copy of Silver, Silver is derived,
and both engines are reading the same facts. So the failure mode is the one
`pg_gold_read` has — a tab that goes dark teaches everyone never to flip the
flag again — and the answer is the same: log the error and answer from DuckDB.

That difference is not a preference. It follows from ClickHouse being an
**optional** store in this architecture: nothing may depend on it being up.

WHAT THE FLAG DOES NOT COVER

Only the query. The Python above it — the retention matrix, the summary
metrics, the cohort insights — stays in its single implementation, because a
second copy of that is how the two Silver definitions diverged in a day.
"""
from __future__ import annotations

import asyncio
import logging
from typing import Any, List, Sequence, Tuple

logger = logging.getLogger(__name__)


# ClickHouse's HTTP interface answers in TabSeparated — every value arrives as
# text, including the numbers. DuckDB hands back typed values, and the callers
# unpack positionally and do arithmetic on what they get, so the types have to
# be restored or the tab would compare a string to an int and quietly show
# nothing. Spelled out per column rather than guessed from the text, because a
# guess turns an empty cohort's `0` into an int on one row and a float on the
# next.
INT = "int"
FLOAT = "float"
TEXT = "text"

RETENTION_TYPES: Tuple[str, ...] = (TEXT, INT, INT, INT, FLOAT)


def _typed(row: Sequence[str], types: Sequence[str]) -> tuple:
    """One TabSeparated row in the shape DuckDB would have returned."""
    out: List[Any] = []
    for value, kind in zip(row, types):
        if value == "\\N":            # ClickHouse's NULL in TabSeparated
            out.append(None)
        elif kind == INT:
            out.append(int(value))
        elif kind == FLOAT:
            out.append(float(value))
        else:
            out.append(value)
    return tuple(out)


# The ClickHouse server is shared with other projects on the host, and its
# total memory limit is the server's, spent by their queries as much as ours.
# When it is full the OvercommitTracker stops a query of its choosing with
# code 241 (MEMORY_LIMIT_EXCEEDED). On 2026-10-06 06:18 UTC that was a cohort
# read, 18 min after our own hourly ship had finished; it fell back to DuckDB
# and restarted the clean week `KS_READ_FALLBACK=off` waits for, and under
# `off` it would have been a 503. A full server is a moment, not a fault, so
# the read is asked again after each delay below and only the last refusal
# reaches the caller's fallback. Only 241, and only here: any other error is
# the caller's at once, and the shippers keep their own failure accounting.
MEMORY_RETRY_DELAYS_S: Tuple[float, ...] = (1.0, 3.0)


def _memory_refused(exc: BaseException) -> bool:
    """ClickHouse refused the query for want of memory (code 241)."""
    return isinstance(exc, RuntimeError) and "Code: 241." in str(exc)


async def fetch(
    sql: str, params: Sequence[Any], types: Sequence[str] = (),
) -> List[Tuple]:
    """Run one analytics query against ClickHouse and return plain tuples.

    Parameters are substituted rather than bound: ClickHouse's HTTP interface
    has no positional binding, and the callers here pass integers only — the
    horizon in months. Anything textual would have to travel a different way,
    and a test pins that these queries bind nothing else.

    Without `KS_CH_URL` there is nothing to ask, and this raises saying so,
    as `core.pg.get_pool` does without `KS_PG_DSN`: the caller refuses it
    (step 14, S14-07). It used to be `available()`, asked by the router
    before it fell back to DuckDB.
    """
    from core.ch_common import URL_ENV, configured, execute, parse_tsv

    if not configured():
        raise RuntimeError(f"{URL_ENV} is not set: there is no ClickHouse to ask")

    rendered = sql
    for value in params:
        if not isinstance(value, int) or isinstance(value, bool):
            raise TypeError(
                f"only integer parameters may be substituted here, got "
                f"{type(value).__name__} — a textual value needs binding, "
                f"which this transport does not have"
            )
        rendered = rendered.replace("?", str(int(value)), 1)
    if "?" in rendered:
        raise ValueError("more placeholders than parameters")

    # TabSeparated is what `execute` returns; the caller unpacks positionally,
    # so the row shape has to match DuckDB's `fetchall()` — see `_typed`.
    for attempt, delay in enumerate((*MEMORY_RETRY_DELAYS_S, None), start=1):
        try:
            raw = await execute(rendered + "\nFORMAT TabSeparated")
            break
        except RuntimeError as exc:
            if delay is None or not _memory_refused(exc):
                raise
            logger.warning(
                "ClickHouse refused a cohort read for want of memory "
                "(attempt %d); asking again in %.0f s", attempt, delay)
            await asyncio.sleep(delay)
    rows = [line.split("\t") for line in raw.splitlines() if line]
    if not types:
        return [tuple(r) for r in rows]
    return [_typed(r, types) for r in rows]
