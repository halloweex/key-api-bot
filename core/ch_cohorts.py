"""Reading the cohort tab from ClickHouse.

The five customer-analytics queries are the natural ClickHouse candidate and
the only one in this dashboard: they are order-level, read-only, and
`silver.orders` is already shipped there hourly by `core/ch_silver.py`. Nothing
new has to be replicated for them to run.

WHY THIS FALLS BACK AND `/sms` DOES NOT

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

import logging
import os
from typing import Any, List, Sequence, Tuple

logger = logging.getLogger(__name__)

ENV = "KS_READ_COHORTS"
_VALID = ("duckdb", "clickhouse")


def enabled() -> bool:
    """Whether the cohort queries should ask ClickHouse.

    An unknown value raises — `KS_BOT_STORE`'s rule. A typo in the variable
    that decides which engine answers should stop the request loudly rather
    than quietly serve the other one.
    """
    value = os.getenv(ENV, "duckdb").strip().lower() or "duckdb"
    if value not in _VALID:
        raise ValueError(
            f"{ENV}={value!r} — unknown engine. Expected one of {_VALID}; "
            f"a typo here must stop the read, not point it at the other store."
        )
    return value == "clickhouse"


def available() -> bool:
    """ClickHouse is configured at all. Without `KS_CH_URL` there is nothing
    to ask, and the flag alone must not turn a working tab into an error."""
    from core.ch_common import configured

    return configured()


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


async def fetch(
    sql: str, params: Sequence[Any], types: Sequence[str] = (),
) -> List[Tuple]:
    """Run one analytics query against ClickHouse and return plain tuples.

    Parameters are substituted rather than bound: ClickHouse's HTTP interface
    has no positional binding, and the callers here pass integers only — the
    horizon in months. Anything textual would have to travel a different way,
    and a test pins that these queries bind nothing else.
    """
    from core.ch_common import execute, parse_tsv

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
    raw = await execute(rendered + "\nFORMAT TabSeparated")
    rows = [line.split("\t") for line in raw.splitlines() if line]
    if not types:
        return [tuple(r) for r in rows]
    return [_typed(r, types) for r in rows]
