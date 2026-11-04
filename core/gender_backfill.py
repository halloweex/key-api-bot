"""Which buyers need a gender verdict, and what the current rules would change.

One home for the question, two callers: chain 4's derivation
(`core.pg_buyers_write.derive_gender_pg`, the hourly scheduler tick and
`scripts/backfill_gender.py`) asks `pending_sql` of `bronze.buyers`, and the
script's preview reads `stored_sql` and `summarise`. A second copy of "which
buyers need a verdict" would differ the first time either learned something
the other did not — this repository's charter rule 1, and the shortest
recorded time it took to happen here is under a day.

Until step 14's S14-11 this module also wrote DuckDB (`pending`, `write`,
`derive_gender`, all through `store.connection()`), and the preview could
read a DuckDB file (`read_stored_duckdb`). Chain 4 writes the verdicts in
Postgres, a constant since S14-00, and S14-11 deleted the DuckDB half with
the flag.
"""
from __future__ import annotations

import logging
from collections import Counter
from typing import Any, Dict, Iterable, List, Tuple

from core.gender import RULES_VERSION, classify
from core.sql_dialect import POSTGRES

logger = logging.getLogger(__name__)

# One body per question, two engines (charter rule 1): the table names are
# the only thing the renderings differ in, and they come from the same
# `Dialect` fields the SMS audience uses. Postgres runs them through
# `sql_dialect.numbered`, which turns the one `?` into `$1` — chain 4's writer
# (`core/pg_buyers_write.py`) asks the same question of `bronze.buyers`.
_PENDING_NEW = """
    SELECT b.id, b.full_name
    FROM {buyers} b
    LEFT JOIN {buyer_gender} g ON g.buyer_id = b.id
    WHERE g.buyer_id IS NULL
       OR (g.override_by_human = FALSE AND g.rules_version < ?)
    ORDER BY b.id
"""

# `--all`: everything except what a human decided. That exclusion is the
# `managers.is_retail` lesson — the column was recomputed on every sync until a
# person's classification became impossible to keep, and eight managers sold
# ₴3.1M into the wrong bucket before anyone noticed.
_PENDING_ALL = """
    SELECT b.id, b.full_name
    FROM {buyers} b
    LEFT JOIN {buyer_gender} g ON g.buyer_id = b.id
    WHERE g.buyer_id IS NULL OR g.override_by_human = FALSE
    ORDER BY b.id
"""


def pending_sql(buyers: str, buyer_gender: str, *,
                rebuild_all: bool = False) -> Tuple[str, List[int]]:
    """`(sql, params)` selecting the buyers that need a verdict, for one engine.

    The params are `[RULES_VERSION]` for the ordinary question and nothing for
    `--all`, in `?` form; Postgres numbers them."""
    if rebuild_all:
        return _PENDING_ALL.format(buyers=buyers, buyer_gender=buyer_gender), []
    return (_PENDING_NEW.format(buyers=buyers, buyer_gender=buyer_gender),
            [RULES_VERSION])


# ── previewing what the current rules would change ───────────────────────────
#
# The preview needs nothing but the names and the stored verdicts, so it reads
# Postgres, inside a read-only transaction, and runs beside a live web. Until
# step 14's S14-11 it could read DuckDB too, from one body rendered for either
# engine; the table names were the only thing the renderings differed in, and
# they still come from the `Dialect` fields the SMS audience uses.

_STORED = """
    SELECT b.id, b.full_name, g.gender, g.rules_version, g.override_by_human
    FROM {buyers} b
    LEFT JOIN {buyer_gender} g ON g.buyer_id = b.id
"""


def stored_sql(buyers: str, buyer_gender: str) -> str:
    """Every buyer beside its stored verdict, if any, for one engine."""
    return _STORED.format(buyers=buyers, buyer_gender=buyer_gender)


def _label(gender) -> str:
    return "NULL" if gender is None else gender


def summarise(rows: Iterable[tuple]) -> Dict[str, Any]:
    """What the hourly tick would write, and what the current rules would change.

    `rows` are `(buyer_id, full_name, gender, rules_version, override_by_human)`
    with the verdict columns NULL where no verdict is stored. Pure, and counts
    only: nothing it returns names a customer, so the result can be printed.

    * `tick_would_write` — rows with no verdict or one from an older
      RULES_VERSION: exactly what `pending` selects, so what the next hourly
      tick will rewrite without anyone asking.
    * `rules_would_change` — stored verdicts the CURRENT code decides
      differently, as `from→to` transitions. This is the number to read before
      bumping RULES_VERSION: it is what the bump will rewrite.
    * Human overrides are never counted as changes — nothing rewrites them —
      but `overrides_disagree` says how many the rules would have decided
      otherwise, because that is the classifier's measured error on the only
      rows a person has checked.
    * `after_rederive` is the gender split a full re-derivation would leave.
    """
    rows = list(rows)
    missing = stale = overrides = overrides_disagree = 0
    changes: Counter = Counter()
    after: Counter = Counter()
    for _buyer_id, name, gender, version, override in rows:
        verdict = classify(name).gender
        if version is None:
            missing += 1
            after[_label(verdict)] += 1
            continue
        if override:
            overrides += 1
            after[_label(gender)] += 1
            if verdict != gender:
                overrides_disagree += 1
            continue
        if version < RULES_VERSION:
            stale += 1
        after[_label(verdict)] += 1
        if verdict != gender:
            changes[f"{_label(gender)}→{_label(verdict)}"] += 1
    return {
        "rules_version": RULES_VERSION,
        "buyers": len(rows),
        "stored": len(rows) - missing,
        "overrides": overrides,
        "missing": missing,
        "stale": stale,
        "tick_would_write": missing + stale,
        "rules_would_change": sum(changes.values()),
        "changes": dict(sorted(changes.items())),
        "overrides_disagree": overrides_disagree,
        "after_rederive": {k: after.get(k, 0) for k in ("f", "m", "NULL")},
    }


async def read_stored_postgres(
    conn, *, buyers: str = POSTGRES.buyers,
    buyer_gender: str = POSTGRES.buyer_gender,
) -> List[tuple]:
    """The preview's rows from Postgres, in one read-only snapshot.

    `readonly=True` is the guarantee, not a courtesy: in production the only
    DSN at hand is the web container's, which is a writer, and a preview that
    could write would inherit that — the lesson of the benchmark that once ran
    a TRUNCATE there against the live roster. The snapshot keeps buyers and
    verdicts consistent with each other while the hourly tick rewrites them.
    """
    async with conn.transaction(isolation="repeatable_read", readonly=True):
        rows = await conn.fetch(stored_sql(buyers, buyer_gender))
    return [tuple(r) for r in rows]
