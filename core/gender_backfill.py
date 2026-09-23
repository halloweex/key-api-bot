"""Derive a gender for every buyer that needs one, and write it to DuckDB.

One home for the rule, two callers: the hourly scheduler tick and
`scripts/backfill_gender.py`. A second copy of "which buyers need a verdict"
would differ the first time either learned something the other did not — this
repository's charter rule 1, and the shortest recorded time it took to happen
here is under a day.

WHY IT RIDES THE REPLICATION TICK, AND WHY IT RUNS FIRST

`replicate_operational` ships `buyer_gender` to Postgres. Deriving in the same
tick and immediately before it is the arrangement `pg_gold` has with
`pg_silver`: one floor, one tick, so the copy can never ship verdicts that the
next statement is about to replace. A separate job would give the two their own
cadences and a window where Postgres holds yesterday's answer for a buyer DuckDB
decided an hour ago.

Cheap when nothing moved, which is what makes it affordable hourly: the pending
query is one LEFT JOIN over 20 145 rows, and on an ordinary tick it returns the
~19 buyers a day KeyCRM adds. The classifier itself does 20 145 names in 0.22 s
with no I/O at all.

NEVER RAISES

It is called from the same runner as four other riders, and a fault here must
not take the operational replication — the only copy of tables nothing can
rebuild — down with it. Errors come back in the result dict, the way
`replicate_operational` returns its own.
"""
from __future__ import annotations

import asyncio
import logging
import time
from collections import Counter
from typing import Any, Dict, Iterable, List, Sequence, Tuple

from core.gender import RULES_VERSION, classify
from core.sql_dialect import DUCKDB, POSTGRES

logger = logging.getLogger(__name__)

# How many rows one transaction carries. DuckDB holds a process-wide store lock
# and the warehouse rebuild takes it every two minutes; one 20 145-row
# transaction is how the SMS tab went from 124 to 38 req/s once already.
CHUNK = 2000

_PENDING_NEW = """
    SELECT b.id, b.full_name
    FROM buyers b
    LEFT JOIN buyer_gender g ON g.buyer_id = b.id
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
    FROM buyers b
    LEFT JOIN buyer_gender g ON g.buyer_id = b.id
    WHERE g.buyer_id IS NULL OR g.override_by_human = FALSE
    ORDER BY b.id
"""

_INSERT = """
    INSERT INTO buyer_gender
        (buyer_id, gender, method, confidence, decided_from, rules_version)
    VALUES (?, ?, ?, ?, ?, ?)
"""


async def pending(store, *, rebuild_all: bool = False) -> List[Tuple[int, str]]:
    """Buyers with no verdict, or one derived by an older RULES_VERSION."""
    sql = _PENDING_ALL if rebuild_all else _PENDING_NEW
    params = [] if rebuild_all else [RULES_VERSION]
    async with store.connection() as conn:
        return [tuple(r) for r in conn.execute(sql, params).fetchall()]


async def write(store, verdicts: Sequence[tuple]) -> int:
    """Replace the verdicts for these buyers. DELETE then INSERT, never upsert.

    The DuckDB table carries no primary key — an ART index there disables vacuum
    for the whole table, and this one is rewritten wholesale whenever
    RULES_VERSION moves — so uniqueness is the writer's job, and deleting the
    ids about to be written is how it is kept.
    """
    if not verdicts:
        return 0
    ids = [v[0] for v in verdicts]
    rows = [
        (bid, v.gender, v.method, v.confidence, v.decided_from, RULES_VERSION)
        for bid, v in verdicts
    ]
    async with store.connection() as conn:
        conn.execute("BEGIN TRANSACTION")
        try:
            placeholders = ",".join("?" * len(ids))
            conn.execute(
                f"DELETE FROM buyer_gender WHERE buyer_id IN ({placeholders})", ids
            )
            conn.executemany(_INSERT, rows)
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            raise
    return len(rows)


async def derive_gender(store, *, rebuild_all: bool = False) -> Dict[str, Any]:
    """Classify every buyer that needs it and store the verdicts. Never raises."""
    out: Dict[str, Any] = {
        "pending": 0, "written": 0, "female": 0, "male": 0, "null": 0,
        "rules_version": RULES_VERSION, "error": None, "ms": 0,
    }
    started = time.monotonic()
    try:
        rows = await pending(store, rebuild_all=rebuild_all)
        out["pending"] = len(rows)
        if not rows:
            return out

        # Pure CPU, no lock held: the classifier touches nothing but strings.
        verdicts = [(bid, classify(name)) for bid, name in rows]
        out["female"] = sum(1 for _, v in verdicts if v.gender == "f")
        out["male"] = sum(1 for _, v in verdicts if v.gender == "m")
        out["null"] = sum(1 for _, v in verdicts if v.gender is None)

        for i in range(0, len(verdicts), CHUNK):
            out["written"] += await write(store, verdicts[i:i + CHUNK])
            # Yield between chunks so the warehouse tick can take the lock.
            await asyncio.sleep(0)

        logger.info(
            "gender: %d derived (f=%d m=%d null=%d, rules v%d)",
            out["written"], out["female"], out["male"], out["null"], RULES_VERSION,
        )
    except Exception as exc:
        out["error"] = f"{type(exc).__name__}: {exc}"
        logger.error("gender derivation failed: %s", out["error"], exc_info=True)
    finally:
        out["ms"] = int((time.monotonic() - started) * 1000)
    return out


# ── previewing what the current rules would change ───────────────────────────
#
# The write path above needs DuckDB, and in production the web container holds
# that file exclusively: a second process cannot open it even read-only, which
# is why `scripts/backfill_gender.py` died there with an IOException for its
# first eleven days. The preview needs nothing but the names and the stored
# verdicts, and Postgres holds an exact copy of both — measured 2026-09-23,
# 20 533 buyers equal in every column, `decided_at` included — so it reads
# there, inside a read-only transaction, and runs beside a live web.
#
# One body, two engines (charter rule 1): the table names are the only thing
# the two renderings differ in, and they come from the same `Dialect` fields the
# SMS audience uses.

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


async def read_stored_duckdb(store) -> List[tuple]:
    """The preview's rows from DuckDB — a laptop copy, or a stopped web."""
    async with store.connection() as conn:
        sql = stored_sql(DUCKDB.buyers, DUCKDB.buyer_gender)
        return [tuple(r) for r in conn.execute(sql).fetchall()]


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
