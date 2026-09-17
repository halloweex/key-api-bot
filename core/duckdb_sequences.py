"""Move a DuckDB sequence above the ids a table already holds.

DuckDB 1.5.5 has no way to *set* a sequence. `ALTER SEQUENCE ... RESTART`
raises "Not implemented", and every sequence this store owns backs a column
DEFAULT, so `DROP SEQUENCE` and `CREATE OR REPLACE SEQUENCE` both raise
DependencyException ("Cannot drop entry ... because there are entries that
depend on it"), while `DROP ... CASCADE` would take the DEFAULT with it. The
only lever left is the one `scripts/compact_duckdb.py` already pulls: read
where the sequence stands and burn the difference with `nextval`.

Two things about that lever are not obvious, and both were measured on 1.5.5
before this module was written.

**Where a sequence stands cannot be read from `last_value`.** Inside the
process that called `nextval`, `duckdb_sequences().last_value` is the value
last handed out. After the database is reopened it is the value the *next*
`nextval` will return — loading a sequence sets both to the stored counter.
The two readings differ by one increment and nothing in the row says which
one you are looking at. A floor computed from `last_value + increment` is
therefore one too high on every boot, and the next id collides with MAX(id),
which is exactly the state this exists to repair. The `sql` column renders
the sequence with the counter as its `START`, in every state tried: fresh,
used in-process, reopened from the WAL, checkpointed, and imported.

**A burn has to be fully read to count.** `SELECT nextval(...) FROM range(n)`
left unfetched evaluates only the chunks the client pulled — it stopped at
129,024 for every n above that — and the statement that follows abandons it
without writing the sequence's new position to the WAL. The position then
reaches disk only if a later checkpoint happens to have other work to write;
otherwise a reopen hands out the burned values again. An aggregate evaluates
every row, `fetchall` completes the statement, and the position survived a
reopen with no checkpoint at all.
"""
from __future__ import annotations

import operator
import re
from typing import Optional, Tuple

# Sequence names are interpolated into `nextval('<name>')`, which cannot take
# a bind parameter, so only plain identifiers are accepted: letters, digits
# and underscores, optionally prefixed by a schema of the same shape.
_IDENTIFIER = re.compile(r"(?:([A-Za-z0-9_]+)\.)?([A-Za-z0-9_]+)")

# `CREATE SEQUENCE s INCREMENT BY 1 MINVALUE 1 MAXVALUE ... START 42 NO CYCLE;`
_START = re.compile(r"\bSTART\s+(-?\d+)\b")

# The whole burn runs inside one statement on the store's only connection, at
# boot and under the store lock. Measured at ~16 ms per million values, so this
# is a couple of seconds at worst — and a floor that needs more than that is a
# stray id in the table, which deserves a human rather than a stalled start.
DEFAULT_MAX_BURN = 100_000_000


def _split(sequence_name: str) -> Tuple[Optional[str], str]:
    if not isinstance(sequence_name, str):
        raise ValueError(f"sequence name must be a string, got {sequence_name!r}")
    match = _IDENTIFIER.fullmatch(sequence_name)
    if match is None:
        raise ValueError(f"not a plain sequence identifier: {sequence_name!r}")
    return match.group(1), match.group(2)


def _state(conn, sequence_name: str) -> Tuple[int, int]:
    """(value the next `nextval` returns, increment) — without consuming one."""
    schema, name = _split(sequence_name)
    rows = conn.execute(
        "SELECT increment_by, start_value, last_value, sql "
        "FROM duckdb_sequences() "
        "WHERE database_name = current_database() "
        "  AND lower(schema_name) = lower(COALESCE(?, current_schema())) "
        "  AND lower(sequence_name) = lower(?)",
        [schema, name],
    ).fetchall()
    if not rows:
        raise LookupError(f"no sequence named {sequence_name!r}")
    increment, start_value, last_value, sql = rows[0]
    if increment is None or increment <= 0:
        # A descending sequence moves away from any floor it is asked to clear.
        raise ValueError(
            f"sequence {sequence_name!r} has increment {increment}; "
            "only ascending sequences can be advanced"
        )

    # A reading that can only err low: `last_value` is at most one increment
    # behind the counter, and NULL means the sequence has never been used, so
    # its counter is still its start. Erring low burns one value too many,
    # which leaves a gap in the ids; erring high hands out an id that exists.
    conservative = last_value if last_value is not None else start_value

    match = _START.search(sql or "")
    if match is not None:
        rendered = int(match.group(1))
        plausible = (
            {conservative}
            if last_value is None
            else {conservative, conservative + increment}
        )
        if rendered in plausible:
            return rendered, increment
    # The rendering is the only exact source and it is not a documented
    # interface. If it ever stops agreeing with the columns, trust the reading
    # that cannot collide.
    return conservative, increment


def next_value(conn, sequence_name: str) -> int:
    """What the next `nextval(sequence_name)` will return. Consumes nothing."""
    return _state(conn, sequence_name)[0]


def advance_to(
    conn,
    sequence_name: str,
    floor: int,
    *,
    max_burn: int = DEFAULT_MAX_BURN,
) -> int:
    """Make the next `nextval(sequence_name)` return more than `floor`.

    A sequence already past the floor is left where it is and nothing is
    consumed. Otherwise exactly as many values are burned as it takes, and the
    new position is read back and checked rather than assumed.

    Returns the number of values burned (0 when the sequence did not move).
    Raises ValueError for a name that is not a plain identifier, a descending
    sequence, or a gap larger than `max_burn`; LookupError for a sequence that
    does not exist; RuntimeError if the position read back is still not past
    the floor.
    """
    floor = operator.index(floor)
    current, increment = _state(conn, sequence_name)
    shortfall = floor + 1 - current
    if shortfall <= 0:
        return 0

    burn = -(-shortfall // increment)  # ceiling division
    if burn > max_burn:
        raise ValueError(
            f"advancing {sequence_name!r} from {current} past {floor} would burn "
            f"{burn:,} values, more than the limit of {max_burn:,}"
        )

    # See the module docstring: an aggregate, fully fetched, or the burn is
    # neither complete nor durable.
    conn.execute(
        f"SELECT max(nextval('{sequence_name}')) FROM range({burn})"
    ).fetchall()

    after = next_value(conn, sequence_name)
    if after <= floor:
        raise RuntimeError(
            f"sequence {sequence_name!r} still hands out {after} after burning "
            f"{burn:,} values; expected more than {floor}"
        )
    return burn
