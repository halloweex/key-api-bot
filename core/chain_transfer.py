"""Carrying a write chain's tables back out of Postgres, and the check before.

WHY THIS EXISTS

Since DN-06 the first Postgres write a chain performs LATCHES it: every
consumer reads `writes_postgres()`, that answers True from the marker on disk
whatever `KS_WRITE_*` says, and editing the variable back to `duckdb` is
therefore no longer a rollback of anything (`core/chain_latch.py`, owner
decision OD-19 (a)). It is not even a no-op — it is a disagreement the shipper
stamps, the canary pages and the daily comparison files as CRITICAL.

So the way back has to be a copy, and this module is it: read every table the
chain owns out of Postgres, write it into DuckDB, compare the two at zero, and
only then release both copies of the latch. Until it existed there was no
rollback for a latched chain at all, which is why the plan blocks the
`KS_WRITE_INVENTORY` flip on it.

THE COPY CAN DESTROY WHAT IT IS ABOUT TO COMPARE, SO THE CHECK COMES FIRST

A full-replace table is written as `DELETE` then `INSERT` of what Postgres
holds. After that statement the two stores agree by construction, so a
comparison run afterwards proves nothing about a row DuckDB held and Postgres
did not: the copy deleted it, and the comparison looked at the copy. The first
draft of this module released a latch on exactly that — a review reproduced it
with an offer catalogued in DuckDB after the last hourly shipment, and the
"clean" copy-back took DuckDB from two offers to one.

So `copy_back` asks the handover question of the two stores **as they stand,
before anything is written** — the same classification `handover_check` runs —
and refuses on any CRITICAL. What it refuses is anything the write would
destroy and cannot bring back: a key only DuckDB holds, a DuckDB value that is
provably later than Postgres's, two rows under one key in a table that writes
each row once, and a Postgres row below the append watermark the copy never
reads. The dry run asks the same question, so it says what `--execute` will
refuse.

THE WRITE AND ITS PROOF ARE ONE TRANSACTION

Everything the copy writes — every table, the watermarks, the id allocator — and
the zero-tolerance comparison of the result run on ONE DuckDB connection inside
ONE transaction, and it commits only on zero findings. A difference found there
rolls the whole copy back, so a refused copy-back leaves DuckDB exactly as it
found it and the latch where it was. The one thing a ROLLBACK does not undo is
the id allocator, measured on DuckDB 1.5.5: a value burned inside a transaction
stays burned for the rest of the process, and reaches the file only if
something commits afterwards — a CHECKPOINT alone does not write it. Either way
the allocator ends at or above where it was, which can leave a gap in the ids
and cannot reissue one.

THE SPECS ARE DERIVED, NEVER A FOURTH LIST

Four descriptions of these tables already exist: the column tuples, the
shipping shape (`_FULL_REPLACE` / `_APPEND_ABOVE` in `core/pg_operational.py`),
the comparison specs (`OPERATIONAL_TABLES` / `APPEND_ONLY_TABLES` in
`core/mirror_reconciliation.py`) and each chain's `CHAIN_TABLES`. A fifth,
written out here, would be a fifth chance to disagree about what a table is —
and the one that decides whether a latch may be released is the worst place
for that. `chain_specs()` therefore intersects the existing lists and **raises**
for a chain table that has no shipping shape or no comparison spec, rather
than skipping it: a chain that gains a table and forgets to give it one would
otherwise be copied back incompletely and released on a clean comparison of
everything else.

WHAT IS NOT DERIVED, AND WHY

Three properties of the comparison are reversed here, because the direction is:

- `synced_column` is dropped from the comparison. It exists to forgive a row
  written between the copy and the check, and nothing can be in flight when
  the copy-back runs — web is stopped, that is the precondition. It is kept in
  one narrower role, as `TableTransfer.clock`, and only where both stores carry
  it as a value: that is what orders two versions of one row.
- the append tables are compared **whole** rather than by fingerprint. The
  fingerprint is the right instrument for 162,883 rows every morning and the
  wrong one for the single comparison that decides whether a latch may be
  released: it cannot see a text column rewritten to exactly the same length,
  and that blind spot is affordable daily and not affordable once.
- **every column the copy writes is compared**, bar the daily spec's
  `ignore_columns` (below). The daily `stock_movements`
  spec leaves `recorded_at` out of its values because it is that spec's clock,
  and a clock that is also a compared value can never forgive a disagreement
  by itself. The copy-back has no clock to excuse — the append tables get no
  `TableTransfer.clock` — so the column was covered neither way, while the copy
  writes it and the SKU rebuild reads it (`kyiv_date(recorded_at)` for the last
  stock-out). A review shifted every copied `recorded_at` by an hour and the
  latch was released on a clean comparison. `_compare_spec` now appends any
  written column the daily spec does not read.

`ignore_columns` is kept exactly as the daily comparison declares it. Those are
the columns the two stores are not expected to agree on, and a second opinion
about them here would be the fifth list this module exists to avoid. Across
both chains today it is exactly two written columns, `bronze.offers.synced_at`
and `app.sku_inventory_status.updated_at`, and forgiving them here is right
for a reason `recorded_at` does not share: each is ONE value stamped across
the whole table by one sync (`INSERT OR REPLACE ... CURRENT_TIMESTAMP`, which
is transaction-stable) or one rebuild (`DELETE` + `INSERT`). It records when
that writer last ran, not anything about an offer or a SKU, and the writer
restamps it wholesale on its next hourly run after `up -d` — the offers sync
for every offer KeyCRM still serves, the stock sync's rebuild for every SKU. So
a bad copy of either could cost a wrong "as of" on /inventory until then
(`MAX(updated_at)`, the stock summary's `snapshot_at`), where a bad
`recorded_at` would date a movement wrongly for good.
`tests/unit/test_chain_transfer.py` computes the set, so a third cannot join
it silently.
"""
from __future__ import annotations

import dataclasses
import logging
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from types import ModuleType
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

from core.data_quality import IntegrityIssue, Severity
from core.mirror_reconciliation import (
    APPEND_ONLY_TABLES,
    OPERATIONAL_TABLES,
    BucketedTable,
    MirroredTable,
    _sample,
    compare_table,
    fetch_duckdb_rows,
    fetch_pg_rows,
)
from core.pg_operational import _APPEND_ABOVE, _FULL_REPLACE, _Append

logger = logging.getLogger(__name__)

# The copy-back's comparison forgives nothing. The daily check's 90 minutes are
# slack for a row written while it was reading; here the writer is stopped and
# the rows were written by the statement above, so any slack would only be
# somewhere for a defect to hide.
COPY_BACK_GRACE_MINUTES = 0

# How many rows one DuckDB INSERT carries. Not `executemany`: DuckDB's Python
# client runs the prepared statement once per row, and measured on 1.5.5 into
# `inventory_sku_history`'s shape (primary key plus index, one transaction)
# that is 5.5 s for 5,000 rows and about eight minutes for 164,000 with no
# memory limit. Under the store's own 4 GB limit — the default, and what a
# one-off container gets — chain 1 at production size does not finish at all:
# `OutOfMemoryException` at 3.7 GiB within 17 s. One multi-row VALUES per
# 1,000 rows moved 163,000 in 3.8 s, and binds exactly as `executemany` does:
# the two were compared value for value on DATE, DECIMAL, TIMESTAMPTZ with and
# without an offset, a NULL in the first row, and text with both quote marks.
DUCKDB_CHUNK = 1000

_ORIGIN_COPY_BACK = (
    "Both sides were written from the same Postgres rows in the same call — "
    "the copy-back read Postgres and wrote DuckDB — so a disagreement here is "
    "the copy having failed, and the latch stays where it is."
)

_IDENTIFIER = re.compile(r"[a-z_][a-z0-9_]*")
_COALESCE = re.compile(r"COALESCE\s*\((.*)\)", re.IGNORECASE)


@dataclass(frozen=True)
class TableTransfer:
    """One table of one chain: how to carry it back, and how to prove it."""
    pg_table: str
    dk_table: str
    columns: Tuple[str, ...]
    order_by: str
    compare: MirroredTable
    # None for a full replace. An append table is carried above the DuckDB
    # MAX rather than replaced, for the reason it is shipped that way: nothing
    # may delete a `stock_movements` row, because it is the only record that a
    # quantity ever changed.
    append: Optional[_Append] = None
    # The columns that date one version of a row in BOTH stores, first
    # non-NULL wins; empty when there are none. See `_shared_clock`.
    clock: Tuple[str, ...] = ()

    @property
    def is_append(self) -> bool:
        return self.append is not None


def _compare_spec(source: MirroredTable | BucketedTable,
                  keys: Sequence[str],
                  written: Sequence[str]) -> MirroredTable:
    """The daily comparison's spec, turned to face the other way.

    A `BucketedTable` carries no key: the daily check finds its suspect rows
    by fingerprint and drills into a bucket. Reading it whole needs one, and
    the honest source is the shipper's own `ON CONFLICT` target — the columns
    it treats as the row's identity when it writes it.

    `written` is what the copy writes, and every one of those columns is read
    here: the daily spec's own, in its order, then any it leaves out. Today
    that adds exactly `app.stock_movements.recorded_at` (see the module
    docstring). `ignore_columns` is untouched — a column the daily check reads
    and deliberately forgives is a different decision from one it never read.
    """
    columns = tuple(source.columns) + tuple(
        c for c in written if c not in source.columns)
    if isinstance(source, MirroredTable):
        return dataclasses.replace(
            source, columns=columns, synced_column=None,
            origin_note=_ORIGIN_COPY_BACK,
        )
    return MirroredTable(
        pg_table=source.pg_table,
        dk_table=source.dk_table,
        columns=columns,
        numeric=source.numeric,
        key_columns=tuple(keys),
        synced_column=None,
        origin_note=_ORIGIN_COPY_BACK,
        # There is no retired category in this direction and there never can
        # be: the copy-back writes every row Postgres holds, so a key on one
        # side only is a failed copy whichever side it is on.
        full_replace=True,
    )


def _shared_clock(source: MirroredTable | BucketedTable) -> Tuple[str, ...]:
    """The columns that say which of two versions of a row is later, if any.

    Derived from the daily comparison's own clock, `synced_column`, and taken
    only when every column it names is also **shipped and compared** — carried
    as a value, so the Postgres copy holds the same stamp DuckDB wrote, and
    Postgres's own writer stamps it the same way when it rewrites the row.
    Today that is `app.manual_expenses` (`COALESCE(updated_at, created_at)`)
    and `app.inventory_history` (`recorded_at`).

    Everywhere else the stamp orders nothing across the two stores:
    `offer_stocks.synced_at` is DuckDB's alone, against Postgres's
    `mirrored_at`; `offers.synced_at` is shipped but never compared, because
    one sync stamps every row it touched with one transaction-stable value;
    `sku_inventory_status.updated_at` is restamped on all rows by every
    rebuild. Those return nothing, and the handover says so rather than
    pretending to a comparison it cannot make.

    Append tables return nothing by design, not for want of a clock: a row in
    them is written once, so two versions of one key are two events.
    """
    expr = source.synced_column if isinstance(source, MirroredTable) else None
    if not expr:
        return ()
    match = _COALESCE.fullmatch(expr.strip())
    names = tuple(
        n.strip() for n in (match.group(1).split(",") if match else [expr])
    )
    if all(
        _IDENTIFIER.fullmatch(n)
        and n in source.columns
        and n not in source.ignore_columns
        for n in names
    ):
        return names
    return ()


def chain_specs(chain: ModuleType) -> Tuple[TableTransfer, ...]:
    """Every table of one chain, in the order the copy-back writes them.

    The order is `CHAIN_TABLES`' own, which for chain 1 puts `bronze.offers`
    and `bronze.offer_stocks` first. That is not load-bearing here — the whole
    chain is one DuckDB transaction, so no writer can see a half-copied
    state — but it matches the rollback the chain map describes, where
    `offer_stocks` has to be restored before its writer resumes or the next
    tick computes a day of drift as one giant movement per offer and labels it
    wrong.
    """
    full = {pg: (dk, cols, order) for pg, dk, cols, order in _FULL_REPLACE}
    appends = {spec.pg_table: spec for spec in _APPEND_ABOVE}
    compare_full = {spec.pg_table: spec for spec in OPERATIONAL_TABLES}
    compare_appended = {spec.pg_table: spec for spec in APPEND_ONLY_TABLES}

    out: List[TableTransfer] = []
    for table in chain.CHAIN_TABLES:
        if table in appends:
            spec = appends[table]
            source = compare_appended.get(table)
            if source is None:
                raise LookupError(
                    f"{table} is shipped above a watermark but has no entry in "
                    "APPEND_ONLY_TABLES; the copy-back cannot prove it landed"
                )
            out.append(TableTransfer(
                pg_table=table, dk_table=spec.dk_table, columns=spec.columns,
                order_by=spec.order_by, append=spec,
                compare=_compare_spec(source, spec.keys, spec.columns),
            ))
            continue
        if table in full:
            dk_table, columns, order_by = full[table]
            source = compare_full.get(table)
            if source is None:
                raise LookupError(
                    f"{table} is replaced whole but has no entry in "
                    "OPERATIONAL_TABLES; the copy-back cannot prove it landed"
                )
            out.append(TableTransfer(
                pg_table=table, dk_table=dk_table, columns=columns,
                order_by=order_by, compare=_compare_spec(source, (), columns),
                clock=_shared_clock(source),
            ))
            continue
        raise LookupError(
            f"{table} is in {chain.CHAIN} but in neither _FULL_REPLACE nor "
            "_APPEND_ABOVE: nothing knows how to carry it in either direction"
        )
    return tuple(out)


@dataclass(frozen=True)
class SequenceTransfer:
    """A DuckDB id allocator that has to clear what Postgres handed out."""
    dk_sequence: str
    dk_table: str
    column: str
    pg_sequence: str


def chain_sequences(chain: ModuleType) -> Tuple[SequenceTransfer, ...]:
    """The id allocators this chain's copy-back has to move, derived twice over.

    From `core.migrations.SEQUENCE_ID_COLUMNS` — the list the boot already
    keeps above each table's MAX — intersected with the chain's own tables. The
    Postgres name is `<table>_<column>_seq`, which is what revisions 0030 and
    0031 created and is PostgreSQL's own spelling for a column-owned sequence;
    a third hand-written list of sequence names is exactly what this module
    refuses to have.
    """
    from core.migrations import SEQUENCE_ID_COLUMNS

    by_dk = {spec.dk_table: spec.pg_table for spec in chain_specs(chain)}
    return tuple(
        SequenceTransfer(
            dk_sequence=seq, dk_table=dk_table, column=column,
            pg_sequence=f"{by_dk[dk_table]}_{column}_seq",
        )
        for seq, dk_table, column in SEQUENCE_ID_COLUMNS
        if dk_table in by_dk
    )


def resolve_chain(name: str) -> ModuleType:
    """A chain by its own name or by the short one an operator types."""
    from core.write_chains import WRITE_CHAINS, chain_name

    wanted = (name or "").strip().lower()
    for chain in WRITE_CHAINS:
        full = chain_name(chain)
        if wanted in {full, full.removeprefix("pg_").removesuffix("_write")}:
            return chain
    known = ", ".join(sorted(chain_name(c) for c in WRITE_CHAINS))
    raise LookupError(f"no write chain called {name!r}; known chains: {known}")


# ─── The handover question, asked of two stores as they stand ────────────────


def _as_utc(value: Any) -> Optional[datetime]:
    if not isinstance(value, datetime):
        return None
    return value if value.tzinfo else value.replace(tzinfo=timezone.utc)


def _clock_of(spec: TableTransfer, row: Tuple[Any, ...]) -> Optional[datetime]:
    for column in spec.clock:
        value = _as_utc(row[spec.compare.columns.index(column)])
        if value is not None:
            return value
    return None


def _later_in_duckdb(spec: TableTransfer, dk_row, pg_row) -> bool:
    """DuckDB's version is provably the later one. A missing stamp proves nothing."""
    dk, pg = _clock_of(spec, dk_row), _clock_of(spec, pg_row)
    return dk is not None and pg is not None and dk > pg


def classify_handover(
    spec: TableTransfer,
    dk_rows: Mapping[Any, Tuple[Any, ...]],
    pg_rows: Mapping[Any, Tuple[Any, ...]],
    *,
    moved_on: bool,
    max_samples: int = 10,
) -> List[IntegrityIssue]:
    """Both sides of one table, already read. Pure, so every branch is testable.

    `moved_on` is whether Postgres has become the writer — owner rows exist
    for the chain. The contract the plan wrote is "DuckDB's rows are a subset
    of Postgres by key, and mutable tables in Postgres are equal or newer", and
    this is that contract, table shape by table shape:

    - **A key only DuckDB holds** is CRITICAL in both states. Before a flip it
      is a row the flip would strand: the shipper stands down the moment the
      chain routes to Postgres and these tables have no backfill. After one, a
      full-replace copy-back would delete it, and an append table would keep
      it on one side only, so the comparison could never be clean.
    - **Append-only tables** have no "newer": a row is written once, so the
      same key with different values is two events, CRITICAL in both states.
      A Postgres row at or below DuckDB's watermark that DuckDB does not hold
      is CRITICAL too — the copy-back reads only above it, so that row can
      never come back.
    - **Mutable tables before a flip** must be equal. DuckDB is the only
      writer and Postgres is fed only by copying it, so Postgres cannot
      legitimately be newer — every difference is the copy being broken, and
      the right answer is to refuse.
    - **Mutable tables after the latch**: Postgres is the writer, so a
      difference is its later write — INFO, the work the copy-back carries —
      unless the row's own clock, carried by both stores, says DuckDB's version
      is the later one. That is an edit made after the last shipment which
      never reached Postgres, and the copy would overwrite it: CRITICAL. Where
      no shared clock exists the difference is taken as Postgres being newer
      and the description says so; `--handover` clean before the flip is what
      makes that true by construction, which is why the runbook requires it.
    """
    table, dk_table = spec.pg_table, spec.dk_table
    issues: List[IntegrityIssue] = []

    missing = sorted(dk_rows.keys() - pg_rows.keys(), key=_sortable)
    if missing:
        if spec.is_append:
            why = (
                f"{table} is append-only in both stores, so nothing can have "
                "removed them from Postgres: they were written after the last "
                "hourly shipment. A copy-back copies in the other direction, "
                "so it can neither carry them nor delete them, and its "
                "comparison would never be clean."
            )
        elif moved_on:
            why = (
                "A copy-back replaces this table with what Postgres holds, so "
                "it would delete them. It cannot tell a row the flip stranded "
                "(written after the last hourly shipment) from one Postgres's "
                "own writer deleted after the latch — a row on one side only "
                "has nothing to compare a clock against. None of chain 1's "
                "writers removes a key from these tables, so there it is "
                "always stranded; for app.manual_expenses it can be either. "
                "Decide per id — insert it into Postgres if it was stranded, "
                "delete it from DuckDB if Postgres deleted it — and run this "
                "again."
            )
        else:
            why = (
                "The shipper stands down the moment this chain routes to "
                "Postgres and these tables have no backfill, so a flip now "
                "strands them. Bring web back with the flag unchanged, let "
                "replicate_operational run, and check again."
            )
        issues.append(IntegrityIssue(
            check_name="handover_rows_missing",
            table_name=table,
            severity=Severity.CRITICAL,
            count=len(missing),
            sample_ids=_sample(spec.compare, missing, max_samples),
            description=(
                f"{len(missing)} row(s) in DuckDB's {dk_table} have no "
                f"counterpart in {table}. {why}"
            ),
        ))

    differing = sorted(
        (key for key in dk_rows.keys() & pg_rows.keys()
         if dk_rows[key] != pg_rows[key]),
        key=_sortable,
    )
    if differing and spec.is_append:
        issues.append(IntegrityIssue(
            check_name="handover_rows_differ",
            table_name=table,
            severity=Severity.CRITICAL,
            count=len(differing),
            sample_ids=_sample(spec.compare, differing, max_samples),
            description=(
                f"{len(differing)} key(s) are held by both stores with "
                f"different rows. {table} is append-only — a row is written "
                "once — so these are two events under one key, not an older "
                "and a newer. For an id-keyed table the usual cause is an id "
                "both allocators handed out: Postgres floors its sequence on "
                "its own MAX(id), so a movement DuckDB wrote after the last "
                "shipment is an id Postgres issues again after the flip. "
                "Nothing here can decide which event keeps the key, and "
                "running this again gives the same answer."
            ),
        ))
    elif differing and not moved_on:
        issues.append(IntegrityIssue(
            check_name="handover_rows_differ",
            table_name=table,
            severity=Severity.CRITICAL,
            count=len(differing),
            sample_ids=_sample(spec.compare, differing, max_samples),
            description=(
                f"{len(differing)} row(s) differ between DuckDB's {dk_table} "
                f"and {table}. Postgres has no writer yet but the hourly copy "
                "of DuckDB, so it cannot be newer: the two must agree, and a "
                "flip now would freeze these values in Postgres."
            ),
        ))
    elif differing:
        newer_in_duckdb = [
            key for key in differing
            if _later_in_duckdb(spec, dk_rows[key], pg_rows[key])
        ]
        if newer_in_duckdb:
            issues.append(IntegrityIssue(
                check_name="handover_rows_newer_in_duckdb",
                table_name=table,
                severity=Severity.CRITICAL,
                count=len(newer_in_duckdb),
                sample_ids=_sample(spec.compare, newer_in_duckdb, max_samples),
                description=(
                    f"{len(newer_in_duckdb)} row(s) differ and DuckDB's "
                    f"version is the later one by the row's own clock "
                    f"({', '.join(spec.clock)}, carried by both stores). "
                    "DuckDB stopped writing this chain at the flip, so these "
                    "are edits made after the last shipment that never "
                    "reached Postgres, and a copy-back would overwrite them "
                    "with the older values. Carry them into Postgres, or "
                    "decide they are to be discarded, before running this "
                    "again."
                ),
            ))
        refused = set(newer_in_duckdb)
        overwritten = [k for k in differing if k not in refused]
        if overwritten:
            clockless = "" if spec.clock else (
                f" Neither store keeps a per-row time for {table} that "
                "orders one version against the other, so this is taken as "
                "Postgres being newer — true by construction when --handover "
                "was clean before the flip."
            )
            issues.append(IntegrityIssue(
                check_name="handover_rows_differ",
                table_name=table,
                severity=Severity.INFO,
                count=len(overwritten),
                sample_ids=_sample(spec.compare, overwritten, max_samples),
                description=(
                    f"{len(overwritten)} row(s) differ between DuckDB's "
                    f"{dk_table} and {table}. Postgres has been the writer "
                    "since the latch, so these are its later writes — the "
                    f"work a copy-back carries.{clockless}"
                ),
            ))

    ahead = sorted(pg_rows.keys() - dk_rows.keys(), key=_sortable)
    behind: List[Any] = []
    if ahead and spec.is_append:
        position = spec.compare.columns.index(spec.append.watermark)
        floor = max((row[position] for row in dk_rows.values()), default=None)
        if floor is not None:
            behind = [
                key for key in ahead
                if pg_rows[key][position] < floor
                or (not spec.append.inclusive and pg_rows[key][position] == floor)
            ]
            if behind:
                issues.append(IntegrityIssue(
                    check_name="handover_rows_behind_watermark",
                    table_name=table,
                    severity=Severity.CRITICAL,
                    count=len(behind),
                    sample_ids=_sample(spec.compare, behind, max_samples),
                    description=(
                        f"{len(behind)} row(s) in {table} sit at or below "
                        f"DuckDB's MAX({spec.append.watermark}) = {floor} and "
                        f"are not in DuckDB's {dk_table}. Nothing deletes from "
                        "an append-only table, so DuckDB lost them or "
                        "something other than the shipper and the chain's own "
                        "writer put them in Postgres — and a copy-back, which "
                        "carries this table only from that MAX upwards, could "
                        "never bring them back."
                    ),
                ))
            unreachable = set(behind)
            ahead = [key for key in ahead if key not in unreachable]
    if ahead:
        issues.append(IntegrityIssue(
            check_name="handover_rows_ahead",
            table_name=table,
            severity=Severity.INFO,
            count=len(ahead),
            sample_ids=_sample(spec.compare, ahead, max_samples),
            description=(
                f"{len(ahead)} row(s) in {table} are not in DuckDB's "
                f"{dk_table}. "
                + ("Written since the chain was latched — the size of the "
                   "copy-back."
                   if moved_on else
                   "Nothing writes Postgres here but the hourly copy of "
                   "DuckDB, so these are rows DuckDB has deleted since it "
                   "last ran, which the next full replace removes — or, for "
                   "an append-only table that never deletes, a writer that "
                   "is not the copy.")
            ),
        ))
    return issues


async def _handover_issues(
    store,
    pool,
    specs: Sequence[TableTransfer],
    *,
    moved_on: bool,
    max_samples: int,
) -> List[IntegrityIssue]:
    """Read both sides one table at a time and classify them.

    One table at a time so that the process holds two copies of one table,
    never two copies of the chain — `inventory_sku_history` alone is 162,883
    rows a side in production.
    """
    issues: List[IntegrityIssue] = []
    for spec in specs:
        async with store.connection() as conn:
            dk_rows, _synced = fetch_duckdb_rows(conn, spec.compare)
        pg_rows = await fetch_pg_rows(pool, spec.compare)
        issues += classify_handover(
            spec, dk_rows, pg_rows, moved_on=moved_on, max_samples=max_samples,
        )
    return issues


async def _owned_since(pool, name: str) -> Optional[str]:
    """When Postgres took the chain, from the owner rows; None if it never did."""
    from core import chain_latch

    return chain_latch.claimed_chains(await chain_latch.read_owners(pool)).get(name)


async def handover_check(
    store,
    chain: ModuleType,
    *,
    max_samples: int = 10,
) -> List[IntegrityIssue]:
    """Has everything DuckDB holds reached Postgres, and is none of it later?

    `classify_handover` is the rule. Which half of it applies is decided by the
    **owner rows**, not by the marker: the owner rows are written inside every
    Postgres write, so they are what says Postgres has actually received writes
    and moved on. A marker without them is a first write that failed, where
    DuckDB is still the truth and every difference is read as the copy being
    behind.

    Two uses, one answer. Before a flip it is the gate — exit 0 is what says
    the flip strands nothing. Before a copy-back it is the preview: its
    CRITICALs are exactly what `copy_back` refuses on, and its INFOs are the
    size of the job.

    It writes nowhere, but it is **not** a live check. It opens the DuckDB file
    the way everything here does, and DuckDB will not let a second process open
    a file the web container holds — not even read-only — so it runs in the
    same stopped window as the copy-back. A pre-flight against a running system
    would have to be an endpoint inside the web process.
    """
    from core.pg import get_pool, require_revision
    from core.write_chains import chain_name

    pool = await get_pool()
    await require_revision()
    moved_on = await _owned_since(pool, chain_name(chain)) is not None
    return await _handover_issues(
        store, pool, chain_specs(chain), moved_on=moved_on, max_samples=max_samples,
    )


def _sortable(key: Any) -> Tuple[str, Any]:
    """Sort mixed keys without comparing an int to a tuple to a date."""
    return (type(key).__name__, key)


# ─── The copy back ───────────────────────────────────────────────────────────


class CopyBackRefused(RuntimeError):
    """A precondition of the copy-back does not hold. Nothing was written."""

    def __init__(self, message: str, issues: Sequence[IntegrityIssue] = ()):
        super().__init__(message)
        self.issues = tuple(issues)


class CommittedNotReleased(RuntimeError):
    """The copy COMMITTED, and the checkpoint or the release after it failed.

    Its own type, because the sentence every other failure here ends on —
    "DuckDB holds what it held" — is false for this one alone. A refusal wrote
    nothing and a finding rolled back; here DuckDB HAS the copy, and an
    operator who read this as the other two would expect `up -d` to return to
    the state before the run. It does not, and what does is decided by which
    copies of the latch survived, which is why the exception carries them:
    `latch` is `{"marker", "owned_since", "owners_error"}` as read after the
    failure, and the message is the next step for that state.
    """

    def __init__(self, message: str, plan: Mapping[str, Any],
                 latch: Mapping[str, Any]):
        super().__init__(message)
        self.plan = dict(plan)
        self.latch = dict(latch)


def _render(issue: IntegrityIssue) -> Dict[str, Any]:
    return {"check": issue.check_name, "table": issue.table_name,
            "severity": issue.severity.value, "count": issue.count,
            "samples": list(issue.sample_ids), "description": issue.description}


async def copy_back(
    store,
    chain: ModuleType,
    *,
    dry_run: bool = True,
    max_samples: int = 10,
    now: Optional[datetime] = None,
) -> Dict[str, Any]:
    """Carry one chain's tables out of Postgres, prove the copy, release it.

    PRECONDITIONS, AND THE ONE THAT IS DELIBERATELY ABSENT

    - **Web is stopped.** DuckDB's file lock is exclusive for writers, so the
      proof is that `store` opened read-write at all: a running web container
      holds that lock and the connect raises. `scripts/chain_copy_back.py`
      turns that exception into a sentence. The check here is the weaker
      restatement — that the connection is not read-only — because a check
      that asks the docker socket whether a container is up can be wrong in
      both directions and this one cannot.
    - **Postgres took the chain: owner rows exist.** The two copies of the
      latch are not alternatives here, because they answer different
      questions. The owner rows are written inside every Postgres write, so
      they are the proof that rows changed hands; with them present this is
      a rollback, whether the marker survived or not — a marker lives on a
      bind mount and an older `./data` loses it while Postgres keeps both the
      rows and the ownership. A **marker without owner rows** is a first write
      that failed after the marker (or a release that died between its two
      deletes): Postgres received nothing DuckDB lacks, so copying it back
      would be a rewind, not a rollback. That state is refused with the
      sentence that clears it, and neither copy is refused into silence.
    - **Nothing the write would destroy.** `handover_check`'s CRITICALs,
      asked before anything is written — see the module docstring.
    - **`KS_WRITE_*` is not a precondition.** Under OD-19 (a) it does not route
      writes while the chain is latched, so requiring it to say anything in
      particular would only add a step that changes nothing. The runbook printed
      at the end is where it is put back, after the rows have landed.

    Then one DuckDB transaction writes every table, the watermarks and the id
    allocator, compares the result with Postgres at zero on the same
    connection, and commits only on zero findings. A half-copied chain is the
    state with no name: `offer_stocks` restored and `stock_movements` not means
    the next sync computes its deltas against the new base and writes them with
    ids the old allocator hands out.

    After the COMMIT come the checkpoint and the release, and either can
    raise. That is `CommittedNotReleased`, never a bare exception: DuckDB has
    changed by then, and its message reads which copies of the latch survived
    and says what to run for that state.

    `dry_run=True` is the default and reads only — it returns the same shape
    with `executed: False` and the counts it would have written, and it is
    refused on exactly what `--execute` would be refused on.
    """
    from core import chain_latch
    from core.pg import get_pool, require_revision
    from core.write_chains import chain_name

    name = chain_name(chain)
    now = now or datetime.now(timezone.utc)

    pool = await get_pool()
    await require_revision()

    marker = chain_latch.latched_at(name)
    owned = await _owned_since(pool, name)
    if owned is None and marker is None:
        raise CopyBackRefused(
            f"{name} is not latched: no local marker in "
            f"{chain_latch.MARKER_DIR} and no owner rows in "
            "meta.chain_watermarks. Nothing has changed hands, so there is "
            "nothing in Postgres this could bring back — and copying anyway "
            "would overwrite DuckDB with a store that is behind it."
        )
    if owned is None:
        raise CopyBackRefused(_marker_only(chain, name, marker))
    if marker is None:
        logger.warning(
            "Copy-back of %s: the local marker is gone and Postgres has owned "
            "these tables since %s. The writers have been following %s again "
            "while the shipper declined to ship — see core/chain_latch.py.",
            name, owned, chain.WRITE_ENV,
        )

    specs = chain_specs(chain)
    sequences = chain_sequences(chain)

    handover = await _handover_issues(
        store, pool, specs, moved_on=True, max_samples=max_samples,
    )
    blocking = [i for i in handover if i.severity is Severity.CRITICAL]
    if blocking:
        raise CopyBackRefused(_refusal(name, blocking), blocking)

    async with store.connection() as conn:
        _refuse_if_read_only(conn)
        floors = {
            spec.pg_table: conn.execute(
                f"SELECT MAX({spec.append.watermark}) FROM {spec.dk_table}"
            ).fetchone()[0]
            for spec in specs if spec.is_append
        }

    rows = {spec.pg_table: await _read_pg(pool, spec, floors.get(spec.pg_table))
            for spec in specs}
    watermarks = await _read_sync_keys(pool, chain)
    issued = await _read_pg_sequences(pool, sequences)

    plan = {
        "chain": name,
        # Both copies, because they can disagree and the disagreement is the
        # state an operator most needs to see on this screen.
        "latched_at": marker,
        "owned_since": owned,
        "rows": {table: len(found) for table, found in rows.items()},
        "sync_keys": watermarks,
        "sequences": {seq.dk_sequence: issued.get(seq.pg_sequence)
                      for seq in sequences},
        # What the copy will overwrite or carry, none of it blocking.
        "handover": [_render(i) for i in handover],
    }
    if dry_run:
        plan["executed"] = False
        plan["runbook"] = _runbook(chain, executed=False)
        return plan

    async with store.connection() as conn:
        _refuse_if_read_only(conn)
        conn.execute("BEGIN TRANSACTION")
        try:
            for spec in specs:
                # Popped, so each table's Postgres rows are released as soon
                # as they are written and the comparison below does not hold
                # them beside its own two copies.
                _write_duckdb(conn, spec, rows.pop(spec.pg_table))
            _write_sync_keys(conn, watermarks)
            # Inside the transaction, so a crash cannot leave the rows
            # committed below an allocator that still hands out their ids.
            # Measured on 1.5.5: a burn committed with the rows survives a
            # reopen without a checkpoint; a burn rolled back with them stays
            # burned in this process only — at or above where it was, never
            # below.
            plan["burned"] = advance_sequences(conn, sequences, issued)
            issues = await verify(conn, pool, specs, now=now,
                                  max_samples=max_samples)
            conn.execute("ROLLBACK" if issues else "COMMIT")
        except BaseException:
            try:
                conn.execute("ROLLBACK")
            except Exception:
                pass
            raise

    plan["executed"] = True
    plan["committed"] = not issues
    plan["findings"] = [_render(issue) for issue in issues]

    if issues:
        plan["released"] = False
        plan["runbook"] = _runbook(chain, executed=True, released=False)
        logger.error(
            "Copy-back of %s: the comparison inside the transaction found %d "
            "difference(s), so the copy was rolled back. DuckDB holds what it "
            "held before and the latch stays where it is.", name, len(issues),
        )
        return plan

    # From here on DuckDB has committed, so nothing below may leave the way a
    # failure before this point does. Either step can raise — a full disk
    # under the checkpoint, Postgres gone between the comparison and the
    # release — and until this was caught it left through the interpreter's
    # own exit 1, the code that means "rolled back", with DuckDB holding the
    # copy. Reproduced by the review with `release_chain` raising.
    stage = "checkpoint"
    try:
        # The WAL, or the next process reads a database that does not hold
        # what was just written. Everything about this script assumes the
        # container exits immediately afterwards.
        await store.checkpoint()
        stage = "release"
        plan["released"] = await release_chain(pool, chain)
    except Exception as exc:
        plan["committed"] = True
        plan["released"] = False
        latch = await _latch_state(pool, name)
        plan["latch"] = latch
        logger.error(
            "Copy-back of %s: committed to DuckDB, then the %s failed: %r",
            name, stage, exc,
        )
        raise CommittedNotReleased(
            _after_commit(chain, name, stage, exc, plan, latch), plan, latch,
        ) from exc
    plan["runbook"] = _runbook(chain, executed=True, released=True)
    logger.warning(
        "Copy-back of %s: every table compares equal and the latch is "
        "released. Put %s back to duckdb before the next write, or the first "
        "one re-latches the chain.", name, chain.WRITE_ENV,
    )
    return plan


def _marker_only(chain: ModuleType, name: str, marker: str) -> str:
    """The refusal for a marker with no owner row behind it, and its lever."""
    return (
        f"{name} has a local marker (since {marker}) and no owner rows in "
        "meta.chain_watermarks. Owner rows are written inside every Postgres "
        "write this chain makes, so Postgres has received nothing DuckDB does "
        "not already hold: this is a first write that failed after the marker "
        "was taken, or a release that removed the owner rows and died before "
        "the marker. A copy-back here would be a rewind, not a rollback — it "
        "would replace DuckDB's tables with a Postgres copy the shipper "
        "stopped feeding when the marker appeared. Nothing was written.\n"
        "To clear it, with web and bot still stopped:\n"
        + "\n".join(_marker_steps(chain, name))
    )


def _marker_steps(chain: ModuleType, name: str) -> List[str]:
    """How a marker with no owner rows is cleared, in the order it must be.

    `--handover` comes first and it is not a formality. The first draft of
    these steps offered "leave the flag at postgres, delete the marker, up -d"
    with nothing before it, and a review reproduced what that strands: an
    expense DuckDB held and never shipped — the shipper stood down the moment
    the marker appeared — so the next Postgres write latches the chain over a
    row every later copy-back refuses on. With no owner rows the handover
    applies the pre-flip rule, which is exactly the question that state asks.

    Shared with the one other place this state is reached — a release that
    died between its two deletes, `_after_commit` — so the two cannot drift.
    """
    from core import chain_latch

    env = chain.WRITE_ENV
    return [
        "  1. run this script with --handover, and nothing else, BEFORE "
        "touching the marker. With no owner rows it applies the pre-flip "
        "rule: exit 0 says everything DuckDB holds is in Postgres; a CRITICAL "
        "is a row DuckDB wrote that the shipper stopped carrying when the "
        "marker appeared;",
        f"  2. set {env}=duckdb in .env. Leave it at postgres only if step 1 "
        "exited 0 and the flip is meant to stand — the next write then takes "
        "the latch again, properly this time. With a CRITICAL outstanding, "
        "that write would latch the chain over a row every later copy-back "
        "refuses on;",
        f"  3. delete the marker: {chain_latch.marker_path(name)} inside this "
        f"container, data/write-chain-owners/{name} on the host;",
        "  4. docker compose up -d web bot. With the flag at duckdb the hourly "
        "copy resumes and carries whatever DuckDB wrote before the marker; ask "
        "--handover again, in a stopped window, before any later flip.",
    ]


async def _latch_state(pool, name: str) -> Dict[str, Any]:
    """Which copies of the latch survived a failure after COMMIT.

    Read, not inferred from which step raised: `release_chain` deletes the
    owner rows and then the marker, and a failure can land between the two.
    The marker is read from the disk rather than the process's cache, because
    an unlink whose directory fsync then raised has removed the file and not
    the cache entry. The owner rows need the Postgres that may be the thing
    that failed, so an unreadable answer is reported as unreadable rather
    than guessed.
    """
    from core import chain_latch

    state: Dict[str, Any] = {"marker": chain_latch.load().get(name),
                             "owned_since": None, "owners_error": None}
    try:
        state["owned_since"] = await _owned_since(pool, name)
    except Exception as exc:
        state["owners_error"] = f"{type(exc).__name__}: {exc}"
    return state


def _after_commit(
    chain: ModuleType,
    name: str,
    stage: str,
    exc: BaseException,
    plan: Mapping[str, Any],
    latch: Mapping[str, Any],
) -> str:
    """What a failure after COMMIT left behind, and the next step for it.

    Three states, told apart by `latch` and not by `stage`:

    - **owner rows held, or unreadable** — the checkpoint failed, or the
      release failed before its Postgres DELETE committed. Postgres is still
      the writer. Running `--execute` again is the step: its handover finds
      DuckDB already equal, and the copy commits and releases — exit 0, both
      copies gone, pinned in `tests/integration/test_chain_copy_back.py`.
    - **owner rows gone, marker held** — the release died between its two
      deletes. `--execute` refuses that state as a rewind, so this prints the
      steps that refusal prints.
    - **both gone** — the release finished and what raised came after the
      unlink (its directory fsync). Released; the ordinary runbook, with one
      check added, because an unlink that never reached the disk comes back.
    """
    from core import chain_latch

    env = chain.WRITE_ENV
    marker, owned, unreadable = (
        latch.get("marker"), latch.get("owned_since"), latch.get("owners_error"))
    lines = [
        f"the copy-back of {name} COMMITTED to DuckDB and the {stage} after "
        f"it failed ({type(exc).__name__}: {exc}). DuckDB is NOT as it was: "
        "it holds what the copy wrote, the chain's watermarks and the "
        "advanced id allocator — all compared equal to Postgres at zero "
        "before the commit. Rows carried from Postgres:",
    ]
    for table, count in sorted((plan.get("rows") or {}).items()):
        lines.append(f"  {table:<32} {count:>9,}")
    if stage == "checkpoint":
        # Measured on 1.5.5: rows committed and never checkpointed, the
        # process killed without a close, and the next open read them back.
        lines.append(
            "The commit is in DuckDB's WAL, which the next open replays; the "
            "checkpoint only folds it into the file.")
    lines.append(
        f"Latch now: marker {marker or 'gone'} "
        f"({chain_latch.marker_path(name)}); owner rows "
        + (f"UNREADABLE ({unreadable})" if unreadable
           else f"held since {owned}" if owned else "gone") + ".")

    if owned or unreadable:
        lines += [
            "Next, with web and bot still stopped"
            + (" and once Postgres answers" if unreadable else "")
            + ": run this script with --execute again. Its handover finds "
            "DuckDB already equal to Postgres, the copy writes the same rows "
            "inside a new transaction, compares them and releases — exit 0. "
            "If it refuses with \"no owner rows\" instead, the release had "
            "reached Postgres before it failed: follow the steps that refusal "
            f"prints. Leave {env} as it is until then.",
            "Bringing web back up instead is safe — the marker still routes "
            "every write to Postgres — but it leaves the chain latched until "
            "the next stopped window."
            if marker else
            "Do not bring web back up first: with the marker gone the writers "
            f"follow {env} again, and at duckdb they write a store the "
            "shipper — standing down on the owner rows — no longer copies.",
        ]
    elif marker:
        lines += [
            "The release died between its two deletes: the owner rows are "
            "gone and the marker is not. writes_postgres() still answers True "
            "from the marker, so nothing writes DuckDB yet, and --execute "
            "refuses this state as a rewind. Next, with web and bot still "
            "stopped:",
            *_marker_steps(chain, name),
        ]
    else:
        lines += [
            "Both copies of the latch are gone, so the chain is released; what "
            "failed came after the marker's unlink. Before step 2 below, check "
            f"that data/write-chain-owners/{name} is still absent on the host — "
            "an unlink whose directory fsync failed may not have reached the "
            "disk.",
            *_runbook(chain, executed=True, released=True),
        ]
    return "\n".join(lines)


def _refusal(name: str, blocking: Sequence[IntegrityIssue]) -> str:
    lines = [
        f"the copy-back of {name} would destroy rows it cannot bring back, so "
        "nothing was written and the latch is where it was. Postgres is still "
        "the writer; bringing web back up returns to the state before this run.",
    ]
    for issue in blocking:
        lines.append(
            f"  [{issue.severity.value}] {issue.check_name} on "
            f"{issue.table_name} ({issue.count}) samples={list(issue.sample_ids)}"
        )
        lines.append(f"      {issue.description}")
    return "\n".join(lines)


def _refuse_if_read_only(conn) -> None:
    """The weaker half of "web is stopped", asked of the connection itself."""
    row = conn.execute(
        "SELECT readonly FROM duckdb_databases() "
        "WHERE database_name = current_database()"
    ).fetchone()
    if row and row[0]:
        raise CopyBackRefused(
            "the DuckDB database is open read-only; the copy-back writes it"
        )


async def _read_pg(pool, spec: TableTransfer, floor: Any) -> List[Tuple[Any, ...]]:
    """One table out of Postgres: whole, or above what DuckDB already holds.

    `inclusive` is honoured because it means the same thing in this direction
    and for the same reason. `inventory_sku_history` is written a whole day at
    a time, so DuckDB's MAX(date) may be a day it holds only part of; `>` would
    step over the rest of it and leave a hole nothing fills.
    """
    columns = ", ".join(spec.columns)
    async with pool.acquire() as conn:
        if spec.append is None or floor is None:
            records = await conn.fetch(
                f"SELECT {columns} FROM {spec.pg_table} ORDER BY {spec.order_by}"
            )
        else:
            op = ">=" if spec.append.inclusive else ">"
            records = await conn.fetch(
                f"SELECT {columns} FROM {spec.pg_table} "
                f"WHERE {spec.append.watermark} {op} $1 ORDER BY {spec.order_by}",
                floor,
            )
    return [tuple(record[c] for c in spec.columns) for record in records]


async def _read_sync_keys(pool, chain: ModuleType) -> Dict[str, str]:
    """The chain's `last_sync_*` values, from where a latched chain keeps them.

    `meta.chain_watermarks`, not `app.sync_metadata`: revision 0032 moved them
    there precisely because the hourly full replace of `sync_metadata` out of
    DuckDB would wipe a watermark the Postgres writer had just set. A chain
    with no sync keys — chain 8 — returns nothing.
    """
    from core.pg_chain_watermarks import read_values

    keys = tuple(getattr(chain, "CHAIN_SYNC_KEYS", ()))
    return await read_values(keys) if keys else {}


async def _read_pg_sequences(
    pool, sequences: Sequence[SequenceTransfer],
) -> Dict[str, Optional[int]]:
    """The highest id Postgres has handed out, per sequence.

    **The floor is not MAX(id), and this is the trap the compaction work found
    first.** An id that was allocated and whose row was then deleted — a
    withdrawn expense — is gone from the table and not from the allocator, so a
    floor taken from the copied rows alone lets DuckDB reissue it. Two stores
    that have both used id 7 for different rows is the state revision 0030
    exists to forbid, and the forensic trail cannot be repaired afterwards.

    `last_value` is read as handed out whatever `is_called` says. Reading it
    one too high leaves a gap in the ids and reading it one too low reissues
    one, so the direction of the error is chosen rather than computed.

    A sequence Postgres does not have reports None and the floor falls back to
    the copied rows — which is right for a table whose ids never came from a
    Postgres allocator, and is nothing at all today, where both do.
    """
    out: Dict[str, Optional[int]] = {}
    async with pool.acquire() as conn:
        for spec in sequences:
            exists = await conn.fetchval("SELECT to_regclass($1)", spec.pg_sequence)
            if exists is None:
                out[spec.pg_sequence] = None
                continue
            out[spec.pg_sequence] = await conn.fetchval(
                f"SELECT last_value FROM {spec.pg_sequence}")
    return out


def _write_duckdb(conn, spec: TableTransfer, rows: Sequence[Tuple[Any, ...]]) -> None:
    """One table into DuckDB, in the shape its shipping direction implies.

    A full replace is a DELETE and an INSERT, which is what its DuckDB writer
    does anyway — safe only because `copy_back` has already refused anything
    the DELETE would take that Postgres does not hold. An append is written
    above the MAX that was read a moment ago, and takes `INSERT OR REPLACE`
    only where the read was inclusive: that spec deliberately re-reads the day
    it resumed from, so its rows can already be there, and the handover has
    already refused a key on that day whose two rows disagree. Everything else
    takes a plain INSERT, so a collision raises instead of overwriting — a row
    above the watermark cannot already exist, and if it does the watermark is
    wrong and the transaction should say so.

    `DUCKDB_CHUNK` rows per statement; see the constant for why not
    `executemany`.

    Columns the spec does not carry are left to their DuckDB defaults.
    `offer_stocks.synced_at` is the case: Postgres keeps `mirrored_at` for that
    purpose, so there is nothing to copy and CURRENT_TIMESTAMP is the honest
    answer — DuckDB did write these rows, now.
    """
    columns = ", ".join(spec.columns)
    row_sql = "(" + ", ".join("?" * len(spec.columns)) + ")"
    if spec.append is None:
        conn.execute(f"DELETE FROM {spec.dk_table}")
        verb = "INSERT INTO"
    else:
        verb = "INSERT OR REPLACE INTO" if spec.append.inclusive else "INSERT INTO"
    for start in range(0, len(rows), DUCKDB_CHUNK):
        part = rows[start:start + DUCKDB_CHUNK]
        conn.execute(
            f"{verb} {spec.dk_table} ({columns}) VALUES "
            + ", ".join([row_sql] * len(part)),
            [value for row in part for value in row],
        )


def _write_sync_keys(conn, watermarks: Mapping[str, str]) -> None:
    """The chain's watermarks back into DuckDB's own `sync_metadata`.

    Inside the same transaction as the rows, because a watermark without the
    rows it describes tells the next sync it has nothing to fetch.
    """
    for key, value in sorted(watermarks.items()):
        conn.execute(
            "INSERT OR REPLACE INTO sync_metadata (key, value, updated_at) "
            "VALUES (?, ?, CURRENT_TIMESTAMP)",
            [key, value],
        )


def advance_sequences(
    conn,
    sequences: Sequence[SequenceTransfer],
    issued: Mapping[str, Optional[int]],
) -> Dict[str, int]:
    """Put each DuckDB allocator above every id either store has used.

    `max(DuckDB MAX(id), the Postgres sequence's position)` — see
    `_read_pg_sequences` for why the second term is not redundant. A module
    function rather than a step inside the transaction so that the guard test
    can take it away and watch the collision it prevents.
    """
    from core.duckdb_sequences import advance_to

    burned: Dict[str, int] = {}
    for spec in sequences:
        row = conn.execute(
            f"SELECT COALESCE(MAX({spec.column}), 0) FROM {spec.dk_table}"
        ).fetchone()
        floor = max(int(row[0]) if row else 0, int(issued.get(spec.pg_sequence) or 0))
        burned[spec.dk_sequence] = advance_to(conn, spec.dk_sequence, floor)
    return burned


async def verify(
    conn,
    pool,
    specs: Sequence[TableTransfer],
    *,
    now: Optional[datetime] = None,
    max_samples: int = 10,
) -> List[IntegrityIssue]:
    """Every table of the chain, both sides read whole, tolerance zero.

    `conn` is the connection holding the copy-back's open transaction, so the
    DuckDB side is what the transaction wrote and nothing is committed yet —
    that is the whole point: a finding here rolls the copy back rather than
    reporting on one already made. One table at a time, for the memory reason
    `_handover_issues` gives.

    The watermark handed to `compare_table` is synthetic, and that is the
    honest thing rather than a shortcut. `meta.mirror_state` describes the
    *forward* shipment, which the latch stood down and the shipper has been
    stamping failing ever since — reading it here would report
    `mirror_failing` about a copy that is not the one being checked. The
    shipment this proves is the one the statements above just made, so its
    clock is now.
    """
    now = now or datetime.now(timezone.utc)
    watermark = {"last_ok_at": now, "failures_since_ok": 0}
    issues: List[IntegrityIssue] = []
    for spec in specs:
        dk_rows, dk_synced = fetch_duckdb_rows(conn, spec.compare)
        pg_rows = await fetch_pg_rows(pool, spec.compare)
        issues += compare_table(
            spec.compare, dk_rows, dk_synced, pg_rows, watermark,
            now=now, grace_minutes=COPY_BACK_GRACE_MINUTES,
            max_samples=max_samples,
        )
    return issues


async def release_chain(pool, chain: ModuleType) -> bool:
    """Drop both copies of the latch, Postgres first.

    The order is chosen and it is not symmetric. Losing the connection between
    the two leaves one copy standing, and only one of the two survivors is
    safe: a marker with no owner row keeps `writes_postgres()` answering True,
    so the writes stay where the rows are, the daily comparison files
    `chain_latch_disagrees`, and a second run of the copy-back refuses with
    the steps that clear it, `--handover` first. The reverse — an owner row
    with no marker — is the dangerous direction `core/chain_latch.py` names,
    where the writers follow `KS_WRITE_*` again while the shipper declines to
    ship.

    The chain's `last_sync_*` rows go with the owner rows. Their value has just
    been written into DuckDB, which is the store that owns the watermark from
    now on; a copy left in `meta.chain_watermarks` is read again by the next
    flip and is stale by then, which would resume a sync from a point DuckDB
    has long passed.
    """
    from core import chain_latch
    from core.write_chains import chain_name

    keys = [chain_latch.owner_key(t) for t in chain.CHAIN_TABLES]
    keys += list(getattr(chain, "CHAIN_SYNC_KEYS", ()))
    async with pool.acquire() as conn:
        async with conn.transaction():
            await conn.execute(
                "DELETE FROM meta.chain_watermarks WHERE key = ANY($1::text[])",
                keys,
            )
    # The return says the CHAIN is released, not that a file was unlinked.
    # `chain_latch.release` answers False when the marker was already gone,
    # which is the very case this function is reached in when only the owner
    # rows were holding the chain down — and reporting that as "not released"
    # would tell an operator to look for a latch that no longer exists.
    chain_latch.release(chain_name(chain))
    return True


def _runbook(chain: ModuleType, *, executed: bool, released: bool = False) -> List[str]:
    """What the operator does next, in the order it has to happen.

    Printed rather than performed: the variable lives in the host's `.env`,
    which no container may edit, and bringing `web` back up is the step that
    has to be taken by whoever can watch the soak checks afterwards.
    """
    name = chain.WRITE_ENV
    soak = ("E1 (expenses copy stood down) and E2"
            if name == "KS_WRITE_EXPENSES"
            else "I1 (inventory copy stood down), I2 and I3")
    if not executed:
        return [
            "This was a dry run. Nothing was written and nothing released.",
            "The handover held: nothing DuckDB holds would be destroyed. "
            "--execute writes the copy, compares it inside the same "
            "transaction and commits only if it is equal at zero.",
            "Re-run with --execute, with web and bot still stopped.",
        ]
    if not released:
        return [
            "Nothing was committed. The copy was written inside one DuckDB "
            "transaction and the comparison ran inside it; it found the "
            "differences above, so the transaction was rolled back and DuckDB "
            "holds exactly the rows and watermarks it held before this run.",
            "The id allocator is the one thing a rollback does not undo: the "
            "values burned stay burned in this process and reach the file "
            "only if something commits afterwards, which nothing here does. "
            "Either way it is at or above where it was — a gap in the ids at "
            "worst, never an id handed out twice.",
            f"The latch is held, so Postgres is still the writer. Leave {name} "
            "as it is; docker compose up -d web bot returns to exactly the "
            "state before this run.",
            "The handover passed before anything was written, so these are "
            "differences the copy itself did not reproduce: a row changed in "
            "Postgres while this ran (something is still writing — find it), "
            "or a value that does not survive the round trip, which is a "
            "defect in core/chain_transfer.py. Each finding names its table "
            "and ids. replicate_operational(full=True) is not a lever while "
            "the chain is latched — it stands down on the latch.",
        ]
    return [
        f"1. Set {name}=duckdb in /opt/key-api-bot/.env. The chain is no "
        "longer latched, so this variable decides again — and the next write "
        "re-latches the chain if it still says postgres.",
        "2. docker compose up -d web bot",
        f"3. At +2 min run deploy/stage4_soak.sh and read {soak}: the hourly "
        "copy must be shipping this chain's tables again, and its watermarks "
        "must be moving.",
        "4. The `owner:` rows and the local marker are gone. /api/health's "
        "write_chains block should show latched: false and mismatch: false.",
    ]
