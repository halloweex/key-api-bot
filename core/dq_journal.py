"""The one door to the data-quality journal: which store writes it, which reads it.

Chain 9 (`core/pg_dq_journal_write.py`, OD-02 (c)) moves the journal's writer
to Postgres and keeps DuckDB fed as a shadow. Every writer and reader of the
journal comes through here, so the five check jobs, the digest, the health
endpoints and the startup catch-up cannot come to disagree about where the
journal lives — `tests/unit/test_dq_journal.py` walks `core/`, `web/`, `bot/`
and `scripts/` and fails on a writer or reader that goes round this module.

UNDER `duckdb` (THE DEFAULT) NOTHING CHANGED

Each branch below marked "duckdb" is the block that stood at its call site,
moved here: one `store.connection()`, the same statements, the same order.
Postgres is not asked anything, not even for a pool.

UNDER `postgres`

- **Writes**: Postgres commits the run first; then DuckDB is handed the same
  rows (`core.shadow_writes.into_duckdb`), whose failure is counted and never
  raised. A Postgres failure raises and DuckDB is not written, so DuckDB ⊆
  Postgres holds by construction.
- **Reads** go to Postgres, with no fallback to DuckDB (the chain module's
  docstring says why). A Postgres error raises into each caller's own
  handling: the digest job fails and logs, the health endpoint returns its
  error, the catch-up logs "skipped", and `/api/health`'s `data_quality`
  becomes null — which the canary pages as `dq_block_missing`, honestly.
"""
from __future__ import annotations

import logging
from datetime import date, datetime
from typing import Any, Dict, List, Optional, Sequence, Tuple

logger = logging.getLogger(__name__)

# The beat's key in either store. `core.scheduler.DQ_DIGEST_LAST_SENT_KEY`
# names the same string; one spelling lives here, beside its two readers.
DIGEST_MARKER_KEY = "dq_digest_last_sent"

# DuckDB's two statements for the beat, as they stood in the digest job.
_DK_MARKER_READ = "SELECT value FROM sync_metadata WHERE key = ?"
_DK_MARKER_WRITE = """
                        INSERT OR REPLACE INTO sync_metadata (key, value, updated_at)
                        VALUES (?, ?, CURRENT_TIMESTAMP)
                    """


def _chain():
    from core import pg_dq_journal_write

    return pg_dq_journal_write


def writes_postgres() -> bool:
    """Where the journal is written. Raises on a flag nobody can read and no
    latch (OD-18 (a)): the run is then journaled nowhere, the page still goes
    out, and `write_chain_flag_invalid` says why."""
    return _chain().writes_postgres()


def reads_postgres() -> bool:
    """Where the journal is read — never raises (`pg_dq_journal_write.reads_postgres`)."""
    return _chain().reads_postgres()


# ─── Writing ──────────────────────────────────────────────────────────────────


async def journal_run(
    store,
    *,
    started_at: datetime,
    ended_at: datetime,
    as_of: datetime,
    window_start: date,
    window_end: date,
    layer: str,
    issues: List[Any],
    discrepancies: List[Any],
    api_calls_used: int = 0,
    error_message: Optional[str] = None,
) -> int:
    """Journal one run; returns its id. The five check jobs' only door."""
    from core.data_quality import insert_journal_rows, persist_run, run_values

    chain = _chain()
    if not chain.writes_postgres():
        # duckdb — the block each site held, moved here unchanged.
        async with store.connection() as conn:
            return persist_run(
                conn,
                started_at=started_at, ended_at=ended_at, as_of=as_of,
                window_start=window_start, window_end=window_end,
                layer=layer, issues=issues, discrepancies=discrepancies,
                api_calls_used=api_calls_used, error_message=error_message,
            )

    from core import shadow_writes

    values = run_values(
        started_at=started_at, ended_at=ended_at, as_of=as_of,
        window_start=window_start, window_end=window_end, layer=layer,
        issues=issues, discrepancies=discrepancies,
        api_calls_used=api_calls_used, error_message=error_message,
    )
    rows = await chain.persist_run(values, issues, discrepancies)
    # After the commit, never before; never inside `store.connection()`.
    await shadow_writes.into_duckdb(
        store, chain.CHAIN, lambda conn: insert_journal_rows(conn, rows))
    return rows.run_id


async def mark_digest_sent(store, value: str) -> None:
    """Move the digest's beat — after a delivered digest only."""
    chain = _chain()
    if not chain.writes_postgres():
        # duckdb — as it stood in the digest job.
        async with store.connection() as conn:
            conn.execute(_DK_MARKER_WRITE, [DIGEST_MARKER_KEY, value])
        return

    from core import shadow_writes

    await chain.set_digest_marker(value)
    await shadow_writes.into_duckdb(
        store, chain.CHAIN,
        lambda conn: conn.execute(_DK_MARKER_WRITE, [DIGEST_MARKER_KEY, value]))


# ─── Reading: one loop, two stores ────────────────────────────────────────────


class _DuckReader:
    """DuckDB's readers behind the async face the loops below call. Each
    method is the sync call that stood at the site, on the held connection."""

    def __init__(self, conn):
        self.conn = conn

    async def latest_run(self, layer):
        from core.data_quality import fetch_latest_run

        return fetch_latest_run(self.conn, layer=layer)

    async def baseline_run(self, layer, *, sent_at, before_run_id):
        from core.data_quality import fetch_baseline_run

        return fetch_baseline_run(self.conn, layer, sent_at=sent_at,
                                  before_run_id=before_run_id)

    async def run_issues(self, run_id, limit):
        from core.data_quality import fetch_run_issues

        return fetch_run_issues(self.conn, run_id, limit=limit)

    async def run_diffs(self, run_id, limit):
        from core.data_quality import fetch_run_diffs

        return fetch_run_diffs(self.conn, run_id, limit=limit)


class _PgReader:
    """The chain module's Postgres readers, on one pooled connection."""

    def __init__(self, conn):
        self.conn = conn

    async def latest_run(self, layer):
        return await _chain().latest_run(self.conn, layer)

    async def baseline_run(self, layer, *, sent_at, before_run_id):
        return await _chain().baseline_run(self.conn, layer, sent_at=sent_at,
                                           before_run_id=before_run_id)

    async def run_issues(self, run_id, limit):
        return await _chain().run_issues(self.conn, run_id, limit)

    async def run_diffs(self, run_id, limit):
        return await _chain().run_diffs(self.conn, run_id, limit)


def _parse_marker(raw: Optional[str]) -> Optional[datetime]:
    if not raw:
        return None
    try:
        return datetime.fromisoformat(raw)
    except ValueError:
        # An unreadable marker must not mute the digest; the next send
        # overwrites it with something parseable.
        logger.warning("Unparseable DQ digest marker: %r", raw)
        return None


async def _sections(reader, layers: Sequence[str], now: datetime,
                    last_sent_at: Optional[datetime]) -> List[Any]:
    """The digest's sections — the loop that stood in the digest job."""
    from core.data_quality import DigestSection

    sections: List[Any] = []
    for layer in layers:
        run = await reader.latest_run(layer)
        if run is None:
            sections.append(DigestSection(layer=layer, run=None))
            continue

        age_hours = None
        if run.get("started_at"):
            started = datetime.fromisoformat(run["started_at"])
            age_hours = (now - started).total_seconds() / 3600

        # "=" must mean "since you last read this", not
        # "since a run six hours ago".
        previous = await reader.baseline_run(
            layer, sent_at=last_sent_at, before_run_id=run["run_id"],
        )
        sections.append(DigestSection(
            layer=layer,
            run=run,
            issues=await reader.run_issues(run["run_id"], 20),
            diffs=await reader.run_diffs(run["run_id"], 20),
            previous_issues=(
                await reader.run_issues(previous["run_id"], 20)
                if previous else []
            ),
            previous_diffs=(
                await reader.run_diffs(previous["run_id"], 20)
                if previous else []
            ),
            age_hours=age_hours,
        ))
    return sections


async def digest_inputs(
    store, layers: Sequence[str], now: datetime,
) -> Tuple[Optional[datetime], List[Any]]:
    """`(last_sent_at, sections)` for the 09:00 digest, from the writer's store.

    Under Postgres the beat is read from `meta.chain_watermarks` and, while
    the key is absent there, **inherited from DuckDB's `sync_metadata`** —
    the beat as of the flip, which the shadow keeps current anyway. Without
    the inherit the first digest after a flip would restate every standing
    finding as new.
    """
    chain = _chain()
    if not chain.reads_postgres():
        # duckdb — the digest job's block, moved: one connection, same order.
        async with store.connection() as conn:
            row = conn.execute(_DK_MARKER_READ, [DIGEST_MARKER_KEY]).fetchone()
            last_sent_at = _parse_marker(row[0] if row else None)
            sections = await _sections(_DuckReader(conn), layers, now, last_sent_at)
        return last_sent_at, sections

    async with chain.reading() as conn:
        raw = await chain.digest_marker(conn)
    if raw is None:
        async with store.connection() as conn:
            row = conn.execute(_DK_MARKER_READ, [DIGEST_MARKER_KEY]).fetchone()
        raw = row[0] if row else None
    last_sent_at = _parse_marker(raw)
    async with chain.reading() as conn:
        sections = await _sections(_PgReader(conn), layers, now, last_sent_at)
    return last_sent_at, sections


async def _health_runs(reader) -> Dict[str, Any]:
    """`/api/health/data-quality`'s body — the endpoint's block, moved."""
    integrity = await reader.latest_run("integrity")
    reconciliation = await reader.latest_run("reconciliation")

    # Include top-N drilldown for the recon run so admins can see
    # WHICH (month, source) drifted without making a second call.
    reconciliation_diffs: List[Any] = []
    if reconciliation:
        reconciliation_diffs = await reader.run_diffs(reconciliation["run_id"], 20)
    integrity_issues: List[Any] = []
    if integrity:
        integrity_issues = await reader.run_issues(integrity["run_id"], 20)

    # The two layers that arrived after this endpoint was written.
    # Found by an audit standing exactly where on-call would stand: a
    # WARN verdict in the mirror-landing log line, and no way to see
    # WHICH findings without opening the database — which the
    # single-writer rule forbids from outside the process. The layer
    # holding the most comparisons must not be the one invisible here.
    mirror_landing = await reader.latest_run("mirror_landing")
    mirror_issues: List[Any] = []
    if mirror_landing:
        mirror_issues = await reader.run_issues(mirror_landing["run_id"], 20)
    reconciliation_pg = await reader.latest_run("reconciliation_pg")
    reconciliation_pg_diffs: List[Any] = []
    if reconciliation_pg:
        reconciliation_pg_diffs = await reader.run_diffs(
            reconciliation_pg["run_id"], 20)

    return {
        "integrity": {
            "last_run": integrity,
            "issues": integrity_issues,
        },
        "reconciliation": {
            "last_run": reconciliation,
            "diffs": reconciliation_diffs,
        },
        "mirror_landing": {
            "last_run": mirror_landing,
            "issues": mirror_issues,
        },
        "reconciliation_pg": {
            "last_run": reconciliation_pg,
            "diffs": reconciliation_pg_diffs,
        },
    }


async def health_runs(store) -> Dict[str, Any]:
    """The latest run per layer with its top findings, from the writer's store."""
    chain = _chain()
    if not chain.reads_postgres():
        async with store.connection() as conn:
            return await _health_runs(_DuckReader(conn))
    async with chain.reading() as conn:
        return await _health_runs(_PgReader(conn))


async def last_success_ages_pg(
    layers: Optional[Sequence[str]] = None,
) -> Dict[str, Dict[str, Any]]:
    """The layer ages out of Postgres — for the callers that read DuckDB's
    inside a connection they already hold (`get_stats`, the catch-up) and
    replace them with these while the chain reads Postgres."""
    from core.data_quality import WATCHED_LAYERS

    chain = _chain()
    layers = tuple(layers or WATCHED_LAYERS)
    async with chain.reading() as conn:
        return await chain.last_success_ages(conn, layers)
