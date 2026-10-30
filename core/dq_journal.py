"""The one door to the data-quality journal: Postgres writes it, Postgres reads it.

Chain 9 (`core/pg_dq_journal_write.py`, OD-02 (c)) moved the journal's writer
to Postgres. Every writer and reader of the journal comes through here, so the
five check jobs, the digest, the health endpoints and the startup catch-up
cannot come to disagree about where the journal lives —
`tests/unit/test_dq_journal.py` walks `core/`, `web/`, `bot/` and `scripts/`
and fails on a writer or reader that goes round this module.

ONE STORE SINCE STEP 14'S S14-10

- **Writes**: Postgres commits the run, its findings and its id in one
  transaction (`pg_dq_journal_write.persist_run`). Until S14-10 DuckDB was
  then handed the same rows as a shadow (`core/shadow_writes.py`), and a
  chain left at `duckdb` wrote DuckDB alone; since S14-00 neither ran in
  web, which runs `KS_DUCKDB=off`, and S14-10 deleted both, with the `store`
  every door here used to take. A Postgres failure raises into each job's
  own handling, as it always did.
- **Reads** go to Postgres, with no fallback (the chain module's docstring
  says why). A Postgres error raises into each caller's own handling: the
  digest job fails and logs, the health endpoint returns its error, the
  catch-up logs "skipped", and `/api/health`'s `data_quality` becomes null —
  which the canary pages as `dq_block_missing`, honestly.
- **The digest's beat** lives in `meta.chain_watermarks`; while the key is
  absent there it is inherited from `app.sync_metadata`, DuckDB's
  `sync_metadata` as the hourly copy last shipped it, whatever `KS_DUCKDB`
  says (S14-10: the inheritance used to read DuckDB's own table under `on`).
"""
from __future__ import annotations

import logging
from datetime import date, datetime
from typing import Any, Dict, List, Optional, Sequence, Tuple

logger = logging.getLogger(__name__)

# The beat's key. `core.scheduler.DQ_DIGEST_LAST_SENT_KEY` names the same
# string; one spelling lives here, beside its two readers.
DIGEST_MARKER_KEY = "dq_digest_last_sent"


def _chain():
    from core import pg_dq_journal_write

    return pg_dq_journal_write


# ─── Writing ──────────────────────────────────────────────────────────────────


def one_finding_per_key(issues: List[Any]) -> List[Any]:
    """The run's findings with at most one per `(check_name, table_name)`.

    `app.data_quality_issues` is keyed on `(run_id, check_name, table_name)`
    and DuckDB's table is not, so two findings under one name in one run
    were refused by Postgres whole under `KS_WRITE_DQ_JOURNAL=postgres` — the
    run landed in neither store — and under `duckdb` DuckDB took them and the
    hourly copy of the journal, which is never pruned, failed every hour from
    then on. A producer that files two is a defect of its own
    (`core.pg_chain_invariants.unwatched_issue` was one); this is the door
    making sure such a defect costs a merged row and an ERROR, not the
    journal. Folded: the most severe severity, the counts summed, the sample
    ids in order without repeats, the descriptions joined. A run with no
    repeated key is returned as the very same list, so the default path
    writes exactly what it wrote before.
    """
    from dataclasses import replace

    at: Dict[Tuple[str, str], int] = {}
    out: List[Any] = []
    folded: List[Tuple[str, str]] = []
    for issue in issues:
        key = (issue.check_name, issue.table_name)
        if key not in at:
            at[key] = len(out)
            out.append(issue)
            continue
        first = out[at[key]]
        worse = (issue.severity if issue.severity.rank() > first.severity.rank()
                 else first.severity)
        text = first.description
        if issue.description and issue.description != first.description:
            text = f"{text}\n{issue.description}" if text else issue.description
        out[at[key]] = replace(
            first, severity=worse, count=first.count + issue.count,
            sample_ids=tuple(dict.fromkeys(
                tuple(first.sample_ids) + tuple(issue.sample_ids))),
            description=text)
        folded.append(key)
    if not folded:
        return issues
    logger.error(
        "data-quality journal: %d finding(s) repeated a (check_name, "
        "table_name) already in the run and were folded into it: %s. The "
        "producer must name them apart.", len(folded),
        ", ".join(f"{c} on {t}" for c, t in dict.fromkeys(folded)))
    return out


async def journal_run(
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
    """Journal one run in Postgres; returns its id. The five check jobs' only door."""
    from core.data_quality import run_values

    issues = one_finding_per_key(issues)
    values = run_values(
        started_at=started_at, ended_at=ended_at, as_of=as_of,
        window_start=window_start, window_end=window_end, layer=layer,
        issues=issues, discrepancies=discrepancies,
        api_calls_used=api_calls_used, error_message=error_message,
    )
    rows = await _chain().persist_run(values, issues, discrepancies)
    return rows.run_id


async def mark_digest_sent(value: str) -> None:
    """Move the digest's beat in Postgres — after a delivered digest only."""
    await _chain().set_digest_marker(value)


# ─── Reading ─────────────────────────────────────────────────────────────────


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
    layers: Sequence[str], now: datetime,
) -> Tuple[Optional[datetime], List[Any]]:
    """`(last_sent_at, sections)` for the 09:00 digest, from Postgres.

    The beat is read from `meta.chain_watermarks` and, while the key is
    absent there, **inherited from `app.sync_metadata`** — DuckDB's
    `sync_metadata` as Postgres last copied it (`core.pg_frozen_metadata`),
    read on the same connection: the beat as of the flip. Without the inherit
    the first digest after a flip would restate every standing finding as
    new. Production has held the key in `meta.chain_watermarks` since chain
    9's first delivered digest (H6, 2026-10-29), so the copy is asked only
    for a key that went missing. Until step 14's S14-10 a process running
    `KS_DUCKDB=on` read DuckDB's own table here instead; web has run `off`
    since S14-00, so production's answer does not move.
    """
    from core.pg_frozen_metadata import values_on

    chain = _chain()
    async with chain.reading() as conn:
        raw = await chain.digest_marker(conn)
        if raw is None:
            raw = (await values_on(conn, (DIGEST_MARKER_KEY,))).get(DIGEST_MARKER_KEY)
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


async def health_runs() -> Dict[str, Any]:
    """The latest run per layer with its top findings, from Postgres."""
    async with _chain().reading() as conn:
        return await _health_runs(_PgReader(conn))


async def last_success_ages_pg(
    layers: Optional[Sequence[str]] = None,
) -> Dict[str, Dict[str, Any]]:
    """The age of the last **successful** run per layer, out of Postgres —
    what `/api/health` publishes for the canary and what the startup catch-up
    reads. Keyed on `error_message IS NULL`, not on a row's existence: a
    failed run writes a row too, so a plain "when did this job last run"
    watchdog stayed silent through all 57 consecutive `429 Too Many
    Attempts` failures of the reconciliation job. One entry per requested
    layer; a layer that never succeeded reads `last_success_at=None,
    age_seconds=None`, so "never ran" is not a missing key. (Until step 14's
    S14-10 DuckDB's twin, `data_quality.fetch_last_success_ages`, answered
    for a process on `KS_DUCKDB=on`; the two callers replaced its answer
    with this one since chain 9 moved.)"""
    from core.data_quality import WATCHED_LAYERS

    chain = _chain()
    layers = tuple(layers or WATCHED_LAYERS)
    async with chain.reading() as conn:
        return await chain.last_success_ages(conn, layers)
