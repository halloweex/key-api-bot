"""An in-memory journal behind chain 9's Postgres functions, for tests whose
subject is a job and not where its journal lives.

Since step 14's S14-10 the data-quality journal is Postgres's alone: every
run goes through `core.dq_journal`'s door into `core.pg_dq_journal_write`.
Tests that ran a check job against a DuckDB store to read its run back
(routing the chain to DuckDB with `tests.conftest.route_chains_to_duckdb`,
which went in step 14's S14-11)
install this instead: the chain module's writers and readers are replaced
with a list of runs, so the door's own code — the fold of repeated keys,
`run_values`, the beat's inheritance, the digest's sections — runs as it does
in production, and only the storage is a list.

The readers keep the statements' semantics, which
`tests/integration/test_dq_journal_semantics_pg.py` proves on Postgres: the
latest run by id, the previous successful run of a layer, the digest's
baseline (`ended_at <= sent_at`, else the previous run), findings worst
first, and the layer ages keyed on success.
"""
from __future__ import annotations

from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Sequence

_RANK = {"CRITICAL": 0, "WARN": 1, "INFO": 2}


class FakeJournal:
    def __init__(self) -> None:
        self.rows: List[Any] = []          # `JournalRows`, in id order
        self.marker: Optional[str] = None  # `meta.chain_watermarks`' beat
        self.copy: Dict[str, str] = {}     # `app.sync_metadata`, for the beat
        self.refuse: Optional[BaseException] = None

    # ── what a test reads back ──

    def run(self, run_id: int) -> Optional[Dict[str, Any]]:
        from core import pg_dq_journal_write as chain
        from core.data_quality import run_dict

        for rows in self.rows:
            if rows.run_id == run_id:
                return run_dict(chain._local(rows.run))
        return None

    def issues(self, run_id: int, limit: int = 100) -> List[Dict[str, Any]]:
        from core.data_quality import issue_dicts

        found = [r[1:] for rows in self.rows if rows.run_id == run_id for r in rows.issues]
        found.sort(key=lambda r: (_RANK.get(r[2], 3), r[0]))
        return issue_dicts(found[:limit])

    def diffs(self, run_id: int, limit: int = 100) -> List[Dict[str, Any]]:
        from core.data_quality import diff_dicts

        found = [r[1:] for rows in self.rows if rows.run_id == run_id for r in rows.diffs]
        found.sort(key=lambda r: (_RANK.get(r[6], 3), r[0], r[1]))
        return diff_dicts(found[:limit])

    def layers(self) -> List[str]:
        return [rows.run[6] for rows in self.rows]

    # ── chain 9's functions ──

    def _check(self) -> None:
        if self.refuse is not None:
            raise self.refuse

    async def persist_run(self, values, issues: Sequence[Any], discrepancies: Sequence[Any]):
        from core.data_quality import journal_rows

        journal_rows(0, values, issues, discrepancies)   # refuses a naive stamp
        self._check()
        rows = journal_rows(len(self.rows) + 1, values, issues, discrepancies)
        self.rows.append(rows)
        return rows

    async def set_digest_marker(self, value: str) -> None:
        self._check()
        self.marker = value

    @asynccontextmanager
    async def reading(self):
        self._check()
        yield self

    async def digest_marker(self, conn) -> Optional[str]:
        return self.marker

    def _of(self, layer: Optional[str]):
        return [rows for rows in self.rows if layer is None or rows.run[6] == layer]

    async def latest_run(self, conn, layer: Optional[str] = None):
        found = self._of(layer)
        return self.run(found[-1].run_id) if found else None

    async def run_by_id(self, conn, run_id: int):
        return self.run(int(run_id))

    def _succeeded(self, rows) -> bool:
        # `error_message IS NULL AND status <> 'FAILED'`
        return rows.run[14] is None and rows.run[7] != "FAILED"

    async def previous_run(self, conn, layer: str, before_run_id: int):
        found = [rows for rows in self._of(layer)
                 if rows.run_id < before_run_id and self._succeeded(rows)]
        return self.run(found[-1].run_id) if found else None

    async def baseline_run(self, conn, layer: str, *, sent_at, before_run_id: int):
        if sent_at is not None:
            found = [rows for rows in self._of(layer)
                     if rows.run_id < before_run_id and self._succeeded(rows)
                     and rows.run[2] is not None and rows.run[2] <= sent_at]
            if found:
                return self.run(found[-1].run_id)
        return await self.previous_run(conn, layer, before_run_id)

    async def run_issues(self, conn, run_id: int, limit: int = 100):
        return self.issues(run_id, limit)

    async def run_diffs(self, conn, run_id: int, limit: int = 100):
        return self.diffs(run_id, limit)

    async def last_success_ages(self, conn, layers: Sequence[str]):
        from core import pg_dq_journal_write as chain
        from core.data_quality import success_ages

        now = datetime.now(timezone.utc)
        out = []
        for layer in layers:
            stamps = [rows.run[1] for rows in self._of(layer) if self._succeeded(rows)]
            if stamps:
                last = max(stamps)
                out.append(chain._local((layer, last, (now - last).total_seconds())))
        return success_ages(out, layers)

    async def values_on(self, conn, keys):
        return {k: v for k, v in self.copy.items() if k in keys}

    # ── wiring ──

    def install(self, monkeypatch) -> "FakeJournal":
        from core import pg_dq_journal_write as chain

        for name in ("persist_run", "set_digest_marker", "reading", "digest_marker",
                     "latest_run", "run_by_id", "previous_run", "baseline_run",
                     "run_issues", "run_diffs", "last_success_ages"):
            monkeypatch.setattr(chain, name, getattr(self, name))
        monkeypatch.setattr("core.pg_frozen_metadata.values_on", self.values_on)
        return self


def install_journal(monkeypatch) -> FakeJournal:
    """A fresh `FakeJournal`, installed for the test."""
    return FakeJournal().install(monkeypatch)
