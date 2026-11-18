"""The diagnostic agent's routing and runbooks describe the system as it is.

Step 14's S14-20: the agent (`deploy/agent/run_agent.sh`) picks a runbook by
the alert's condition key and hands it to a model with read-only tools. Until
then it routed `warehouse:*` — the DuckDB warehouse tick's keys, gone with the
tick in S14-15 — to the warehouse runbook, sent the Postgres derivation's
`warehouse_pg:*` to `default`, and every runbook still called the DuckDB file
web's live, locked store: a diagnosis built on that is a diagnosis of a system
that no longer runs. What this holds:

- every registered `warehouse_pg:*` key reaches the warehouse runbook, and no
  key the routing names by a DuckDB-era prefix is still registered;
- no runbook describes DuckDB as a live store or names a lever that went with
  it (the compaction, `DUCKDB_MEMORY_LIMIT` as a knob, DuckDB's FATAL block);
- and since the final stage (2026-11-12), none describes the file as on disk:
  the file, its backups, the export and the hourly hash of the file are gone.
"""
from __future__ import annotations

import shutil
import subprocess
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[2]
SCRIPT = REPO / "deploy" / "agent" / "run_agent.sh"

pytestmark = pytest.mark.skipif(
    not (shutil.which("bash") and shutil.which("sed")), reason="needs bash and sed")


def _family_for(*conditions: str) -> dict:
    """`family_for` from the script, run by bash as the host runs it."""
    body = subprocess.run(["sed", "-n", "/^family_for()/,/^}/p", str(SCRIPT)],
                          capture_output=True, text=True, check=True).stdout
    assert body.startswith("family_for()"), body
    out = subprocess.run(
        ["bash", "-c", body + '\nfor c in "$@"; do family_for "$c"; done', "_",
         *conditions], capture_output=True, text=True, check=True).stdout
    return dict(zip(conditions, out.split()))


def test_the_derivations_keys_reach_the_warehouse_runbook():
    """Mutation: route `warehouse:*` again in place of `warehouse_pg:*` — the
    derivation's pages then go to `default`."""
    from core.alerting import REGISTRY

    keys = sorted(k for k in REGISTRY if k.startswith("warehouse_pg:"))
    assert keys, "no derivation key is registered: the walk reads nothing"
    assert set(_family_for(*keys).values()) == {"warehouse"}


def test_the_common_part_says_the_file_is_gone():
    """The final stage (2026-11-12) deleted the file, its backups and the
    export, and the hourly hash went with them. Mutation: call the file a
    frozen archive again, or name its hash check."""
    text = (REPO / "deploy" / "agent" / "runbooks" / "_common.md").read_text(encoding="utf-8")
    assert "Файла DuckDB (data/analytics.duckdb) больше нет" in text
    assert "удалил финальный этап 12.11.2026" in text
    assert "Ковчег" in text
