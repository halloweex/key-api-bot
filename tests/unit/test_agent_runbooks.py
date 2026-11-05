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
  it (the compaction, `DUCKDB_MEMORY_LIMIT` as a knob, DuckDB's FATAL block).
"""
from __future__ import annotations

import re
import shutil
import subprocess
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[2]
SCRIPT = REPO / "deploy" / "agent" / "run_agent.sh"
RUNBOOKS = sorted((REPO / "deploy" / "agent" / "runbooks").glob("*.md"))

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


def test_no_routed_prefix_is_a_key_that_cannot_fire():
    """A case arm for a key nothing emits is a runbook nobody reads. The
    DuckDB tick's `warehouse:` and the DuckDB switch's `duckdb_` are not
    registered since step 14. Mutation: register a `duckdb_` key again, or
    route `warehouse:*` again."""
    from core.alerting import REGISTRY

    assert not [k for k in REGISTRY if k.startswith(("warehouse:", "duckdb_"))]
    body = subprocess.run(["sed", "-n", "/^family_for()/,/^}/p", str(SCRIPT)],
                          capture_output=True, text=True, check=True).stdout
    code = "\n".join(line.split("#", 1)[0] for line in body.splitlines())
    assert not re.search(r"\bwarehouse:\*|\bduckdb_\*", code), code


# What a runbook may no longer say: DuckDB as web's live store, or a lever
# that went with it. Each is a phrase one of them carried until S14-20.
STALE = (
    ("ЗАЛОЧЕН", "the DuckDB file is no longer locked by web"),
    ("DuckDB + Postgres", "the platform is Postgres + ClickHouse"),
    ("weekly_compact", "the Sunday compaction is deleted (S14-18)"),
    ("недельный компакт", "the Sunday compaction is deleted (S14-18)"),
    ("duckdb_fatal", "/api/health has no DuckDB FATAL block (S14-17)"),
    ("блоки duckdb", "/api/health has no duckdb block (S14-17)"),
    ("лежат в DuckDB", "the DQ journal is written in Postgres (S14-10)"),
    ("часовой копии журнала", "nothing copies the DQ journal (S14-13)"),
    ("поднять лимит или DUCKDB_MEMORY_LIMIT", "a step-14 web loads no DuckDB"),
)


@pytest.mark.parametrize("path", RUNBOOKS, ids=lambda p: p.name)
def test_no_runbook_describes_duckdb_as_live(path):
    """Mutation: put any of the phrases above back into a runbook."""
    text = path.read_text(encoding="utf-8")
    found = [(phrase, why) for phrase, why in STALE if phrase in text]
    assert not found, (path.name, found)


def test_the_common_part_says_the_file_is_an_archive():
    text = (REPO / "deploy" / "agent" / "runbooks" / "_common.md").read_text(encoding="utf-8")
    assert "замороженный архив" in text and "duckdb_silence_check.sh" in text
