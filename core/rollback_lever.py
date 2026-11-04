"""The lever every page about a chain's ownership names since step 14.

Step 14's S14-04 deleted the way back — `scripts/chain_copy_back.py`, its
handover (`core/chain_transfer.py`) and the lever journal
(`core/lever_journal.py`). Every page, finding, health description and soak
check that named the copy-back as the cure names this instead.

A step-14 image routes by no flag, latch or precondition (S14-00), so what is
wrong is a fact about the one copy in Postgres, and `core.pg_chain_invariants`
is what reads those. The only way back is an image from before step 14, and
that image routes a chain by its marker under `data/write-chain-owners/` —
with the marker lost, by its flag and its preconditions, any unmet one of
which sends its writes to DuckDB. So for a lost marker the rollback is exactly
the dangerous step: it comes after the markers are checked against the owner
rows, never before ("redeploy and look" would be the wrong order).

A module of its own, with nothing imported: the canary must not import the
latch module (`tests/unit/test_chain_latch.py` holds that), and the alert
registry, the remediation table and the latch all read the same two strings.
`tests/unit/test_rollback_lever.py` holds every such text to them, the soak
SQL included, and fails on a lever text that still names the copy-back.
"""
from __future__ import annotations

ROLLBACK_LEVER = ("Investigate with pg_chain_invariants; roll back to "
                  "rollback-step14 only after checking the markers against the "
                  "owner rows")

# Where the checks before a rollback are written down.
ROLLBACK_RUNBOOK = ('.claude/CLAUDE.md, "Before any rollback to an image from '
                    'before step 14"')
