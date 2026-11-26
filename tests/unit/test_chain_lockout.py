"""Every chain's tables are known to the revision that locks out the images
built before it — held as a frozen literal since step 14.

#280 registered eight chains in a build that still required 0034, so on a 0034
database setting any of their flags latched the chain at once, every older
image still admitted (batch-E lock review, 2026-10-09). So every chain
registered from #280 on named its lock-out (`LOCKOUT_REVISION`), and its flag
moved its writes only in a build that required that revision or a later one
(`core.chain_latch.lockout_unmet`, read off the four digits every revision id
starts with). Step 14's S14-00 made every chain write Postgres, a constant;
S14-09 deleted chains 3's and 6's constants and S14-11 the rest with
`lockout_unmet` and the flags. What stays:

- the revision each chain's lock-out was, frozen in `LOCKOUT` below;
- its writers ask `require_revision()` before the latch, so the database is at
  that revision too when the chain first writes Postgres;
- the revision really does know the chain: as of it, every table the chain
  owns carries a comment naming the chain's module.

That last test is the one a later change to a chain fails — a table added, a
module renamed — and it says what to do: a NEW revision, never an edit of an
applied one. The revision files themselves are held to their frozen text by
their own tests (`test_batch_e_chains_revision.py`, `test_buyer_chain_revision.py`).
"""
from __future__ import annotations

import ast
import inspect
import textwrap

import pytest

from core import write_chains
from tests import migration_replay as replay

# The chains registered before this rule existed. Each was registered before
# revision 0034 shipped, so an image without it requires 0033 or less and is
# refused by 0034, which every build carrying this rule requires or descends
# from (held below): their lock-out is in force by history, and nothing about
# them waits at runtime. Frozen — a chain registered from now on is not added
# here; it names its `LOCKOUT_REVISION`.
REGISTERED_BEFORE_THE_RULE = frozenset({
    "pg_inventory_write", "pg_expenses_write", "pg_goals_write",
    "pg_expense_types_write", "pg_buyers_write",
})


def _chains():
    return {write_chains.chain_name(chain): chain for chain in write_chains.WRITE_CHAINS}


def _ruled():
    return sorted(name for name in _chains() if name not in REGISTERED_BEFORE_THE_RULE)


def _before(revision: str) -> str:
    """The revision a build required just before `revision` shipped — what
    #280's build required of the eight chains' lock-out."""
    return replay.revisions()[revision].down_revision


class TestTheWritersAskTheRevisionBeforeTheLatch:
    @pytest.mark.parametrize("name", _ruled())
    def test_every_latch_follows_a_revision_check(self, name):
        """The other half of "a flip waits for the lock-out": the build
        requires it (above), and the database is at it, because every write
        that latches has passed `require_revision()` first. Kills: a writer
        that takes the latch on a connection got any other way."""
        module = _chains()[name]
        tree = ast.parse(textwrap.dedent(inspect.getsource(module)))
        funcs = {node.name: node for node in ast.walk(tree)
                 if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))}

        def calls(node, callee):
            return [n for n in ast.walk(node) if isinstance(n, ast.Call)
                    and getattr(n.func, "id", getattr(n.func, "attr", None)) == callee]

        assert calls(funcs["_pool"], "require_revision"), f"{name}._pool"
        latching = [f for f in funcs.values() if f.name != "_latch" and calls(f, "_latch")]
        assert latching, f"{name} has no writer that latches"
        for func in latching:
            first_latch = min(c.lineno for c in calls(func, "_latch"))
            pools = [c.lineno for c in calls(func, "_pool")]
            assert pools and min(pools) < first_latch, f"{name}.{func.name}"
