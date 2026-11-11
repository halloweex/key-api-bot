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

from core import pg, write_chains
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

# Each chain registered from #280 on and the revision that locks out the
# images built before it — what its `LOCKOUT_REVISION` said until step 14
# deleted the constants (S14-09 chains 3 and 6, S14-11 the six left).
# Frozen: a table added to one of these chains needs a new revision, which
# a later chain of its own would name here.
LOCKOUT = {
    "pg_orders_write": "0035_batch_e_chains",
    "pg_catalogue_write": "0035_batch_e_chains",
    "pg_managers_write": "0035_batch_e_chains",
    "pg_forecast_write": "0035_batch_e_chains",
    "pg_dq_journal_write": "0035_batch_e_chains",
    "pg_watchdog_write": "0035_batch_e_chains",
    "pg_weekly_ledger_write": "0035_batch_e_chains",
    "pg_traffic_ledger_write": "0035_batch_e_chains",
}


NEW_REVISION = (
    "Never edit an applied revision to make this pass: alembic does not re-run "
    "it, so production keeps the old comment while CI's freshly migrated "
    "database and the replay agree with the edit. Write a new revision that "
    "comments the chain's tables (revision 0035's shape), move "
    "REQUIRED_REVISION to it, and point the chain's entry in LOCKOUT at it."
)


def _chains():
    return {write_chains.chain_name(chain): chain for chain in write_chains.WRITE_CHAINS}


def _ruled():
    return sorted(name for name in _chains() if name not in REGISTERED_BEFORE_THE_RULE)


def _revision_number(revision: str) -> int:
    """The four digits every revision id starts with — what
    `chain_latch._revision_number` read until S14-11."""
    head = revision.split("_", 1)[0]
    assert len(head) == 4 and head.isdigit(), revision
    return int(head)


def _lineage_ids(head: str):
    return [module.revision for module in replay.lineage(head)]


def _before(revision: str) -> str:
    """The revision a build required just before `revision` shipped — what
    #280's build required of the eight chains' lock-out."""
    return replay.revisions()[revision].down_revision


class TestEveryChainNamesItsLockout:
    def test_the_chains_before_the_rule_stay_held_by_0034(self):
        """Kills: the head moved onto a line that no longer descends from
        0034, which is what refuses every image without the five."""
        assert REGISTERED_BEFORE_THE_RULE <= set(_chains())
        assert "0034_buyer_chain" in _lineage_ids(pg.REQUIRED_REVISION)

    def test_every_chain_registered_since_has_one(self):
        """Kills: a chain registered with no lock-out — an image built before
        it would start against its tables. Name the revision that refuses
        them in `LOCKOUT`. And no chain carries the constant any more: the
        literal is the one place it is said."""
        assert set(_ruled()) == set(LOCKOUT), sorted(set(_ruled()) ^ set(LOCKOUT))
        for name in _chains():
            assert not hasattr(_chains()[name], "LOCKOUT_REVISION"), (
                f"{name}'s lock-out is the literal here since S14-11")

    @pytest.mark.parametrize("name", _ruled())
    def test_a_lockout_in_force_is_a_revision_that_knows_the_chain(self, name):
        """As of the lock-out every table the chain owns carries a comment
        naming the chain's module: the revision was written knowing what the
        chain owns. Kills: a lock-out named by a revision older than the
        chain, and a table the chain gained after its lock-out shipped — an
        image between the two does not know the chain owns it."""
        chain = _chains()[name]
        lockout = LOCKOUT[name]
        assert _revision_number(lockout) <= _revision_number(pg.REQUIRED_REVISION)
        assert lockout in _lineage_ids(pg.REQUIRED_REVISION), lockout
        comments = replay.comments_at(lockout)
        for table in chain.CHAIN_TABLES:
            assert f"core/{name}.py" in (comments.get(table) or ""), (
                f"as of {lockout}, {table}'s comment does not name "
                f"core/{name}.py: the revision that locks the older images out "
                f"was written before {name} owned it. {NEW_REVISION}")

    def test_revision_numbers_are_the_ancestry(self):
        """"This revision or a later one" is read off the four digits every
        id starts with (the lock-out above, and every image before step 14,
        whose `lockout_unmet` read it so). Kills: a branch, a reused number,
        or an id whose number disagrees with its file."""
        mods = replay.revisions()
        for revision, module in mods.items():
            number = _revision_number(revision)
            assert module.__file__.rsplit("/", 1)[-1].startswith(f"{number:04d}_"), revision
        line = _lineage_ids(pg.REQUIRED_REVISION)
        assert set(line) == set(mods), "a revision off the line to the head"
        numbers = [_revision_number(r) for r in line]
        assert numbers == sorted(set(numbers)), numbers


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
