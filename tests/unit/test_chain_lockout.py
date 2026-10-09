"""A chain's flag moves its writes only in a build that locks out the images
built before the chain — held at runtime, not by the order of an operator's
steps (`core.chain_latch.lockout_unmet`).

#280 registered eight chains in a build that still required 0034, so on a 0034
database setting any of their flags latched the chain at once, every older
image still admitted, and the only thing that kept a flip after the lock-out
was the operator (batch-E lock review, 2026-10-09; reproduced against a real
0034 database: chain 11a latched, owner row written, no revision asked). The
guard before this one (`TestEveryRegisteredChainIsHeld`, beside 0035's own
tests) only checked that every registered module's name was in one of two
hand-kept sets.

Now every chain registered from #280 on names its lock-out
(`LOCKOUT_REVISION`), and:

- its flag moves nothing while this build requires an earlier revision — the
  chain runs as duckdb and `/api/health` names `lockout`;
- its writers ask `require_revision()` before the latch, so the database is at
  that revision too when the chain first writes Postgres;
- the revision it names really does know the chain: as of it, every table the
  chain owns carries a comment naming the chain's module.

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

from core import chain_latch, pg, write_chains
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

NEW_REVISION = (
    "Never edit an applied revision to make this pass: alembic does not re-run "
    "it, so production keeps the old comment while CI's freshly migrated "
    "database and the replay agree with the edit. Write a new revision that "
    "comments the chain's tables (revision 0035's shape), move "
    "REQUIRED_REVISION to it, and point the chain's LOCKOUT_REVISION at it."
)


def _chains():
    return {write_chains.chain_name(chain): chain for chain in write_chains.WRITE_CHAINS}


def _ruled():
    return sorted(name for name in _chains() if name not in REGISTERED_BEFORE_THE_RULE)


def _lineage_ids(head: str):
    return [module.revision for module in replay.lineage(head)]


def _before(revision: str) -> str:
    """The revision a build required just before `revision` shipped — what
    #280's build required of the eight chains' lock-out."""
    return replay.revisions()[revision].down_revision


@pytest.fixture
def flags(monkeypatch):
    """No chain's variable set; the conftest fixture already points the
    marker directory at this test's tmp_path, so nothing is latched."""
    for chain in write_chains.WRITE_CHAINS:
        monkeypatch.delenv(chain.WRITE_ENV, raising=False)
    return monkeypatch


class TestEveryChainNamesItsLockout:
    def test_the_chains_before_the_rule_stay_held_by_0034(self):
        """Kills: the head moved onto a line that no longer descends from
        0034, which is what refuses every image without the five."""
        assert REGISTERED_BEFORE_THE_RULE <= set(_chains())
        assert "0034_buyer_chain" in _lineage_ids(pg.REQUIRED_REVISION)

    def test_every_chain_registered_since_names_one(self):
        """Kills: a chain registered with no lock-out — its flag would latch
        it in the build that registered it, with every older image admitted
        (#280). Name the revision that will refuse them; until a build
        requires it, the flag moves nothing."""
        missing = [name for name in _ruled()
                   if not isinstance(getattr(_chains()[name], "LOCKOUT_REVISION", None), str)]
        assert not missing, (f"registered without a LOCKOUT_REVISION: {missing}")

    @pytest.mark.parametrize("name", _ruled())
    def test_a_lockout_in_force_is_a_revision_that_knows_the_chain(self, name):
        """As of the lock-out every table the chain owns carries a comment
        naming the chain's module: the revision was written knowing what the
        chain owns. Kills: a lock-out named by a revision older than the
        chain, and a table the chain gained after its lock-out shipped — an
        image between the two does not know the chain owns it."""
        chain = _chains()[name]
        lockout = chain.LOCKOUT_REVISION
        if chain_latch.lockout_unmet(lockout) is not None:
            pytest.skip(f"{lockout} is not in this build yet; the flag waits for it")
        assert lockout in _lineage_ids(pg.REQUIRED_REVISION), lockout
        comments = replay.comments_at(lockout)
        for table in chain.CHAIN_TABLES:
            assert f"core/{name}.py" in (comments.get(table) or ""), (
                f"as of {lockout}, {table}'s comment does not name "
                f"core/{name}.py: the revision that locks the older images out "
                f"was written before {name} owned it. {NEW_REVISION}")

    def test_revision_numbers_are_the_ancestry(self):
        """`lockout_unmet` reads "this revision or a later one" off the four
        digits every id starts with, because neither image carries the
        migrations. Kills: a branch, a reused number, or an id whose number
        disagrees with its file — any of which would make the comparison
        answer something other than the ancestry."""
        mods = replay.revisions()
        for revision, module in mods.items():
            number = chain_latch._revision_number(revision)
            assert module.__file__.rsplit("/", 1)[-1].startswith(f"{number:04d}_"), revision
        line = _lineage_ids(pg.REQUIRED_REVISION)
        assert set(line) == set(mods), "a revision off the line to the head"
        numbers = [chain_latch._revision_number(r) for r in line]
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


class TestAFlipWaitsForTheLockout:
    @pytest.mark.parametrize("name", _ruled())
    def test_before_the_lockout_is_required_the_flag_moves_nothing(self, name, flags):
        """#280's build, in this code: the chain registered, its lock-out not
        yet required. Before the fix every one of the eight wrote Postgres
        here, and the first write latched it. Kills: a chain whose routing
        does not ask the lock-out — writer, registry or reader."""
        chain = _chains()[name]
        flags.setattr(pg, "REQUIRED_REVISION", _before(chain.LOCKOUT_REVISION))
        flags.setenv(chain.WRITE_ENV, "postgres")

        assert chain.writes_postgres() is False
        state = write_chains.chain_modes()[name]
        assert state["mode"] == "duckdb", state
        assert "lockout:" in (state["unmet_precondition"] or ""), state
        assert "lockout:" in (chain.unmet_precondition() or "")
        if hasattr(chain, "reads_postgres"):
            assert chain.reads_postgres() is False

    @pytest.mark.parametrize("name", _ruled())
    def test_once_it_is_required_the_lockout_is_no_reason(self, name, flags):
        """The build that requires it — this one, for the eight. Kills: a
        lock-out that holds a chain whose lock is already in force. A shadow
        chain has no other precondition, so it moves."""
        chain = _chains()[name]
        flags.setattr(pg, "REQUIRED_REVISION", chain.LOCKOUT_REVISION)
        flags.setenv(chain.WRITE_ENV, "postgres")

        assert "lockout:" not in (chain.unmet_precondition() or "")
        if write_chains.is_shadow(chain):
            assert chain.writes_postgres() is True
            assert write_chains.chain_modes()[name]["mode"] == "postgres"

    @pytest.mark.parametrize("name", _ruled())
    def test_a_latched_chain_is_not_held_by_it(self, name, flags):
        """OD-19 (a): once a chain has written Postgres the latch outranks
        every precondition — routing it back to DuckDB would start a second
        writer. The reason is still published. Kills: a lock-out that
        outranks the latch."""
        chain = _chains()[name]
        flags.setattr(pg, "REQUIRED_REVISION", _before(chain.LOCKOUT_REVISION))
        flags.setenv(chain.WRITE_ENV, "postgres")
        chain_latch.latch(name, chain.WRITE_ENV)

        assert chain.writes_postgres() is True
        state = write_chains.chain_modes()[name]
        assert state["mode"] == "postgres"
        assert "lockout:" in (state["unmet_precondition"] or ""), state


class TestLockoutUnmet:
    def test_the_answers(self, monkeypatch):
        monkeypatch.setattr(pg, "REQUIRED_REVISION", "0035_batch_e_chains")
        assert chain_latch.lockout_unmet("0035_batch_e_chains") is None
        assert chain_latch.lockout_unmet("0034_buyer_chain") is None
        later = chain_latch.lockout_unmet("0036_something")
        assert later.startswith("lockout: ") and "0035_batch_e_chains" in later
        assert chain_latch.lockout_unmet(None).startswith("lockout: ")
        assert chain_latch.lockout_unmet("").startswith("lockout: ")

    def test_an_id_it_cannot_read_is_unmet_and_never_raises(self, monkeypatch):
        monkeypatch.setattr(pg, "REQUIRED_REVISION", "0035_batch_e_chains")
        assert chain_latch.lockout_unmet("35_short").startswith("lockout: ")
        monkeypatch.setattr(pg, "REQUIRED_REVISION", "head")
        assert chain_latch.lockout_unmet("0035_batch_e_chains").startswith("lockout: ")
