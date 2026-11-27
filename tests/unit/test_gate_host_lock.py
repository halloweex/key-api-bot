"""Every gate that owns throwaway stores takes the one host lock first.

The gates name their stores (`gate-pg`, `quick-pg`, …) and build the same image
tag, so two runs at once migrate one database, test each other's image, and each
one's cleanup removes the other's stores. On 2026-09-24 two sessions gated in
parallel: one died at the migration on a duplicate `alembic_version`, and its
cleanup took the other's Postgres with it.

The guard walks `deploy/` for any script that removes named stores, rather than
naming the two that exist today — a third gate written tomorrow is exactly the
one nobody would think to add to a list. The step-13 rehearsal was that third
script: its Postgres was `reh-pg`, held in `$REH_PG`, which the first form of
this walk (`docker rm -f -v "$PG"`, the gates' spelling) could not see. Step
14's S14-03 deleted the rehearsal; the walk stays as it was.

Every gate it finds is then run, under a `docker` and a `flock` that record
what they are asked (`tests/helpers/ci_bot_scheduler_tools.py`), and judged
by its calls.
"""
from __future__ import annotations

import re
from pathlib import Path

from tests.helpers.ci_bot_scheduler_tools import (
    FakeTools, absolute_calls, docker_run, pip_installs,
)

ROOT = Path(__file__).resolve().parents[2]


def _gates():
    """Every script that removes a Postgres store of its own: a variable
    assigned a literal `*-pg` name and handed to `docker rm -f -v`."""
    for path in sorted((ROOT / "deploy").glob("*.sh")):
        text = path.read_text(encoding="utf-8")
        stores = set(re.findall(r"^([A-Z_]+)=[\w.-]+-pg$", text, re.M))
        removed = set()
        for args in re.findall(r"docker rm -f -v ((?:\"\$[A-Z_]+\"\s*)+)", text):
            removed |= set(re.findall(r"\$([A-Z_]+)", args))
        if stores & removed:
            yield path, text


def _run(path: Path, root: Path, checkout: Path):
    """One run of a gate from `checkout`, docker and flock faked; its calls."""
    assert absolute_calls(path.read_text(encoding="utf-8"), ("docker", "flock")) == [], (
        f"{path.name} names docker or flock by a path the fakes cannot stand in for")
    tools = FakeTools(root, ("docker", "flock"))
    checkout.mkdir(parents=True, exist_ok=True)
    # quick_gate.sh needs the files it runs; the others ignore an argument.
    result = tools.run(path, "tests/unit/test_example.py", env={"REPO": str(checkout)})
    assert result.returncode == 0, f"{path.name}: {result.stdout}{result.stderr}"
    return tools.calls()


def test_the_walk_finds_both_gates():
    names = {path.name for path, _ in _gates()}
    assert {"gate_with_stores.sh", "quick_gate.sh"} <= names


def test_every_gate_waits_for_the_one_host_lock_before_it_touches_docker(tmp_path):
    """Before its cleanup removes a store, before it builds the shared tag,
    before anything: the gate waits (`flock` without `-n`) on a descriptor
    holding one file — the same file for every gate and every checkout, or
    two runs would not exclude each other."""
    held = set()
    for path, _ in _gates():
        for checkout in ("checkout-a", "checkout-b"):
            calls = _run(path, tmp_path / f"{path.stem}-{checkout}", tmp_path / checkout)
            names = [c.tool for c in calls]
            assert "flock" in names, f"{path.name}: never waits on the host lock"
            lock = calls[names.index("flock")]
            assert "docker" not in names[:names.index("flock")], (
                f"{path.name}: docker is called before the lock, so this run can "
                f"remove or rebuild what another run is using")
            assert lock.notes["lock"] is not None, (
                f"{path.name}: flock {lock.argv} holds no file: {lock.notes['lock_error']}")
            assert lock.notes["nonblocking"] is False, (
                f"{path.name}: flock {lock.argv} gives up instead of waiting")
            held.add(tuple(lock.notes["lock"]))
    assert len(held) == 1, "the gates do not all wait on one file"


def test_every_gate_installs_the_migration_tools_at_the_locks_versions(tmp_path):
    """An unpinned `pip install alembic` pulled a SQLAlchemy that no longer
    brings greenlet, and on 2026-09-25 every gate died at the migration with
    the suite never started. Each gate's install of alembic, in the container
    it starts, is constrained by the checkout's requirements-dev.lock as
    mounted there, and asks for SQLAlchemy with the extra that carries
    greenlet."""
    for path, _ in _gates():
        checkout = tmp_path / "checkout"
        calls = _run(path, tmp_path / path.stem, checkout)
        runs = [docker_run(c.argv) for c in calls if c.tool == "docker" and c.argv[0] == "run"]
        installs = [(run, pip) for run in runs for pip in pip_installs(run.shell() or "")
                    if "alembic" in pip.requirements]
        # The rehearsal migrated with the migrate image instead, on a network
        # with no route out; it went in S14-03, so every gate left installs.
        assert installs, f"{path.name}: migrates without installing alembic itself"
        for run, pip in installs:
            pinned = run.target_of(checkout / "requirements-dev.lock")
            assert pinned is not None and pinned in pip.constraints, (
                f"{path.name}: alembic is installed with constraints {pip.constraints}, "
                f"not the checkout's lock (mounted at {pinned})")
            assert "sqlalchemy" in pip.requirements, f"{path.name}: {pip.requirements}"
            assert "asyncio" in pip.requirements["sqlalchemy"].extras, (
                f"{path.name}: SQLAlchemy without the asyncio extra brings no greenlet")
