"""Every gate that owns throwaway stores takes the one host lock first.

The gates name their stores (`gate-pg`, `quick-pg`, …) and build the same image
tag, so two runs at once migrate one database, test each other's image, and each
one's cleanup removes the other's stores. On 2026-09-24 two sessions gated in
parallel: one died at the migration on a duplicate `alembic_version`, and its
cleanup took the other's Postgres with it.

The guard walks `deploy/` for any script that removes named stores, rather than
naming the two that exist today — a third gate written tomorrow is exactly the
one nobody would think to add to a list.
"""
from __future__ import annotations

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
LOCK = "exec 9>/tmp/ks-gate.lock"


def _gates():
    for path in sorted((ROOT / "deploy").glob("*.sh")):
        text = path.read_text(encoding="utf-8")
        if re.search(r'docker rm -f -v "\$PG"', text):
            yield path, text


def _first(lines, pattern):
    for number, line in enumerate(lines):
        if re.match(pattern, line.strip()):
            return number
    return None


def test_the_walk_finds_both_gates():
    names = {path.name for path, _ in _gates()}
    assert {"gate_with_stores.sh", "quick_gate.sh"} <= names


def test_every_gate_waits_for_the_lock_before_it_touches_a_store():
    for path, text in _gates():
        lines = text.splitlines()
        lock = _first(lines, re.escape(LOCK) + "$")
        wait = _first(lines, r"flock 9$")
        first_cleanup = _first(lines, r"cleanup$")
        first_run = _first(lines, r"docker run\b")
        assert lock is not None, f"{path.name}: no host lock"
        assert wait is not None and wait > lock, f"{path.name}: the lock is never waited on"
        assert first_cleanup is not None and wait < first_cleanup, (
            f"{path.name}: cleanup runs before the lock, so it can remove another run's stores")
        assert first_run is not None and wait < first_run, (
            f"{path.name}: a store starts before the lock")


def test_every_gate_installs_the_migration_tools_at_the_locks_versions():
    """An unpinned `pip install alembic` pulled a SQLAlchemy that no longer
    brings greenlet, and on 2026-09-25 every gate died at the migration with
    the suite never started. Each gate's migrate step installs alembic under
    the lock's constraints, with the extra that carries greenlet."""
    for path, text in _gates():
        code = [l for l in text.splitlines() if not l.lstrip().startswith("#")]
        installs = [l for l in code if "pip install" in l and "alembic" in l]
        assert installs, f"{path.name}: no alembic install found"
        for line in installs:
            assert "-c /app/requirements-dev.lock" in line, f"{path.name}: {line.strip()}"
            assert "sqlalchemy[asyncio]" in line, f"{path.name}: {line.strip()}"
        assert "requirements-dev.lock:/app/requirements-dev.lock:ro" in text, path.name
