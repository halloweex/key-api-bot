"""The Postgres plumbing, and the one drift it can silently develop.

Nothing in this application reads or writes Postgres yet. What these tests
protect is the contract that makes it safe to start: the DSN is never guessed,
the schema version is declared rather than healed, and the revision the code
requires is the revision the migrations actually produce.

That last one is the drift worth a test. `REQUIRED_REVISION` and the head of
`migrations/versions/` are two statements of the same fact in two files. Let
them part and the failure surfaces at runtime, in production, as a refusal to
start — which is the correct behaviour and the worst possible moment to
discover the typo.

The live-server checks are not here. They ran against postgres:17.2-alpine —
the production image — with the real `postgres/initdb/*.sql` and the real
`ks_app` role: the revision applied, was idempotent, downgraded, re-applied,
landed `meta.alembic_version` and `meta.mirror_state` owned by `ks_app`, and
both CHECK constraints rejected what they exist to reject. A test needing a
server does not belong in a suite that must run with no network.
"""
from __future__ import annotations

import re
import subprocess
import sys
from pathlib import Path

import pytest

from core import pg

REPO = Path(__file__).resolve().parents[2]
VERSIONS = REPO / "migrations" / "versions"


class TestTheDsnIsNeverGuessed:
    def test_it_raises_when_unset(self, monkeypatch):
        """Defaulting to localhost is how you migrate the wrong database."""
        monkeypatch.delenv(pg.DSN_ENV, raising=False)
        with pytest.raises(RuntimeError, match="refusing to guess"):
            pg.dsn()

    def test_whitespace_is_not_a_dsn(self, monkeypatch):
        monkeypatch.setenv(pg.DSN_ENV, "   ")
        with pytest.raises(RuntimeError, match="refusing to guess"):
            pg.dsn()

    def test_it_returns_what_was_set(self, monkeypatch):
        monkeypatch.setenv(pg.DSN_ENV, "postgresql://ks_app:x@postgres:5432/ks")
        assert pg.dsn() == "postgresql://ks_app:x@postgres:5432/ks"

    def test_it_is_not_called_DATABASE_URL(self):
        """The sibling bot's only database is Postgres, so `DATABASE_URL` means
        something there. This repository's main store is DuckDB, and an
        unqualified name would be read as that one."""
        assert pg.DSN_ENV == "KS_PG_DSN"


class _Conn:
    """The two answers `current_revision` has to tell apart."""

    def __init__(self, value=None, raises=False):
        self._value, self._raises = value, raises

    async def fetchval(self, _sql):
        if self._raises:
            raise RuntimeError("relation \"meta.alembic_version\" does not exist")
        return self._value


class TestTheVersionGateFailsClosed:
    @pytest.mark.asyncio
    async def test_a_matching_revision_passes(self):
        await pg.require_revision(conn=_Conn(pg.REQUIRED_REVISION))

    @pytest.mark.asyncio
    async def test_an_unmigrated_database_says_so(self):
        """No table and an empty table are the same fact — nobody has
        migrated this database — and the message has to name the fix."""
        for conn in (_Conn(raises=True), _Conn(None)):
            with pytest.raises(pg.SchemaVersionError, match="never been migrated"):
                await pg.require_revision(conn=conn)

    @pytest.mark.asyncio
    async def test_an_older_revision_stops_the_process(self):
        with pytest.raises(pg.SchemaVersionError, match="0000_older"):
            await pg.require_revision(conn=_Conn("0000_older"))

    @pytest.mark.asyncio
    async def test_a_newer_revision_stops_it_too(self):
        """Deliberately not "older only". Alembic revisions are opaque strings
        with no order to compare, and code about to read columns whose meaning
        it does not know is as much a reason to stop."""
        with pytest.raises(pg.SchemaVersionError, match="9999_from_the_future"):
            await pg.require_revision(conn=_Conn("9999_from_the_future"))

    @pytest.mark.asyncio
    async def test_current_revision_reports_none_rather_than_raising(self):
        assert await pg.current_revision(conn=_Conn(raises=True)) is None


def _revisions() -> dict[str, str | None]:
    """{revision: down_revision} parsed from the migration files themselves."""
    found: dict[str, str | None] = {}
    for path in VERSIONS.glob("*.py"):
        src = path.read_text()
        rev = re.search(r'^revision\s*=\s*"([^"]+)"', src, re.M)
        down = re.search(r'^down_revision\s*=\s*(?:"([^"]+)"|None)', src, re.M)
        assert rev, f"{path.name} declares no revision"
        found[rev.group(1)] = down.group(1) if down and down.group(1) else None
    return found


class TestTheCodeAndTheMigrationsAgree:
    def test_there_is_exactly_one_head(self):
        revs = _revisions()
        assert revs, "no migrations found"
        parents = {d for d in revs.values() if d}
        heads = set(revs) - parents
        assert len(heads) == 1, f"expected one head, found {sorted(heads)}"

    def test_the_head_is_what_the_code_requires(self):
        """The drift this file exists for: add a revision, forget to bump
        `REQUIRED_REVISION`, and the application refuses to start in
        production instead of failing here."""
        revs = _revisions()
        head = (set(revs) - {d for d in revs.values() if d}).pop()
        assert head == pg.REQUIRED_REVISION, (
            f"migrations head is {head!r} but core.pg.REQUIRED_REVISION is "
            f"{pg.REQUIRED_REVISION!r} — bump it in the same commit"
        )

    def test_every_revision_is_reachable_from_base(self):
        revs = _revisions()
        seen, cur = set(), (set(revs) - {d for d in revs.values() if d}).pop()
        while cur is not None:
            assert cur in revs, f"{cur!r} is referenced but does not exist"
            assert cur not in seen, f"cycle through {cur!r}"
            seen.add(cur)
            cur = revs[cur]
        assert seen == set(revs), f"orphaned: {sorted(set(revs) - seen)}"


class TestTheRevisionCompiles:
    def test_it_renders_offline_without_a_database(self, monkeypatch, tmp_path):
        """`alembic upgrade head --sql` is how a revision is reviewed before
        any database exists. It is also the only check here that runs the real
        Alembic machinery, so it catches a broken env.py as well.
        """
        alembic = pytest.importorskip(
            "alembic", reason="dev dependency; runtime image does not carry it"
        )
        del alembic

        out = subprocess.run(
            [sys.executable, "-m", "alembic", "upgrade", "head", "--sql"],
            cwd=REPO, capture_output=True, text=True, timeout=120,
            env={"PATH": "/usr/bin:/bin", "HOME": str(tmp_path)},
        )
        assert out.returncode == 0, out.stderr[-2000:]
        sql = out.stdout

        assert "CREATE TABLE meta.mirror_state" in sql
        assert "meta.alembic_version" in sql, "bookkeeping must land in meta"
        assert "CREATE SCHEMA" not in sql, (
            "the platform owns schemas, this owns tables — charter rule 11"
        )

    def test_the_rendered_sql_never_ends_inside_a_string_literal(
        self, monkeypatch, tmp_path,
    ):
        """The one syntax error this suite can see without a server.

        Rendering offline proves Alembic runs; it does **not** prove the SQL
        parses. Revision 0009 put "a human's decision" inside a single-quoted
        `COMMENT ON` literal and failed on a real server *after four tables had
        already been created* — every test that had looked at that file parsed
        the DDL as text, and none of them executed it.

        One stray apostrophe makes the number of quote delimiters odd, so the
        file ends inside a literal. A parity check over the whole output is all
        that is, and it costs one pass.

        What it does not catch: two stray apostrophes, which cancel. The real
        answer to that is a Postgres service in CI, which this repository does
        not have — see the handoff.
        """
        pytest.importorskip("alembic", reason="dev dependency")
        out = subprocess.run(
            [sys.executable, "-m", "alembic", "upgrade", "head", "--sql"],
            cwd=REPO, capture_output=True, text=True, timeout=120,
            env={"PATH": "/usr/bin:/bin", "HOME": str(tmp_path)},
        )
        assert out.returncode == 0, out.stderr[-2000:]

        # `--` line comments first: Alembic writes its own, and a revision
        # message may legitimately contain an apostrophe. Then `\'\'`, which is
        # SQL's own escape and not a delimiter.
        body = "\n".join(
            line.split("--", 1)[0] for line in out.stdout.splitlines()
        )
        assert body.replace("''", "").count("'") % 2 == 0, (
            "the rendered migration SQL ends inside a string literal — "
            "an apostrophe somewhere is opening a quote it never closes"
        )
