"""Who may open the dashboard, and which store is asked.

WHAT THIS IS PROTECTING

`_resolve_session` re-reads status and role on **every** request, and that read
is what revocation actually is here: the session cookie is a stateless signed
token that cannot be withdrawn, so taking access away means the next request
finds the account no longer approved. Until 2026-09-07 the read went to DuckDB,
which meant every authenticated request to every tab acquired the process-wide
store lock — the same lock a warehouse rebuild holds for seconds at a time.

Moving it is therefore worth doing and dangerous to get wrong, and the two
failure directions are not symmetric: reading the wrong store could lock
everyone out (loud, recoverable) or hand somebody access they no longer have
(silent, not). The tests below lean on the second.

WHY THE OBVIOUS SHORTCUT WAS NOT TAKEN

`app.authorized_users` already exists in Postgres and holds users. It is the
*bot's* list, it has no `role` column, and on the day of the move it disagreed
with the dashboard's list about **twelve of sixteen** approved people —
`revoke_user` in the bot store is implemented as `deny`, and taking somebody's
bot access away was never a statement about their dashboard access. Pointing
the session at it would have locked twelve people out, one of them an owner who
survives only because their id is hardcoded. Hence a second table, and hence
`test_the_two_lists_are_not_confused_for_each_other`.
"""
from __future__ import annotations

import ast
import inspect
import pathlib
import re
from pathlib import Path

import pytest

from core import pg_dashboard_users as duf
from core.duckdb_store import DuckDBStore

REPO = Path(__file__).resolve().parents[2]
MIGRATION = REPO / "migrations" / "versions" / "0016_dashboard_users.py"
REPOSITORY = REPO / "core" / "repositories" / "users.py"


# ─── the switch ──────────────────────────────────────────────────────────────


class TestTheSwitch:
    def test_unset_means_duckdb(self, monkeypatch):
        monkeypatch.delenv("KS_USER_STORE", raising=False)
        assert duf.user_store_is_postgres() is False

    @pytest.mark.parametrize("value,expected", (("duckdb", False), ("postgres", True)))
    def test_it_says_what_it_is_told(self, monkeypatch, value, expected):
        monkeypatch.setenv("KS_USER_STORE", value)
        assert duf.user_store_is_postgres() is expected

    def test_a_typo_stops_the_read(self, monkeypatch):
        """`KS_BOT_STORE`'s rule, and it bites hardest here: the variable
        decides where *authorisation* is read from, so a typo must stop the
        container rather than quietly pick the copy that is going stale."""
        monkeypatch.setenv("KS_USER_STORE", "postgress")
        with pytest.raises(ValueError, match="KS_USER_STORE"):
            duf.user_store_is_postgres()

    def test_an_empty_value_is_the_default(self, monkeypatch):
        monkeypatch.setenv("KS_USER_STORE", "   ")
        assert duf.user_store_is_postgres() is False


# ─── the copy, and when it must not run ──────────────────────────────────────


class TestTheReplicationStandsDown:
    @pytest.mark.asyncio
    async def test_it_refuses_once_postgres_is_the_writer(self, monkeypatch):
        """The single worst outcome available: an hourly full replace out of a
        frozen DuckDB would roll back every approval and role change made
        since the switch, once an hour, looking healthy in between."""
        monkeypatch.setenv("KS_USER_STORE", "postgres")
        from unittest.mock import patch

        with patch("core.mirror_reconciliation.configured", return_value=True):
            out = await duf.replicate_dashboard_users(object())
        assert "skipped" in out and "no longer the writer" in out["skipped"]

    @pytest.mark.asyncio
    async def test_without_a_dsn_it_does_nothing(self, monkeypatch):
        monkeypatch.delenv("KS_USER_STORE", raising=False)
        from unittest.mock import patch

        with patch("core.mirror_reconciliation.configured", return_value=False):
            out = await duf.replicate_dashboard_users(object())
        assert out == {"skipped": "KS_PG_DSN is not set"}

    @pytest.mark.asyncio
    async def test_the_comparison_stands_down_with_it(self, monkeypatch):
        """Otherwise every approval made after the switch reads as a
        discrepancy the check itself created."""
        monkeypatch.setenv("KS_USER_STORE", "postgres")
        from core.mirror_reconciliation import reconcile_dashboard_users

        assert await reconcile_dashboard_users(object()) == []


# ─── the column contract, both ends ──────────────────────────────────────────


def _migration_columns() -> list[str]:
    """Parsed from the DDL. A grep is satisfied by a comment naming a column,
    which is the failure this repository has hit six times.

    The table is 0016's `CREATE TABLE` **plus every `ALTER TABLE ... ADD
    COLUMN` since** — `allowed_features` arrived that way in 0021, and a parser
    that read only the create statement would report a shipped column as
    undeclared and teach whoever hit it to loosen the assertion.
    """
    body = MIGRATION.read_text(encoding="utf-8").split(
        "CREATE TABLE app.dashboard_users (", 1)[1]
    out = []
    for raw in body.splitlines():
        line = raw.split("--")[0].strip()
        if not line:
            continue
        if line.startswith(")"):
            break
        m = re.match(r"^([a-z_]+)\s+(TEXT|BIGINT|INTEGER|TIMESTAMPTZ)", line)
        if m:
            out.append(m.group(1))

    added = re.compile(
        r"ALTER\s+TABLE\s+app\.dashboard_users\s+ADD\s+COLUMN\s+"
        r"(?:IF\s+NOT\s+EXISTS\s+)?([a-z_]+)\s+(TEXT|BIGINT|INTEGER|TIMESTAMPTZ)",
        re.IGNORECASE,
    )
    for path in sorted(MIGRATION.parent.glob("*.py")):
        text = path.read_text(encoding="utf-8")
        # `upgrade()` only: a downgrade drops the column it names.
        if "def upgrade" not in text:
            continue
        upgrade = text.split("def upgrade", 1)[1].split("def downgrade", 1)[0]
        out.extend(m.group(1) for m in added.finditer(upgrade))
    return out


class TestTheColumnsAgree:
    def test_the_migration_declares_everything_shipped(self):
        declared = set(_migration_columns())
        assert declared, "parsed no columns out of the migration"
        missing = set(duf.USER_COLUMNS) - declared
        assert not missing, f"shipped and not declared: {sorted(missing)}"

    @pytest.mark.asyncio
    async def test_duckdb_has_everything_read(self, tmp_path):
        store = DuckDBStore(db_path=tmp_path / "u.duckdb")
        await store.connect()
        try:
            async with store.connection() as conn:
                have = {r[0] for r in conn.execute("DESCRIBE users").fetchall()}
        finally:
            await store.close()
        missing = set(duf.USER_COLUMNS) - have
        assert not missing, f"read from DuckDB and not there: {sorted(missing)}"

    def test_bookkeeping_is_not_shared(self):
        """Two correct copies differ on when each of them wrote the row."""
        assert "mirrored_at" not in duf.USER_COLUMNS
        assert "mirrored_at" in MIGRATION.read_text(encoding="utf-8")

    def test_the_role_column_is_the_reason_this_table_exists(self):
        """`app.authorized_users` has never had one, which is why the session's
        fallback path downgrades to viewer instead of trusting the cookie."""
        assert "role" in duf.USER_COLUMNS
        bot_migration = (REPO / "migrations" / "versions" / "0009_bot_state.py")
        block = bot_migration.read_text(encoding="utf-8").split(
            "CREATE TABLE app.authorized_users (", 1)[1].split(")")[0]
        assert not re.search(r"^\s*role\s", block, re.M), (
            "the bot's list grew a role — if the two lists are being merged, "
            "this table and its switch should be retired, not left beside it"
        )


# ─── the routed bodies ───────────────────────────────────────────────────────


class TestOneBodyTwoEngines:
    def _statements(self) -> list[tuple[int, str]]:
        tree = ast.parse(REPOSITORY.read_text(encoding="utf-8"))
        out = []
        for node in ast.walk(tree):
            if not (isinstance(node, ast.Call)
                    and isinstance(node.func, ast.Attribute)
                    and node.func.attr == "_users_run"):
                continue
            for lit in ast.walk(node):
                if (isinstance(lit, ast.Constant) and isinstance(lit.value, str)
                        and "{users}" in lit.value):
                    out.append((lit.lineno, lit.value))
        return out

    def test_the_scan_finds_them(self):
        assert len(self._statements()) >= 8, "the routed statements are not being found"

    def test_both_renderings_leave_no_hole(self):
        for lineno, sql in self._statements():
            for users, own in (("users", "users"),
                               ("app.dashboard_users", "dashboard_users")):
                rendered = sql.format(users=users, self=own)
                assert "{" not in rendered and "}" not in rendered, (
                    f"line {lineno} still has a hole after rendering"
                )

    def test_the_conflict_target_is_never_schema_qualified(self):
        """PostgreSQL rejects `app.dashboard_users.username` inside
        `ON CONFLICT DO UPDATE`; the existing row is reached by the bare
        relation name. That is the whole reason `{self}` exists."""
        for lineno, sql in self._statements():
            if "ON CONFLICT" not in sql.upper():
                continue
            after = sql.upper().split("DO UPDATE", 1)[1]
            original = sql[len(sql) - len(after):]
            assert "{users}." not in original, (
                f"line {lineno} reaches the stored row through the qualified "
                f"name, which PostgreSQL will not accept"
            )
            assert "{self}." in original

    def test_every_ordered_read_breaks_its_ties(self):
        """Two engines order ties differently, and both of these reads are
        rendered as a list — one of them under LIMIT/OFFSET, where a tie means
        different *people* on the same page rather than a different order."""
        for lineno, sql in self._statements():
            upper = sql.upper()
            if "ORDER BY" not in upper:
                continue
            tail = re.split(r"LIMIT", sql[upper.index("ORDER BY") + 8:], flags=re.I)[0]
            last = [c for c in tail.split(",") if c.strip()][-1]
            assert "user_id" in last, f"line {lineno} can tie: {tail.strip()!r}"


class TestNothingGoesRoundTheRouter:
    def test_no_user_method_opens_the_store_itself(self):
        """A method left on `self.connection()` would keep working on DuckDB
        and silently ignore the switch — the half-migrated state that makes an
        authorisation bug invisible."""
        source = REPOSITORY.read_text(encoding="utf-8")
        tree = ast.parse(source)
        allowed = {"_users_run", "get_role_permissions", "get_all_permissions",
                   "set_permission", "seed_default_permissions"}
        offenders = []
        for node in ast.walk(tree):
            if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                continue
            if node.name in allowed:
                continue
            body = ast.get_source_segment(source, node) or ""
            if "self.connection()" in body:
                offenders.append(node.name)
        assert not offenders, (
            f"{offenders} open DuckDB directly — the permissions matrix is the "
            f"only part of this file that has not moved, and it is cached "
            f"per role rather than read per request"
        )

    def test_denial_is_one_statement(self):
        """It was a SELECT and then an UPDATE, safe only because DuckDB's lock
        made them one acquisition. On a pool, two admins refusing the same
        person would read the same count and lose a refusal on the way to the
        freeze."""
        source = REPOSITORY.read_text(encoding="utf-8")
        tree = ast.parse(source)
        deny = next(n for n in ast.walk(tree)
                    if isinstance(n, ast.AsyncFunctionDef) and n.name == "deny_user")
        calls = [n for n in ast.walk(deny)
                 if isinstance(n, ast.Call) and isinstance(n.func, ast.Attribute)
                 and n.func.attr == "_users_run"]
        assert len(calls) == 1, f"{len(calls)} statements, so the count can be lost"


def _code_only(path) -> str:
    """The module with its comments and docstrings removed — and nothing else.

    A plain grep over the source is satisfied by prose: a docstring that
    *explains* which table the session reads fails an assertion about the
    session *reading* it. That is `_migration_columns`' lesson one file over,
    and it went green-to-red for exactly that reason once — a cache added to
    `get_current_user` named `app.dashboard_users` in a sentence about why the
    read is expensive, with no change to the code at all.

    **Ordinary string literals are kept, deliberately.** A table name reaches
    the database inside one, so dropping every STRING would blind the check to
    the thing it exists to catch — a SQL statement written here instead of in
    the repository. Docstrings are found through `ast`, which knows which
    string expressions are documentation, and subtracted by position.
    """
    import io
    import tokenize

    source = pathlib.Path(path).read_text(encoding="utf-8")
    tree = ast.parse(source)

    docstrings = set()
    for node in ast.walk(tree):
        if not isinstance(
            node, (ast.Module, ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef),
        ):
            continue
        body = getattr(node, "body", None)
        if not body:
            continue
        first = body[0]
        if isinstance(first, ast.Expr) and isinstance(first.value, ast.Constant) \
                and isinstance(first.value.value, str):
            docstrings.add((first.value.lineno, first.value.col_offset))

    kept = []
    for token in tokenize.generate_tokens(io.StringIO(source).readline):
        if token.type == tokenize.COMMENT:
            continue
        if token.type == tokenize.STRING and token.start in docstrings:
            continue
        kept.append(token.string)
    return " ".join(kept)


class TestTheTwoListsAreNotConfusedForEachOther:
    def test_the_session_does_not_read_the_bots_list(self):
        """Twelve of sixteen approved dashboard users were `denied` in
        `app.authorized_users` on the day this moved. Reading it here would
        have locked them out, and the one it would not lock out is an owner
        whose id is hardcoded."""
        code = _code_only(REPO / "web" / "routes" / "auth.py")
        assert "authorized_users" not in code
        assert "dashboard_users" not in code, (
            "the session should reach the list through the store, not name a "
            "table — the switch is what chooses the engine"
        )

    def test_the_replicator_copies_the_dashboards_list(self):
        body = inspect.getsource(duf.replicate_dashboard_users)
        assert "FROM users" in body
        assert "authorized_users" not in body
