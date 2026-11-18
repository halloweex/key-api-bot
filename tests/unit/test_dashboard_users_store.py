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
import pathlib
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[2]
MIGRATION = REPO / "migrations" / "versions" / "0016_dashboard_users.py"
REPOSITORY = REPO / "core" / "repositories" / "users.py"


# ─── the switch ──────────────────────────────────────────────────────────────


# ─── the copy, and when it must not run ──────────────────────────────────────


# `TestTheReplicationStandsDown` held `replicate_dashboard_users` to nothing
# under the switch and without a DSN, until step 14's S14-13 deleted it.


# ─── the routed bodies ───────────────────────────────────────────────────────


class TestNothingGoesRoundTheRouter:

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


# ─── the matrix followed the list ────────────────────────────────────────────


class TestTheRoleMatrixLeftDuckDB:
    """`KS_USER_STORE` decides where *all* of access lives, not half of it.

    Revision 0016 moved who may open the dashboard and left the matrix saying
    what each role may do behind, cached per role and read out of DuckDB. Per
    user tabs made that untenable: a permission dependency sits on roughly 120
    of the 140 endpoints now, so every cache miss took DuckDB's process-wide
    store lock on an authorisation path — the lock a warehouse rebuild holds
    every two minutes.
    """

    @pytest.mark.asyncio
    async def test_seeding_is_one_statement(self, tmp_path, monkeypatch):
        """It runs on the request path of the first authorization check after
        every restart, and there is a pair per (level, feature).

        A statement per row meant 48 acquisitions of DuckDB's store lock where
        the single-connection version it replaced took one — and under Postgres
        48 × (require_revision + insert). Measured with two concurrent callers
        on a cold table before this was fixed: 93 statements for 48 rows. On
        Postgres, the matrix's only store since step 14's S14-08 (this ran on
        DuckDB until then); skipped without `KS_PG_DSN`.
        """
        from tests.integration import pg_sms_seed

        if not pg_sms_seed.DSN:
            pytest.skip("needs a live PostgreSQL at KS_PG_DSN")
        async with pg_sms_seed.on_postgres(
                monkeypatch, pg_sms_seed.ACCESS_TABLES, what="the access path"):
            store = pg_sms_seed.store(tmp_path, "seed.duckdb")
            calls = []
            original = store._perms_run

            async def counting(sql, params=None, *, mode="all"):
                calls.append(sql)
                return await original(sql, params, mode=mode)

            store._perms_run = counting
            await store.seed_default_permissions()

            inserts = [sql for sql in calls if "INSERT" in sql.upper()]
            assert len(inserts) == 1, f"{len(inserts)} inserts for one seed"

            from core.permissions import ROLE_PERMISSIONS

            matrix = await store.get_all_permissions()
            pairs = sum(len(features) for features in matrix.values())
            expected = sum(len(f) for f in ROLE_PERMISSIONS.values())
            assert pairs == expected, (pairs, expected)

            # And it stays idempotent, which is what lets every read path seed.
            calls.clear()
            await store.seed_default_permissions()
            assert not [sql for sql in calls if "INSERT" in sql.upper()]

    def test_the_migration_and_the_code_agree_on_the_table(self):
        migration = (MIGRATION.parent / "0022_role_permissions.py").read_text(
            encoding="utf-8")
        from core.repositories.users import PERMISSIONS_TABLE

        assert PERMISSIONS_TABLE == "app.role_permissions"
        assert "CREATE TABLE IF NOT EXISTS app.role_permissions" in migration
        for column in ("role", "feature", "can_view", "can_edit", "can_delete",
                       "updated_at", "updated_by"):
            assert column in migration, column
