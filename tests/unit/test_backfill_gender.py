"""`scripts/backfill_gender.py` and the preview behind it.

The script's first version advertised `docker exec keycrm-web python
/app/scripts/backfill_gender.py`, and that could never have worked: the web
container held the DuckDB file exclusively. Since step 14's S14-11 the script
is Postgres alone — chain 4's derivation for the write path, a read-only
transaction for the preview — and these tests pin both:

* the write path runs `derive_gender_pg` and never imports `duckdb`, whatever
  `KS_WRITE_BUYERS` says;
* the preview reads Postgres inside a READ-ONLY transaction — in production the
  only DSN at hand is the web container's writer DSN — and summarises counts
  only, never a name.

The executed half needs a real PostgreSQL and skips without `KS_PG_DSN`. Its
reason says "needs a live PostgreSQL" on purpose: that phrase is what CI counts
to fail a run in which a store test skipped, and a skip spelled any other way
would stop running without anyone being told.
"""
from __future__ import annotations

import ast
import importlib.util
import inspect
import os
from pathlib import Path
from unittest.mock import AsyncMock

import pytest

from core import gender_backfill
from core.gender import RULES_VERSION, classify
from core.gender_backfill import stored_sql, summarise
from core.sql_dialect import DUCKDB, POSTGRES

ROOT = Path(__file__).resolve().parents[2]
_spec = importlib.util.spec_from_file_location(
    "backfill_gender_script", ROOT / "scripts" / "backfill_gender.py")
script = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(script)

DSN = os.getenv("KS_PG_DSN", "").strip()
needs_pg = pytest.mark.skipif(not DSN, reason="needs a live PostgreSQL at KS_PG_DSN")

# Names whose verdicts are not in doubt, so the tests pin the bookkeeping and
# not the classifier.
WOMAN = "Олена Петренко"
MAN = "Андрій Коваль"
assert classify(WOMAN).gender == "f" and classify(MAN).gender == "m"
UNDECIDABLE = "ТОВ Ромашка"
assert classify(UNDECIDABLE).gender is None


def _row(buyer_id, name, gender="__none__", version=RULES_VERSION, override=False):
    """(id, name, gender, rules_version, override); no verdict when omitted."""
    if gender == "__none__":
        return (buyer_id, name, None, None, None)
    return (buyer_id, name, gender, version, override)


class TestSummarise:
    def test_an_agreeing_table_changes_nothing(self):
        s = summarise([_row(1, WOMAN, "f"), _row(2, MAN, "m")])
        assert s["tick_would_write"] == 0
        assert s["rules_would_change"] == 0
        assert s["changes"] == {}
        assert s["after_rederive"] == {"f": 1, "m": 1, "NULL": 0}

    def test_a_buyer_with_no_verdict_is_what_the_tick_writes(self):
        s = summarise([_row(1, WOMAN)])
        assert (s["missing"], s["stale"], s["tick_would_write"]) == (1, 0, 1)
        # No stored verdict is not a change of one.
        assert s["rules_would_change"] == 0
        assert s["stored"] == 0

    def test_an_older_rules_version_is_written_by_the_tick(self):
        s = summarise([_row(1, WOMAN, "f", version=RULES_VERSION - 1)])
        assert (s["stale"], s["tick_would_write"]) == (1, 1)
        assert s["rules_would_change"] == 0      # same answer, older stamp

    def test_a_verdict_the_current_rules_decide_otherwise_is_a_transition(self):
        s = summarise([
            _row(1, WOMAN, "m"),            # rules now say f
            _row(2, UNDECIDABLE, "f"),      # rules now refuse
            _row(3, MAN, "m"),
        ])
        assert s["rules_would_change"] == 2
        assert s["changes"] == {"f→NULL": 1, "m→f": 1}
        assert s["after_rederive"] == {"f": 1, "m": 1, "NULL": 1}

    def test_a_human_override_is_never_a_change_but_its_disagreement_is_counted(self):
        s = summarise([_row(1, WOMAN, "m", override=True)])
        assert s["overrides"] == 1
        assert s["rules_would_change"] == 0
        assert s["overrides_disagree"] == 1
        # The override survives a re-derivation, so the split keeps its answer.
        assert s["after_rederive"] == {"f": 0, "m": 1, "NULL": 0}

    def test_it_returns_counts_and_never_a_name(self):
        s = summarise([_row(1, WOMAN, "m"), _row(2, MAN)])
        text = repr(s)
        for token in (*WOMAN.split(), *MAN.split()):
            assert token not in text


class TestOneBodyTwoEngines:
    def test_the_renderings_differ_only_in_table_names(self):
        duck = stored_sql(DUCKDB.buyers, DUCKDB.buyer_gender)
        pg = stored_sql(POSTGRES.buyers, POSTGRES.buyer_gender)
        assert duck != pg
        undone = (pg.replace(POSTGRES.buyer_gender, DUCKDB.buyer_gender)
                    .replace(POSTGRES.buyers, DUCKDB.buyers))
        assert undone == duck

    @pytest.mark.parametrize("rebuild_all", [False, True])
    def test_the_pending_question_too(self, rebuild_all):
        """Chain 4's derivation asks it of Postgres (`pg_buyers_write._pending`)
        — one body, so the two engines cannot come to select different
        buyers."""
        duck, duck_params = gender_backfill.pending_sql(
            DUCKDB.buyers, DUCKDB.buyer_gender, rebuild_all=rebuild_all)
        pg, pg_params = gender_backfill.pending_sql(
            POSTGRES.buyers, POSTGRES.buyer_gender, rebuild_all=rebuild_all)
        assert duck_params == pg_params == ([] if rebuild_all else [RULES_VERSION])
        undone = (pg.replace(POSTGRES.buyer_gender, DUCKDB.buyer_gender)
                    .replace(POSTGRES.buyers, DUCKDB.buyers))
        assert duck != pg and undone == duck
        assert "override_by_human = FALSE" in duck


class TestThePreviewIsReadOnly:
    def test_the_postgres_read_opens_a_read_only_transaction(self):
        """Structure, not prose: the call itself must carry readonly=True."""
        tree = ast.parse(inspect.getsource(gender_backfill.read_stored_postgres))
        calls = [n for n in ast.walk(tree)
                 if isinstance(n, ast.Call)
                 and isinstance(n.func, ast.Attribute) and n.func.attr == "transaction"]
        assert calls, "read_stored_postgres opens no transaction"
        for call in calls:
            kw = {k.arg: k.value for k in call.keywords}
            assert isinstance(kw.get("readonly"), ast.Constant) and kw["readonly"].value is True


class TestTheWritePathIsPostgres:
    """Since step 14's S14-11 the write path is chain 4's derivation,
    whatever `KS_WRITE_BUYERS` says, latched or not. Until then it followed
    the chain's `mode()` — DuckDB's `derive_gender`, or exit 4 for a chain
    flagged and not latched, or a flag nobody could read."""

    @pytest.fixture
    def pg(self, monkeypatch):
        from core import pg_buyers_write, store as core_store

        monkeypatch.setattr(core_store, "get_store", AsyncMock(
            side_effect=AssertionError("DuckDB opened by the script")))
        derive = AsyncMock(return_value={
            "pending": 3, "written": 3, "female": 2, "male": 1, "null": 0,
            "rules_version": RULES_VERSION, "error": None, "ms": 5})
        monkeypatch.setattr(pg_buyers_write, "derive_gender_pg", derive)
        return monkeypatch, derive

    @pytest.mark.asyncio
    @pytest.mark.parametrize("flag", [None, "duckdb", "postgres", "postgre"])
    @pytest.mark.parametrize("latched", [True, False])
    async def test_it_derives_in_postgres_whatever_the_flag(self, pg, flag, latched):
        """Mutation: route by the flag or the latch again."""
        from core import chain_latch, pg_buyers_write

        env, derive = pg
        if flag is None:
            env.delenv(pg_buyers_write.WRITE_ENV, raising=False)
        else:
            env.setenv(pg_buyers_write.WRITE_ENV, flag)
        if latched:
            chain_latch.latch(pg_buyers_write.CHAIN, pg_buyers_write.WRITE_ENV)
        assert await script.main(rebuild_all=True, dry_run=False) == script.EXIT_OK
        derive.assert_awaited_once_with(rebuild_all=True)

    @pytest.mark.asyncio
    async def test_a_failed_derivation_is_exit_1(self, pg):
        _env, derive = pg
        derive.return_value = {**derive.return_value, "error": "OSError: down"}
        assert await script.main(rebuild_all=False, dry_run=False) == script.EXIT_FAILED

    def test_the_script_never_imports_duckdb(self):
        """Structure: no `import duckdb` and no `--duckdb` left in it."""
        tree = ast.parse((ROOT / "scripts" / "backfill_gender.py").read_text())
        names = {alias.name for node in ast.walk(tree)
                 if isinstance(node, (ast.Import, ast.ImportFrom))
                 for alias in node.names}
        modules = {node.module for node in ast.walk(tree)
                   if isinstance(node, ast.ImportFrom)}
        assert "duckdb" not in names and "duckdb" not in modules
        assert '"--duckdb"' not in (ROOT / "scripts" / "backfill_gender.py").read_text()


@needs_pg
class TestAgainstARealPostgres:
    @pytest.mark.asyncio
    async def test_the_postgres_rendering_runs_and_writes_nothing(self):
        import asyncpg

        conn = await asyncpg.connect(DSN)
        try:
            # Stand-ins in `app`, not TEMP tables: production revokes TEMP from
            # PUBLIC, and a test that only passes with TEMP proves nothing about
            # a correctly provisioned database.
            await conn.execute("DROP TABLE IF EXISTS app.t_bg_gender")
            await conn.execute("DROP TABLE IF EXISTS app.t_bg_buyers")
            await conn.execute(
                "CREATE TABLE app.t_bg_buyers (id INTEGER PRIMARY KEY, full_name TEXT)")
            await conn.execute("""
                CREATE TABLE app.t_bg_gender (
                    buyer_id INTEGER PRIMARY KEY, gender TEXT,
                    rules_version INTEGER NOT NULL,
                    override_by_human BOOLEAN NOT NULL DEFAULT FALSE)""")
            await conn.executemany(
                "INSERT INTO app.t_bg_buyers VALUES ($1, $2)",
                [(1, WOMAN), (2, MAN), (3, UNDECIDABLE)])
            await conn.executemany(
                "INSERT INTO app.t_bg_gender VALUES ($1, $2, $3, $4)",
                [(1, "m", RULES_VERSION, False),       # the rules now say f
                 (2, "m", RULES_VERSION - 1, False)])  # stale; 3 has none

            rows = await gender_backfill.read_stored_postgres(
                conn, buyers="app.t_bg_buyers", buyer_gender="app.t_bg_gender")
            s = summarise(rows)
            assert (s["buyers"], s["missing"], s["stale"]) == (3, 1, 1)
            assert s["changes"] == {"m→f": 1}

            # Nothing written, and the transaction really was read-only: the
            # same snapshot shape refuses a write outright.
            assert await conn.fetchval("SELECT count(*) FROM app.t_bg_gender") == 2
            with pytest.raises(asyncpg.exceptions.ReadOnlySQLTransactionError):
                async with conn.transaction(isolation="repeatable_read", readonly=True):
                    await conn.execute("DELETE FROM app.t_bg_gender")
        finally:
            await conn.execute("DROP TABLE IF EXISTS app.t_bg_gender")
            await conn.execute("DROP TABLE IF EXISTS app.t_bg_buyers")
            await conn.close()
