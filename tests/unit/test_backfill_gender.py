"""`scripts/backfill_gender.py` and the preview behind it.

The script's first version advertised `docker exec keycrm-web python
/app/scripts/backfill_gender.py`, and that could never have worked: the web
container holds the DuckDB file exclusively and a second process cannot open it
even read-only. Nobody found out for eleven days because nobody ran it. These
tests pin the two halves that replaced it:

* the write path, when the file is held elsewhere, refuses with exit 2 and
  writes nothing, instead of dying with an IOException mid-stack;
* the preview reads Postgres inside a READ-ONLY transaction — in production the
  only DSN at hand is the web container's writer DSN — and summarises counts
  only, never a name.

The executed half needs a real PostgreSQL and skips without `KS_PG_DSN`; CI
provides one and fails the job if anything still skips for want of it.
"""
from __future__ import annotations

import ast
import asyncio
import importlib.util
import inspect
import os
from pathlib import Path
from unittest.mock import AsyncMock

import duckdb
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
needs_pg = pytest.mark.skipif(not DSN, reason="KS_PG_DSN is not set")

# The message DuckDB 1.5.5 gives a second process, verbatim from production.
LOCKED = ('IO Error: Could not set lock on file "/app/data/analytics.duckdb": '
          'Conflicting lock is held in /usr/local/bin/python3.14 (PID 1). '
          'See also https://duckdb.org/docs/stable/connect/concurrency')

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

    @pytest.mark.asyncio
    async def test_the_duckdb_rendering_runs(self):
        conn = duckdb.connect(":memory:")
        conn.execute("CREATE TABLE buyers (id INTEGER, full_name VARCHAR)")
        conn.execute("""CREATE TABLE buyer_gender (buyer_id INTEGER, gender VARCHAR,
                        rules_version INTEGER, override_by_human BOOLEAN)""")
        conn.execute("INSERT INTO buyers VALUES (1, ?), (2, ?)", [WOMAN, MAN])
        conn.execute("INSERT INTO buyer_gender VALUES (1, 'f', ?, FALSE)",
                     [RULES_VERSION])

        class Store:
            def connection(self):
                class _C:
                    async def __aenter__(self_):
                        return conn

                    async def __aexit__(self_, *a):
                        return False
                return _C()

        rows = sorted(await gender_backfill.read_stored_duckdb(Store()))
        assert rows == [(1, WOMAN, "f", RULES_VERSION, False),
                        (2, MAN, None, None, None)]


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


class TestTheWritePathWhenTheFileIsHeld:
    def test_the_lock_is_recognised_and_other_io_errors_are_not(self):
        assert script.held_by_another_process(duckdb.IOException(LOCKED))
        assert not script.held_by_another_process(
            duckdb.IOException("IO Error: No space left on device"))
        assert not script.held_by_another_process(RuntimeError(LOCKED))

    @pytest.mark.asyncio
    async def test_it_exits_2_and_derives_nothing(self, monkeypatch, capsys):
        from core import duckdb_store

        monkeypatch.setattr(duckdb_store, "get_store",
                            AsyncMock(side_effect=duckdb.IOException(LOCKED)))
        derive = AsyncMock()
        monkeypatch.setattr(script, "derive_gender", derive)

        assert await script.main(rebuild_all=True, dry_run=False, use_duckdb=False) == 2
        derive.assert_not_called()
        err = capsys.readouterr().err
        assert "Nothing was written" in err
        assert "replicate_operational/trigger" in err
        assert "--dry-run" in err

    @pytest.mark.asyncio
    async def test_the_duckdb_preview_refuses_the_same_way(self, monkeypatch):
        from core import duckdb_store

        monkeypatch.setattr(duckdb_store, "get_store",
                            AsyncMock(side_effect=duckdb.IOException(LOCKED)))
        assert await script.main(rebuild_all=False, dry_run=True, use_duckdb=True) == 2

    @pytest.mark.asyncio
    async def test_any_other_io_error_is_not_dressed_up_as_a_lock(self, monkeypatch):
        from core import duckdb_store

        monkeypatch.setattr(duckdb_store, "get_store", AsyncMock(
            side_effect=duckdb.IOException("IO Error: No space left on device")))
        with pytest.raises(duckdb.IOException):
            await script.main(rebuild_all=False, dry_run=False, use_duckdb=False)

    @pytest.mark.asyncio
    async def test_a_preview_without_a_dsn_says_so(self, monkeypatch):
        monkeypatch.delenv("KS_PG_DSN", raising=False)
        assert await script.main(rebuild_all=False, dry_run=True, use_duckdb=False) == 3


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
