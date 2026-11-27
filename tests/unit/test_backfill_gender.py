"""`scripts/backfill_gender.py` and the preview behind it.

The script's first version advertised `docker exec keycrm-web python
/app/scripts/backfill_gender.py`, and that could never have worked: the web
container held the DuckDB file exclusively. Since step 14's S14-11 the script
is Postgres alone — chain 4's derivation for the write path, a read-only
transaction for the preview — and these tests pin both:

* the write path, run against chain 4's tables, decides the buyers it should
  and never a human's override;
* the preview reads Postgres inside a READ-ONLY transaction — in production the
  only DSN at hand is the web container's writer DSN — and summarises counts
  only, never a name.

The executed half needs a real PostgreSQL and skips without `KS_PG_DSN`. Its
reason says "needs a live PostgreSQL" on purpose: that phrase is what CI counts
to fail a run in which a store test skipped, and a skip spelled any other way
would stop running without anyone being told.
"""
from __future__ import annotations

import importlib.util
import os
from pathlib import Path
from unittest.mock import AsyncMock

import pytest

from core import gender_backfill
from core.gender import RULES_VERSION, classify
from core.gender_backfill import summarise

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


class TestTheWritePathIsPostgres:
    """Since step 14's S14-11 the write path is chain 4's derivation. What it
    writes is `TestAgainstARealPostgres`'s; here, how a failure is answered."""

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
    async def test_a_failed_derivation_is_exit_1(self, pg):
        _env, derive = pg
        derive.return_value = {**derive.return_value, "error": "OSError: down"}
        assert await script.main(rebuild_all=False, dry_run=False) == script.EXIT_FAILED


@needs_pg
class TestAgainstARealPostgres:
    @pytest.mark.asyncio
    async def test_the_preview_reads_in_a_read_only_transaction_and_writes_nothing(self):
        """In production the preview's only DSN is web's writer DSN, so its
        read must be one a write cannot ride on. The buyers it reads are a
        view whose every row asks Postgres whether the transaction reading it
        is read-only, and refuses to be read otherwise."""
        import asyncpg

        conn = await asyncpg.connect(DSN)
        try:
            # Stand-ins in `app`, not TEMP tables: production revokes TEMP from
            # PUBLIC, and a test that only passes with TEMP proves nothing about
            # a correctly provisioned database.
            await _drop_stand_ins(conn)
            await conn.execute(
                "CREATE TABLE app.t_bg_names (id INTEGER PRIMARY KEY, full_name TEXT)")
            await conn.execute(
                "CREATE FUNCTION app.t_bg_read_only() RETURNS boolean LANGUAGE plpgsql AS $$ "
                "BEGIN IF current_setting('transaction_read_only') <> 'on' THEN "
                "RAISE EXCEPTION 'read outside a read-only transaction'; END IF; "
                "RETURN TRUE; END $$")
            await conn.execute(
                "CREATE VIEW app.t_bg_buyers AS SELECT id, full_name "
                "FROM app.t_bg_names WHERE app.t_bg_read_only()")
            await conn.execute("""
                CREATE TABLE app.t_bg_gender (
                    buyer_id INTEGER PRIMARY KEY, gender TEXT,
                    rules_version INTEGER NOT NULL,
                    override_by_human BOOLEAN NOT NULL DEFAULT FALSE)""")
            await conn.executemany(
                "INSERT INTO app.t_bg_names VALUES ($1, $2)",
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
            assert await conn.fetchval("SELECT count(*) FROM app.t_bg_gender") == 2
        finally:
            await _drop_stand_ins(conn)
            await conn.close()

    @pytest.mark.asyncio
    async def test_all_rederives_every_buyer_but_a_humans_decision(self):
        """`--all` through the script against chain 4's tables: a buyer with
        no verdict is decided, a current verdict the rules decide otherwise
        is rewritten, and a human's override is kept."""
        from tests.helpers.buyers_fixture import buyers_in_postgres

        async with buyers_in_postgres() as pool:
            await pool.executemany(
                "INSERT INTO bronze.buyers (id, full_name) VALUES ($1, $2)",
                [(1, WOMAN), (2, MAN), (3, UNDECIDABLE), (4, WOMAN)])
            await pool.executemany(
                "INSERT INTO app.buyer_gender (buyer_id, gender, method, rules_version, "
                "override_by_human) VALUES ($1, $2, 'test', $3, $4)",
                [(2, "f", RULES_VERSION, False), (4, "m", RULES_VERSION, True)])

            assert await script.main(rebuild_all=True, dry_run=False) == script.EXIT_OK

            verdicts = {r["buyer_id"]: (r["gender"], r["override_by_human"])
                        for r in await pool.fetch(
                            "SELECT buyer_id, gender, override_by_human FROM app.buyer_gender")}
        assert verdicts == {1: ("f", False), 2: ("m", False), 3: (None, False),
                            4: ("m", True)}


async def _drop_stand_ins(conn) -> None:
    await conn.execute("DROP VIEW IF EXISTS app.t_bg_buyers")
    await conn.execute("DROP FUNCTION IF EXISTS app.t_bg_read_only()")
    await conn.execute("DROP TABLE IF EXISTS app.t_bg_names")
    await conn.execute("DROP TABLE IF EXISTS app.t_bg_gender")
