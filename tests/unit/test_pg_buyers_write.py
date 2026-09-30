"""Chain 4's switch: what `KS_WRITE_BUYERS` decides, and what it cannot.

The writers themselves are exercised against a live Postgres in
`tests/integration/test_buyers_chain_writer.py` and, for the latch, in
`tests/integration/test_latch_waits_for_a_connection.py`. This file pins the
decision every consumer reads — the flag, the latch, the three readers that
must follow the writes — and what is refused before the latch.
"""
from __future__ import annotations

import logging
import re
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace

import pytest

from core import chain_latch, pg_buyers_write as chain, write_chains
from core import pg_chain_invariants as inv

READERS = ("KS_SMS_STORE", "KS_READ_SEARCH_INDEX", "KS_READ_DASHBOARD")


@pytest.fixture
def flags(monkeypatch):
    """No flag of this chain's and every reader of the buyers on postgres —
    production's readers on 2026-09-25 — so a test moves exactly what it
    means to."""
    monkeypatch.delenv(chain.WRITE_ENV, raising=False)
    for env in READERS:
        monkeypatch.setenv(env, "postgres")
    chain._unmet_warned.clear()
    return monkeypatch


def _mode() -> str:
    return write_chains.chain_modes()[chain.CHAIN]["mode"]


class TestTheShape:
    def test_the_tables_and_the_key(self):
        assert chain.CHAIN == "pg_buyers_write"
        assert chain.CHAIN_TABLES == (
            "bronze.buyers", "bronze.buyer_contacts", "app.buyer_gender")
        assert chain.CHAIN_SYNC_KEYS == ("last_sync_buyers",)

    def test_its_bronze_tables_are_the_mirrors_whole_unit(self):
        """Whoever takes the buyers takes their contacts: the mirror writes
        both in one transaction, and a chain that took one would leave the
        mirror writing the other over it."""
        from core.pg_buyers import BUYER_UNIT

        assert set(BUYER_UNIT) <= set(chain.CHAIN_TABLES)

    def test_its_watermark_is_left_to_the_canary(self):
        """`buyer_sync_stalled` already pages on the step's own state at 90
        minutes; judging the stamp the same step writes would page twice."""
        assert chain.CHAIN_WATERMARK_MAX_AGE_MIN is None
        assert inv.watermark_limit_min(chain) is None

    def test_the_numeric_columns_are_the_migrations(self):
        text = Path("migrations/versions/0011_buyers_lines_vitrina.py").read_text()
        for column, (precision, scale) in chain.NUMERIC_COLUMNS.items():
            assert re.search(
                rf"{column}\s+NUMERIC\({precision},\s*{scale}\)", text), column

    def test_the_verdict_values_are_the_checks(self):
        """The CHECK constraints of revision 0024 against the classifier's
        vocabulary: a verdict the table refuses would cost the portion's
        verdicts, every tick, and nothing would say why but a log line."""
        from core.gender import CONFIDENCE_LADDER, GENDERS

        text = Path("migrations/versions/0024_buyer_gender.py").read_text()
        assert "gender IN ('f', 'm')" in text and GENDERS == ("f", "m")
        assert ("confidence IN ('certain', 'high', 'medium')" in text
                and CONFIDENCE_LADDER == ("certain", "high", "medium"))


class TestTheFlag:
    def test_the_default_is_duckdb(self, flags):
        assert chain.writes_postgres() is False
        assert _mode() == "duckdb"

    def test_postgres_with_every_reader_following(self, flags):
        flags.setenv(chain.WRITE_ENV, "postgres")
        assert chain.unmet_precondition() is None
        assert chain.writes_postgres() is True
        assert _mode() == "postgres"

    def test_a_typo_raises_in_the_writer_and_is_none_in_the_registry(self, flags):
        flags.setenv(chain.WRITE_ENV, "postgre")
        with pytest.raises(RuntimeError, match="KS_WRITE_BUYERS"):
            chain.writes_postgres()
        assert chain.mode() is None
        assert "KS_WRITE_BUYERS" in write_chains.chain_modes()[chain.CHAIN]["error"]

    @pytest.mark.parametrize("value", [None, "duckdb", "postgre"])
    def test_the_latch_outranks_the_flag(self, flags, value):
        chain_latch.latch(chain.CHAIN, chain.WRITE_ENV)
        if value is None:
            flags.delenv(chain.WRITE_ENV, raising=False)
        else:
            flags.setenv(chain.WRITE_ENV, value)
        assert chain.writes_postgres() is True
        assert chain.mode() == "postgres"

    @pytest.mark.parametrize("write", [None, "duckdb", "postgres", "postgre"])
    @pytest.mark.parametrize("lagging", [None, *READERS])
    @pytest.mark.parametrize("latched", [False, True])
    def test_mode_is_the_registrys_answer_in_every_state(
            self, flags, write, lagging, latched):
        """`mode()` is what the buyer selection, the gender rider and the
        stats branch on, and `writes_postgres()` is what the writer branches
        on; a state where they differ is the selection reading one store while
        the writes go to the other."""
        if write is None:
            flags.delenv(chain.WRITE_ENV, raising=False)
        else:
            flags.setenv(chain.WRITE_ENV, write)
        if lagging:
            flags.setenv(lagging, "duckdb")
        if latched:
            chain_latch.latch(chain.CHAIN, chain.WRITE_ENV)
        assert chain.mode() == _mode()
        try:
            answer = "postgres" if chain.writes_postgres() else "duckdb"
        except RuntimeError:
            answer = None
        assert chain.mode() == answer


class TestTheReadersComeFirst:
    """DN-27's rule, which DN-26 made generic: an unlatched chain whose readers
    still read DuckDB runs as duckdb, whatever its flag says."""

    @pytest.mark.parametrize("reader", READERS)
    @pytest.mark.parametrize("value", ["duckdb", "yes"])
    def test_each_reader_holds_the_chain_on_duckdb(self, flags, reader, value):
        flags.setenv(chain.WRITE_ENV, "postgres")
        flags.setenv(reader, value)
        state = write_chains.chain_modes()[chain.CHAIN]
        assert chain.writes_postgres() is False
        assert state["mode"] == "duckdb" and state["error"] is None
        assert reader in state["unmet_precondition"]
        # The shippers keep shipping what DuckDB still writes.
        assert not set(chain.CHAIN_TABLES) & write_chains.stood_down_tables()
        assert "last_sync_buyers" not in write_chains.stood_down_sync_keys()

    def test_an_unset_reader_is_duckdb_and_unmet(self, flags):
        flags.setenv(chain.WRITE_ENV, "postgres")
        flags.delenv("KS_SMS_STORE", raising=False)
        assert "KS_SMS_STORE is not postgres" in chain.unmet_precondition()

    def test_it_never_asks_postgres(self, flags, monkeypatch):
        """`/api/health` reads this through the registry and must answer with
        Postgres down."""
        from unittest.mock import AsyncMock

        monkeypatch.setattr("core.pg.get_pool",
                            AsyncMock(side_effect=AssertionError("asked Postgres")))
        flags.setenv(chain.WRITE_ENV, "postgres")
        flags.setenv("KS_READ_DASHBOARD", "duckdb")
        assert chain.unmet_precondition()

    def test_each_reader_is_the_parser_of_the_flag_it_names(self):
        """The list is what whoever removes a reader must change in the same
        commit (OD-10's PR-C takes `KS_READ_SEARCH_INDEX` away): a parser
        that no longer exists fails here, not by holding the chain on DuckDB
        for good behind a warning."""
        import ast
        import importlib
        import inspect
        import textwrap

        assert [r[0] for r in chain.READERS] == list(READERS)
        for name, module, parser in chain.READERS:
            mod = importlib.import_module(module)
            fn = getattr(mod, parser)
            assert callable(fn), (module, parser)
            if hasattr(mod, "ENV"):
                assert mod.ENV == name, (module, mod.ENV)
            else:
                strings = {n.value for n in ast.walk(ast.parse(
                    textwrap.dedent(inspect.getsource(fn))))
                    if isinstance(n, ast.Constant) and isinstance(n.value, str)}
                assert name in strings, f"{module}.{parser} does not read {name}"

    def test_a_reader_that_is_gone_holds_the_chain_and_says_so(self, flags, monkeypatch):
        """Not a raise out of the whole question: one reader unmet, named."""
        monkeypatch.setattr(chain, "READERS", chain.READERS[:1] + (
            ("KS_READ_SEARCH_INDEX", "core.no_such_module", "enabled"),))
        flags.setenv(chain.WRITE_ENV, "postgres")
        state = write_chains.chain_modes()[chain.CHAIN]
        assert state["mode"] == "duckdb"
        assert "KS_READ_SEARCH_INDEX is not understood" in state["unmet_precondition"]
        assert "could not be read" not in state["unmet_precondition"]

    def test_a_latched_chain_keeps_writing_and_says_its_readers_lag(self, flags):
        chain_latch.latch(chain.CHAIN, chain.WRITE_ENV)
        flags.setenv(chain.WRITE_ENV, "postgres")
        flags.setenv("KS_READ_SEARCH_INDEX", "duckdb")
        state = write_chains.chain_modes()[chain.CHAIN]
        assert chain.writes_postgres() is True and state["mode"] == "postgres"
        assert "KS_READ_SEARCH_INDEX" in state["unmet_precondition"]

    def test_the_warning_is_not_a_line_a_minute(self, flags, caplog):
        """The buyers step reads `last_sync_buyers` through this every tick;
        chain 6a's warn-per-call would be a line a minute for as long as the
        readers lag (the plan critic's a3)."""
        flags.setenv(chain.WRITE_ENV, "postgres")
        flags.setenv("KS_READ_DASHBOARD", "duckdb")
        with caplog.at_level(logging.WARNING, logger=chain.__name__):
            for _ in range(5):
                assert chain.writes_postgres() is False
        warned = [r for r in caplog.records if "stays on DuckDB" in r.message]
        assert len(warned) == 1

    def test_a_different_reason_is_said_at_once(self, flags, caplog):
        flags.setenv(chain.WRITE_ENV, "postgres")
        flags.setenv("KS_READ_DASHBOARD", "duckdb")
        with caplog.at_level(logging.WARNING, logger=chain.__name__):
            chain.writes_postgres()
            flags.setenv("KS_SMS_STORE", "duckdb")
            chain.writes_postgres()
        assert len([r for r in caplog.records if "stays on DuckDB" in r.message]) == 2


def _row(**kw):
    base = dict(id=1, loyalty_discount=None, loyalty_amount=None)
    base.update(kw)
    return SimpleNamespace(**base)


class TestWhatIsRefusedBeforeTheLatch:
    """A value Postgres refuses raises inside the write, after the marker is on
    disk: a first batch refused whole would latch the chain with no owner row
    behind it. So the writer asks first."""

    def test_a_loyalty_figure_over_its_numeric(self):
        assert chain._refusal(_row(loyalty_discount=1000.0), ()) is not None
        assert chain._refusal(_row(loyalty_discount=999.99), ()) is None
        assert chain._refusal(_row(loyalty_amount=1e10), ()) is not None
        assert chain._refusal(_row(loyalty_amount=float("nan")), ()) is not None

    def test_an_id_that_is_not_an_integer(self):
        assert chain._refusal(_row(id=None), ()) is not None
        assert chain._refusal(_row(id=True), ()) is not None

    def test_a_contact_naming_another_buyer(self):
        contact = SimpleNamespace(buyer_id=2)
        assert chain._refusal(_row(id=1), (contact,)) is not None

    @pytest.mark.asyncio
    async def test_a_batch_refused_whole_never_reaches_the_latch(self, flags, monkeypatch):
        from unittest.mock import AsyncMock

        from core.models import Buyer

        pool = AsyncMock(side_effect=AssertionError("asked for a pool"))
        monkeypatch.setattr(chain, "_pool", pool)
        skipped = []
        bad = Buyer.from_api({"id": 5, "full_name": "Олена",
                              "loyalty": [{"discount": 5000}]})
        assert bad.loyalty_discount == 5000.0, "the fixture must carry the value"
        assert await chain.upsert_buyers([bad], skipped_out=skipped) == 0
        assert skipped == [5]
        pool.assert_not_awaited()
        assert not chain_latch.latched(chain.CHAIN)

    @pytest.mark.asyncio
    async def test_an_empty_batch_asks_nothing(self, monkeypatch):
        from unittest.mock import AsyncMock

        pool = AsyncMock(side_effect=AssertionError("asked for a pool"))
        monkeypatch.setattr(chain, "_pool", pool)
        assert await chain.upsert_buyers([]) == 0
        pool.assert_not_awaited()


class TestTheDerivationNeverRaises:
    @pytest.mark.asyncio
    async def test_a_pool_it_cannot_get_is_a_result(self, monkeypatch):
        from unittest.mock import AsyncMock

        monkeypatch.setattr(chain, "_pool",
                            AsyncMock(side_effect=OSError("connection refused")))
        out = await chain.derive_gender_pg()
        assert out["error_class"] == "OSError" and out["written"] == 0
        assert not chain_latch.latched(chain.CHAIN)

    @pytest.mark.asyncio
    async def test_a_cancellation_still_goes_through(self, monkeypatch):
        import asyncio
        from unittest.mock import AsyncMock

        monkeypatch.setattr(chain, "_pool",
                            AsyncMock(side_effect=asyncio.CancelledError()))
        with pytest.raises(asyncio.CancelledError):
            await chain.derive_gender_pg()

    def test_its_clock_is_utc_and_the_web_processs(self):
        stamp = chain._now_utc()
        assert isinstance(stamp, datetime) and stamp.utcoffset().total_seconds() == 0


class TestTheCompletenessCheckFollowsTheChain:
    """`reconcile_buyer_completeness` is the one Postgres-side check that the
    chain's writer is alive; it must not switch off with the landing mirror's
    flag, which the chain does not consult (the plan critic's b2)."""

    @pytest.mark.asyncio
    async def test_mirror_off_and_chain_off_asks_nothing(self, flags, monkeypatch):
        from unittest.mock import AsyncMock

        from core.mirror_reconciliation import reconcile_buyer_completeness

        monkeypatch.setenv("KS_MIRROR_LANDING", "0")
        monkeypatch.setattr("core.pg.get_pool",
                            AsyncMock(side_effect=AssertionError("asked Postgres")))
        assert await reconcile_buyer_completeness() == []

    @pytest.mark.asyncio
    async def test_mirror_off_and_chain_on_still_asks(self, flags, monkeypatch):
        from unittest.mock import AsyncMock

        from core.mirror_reconciliation import reconcile_buyer_completeness

        monkeypatch.setenv("KS_MIRROR_LANDING", "0")
        flags.setenv(chain.WRITE_ENV, "postgres")
        asked = AsyncMock(side_effect=RuntimeError("reached Postgres"))
        monkeypatch.setattr("core.pg.get_pool", asked)
        with pytest.raises(RuntimeError, match="reached Postgres"):
            await reconcile_buyer_completeness()
        asked.assert_awaited_once()
