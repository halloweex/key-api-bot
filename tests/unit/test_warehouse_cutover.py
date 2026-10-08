"""Step 13a (DN-28): the warehouse writer's mode, what it stands down, and the
precondition evaluator. The switch those feed is DN-29's, and its own tests are
in `tests/unit/test_warehouse_writer.py`; what is pinned here of it is that
`postgres` with a precondition unmet still runs as `duckdb`.

The integrity plumbing the stand-down feeds — the scan skipping the checks and
the twins standing in — is in `tests/unit/test_pg_warehouse_dq.py`; the
Postgres-only Gold check is in `tests/unit/test_pg_gold.py` and
`tests/unit/test_mirror_landing_isolation.py`.
"""
from __future__ import annotations

import ast
import asyncio
import dataclasses
import logging
import pathlib
import re
from unittest.mock import AsyncMock, patch

import pytest

from core import warehouse_cutover as wc
from core.pg import REQUIRED_REVISION

REPO = pathlib.Path(__file__).resolve().parents[2]


@pytest.fixture
def fresh(monkeypatch):
    """The module's cache as a new process has it, put back afterwards."""
    monkeypatch.setattr(wc, "_value", None)
    monkeypatch.setattr(wc, "_mode", None)
    monkeypatch.setattr(wc, "_mode_error", None)
    return wc


@pytest.fixture
def postgres_writer(monkeypatch):
    """The state DN-29 reaches when every precondition holds, set directly so
    the plumbing that consumes it is proven without assembling them."""
    monkeypatch.setattr(wc, "_mode", wc.POSTGRES)


# ─── The mode ────────────────────────────────────────────────────────────────


class TestTheMode:
    def test_unset_is_duckdb(self, fresh, monkeypatch):
        monkeypatch.delenv(wc.ENV, raising=False)
        assert fresh.configure_mode() == "duckdb"
        assert fresh.mode_error() is None and not fresh.writes_postgres()

    def test_before_configure_it_is_duckdb(self, fresh):
        assert fresh.mode() == "duckdb" and not fresh.writes_postgres()

    def test_an_unknown_value_runs_as_duckdb_and_publishes_the_error(
        self, fresh, monkeypatch, caplog,
    ):
        monkeypatch.setenv(wc.ENV, "postgress")
        with caplog.at_level(logging.ERROR, logger="core.warehouse_cutover"):
            assert fresh.configure_mode() == "duckdb"
            fresh.configure_mode()
        assert "postgress" in fresh.mode_error()
        assert fresh.status()["error"] == fresh.mode_error()
        # web's startup and the scheduler both configure: one ERROR per cause.
        assert caplog.text.count("postgress") == 1

    def test_postgres_with_its_preconditions_unmet_runs_as_duckdb(
        self, fresh, monkeypatch, caplog,
    ):
        """OD-09 (b): nothing in this environment is met, so the value is
        read, the verdict published, and DuckDB goes on deriving with every
        check up — the other half of a switch never reached."""
        monkeypatch.setenv(wc.ENV, " Postgres ")
        with caplog.at_level(logging.WARNING, logger="core.warehouse_cutover"):
            assert fresh.configure_mode() == "duckdb"
        assert fresh.value() == "postgres" and fresh.mode_error() is None
        assert not fresh.writes_postgres() and fresh.duckdb_derives()
        assert fresh.stood_down_duckdb_checks() == frozenset()
        assert "precondition(s) of the switch are unmet" in caplog.text
        status = fresh.status()
        assert (status["value"], status["mode"], status["switch_built"]) == (
            "postgres", "duckdb", True)
        assert "pg_derive_own" in {u["key"] for u in status["preconditions_unmet"]}

    def test_this_build_carries_the_switch(self):
        assert wc.SWITCH_BUILT is True

    def test_configure_modes_reads_it_before_the_boot_sync(self, fresh, monkeypatch):
        """Read where every cached mode is read (DN-05b), so DN-29 finds it
        set before `init_and_sync` runs. The walk in `test_runtime_modes.py`
        fails as well if the call is dropped."""
        from core.runtime_modes import configure_modes

        monkeypatch.setenv(wc.ENV, "nope")
        modes = configure_modes()
        assert modes[wc.ENV] == "duckdb"
        assert fresh.value() == "nope" and fresh.mode_error()


# ─── Where a typo is seen ────────────────────────────────────────────────────


class TestAnUnknownValueIsSeen:
    """Published on `/api/health` and judged by the canary, as KS_PG_DERIVE
    and KS_READ_FALLBACK are. The review found it only in a log line and on
    the admin status page, which nothing watches: in this build a typo costs
    nothing, and the first time it would is the DN-29 flip."""

    def test_health_publishes_the_value_the_mode_and_the_error(self, fresh, monkeypatch):
        from web.routes.api.health import _warehouse_writer_mode

        monkeypatch.setenv(wc.ENV, "postgress")
        fresh.configure_mode()
        block = _warehouse_writer_mode()
        assert (block["mode"], block["value"]) == ("duckdb", "postgress")
        assert "KS_WRITE_WAREHOUSE='postgress'" in block["error"]

    def test_the_public_endpoint_carries_it_through_its_response_model(
        self, fresh, monkeypatch,
    ):
        """Through the app: `/api/health` declares a response model, and a key
        the model does not name is dropped on the way out without a word."""
        from fastapi.testclient import TestClient

        from web.main import app
        from web.ratelimit import limiter

        monkeypatch.setenv(wc.ENV, "postgress")
        fresh.configure_mode()
        limiter.reset()
        try:
            body = TestClient(app).get("/api/health").json()
        finally:
            limiter.reset()
        block = body["warehouse_writer_mode"]
        assert (block["mode"], block["value"]) == ("duckdb", "postgress")
        assert "KS_WRITE_WAREHOUSE='postgress'" in block["error"]

    def test_the_canary_warns_on_an_error_and_is_quiet_otherwise(self):
        from bot.canary import check_warehouse_writer_mode

        block = {"mode": "duckdb", "value": "postgress",
                 "error": "KS_WRITE_WAREHOUSE='postgress' is not one of ('duckdb', 'postgres')"}
        assert [k for k, _ in check_warehouse_writer_mode(
            {"warehouse_writer_mode": block})] == ["warehouse_mode_invalid"]
        assert check_warehouse_writer_mode({}) == []
        # `postgres` is understood — published, not acted on, not a failure.
        assert check_warehouse_writer_mode({"warehouse_writer_mode": {
            "mode": "duckdb", "value": "postgres", "error": None}}) == []

    @pytest.mark.asyncio
    async def test_the_wiring_warns_and_names_its_lever(self):
        from datetime import datetime, timedelta, timezone

        import httpx

        from bot import canary
        from tests.unit.test_canary import DASHBOARD, _healthy_payload, _mock_transport

        payload = _healthy_payload()
        payload["warehouse_writer_mode"] = {
            "mode": "duckdb", "value": "postgress",
            "error": "KS_WRITE_WAREHOUSE='postgress' is not one of ('duckdb', 'postgres')"}

        def handler(request):
            return httpx.Response(200, json=payload)

        future = datetime.now(timezone.utc) + timedelta(days=60)
        cert = {"notAfter": future.strftime("%b %d %H:%M:%S %Y GMT")}
        async with _mock_transport(handler) as client:
            with patch.object(canary, "_fetch_peer_cert", return_value=cert):
                result = await canary.run_canary(DASHBOARD, client=client)
        assert result.severity == "warn"
        assert result.failure_keys == ["warehouse_mode_invalid"]
        assert "KS_WRITE_WAREHOUSE" in canary._what_to_do(result)

    def test_it_is_a_registered_condition(self):
        from core.alerting import Kind, spec_for

        assert spec_for("warehouse_mode_invalid").kind is Kind.CONDITION


# ─── What stands down ────────────────────────────────────────────────────────


class TestWhatStandsDown:
    def test_nothing_while_duckdb_derives(self, fresh):
        assert wc.stood_down_duckdb_checks() == frozenset()

    def test_the_five_silver_and_gold_checks_under_postgres(self, postgres_writer):
        assert wc.stood_down_duckdb_checks() == wc.STOOD_DOWN_WHEN_POSTGRES
        assert wc.STOOD_DOWN_WHEN_POSTGRES == {
            "silver_arc", "attribution_coverage", "gold_cell_values",
            "headline_vs_line_items", "goods_shipped_without_sale"}

    def test_each_is_a_name_the_scan_guards_a_check_under(self):
        """A name the scan does not guard under would stand nothing down."""
        from core.data_quality import GUARDED_CHECK_CONDITIONS

        assert wc.STOOD_DOWN_WHEN_POSTGRES <= set(GUARDED_CHECK_CONDITIONS)

    def test_no_check_over_landing_stands_down(self):
        """The switch freezes Silver, Gold and UTM, not `orders`: the landing
        checks stay up until chains 3, 4 and 6."""
        from core import pg_warehouse_dq as twins

        landing = {g for g, _d, _t in twins.LANDING_TWINS} | twins.DUCKDB_BARE_CHECKS
        assert not wc.STOOD_DOWN_WHEN_POSTGRES & landing

    def test_each_has_its_postgres_replacement(self):
        """Stood down only where something on the Postgres side asks the same
        question: the twins' group, or the standalone Gold check."""
        from core import mirror_reconciliation, pg_warehouse_dq as twins

        replacement = {
            "silver_arc": "silver_arc",
            "attribution_coverage": "attribution",
            "headline_vs_line_items": "line_items",
            "goods_shipped_without_sale": "line_items",
        }
        for guard in wc.STOOD_DOWN_WHEN_POSTGRES - {"gold_cell_values"}:
            assert replacement[guard] in twins.GROUPS
        assert callable(mirror_reconciliation.pg_gold_internal_check)


# ─── The preconditions ───────────────────────────────────────────────────────

MET_ENV = {
    "KS_PG_DERIVE": "own",
    "KS_DQ_PG_WAREHOUSE": "on",
    "KS_UTM_PARSE": "postgres",
    "KS_READ_FALLBACK": "off",
    "KS_PG_DSN": "postgresql://ks_app:x@db:5432/ks",
    **{name: "postgres" for name in wc.WAREHOUSE_READERS},
    "KS_READ_COHORTS": "clickhouse",
    "KS_CH_URL": "http://ch:8123",
    "KS_GOALS_HISTORY": "silver",
}
MET_FACTS = wc.Facts(revision=REQUIRED_REVISION, required_revision=REQUIRED_REVISION,
                     bridge_owners={}, expenses_backfilled=True)


# A door somebody adds after OD-10 retired every one there was (2026-09-30):
# `OD10_DOORS` is empty in this build, so `od10_doors` is broken with this.
A_DOOR = (("web/routes/api/example.py:a_new_door", "GET /api/a-new-door"),)


def _breaking(key: str):
    """`(env, facts)` with every precondition met except `key`."""
    env, facts = dict(MET_ENV), MET_FACTS
    simple = {
        "value_understood": ("KS_WRITE_WAREHOUSE", "postgre"),
        "pg_derive_own": ("KS_PG_DERIVE", "piggyback"),
        "pg_twins_on": ("KS_DQ_PG_WAREHOUSE", None),
        "utm_parse_postgres": ("KS_UTM_PARSE", "duckdb"),
        "read_fallback_off": ("KS_READ_FALLBACK", None),
        "pg_dsn": ("KS_PG_DSN", None),
        "mirror_landing": ("KS_MIRROR_LANDING", "0"),
        "cohorts_clickhouse": ("KS_READ_COHORTS", "duckdb"),
        "ch_url": ("KS_CH_URL", " "),
        "goals_bridge": ("KS_GOALS_HISTORY", "bridge"),
    }
    if key in simple:
        name, value = simple[key]
        if value is None:
            env.pop(name, None)
        else:
            env[name] = value
    elif key.startswith("reader:"):
        env[key[len("reader:"):]] = "duckdb"
    elif key == "pg_revision":
        facts = dataclasses.replace(MET_FACTS, revision="0032_manual_goal_ids")
    elif key == "expenses_backfilled":
        facts = dataclasses.replace(MET_FACTS, expenses_backfilled=False)
    elif key == "retired_conditions_clear":
        facts = dataclasses.replace(
            MET_FACTS, open_retired={"silver_missing_rows": "dq:integrity"})
    elif key == "od10_doors":
        facts = dataclasses.replace(MET_FACTS, od10_doors=A_DOOR)
    else:
        raise AssertionError(f"no way to break {key!r} — add one here")
    return env, facts


KEYS = [key for key, _ in wc.PRECONDITIONS]



class TestTheEvaluator:
    def test_all_met_is_ready(self):
        assert wc.evaluate_preconditions(MET_ENV, MET_FACTS) == []

    def test_the_keys_are_unique(self):
        assert len(KEYS) == len(set(KEYS))

    @pytest.mark.parametrize("key", KEYS)
    def test_each_unmet_on_its_own_is_named_and_nothing_else(self, key):
        env, facts = _breaking(key)
        unmet = wc.evaluate_preconditions(env, facts)
        assert [u.key for u in unmet] == [key]
        assert unmet[0].detail

    def test_the_plans_list_is_all_there(self):
        """The DN-28 list, item by item: own; twins on; UTM parse postgres;
        fallback off; DSN and revision; KS_MIRROR_LANDING; every Silver, Gold
        and UTM reader postgres; cohorts with KS_CH_URL; the DN-12 tripwire."""
        assert {"pg_derive_own", "pg_twins_on", "utm_parse_postgres",
                "read_fallback_off", "pg_dsn", "pg_revision", "mirror_landing",
                "cohorts_clickhouse", "ch_url", "goals_bridge"} <= set(KEYS)
        assert {f"reader:{name}" for name in wc.WAREHOUSE_READERS} <= set(KEYS)
        assert "reader:KS_READ_TRAFFIC" in KEYS   # the UTM reader
        # And the review's: no page open under a check the switch retires.
        assert "retired_conditions_clear" in KEYS

    def test_with_nothing_met_it_names_exactly_the_published_list(self):
        """Both directions at once: an item the evaluator checks but the
        published list leaves out is one nobody reading the list would know
        to do, and the reverse is a list item nothing checks."""
        # A revision that answered, and wrongly: the expenses history is asked
        # only of a Postgres that said one.
        unmet = wc.evaluate_preconditions({"KS_MIRROR_LANDING": "0",
                                           "KS_WRITE_WAREHOUSE": "postgress"}, wc.Facts(
            revision="0000_somebody_elses", required_revision=REQUIRED_REVISION,
            expenses_backfilled=False, bridge_owners=None, bridge_error="x",
            open_retired=None, open_retired_error="x", od10_doors=A_DOOR))
        assert [u.key for u in unmet] == KEYS

    def test_it_reads_values_as_the_modules_do(self):
        env = {**MET_ENV, "KS_PG_DERIVE": " OWN ", "KS_READ_GOLD": "Postgres"}
        assert wc.evaluate_preconditions(env, MET_FACTS) == []

    def test_an_empty_environment_names_everything_it_can(self):
        unmet = wc.evaluate_preconditions({}, wc.Facts(
            revision_error="not asked: KS_PG_DSN is not set", bridge_owners={}))
        assert [u.key for u in unmet] == [
            k for k in KEYS
            if k not in ("value_understood", "mirror_landing",
                         "retired_conditions_clear", "od10_doors",
                         # not asked: no DSN, so `pg_revision` names it
                         "expenses_backfilled")]

    def test_a_detail_says_what_was_found_and_what_is_needed(self):
        env, facts = _breaking("utm_parse_postgres")
        (u,) = wc.evaluate_preconditions(env, facts)
        assert "KS_UTM_PARSE" in u.detail and "'duckdb'" in u.detail \
            and "'postgres'" in u.detail

    def test_an_unreadable_revision_says_why(self):
        facts = wc.Facts(revision_error="SchemaVersionError: behind",
                         required_revision=REQUIRED_REVISION, bridge_owners={})
        (u,) = wc.evaluate_preconditions(MET_ENV, facts)
        assert u.key == "pg_revision" and "SchemaVersionError" in u.detail

    def test_an_unreadable_registry_is_said_under_the_bridge(self):
        env = {**MET_ENV, "KS_GOALS_HISTORY": "bridge"}
        facts = dataclasses.replace(MET_FACTS, bridge_owners=None,
                                    bridge_error="ImportError: gone")
        (u,) = wc.evaluate_preconditions(env, facts)
        assert u.key == "goals_bridge" and "ImportError" in u.detail

    def test_under_silver_the_registry_does_not_matter(self):
        """Met by construction: no calculator reads the three tables then
        (`tests/unit/test_goals_history_silver.py`), so who owns them, or a
        registry that cannot say, changes nothing. Mutation M19: judge the
        owners alone again, and the owned case here is unmet."""
        for facts in (
            dataclasses.replace(MET_FACTS, bridge_owners=None, bridge_error="x"),
            dataclasses.replace(MET_FACTS, bridge_owners={
                "pg_orders_write": ("bronze.orders",)}),
        ):
            assert wc.evaluate_preconditions(MET_ENV, facts) == []

    @pytest.mark.parametrize("value", [None, "", "bridge", " Bridge "])
    def test_the_bridge_is_unmet_with_nobody_owning_anything(self, value):
        """Step 13 freezes DuckDB's orders and classification whether or not
        a chain owns them, so the bridge alone holds the switch. Mutation
        M19's other half: evaluate owners only, and this is met."""
        env = dict(MET_ENV)
        if value is None:
            env.pop("KS_GOALS_HISTORY")
        else:
            env["KS_GOALS_HISTORY"] = value
        (u,) = wc.evaluate_preconditions(env, MET_FACTS)
        assert u.key == "goals_bridge"
        assert "KS_GOALS_HISTORY is 'bridge'" in u.detail
        assert "diverging" not in u.detail

    def test_a_value_it_does_not_understand_is_unmet_by_its_class(self):
        env = {**MET_ENV, "KS_GOALS_HISTORY": "silvr"}
        (u,) = wc.evaluate_preconditions(env, MET_FACTS)
        assert u.key == "goals_bridge"
        assert "'silvr'" in u.detail and "ValueError" in u.detail

    def test_the_mirror_is_read_as_the_mirror_reads_it(self, monkeypatch):
        """One rule for `KS_MIRROR_LANDING`, the mirror's own."""
        from core import pg_landing

        for value in (None, "1", "true", "yes", "0", "false", "no", "", " NO "):
            if value is None:
                monkeypatch.delenv("KS_MIRROR_LANDING", raising=False)
                env = dict(MET_ENV)
            else:
                monkeypatch.setenv("KS_MIRROR_LANDING", value)
                env = {**MET_ENV, "KS_MIRROR_LANDING": value}
            unmet = {u.key for u in wc.evaluate_preconditions(env, MET_FACTS)}
            assert ("mirror_landing" not in unmet) is pg_landing.enabled(), value


class TestTheModesAreReadAsTheirOwnModulesReadThem:
    """R1-4: the preconditions name other modules' switches by value — the
    variable and the value that module acts on. After DN-20c (#264) is under
    this branch, `read_fallback_off` must be met exactly when DN-20c's own
    `configure_mode` answers `off`, or the switch could wait on a refusal mode
    that is on, or run beside one that is not."""

    @pytest.fixture(autouse=True)
    def _put_back(self, monkeypatch):
        from core import read_fallback

        for name in ("_mode", "_mode_error", "_misconfigured"):
            monkeypatch.setattr(read_fallback, name, getattr(read_fallback, name))

    def test_the_names_and_values_are_the_modules_own(self):
        from core import pg_derivation, pg_utm_parse, pg_warehouse_dq, read_fallback

        assert (wc.READ_FALLBACK, "off") == (read_fallback.ENV, read_fallback.OFF)
        assert (wc.UTM_PARSE, "postgres") == (pg_utm_parse.ENV, pg_utm_parse.POSTGRES)
        assert (wc.PG_DERIVE, "own") == (pg_derivation.ENV, pg_derivation.OWN)
        assert wc.PG_TWINS == pg_warehouse_dq.ENV and "on" in pg_warehouse_dq._VALID
        assert dict(wc.PRECONDITIONS)["read_fallback_off"] == f"{read_fallback.ENV}=off"

    @pytest.mark.parametrize("value", [None, "off", " OFF ", "Off", "duckdb", "",
                                       "of", "0"])
    def test_read_fallback_off_is_met_exactly_when_dn20c_refuses(self, value,
                                                                  monkeypatch):
        from core import read_fallback

        env = dict(MET_ENV)
        if value is None:
            env.pop(read_fallback.ENV)
            monkeypatch.delenv(read_fallback.ENV, raising=False)
        else:
            env[read_fallback.ENV] = value
            monkeypatch.setenv(read_fallback.ENV, value)
        met = "read_fallback_off" not in [
            u.key for u in wc.evaluate_preconditions(env, MET_FACTS)]
        assert met is (read_fallback.configure_mode() == read_fallback.OFF)


class TestEveryReadSwitchIsDecided:
    """A read switch added later must be put on the list or excluded by name —
    the walk, not the list, is what finds it.

    It reads every string constant in every node, across `core/`, `web/` and
    `bot/`: the first walk read module-level `NAME = "KS_READ_..."` in `core/`
    alone, and the review declared a reader three ways it never saw — an
    annotated assignment, an inline `os.getenv(...)` (which is how
    `KS_SMS_STORE` has always been read) and a module under `web/`. Removing
    `KS_SMS_STORE` from the list left the suite green."""

    SWITCH = re.compile(r"KS_READ_[A-Z][A-Z_]*[A-Z]|KS_[A-Z][A-Z_]*_STORE")
    ROOTS = ("core", "web", "bot")
    # The list itself: counting what it names would make the walk vacuous.
    SKIP = {pathlib.Path("core/warehouse_cutover.py")}

    # Switches that are not readers of Silver, Gold or UTM, each by its reason.
    NOT_READERS = {
        # How every reader fails, not what it reads — its own precondition.
        "KS_READ_FALLBACK",
        # Where the bot's approval list and preferences live: app.*, which
        # nothing derives and step 13 does not freeze.
        "KS_BOT_STORE",
        # Where the dashboard's user list and role matrix live: app.* too.
        "KS_USER_STORE",
    }

    @classmethod
    def _names_in(cls, source: str) -> set:
        return {node.value for node in ast.walk(ast.parse(source))
                if isinstance(node, ast.Constant) and isinstance(node.value, str)
                and cls.SWITCH.fullmatch(node.value)}

    @classmethod
    def _declared(cls) -> dict:
        """`{switch: {module that names it, ...}}`."""
        found: dict = {}
        for root in cls.ROOTS:
            for path in sorted((REPO / root).rglob("*.py")):
                rel = path.relative_to(REPO)
                if rel in cls.SKIP or "node_modules" in rel.parts:
                    continue
                for name in cls._names_in(path.read_text(encoding="utf-8")):
                    found.setdefault(name, set()).add(rel.as_posix())
        return found

    def test_every_read_switch_is_a_precondition_or_excluded_by_name(self):
        declared = self._declared()
        assert len(declared) >= 22, "the walk is not looking"
        decided = set(wc.WAREHOUSE_READERS) | {wc.COHORTS} | self.NOT_READERS
        undecided = {name: sorted(where) for name, where in declared.items()
                     if name not in decided}
        assert undecided == {}, (
            "a read switch nobody decided about: put it on WAREHOUSE_READERS "
            "or name it in NOT_READERS with its reason")

    @pytest.mark.parametrize("source", [
        'ENV: str = "KS_READ_NEWTAB"\n',
        'import os\n\ndef mode():\n    return os.getenv("KS_READ_NEWTAB", "duckdb")\n',
        'class Reader:\n    FLAG = "KS_READ_NEWTAB"\n',
        'def read(flag="KS_NEWTAB_STORE"):\n    return flag\n',
    ], ids=["annotated", "inline-getenv", "class-attribute", "store-default-arg"])
    def test_it_sees_every_shape_a_switch_is_declared_in(self, source):
        assert self._names_in(source) & {"KS_READ_NEWTAB", "KS_NEWTAB_STORE"}

    def test_it_walks_web_and_bot_as_well_as_core(self):
        assert set(self.ROOTS) == {"core", "web", "bot"}
        declared = self._declared()
        # `bot/main.py` reads both stores inline, beside the modules in `core/`
        # that declare them.
        assert "bot/main.py" in declared["KS_BOT_STORE"]
        assert "bot/main.py" in declared["KS_USER_STORE"]

    def test_the_sms_store_is_found_where_the_sms_tab_reads_it(self):
        """Inline `os.getenv("KS_SMS_STORE", ...)` in `core/pg_sms.py` — the
        shape the first walk could not see — and on the list."""
        assert "core/pg_sms.py" in self._declared()["KS_SMS_STORE"]
        assert "KS_SMS_STORE" in wc.WAREHOUSE_READERS

    def test_the_list_names_only_switches_that_exist(self):
        declared = set(self._declared())
        assert set(wc.WAREHOUSE_READERS) | {wc.COHORTS} <= declared
        assert self.NOT_READERS <= declared


# ─── A page under a retired check holds the switch ───────────────────────────


class TestAPageUnderARetiredCheckHoldsTheSwitch:
    """A stood-down check is not a raised one, so the first integrity run under
    the switch holds none of its conditions and announces every page it had
    delivered "✅ Resolved" — a recovery no check looked at. The switch waits
    for there to be none (`retired_conditions_clear`) rather than holding them
    for as long as Postgres derives."""

    def test_they_are_the_stood_down_checks_conditions(self):
        """And, since DN-29 retires `reconcile_gold` with them, the Gold
        comparison's own two."""
        assert wc.retired_conditions() == {
            "silver_missing_rows", "silver_orphan_rows", "silver_row_values",
            "attribution_coverage_website", "gold_cell_values",
            "headline_vs_line_items", "goods_shipped_without_sale",
            "gold_missing_cells", "gold_orphan_cells"}

    @staticmethod
    def _still_firing(tmp_path, monkeypatch, mode):
        """What the integrity job holds as firing, every stood-down check
        raising if it is called at all — as one over a frozen table may."""
        from core import data_quality as dq
        from core.duckdb_store import DuckDBStore
        from core.scheduler import BackgroundScheduler

        for env in ("KS_WRITE_EXPENSES", "KS_WRITE_INVENTORY", "KS_WRITE_GOALS",
                    "KS_DQ_PG_WAREHOUSE"):
            monkeypatch.delenv(env, raising=False)
        monkeypatch.setattr(wc, "_mode", mode)
        for fn in ("_silver_arc_check", "_attribution_coverage_check",
                   "_gold_cell_values_check", "_headline_vs_line_items_check",
                   "_goods_shipped_without_sale_check"):
            monkeypatch.setattr(dq, fn, lambda *_a, **_k: (_ for _ in ()).throw(
                RuntimeError("frozen")))
        store = DuckDBStore(db_path=tmp_path / f"{mode}.duckdb")
        asyncio.run(store.connect())
        scheduler = BackgroundScheduler()
        monkeypatch.setattr(scheduler, "_send_dq_alert_throttled",
                            AsyncMock(return_value=True))
        resolve = AsyncMock(return_value=0)
        try:
            with patch("core.duckdb_store.get_store", new=AsyncMock(return_value=store)), \
                 patch("core.alerting.resolve_group", resolve):
                asyncio.run(scheduler._run_dq_integrity())
        finally:
            asyncio.run(store.close())
        (call,) = [c for c in resolve.await_args_list
                   if c.args and c.args[0] == "dq:integrity"]
        return set(call.kwargs["still_firing"])

    def test_they_are_exactly_what_the_stand_down_stops_holding(self, tmp_path, monkeypatch):
        """The review's reproduction as the definition: a raised check's
        conditions are held, a stood-down one's are not, and the difference is
        the set the precondition reads."""
        from core.data_quality import GUARDED_CHECK_CONDITIONS

        held_when_raised = self._still_firing(tmp_path, monkeypatch, wc.DUCKDB)
        held_when_stood_down = self._still_firing(tmp_path, monkeypatch, wc.POSTGRES)
        integrity = {c for guard in wc.STOOD_DOWN_WHEN_POSTGRES
                     for c in GUARDED_CHECK_CONDITIONS[guard]}
        assert held_when_raised - held_when_stood_down == integrity
        assert integrity <= wc.retired_conditions()

    @staticmethod
    def _unmet(**delivered):
        from core.alerting import _gate

        for key, group in delivered.items():
            _gate.note_delivered_conditions([key], group)
        facts = asyncio.run(wc.gather_facts({}))
        return [u for u in wc.evaluate_preconditions(MET_ENV, facts)
                if u.key == "retired_conditions_clear"]

    def test_a_delivered_page_under_one_holds_it_and_is_named(self):
        (u,) = self._unmet(silver_missing_rows="dq:integrity")
        assert "silver_missing_rows (dq:integrity)" in u.detail

    def test_whatever_group_delivered_it(self):
        """`gold_cell_values` is also the mirror-landing Gold comparison's,
        which DN-29 retires in the job itself."""
        (u,) = self._unmet(gold_cell_values="dq:mirror_landing")
        assert "gold_cell_values" in u.detail

    def test_a_page_a_check_that_stays_up_owns_is_not_its_business(self):
        assert self._unmet(inventory_snapshot_gaps="dq:integrity",
                           pg_silver_missing_rows="dq:integrity") == []

    def test_once_announced_resolved_it_holds_nothing(self):
        from core.alerting import _gate

        _gate.note_delivered_conditions(["silver_row_values"], "dq:integrity")
        assert set(_gate.take_resolved("dq:integrity")) == {"silver_row_values"}
        assert self._unmet() == []

    def test_an_unreadable_gate_is_unmet_by_its_class(self):
        with patch("core.alerting.delivered_conditions",
                   side_effect=RuntimeError("state at /secret/path")):
            facts = asyncio.run(wc.gather_facts({}))
        assert facts.open_retired is None and facts.open_retired_error == "RuntimeError"
        (u,) = [u for u in wc.evaluate_preconditions(MET_ENV, facts)
                if u.key == "retired_conditions_clear"]
        assert "RuntimeError" in u.detail and "/secret/path" not in u.detail


# ─── The DN-12 tripwire, at run time ─────────────────────────────────────────


class TestTheBridgeTripwire:
    def test_it_is_green_today(self):
        from core.repositories.goals import sales_type_bridge_owners

        assert sales_type_bridge_owners() == {}

    def test_it_watches_the_tables_the_static_tripwire_watches(self):
        """Two answers to one question — CI's and the deployed build's — must
        be about the same tables."""
        from core.repositories.goals import SALES_TYPE_BRIDGE_TABLES

        tree = ast.parse((REPO / "tests/unit/test_goals_off_duckdb_silver.py")
                         .read_text(encoding="utf-8"))
        (assign,) = [n for n in tree.body if isinstance(n, ast.Assign)
                     and any(isinstance(t, ast.Name) and t.id == "PREDICATE_TABLES"
                             for t in n.targets)]
        tables = {n.value for n in ast.walk(assign.value)
                  if isinstance(n, ast.Constant) and isinstance(n.value, str)}
        assert tables == set(SALES_TYPE_BRIDGE_TABLES)

    @staticmethod
    def _chain(monkeypatch, table, env):
        import types

        from core import write_chains

        chain = types.ModuleType("core.pg_chain_x_write")
        chain.WRITE_ENV = "KS_WRITE_CHAIN_X"
        chain.CHAIN_TABLES = ("app.other", table)
        chain.env_writes_postgres = env
        monkeypatch.setattr(write_chains, "WRITE_CHAINS",
                            write_chains.WRITE_CHAINS + (chain,))
        return chain

    @pytest.mark.parametrize("table", ["bronze.orders", "bronze.managers",
                                       "app.manager_classifications"])
    def test_a_chain_declaring_one_with_its_flag_off_moves_nothing(self, monkeypatch, table):
        """Registered is not moved (chain 3, 2026-10-01): its writes still go
        to DuckDB, so the bridge reads what it always read. Mutation: count
        the declaration — a flag-off registration would then fail step 13's
        `goals_bridge` at the next start, a full DuckDB rebuild for nothing."""
        from core.repositories.goals import sales_type_bridge_owners

        self._chain(monkeypatch, table, lambda: False)
        assert sales_type_bridge_owners() == {}

    @pytest.mark.parametrize("state", ["flag", "latched", "typo"])
    def test_a_chain_that_moved_trips_it(self, monkeypatch, state):
        """Its writes left DuckDB — by its flag, by a latch whatever the flag
        says, or by a flag nobody can read (stood down, so written nowhere)."""
        from core import chain_latch
        from core.repositories.goals import sales_type_bridge_owners

        def typo():
            raise RuntimeError("KS_WRITE_CHAIN_X='postgrse' is not understood")

        self._chain(monkeypatch, "bronze.orders",
                    {"flag": lambda: True, "latched": lambda: False,
                     "typo": typo}[state])
        if state == "latched":
            chain_latch.latch("pg_chain_x_write")
        assert sales_type_bridge_owners() == {"pg_chain_x_write": ("bronze.orders",)}

    @pytest.mark.parametrize("table", ["bronze.orders", "bronze.managers",
                                       "app.manager_classifications"])
    def test_a_chain_owning_one_trips_it_and_is_named(self, monkeypatch, table):
        from core.repositories.goals import sales_type_bridge_owners

        self._chain(monkeypatch, table, lambda: True)
        assert sales_type_bridge_owners() == {"pg_chain_x_write": (table,)}

        facts = asyncio.run(wc.gather_facts({}))
        env = {**MET_ENV, "KS_GOALS_HISTORY": "bridge"}
        (u,) = [u for u in wc.evaluate_preconditions(env, facts)
                if u.key == "goals_bridge"]
        assert "pg_chain_x_write" in u.detail and table in u.detail
        assert "diverging" in u.detail
        # Ported, the chain owning it changes nothing.
        assert [u for u in wc.evaluate_preconditions(MET_ENV, facts)
                if u.key == "goals_bridge"] == []


# ─── Gathering and publishing ────────────────────────────────────────────────


class TestGatheringTheFacts:
    def test_without_a_dsn_postgres_is_not_asked(self):
        ask = AsyncMock()
        with patch("core.pg.current_revision", ask):
            facts = asyncio.run(wc.gather_facts({}))
        ask.assert_not_awaited()
        assert facts.revision is None and "not asked" in facts.revision_error
        assert facts.required_revision == REQUIRED_REVISION

    def test_with_a_dsn_it_reads_the_revision(self):
        with patch("core.pg.current_revision",
                   AsyncMock(return_value=REQUIRED_REVISION)):
            facts = asyncio.run(wc.gather_facts({"KS_PG_DSN": "postgresql://x"}))
        assert facts.revision == REQUIRED_REVISION and facts.revision_error is None

    # What asyncpg says to a wrong password and to a closed port — the review
    # measured both on a real server: the user, the host and the port.
    DRIVER_TEXT = ('password authentication failed for user "ks_app"',
                   "[Errno 61] Connect call failed ('127.0.0.1', 55582)")

    @pytest.mark.parametrize("text", DRIVER_TEXT)
    def test_a_read_that_raises_is_a_reason_by_its_class_alone(self, text, caplog):
        with caplog.at_level(logging.ERROR, logger="core.warehouse_cutover"), \
                patch("core.pg.current_revision", AsyncMock(side_effect=OSError(text))):
            facts = asyncio.run(wc.gather_facts({"KS_PG_DSN": "postgresql://x"}))
        assert facts.revision is None and facts.revision_error == "OSError"
        (u,) = [u for u in wc.evaluate_preconditions(MET_ENV, facts)
                if u.key == "pg_revision"]
        assert "OSError" in u.detail and text not in u.detail
        assert text in caplog.text   # the whole of it, where a person looks next

    def test_a_read_that_hangs_is_bounded(self, monkeypatch):
        async def hang(**_kwargs):
            await asyncio.sleep(10)

        monkeypatch.setattr(wc, "REVISION_READ_TIMEOUT_S", 0.05)
        with patch("core.pg.current_revision", hang):
            facts = asyncio.run(wc.gather_facts({"KS_PG_DSN": "postgresql://x"}))
        assert facts.revision is None and "did not answer" in facts.revision_error

    def test_a_database_never_migrated_says_so(self):
        with patch("core.pg.current_revision", AsyncMock(return_value=None)):
            facts = asyncio.run(wc.gather_facts({"KS_PG_DSN": "postgresql://x"}))
        assert "no Alembic revision" in facts.revision_error

    # ── the expenses history, asked of a Postgres that said its revision ──

    DSN = {"KS_PG_DSN": "postgresql://x"}

    def _gather(self, history, revision=REQUIRED_REVISION):
        with patch("core.pg.current_revision", AsyncMock(return_value=revision)), \
                patch.object(wc, "_expenses_backfilled", history):
            return asyncio.run(wc.gather_facts(self.DSN))

    def test_with_a_revision_it_asks_whether_the_expenses_hold_history(self):
        history = AsyncMock(return_value=True)
        facts = self._gather(history)
        history.assert_awaited_once_with()   # on the pool, not a connection of its own
        assert facts.expenses_backfilled is True and facts.expenses_backfill_error is None

    def test_no_backfill_is_an_answer_and_unmet_with_its_levers(self):
        facts = self._gather(AsyncMock(return_value=False))
        assert facts.expenses_backfilled is False and facts.expenses_history_row
        (u,) = [u for u in wc.evaluate_preconditions(MET_ENV, facts)
                if u.key == "expenses_backfilled"]
        assert "backfilled_at is NULL for bronze.expenses" in u.detail
        assert "KS_READ_EXPENSES" in u.detail
        assert "/api/mirror/backfill/expenses" in u.detail

    def test_no_row_at_all_is_the_same_verdict_and_says_so(self):
        """The read gate treats a missing row as no history, and so does the
        switch — but the detail names the state it found: nothing has shipped
        the table here yet, which is not a backfill left half done."""
        facts = self._gather(AsyncMock(return_value=None))
        assert facts.expenses_backfilled is False and not facts.expenses_history_row
        assert facts.expenses_backfill_error is None
        (u,) = [u for u in wc.evaluate_preconditions(MET_ENV, facts)
                if u.key == "expenses_backfilled"]
        assert "no row for bronze.expenses" in u.detail
        assert "is NULL" not in u.detail
        assert "/api/mirror/backfill/expenses" in u.detail

    def test_a_postgres_that_did_not_say_its_revision_is_not_asked(self):
        """That is `pg_revision`'s, and names it alone."""
        history = AsyncMock(return_value=True)
        with patch("core.pg.current_revision", AsyncMock(side_effect=OSError("down"))), \
                patch.object(wc, "_expenses_backfilled", history):
            facts = asyncio.run(wc.gather_facts(self.DSN))
        history.assert_not_awaited()
        assert [u.key for u in wc.evaluate_preconditions(MET_ENV, facts)] == ["pg_revision"]
        history.reset_mock()
        facts = self._gather(history, revision=None)       # never migrated
        history.assert_not_awaited()
        assert [u.key for u in wc.evaluate_preconditions(MET_ENV, facts)] == ["pg_revision"]

    @pytest.mark.parametrize("text", DRIVER_TEXT)
    def test_a_history_read_that_raises_is_unmet_by_its_class(self, text, caplog):
        with caplog.at_level(logging.ERROR, logger="core.warehouse_cutover"):
            facts = self._gather(AsyncMock(side_effect=OSError(text)))
        assert facts.revision == REQUIRED_REVISION
        assert facts.expenses_backfilled is None and facts.expenses_backfill_error == "OSError"
        (u,) = wc.evaluate_preconditions(MET_ENV, facts)
        assert u.key == "expenses_backfilled"
        assert "OSError" in u.detail and text not in u.detail
        assert text in caplog.text

    def test_a_history_read_that_hangs_is_bounded(self, monkeypatch):
        async def hang():
            await asyncio.sleep(10)

        monkeypatch.setattr(wc, "REVISION_READ_TIMEOUT_S", 0.05)
        facts = self._gather(hang)
        assert facts.expenses_backfilled is None
        assert "did not answer" in facts.expenses_backfill_error

    def test_the_read_itself(self):
        """What it asks, and that no row is told apart from a NULL."""
        class Conn:
            def __init__(self, answer):
                self.answer, self.asked = answer, None

            async def fetchval(self, sql, *args):
                self.asked = (sql, args)
                return self.answer

        # None is no row; the read gate reads it as False, and the switch
        # does too (`expenses_backfilled`), only saying which it found.
        for answer, expected in ((True, True), (False, False), (None, None)):
            conn = Conn(answer)
            assert asyncio.run(wc._expenses_backfilled(conn)) is expected
            assert conn.asked == (wc._EXPENSES_HISTORY_SQL, (wc.EXPENSES_HISTORY,))

    def test_it_asks_what_the_read_gate_asks(self):
        """One question, two askers: the switch must not be satisfied by a
        different column or table than the one `_expenses_run` routes on."""
        from core.pg_expense_backfill import EXPENSES_TABLE

        assert wc.EXPENSES_HISTORY == EXPENSES_TABLE
        tree = ast.parse((REPO / "core/pg_expenses_read.py").read_text(encoding="utf-8"))
        (fn,) = [n for n in ast.walk(tree)
                 if isinstance(n, ast.AsyncFunctionDef) and n.name == "backfilled"]
        statements = {" ".join(n.value.split()) for n in ast.walk(fn)
                      if isinstance(n, ast.Constant) and isinstance(n.value, str)}
        assert " ".join(wc._EXPENSES_HISTORY_SQL.split()) in statements

    def test_a_registry_that_raises_is_a_reason_by_its_class(self, caplog):
        with caplog.at_level(logging.ERROR, logger="core.warehouse_cutover"), \
                patch("core.repositories.goals.sales_type_bridge_owners",
                      side_effect=RuntimeError("registry at /opt/app")):
            facts = asyncio.run(wc.gather_facts({}))
        assert facts.bridge_owners is None and facts.bridge_error == "RuntimeError"
        assert "registry at /opt/app" in caplog.text


class TestReadiness:
    @pytest.fixture(autouse=True)
    def _od10_answered(self, monkeypatch):
        """The readiness of a build with no door OD-10 would have to decide —
        this one since 2026-09-30, pinned here so a door added later is
        `od10_doors`' own tests' business, not every readiness test's."""
        monkeypatch.setattr(wc, "OD10_DOORS", ())
        # The expenses history, asked on the pool beside the revision; read
        # for real in TestGatheringTheFacts and against a live Postgres.
        monkeypatch.setattr(wc, "_expenses_backfilled", AsyncMock(return_value=True))

    def test_it_publishes_the_mode_and_every_unmet_item(self, fresh, monkeypatch):
        monkeypatch.setenv(wc.ENV, "postgres")
        fresh.configure_mode()
        with patch("core.pg.current_revision",
                   AsyncMock(return_value=REQUIRED_REVISION)):
            body = asyncio.run(wc.readiness({**MET_ENV, "KS_UTM_PARSE": "duckdb"}))
        assert body["variable"] == "KS_WRITE_WAREHOUSE"
        assert (body["value"], body["mode"], body["switch_built"]) == (
            "postgres", "duckdb", True)
        assert body["preconditions_met"] is False
        assert [u["key"] for u in body["unmet"]] == ["utm_parse_postgres"]
        assert body["preconditions"] == KEYS
        assert body["stood_down_duckdb_checks"] == []

    def test_all_met_says_so_and_a_readiness_switches_nothing(self, fresh):
        fresh.configure_mode()
        with patch("core.pg.current_revision",
                   AsyncMock(return_value=REQUIRED_REVISION)):
            body = asyncio.run(wc.readiness(MET_ENV))
        assert body["preconditions_met"] is True and body["unmet"] == []
        assert body["mode"] == "duckdb" and not wc.writes_postgres()

    def test_the_dsn_is_never_published(self, fresh):
        with patch("core.pg.current_revision",
                   AsyncMock(return_value=REQUIRED_REVISION)):
            body = asyncio.run(wc.readiness(MET_ENV))
        assert "ks_app:x" not in repr(body) and "http://ch:8123" not in repr(body)


class TestTheReadersNotOnPostgresAreOneReading:
    """`readers_not_on_postgres` is what the switch files as `reader:*` and
    what chain 6 holds its catalogue writes on (`pg_catalogue_write`). One
    reading, so the two cannot disagree about which readers still read DuckDB."""

    @pytest.mark.parametrize("off", [(), ("KS_READ_GOLD",),
                                     ("KS_SMS_STORE", "KS_READ_CHAT"),
                                     tuple(wc.WAREHOUSE_READERS)])
    def test_it_is_exactly_the_reader_keys_the_switch_files(self, off):
        env = {**MET_ENV, **{name: "duckdb" for name in off}}
        filed = {u.key for u in wc.evaluate_preconditions(env, MET_FACTS)
                 if u.key.startswith("reader:")}
        assert filed == {f"reader:{n}" for n in wc.readers_not_on_postgres(env)}
        assert set(wc.readers_not_on_postgres(env)) == set(off)

    def test_unset_and_a_typo_are_not_postgres_and_case_is(self):
        env = dict(MET_ENV)
        env.pop("KS_READ_GOLD")
        env["KS_READ_SILVER"] = "postgress"
        env["KS_READ_DASHBOARD"] = " PostgreS "
        assert wc.readers_not_on_postgres(env) == ("KS_READ_GOLD", "KS_READ_SILVER")

    def test_in_the_lists_order(self):
        assert wc.readers_not_on_postgres({}) == tuple(wc.WAREHOUSE_READERS)
