"""Step 13a (DN-28): the warehouse writer's mode, what it stands down, and the
precondition evaluator. Nothing here switches, and these pin that too.

The integrity plumbing the stand-down feeds — the scan skipping the checks and
the twins standing in — is in `tests/unit/test_pg_warehouse_dq.py`; the
Postgres-only Gold check is in `tests/unit/test_pg_gold.py` and
`tests/unit/test_mirror_landing_isolation.py`.
"""
from __future__ import annotations

import ast
import asyncio
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
    """The state DN-29 makes reachable. This build never configures it, so the
    plumbing that consumes it is proven by setting it directly."""
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

    def test_postgres_is_read_and_published_and_not_acted_on(
        self, fresh, monkeypatch, caplog,
    ):
        """The switch is DN-29. Half of it — the checks stood down while
        DuckDB goes on deriving — must not be reachable from the variable."""
        monkeypatch.setenv(wc.ENV, " Postgres ")
        with caplog.at_level(logging.WARNING, logger="core.warehouse_cutover"):
            assert fresh.configure_mode() == "duckdb"
        assert fresh.value() == "postgres" and fresh.mode_error() is None
        assert not fresh.writes_postgres()
        assert fresh.stood_down_duckdb_checks() == frozenset()
        assert "DN-29" in caplog.text
        status = fresh.status()
        assert (status["value"], status["mode"], status["switch_built"]) == (
            "postgres", "duckdb", False)

    def test_this_build_does_not_carry_the_switch(self):
        assert wc.SWITCH_BUILT is False

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
}
MET_FACTS = wc.Facts(revision=REQUIRED_REVISION, required_revision=REQUIRED_REVISION,
                     bridge_owners={})


def _breaking(key: str):
    """`(env, facts)` with every precondition met except `key`."""
    env, facts = dict(MET_ENV), MET_FACTS
    simple = {
        "pg_derive_own": ("KS_PG_DERIVE", "piggyback"),
        "pg_twins_on": ("KS_DQ_PG_WAREHOUSE", None),
        "utm_parse_postgres": ("KS_UTM_PARSE", "duckdb"),
        "read_fallback_off": ("KS_READ_FALLBACK", None),
        "pg_dsn": ("KS_PG_DSN", None),
        "mirror_landing": ("KS_MIRROR_LANDING", "0"),
        "cohorts_clickhouse": ("KS_READ_COHORTS", "duckdb"),
        "ch_url": ("KS_CH_URL", " "),
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
        facts = wc.Facts(revision="0032_manual_goal_ids",
                         required_revision=REQUIRED_REVISION, bridge_owners={})
    elif key == "goals_bridge":
        facts = wc.Facts(revision=REQUIRED_REVISION, required_revision=REQUIRED_REVISION,
                         bridge_owners={"pg_managers_write": ("bronze.managers",)})
    elif key == "retired_conditions_clear":
        facts = wc.Facts(revision=REQUIRED_REVISION, required_revision=REQUIRED_REVISION,
                         bridge_owners={},
                         open_retired={"silver_missing_rows": "dq:integrity"})
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
        unmet = wc.evaluate_preconditions({"KS_MIRROR_LANDING": "0"}, wc.Facts(
            revision_error="x", bridge_owners=None, bridge_error="x",
            open_retired=None, open_retired_error="x"))
        assert [u.key for u in unmet] == KEYS

    def test_it_reads_values_as_the_modules_do(self):
        env = {**MET_ENV, "KS_PG_DERIVE": " OWN ", "KS_READ_GOLD": "Postgres"}
        assert wc.evaluate_preconditions(env, MET_FACTS) == []

    def test_an_empty_environment_names_everything_it_can(self):
        unmet = wc.evaluate_preconditions({}, wc.Facts(
            revision_error="not asked: KS_PG_DSN is not set", bridge_owners={}))
        assert [u.key for u in unmet] == [
            k for k in KEYS
            if k not in ("mirror_landing", "goals_bridge", "retired_conditions_clear")]

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

    def test_an_unreadable_registry_is_unmet_not_green(self):
        facts = wc.Facts(revision=REQUIRED_REVISION, required_revision=REQUIRED_REVISION,
                         bridge_owners=None, bridge_error="ImportError: gone")
        (u,) = wc.evaluate_preconditions(MET_ENV, facts)
        assert u.key == "goals_bridge" and "ImportError" in u.detail

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
        assert wc.retired_conditions() == {
            "silver_missing_rows", "silver_orphan_rows", "silver_row_values",
            "attribution_coverage_website", "gold_cell_values",
            "headline_vs_line_items", "goods_shipped_without_sale"}

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
        held_when_raised = self._still_firing(tmp_path, monkeypatch, wc.DUCKDB)
        held_when_stood_down = self._still_firing(tmp_path, monkeypatch, wc.POSTGRES)
        assert held_when_raised - held_when_stood_down == wc.retired_conditions()

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

    @pytest.mark.parametrize("table", ["bronze.orders", "bronze.managers",
                                       "app.manager_classifications"])
    def test_a_chain_owning_one_trips_it_and_is_named(self, monkeypatch, table):
        import types

        from core import write_chains
        from core.repositories.goals import sales_type_bridge_owners

        chain = types.ModuleType("core.pg_chain_x_write")
        chain.CHAIN_TABLES = ("app.other", table)
        monkeypatch.setattr(write_chains, "WRITE_CHAINS",
                            write_chains.WRITE_CHAINS + (chain,))
        assert sales_type_bridge_owners() == {"pg_chain_x_write": (table,)}

        facts = asyncio.run(wc.gather_facts({}))
        (u,) = [u for u in wc.evaluate_preconditions(MET_ENV, facts)
                if u.key == "goals_bridge"]
        assert "pg_chain_x_write" in u.detail and table in u.detail


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
        async def hang():
            await asyncio.sleep(10)

        monkeypatch.setattr(wc, "REVISION_READ_TIMEOUT_S", 0.05)
        with patch("core.pg.current_revision", hang):
            facts = asyncio.run(wc.gather_facts({"KS_PG_DSN": "postgresql://x"}))
        assert facts.revision is None and "did not answer" in facts.revision_error

    def test_a_database_never_migrated_says_so(self):
        with patch("core.pg.current_revision", AsyncMock(return_value=None)):
            facts = asyncio.run(wc.gather_facts({"KS_PG_DSN": "postgresql://x"}))
        assert "no Alembic revision" in facts.revision_error

    def test_a_registry_that_raises_is_a_reason_by_its_class(self, caplog):
        with caplog.at_level(logging.ERROR, logger="core.warehouse_cutover"), \
                patch("core.repositories.goals.sales_type_bridge_owners",
                      side_effect=RuntimeError("registry at /opt/app")):
            facts = asyncio.run(wc.gather_facts({}))
        assert facts.bridge_owners is None and facts.bridge_error == "RuntimeError"
        assert "registry at /opt/app" in caplog.text


class TestReadiness:
    def test_it_publishes_the_mode_and_every_unmet_item(self, fresh, monkeypatch):
        monkeypatch.setenv(wc.ENV, "postgres")
        fresh.configure_mode()
        with patch("core.pg.current_revision",
                   AsyncMock(return_value=REQUIRED_REVISION)):
            body = asyncio.run(wc.readiness({**MET_ENV, "KS_UTM_PARSE": "duckdb"}))
        assert body["variable"] == "KS_WRITE_WAREHOUSE"
        assert (body["value"], body["mode"], body["switch_built"]) == (
            "postgres", "duckdb", False)
        assert body["preconditions_met"] is False
        assert [u["key"] for u in body["unmet"]] == ["utm_parse_postgres"]
        assert body["preconditions"] == KEYS
        assert body["stood_down_duckdb_checks"] == []

    def test_all_met_says_so_and_still_switches_nothing(self, fresh):
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
