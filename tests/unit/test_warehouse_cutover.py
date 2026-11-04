"""Step 13a (DN-28): the warehouse writer's mode, what it stands down, and the
precondition evaluator. The switch those feed is DN-29's, and its own tests are
in `tests/unit/test_warehouse_writer.py`; what is pinned here of it is that
`postgres` with a precondition unmet still runs as `duckdb`.

Since step 14's S14-00 the mode is a constant, `postgres`, and every test of
the mode's choosing went with it; the evaluator stays as the status page's
report until S14-15.

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


    def test_this_build_carries_the_switch(self):
        assert wc.SWITCH_BUILT is True


# ─── Where a typo is seen ────────────────────────────────────────────────────


class TestAnUnknownValueIsSeen:
    """Published on `/api/health` and judged by the canary, as KS_PG_DERIVE
    and KS_READ_FALLBACK are. The review found it only in a log line and on
    the admin status page, which nothing watches: in this build a typo costs
    nothing, and the first time it would is the DN-29 flip."""


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
                     expenses_backfilled=True)


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
# Since step 14's S14-00 `pg_landing.mirror_on` is True for any value, so the
# landing mirror's precondition can no longer be unmet; the evaluator stays,
# as `/api/warehouse/status`'s report, until S14-15.
BREAKABLE = [key for key in KEYS if key != "mirror_landing"]


class TestTheEvaluator:
    def test_all_met_is_ready(self):
        assert wc.evaluate_preconditions(MET_ENV, MET_FACTS) == []

    def test_the_keys_are_unique(self):
        assert len(KEYS) == len(set(KEYS))

    @pytest.mark.parametrize("key", BREAKABLE)
    def test_each_unmet_on_its_own_is_named_and_nothing_else(self, key):
        env, facts = _breaking(key)
        unmet = wc.evaluate_preconditions(env, facts)
        assert [u.key for u in unmet] == [key]
        assert unmet[0].detail

    def test_the_plans_list_is_all_there(self):
        """The DN-28 list, item by item: own; twins on; UTM parse postgres;
        fallback off; DSN and revision; KS_MIRROR_LANDING; every Silver, Gold
        and UTM reader postgres; cohorts with KS_CH_URL. The DN-12 tripwire
        (`goals_bridge`) left the list with the bridge (chain 7b-4)."""
        assert {"pg_derive_own", "pg_twins_on", "utm_parse_postgres",
                "read_fallback_off", "pg_dsn", "pg_revision", "mirror_landing",
                "cohorts_clickhouse", "ch_url"} <= set(KEYS)
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
            expenses_backfilled=False,
            open_retired=None, open_retired_error="x", od10_doors=A_DOOR))
        assert [u.key for u in unmet] == BREAKABLE

    def test_it_reads_values_as_the_modules_do(self):
        env = {**MET_ENV, "KS_PG_DERIVE": " OWN ", "KS_READ_GOLD": "Postgres"}
        assert wc.evaluate_preconditions(env, MET_FACTS) == []

    def test_an_empty_environment_names_everything_it_can(self):
        unmet = wc.evaluate_preconditions({}, wc.Facts(
            revision_error="not asked: KS_PG_DSN is not set"))
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
                         required_revision=REQUIRED_REVISION)
        (u,) = wc.evaluate_preconditions(MET_ENV, facts)
        assert u.key == "pg_revision" and "SchemaVersionError" in u.detail

    @pytest.mark.parametrize("value", [None, "", "silver", "bridge", "silvr"])
    def test_the_goal_history_is_no_precondition_any_more(self, value):
        """Chain 7b-4 deleted the DN-12 bridge: no goal calculator reads
        DuckDB's orders, managers or manager_classifications under any value
        of the retired `KS_GOALS_HISTORY` — until step 14's S14-07 a value
        other than silver refused every goal history read and the canary
        paged it (`goals_history_mode_invalid`); since then nothing reads it
        — so step 13 has nothing to wait for.
        What makes the goal history Postgres' is `reader:KS_READ_GOALS` and
        `read_fallback_off`, both still here. Mutation: put `goals_bridge`
        back in `PRECONDITIONS` — `bridge`, unset or a typo is unmet again."""
        env = dict(MET_ENV)
        if value is not None:
            env["KS_GOALS_HISTORY"] = value
        assert wc.evaluate_preconditions(env, MET_FACTS) == []
        assert "goals_bridge" not in KEYS
        assert not [f for f in dataclasses.fields(wc.Facts) if "bridge" in f.name]

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
    # Read by nothing since step 14's S14-05, whose six routers ask Postgres
    # whatever these say, S14-06, whose dashboard does the same with the next
    # three, and S14-07, the rest. Still on `WAREHOUSE_READERS` — the
    # cutover's readiness on `/api/warehouse/status` reads the environment
    # for them until S14-15 removes the cutover — so production's answer
    # there is unchanged; no module declares them any more. The cohorts'
    # switch (`COHORTS`) went with S14-07 too, and is asked apart below.
    RETIRED = {"KS_READ_TRAFFIC", "KS_READ_MARGIN", "KS_READ_PRODUCTS_INTEL",
               "KS_READ_REPORTS", "KS_READ_MARKETING", "KS_READ_LOOKUPS",
               "KS_READ_GOLD", "KS_READ_SILVER", "KS_READ_DASHBOARD",
               "KS_READ_EXPENSES", "KS_READ_INVENTORY", "KS_READ_GOALS",
               "KS_READ_WEEKLY", "KS_READ_FORECAST_INPUT", "KS_READ_SEARCH_INDEX",
               "KS_READ_CHAT", "KS_READ_BUYER_SYNC"}

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
        # 22 until S14-05 retired six, S14-06 three more and S14-07 nine
        # (`RETIRED`, and the cohorts'): the four left are the stores' and
        # the fallback's (`NOT_READERS`, `KS_SMS_STORE`).
        assert len(declared) >= 4, "the walk is not looking"
        assert {"KS_SMS_STORE"} | self.NOT_READERS <= set(declared)
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
        # `bot/main.py` reads the bot's store inline, beside the module in
        # `core/` that declares it. (It read `KS_USER_STORE` too until S14-00
        # made the user list's store a constant.)
        assert "bot/main.py" in declared["KS_BOT_STORE"]

    def test_the_sms_store_is_found_and_on_the_list(self):
        """`core/pg_sms.py` read it inline until S14-00 made the SMS store a
        constant; the name is still declared (the table-fates manifest) and
        still on the list the report reads."""
        assert self._declared()["KS_SMS_STORE"]
        assert "KS_SMS_STORE" in wc.WAREHOUSE_READERS

    def test_the_list_names_only_switches_that_exist(self):
        declared = set(self._declared())
        assert set(wc.WAREHOUSE_READERS) - self.RETIRED <= declared
        assert self.NOT_READERS <= declared

    def test_a_retired_switch_is_declared_nowhere(self):
        """Mutation: give one of S14-05's or S14-06's routers its switch
        back — it is then a reader again and must come off `RETIRED`."""
        assert not self.RETIRED & set(self._declared())
        assert self.RETIRED <= set(wc.WAREHOUSE_READERS)
        # The cohorts' switch, which `ch_cohorts` declared until S14-07.
        assert wc.COHORTS not in self._declared()


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

    def test_gathering_asks_no_write_chain_its_mode(self, monkeypatch):
        """The registry walk went with the DN-12 bridge (chain 7b-4): step
        13's facts no longer ask any write chain for its mode, so its start
        cannot ask chain 3 or 5 a question whose answer waits on step 13's
        own verdict. Mutation: walk the chains' modes in `_local_facts`
        again, as `sales_type_bridge_owners` did — inside its `try`, so only
        a record of the asking can see it."""
        from core import write_chains

        asked = []

        def chain_state(chain, *a, **k):
            asked.append(write_chains.chain_name(chain))
            return {"mode": "duckdb", "error": None}

        def chain_modes(*a, **k):
            asked.append("chain_modes")
            return {}

        monkeypatch.setattr(write_chains, "_chain_state", chain_state)
        monkeypatch.setattr(write_chains, "chain_modes", chain_modes)
        facts = asyncio.run(wc.gather_facts({}))
        assert asked == []
        assert facts.open_retired_error is None


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
        # Since S14-00 an unmet item is a report: the mode is `postgres`.
        assert (body["value"], body["mode"], body["switch_built"]) == (
            "postgres", "postgres", True)
        assert body["preconditions_met"] is False
        assert [u["key"] for u in body["unmet"]] == ["utm_parse_postgres"]
        assert body["preconditions"] == KEYS
        assert body["stood_down_duckdb_checks"] == sorted(wc.stood_down_duckdb_checks())

    def test_all_met_says_so_and_a_readiness_switches_nothing(self, fresh):
        fresh.configure_mode()
        with patch("core.pg.current_revision",
                   AsyncMock(return_value=REQUIRED_REVISION)):
            body = asyncio.run(wc.readiness(MET_ENV))
        assert body["preconditions_met"] is True and body["unmet"] == []
        assert body["mode"] == "postgres" and wc.writes_postgres()     # S14-00

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
