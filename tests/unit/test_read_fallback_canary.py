"""The canary's evidence for OD-07: a page on every read answered from DuckDB,
and a durable watch that says since when nothing was.

KS_READ_FALLBACK=off may be set only after a covered, clean week (owner
decision OD-07 (a)). The counters live in web's process and every deploy
empties them with its log, so the week is proven by the process that outlives
web's: the canary pages `read_fallback_used` whenever `/api/health`'s
`read_fallbacks` is non-empty, and rewrites one `watch:read_fallbacks` row on
every probe that read the block. `deploy/stage4_soak/22_f1_read_fallbacks.sql`
judges the two, and `tests/integration/test_stage4_soak_sql.py` holds it to
them against a real Postgres; the watch row's own arithmetic is held in
`tests/integration/test_read_fallback_watch.py`.

What is held here: what the canary reads and says, that a refusal under `off`
is never a fallback, what production's payload today does (nothing), that the
bot writes the watch on every probe that read the block, and that the writer
wears the archive's armour.
"""
from __future__ import annotations

import ast
import asyncio
import logging
import textwrap
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest

from bot import canary
from core import alert_archive
from core.alerting import REGISTRY, Kind, spec_for
from tests.unit.test_canary import DASHBOARD, _healthy_payload, _mock_transport
from tests.unit.test_read_fallback_sites import (  # noqa: F401  (fixtures)
    _seed_gold,
    admin_client,
    fresh_read_fallback,
)

REPO = Path(__file__).resolve().parents[2]
NOW = datetime(2026, 9, 30, 12, 0, tzinfo=timezone.utc)


def _entry(count, minutes_ago=5):
    return {"count": count,
            "last_at": (NOW - timedelta(minutes=minutes_ago)).isoformat(timespec="seconds")}


async def _probe(payload):
    def handler(request):
        return httpx.Response(200, json=payload)

    future = datetime.now(timezone.utc) + timedelta(days=60)
    cert = {"notAfter": future.strftime("%b %d %H:%M:%S %Y GMT")}
    async with _mock_transport(handler) as client:
        with patch.object(canary, "_fetch_peer_cert", return_value=cert):
            return await canary.run_canary(DASHBOARD, client=client)


# ─── The judge ─────────────────────────────────────────────────────────────

class TestReadFallbacks:
    def test_an_absent_block_is_not_judged_and_not_watched(self):
        """An older web publishes none, and an unreachable one publishes
        nothing: neither says anything about fallbacks, either way."""
        for payload in (None, {}, {"read_fallbacks": None}, {"read_fallbacks": []}):
            assert canary.check_read_fallbacks(payload) == []
            assert canary.read_fallbacks_clean(payload) is None

    def test_an_empty_block_is_clean(self):
        assert canary.check_read_fallbacks({"read_fallbacks": {}}) == []
        assert canary.read_fallbacks_clean({"read_fallbacks": {}}) is True

    def test_a_fallback_pages_naming_its_surfaces_and_when(self):
        block = {"traffic": _entry(1, 90), "dashboard": _entry(2, 5)}
        [(key, message)] = canary.check_read_fallbacks({"read_fallbacks": block})
        assert key == "read_fallback_used"
        assert message.startswith(canary.READ_FALLBACK_LINE)
        assert f"dashboard ×2 (last {block['dashboard']['last_at']})" in message
        assert f"traffic ×1 (last {block['traffic']['last_at']})" in message
        assert message.index("dashboard") < message.index("traffic"), "sorted"
        assert canary.read_fallbacks_clean({"read_fallbacks": block}) is False

    def test_the_key_is_the_condition_never_a_count_or_a_surface(self):
        """The charter: a key that moved with the count would be a new
        incident on every fallback, and one per surface a new one per tab."""
        one = canary.check_read_fallbacks({"read_fallbacks": {"dashboard": _entry(1)}})
        many = canary.check_read_fallbacks(
            {"read_fallbacks": {"goals": _entry(40), "cohorts": _entry(3)}})
        assert [k for k, _ in one] == [k for k, _ in many] == ["read_fallback_used"]

    def test_three_surfaces_are_named_and_the_rest_counted(self):
        block = {name: _entry(1) for name in ("a", "b", "c", "d", "e")}
        [(_, message)] = canary.check_read_fallbacks({"read_fallbacks": block})
        assert "a ×1" in message and "c ×1" in message
        assert "d ×1" not in message and message.endswith("+2 more")

    def test_a_malformed_entry_still_pages(self):
        """A message, not a parser: an odd entry costs its details, never
        the page that says a fallback happened."""
        [(key, message)] = canary.check_read_fallbacks(
            {"read_fallbacks": {"dashboard": None, "goals": {"count": "x"}}})
        assert key == "read_fallback_used"
        assert "dashboard ×? (last unknown)" in message
        assert "goals ×? (last unknown)" in message

    def test_the_canarys_key_is_a_registered_condition(self):
        assert spec_for("read_fallback_used").kind is Kind.CONDITION
        assert spec_for("read_refused").kind is Kind.CONDITION
        assert canary.READ_FALLBACK_WATCH_KEY not in REGISTRY, (
            "the watch is a record, not a condition anything may raise")


class TestRefusalsAreNotFallbacks:
    """Under KS_READ_FALLBACK=off a failing engine is answered with a 503, not
    from DuckDB. Counting that as a fallback would fail the very soak that
    licenses the flip, so it is judged on its own key, and only while recent."""

    @staticmethod
    def _off(refused):
        return {"read_fallbacks": {},
                "read_fallback_mode": {"mode": "off", "error": None,
                                       "misconfigured": [], "refused": refused}}

    def test_a_refusal_is_never_read_fallback_used(self):
        payload = self._off({"traffic": _entry(4)})
        assert canary.check_read_fallbacks(payload) == []
        assert canary.read_fallbacks_clean(payload) is True, (
            "a refusal served nothing from DuckDB; the clean week goes on")

    def test_a_recent_refusal_warns_under_its_own_key(self):
        [(key, message)] = canary.check_read_refusals(
            self._off({"traffic": _entry(4, 10)}), NOW)
        assert key == "read_refused"
        assert message.startswith(canary.READ_REFUSED_LINE)
        assert "traffic ×4" in message

    def test_an_old_refusal_is_quiet(self):
        """Unlike a fallback, a refusal left no wrong number to explain; the
        page stands while reads are being refused, not until a restart."""
        limit_min = canary.READ_REFUSED_RECENT_S // 60
        assert canary.check_read_refusals(
            self._off({"traffic": _entry(4, limit_min + 1)}), NOW) == []
        assert canary.check_read_refusals(
            self._off({"traffic": _entry(4, limit_min)}), NOW) != []

    def test_only_the_recent_surfaces_are_named(self):
        [(_, message)] = canary.check_read_refusals(self._off(
            {"traffic": _entry(4, 10), "goals": _entry(1, 300)}), NOW)
        assert "traffic" in message and "goals" not in message

    def test_a_time_that_cannot_be_read_counts_as_recent(self):
        [(key, _)] = canary.check_read_refusals(
            self._off({"traffic": {"count": 1, "last_at": "yesterday"}}), NOW)
        assert key == "read_refused"

    def test_under_duckdb_there_is_nothing_to_judge(self):
        """Production today: the block carries no `refused` at all."""
        payload = {"read_fallback_mode": {"mode": "duckdb", "error": None,
                                          "misconfigured": []}}
        assert canary.check_read_refusals(payload, NOW) == []
        assert canary.check_read_refusals({}, NOW) == []
        assert canary.check_read_refusals(self._off({}), NOW) == []


# ─── Through run_canary ────────────────────────────────────────────────────

class TestThroughRunCanary:
    @pytest.mark.asyncio
    async def test_production_today_pages_nothing(self):
        """What web publishes today, with no fallback since it started and
        KS_READ_FALLBACK unset: no key, no warn, and a clean probe for the
        watch. This is the merge: nothing new pages."""
        payload = _healthy_payload()
        payload["read_fallbacks"] = {}
        payload["read_fallback_mode"] = {"mode": "duckdb", "error": None,
                                         "misconfigured": []}
        result = await _probe(payload)
        assert result.ok and result.severity == "ok" and result.failure_keys == []
        assert result.read_fallbacks_clean is True

    @pytest.mark.asyncio
    async def test_a_fallback_warns_and_names_its_lever(self):
        payload = _healthy_payload()
        payload["read_fallbacks"] = {"dashboard": _entry(3)}
        result = await _probe(payload)
        assert result.severity == "warn"
        assert result.failure_keys == ["read_fallback_used"]
        assert result.read_fallbacks_clean is False
        assert "falling back to DuckDB" in canary._what_to_do(result)
        message = canary.format_alert(result, DASHBOARD)
        assert canary.READ_FALLBACK_LINE + "dashboard ×3" in message

    @pytest.mark.asyncio
    async def test_a_recent_refusal_warns_and_is_not_a_fallback(self):
        payload = _healthy_payload()
        payload["read_fallbacks"] = {}
        payload["read_fallback_mode"] = {
            "mode": "off", "error": None, "misconfigured": [],
            "refused": {"traffic": {"count": 2, "last_at": datetime.now(
                timezone.utc).isoformat(timespec="seconds")}}}
        result = await _probe(payload)
        assert result.severity == "warn"
        assert result.failure_keys == ["read_refused"]
        assert result.read_fallbacks_clean is True
        assert "'read refused'" in canary._what_to_do(result)

    @pytest.mark.asyncio
    async def test_a_cause_names_its_lever_before_what_it_cost(self):
        """A failing mirror and the fallback it caused: the lever is the
        mirror's, because that is what fixing looks like."""
        payload = _healthy_payload()
        payload["mirrors"]["bronze.orders"].update(failures_since_ok=4, failing=True)
        payload["read_fallbacks"] = {"dashboard": _entry(3)}
        result = await _probe(payload)
        assert set(result.failure_keys) == {"mirror_failing:bronze.orders",
                                            "read_fallback_used"}
        assert canary._what_to_do(result).startswith("Check meta.mirror_state")

    @pytest.mark.asyncio
    async def test_an_unreachable_web_is_not_a_probe_of_the_block(self):
        def handler(request):
            raise httpx.ConnectError("refused")

        async with _mock_transport(handler) as client:
            with patch.object(canary, "_fetch_peer_cert",
                              side_effect=OSError("no route")):
                result = await canary.run_canary(DASHBOARD, client=client)
        assert result.read_fallbacks_clean is None

    def test_each_new_lever_is_one_line_of_at_most_150(self):
        actions = dict(canary._ACTIONS)
        for key in ("read_fallback_used", "read_routed_to_duckdb", "read_refused"):
            assert "\n" not in actions[key] and len(actions[key]) <= 150, key


# ─── Through the app: what web publishes is what the canary reads ────────────

def _production_switches(monkeypatch, fresh_read_fallback):
    """Cohorts on ClickHouse with its address, as production has run since
    31.08; `fresh_read_fallback` unsets every other KS_READ_*."""
    monkeypatch.setenv("KS_READ_COHORTS", "clickhouse")
    monkeypatch.setenv("KS_CH_URL", "http://ks-clickhouse:8123")
    fresh_read_fallback.configure_mode()


class TestWhatWebPublishes:
    def test_no_fallback_is_a_clean_probe(
        self, admin_client, fresh_read_fallback, monkeypatch,
    ):
        _production_switches(monkeypatch, fresh_read_fallback)
        health = admin_client.get("/api/health").json()
        assert health["read_fallback_mode"]["misconfigured"] == []
        assert health["read_fallback_mode"]["no_engine"] == []
        assert canary.check_read_fallbacks(health) == []
        assert canary.check_read_routes(health) == []
        assert canary.check_read_refusals(health) == []
        assert canary.read_fallbacks_clean(health) is True
        assert canary.web_uptime_s(health) == health["uptime_seconds"]

    def test_a_switch_without_its_address_pages_and_is_not_clean(
        self, admin_client, fresh_read_fallback, monkeypatch,
    ):
        """The review's case: KS_CH_URL lost while cohorts read ClickHouse.
        Every cohort read is served from DuckDB and nothing counts one, so
        `read_fallbacks` stays empty; the watch must not call that clean."""
        monkeypatch.setenv("KS_READ_COHORTS", "clickhouse")
        monkeypatch.delenv("KS_CH_URL", raising=False)
        fresh_read_fallback.configure_mode()

        health = admin_client.get("/api/health").json()
        assert health["read_fallbacks"] == {}
        assert canary.check_read_fallbacks(health) == []
        [(key, message)] = canary.check_read_routes(health)
        assert key == "read_routed_to_duckdb"
        assert message == (canary.READ_ROUTED_LINE
                           + "KS_READ_COHORTS=clickhouse without KS_CH_URL")
        assert canary.read_fallbacks_clean(health) is False

    def test_cohorts_not_on_clickhouse_page_and_are_not_clean(
        self, admin_client, fresh_read_fallback, monkeypatch,
    ):
        """No address is missing, and `off` still refuses every cohort read:
        they have no Postgres body. Nothing but `no_engine` says so."""
        fresh_read_fallback.configure_mode()
        health = admin_client.get("/api/health").json()
        assert health["read_fallback_mode"]["misconfigured"] == []
        assert health["read_fallback_mode"]["no_engine"] == [
            "cohorts: KS_READ_COHORTS=duckdb, and only clickhouse may answer it "
            "under KS_READ_FALLBACK=off"]
        assert [k for k, _ in canary.check_read_routes(health)] == [
            "read_routed_to_duckdb"]
        assert canary.read_fallbacks_clean(health) is False

    def test_a_counted_fallback_pages_naming_the_surface(
        self, admin_client, fresh_read_fallback, monkeypatch,
    ):
        day = date.today() - timedelta(days=3)
        _seed_gold(day, 100.0, 1)
        monkeypatch.setenv("KS_READ_GOLD", "postgres")
        monkeypatch.setenv("KS_PG_DSN", "postgresql://nobody@127.0.0.1:1/none")
        monkeypatch.setattr("core.pg.get_pool",
                            AsyncMock(side_effect=OSError("connection refused")))
        admin_client.get("/api/summary", params={"start_date": day.isoformat(),
                                                 "end_date": day.isoformat()})

        health = admin_client.get("/api/health").json()
        [(key, message)] = canary.check_read_fallbacks(health)
        assert key == "read_fallback_used"
        last = health["read_fallbacks"]["dashboard"]["last_at"]
        assert f"dashboard ×1 (last {last})" in message
        assert canary.read_fallbacks_clean(health) is False

    def test_a_refusal_under_off_is_read_refused_and_the_week_stays_clean(
        self, admin_client, fresh_read_fallback, monkeypatch,
    ):
        day = date.today() - timedelta(days=3)
        monkeypatch.setenv("KS_READ_FALLBACK", "off")
        fresh_read_fallback.configure_mode()
        monkeypatch.setenv("KS_READ_GOLD", "postgres")
        monkeypatch.setenv("KS_PG_DSN", "postgresql://nobody@127.0.0.1:1/none")
        monkeypatch.setattr("core.pg.get_pool",
                            AsyncMock(side_effect=OSError("connection refused")))
        response = admin_client.get(
            "/api/summary", params={"start_date": day.isoformat(),
                                    "end_date": day.isoformat()})
        assert response.status_code == 503

        health = admin_client.get("/api/health").json()
        assert health["read_fallbacks"] == {}
        assert canary.check_read_fallbacks(health) == []
        assert canary.read_fallbacks_clean(health) is True
        assert [k for k, _ in canary.check_read_refusals(health)] == ["read_refused"]


# ─── Reads served from DuckDB by configuration ────────────────────────────

class TestRoutedReads:
    """A switch that sends reads to DuckDB with nothing to count: a read
    engine named without its address, or the cohorts' switch not naming
    ClickHouse. `off` refuses every such read, so the week that licenses it
    must not read clean over one (review of OD-07, finding 2)."""

    @staticmethod
    def _mode(mode="duckdb", misconfigured=(), no_engine=()):
        return {"read_fallbacks": {},
                "read_fallback_mode": {"mode": mode, "error": None,
                                       "misconfigured": list(misconfigured),
                                       "no_engine": list(no_engine)}}

    def test_nothing_routed_is_clean(self):
        assert canary.check_read_routes(self._mode()) == []
        assert canary.read_fallbacks_clean(self._mode()) is True

    def test_a_switch_without_its_address_pages_and_dirties_the_watch(self):
        payload = self._mode(misconfigured=["KS_READ_COHORTS=clickhouse without KS_CH_URL"])
        [(key, message)] = canary.check_read_routes(payload)
        assert key == "read_routed_to_duckdb"
        assert message == (canary.READ_ROUTED_LINE
                           + "KS_READ_COHORTS=clickhouse without KS_CH_URL")
        assert canary.check_read_fallbacks(payload) == [], (
            "not a counted fallback: its own key, its own lever")
        assert canary.read_fallbacks_clean(payload) is False

    def test_a_cohort_route_pages_and_dirties_the_watch(self):
        payload = self._mode(no_engine=["cohorts: KS_READ_COHORTS=duckdb, …"])
        assert [k for k, _ in canary.check_read_routes(payload)] == [
            "read_routed_to_duckdb"]
        assert canary.read_fallbacks_clean(payload) is False

    def test_both_lists_are_named_three_and_the_rest_counted(self):
        payload = self._mode(misconfigured=["A", "B", "C"], no_engine=["D"])
        [(_, message)] = canary.check_read_routes(payload)
        assert message == canary.READ_ROUTED_LINE + "A; B; C; +1 more"

    def test_under_off_a_route_is_a_refusal_not_a_read_from_duckdb(self):
        """Under `off` the same switch answers 503 per request, counted and
        paged as `read_refused`; nothing was served from DuckDB."""
        payload = self._mode(mode="off", misconfigured=["X=postgres without KS_PG_DSN"],
                             no_engine=["cohorts: …"])
        assert canary.check_read_routes(payload) == []
        assert canary.read_fallbacks_clean(payload) is True

    def test_an_older_web_without_the_list_is_judged_by_what_it_has(self):
        payload = {"read_fallbacks": {},
                   "read_fallback_mode": {"mode": "duckdb", "error": None,
                                          "misconfigured": []}}
        assert canary.check_read_routes(payload) == []
        assert canary.read_fallbacks_clean(payload) is True
        for junk in (None, "x", [None, ""]):
            bad = self._mode()
            bad["read_fallback_mode"]["no_engine"] = junk
            assert canary.check_read_routes(bad) == []

    def test_a_count_and_a_route_are_both_named(self):
        payload = self._mode(misconfigured=["KS_READ_GOLD=postgres without KS_PG_DSN"])
        payload["read_fallbacks"] = {"traffic": _entry(1)}
        assert {k for k, _ in canary.check_read_fallbacks(payload)
                + canary.check_read_routes(payload)} == {
            "read_fallback_used", "read_routed_to_duckdb"}

    @pytest.mark.asyncio
    async def test_through_run_canary_it_warns_and_names_the_address(self):
        payload = _healthy_payload()
        payload.update(self._mode(
            misconfigured=["KS_READ_COHORTS=clickhouse without KS_CH_URL"]))
        result = await _probe(payload)
        assert result.severity == "warn"
        assert result.failure_keys == ["read_routed_to_duckdb"]
        assert result.read_fallbacks_clean is False
        assert "KS_CH_URL" in canary._what_to_do(result)

    def test_the_key_is_a_registered_condition_with_a_lever(self):
        assert spec_for("read_routed_to_duckdb").kind is Kind.CONDITION
        lever = dict(canary._ACTIONS)["read_routed_to_duckdb"]
        assert "\n" not in lever and len(lever) <= 150


class TestWhatOffWouldRefuseIsPublished:
    """Web publishes a route to DuckDB exactly when `off` would refuse it.
    The cohorts' gate is `ch_cohorts.enabled() and ch_cohorts.available()`,
    and under `off` everything else goes to `no_engine` and is refused;
    `misconfigured` and `no_engine` together must name exactly that."""

    def test_the_engine_only_table_is_the_cohorts_gate(self):
        from core import ch_cohorts, read_fallback

        assert read_fallback.ENGINE_ONLY == {"cohorts": (ch_cohorts.ENV, "clickhouse")}

    @pytest.mark.parametrize("switch", [None, "duckdb", "clickhouse", " ClickHouse "])
    @pytest.mark.parametrize("url", [None, "http://ks-clickhouse:8123"])
    def test_published_exactly_when_refused(self, fresh_read_fallback, monkeypatch,
                                            switch, url):
        from core import ch_cohorts

        if switch is None:
            monkeypatch.delenv("KS_READ_COHORTS", raising=False)
        else:
            monkeypatch.setenv("KS_READ_COHORTS", switch)
        if url is None:
            monkeypatch.delenv("KS_CH_URL", raising=False)
        else:
            monkeypatch.setenv("KS_CH_URL", url)
        fresh_read_fallback.configure_mode()

        refused_under_off = not (ch_cohorts.enabled() and ch_cohorts.available())
        published = [line for line in (fresh_read_fallback.misconfigured()
                                       + fresh_read_fallback.no_engine_routes())
                     if "KS_READ_COHORTS" in line]
        assert bool(published) == refused_under_off, published
        assert len(published) <= 1, "one cause, one line"


# ─── A probe that read nothing clears nothing ──────────────────────────────

def _job_factory():
    """`canary_job` itself, compiled out of bot/main.py: it is a closure over
    `canary_prev_failures`, so it is wrapped in a factory that holds one. The
    decisions under test are the job's own, not a transcription of them."""
    source = (REPO / "bot" / "main.py").read_text(encoding="utf-8")
    segment = textwrap.dedent(ast.get_source_segment(source, _canary_job(), padded=True))
    factory = ("def _make():\n    canary_prev_failures = set()\n"
               + textwrap.indent(segment, "    ") + "\n    return canary_job\n")
    return compile(factory, "bot/main.py:canary_job", "exec")


class TestABlindProbeKeepsThePage:
    """The review's reproduction, through the real job: a standing
    `read_fallback_used`, a probe that times out (a first-time blip the job
    holds back), then the same web process again. The blind probe must not
    announce it resolved, and the next must not page it as a new incident."""

    FALLBACK = {"dashboard": {"count": 1, "last_at": "2026-09-30T04:10:00+00:00"}}

    @pytest.fixture(autouse=True)
    def _gate(self, monkeypatch):
        from core.alerting import reset_gate

        monkeypatch.delenv("KS_PG_DSN", raising=False)
        reset_gate()
        yield
        reset_gate()

    async def _run(self, probes):
        """Each probe is a payload, or None for a timeout. Returns the first
        line of every message sent and the agent's task buckets."""
        future = datetime.now(timezone.utc) + timedelta(days=60)
        cert = {"notAfter": future.strftime("%b %d %H:%M:%S %Y GMT")}
        script = iter(probes)

        async def run_canary(url):
            payload = next(script)

            def handler(request):
                if payload is None:
                    raise httpx.ReadTimeout("event loop frozen")
                return httpx.Response(200, json=payload)

            async with _mock_transport(handler) as client:
                with patch.object(canary, "_fetch_peer_cert", return_value=cert):
                    return await canary.run_canary(url, client=client)

        namespace = {"run_canary": run_canary, "format_alert": canary.format_alert,
                     "DASHBOARD_URL": DASHBOARD, "logger": logging.getLogger("t")}
        exec(_job_factory(), namespace)
        job = namespace["_make"]()

        sent, agent = [], []

        async def send(text, *a, **k):
            sent.append(text.splitlines()[0])
            return 1

        with patch("bot.main.send_admin_message", new=AsyncMock(side_effect=send)), \
             patch("core.alert_agent_spool.drop_task",
                   side_effect=lambda *a, **k: agent.append(a[1])), \
             patch("core.alert_archive.write_resolved_now",
                   new=AsyncMock(return_value=True)), \
             patch("core.alert_escalator.escalate_due", new=AsyncMock()), \
             patch("core.alert_archive.record_watch") as watch:
            for _ in probes:
                await job(None)
        return sent, agent, watch

    def _payload(self, fallbacks):
        payload = _healthy_payload()
        payload["read_fallbacks"] = fallbacks
        payload["read_fallback_mode"] = {"mode": "duckdb", "error": None,
                                         "misconfigured": [], "no_engine": []}
        return payload

    @pytest.mark.asyncio
    async def test_a_deferred_blip_neither_resolves_nor_repages(self):
        standing = self._payload(self.FALLBACK)
        sent, agent, watch = await self._run([standing, standing, None, standing])
        assert len(sent) == 1, sent
        assert "Dashboard warning" in sent[0]
        assert not any("Resolved" in line for line in sent), sent
        assert agent == ["canary:read_fallback_used"], agent
        # Three probes read the block; the blind one wrote nothing.
        assert [c.kwargs["clean"] for c in watch.call_args_list] == [False] * 3
        assert {c.kwargs["web_uptime_s"] for c in watch.call_args_list} == {100.0}

    @pytest.mark.asyncio
    async def test_a_restarted_web_still_resolves_it(self):
        """The hold is for a probe that saw nothing. One that reads the block
        empty — web restarted — announces the recovery as before."""
        standing = self._payload(self.FALLBACK)
        sent, _, _ = await self._run([standing, None, self._payload({})])
        assert len(sent) == 2, sent
        assert sent[1].startswith("✅ Resolved"), sent

    @pytest.mark.asyncio
    async def test_a_refusal_page_is_held_the_same_way(self):
        now = datetime.now(timezone.utc).isoformat(timespec="seconds")
        refusing = self._payload({})
        refusing["read_fallback_mode"] = {
            "mode": "off", "error": None, "misconfigured": [], "no_engine": [],
            "refused": {"traffic": {"count": 2, "last_at": now}}}
        sent, agent, _ = await self._run([refusing, None, refusing])
        assert len(sent) == 1 and not any("Resolved" in s for s in sent), sent
        assert agent == ["canary:read_refused"], agent

    @pytest.mark.asyncio
    async def test_chain_4s_page_is_held_through_a_blind_probe(self):
        """Under chain 4 the buyers step is the only writer of buyers; a stall
        outlives the 05:15 freeze and every recreate, and read blind it was
        announced resolved and paged again as a new incident (review of chain
        4's merge with OD-07)."""
        stalled = self._payload({})
        stalled["buyer_sync"] = {"last_ok_age_s": canary.BUYER_SYNC_CHAIN_STALE_S + 600,
                                 "watermark_age_s": canary.BUYER_SYNC_CHAIN_STALE_S + 600,
                                 "last_attempt_age_s": 120, "consecutive_failures": 0}
        stalled["write_chains"] = {canary.BUYER_CHAIN: {"mode": "postgres"}}
        sent, agent, _ = await self._run([stalled, None, stalled])
        assert len(sent) == 1 and not any("Resolved" in s for s in sent), sent


class TestUnjudgedKeys:
    _CHAIN4 = {"buyer_sync": {"last_ok_age_s": 60},
               "write_chains": {canary.BUYER_CHAIN: {"mode": "duckdb"}}}
    _SWITCH = {"duckdb_switch": {"mode": "on", "opened_while_off": {}}}

    def test_no_payload_judges_none_of_them(self):
        assert canary.unjudged_keys(None) == [
            "read_fallback_used", "read_routed_to_duckdb", "read_refused",
            "buyer_sync_stalled", "buyer_sync_stalled_chain",
            "duckdb_opened_while_off"]

    def test_a_payload_with_every_block_judges_all_of_them(self):
        payload = {"read_fallbacks": {}, "read_fallback_mode": {"mode": "duckdb"},
                   **self._CHAIN4, **self._SWITCH}
        assert canary.unjudged_keys(payload) == []

    def test_each_block_answers_for_its_own_keys(self):
        assert canary.unjudged_keys({"read_fallback_mode": {}, **self._CHAIN4,
                                     **self._SWITCH}) == ["read_fallback_used"]
        assert canary.unjudged_keys({"read_fallbacks": {}, **self._CHAIN4,
                                     **self._SWITCH}) == [
            "read_routed_to_duckdb", "read_refused"]
        both = {"read_fallbacks": {}, "read_fallback_mode": {}, **self._SWITCH}
        assert canary.unjudged_keys({**both, "buyer_sync": {"last_ok_age_s": 1}}) == [
            "buyer_sync_stalled_chain"], "no chain entry"
        assert canary.unjudged_keys({**both, "buyer_sync": None,
                                     "write_chains": self._CHAIN4["write_chains"]}) == [
            "buyer_sync_stalled", "buyer_sync_stalled_chain"], "no step block"
        assert canary.unjudged_keys({"read_fallbacks": {}, "read_fallback_mode": {},
                                     **self._CHAIN4}) == [
            "duckdb_opened_while_off"], "no switch block"

    def test_only_the_od07_keys_and_the_buyers_steps_are_ever_held(self):
        """Every other payload-derived key keeps today's behaviour; each held
        key is one a check emits."""
        emitted = {k for k, _ in canary.check_read_fallbacks(
            {"read_fallbacks": {"a": _entry(1)}})}
        emitted |= {k for k, _ in canary.check_read_routes(
            {"read_fallback_mode": {"mode": "duckdb", "misconfigured": ["x"]}})}
        emitted |= {k for k, _ in canary.check_read_refusals(
            {"read_fallback_mode": {"refused": {"a": {"last_at": "?"}}}})}
        emitted |= {k for k, _ in canary.check_buyer_sync(
            {"buyer_sync": {"consecutive_failures": 3}})}
        emitted |= {k for k, _ in canary.check_buyer_sync_chain(
            {"buyer_sync": {"last_ok_age_s": canary.BUYER_SYNC_CHAIN_STALE_S + 1,
                            "last_attempt_age_s": 60},
             "write_chains": {canary.BUYER_CHAIN: {"mode": "postgres"}}})}
        emitted |= {k for k, _ in canary.check_duckdb_switch(
            {"duckdb_switch": {"opened_while_off": {"a": _entry(1)}}})}
        assert set(canary.unjudged_keys(None)) == emitted

    @pytest.mark.asyncio
    async def test_run_canary_publishes_them(self):
        def handler(request):
            raise httpx.ConnectError("refused")

        async with _mock_transport(handler) as client:
            with patch.object(canary, "_fetch_peer_cert",
                              side_effect=OSError("no route")):
                result = await canary.run_canary(DASHBOARD, client=client)
        assert result.unjudged_keys == canary.unjudged_keys(None)
        assert result.web_uptime_s is None

    @pytest.mark.parametrize("value, expected", [
        (3868, 3868.0), (0, 0.0), (12.5, 12.5), (-1, None), (True, None),
        ("3868", None), (None, None)])
    def test_uptime_is_a_number_or_nothing(self, value, expected):
        assert canary.web_uptime_s({"uptime_seconds": value}) == expected


# ─── The bot writes the watch ──────────────────────────────────────────────

def _canary_job() -> ast.AsyncFunctionDef:
    tree = ast.parse((REPO / "bot" / "main.py").read_text(encoding="utf-8"))
    found = [n for n in ast.walk(tree)
             if isinstance(n, ast.AsyncFunctionDef) and n.name == "canary_job"]
    assert len(found) == 1, "canary_job moved — this test reads it by name"
    return found[0]


def _parents(root: ast.AST) -> dict:
    return {child: node for node in ast.walk(root) for child in ast.iter_child_nodes(node)}


class TestTheBotWritesTheWatch:
    def test_every_probe_that_read_the_block_is_written(self):
        """Parsed, not grepped: `record_watch` is called in `canary_job` with
        the canary's key, the probe's reading and the canary's gap — and
        under no branch of what the probe found, because a clean probe is the
        one that matters most and pages nothing."""
        job = _canary_job()
        # This watch's calls: the week of silence's watch sits beside it under
        # its own key (tests/unit/test_duckdb_switch.py).
        calls = [n for n in ast.walk(job) if isinstance(n, ast.Call)
                 and isinstance(n.func, ast.Name) and n.func.id == "record_watch"
                 and n.args and ast.unparse(n.args[0]) == "READ_FALLBACK_WATCH_KEY"]
        assert len(calls) == 1, "the watch is written once per probe"
        call = calls[0]
        assert [ast.unparse(a) for a in call.args] == ["READ_FALLBACK_WATCH_KEY"]
        kwargs = {k.arg: ast.unparse(k.value) for k in call.keywords}
        assert kwargs == {"clean": "result.read_fallbacks_clean",
                          "gap_s": "READ_FALLBACK_WATCH_GAP_S",
                          "web_uptime_s": "result.web_uptime_s"}

        parents = _parents(job)
        node, guards = call, []
        while node is not job:
            node = parents[node]
            if isinstance(node, ast.If):
                guards.append(ast.unparse(node.test))
        assert guards == ["result.read_fallbacks_clean is not None"], guards

    def test_it_is_written_before_the_page(self):
        """Whatever the page does after it — deferred, suppressed, raised —
        the watch has already been told what this probe read."""
        job = _canary_job()
        body = [ast.unparse(stmt) for stmt in job.body]
        watch = next(i for i, s in enumerate(body) if "record_watch(" in s)
        raise_ = next(i for i, s in enumerate(body) if "raise_alert(" in s)
        assert watch < raise_


class TestTheWatchWriter:
    def test_no_dsn_writes_nothing(self, monkeypatch):
        monkeypatch.delenv("KS_PG_DSN", raising=False)

        async def main():
            return alert_archive.record_watch("watch:x", clean=True, gap_s=60)

        assert asyncio.run(main()) is None

    def test_a_dead_postgres_costs_one_warning_and_never_the_probe(
        self, monkeypatch, caplog,
    ):
        monkeypatch.setenv("KS_PG_DSN", "postgresql://x@nowhere:5/ks")
        monkeypatch.setattr(alert_archive, "_standing_down", False)
        monkeypatch.setattr("core.pg.get_pool",
                            AsyncMock(side_effect=ConnectionError("refused")))

        async def main():
            with caplog.at_level(logging.WARNING, logger="core.alert_archive"):
                for _ in range(3):
                    task = alert_archive.record_watch("watch:x", clean=True, gap_s=60)
                    assert task is not None
                    await task  # never raises

        asyncio.run(main())
        assert caplog.text.count("standing down") == 1

    def test_the_write_carries_its_own_timeout(self, monkeypatch):
        monkeypatch.setenv("KS_PG_DSN", "postgresql://x@nowhere:5/ks")
        monkeypatch.setattr(alert_archive, "WRITE_TIMEOUT_S", 0.05)
        monkeypatch.setattr(alert_archive, "_standing_down", False)

        async def hang(*a, **kw):
            await asyncio.sleep(30)

        async def main():
            with patch("core.alert_archive._write_watch", new=hang):
                task = alert_archive.record_watch("watch:x", clean=False, gap_s=60)
                await asyncio.wait_for(task, timeout=1.0)

        asyncio.run(main())

    def test_it_hands_the_statement_what_the_canary_read(self, monkeypatch):
        monkeypatch.setenv("KS_PG_DSN", "postgresql://x@nowhere:5/ks")
        monkeypatch.setenv("KS_INSTANCE", "test-host")
        conn = MagicMock()
        conn.execute = AsyncMock()
        acquire = MagicMock()
        acquire.__aenter__ = AsyncMock(return_value=conn)
        acquire.__aexit__ = AsyncMock(return_value=False)
        pool = MagicMock()
        pool.acquire = MagicMock(return_value=acquire)
        monkeypatch.setattr("core.pg.get_pool", AsyncMock(return_value=pool))

        async def main():
            await alert_archive.record_watch(
                canary.READ_FALLBACK_WATCH_KEY, clean=False,
                gap_s=canary.READ_FALLBACK_WATCH_GAP_S)

        asyncio.run(main())
        sql, *params = conn.execute.await_args.args
        assert sql == alert_archive._WATCH_SQL
        assert params == ["watch:read_fallbacks", False, 2100.0, "test-host", None]

    def test_it_hands_the_statement_webs_uptime(self, monkeypatch):
        """What tells the web process read last time from a new one."""
        monkeypatch.setenv("KS_PG_DSN", "postgresql://x@nowhere:5/ks")
        conn = MagicMock()
        conn.execute = AsyncMock()
        acquire = MagicMock()
        acquire.__aenter__ = AsyncMock(return_value=conn)
        acquire.__aexit__ = AsyncMock(return_value=False)
        pool = MagicMock()
        pool.acquire = MagicMock(return_value=acquire)
        monkeypatch.setattr("core.pg.get_pool", AsyncMock(return_value=pool))

        async def main():
            await alert_archive.record_watch(
                canary.READ_FALLBACK_WATCH_KEY, clean=True, gap_s=60,
                web_uptime_s=3868)

        asyncio.run(main())
        assert conn.execute.await_args.args[-1] == 3868.0
