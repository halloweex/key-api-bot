"""The canary on refused reads: `read_refused`, and the watch row's writer.

A router whose engine fails refuses the read, and web counts the refusal per
surface under `/api/health`'s `read_fallback_mode.refused`. The canary pages a
recent one as `read_refused` (WARN), and holds the page through a probe that
could not read the block.

Until step 14's S14-16 this held OD-07's evidence too: a page on every read
answered from DuckDB (`read_fallback_used`, from the `read_fallbacks`
block), one on every read a switch routed there uncounted
(`read_routed_to_duckdb`), the typo page (`read_fallback_mode_invalid`), and
the `watch:read_fallbacks` row the bot rewrote on every probe that read the
block. No read falls back since S14-00 and no switch routes one since S14-07,
so S14-16 deleted them with the blocks; what is held here instead is that
none of them is a condition any more, that the bot writes no such row, and —
since `core.alert_archive.record_watch` still writes the week of silence's
watch — that the writer wears the archive's armour, under that watch's key.
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

class TestTheRetiredKeys:
    """S14-16 deleted the three checks over reads answered from DuckDB."""

    RETIRED = ("read_fallback_used", "read_routed_to_duckdb",
               "read_fallback_mode_invalid")

    def test_none_is_a_condition_or_a_lever(self):
        """Mutation: leave one in the REGISTRY or in `_ACTIONS`."""
        actions = {prefix for prefix, _ in canary._ACTIONS}
        for key in self.RETIRED:
            assert key not in REGISTRY, key
            assert key not in actions, key
        assert "watch:read_fallbacks" not in REGISTRY

    @pytest.mark.asyncio
    async def test_a_payload_that_still_carries_the_old_blocks_pages_nothing(self):
        """An image from before S14-16 — the rollback target — publishes
        `read_fallbacks` and the mode beside the refusals; this canary reads
        only the refusals, and pages none of the three. Mutation: judge
        either old block again."""
        payload = _healthy_payload()
        payload["read_fallbacks"] = {"dashboard": _entry(3)}
        payload["read_fallback_mode"] = {
            "mode": "duckdb", "error": "KS_READ_FALLBACK='of'",
            "misconfigured": ["KS_READ_GOLD=postgres without KS_PG_DSN"],
            "no_engine": ["cohorts: KS_READ_COHORTS=duckdb"]}
        result = await _probe(payload)
        assert result.ok and result.failure_keys == [], result.failure_keys
        assert not set(self.RETIRED) & set(result.unjudged_keys)

    def test_the_refusal_key_is_a_registered_condition(self):
        assert spec_for("read_refused").kind is Kind.CONDITION


class TestRefusalsAreNotFallbacks:
    """A failing engine is answered with a 503. It is judged on its own key,
    and only while recent."""

    @staticmethod
    def _off(refused):
        return {"read_fallback_mode": {"refused": refused}}

    def test_the_key_is_the_condition_never_a_count_or_a_surface(self):
        """The charter: a key that moved with the count would be a new
        incident on every refusal, and one per surface a new one per tab."""
        one = canary.check_read_refusals(self._off({"dashboard": _entry(1)}), NOW)
        many = canary.check_read_refusals(
            self._off({"goals": _entry(40), "cohorts": _entry(3)}), NOW)
        assert [k for k, _ in one] == [k for k, _ in many] == ["read_refused"]

    def test_three_surfaces_are_named_and_the_rest_counted(self):
        block = {name: _entry(1) for name in ("a", "b", "c", "d", "e")}
        [(_, message)] = canary.check_read_refusals(self._off(block), NOW)
        assert "a ×1" in message and "c ×1" in message
        assert "d ×1" not in message and message.endswith("+2 more")

    def test_a_malformed_entry_still_pages(self):
        """A message, not a parser: an odd entry costs its details, never
        the page that says a read was refused. One whose time cannot be read
        counts as recent."""
        [(key, message)] = canary.check_read_refusals(
            self._off({"dashboard": None, "goals": {"count": "x"}}), NOW)
        assert key == "read_refused"
        assert "dashboard ×? (last unknown)" in message
        assert "goals ×? (last unknown)" in message

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

    def test_a_block_with_no_refusal_is_nothing_to_judge(self):
        """An older web's block under `duckdb` carried no `refused` at all."""
        payload = {"read_fallback_mode": {"mode": "duckdb", "error": None,
                                          "misconfigured": []}}
        assert canary.check_read_refusals(payload, NOW) == []
        assert canary.check_read_refusals({}, NOW) == []
        assert canary.check_read_refusals(self._off({}), NOW) == []


# ─── Through run_canary ────────────────────────────────────────────────────

class TestThroughRunCanary:
    @pytest.mark.asyncio
    async def test_production_today_pages_nothing(self):
        """What web publishes since step 14's S14-16 with nothing refused
        since it started: no key, no warn. This is the merge: nothing new
        pages."""
        payload = _healthy_payload()
        payload["read_fallback_mode"] = {"refused": {}}
        result = await _probe(payload)
        assert result.ok and result.severity == "ok" and result.failure_keys == []
        assert "read_refused" not in result.unjudged_keys

    @pytest.mark.asyncio
    async def test_a_recent_refusal_warns_and_names_its_lever(self):
        payload = _healthy_payload()
        payload["read_fallback_mode"] = {
            "refused": {"traffic": {"count": 2, "last_at": datetime.now(
                timezone.utc).isoformat(timespec="seconds")}}}
        result = await _probe(payload)
        assert result.severity == "warn"
        assert result.failure_keys == ["read_refused"]
        assert "'read refused'" in canary._what_to_do(result)
        message = canary.format_alert(result, DASHBOARD)
        assert canary.READ_REFUSED_LINE + "traffic ×2" in message

    @pytest.mark.asyncio
    async def test_a_cause_names_its_lever_before_what_it_cost(self):
        """A failing mirror and the refusal it caused: the lever is the
        mirror's, because that is what fixing looks like."""
        payload = _healthy_payload()
        payload["mirrors"]["bronze.orders"].update(failures_since_ok=4, failing=True)
        payload["read_fallback_mode"] = {"refused": {"dashboard": {
            "count": 3, "last_at": datetime.now(timezone.utc).isoformat(
                timespec="seconds")}}}
        result = await _probe(payload)
        assert set(result.failure_keys) == {"mirror_failing:bronze.orders",
                                            "read_refused"}
        assert canary._what_to_do(result).startswith("Check meta.mirror_state")

    def test_the_lever_is_one_line_of_at_most_150(self):
        lever = dict(canary._ACTIONS)["read_refused"]
        assert "\n" not in lever and len(lever) <= 150


# ─── Through the app: what web publishes is what the canary reads ────────────

def _production_switches(monkeypatch, fresh_read_fallback):
    """Cohorts on ClickHouse with its address, as production has run since
    31.08; `fresh_read_fallback` unsets every other KS_READ_*. (Nothing
    reads either switch since step 14's S14-07.)"""
    monkeypatch.setenv("KS_READ_COHORTS", "clickhouse")
    monkeypatch.setenv("KS_CH_URL", "http://ks-clickhouse:8123")


class TestWhatWebPublishes:
    def test_no_fallback_is_a_clean_probe(
        self, admin_client, fresh_read_fallback, monkeypatch,
    ):
        _production_switches(monkeypatch, fresh_read_fallback)
        health = admin_client.get("/api/health").json()
        assert health["read_fallback_mode"] == {"refused": {}}
        assert "read_fallbacks" not in health
        assert canary.check_read_refusals(health) == []
        assert "read_refused" not in canary.unjudged_keys(health)
        assert canary.web_uptime_s(health) == health["uptime_seconds"]


    def test_a_refusal_under_off_is_read_refused_and_the_week_stays_clean(
        self, admin_client, fresh_read_fallback, monkeypatch,
    ):
        """A refusal in web reaches the canary as `read_refused`, and as
        nothing else."""
        day = date.today() - timedelta(days=3)
        monkeypatch.setenv("KS_READ_GOLD", "postgres")
        monkeypatch.setenv("KS_PG_DSN", "postgresql://nobody@127.0.0.1:1/none")
        monkeypatch.setattr("core.pg.get_pool",
                            AsyncMock(side_effect=OSError("connection refused")))
        response = admin_client.get(
            "/api/summary", params={"start_date": day.isoformat(),
                                    "end_date": day.isoformat()})
        assert response.status_code == 503

        health = admin_client.get("/api/health").json()
        assert "read_fallbacks" not in health
        assert [k for k, _ in canary.check_read_refusals(health)] == ["read_refused"]


class TestWhatOffWouldRefuseIsSeen:
    """Until step 14's S14-07 the cohorts' gate was `ch_cohorts.enabled() and
    ch_cohorts.available()`, and web published a route to DuckDB
    (`misconfigured`, `no_engine`, both deleted in S14-16) exactly when `off`
    would refuse it. The cohorts ask ClickHouse whatever `KS_READ_COHORTS`
    says now, so nothing routes them anywhere: they are refused exactly when
    there is no ClickHouse to ask, and every such refusal is counted, which
    the canary pages as `read_refused`. Mutation: let a missing `KS_CH_URL`
    answer, or refuse a configured one by the switch."""

    @pytest.mark.asyncio
    @pytest.mark.parametrize("switch", [None, "duckdb", "clickhouse", " ClickHouse "])
    @pytest.mark.parametrize("url", [None, "http://ks-clickhouse:8123"])
    async def test_refused_exactly_without_an_address_and_counted(
        self, fresh_read_fallback, monkeypatch, tmp_path, switch, url,
    ):
        from core.duckdb_store import DuckDBStore

        if switch is None:
            monkeypatch.delenv("KS_READ_COHORTS", raising=False)
        else:
            monkeypatch.setenv("KS_READ_COHORTS", switch)
        if url is None:
            monkeypatch.delenv("KS_CH_URL", raising=False)
        else:
            monkeypatch.setenv("KS_CH_URL", url)

        async def _answers(sql, **_):
            return ""

        monkeypatch.setattr("core.ch_common.execute", _answers)
        store = DuckDBStore(db_path=tmp_path / "never.duckdb")
        try:
            rows = await store._analytics_rows(
                lambda d: f"SELECT 1 FROM {d.silver_orders} o", [], ("int",))
        except fresh_read_fallback.ReadUnavailable as refused:
            assert url is None, "a configured ClickHouse was refused"
            assert refused.surface == "cohorts"
            assert fresh_read_fallback.refusals()["cohorts"]["count"] == 1
        else:
            assert url is not None, "no ClickHouse, and the cohorts answered"
            assert rows == []


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
    """The review's reproduction, through the real job: a standing page, a
    probe that times out (a first-time blip the job holds back), then the
    same web process again. The blind probe must not announce it resolved,
    and the next must not page it as a new incident. Shown on `read_refused`
    since step 14's S14-16 took `read_fallback_used`, on which it was
    found."""

    @staticmethod
    def _refused():
        return {"traffic": {"count": 2, "last_at": datetime.now(
            timezone.utc).isoformat(timespec="seconds")}}

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

    def _payload(self, refused):
        payload = _healthy_payload()
        payload["read_fallback_mode"] = {"refused": refused}
        return payload

    @pytest.mark.asyncio
    async def test_a_deferred_blip_neither_resolves_nor_repages(self):
        standing = self._payload(self._refused())
        sent, agent, watch = await self._run([standing, standing, None, standing])
        assert len(sent) == 1, sent
        assert "Dashboard warning" in sent[0]
        assert not any("Resolved" in line for line in sent), sent
        assert agent == ["canary:read_refused"], agent
        # No watch row: the read-fallback watch went in S14-16, and the week
        # of silence's is written only for a probe that read the switch block
        # under `off`, which these payloads carry none of.
        assert watch.call_args_list == []

    @pytest.mark.asyncio
    async def test_a_restarted_web_still_resolves_it(self):
        """The hold is for a probe that saw nothing. One that reads the block
        empty — web restarted — announces the recovery as before."""
        standing = self._payload(self._refused())
        sent, _, _ = await self._run([standing, None, self._payload({})])
        assert len(sent) == 2, sent
        assert sent[1].startswith("✅ Resolved"), sent

    @pytest.mark.asyncio
    async def test_a_refusal_page_is_held_the_same_way(self):
        refusing = self._payload(self._refused())
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
        # No `write_chains` entry: since step 14's S14-11 the CRITICAL is
        # judged on the `buyer_sync` block alone.
        sent, agent, _ = await self._run([stalled, None, stalled])
        assert len(sent) == 1 and not any("Resolved" in s for s in sent), sent

    @pytest.mark.asyncio
    async def test_chain_3s_page_is_held_through_a_blind_probe(self):
        """Under chain 3 the order step is the only writer of orders, and a
        hung Postgres hangs /api/health too: the blind probe announced the
        CRITICAL resolved, and the next paged it again as a new incident,
        agent and all — chain 4's defect, fixed for chain 4 only (batch-E
        review). Mutation: drop `orders_sync_failing` from
        `unjudged_keys`."""
        failing = self._payload({})
        failing["write_chains"] = {canary.ORDERS_CHAIN: {
            "sync_step": {
                "consecutive_failures": canary.ORDERS_SYNC_FAILURES + 2,
                "last_ok_age_s": 3600, "last_attempt_age_s": 60,
                "last_error_class": "TimeoutError"}}}
        sent, agent, _ = await self._run([failing, None, failing])
        assert len(sent) == 1 and not any("Resolved" in s for s in sent), sent
        assert agent == ["canary:orders_sync_failing"], agent


class TestUnjudgedKeys:
    # The buyers step's block and chain 3's step: both judged.
    _CHAIN4 = {"buyer_sync": {"last_ok_age_s": 60},
               "write_chains": {canary.ORDERS_CHAIN: {
                   "sync_step": {"consecutive_failures": 0}}}}
    # The switch block, and since S14-00 the backup evidence's.
    _SWITCH = {"duckdb_switch": {"mode": "on", "opened_while_off": {}},
               "backups": {"pitr_drill_age_h": 1.0}}

    def test_no_payload_judges_none_of_them(self):
        assert canary.unjudged_keys(None) == [
            "read_refused",
            "buyer_sync_stalled", "buyer_sync_stalled_chain",
            "duckdb_opened_while_off", "duckdb_off_held_back", "orders_sync_failing",
            "backup_evidence_stale"]

    def test_a_payload_with_every_block_judges_all_of_them(self):
        payload = {"read_fallback_mode": {"refused": {}},
                   **self._CHAIN4, **self._SWITCH}
        assert canary.unjudged_keys(payload) == []

    def test_each_block_answers_for_its_own_keys(self):
        """`read_fallbacks` answers for nothing since S14-16: a payload
        without it judges every key a payload with it did."""
        assert canary.unjudged_keys({"read_fallback_mode": {}, **self._CHAIN4,
                                     **self._SWITCH}) == []
        assert canary.unjudged_keys({"read_fallbacks": {}, **self._CHAIN4,
                                     **self._SWITCH}) == ["read_refused"]
        both = {"read_fallback_mode": {}, **self._SWITCH}
        # Since step 14's S14-11 chain 4's CRITICAL is held by the
        # `buyer_sync` block alone, never by a `write_chains` entry.
        assert canary.unjudged_keys({**both, "buyer_sync": {"last_ok_age_s": 1}}) == [
            "orders_sync_failing"], "no chain entry"
        assert canary.unjudged_keys({**both, "buyer_sync": None,
                                     "write_chains": self._CHAIN4["write_chains"]}) == [
            "buyer_sync_stalled", "buyer_sync_stalled_chain"], "no step block"
        assert canary.unjudged_keys({"read_fallback_mode": {},
                                     "backups": {}, **self._CHAIN4}) == [
            "duckdb_opened_while_off", "duckdb_off_held_back"], "no switch block"
        assert canary.unjudged_keys({"read_fallback_mode": {},
                                     "duckdb_switch": {}, **self._CHAIN4}) == [
            "backup_evidence_stale"], "no backups block"

    def test_only_the_od07_keys_and_the_buyers_steps_are_ever_held(self):
        """Every other payload-derived key keeps today's behaviour; each held
        key is one a check emits (and, since S14-00, the backup evidence's)."""
        emitted = {k for k, _ in canary.check_read_refusals(
            {"read_fallback_mode": {"refused": {"a": {"last_at": "?"}}}})}
        emitted |= {k for k, _ in canary.check_buyer_sync(
            {"buyer_sync": {"consecutive_failures": 3}})}
        emitted |= {k for k, _ in canary.check_buyer_sync_chain(
            {"buyer_sync": {"last_ok_age_s": canary.BUYER_SYNC_CHAIN_STALE_S + 1,
                            "last_attempt_age_s": 60}})}
        emitted |= {k for k, _ in canary.check_duckdb_switch(
            {"duckdb_switch": {"opened_while_off": {"a": _entry(1)}}})}
        emitted |= {k for k, _ in canary.check_duckdb_off_held_back(
            {"duckdb_switch": {"held_back": ["user_store"]}})}
        emitted |= {k for k, _ in canary.check_orders_sync_chain(
            {"write_chains": {canary.ORDERS_CHAIN: {
                "sync_step": {"consecutive_failures": 9}}}})}
        emitted |= {k for k, _ in canary.check_backup_evidence(
            {"backups": {"pitr_drill_age_h": None}})}
        assert set(canary.unjudged_keys(None)) == emitted

    @pytest.mark.parametrize("entry, held", [
        (None, True),                                              # no entry at all
        ({}, True),                                                # no step to judge
        ({"sync_step": None}, True),
        ({"sync_step": {"consecutive_failures": 0}}, False),
        # An older web's entry: since step 14's S14-11 `mode` decides nothing,
        # so a step-less entry is held whatever it says and a step is judged.
        ({"mode": "postgres"}, True),
        ({"mode": "duckdb"}, True),
        ({"mode": "duckdb", "sync_step": {"consecutive_failures": 0}}, False),
    ])
    def test_chain_3s_key_is_held_only_when_it_cannot_be_judged(self, entry, held):
        """By `sync_step` alone (S14-11). Mutation: gate the hold on `mode`
        again."""
        payload = {"read_fallback_mode": {}, **self._SWITCH,
                   "buyer_sync": {"last_ok_age_s": 1},
                   "write_chains": {**({canary.ORDERS_CHAIN: entry}
                                       if entry is not None else {})}}
        assert ("orders_sync_failing" in canary.unjudged_keys(payload)) is held

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
    def test_no_read_fallback_watch_is_written(self):
        """Parsed, not grepped: since step 14's S14-16 `canary_job` calls
        `record_watch` once, under the week of silence's key
        (tests/unit/test_duckdb_switch.py holds that call), and never under
        `watch:read_fallbacks`, whose row stays in `app.alert_series` as
        history. Mutation: write it again."""
        job = _canary_job()
        calls = [n for n in ast.walk(job) if isinstance(n, ast.Call)
                 and isinstance(n.func, ast.Name) and n.func.id == "record_watch"]
        assert [ast.unparse(c.args[0]) for c in calls] == ["DUCKDB_SWITCH_WATCH_KEY"]
        assert "read_fallbacks" not in ast.unparse(job)
        assert not hasattr(canary, "READ_FALLBACK_WATCH_KEY")

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
                canary.DUCKDB_SWITCH_WATCH_KEY, clean=False,
                gap_s=canary.DUCKDB_SWITCH_WATCH_GAP_S)

        asyncio.run(main())
        sql, *params = conn.execute.await_args.args
        assert sql == alert_archive._WATCH_SQL
        assert params == ["watch:duckdb_switch", False, 2100.0, "test-host", None]

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
                canary.DUCKDB_SWITCH_WATCH_KEY, clean=True, gap_s=60,
                web_uptime_s=3868)

        asyncio.run(main())
        assert conn.execute.await_args.args[-1] == 3868.0
