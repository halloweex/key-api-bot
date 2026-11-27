"""API tests for the TurboSMS delivery-report webhook.

This is the only write endpoint reachable without a session, so the tests that
matter are the ones that keep it shut: no signature, wrong signature, and a
missing local secret.
"""
import hashlib
import logging
import os
import time
from datetime import datetime
from zoneinfo import ZoneInfo

import httpx
import pytest
import pytest_asyncio
from fastapi.testclient import TestClient

from tests.integration import pg_sms_seed
from web.main import app
from web.routes.api import webhooks
from web.routes.auth import PUBLIC_API_PATHS


PATH = "/api/webhooks/turbosms"
SECRET = "s3cret"


def _signed(event_id: str, message_id: str, status: str, secret: str = SECRET) -> dict:
    return {
        "id": event_id,
        "signature": hashlib.sha1(f"{secret}{event_id}".encode()).hexdigest(),
        "type": "dlr",
        "data": {
            "message_id": message_id,
            "status": status,
            "dlr_date": "2026-08-10 09:05:00",
        },
    }


class _FakeStore:
    def __init__(self):
        self.calls = []
        self.known = True

    async def record_sms_delivery(self, **kwargs):
        self.calls.append(kwargs)
        return self.known


@pytest.fixture(autouse=True)
def reset_rate_limiter():
    from web.ratelimit import limiter
    limiter.reset()
    yield
    limiter.reset()


@pytest.fixture(autouse=True)
def reset_rejection_counters():
    """The counters are process-wide by design — they have to survive requests
    to notice a burst — so a test must not inherit another test's tally."""
    webhooks._dlr_counts.clear()
    yield
    webhooks._dlr_counts.clear()


@pytest.fixture
def client():
    return TestClient(app)


@pytest.fixture
def store(monkeypatch):
    fake = _FakeStore()

    async def _get_store():
        return fake

    monkeypatch.setattr("web.routes.api.webhooks.get_store", _get_store)
    monkeypatch.setenv("TURBOSMS_WEBHOOK_SECRET", SECRET)
    return fake


@pytest.fixture
def alerts(monkeypatch):
    """Every alert the endpoint raised: its text, bucket and conditions."""
    sent: list[dict] = []

    # Intercepted at the Gate, not at the transport: the condition
    # identity travels as `bucket`/`conditions` since step 02, and the
    # transport below no longer sees a key at all.
    async def _capture(text, *, conditions, bucket, parse_mode="HTML"):
        sent.append({"text": text, "key": bucket,
                     "conditions": list(conditions)})
        return 2

    monkeypatch.setattr("core.alerting.raise_alert", _capture)
    return sent


class TestSignature:
    def test_valid_signature_is_accepted(self, client, store):
        r = client.post(PATH, json=_signed("evt-1", "mid-1", "DELIVRD"))

        assert r.status_code == 200
        assert r.json() == {"ok": True, "matched": True}
        assert store.calls[0]["message_id"] == "mid-1"
        assert store.calls[0]["delivered"] is True

    def test_forged_signature_is_rejected(self, client, store):
        payload = _signed("evt-1", "mid-1", "DELIVRD")
        payload["signature"] = "deadbeef"

        assert client.post(PATH, json=payload).status_code == 401
        assert store.calls == [], "nothing is written on a bad signature"

    def test_missing_signature_is_rejected(self, client, store):
        payload = _signed("evt-1", "mid-1", "DELIVRD")
        del payload["signature"]

        assert client.post(PATH, json=payload).status_code == 401

    def test_signature_from_another_event_is_rejected(self, client, store):
        payload = _signed("evt-2", "mid-1", "DELIVRD")
        payload["id"] = "evt-1"

        assert client.post(PATH, json=payload).status_code == 401

    def test_unset_local_secret_fails_closed(self, client, store, monkeypatch):
        """No configured secret must mean 'reject', never 'accept anything'."""
        monkeypatch.delenv("TURBOSMS_WEBHOOK_SECRET", raising=False)

        r = client.post(PATH, json=_signed("evt-1", "mid-1", "DELIVRD"))

        assert r.status_code == 503
        assert store.calls == []


class TestPayload:
    def test_failure_status_recorded_as_undelivered(self, client, store):
        client.post(PATH, json=_signed("evt-1", "mid-1", "UNDELIV"))
        assert store.calls[0]["delivered"] is False

    def test_non_final_status_stays_open(self, client, store):
        client.post(PATH, json=_signed("evt-1", "mid-1", "Sent"))
        assert store.calls[0]["delivered"] is None

    def test_bad_timestamp_does_not_reject_the_report(self, client, store):
        payload = _signed("evt-1", "mid-1", "DELIVRD")
        payload["data"]["dlr_date"] = "not a date"

        assert client.post(PATH, json=payload).status_code == 200
        assert store.calls[0]["delivered_at"] is None

    def test_missing_message_id_is_a_bad_request(self, client, store):
        payload = _signed("evt-1", "", "DELIVRD")
        assert client.post(PATH, json=payload).status_code == 400

    def test_non_json_body_is_a_bad_request(self, client, store):
        r = client.post(PATH, content=b"not json",
                        headers={"Content-Type": "application/json"})
        assert r.status_code == 400

    def test_unknown_message_is_refused_so_the_gateway_retries(self, client, store):
        """The first reports of a campaign race `record_sms_send`: the gateway
        starts delivering the moment it accepts a batch, and our ids land only
        when the whole send returns. A 200 here made every report in that
        window unrecoverable — the gateway retries only on a non-2xx."""
        store.known = False

        for attempt in (None, 1, 2, 3):
            payload = _signed("evt-1", "mid-x", "DELIVRD")
            if attempt is not None:
                payload["try"] = attempt
            r = client.post(PATH, json=payload)
            assert r.status_code == 404, f"try={attempt}"

        # The report was still written through to the store (the event id is
        # bound there), and nothing was counted as accepted or as a rejection.
        assert len(store.calls) == 4
        assert webhooks._dlr_counts["accepted"] == 0
        assert webhooks._dlr_counts["unknown_retry_asked"] == 4
        assert not any(k in webhooks._dlr_counts
                       for k in ("bad_signature", "no_message_id", "event_rebound"))

    def test_unknown_message_is_acknowledged_once_retries_are_spent(self, client, store):
        """Past the retry window the id is genuinely foreign (a panel test send,
        another integration); insisting further only burns the gateway's
        remaining retries and our error counter."""
        store.known = False

        payload = _signed("evt-1", "mid-x", "DELIVRD")
        payload["try"] = 4
        r = client.post(PATH, json=payload)

        assert r.status_code == 200
        assert r.json()["matched"] is False
        assert webhooks._dlr_counts["accepted"] == 1


# ─── A sent campaign on Postgres, for the tests that read the roster back ───

def _member(buyer_id: int) -> dict:
    return {
        "buyerId": buyer_id, "phone": f"3809{buyer_id:08d}", "tier": "CORE",
        "assignment": "target", "orders": 2, "revenueLtv": 5000.0,
        "marginLtv": 3000.0, "recencyDays": 30,
    }


async def _sent_campaign(tmp_path, monkeypatch):
    """A sent campaign with two recipients, and the webhook wired to it."""
    store = pg_sms_seed.store(tmp_path, "dlr.duckdb")
    await store.freeze_sms_campaign(
        campaign="aug", customers=[_member(1), _member(2)],
        criteria={}, ltv_basis="margin", sales_type="retail", holdout_pct=0,
    )
    await store.record_sms_send(
        "aug", {1: "mid-attacker", 2: "mid-victim"}, [], {},
    )

    async def _get_store():
        return store

    monkeypatch.setattr("web.routes.api.webhooks.get_store", _get_store)
    monkeypatch.setenv("TURBOSMS_WEBHOOK_SECRET", SECRET)
    return store


def _asgi_client():
    # ASGITransport runs the app in this test's own event loop, so the
    # store's asyncio lock is used from one loop throughout. TestClient
    # builds a fresh loop per request unless entered as a context manager,
    # and entering it would run the app's whole startup.
    return httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://testserver",
    )


async def _row(store, message_id: str) -> tuple:
    async with pg_sms_seed.pg_conn() as conn:
        return pg_sms_seed.as_tuple(await conn.fetchrow(
            "SELECT delivery_status, delivered FROM app.sms_campaign_members"
            " WHERE message_id = $1", message_id,
        ))


class TestTheSignedIdIsBoundToItsMessage:
    """The signature proves the caller knew the secret; it says nothing about
    which message the callback is for.

    TurboSMS signs SHA1(secret + id) and nothing else, so the object actually
    written — ``data.message_id`` — is outside the proof. One captured
    (id, signature) pair therefore used to be a permanent write into any
    recipient's delivery state: the pair never expires, there is no nonce, and
    the pairs are obtainable (TURBOSMS_WEBHOOK_DEBUG prints one, and the
    gateway hands one to us with every callback). Delivery state is what the
    campaign's measured lift is computed from.

    These run against a real store, not the fake: what is being proved is that
    the write does not land, and a fake cannot show that. The store is
    Postgres, `/sms`'s only one since step 14's S14-08 (it was DuckDB until
    then; `tests/integration/pg_sms_seed.py`); skipped without `KS_PG_DSN`.
    """

    @pytest_asyncio.fixture(autouse=True)
    async def _postgres(self, monkeypatch):
        if not pg_sms_seed.DSN:
            pytest.skip("needs a live PostgreSQL at KS_PG_DSN")
        async with pg_sms_seed.on_postgres(monkeypatch):
            yield

    @pytest.mark.asyncio
    async def test_a_captured_pair_cannot_be_repointed_at_another_message(
        self, tmp_path, monkeypatch,
    ):
        """The whole exploit in two calls: sign one event, then reuse that same
        signature for somebody else's message."""
        store = await _sent_campaign(tmp_path, monkeypatch)
        forged = _signed("evt-A", "mid-attacker", "Sent")

        async with _asgi_client() as ac:
            first = await ac.post(PATH, json=forged)
            # Same id, same signature — only the object reference is swapped.
            forged["data"]["message_id"] = "mid-victim"
            forged["data"]["status"] = "UNDELIV"
            second = await ac.post(PATH, json=forged)

        assert first.status_code == 200
        assert second.status_code != 200, "a re-pointed pair must not be accepted"
        assert await _row(store, "mid-victim") == ("Accepted", None), (
            "the victim's delivery state is what the campaign's lift is "
            "computed from, and a callback that never referred to them "
            "must not touch it"
        )

    @pytest.mark.asyncio
    async def test_a_burst_of_rebounds_alerts_under_its_own_condition(
        self, tmp_path, monkeypatch, alerts,
    ):
        """A burst of these is either a replay or the gateway reusing ids
        across messages, and the alert is how anyone hears of it — under its
        own condition, not as a wrong secret."""
        await _sent_campaign(tmp_path, monkeypatch)
        forged = _signed("evt-A", "mid-attacker", "Sent")

        async with _asgi_client() as ac:
            assert (await ac.post(PATH, json=forged)).status_code == 200
            forged["data"]["message_id"] = "mid-victim"
            for _ in range(webhooks._ALERT_AT):
                assert (await ac.post(PATH, json=forged)).status_code == 409

        assert [a["key"] for a in alerts] == ["turbosms:webhook:event_rebound"]

    @pytest.mark.asyncio
    async def test_the_gateways_nine_retries_are_still_recorded(
        self, tmp_path, monkeypatch,
    ):
        """The regression that matters most.

        TurboSMS retries each event nine times over 4.5 hours and offers no
        replay, so a callback this endpoint refuses is a delivery result lost
        for good. A legitimate retry carries the same id and the same data —
        it must never look like a replay.
        """
        store = await _sent_campaign(tmp_path, monkeypatch)
        retry = _signed("evt-A", "mid-victim", "DELIVRD")

        async with _asgi_client() as ac:
            for attempt in range(9):
                r = await ac.post(PATH, json={**retry, "try": attempt + 1})
                assert r.status_code == 200, f"retry {attempt + 1} refused"
                assert r.json() == {"ok": True, "matched": True}

        assert await _row(store, "mid-victim") == ("DELIVRD", True)
        assert webhooks._dlr_counts["accepted"] == 9

    @pytest.mark.asyncio
    async def test_a_second_event_for_the_same_message_still_records(
        self, tmp_path, monkeypatch,
    ):
        """Sent then DELIVRD are two events about one message. Binding the id
        to the message must not turn the second one into a replay."""
        store = await _sent_campaign(tmp_path, monkeypatch)

        async with _asgi_client() as ac:
            await ac.post(PATH, json=_signed("evt-A", "mid-victim", "Sent"))
            r = await ac.post(PATH, json=_signed("evt-B", "mid-victim", "DELIVRD"))

        assert r.status_code == 200
        assert await _row(store, "mid-victim") == ("DELIVRD", True)


@pytest.fixture(params=["UTC", "Europe/Kyiv"])
def process_clock(request):
    """The process's own zone, both ways: UTC, as CI runs, and Europe/Kyiv, as
    the web container does (docker-compose.yml). asyncpg reads a naive time in
    the process's zone, so a parse that returned one was right in production
    only by that setting, and three hours late under UTC."""
    before = os.environ.get("TZ")
    os.environ["TZ"] = request.param
    time.tzset()
    yield
    if before is None:
        os.environ.pop("TZ", None)
    else:
        os.environ["TZ"] = before
    time.tzset()


class TestTheReportLandsOnTheRoster:
    """What an accepted report writes, read back out of Postgres."""

    @pytest_asyncio.fixture(autouse=True)
    async def _postgres(self, monkeypatch):
        if not pg_sms_seed.DSN:
            pytest.skip("needs a live PostgreSQL at KS_PG_DSN")
        async with pg_sms_seed.on_postgres(monkeypatch):
            yield

    @pytest.mark.asyncio
    async def test_the_delivery_time_is_the_kyiv_instant_the_gateway_named(
        self, tmp_path, monkeypatch, process_clock,
    ):
        """TurboSMS writes `dlr_date` as Kyiv wall time: 09:05 on 10 August is
        06:05 UTC, and that instant is when the message was delivered, in
        whatever zone the process runs."""
        await _sent_campaign(tmp_path, monkeypatch)

        async with _asgi_client() as ac:
            r = await ac.post(PATH, json=_signed("evt-1", "mid-victim", "DELIVRD"))

        assert r.status_code == 200
        async with pg_sms_seed.pg_conn() as conn:
            delivered_at = await conn.fetchval(
                "SELECT delivered_at FROM app.sms_campaign_members WHERE message_id = $1",
                "mid-victim")
        assert delivered_at == datetime(2026, 8, 10, 9, 5, tzinfo=ZoneInfo("Europe/Kyiv"))


def _without_an_id(client, monkeypatch, i):
    payload = _signed(f"evt-{i}", "mid-1", "DELIVRD")
    del payload["id"]
    return client.post(PATH, json=payload)


def _with_no_secret_set(client, monkeypatch, i):
    monkeypatch.delenv("TURBOSMS_WEBHOOK_SECRET", raising=False)
    return client.post(PATH, json=_signed(f"evt-{i}", "mid-1", "DELIVRD"))


def _hanging_up_mid_body(client, monkeypatch, i):
    from starlette.requests import ClientDisconnect

    async def _hang_up(self):
        raise ClientDisconnect()

    monkeypatch.setattr("starlette.requests.Request.json", _hang_up)
    return client.post(PATH, json=_signed(f"evt-{i}", "mid-1", "DELIVRD"))


def _with_broken_json(client, monkeypatch, i):
    return client.post(
        PATH, content=b"{not json", headers={"Content-Type": "application/json"},
    )


# Each way a callback is refused before anything is written: the condition it
# is counted under, the status the caller is answered, and how to provoke it.
_REJECTIONS = [
    # Neither a wrong secret nor an unparsed signature: there is nothing to
    # check the signature against.
    ("no_event_id", 401, _without_an_id),
    # No configured secret fails closed, and says so under its own name.
    ("secret_unset", 503, _with_no_secret_set),
    # A gateway that hangs up mid-body is not sending bad JSON. Both land in
    # the same `except` if you let them, and then abandoned requests read as
    # bad payloads — on 2026-08-27, 25 of them sent their reader off to
    # compare TURBOSMS_WEBHOOK_SECRET while the secret was provably fine.
    # 408, not 400: nothing was wrong with the payload, and the client that
    # would read the status has gone (nginx records the exchange as 499).
    ("client_disconnected", 408, _hanging_up_mid_body),
    ("malformed_body", 400, _with_broken_json),
]


class TestDiagnosis:
    """A wrong shared secret and an unparsed payload are the same 401 to the
    caller, and they need opposite fixes. Each refusal is counted, and past a
    burst alerted on, under a condition of its own — not knowing which cost
    four days of guessing after the 2026-08-05 campaign.
    """

    @pytest.mark.parametrize("kind, status, send", _REJECTIONS,
                             ids=[r[0] for r in _REJECTIONS])
    def test_a_burst_alerts_under_its_own_condition(
        self, client, store, alerts, monkeypatch, kind, status, send,
    ):
        for i in range(webhooks._ALERT_AT):
            assert send(client, monkeypatch, i).status_code == status
        assert [a["key"] for a in alerts] == [f"turbosms:webhook:{kind}"]
        assert store.calls == [], "a refused callback writes nothing"

    def test_a_wrong_secret_is_counted_as_bad_signature(self, client, store):
        payload = _signed("evt-1", "mid-1", "DELIVRD", secret="the-wrong-one")

        assert client.post(PATH, json=payload).status_code == 401
        assert webhooks._dlr_counts["bad_signature"] == 1
        assert webhooks._dlr_counts["no_signature"] == 0

    def test_a_payload_without_a_signature_field_is_counted_apart(self, client, store):
        payload = _signed("evt-1", "mid-1", "DELIVRD")
        del payload["signature"]

        assert client.post(PATH, json=payload).status_code == 401
        assert webhooks._dlr_counts["no_signature"] == 1
        assert webhooks._dlr_counts["bad_signature"] == 0

    def test_accepted_reports_are_counted_too(self, client, store):
        """Without a denominator, 'some rejections' cannot be read as a fault."""
        client.post(PATH, json=_signed("evt-1", "mid-1", "DELIVRD"))
        assert webhooks._dlr_counts["accepted"] == 1

    def test_the_signature_is_never_logged(self, client, store, caplog):
        """It is derived from the shared secret; the length is the useful part."""
        payload = _signed("evt-1", "mid-1", "DELIVRD", secret="the-wrong-one")
        sent = payload["signature"]

        with caplog.at_level(logging.WARNING, logger="web.routes.api.webhooks"):
            client.post(PATH, json=payload)

        assert sent not in caplog.text
        assert "signature_len" in caplog.text


class TestDebugLogging:
    """TURBOSMS_WEBHOOK_DEBUG is an opt-in, off-by-default diagnostic: it exists
    so one real (id, signature) pair from a rejected callback can reach
    scripts/check_turbosms_signature.py. That pair has to be logged for the tool
    to work, but nothing else does — dumping the whole attacker-controlled
    payload verbatim lets a caller inject fake log lines and bloats the log.
    """

    def test_debug_off_by_default_logs_nothing_extra(self, client, store, caplog):
        with caplog.at_level(logging.WARNING, logger="web.routes.api.webhooks"):
            client.post(PATH, json=_signed("evt-1", "mid-1", "DELIVRD",
                                           secret="the-wrong-one"))
        assert "webhook debug" not in caplog.text.lower()

    def test_debug_logs_the_pair_the_diagnostic_needs(self, client, store,
                                                      monkeypatch, caplog):
        monkeypatch.setenv("TURBOSMS_WEBHOOK_DEBUG", "1")
        payload = _signed("evt-1", "mid-1", "DELIVRD", secret="the-wrong-one")
        sent_sig = payload["signature"]

        with caplog.at_level(logging.WARNING, logger="web.routes.api.webhooks"):
            client.post(PATH, json=payload)

        assert "evt-1" in caplog.text
        assert sent_sig in caplog.text, "the tool cannot test candidate secrets without it"

    def test_debug_does_not_reflect_arbitrary_payload_fields(self, client, store,
                                                            monkeypatch, caplog):
        """The payload is attacker-controlled. Only the diagnostic fields are
        logged, so a crafted value cannot land verbatim in the log."""
        monkeypatch.setenv("TURBOSMS_WEBHOOK_DEBUG", "1")
        payload = _signed("evt-1", "mid-1", "DELIVRD", secret="the-wrong-one")
        payload["data"]["status"] = "INJECTED-MARKER"
        payload["surprise"] = "SHOULD-NOT-APPEAR"

        with caplog.at_level(logging.WARNING, logger="web.routes.api.webhooks"):
            client.post(PATH, json=payload)

        assert "INJECTED-MARKER" not in caplog.text
        assert "SHOULD-NOT-APPEAR" not in caplog.text


class TestAlerting:
    """A rejected callback is a delivery result lost for good — the gateway
    gives up after 4.5 hours and cannot replay one. Silence is the real defect:
    20 618 callbacks failed across four days and nobody heard.
    """

    def _reject_n(self, client, n: int) -> None:
        for i in range(n):
            client.post(PATH, json=_signed(f"evt-{i}", "mid-1", "DELIVRD",
                                           secret="the-wrong-one"))

    def test_a_couple_of_dlr_counts_stay_quiet(self, client, store, alerts):
        """The gateway retries nine times per event; a trickle is normal noise."""
        self._reject_n(client, webhooks._ALERT_AT - 1)
        assert alerts == []

    def test_a_burst_reaches_a_human(self, client, store, alerts):
        self._reject_n(client, webhooks._ALERT_AT)

        assert len(alerts) == 1
        assert "bad_signature" in alerts[0]["text"]

    def test_the_alert_names_the_check_that_settles_it(self, client, store, alerts):
        self._reject_n(client, webhooks._ALERT_AT)

        assert "TURBOSMS_WEBHOOK_SECRET" in alerts[0]["text"]

    def test_the_throttle_key_is_the_condition_not_the_text(self, client, store, alerts):
        """Live counters in the message would make every alert a new string and
        defeat the throttle — the same mistake that once produced 404 identical
        warehouse alerts."""
        self._reject_n(client, webhooks._ALERT_AT)

        assert alerts[0]["key"] == "turbosms:webhook:bad_signature"

    def test_conditions_alert_independently(self, client, store, alerts):
        self._reject_n(client, webhooks._ALERT_AT)
        for i in range(webhooks._ALERT_AT):
            payload = _signed(f"e-{i}", "mid-1", "DELIVRD")
            del payload["signature"]
            client.post(PATH, json=payload)

        assert {a["key"] for a in alerts} == {
            "turbosms:webhook:bad_signature",
            "turbosms:webhook:no_signature",
        }

    def test_a_broken_alerter_still_returns_the_rejection(self, client, store,
                                                          monkeypatch):
        """Failing to complain must not also fail to answer the caller."""
        import importlib
        bot_main = importlib.import_module("bot.main")

        async def _boom(*a, **kw):
            raise RuntimeError("telegram is down")

        monkeypatch.setattr(bot_main, "send_admin_message", _boom)

        self._reject_n(client, webhooks._ALERT_AT - 1)
        last = client.post(PATH, json=_signed("evt-x", "mid-1", "DELIVRD",
                                              secret="the-wrong-one"))

        assert last.status_code == 401


class TestRateLimit:
    """The limiter rejected 13 211 callbacks on 2026-08-05 — 3.6× more than the
    signature check did. A send of 5 000 produces 5 000 reports, ~98% of them
    inside one minute, so a limit sized for hand-typed requests loses the lot.
    """

    def test_a_campaign_sized_burst_is_accepted(self, client, store):
        """A thousand reports in well under a minute: a limit of 600 a minute,
        which already lost reports, refuses the last four hundred."""
        statuses = [
            client.post(PATH, json=_signed(f"evt-{i}", f"mid-{i}", "DELIVRD")).status_code
            for i in range(1000)
        ]

        assert statuses.count(200) == 1000, (
            f"{statuses.count(429)} reports rate-limited, the first at "
            f"{statuses.index(429) if 429 in statuses else None}")
        assert len(store.calls) == 1000


class TestExposure:
    def test_webhook_is_a_declared_public_path(self):
        assert PATH in PUBLIC_API_PATHS

    def test_public_paths_stay_small(self):
        """Every entry here is reachable without a login — keep the list audited."""
        assert PUBLIC_API_PATHS == {"/api/health", "/api/webhooks/turbosms"}


class TestTheAlertNamesItsOwnCause:
    """The message used to name `bad_signature` whatever the condition was."""

    def test_a_disconnect_does_not_send_anyone_after_the_secret(self):
        text = webhooks._GUIDANCE["client_disconnected"]

        assert "TURBOSMS_WEBHOOK_SECRET" not in text
        assert "check_turbosms_signature" not in text

    def test_a_bad_signature_still_points_at_the_secret(self):
        assert "TURBOSMS_WEBHOOK_SECRET" in webhooks._GUIDANCE["bad_signature"]

    def test_every_condition_the_endpoint_raises_has_guidance(self):
        """A kind with no entry falls back, and the fallback must not claim
        something the condition does not imply."""
        for kind in ("bad_signature", "secret_unset", "client_disconnected",
                     "event_rebound"):
            assert kind in webhooks._GUIDANCE, kind
        assert "TURBOSMS_WEBHOOK_SECRET" not in webhooks._GUIDANCE_DEFAULT

    def test_a_rebound_event_is_not_read_as_a_wrong_secret(self):
        """The secret verified — that is what makes this condition what it is."""
        text = webhooks._GUIDANCE["event_rebound"]

        assert "TURBOSMS_WEBHOOK_SECRET" not in text
        assert "message_id" in text
