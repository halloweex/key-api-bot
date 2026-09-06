"""API tests for the TurboSMS delivery-report webhook.

This is the only write endpoint reachable without a session, so the tests that
matter are the ones that keep it shut: no signature, wrong signature, and a
missing local secret.
"""
import hashlib
import logging

import httpx
import pytest
from fastapi.testclient import TestClient

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

    def test_dlr_timestamp_is_parsed(self, client, store):
        client.post(PATH, json=_signed("evt-1", "mid-1", "DELIVRD"))
        assert store.calls[0]["delivered_at"].year == 2026

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
    the write does not land, and a fake cannot show that.
    """

    @staticmethod
    def _member(buyer_id: int) -> dict:
        return {
            "buyerId": buyer_id, "phone": f"3809{buyer_id:08d}", "tier": "CORE",
            "assignment": "target", "orders": 2, "revenueLtv": 5000.0,
            "marginLtv": 3000.0, "recencyDays": 30,
        }

    async def _campaign(self, tmp_path, monkeypatch):
        """A sent campaign with two recipients, and the webhook wired to it."""
        from core.duckdb_store import DuckDBStore

        store = DuckDBStore(db_path=tmp_path / "dlr.duckdb")
        await store.connect()
        await store.freeze_sms_campaign(
            campaign="aug", customers=[self._member(1), self._member(2)],
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

    @staticmethod
    def _client():
        # ASGITransport runs the app in this test's own event loop, so the
        # store's asyncio lock is used from one loop throughout. TestClient
        # builds a fresh loop per request unless entered as a context manager,
        # and entering it would run the app's whole startup.
        return httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://testserver",
        )

    @staticmethod
    async def _row(store, message_id: str) -> tuple:
        async with store.connection() as conn:
            return conn.execute(
                "SELECT delivery_status, delivered FROM sms_campaign_members"
                " WHERE message_id = ?", [message_id],
            ).fetchone()

    @pytest.mark.asyncio
    async def test_a_captured_pair_cannot_be_repointed_at_another_message(
        self, tmp_path, monkeypatch,
    ):
        """The whole exploit in two calls: sign one event, then reuse that same
        signature for somebody else's message."""
        store = await self._campaign(tmp_path, monkeypatch)
        forged = _signed("evt-A", "mid-attacker", "Sent")

        async with self._client() as ac:
            first = await ac.post(PATH, json=forged)
            # Same id, same signature — only the object reference is swapped.
            forged["data"]["message_id"] = "mid-victim"
            forged["data"]["status"] = "UNDELIV"
            second = await ac.post(PATH, json=forged)

        assert first.status_code == 200
        assert second.status_code != 200, "a re-pointed pair must not be accepted"
        assert await self._row(store, "mid-victim") == ("Accepted", None), (
            "the victim's delivery state is what the campaign's lift is "
            "computed from, and a callback that never referred to them "
            "must not touch it"
        )
        await store.close()

    @pytest.mark.asyncio
    async def test_the_rejection_is_counted_under_its_own_condition(
        self, tmp_path, monkeypatch,
    ):
        """A burst of these is either a replay or the gateway reusing ids
        across messages, and the counters are how anyone tells."""
        store = await self._campaign(tmp_path, monkeypatch)
        forged = _signed("evt-A", "mid-attacker", "Sent")

        async with self._client() as ac:
            await ac.post(PATH, json=forged)
            forged["data"]["message_id"] = "mid-victim"
            await ac.post(PATH, json=forged)

        assert webhooks._dlr_counts["event_rebound"] == 1
        assert webhooks._dlr_counts["accepted"] == 1, "the forged one is not accepted"
        await store.close()

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
        store = await self._campaign(tmp_path, monkeypatch)
        retry = _signed("evt-A", "mid-victim", "DELIVRD")

        async with self._client() as ac:
            for attempt in range(9):
                r = await ac.post(PATH, json={**retry, "try": attempt + 1})
                assert r.status_code == 200, f"retry {attempt + 1} refused"
                assert r.json() == {"ok": True, "matched": True}

        assert await self._row(store, "mid-victim") == ("DELIVRD", True)
        assert webhooks._dlr_counts["accepted"] == 9
        await store.close()

    @pytest.mark.asyncio
    async def test_a_second_event_for_the_same_message_still_records(
        self, tmp_path, monkeypatch,
    ):
        """Sent then DELIVRD are two events about one message. Binding the id
        to the message must not turn the second one into a replay."""
        store = await self._campaign(tmp_path, monkeypatch)

        async with self._client() as ac:
            await ac.post(PATH, json=_signed("evt-A", "mid-victim", "Sent"))
            r = await ac.post(PATH, json=_signed("evt-B", "mid-victim", "DELIVRD"))

        assert r.status_code == 200
        assert await self._row(store, "mid-victim") == ("DELIVRD", True)
        await store.close()


class TestDiagnosis:
    """A wrong shared secret and an unparsed payload are the same 401 to the
    caller, and they need opposite fixes. The counters have to tell them apart —
    not knowing which cost four days of guessing after the 2026-08-05 campaign.
    """

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

    def test_a_payload_without_an_id_field_is_counted_apart(self, client, store):
        payload = _signed("evt-1", "mid-1", "DELIVRD")
        del payload["id"]

        assert client.post(PATH, json=payload).status_code == 401
        assert webhooks._dlr_counts["no_event_id"] == 1
        assert webhooks._dlr_counts["bad_signature"] == 0

    def test_an_unset_secret_is_counted_apart(self, client, store, monkeypatch):
        monkeypatch.delenv("TURBOSMS_WEBHOOK_SECRET", raising=False)

        assert client.post(PATH, json=_signed("evt-1", "mid-1", "DELIVRD")).status_code == 503
        assert webhooks._dlr_counts["secret_unset"] == 1

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

    @pytest.fixture
    def alerts(self, monkeypatch):
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

    def test_the_limit_is_sized_for_a_real_send(self):
        per_minute = int(webhooks._DLR_RATE_LIMIT.split("/")[0])
        assert per_minute >= 600, (
            "a 5 000-recipient campaign must be absorbable within the gateway's "
            "4.5-hour retry window"
        )

    def test_a_campaign_sized_burst_is_accepted(self, client, store):
        for i in range(300):
            r = client.post(PATH, json=_signed(f"evt-{i}", f"mid-{i}", "DELIVRD"))
            assert r.status_code == 200, f"rejected at report {i}"

        assert len(store.calls) == 300


class TestExposure:
    def test_webhook_is_a_declared_public_path(self):
        assert PATH in PUBLIC_API_PATHS

    def test_public_paths_stay_small(self):
        """Every entry here is reachable without a login — keep the list audited."""
        assert PUBLIC_API_PATHS == {"/api/health", "/api/webhooks/turbosms"}


class TestAbandonedRequestsAreNotMalformedPayloads:
    """A gateway that hangs up mid-body is not sending bad JSON.

    Both land in the same `except` if you let them, and then a burst of
    abandoned requests reads as a burst of bad payloads — which is what
    happened on 2026-08-27, when 25 of them sent their reader off to compare
    TURBOSMS_WEBHOOK_SECRET while the secret was provably fine. nginx logs
    these as 499 and never as 400; that difference is the whole point.
    """

    def test_a_disconnect_is_counted_apart_from_a_bad_payload(
        self, client, store, monkeypatch,
    ):
        from starlette.requests import ClientDisconnect

        async def _hang_up(self):
            raise ClientDisconnect()

        monkeypatch.setattr("starlette.requests.Request.json", _hang_up)

        r = client.post(PATH, json=_signed("evt-1", "mid-1", "DELIVRD"))

        # 408, not 400: nothing was wrong with the payload, and the status is
        # academic anyway — the client that would read it has already gone, so
        # nginx records the exchange as 499.
        assert r.status_code == 408
        assert webhooks._dlr_counts["client_disconnected"] == 1
        assert webhooks._dlr_counts["malformed_body"] == 0
        assert store.calls == []

    def test_genuinely_broken_json_still_counts_as_malformed(self, client, store):
        r = client.post(
            PATH, content=b"{not json", headers={"Content-Type": "application/json"},
        )

        assert r.status_code == 400
        assert webhooks._dlr_counts["malformed_body"] == 1
        assert webhooks._dlr_counts["client_disconnected"] == 0


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
