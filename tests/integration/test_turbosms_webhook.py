"""API tests for the TurboSMS delivery-report webhook.

This is the only write endpoint reachable without a session, so the tests that
matter are the ones that keep it shut: no signature, wrong signature, and a
missing local secret.
"""
import hashlib
import logging

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

    def test_unknown_message_still_acknowledged(self, client, store):
        """TurboSMS retries for 4.5h on non-200; retrying an unknown id is futile."""
        store.known = False

        r = client.post(PATH, json=_signed("evt-1", "mid-x", "DELIVRD"))

        assert r.status_code == 200
        assert r.json()["matched"] is False


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


class TestAlerting:
    """A rejected callback is a delivery result lost for good — the gateway
    gives up after 4.5 hours and cannot replay one. Silence is the real defect:
    20 618 callbacks failed across four days and nobody heard.
    """

    @pytest.fixture
    def alerts(self, monkeypatch):
        import importlib
        bot_main = importlib.import_module("bot.main")
        sent: list[dict] = []

        async def _capture(text, parse_mode="HTML", *, key=None):
            sent.append({"text": text, "key": key})

        monkeypatch.setattr(bot_main, "send_admin_message", _capture)
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
