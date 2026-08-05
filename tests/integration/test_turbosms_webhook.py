"""API tests for the TurboSMS delivery-report webhook.

This is the only write endpoint reachable without a session, so the tests that
matter are the ones that keep it shut: no signature, wrong signature, and a
missing local secret.
"""
import hashlib

import pytest
from fastapi.testclient import TestClient

from web.main import app
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


class TestExposure:
    def test_webhook_is_a_declared_public_path(self):
        assert PATH in PUBLIC_API_PATHS

    def test_public_paths_stay_small(self):
        """Every entry here is reachable without a login — keep the list audited."""
        assert PUBLIC_API_PATHS == {"/api/health", "/api/webhooks/turbosms"}
