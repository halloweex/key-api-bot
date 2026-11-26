"""A campaign send goes in batches, and each batch is on record when it lands.

On 2026-11-26 an 8 403 roster went to the gateway as 5 000 + 3 403. The first
request outlived the client's 20 s read timeout. The gateway sent it anyway.
The route recorded nothing, because it wrote the whole answer once at the end,
and the 3 403 never left. These pin what replaced that:

* the roster leaves in batches of RECIPIENTS_PER_REQUEST, each written to the
  roster (`final=False`) as soon as its answer is back;
* a batch with no answer is recorded as uncertain, never as unsent, so nothing
  can message those people a second time; the rest of the roster is NotSent,
  and the claim stays;
* a batch the gateway refused outright, after earlier ones went, is a partial
  send, not a failure: what went is recorded and the claim stays;
* the page can read how far a send has got, as counts only.

The gateway is the real `TurboSmsClient` over an `httpx.MockTransport`, so a
read timeout is raised and classified the way production classifies it.
"""
from __future__ import annotations

import json
import time

import httpx
import pytest
from fastapi.testclient import TestClient

from core.permissions import ADMIN_USER_IDS
from core.turbosms import RECIPIENTS_PER_REQUEST, TurboSmsClient, TurboSmsConfig
from web.main import app
from web.routes.auth import SESSION_COOKIE, create_session_data, session_serializer

ADMIN_ID = sorted(ADMIN_USER_IDS)[0]
SEND = "/api/customers/sms-campaigns/bf/send"
PROGRESS = "/api/customers/sms-campaigns/bf/send-progress"
ROSTER = 2 * RECIPIENTS_PER_REQUEST + 500


def _headers() -> dict:
    data = create_session_data(
        {"id": str(ADMIN_ID), "first_name": "Test", "username": "tester",
         "auth_date": str(int(time.time()))},
        role="admin",
    )
    return {"Cookie": f"{SESSION_COOKIE}={session_serializer.dumps(data)}"}


@pytest.fixture(autouse=True)
def reset_rate_limiter():
    from web.ratelimit import limiter
    limiter.reset()
    yield
    limiter.reset()


@pytest.fixture
def client():
    return TestClient(app)


class _Gateway:
    """Answers every send, except the batch named in `fail_batch`, which it
    answers the way that day's gateway did (`timeout`) or refuses (`refuse`)."""

    def __init__(self):
        self.batches: list[int] = []
        self.fail_batch: int | None = None
        self.failure = "timeout"

    def handler(self, request: httpx.Request) -> httpx.Response:
        path = request.url.path.strip("/")
        if path == "user/balance.json":
            return httpx.Response(200, json={
                "response_code": 0, "response_status": "OK",
                "response_result": {"balance": 1_000_000.0},
            })
        recipients = json.loads(request.content)["recipients"]
        self.batches.append(len(recipients))
        if len(self.batches) == self.fail_batch:
            if self.failure == "timeout":
                raise httpx.ReadTimeout("timed out", request=request)
            return httpx.Response(200, json={
                "response_code": 405,
                "response_status": "NOT_ALLOWED_RECIPIENTS_LIMIT",
                "response_result": [],
            })
        return httpx.Response(200, json={
            "response_code": 0, "response_status": "OK",
            "response_result": [
                {"phone": p, "response_code": 0, "message_id": f"m-{p}",
                 "response_status": "OK"}
                for p in recipients
            ],
        })


@pytest.fixture
def gateway(monkeypatch):
    gw = _Gateway()
    config = TurboSmsConfig(token="test-token-not-real", sender="KoreanStory",
                            price_per_part=1.28)

    def factory(*_a, **_kw):
        return TurboSmsClient(config, transport=httpx.MockTransport(gw.handler))

    monkeypatch.setattr("web.routes.api.customers.TurboSmsClient", factory)
    monkeypatch.setattr("web.routes.api.customers.TurboSmsConfig", lambda: config)
    return gw


class _Store:
    """Claims like the real store and keeps every write the route makes."""

    def __init__(self):
        self.targets = [{"buyerId": i, "phone": f"38050{i:07d}", "tier": "ALL"}
                        for i in range(1, ROSTER + 1)]
        self.claimed = False
        self.releases = 0
        self.writes: list[dict] = []

    async def get_sms_campaign_targets(self, campaign):
        if self.claimed:
            raise ValueError(f"campaign {campaign!r} was already sent")
        self.claimed = True
        return list(self.targets)

    async def release_sms_campaign(self, campaign):
        self.claimed = False
        self.releases += 1

    async def find_sms_send_conflicts(self, campaign):
        return []

    async def record_sms_send(self, campaign, accepted, stoplisted, failed, **kw):
        self.writes.append({"accepted": dict(accepted), "stoplisted": list(stoplisted),
                            "failed": dict(failed), **kw})
        uncertain = kw.get("uncertain", ())
        return {"campaign": campaign, "accepted": len(accepted),
                "stoplisted": len(stoplisted), "failed": len(failed),
                "uncertain": len(uncertain), "notSent": 0}

    async def get_sms_send_progress(self, campaign):
        if campaign != "bf":
            raise ValueError(f"campaign {campaign!r} has no target recipients")
        return {"campaign": campaign, "total": ROSTER, "done": 1000,
                "pending": ROSTER - 1000, "accepted": 1000, "stoplisted": 0,
                "failed": 0, "uncertain": 0, "notSent": 0, "sentAt": None}

    @property
    def batch_writes(self):
        return [w for w in self.writes if w.get("final") is False]

    @property
    def final_write(self):
        finals = [w for w in self.writes if w.get("final") is not False]
        assert len(finals) <= 1, "the send is stamped once"
        return finals[0] if finals else None


@pytest.fixture
def store(monkeypatch):
    fake = _Store()

    async def _get_store():
        return fake

    monkeypatch.setattr("web.routes.api.customers.get_store", _get_store)
    return fake


def _send(client):
    return client.post(SEND, params={"text": "BLACK FRIDAY", "confirm_campaign": "bf"},
                       headers=_headers())


class TestTheRosterLeavesInBatches:
    def test_each_batch_is_on_record_as_it_lands(self, client, store, gateway):
        r = _send(client)

        assert r.status_code == 200, r.text
        assert gateway.batches == [RECIPIENTS_PER_REQUEST, RECIPIENTS_PER_REQUEST, 500]
        assert [len(w["accepted"]) for w in store.batch_writes] == gateway.batches
        assert len(store.final_write["accepted"]) == ROSTER
        assert r.json()["unsent"] == 0


class TestABatchWithNoAnswer:
    """The shape of 2026-11-26, one batch later: the second batch times out."""

    def test_it_is_uncertain_never_unsent_and_the_claim_stays(
        self, client, store, gateway,
    ):
        gateway.fail_batch = 2
        r = _send(client)

        assert r.status_code == 502
        body = r.json()
        assert body["reason"] == "gateway_outcome_unknown"
        assert (body["accepted"], body["uncertain"], body["notSent"]) == (
            RECIPIENTS_PER_REQUEST, RECIPIENTS_PER_REQUEST, 500)
        assert store.releases == 0, "messages may have left: the claim stays"

        final = store.final_write
        assert len(final["accepted"]) == RECIPIENTS_PER_REQUEST
        second = {t["buyerId"] for t in
                  store.targets[RECIPIENTS_PER_REQUEST:2 * RECIPIENTS_PER_REQUEST]}
        assert set(final["uncertain"]) == second, (
            "the unanswered batch is recorded, so no later send can reach it")

    def test_the_batches_after_it_never_go(self, client, store, gateway):
        gateway.fail_batch = 2
        _send(client)

        assert gateway.batches == [RECIPIENTS_PER_REQUEST, RECIPIENTS_PER_REQUEST]


class TestABatchTheGatewayRefused:
    def test_after_earlier_ones_went_it_is_a_partial_send(self, client, store, gateway):
        gateway.fail_batch, gateway.failure = 2, "refuse"
        r = _send(client)

        assert r.status_code == 200, r.text
        body = r.json()
        assert body["unsent"] == RECIPIENTS_PER_REQUEST + 500
        assert "NOT_ALLOWED_RECIPIENTS_LIMIT" in body["partialError"]
        assert store.releases == 0
        assert len(store.final_write["accepted"]) == RECIPIENTS_PER_REQUEST
        assert store.final_write["uncertain"] == []

    def test_as_the_first_batch_nothing_went_and_the_claim_is_handed_back(
        self, client, store, gateway,
    ):
        gateway.fail_batch, gateway.failure = 1, "refuse"
        r = _send(client)

        assert r.status_code == 502
        assert r.json()["reason"] == "gateway_refused"
        assert store.releases == 1
        assert store.writes == []


class TestTheProgress:
    def test_it_is_counts_only(self, client, store):
        r = client.get(PROGRESS, headers=_headers())

        assert r.status_code == 200
        body = r.json()
        assert (body["done"], body["total"]) == (1000, ROSTER)
        assert all(not isinstance(v, (list, dict)) for v in body.values()), (
            "no roster, no names, no numbers")

    def test_an_unknown_campaign_is_a_404(self, client, store):
        r = client.get("/api/customers/sms-campaigns/nope/send-progress",
                       headers=_headers())
        assert r.status_code == 404

    def test_it_needs_a_session(self, client, store):
        assert client.get(PROGRESS).status_code in (401, 403)
