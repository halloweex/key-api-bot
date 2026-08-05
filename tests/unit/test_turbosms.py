"""Tests for the TurboSMS client and its delivery-report handling.

The gateway's per-recipient answer is what makes a campaign measurable, so the
cases that matter are the ones that are easy to swallow: a stoplist refusal
that looks like a failure, a status that is not final yet, and a forged
delivery report.
"""
import hashlib

import httpx
import pytest

from core.turbosms import (
    TurboSmsClient,
    TurboSmsConfig,
    TurboSmsError,
    classify_dlr,
    verify_webhook_signature,
)


def _config(**kw) -> TurboSmsConfig:
    base = {"token": "tok", "sender": "KoreanStory", "webhook_secret": "s3cret"}
    base.update(kw)
    return TurboSmsConfig(**base)


def _client_with(handler) -> TurboSmsClient:
    """A client wired to an in-process transport instead of the network."""
    return TurboSmsClient(_config(), transport=httpx.MockTransport(handler))


# ─── send ────────────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_send_separates_accepted_stoplisted_and_failed():
    """A stoplist refusal is a signal to record, not a generic failure."""
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={
            "response_code": 0, "response_status": "OK",
            "response_result": [
                {"phone": "380961111111", "response_code": 0,
                 "message_id": "aaa-111", "response_status": "OK"},
                {"phone": "380962222222", "response_code": 404,
                 "message_id": None, "response_status": "NOT_ALLOWED_NUMBER_STOPLIST"},
                {"phone": "380963333333", "response_code": 903,
                 "message_id": None, "response_status": "INVALID_NUMBER"},
            ],
        })

    async with _client_with(handler) as c:
        results = await c.send(
            ["380961111111", "380962222222", "380963333333"], "hi",
        )

    by_phone = {r.phone: r for r in results}
    assert by_phone["380961111111"].accepted is True
    assert by_phone["380961111111"].message_id == "aaa-111"

    assert by_phone["380962222222"].stoplisted is True
    assert by_phone["380962222222"].accepted is False

    assert by_phone["380963333333"].stoplisted is False
    assert by_phone["380963333333"].accepted is False


@pytest.mark.asyncio
async def test_queued_message_counts_as_accepted():
    """Code 800 means queued — there is a message_id, so it is trackable."""
    def handler(request):
        return httpx.Response(200, json={
            "response_code": 0, "response_status": "OK",
            "response_result": [{"phone": "380961111111", "response_code": 800,
                                 "message_id": "bbb-222",
                                 "response_status": "SUCCESS_MESSAGE_ACCEPTED"}],
        })

    async with _client_with(handler) as c:
        (result,) = await c.send(["380961111111"], "hi")

    assert result.accepted is True


@pytest.mark.asyncio
async def test_accepted_requires_a_message_id():
    """Without an id there is nothing to track, whatever the code says."""
    def handler(request):
        return httpx.Response(200, json={
            "response_code": 0, "response_status": "OK",
            "response_result": [{"phone": "380961111111", "response_code": 0,
                                 "message_id": None, "response_status": "OK"}],
        })

    async with _client_with(handler) as c:
        (result,) = await c.send(["380961111111"], "hi")

    assert result.accepted is False


@pytest.mark.asyncio
async def test_request_level_rejection_raises():
    """A rejected request must not be mistaken for an empty send."""
    def handler(request):
        return httpx.Response(200, json={
            "response_code": 103, "response_status": "INVALID_TOKEN",
            "response_result": None,
        })

    async with _client_with(handler) as c:
        with pytest.raises(TurboSmsError, match="INVALID_TOKEN"):
            await c.send(["380961111111"], "hi")


@pytest.mark.asyncio
async def test_send_posts_expected_payload():
    seen = {}

    def handler(request: httpx.Request) -> httpx.Response:
        import json
        seen.update(json.loads(request.content))
        return httpx.Response(200, json={
            "response_code": 0, "response_status": "OK", "response_result": [],
        })

    async with _client_with(handler) as c:
        await c.send(["380961111111"], "Знижка 20%")

    assert seen["recipients"] == ["380961111111"]
    assert seen["sms"] == {"sender": "KoreanStory", "text": "Знижка 20%"}


@pytest.mark.asyncio
async def test_empty_send_does_not_call_the_gateway():
    def handler(request):
        raise AssertionError("should not be called")

    async with _client_with(handler) as c:
        assert await c.send([], "hi") == []


@pytest.mark.asyncio
async def test_transport_failure_becomes_turbosms_error():
    def handler(request):
        raise httpx.ConnectError("boom")

    async with _client_with(handler) as c:
        with pytest.raises(TurboSmsError):
            await c.send(["380961111111"], "hi")


@pytest.mark.asyncio
async def test_unconfigured_client_refuses_to_send():
    """No token means no send attempt at all, rather than a gateway round-trip."""
    def handler(request):
        raise AssertionError("must not reach the gateway without credentials")

    client = TurboSmsClient(
        TurboSmsConfig(token="", sender=""),
        transport=httpx.MockTransport(handler),
    )
    async with client as c:
        with pytest.raises(TurboSmsError, match="not configured"):
            await c.send(["380961111111"], "hi")


# ─── status ──────────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_statuses_maps_ids_to_states():
    def handler(request):
        return httpx.Response(200, json={
            "response_code": 0, "response_status": "OK",
            "response_result": [
                {"message_id": "aaa-111", "status": "Delivered", "response_code": 0},
                {"message_id": "bbb-222", "status": "Undelivered", "response_code": 0},
            ],
        })

    async with _client_with(handler) as c:
        assert await c.statuses(["aaa-111", "bbb-222"]) == {
            "aaa-111": "Delivered", "bbb-222": "Undelivered",
        }


# ─── DLR classification ──────────────────────────────────────────────────

@pytest.mark.parametrize("status,expected", [
    ("DELIVRD", True), ("READ", True), ("Delivered", True),
    ("UNDELIV", False), ("REJECTD", False), ("EXPIRED", False),
    # Not final yet — must stay open rather than counting as a failure
    ("Sent", None), ("In Queue", None), ("In Process", None), ("weird", None),
])
def test_classify_dlr(status, expected):
    assert classify_dlr(status) is expected


# ─── webhook signature ───────────────────────────────────────────────────

class TestWebhookSignature:
    """An unsigned callback would let anyone rewrite campaign results."""

    def test_accepts_a_correct_signature(self):
        sig = hashlib.sha1(b"s3cretevt-1").hexdigest()
        assert verify_webhook_signature("evt-1", sig, "s3cret") is True

    def test_accepts_uppercase_signature(self):
        sig = hashlib.sha1(b"s3cretevt-1").hexdigest().upper()
        assert verify_webhook_signature("evt-1", sig, "s3cret") is True

    def test_rejects_a_forged_signature(self):
        assert verify_webhook_signature("evt-1", "deadbeef", "s3cret") is False

    def test_rejects_signature_from_another_event(self):
        sig = hashlib.sha1(b"s3cretevt-2").hexdigest()
        assert verify_webhook_signature("evt-1", sig, "s3cret") is False

    @pytest.mark.parametrize("event_id,sig,secret", [
        ("", "abc", "s3cret"),
        ("evt-1", "", "s3cret"),
        ("evt-1", "abc", ""),          # unset local secret must fail closed
    ])
    def test_rejects_missing_pieces(self, event_id, sig, secret):
        assert verify_webhook_signature(event_id, sig, secret) is False
