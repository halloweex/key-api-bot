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
    count_segments,
    PartialSendError,
    ViberMessage,
    match_webhook_signature,
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


class TestSignatureOrder:
    """The gateway's docs say "SHA1 of the secret key and id" and never say in
    which order, with no worked example. This code guessed secret+id, and the
    2026-08-05 campaign had all 3 655 callbacks rejected against a secret the
    owner confirmed matches the panel — so the order is the live suspect.

    Both are accepted because both require knowing the secret. What the match
    is named for is the logs: it closes the ambiguity with evidence.
    """

    def test_accepts_the_documented_reading(self):
        sig = hashlib.sha1(b"s3cretevt-1").hexdigest()
        assert match_webhook_signature("evt-1", sig, "s3cret") == "sha1(secret+id)"

    def test_accepts_the_other_reading(self):
        sig = hashlib.sha1(b"evt-1s3cret").hexdigest()
        assert match_webhook_signature("evt-1", sig, "s3cret") == "sha1(id+secret)"

    def test_a_forgery_matches_neither(self):
        assert match_webhook_signature("evt-1", "deadbeef", "s3cret") is None

    def test_neither_order_helps_without_the_secret(self):
        """Accepting two orders must not widen the door: both need the secret."""
        for material in (b"wrongevt-1", b"evt-1wrong"):
            sig = hashlib.sha1(material).hexdigest()
            assert match_webhook_signature("evt-1", sig, "s3cret") is None


# ─── configuration ───────────────────────────────────────────────────────

class TestTokenEnvNames:
    """The project's .env already uses TURBOSMS_API_TOKEN; both spellings work."""

    def test_reads_api_token(self, monkeypatch):
        monkeypatch.setenv("TURBOSMS_API_TOKEN", "from-api-token")
        monkeypatch.delenv("TURBOSMS_TOKEN", raising=False)
        assert TurboSmsConfig().token == "from-api-token"

    def test_falls_back_to_plain_token(self, monkeypatch):
        monkeypatch.delenv("TURBOSMS_API_TOKEN", raising=False)
        monkeypatch.setenv("TURBOSMS_TOKEN", "from-plain")
        assert TurboSmsConfig().token == "from-plain"

    def test_api_token_wins(self, monkeypatch):
        monkeypatch.setenv("TURBOSMS_API_TOKEN", "primary")
        monkeypatch.setenv("TURBOSMS_TOKEN", "alias")
        assert TurboSmsConfig().token == "primary"

    def test_configured_needs_a_sender_too(self, monkeypatch):
        monkeypatch.setenv("TURBOSMS_API_TOKEN", "tok")
        monkeypatch.delenv("TURBOSMS_SENDER", raising=False)
        assert TurboSmsConfig().configured is False


# ─── billing ─────────────────────────────────────────────────────────────

class TestCountSegments:
    """What a message costs is invisible while writing it, and it is not len()."""

    def test_latin_text_fits_a_full_gsm7_segment(self):
        cost = count_segments("A" * 160)
        assert (cost.encoding, cost.parts) == ("gsm7", 1)

    def test_one_character_past_the_limit_splits_in_two(self):
        assert count_segments("A" * 161).parts == 2

    def test_a_single_cyrillic_character_halves_the_whole_message(self):
        """Encoding is a property of the text, not of the offending character."""
        latin = count_segments("A" * 100)
        mixed = count_segments("A" * 99 + "я")

        assert latin.parts == 1
        assert (mixed.encoding, mixed.parts) == ("ucs2", 2)

    def test_ukrainian_text_fits_seventy_characters(self):
        assert count_segments("я" * 70).parts == 1
        assert count_segments("я" * 71).parts == 2

    def test_extended_characters_cost_two_septets(self):
        # 159 plain + one escaped character = 161 septets, so it splits.
        cost = count_segments("A" * 159 + "€")
        assert (cost.encoding, cost.characters, cost.parts) == ("gsm7", 161, 2)

    def test_concatenation_overhead_shrinks_later_parts(self):
        """Two parts hold 153 septets each, not 160."""
        assert count_segments("A" * 306).parts == 2
        assert count_segments("A" * 307).parts == 3

    def test_empty_text_costs_nothing(self):
        assert count_segments("").parts == 0

    def test_newlines_stay_within_gsm7(self):
        """A line break is in the basic alphabet — it must not force UCS-2."""
        assert count_segments("Line one\nLine two").encoding == "gsm7"


# ─── viber ───────────────────────────────────────────────────────────────

class TestViberMessage:
    """The button is the whole point — an SMS can only ever show a bare URL."""

    def test_button_needs_a_destination(self):
        with pytest.raises(ValueError, match="caption and an action"):
            ViberMessage(text="hi", caption="Korean Story")

    def test_destination_needs_a_label(self):
        with pytest.raises(ValueError, match="caption and an action"):
            ViberMessage(text="hi", action="https://example.com")

    def test_caption_is_capped_at_thirty(self):
        with pytest.raises(ValueError, match="caption exceeds"):
            ViberMessage(text="hi", caption="x" * 31, action="https://example.com")

    def test_text_is_capped_at_a_thousand(self):
        with pytest.raises(ValueError, match="text exceeds"):
            ViberMessage(text="x" * 1001)

    def test_empty_text_is_refused(self):
        with pytest.raises(ValueError, match="empty"):
            ViberMessage(text="   ")

    def test_ttl_stays_inside_the_gateway_range(self):
        with pytest.raises(ValueError, match="ttl"):
            ViberMessage(text="hi", ttl=30)

    def test_payload_omits_absent_fields(self):
        assert ViberMessage(text="hi").payload("KS") == {"sender": "KS", "text": "hi"}

    def test_payload_carries_the_button(self):
        payload = ViberMessage(
            text="hi", caption="Korean Story", action="https://example.com",
        ).payload("KS")

        assert payload["caption"] == "Korean Story"
        assert payload["action"] == "https://example.com"


@pytest.mark.asyncio
async def test_hybrid_send_asks_for_viber_and_sms_together():
    """Both blocks present is what makes the gateway fall back rather than choose."""
    seen = {}

    def handler(request: httpx.Request) -> httpx.Response:
        import json
        seen.update(json.loads(request.content))
        return httpx.Response(200, json={
            "response_code": 0, "response_status": "OK",
            "response_result": [
                {"phone": "380961111111", "response_code": 0,
                 "message_id": "aaa-111", "response_status": "OK"},
            ],
        })

    client = TurboSmsClient(
        _config(viber_sender="KoreanStoryViber"),
        transport=httpx.MockTransport(handler),
    )
    async with client as c:
        results = await c.send(
            ["380961111111"], "Sale, https://example.com",
            viber=ViberMessage(text="Sale", caption="Korean Story",
                               action="https://example.com"),
        )

    assert seen["sms"] == {"sender": "KoreanStory", "text": "Sale, https://example.com"}
    assert seen["viber"]["sender"] == "KoreanStoryViber"
    assert seen["viber"]["caption"] == "Korean Story"
    assert results[0].accepted is True


@pytest.mark.asyncio
async def test_sms_only_send_carries_no_viber_block():
    seen = {}

    def handler(request: httpx.Request) -> httpx.Response:
        import json
        seen.update(json.loads(request.content))
        return httpx.Response(200, json={
            "response_code": 0, "response_status": "OK", "response_result": [],
        })

    async with _client_with(handler) as c:
        await c.send(["380961111111"], "Sale")

    assert "viber" not in seen


@pytest.mark.asyncio
async def test_hybrid_send_refuses_without_a_registered_viber_sender():
    """Viber names are registered separately; a working SMS setup proves nothing."""
    def handler(request: httpx.Request) -> httpx.Response:  # pragma: no cover
        raise AssertionError("must not reach the gateway")

    async with _client_with(handler) as c:
        with pytest.raises(TurboSmsError, match="TURBOSMS_VIBER_SENDER"):
            await c.send(["380961111111"], "Sale", viber=ViberMessage(text="Sale"))


class TestViberConfig:
    def test_viber_needs_its_own_sender(self, monkeypatch):
        monkeypatch.setenv("TURBOSMS_API_TOKEN", "tok")
        monkeypatch.setenv("TURBOSMS_SENDER", "KoreanStory")
        monkeypatch.delenv("TURBOSMS_VIBER_SENDER", raising=False)

        config = TurboSmsConfig()
        assert config.configured is True
        assert config.viber_configured is False

    def test_viber_sender_is_read_from_the_environment(self, monkeypatch):
        monkeypatch.setenv("TURBOSMS_API_TOKEN", "tok")
        monkeypatch.setenv("TURBOSMS_VIBER_SENDER", "KoreanStoryViber")
        assert TurboSmsConfig().viber_configured is True


# ─── success codes ───────────────────────────────────────────────────────

class TestSuccessCodes:
    """800-803 all mean success. Reading one as a failure re-sends the roster.

    The campaign is only stamped sent when the call returns cleanly, so a
    success mistaken for an error leaves messages delivered and the campaign
    looking unsent — and the obvious next move is to send it all again.
    """

    @pytest.mark.parametrize("code,status", [
        (0, "OK"),
        (800, "SUCCESS_MESSAGE_ACCEPTED"),
        (801, "SUCCESS_MESSAGE_SENT"),
        (802, "SUCCESS_MESSAGE_PARTIAL_ACCEPTED"),
        (803, "SUCCESS_MESSAGE_PARTIAL_SENT"),
    ])
    @pytest.mark.asyncio
    async def test_request_level_success_is_not_an_error(self, code, status):
        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(200, json={
                "response_code": code, "response_status": status,
                "response_result": [
                    {"phone": "380961111111", "response_code": code,
                     "message_id": "aaa-111", "response_status": status},
                ],
            })

        async with _client_with(handler) as c:
            results = await c.send(["380961111111"], "Sale")

        assert results[0].accepted is True, f"{code} {status} must count as sent"

    @pytest.mark.parametrize("code", [302, 400])
    @pytest.mark.asyncio
    async def test_sender_rejections_still_raise(self, code):
        """A refused alpha name must not look like a delivery."""
        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(200, json={
                "response_code": code, "response_status": "NOT_ALLOWED_MESSAGE_SENDER",
                "response_result": [],
            })

        async with _client_with(handler) as c:
            with pytest.raises(TurboSmsError):
                await c.send(["380961111111"], "Sale")

    @pytest.mark.asyncio
    async def test_per_recipient_stoplist_is_still_not_accepted(self):
        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(200, json={
                "response_code": 801, "response_status": "SUCCESS_MESSAGE_SENT",
                "response_result": [
                    {"phone": "380961111111", "response_code": 404,
                     "message_id": None,
                     "response_status": "NOT_ALLOWED_NUMBER_STOPLIST"},
                ],
            })

        async with _client_with(handler) as c:
            results = await c.send(["380961111111"], "Sale")

        assert results[0].accepted is False
        assert results[0].stoplisted is True

    def test_emoji_costs_two_ucs2_units(self):
        """UCS-2 is billed in 16-bit units, and an emoji is a surrogate pair.

        Counting it as one code point understates the message and can hide a
        whole extra segment from whoever is writing it.
        """
        assert count_segments("\U0001F389").characters == 2

    def test_emoji_can_push_a_message_over_the_edge(self):
        cost = count_segments("я" * 69 + "\U0001F389")
        assert (cost.characters, cost.parts) == (71, 2)

    def test_bmp_symbol_is_still_one_unit(self):
        assert count_segments("❤").characters == 1


# ─── batching ────────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_a_roster_over_the_limit_is_split():
    """The gateway refuses more than 5000 in one call with 405."""
    batches = []

    def handler(request: httpx.Request) -> httpx.Response:
        import json
        batches.append(len(json.loads(request.content)["recipients"]))
        return httpx.Response(200, json={
            "response_code": 801, "response_status": "SUCCESS_MESSAGE_SENT",
            "response_result": [],
        })

    phones = [f"38096{i:07d}" for i in range(5550)]
    async with _client_with(handler) as c:
        await c.send(phones, "Sale")

    assert batches == [5000, 550]


@pytest.mark.asyncio
async def test_one_request_is_still_one_request():
    calls = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(1)
        return httpx.Response(200, json={
            "response_code": 0, "response_status": "OK", "response_result": [],
        })

    async with _client_with(handler) as c:
        await c.send(["380961111111"], "Sale")

    assert len(calls) == 1


@pytest.mark.asyncio
async def test_a_failed_batch_does_not_discard_what_already_went():
    """Losing the earlier results is the one unrecoverable mistake here.

    Those people have the message; a retry that did not know would send it
    twice, spend the budget twice, and destroy the campaign's measurement.
    """
    seen = []

    def handler(request: httpx.Request) -> httpx.Response:
        import json
        seen.append(1)
        if len(seen) == 1:
            return httpx.Response(200, json={
                "response_code": 801, "response_status": "SUCCESS_MESSAGE_SENT",
                "response_result": [
                    {"phone": "380960000000", "response_code": 0,
                     "message_id": "aaa-111", "response_status": "OK"},
                ],
            })
        return httpx.Response(200, json={
            "response_code": 405,
            "response_status": "NOT_ALLOWED_RECIPIENTS_LIMIT",
            "response_result": [],
        })

    phones = [f"38096{i:07d}" for i in range(5550)]
    async with _client_with(handler) as c:
        with pytest.raises(PartialSendError) as excinfo:
            await c.send(phones, "Sale")

    error = excinfo.value
    assert error.sent == 5000
    assert error.unsent == 550
    assert [r.message_id for r in error.results] == ["aaa-111"]


@pytest.mark.asyncio
async def test_a_first_batch_failure_is_a_plain_error():
    """Nothing went out, so the campaign must stay retryable."""
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={
            "response_code": 405,
            "response_status": "NOT_ALLOWED_RECIPIENTS_LIMIT",
            "response_result": [],
        })

    async with _client_with(handler) as c:
        with pytest.raises(TurboSmsError) as excinfo:
            await c.send(["380961111111"], "Sale")

    assert not isinstance(excinfo.value, PartialSendError)


@pytest.mark.asyncio
async def test_every_batch_carries_the_same_viber_block():
    payloads = []

    def handler(request: httpx.Request) -> httpx.Response:
        import json
        payloads.append(json.loads(request.content))
        return httpx.Response(200, json={
            "response_code": 801, "response_status": "SUCCESS_MESSAGE_SENT",
            "response_result": [],
        })

    phones = [f"38096{i:07d}" for i in range(5001)]
    client = TurboSmsClient(
        _config(viber_sender="KoreanStoryViber"),
        transport=httpx.MockTransport(handler),
    )
    async with client as c:
        await c.send(phones, "Sale", viber=ViberMessage(text="Sale"))

    assert len(payloads) == 2
    assert payloads[0]["viber"] == payloads[1]["viber"]
