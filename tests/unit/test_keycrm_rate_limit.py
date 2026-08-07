"""A rate-limit reply must be retried, not fatal.

The daily reconciliation makes ~250 paginated calls. 429 was raised as
KeyCRMAPIError, which is not in retryable_exceptions, so one throttled page
aborted the entire run — 14 of the 25 runs before 2026-08-07 died that way and
the comparison against KeyCRM simply did not happen on those days.
"""
from unittest.mock import MagicMock

import pytest

from core.exceptions import KeyCRMAPIError, KeyCRMConnectionError
from core.keycrm import MAX_RETRY_AFTER_SECONDS, KeyCRMClient, _retry_after_seconds


def response(status_code, headers=None):
    r = MagicMock()
    r.status_code = status_code
    r.headers = headers or {}
    r.text = "{}"
    r.content = b"{}"
    r.json = MagicMock(return_value={})
    return r


class TestRetryAfterParsing:
    def test_reads_delta_seconds(self):
        assert _retry_after_seconds(response(429, {"Retry-After": "30"})) == 30.0

    def test_clamps_absurd_values(self):
        parsed = _retry_after_seconds(response(429, {"Retry-After": "86400"}))
        assert parsed == MAX_RETRY_AFTER_SECONDS

    def test_negative_becomes_zero(self):
        assert _retry_after_seconds(response(429, {"Retry-After": "-5"})) == 0.0

    @pytest.mark.parametrize("value", ["Wed, 21 Oct 2026 07:28:00 GMT", "soon", ""])
    def test_unparseable_falls_back_to_a_small_delay(self, value):
        assert _retry_after_seconds(response(429, {"Retry-After": value})) == 5.0

    def test_missing_header_falls_back(self):
        assert _retry_after_seconds(response(429)) == 5.0


class TestTransientStatusesAreRetryable:
    @pytest.mark.asyncio
    @pytest.mark.parametrize("status", [429, 500, 502, 503])
    async def test_transient_raises_the_retryable_type(self, status):
        client = KeyCRMClient()
        client._client = MagicMock()

        async def request(**_kwargs):
            return response(status, {"Retry-After": "7"})

        client._client.request = request
        with pytest.raises(KeyCRMConnectionError) as exc:
            await client._do_request("GET", "order")

        assert exc.value.retry_after == 7.0

    @pytest.mark.asyncio
    @pytest.mark.parametrize("status", [400, 401, 403, 404, 422])
    async def test_client_errors_stay_fatal(self, status):
        """Retrying a bad request just burns the rate limit faster."""
        client = KeyCRMClient()
        client._client = MagicMock()

        async def request(**_kwargs):
            return response(status)

        client._client.request = request
        with pytest.raises(KeyCRMAPIError):
            await client._do_request("GET", "order")


class TestBackoffHonoursRetryAfter:
    @pytest.mark.asyncio
    async def test_delay_is_at_least_retry_after(self, monkeypatch):
        import core.resilience as resilience

        slept = []

        async def fake_sleep(seconds):
            slept.append(seconds)

        monkeypatch.setattr(resilience.asyncio, "sleep", fake_sleep)

        attempts = {"n": 0}

        async def flaky():
            attempts["n"] += 1
            if attempts["n"] < 2:
                raise KeyCRMConnectionError("throttled", retry_after=45)
            return "ok"

        result = await resilience.retry_with_backoff(
            flaky,
            config=resilience.RetryConfig(max_attempts=3, base_delay=1.0),
            retryable_exceptions=(KeyCRMConnectionError,),
        )

        assert result == "ok"
        assert slept and slept[0] >= 45
