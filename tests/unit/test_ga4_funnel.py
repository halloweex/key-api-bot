"""The GA4 funnel on /traffic: the client's seams and the service's arithmetic.

No test reaches Google. The token endpoint and GA4 are an
``httpx.MockTransport`` that answers in the shape GA4 answered on 2026-11-04,
and the key is generated here, so nothing secret is in the repository.
"""
from __future__ import annotations

import base64
import json
from datetime import date
from typing import Dict, List

import httpx
import pytest
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding, rsa
from fastapi.testclient import TestClient

from core import ga4
from web.services import ga4_funnel_service as svc

KEY = rsa.generate_private_key(public_exponent=65537, key_size=2048)
PEM = KEY.private_bytes(
    serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8, serialization.NoEncryption(),
).decode()
INFO = {
    "type": "service_account",
    "client_email": "ga4-reader@example.iam.gserviceaccount.com",
    "private_key": PEM,
    "token_uri": "https://oauth2.example/token",
}

OCT = (date(2026, 10, 1), date(2026, 10, 31))
SEP = (date(2026, 8, 31), date(2026, 9, 30))
OCT_2025 = (date(2025, 10, 1), date(2025, 10, 31))

#: channel → people at each step, per window; RESERVED_TOTAL is the whole site.
CHANNELS = {
    OCT: {
        "RESERVED_TOTAL": [1000, 600, 80, 40, 30],
        "Paid Social": [500, 350, 20, 10, 6],
        "Organic Social": [400, 200, 50, 25, 20],
        "Unassigned": [100, 50, 10, 5, 4],
    },
    SEP: {
        "RESERVED_TOTAL": [900, 500, 60, 30, 20],
        "Paid Social": [450, 300, 15, 8, 5],
        "Organic Social": [450, 200, 45, 22, 15],
    },
    OCT_2025: {
        "RESERVED_TOTAL": [1500, 600, 60, 25, 15],
        "Paid Social": [1500, 600, 60, 25, 15],
    },
}
DEVICES = {
    window: {
        "RESERVED_TOTAL": counts["RESERVED_TOTAL"],
        "mobile": [round(n * 0.8) for n in counts["RESERVED_TOTAL"]],
        "desktop": [n - round(n * 0.8) for n in counts["RESERVED_TOTAL"]],
    }
    for window, counts in CHANNELS.items()
}


@pytest.fixture(autouse=True)
def _fresh(monkeypatch, tmp_path):
    svc._reset_cache()
    ga4._reset_token()
    key = tmp_path / "ga4-reader.json"
    key.write_text(json.dumps(INFO))
    monkeypatch.setenv("KS_GA4_KEY_FILE", str(key))
    monkeypatch.setenv("KS_GA4_PROPERTY_ID", "123")
    yield
    svc._reset_cache()
    ga4._reset_token()


def _table(rows: Dict[str, List[int]], breakdown: str | None) -> dict:
    dims = [{"name": "funnelStepName"}] + ([{"name": breakdown}] if breakdown else [])
    return {"funnelTable": {
        "dimensionHeaders": dims,
        "metricHeaders": [{"name": "activeUsers"}, {"name": "funnelStepCompletionRate"}],
        "rows": [
            {
                "dimensionValues": [{"value": f"{i + 1}. {ga4.COLUMNS[i]}"}]
                + ([{"value": key}] if breakdown else []),
                "metricValues": [{"value": str(n)}, {"value": "0.5"}],
            }
            for key, counts in rows.items()
            for i, n in enumerate(counts)
        ],
    }}


class _Google:
    """The token endpoint and GA4, answering from the tables above."""

    def __init__(self, *, down: bool = False, status: int = 200) -> None:
        self.down = down
        self.status = status
        self.tokens = 0
        self.funnels: List[dict] = []

    def __call__(self, request: httpx.Request) -> httpx.Response:
        if request.url.host == "oauth2.example":
            self.tokens += 1
            return httpx.Response(200, json={"access_token": "tok", "expires_in": 3600})
        if self.down:
            raise httpx.ConnectError("down")
        if self.status != 200:
            return httpx.Response(self.status, json={"error": {"message": "Exhausted property tokens per hour"}})
        body = json.loads(request.content)
        self.funnels.append(body)
        span = body["dateRanges"][0]
        window = (date.fromisoformat(span["startDate"]), date.fromisoformat(span["endDate"]))
        breakdown = body["funnelBreakdown"]["breakdownDimension"]["name"]
        source = CHANNELS if breakdown == svc.CHANNEL else DEVICES
        return httpx.Response(200, json=_table(source.get(window, {}), breakdown))


class _Clock:
    def __init__(self) -> None:
        self.now = 1000.0

    def __call__(self) -> float:
        return self.now


async def _get(google: _Google, window=OCT, clock=None) -> dict:
    async with httpx.AsyncClient(transport=httpx.MockTransport(google)) as client:
        return await svc.get_funnel(
            *window, client=client, clock=clock or _Clock(), today=date(2026, 11, 4),
        )


def _b64decode(part: str) -> bytes:
    return base64.urlsafe_b64decode(part + "=" * (-len(part) % 4))


class TestTheClient:

    def test_the_assertion_is_signed_by_the_key_and_asks_to_read_only(self):
        token = ga4.assertion(INFO, now=1_800_000_000)
        header, claims, signature = token.split(".")
        payload = json.loads(_b64decode(claims))
        assert json.loads(_b64decode(header)) == {"alg": "RS256", "typ": "JWT"}
        assert payload["scope"] == "https://www.googleapis.com/auth/analytics.readonly"
        assert payload["aud"] == INFO["token_uri"]
        assert payload["iss"] == INFO["client_email"]
        assert payload["exp"] - payload["iat"] == 3600
        KEY.public_key().verify(
            _b64decode(signature), f"{header}.{claims}".encode(), padding.PKCS1v15(), hashes.SHA256(),
        )

    def test_an_unreadable_key_is_refused_without_quoting_it(self, tmp_path, monkeypatch):
        bad = tmp_path / "bad.json"
        bad.write_text('{"private_key": "-----BEGIN PRIVATE KEY-----\\nsecret')
        monkeypatch.setenv("KS_GA4_KEY_FILE", str(bad))
        with pytest.raises(ga4.GA4Unavailable) as caught:
            ga4._load_key()
        assert "secret" not in str(caught.value)

    def test_configured_needs_the_property_and_the_file(self, monkeypatch, tmp_path):
        assert ga4.configured()
        monkeypatch.setenv("KS_GA4_PROPERTY_ID", "")
        assert not ga4.configured()
        monkeypatch.setenv("KS_GA4_PROPERTY_ID", "123")
        monkeypatch.setenv("KS_GA4_KEY_FILE", str(tmp_path / "absent.json"))
        assert not ga4.configured()

    def test_steps_are_read_by_their_number_and_the_total_row_by_its_name(self):
        payload = _table({"RESERVED_TOTAL": [10, 6, 3, 2, 1], "Direct": [4, 2, 1, 1, 1]}, svc.CHANNEL)
        payload["funnelTable"]["rows"].append(
            {"dimensionValues": [{"value": "9. elsewhere"}, {"value": "Direct"}], "metricValues": [{"value": "99"}]},
        )
        parsed = ga4.parse_funnel(payload, svc.CHANNEL)
        assert parsed[ga4.TOTAL] == dict(zip(ga4.COLUMNS, [10, 6, 3, 2, 1]))
        assert parsed["Direct"] == dict(zip(ga4.COLUMNS, [4, 2, 1, 1, 1]))

    @pytest.mark.asyncio
    async def test_the_token_is_reused_until_a_minute_before_it_expires(self):
        google = _Google()
        async with httpx.AsyncClient(transport=httpx.MockTransport(google)) as client:
            await ga4.access_token(client, now=0)
            await ga4.access_token(client, now=3500)
            assert google.tokens == 1
            await ga4.access_token(client, now=3550)
        assert google.tokens == 2

    @pytest.mark.asyncio
    async def test_a_refusal_carries_ga4s_reason(self):
        async with httpx.AsyncClient(transport=httpx.MockTransport(_Google(status=429))) as client:
            with pytest.raises(ga4.GA4Unavailable) as caught:
                await ga4.run_funnel(client, "tok", *OCT, svc.CHANNEL)
        assert "429" in str(caught.value) and "tokens per hour" in str(caught.value)


class TestTheService:

    def test_each_channel_group_has_a_traffic_type(self):
        assert svc.channel_type("Paid Social") == "cold"
        assert svc.channel_type("Cross-network") == "cold"
        assert svc.channel_type("Organic Social") == "warm"
        assert svc.channel_type("Paid Search") == "warm"
        assert svc.channel_type("Unassigned") == "other"
        assert svc.channel_type("AI Assistant") == "other"

    def test_the_windows(self):
        assert svc.previous_window(*OCT) == SEP
        assert svc.year_ago(date(2024, 2, 29)) == date(2023, 2, 28)
        assert svc.earliest_day(date(2026, 11, 4)) == date(2025, 9, 1)
        assert svc.earliest_day(date(2026, 1, 15)) == date(2024, 11, 1)

    @pytest.mark.asyncio
    async def test_three_windows_by_channel_and_device_make_the_answer(self):
        google = _Google()
        out = await _get(google)

        assert google.tokens == 1 and len(google.funnels) == 6
        assert out["periods"]["previous"] == {"start": "2026-08-31", "end": "2026-09-30"}
        assert out["periods"]["year_ago"] == {"start": "2025-10-01", "end": "2025-10-31"}
        assert out["earliest"] == "2025-09-01"
        assert out["total"]["current"] == dict(zip(ga4.COLUMNS, [1000, 600, 80, 40, 30]))
        assert out["total"]["year_ago"]["entered"] == 1500

        types = {r["key"]: r for r in out["by_type"]}
        assert [r["key"] for r in out["by_type"]] == ["cold", "warm", "other"]
        assert types["cold"]["current"]["entered"] == 500
        assert types["warm"]["previous"]["entered"] == 450
        assert types["other"]["year_ago"]["entered"] == 0

        assert [r["key"] for r in out["by_channel"]] == ["Paid Social", "Organic Social", "Unassigned"]
        assert out["by_channel"][2]["type"] == "other"
        assert out["by_channel"][2]["previous"]["entered"] == 0
        assert {r["key"] for r in out["by_device"]} == {"mobile", "desktop"}
        assert out["stale"] is False

    @pytest.mark.asyncio
    async def test_a_second_look_inside_the_hour_asks_nothing(self):
        google, clock = _Google(), _Clock()
        await _get(google, clock=clock)
        clock.now += svc.TTL_S - 1
        await _get(google, clock=clock)
        assert len(google.funnels) == 6

    @pytest.mark.asyncio
    async def test_ga4_down_after_the_hour_costs_freshness_not_the_numbers(self):
        clock = _Clock()
        await _get(_Google(), clock=clock)
        clock.now += svc.TTL_S + 1
        out = await _get(_Google(down=True), clock=clock)
        assert out["stale"] is True
        assert out["total"]["current"]["entered"] == 1000

    @pytest.mark.asyncio
    async def test_with_no_earlier_answer_ga4_down_is_an_error(self):
        with pytest.raises(ga4.GA4Unavailable):
            await _get(_Google(down=True))

    @pytest.mark.asyncio
    async def test_the_cache_keeps_at_most_its_bound(self):
        google = _Google()
        for offset in range(svc.MAX_ENTRIES + 3):
            day = date(2026, 10, 1 + offset % 28)
            await _get(google, window=(day, date(2026, 10, 28 + offset // 28)))
        assert len(svc._cache) == svc.MAX_ENTRIES


def _admin_client() -> TestClient:
    from tests.unit.duckdb_off_sweep_doors import hardcoded_admin, session_cookie
    from web.main import app

    client = TestClient(app)
    name, value = session_cookie(hardcoded_admin(), "admin").split("=", 1)
    client.cookies.set(name, value)
    return client


class TestTheRoute:

    def test_unconfigured_it_answers_503_and_says_why(self, monkeypatch):
        monkeypatch.delenv("KS_GA4_KEY_FILE")
        response = _admin_client().get("/api/traffic/funnel?period=month")
        assert response.status_code == 503
        assert response.json()["detail"] == "GA4 is not configured"

    def test_a_period_it_does_not_know_is_a_400(self):
        assert _admin_client().get("/api/traffic/funnel?period=fortnight").status_code == 400
