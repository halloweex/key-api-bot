"""The GA4 funnel on /traffic: the client's seams and the service's arithmetic.

No test reaches Google. The token endpoint and GA4 are an
``httpx.MockTransport`` that answers in the shape GA4 answered on 2026-11-04,
and the key is generated here, so nothing secret is in the repository.
"""
from __future__ import annotations

import base64
import json
from datetime import date

import httpx
import pytest
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric import padding
from fastapi.testclient import TestClient

from core import ga4
from web.services import ga4_funnel_service as svc
from tests.helpers.ga4_funnel_fakes import INFO, KEY, OCT, SEP, _Google, _table


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


class _Clock:
    def __init__(self) -> None:
        self.now = 1000.0

    def __call__(self) -> float:
        return self.now


async def _get(google: _Google, window=OCT, clock=None, scope=svc.DEFAULT_SCOPE) -> dict:
    async with httpx.AsyncClient(transport=httpx.MockTransport(google)) as client:
        return await svc.get_funnel(
            *window, scope=scope, client=client, clock=clock or _Clock(), today=date(2026, 11, 4),
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

    def test_countries_filter_the_whole_report_and_leave_the_steps_alone(self):
        plain = ga4.funnel_request(*OCT, svc.CHANNEL)
        assert "dimensionFilter" not in plain
        body = ga4.funnel_request(*OCT, svc.CHANNEL, ("Ukraine", "(not set)"))
        assert body["dimensionFilter"] == {"filter": {
            "fieldName": "country", "inListFilter": {"values": ["Ukraine", "(not set)"]},
        }}
        assert body["funnel"] == plain["funnel"]

    @pytest.mark.asyncio
    async def test_a_refusal_is_ga4_unavailable(self):
        async with httpx.AsyncClient(transport=httpx.MockTransport(_Google(status=429))) as client:
            with pytest.raises(ga4.GA4Unavailable):
                await ga4.run_funnel(client, "tok", *OCT, svc.CHANNEL)


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
        assert svc.earliest_day(date(2026, 11, 4)) == date(2025, 9, 4)
        assert svc.earliest_day(date(2026, 1, 15)) == date(2024, 11, 15)
        assert svc.earliest_day(date(2026, 4, 30)) == date(2025, 2, 28)

    def test_only_the_days_ga4_holds_are_asked(self):
        today = date(2026, 11, 4)  # GA4 holds funnels from 2025-09-04
        assert svc.asked_window(*OCT, today) == OCT
        assert svc.asked_window(date(2025, 8, 1), date(2025, 9, 30), today) == (date(2025, 9, 4), date(2025, 9, 30))
        assert svc.asked_window(date(2026, 11, 1), date(2026, 11, 30), today) == (date(2026, 11, 1), today)
        assert svc.asked_window(date(2000, 1, 1), date(2025, 9, 3), today) is None
        assert svc.asked_window(date(2026, 11, 5), date(2099, 12, 31), today) is None
        assert svc.asked_window(date(2026, 10, 31), date(2026, 10, 1), today) is None

    @pytest.mark.asyncio
    async def test_a_window_is_cut_to_the_days_ga4_holds(self):
        # September 2025 is asked from the 4th. Its previous month and its year
        # ago lie wholly before 2025-09-04, so neither is asked.
        google = _Google()
        out = await _get(google, window=(date(2025, 9, 1), date(2025, 9, 30)))
        spans = {(f["dateRanges"][0]["startDate"], f["dateRanges"][0]["endDate"]) for f in google.funnels}
        assert len(google.funnels) == 2 and spans == {("2025-09-04", "2025-09-30")}
        assert out["periods"]["current"] == {"start": "2025-09-01", "end": "2025-09-30"}
        assert out["total"]["year_ago"] == dict.fromkeys(ga4.COLUMNS, 0)

    @pytest.mark.asyncio
    async def test_a_window_with_no_day_ga4_holds_asks_nothing(self):
        # The dashboard's date window admits both, and GA4 refuses both.
        for window in [(date(2000, 1, 1), date(2000, 1, 1)), (date(2099, 12, 31), date(2000, 1, 1))]:
            google = _Google()
            out = await _get(google, window=window)
            assert google.tokens == 0 and google.funnels == []
            assert out["total"]["current"] == dict.fromkeys(ga4.COLUMNS, 0)
            assert out["by_channel"] == [] and out["by_device"] == []

    @pytest.mark.asyncio
    async def test_three_windows_by_channel_and_device_make_the_answer(self):
        google = _Google()
        out = await _get(google)

        assert google.tokens == 1 and len(google.funnels) == 6
        assert out["periods"]["previous"] == {"start": "2026-08-31", "end": "2026-09-30"}
        assert out["periods"]["year_ago"] == {"start": "2025-10-01", "end": "2025-10-31"}
        assert out["earliest"] == "2025-09-04"
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
    async def test_by_default_it_counts_ukraine_and_the_visitors_ga4_could_not_place(self):
        google = _Google()
        out = await _get(google)
        assert out["country"] == "ua"
        assert len(google.funnels) == 6
        for body in google.funnels:
            assert body["dimensionFilter"]["filter"]["inListFilter"]["values"] == ["Ukraine", "(not set)"]

    @pytest.mark.asyncio
    async def test_all_countries_asks_with_no_filter(self):
        google = _Google()
        out = await _get(google, scope="all")
        assert out["country"] == "all"
        assert len(google.funnels) == 6
        assert all("dimensionFilter" not in body for body in google.funnels)

    @pytest.mark.asyncio
    async def test_each_scope_is_its_own_answer_in_the_cache(self):
        google, clock = _Google(), _Clock()
        await _get(google, clock=clock)
        await _get(google, clock=clock, scope="all")
        again = await _get(google, clock=clock)
        assert len(google.funnels) == 12
        assert len(svc._cache) == 2
        assert again["country"] == "ua"

    @pytest.mark.asyncio
    async def test_a_scope_it_does_not_know_is_refused_before_ga4_is_asked(self):
        google = _Google()
        with pytest.raises(ValueError):
            await _get(google, scope="de")
        assert google.tokens == 0 and google.funnels == []

    @pytest.mark.asyncio
    async def test_the_cache_keeps_at_most_its_bound(self):
        google = _Google()
        for offset in range(svc.MAX_ENTRIES + 3):
            day = date(2026, 10, 1 + offset % 28)
            await _get(google, window=(day, date(2026, 10, 28 + offset // 28)))
        assert len(svc._cache) == svc.MAX_ENTRIES


def _admin_client() -> TestClient:
    from tests.sessions import hardcoded_admin, session_cookie
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

    def test_a_country_it_does_not_know_is_a_400_that_names_it(self):
        response = _admin_client().get("/api/traffic/funnel?period=month&country=de")
        assert response.status_code == 400
        assert response.json()["detail"] == "country must be 'ua' or 'all'"

    def test_the_country_filter_reaches_ga4_and_ukraine_is_the_default(self, monkeypatch):
        """Through the route over GA4 at the transport: the answer names the
        country it counted and every report GA4 was sent carries the filter,
        or none for all countries."""
        google = _Google()
        real = httpx.AsyncClient

        class _OverGoogle(real):
            def __init__(self, *args, **kwargs):
                kwargs["transport"] = httpx.MockTransport(google)
                super().__init__(*args, **kwargs)

        monkeypatch.setattr(httpx, "AsyncClient", _OverGoogle)
        client = _admin_client()

        default = client.get("/api/traffic/funnel?period=month")
        assert default.status_code == 200
        assert default.json()["country"] == "ua"
        assert google.funnels
        for body in google.funnels:
            assert body["dimensionFilter"]["filter"]["inListFilter"]["values"] == [
                "Ukraine", "(not set)"]

        google.funnels.clear()
        svc._reset_cache()
        everywhere = client.get("/api/traffic/funnel?period=month&country=all")
        assert everywhere.status_code == 200
        assert everywhere.json()["country"] == "all"
        assert google.funnels
        assert all("dimensionFilter" not in body for body in google.funnels)
