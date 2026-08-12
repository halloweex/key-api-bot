"""API tests for /api/customers/sms-segments and its CSV export.

The store is stubbed out — segmentation logic itself is covered by
tests/unit/test_sms_segments.py. What matters here is the HTTP contract:
these endpoints hand out customer names and phone numbers, so authorization,
parameter validation and holdout handling must all hold at the route layer.
"""
import csv
import io
import time

import pytest
from fastapi.testclient import TestClient

from web.main import app
from web.routes.auth import (
    session_serializer,
    create_session_data,
    require_admin,
    SESSION_COOKIE,
)
from core.permissions import ADMIN_USER_IDS

ADMIN_ID = sorted(ADMIN_USER_IDS)[0]

SEGMENTS_PATH = "/api/customers/sms-segments"
CSV_PATH = "/api/customers/sms-segments/export/csv"
RESULTS_PATH = "/api/customers/sms-campaigns/aug/results"
TEST_SEND_PATH = "/api/customers/sms/test-send"
CHANNELS_PATH = "/api/customers/sms/channels"


def _make_cookie(user_id: int, role: str = "admin") -> str:
    data = create_session_data(
        {
            "id": str(user_id),
            "first_name": "Test",
            "last_name": "User",
            "username": "tester",
            "auth_date": str(int(time.time())),
        },
        role=role,
    )
    return session_serializer.dumps(data)


def _admin_headers() -> dict:
    return {"Cookie": f"{SESSION_COOKIE}={_make_cookie(ADMIN_ID)}"}


def _all_dep_calls(dependant) -> set:
    calls = set()
    for d in dependant.dependencies:
        if d.call is not None:
            calls.add(d.call)
        calls |= _all_dep_calls(d)
    return calls


def _route(path: str, method: str = "GET"):
    for r in app.routes:
        if getattr(r, "path", None) == path and method in getattr(r, "methods", set()):
            return r
    return None


def _customer(buyer_id: int, tier: str, assignment: str) -> dict:
    return {
        "buyerId": buyer_id,
        "fullName": f"Клієнт {buyer_id}",
        "phone": f"3809{buyer_id:08d}",
        "city": "Kyiv",
        "tier": tier,
        "orders": 3,
        "ltv": 12000.0,
        "avgOrderValue": 4000.0,
        "revenueLtv": 12000.0,
        "marginLtv": 6600.0,
        "marginPct": 55.0,
        "costCoverage": 96.5,
        "recencyDays": 20,
        "lastOrderDate": "2026-07-16",
        "firstOrderDate": "2025-11-02",
        "lastOrderId": 90000 + buyer_id,
        "lastOrderTotal": 2450.0,
        "lastOrderItemCount": 5,
        "lastOrderItems": "Abib COLLAGEN GEL MASK | LALARECIPE Cica 3in1 | NARD Treatment +2 ещё",
        "assignment": assignment,
    }


class _FakeStore:
    """Records the kwargs it was called with and returns a canned segmentation."""

    def __init__(self):
        self.calls = []
        self.freezes = []
        self.freeze_error = None
        self.truncated = False
        self.result_calls = []
        self.results_error = None

    async def get_sms_campaign_results(self, campaign, window_days=30,
                                       delivered_only=False):
        if self.results_error:
            raise ValueError(self.results_error)
        self.result_calls.append({"campaign": campaign, "window_days": window_days,
                                  "delivered_only": delivered_only})
        return {"campaign": campaign, "windowDays": window_days,
                "deliveredOnly": delivered_only, "segments": []}

    async def freeze_sms_campaign(self, **kwargs):
        if self.freeze_error:
            raise ValueError(self.freeze_error)
        self.freezes.append(kwargs)
        roster = kwargs["customers"]
        return {
            "campaign": kwargs["campaign"],
            "frozen": True,
            "segments": [],
            "totals": {
                "customers": len(roster),
                "target": sum(1 for c in roster if c["assignment"] == "target"),
                "holdout": sum(1 for c in roster if c["assignment"] == "holdout"),
            },
        }

    async def get_sms_segments(self, **kwargs):
        self.calls.append(kwargs)
        customers = [
            _customer(1, "VIP", "target"),
            _customer(2, "CORE", "holdout"),
            _customer(3, "REACTIVATION", "target"),
        ] if kwargs.get("include_customers") else []
        return {
            "campaign": kwargs.get("campaign"),
            "salesType": kwargs.get("sales_type"),
            "ltvBasis": kwargs.get("ltv_basis"),
            "criteria": {
                "vipLtv": kwargs.get("vip_ltv"),
                "coreLtv": kwargs.get("core_ltv"),
            },
            "segments": [{"tier": "VIP", "total": 1, "target": 1, "holdout": 0}],
            "totals": {"customers": 3, "target": 2, "holdout": 1},
            "customers": customers,
            "truncated": self.truncated,
        }


@pytest.fixture(autouse=True)
def reset_rate_limiter():
    """The limiter is shared app-wide and its counters outlive a single test.

    The CSV export is capped at 5/minute on purpose (it dumps PII), which is
    below the number of cases in this file — so clear the buckets between
    tests instead of relaxing the production limit.
    """
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

    async def _fake_get_store():
        return fake

    monkeypatch.setattr("web.routes.api.customers.get_store", _fake_get_store)
    return fake


# ─── Authorization ────────────────────────────────────────────────────────

class TestSmsSegmentsAuth:
    """PII endpoints must be admin-only, not merely session-gated."""

    @pytest.mark.parametrize("path", [SEGMENTS_PATH, CSV_PATH, RESULTS_PATH])
    def test_requires_session(self, client, path):
        assert client.get(path).status_code == 401

    @pytest.mark.parametrize("path", [SEGMENTS_PATH, CSV_PATH])
    def test_requires_admin_dependency(self, path):
        route = _route(path)
        assert route is not None, f"{path} is not registered"
        assert require_admin in _all_dep_calls(route.dependant), \
            f"{path} exports phone numbers and must keep require_admin"

    @pytest.mark.parametrize("path", [SEGMENTS_PATH, CSV_PATH])
    def test_viewer_is_forbidden(self, client, monkeypatch, path):
        viewer_id = 555_000_222
        assert viewer_id not in ADMIN_USER_IDS

        class _Store:
            async def get_user(self, uid):
                return {"status": "approved", "role": "viewer"}

        async def _fake_get_store():
            return _Store()

        monkeypatch.setattr("core.duckdb_store.get_store", _fake_get_store)

        r = client.get(
            path,
            headers={"Cookie": f"{SESSION_COOKIE}={_make_cookie(viewer_id, role='viewer')}"},
        )
        assert r.status_code == 403


# ─── JSON endpoint ────────────────────────────────────────────────────────

class TestSmsSegmentsJson:
    def test_defaults_are_passed_through(self, client, store):
        r = client.get(SEGMENTS_PATH, headers=_admin_headers())
        assert r.status_code == 200

        call = store.calls[0]
        assert call["max_recency_days"] == 270
        assert call["vip_ltv"] == 10000
        assert call["core_ltv"] == 5000
        assert call["core_min_orders"] == 2
        assert call["reactivation_max_recency"] == 120
        assert call["holdout_pct"] == 10
        assert call["sales_type"] == "retail"
        assert call["tier"] is None
        assert call["ltv_basis"] == "revenue", "revenue stays the default basis"

    def test_margin_basis_swaps_the_default_thresholds(self, client, store):
        """Margin cut-offs are calibrated to select the same share of the base.

        Without this, switching basis would silently shrink every tier by
        roughly the blended margin rate.
        """
        r = client.get(
            SEGMENTS_PATH, params={"ltv_basis": "margin"}, headers=_admin_headers(),
        )
        assert r.status_code == 200

        call = store.calls[0]
        assert call["ltv_basis"] == "margin"
        assert call["vip_ltv"] == 5500.0
        assert call["core_ltv"] == 2750.0

    def test_explicit_thresholds_override_basis_defaults(self, client, store):
        client.get(
            SEGMENTS_PATH,
            params={"ltv_basis": "margin", "vip_ltv": 9000, "core_ltv": 1000},
            headers=_admin_headers(),
        )
        assert store.calls[0]["vip_ltv"] == 9000
        assert store.calls[0]["core_ltv"] == 1000

    def test_threshold_coherence_checked_against_basis_defaults(self, client, store):
        """core_ltv=8000 is fine on revenue (VIP 10000) but not on margin (VIP 5500)."""
        ok = client.get(
            SEGMENTS_PATH, params={"core_ltv": 8000}, headers=_admin_headers(),
        )
        assert ok.status_code == 200

        bad = client.get(
            SEGMENTS_PATH,
            params={"ltv_basis": "margin", "core_ltv": 8000},
            headers=_admin_headers(),
        )
        assert bad.status_code == 400

    def test_customers_withheld_by_default(self, client, store):
        r = client.get(SEGMENTS_PATH, headers=_admin_headers())
        assert r.json()["customers"] == []
        assert store.calls[0]["include_customers"] is False

    def test_customers_returned_on_request(self, client, store):
        r = client.get(
            SEGMENTS_PATH, params={"include_customers": "true"}, headers=_admin_headers(),
        )
        assert len(r.json()["customers"]) == 3

    def test_tier_is_normalised(self, client, store):
        r = client.get(SEGMENTS_PATH, params={"tier": "vip"}, headers=_admin_headers())
        assert r.status_code == 200
        assert store.calls[0]["tier"] == ["VIP"]


# ─── Parameter validation ─────────────────────────────────────────────────

class TestSmsSegmentsValidation:
    @pytest.mark.parametrize("params", [
        {"tier": "GOLD"},                              # unknown tier
        {"core_ltv": 20000, "vip_ltv": 10000},         # CORE above VIP
        {"reactivation_max_recency": 400},             # wider than the overall window
        {"ltv_basis": "profit"},                       # unknown basis
    ])
    def test_incoherent_criteria_rejected(self, client, store, params):
        r = client.get(SEGMENTS_PATH, params=params, headers=_admin_headers())
        assert r.status_code == 400

    @pytest.mark.parametrize("params", [
        {"holdout_pct": 90},           # above the 50% cap
        {"holdout_pct": -1},
        {"max_recency_days": 5000},
        {"campaign": "drop; table"},   # outside the allowed charset
    ])
    def test_out_of_range_params_rejected(self, client, store, params):
        r = client.get(SEGMENTS_PATH, params=params, headers=_admin_headers())
        assert r.status_code == 422


# ─── CSV export ───────────────────────────────────────────────────────────

class TestSmsSegmentsCsv:
    def _rows(self, response) -> list[dict]:
        text = response.content.decode("utf-8-sig")
        return list(csv.DictReader(io.StringIO(text)))

    def test_holdout_excluded_by_default(self, client, store):
        r = client.get(CSV_PATH, headers=_admin_headers())
        assert r.status_code == 200

        rows = self._rows(r)
        assert [row["buyer_id"] for row in rows] == ["1", "3"], \
            "the holdout must not be messaged, or uplift cannot be measured"
        assert all(row["assignment"] == "target" for row in rows)
        assert r.headers["X-Segment-Rows"] == "2"

    def test_holdout_included_on_request(self, client, store):
        r = client.get(CSV_PATH, params={"include_holdout": "true"}, headers=_admin_headers())
        rows = self._rows(r)
        assert len(rows) == 3
        assert {row["assignment"] for row in rows} == {"target", "holdout"}

    def test_phone_is_e164(self, client, store):
        rows = self._rows(client.get(CSV_PATH, headers=_admin_headers()))
        assert rows[0]["phone"].startswith("+380"), \
            "gateways want E.164, and the + stops Excel reading it as a number"

    def test_headers_and_filename(self, client, store):
        r = client.get(
            CSV_PATH, params={"campaign": "aug-promo", "tier": "VIP"}, headers=_admin_headers(),
        )
        assert r.headers["content-type"].startswith("text/csv")
        disposition = r.headers["content-disposition"]
        assert "attachment" in disposition
        assert "sms_aug-promo_vip_" in disposition

    def test_columns(self, client, store):
        rows = self._rows(client.get(CSV_PATH, headers=_admin_headers()))
        assert list(rows[0]) == [
            "buyer_id", "full_name", "phone", "city", "tier", "assignment",
            "orders", "ltv", "ltv_basis", "avg_order_value",
            "revenue_ltv", "margin_ltv", "margin_pct", "cost_coverage_pct",
            "recency_days", "last_order_date", "first_order_date",
            "last_order_id", "last_order_total", "last_order_item_count",
            "last_order_items",
        ]

    def test_last_order_columns_carry_the_personalisation_hook(self, client, store):
        rows = self._rows(client.get(CSV_PATH, headers=_admin_headers()))
        row = rows[0]

        assert row["last_order_date"] == "2026-07-16"
        assert row["last_order_id"] == "90001"
        assert row["last_order_total"] == "2450.0"
        assert row["last_order_item_count"] == "5"
        assert "Abib COLLAGEN GEL MASK" in row["last_order_items"]
        assert row["last_order_items"].endswith("+2 ещё")

    def test_both_bases_travel_with_the_file(self, client, store):
        """The export must be re-rankable in a spreadsheet without a second pull."""
        rows = self._rows(client.get(
            CSV_PATH, params={"ltv_basis": "margin"}, headers=_admin_headers(),
        ))
        assert rows[0]["ltv_basis"] == "margin"
        assert rows[0]["revenue_ltv"] == "12000.0"
        assert rows[0]["margin_ltv"] == "6600.0"
        assert rows[0]["cost_coverage_pct"] == "96.5"

    def test_basis_is_in_the_filename(self, client, store):
        r = client.get(
            CSV_PATH,
            params={"campaign": "aug-promo", "ltv_basis": "margin"},
            headers=_admin_headers(),
        )
        assert "sms_aug-promo_all_margin_" in r.headers["content-disposition"]

    def test_export_always_requests_customer_rows(self, client, store):
        client.get(CSV_PATH, headers=_admin_headers())
        assert store.calls[0]["include_customers"] is True

    def test_no_freeze_by_default(self, client, store):
        """Exploratory pulls must not litter the campaign table."""
        r = client.get(CSV_PATH, headers=_admin_headers())
        assert r.status_code == 200
        assert store.freezes == []
        assert r.headers["X-Campaign-Frozen"] == "false"

    def test_freeze_records_the_whole_roster(self, client, store):
        """The holdout is excluded from the CSV but must still be recorded."""
        r = client.get(
            CSV_PATH,
            params={"freeze": "true", "campaign": "aug-promo", "promocode": "KS-AUG"},
            headers=_admin_headers(),
        )
        assert r.status_code == 200

        assert len(store.freezes) == 1
        frozen = store.freezes[0]
        assert frozen["campaign"] == "aug-promo"
        assert frozen["promocode"] == "KS-AUG"
        assert len(frozen["customers"]) == 3, "target AND holdout are frozen"
        assert {c["assignment"] for c in frozen["customers"]} == {"target", "holdout"}

        # ...while the file itself still carries only the target group
        assert len(self._rows(r)) == 2
        assert r.headers["X-Campaign-Frozen"] == "true"
        assert r.headers["X-Campaign-Holdout"] == "1"

    def test_truncated_roster_refuses_to_freeze(self, client, store):
        """A partial roster would silently shrink the control group."""
        store.truncated = True
        r = client.get(
            CSV_PATH, params={"freeze": "true", "campaign": "aug-promo"},
            headers=_admin_headers(),
        )
        assert r.status_code == 400
        assert "limit" in r.json()["detail"]
        assert store.freezes == []

    def test_refreezing_conflicts(self, client, store):
        store.freeze_error = "campaign 'aug-promo' is already frozen"
        r = client.get(
            CSV_PATH, params={"freeze": "true", "campaign": "aug-promo"},
            headers=_admin_headers(),
        )
        assert r.status_code == 409
        assert "already frozen" in r.json()["detail"]

    def test_results_defaults_and_window(self, client, store):
        r = client.get(RESULTS_PATH, headers=_admin_headers())
        assert r.status_code == 200
        assert store.result_calls[0] == {"campaign": "aug", "window_days": 30,
                                         "delivered_only": False}

        client.get(RESULTS_PATH, params={"window_days": 60}, headers=_admin_headers())
        assert store.result_calls[1]["window_days"] == 60

    def test_results_window_bounds(self, client, store):
        assert client.get(
            RESULTS_PATH, params={"window_days": 400}, headers=_admin_headers(),
        ).status_code == 422

    def test_unknown_campaign_is_404(self, client, store):
        store.results_error = "campaign 'aug' is not frozen"
        r = client.get(RESULTS_PATH, headers=_admin_headers())
        assert r.status_code == 404

    def test_unsent_campaign_is_409(self, client, store):
        """Frozen but unsent is a state problem, not a missing resource."""
        store.results_error = "campaign 'aug' has no send date — mark it sent"
        r = client.get(RESULTS_PATH, headers=_admin_headers())
        assert r.status_code == 409
        assert "send date" in r.json()["detail"]

    def test_export_is_rate_limited(self, client, store):
        """Bulk PII export is capped — a leaked session can't drain the base."""
        codes = [client.get(CSV_PATH, headers=_admin_headers()).status_code for _ in range(7)]
        assert codes[:5] == [200] * 5
        assert 429 in codes[5:]


# ─── Test send ────────────────────────────────────────────────────────────

class _FakeTurboClient:
    """Stands in for the gateway; records what it was asked to send."""

    sent: list = []
    error: Exception | None = None
    results: list = []

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    async def send(self, phones, text, viber=None):
        if type(self).error:
            raise type(self).error
        type(self).sent.append({"phones": phones, "text": text, "viber": viber})
        return type(self).results


@pytest.fixture
def gateway(monkeypatch):
    from core.turbosms import SendResult

    _FakeTurboClient.sent = []
    _FakeTurboClient.error = None
    _FakeTurboClient.results = [
        SendResult(phone="380961111111", message_id="msg-1", code=0, status="OK"),
    ]
    monkeypatch.setattr(
        "web.routes.api.customers.TurboSmsClient", lambda *a, **kw: _FakeTurboClient()
    )
    return _FakeTurboClient


class TestTestSend:
    """A rehearsal must reach the gateway and touch nothing else."""

    def test_requires_admin_dependency(self):
        route = _route(TEST_SEND_PATH, "POST")
        assert route is not None, "test-send is not registered"
        assert require_admin in _all_dep_calls(route.dependant)

    def test_requires_session(self, client):
        assert client.post(
            TEST_SEND_PATH, params={"phone": "380961111111", "text": "hi"},
        ).status_code == 401

    def test_sends_the_normalised_number(self, client, gateway):
        r = client.post(
            TEST_SEND_PATH,
            params={"phone": "+38 (096) 111-11-11", "text": "hi"},
            headers=_admin_headers(),
        )

        assert r.status_code == 200
        assert gateway.sent == [
            {"phones": ["380961111111"], "text": "hi", "viber": None},
        ]
        assert r.json()["accepted"] is True

    @pytest.mark.parametrize("phone", ["0961111111", "380961111", "123456789012"])
    def test_rejects_anything_a_campaign_could_not_contain(self, client, gateway, phone):
        r = client.post(
            TEST_SEND_PATH, params={"phone": phone, "text": "hi"},
            headers=_admin_headers(),
        )

        # 400 from the 380-prefix rule, 422 when it is too short to reach it.
        assert r.status_code in (400, 422)
        assert gateway.sent == [], "a bad number must not reach the gateway"

    def test_reports_the_billed_cost(self, client, gateway):
        """The cost cliff is the point: Cyrillic drops the limit to 70."""
        r = client.post(
            TEST_SEND_PATH, params={"phone": "380961111111", "text": "я" * 71},
            headers=_admin_headers(),
        )

        assert r.json()["cost"] == {"encoding": "ucs2", "characters": 71, "parts": 2}

    def test_stoplist_refusal_is_reported_not_recorded(self, client, gateway, store):
        from core.turbosms import SendResult

        gateway.results = [
            SendResult(phone="380961111111", message_id=None, code=404,
                       status="NOT_ALLOWED_NUMBER_STOPLIST"),
        ]

        body = client.post(
            TEST_SEND_PATH, params={"phone": "380961111111", "text": "hi"},
            headers=_admin_headers(),
        ).json()

        assert (body["accepted"], body["stoplisted"]) == (False, True)
        assert store.freezes == [], "a rehearsal must not create a campaign"

    def test_gateway_failure_is_a_502(self, client, gateway):
        from core.turbosms import TurboSmsError

        gateway.error = TurboSmsError("TurboSMS is not configured")
        r = client.post(
            TEST_SEND_PATH, params={"phone": "380961111111", "text": "hi"},
            headers=_admin_headers(),
        )

        assert r.status_code == 502
        assert "not configured" in r.json()["detail"]


class TestChannelSelection:
    """Viber is a different message, not a nicer SMS — the route has to say so."""

    def test_channels_endpoint_reports_what_is_configured(self, client, monkeypatch):
        monkeypatch.setenv("TURBOSMS_API_TOKEN", "tok")
        monkeypatch.setenv("TURBOSMS_SENDER", "KoreanStory")
        monkeypatch.delenv("TURBOSMS_VIBER_SENDER", raising=False)

        body = client.get(CHANNELS_PATH, headers=_admin_headers()).json()

        assert body["sms"] is True
        assert body["viber"] is False

    def test_channels_endpoint_is_admin_only(self):
        route = _route(CHANNELS_PATH)
        assert route is not None
        assert require_admin in _all_dep_calls(route.dependant)

    def test_hybrid_send_passes_a_viber_message(self, client, gateway):
        client.post(
            TEST_SEND_PATH,
            params={
                "phone": "380961111111",
                "text": "Знижка https://example.com",
                "channel": "viber_sms",
                "viber_text": "Знижка",
                "button_caption": "Korean Story",
                "button_url": "https://example.com",
            },
            headers=_admin_headers(),
        )

        viber = gateway.sent[0]["viber"]
        assert viber is not None
        assert (viber.text, viber.caption) == ("Знижка", "Korean Story")
        # The fallback has no button, so its own text must carry the link.
        assert gateway.sent[0]["text"] == "Знижка https://example.com"

    def test_sms_channel_sends_no_viber_message(self, client, gateway):
        client.post(
            TEST_SEND_PATH,
            params={"phone": "380961111111", "text": "Знижка", "channel": "sms"},
            headers=_admin_headers(),
        )

        assert gateway.sent[0]["viber"] is None

    def test_viber_text_defaults_to_the_sms_text(self, client, gateway):
        client.post(
            TEST_SEND_PATH,
            params={"phone": "380961111111", "text": "Знижка", "channel": "viber_sms"},
            headers=_admin_headers(),
        )

        assert gateway.sent[0]["viber"].text == "Знижка"

    def test_a_button_without_a_url_is_rejected_before_sending(self, client, gateway):
        r = client.post(
            TEST_SEND_PATH,
            params={
                "phone": "380961111111", "text": "Знижка",
                "channel": "viber_sms", "button_caption": "Korean Story",
            },
            headers=_admin_headers(),
        )

        assert r.status_code == 400
        assert "action URL" in r.json()["detail"]
        assert gateway.sent == []

    def test_unknown_channel_is_refused(self, client, gateway):
        r = client.post(
            TEST_SEND_PATH,
            params={"phone": "380961111111", "text": "hi", "channel": "telegram"},
            headers=_admin_headers(),
        )

        assert r.status_code == 422
        assert gateway.sent == []


class TestMultiTierSelection:
    """A discount suits Core and Reactivation and cannibalises VIP.

    That is one campaign with one text, so the filter has to take a set —
    splitting it into two rosters would mean two sends to reconcile later.
    """

    def test_several_tiers_reach_the_store_as_a_list(self, client, store):
        client.get(
            SEGMENTS_PATH, params={"tier": "CORE,REACTIVATION"},
            headers=_admin_headers(),
        )

        assert store.calls[0]["tier"] == ["CORE", "REACTIVATION"]

    def test_whitespace_and_case_are_forgiven(self, client, store):
        client.get(
            SEGMENTS_PATH, params={"tier": " core , Reactivation "},
            headers=_admin_headers(),
        )

        assert store.calls[0]["tier"] == ["CORE", "REACTIVATION"]

    def test_duplicates_collapse(self, client, store):
        client.get(
            SEGMENTS_PATH, params={"tier": "CORE,CORE"}, headers=_admin_headers(),
        )

        assert store.calls[0]["tier"] == ["CORE"]

    def test_one_bad_name_rejects_the_whole_request(self, client, store):
        """Silently dropping it would send to fewer people than were asked for."""
        r = client.get(
            SEGMENTS_PATH, params={"tier": "CORE,GOLD"}, headers=_admin_headers(),
        )

        assert r.status_code == 400
        assert "GOLD" in r.json()["detail"]
        assert store.calls == []

    def test_empty_tier_means_every_tier(self, client, store):
        client.get(SEGMENTS_PATH, params={"tier": ""}, headers=_admin_headers())

        assert store.calls[0]["tier"] is None

    def test_export_filename_names_the_tiers_it_holds(self, client, store):
        r = client.get(
            CSV_PATH, params={"tier": "CORE,REACTIVATION", "campaign": "aug"},
            headers=_admin_headers(),
        )

        assert "sms_aug_core-reactivation" in r.headers["content-disposition"]


class TestPartialSend:
    """A batch failing mid-roster must not look like a clean failure.

    Earlier batches are already on people's phones. Discarding them would let
    a retry message those people twice — twice the spend, and the control
    comparison ruined.
    """

    SEND_PATH = "/api/customers/sms-campaigns/aug/send"

    @pytest.fixture
    def sending_store(self, monkeypatch):
        recorded = {}

        class _Store:
            async def get_sms_campaign_targets(self, campaign):
                return [
                    {"buyerId": 1, "phone": "380961111111", "tier": "CORE"},
                    {"buyerId": 2, "phone": "380962222222", "tier": "CORE"},
                ]

            async def record_sms_send(self, campaign, accepted, stoplisted, failed, **kw):
                recorded.update({"accepted": accepted, "stoplisted": stoplisted,
                                 "failed": failed})
                return {"campaign": campaign, "accepted": len(accepted),
                        "stoplisted": len(stoplisted), "failed": len(failed)}

            async def release_sms_campaign(self, campaign):
                recorded["released"] = campaign

        async def _fake_get_store():
            return _Store()

        monkeypatch.setattr("web.routes.api.customers.get_store", _fake_get_store)
        return recorded

    def test_what_already_went_is_recorded(self, client, sending_store, monkeypatch):
        from core.turbosms import PartialSendError, SendResult, TurboSmsError

        class _Client:
            async def __aenter__(self):
                return self

            async def __aexit__(self, *exc):
                return False

            async def send(self, phones, text, viber=None):
                raise PartialSendError(
                    [SendResult(phone="380961111111", message_id="m-1",
                                code=0, status="OK")],
                    sent=1, unsent=1,
                    cause=TurboSmsError("405 NOT_ALLOWED_RECIPIENTS_LIMIT"),
                )

        monkeypatch.setattr(
            "web.routes.api.customers.TurboSmsClient", lambda *a, **kw: _Client()
        )

        body = client.post(
            self.SEND_PATH, params={"text": "hi"}, headers=_admin_headers(),
        ).json()

        assert sending_store["accepted"] == {1: "m-1"}, "the delivered one is kept"
        assert body["unsent"] == 1
        assert "NOT_ALLOWED_RECIPIENTS_LIMIT" in body["partialError"]

    def test_a_total_failure_records_nothing_and_stays_retryable(
        self, client, sending_store, monkeypatch,
    ):
        from core.turbosms import TurboSmsError

        class _Client:
            async def __aenter__(self):
                return self

            async def __aexit__(self, *exc):
                return False

            async def send(self, phones, text, viber=None):
                raise TurboSmsError("405 NOT_ALLOWED_RECIPIENTS_LIMIT")

        monkeypatch.setattr(
            "web.routes.api.customers.TurboSmsClient", lambda *a, **kw: _Client()
        )

        r = client.post(
            self.SEND_PATH, params={"text": "hi"}, headers=_admin_headers(),
        )

        assert r.status_code == 502
        assert "accepted" not in sending_store, "nothing went out, nothing recorded"
        # The claim is taken before the gateway call, so a total failure has to
        # hand it back or the campaign is stuck unsendable.
        assert sending_store["released"] == "aug"

    def test_a_partial_failure_keeps_the_claim(self, client, sending_store, monkeypatch):
        """Messages left, so the campaign must never become sendable again."""
        from core.turbosms import PartialSendError, SendResult, TurboSmsError

        class _Client:
            async def __aenter__(self):
                return self

            async def __aexit__(self, *exc):
                return False

            async def send(self, phones, text, viber=None):
                raise PartialSendError(
                    [SendResult(phone="380961111111", message_id="m-1",
                                code=0, status="OK")],
                    sent=1, unsent=1, cause=TurboSmsError("gateway said no"),
                )

        monkeypatch.setattr(
            "web.routes.api.customers.TurboSmsClient", lambda *a, **kw: _Client()
        )

        client.post(self.SEND_PATH, params={"text": "hi"}, headers=_admin_headers())

        assert "released" not in sending_store
