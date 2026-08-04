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

    async def get_sms_campaign_results(self, campaign, window_days=30):
        if self.results_error:
            raise ValueError(self.results_error)
        self.result_calls.append({"campaign": campaign, "window_days": window_days})
        return {"campaign": campaign, "windowDays": window_days, "segments": []}

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
        assert store.calls[0]["tier"] == "VIP"


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
        assert store.result_calls[0] == {"campaign": "aug", "window_days": 30}

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
