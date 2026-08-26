"""API tests for audience filters, saved audiences, and creating a campaign.

Three contracts, and each one has already been got wrong somewhere:

* the filter parameters must reach the store as a filter object, not as loose
  keywords that the segmentation silently ignores;
* a preset is stored form state — it must round-trip verbatim and must not be
  able to shadow a built-in;
* freezing a roster must be reachable without downloading a file of phone
  numbers, and must refuse the cases where the frozen list would be wrong.
"""
import time

import pytest
from fastapi.testclient import TestClient

from web.main import app
from web.routes.auth import session_serializer, create_session_data, SESSION_COOKIE
from core.permissions import ADMIN_USER_IDS
from core.repositories.customers import BUILTIN_AUDIENCE_PRESETS

ADMIN_ID = sorted(ADMIN_USER_IDS)[0]

SEGMENTS = "/api/customers/sms-segments"
PRESETS = "/api/customers/sms-audience-presets"
CAMPAIGNS = "/api/customers/sms-campaigns"


def _headers() -> dict:
    data = create_session_data(
        {
            "id": str(ADMIN_ID),
            "first_name": "Test",
            "username": "tester",
            "auth_date": str(int(time.time())),
        },
        role="admin",
    )
    return {"Cookie": f"{SESSION_COOKIE}={session_serializer.dumps(data)}"}


def _customer(buyer_id: int, assignment: str) -> dict:
    return {
        "buyerId": buyer_id, "fullName": f"Buyer {buyer_id}",
        "phone": f"3809{buyer_id:08d}", "city": "Kyiv", "tier": "ALL",
        "orders": 1, "ltv": 900.0, "avgOrderValue": 900.0,
        "revenueLtv": 900.0, "marginLtv": 500.0, "marginPct": 55.0,
        "costCoverage": 99.0, "recencyDays": 20,
        "lastOrderDate": "2026-08-01", "firstOrderDate": "2026-08-01",
        "lastOrderId": 5, "lastOrderTotal": 900.0, "lastOrderItemCount": 1,
        "lastOrderItems": "Product", "assignment": assignment,
    }


class _FakeStore:
    def __init__(self):
        self.calls = []
        self.freezes = []
        self.presets = []
        self.saved = []
        self.deleted = []
        self.customers = [_customer(1, "target"), _customer(2, "holdout")]
        self.truncated = False
        self.freeze_error = None
        self.save_error = None
        self.delete_result = True

    async def get_sms_segments(self, **kwargs):
        self.calls.append(kwargs)
        return {
            "campaign": kwargs.get("campaign"),
            "grouping": kwargs.get("grouping"),
            "criteria": {"grouping": kwargs.get("grouping"), "filters": {}},
            "segments": [{"tier": "ALL", "total": 2, "target": 1, "holdout": 1}],
            "totals": {"customers": 2, "target": 1, "holdout": 1},
            "funnel": [{"stage": "customers", "remaining": 9}],
            "customers": self.customers if kwargs.get("include_customers") else [],
            "truncated": self.truncated,
        }

    async def freeze_sms_campaign(self, **kwargs):
        if self.freeze_error:
            raise ValueError(self.freeze_error)
        self.freezes.append(kwargs)
        return {"campaign": kwargs["campaign"], "frozen": True}

    async def list_sms_audience_presets(self):
        return self.presets

    async def save_sms_audience_preset(self, name, criteria, created_by=None):
        if self.save_error:
            raise ValueError(self.save_error)
        self.saved.append((name, criteria, created_by))
        return {"name": name, "criteria": criteria, "builtin": False}

    async def delete_sms_audience_preset(self, name):
        self.deleted.append(name)
        return self.delete_result


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

    async def _fake_get_store():
        return fake

    monkeypatch.setattr("web.routes.api.customers.get_store", _fake_get_store)
    return fake


# ─── Filters reach the store ──────────────────────────────────────────────

class TestFilterParameters:
    def test_filters_are_passed_as_a_filter_object(self, client, store):
        r = client.get(SEGMENTS, params={
            "grouping": "single",
            "brand": "Medi-Peel, Anua",
            "category_id": "10,11",
            "source_id": "1",
            "city": "Kyiv",
            "promocode_used": "KS-AUG",
            "bought_within_days": 90,
            "recency_min": 30,
            "recency_max": 200,
            "orders_min": 2,
            "ltv_min": 1000,
            "aov_max": 5000,
            "first_order_from": "2026-01-01",
        }, headers=_headers())
        assert r.status_code == 200, r.text

        sent = store.calls[-1]
        assert sent["grouping"] == "single"
        f = sent["filters"]
        assert f.brands == ("Medi-Peel", "Anua")
        assert f.category_ids == (10, 11)
        assert f.source_ids == (1,)
        assert f.cities == ("Kyiv",)
        assert f.promocode == "KS-AUG"
        assert f.bought_within_days == 90
        assert (f.recency_min_days, f.recency_max_days) == (30, 200)
        assert f.orders_min == 2
        assert f.ltv_min == 1000
        assert f.aov_max == 5000
        assert f.first_order_from.isoformat() == "2026-01-01"

    def test_no_filter_parameters_means_an_empty_filter_set(self, client, store):
        client.get(SEGMENTS, headers=_headers())
        sent = store.calls[-1]
        assert sent["filters"].is_empty()
        assert sent["grouping"] == "rfm"

    def test_duplicate_brands_collapse(self, client, store):
        client.get(SEGMENTS, params={"brand": "Anua, anua , Anua"}, headers=_headers())
        assert store.calls[-1]["filters"].brands == ("Anua",)

    @pytest.mark.parametrize("params,detail", [
        ({"grouping": "cohorts"}, "grouping"),
        ({"recency_min": 200, "recency_max": 30}, "recency"),
        ({"ltv_min": 900, "ltv_max": 100}, "ltv"),
        ({"category_id": "10,face"}, "category_id"),
    ])
    def test_nonsense_is_refused_with_a_reason(self, client, store, params, detail):
        r = client.get(SEGMENTS, params=params, headers=_headers())
        assert r.status_code == 400
        assert detail in r.json()["detail"]


# ─── Saved audiences ──────────────────────────────────────────────────────

class TestPresets:
    def test_listing_is_whatever_the_store_holds(self, client, store):
        store.presets = [{"name": "RFM tiers", "criteria": {}, "builtin": True}]
        r = client.get(PRESETS, headers=_headers())
        assert r.status_code == 200
        assert r.json()["presets"] == store.presets

    def test_saving_round_trips_the_criteria(self, client, store):
        body = {"grouping": "single", "filters": {"brands": ["Anua"]}}
        r = client.put(f"{PRESETS}/Anua buyers", json=body, headers=_headers())
        assert r.status_code == 200
        assert r.json()["criteria"] == body
        name, criteria, created_by = store.saved[-1]
        assert (name, criteria, created_by) == ("Anua buyers", body, ADMIN_ID)

    def test_a_builtin_name_is_refused(self, client, store):
        store.save_error = "'RFM tiers' is a built-in audience and cannot be replaced"
        r = client.put(
            f"{PRESETS}/{list(BUILTIN_AUDIENCE_PRESETS)[0]}", json={}, headers=_headers(),
        )
        assert r.status_code == 409

    def test_a_non_object_body_is_refused(self, client, store):
        r = client.put(f"{PRESETS}/x", json=["not", "an", "object"], headers=_headers())
        assert r.status_code == 400
        assert store.saved == []

    def test_deleting_a_missing_audience_is_404(self, client, store):
        store.delete_result = False
        assert client.delete(f"{PRESETS}/ghost", headers=_headers()).status_code == 404

    def test_presets_need_a_session(self, client):
        assert client.get(PRESETS).status_code == 401
        assert client.put(f"{PRESETS}/x", json={}).status_code == 401

    def test_presets_carry_the_sms_permission(self):
        """These endpoints travel with the other nine, not behind require_admin.

        A manager who can see the wizard and cannot save an audience or freeze
        a campaign has a page that does nothing — which is what leaving these
        four on the admin gate would have produced.
        """
        from tests.routes_helper import route_dependencies

        def gates(path, method):
            found = set()
            for dep in route_dependencies(app, path, method):
                code = getattr(dep, "__code__", None)
                closure = getattr(dep, "__closure__", None)
                if code is None or not closure:
                    continue
                cells = dict(zip(code.co_freevars, closure))
                if "feature" in cells and "action" in cells:
                    found.add((cells["feature"].cell_contents,
                               cells["action"].cell_contents))
            return found

        assert ("sms", "view") in gates(PRESETS, "GET")
        assert ("sms", "edit") in gates(
            "/api/customers/sms-audience-presets/{name}", "PUT")
        assert ("sms", "edit") in gates(
            "/api/customers/sms-audience-presets/{name}", "DELETE")
        assert ("sms", "edit") in gates(CAMPAIGNS, "POST")


# ─── Creating a campaign without a CSV ────────────────────────────────────

class TestCreateCampaign:
    def test_freezes_the_roster_and_reports_it(self, client, store):
        r = client.post(
            CAMPAIGNS,
            params={"campaign": "sep-brand", "grouping": "single", "holdout_pct": 30},
            headers=_headers(),
        )
        assert r.status_code == 200, r.text
        body = r.json()
        assert body["campaign"] == "sep-brand"
        assert body["frozen"]["frozen"] is True
        assert body["totals"] == {"customers": 2, "target": 1, "holdout": 1}

        frozen = store.freezes[-1]
        assert frozen["campaign"] == "sep-brand"
        assert frozen["holdout_pct"] == 30
        assert len(frozen["customers"]) == 2

    def test_the_placeholder_name_is_refused(self, client, store):
        r = client.post(CAMPAIGNS, headers=_headers())
        assert r.status_code == 400
        assert store.freezes == []

    def test_an_empty_audience_is_refused(self, client, store):
        store.customers = []
        r = client.post(CAMPAIGNS, params={"campaign": "empty"}, headers=_headers())
        assert r.status_code == 400
        assert "empty" in r.json()["detail"]
        assert store.freezes == []

    def test_a_truncated_roster_is_refused(self, client, store):
        store.truncated = True
        r = client.post(CAMPAIGNS, params={"campaign": "big"}, headers=_headers())
        assert r.status_code == 400
        assert store.freezes == []

    def test_an_existing_campaign_conflicts(self, client, store):
        store.freeze_error = "campaign 'sep-brand' already frozen"
        r = client.post(CAMPAIGNS, params={"campaign": "sep-brand"}, headers=_headers())
        assert r.status_code == 409

    def test_creating_needs_a_session(self, client):
        assert client.post(CAMPAIGNS, params={"campaign": "x"}).status_code == 401
