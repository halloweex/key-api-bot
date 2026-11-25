"""API tests for the SMS routes: who may call them, the audience and its CSV,
the results, the test send and the opt-out.

These hand out customer names and phone numbers and spend money, so who may
call what, the holdout and what a call writes must all hold at the route.
The tests of those run the route over the test Postgres as a signed-in
visitor (`tests/helpers/app_client.py`), with buyers and Silver's orders
seeded (`tests/integration/pg_sms_seed.py`), and read back what it answered
and wrote. The rest — parameter validation, the CSV's shape, the test send's
message — stand on a stubbed store and gateway; segmentation itself is
covered by tests/unit/test_sms_segments.py.
"""
import csv
import io
import time
from datetime import timedelta
from urllib.parse import urlencode

import pytest
import pytest_asyncio
from fastapi.testclient import TestClient

from tests.helpers.app_client import signed_in
from tests.helpers.sms_send_gateway import install_gateway
from tests.integration import pg_sms_seed
from tests.integration.pg_sms_seed import insert, order_row, pg_conn
from tests.routes_helper import iter_endpoints
from web.main import app
from web.routes.auth import (
    session_serializer,
    create_session_data,
    SESSION_COOKIE,
)
from core.permissions import ADMIN_USER_IDS

ADMIN_ID = sorted(ADMIN_USER_IDS)[0]

SEGMENTS_PATH = "/api/customers/sms-segments"
CSV_PATH = "/api/customers/sms-segments/export/csv"
RESULTS_PATH = "/api/customers/sms-campaigns/aug/results"
TEST_SEND_PATH = "/api/customers/sms/test-send"
CHANNELS_PATH = "/api/customers/sms/channels"
OPTOUT_PATH = "/api/customers/sms-campaigns/optout"


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
        self.optouts = []

    async def add_marketing_optout(self, **kwargs):
        self.optouts.append(kwargs)
        return {"buyerId": kwargs.get("buyer_id"), "channel": "sms",
                "totalOptouts": len(self.optouts)}

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


# ─── Over the test Postgres ───────────────────────────────────────────────

@pytest_asyncio.fixture
async def on_pg(monkeypatch):
    """`/sms`'s tables and the dashboard's access lists, emptied before and
    after, on the test Postgres."""
    if not pg_sms_seed.DSN:
        pytest.skip("needs a live PostgreSQL at KS_PG_DSN")
    async with pg_sms_seed.on_postgres(
        monkeypatch, pg_sms_seed.SMS_TABLES + pg_sms_seed.ACCESS_TABLES,
    ) as pool:
        yield pool


# Every line sells one unit of PRODUCT at ₴1 000; it costs ₴200, an 80 % margin.
PRODUCT = 102


async def _catalogue(conn) -> None:
    await insert(conn, "bronze.products", [
        dict(id=PRODUCT, name="Cream", sku="SKU-102", price=1000)])
    await insert(conn, "bronze.offer_stocks", [
        dict(id=1, sku="SKU-102", price=1000, purchased_price=200, quantity=10)])


async def _buyer(conn, buyer_id: int, phone: str, orders) -> None:
    """A retail buyer and their orders, each `(order id, days ago, units)` —
    one line of PRODUCT, billed in full."""
    await insert(conn, "bronze.buyers", [
        dict(id=buyer_id, full_name=f"Buyer {buyer_id}", phone=phone, city="Kyiv")])
    for oid, days_ago, units in orders:
        day = pg_sms_seed.kyiv_today() - timedelta(days=days_ago)
        await insert(conn, "silver.orders", [
            order_row(oid=oid, buyer_id=buyer_id, day=day, total=1000 * units)])
        await insert(conn, "bronze.order_products", [dict(
            id=oid, order_id=oid, product_id=PRODUCT, name="Cream",
            quantity=units, price_sold=1000)])


async def _ladder() -> None:
    """One buyer for each tier on the revenue basis, and one the basis decides.

    1 — three orders of 10 units, ₴30 000: VIP on either basis.
    2 — two orders of one unit: CORE by its order count.
    3 — one unit, 60 days ago: REACTIVATION.
    4 — one order of 8 units: ₴8 000 is CORE on revenue (VIP from ₴10 000);
        its ₴6 400 margin is VIP on margin's own cut-off (₴5 500), and CORE
        under a cut-off typed at ₴9 000.
    """
    async with pg_conn() as conn:
        await _catalogue(conn)
        await _buyer(conn, 1, "380960000001", [(11, 10, 10), (12, 11, 10), (13, 12, 10)])
        await _buyer(conn, 2, "380960000002", [(21, 20, 1), (22, 40, 1)])
        await _buyer(conn, 3, "380960000003", [(31, 60, 1)])
        await _buyer(conn, 4, "380960000004", [(41, 10, 8)])


LADDER_TIERS = {1: "VIP", 2: "CORE", 3: "REACTIVATION", 4: "CORE"}


def _tiers(response) -> dict:
    assert response.status_code == 200, response.text
    return {c["buyerId"]: c["tier"] for c in response.json()["customers"]}


# ─── Who may call each route ──────────────────────────────────────────────

# Every SMS route: (method, route, query, the action on `sms` it needs). Each
# query is one the route answers without sending or writing anything once
# the visitor is through the gate — no confirmation for the send, a number
# the test send and the opt-out refuse, campaigns and presets that do not
# exist — so its status says only whether the gate let the visitor in.
SMS_ROUTES = [
    ("GET", "/api/customers/sms-segments", {}, "view"),
    # The sizes are `view`; the names and phone numbers behind them are `edit`.
    ("GET", "/api/customers/sms-segments", {"include_customers": "true"}, "edit"),
    ("GET", "/api/customers/sms-segments/export/csv", {}, "edit"),
    ("GET", "/api/customers/sms-audience-presets", {}, "view"),
    ("PUT", "/api/customers/sms-audience-presets/{name}", {}, "edit"),
    ("DELETE", "/api/customers/sms-audience-presets/{name}", {}, "edit"),
    ("GET", "/api/customers/sms-campaigns", {}, "view"),
    ("POST", "/api/customers/sms-campaigns", {"campaign": "matrix"}, "edit"),
    ("GET", "/api/customers/sms-campaigns/{campaign}/results", {}, "view"),
    # Marking a campaign sent sets where every measurement window starts.
    ("POST", "/api/customers/sms-campaigns/{campaign}/sent", {}, "edit"),
    ("POST", "/api/customers/sms-campaigns/{campaign}/send", {"text": "hi"}, "edit"),
    ("POST", "/api/customers/sms-campaigns/optout",
     {"buyer_id": 1, "phone": "0961111111"}, "edit"),
    ("GET", "/api/customers/sms/channels", {}, "view"),
    ("POST", "/api/customers/sms/test-send", {"phone": "0961111111", "text": "hi"}, "edit"),
]


def _label(method: str, route: str, query: dict) -> str:
    return f"{method} {route}" + (f"?{urlencode(query)}" if query else "")


def _url(route: str) -> str:
    return route.format(name="matrix", campaign="matrix")


class TestWhoMayUseEachRoute:
    """The `sms` tab decides which of these a person may open, and the level
    what they may do there: `view` is "the tab is in my set", `edit` is that
    and editor or above. The rows, the CSV, the send and the opt-out are
    names, phone numbers and money; a plain viewer must never reach them,
    and a viewer holding the tab reads sizes and results without sending."""

    def test_every_sms_route_is_in_the_table(self):
        served = {
            (method, endpoint.path)
            for endpoint in iter_endpoints(app)
            if endpoint.path.startswith("/api/customers/sms")
            for method in endpoint.methods - {"HEAD"}
        }
        assert served == {(method, route) for method, route, _q, _a in SMS_ROUTES}

    def test_a_visitor_without_a_session_is_refused_everywhere(self):
        client = TestClient(app)

        statuses = {
            _label(method, route, query):
                client.request(method, _url(route), params=query).status_code
            for method, route, query, _action in SMS_ROUTES
        }

        assert statuses == {label: 401 for label in statuses}

    @pytest.mark.parametrize("role, tabs, may", [
        # The level's own tabs hold no SMS, an editor's no more than a viewer's.
        ("viewer", None, set()),
        ("editor", None, set()),
        ("viewer", "sms", {"view"}),
        ("editor", "sms", {"view", "edit"}),
    ])
    @pytest.mark.asyncio
    async def test_the_tab_opens_the_routes_and_the_level_the_edits(
        self, on_pg, monkeypatch, role, tabs, may,
    ):
        install_gateway(monkeypatch)   # nothing here may reach TurboSMS

        async with signed_in(role, tabs=tabs) as visitor:
            answered = {}
            for method, route, query, _action in SMS_ROUTES:
                r = await visitor.request(method, _url(route), params=query)
                answered[_label(method, route, query)] = (
                    "refused" if r.status_code == 403
                    else "no session" if r.status_code == 401 else "let in")

        assert answered == {
            _label(method, route, query): "let in" if action in may else "refused"
            for method, route, query, action in SMS_ROUTES
        }


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


class TestTheAudienceOverPostgres:
    """What the preview answers about real buyers (`_ladder`): the cut-offs
    each basis brings, the rows withheld unless asked for, the tiers a
    campaign names."""

    @pytest.mark.asyncio
    async def test_the_margin_basis_brings_its_own_cut_offs(self, on_pg):
        """Margin cut-offs are set to select the same share of the base, so
        switching the basis swaps them — or every tier would shrink by about
        the blended margin rate — and a cut-off typed by hand wins over
        either. Buyer 4's ₴6 400 margin is VIP on margin's own cut-off and
        CORE under a typed ₴9 000; its ₴8 000 revenue is CORE."""
        await _ladder()

        async with signed_in("admin") as admin:
            revenue = await admin.get(SEGMENTS_PATH, params={"include_customers": "true"})
            margin = await admin.get(SEGMENTS_PATH, params={
                "include_customers": "true", "ltv_basis": "margin"})
            typed = await admin.get(SEGMENTS_PATH, params={
                "include_customers": "true", "ltv_basis": "margin",
                "vip_ltv": 9000, "core_ltv": 1000})

        assert _tiers(revenue) == LADDER_TIERS
        assert _tiers(margin) == {**LADDER_TIERS, 4: "VIP"}
        assert _tiers(typed) == LADDER_TIERS

    @pytest.mark.asyncio
    async def test_customer_rows_come_only_when_asked_for(self, on_pg):
        """The preview counts people; their names and phone numbers leave
        only on request."""
        await _ladder()

        async with signed_in("admin") as admin:
            sizes = await admin.get(SEGMENTS_PATH)
            rows = await admin.get(SEGMENTS_PATH, params={"include_customers": "true"})

        assert sizes.status_code == 200, sizes.text
        assert sizes.json()["totals"]["customers"] == 4
        assert sizes.json()["customers"] == []
        assert set(_tiers(rows)) == {1, 2, 3, 4}

    @pytest.mark.parametrize("tier, chosen", [
        ("vip", ["VIP"]),
        ("CORE,REACTIVATION", ["CORE", "REACTIVATION"]),
        (" core , Reactivation ", ["CORE", "REACTIVATION"]),
        ("CORE,CORE", ["CORE"]),
        ("", ["VIP", "CORE", "REACTIVATION"]),
    ])
    @pytest.mark.asyncio
    async def test_the_tiers_a_campaign_names_are_read_forgivingly(
        self, on_pg, tier, chosen,
    ):
        """One campaign may take several tiers — a discount suits Core and
        Reactivation and cannibalises VIP — so the filter is a set: case,
        spaces and a repeat are forgiven, and no tier is every tier."""
        await _ladder()

        async with signed_in("admin") as admin:
            r = await admin.get(SEGMENTS_PATH, params={
                "tier": tier, "include_customers": "true"})

        assert _tiers(r) == {b: t for b, t in LADDER_TIERS.items() if t in chosen}
        assert [s["tier"] for s in r.json()["segments"]] == chosen


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


# ─── Opt-out ──────────────────────────────────────────────────────────────

class TestOptout:
    """Recording an opt-out must store a phone the segmentation can match.

    Segmentation suppresses a customer when their buyer_id OR their phone is in
    ``marketing_optouts`` (customers.py exclusion: ``o.phone = scored.phone``),
    and the phone it compares against is the canonical 380+9-digit form
    (``length(phone)=12 AND phone LIKE '380%'``). So a phone stored in any other
    shape silently never suppresses by phone — the very case the phone column
    exists for (the same number under a second buyer record).
    """

    def test_normalises_the_phone(self, client, store):
        r = client.post(
            OPTOUT_PATH,
            params={"buyer_id": 1, "phone": "+38 (096) 111-11-11"},
            headers=_admin_headers(),
        )

        assert r.status_code == 200
        assert store.optouts == [
            {"buyer_id": 1, "phone": "380961111111", "reason": "manual",
             "source": str(ADMIN_ID)},
        ], "a phone that isn't canonical can never suppress by phone"

    def test_buyer_only_optout_still_works(self, client, store):
        r = client.post(
            OPTOUT_PATH, params={"buyer_id": 7}, headers=_admin_headers(),
        )

        assert r.status_code == 200
        assert store.optouts == [
            {"buyer_id": 7, "phone": None, "reason": "manual",
             "source": str(ADMIN_ID)},
        ]

    @pytest.mark.parametrize("phone", ["0961111111", "380961111", "123456789012"])
    def test_rejects_a_phone_a_campaign_could_not_contain(self, client, store, phone):
        r = client.post(
            OPTOUT_PATH, params={"buyer_id": 1, "phone": phone},
            headers=_admin_headers(),
        )

        # 400 from the 380-prefix rule, 422 when too short/long for the field.
        assert r.status_code in (400, 422)
        assert store.optouts == [], "garbage must not reach the stoplist"


class TestChannelSelection:
    """Viber is a different message, not a nicer SMS — the route has to say so."""

    def test_channels_endpoint_reports_what_is_configured(self, client, monkeypatch):
        monkeypatch.setenv("TURBOSMS_API_TOKEN", "tok")
        monkeypatch.setenv("TURBOSMS_SENDER", "KoreanStory")
        monkeypatch.delenv("TURBOSMS_VIBER_SENDER", raising=False)

        body = client.get(CHANNELS_PATH, headers=_admin_headers()).json()

        assert body["sms"] is True
        assert body["viber"] is False

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

    def test_one_bad_name_rejects_the_whole_request(self, client, store):
        """Silently dropping it would send to fewer people than were asked for."""
        r = client.get(
            SEGMENTS_PATH, params={"tier": "CORE,GOLD"}, headers=_admin_headers(),
        )

        assert r.status_code == 400
        assert "GOLD" in r.json()["detail"]
        assert store.calls == []

    def test_export_filename_names_the_tiers_it_holds(self, client, store):
        r = client.get(
            CSV_PATH, params={"tier": "CORE,REACTIVATION", "campaign": "aug"},
            headers=_admin_headers(),
        )

        assert "sms_aug_core-reactivation" in r.headers["content-disposition"]
