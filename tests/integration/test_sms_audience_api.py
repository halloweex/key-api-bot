"""API tests for audience filters, saved audiences, and creating a campaign.

Three contracts, and each one has already been got wrong somewhere:

* every filter parameter narrows the audience it names — a parameter FastAPI
  does not declare is dropped without a word, so a control on the screen can
  do nothing and look exactly like one that works;
* a preset is stored form state — it must round-trip verbatim and must not be
  able to shadow a built-in;
* freezing a roster must be reachable without downloading a file of phone
  numbers, must record exactly the audience the same query previews, and must
  refuse the cases where the frozen list would be wrong.

The behaviour runs through the routes as a signed-in visitor over the test
Postgres (`tests/helpers/sms_audience_routes.py`, on the fixture of
`tests/sms_audience_fixture.py`). The refusals made before any store is read
run against a fake store (`tests/helpers/sms_api.py`), which also proves the
store was never reached.
"""
import logging
import urllib.parse

import pytest
import pytest_asyncio
from fastapi.testclient import TestClient

from web.main import app
from tests import sms_audience_fixture as fx
from tests.helpers.app_client import signed_in
from tests.helpers.sms_api import _FakeStore, _headers
from tests.helpers.sms_audience_routes import (
    BASE, CAMPAIGNS, PRESETS, SEGMENTS, arms, audience_on_postgres, buyer_ids,
    frozen_campaign, frozen_members,
)
from tests.integration import pg_sms_seed
from tests.integration.pg_sms_seed import needs_postgres


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


@pytest_asyncio.fixture
async def admin(monkeypatch):
    """A hardcoded admin, the audience fixture in Postgres."""
    async with audience_on_postgres(monkeypatch):
        async with signed_in("admin") as visitor:
            yield visitor


def _preview(visitor, **params):
    return visitor.get(SEGMENTS, params={"include_customers": "true", **params})


# ─── Every filter narrows the audience ────────────────────────────────────

# Each flat parameter as the page sends it, and the buyers it leaves of the
# fixture under one arm and the 270-day window (`BASE` with no filter). A
# filter runs before the rules common to the campaign, so 8 — who shares a
# phone with 9 — is in every audience 9 is not.
FILTER_CASES = [
    ("no filter", lambda: {}, BASE),
    ("brand", lambda: {"brand": "Anua"}, {1, 2, 3, 4, 8, 14}),
    ("brand, bought recently", lambda: {"brand": "Anua", "bought_within_days": 60},
     {2, 3}),
    ("category", lambda: {"category_id": "20"}, {1, 2, 3, 4, 8, 14}),
    ("sources", lambda: {"source_id": "2,4"}, {2, 3}),
    ("city", lambda: {"city": "Львів"}, {2, 4, 6, 8, 12, 14}),
    ("promocode, any case", lambda: {"promocode_used": " aug "}, {1, 14}),
    ("recency range", lambda: {"recency_min": 30, "recency_max": 200},
     {1, 2, 5, 6, 7, 8, 12, 13, 14}),
    ("one order only", lambda: {"orders_max": 1}, {5, 6, 7, 9, 12, 13}),
    ("value", lambda: {"ltv_min": 2000}, {1, 2, 3, 4, 9, 14}),
    ("small baskets", lambda: {"aov_max": 500}, {8}),
    ("first order since", lambda: {"first_order_from": fx.ago(100).isoformat()},
     {9, 12, 13}),
    ("first order before", lambda: {"first_order_to": fx.ago(300).isoformat()},
     {2, 4}),
    ("two filters are ANDed", lambda: {"brand": "Anua", "city": "Львів"},
     {2, 4, 8, 14}),
    # Genders (`sms_audience_routes.GENDERS`): 1, 2 f/certain, 3 f/medium,
    # 4, 9 m/high, 5 m/medium, 6 refused (NULL), the rest no verdict.
    ("gender", lambda: {"gender": "f"}, {1, 2, 3}),
    ("gender and a floor", lambda: {"gender": "m", "gender_min_confidence": "high"},
     {4, 9}),
    ("both genders", lambda: {"gender": "f,m"}, {1, 2, 3, 4, 5, 9}),
    ("case and spacing forgiven",
     lambda: {"gender": " F , M ", "gender_min_confidence": "HIGH"}, {1, 2, 4, 9}),
    ("a floor alone", lambda: {"gender_min_confidence": "certain"}, {1, 2}),
]


@needs_postgres
class TestTheFiltersNarrowTheAudience:
    @pytest.mark.asyncio
    @pytest.mark.parametrize("label,params,expected", FILTER_CASES,
                             ids=[c[0] for c in FILTER_CASES])
    async def test_the_parameter_selects_its_buyers(self, admin, label, params,
                                                    expected):
        r = await _preview(admin, grouping="single", **params())
        assert r.status_code == 200, r.text
        assert buyer_ids(r) == expected

    @pytest.mark.asyncio
    async def test_the_criteria_record_the_filters_as_applied(self, admin):
        """What is frozen with a campaign and read back later to say who was
        messaged: duplicates collapsed, spacing and case forgiven, lists
        split — the filters the query ran, not the text that was typed."""
        r = await admin.get(SEGMENTS, params={
            "grouping": "single", "brand": "Anua, anua , Cosrx",
            "category_id": "20,11,20", "source_id": "1", "city": " Львів ,львів",
            "promocode_used": " aug ", "bought_within_days": 90,
            "recency_min": 30, "recency_max": 200, "orders_min": 2,
            "ltv_min": 1000, "aov_max": 5000, "first_order_from": "2025-01-01",
            "gender": " F ", "gender_min_confidence": "HIGH",
        })
        assert r.status_code == 200, r.text
        assert r.json()["criteria"]["filters"] == {
            "recency_min_days": 30, "recency_max_days": 200, "orders_min": 2,
            "ltv_min": 1000, "aov_max": 5000, "first_order_from": "2025-01-01",
            "cities": ["Львів"], "genders": ["f"], "gender_min_confidence": "high",
            "brands": ["Anua", "Cosrx"], "category_ids": [20, 11],
            "source_ids": [1], "promocode": "aug", "bought_within_days": 90,
        }

    @pytest.mark.asyncio
    async def test_no_filter_records_none(self, admin):
        r = await admin.get(SEGMENTS, params={"grouping": "single"})
        assert r.status_code == 200, r.text
        assert r.json()["criteria"]["filters"] == {}


# ─── The window, and the levels inside it ─────────────────────────────────

@needs_postgres
class TestTheWindowAndTheLevels:
    """The window is the outermost rule; the reactivation level lives inside
    it and is one-order buyers of the last 120 days unless a narrower window
    says otherwise."""

    @pytest.mark.asyncio
    @pytest.mark.parametrize("window,expected", [
        (92, {2, 3, 9, 12}),          # 12 at 92 days is in, 13 at 93 is not
        (270, BASE),
        (3650, BASE | {15}),          # one order 1000 days ago
    ])
    async def test_the_window_bounds_the_last_order(self, admin, window, expected):
        r = await _preview(admin, grouping="single", max_recency_days=window)
        assert r.status_code == 200, r.text
        assert buyer_ids(r) == expected

    @pytest.mark.asyncio
    async def test_the_default_levels_drop_one_order_buyers_past_120_days(self, admin):
        """The defaults are three levels over 270 days: 5, 6 and 7 bought
        once 150 days ago — in the window, in no level — and are dropped;
        12 and 13, once 92 and 93 days ago, are reactivation."""
        r = await _preview(admin)
        assert r.status_code == 200, r.text
        assert {c["buyerId"]: c["tier"] for c in r.json()["customers"]} == {
            1: "CORE", 2: "CORE", 3: "CORE", 4: "CORE", 14: "CORE",
            9: "REACTIVATION", 12: "REACTIVATION", 13: "REACTIVATION",
        }

    @pytest.mark.asyncio
    async def test_a_narrow_window_narrows_reactivation_instead_of_failing(self, admin):
        """A 60-day window used to be a 400: the reactivation window defaulted
        to 120 and was refused for outlasting it, so asking for a recent
        audience produced an error rather than a smaller one."""
        r = await _preview(admin, max_recency_days=60)
        assert r.status_code == 200, r.text
        assert {c["buyerId"]: c["tier"] for c in r.json()["customers"]} == {
            2: "CORE", 3: "CORE", 9: "REACTIVATION",
        }


# ─── Saved audiences ──────────────────────────────────────────────────────

@pytest_asyncio.fixture
async def editor(monkeypatch):
    """An editor holding the SMS tab, the `/sms` tables empty."""
    async with pg_sms_seed.on_postgres(monkeypatch):
        async with signed_in("editor", tabs="sms") as visitor:
            yield visitor


def _preset(name: str) -> str:
    return f"{PRESETS}/{urllib.parse.quote(name, safe='')}"


async def _listed(visitor) -> list:
    r = await visitor.get(PRESETS)
    assert r.status_code == 200, r.text
    return r.json()["presets"]


async def _stored_presets() -> int:
    async with pg_sms_seed.pg_conn() as conn:
        return await conn.fetchval("SELECT count(*) FROM app.sms_audience_presets")


@needs_postgres
class TestSavedAudiences:
    # The form state the page saves: two levels, three where a filter holds
    # a list — well inside the nesting bound refused further down.
    PAGE_STATE = {"grouping": "single",
                  "filters": {"brands": ["Anua"], "cities": ["Kyiv"]},
                  "tierRules": {"coreMinOrders": 2}}

    @pytest.mark.asyncio
    @pytest.mark.parametrize("name", [
        "Anua buyers",
        "VIP клієнти",         # Cyrillic — a Ukrainian team names audiences too
        "Winter-2026 (core)",
        "50% off & more",
        "Anua_buyers",
    ])
    async def test_a_saved_audience_is_listed_as_it_was_saved(self, editor, name):
        r = await editor.put(_preset(name), json=self.PAGE_STATE)
        assert r.status_code == 200, r.text

        saved = [p for p in await _listed(editor) if p["name"] == name]
        assert len(saved) == 1
        assert saved[0]["criteria"] == self.PAGE_STATE
        assert saved[0]["createdBy"] == editor.user_id
        assert saved[0]["builtin"] is False

    @pytest.mark.asyncio
    @pytest.mark.parametrize("method", ["PUT", "DELETE"])
    async def test_a_builtin_can_be_neither_replaced_nor_deleted(self, editor, method):
        """The reference cohort has to keep meaning what past campaigns meant,
        so no stored row may shadow it."""
        before = await _listed(editor)
        builtin = next(p for p in before if p["builtin"])

        r = await editor.request(method, _preset(builtin["name"]),
                                 json={"grouping": "single", "filters": {}})

        assert r.status_code == 409
        assert await _listed(editor) == before
        assert await _stored_presets() == 0

    @pytest.mark.asyncio
    async def test_a_deleted_audience_is_gone(self, editor):
        assert (await editor.put(_preset("temp audience"), json=self.PAGE_STATE)
                ).status_code == 200

        r = await editor.delete(_preset("temp audience"))

        assert r.status_code == 200
        assert "temp audience" not in {p["name"] for p in await _listed(editor)}
        assert await _stored_presets() == 0
        assert (await editor.delete(_preset("temp audience"))).status_code == 404


# ─── Freezing a campaign without a CSV ────────────────────────────────────

@needs_postgres
class TestFreezingAnAudience:
    PARAMS = {"campaign": "sep-brand", "grouping": "single", "holdout_pct": 30}

    @pytest.mark.asyncio
    async def test_the_roster_frozen_is_the_audience_previewed(self, admin):
        preview = await _preview(admin, **self.PARAMS)
        r = await admin.post(CAMPAIGNS, params=self.PARAMS)

        assert r.status_code == 200, r.text
        body = r.json()
        assert body["campaign"] == "sep-brand"
        assert body["frozen"]["frozen"] is True
        members = await frozen_members("sep-brand")
        assert set(members) == BASE
        assert members == arms(preview), "each person in the arm the preview showed"
        holdout = sum(1 for arm in members.values() if arm == "holdout")
        # The fixture and this name give a real split, so the arms compared
        # above are not all one value.
        assert 0 < holdout < len(BASE)
        assert body["totals"] == {"customers": len(BASE),
                                  "target": len(BASE) - holdout, "holdout": holdout}

        listing = await admin.get(CAMPAIGNS)
        entry = {c["campaign"]: c for c in listing.json()["campaigns"]}["sep-brand"]
        assert (entry["members"], entry["target"], entry["holdout"],
                entry["holdoutPct"]) == (len(BASE), len(BASE) - holdout, holdout, 30)

    @pytest.mark.asyncio
    async def test_a_name_already_frozen_is_refused_and_keeps_its_roster(self, admin):
        assert (await admin.post(CAMPAIGNS, params=self.PARAMS)).status_code == 200
        roster = await frozen_members("sep-brand")

        again = await admin.post(CAMPAIGNS, params=dict(self.PARAMS, brand="Anua"))

        assert again.status_code == 409
        assert again.json()["reason"] == "already_frozen"
        assert await frozen_members("sep-brand") == roster

    @pytest.mark.asyncio
    async def test_a_truncated_roster_is_refused_and_nothing_is_frozen(self, admin):
        """A roster cut at `limit` would freeze a control group that is not
        the campaign's. The whole audience fits exactly at its own size."""
        r = await admin.post(CAMPAIGNS, params=dict(self.PARAMS, limit=len(BASE) - 1))

        assert r.status_code == 400
        assert r.json()["reason"] == "roster_truncated"
        assert await frozen_campaign("sep-brand") is None
        assert await frozen_members("sep-brand") == {}

        whole = await admin.post(CAMPAIGNS, params=dict(self.PARAMS, limit=len(BASE)))
        assert whole.status_code == 200, whole.text
        assert set(await frozen_members("sep-brand")) == BASE

    @pytest.mark.asyncio
    async def test_an_audience_the_filters_empty_is_refused(self, admin):
        r = await admin.post(CAMPAIGNS, params=dict(self.PARAMS, orders_min=50))

        assert r.status_code == 400
        assert r.json()["reason"] == "audience_empty"
        assert await frozen_campaign("sep-brand") is None


# ─── Refusals made before the store is read ───────────────────────────────

class TestFilterParameters:
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
    def test_a_non_object_body_is_refused(self, client, store):
        r = client.put(f"{PRESETS}/x", json=["not", "an", "object"], headers=_headers())
        assert r.status_code == 400
        assert store.saved == []

    # ─── Criteria are handed back, so what cannot be rendered is not stored ──
    # Size and shape were the only checks, and nesting is neither. `json.loads`
    # accepts hundreds of levels; the response serializer refuses past 254 —
    # and the row is written before the response is rendered. So one PUT of
    # 1 787 characters, well inside the 8 000-character cap, wrote a preset and
    # then made `GET /sms-audience-presets` 500 for every `sms:view` reader,
    # until somebody deleted it by a name the dead listing no longer showed.
    # At 251 levels the write even answered 200, so nothing warned the author.

    @staticmethod
    def _nested(depth: int) -> dict:
        top: dict = {}
        node = top
        for _ in range(depth):
            node["a"] = {}
            node = node["a"]
        return top

    @pytest.mark.parametrize("depth", [21, 251, 300])
    def test_criteria_nested_past_the_bound_is_refused(self, client, store, depth):
        r = client.put(
            f"{PRESETS}/deep", json=self._nested(depth), headers=_headers(),
        )
        assert r.status_code == 400
        assert store.saved == []

    def test_the_bound_is_under_what_the_listing_can_be_rendered_at(self):
        """The number is not a taste: past it the listing cannot be sent at all.

        Rendered rather than asserted about, so this fails if the serializer's
        own limit ever drops towards the bound instead of drifting away from it.
        The listing envelope is what matters — it wraps `criteria` three levels
        deeper than the PUT response does, which is why a preset that saved
        with a 200 could still kill the listing.
        """
        from fastapi.responses import ORJSONResponse

        from web.routes.api.customers import _PRESET_CRITERIA_MAX_DEPTH

        def listing(depth: int) -> dict:
            return {"presets": [{"name": "x", "criteria": self._nested(depth)}]}

        ORJSONResponse(listing(_PRESET_CRITERIA_MAX_DEPTH))
        with pytest.raises(TypeError):
            ORJSONResponse(listing(300))

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

    # ─── The {name} path parameter is validated ───────────────────────────
    # A preset name is stored verbatim as a primary key, echoed back to the
    # page, and interpolated into the audit log. Until it is validated, a name
    # carrying a newline forges a second log line (`name=evil\nname` splits the
    # record), control characters and markup ride into storage, and DELETE — a
    # bare `.strip()` — accepts what PUT would have refused for length. The two
    # verbs must apply the identical rule.

    @staticmethod
    def _enc(name: str) -> str:
        return urllib.parse.quote(name, safe="")

    def test_put_refuses_a_newline_in_the_name(self, client, store):
        r = client.put(
            f"{PRESETS}/{self._enc('evil\nname')}",
            json={"grouping": "single"}, headers=_headers(),
        )
        assert r.status_code == 400
        assert store.saved == []

    @pytest.mark.parametrize("bad", [
        "a\x01b",      # C0 control
        "a\tb",        # tab
        "a\x7fb",      # DEL
        "a<script>b",  # markup
        'a"b',         # double quote
        "a{b}",        # brace / format metacharacter
    ])
    def test_put_refuses_control_and_markup_characters(self, client, store, bad):
        r = client.put(
            f"{PRESETS}/{self._enc(bad)}",
            json={"grouping": "single"}, headers=_headers(),
        )
        assert r.status_code == 400, bad
        assert store.saved == []

    def test_delete_enforces_the_same_name_rules_as_put(self, client, store):
        # A bad character DELETE must be refused the same way PUT refuses it,
        # and must never reach the store.
        r = client.delete(
            f"{PRESETS}/{self._enc('a<script>b')}", headers=_headers(),
        )
        assert r.status_code == 400
        assert store.deleted == []

    def test_delete_refuses_an_over_long_name(self, client, store):
        r = client.delete(
            f"{PRESETS}/{self._enc('x' * 200)}", headers=_headers(),
        )
        assert r.status_code == 400
        assert store.deleted == []

    def test_delete_logs_who_removed_a_shared_preset(self, client, store, caplog):
        # Presets are a shared resource — any sms:edit user can delete any
        # preset — so removal has to leave an audit trail the way saving does.
        with caplog.at_level(logging.INFO, logger="web.routes.api.customers"):
            r = client.delete(f"{PRESETS}/temp audience", headers=_headers())
        assert r.status_code == 200
        assert any(
            "preset deleted" in rec.getMessage().lower()
            and "temp audience" in rec.getMessage()
            for rec in caplog.records
        )


# ─── Creating a campaign without a CSV ────────────────────────────────────

class TestCreateCampaign:
    def test_the_placeholder_name_is_refused(self, client, store):
        r = client.post(CAMPAIGNS, headers=_headers())
        assert r.status_code == 400
        assert store.freezes == []

    def test_creating_needs_a_session(self, client):
        assert client.post(CAMPAIGNS, params={"campaign": "x"}).status_code == 401


class TestBaseWindow:
    """The outermost rule: it runs before every filter."""

    def test_an_explicit_contradiction_is_still_refused(self, client, store):
        r = client.get(
            SEGMENTS,
            params={"max_recency_days": 60, "reactivation_max_recency": 120},
            headers=_headers(),
        )
        assert r.status_code == 400
        assert "reactivation_max_recency" in r.json()["detail"]

class TestChannelsCarryTheTariff:
    """The page prices a campaign before it is sent."""

    def test_price_per_part_is_published(self, client, store, monkeypatch):
        monkeypatch.setenv("TURBOSMS_PRICE_PER_PART", "1.32")

        body = client.get("/api/customers/sms/channels", headers=_headers()).json()

        # From config, never restated in the frontend: two numbers for one
        # price is how a page promises one figure and the gateway charges
        # another.
        assert body["pricePerPart"] == 1.32


class TestGenderReachesTheStore:
    """The filter must arrive as a filter object, not be silently dropped.

    That is the first contract this file names, and it is named because it has
    been got wrong before: a parameter FastAPI does not declare is discarded
    without a word, so a control on the screen can do nothing at all and look
    exactly like one that works.
    """

    @pytest.mark.parametrize("value", ["x", "female", "1", "unknown", "null"])
    def test_an_unknown_gender_is_refused_with_the_vocabulary(
        self, client, store, value,
    ):
        r = client.get(SEGMENTS, params={"gender": value}, headers=_headers())
        assert r.status_code == 400
        assert "f, m" in r.json()["detail"]

    @pytest.mark.parametrize("value", ["low", "sure", "yes", "0"])
    def test_an_unknown_confidence_is_refused(self, client, store, value):
        r = client.get(
            SEGMENTS, params={"gender_min_confidence": value}, headers=_headers(),
        )
        assert r.status_code == 400
        assert "certain, high, medium" in r.json()["detail"]

    def test_there_is_no_spelling_that_selects_a_refused_row(self, client, store):
        """NULL is not a gender, and offering it as one would be a campaign
        addressing people by a gender nobody could decide."""
        r = client.get(SEGMENTS, params={"gender": "unknown"}, headers=_headers())
        assert r.status_code == 400
