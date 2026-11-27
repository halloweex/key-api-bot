"""Audience groups on the wire: one JSON query parameter, refused strictly.

The first group is the flat parameters, exactly as before groups — so every
link, preset and test written against them still names one group. Further
groups travel as `or_groups`, a JSON array whose keys are those same
parameters' names. Still the query string, because the CSV download is a link
the browser follows and `api_gate` reads `sales_type` from it.

What these hold:

* a query string selects the audience its groups describe, in either form —
  flat for the first group, `or_groups` for the rest — on the fixture of
  `tests/sms_audience_fixture.py` in Postgres, called as a signed-in visitor
  (`tests/helpers/sms_audience_routes.py`);
* the preview, the CSV export and the freeze answer one query string with one
  roster, so the roster frozen is the roster that was on screen;
* the level rules follow the widest group's window, and the reactivation
  level keeps its 120 days however wide the window;
* anything malformed is a 400 that names the group, never a group silently
  dropped or a key silently ignored (against a fake store, which also proves
  the store was never reached);
* a follow-up still refuses every parameter that shapes an audience;
* the JSON form of a group cannot drift from the flat one — same keys, same
  bounds — because a filter added to one and not the other is a condition the
  page shows and the server never applies.
"""
import csv
import inspect
import io
import json

import pytest
import pytest_asyncio
from fastapi.testclient import TestClient

from core.repositories.customers import MAX_AUDIENCE_GROUPS
from tests import sms_audience_fixture as fx
from tests.helpers.app_client import signed_in
from tests.helpers.sms_api import _FakeStore, _headers
from tests.helpers.sms_audience_routes import (
    BASE, CAMPAIGNS, EXPORT, SEGMENTS, arms, audience_on_postgres, buyer_ids,
    frozen_campaign, frozen_members,
)
from tests.integration.pg_sms_seed import needs_postgres
from web.main import app
from web.routes.api import customers as api


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


def _get(client, params, path=SEGMENTS):
    return client.get(path, params=params, headers=_headers())


def _owner() -> dict:
    """The owner's campaign as the page sends it. Group 1, flat: two or more
    purchases since `fx.period_from()`, in a window that holds them. Group 2:
    anybody whose last order is within 92 days. A function, because the date
    is the query's today, not the collection's."""
    return {"max_recency_days": 365, "period_orders_min": 2,
            "period_from": fx.period_from().isoformat(),
            "or_groups": json.dumps([{"max_recency_days": 92}])}


# A first group nobody matches, so the audience is the second group's alone:
# no buyer of the fixture has fifty orders.
NOBODY = {"orders_min": 50}


def _as_params(fields: dict) -> dict:
    """A group's filter fields with dates as the wire writes them."""
    return {k: v.isoformat() if hasattr(v, "isoformat") else v
            for k, v in fields.items()}


def _preview(visitor, **params):
    return visitor.get(SEGMENTS, params={"grouping": "single",
                                         "include_customers": "true", **params})


# The period's shapes from the fixture, and one more the fixture does not
# hold: no order up to a date, over all time — everybody whose first order is
# after it. 2, 4 and 15 bought before it; 10 is opted out, 11 has no usable
# phone, and 9 takes the number 8 shares.
PERIOD_CASES = fx.PERIOD_CASES + [
    ("none up to a date, all time", 3650,
     lambda: dict(period_orders_max=0, period_to=fx.period_from()),
     dict(in_period=lambda d: d <= fx.period_from(), at_most=0),
     {1, 3, 5, 6, 7, 9, 12, 13, 14}),
]


@needs_postgres
class TestAQueryStringSelectsItsGroups:
    @pytest.mark.asyncio
    async def test_the_owners_campaign(self, admin):
        r = await _preview(admin, **_owner())

        assert r.status_code == 200, r.text
        assert buyer_ids(r) == fx.UNION
        assert r.json()["groups"] == [
            {"group": 1, "matched": len(fx.A_MATCHED),
             "recipients": fx.A_RECIPIENTS, "onlyHere": fx.A_ONLY},
            {"group": 2, "matched": len(fx.B_MATCHED),
             "recipients": fx.B_RECIPIENTS, "onlyHere": fx.B_ONLY},
        ]
        assert r.json()["criteria"]["groups"] == [
            {"maxRecencyDays": 365, "filters": {
                "period_orders_min": 2, "period_from": fx.period_from().isoformat()}},
            {"maxRecencyDays": 92, "filters": {}},
        ]

    @pytest.mark.asyncio
    @pytest.mark.parametrize("label,window,fields,terms,answer", PERIOD_CASES,
                             ids=[c[0] for c in PERIOD_CASES])
    async def test_a_period_flat(self, admin, label, window, fields, terms, answer):
        assert fx.recipients(fx.matched(window, **terms)) == answer, \
            f"{label}: the oracle disagrees with the hand"

        r = await _preview(admin, max_recency_days=window, **_as_params(fields()))

        assert r.status_code == 200, r.text
        assert buyer_ids(r) == answer

    @pytest.mark.asyncio
    @pytest.mark.parametrize("label,window,fields,terms,answer", PERIOD_CASES,
                             ids=[c[0] for c in PERIOD_CASES])
    async def test_a_period_inside_or_groups(self, admin, label, window, fields,
                                             terms, answer):
        group = dict(max_recency_days=window, **_as_params(fields()))

        r = await _preview(admin, **NOBODY, or_groups=json.dumps([group]))

        assert r.status_code == 200, r.text
        assert buyer_ids(r) == answer
        first, second = r.json()["groups"]
        assert first["matched"] == 0
        assert second["matched"] == len(fx.matched(window, **terms))

    @pytest.mark.asyncio
    async def test_an_empty_group_is_its_window(self, admin):
        """`{}` is "everyone whose last order is inside 270 days" — the
        owner's second group, said with no filter at all."""
        r = await _preview(admin, **NOBODY, or_groups="[{}]")

        assert r.status_code == 200, r.text
        assert buyer_ids(r) == BASE

    @pytest.mark.asyncio
    async def test_a_group_is_recorded_as_the_filters_it_ran(self, admin):
        """Every filter a group can carry, typed loosely: what is frozen with a
        campaign is each one normalised — duplicates collapsed, case and
        spacing forgiven — and none of them dropped on the way."""
        group = {
            "max_recency_days": 400, "recency_min": 10, "recency_max": 300,
            "orders_min": 1, "orders_max": 9, "ltv_min": 100, "ltv_max": 99999.5,
            "aov_min": 50, "aov_max": 5000, "first_order_from": "2025-01-01",
            "first_order_to": "2026-06-30", "city": ["Київ", " київ ", "Lviv"],
            "gender": ["F"], "gender_min_confidence": "HIGH",
            "brand": ["Anua", "anua"], "category_id": [10, 10, 11],
            "source_id": [1, 4], "promocode_used": " KS-AUG ",
            "bought_within_days": 180, "period_orders_min": 3,
            "period_orders_max": 5, "period_days": 60,
        }

        r = await _preview(admin, or_groups=json.dumps([group]))

        assert r.status_code == 200, r.text
        assert r.json()["criteria"]["groups"][1] == {"maxRecencyDays": 400, "filters": {
            "recency_min_days": 10, "recency_max_days": 300,
            "orders_min": 1, "orders_max": 9,
            "ltv_min": 100, "ltv_max": 99999.5, "aov_min": 50, "aov_max": 5000,
            "first_order_from": "2025-01-01", "first_order_to": "2026-06-30",
            "cities": ["Київ", "Lviv"], "genders": ["f"],
            "gender_min_confidence": "high", "brands": ["Anua"],
            "category_ids": [10, 11], "source_ids": [1, 4], "promocode": "KS-AUG",
            "bought_within_days": 180, "period_orders_min": 3,
            "period_orders_max": 5, "period_days": 60,
        }}


@needs_postgres
class TestPreviewExportAndFreezeAgree:
    @pytest.mark.asyncio
    async def test_one_query_string_one_roster(self, admin):
        """What was on screen is what the file holds and what is frozen —
        each person in the same arm."""
        params = dict(_owner(), grouping="single", campaign="owner-oct",
                      holdout_pct=50)

        preview = await admin.get(SEGMENTS, params=dict(params, include_customers="true"))
        export = await admin.get(EXPORT, params=dict(params, include_holdout="true"))
        frozen = await admin.post(CAMPAIGNS, params=params)

        assert preview.status_code == 200, preview.text
        assert export.status_code == 200, export.text
        assert frozen.status_code == 200, frozen.text
        shown = arms(preview)
        assert set(shown) == fx.UNION
        assert 0 < list(shown.values()).count("holdout") < len(shown), \
            "a real split, so the arms compared are not all one value"
        rows = csv.DictReader(io.StringIO(export.content.decode("utf-8-sig")))
        assert {int(row["buyer_id"]): row["assignment"] for row in rows} == shown
        assert await frozen_members("owner-oct") == shown
        stored = json.loads((await frozen_campaign("owner-oct"))["criteria"])
        assert stored["groups"] == preview.json()["criteria"]["groups"]


@needs_postgres
class TestTheLevelsFollowTheWidestWindow:
    """The reactivation level is one-order buyers of the last 120 days unless
    the window is narrower — and with groups the window is the widest of
    theirs. Under three levels (`tier=REACTIVATION` here) 9, 12 and 13 are
    the fixture's one-order buyers inside 120 days."""

    @pytest.mark.asyncio
    async def test_a_narrow_first_group_does_not_pin_the_level(self, admin):
        """A 60-day first group beside a 365-day second must not pin the
        level to 60: 12 and 13, at 92 and 93 days, stay reactivation."""
        wide = json.dumps([{"max_recency_days": 365}])

        r = await admin.get(SEGMENTS, params={
            "max_recency_days": 60, "or_groups": wide, "tier": "REACTIVATION",
            "include_customers": "true"})

        assert r.status_code == 200, r.text
        assert buyer_ids(r) == {9, 12, 13}
        ok = await admin.get(SEGMENTS, params={
            "max_recency_days": 60, "reactivation_max_recency": 300, "or_groups": wide})
        assert ok.status_code == 200, ok.text
        too_wide = await admin.get(SEGMENTS, params={
            "max_recency_days": 60, "reactivation_max_recency": 400, "or_groups": wide})
        assert too_wide.status_code == 400

    @pytest.mark.asyncio
    async def test_an_all_time_window_keeps_reactivation_at_120_days(self, admin):
        """15 bought once, 1000 days ago: inside the all-time window and in no
        level. A typed reactivation window may go to 730 and no further."""
        r = await admin.get(SEGMENTS, params={
            "max_recency_days": 3650, "tier": "REACTIVATION",
            "include_customers": "true"})

        assert r.status_code == 200, r.text
        assert buyer_ids(r) == {9, 12, 13}
        assert (await admin.get(SEGMENTS, params={
            "max_recency_days": 3650, "reactivation_max_recency": 730})
                ).status_code == 200
        assert (await admin.get(SEGMENTS, params={
            "max_recency_days": 3650, "reactivation_max_recency": 731})
                ).status_code == 422


# ─── Refusals made before the store is read ───────────────────────────────

class TestMalformedIsRefused:
    @pytest.mark.parametrize("raw,needle", [
        ("not json", "or_groups"),
        ('{"max_recency_days": 92}', "or_groups"),            # an object, not a list
        ("[]", "at least one group"),
        ("[1]", "group 2"),
        ('[{"orders_mn": 2}]', "group 2: orders_mn"),           # a typo is not ignored
        ('[{"orders_min": "2"}]', "group 2: orders_min"),       # no quiet coercion
        ('[{"orders_min": true}]', "group 2: orders_min"),
        ('[{"orders_min": 2.5}]', "group 2: orders_min"),
        ('[{"max_recency_days": 10}]', "group 2: max_recency_days"),
        # The cap was 730 until "all time" needed a window; it is 3650 now.
        ('[{"max_recency_days": 3651}]', "group 2: max_recency_days"),
        ('[{"period_orders_max": -1, "period_days": 30}]', "group 2: period_orders_max"),
        ('[{"period_orders_max": 1001, "period_days": 30}]', "group 2: period_orders_max"),
        ('[{"period_to": "end of march"}]', "group 2: period_to"),
        ('[{"period_from": "yesterday"}]', "group 2: period_from"),
        ('[{"brand": "Anua"}]', "group 2: brand"),             # lists are lists
        ('[{}, {"city": ["' + "x" * 201 + '"]}]', "group 3: city"),
    ])
    def test_a_bad_group_is_a_400_that_names_it(self, client, store, raw, needle):
        r = _get(client, {"or_groups": raw})
        assert r.status_code == 400, r.text
        assert needle in r.json()["detail"]
        assert store.calls == []

    @pytest.mark.parametrize("group,needle", [
        ({"orders_min": 5, "orders_max": 2}, "group 2: orders_min must not exceed"),
        ({"gender": ["x"]}, "group 2: gender must be one of"),
        ({"gender_min_confidence": "sure"}, "group 2: gender_min_confidence"),
        ({"period_days": 30, "period_from": "2026-01-01"}, "group 2: period_from and"),
        ({"period_orders_min": 2}, "group 2: period_orders_min needs a period"),
        ({"period_days": 30, "period_to": "2026-01-01"},
         "group 2: period_to and period_days"),
        ({"period_from": "2026-03-01", "period_to": "2026-01-01"},
         "group 2: period_from must not exceed period_to"),
        ({"period_orders_max": 0}, "group 2: period_orders_max needs a period"),
        ({"period_orders_min": 3, "period_orders_max": 2, "period_days": 30},
         "group 2: period_orders_min must not exceed period_orders_max"),
    ])
    def test_the_rules_of_a_group_hold_inside_or_groups(self, client, store, group,
                                                        needle):
        r = _get(client, {"or_groups": json.dumps([group])})
        assert r.status_code == 400, r.text
        assert needle in r.json()["detail"]

    def test_too_many_groups(self, client, store):
        raw = json.dumps([{}] * MAX_AUDIENCE_GROUPS)        # plus the flat one
        r = _get(client, {"or_groups": raw})
        assert r.status_code == 400
        assert f"at most {MAX_AUDIENCE_GROUPS}" in r.json()["detail"]
        assert _get(client, {"or_groups": json.dumps([{}] * (MAX_AUDIENCE_GROUPS - 1))}
                    ).status_code == 200

    def test_a_huge_parameter_is_refused_before_parsing(self, client, store):
        r = _get(client, {"or_groups": "[" + " " * 9000 + "{}]"})
        assert r.status_code == 400
        assert "too long" in r.json()["detail"]

    def test_the_first_group_says_its_number_only_when_there_are_others(
        self, client, store,
    ):
        """Alone, it refuses in exactly the words it always did; beside other
        groups the words say which one."""
        r = _get(client, {"orders_min": 5, "orders_max": 2})
        assert r.json()["detail"] == "orders_min must not exceed orders_max"
        r = _get(client, {"orders_min": 5, "orders_max": 2, "or_groups": "[{}]"})
        assert r.json()["detail"] == "group 1: orders_min must not exceed orders_max"

    @pytest.mark.parametrize("params,needle", [
        ({"period_orders_min": 2}, "period_orders_min needs a period"),
        ({"period_days": 30, "period_from": "2026-01-01"}, "period_from and"),
        ({"period_days": 30, "period_to": "2026-01-01"}, "period_to and period_days"),
        ({"period_from": "2026-03-01", "period_to": "2026-01-01"},
         "period_from must not exceed period_to"),
        ({"period_orders_max": 0}, "period_orders_max needs a period"),
        ({"period_orders_min": 3, "period_orders_max": 2, "period_days": 30},
         "period_orders_min must not exceed period_orders_max"),
    ])
    def test_the_flat_period_is_refused_the_same_way(self, client, store, params,
                                                     needle):
        r = _get(client, params)
        assert r.status_code == 400 and needle in r.json()["detail"]
        assert store.calls == []

    @pytest.mark.parametrize("params", [
        {"period_orders_max": -1, "period_days": 30},
        {"period_orders_max": 1001, "period_days": 30},
        {"max_recency_days": 3651},
    ])
    def test_a_flat_bound_is_the_frameworks_422(self, client, store, params):
        """A flat parameter outside its declared bounds is refused by FastAPI
        before the handler runs — a 422, as every flat bound here always was
        (`max_recency_days=5000` in `test_sms_segments_api.py`). The same value
        inside `or_groups` is the 400 above, because that JSON is parsed by
        the handler."""
        r = _get(client, params)
        assert r.status_code == 422, r.text
        assert store.calls == []


class TestAFollowUpRefusesGroupsToo:
    @pytest.mark.parametrize("params", [
        {"or_groups": "[{}]"},
        {"period_from": "2026-01-01"},
        {"period_to": "2026-01-01"},
        {"period_days": 30},
        {"period_orders_min": 2},
        {"period_orders_max": 0},
    ])
    def test_every_new_audience_parameter_is_audience_shaping(self, client, store,
                                                              params):
        r = _get(client, dict(params, follow_up_of="oct-main", campaign="oct-push"))
        assert r.status_code == 400
        assert next(iter(params)) in r.json()["detail"]


class TestTheTwoFormsCannotDrift:
    """`or_groups` carries a group's parameters under the flat names. A filter
    added to the flat parameters and forgotten here would apply to group 1
    and be refused in every other group."""

    COMMON = {
        "request", "ltv_basis", "vip_ltv", "core_ltv", "core_min_orders",
        "reactivation_max_recency", "sales_type", "holdout_pct", "campaign",
        "tier", "grouping", "or_groups", "follow_up_of",
    }

    @staticmethod
    def _flat():
        return inspect.signature(api._sms_segment_params).parameters

    def test_every_per_group_parameter_has_a_json_twin(self):
        per_group = set(self._flat()) - self.COMMON
        assert per_group == set(api._AudienceGroupParams.model_fields)

    def test_the_numeric_bounds_are_the_same(self):
        def bounds(metadata):
            out = set()
            for m in metadata:
                for attr in ("ge", "le"):
                    if hasattr(m, attr) and type(m).__name__.lower() == attr:
                        out.add((attr, getattr(m, attr)))
            return out

        checked = 0
        for name, field in api._AudienceGroupParams.model_fields.items():
            flat = self._flat()[name].default
            assert bounds(field.metadata) == bounds(flat.metadata), name
            checked += bool(bounds(field.metadata))
        assert checked >= 10, "the comparison found no bounds to compare"

    def test_every_per_group_parameter_is_audience_shaping(self):
        per_group = set(api._AudienceGroupParams.model_fields) | {"or_groups"}
        assert per_group <= api._AUDIENCE_SHAPING_PARAMS
