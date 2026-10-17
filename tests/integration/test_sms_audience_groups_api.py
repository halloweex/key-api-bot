"""Audience groups on the wire: one JSON query parameter, refused strictly.

The first group is the flat parameters, exactly as before groups — so every
link, preset and test written against them still names one group. Further
groups travel as `or_groups`, a JSON array whose keys are those same
parameters' names. Still the query string, because the CSV download is a link
the browser follows and `api_gate` reads `sales_type` from it.

What these hold:

* the preview, the CSV export and the freeze parse one query string into the
  same groups, so the roster frozen is the roster that was on screen;
* anything malformed is a 400 that names the group, never a group silently
  dropped or a key silently ignored;
* a follow-up still refuses every parameter that shapes an audience;
* the JSON form of a group cannot drift from the flat one — same keys, same
  bounds — because a filter added to one and not the other is a condition the
  page shows and the server never applies.
"""
import inspect
import json
from datetime import date

import pytest
from fastapi.testclient import TestClient

from core.repositories.customers import (
    MAX_AUDIENCE_GROUPS, SmsAudienceFilters, SmsAudienceGroup,
)
from tests.integration.test_sms_audience_api import _FakeStore, _headers
from web.main import app
from web.routes.api import customers as api

SEGMENTS = "/api/customers/sms-segments"
EXPORT = "/api/customers/sms-segments/export/csv"
CAMPAIGNS = "/api/customers/sms-campaigns"

OWNER = {
    # Group 1: two or more purchases since 1 January, in a window wide
    # enough to hold the whole year.
    "max_recency_days": 365,
    "period_orders_min": 2,
    "period_from": "2026-01-01",
    # Group 2: anybody whose last order is within 92 days.
    "or_groups": json.dumps([{"max_recency_days": 92}]),
}


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


def _get(client, params, path=SEGMENTS):
    return client.get(path, params=params, headers=_headers())


class TestTheGroupsReachTheStore:
    def test_the_owners_campaign(self, client, store):
        r = _get(client, OWNER)
        assert r.status_code == 200, r.text
        sent = store.calls[-1]
        assert "filters" not in sent and "max_recency_days" not in sent
        assert sent["groups"] == [
            SmsAudienceGroup(365, SmsAudienceFilters(
                period_orders_min=2, period_from=date(2026, 1, 1))),
            SmsAudienceGroup(92, SmsAudienceFilters()),
        ]

    def test_one_group_is_asked_for_as_it_always_was(self, client, store):
        r = _get(client, {"period_days": 92, "max_recency_days": 270})
        assert r.status_code == 200, r.text
        sent = store.calls[-1]
        assert "groups" not in sent
        assert sent["max_recency_days"] == 270
        assert sent["filters"] == SmsAudienceFilters(period_days=92)

    def test_every_filter_travels_inside_a_group(self, client, store):
        group = {
            "max_recency_days": 400, "recency_min": 10, "recency_max": 300,
            "orders_min": 1, "orders_max": 9, "ltv_min": 100, "ltv_max": 99999.5,
            "aov_min": 50, "aov_max": 5000, "first_order_from": "2025-01-01",
            "first_order_to": "2026-06-30", "city": ["Київ", " київ ", "Lviv"],
            "gender": ["F"], "gender_min_confidence": "HIGH",
            "brand": ["Anua", "anua"], "category_id": [10, 10, 11],
            "source_id": [1, 4], "promocode_used": " KS-AUG ",
            "bought_within_days": 180, "period_orders_min": 3, "period_days": 60,
        }
        r = _get(client, {"or_groups": json.dumps([group])})
        assert r.status_code == 200, r.text
        second = store.calls[-1]["groups"][1]
        assert second.max_recency_days == 400
        f = second.filters
        assert (f.recency_min_days, f.recency_max_days) == (10, 300)
        assert (f.orders_min, f.orders_max) == (1, 9)
        assert (f.ltv_min, f.ltv_max, f.aov_min, f.aov_max) == (100, 99999.5, 50, 5000)
        assert f.first_order_from == date(2025, 1, 1)
        assert f.cities == ("Київ", "Lviv")
        assert f.genders == ("f",) and f.gender_min_confidence == "high"
        assert f.brands == ("Anua",)
        assert f.category_ids == (10, 11) and f.source_ids == (1, 4)
        assert f.promocode == "KS-AUG" and f.bought_within_days == 180
        assert (f.period_orders_min, f.period_days, f.period_from) == (3, 60, None)

    def test_an_empty_group_is_its_window(self, client, store):
        """`{}` is "everyone whose last order is inside 270 days" — the
        owner's second group, said with no filter at all."""
        r = _get(client, {"or_groups": "[{}]"})
        assert r.status_code == 200, r.text
        assert store.calls[-1]["groups"][1] == SmsAudienceGroup()

    def test_the_level_rules_follow_the_widest_window(self, client, store):
        """The reactivation window may not outlast the base window, and with
        groups the base window is the widest of theirs — a 60-day first group
        beside a 365-day second must not pin it to 60."""
        wide = json.dumps([{"max_recency_days": 365}])
        r = _get(client, {"max_recency_days": 60, "or_groups": wide})
        assert r.status_code == 200, r.text
        assert store.calls[-1]["reactivation_max_recency"] == 120

        r = _get(client, {"max_recency_days": 60, "reactivation_max_recency": 300,
                          "or_groups": wide})
        assert r.status_code == 200, r.text
        r = _get(client, {"max_recency_days": 60, "reactivation_max_recency": 400,
                          "or_groups": wide})
        assert r.status_code == 400


class TestPreviewExportAndFreezeAgree:
    def test_one_query_string_one_audience(self, client, store):
        params = dict(OWNER, grouping="single", campaign="owner-oct")
        assert _get(client, params).status_code == 200
        assert _get(client, params, EXPORT).status_code == 200
        r = client.post(CAMPAIGNS, params=params, headers=_headers())
        assert r.status_code == 200, r.text
        preview, export, freeze = store.calls[-3:]
        assert preview["groups"] == export["groups"] == freeze["groups"]
        assert len(preview["groups"]) == 2


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
        ('[{"max_recency_days": 900}]', "group 2: max_recency_days"),
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
    ])
    def test_the_flat_period_is_refused_the_same_way(self, client, store, params,
                                                     needle):
        r = _get(client, params)
        assert r.status_code == 400 and needle in r.json()["detail"]


class TestAFollowUpRefusesGroupsToo:
    @pytest.mark.parametrize("params", [
        {"or_groups": "[{}]"},
        {"period_from": "2026-01-01"},
        {"period_days": 30},
        {"period_orders_min": 2},
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
