"""`GET /api/events?event_type=…` filters, or says it cannot.

The garbage sweep removed eight `SyncEvent` members nothing emitted. An
unknown `event_type` used to fall through to the unfiltered history, so a
filter on a removed (or misspelt) type answered with every event: a filter
that filters nothing reads as an answer. It is a 400 naming the field now.
"""
from __future__ import annotations

from tests.unit.test_read_fallback_sites import admin_client  # noqa: F401 — fixture


def test_an_unknown_event_type_is_a_400_naming_the_field(admin_client):  # noqa: F811
    """Mutation: drop the `else: raise` — the answer is the unfiltered 200."""
    r = admin_client.get("/api/events", params={"event_type": "categories.synced"})
    assert r.status_code == 400, r.text
    assert "event_type" in r.json()["detail"]


def test_a_known_event_type_by_value_or_name_still_filters(admin_client):  # noqa: F811
    from core.events import SyncEvent

    member = next(iter(SyncEvent))
    for spelling in (member.value, member.name.lower()):
        r = admin_client.get("/api/events", params={"event_type": spelling})
        assert r.status_code == 200, (spelling, r.text)
        assert set(r.json()) == {"events", "handlers"}


def test_no_filter_is_the_whole_history(admin_client):  # noqa: F811
    r = admin_client.get("/api/events")
    assert r.status_code == 200, r.text
