"""The promotions calendar without a database (revision 0039): what a save
accepts, what the API answers, and who writes the table.

Pinned here, each named by the mutation it kills:

- `parse` holds every field to the table's bounds and names the field it
  refuses, in English, for the API's 400;
- the JSON is the contract's, key for key, `recorded_by` never in it;
- the writer takes the lock before it reads, writes the next version, writes
  nothing for a save that changes nothing, and refuses what the status
  refuses (an in-memory connection stands in for Postgres);
- the routes: 201, 400, 404, 409, a refused read as the 503 naming
  `promotions`, and the `marketing` tab — view to read, edit to save;
- the table has one writer, reached from the four saves alone, and they from
  the routes alone.

The server's half — the CHECKs, concurrent saves, `as_of`, the API over a
real database — is `tests/integration/test_promotions_pg.py`.
"""
from __future__ import annotations

import time as _time
from dataclasses import replace
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal
from typing import Any, Dict, List, Optional
from unittest.mock import AsyncMock

import pytest

from core import promotions
from core.exceptions import ValidationError
from tests.unit.test_read_fallback_sites import (  # noqa: F401 — fixtures
    admin_client,
    fresh_read_fallback,
)

UTC = timezone.utc
TABLE = "app.promotion_versions"


def _payload(**over) -> Dict[str, Any]:
    body = {
        "name": "Весняний розпродаж", "starts_on": "2027-03-05", "ends_on": "2027-03-08",
        "start_time": "18:00", "kind": "other", "discount_pct": 20,
        "scope": "brands", "scope_items": ["COSRX"], "channels": ["sms", "instagram"],
        "sms_campaign": None, "note": None, "source": "form",
    }
    body.update(over)
    return body


def _refused(payload) -> ValidationError:
    with pytest.raises(ValidationError) as caught:
        promotions.parse(payload)
    return caught.value


# ─── What a save accepts ─────────────────────────────────────────────────────

class TestParse:
    def test_the_contracts_example_is_accepted_as_it_is(self):
        data = promotions.parse(_payload())
        assert data == promotions.PromotionIn(
            name="Весняний розпродаж", starts_on=date(2027, 3, 5), ends_on=date(2027, 3, 8),
            kind="other", scope="brands", start_time=time(18, 0),
            discount_pct=Decimal("20.00"), scope_items=("COSRX",),
            channels=("sms", "instagram"), sms_campaign=None, note=None, source="form")

    def test_the_optional_fields_may_be_left_out(self):
        data = promotions.parse({"name": "X", "starts_on": "2027-03-05",
                                 "ends_on": "2027-03-05", "kind": "black_friday",
                                 "scope": "store"})
        assert (data.start_time, data.discount_pct, data.scope_items, data.channels,
                data.sms_campaign, data.note, data.source) == (
            None, None, (), (), None, None, "form")

    def test_text_is_stripped_and_empty_text_is_none(self):
        data = promotions.parse(_payload(name="  Акція  ", note="   ", sms_campaign=""))
        assert (data.name, data.note, data.sms_campaign) == ("Акція", None, None)

    def test_channels_come_back_in_one_order_and_once(self):
        """Kills: two saves of the same channels in another order read as a
        change, and a version is written for nothing."""
        data = promotions.parse(_payload(channels=["site", "sms", "site", "telegram"]))
        assert data.channels == ("sms", "telegram", "site")

    def test_items_keep_their_order_and_lose_their_repeats(self):
        data = promotions.parse(_payload(scope_items=["COSRX", " Anua ", "COSRX"]))
        assert data.scope_items == ("COSRX", "Anua")

    def test_a_discount_is_kept_to_two_places(self):
        assert promotions.parse(_payload(discount_pct=12.345)).discount_pct == Decimal("12.35")
        assert promotions.parse(_payload(discount_pct=90)).discount_pct == Decimal("90.00")
        assert promotions.parse(_payload(discount_pct=0)).discount_pct == Decimal("0.00")

    def test_a_start_time_is_minutes(self):
        assert promotions.parse(_payload(start_time="09:30:45")).start_time == time(9, 30)
        assert promotions.parse(_payload(start_time="")).start_time is None

    def test_the_longest_promotion_is_62_days_both_ends_included(self):
        """Kills: an off-by-one between the API and the table's CHECK
        (`ends_on - starts_on < 62`) — a 500 for a promotion the page let
        through."""
        start = date(2027, 1, 1)
        ok = promotions.parse(_payload(starts_on=start.isoformat(),
                                       ends_on=(start + timedelta(days=61)).isoformat()))
        assert (ok.ends_on - ok.starts_on).days == 61
        err = _refused(_payload(starts_on=start.isoformat(),
                                ends_on=(start + timedelta(days=62)).isoformat()))
        assert (err.field, str(err)) == ("ends_on", "ends_on: A promotion lasts at most 62 days")

    @pytest.mark.parametrize("over,field,reason", [
        ({"name": None}, "name", "Is required"),
        ({"name": "   "}, "name", "Must not be empty"),
        ({"name": 7}, "name", "Must be text"),
        ({"name": "x" * 121}, "name", "Must be at most 120 characters"),
        ({"name": "a\x00b"}, "name", "Must not contain a NUL character"),
        ({"starts_on": None}, "starts_on", "Is required"),
        ({"starts_on": "2027-3-5"}, "starts_on", "Must be a date (YYYY-MM-DD)"),
        ({"starts_on": "20270305"}, "starts_on", "Must be a date (YYYY-MM-DD)"),
        ({"starts_on": "2027-02-30"}, "starts_on", "Not a valid date (YYYY-MM-DD)"),
        ({"starts_on": "1999-12-31"}, "starts_on",
         "Date must be between 2000-01-01 and 2099-12-31"),
        ({"ends_on": "2027-03-04"}, "ends_on", "Must not be before starts_on"),
        ({"start_time": "25:00"}, "start_time", "Must be a time (HH:MM)"),
        ({"start_time": "6pm"}, "start_time", "Must be a time (HH:MM)"),
        ({"kind": "flash"}, "kind", "Must be one of black_friday, other"),
        ({"kind": None}, "kind", "Must be one of black_friday, other"),
        ({"discount_pct": "20"}, "discount_pct", "Must be a number"),
        ({"discount_pct": True}, "discount_pct", "Must be a number"),
        ({"discount_pct": float("nan")}, "discount_pct", "Must be a number"),
        ({"discount_pct": 90.01}, "discount_pct", "Must be between 0 and 90"),
        ({"discount_pct": 90.005}, "discount_pct", "Must be between 0 and 90"),
        ({"discount_pct": -1}, "discount_pct", "Must be between 0 and 90"),
        # More digits than the decimal context holds: refused, not a 500.
        ({"discount_pct": 1e308}, "discount_pct", "Must be between 0 and 90"),
        ({"discount_pct": 10 ** 40}, "discount_pct", "Must be between 0 and 90"),
        ({"scope": "everything"}, "scope", "Must be one of store, brands, categories"),
        ({"scope": "store"}, "scope_items", "Must be empty when scope is store"),
        ({"scope_items": []}, "scope_items",
         "Must name at least one item when scope is brands"),
        ({"scope_items": "COSRX"}, "scope_items", "Must be a list"),
        ({"scope_items": [1]}, "scope_items", "Must be a list of text"),
        ({"scope_items": [" "]}, "scope_items", "Must not hold an empty item"),
        ({"scope_items": [f"b{i}" for i in range(51)]}, "scope_items",
         "Must hold at most 50 items"),
        ({"scope_items": ["x" * 81]}, "scope_items",
         "Each item must be at most 80 characters"),
        ({"channels": ["fax"]}, "channels", "Must be among sms, instagram, telegram, site"),
        ({"sms_campaign": "x" * 201}, "sms_campaign", "Must be at most 200 characters"),
        ({"note": "x" * 501}, "note", "Must be at most 500 characters"),
        ({"source": "import"}, "source", "Must be one of form, form_from_sms"),
    ])
    def test_a_bad_field_is_named_with_its_reason(self, over, field, reason):
        """The API's 400 is `<field>: <reason>`. Kills: a field let through
        to the table's CHECK (a 500), or refused under another name."""
        err = _refused(_payload(**over))
        assert (err.field, err.message) == (field, reason)

    @pytest.mark.parametrize("body", [None, [], "x", 5])
    def test_a_body_that_is_not_an_object_is_refused(self, body):
        assert _refused(body).field == "body"

    def test_keys_it_does_not_know_are_ignored(self):
        """The page may send a promotion back whole, `id` and `status` in it."""
        data = promotions.parse(_payload(id=12, status="cancelled", version=3))
        assert data == promotions.parse(_payload())


class TestSameContent:
    def test_the_source_is_not_the_content(self):
        a = promotions.parse(_payload())
        assert a.same_content(replace(a, source="form_from_sms"))

    @pytest.mark.parametrize("field,value", [
        ("name", "Інша"), ("ends_on", date(2027, 3, 9)), ("start_time", None),
        ("discount_pct", Decimal("25")), ("scope_items", ("COSRX", "Anua")),
        ("channels", ("sms",)), ("note", "у всіх магазинах"),
    ])
    def test_any_other_field_is(self, field, value):
        a = promotions.parse(_payload())
        assert not a.same_content(replace(a, **{field: value}))


# ─── What the API answers ────────────────────────────────────────────────────

T1 = datetime(2026, 11, 19, 8, 12, 3, 512, tzinfo=UTC)      # 10:12:03 Kyiv
T2 = datetime(2026, 11, 20, 7, 1, 44, tzinfo=UTC)


def _promotion(version=1, status="planned", recorded_at=T1, recorded_by=555,
               **over) -> promotions.Promotion:
    return promotions.Promotion(
        id=12, version=version, status=status, recorded_at=recorded_at,
        recorded_by=recorded_by, created_at=T1,
        data=promotions.parse(_payload(**over)))


class TestTheJson:
    def test_a_promotion_is_the_contracts_key_for_key(self):
        """Kills: a key renamed, added or left out — the page is built to
        this shape in parallel and reads it blind."""
        out = promotions.to_json(_promotion(version=3, recorded_at=T2))
        assert out == {
            "id": 12, "version": 3, "name": "Весняний розпродаж",
            "starts_on": "2027-03-05", "ends_on": "2027-03-08", "start_time": "18:00",
            "kind": "other", "discount_pct": 20, "scope": "brands",
            "scope_items": ["COSRX"], "channels": ["sms", "instagram"],
            "sms_campaign": None, "note": None, "status": "planned",
            "created_at": "2026-11-19T10:12:03+02:00",
            "updated_at": "2026-11-20T09:01:44+02:00",
            "source": "form",
        }
        assert list(out) == ["id", "version", "name", "starts_on", "ends_on", "start_time",
                             "kind", "discount_pct", "scope", "scope_items", "channels",
                             "sms_campaign", "note", "status", "created_at",
                             "updated_at", "source"]

    def test_a_percent_is_a_number_as_typed(self):
        assert promotions.to_json(_promotion(discount_pct=12.5))["discount_pct"] == 12.5
        assert type(promotions.to_json(_promotion(discount_pct=20))["discount_pct"]) is int
        assert promotions.to_json(_promotion(discount_pct=None))["discount_pct"] is None

    def test_who_saved_it_is_never_served(self):
        """`recorded_by` is a person's id: the history says only whether it
        was the caller. Kills: the id leaking into either shape."""
        promotion = _promotion(recorded_by=555)
        assert "recorded_by" not in promotions.to_json(promotion)
        mine = promotions.version_json(promotion, 555)
        assert mine["recorded_by_me"] is True and "recorded_by" not in mine
        assert mine["recorded_at"] == mine["updated_at"]
        assert promotions.version_json(promotion, 556)["recorded_by_me"] is False
        assert promotions.version_json(promotion, None)["recorded_by_me"] is False
        assert promotions.version_json(_promotion(recorded_by=None), 555)["recorded_by_me"] is False

    def test_the_default_window_is_90_days_back_and_a_year_ahead(self):
        assert promotions.default_window(date(2026, 11, 19)) == (
            date(2026, 8, 21), date(2027, 11, 19))


# ─── The writer, on a connection held in memory ──────────────────────────────

class _Memory:
    """The table, the sequence and the lock, as the writer's statements use
    them — matched by the module's own constants, so a statement renamed
    fails here rather than passing on a guess."""

    def __init__(self):
        self.rows: List[Dict[str, Any]] = []
        self.next_id = 0
        self.log: List[str] = []
        self.clock = datetime(2026, 11, 19, 8, 0, tzinfo=UTC)
        self.acquired = 0

    def tick(self) -> datetime:
        self.clock += timedelta(seconds=1)
        return self.clock

    def latest(self, pid):
        mine = [r for r in self.rows if r["promotion_id"] == pid]
        if not mine:
            return None
        row = max(mine, key=lambda r: r["version"])
        first = min(mine, key=lambda r: r["version"])
        return {**row, "created_at": first["recorded_at"]}

    def acquire(self):
        memory = self

        class _Ctx:
            async def __aenter__(self):
                memory.acquired += 1
                return _Conn(memory)

            async def __aexit__(self, *exc):
                return False

        return _Ctx()


class _Conn:
    def __init__(self, memory: _Memory):
        self.m = memory

    def transaction(self):
        memory = self.m

        class _Tx:
            async def __aenter__(self):
                memory.log.append("BEGIN")

            async def __aexit__(self, exc_type, *exc):
                memory.log.append("ROLLBACK" if exc_type else "COMMIT")
                return False

        return _Tx()

    async def execute(self, sql, *args):
        assert sql == promotions._LOCK_SQL and args == (promotions.PROMOTIONS_LOCK_KEY,)
        self.m.log.append("LOCK")

    async def fetchval(self, sql, *args):
        if sql == promotions._NEXT_ID_SQL:
            self.m.log.append("NEXTVAL")
            self.m.next_id += 1
            return self.m.next_id
        assert sql == promotions._INSERT_SQL, sql
        names = ("promotion_id", "version", "previous", "recorded_by", "source", "status",
                 "name", "starts_on", "ends_on", "start_time", "kind", "discount_pct",
                 "scope", "scope_items", "channels", "sms_campaign", "note")
        row = dict(zip(names, args))
        previous = row.pop("previous")
        row["recorded_at"] = max(self.m.tick(), previous) if previous else self.m.tick()
        self.m.log.append(f"INSERT v{row['version']}")
        self.m.rows.append(row)
        return row["recorded_at"]

    async def fetchrow(self, sql, *args):
        assert sql == promotions._LATEST_SQL, sql
        self.m.log.append("LATEST")
        return self.m.latest(args[0])


@pytest.fixture
def memory(monkeypatch):
    m = _Memory()

    async def pool():
        return m

    monkeypatch.setattr(promotions, "_pool", pool)
    return m


class TestTheWriter:
    @pytest.mark.asyncio
    async def test_a_new_promotion_takes_an_id_and_writes_version_one(self, memory):
        data = promotions.parse(_payload())
        created = await promotions.create(data, by=555)
        assert (created.id, created.version, created.status, created.recorded_by) == (
            1, 1, "planned", 555)
        assert created.created_at == created.recorded_at
        assert memory.log == ["BEGIN", "LOCK", "NEXTVAL", "INSERT v1", "COMMIT"]

    @pytest.mark.asyncio
    async def test_an_edit_is_the_next_version_read_under_the_lock(self, memory):
        """Kills: the latest version read before the lock — two saves would
        both write version N+1."""
        first = await promotions.create(promotions.parse(_payload()), by=555)
        memory.log.clear()
        edited = await promotions.update(
            first.id, promotions.parse(_payload(discount_pct=25)), by=777)
        assert memory.log == ["BEGIN", "LOCK", "LATEST", "INSERT v2", "COMMIT"]
        assert (edited.version, edited.recorded_by, edited.data.discount_pct) == (
            2, 777, Decimal("25.00"))
        assert edited.created_at == first.recorded_at
        assert edited.recorded_at > first.recorded_at

    @pytest.mark.asyncio
    async def test_a_save_that_changes_nothing_writes_nothing(self, memory):
        """A version is a change, not an observation. Kills: a version for
        every press of Save — a history of "no change" entries."""
        first = await promotions.create(promotions.parse(_payload()), by=555)
        again = await promotions.update(
            first.id, promotions.parse(_payload(source="form_from_sms")), by=777)
        assert again == first
        assert len(memory.rows) == 1

    @pytest.mark.asyncio
    async def test_cancel_and_restore_are_versions_with_the_content_kept(self, memory):
        first = await promotions.create(promotions.parse(_payload()), by=555)
        cancelled = await promotions.cancel(first.id, by=777)
        restored = await promotions.restore(first.id, by=555)
        assert [(r["version"], r["status"]) for r in memory.rows] == [
            (1, "planned"), (2, "cancelled"), (3, "planned")]
        assert cancelled.data == first.data == restored.data
        assert (cancelled.status, restored.status) == ("cancelled", "planned")

    @pytest.mark.asyncio
    async def test_what_the_status_refuses_writes_nothing(self, memory):
        """Kills: an edit of a cancelled promotion that quietly restored it,
        a second cancel, or a restore of a planned one — each a version
        recording what nobody did."""
        first = await promotions.create(promotions.parse(_payload()), by=555)
        with pytest.raises(promotions.PromotionConflict, match="is not cancelled"):
            await promotions.restore(first.id, by=555)
        await promotions.cancel(first.id, by=555)
        with pytest.raises(promotions.PromotionConflict, match="already cancelled"):
            await promotions.cancel(first.id, by=555)
        with pytest.raises(promotions.PromotionConflict, match="is cancelled"):
            await promotions.update(first.id, promotions.parse(_payload(name="Y")), by=555)
        assert len(memory.rows) == 2
        assert memory.log[-1] == "ROLLBACK"

    @pytest.mark.asyncio
    @pytest.mark.parametrize("save", ["update", "cancel", "restore"])
    async def test_an_unknown_id_is_not_found(self, memory, save):
        args = (99, promotions.parse(_payload())) if save == "update" else (99,)
        with pytest.raises(promotions.PromotionNotFound):
            await getattr(promotions, save)(*args, by=1)
        assert memory.rows == []

    @pytest.mark.asyncio
    async def test_a_version_is_never_older_than_the_one_before(self, memory):
        """`recorded_at` is GREATEST(the clock, the previous version's), so
        "as of T" and "the highest version" agree though a clock step back."""
        first = await promotions.create(promotions.parse(_payload()), by=1)
        memory.clock = first.recorded_at - timedelta(hours=1)
        second = await promotions.update(first.id, promotions.parse(_payload(name="Y")), by=1)
        assert second.recorded_at == first.recorded_at

    def test_one_lock_key_for_every_promotion(self):
        assert 0 < promotions.PROMOTIONS_LOCK_KEY < 2 ** 63
        assert "pg_advisory_xact_lock" in promotions._LOCK_SQL


# ─── The routes ──────────────────────────────────────────────────────────────

@pytest.fixture
def saves(monkeypatch):
    """The four saves and two reads replaced by recorders over one promotion."""
    calls: List[tuple] = []
    state = {"promotion": _promotion(), "fail": None}

    def record(name):
        async def fake(*args, **kwargs):
            calls.append((name, args, kwargs))
            if state["fail"] is not None:
                raise state["fail"]
            if name in ("current", "versions"):
                return [state["promotion"]]
            return state["promotion"]
        return fake

    for name in ("create", "update", "cancel", "restore", "current", "versions"):
        monkeypatch.setattr(promotions, name, record(name))
    state["calls"] = calls
    return state


class TestTheRoutes:
    def test_a_new_promotion_is_201_and_the_promotion(self, admin_client, saves):
        response = admin_client.post("/api/promotions", json=_payload())
        assert response.status_code == 201, response.text
        assert response.json() == promotions.to_json(saves["promotion"])
        (name, args, kwargs), = saves["calls"]
        assert name == "create" and args == (promotions.parse(_payload()),)
        assert isinstance(kwargs["by"], int)

    @pytest.mark.parametrize("body,detail", [
        (_payload(name=""), "name: Must not be empty"),
        (_payload(discount_pct=95), "discount_pct: Must be between 0 and 90"),
        ([1, 2], "body: Must be a JSON object"),
    ])
    def test_a_bad_body_is_400_naming_the_field(self, admin_client, saves, body, detail):
        response = admin_client.post("/api/promotions", json=body)
        assert response.status_code == 400
        assert response.json() == {"detail": detail}
        assert saves["calls"] == []

    def test_a_body_that_is_not_json_is_400(self, admin_client, saves):
        response = admin_client.post("/api/promotions", content=b"{name",
                                     headers={"content-type": "application/json"})
        assert response.status_code == 400
        assert response.json() == {"detail": "body: Must be a JSON object"}

    @pytest.mark.parametrize("method,path,save", [
        ("PUT", "/api/promotions/12", "update"),
        ("POST", "/api/promotions/12/cancel", "cancel"),
        ("POST", "/api/promotions/12/restore", "restore"),
    ])
    def test_the_saves_answer_the_promotion(self, admin_client, saves, method, path, save):
        response = admin_client.request(method, path, json=_payload())
        assert response.status_code == 200, response.text
        assert response.json()["id"] == 12
        assert saves["calls"][0][0] == save and saves["calls"][0][1][0] == 12

    @pytest.mark.parametrize("method,path", [
        ("PUT", "/api/promotions/99"), ("POST", "/api/promotions/99/cancel"),
        ("POST", "/api/promotions/99/restore"),
    ])
    def test_an_unknown_id_is_404(self, admin_client, saves, method, path):
        saves["fail"] = promotions.PromotionNotFound(99)
        response = admin_client.request(method, path, json=_payload())
        assert response.status_code == 404
        assert response.json() == {"detail": "No promotion 99"}

    @pytest.mark.parametrize("method,path,message", [
        ("PUT", "/api/promotions/12", "is cancelled: restore it first"),
        ("POST", "/api/promotions/12/cancel", "is already cancelled"),
        ("POST", "/api/promotions/12/restore", "is not cancelled"),
    ])
    def test_what_the_status_refuses_is_409(self, admin_client, saves, method, path, message):
        saves["fail"] = promotions.PromotionConflict(12, message)
        response = admin_client.request(method, path, json=_payload())
        assert response.status_code == 409
        assert response.json() == {"detail": f"Promotion 12 {message}"}

    def test_the_list_takes_its_window_and_the_cancelled_flag(self, admin_client, saves):
        response = admin_client.get("/api/promotions", params={
            "from": "2027-01-01", "to": "2027-06-30", "include_cancelled": "true"})
        assert response.status_code == 200
        assert response.json() == {"promotions": [promotions.to_json(saves["promotion"])]}
        (name, args, kwargs), = saves["calls"]
        assert (name, args, kwargs) == (
            "current", (date(2027, 1, 1), date(2027, 6, 30)), {"include_cancelled": True})

    def test_the_list_without_a_window_is_the_default_one(self, admin_client, saves):
        admin_client.get("/api/promotions")
        (_name, args, kwargs), = saves["calls"]
        assert args == promotions.default_window() and kwargs == {"include_cancelled": False}

    @pytest.mark.parametrize("params,detail", [
        ({"from": "2027-13-01"}, "from: Not a valid date (YYYY-MM-DD)"),
        ({"to": "tomorrow"}, "to: Must be a date (YYYY-MM-DD)"),
        ({"from": "2027-02-01", "to": "2027-01-01"}, "to: Must not be before from"),
        ({"include_cancelled": "maybe"}, "include_cancelled: Must be true or false"),
    ])
    def test_a_bad_query_is_400(self, admin_client, saves, params, detail):
        response = admin_client.get("/api/promotions", params=params)
        assert response.status_code == 400
        assert response.json() == {"detail": detail}

    def test_the_history_is_newest_first_and_says_who_was_me(self, admin_client, saves):
        from core.permissions import ADMIN_USER_IDS

        saves["promotion"] = _promotion(recorded_by=sorted(ADMIN_USER_IDS)[0])
        response = admin_client.get("/api/promotions/12/versions")
        assert response.status_code == 200
        (entry,) = response.json()["versions"]
        assert entry["recorded_by_me"] is True and "recorded_by" not in entry

    def test_the_history_of_an_unknown_id_is_404(self, admin_client, saves, monkeypatch):
        monkeypatch.setattr(promotions, "versions", AsyncMock(return_value=[]))
        response = admin_client.get("/api/promotions/99/versions")
        assert response.status_code == 404

    @pytest.mark.parametrize("path", ["/api/promotions", "/api/promotions/12/versions"])
    def test_a_read_whose_postgres_fails_is_refused(
            self, admin_client, saves, fresh_read_fallback, path):  # noqa: F811
        """The 503 naming `promotions`, the cause in the log only. Kills: an
        uncounted 500, or a page told the calendar is empty."""
        saves["fail"] = OSError("connection refused")
        response = admin_client.get(path)
        assert response.status_code == 503
        assert response.json()["surface"] == "promotions"
        assert "connection refused" not in response.text
        assert fresh_read_fallback.refusals()["promotions"]["count"] == 1


# ─── Who may ─────────────────────────────────────────────────────────────────

PERSON = 555_000_777


@pytest.fixture
def as_person(monkeypatch, saves):
    """A client signed in as somebody who is not a hardcoded admin, with the
    role and the tabs a test gives them, over the code's own matrix."""
    from fastapi.testclient import TestClient

    from core import permissions
    from web.main import app
    from web.ratelimit import limiter
    from web.routes.auth import SESSION_COOKIE, create_session_data, session_serializer

    assert PERSON not in permissions.ADMIN_USER_IDS
    who: Dict[str, Any] = {}

    async def resolve(session):
        return {"user_id": PERSON, **who}

    async def matrix(role):
        return permissions.get_permissions_for_role(role)

    monkeypatch.setattr("web.routes.auth._resolve_session", resolve)
    monkeypatch.setattr(permissions, "get_permissions_for_role_async", matrix)
    limiter.reset()
    client = TestClient(app)
    client.cookies.set(SESSION_COOKIE, session_serializer.dumps(create_session_data(
        {"id": str(PERSON), "first_name": "T", "last_name": "U", "username": "t",
         "auth_date": str(int(_time.time()))}, role="viewer")))

    def sign_in(role: str, tabs: Optional[List[str]] = None):
        who.clear()
        who.update(role=role, allowed_features=tabs)
        return client

    yield sign_in
    limiter.reset()


class TestTheMarketingTab:
    def test_a_viewer_of_the_tab_reads_and_may_not_save(self, as_person):
        client = as_person("viewer", ["marketing"])
        assert client.get("/api/promotions").status_code == 200
        assert client.get("/api/promotions/12/versions").status_code == 200
        for method, path in (("POST", "/api/promotions"), ("PUT", "/api/promotions/12"),
                             ("POST", "/api/promotions/12/cancel"),
                             ("POST", "/api/promotions/12/restore")):
            assert client.request(method, path, json=_payload()).status_code == 403, path

    def test_an_editor_of_the_tab_saves(self, as_person):
        client = as_person("editor", ["marketing"])
        assert client.post("/api/promotions", json=_payload()).status_code == 201
        assert client.post("/api/promotions/12/cancel").status_code == 200

    def test_an_editor_without_the_tab_sees_nothing(self, as_person):
        client = as_person("editor", ["traffic"])
        assert client.get("/api/promotions").status_code == 403
        assert client.post("/api/promotions", json=_payload()).status_code == 403

    def test_the_default_tabs_of_a_viewer_hold_it(self, as_person):
        """No override: a viewer's default set (`standard`) has marketing."""
        client = as_person("viewer", None)
        assert client.get("/api/promotions").status_code == 200
        assert client.post("/api/promotions", json=_payload()).status_code == 403


# ─── One writer ─────────────────────────────────────────────────────────────

@pytest.fixture(scope="module")
def walk():
    from tests.unit.test_write_chains import _CAPTURE_WALKED, _Walk

    return _Walk(_CAPTURE_WALKED)


ROUTES = "web/routes/api/promotions.py"


class TestOneWriter:
    """Read off the repository walk `tests/unit/test_write_chains.py` keeps,
    over every tree a container runs."""

    def test_the_table_has_one_writer(self, walk):
        """Kills: a second writer of the versions anywhere — a route, a
        script or the bot writing a row of its own."""
        writers = {key for key, targets in walk.writers().items() if TABLE in targets}
        assert writers == {("core/promotions.py", "_write_version")}

    def test_it_only_ever_inserts(self, walk):
        """Append-only: no UPDATE, DELETE or TRUNCATE of any of revision
        0039's tables in any statement the walk can read. Kills: an edit
        written over its version, or history deleted."""
        import re

        bad = re.compile(r"\b(UPDATE|DELETE\s+FROM|TRUNCATE(\s+TABLE)?)\s+app\.promotion_\w+",
                         re.I)
        found = []
        for key, (module, fn) in walk.fns.items():
            for text in module.texts(fn, walk.modules):
                if bad.search(text):
                    found.append(key)
        assert found == []

    def test_the_writer_is_reached_from_the_four_saves_and_they_from_the_routes(self, walk):
        """Kills: a save reached from a GET, a job or the bot."""
        import ast

        assert walk.callers.get(("core/promotions.py", "_write_version")) == {
            ("core/promotions.py", "create"), ("core/promotions.py", "update"),
            ("core/promotions.py", "cancel"), ("core/promotions.py", "restore")}
        expected = {
            "create": {(ROUTES, "create_promotion")},
            "update": {(ROUTES, "update_promotion")},
            "cancel": {(ROUTES, "cancel_promotion")},
            "restore": {(ROUTES, "restore_promotion")},
        }
        for save, routes in expected.items():
            callers = walk.callers.get(("core/promotions.py", save), set())
            assert {c for c in callers if c[0] == ROUTES} == routes, save

        # `x.update()` elsewhere resolves to every function of that name (the
        # walk's "more callers, never fewer"), so who reaches the saves is
        # read off the code itself: a call through a name bound to
        # `core.promotions`, or a save imported from it by name.
        reaching = set()
        for module in walk.modules.values():
            aliases = {a for a, target in module.module_aliases.items()
                       if target == "core.promotions"}
            imported = {name for name, (src, attr) in module.imports.items()
                        if src == "core.promotions" and attr in expected}
            for node in ast.walk(module.tree):
                if not isinstance(node, ast.Call):
                    continue
                f = node.func
                if (isinstance(f, ast.Attribute) and isinstance(f.value, ast.Name)
                        and f.value.id in aliases and f.attr in expected):
                    reaching.add(module.rel)
                if isinstance(f, ast.Name) and f.id in imported:
                    reaching.add(module.rel)
        assert reaching == {ROUTES}
