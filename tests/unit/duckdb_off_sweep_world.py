"""The stage 5 sweep's world outside the process: a KeyCRM and a TurboSMS
that answer with canned payloads, or refuse — never collected on its own, the
name does not match `test_*.py`.

Until the review of PR-9 the sweep refused both at the socket in every
flavour, so nothing ran that runs only once data has arrived: no sync wrote an
order it had fetched; the gap backfill, the status refresh, the half-written
repair, the resync and both buyer syncs stopped at their first request; the
SMS send answered 502, so the delivery report for its message was never a
known one. The code was clean on every one of those paths given data — a gap
in the proof, not a defect — but a reach for the file that only a payload
takes would have passed the sweep.

The world is served through `httpx.MockTransport`, under the clients rather
than in place of them: KeyCRM's retries, circuit breaker and status codes,
TurboSMS's configuration check and response codes all run as they do against
the real services. Refusing, it raises what a refused connection raises, so a
flavour with the world offline meets KeyCRM's and TurboSMS's outage branches
as it did at the socket. Every request is recorded with what it was served or
that it was refused — a request KeyCRM's open circuit breaker turned away
included, which never reaches the transport and means the same thing — and
the runner files each under the step that made it, as it files the refused
sites.

WHAT KEYCRM HOLDS

Shaped so that each path writing what KeyCRM serves has something only it
writes, on top of the history the live flavour seeds (`_seed_live_history`:
orders 1 and 100–108, buyers 1–3, products 1–3 in categories 1–2, manager 1).
The filters `created_between` and `updated_between` are applied, so a window
decides what a path is given, as it does against KeyCRM:

- orders 109–111, created days ago and changed minutes before each request:
  every window serves them, and each fetch finds them changed, so each sync
  writes them again. One is the website's, with UTM tags; one names buyer 4,
  whom nobody holds;
- order 60, created 200 days ago: inside the 365 days of the weekly full sync
  and of the resync, and of nothing else;
- order 50, created 450 days ago: no window reaches it, and it is a hole in
  the ids Postgres holds — the gap backfill's, by id;
- order 1, the seeded order with revenue and no line items, here with its
  lines — the half-written repair's, by id;
- buyers 1–3 in the listing, and 4 by id alone. The door that walks the
  listing (`sync-all-buyers`) runs just before the one that fetches, by id,
  the buyers Silver's orders name and nobody holds (`sync-buyers`); buyer 4
  is the one the listing leaves it. (The minute sync's buyers step is that
  same function and reads the same Silver, which in the process sweep is
  derived only by a later job, so there it finds nobody missing.);
- the catalogue the seed holds, one offer and its stock, the manager, and an
  expense type named by a localisation key, as KeyCRM names some.

Any other id answers 404, as KeyCRM does for an order it does not have.

TurboSMS accepts every recipient, issuing `sweep-message-<n>` in order, so a
delivery report can name a message the send recorded (`message_ids`).
"""
from __future__ import annotations

import json
import re
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Dict, List, Optional, Tuple
from urllib.parse import urlsplit
from zoneinfo import ZoneInfo

import httpx

KEYCRM, TURBOSMS = "keycrm", "turbosms"
KYIV = ZoneInfo("Europe/Kyiv")
OFFLINE = "the world outside is offline in this sweep"

# The orders the world holds: id → (created days ago, updated minutes before
# the request — None for "when it was created" —, source, buyer, total).
ORDERS: Dict[int, Tuple[float, Optional[float], int, int, float]] = {
    109: (1, 10, 1, 1, 900.0),
    110: (2, 20, 2, 2, 1100.0),
    111: (3, 30, 4, 4, 1300.0),
    60: (200, None, 1, 2, 800.0),
    50: (450, None, 2, 3, 700.0),
    1: (400, None, 4, 1, 100.0),
}
LISTED_BUYERS = (1, 2, 3)
BUYERS = (*LISTED_BUYERS, 4)
UTM_COMMENT = "utm_source=facebook&utm_medium=cpc&utm_campaign=sweep"


def _what(endpoint: str) -> str:
    """An endpoint as the record names it: `order/{id}`, not `order/50`."""
    return re.sub(r"/\d+$", "/{id}", endpoint.strip("/"))


def _iso(moment: datetime) -> str:
    return moment.astimezone(timezone.utc).isoformat()


def _bound(text: str, *, end: bool) -> datetime:
    """One end of a KeyCRM `*_between` filter, in the forms the code sends:
    a date, a local `YYYY-MM-DD HH:MM:SS`, or an ISO moment with its offset.
    A date as the end covers the whole day."""
    text = text.strip()
    moment = datetime.fromisoformat(text)
    if moment.tzinfo is None:
        moment = moment.replace(tzinfo=KYIV)
    if end and len(text) == 10:
        moment += timedelta(days=1)
    return moment


class World:
    """KeyCRM and TurboSMS, answering when `answers` and refusing otherwise,
    with every request recorded until `drain` hands the record over."""

    def __init__(self, answers: bool) -> None:
        self.answers = answers
        self.started = datetime.now(timezone.utc)
        self.message_ids: List[str] = []
        self._calls: Dict[str, Dict[str, int]] = defaultdict(
            lambda: {"requests": 0, "rows": 0, "refused": 0})

    # ── the record ──

    def note(self, service: str, what: str, rows: int = 0, refused: bool = False) -> None:
        entry = self._calls[f"{service} {what}"]
        entry["requests"] += 1
        entry["rows"] += rows
        entry["refused"] += int(refused)

    def drain(self) -> Dict[str, Dict[str, int]]:
        """What was asked since the last drain: per `<service> <endpoint>`,
        the requests, the records served, and how many were refused."""
        calls, self._calls = dict(self._calls), defaultdict(
            lambda: {"requests": 0, "rows": 0, "refused": 0})
        return calls

    # ── KeyCRM ──

    def _order(self, oid: int, now: datetime) -> Dict[str, Any]:
        days, minutes, source, buyer, total = ORDERS[oid]
        created = self.started - timedelta(days=days)
        updated = created if minutes is None else now - timedelta(minutes=minutes)
        return {
            "id": oid, "source_id": source, "status_id": 1, "status_group_id": 1,
            "grand_total": f"{total:.2f}", "ordered_at": _iso(created),
            "created_at": _iso(created), "updated_at": _iso(updated),
            "buyer": {"id": buyer, "full_name": f"Sweep Buyer {buyer}"},
            "manager": {"id": 1, "full_name": "Sweep Manager"},
            "manager_comment": UTM_COMMENT if source == 4 else None,
            "promocode": "ab1" if oid == 110 else None,
            "products": [
                {"id": oid * 1000 + 1, "name": "Serum A", "quantity": 1,
                 "price_sold": "500.00", "offer": {"id": 1, "product_id": 1, "sku": "SKU-1"}},
                {"id": oid * 1000 + 2, "name": "Cream B", "quantity": 1,
                 "price_sold": f"{total - 500:.2f}",
                 "offer": {"id": 2, "product_id": 2, "sku": "SKU-2"}},
            ],
            "expenses": [{"id": 90_000 + oid, "expense_type_id": 1, "amount": 50.0,
                          "description": "Delivery", "status": "paid",
                          "payment_date": _iso(created), "created_at": _iso(created)}],
        }

    def _buyer(self, bid: int) -> Dict[str, Any]:
        return {"id": bid, "full_name": f"Sweep Buyer {bid}",
                "phone": [f"+38050111111{bid}"], "email": [],
                "created_at": _iso(self.started - timedelta(days=90)),
                "updated_at": _iso(self.started - timedelta(days=1)),
                "shipping": [{"city": "Kyiv", "region": "Kyiv"}],
                "loyalty": [{"loyalty_program_name": "Sweep",
                             "loyalty_program_level_name": "Base",
                             "discount": 5, "amount": 100}]}

    @staticmethod
    def _catalogue(endpoint: str) -> Optional[List[Dict[str, Any]]]:
        brand = lambda name: [{"uuid": "CT_1001", "name": "Brand", "value": [name]}]  # noqa: E731
        return {
            "products/categories": [{"id": 1, "name": "Care", "parent_id": None},
                                    {"id": 2, "name": "Serums", "parent_id": 1}],
            "products": [
                {"id": 1, "name": "Serum A", "category_id": 2, "sku": "SKU-1",
                 "price": 500, "custom_fields": brand("ab1")},
                {"id": 2, "name": "Cream B", "category_id": 2, "sku": "SKU-2",
                 "price": 300, "custom_fields": brand("Brand B")},
                {"id": 3, "name": "Toner C", "category_id": 1, "sku": "SKU-3",
                 "price": 200, "custom_fields": brand("ab1")}],
            "order/expense-type": [{"id": 1, "name": "dictionaries.expense_types.delivery",
                                    "alias": "delivery", "is_active": True}],
            "users": [{"id": 1, "full_name": "Sweep Manager", "email": "m@sweep.test",
                       "status": "active"}],
            "offers": [{"id": 1, "product_id": 1, "sku": "SKU-1"},
                       {"id": 2, "product_id": 2, "sku": "SKU-2"}],
            "offers/stocks": [{"id": 1, "sku": "SKU-1", "price": 500, "purchased_price": 250,
                               "quantity": 5, "reserve": 1}],
        }.get(endpoint)

    @staticmethod
    def _in_window(order: Dict[str, Any], params) -> bool:
        for name, field in (("filter[created_between]", "created_at"),
                            ("filter[updated_between]", "updated_at")):
            window = params.get(name)
            if window:
                start, _, end = window.partition(",")
                moment = datetime.fromisoformat(order[field])
                if not _bound(start, end=False) <= moment <= _bound(end, end=True):
                    return False
        return True

    def keycrm(self, request: httpx.Request) -> httpx.Response:
        from core.config import KEYCRM_BASE_URL

        base = urlsplit(KEYCRM_BASE_URL).path.rstrip("/")
        endpoint = request.url.path[len(base):].strip("/")
        what = _what(endpoint)
        if not self.answers:
            self.note(KEYCRM, what, refused=True)
            raise httpx.ConnectError(OFFLINE, request=request)
        now = datetime.now(timezone.utc)
        params = request.url.params
        one = re.fullmatch(r"(order|buyer)/(\d+)", endpoint)
        if one:
            kind, ident = one.group(1), int(one.group(2))
            found = (self._order(ident, now) if kind == "order" and ident in ORDERS
                     else self._buyer(ident) if kind == "buyer" and ident in BUYERS
                     else None)
            self.note(KEYCRM, what, rows=int(found is not None))
            if found is None:
                return httpx.Response(404, json={"message": "Not found"})
            return httpx.Response(200, json=found)
        if endpoint == "order":
            items = [o for o in (self._order(i, now) for i in sorted(ORDERS))
                     if self._in_window(o, params)]
        elif endpoint == "buyer":
            items = [self._buyer(b) for b in LISTED_BUYERS]
        else:
            items = self._catalogue(endpoint)
            if items is None:
                self.note(KEYCRM, what)
                return httpx.Response(404, json={"message": "Not found"})
        page, limit = int(params.get("page") or 1), int(params.get("limit") or 50)
        served = items[(page - 1) * limit:page * limit]
        self.note(KEYCRM, what, rows=len(served))
        more = page * limit < len(items)
        return httpx.Response(200, json={
            "total": len(items), "current_page": page, "per_page": limit,
            "data": served,
            "next_page_url": f"{request.url.copy_merge_params({'page': page + 1})}"
                             if more else None})

    # ── TurboSMS ──

    def turbosms(self, request: httpx.Request) -> httpx.Response:
        path = request.url.path.strip("/")
        if not self.answers:
            self.note(TURBOSMS, path, refused=True)
            raise httpx.ConnectError(OFFLINE, request=request)
        body = json.loads(request.content or b"{}")
        if path == "message/send.json":
            result = []
            for phone in body.get("recipients") or []:
                message_id = f"sweep-message-{len(self.message_ids) + 1}"
                self.message_ids.append(message_id)
                result.append({"phone": phone, "response_code": 0,
                               "message_id": message_id, "response_status": "OK"})
        elif path == "message/status.json":
            result = [{"message_id": m, "response_code": 0, "status": "Delivered"}
                      for m in body.get("messages") or []]
        elif path == "user/balance.json":
            result = {"balance": 1000.0}
        else:
            self.note(TURBOSMS, path)
            return httpx.Response(404, json={"response_code": 404})
        self.note(TURBOSMS, path, rows=len(result) if isinstance(result, list) else 1)
        return httpx.Response(200, json={"response_code": 0, "response_status": "OK",
                                         "response_result": result})


def install(monkeypatch, world: World) -> None:
    """Route every KeyCRM and TurboSMS client in the process to `world`. Both
    are built with an explicit transport, which also keeps a proxy in the
    environment from being asked instead."""
    from core.keycrm import CircuitOpenError, KeyCRMClient
    from core.turbosms import TurboSmsClient

    async def connect(self) -> None:
        if self._client is None:
            self._client = httpx.AsyncClient(
                headers=self.headers, timeout=self.timeout,
                transport=httpx.MockTransport(world.keycrm))

    real_request: Callable = KeyCRMClient._request

    async def request(self, method, endpoint, params=None, json=None):
        try:
            return await real_request(self, method, endpoint, params=params, json=json)
        except CircuitOpenError:
            world.note(KEYCRM, _what(endpoint), refused=True)
            raise

    real_enter: Callable = TurboSmsClient.__aenter__

    async def enter(self):
        if self._transport is None:
            self._transport = httpx.MockTransport(world.turbosms)
        return await real_enter(self)

    monkeypatch.setattr(KeyCRMClient, "connect", connect)
    monkeypatch.setattr(KeyCRMClient, "_request", request)
    monkeypatch.setattr(TurboSmsClient, "__aenter__", enter)


def served(world_record: Dict[str, Dict[str, int]], service: str) -> int:
    """The records `service` served during a step, from its `world` entry."""
    return sum(v["rows"] for k, v in world_record.items() if k.startswith(service + " "))


def refused(world_record: Dict[str, Dict[str, int]]) -> int:
    """The requests refused during a step, from its `world` entry."""
    return sum(v["refused"] for v in world_record.values())


ORDER_ENDPOINTS = (f"{KEYCRM} order", f"{KEYCRM} order/{{id}}")


def orders_served(world_record: Dict[str, Dict[str, int]]) -> int:
    """The orders KeyCRM served during a step — listed or by id."""
    return sum(world_record.get(k, {}).get("rows", 0) for k in ORDER_ENDPOINTS)
