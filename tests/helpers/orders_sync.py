"""The order syncs end to end: KeyCRM faked at its HTTP transport, Postgres
real (batch 2, orders).

`keycrm_serving_orders(monkeypatch, orders)` answers as KeyCRM does, under the
real `core.keycrm.KeyCRMClient`, so its paging, retries, circuit breaker and
parse all run:

* `GET /order` lists the orders created (`filter[created_between]`) or
  updated (`filter[updated_between]`) on a day of the window, in pages of
  `limit`;
* `GET /order/{id}` answers that order, or 404;
* every other listing (managers, the catalogue, expense types, offers,
  stocks) is empty.

An `answer(request)` that returns a response serves it instead, a 429 storm
say (`answered(429)`). The process's client and circuit breaker are new for
the test and retries do not wait; all three are put back after it.

`orders_in_postgres()` empties chain 3's tables before and after, and puts
back the bookkeeping a sync stamps: `meta.chain_watermarks`,
`meta.mirror_state` and `meta.derivation_signal`. Ids, buyers and amounts are
synthetic.
"""
from __future__ import annotations

import dataclasses
import os
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple

import httpx

ORDER_TABLES = ("bronze.order_products", "bronze.expenses", "bronze.orders",
                "app.order_backfill_misses", "app.order_versions")
KEPT = ("meta.chain_watermarks", "meta.mirror_state", "meta.derivation_signal")


def order_payload(oid: int, *, created: datetime, status: int = 12, group: int = 4,
                  updated: Optional[datetime] = None, ordered: Optional[datetime] = None,
                  total: str = "100.00") -> Dict[str, Any]:
    """One order as KeyCRM serves it, with one line item and one expense."""
    return {
        "id": oid, "source_id": 1, "status_id": status, "status_group_id": group,
        "grand_total": total, "ordered_at": (ordered or created).isoformat(),
        "created_at": created.isoformat(), "updated_at": (updated or created).isoformat(),
        "buyer": {"id": 5401}, "manager": {"id": 4},
        "manager_comment": None, "promocode": None,
        "products": [{"name": "Test cream", "quantity": 1, "price_sold": total,
                      "offer": {"product_id": 701}}],
        "expenses": [{"id": oid * 10, "expense_type_id": 1, "amount": 10.5,
                      "status": "paid"}],
    }


def answered(status: int) -> httpx.Response:
    """KeyCRM answering `status`, asking for no wait before a retry."""
    return httpx.Response(status, headers={"Retry-After": "0"},
                          json={"message": f"faked {status}"})


def order_id_asked(request: httpx.Request) -> Optional[int]:
    """The id of a `GET /order/{id}`, else None."""
    *_, parent, last = request.url.path.rstrip("/").split("/")
    return int(last) if parent == "order" and last.isdigit() else None


def listing_window(request: httpx.Request) -> Optional[Tuple[str, str, str]]:
    """`(field, first day, last day)` of an order listing's window, else None."""
    if not request.url.path.rstrip("/").endswith("/order"):
        return None
    for key, field in (("filter[created_between]", "created_at"),
                       ("filter[updated_between]", "updated_at")):
        value = request.url.params.get(key)
        if value:
            first, last = (part.strip()[:10] for part in value.split(","))
            return field, first, last
    return "created_at", "", "9999-12-31"


class KeyCRMOrders:
    """KeyCRM holding `orders`, and every request it received, in order."""

    def __init__(self, orders: Sequence[Dict[str, Any]]):
        self.orders = {int(o["id"]): o for o in orders}
        self.requests: List[httpx.Request] = []

    @property
    def by_id(self) -> List[int]:
        """The orders asked for one by one."""
        return [oid for oid in map(order_id_asked, self.requests) if oid is not None]

    def respond(self, request: httpx.Request) -> httpx.Response:
        oid = order_id_asked(request)
        if oid is not None:
            if oid in self.orders:
                return httpx.Response(200, json=self.orders[oid])
            return httpx.Response(404, json={"message": "Requested entity not found"})
        window = listing_window(request)
        if window is None:
            return httpx.Response(200, json={"data": []})
        field, first, last = window
        listed = [o for _id, o in sorted(self.orders.items())
                  if first <= str(o.get(field) or "")[:10] <= last]
        limit = int(request.url.params.get("limit", "50"))
        page = int(request.url.params.get("page", "1"))
        return httpx.Response(200, json={"data": listed[(page - 1) * limit:page * limit]})


def keycrm_serving_orders(
    monkeypatch, orders: Sequence[Dict[str, Any]],
    answer: Optional[Callable[[httpx.Request], Optional[httpx.Response]]] = None,
) -> KeyCRMOrders:
    from core import keycrm
    from core.resilience import CircuitBreaker

    served = KeyCRMOrders(orders)

    def handler(request: httpx.Request) -> httpx.Response:
        served.requests.append(request)
        instead = answer(request) if answer is not None else None
        return instead if instead is not None else served.respond(request)

    transport = httpx.MockTransport(handler)
    real = httpx.AsyncClient

    class _OverTheFake(real):
        def __init__(self, *args, **kwargs):
            kwargs["transport"] = transport
            super().__init__(*args, **kwargs)

    monkeypatch.setattr(keycrm.httpx, "AsyncClient", _OverTheFake)
    monkeypatch.setattr(keycrm, "_client_instance", None)
    monkeypatch.setattr(keycrm, "_circuit_breaker",
                        CircuitBreaker(config=keycrm.CIRCUIT_BREAKER_CONFIG))
    monkeypatch.setattr(keycrm, "RETRY_CONFIG",
                        dataclasses.replace(keycrm.RETRY_CONFIG, base_delay=0.0))
    return served


async def _clear(conn) -> None:
    for table in ORDER_TABLES:
        await conn.execute(f"DELETE FROM {table}")


@asynccontextmanager
async def orders_in_postgres(dsn: Optional[str] = None):
    """Chain 3's tables empty for the block, and the stamps a sync writes put
    back as they were."""
    import asyncpg

    conn = await asyncpg.connect(dsn or os.environ["KS_PG_DSN"])
    try:
        kept = {table: await conn.fetch(f"SELECT * FROM {table}") for table in KEPT}
        await _clear(conn)
        try:
            yield
        finally:
            await _clear(conn)
            for table, rows in kept.items():
                await conn.execute(f"DELETE FROM {table}")
                for row in rows:
                    columns = list(row.keys())
                    holes = ", ".join(f"${i}" for i in range(1, len(columns) + 1))
                    await conn.execute(
                        f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({holes})",
                        *row.values())
    finally:
        await conn.close()


async def orders_held(dsn: Optional[str] = None) -> Dict[int, Dict[str, Any]]:
    """Every order Postgres holds: its status, line item ids and expense ids."""
    import asyncpg

    conn = await asyncpg.connect(dsn or os.environ["KS_PG_DSN"])
    try:
        held: Dict[int, Dict[str, Any]] = {}

        def order(oid: int) -> Dict[str, Any]:
            return held.setdefault(oid, {"status": None, "lines": [], "expenses": []})

        for r in await conn.fetch("SELECT id, status_id FROM bronze.orders"):
            order(r["id"])["status"] = r["status_id"]
        for r in await conn.fetch("SELECT id, order_id FROM bronze.order_products ORDER BY id"):
            order(r["order_id"])["lines"].append(r["id"])
        for r in await conn.fetch("SELECT id, order_id FROM bronze.expenses ORDER BY id"):
            order(r["order_id"])["expenses"].append(r["id"])
        return held
    finally:
        await conn.close()


async def versions_of(oid: int, dsn: Optional[str] = None) -> List[Tuple[str, int]]:
    """`(kind, status_id)` of every archived version of an order, oldest first."""
    import asyncpg

    conn = await asyncpg.connect(dsn or os.environ["KS_PG_DSN"])
    try:
        return [(r["kind"], r["status_id"]) for r in await conn.fetch(
            "SELECT kind, status_id FROM app.order_versions WHERE order_id = $1 ORDER BY id",
            oid)]
    finally:
        await conn.close()
