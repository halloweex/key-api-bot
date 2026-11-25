"""Two years of orders across every sales type, in Bronze, with the managers'
classifications over time: the goals history's fixture, and what each
calculator should make of it.
"""
from __future__ import annotations

import os
from datetime import date, datetime, time, timedelta
from zoneinfo import ZoneInfo

from core.duckdb_constants import (
    B2B_MANAGER_ID, EXHIBITION_SOURCE_ID, OFFLINE_STORE_SOURCE_ID,
    RETAIL_MANAGER_IDS,
)
from tests.integration.pg_tab_seed import derive, seed


DSN = os.getenv("KS_PG_DSN")


KYIV = ZoneInfo("Europe/Kyiv")


TIMEOUT_S = 60


RETAIL = RETAIL_MANAGER_IDS[0]          # listed, classified retail


NEVER_CLASSIFIED = RETAIL_MANAGER_IDS[1]  # listed, is_retail, no interval


LISTED_BUT_NOT_RETAIL = RETAIL_MANAGER_IDS[2]  # listed, is_retail FALSE, no interval


AS_OF = 34                               # retail from SWITCH on, internal before


UNLISTED_RETAIL = 28                     # is_retail TRUE, not listed, no interval


INTERNAL = 40


SWITCH = date(2026, 3, 1)


MANAGERS = [  # (id, is_retail)
    (RETAIL, True), (NEVER_CLASSIFIED, True), (LISTED_BUT_NOT_RETAIL, False),
    (AS_OF, False), (UNLISTED_RETAIL, True), (INTERNAL, False),
    (B2B_MANAGER_ID, False),
]


INTERVALS = [  # (manager_id, is_retail, valid_from, valid_to)
    (RETAIL, True, date(1970, 1, 1), None),
    (AS_OF, False, date(1970, 1, 1), SWITCH),
    (AS_OF, True, SWITCH, None),
    (INTERNAL, False, date(1970, 1, 1), None),
    (B2B_MANAGER_ID, False, date(1970, 1, 1), None),
]


def _kyiv_noon(d: date) -> datetime:
    return datetime.combine(d, time(12, 0), tzinfo=KYIV)


# (id, source_id, status_id, manager_id, ordered_at)
ORDERS = [
    (1, 1, 1, None, _kyiv_noon(date(2026, 2, 10))),
    (2, 1, 1, B2B_MANAGER_ID, _kyiv_noon(date(2026, 2, 10))),
    (3, EXHIBITION_SOURCE_ID, 1, RETAIL, _kyiv_noon(date(2026, 2, 11))),
    (4, 1, 1, RETAIL, _kyiv_noon(date(2026, 2, 11))),
    (5, 2, 1, AS_OF, _kyiv_noon(SWITCH - timedelta(days=5))),
    (6, 2, 1, AS_OF, _kyiv_noon(SWITCH + timedelta(days=5))),
    (7, 2, 1, NEVER_CLASSIFIED, _kyiv_noon(date(2026, 2, 12))),
    (8, 1, 1, INTERNAL, _kyiv_noon(date(2026, 2, 12))),
    (9, EXHIBITION_SOURCE_ID, 1, None, _kyiv_noon(date(2026, 2, 13))),
    (10, EXHIBITION_SOURCE_ID, 1, B2B_MANAGER_ID, _kyiv_noon(date(2026, 2, 13))),
    (11, 4, 19, None, _kyiv_noon(date(2026, 2, 14))),   # a return: status is not this rule
    # 22:30 Kyiv on the eve of SWITCH is 20:30 UTC the same day; 00:30 Kyiv
    # on SWITCH is 22:30 UTC the day before. Both sides must date it by Kyiv.
    (12, 1, 1, AS_OF, datetime.combine(SWITCH, time(0, 30), tzinfo=KYIV)),
    (13, 1, 1, LISTED_BUT_NOT_RETAIL, _kyiv_noon(date(2026, 2, 15))),
    (14, 1, 1, UNLISTED_RETAIL, _kyiv_noon(date(2026, 2, 15))),
    (15, 1, 1, AS_OF, datetime.combine(SWITCH - timedelta(days=1), time(23, 30), tzinfo=KYIV)),
    # The offline store (the owner's decision of 2026-11-15): its own sales
    # type whoever sells there — a manager on no retail list, a retail one,
    # nobody named.
    (16, OFFLINE_STORE_SOURCE_ID, 1, INTERNAL, _kyiv_noon(date(2026, 2, 16))),
    (17, OFFLINE_STORE_SOURCE_ID, 1, RETAIL, _kyiv_noon(date(2026, 2, 16))),
    (18, OFFLINE_STORE_SOURCE_ID, 1, None, _kyiv_noon(date(2026, 2, 17))),
]


ALL_IDS = {o[0] for o in ORDERS}


EXHIBITION = {3, 9, 10}


OFFLINE = {16, 17, 18}


# What each scenario must select, written out rather than derived, so that a
# rule changed in the CASE itself — and therefore in Silver too — still fails.
EXPECTED = {
    "classified": {
        "retail": {1, 4, 6, 7, 11, 12, 14},
        "b2b": {2},
        "exhibition": EXHIBITION,
        "offline": OFFLINE,
        "internal": {5, 8, 13, 15},
    },
    # No intervals at all: `managers.is_retail` decides.
    "no_classifications": {
        "retail": {1, 4, 7, 11, 14},
        "b2b": {2},
        "exhibition": EXHIBITION,
        "offline": OFFLINE,
        "internal": {5, 6, 8, 12, 13, 15},
    },
    # Neither table has spoken: the listed ids decide.
    "nothing_synced": {
        "retail": {1, 4, 7, 11, 13},
        "b2b": {2},
        "exhibition": EXHIBITION,
        "offline": OFFLINE,
        "internal": {5, 6, 8, 12, 14, 15},
    },
}


# Production's shape: the first order is 2023-12-02, so there are two full
# years and one pair of them. (Three full years reached a separate, older
# defect — `yoy_rates` held DuckDB DECIMALs and the recency weights are floats,
# so a second pair raised TypeError. Chain 7b-1 fixed it; the pinning is in
# `tests/unit/test_goals_read_paths_do_not_write.py`.)
HISTORY_START = date(2023, 12, 2)


HISTORY_END = date(2025, 12, 31)


def _history_rows():
    """One retail order a day with a seasonal and a yearly shape, plus a b2b
    order on eight days in nine (enough for b2b to reach every month's 20-day
    floor), an exhibition order every seventh (by a retail manager — the case
    the old filter got wrong) and a return every eleventh."""
    rows, oid, d = [], 1, HISTORY_START
    while d <= HISTORY_END:
        growth = 1.0 + 0.15 * (d.year - HISTORY_START.year)
        season = 1.0 + 0.4 * (d.month in (11, 12)) - 0.2 * (d.month in (6, 7))
        total = round(1000.0 * growth * season + (d.day % 7) * 13.0, 2)
        rows.append((oid, 1, 1, total, _kyiv_noon(d), None)); oid += 1
        if d.toordinal() % 9 != 0:
            rows.append((oid, 1, 1, 500.0 + d.month * 11.0, _kyiv_noon(d), B2B_MANAGER_ID)); oid += 1
        if d.toordinal() % 7 == 0:
            rows.append((oid, EXHIBITION_SOURCE_ID, 1, 700.0, _kyiv_noon(d), RETAIL)); oid += 1
        if d.toordinal() % 11 == 0:
            rows.append((oid, 2, 19, 900.0, _kyiv_noon(d), None)); oid += 1
        d += timedelta(days=1)
    return rows


async def _seed_history_pg(conn, pool) -> None:
    """The history in Postgres, as the sync lands it: Bronze orders, the
    two managers and their classification — Postgres derives Silver and Gold
    itself (`tests.integration.pg_tab_seed.derive`), the production path."""
    await conn.execute("DELETE FROM app.manager_classifications")
    await conn.execute("DELETE FROM bronze.managers")
    await conn.executemany(
        "INSERT INTO bronze.managers (id, name, is_retail) VALUES ($1, 'm', $2)",
        [(RETAIL, True), (B2B_MANAGER_ID, False)])
    await conn.executemany(
        "INSERT INTO app.manager_classifications "
        "(manager_id, is_retail, valid_from, valid_to) VALUES ($1, $2, $3, NULL)",
        [(RETAIL, True, date(1970, 1, 1)), (B2B_MANAGER_ID, False, date(1970, 1, 1))])
    await seed(conn, orders=[
        (oid, src, status, total, when, 1, mgr, None)
        for oid, src, status, total, when, mgr in _history_rows()])
    for table in ("app.seasonal_indices", "app.weekly_patterns", "app.growth_metrics"):
        await conn.execute(f"DELETE FROM {table}")
    await derive(pool)


def _settled(value):
    """Floats to twelve places. DuckDB aggregates DOUBLEs in whatever order the
    plan hands it rows, so AVG/STDDEV over the same values can differ in the
    last bit between two plans — 0.21506006243397469 against …463 was seen for
    the growth cap. That is the engine, not the answer."""
    if isinstance(value, dict):
        return {k: _settled(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_settled(v) for v in value]
    if isinstance(value, float) or hasattr(value, "as_tuple"):
        return round(float(value), 12)
    return value


async def _calculators(store, sales_type):
    seasonality = await store.calculate_seasonality_indices(sales_type)
    yoy = await store.calculate_yoy_growth(sales_type)
    weekly = await store.calculate_weekly_patterns(sales_type)
    caps = [await store._dynamic_growth_cap(m, sales_type) for m in range(1, 13)]
    return _settled([seasonality, yoy, weekly, caps])


def _without_clock(smart):
    smart = dict(smart)
    smart["metadata"] = {k: v for k, v in smart["metadata"].items()
                         if k != "calculatedAt"}
    return _settled(smart)
