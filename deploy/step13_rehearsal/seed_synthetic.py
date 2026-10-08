"""--local only: a small, entirely invented KeyCRM account for the rehearsal.

`deploy/step13_rehearsal.sh --local` cannot use production's backups — they
never leave the host — so it builds its own: this writes the payloads, the
KeyCRM stub serves them, and a `reh-web` booted over an empty data directory
syncs them exactly as production once synced the real ones (full sync, the
landing mirror into Postgres, the backfills, both derivations, the nightly
DuckDB backup). The rehearsal proper then runs unchanged over that dump and
that backup. No production byte is involved; every name, phone and comment
here is generated.

    python seed_synthetic.py OUT_DIR [--orders 400] [--days 120]

Deterministic (a fixed seed), so two local runs rehearse the same history.
Standard library only.
"""
from __future__ import annotations

import argparse
import json
import random
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List

# The application's own constants, restated because this runs without it:
# core.duckdb_constants (B2B_MANAGER_ID = 15, RETAIL_MANAGER_IDS) and the
# known status ids of core.data_quality.KNOWN_STATUS_IDS with their groups.
B2B_MANAGER = 15
RETAIL_MANAGERS = (4, 8)
INTERNAL_MANAGERS = (30, 31)
STATUSES = ((1, 1), (2, 1), (8, 4), (9, 4), (12, 5), (20, 4))
RETURNS = ((19, 6), (22, 6))
SOURCES = (1, 1, 2, 4, 4, 4, 3)   # Instagram, Telegram, website; one Opencart
BRAND_FIELD = "Brand"       # core.landing_rows.BRAND_FIELD_NAME

FIRST_ORDER_ID = 900001
FIRST_BUYER_ID = 500001


def _iso(moment: datetime) -> str:
    return moment.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000000Z")


def build(orders: int, days: int, seed: int = 13) -> Dict[str, List[Dict[str, Any]]]:
    rng = random.Random(seed)
    now = datetime.now(timezone.utc).replace(microsecond=0)

    categories = [
        {"id": 101, "name": "Care", "parent_id": None},
        {"id": 102, "name": "Makeup", "parent_id": None},
        {"id": 111, "name": "Cleansers", "parent_id": 101},
        {"id": 112, "name": "Creams", "parent_id": 101},
        {"id": 121, "name": "Lips", "parent_id": 102},
        {"id": 122, "name": "Eyes", "parent_id": 102},
    ]
    brands = ("Reh Alpha", "Reh Beta", "Reh Gamma", None)
    products = []
    for n in range(30):
        brand = brands[n % len(brands)]
        products.append({
            "id": 2001 + n, "name": f"Synthetic product {n + 1}",
            "category_id": (111, 112, 121, 122)[n % 4], "sku": f"REH-{n + 1:03d}",
            "price": float(150 + 25 * (n % 12)),
            "custom_fields": ([{"name": BRAND_FIELD, "value": [brand]}] if brand else []),
        })
    users = [{"id": i, "full_name": f"Synthetic manager {i}", "email": f"m{i}@reh.invalid",
              "status": "active"}
             for i in (*RETAIL_MANAGERS, B2B_MANAGER, *INTERNAL_MANAGERS)]
    expense_types = [
        {"id": 1, "name": "dictionaries.expense_types.delivery", "alias": "delivery", "is_active": True},
        {"id": 2, "name": "Commission", "alias": "commission", "is_active": True},
        {"id": 3, "name": "Packaging", "alias": None, "is_active": True},
    ]

    buyers: Dict[int, Dict[str, Any]] = {}
    payloads = []
    for n in range(orders):
        oid = FIRST_ORDER_ID + n
        # Spread over the window, oldest first, the newest a couple of hours old.
        ordered = now - timedelta(days=days) + timedelta(
            seconds=int((days * 86400 - 7200) * n / max(orders - 1, 1)))
        source = rng.choice(SOURCES)
        status, group = rng.choice(RETURNS) if rng.random() < 0.08 else rng.choice(STATUSES)
        if source == 4 and rng.random() < 0.7:
            manager = None
        else:
            roll = rng.random()
            mid = (rng.choice(RETAIL_MANAGERS) if roll < 0.75 else
                   B2B_MANAGER if roll < 0.9 else rng.choice(INTERNAL_MANAGERS))
            manager = {"id": mid, "name": f"Synthetic manager {mid}"}
        bid = FIRST_BUYER_ID + rng.randrange(0, max(orders // 3, 1))
        buyer = buyers.setdefault(bid, {
            "id": bid, "full_name": f"Synthetic buyer {bid}",
            "phone": [f"+38000{bid:07d}"], "email": [f"b{bid}@reh.invalid"],
            "created_at": _iso(ordered), "updated_at": _iso(ordered),
            "shipping": [{"city": rng.choice(("Kyiv", "Lviv", "Odesa")), "region": None}],
        })
        lines, total = [], 0.0
        for pos in range(rng.randint(1, 3)):
            product = rng.choice(products)
            qty = rng.randint(1, 2)
            price = product["price"]
            total += qty * price
            lines.append({"id": oid * 10 + pos, "name": product["name"], "quantity": qty,
                          "price_sold": f"{price:.2f}",
                          "offer": {"product_id": product["id"], "sku": product["sku"]}})
        comment = None
        if source == 4:
            kind = rng.random()
            if kind < 0.5:
                comment = (f"utm_source={rng.choice(('facebook', 'instagram', 'google'))}"
                           f"&utm_medium=cpc&utm_campaign=reh_{rng.randint(1, 6)}")
            elif kind < 0.8:
                comment = f"_fbp=fb.1.{oid}.1 ttp=reh{oid}"
        expenses = []
        if rng.random() < 0.4:
            expenses.append({"id": 300000 + n, "expense_type_id": rng.choice((1, 2, 3)),
                             "amount": f"{rng.choice((45, 60, 80)):.2f}", "description": None,
                             "status": "paid", "payment_date": _iso(ordered),
                             "created_at": _iso(ordered)})
        updated = ordered + timedelta(minutes=rng.randint(5, 600))
        if updated > now - timedelta(minutes=30):
            updated = now - timedelta(minutes=30)
        payloads.append({
            "id": oid, "source_id": source, "status_id": status, "status_group_id": group,
            "grand_total": f"{total:.2f}", "ordered_at": _iso(ordered),
            "created_at": _iso(ordered), "updated_at": _iso(updated),
            "buyer": buyer, "manager": manager, "products": lines,
            "manager_comment": comment,
            "promocode": "REHTEN" if rng.random() < 0.1 else None,
            "expenses": expenses,
        })
    return {
        "order.json": payloads, "products.json": products, "categories.json": categories,
        "users.json": users, "expense_types.json": expense_types,
        "buyers.json": sorted(buyers.values(), key=lambda b: b["id"]),
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("out")
    parser.add_argument("--orders", type=int, default=400)
    parser.add_argument("--days", type=int, default=120)
    args = parser.parse_args()
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    for name, rows in build(args.orders, args.days).items():
        (out / name).write_text(json.dumps(rows, ensure_ascii=False))
    print(json.dumps({name: len(rows) for name, rows in build(args.orders, args.days).items()}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
