"""KeyCRM, as far as the rehearsal's web process can tell — and nothing more.

The rehearsal must never call the real KeyCRM (its quota is shared with
production), and nothing in the application switches the sync off: the boot
sync, `incremental_sync`, the reconciliation and the repair jobs all call it
unconditionally. So `reh-web` is pointed here with `KEYCRM_BASE_URL`, on a
Docker network that has no route out at all, and this answers like a KeyCRM
account with nothing in it — plus whatever the rehearsal puts in the data
directory:

    order.json         the orders `GET /v1/order` serves (the rehearsal's one
                       fixture order, or the synthetic history in --local)
    products.json, categories.json, users.json, expense_types.json,
    buyers.json, offers.json, stocks.json     (--local's synthetic seed only)

Every file is read on every request, so the rehearsal changes what KeyCRM
"says" by replacing a file, atomically, from the host. A missing file is an
empty list. Every request is logged as one `REH-KEYCRM <METHOD> <path>?<query>`
line — the evidence row K0 counts. Standard library only.
"""
from __future__ import annotations

import json
import os
import re
import sys
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import parse_qs, urlsplit

DATA = Path(os.environ.get("REH_KEYCRM_DIR", "/keycrm"))

# resource path (after /v1/) -> file
LISTS = {
    "order": "order.json",
    "order/expense-type": "expense_types.json",
    "products": "products.json",
    "products/categories": "categories.json",
    "users": "users.json",
    "buyer": "buyers.json",
    "offers": "offers.json",
    "offers/stocks": "stocks.json",
}
BY_ID = {"order": "order.json", "buyer": "buyers.json", "products": "products.json"}
SAFE_KEYS = {"page", "limit", "include", "filter[created_between]", "filter[updated_between]"}

try:
    from zoneinfo import ZoneInfo

    KYIV: Any = ZoneInfo("Europe/Kyiv")
except Exception:  # noqa: BLE001 — a stub without tzdata still filters by day
    KYIV = timezone(timedelta(hours=3))


def _read(name: str) -> List[Dict[str, Any]]:
    try:
        data = json.loads((DATA / name).read_text())
    except (FileNotFoundError, ValueError):
        return []
    return data if isinstance(data, list) else []


def _instant(value: Any) -> Optional[datetime]:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=KYIV)


def _bounds(raw: str) -> Optional[Tuple[datetime, datetime]]:
    """KeyCRM's `"A, B"` window, in Kyiv time; a bare date is the whole day."""
    parts = [p.strip() for p in raw.split(",")]
    if len(parts) != 2:
        return None
    out = []
    for i, part in enumerate(parts):
        moment = _instant(part.replace(" ", "T", 1))
        if moment is None:
            return None
        if re.fullmatch(r"\d{4}-\d{2}-\d{2}", part) and i == 1:
            moment += timedelta(days=1)
        out.append(moment)
    return out[0], out[1]


def _filtered(items: List[Dict[str, Any]], query: Dict[str, List[str]]) -> List[Dict[str, Any]]:
    for key, field in (("filter[created_between]", "created_at"),
                       ("filter[updated_between]", "updated_at")):
        if key in query:
            window = _bounds(query[key][0])
            if window is None:
                return []
            lo, hi = window
            items = [i for i in items
                     if (_instant(i.get(field)) is not None and lo <= _instant(i.get(field)) <= hi)]
    # A search by phone or e-mail finds nobody.
    if any(k.startswith("filter[") and k not in ("filter[created_between]", "filter[updated_between]")
           for k in query):
        return []
    return items


class Handler(BaseHTTPRequestHandler):
    server_version = "reh-keycrm/1"

    def log_message(self, fmt: str, *args: Any) -> None:  # the one line below instead
        return

    def _send(self, code: int, body: Any) -> None:
        raw = json.dumps(body).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(raw)))
        self.end_headers()
        self.wfile.write(raw)

    def _handle(self) -> None:
        parts = urlsplit(self.path)
        query = parse_qs(parts.query)
        # Values only for the keys that cannot carry a person: a search by
        # phone or e-mail would otherwise write one into the log.
        shown = "&".join(
            f"{k}={v[0] if k in SAFE_KEYS else '<redacted>'}" for k, v in sorted(query.items()))
        print(f"REH-KEYCRM {self.command} {parts.path}?{shown}", flush=True)
        path = parts.path
        if path.startswith("/v1/"):
            path = path[len("/v1/"):]
        path = path.strip("/")
        if self.command != "GET":
            self._send(405, {"message": "the rehearsal's KeyCRM is read-only"})
            return
        match = re.fullmatch(r"(order|buyer|products)/(\d+)", path)
        if match:
            wanted = int(match.group(2))
            for item in _read(BY_ID[match.group(1)]):
                if item.get("id") == wanted:
                    self._send(200, item)
                    return
            self._send(404, {"message": "Not found"})
            return
        items = _filtered(_read(LISTS[path]), query) if path in LISTS else []
        try:
            limit = max(1, int(query.get("limit", ["50"])[0]))
            page = max(1, int(query.get("page", ["1"])[0]))
        except ValueError:
            limit, page = 50, 1
        chunk = items[(page - 1) * limit: page * limit]
        more = page * limit < len(items)
        self._send(200, {"data": chunk, "total": len(items), "current_page": page,
                         "per_page": limit, "next_page_url": "next" if more else None})

    do_GET = _handle
    do_POST = _handle
    do_PUT = _handle
    do_DELETE = _handle


def main() -> int:
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 8080
    print(f"REH-KEYCRM-STUB listening on :{port}, data {DATA}", flush=True)
    ThreadingHTTPServer(("0.0.0.0", port), Handler).serve_forever()
    return 0


if __name__ == "__main__":
    sys.exit(main())
