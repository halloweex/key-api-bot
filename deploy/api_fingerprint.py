#!/usr/bin/env python3
"""Did a deploy change what the dashboard answers? A sha256 per response,
taken just before and just after, compared. Read-only.

Step 14 deletes DuckDB from the code one read router at a time, and each of
those PRs promises one thing: under today's `.env` no number on any screen
moves. A green suite says the code agrees with the tests; this says production
agrees with itself across the deploy. It asks a fixed list of GET routes for a
fixed, past window, keeps the sha256 and the size of every body — never the
body — and `compare` names every route whose answer moved.

WHY A PAST WINDOW, AND HOW FAR BACK

Every route that takes a window is asked for an absolute one that ended at
least 40 days ago: by default the last whole calendar month that did. The
05:15 status refresh rewrites the orders of the last 30 days every morning,
and the backdated refresh reaches orders dated into them, so a window closer
than that moves without any PR; 40 leaves ten days of margin. A request never
says `period=month` — that is "this month", and it moves with every order.

Some routes take no window at all — cohorts, the SMS audience preview and
the campaign list, the stock pages, the goal tables and their history, the
forecast, the expense list, and the header's filter lookups. Others take one
and still answer something counted from today:

  * the smart goals, for a month, count one signal back from today;
  * the customer insights answer the window's new and returning customers
    beside all-time figures over every order ever — lifetime value, purchase
    frequency and lifespan, the true repeat rate — so any order moves them;
  * the six /margin routes cost the window's lines at today's purchase
    price (`bronze.offer_stocks.purchased_price`, joined by SKU), which the
    hourly stock sync rewrites.

They are asked anyway, with the most absolute form each accepts (the SMS
preview with an absolute period, not a preset's "last N days"; the goals for
the window's month; the insights and the margin for the window), and marked
`live`: a new order, the 05:15 refresh, the hourly stock or catalogue sync,
or a training between the two runs can move them with nothing wrong.
`compare` lists them apart and gives them their own exit code, so a moved
live route is a question and a moved past window is a finding. Take the
"before" just before the merge and the "after" as soon as the deploy is
done: the less lands between them, the fewer live routes move. One that did
move is a question two hashes cannot answer — whether an order, a purchase
price or a training landed in between is read elsewhere — and two "before"s
settle nothing about it.

What an absolute route still reads from today is the catalogue. A line's
brand and category come from the products table, not from the order, so a
brand renamed or a product moved to another category in KeyCRM moves every
past-window answer that groups by them (the products, brands, reports and
product-intel routes) at the next hourly sync. Rare, and not noise: when
only those moved, ask whether the catalogue was edited between the runs.

Not asked: routes that need an id or a name from production in their path
or query (a category's drill-down, one campaign's results, one ABC class),
the CSV exports, search, and the admin and health pages.

THE SESSION

The routes need an admin session, which is the `dashboard_session` cookie of
an admin signed in to the dashboard. It is read from the environment variable
KS_FINGERPRINT_SESSION and from nowhere else: never an argument (the process
table shows those), never printed, never written to the snapshot or anywhere
else, and sent only to `--base`, only over https (http is accepted for a
loopback address alone), and never after a redirect — a 3xx is recorded as
its status, not followed. The token is a stateless 7-day signature that
logout does not revoke, so the owner runs this in their own terminal and
nobody pastes the value anywhere:

    read -rs KS_FINGERPRINT_SESSION && export KS_FINGERPRINT_SESSION
    deploy/api_fingerprint.py snap --base https://<dashboard host> --out /root/step14/before.json
    #   ... merge, wait for the deploy to finish ...
    deploy/api_fingerprint.py snap --base https://<dashboard host> --out /root/step14/after.json
    deploy/api_fingerprint.py compare /root/step14/before.json /root/step14/after.json
    unset KS_FINGERPRINT_SESSION

Two snapshots taken minutes apart can still differ where a query returns rows
in an order its ORDER BY does not fix. Take two "before" snapshots and compare
them first: a past-window route that moves between those is noise, not the
deploy. They say nothing about a live route, which can move between them for
the reasons above.

WHAT IT WRITES

`snap` writes one JSON file, refusing one that exists (a "before" is not to be
overwritten by an "after"): the window, the base URL's scheme and host, and per
route its name, path and query, kind, HTTP status, sha256 and size. It writes
nothing else and sends nothing but GETs. Standard library only, so it runs on
the host's own python3.

EXIT
  snap     0 every route answered 200 · 1 the file is written, and a route
           answered otherwise (named) · 2 refused, nothing asked or written
  compare  0 no route moved · 1 a past-window route's answer moved, or any
           route's status changed · 3 only live routes moved · 2 the two
           snapshots cannot be compared (another window or base, unreadable)
  list     the routes and their queries, asking nothing
"""
from __future__ import annotations

import argparse
import hashlib
import ipaddress
import json
import os
import sys
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import date, datetime, timedelta, timezone
from typing import Callable, Dict, List, Optional, Sequence, Tuple

SESSION_ENV = "KS_FINGERPRINT_SESSION"
# `web.routes.auth.SESSION_COOKIE`, spelled here because this runs on the host,
# where the application is not importable. A test holds the two equal.
SESSION_COOKIE = "dashboard_session"
# The 05:15 refresh rewrites the last 30 days; ten more for margin.
MIN_AGE_DAYS = 40
FORMAT = 1
TOOL = "api_fingerprint"
USER_AGENT = "ks-api-fingerprint/1"
TIMEOUT_S = 180.0
PACE_S = 0.5

ABSOLUTE = "absolute"
LIVE = "live"

EXIT_OK, EXIT_FOUND, EXIT_REFUSED, EXIT_LIVE_ONLY = 0, 1, 2, 3

Fetch = Callable[[str], Tuple[int, bytes]]


# ── the window ────────────────────────────────────────────────────────────────

def kyiv_today() -> date:
    try:
        from zoneinfo import ZoneInfo
        return datetime.now(ZoneInfo("Europe/Kyiv")).date()
    except Exception:  # no tz database on this host: a day of slack is harmless at 40
        return datetime.now(timezone.utc).date()


def default_window(today: date) -> Tuple[date, date]:
    """The last whole calendar month that ended at least MIN_AGE_DAYS ago."""
    latest = today - timedelta(days=MIN_AGE_DAYS)
    first_of_next = latest + timedelta(days=1)
    if first_of_next.day == 1:            # `latest` is a month's last day
        end = latest
    else:
        end = latest.replace(day=1) - timedelta(days=1)
    return end.replace(day=1), end


def check_window(start: date, end: date, today: date) -> Optional[str]:
    if start > end:
        return f"the window starts after it ends ({start} > {end})"
    if end > today - timedelta(days=MIN_AGE_DAYS):
        return (f"the window ends {end}, under {MIN_AGE_DAYS} days before {today}: the "
                f"05:15 refresh rewrites the last 30 days, so its answers move without "
                f"any deploy. End it on or before {today - timedelta(days=MIN_AGE_DAYS)}")
    return None


# ── the routes ────────────────────────────────────────────────────────────────

def routes(start: date, end: date) -> List[Dict[str, str]]:
    """Every route asked, in order: (name, path, query, kind). Retail unless
    the name says otherwise, the dashboard's own default."""
    w = {"start_date": start.isoformat(), "end_date": end.isoformat()}

    def r(name, path, kind=ABSOLUTE, **params):
        query = urllib.parse.urlencode(params)
        return {"name": name, "url": path + ("?" + query if query else ""), "kind": kind}

    month = {"year": str(end.year), "month": str(end.month)}
    return [
        # the dashboard
        r("summary", "/api/summary", **w),
        r("summary_all", "/api/summary", **w, sales_type="all"),
        r("revenue_trend", "/api/revenue/trend", **w),
        r("revenue_trend_all", "/api/revenue/trend", **w, sales_type="all"),
        r("sales_by_source", "/api/sales/by-source", **w),
        r("products_top", "/api/products/top", **w),
        r("products_performance", "/api/products/performance", **w),
        r("brands_analytics", "/api/brands/analytics", **w),
        r("promocodes_analytics", "/api/promocodes/analytics", **w),
        # Live: beside the window's new and returning customers it answers
        # all-time figures over every order in Silver, with no date bound
        # (`_INSIGHTS_CLV_SQL`, `_INSIGHTS_ALLTIME_SQL`): lifetime value,
        # purchase frequency and lifespan, the true repeat rate. Any order
        # that lands between two runs moves them.
        r("customers_insights", "/api/customers/insights", LIVE, **w),
        r("returns", "/api/returns", **w),
        # /traffic
        r("traffic_analytics", "/api/traffic/analytics", **w),
        r("traffic_organic", "/api/traffic/organic", **w),
        r("traffic_trend", "/api/traffic/trend", **w),
        r("traffic_utm_campaigns", "/api/traffic/utm-campaigns", **w),
        r("traffic_transactions", "/api/traffic/transactions", **w, limit="50"),
        r("traffic_roas", "/api/traffic/roas", **w),
        # /margin, live: the window's lines are costed at today's purchase
        # price — `LEFT JOIN {offer_stocks} os ON l.sku = os.sku` in every
        # one of the six — and chain 1's hourly stock step upserts
        # `purchased_price`, the sync that makes the stock pages live.
        r("margin_overview", "/api/margin/overview", LIVE, **w),
        r("margin_by_brand", "/api/margin/by-brand", LIVE, **w),
        r("margin_by_category", "/api/margin/by-category", LIVE, **w),
        r("margin_trend", "/api/margin/trend", LIVE, **w),
        r("margin_brand_category", "/api/margin/brand-category", LIVE, **w),
        r("margin_alerts", "/api/margin/alerts", LIVE, **w),
        # /reports and /marketing
        r("reports_summary", "/api/reports/summary", **w),
        r("reports_top_products", "/api/reports/top-products", **w),
        r("reports_all_products", "/api/reports/all-products", **w),
        r("reports_marketing_summary", "/api/reports/marketing-summary", **w),
        # /products
        r("intel_summary", "/api/products/intel/summary", **w),
        r("intel_pairs", "/api/products/intel/pairs", **w),
        r("intel_basket_distribution", "/api/products/intel/basket-distribution", **w),
        r("intel_category_combos", "/api/products/intel/category-combos", **w),
        r("intel_brand_affinity", "/api/products/intel/brand-affinity", **w),
        r("intel_momentum", "/api/products/intel/momentum", **w),
        # /expenses
        r("expenses_summary", "/api/expenses/summary", **w),
        r("expenses_profit", "/api/expenses/profit", **w),
        # The goals for the window's month take a month and are still live:
        # one signal is "the last three complete months" counted from today
        # (`_recent_three_month_average`), whose newest days the 05:15 refresh
        # rewrites, and another the ML forecast the Monday/Thursday training
        # stores.
        r("goals_smart", "/api/goals/smart", LIVE, **month),
        r("goals_forecast", "/api/goals/forecast", LIVE, **month),
        # No window in the API: asked in their most absolute form, and live.
        r("sms_audience", "/api/customers/sms-segments", LIVE,
          grouping="single", max_recency_days="3650", period_from=start.isoformat(),
          period_to=end.isoformat(), period_orders_min="1", holdout_pct="10",
          campaign="fingerprint"),
        r("cohort_retention", "/api/customers/cohort-retention", LIVE, months_back="12"),
        r("cohort_ltv", "/api/customers/cohort-ltv", LIVE, months_back="12"),
        r("purchase_timing", "/api/customers/purchase-timing", LIVE, months_back="12"),
        r("customers_at_risk", "/api/customers/at-risk", LIVE),
        # /inventory: the hourly stock sync moves every one of these.
        r("stocks_summary", "/api/stocks/summary", LIVE),
        r("stocks_trend", "/api/stocks/trend", LIVE, days="90"),
        r("stocks_average", "/api/stocks/average", LIVE),
        r("stocks_analysis", "/api/stocks/analysis", LIVE),
        r("stocks_skus", "/api/stocks/skus", LIVE),
        r("stocks_turnover", "/api/stocks/turnover", LIVE),
        r("stocks_brand_rotation", "/api/stocks/brand-rotation", LIVE),
        r("stocks_actions", "/api/stocks/actions", LIVE),
        r("stocks_alerts", "/api/stocks/alerts", LIVE),
        # The goal tables and the forecast: rewritten by the Monday job and
        # the Monday/Thursday training, and the goals by a person on /goals.
        r("goals", "/api/goals", LIVE),
        r("goals_seasonality", "/api/goals/seasonality", LIVE),
        r("goals_growth", "/api/goals/growth", LIVE),
        r("goals_weekly_patterns", "/api/goals/weekly-patterns", LIVE),
        r("revenue_forecast", "/api/revenue/forecast", LIVE),
        r("goals_history_weekly", "/api/goals/history", LIVE, period_type="weekly",
          weeks_back="12"),
        # /expenses' list and the campaigns on /sms: whatever was typed or sent.
        r("expenses_list", "/api/expenses", LIVE, limit="200"),
        r("sms_campaigns", "/api/customers/sms-campaigns", LIVE),
        # The header's filter lookups on every page: the catalogue sync and
        # new orders move them.
        r("categories", "/api/categories", LIVE),
        r("brands", "/api/brands", LIVE),
        r("promocodes", "/api/promocodes", LIVE),
        r("expense_types", "/api/expense-types", LIVE),
    ]


# ── the transport ─────────────────────────────────────────────────────────────

def check_base(base: str) -> Optional[str]:
    """None when the session may be sent to `base`; otherwise why not, in
    words that never repeat anything but the scheme and the host."""
    parts = urllib.parse.urlsplit(base)
    if parts.scheme not in ("https", "http") or not parts.hostname:
        return "--base must be http(s)://host[:port]"
    if parts.username or parts.password:
        return "--base carries credentials; give the host alone"
    if parts.path not in ("", "/") or parts.query or parts.fragment:
        return "--base must be the origin alone, with no path or query"
    if parts.scheme == "http" and not _loopback(parts.hostname):
        return ("--base is plain http to a host that is not this machine: the session "
                "would cross the network in the clear. Use https")
    return None


def _loopback(host: str) -> bool:
    if host == "localhost":
        return True
    try:
        return ipaddress.ip_address(host).is_loopback
    except ValueError:
        return False


def check_session(value: Optional[str]) -> Optional[str]:
    """Why the value cannot be a cookie, never quoting it."""
    if not value:
        return (f"{SESSION_ENV} is not set: read the admin's {SESSION_COOKIE} cookie into "
                f"it without echoing it — `read -rs {SESSION_ENV} && export {SESSION_ENV}`")
    if any(ch in value for ch in "\r\n;, \t\"\\") or not value.isprintable():
        return (f"{SESSION_ENV} holds characters a cookie value cannot carry; paste the "
                f"value of {SESSION_COOKIE} alone")
    return None


def request_headers(session: str) -> Dict[str, str]:
    return {"Cookie": f"{SESSION_COOKIE}={session}",
            "Accept": "application/json",
            "Accept-Encoding": "identity",
            "User-Agent": USER_AGENT}


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    """A redirect is an answer to record, never a place to send the cookie."""

    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def urllib_fetch(base: str, headers: Dict[str, str], timeout: float = TIMEOUT_S) -> Fetch:
    opener = urllib.request.build_opener(_NoRedirect())
    origin = base.rstrip("/")

    def fetch(url: str) -> Tuple[int, bytes]:
        req = urllib.request.Request(origin + url, headers=headers, method="GET")
        try:
            with opener.open(req, timeout=timeout) as resp:
                return resp.status, resp.read()
        except urllib.error.HTTPError as exc:
            try:
                body = exc.read()
            except Exception:
                body = b""
            return exc.code, body
    return fetch


# ── snap ──────────────────────────────────────────────────────────────────────

def snapshot(fetch: Fetch, table: Sequence[Dict[str, str]], pace: float = 0.0) -> List[dict]:
    out = []
    for i, route in enumerate(table):
        if i and pace:
            time.sleep(pace)
        try:
            status, body = fetch(route["url"])
            error = None
        except Exception as exc:  # the class only: a message can carry anything
            status, body, error = 0, b"", type(exc).__name__
        row = {**route, "status": status, "sha256": hashlib.sha256(body).hexdigest(),
               "bytes": len(body)}
        if error:
            row["error"] = error
        out.append(row)
    return out


def _write_new(path: str, payload: dict) -> None:
    directory = os.path.dirname(os.path.abspath(path))
    fd, tmp = tempfile.mkstemp(prefix=".fingerprint.", dir=directory)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            json.dump(payload, fh, indent=2, sort_keys=True)
            fh.write("\n")
        # link() refuses an existing name where rename() would replace it.
        os.link(tmp, path)
    finally:
        os.unlink(tmp)


def _origin(base: str) -> str:
    parts = urllib.parse.urlsplit(base)
    return f"{parts.scheme}://{parts.netloc}"


def cmd_snap(args, environ, out, err, fetch_factory) -> int:
    real_today = kyiv_today()
    today = args.today or real_today
    start, end = default_window(today)
    if args.start or args.end:
        if not (args.start and args.end):
            print("refused: --start and --end go together", file=err)
            return EXIT_REFUSED
        start, end = args.start, args.end
    # --today picks the default window; it never licenses a recent one. The
    # window is judged against the earlier of the two days, so a --today in
    # the future cannot let through a window the 05:15 refresh still rewrites.
    why = check_window(start, end, min(today, real_today)) or check_base(args.base)
    if why:
        print(f"refused: {why}", file=err)
        return EXIT_REFUSED
    session = environ.get(SESSION_ENV)
    why = check_session(session)
    if why:
        print(f"refused: {why}", file=err)
        return EXIT_REFUSED
    if os.path.exists(args.out):
        print(f"refused: {args.out} exists; a snapshot is never written over", file=err)
        return EXIT_REFUSED
    if not os.path.isdir(os.path.dirname(os.path.abspath(args.out))):
        print(f"refused: no directory for {args.out}", file=err)
        return EXIT_REFUSED

    table = routes(start, end)
    fetch = fetch_factory(_origin(args.base), request_headers(session))
    session = None
    rows = snapshot(fetch, table, pace=args.pace)
    _write_new(args.out, {
        "tool": TOOL, "format": FORMAT,
        "taken_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "base": _origin(args.base),
        "window": {"start": start.isoformat(), "end": end.isoformat(),
                   "today": today.isoformat()},
        "routes": rows,
    })
    bad = [r for r in rows if r["status"] != 200]
    print(f"window {start} .. {end}: {len(rows)} routes, {len(rows) - len(bad)} answered 200",
          file=out)
    for r in bad:
        print(f"  {r['name']}: {r['status'] or r.get('error', 'no answer')}", file=out)
    print(f"written: {args.out}", file=out)
    return EXIT_FOUND if bad else EXIT_OK


# ── compare ───────────────────────────────────────────────────────────────────

def _load(path: str) -> dict:
    with open(path, encoding="utf-8") as fh:
        data = json.load(fh)
    if data.get("tool") != TOOL or data.get("format") != FORMAT:
        raise ValueError(f"{path} is not a snapshot this version writes")
    return data


def compare(before: dict, after: dict) -> Tuple[int, List[str]]:
    lines: List[str] = []
    wb, wa = before["window"], after["window"]
    if (wb["start"], wb["end"]) != (wa["start"], wa["end"]):
        return EXIT_REFUSED, [
            f"not comparable: windows {wb['start']}..{wb['end']} and {wa['start']}..{wa['end']}; "
            f"take both with the same --start and --end"]
    if before["base"] != after["base"]:
        return EXIT_REFUSED, [f"not comparable: {before['base']} and {after['base']}"]

    keyed_b = {(r["name"], r["url"]): r for r in before["routes"]}
    keyed_a = {(r["name"], r["url"]): r for r in after["routes"]}
    common = [k for k in keyed_b if k in keyed_a]
    if not common:
        return EXIT_REFUSED, ["not comparable: the two snapshots share no route"]

    found, live = [], []
    for key in common:
        b, a = keyed_b[key], keyed_a[key]
        if b["status"] != a["status"]:
            found.append(f"{key[0]}: status {b['status']} -> {a['status']}  ({key[1]})")
        elif b["sha256"] != a["sha256"]:
            line = f"{key[0]}: answer moved, {b['bytes']} -> {a['bytes']} bytes  ({key[1]})"
            (live if b["kind"] == LIVE else found).append(line)

    lines.append(f"window {wb['start']} .. {wb['end']}, {before['base']}: "
                 f"{len(common)} routes compared")
    for name in sorted(set(keyed_b) ^ set(keyed_a)):
        side = "before" if name in keyed_b else "after"
        lines.append(f"  not compared, only {side}: {name[0]}")
    if found:
        lines.append("MOVED (a past window, or a status):")
        lines += [f"  {line}" for line in found]
    if live:
        lines.append("moved, live (they answer something counted from today: an order, "
                     "the hourly stock sync or a training between the runs moves these, "
                     "so read what landed in between before calling it the deploy):")
        lines += [f"  {line}" for line in live]
    if not found and not live:
        lines.append("every route answered the same")
    code = EXIT_FOUND if found else EXIT_LIVE_ONLY if live else EXIT_OK
    return code, lines


def cmd_compare(args, out, err) -> int:
    try:
        before, after = _load(args.before), _load(args.after)
    except (OSError, ValueError, KeyError) as exc:
        print(f"not comparable: {exc}", file=err)
        return EXIT_REFUSED
    code, lines = compare(before, after)
    for line in lines:
        print(line, file=out if code != EXIT_REFUSED else err)
    return code


def cmd_list(args, out) -> int:
    today = args.today or kyiv_today()
    start, end = (args.start, args.end) if (args.start and args.end) else default_window(today)
    print(f"window {start} .. {end}", file=out)
    for r in routes(start, end):
        print(f"  {r['name']:<28} {r['kind']:<8} {r['url']}", file=out)
    return EXIT_OK


# ── main ──────────────────────────────────────────────────────────────────────

def _day(text: str) -> date:
    return date.fromisoformat(text)


def parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="api_fingerprint.py", description=(
        "sha256 of fixed past-window API answers, before and after a deploy"))
    sub = p.add_subparsers(dest="command", required=True)
    for name in ("snap", "list"):
        s = sub.add_parser(name)
        s.add_argument("--start", type=_day, help="window start, YYYY-MM-DD (with --end)")
        s.add_argument("--end", type=_day, help=f"window end, at least {MIN_AGE_DAYS} days ago")
        s.add_argument("--today", type=_day, help=(
            "the day the default window is counted from; a window must still end "
            f"{MIN_AGE_DAYS} days before the real one"))
        if name == "snap":
            s.add_argument("--base", required=True, help="https://host, the origin alone")
            s.add_argument("--out", required=True, help="the snapshot file; never overwritten")
            s.add_argument("--pace", type=float, default=PACE_S,
                           help="seconds between two requests")
    c = sub.add_parser("compare")
    c.add_argument("before")
    c.add_argument("after")
    return p


def main(argv: Optional[Sequence[str]] = None, *, environ=None, stdout=None, stderr=None,
         fetch_factory: Callable[[str, Dict[str, str]], Fetch] = urllib_fetch) -> int:
    out, err = stdout or sys.stdout, stderr or sys.stderr
    environ = os.environ if environ is None else environ
    args = parser().parse_args(argv)
    if args.command == "snap":
        return cmd_snap(args, environ, out, err, fetch_factory)
    if args.command == "compare":
        return cmd_compare(args, out, err)
    return cmd_list(args, out)


if __name__ == "__main__":
    sys.exit(main())
