"""What the dashboard's frontend can send to an endpoint, read from its source.

The Rust ports are held to Python byte for byte on exactly this input — the
contract — and on status alone on anything else. So the contract has to follow
the frontend, never be typed in beside it: when a page starts sending a
parameter, the harness must start sending it too, or a port is proven on
requests nobody makes.

Each reader takes one TypeScript file and the simplest constructs in it, and
raises `FrontendShapeError` when the construct it expects is not there. A
refactor that moves the code therefore fails the drift test
(`tests/unit/test_frontend_contract.py`) instead of quietly emptying the
contract.
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, FrozenSet, List, Optional, Sequence, Tuple

SRC = Path(__file__).resolve().parents[1] / "web" / "frontend" / "src"

# The filter store's names for the four header filters, and what they send.
FILTER_PARAMS = {
    "sourceId": "source_id",
    "categoryId": "category_id",
    "brand": "brand",
    "promocode": "promocode",
}


class FrontendShapeError(AssertionError):
    """The frontend's code is not shaped as this reader expects."""


def _read(rel: str) -> str:
    return (SRC / rel).read_text(encoding="utf-8")


def _body(text: str, opener: str) -> str:
    """The braces-matched body after the first match of `opener`."""
    m = re.search(opener, text)
    if not m:
        raise FrontendShapeError(f"no match for {opener!r}")
    i = text.index("{", m.end() - 1)
    depth = 0
    for j in range(i, len(text)):
        depth += {"{": 1, "}": -1}.get(text[j], 0)
        if depth == 0:
            return text[i + 1:j]
    raise FrontendShapeError(f"{opener!r} is never closed")


def _union(name: str) -> Tuple[str, ...]:
    m = re.search(rf"export type {name}\s*=\s*([^\n]+)", _read("types/filters.ts"))
    if not m:
        raise FrontendShapeError(f"types/filters.ts: no `export type {name} = …`")
    return tuple(re.findall(r"'([^']+)'", m.group(1)))


def periods() -> Tuple[str, ...]:
    """`Period`, less `custom`, which sends two dates instead."""
    return tuple(p for p in _union("Period") if p != "custom")


def sales_types() -> Tuple[str, ...]:
    return _union("SalesType")


def sources() -> Tuple[int, ...]:
    """The source ids the header offers: `SourceFilter`'s `SOURCES`. The
    other three filters offer what `/api/categories`, `/api/brands` and
    `/api/promocodes` answer, so a harness reads those off the app."""
    m = re.search(r"const SOURCES\s*=\s*\[(.*?)\]", _read("components/SourceFilter.tsx"), re.S)
    ids = tuple(int(i) for i in re.findall(r"\bid:\s*(\d+)", m.group(1))) if m else ()
    if not ids:
        raise FrontendShapeError("components/SourceFilter.tsx: no `const SOURCES = [{ id: … }]`")
    return ids


def global_params() -> Tuple[str, ...]:
    """Every name `useQueryParams` sets, in order. It must only `set`."""
    body = _body(_read("store/filterStore.ts"), r"export const useQueryParams\s*=\s*\(\)\s*=>\s*\{")
    if re.search(r"params\.(append|delete)\(", body):
        raise FrontendShapeError("useQueryParams appends or deletes: read it again")
    names = tuple(re.findall(r"params\.set\('([a-z_]+)'", body))
    if not names:
        raise FrontendShapeError("useQueryParams sets nothing this reader can see")
    return names


def filters_for_path(path: str) -> FrozenSet[str]:
    """`utils/pageFilters.filtersForPath(path)`: the header filters a page sends."""
    text = _read("utils/pageFilters.ts")
    all_m = re.search(r"const ALL[^=]*=\s*\[([^\]]*)\]", text)
    by_m = re.search(r"const BY_PATH[^=]*=\s*\{([^}]*)\}", text)
    if not (all_m and by_m):
        raise FrontendShapeError("utils/pageFilters.ts: no ALL or BY_PATH")
    by_path = {
        p: tuple(re.findall(r"'([A-Za-z]+)'", names))
        for p, names in re.findall(r"'([^']+)':\s*\[([^\]]*)\]", by_m.group(1))
    }
    return frozenset(by_path.get(path, tuple(re.findall(r"'([A-Za-z]+)'", all_m.group(1)))))


def hook_calls(hooks: Tuple[str, ...]) -> Dict[str, Tuple[str, Tuple[str, ...], Tuple[str, ...]]]:
    """For each hook in `hooks/useApi.ts`, the one `api.<fn>` it calls, the
    parameters it sets on `useQueryParams()`'s string, and which of those it
    sets unconditionally.

    Two shapes are read, and nothing else: `api.fn(queryParams)`, which adds
    nothing, and `const W = new URLSearchParams(queryParams)`, then
    `W.set('<name>', …)` — under an `if (…)` on its line, or not — then
    `const V = W.toString()` and `api.fn(V)`.
    """
    text = _read("hooks/useApi.ts")
    calls = {}
    for hook in hooks:
        body = _body(text, rf"export function {hook}\([^)]*\)\s*\{{")
        if "const queryParams = useQueryParams()" not in body:
            raise FrontendShapeError(f"{hook} does not build its query with useQueryParams()")
        found = re.findall(r"api\.(\w+)\(([^)]*)\)", body)
        if len(found) != 1:
            raise FrontendShapeError(f"{hook} calls {found}, not one api.<fn>(…)")
        fn, arg = found[0][0], found[0][1].strip()
        extras: Tuple[str, ...] = ()
        always: Tuple[str, ...] = ()
        if arg != "queryParams":
            built = re.search(rf"const {re.escape(arg)} = (\w+)\.toString\(\)", body)
            source = built and re.search(
                rf"const {built.group(1)} = new URLSearchParams\(queryParams\)", body)
            if not (built and source):
                raise FrontendShapeError(
                    f"{hook} passes {arg!r}, not queryParams nor a URLSearchParams built on it")
            params = built.group(1)
            if re.search(rf"{params}\.(append|delete)\(", body):
                raise FrontendShapeError(f"{hook} appends or deletes: read it again")
            sets = re.findall(rf"^(.*?){params}\.set\('([a-z_]+)'", body, re.M)
            extras = tuple(name for _, name in sets)
            always = tuple(name for before, name in sets if "if (" not in before)
        calls[hook] = (fn, extras, always)
    return calls


def client_path(fn: str) -> str:
    """`api/client.ts`: the path `fn` fetches, with the params as passed."""
    m = re.search(
        rf"{fn}:\s*\(params: string, options\?: FetchOptions\)\s*=>\s*"
        rf"fetchApi<[^>]+>\(\s*'([^']+)',\s*params,\s*options\s*\)",
        _read("api/client.ts"),
    )
    if not m:
        raise FrontendShapeError(f"api/client.ts: {fn} is not fetchApi('<path>', params, options)")
    return m.group(1)


@dataclass(frozen=True)
class Contract:
    page: str
    endpoints: Tuple[str, ...]
    periods: Tuple[str, ...]
    sales_types: Tuple[str, ...]
    params: Tuple[str, ...]
    # What a hook adds to the page's parameters, by endpoint, and of that
    # what it sets whatever the value.
    extras: Dict[str, Tuple[str, ...]] = field(default_factory=dict)
    always: Dict[str, Tuple[str, ...]] = field(default_factory=dict)


def contract(page: str, hooks: Tuple[str, ...]) -> Contract:
    """What `page`'s `hooks` can send: their endpoints, the parameters
    `useQueryParams` sets there once `filtersForPath` has dropped some, and
    what each hook adds."""
    applies = filters_for_path(page)
    sent = {FILTER_PARAMS[f] for f in applies if f in FILTER_PARAMS}
    params = tuple(
        name for name in global_params()
        if name not in FILTER_PARAMS.values() or name in sent
    )
    calls = list(hook_calls(hooks).values())
    endpoints = tuple("/api" + client_path(fn) for fn, _, _ in calls)
    extras = {path: added for path, (_, added, _) in zip(endpoints, calls) if added}
    always = {path: forced for path, (_, _, forced) in zip(endpoints, calls) if forced}
    return Contract(page, endpoints, periods(), sales_types(), params, extras, always)


def url_set(pairs: Sequence[Tuple[str, str]], name: str, value: str) -> List[Tuple[str, str]]:
    """`URLSearchParams.set`: the first pair named `name` takes the value
    where it stands and any later ones go; with none, it is appended."""
    out: List[Tuple[str, str]] = []
    done = False
    for key, old in pairs:
        if key != name:
            out.append((key, old))
        elif not done:
            out.append((key, value))
            done = True
    if not done:
        out.append((name, value))
    return out


def query(
    c: Contract,
    *,
    period: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    sales_type: str,
    source_id: Optional[int] = None,
    category_id: Optional[int] = None,
    brand: Optional[str] = None,
    promocode: Optional[str] = None,
    endpoint: Optional[str] = None,
    extra: Optional[Dict[str, object]] = None,
) -> str:
    """The query string `useQueryParams` builds for this state, on `c`'s page,
    with what the hook for `endpoint` sets on it (`extra`).

    Its branches, as written there: a period unless it is `custom`, else both
    dates or neither; the sales type always; a filter only when it is set —
    truthy, so `0` and `''` are never sent — and the page applies it. A
    hook's own parameter is `URLSearchParams.set` (`url_set`): when truthy,
    or always where the hook sets it unconditionally.
    """
    return encode(_pairs(c, period=period, start_date=start_date, end_date=end_date,
                         sales_type=sales_type, source_id=source_id,
                         category_id=category_id, brand=brand, promocode=promocode,
                         endpoint=endpoint, extra=extra))


def _pairs(c: Contract, *, period, start_date, end_date, sales_type, source_id,
           category_id, brand, promocode, endpoint, extra) -> List[Tuple[str, str]]:
    pairs: List[Tuple[str, str]] = []
    if period is not None and period != "custom":
        pairs.append(("period", period))
    elif start_date and end_date:
        pairs += [("start_date", start_date), ("end_date", end_date)]
    pairs.append(("sales_type", sales_type))
    for name, value in (("source_id", source_id), ("category_id", category_id),
                        ("brand", brand), ("promocode", promocode)):
        if value and name in c.params:
            pairs.append((name, str(value)))
    unknown = {name for name, _ in pairs} - set(c.params)
    if unknown:
        raise FrontendShapeError(f"{c.page} does not send {sorted(unknown)}")
    added = c.extras.get(endpoint or "", ())
    forced = c.always.get(endpoint or "", ())
    extra = extra or {}
    for name in set(extra) - set(added):
        raise FrontendShapeError(f"{endpoint} is never sent {name!r}")
    for name in added:
        value = extra.get(name)
        if name in forced:
            if value is None:
                raise FrontendShapeError(f"{endpoint} always sends {name!r}: give it a value")
            pairs = url_set(pairs, name, str(value))
        elif value:
            pairs = url_set(pairs, name, str(value))
    return pairs


def encode(pairs: Sequence[Tuple[str, str]]) -> str:
    """`URLSearchParams.toString()`: WHATWG's urlencoded serializer. A space
    is `+`; every byte but ASCII letters, digits and `*-._` is
    percent-encoded, upper case. Not `urllib.parse.urlencode`, which keeps
    `~` and encodes `*`."""
    def one(text: str) -> str:
        out = []
        for byte in text.encode("utf-8"):
            char = chr(byte)
            if char.isascii() and (char.isalnum() or char in "*-._"):
                out.append(char)
            elif byte == 0x20:
                out.append("+")
            else:
                out.append(f"%{byte:02X}")
        return "".join(out)
    return "&".join(f"{one(k)}={one(v)}" for k, v in pairs)


MARGIN_HOOKS = (
    "useMarginOverview", "useMarginByBrand", "useMarginByCategory",
    "useMarginTrend", "useMarginBrandCategory", "useMarginAlerts",
)


def margin() -> Contract:
    return contract("/margin", MARGIN_HOOKS)


PRODUCTS_HOOKS = (
    "useBasketSummary", "useProductPairs", "useBasketDistribution",
    "useCategoryCombos", "useBrandAffinity", "useProductMomentum",
)


def products() -> Contract:
    return contract("/products", PRODUCTS_HOOKS)


REPORTS_HOOKS = ("useReportSummary", "useReportTopProducts", "useReportAllProducts")


def reports() -> Contract:
    return contract("/reports", REPORTS_HOOKS)


def marketing_page() -> Contract:
    return contract("/marketing", ("useMarketingReport",))


def _constant(rel: str, name: str) -> str:
    m = re.search(rf"const {name}\s*=\s*\[(.*?)\]\s*as const", _read(rel), re.S)
    if not m:
        raise FrontendShapeError(f"{rel}: no `const {name} = [ … ] as const`")
    return m.group(1)


def report_sources() -> Tuple[int, ...]:
    """The source chips of /reports' top products: `SOURCE_FILTERS`' ids,
    less `null` (all sources, which sends nothing)."""
    ids = tuple(int(i) for i in re.findall(
        r"\bid:\s*(\d+)", _constant("components/TopProductsTab.tsx", "SOURCE_FILTERS")))
    if not ids:
        raise FrontendShapeError("TopProductsTab.tsx: SOURCE_FILTERS has no ids")
    return ids


def report_limits() -> Tuple[int, ...]:
    """`LIMIT_OPTIONS`; 0 is "all", which asks /all-products instead."""
    return tuple(int(n) for n in re.findall(
        r"\d+", _constant("components/TopProductsTab.tsx", "LIMIT_OPTIONS")))


def _require(rel: str, *patterns: str) -> str:
    """Each regex must match `rel`, or its shape changed under the reader."""
    text = _read(rel)
    for pattern in patterns:
        if not re.search(pattern, text, re.S):
            raise FrontendShapeError(f"{rel}: no match for {pattern!r}; read it again")
    return text


def download_csv_path() -> str:
    """`useDownloadCsv`: the page's parameters, `type` set on them, then each
    of the caller's `extra` set — and the path it fetches. The two callers'
    extras are read too: the summary sends none, the top products their
    source chip when one is picked and the limit, 5000 for "all"."""
    text = _require(
        "components/useDownloadCsv.ts",
        r"const params = new URLSearchParams\(queryParams\)",
        r"params\.set\('type', type\)",
        r"const extraParams = new URLSearchParams\(extra\)\s*"
        r"extraParams\.forEach\(\(v, k\) => params\.set\(k, v\)\)",
    )
    _require("components/SummaryTab.tsx", r"downloadCsv\('summary'\)")
    _require(
        "components/TopProductsTab.tsx",
        r"const extra = new URLSearchParams\(\)\s*"
        r"if \(sourceFilter\) extra\.set\('source_id', String\(sourceFilter\)\)\s*"
        r"extra\.set\('limit', isAll \? '5000' : String\(limit\)\)\s*"
        r"downloadCsv\('top_products', extra\.toString\(\)\)",
        r"useReportTopProducts\(sourceFilter, isAll \? 10 : limit\)",
    )
    m = re.search(r"exportCsvFromUrl\(`(/api/[^?`]+)\?\$\{params\.toString\(\)\}`", text)
    if not m:
        raise FrontendShapeError("useDownloadCsv.ts: the URL is not `/api/…?${params.toString()}`")
    return m.group(1)


def download_csv_query(c: Contract, type_: str, *, chip: Optional[int] = None,
                       limit: Optional[int] = None, **state) -> str:
    """The query `useDownloadCsv(type_, extra)` sends from `c`'s page:
    `state` is the page's (the header's `source_id` among it), `chip` the
    top products' source chip, which replaces it where it stands."""
    pairs = url_set(_pairs(c, endpoint=None, extra=None, **{
        **dict(period=None, start_date=None, end_date=None, source_id=None,
               category_id=None, brand=None, promocode=None), **state}), "type", type_)
    if type_ == "top_products":
        if chip:
            pairs = url_set(pairs, "source_id", str(chip))
        pairs = url_set(pairs, "limit", str(limit))
    return encode(pairs)


def marketing_csv_path() -> str:
    """`MonthlyReport`'s CSV link, built by hand rather than by
    `useQueryParams`: the sales type first, then a period or both dates, then
    the category and the brand when set — no source, no promocode."""
    text = _require(
        "components/MonthlyReport.tsx",
        r"const params = new URLSearchParams\(\{ sales_type: salesType \}\)\s*"
        r"if \(period !== 'custom'\) \{\s*params\.set\('period', period\)\s*"
        r"\} else if \(startDate && endDate\) \{\s*params\.set\('start_date', startDate\)\s*"
        r"params\.set\('end_date', endDate\)\s*\}\s*"
        r"if \(categoryId\) params\.set\('category_id', String\(categoryId\)\)\s*"
        r"if \(brand\) params\.set\('brand', brand\)",
    )
    m = re.search(r"exportCsvFromUrl\(\s*`(/api/[^?`]+)\?\$\{params\}`", text)
    if not m:
        raise FrontendShapeError("MonthlyReport.tsx: the CSV URL is not `/api/…?${params}`")
    return m.group(1)


def marketing_csv_query(*, sales_type: str, period: Optional[str] = None,
                        start_date: Optional[str] = None, end_date: Optional[str] = None,
                        category_id: Optional[int] = None, brand: Optional[str] = None) -> str:
    pairs: List[Tuple[str, str]] = [("sales_type", sales_type)]
    if period is not None and period != "custom":
        pairs = url_set(pairs, "period", period)
    elif start_date and end_date:
        pairs = url_set(url_set(pairs, "start_date", start_date), "end_date", end_date)
    if category_id:
        pairs = url_set(pairs, "category_id", str(category_id))
    if brand:
        pairs = url_set(pairs, "brand", brand)
    return encode(pairs)


# /expenses' reads. The summary and the profit analysis are asked by the hooks
# below, whose component — `ExpensesChart` — no page mounts today
# (`LazyExpensesChart` has no importer). Their contract is what the hooks
# send from any page but /traffic: the header's six filters, the widest.
EXPENSES_HOOKS = ("useExpenseSummary", "useProfitAnalysis")


def expenses() -> Contract:
    return contract("/", EXPENSES_HOOKS)


def expense_types_path() -> str:
    """`getExpenseTypes`: the dictionary, asked with no parameters at all."""
    text = _require(
        "api/client.ts",
        r"getExpenseTypes:\s*\(options\?: FetchOptions\)\s*=>\s*"
        r"fetchApi<ExpenseType\[\]>\(\s*'(/expense-types)',\s*undefined,\s*options\s*\)",
    )
    m = re.search(r"fetchApi<ExpenseType\[\]>\(\s*'(/expense-types)'", text)
    assert m
    return "/api" + m.group(1)


def expense_categories() -> Tuple[str, ...]:
    """`ManualExpensesTable`'s category filter: `CATEGORIES`, one option each."""
    text = _require("components/ManualExpensesTable.tsx",
                    r"const options = CATEGORIES\.map\(\(cat\) => \{.*?value: cat,")
    m = re.search(r"const CATEGORIES = \[([^\]]*)\]", text)
    if not m:
        raise FrontendShapeError("ManualExpensesTable.tsx: no `const CATEGORIES = [ … ]`")
    return tuple(re.findall(r"'([^']+)'", m.group(1)))


def manual_expenses_fetches() -> Tuple[Tuple[str, Tuple[Optional[str], ...], int], ...]:
    """The two places that GET `/api/expenses`, as (where, the categories it
    can send — None for none — its limit). Both send the filter store's
    period whenever it is set, `custom` included: `if (period)` is all they
    ask. The dashboard's table sends its category filter when one is picked;
    /traffic's recent entries always send `marketing`."""
    shape = (r"const params = new URLSearchParams\(\)\s*"
             r"if \(period\) params\.set\('period', period\)\s*{category}"
             r"params\.set\('limit', '(\d+)'\)\s*"
             r"const response = await fetch\(`/api/expenses\?\$\{{params\}}`\)")
    table = re.search(shape.format(
        category=r"if \(categoryFilter\) params\.set\('category', categoryFilter\)\s*"),
        _read("components/ManualExpensesTable.tsx"))
    roas = re.search(shape.format(category=r"params\.set\('category', '([a-z]+)'\)\s*"),
                     _read("components/ROASSection.tsx"))
    if not (table and roas):
        raise FrontendShapeError("ManualExpensesTable.tsx / ROASSection.tsx: the "
                                 "`/api/expenses` query is not built as read; read it again")
    return (("ManualExpensesTable", (None, *expense_categories()), int(table.group(1))),
            ("ROASSection", (roas.group(1),), int(roas.group(2))))


def manual_expenses_query(*, period: Optional[str], category: Optional[str], limit: int) -> str:
    pairs: List[Tuple[str, str]] = []
    if period:
        pairs.append(("period", period))
    if category:
        pairs.append(("category", category))
    pairs.append(("limit", str(limit)))
    return encode(pairs)


# The goal routes. `api/client.ts` builds their queries itself, as template
# literals with no encoding — not through `useQueryParams` — and only the
# sales type comes from the store. The milestone widget asks `/goals/smart`
# alone; the other hooks are exported and called by nothing today.
_GOAL_CALLS = {
    "/goals": "getGoals", "/goals/history": "getGoalHistory",
    "/goals/seasonality": "getSeasonality", "/goals/growth": "getGrowthMetrics",
    "/goals/weekly-patterns": "getWeeklyPatterns", "/goals/forecast": "getGoalForecast",
}


def goal_queries() -> Dict[str, Tuple[str, ...]]:
    """Each goal GET's path and the parameters its template sends, in order;
    `/goals/smart`'s `year` and `month` only when given."""
    text = _read("api/client.ts")
    found: Dict[str, Tuple[str, ...]] = {}
    for path, fn in _GOAL_CALLS.items():
        m = re.search(rf"{fn}: \([^)]*\)[^=]*=>\s*fetchApi<[^(]+>\(\s*'{re.escape(path)}',"
                      r"\s*`([^`]*)`", text)
        if not m:
            raise FrontendShapeError(f"api/client.ts: {fn} is not fetchApi('{path}', `…`)")
        pairs = m.group(1).split("&")
        if not all(re.fullmatch(r"[a-z_]+=\$\{\w+\}", p) for p in pairs):
            raise FrontendShapeError(f"api/client.ts: {fn}'s query is not name=${{value}} pairs")
        found["/api" + path] = tuple(p.split("=")[0] for p in pairs)
    _require("api/client.ts",
             r"let params = `sales_type=\$\{salesType\}`\s*"
             r"if \(year != null\) params \+= `&year=\$\{year\}`\s*"
             r"if \(month != null\) params \+= `&month=\$\{month\}`\s*"
             r"return fetchApi<SmartGoalsResponse>\('/goals/smart', params, options\)")
    found["/api/goals/smart"] = ("sales_type", "year", "month")
    # The forecast's hook never passes `recalculate`, so it sends the default.
    _require("hooks/useApi.ts", r"api\.getGoalForecast\(year, month, salesType\)")
    _require("api/client.ts", r"getGoalForecast: \([^)]*recalculate = false")
    return found


def goal_query(path: str, **values: object) -> str:
    """The template's string: `name=value` in its order, a value as JS
    interpolates it (a boolean as `true`/`false`), nothing encoded; a
    smart goal's year and month left out when None."""
    def text(v: object) -> str:
        return ("true" if v else "false") if isinstance(v, bool) else str(v)

    names = goal_queries()[path]
    return "&".join(f"{n}={text(values[n])}" for n in names
                    if not (path == "/api/goals/smart" and n in ("year", "month")
                            and values.get(n) is None))


# ─── /inventory ──────────────────────────────────────────────────────────────

# The inventory GETs the client sends with no query at all.
_BARE_STOCK_CALLS = {
    "/stocks/analysis": "getInventoryAnalysis", "/stocks/brand-rotation": "getBrandRotation",
    "/stocks/skus": "getSkus", "/stocks/actions": "getStockActions",
    "/stocks/alerts": "getRestockAlerts",
}
# The turnover's URLSearchParams, in the order `getInventoryTurnover` sets them.
TURNOVER_PARAMS = ("days", "lead_time", "safety_multiplier", "buffer_days",
                   "max_acceptable_days")


def js_number(x: float) -> str:
    """`String(x)` in JavaScript: an integral number without a fraction (`1`,
    not `1.0`), otherwise the shortest round trip, as `repr` writes it in
    the range the page offers."""
    return str(int(x)) if float(x).is_integer() else repr(float(x))


def inventory_stock_summary_limit() -> int:
    """`useStockSummary(n)` in the stock cards: the one `limit` sent."""
    _require("api/client.ts",
             r"getStockSummary: \(limit = 20, options\?: FetchOptions\) =>\s*"
             r"fetchApi<StockSummaryResponse>\('/stocks/summary', `limit=\$\{limit\}`, options\)")
    m = re.search(r"useStockSummary\((\d+)\)", _read("components/StockSummaryChart.tsx"))
    if not m:
        raise FrontendShapeError("StockSummaryChart.tsx: no useStockSummary(n)")
    return int(m.group(1))


def inventory_trend_options() -> Tuple[Tuple[int, ...], Tuple[str, ...]]:
    """The trend chart's period buttons and its two granularities."""
    _require("api/client.ts",
             r"fetchApi<InventoryTrendResponse>\('/stocks/trend', "
             r"`days=\$\{days\}&granularity=\$\{granularity\}`, options\)")
    text = _require("components/InventoryTrendChart.tsx",
                    r"useInventoryTrend\(days, granularity\)")
    days = tuple(int(d) for d in re.findall(r"setDays\((\d+)\)", text))
    granularities = tuple(re.findall(r"setGranularity\('(\w+)'\)", text))
    if not days or not granularities:
        raise FrontendShapeError("InventoryTrendChart.tsx: no period or granularity buttons")
    return days, granularities


def inventory_turnover_options() -> Dict[str, Tuple[float, ...]]:
    """What the turnover card can send: its period buttons, and each setting's
    `min` and `max` as the inputs enforce them (the multiplier is the one
    the page does not round to an integer)."""
    m = re.search(r"getInventoryTurnover: \(params: \{(.*?)"
                  r"return fetchApi<InventoryTurnoverResponse>\('/stocks/turnover', p\.toString\(\)",
                  _read("api/client.ts"), re.S)
    if not m:
        raise FrontendShapeError("api/client.ts: getInventoryTurnover is not URLSearchParams")
    sets = re.findall(r"p\.set\('(\w+)'", m.group(1))
    if tuple(sets) != TURNOVER_PARAMS:
        raise FrontendShapeError(f"api/client.ts: getInventoryTurnover sets {sets}")
    _require("api/client.ts", r"p\.set\('days', String\(params\.days \?\? 30\)\)")
    text = _require(
        "components/InventoryTurnoverChart.tsx",
        r"useInventoryTurnover\(\{\s*days, leadTime, safetyMultiplier: safetyMult, "
        r"bufferDays, maxAcceptableDays: maxDays,\s*\}\)",
        r"onChange\(step < 1 \? v : Math\.round\(v\)\)")
    periods = tuple(int(v) for v in re.findall(r"\{ value: (\d+), label: '\d+d' \}", text))
    bounds = re.findall(r"min=\{([\d.]+)\} max=\{([\d.]+)\}", text)
    if not periods or len(bounds) != 4:
        raise FrontendShapeError("InventoryTurnoverChart.tsx: periods or settings moved")
    out: Dict[str, Tuple[float, ...]] = {"days": tuple(float(p) for p in periods)}
    for name, (lo, hi) in zip(TURNOVER_PARAMS[1:], bounds):
        out[name] = (float(lo), float(hi))
    return out


def inventory_turnover_query(**values: float) -> str:
    """`URLSearchParams` as the client fills it: all five, in its order."""
    return "&".join(f"{n}={js_number(values[n])}" for n in TURNOVER_PARAMS)


def inventory_abc_classes() -> Tuple[str, ...]:
    """The ABC rows that expand into `/stocks/abc/{class}`."""
    _require("api/client.ts",
             r"fetchApi<ABCSkuItem\[\]>\(`/stocks/abc/\$\{abcClass\}`, undefined, options\)")
    m = re.search(r"\(\[('[A-Z]'(?:, '[A-Z]')*)\] as const\)\.map\(cls",
                  _read("components/InventoryTurnoverChart.tsx"))
    if not m:
        raise FrontendShapeError("InventoryTurnoverChart.tsx: no (['A', …] as const).map(cls")
    return tuple(re.findall(r"'([A-Z])'", m.group(1)))


def inventory_bare_paths() -> Tuple[str, ...]:
    """The stock GETs the client sends without a query."""
    text = _read("api/client.ts")
    for path, fn in _BARE_STOCK_CALLS.items():
        if not re.search(rf"{fn}: \(options\?: FetchOptions\) =>\s*"
                         rf"fetchApi<\w+(?:\[\])?>\('{re.escape(path)}', undefined, options\)",
                         text):
            raise FrontendShapeError(f"api/client.ts: {fn} is not fetchApi('{path}', undefined)")
    if "/stocks/average" in text:
        raise FrontendShapeError("api/client.ts: /stocks/average has a caller now")
    return tuple("/api" + p for p in _BARE_STOCK_CALLS)
