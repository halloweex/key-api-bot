"""What the dashboard sends to `GET /api/revenue/trend`, read from its source.

The Rust trend is held to Python byte for byte on the matrix of
`tests/integration/test_rust_pilot_trend.py`, and nothing the page itself
sends may be handed to Python (plan, revision 3) — a line-level filter
excepted, which the pilot leaves to Python on purpose. So what the page sends
has to follow the frontend, never be typed in beside it: when the page starts
sending a parameter, the harness must start sending it too, or the port is
proven on requests nobody makes.

Each reader takes one TypeScript file and the simplest constructs in it, and
raises `FrontendShapeError` when the construct it expects is not there. A
refactor that moves the code therefore fails the drift test
(`tests/unit/test_frontend_contract.py`) instead of quietly emptying the
contract.
"""
from __future__ import annotations

import re
from datetime import date
from pathlib import Path
from typing import FrozenSet, List, Optional, Tuple
from urllib.parse import quote_plus

SRC = Path(__file__).resolve().parents[1] / "web" / "frontend" / "src"

# The page the trend is drawn on, and the hook's default comparison
# (`useRevenueTrend()`, as `MilestoneProgress` calls it).
DASHBOARD_PATH = "/"
DEFAULT_COMPARE = "previous_period"


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


def _code(text: str) -> str:
    """`text` without its `//` comments, whitespace runs folded to one space."""
    return re.sub(r"\s+", " ", re.sub(r"//[^\n]*", "", text)).strip()


def _union(name: str, rel: str = "types/filters.ts") -> Tuple[str, ...]:
    m = re.search(rf"export type {name}\s*=\s*([^\n]+)", _read(rel))
    if not m:
        raise FrontendShapeError(f"{rel}: no `export type {name} = …`")
    return tuple(re.findall(r"'([^']+)'", m.group(1)))


def periods() -> Tuple[str, ...]:
    """`Period`, less `custom`, which sends two dates instead."""
    return tuple(p for p in _union("Period") if p != "custom")


def sales_types() -> Tuple[str, ...]:
    return _union("SalesType")


def compare_types() -> Tuple[str, ...]:
    """`CompareType`: what the chart's comparison switch offers."""
    return _union("CompareType", "types/api.ts")


def sources() -> Tuple[int, ...]:
    """The source ids the header offers: `SourceFilter`'s `SOURCES`. The
    other three filters offer what `/api/categories`, `/api/brands` and
    `/api/promocodes` answer, so a harness reads those off the app."""
    m = re.search(r"const SOURCES\s*=\s*\[(.*?)\]", _read("components/SourceFilter.tsx"), re.S)
    ids = tuple(int(i) for i in re.findall(r"\bid:\s*(\d+)", m.group(1))) if m else ()
    if not ids:
        raise FrontendShapeError("components/SourceFilter.tsx: no `const SOURCES = [{ id: … }]`")
    return ids


def chart_default_compare() -> str:
    """The comparison `RevenueTrendChart` opens on."""
    m = re.search(r"useState<CompareType>\('([a-z_]+)'\)", _read("components/RevenueTrendChart.tsx"))
    if not m:
        raise FrontendShapeError("RevenueTrendChart: no `useState<CompareType>('…')`")
    return m.group(1)


# `useQueryParams`, statement by statement: the period, then each filter the
# page applies, set and never appended or deleted. `trend_query` repeats it.
_QUERY_PARAMS = (
    "const applies = filtersForPath(path)",
    "const params = new URLSearchParams()",
    "setPeriodParams(params, period, startDate, endDate)",
    "if (applies.has('salesType')) params.set('sales_type', salesType)",
    "if (sourceId && applies.has('sourceId')) params.set('source_id', String(sourceId))",
    "if (categoryId && applies.has('categoryId')) params.set('category_id', String(categoryId))",
    "if (brand && applies.has('brand')) params.set('brand', brand)",
    "if (promocode && applies.has('promocode')) params.set('promocode', promocode)",
    "return params.toString()",
)
_SET_PERIOD = (
    "if (period !== 'custom') { params.set('period', period) } "
    "else if (startDate && endDate) { params.set('start_date', startDate) "
    "params.set('end_date', endDate) }"
)
# `useRevenueTrend`: what it adds to that string, in order.
_REVENUE_TREND = (
    "const queryParams = useQueryParams()",
    "const { period, sourceId, categoryId, brand, promocode } = useFilterStore()",
    "let fullParams = compareType !== 'previous_period' "
    "? `${queryParams}&compare_type=${compareType}` : queryParams",
    "const { endDate } = useFilterStore()",
    "const hasFutureDates = period === 'custom' && endDate && "
    "endDate > new Date().toISOString().split('T')[0]",
    "const isCurrentPeriod = period === 'month' || period === 'week'",
    "const wantsForecast = (isCurrentPeriod || hasFutureDates) && !sourceId && "
    "!categoryId && !brand && !promocode",
    "if (wantsForecast) { fullParams += '&include_forecast=true' }",
    "if (period === 'all_time') { fullParams += '&granularity=monthly' }",
    "queryFn: () => api.getRevenueTrend(fullParams)",
)


def _in_order(code: str, statements: Tuple[str, ...], where: str) -> None:
    at = 0
    for statement in statements:
        found = code.find(statement, at)
        if found < 0:
            raise FrontendShapeError(f"{where}: `{statement}` is not where this reader "
                                     "expects it; read the code again")
        at = found + len(statement)


def check_shapes() -> None:
    """Raise unless the page builds the trend's query as `trend_query` does."""
    store = _read("store/filterStore.ts")
    query_params = _code(_body(store, r"export const useQueryParams\s*=\s*\(\)\s*=>\s*\{"))
    if re.search(r"params\.(append|delete)\(", query_params):
        raise FrontendShapeError("useQueryParams appends or deletes: read it again")
    _in_order(query_params, _QUERY_PARAMS, "useQueryParams")
    if _code(_body(store, r"export function setPeriodParams\(")) != _SET_PERIOD:
        raise FrontendShapeError("setPeriodParams is not built as read; read it again")

    hook = _code(_body(_read("hooks/useApi.ts"),
                       r"export function useRevenueTrend\(compareType: string = 'previous_period'\)"))
    _in_order(hook, _REVENUE_TREND, "useRevenueTrend")
    if hook.count("fullParams +=") != 2 or hook.count("fullParams =") != 1:
        raise FrontendShapeError("useRevenueTrend changes fullParams elsewhere: read it again")

    client = _code(_read("api/client.ts"))
    for statement in ("fetchApi<RevenueTrendResponse>('/revenue/trend', params, options)",
                      "const url = queryParams ? `${API_BASE}${endpoint}?${queryParams}`"):
        if statement not in client:
            raise FrontendShapeError(f"api/client.ts: no `{statement}`")


def dashboard_filters() -> FrozenSet[str]:
    """`filtersForPath('/')`: every header filter, unless `/` became a
    /traffic section."""
    text = _read("utils/pageFilters.ts")
    all_m = re.search(r"const ALL[^=]*=\s*\[([^\]]*)\]", text)
    body = _code(_body(text, r"export function filtersForPath\(path: string\)[^{]*\{"))
    if not all_m or body != ("const section = trafficTabFor(path) "
                             "return new Set(section ? TRAFFIC[section] : ALL)"):
        raise FrontendShapeError("utils/pageFilters.ts: filtersForPath is not ALL or a "
                                 "/traffic section; read it again")
    paths = re.findall(r"\{ tab: '[a-z]+', path: '([^']+)' \}", _read("utils/trafficTabs.ts"))
    if not paths or DASHBOARD_PATH in paths:
        raise FrontendShapeError("utils/trafficTabs.ts: no section paths, or `/` is one")
    return frozenset(re.findall(r"'([A-Za-z]+)'", all_m.group(1)))


def _form(value: str) -> str:
    """`URLSearchParams`' serializer: letters, digits and `*-._` as they
    are, a space as `+`, everything else UTF-8 percent-encoded."""
    return quote_plus(value, safe="*").replace("~", "%7E")


def trend_query(*, period: str, sales_type: str, today_utc: date,
                compare_type: str = DEFAULT_COMPARE,
                start_date: Optional[str] = None, end_date: Optional[str] = None,
                source_id: Optional[int] = None, category_id: Optional[int] = None,
                brand: Optional[str] = None, promocode: Optional[str] = None) -> str:
    """The query string `useRevenueTrend(compare_type)` sends from `/`.

    `today_utc` is the browser's `new Date().toISOString()` date: the hook
    asks for a forecast of a custom range by the UTC day, not Kyiv's.
    """
    applies = dashboard_filters()
    pairs: List[Tuple[str, str]] = []
    if period != "custom":
        pairs.append(("period", period))
    elif start_date and end_date:
        pairs += [("start_date", start_date), ("end_date", end_date)]
    if "salesType" in applies:
        pairs.append(("sales_type", sales_type))
    for name, param, value in (("sourceId", "source_id", source_id),
                               ("categoryId", "category_id", category_id),
                               ("brand", "brand", brand),
                               ("promocode", "promocode", promocode)):
        if value and name in applies:
            pairs.append((param, str(value)))
    query = "&".join(f"{_form(k)}={_form(v)}" for k, v in pairs)

    full = f"{query}&compare_type={compare_type}" if compare_type != "previous_period" else query
    future = period == "custom" and bool(end_date) and str(end_date) > today_utc.isoformat()
    if (period in ("month", "week") or future) and not (source_id or category_id or brand
                                                         or promocode):
        full += "&include_forecast=true"
    if period == "all_time":
        full += "&granularity=monthly"
    return full
