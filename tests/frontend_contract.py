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


def hook_calls(hooks: Tuple[str, ...]) -> Dict[str, Tuple[str, Tuple[str, ...]]]:
    """For each hook in `hooks/useApi.ts`, the one `api.<fn>` it calls and the
    parameters it adds to `useQueryParams()`'s string.

    Two shapes are read, and nothing else: `api.fn(queryParams)`, which adds
    nothing, and `const W = new URLSearchParams(queryParams)`, then
    `W.set('<name>', …)`, then `const V = W.toString()` and `api.fn(V)` —
    `useProductPairs`'s, whose `set` appends after the page's parameters.
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
            extras = tuple(re.findall(rf"{params}\.set\('([a-z_]+)'", body))
        calls[hook] = (fn, extras)
    return calls


def client_path(fn: str) -> str:
    """`api/client.ts`: the path `fn` fetches, with the params as passed."""
    m = re.search(
        rf"{fn}:\s*\(params: string, options\?: FetchOptions\)\s*=>\s*"
        rf"fetchApi<[^>]+>\('([^']+)', params, options\)",
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
    # What a hook adds to the page's parameters, by endpoint.
    extras: Dict[str, Tuple[str, ...]] = field(default_factory=dict)


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
    calls = hook_calls(hooks).values()
    endpoints = tuple("/api" + client_path(fn) for fn, _ in calls)
    extras = {path: added for path, (_, added) in zip(endpoints, calls) if added}
    return Contract(page, endpoints, periods(), sales_types(), params, extras)


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
    with what the hook for `endpoint` adds (`extra`) after it.

    Its branches, as written there: a period unless it is `custom`, else both
    dates or neither; the sales type always; a filter only when it is set —
    truthy, so `0` and `''` are never sent — and the page applies it. A
    hook's own parameter is set the same way: when truthy.
    """
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
    for name in set(extra or {}) - set(added):
        raise FrontendShapeError(f"{endpoint} is never sent {name!r}")
    pairs += [(name, str((extra or {})[name])) for name in added if (extra or {}).get(name)]
    return encode(pairs)


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
