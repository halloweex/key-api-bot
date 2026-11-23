"""No answer under /api/ is kept by the browser.

WHAT HAPPENED

`location /api/` said `expires 60s` from the first nginx config until
2026-11-20, so every 200 under /api/ went out with `Cache-Control:
max-age=60`, and the frontend's `fetch` used the default cache mode. Found by
the review of #397: a promotion saved on /marketing invalidated the list, the
list was asked again, and the browser answered from its own copy of a minute
before. The new promotion was not on the page, the page's own cache then held
the old list for five minutes, and whoever saved it again had it twice. Every
tab had the same minute after every write; the calendar was only where it
was first seen.

What to reuse is the page's own cache's decision (TanStack Query): it knows
when a save has made an answer stale, and the browser cannot. So the browser
keeps nothing: nginx says `no-cache` on every location that answers /api/, and
the client asks with `cache: 'no-store'` (its half is a vitest,
`web/frontend/src/api/__tests__/requestDedup.test.ts`).

HOW IT IS CHECKED

There is no nginx here, so the configuration text is read, parsed into its
blocks rather than grepped (a `{` inside a quoted `log_format` would fool a
brace count). Which location answers a path is nginx's own rule, applied to a
set of paths under /api/: an exact match, else the longest prefix if it says
`^~`, else the first regex in the file that matches, else the longest prefix.
A regex location anchored at `^/api/` is checked whichever paths it takes.
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterator, Optional

import pytest

REPO = Path(__file__).resolve().parents[2]
NGINX = REPO / "nginx" / "nginx.conf"

# Paths the dashboard asks, the slow levers of the regex location among them.
PROBES = (
    "/api/promotions",
    "/api/promotions/12/versions",
    "/api/summary",
    "/api/revenue/trend",
    "/api/revenue/forecast",
    "/api/revenue/forecast/train",
    "/api/revenue/forecast/evaluate",
    "/api/stocks/analysis",
    "/api/goals/recalculate",
    "/api/sync/resync",
    "/api/customers/sms-segments/export/csv",
    "/api/me",
)


# ── the configuration, as nginx reads it ────────────────────────────────────


@dataclass
class Directive:
    name: str
    args: list[str]
    block: Optional[list["Directive"]] = None


@dataclass
class Location:
    modifier: str            # "", "=", "^~", "~" or "~*"
    pattern: str
    directives: list[Directive] = field(default_factory=list)

    @property
    def label(self) -> str:
        return f"location {self.modifier + ' ' if self.modifier else ''}{self.pattern}"

    def matches(self, uri: str) -> bool:
        flags = re.IGNORECASE if self.modifier == "~*" else 0
        return re.search(self.pattern, uri, flags) is not None


def _tokens(text: str) -> Iterator[str]:
    """Words, quoted strings, and `{`, `}`, `;`; comments dropped."""
    i, n = 0, len(text)
    while i < n:
        c = text[i]
        if c.isspace():
            i += 1
        elif c == "#":
            while i < n and text[i] != "\n":
                i += 1
        elif c in "{};":
            yield c
            i += 1
        elif c in "\"'":
            j = i + 1
            while j < n and text[j] != c:
                j += 2 if text[j] == "\\" else 1
            yield text[i:j + 1]
            i = j + 1
        else:
            j = i
            while j < n and not text[j].isspace() and text[j] not in "{};\"'":
                j += 1
            yield text[i:j]
            i = j


def _parse(tokens: Iterator[str]) -> list[Directive]:
    directives: list[Directive] = []
    words: list[str] = []
    for token in tokens:
        if token == ";":
            directives.append(Directive(words[0], words[1:]))
            words = []
        elif token == "{":
            directives.append(Directive(words[0], words[1:], _parse(tokens)))
            words = []
        elif token == "}":
            assert not words, f"`{' '.join(words)}` is not closed before `}}`"
            return directives
        else:
            words.append(token)
    assert not words, f"`{' '.join(words)}` is not closed at the end of the file"
    return directives


def _unquote(word: str) -> str:
    return word[1:-1] if len(word) > 1 and word[0] == word[-1] and word[0] in "\"'" else word


def _https_locations() -> list[Location]:
    assert NGINX.exists(), (
        f"{NGINX} is not here. The runtime image does not carry `nginx/`, so "
        f"`deploy/gate_with_stores.sh` has to mount it."
    )
    top = _parse(_tokens(NGINX.read_text(encoding="utf-8")))
    (http,) = [d for d in top if d.name == "http"]
    servers = [d for d in http.block or [] if d.name == "server"]
    https = [
        s for s in servers
        if any(d.name == "listen" and d.args[0].startswith("443") for d in s.block or [])
    ]
    assert len(https) == 1, f"expected one server on 443, found {len(https)}"
    locations = []
    for d in https[0].block or []:
        if d.name != "location":
            continue
        modifier, pattern = (d.args[0], d.args[1]) if len(d.args) == 2 else ("", d.args[0])
        locations.append(Location(modifier, _unquote(pattern), d.block or []))
    return locations


def _chosen(locations: list[Location], uri: str) -> Optional[Location]:
    """The location nginx gives `uri` to."""
    for loc in locations:
        if loc.modifier == "=" and loc.pattern == uri:
            return loc
    prefixes = [l for l in locations if l.modifier in ("", "^~") and uri.startswith(l.pattern)]
    longest = max(prefixes, key=lambda l: len(l.pattern), default=None)
    if longest is not None and longest.modifier == "^~":
        return longest
    for loc in locations:
        if loc.modifier in ("~", "~*") and loc.matches(uri):
            return loc
    return longest


def _api_locations() -> list[Location]:
    locations = _https_locations()
    found: list[Location] = []
    for loc in locations:
        anchored = loc.modifier in ("~", "~*") and loc.pattern.startswith("^/api/")
        prefix = loc.modifier in ("", "^~", "=") and loc.pattern.startswith("/api/")
        if anchored or prefix:
            found.append(loc)
    for uri in PROBES:
        loc = _chosen(locations, uri)
        assert loc is not None, f"no location answers {uri}"
        if loc not in found:
            found.append(loc)
    return found


def _what_the_browser_is_told(loc: Location) -> Optional[str]:
    """None when the answer may not be reused without asking; else why it may."""
    for d in loc.directives:
        if d.name == "proxy_cache" and d.args != ["off"]:
            return f"`proxy_cache {' '.join(d.args)}`: nginx keeps a copy itself"
    expires = [d.args for d in loc.directives if d.name == "expires"]
    cache_control = [
        _unquote(d.args[1]) for d in loc.directives
        if d.name == "add_header" and d.args and d.args[0].lower() == "cache-control"
    ]
    for args in expires:
        value = args[-1]
        if value != "epoch" and not value.startswith("-"):
            # A positive time, `max`, `@15h30m`, or `off` (and then nothing
            # is said, and a browser may guess).
            return f"`expires {' '.join(args)}`"
    for value in cache_control:
        if not re.search(r"\bno-(cache|store)\b", value):
            return f'`add_header Cache-Control "{value}"`'
    if not expires and not cache_control:
        return "nothing about caching, so a browser may guess"
    return None


# ── the checks ───────────────────────────────────────────────────────────────


def test_the_parse_finds_the_api_locations_at_all():
    """A guard on the guard: a check over no locations passes vacuously."""
    labels = [loc.label for loc in _api_locations()]
    assert "location /api/" in labels, labels
    # The slow levers' regex is the second one, and it serves GETs too.
    assert any(label.startswith("location ~ ^/api/") for label in labels), labels


@pytest.mark.parametrize("uri", PROBES)
def test_every_api_path_is_answered_by_an_api_location(uri):
    """Not by the catch-all, whose `no-cache` is the HTML shell's reason."""
    loc = _chosen(_https_locations(), uri)
    assert loc is not None and loc.pattern != "/", f"{uri} falls to {loc and loc.label}"


def test_no_api_location_lets_the_browser_keep_an_answer():
    kept = {
        loc.label: why
        for loc in _api_locations()
        if (why := _what_the_browser_is_told(loc)) is not None
    }
    assert not kept, (
        f"{kept}: a refetch the page asks for after a save would be answered "
        f"from the browser's copy, and the save would be missing from the "
        f"page. Use `expires -1;` (Cache-Control: no-cache), which keeps the "
        f"server-level security headers inherited."
    )


def test_the_reading_of_a_cache_header_is_not_vacuous():
    """The rule above, on the shapes it must refuse and the ones it must take."""
    def told(*lines: str) -> Optional[str]:
        block = _parse(_tokens("\n".join(lines) + "\n"))
        return _what_the_browser_is_told(Location("", "/api/", block))

    assert told("expires 60s;") == "`expires 60s`"
    assert told("expires max;") == "`expires max`"
    assert told("expires off;") == "`expires off`"
    assert told('add_header Cache-Control "public, max-age=60" always;') is not None
    assert told("proxy_cache api;", "expires -1;") is not None
    assert told("proxy_pass http://$web_upstream:8080;") is not None
    assert told("expires -1;") is None
    assert told("expires epoch;") is None
    assert told('add_header Cache-Control "no-store" always;') is None
