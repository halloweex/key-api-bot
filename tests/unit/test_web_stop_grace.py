"""web gets long enough to shut down before Docker kills it — for an
ordinary request. Not for a slow one, and that is stated, not hidden.

Docker's default is 10 s, and web's shutdown waits on three things in turn:

1. uvicorn draining in-flight requests. For an ordinary path that is
   `DEFAULT_REQUEST_TIMEOUT` (30 s) — and `BaseHTTPMiddleware` lets a handler
   run on past its 504, so the bound is really the handler's own runtime;
2. `close_store()`, whose executor waits for the DuckDB work a cancelled
   scheduler job left in flight — `AsyncIOExecutor.shutdown` cancels and waits
   for nothing, so this is bounded by a whole warehouse refresh, ~30 s at
   production row counts on web's one CPU;
3. `close()`'s own checkpoint, ~7 s with the 900 MB of WAL the most a writer
   can accumulate under `wal_autocheckpoint='1GB'`.

About 67 s for an ordinary request, and 120 s covers it. **A request to one
of `SLOW_ENDPOINTS` is not covered**: they get `SLOW_ENDPOINT_TIMEOUT` (300 s),
uvicorn drains them too, and five write DuckDB (`SLOW_DUCKDB_WRITERS`, derived
from the code by `test_the_slow_writers_are_derived_from_the_code`). One
in flight at a stop keeps web draining past the grace, and Docker kills it.
Covering them would take ~340 s of grace, and with the same margin the
ordinary case gets, a deploy — pull, stop, migrate, a 180 s health gate —
would no longer fit the ssh step's 10-minute `command_timeout`. What makes the kill survivable is `open_file`: the
WAL it leaves is checkpointed whole at the next start (see
`test_duckdb_kill_guard.py`); the grace only makes the kill rarer. dockerd
logged three forced kills in thirty days at the 10 s default. Verified on a
throwaway compose project that the setting becomes the container's
StopTimeout and that both a deploy's recreate and `docker compose stop` wait
for it.
"""
from __future__ import annotations

import ast
import re
from pathlib import Path

import yaml

REPO = Path(__file__).resolve().parents[2]
COMPOSE = yaml.safe_load((REPO / "docker-compose.yml").read_text())
WORKFLOW = yaml.safe_load((REPO / ".github" / "workflows" / "deploy.yml").read_text())

WAREHOUSE_REFRESH_S = 30     # measured, production row counts, cpus: 1.0
CLOSING_CHECKPOINT_S = 7     # measured, 900 MB of WAL
SSH_COMMAND_TIMEOUT_S = 600  # appleboy/ssh-action's default at the pinned commit
HEALTH_GATE_S = 180          # the deploy's `timeout 180` wait for /api/health

# The slow paths a stop can be killed in the middle of, by name: adding one is
# a decision about this gap, and CLAUDE.md and docker-compose.yml name them.
KNOWN_SLOW_ENDPOINTS = {
    "/api/duckdb/resync",
    "/api/duckdb/refresh-statuses",
    "/api/goals/recalculate",
    "/api/stocks/analysis",
    "/api/revenue/forecast/train",
    "/api/revenue/forecast/evaluate",
    "/api/revenue/forecast/tune",
    "/api/traffic/reclassify",
}
# Of those, the ones that write DuckDB — a kill there leaves a WAL. Under
# today's flags: the recalculation and the training write the goal and
# forecast tables to DuckDB until chain 7b-3 (`KS_WRITE_FORECAST=postgres`)
# moves them, and the list keeps them while the code can. Derived, not kept:
# it once named three, and the review that found the other two found that
# nothing checked it (batch-E).
SLOW_DUCKDB_WRITERS = {
    "/api/duckdb/resync",
    "/api/duckdb/refresh-statuses",
    "/api/goals/recalculate",
    "/api/revenue/forecast/train",
    "/api/traffic/reclassify",
}


def _seconds(value: str) -> int:
    parts = re.fullmatch(r"(?:(\d+)h)?(?:(\d+)m)?(?:(\d+)s)?", str(value).strip())
    assert parts and any(parts.groups()), f"unparsable duration {value!r}"
    h, m, s = (int(g or 0) for g in parts.groups())
    return h * 3600 + m * 60 + s


def test_web_waits_for_an_ordinary_shutdown():
    from web.middleware import DEFAULT_REQUEST_TIMEOUT

    grace = _seconds(COMPOSE["services"]["web"]["stop_grace_period"])
    ordinary = DEFAULT_REQUEST_TIMEOUT + WAREHOUSE_REFRESH_S + CLOSING_CHECKPOINT_S
    assert grace >= 120 and grace >= 1.5 * ordinary, (grace, ordinary)


def test_a_slow_endpoint_outlasts_the_grace_and_is_named():
    """The arithmetic the earlier version of this file left out: a slow
    endpoint in flight at a stop is killed, and the kill is `open_file`'s
    to absorb. If the timeout or the grace ever change so that the grace
    covers it, this fails — and the docs saying it does not must change."""
    from web.middleware import SLOW_ENDPOINT_TIMEOUT, SLOW_ENDPOINTS

    grace = _seconds(COMPOSE["services"]["web"]["stop_grace_period"])
    slow = SLOW_ENDPOINT_TIMEOUT + WAREHOUSE_REFRESH_S + CLOSING_CHECKPOINT_S
    assert slow > grace, (slow, grace)
    assert set(SLOW_ENDPOINTS) == KNOWN_SLOW_ENDPOINTS
    assert SLOW_DUCKDB_WRITERS <= KNOWN_SLOW_ENDPOINTS
    # Covering them with the ordinary case's margin would not fit a deploy:
    # the stop, then the health gate, inside the ssh step's timeout.
    deploy = next(s for job in WORKFLOW["jobs"].values() for s in job.get("steps", [])
                  if "appleboy/ssh-action" in str(s.get("uses", "")))
    timeout = _seconds(deploy.get("with", {}).get("command_timeout", f"{SSH_COMMAND_TIMEOUT_S}s"))
    assert 1.5 * slow + HEALTH_GATE_S > timeout, "a grace covering them now fits: reconsider"


def test_a_deploy_still_fits_the_ssh_step():
    grace = _seconds(COMPOSE["services"]["web"]["stop_grace_period"])
    deploy = next(s for job in WORKFLOW["jobs"].values() for s in job.get("steps", [])
                  if "appleboy/ssh-action" in str(s.get("uses", "")))
    timeout = _seconds(deploy.get("with", {}).get("command_timeout", f"{SSH_COMMAND_TIMEOUT_S}s"))
    assert grace <= 240
    assert grace + HEALTH_GATE_S < timeout, (grace, timeout)


# ─── Which slow endpoints write DuckDB, read off the code ───────────────────
#
# The call graph the read-fallback walk builds over core/ and web/, from each
# slow endpoint's handler, to a method of the store that executes a statement
# writing the file — an INSERT, UPDATE, DELETE, CREATE, DROP, ALTER, TRUNCATE
# or COPY FROM, in its own body or in a module constant it names. Statically,
# so a write behind a flag that is off counts: that is what the list means.
# Two things are left out, each because it would put every endpoint in: the
# store's own open (`connect`, whose schema and migrations every first use
# runs), and calls resolved by name alone (`guessed`: `m.start()` on a regex
# match, taken for the scheduler's `start`).

_WRITES = re.compile(
    r"\b(?:INSERT\s+(?:OR\s+\w+\s+)?INTO|UPDATE\s+[\w{}.]+(?:\s+\w+)?\s+SET|DELETE\s+FROM"
    r"|CREATE\s+(?:OR\s+REPLACE\s+)?(?:TABLE|VIEW|INDEX|SEQUENCE)"
    r"|DROP\s+(?:TABLE|VIEW|INDEX|SEQUENCE)|ALTER\s+TABLE|TRUNCATE|COPY\s+\w+\s+FROM)\b")
_EXECUTES = {"execute", "executemany", "sql"}


def _text(node) -> "str | None":
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value
    if isinstance(node, ast.JoinedStr):
        return "".join(v.value if isinstance(v, ast.Constant) else "{}"
                       for v in node.values)
    return None


def derived_slow_duckdb_writers() -> set:
    from tests.unit.test_read_fallback_consumers import _own, _routes, build_graph
    from web.middleware import SLOW_ENDPOINTS

    graph = build_graph(("core", "web"))
    constants = {
        (rel, st.targets[0].id): _text(st.value)
        for rel, tree in graph.trees.items() for st in tree.body
        if isinstance(st, ast.Assign) and len(st.targets) == 1
        and isinstance(st.targets[0], ast.Name) and _text(st.value) is not None}

    def writes_duckdb(fn) -> bool:
        if (fn.rel, fn.cls) not in graph.store:
            return False
        body = fn.node.body
        docstring = (body[0].value if body and isinstance(body[0], ast.Expr)
                     and _text(body[0].value) is not None else None)
        texts, executes = [], False
        for node in _own(fn.node):
            if node is docstring:
                continue
            text = _text(node)
            if text is None and isinstance(node, ast.Name):
                text = constants.get((fn.rel, node.id))
            if text is not None:
                texts.append(text)
            executes |= (isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
                         and node.func.attr in _EXECUTES)
        return executes and any(_WRITES.search(t) for t in texts)

    opens = {fn for fn in graph.functions
             if (fn.rel, fn.cls) in graph.store and fn.name == "connect"}

    def reaches_a_write(start) -> bool:
        seen, stack = {start}, [start]
        while stack:
            fn = stack.pop()
            if writes_duckdb(fn):
                return True
            for call, targets in graph.sites.get(fn, ()):
                if call in graph.guessed:
                    continue
                for target in targets - opens - seen:
                    seen.add(target)
                    stack.append(target)
        return False

    handlers = {path: fn for fn, endpoints in _routes(graph).items()
                for _method, path in endpoints if path in SLOW_ENDPOINTS}
    assert set(handlers) == set(SLOW_ENDPOINTS), set(SLOW_ENDPOINTS) - set(handlers)
    return {path for path, fn in handlers.items() if reaches_a_write(fn)}


def test_the_slow_writers_are_derived_from_the_code():
    """Mutation: drop `/api/goals/recalculate` from `SLOW_DUCKDB_WRITERS`
    (the review's omission), or name `/api/stocks/analysis`, which only
    reads."""
    assert derived_slow_duckdb_writers() == SLOW_DUCKDB_WRITERS


def test_the_derivation_sees_a_write_through_a_constant_and_a_mixin():
    """Not blind: the recalculation writes through `_SEASONAL_UPSERT_SQL`, a
    module constant, from a mixin method — the shape the review's omission
    had. Mutation: read only the strings written in the function."""
    assert {"/api/goals/recalculate", "/api/revenue/forecast/train"} <= (
        derived_slow_duckdb_writers())


def test_compose_and_claude_md_name_every_writer():
    """The two places a person reads about the gap say what the list says."""
    compose = (REPO / "docker-compose.yml").read_text()
    comment = compose[:compose.index("stop_grace_period:")]
    comment = comment[comment.rindex("# How long a stop waits"):]
    claude = (REPO / ".claude" / "CLAUDE.md").read_text()
    for path in SLOW_DUCKDB_WRITERS:
        assert path.rsplit("/", 1)[-1] in comment, (path, "docker-compose.yml")
        assert f"`{path}`" in claude, (path, "CLAUDE.md")
