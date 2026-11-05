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
uvicorn drains them too, and none writes DuckDB since step 14's S14-15
(`SLOW_DUCKDB_WRITERS`, derived from the code by
`test_the_slow_writers_are_derived_from_the_code`; three did until then). One
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

from tests.repo_tree import needs_checkout

REPO = Path(__file__).resolve().parents[2]
COMPOSE = yaml.safe_load((REPO / "docker-compose.yml").read_text())
WORKFLOW = yaml.safe_load((REPO / ".github" / "workflows" / "deploy.yml").read_text())

WAREHOUSE_REFRESH_S = 30     # measured, production row counts, cpus: 1.0
CLOSING_CHECKPOINT_S = 7     # measured, 900 MB of WAL
SSH_COMMAND_TIMEOUT_S = 600  # appleboy/ssh-action's default at the pinned commit
HEALTH_GATE_S = 180          # the deploy's `timeout 180` wait for /api/health

# The slow paths a stop can be killed in the middle of, by name: adding one is
# a decision about this gap, and CLAUDE.md and docker-compose.yml name them.
# The resync and the status refresh by both their names since step 14's
# S14-17: `/api/sync/…`, and the `/api/duckdb/…` alias they keep until the
# final stage, one function behind each pair (tests/unit/test_sync_doors.py).
KNOWN_SLOW_ENDPOINTS = {
    "/api/sync/resync",
    "/api/sync/refresh-statuses",
    "/api/duckdb/resync",
    "/api/duckdb/refresh-statuses",
    "/api/goals/recalculate",
    "/api/stocks/analysis",
    "/api/revenue/forecast/train",
    "/api/revenue/forecast/evaluate",
    "/api/revenue/forecast/tune",
    "/api/traffic/reclassify",
}
# Of those, the ones that write DuckDB — a kill there leaves a WAL. Derived,
# not kept: it once named three, and the review that found two more found that
# nothing checked it (batch-E). Those two — the recalculation and the
# training, which wrote the goal and forecast tables to DuckDB while chain
# 7b-3's flag could say duckdb — write Postgres alone since step 14's S14-11
# (`MOVED_TO_POSTGRES`). The other three — the resync and the status refresh,
# through DuckDB's warehouse rebuild, and the reclassify, through DELETE FROM
# silver_order_utm — reach no DuckDB write since S14-15 deleted the rebuild,
# DuckDB's UTM parse and the doors' DuckDB halves.
SLOW_DUCKDB_WRITERS: set = set()
MOVED_TO_POSTGRES = {"/api/goals/recalculate", "/api/revenue/forecast/train",
                     "/api/sync/resync", "/api/sync/refresh-statuses",
                     "/api/duckdb/resync", "/api/duckdb/refresh-statuses",
                     "/api/traffic/reclassify"}


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


def _module_constants(trees: dict) -> dict:
    """`{(rel, NAME): text}` for every module-level `NAME = "<sql>"`."""
    return {
        (rel, st.targets[0].id): _text(st.value)
        for rel, tree in trees.items() for st in tree.body
        if isinstance(st, ast.Assign) and len(st.targets) == 1
        and isinstance(st.targets[0], ast.Name) and _text(st.value) is not None}


def _function_writes(node, rel: str, constants: dict) -> bool:
    """Whether the function `node` (in module `rel`) executes a statement
    that writes: an INSERT, UPDATE, DELETE, CREATE, DROP, ALTER, TRUNCATE or
    COPY FROM in its own body, or in a module constant it names, beside an
    `execute`/`executemany`/`sql` call. Its docstring is not a statement."""
    from tests.unit.test_read_fallback_consumers import _own

    body = node.body
    docstring = (body[0].value if body and isinstance(body[0], ast.Expr)
                 and _text(body[0].value) is not None else None)
    texts, executes = [], False
    for child in _own(node):
        if child is docstring:
            continue
        text = _text(child)
        if text is None and isinstance(child, ast.Name):
            text = constants.get((rel, child.id))
        if text is not None:
            texts.append(text)
        executes |= (isinstance(child, ast.Call) and isinstance(child.func, ast.Attribute)
                     and child.func.attr in _EXECUTES)
    return executes and any(_WRITES.search(t) for t in texts)


def derived_slow_duckdb_writers() -> set:
    from tests.unit.test_read_fallback_consumers import _routes, build_graph
    from web.middleware import SLOW_ENDPOINTS

    graph = build_graph(("core", "web"))
    constants = _module_constants(graph.trees)

    def writes_duckdb(fn) -> bool:
        if (fn.rel, fn.cls) not in graph.store:
            return False
        return _function_writes(fn.node, fn.rel, constants)

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
    """Mutation: drop `/api/sync/resync` from `SLOW_DUCKDB_WRITERS`, or
    name `/api/stocks/analysis`, which only reads."""
    assert derived_slow_duckdb_writers() == SLOW_DUCKDB_WRITERS


# The shape the batch-E review found the derivation blind to, and the reason
# it reads module constants: a mixin method that executes a write whose SQL
# sits in a module constant. Since S14-11 no store method has that shape (the
# doors that had it write Postgres), so the derivation answering the empty set
# no longer proves it can see one — this does, on a module of its own.
_SYNTHETIC_MIXIN = '''
_SEASONAL_UPSERT_SQL = "INSERT INTO seasonal_indices (month, idx) VALUES (?, ?)"
_SEASONAL_READ_SQL = "SELECT month, idx FROM seasonal_indices"


class GoalsMixin:
    async def store_indices(self, rows):
        """INSERT INTO is in this docstring, which is not a statement."""
        async with self.connection() as conn:
            conn.executemany(_SEASONAL_UPSERT_SQL, rows)

    async def read_indices(self):
        async with self.connection() as conn:
            return conn.execute(_SEASONAL_READ_SQL).fetchall()

    async def only_says_it(self):
        """INSERT INTO seasonal_indices — in prose, and nothing executes."""
        return _SEASONAL_UPSERT_SQL
'''


def test_the_derivation_sees_a_write_through_a_constant_from_a_mixin():
    """Mutation: drop the module-constant lookup from `_function_writes` (or
    from `derived_slow_duckdb_writers`) — `store_indices` reads as a reader."""
    rel = "core/repositories/synthetic_goals.py"
    tree = ast.parse(_SYNTHETIC_MIXIN)
    constants = _module_constants({rel: tree})
    assert (rel, "_SEASONAL_UPSERT_SQL") in constants
    methods = {n.name: n for n in ast.walk(tree)
               if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef))}
    assert _function_writes(methods["store_indices"], rel, constants)
    assert not _function_writes(methods["read_indices"], rel, constants)
    assert not _function_writes(methods["only_says_it"], rel, constants)
    # Without the constants the write is invisible: what the lookup is for.
    assert not _function_writes(methods["store_indices"], rel, {})


def test_the_goal_and_forecast_doors_write_postgres_alone():
    """Step 14's S14-11: the recalculation and the training reach chain
    7b-3's writer and no DuckDB statement (and since S14-15 the resync, the
    status refresh and the reclassify, which reached DuckDB's warehouse
    rebuild or its UTM table). Until then they reached DuckDB
    through `_SEASONAL_UPSERT_SQL`, a module constant, from a mixin method —
    the shape the review's omission had, which the derivation was held to
    see. Mutation: give either a DuckDB branch again."""
    assert not MOVED_TO_POSTGRES & derived_slow_duckdb_writers()
    assert MOVED_TO_POSTGRES <= KNOWN_SLOW_ENDPOINTS


@needs_checkout
def test_compose_and_claude_md_name_every_writer():
    """The two places a person reads about the gap say what the list says."""
    compose = (REPO / "docker-compose.yml").read_text()
    comment = compose[:compose.index("stop_grace_period:")]
    comment = comment[comment.rindex("# How long a stop waits"):]
    claude = (REPO / ".claude" / "CLAUDE.md").read_text()
    for path in SLOW_DUCKDB_WRITERS:
        assert path.rsplit("/", 1)[-1] in comment, (path, "docker-compose.yml")
        assert f"`{path}`" in claude, (path, "CLAUDE.md")
