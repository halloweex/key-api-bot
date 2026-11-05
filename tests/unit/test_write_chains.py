"""The write chains' registry, and the walk that finds every Postgres writer.

Stage 4 moved WRITES chain by chain, and until step 14's S14-13 everything
that shipped a chain's tables out of DuckDB — the mirrors, the backfills, the
hourly ids-diffs and copies — had to ask the registry first, and a walk
(DN-22a/b) failed on any Postgres writer of `bronze.*`/`app.*` that did not.
S14-13 deleted every such shipper (`core.pg_backfill`, `pg_expense_backfill`,
`pg_buyers`, `pg_replication`, `pg_operational`, the mirror's half of
`pg_landing`), and the question they asked (`tables_stood_down*`) with them,
so the tests of asking went too.

What stays: that every module declaring a chain is registered; the walk
itself, and the two things it still holds — the order tables and the archive
have exactly one writer, and the `/sms` and access store helpers write
Postgres alone and ask no switch (S14-08); and that a chain declares both
order tables or neither.
"""
from __future__ import annotations

import ast
import pathlib
import re
import textwrap

import pytest

CORE = pathlib.Path(__file__).resolve().parents[2] / "core"


def _declares_a_write_chain(path: pathlib.Path) -> bool:
    """Top-level `CHAIN_TABLES = ...` and `def _latch` — parsed, not
    imported, so walking the package has no side effects. (`writes_postgres`
    until step 14's S14-11 deleted it with the flags; every chain still takes
    its latch on its first write.)"""
    tree = ast.parse(path.read_text(encoding="utf-8"))
    names = set()
    for node in tree.body:
        if isinstance(node, (ast.Assign, ast.AnnAssign)):
            targets = node.targets if isinstance(node, ast.Assign) else [node.target]
            names.update(t.id for t in targets if isinstance(t, ast.Name))
        elif isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            names.add(node.name)
    return {"CHAIN_TABLES", "_latch"} <= names


class TestEveryWriteChainIsRegistered:
    def test_the_walk_finds_them_all_and_the_registry_holds_them_all(self):
        from core import write_chains

        found = {p.stem for p in CORE.glob("*.py") if _declares_a_write_chain(p)}
        registered = {m.__name__.rsplit(".", 1)[-1] for m in write_chains.WRITE_CHAINS}
        assert found, "the walk found no write chain — it is not looking"
        assert found == registered, (
            f"declared but not registered: {sorted(found - registered)}; "
            f"registered but not declaring: {sorted(registered - found)}")

    def test_the_walk_is_not_vacuous(self):
        """Every chain that exists today is found, so an empty walk cannot pass
        — chain 7a (DN-25) included, which is the one this walk must see for
        the registry above to be proven rather than assumed."""
        found = {p.stem for p in CORE.glob("*.py") if _declares_a_write_chain(p)}
        assert {"pg_inventory_write", "pg_expenses_write", "pg_goals_write",
                "pg_expense_types_write"} <= found


# ─── The walk: every function that writes bronze.* or app.* in Postgres ─────
#
# Walked, not listed. The walk finds every function in core/, web/ and
# scripts/ that executes a Postgres write naming a `bronze.` or `app.` table,
# or one whose target it cannot read (`INSERT INTO {table}`: a guard that could
# not resolve a name must not read it as "not ours"). Two shapes the DN-22b
# review found are read: a helper that transforms the SQL it is handed before
# executing it (`conn.execute(numbered(sql))`, or handing it on to another such
# helper — `_users_run` → `execute(rendered)`), whose caller's literal names the
# target; and `copy_records_to_table`/`copy_to_table`, which carry no SQL at all
# and are read by their `schema_name` — an unresolved or absent one counts.

_WALKED = ("core", "web", "scripts")
_WRITE_SQL = re.compile(
    r"\b(?:INSERT\s+INTO|UPDATE|DELETE\s+FROM|TRUNCATE(?:\s+TABLE)?)\s+"
    r"(?:ONLY\s+)?(bronze\.\w+|app\.\w+|\{[^}]*\})",
    re.IGNORECASE)
# An awaited call on one of these is asyncpg; DuckDB's are synchronous.
_PG_EXECUTE = {"execute", "executemany", "copy_records_to_table", "copy_to_table",
               "fetch", "fetchval", "fetchrow"}
# The two that write rows without a statement the regex could read.
_PG_COPY_IN = {"copy_records_to_table", "copy_to_table"}

# The store helpers through which `/sms` and the access lists write:
# Postgres is where those tables' writer lives, and there is no copy to ship. They routed by a switch
# of their own (`sms_store_is_postgres`, `user_store_is_postgres`) until step
# 14's S14-08 took their DuckDB tails away; they run on Postgres alone now.
# A writer whose only Postgres calls, in its own module, are to these is
# exempt through them, and the test checks each still executes what it is
# handed, still carries a writer, and asks no switch.
#
# `none`, `many`, `one` and `all` are `_PgTx`'s, the transaction `_sms_tx`
# yields: the walk reads methods by name, and until S14-08 the first
# definition of each name in the module was `_DuckTx`'s, which executed
# nothing asyncpg could see — so the three writers that run inside the
# transaction (the freeze, the send's record, a delivery report) were exempt
# by that accident rather than by a rule.
_POSTGRES_STORE_HELPERS = {
    ("core/repositories/customers.py", "_sms_run"): "one /sms statement",
    ("core/repositories/customers.py", "none"): "a statement in _sms_tx's transaction",
    ("core/repositories/customers.py", "many"): "a statement in _sms_tx's transaction",
    ("core/repositories/customers.py", "one"): "a statement in _sms_tx's transaction",
    ("core/repositories/customers.py", "all"): "a statement in _sms_tx's transaction",
    ("core/repositories/users.py", "_users_run"): "one statement on the user list",
    ("core/repositories/users.py", "_perms_run"): "one statement on the role matrix",
}
# The switches those helpers asked until S14-08. None may come back into one.
_RETIRED_STORE_SWITCHES = ("sms_store_is_postgres", "user_store_is_postgres")


class _Module:
    """One parsed module: its string constants, functions and imports."""

    def __init__(self, path: pathlib.Path):
        self._parse(path.relative_to(CORE.parent).as_posix(),
                    path.read_text(encoding="utf-8"))

    @classmethod
    def from_source(cls, rel: str, source: str) -> "_Module":
        """A module that is not on disk: the walk's own tests feed it shapes."""
        module = cls.__new__(cls)
        module._parse(rel, textwrap.dedent(source))
        return module

    def _parse(self, rel: str, source: str) -> None:
        self.rel = rel
        self.name = rel[:-len(".py")].replace("/", ".")
        self.tree = ast.parse(source)
        self.consts: dict = {}
        self.funcs: dict = {}
        self.imports: dict = {}
        self.module_aliases: dict = {}
        for node in ast.walk(self.tree):
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                self.funcs.setdefault(node.name, node)
            elif isinstance(node, ast.ImportFrom) and node.module:
                for a in node.names:
                    self.imports[a.asname or a.name] = (node.module, a.name)
                    self.module_aliases[a.asname or a.name] = f"{node.module}.{a.name}"
            elif isinstance(node, ast.Import):
                for a in node.names:
                    self.module_aliases[a.asname or a.name] = a.name

    def load_consts(self, modules) -> None:
        """Module-level strings, rendered in order, f-strings with the names
        they interpolate; a constant imported from a walked module too, and a
        constant built by calling a helper of this module."""
        for node in self.tree.body:
            if not isinstance(node, (ast.Assign, ast.AnnAssign)):
                continue
            targets = node.targets if isinstance(node, ast.Assign) else [node.target]
            if node.value is None:
                continue
            text = self.render(node.value, modules)
            if text is None and isinstance(node.value, ast.Call) \
                    and isinstance(node.value.func, ast.Name) \
                    and node.value.func.id in self.funcs:
                text = " ".join(self.texts(self.funcs[node.value.func.id], modules))
            if text is None:
                continue
            for t in targets:
                if isinstance(t, ast.Name):
                    self.consts[t.id] = text

    def const(self, name, modules, bindings=None):
        """A name's string value: an argument bound at the call being
        inlined, a constant of this module, or one it imports."""
        if bindings and name in bindings:
            return bindings[name]
        if name in self.consts:
            return self.consts[name]
        if name in self.imports:
            src, attr = self.imports[name]
            other = modules.get(src)
            if other is not None and other is not self:
                return other.consts.get(attr)
        return None

    def value(self, node, modules, bindings=None):
        """A call argument as a string, when the walk can know it."""
        if isinstance(node, ast.Name):
            return self.const(node.id, modules, bindings)
        return self.render(node, modules, bindings)

    def render(self, node, modules, bindings=None):
        if isinstance(node, ast.Constant) and isinstance(node.value, str):
            return node.value
        if isinstance(node, ast.JoinedStr):
            parts = []
            for v in node.values:
                if isinstance(v, ast.Constant):
                    parts.append(str(v.value))
                elif isinstance(v, ast.FormattedValue) and isinstance(v.value, ast.Name) \
                        and self.const(v.value.id, modules, bindings) is not None:
                    parts.append(self.const(v.value.id, modules, bindings))
                else:
                    parts.append("{?}")
            return "".join(parts)
        if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Add):
            left = self.render(node.left, modules, bindings)
            right = self.render(node.right, modules, bindings)
            if left is not None and right is not None:
                return left + right
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute) \
                and node.func.attr == "format":
            # `_UPSERT.format(table=table, ...)`: the template with every
            # keyword the walk can know filled in; the rest stay `{...}`.
            template = self.value(node.func.value, modules, bindings)
            if template is None:
                return None
            for kw in node.keywords:
                known = self.value(kw.value, modules, bindings) if kw.arg else None
                if known is not None:
                    template = template.replace("{" + kw.arg + "}", known)
            return template
        return None

    def texts(self, fn, modules, seen=None, bindings=None):
        """Every SQL-ish string `fn` can execute: its own literals and
        f-strings, the constants it names, and — transitively — what the
        synchronous helpers it calls return (`_statement`, `_insert`,
        `silver_pass2_sql`), rendered with the arguments this call passes
        them where the walk can know those."""
        seen = set() if seen is None else seen
        mark = (self.name, fn.name, tuple(sorted((bindings or {}).items())))
        if mark in seen:
            return []
        seen.add(mark)
        doc = ast.get_docstring(fn)
        # A template read through `.format` is rendered filled in, never raw.
        formatted = {id(n.func.value) for n in ast.walk(fn)
                     if isinstance(n, ast.Call) and isinstance(n.func, ast.Attribute)
                     and n.func.attr == "format"}
        out = []
        for node in ast.walk(fn):
            if id(node) in formatted:
                continue
            text = self.render(node, modules, bindings)
            if text and text != doc:
                out.append(text)
            if isinstance(node, ast.Name):
                text = self.const(node.id, modules, bindings)
                if text:
                    out.append(text)
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Name):
                target = _resolve(self, node.func.id, modules)
                if target and isinstance(target[1], ast.FunctionDef):
                    callee_module, callee = target
                    params = [a.arg for a in callee.args.args]
                    bound = {}
                    for name, arg in zip(params, node.args):
                        known = self.value(arg, modules, bindings)
                        if known is not None:
                            bound[name] = known
                    for kw in node.keywords:
                        known = self.value(kw.value, modules, bindings) if kw.arg else None
                        if known is not None:
                            bound[kw.arg] = known
                    out += callee_module.texts(callee, modules, seen, bound)
        return out


def _resolve(module, name, modules):
    """`(module, function)` for a name called in `module`, or None."""
    if name in module.funcs:
        return module, module.funcs[name]
    if name in module.imports:
        src, attr = module.imports[name]
        other = modules.get(src)
        if other is not None and attr in other.funcs:
            return other, other.funcs[attr]
    return None


def _walk_modules() -> dict:
    root = CORE.parent
    modules = {}
    for folder in _WALKED:
        for path in sorted((root / folder).rglob("*.py")):
            m = _Module(path)
            modules[m.name] = m
    for m in modules.values():
        m.load_consts(modules)
    return modules


def _called_names(fn) -> set:
    names = set()
    for node in ast.walk(fn):
        if isinstance(node, ast.Call):
            if isinstance(node.func, ast.Name):
                names.add(node.func.id)
            elif isinstance(node.func, ast.Attribute):
                names.add(node.func.attr)
    return names


def _awaited_names(fn) -> set:
    """What `fn` awaits a call to, by name."""
    out = set()
    for n in ast.walk(fn):
        if isinstance(n, ast.Await) and isinstance(n.value, ast.Call):
            f = n.value.func
            out.add(f.id if isinstance(f, ast.Name) else getattr(f, "attr", ""))
    return out


def _carriers(fn) -> set:
    """The names in `fn` that carry one of its parameters: the parameters
    (bar `self`/`cls`, which carry the object and not the statement), and every
    local assigned from an expression that names one, to a fixed point —
    `rendered = numbered(sql.format(...))` carries `sql`."""
    args = fn.args
    carried = {a.arg for a in (*args.posonlyargs, *args.args, *args.kwonlyargs)
               if a.arg not in ("self", "cls")}
    if args.vararg:
        carried.add(args.vararg.arg)
    assigns = [n for n in ast.walk(fn)
               if isinstance(n, (ast.Assign, ast.AnnAssign, ast.AugAssign))
               and n.value is not None]
    while True:
        more = set()
        for n in assigns:
            if not any(isinstance(x, ast.Name) and x.id in carried
                       for x in ast.walk(n.value)):
                continue
            for target in (n.targets if isinstance(n, ast.Assign) else [n.target]):
                more |= {x.id for x in ast.walk(target)
                         if isinstance(x, ast.Name) and x.id not in carried}
        if not more:
            return carried
        carried |= more


def _executors(modules) -> set:
    """Helpers that execute SQL they are handed: awaiting one is executing,
    although the text is the caller's. `_write_chunked(conn, sql, rows)` hands
    it straight to asyncpg; `conn.execute(numbered(sql))` transforms it first;
    `_users_run` hands it, rendered, to `execute`, which hands it to asyncpg.
    So a helper counts when an awaited asyncpg call — or an awaited call to
    another executor — takes as its statement an expression carrying one of
    its own parameters, to a fixed point. By name, like `_awaited_names`."""
    sites = []
    for m in modules.values():
        for name, fn in m.funcs.items():
            carried = _carriers(fn)
            callees = set()
            for n in ast.walk(fn):
                if isinstance(n, ast.Await) and isinstance(n.value, ast.Call) \
                        and n.value.args \
                        and any(isinstance(x, ast.Name) and x.id in carried
                                for x in ast.walk(n.value.args[0])):
                    f = n.value.func
                    callees.add(f.id if isinstance(f, ast.Name) else getattr(f, "attr", ""))
            if callees:
                sites.append((name, callees))
    found: set = set()
    while True:
        more = {name for name, callees in sites
                if name not in found and callees & (_PG_EXECUTE | found)}
        if not more:
            return found
        found |= more


def _copy_targets(module, fn, modules) -> set:
    """What an awaited `copy_records_to_table`/`copy_to_table` writes. There
    is no statement for the regex to read, so the target is its `schema_name`
    and table: `bronze.x`/`app.x` when both resolve, nothing for a schema that
    resolves to another one, and `{?}` for a schema the walk cannot read — or
    none at all, where the search_path decides."""
    found = set()
    for n in ast.walk(fn):
        if not (isinstance(n, ast.Await) and isinstance(n.value, ast.Call)
                and isinstance(n.value.func, ast.Attribute)
                and n.value.func.attr in _PG_COPY_IN):
            continue
        call = n.value
        keywords = {k.arg: k.value for k in call.keywords if k.arg}
        table_node = call.args[0] if call.args else keywords.get("table_name")
        table = module.value(table_node, modules) if table_node is not None else None
        schema_node = keywords.get("schema_name")
        schema = module.value(schema_node, modules) if schema_node is not None else None
        if schema is None:
            found.add("{?}")
        elif schema in ("bronze", "app"):
            found.add(f"{schema}.{table}" if table else "{?}")
    return found


def _pg_write_targets(module, fn, modules, executors=frozenset()) -> set:
    """The `bronze.`/`app.` tables `fn` writes in Postgres, `{?}` for one it
    names only at run time. Empty unless `fn` awaits an asyncpg call, or a
    helper that executes the SQL it is handed."""
    if not _awaited_names(fn) & (_PG_EXECUTE | set(executors)):
        return set()
    found = _copy_targets(module, fn, modules)
    for text in module.texts(fn, modules):
        for m in _WRITE_SQL.finditer(text):
            target = m.group(1)
            found.add(target if not target.startswith("{") else "{?}")
    return found


class _Walk:
    """The whole tree, once per test session."""

    def __init__(self):
        self.modules = _walk_modules()
        self.fns = {(m.rel, name): (m, fn)
                    for m in self.modules.values() for name, fn in m.funcs.items()}
        self.calls = {key: _called_names(fn) for key, (_m, fn) in self.fns.items()}
        self.callers = self._callers()
        self.executors = _executors(self.modules)

    def _callers(self) -> dict:
        """`{(rel, name): {(rel, name) of each caller}}`, by resolved name:
        a plain call through this module or its imports, `alias.f()` through
        a module alias, and any other `x.f()` to every function called `f`
        (a method, or a name the walk cannot place) — more callers, never fewer."""
        by_name: dict = {}
        for key in self.fns:
            by_name.setdefault(key[1], []).append(key)
        callers: dict = {}
        for key, (module, fn) in self.fns.items():
            for node in ast.walk(fn):
                if not isinstance(node, ast.Call):
                    continue
                targets = []
                if isinstance(node.func, ast.Name):
                    hit = _resolve(module, node.func.id, self.modules)
                    if hit:
                        targets = [(hit[0].rel, hit[1].name)]
                elif isinstance(node.func, ast.Attribute):
                    base = node.func.value
                    alias = module.module_aliases.get(base.id) if isinstance(base, ast.Name) else None
                    other = self.modules.get(alias) if alias else None
                    if other is not None and node.func.attr in other.funcs:
                        targets = [(other.rel, node.func.attr)]
                    else:
                        targets = by_name.get(node.func.attr, [])
                for target in targets:
                    callers.setdefault(target, set()).add(key)
        return callers

    def writers(self) -> dict:
        """`{(rel, name): targets}` for every Postgres writer of our tables."""
        found = {}
        for key, (module, fn) in self.fns.items():
            targets = _pg_write_targets(module, fn, self.modules, self.executors)
            if targets:
                found[key] = targets
        return found

    def routed(self, key) -> bool:
        """Its only Postgres calls are to its own module's store helpers that
        write Postgres alone (`_POSTGRES_STORE_HELPERS`)."""
        helpers = {name for rel, name in _POSTGRES_STORE_HELPERS if rel == key[0]}
        _module, fn = self.fns[key]
        reached = _awaited_names(fn) & (_PG_EXECUTE | self.executors)
        return bool(reached) and reached <= helpers


@pytest.fixture(scope="module")
def walk():
    return _Walk()


def _chain_modules() -> set:
    from core.write_chains import WRITE_CHAINS

    return {c.__name__.replace(".", "/") + ".py" for c in WRITE_CHAINS}


class TestTheWalk:
    def test_the_walk_is_not_vacuous(self, walk):
        """A walk that found nothing, or stopped resolving what it found,
        would pass every test that reads it. The writers every chain 3–4
        table goes through, and the archive's capture. (At least 27 writers
        were found before step 14's S14-13 deleted the shippers and the copies
        among them.)"""
        writers = walk.writers()
        assert {
            ("core/pg_landing.py", "_write_rows"),
            ("core/pg_landing.py", "_write_order_rows"),
            ("core/pg_buyer_rows.py", "_write_buyer_rows"),
            ("core/pg_order_versions.py", "capture_versions"),
        } <= set(writers), sorted(writers)
        assert len(writers) >= 15, sorted(writers)

    def test_no_shipper_out_of_duckdb_is_left(self, walk):
        """What S14-13 deleted stays deleted: none of the modules that shipped
        a copy out of DuckDB comes back, and nothing in the tree writes
        through the mirror's own statements."""
        gone = {"core/pg_backfill.py", "core/pg_expense_backfill.py",
                "core/pg_buyers.py", "core/pg_replication.py",
                "core/pg_operational.py"}
        assert not gone & {m.rel for m in walk.modules.values()}
        shippers = ("_mirror", "mirror_categories", "mirror_products",
                    "mirror_expenses", "mirror_orders", "carry_retired_catalogue",
                    "tables_stood_down", "tables_stood_down_or_owned",
                    "order_tables_stood_down", "order_tables_stood_down_or_owned")
        assert not {key for key in walk.fns if key[0] == "core/pg_landing.py"
                    and key[1] in shippers}

    def test_the_routed_helpers_are_exactly_what_the_walk_finds(self, walk):
        """Each is an executor the walk finds, still carries a writer, and
        asks no store switch — so neither a helper that grew an engine choice
        back nor a stale entry keeps its place (S14-08)."""
        writers = walk.writers()
        for key in _POSTGRES_STORE_HELPERS:
            _module, fn = walk.fns[key]
            names = {n.id for n in ast.walk(fn) if isinstance(n, ast.Name)} | \
                _called_names(fn)
            for switch in _RETIRED_STORE_SWITCHES:
                assert switch not in names, f"{key} asks {switch} again"
            assert key[1] in walk.executors, f"{key} no longer executes what it is handed"
            assert any(key[0] == w[0] and key[1] in _awaited_names(walk.fns[w][1])
                       and walk.routed(w)
                       for w in writers), f"nothing writes through {key}"
        # And nothing else is excused by them: a writer that also calls
        # asyncpg itself, or any other executor, is not routed.
        assert {w[0] for w in writers if walk.routed(w)} == {
            "core/repositories/customers.py", "core/repositories/users.py"}

    def test_every_registered_chain_writes_postgres(self, walk):
        """Found here, so a chain that stopped writing Postgres — its writer
        moved somewhere the walk does not see — fails rather than passing as
        a registered module with nothing in it."""
        chains = _chain_modules()
        writing = {key[0] for key in walk.writers()}
        assert chains <= writing, chains - writing


def _targets_of(source: str, name: str) -> set:
    """What the walk says `name` writes, in a module made of `source` alone."""
    module = _Module.from_source("core/_walk_probe.py", source)
    modules = {module.name: module}
    module.load_consts(modules)
    return _pg_write_targets(module, module.funcs[name], modules, _executors(modules))


class TestTheWalkSeesWhatTheReviewFound:
    """The DN-22b review appended each of these to `core/pg_buyers.py` and the
    walk passed them: a helper that transforms the statement it is handed
    before executing it — `numbered()` is this codebase's own idiom — and a
    COPY, which carries no statement at all. Fed here as shapes, with the
    direct literal the walk always saw as the control."""

    _NUMBERED = """
        async def _exec_numbered(sql, params):
            pool = await get_pool()
            async with pool.acquire() as conn:
                await conn.execute(numbered(sql), *params)

        async def rename_buyer(buyer_id, name):
            await _exec_numbered(
                "UPDATE bronze.buyers SET full_name = ? WHERE id = ?", [name, buyer_id])

        async def read_buyer(buyer_id):
            await _exec_numbered("SELECT full_name FROM bronze.buyers WHERE id = ?", [buyer_id])
    """

    def test_the_control_a_direct_literal(self):
        assert _targets_of("""
            async def rename_buyer(buyer_id, name):
                pool = await get_pool()
                async with pool.acquire() as conn:
                    await conn.execute(
                        "UPDATE bronze.buyers SET full_name = $1 WHERE id = $2", name, buyer_id)
        """, "rename_buyer") == {"bronze.buyers"}

    def test_a_helper_that_transforms_the_statement_executes_it(self):
        assert _targets_of(self._NUMBERED, "rename_buyer") == {"bronze.buyers"}

    def test_a_read_through_the_same_helper_is_not_a_write(self):
        assert _targets_of(self._NUMBERED, "read_buyer") == set()

    def test_a_statement_handed_on_through_two_helpers(self):
        """`_users_run`'s shape: rendered into a local, handed to a helper that
        hands it to asyncpg. The target is a hole the helper fills, so it
        counts as unknown — which is what makes the store's writers visible
        and `_POSTGRES_STORE_HELPERS` necessary. The inner helper is not called
        `execute` here, so only the fixed point can reach it."""
        assert _targets_of("""
            async def _send(sql, params):
                pool = await get_pool()
                async with pool.acquire() as conn:
                    await conn.execute(sql, *params)

            async def _run(self, sql, params):
                rendered = numbered(sql.format(users=TABLE))
                await _send(rendered, params)

            async def approve(self, user_id):
                await self._run("UPDATE {users} SET status = 'approved' "
                                "WHERE user_id = ?", [user_id])
        """, "approve") == {"{?}"}

    def test_self_alone_does_not_make_a_helper_an_executor(self):
        """`self` carries the object, not a statement: a method reading its own
        fixed query is not handed anything."""
        module = _Module.from_source("core/_walk_probe.py", """
            async def count(self):
                async with self.pool.acquire() as conn:
                    await conn.fetchval(self.COUNT_SQL)
        """)
        assert "count" not in _executors({module.name: module})

    @pytest.mark.parametrize("call,expected", [
        ('conn.copy_records_to_table("buyers", schema_name="bronze", records=rows)',
         {"bronze.buyers"}),
        ('conn.copy_to_table("orders", source=path, schema_name="bronze")',
         {"bronze.orders"}),
        ('conn.copy_records_to_table(table_name="gender", schema_name=APP, records=rows)',
         {"app.gender"}),
        ('conn.copy_records_to_table(table, schema_name=schema, records=rows)', {"{?}"}),
        ('conn.copy_records_to_table("buyers", records=rows)', {"{?}"}),
        ('conn.copy_records_to_table("orders", schema_name="silver", records=rows)', set()),
    ])

    def test_a_copy_is_read_by_its_schema(self, call, expected):
        assert _targets_of(f"""
            APP = "app"

            async def ship(rows, table, schema, path):
                pool = await get_pool()
                async with pool.acquire() as conn:
                    await {call}
        """, "ship") == expected


@pytest.fixture
def flags(monkeypatch):
    for env in ("KS_WRITE_INVENTORY", "KS_WRITE_EXPENSES", "KS_WRITE_GOALS",
                "KS_WRITE_EXPENSE_TYPES"):
        monkeypatch.delenv(env, raising=False)
    return monkeypatch


ORDERS = "bronze.orders"
LINES = "bronze.order_products"
WHEN = "2026-08-20T12:00:00+00:00"


def _order(order_id, products=2):
    return {
        "id": order_id, "source_id": 1, "status_id": 12, "status_group_id": 4,
        "grand_total": "100.00", "ordered_at": WHEN, "created_at": WHEN,
        "updated_at": WHEN, "buyer": {"id": 500 + order_id},
        "manager": {"id": 4}, "manager_comment": None, "promocode": None,
        "products": [
            {"name": f"Товар {i}", "quantity": 1, "price_sold": "50.00",
             "offer": {"product_id": 700 + i}}
            for i in range(products)
        ],
    }


class _Ctx:
    def __init__(self, value=None):
        self.value = value

    async def __aenter__(self):
        return self.value

    async def __aexit__(self, *exc):
        return False


class _Conn:
    """Answers every read with nothing and remembers every statement."""

    def __init__(self, recorder):
        self.recorder = recorder

    def transaction(self):
        return _Ctx()

    async def execute(self, sql, *args):
        self.recorder.sql.append(sql)
        return "UPDATE 1"

    async def executemany(self, sql, rows):
        self.recorder.sql.append(sql)

    async def fetch(self, sql, *args):
        self.recorder.sql.append(sql)
        return []

    async def fetchval(self, sql, *args):
        self.recorder.sql.append(sql)
        return None

    async def fetchrow(self, sql, *args):
        self.recorder.sql.append(sql)
        return None


class _RecordingPool:
    def __init__(self):
        self.sql = []

    def acquire(self):
        return _Ctx(_Conn(self))

    def wrote(self, table: str) -> bool:
        return any(table in s and "INSERT INTO" in s for s in self.sql)


@pytest.fixture
def pool(flags):
    """The recording pool behind `core.pg.get_pool`."""
    from unittest.mock import AsyncMock

    recorder = _RecordingPool()
    flags.setattr("core.pg.get_pool", AsyncMock(return_value=recorder))
    flags.setattr("core.pg.require_revision", AsyncMock())
    return recorder


def _chains_splitting_the_order_tables(chains) -> list:
    """The chains that declare one order table and not the other."""
    from core.write_chains import chain_name

    both = {ORDERS, LINES}
    return [chain_name(c) for c in chains
            if both & set(c.CHAIN_TABLES) and not both <= set(c.CHAIN_TABLES)]


class TestOwnershipOfTheOrderTablesPassesAsAUnit:
    """Why the two order tables are one chain's. Not that they ship together —
    `write_orders`' core with `replace_products=False` writes headers alone
    every day, from the 05:15 status refresh and the comment backfills (the
    comment ship until step 14's S14-09) — but that whoever takes the
    headers takes their line items: a chain declares both or neither. Walked
    over the real registry, which `TestEveryWriteChainIsRegistered` holds equal
    to every module that declares a chain."""

    def test_no_registered_chain_declares_one_without_the_other(self):
        from core.write_chains import WRITE_CHAINS

        assert _chains_splitting_the_order_tables(WRITE_CHAINS) == []

    def test_the_check_sees_a_split_when_there_is_one(self):
        """So the empty answer above is a finding, not a check that cannot fail."""
        import types

        for tables in ((ORDERS,), (LINES,), (LINES, "app.something_else")):
            fake = types.ModuleType("core.pg_orders_write")
            fake.CHAIN_TABLES = tables
            assert _chains_splitting_the_order_tables((fake,)) == ["pg_orders_write"]
        whole = types.ModuleType("core.pg_orders_write")
        whole.CHAIN_TABLES = (ORDERS, LINES)
        assert _chains_splitting_the_order_tables((whole,)) == []


class TestTheOrderWatchesOutliveTheStandDown:
    """The 23.09 question (DN-22a's review), settled in DN-22b (the stand-down
    itself went with the sync's mirror in step 14's S14-09 and S14-13): when a chain
    owns the order tables, the stand-down cuts the sync's mirror off from
    `write_orders` — and `write_orders` is the only thing that moves the
    `bronze.orders` watermark the canary pages on (`mirror_stale`, 8 h) and
    the only thing that writes `app.order_versions`, whose liveness check
    files `order_versions_stalled` (CRITICAL). Either the two watches learn to
    read the stand-down, or the chain's writer keeps feeding both.

    The writer keeps feeding both. The chain-3 writer ships through
    `write_orders`, because nothing else may write the order tables or the
    archive: the capture must land in the transaction that writes the row
    (`core/pg_order_versions.py`), and a second writer of `bronze.orders`
    beside it is exactly what `order_versions_missing` already reports as
    "something is writing bronze.orders without going through write_orders".
    So the watermark keeps moving and the archive keeps being written under
    the chain, and both watches keep meaning what they say. Teaching them the
    stand-down instead would blind the one liveness check on the one table
    nothing can rebuild, at the moment its writer changes hands.

    Pinned here, from the walk: the order tables and the archive each have
    one writer — a chain module included, which the registry walk would
    otherwise excuse as a destination."""

    ORDER_TABLES = {ORDERS, LINES}
    ARCHIVE = "app.order_versions"

    def _writers_of(self, walk, tables) -> set:
        return {key for key, targets in walk.writers().items()
                if targets & set(tables)}

    # Since chain 3's PR-1 the statements live in `_write_order_rows`, on the
    # caller's transaction: `write_orders` runs them for every path that
    # ships DuckDB's orders, and chain 3's writer for its own. Still one
    # writer — the chain module spells no statement against these tables.
    CORE = ("core/pg_landing.py", "_write_order_rows")

    def test_the_order_tables_have_one_writer(self, walk):
        assert self._writers_of(walk, self.ORDER_TABLES) == {self.CORE}

    def test_the_archive_is_written_only_inside_it(self, walk):
        capture = ("core/pg_order_versions.py", "capture_versions")
        assert self._writers_of(walk, {self.ARCHIVE}) == {capture}
        assert walk.callers.get(capture) == {self.CORE}

    @pytest.mark.asyncio
    async def test_it_moves_the_watched_watermark_and_writes_the_archive(self, pool):
        """What a writer going through it keeps feeding: the watermark row the
        canary reads and a version for an order it has not seen."""
        from core.landing_rows import landed_orders
        from core.mirror_reconciliation import WATCHED_MIRRORS
        from core.pg_landing import write_orders
        from core.pg_order_versions import TABLE

        orders, products = landed_orders([_order(1)])
        await write_orders(orders, products, replace_products=True)
        assert ORDERS in WATCHED_MIRRORS
        assert any("INSERT INTO meta.mirror_state" in s for s in pool.sql)
        assert pool.wrote(TABLE)


class TestEveryChainNamesItselfOnce:
    """`CHAIN` is the string the latch, the owner rows, health and the canary
    key on; `chain_name` is what the registry derives from the module. A chain
    whose two disagree is held down by its owner rows while the health block
    and the canary look for it under the other name and see nothing — found
    reviewing chain 4's plan, before a third chain could be the first to do it.
    """

    @pytest.mark.parametrize("chain", __import__(
        "core.write_chains", fromlist=["WRITE_CHAINS"]).WRITE_CHAINS,
        ids=lambda c: c.__name__)

    def test_chain_is_the_module_basename(self, chain):
        from core.write_chains import chain_name
        assert chain.CHAIN == chain_name(chain)

    def test_the_flag_fixture_covers_every_chain(self):
        """The autouse fixture in tests/conftest.py iterates WRITE_CHAINS; this
        is the reading of it a new chain will rely on."""
        import os

        from core.write_chains import WRITE_CHAINS
        assert all(os.getenv(c.WRITE_ENV) is None for c in WRITE_CHAINS)

