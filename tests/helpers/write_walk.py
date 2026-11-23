"""The repository walk for the write chains: every statement that writes a
table, and the module it is written from.
"""
from __future__ import annotations

import ast
import pathlib
import re
import textwrap


CORE = pathlib.Path(__file__).resolve().parents[2] / "core"


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


def _walk_modules(folders=_WALKED) -> dict:
    root = CORE.parent
    modules = {}
    for folder in folders:
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

    def __init__(self, folders=_WALKED):
        self.modules = _walk_modules(folders)
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


#
# Walked over every tree that holds Python a container runs — the bot and the
# host tools too, which the order-table walk above has no need of: a capture
# table's second writer anywhere would be a history written two ways.
_CAPTURE_WALKED = _WALKED + ("bot", "deploy")
