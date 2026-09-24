"""Which tables have changed hands — one answer, found by walking, not listed.

Stage 4 moves writes chain by chain. When a chain writes Postgres, everything
that ships its tables out of DuckDB and everything that compares them must stop
touching them, and must stop together. All of them ask the registry
(`core.write_chains`), and since DN-22b a walk finds every function that writes
`bronze.*` or `app.*` in Postgres, and every comparison of those tables, and
fails on one that does not ask.

The guard below walks `core/` rather than trusting `WRITE_CHAINS`: a guard that
names its subjects guards only the ones somebody remembered (the mirror-spec
guard saw 2 of 7 groups; the volume bound was written into one gate of two; the
transient-key exclusion named a retired key). A new write chain that forgets to
register itself would keep being full-replaced out of a frozen DuckDB — the
silent hourly rollback — and this is what makes that fail in CI instead.
"""
from __future__ import annotations

import ast
import inspect
import pathlib
import re
import textwrap

import pytest

CORE = pathlib.Path(__file__).resolve().parents[2] / "core"


def _declares_a_write_chain(path: pathlib.Path) -> bool:
    """Top-level `CHAIN_TABLES = ...` and `def writes_postgres` — parsed, not
    imported, so walking the package has no side effects."""
    tree = ast.parse(path.read_text(encoding="utf-8"))
    names = set()
    for node in tree.body:
        if isinstance(node, (ast.Assign, ast.AnnAssign)):
            targets = node.targets if isinstance(node, ast.Assign) else [node.target]
            names.update(t.id for t in targets if isinstance(t, ast.Name))
        elif isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            names.add(node.name)
    return {"CHAIN_TABLES", "writes_postgres"} <= names


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


# ─── Every path that writes bronze.* or app.* asks who owns it (DN-22b) ──────
#
# Walked, not listed. This used to name its two subjects — the hourly shipper
# and the daily comparison of the operational tables — and so guarded exactly
# those two while the catalogue, the expenses, the buyers and the manager
# classification shipped past the registry. The walk finds every function in
# core/, web/ and scripts/ that executes a Postgres write naming a `bronze.` or
# `app.` table, or one whose target it cannot read (`INSERT INTO {table}`: a
# guard that could not resolve a name must not read it as "not ours"). Each
# must ask the registry itself, or be reached only from functions that do —
# `write_orders`, `pg_buyers._write` and `write_managers` are such primitives,
# and it is their callers that ask. What is exempt is the destination side,
# each entry with its reason, and the list must be exactly what the walk finds.

_WALKED = ("core", "web", "scripts")
_WRITE_SQL = re.compile(
    r"\b(?:INSERT\s+INTO|UPDATE|DELETE\s+FROM|TRUNCATE(?:\s+TABLE)?)\s+"
    r"(?:ONLY\s+)?(bronze\.\w+|app\.\w+|\{[^}]*\})",
    re.IGNORECASE)
# An awaited call on one of these is asyncpg; DuckDB's are synchronous.
_PG_EXECUTE = {"execute", "executemany", "copy_records_to_table",
               "fetch", "fetchval", "fetchrow"}

# The destination side: code that writes Postgres because Postgres is where
# that table's writer lives, not because it ships a copy out of DuckDB. Each
# value is the reason and, where the reason is a switch the function checks,
# the name the function must evaluate for the reason to be true of it.
# The registered write chains — the destination the registry routes to — are
# exempt by being registered, not by being listed here.
_DESTINATION = {
    ("core/alert_actions.py", "request"): (
        "the alert journal lives in Postgres alone; there is no copy", None),
    ("core/alert_actions.py", "complete"): (
        "the alert journal lives in Postgres alone; there is no copy", None),
    ("core/alert_archive.py", "_write_fired"): (
        "the alert journal lives in Postgres alone; there is no copy", None),
    ("core/alert_archive.py", "_write_resolved"): (
        "the alert journal lives in Postgres alone; there is no copy", None),
    ("core/alert_archive.py", "_write_escalated"): (
        "the alert journal lives in Postgres alone; there is no copy", None),
    ("core/ch_history.py", "ship_history"): (
        "writes ClickHouse's history.*, not Postgres", None),
    ("core/pg_silver.py", "rebuild_silver"): (
        "derives silver.orders inside Postgres; its UPDATE's target is the "
        "dialect's Silver table, which the walk cannot read", "SILVER_TABLE"),
    ("core/pg_vitrina.py", "rebuild_customer_profile"): (
        "derived inside Postgres from Postgres's own Silver", None),
    ("core/pg_vitrina.py", "reconcile_customer_profile"): (
        "derived inside Postgres from Postgres's own Silver", None),
    ("core/pg_sms.py", "replicate_sms"): (
        "moved by its own switch before the registry existed", "sms_store_is_postgres"),
    ("core/pg_dashboard_users.py", "replicate_dashboard_users"): (
        "moved by its own switch before the registry existed", "user_store_is_postgres"),
    ("core/pg_bot_state.py", "replicate_bot_state"): (
        "moved by its own switch before the registry existed", "ENGINE_ENV"),
}


class _Module:
    """One parsed module: its string constants, functions and imports."""

    def __init__(self, path: pathlib.Path):
        self.rel = path.relative_to(CORE.parent).as_posix()
        self.name = ".".join(path.relative_to(CORE.parent).with_suffix("").parts)
        self.tree = ast.parse(path.read_text(encoding="utf-8"))
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


def _executors(modules) -> set:
    """Helpers that execute SQL they are handed (`_write_chunked(conn, sql,
    rows)`): awaiting one is executing, although the text is the caller's."""
    found = set()
    for m in modules.values():
        for name, fn in m.funcs.items():
            params = {a.arg for a in fn.args.args}
            for n in ast.walk(fn):
                if isinstance(n, ast.Await) and isinstance(n.value, ast.Call) \
                        and isinstance(n.value.func, ast.Attribute) \
                        and n.value.func.attr in _PG_EXECUTE and n.value.args \
                        and isinstance(n.value.args[0], ast.Name) \
                        and n.value.args[0].id in params:
                    found.add(name)
    return found


def _pg_write_targets(module, fn, modules, executors=frozenset()) -> set:
    """The `bronze.`/`app.` tables `fn` writes in Postgres, `{?}` for one it
    names only at run time. Empty unless `fn` awaits an asyncpg call, or a
    helper that executes the SQL it is handed."""
    if not _awaited_names(fn) & (_PG_EXECUTE | set(executors)):
        return set()
    found = set()
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
        self.consulting = self._consulting()
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

    def question_names(self) -> set:
        """The names that ask which tables have changed hands:
        `core.write_chains`' `stood_down_*` questions about tables, and every
        helper that answers the same question by asking one of them — named
        for it (`*stood_down*`), so a wrapper such as
        `pg_landing.tables_stood_down` counts and a caller that merely reaches
        a shipper that asks does not."""
        roots = {key for key in self.fns if key[0] == "core/write_chains.py"
                 and key[1].startswith("stood_down") and "sync_keys" not in key[1]}
        names = {key[1] for key in roots}
        while True:
            more = {key[1] for key, called in self.calls.items()
                    if "stood_down" in key[1] and key[1] not in names
                    and called & names}
            if not more:
                return names
            names |= more

    def _consulting(self) -> set:
        """Every function that asks one of those questions itself."""
        names = self.question_names()
        return {key for key, called in self.calls.items() if called & names}

    def writers(self) -> dict:
        """`{(rel, name): targets}` for every Postgres writer of our tables,
        and every caller of the two primitives the plan names."""
        found = {}
        for key, (module, fn) in self.fns.items():
            targets = _pg_write_targets(module, fn, self.modules, self.executors)
            if targets:
                found[key] = targets
        return found

    def covered(self, key, seen=None) -> bool:
        """It asks, or it is reached only from functions that do."""
        seen = set() if seen is None else seen
        if key in self.consulting:
            return True
        if key in seen:
            return False
        seen = seen | {key}
        callers = self.callers.get(key, set())
        return bool(callers) and all(self.covered(c, seen) for c in callers)


@pytest.fixture(scope="module")
def walk():
    return _Walk()


def _chain_modules() -> set:
    from core.write_chains import WRITE_CHAINS

    return {c.__name__.replace(".", "/") + ".py" for c in WRITE_CHAINS}


class TestEveryPostgresWriterAsksTheRegistry:
    def test_each_one_asks_or_is_reached_only_by_askers(self, walk):
        chains = _chain_modules()
        uncovered = sorted(
            key for key in walk.writers()
            if key[0] not in chains and key not in _DESTINATION
            and not walk.covered(key))
        assert not uncovered, (
            "writes bronze.*/app.* in Postgres without asking who owns the "
            f"table, and not only from a function that asks: {uncovered}")

    def test_the_callers_of_the_named_primitives_ask(self, walk):
        """The plan names `pg_buyers._write` and `write_managers`: every
        function that calls either asks, directly or through its callers."""
        for primitive in (("core/pg_buyers.py", "_write"),
                          ("core/pg_replication.py", "write_managers")):
            callers = walk.callers.get(primitive, set())
            assert callers, f"nothing calls {primitive} — the walk is not looking"
            assert all(walk.covered(c) for c in callers), (primitive, callers)

    def test_the_walk_is_not_vacuous(self, walk):
        """A walk that found nothing, or stopped resolving what it found,
        would pass the test above. The sites every chain 3–6 table ships
        through, and at least as many writers as there were on the day this
        was written."""
        writers = walk.writers()
        assert {
            ("core/pg_landing.py", "_write"),
            ("core/pg_landing.py", "write_orders"),
            ("core/pg_buyers.py", "_write"),
            ("core/pg_replication.py", "write_managers"),
            ("core/pg_expense_backfill.py", "backfill_expenses"),
            ("core/pg_operational.py", "replicate_operational"),
            ("core/pg_order_versions.py", "capture_versions"),
        } <= set(writers), sorted(writers)
        assert len(writers) >= 27, sorted(writers)
        assert {
            ("core/pg_landing.py", "_mirror"),
            ("core/pg_buyers.py", "mirror_buyers"),
            ("core/pg_buyers.py", "backfill_buyers"),
            ("core/pg_buyers.py", "hourly_ids_diff"),
            ("core/pg_replication.py", "replicate_managers"),
            ("core/pg_expense_backfill.py", "backfill_expenses"),
            ("core/pg_expense_backfill.py", "hourly_expenses_ids_diff"),
            ("core/duckdb_store.py", "upsert_orders"),
            ("web/routes/api/admin.py", "backfill_mirror_expenses"),
        } <= walk.consulting

    def test_the_exemptions_are_exactly_what_the_walk_finds(self, walk):
        """No stale entry — an exemption for a function that no longer writes
        is where the next writer would hide — and a switch named as the reason
        is one the function actually evaluates."""
        writers = walk.writers()
        assert set(_DESTINATION) <= set(writers), set(_DESTINATION) - set(writers)
        for key, (_reason, switch) in _DESTINATION.items():
            if switch is None:
                continue
            _module, fn = walk.fns[key]
            names = {n.id for n in ast.walk(fn) if isinstance(n, ast.Name)} | \
                _called_names(fn)
            assert switch in names, f"{key} names {switch} as its reason and never reads it"

    def test_the_exempt_modules_are_the_registered_chains(self, walk):
        """The chain writers are the destination the registry routes to, and
        are exempt for being registered — found here, so a chain that stopped
        writing Postgres would not keep an exemption it no longer needs."""
        chains = _chain_modules()
        writing = {key[0] for key in walk.writers()}
        assert chains <= writing, chains - writing

    def test_the_operational_pair_asks_the_checked_form(self):
        """Since DN-01 the hourly shipper and the daily comparison of the
        operational tables ask the form that hands a flag typo out to be
        reported; the other paths ask the narrow one, which logs it."""
        from core import mirror_reconciliation, pg_operational

        for fn in (pg_operational.replicate_operational,
                   mirror_reconciliation.reconcile_operational):
            tree = ast.parse(textwrap.dedent(inspect.getsource(fn)))
            calls = {n.func.id for n in ast.walk(tree)
                     if isinstance(n, ast.Call) and isinstance(n.func, ast.Name)}
            assert "stood_down_tables_checked" in calls, fn.__name__

    def test_no_asker_spells_a_chain(self, walk):
        """The rule has one home: nothing that ships or compares asks a single
        chain's `writes_postgres` in place of the registry."""
        chains = _chain_modules()
        spelled = sorted(
            key for key in walk.consulting
            if key[0] not in chains and key[0] != "core/write_chains.py"
            and walk.calls[key] & {"writes_postgres", "env_writes_postgres"})
        assert not spelled, spelled

    def test_one_that_takes_the_pool_reads_the_owner_rows_too(self, walk):
        """DN-06: anything already holding a Postgres connection stands down
        on either copy of the latch. Walked over every asker — the sync's
        shippers take no pool before they ask, and so are not held to it."""
        owner_reads = {"order_tables_stood_down_or_owned",
                       "tables_stood_down_or_owned", "read_owners"}
        holding = [key for key in walk.consulting if "get_pool" in walk.calls[key]]
        assert len(holding) >= 10, holding
        missing = sorted(key for key in holding if not walk.calls[key] & owner_reads)
        assert not missing, f"holds a pool but reads only the local latch: {missing}"


_SPEC_TYPES = {"MirroredTable", "BucketedTable"}
# Reading a spec's Postgres copy, or comparing the two: what makes a function
# a comparison rather than a reader of DuckDB's side.
_PG_COMPARE = {"fetch_pg_rows", "pg_fingerprints", "_read_pg_bucket",
               "compare_table", "compare_bucket"}


def _spec_tables(modules) -> dict:
    """`{(module name, binding): {pg_table, ...}}` for every module-level spec,
    tuple of specs, and spec-building function in the walked tree."""
    found: dict = {}
    for m in modules.values():
        def tables_in(node):
            out = set()
            for n in ast.walk(node):
                if isinstance(n, ast.Call) and isinstance(n.func, ast.Name) \
                        and n.func.id in _SPEC_TYPES:
                    for kw in n.keywords:
                        if kw.arg == "pg_table":
                            value = m.value(kw.value, modules)
                            if value:
                                out.add(value)
                elif isinstance(n, ast.Name) and (m.name, n.id) in found:
                    out |= found[(m.name, n.id)]
            return out

        for node in m.tree.body:
            if isinstance(node, (ast.Assign, ast.AnnAssign)) and node.value is not None:
                targets = node.targets if isinstance(node, ast.Assign) else [node.target]
                tables = tables_in(node.value)
                for t in targets:
                    if isinstance(t, ast.Name) and tables:
                        found[(m.name, t.id)] = tables
            elif isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                tables = {v for n in ast.walk(node) if isinstance(n, ast.Call)
                          and isinstance(n.func, ast.Name) and n.func.id in _SPEC_TYPES
                          for kw in n.keywords if kw.arg == "pg_table"
                          for v in [m.value(kw.value, modules)] if v}
                if tables:
                    found[(m.name, node.name)] = tables
    return found


def _comparisons(walk) -> dict:
    """`{(rel, name): tables}` for every function that compares a spec naming
    a `bronze.`/`app.` table against its Postgres copy."""
    specs = _spec_tables(walk.modules)
    out = {}
    for key, (m, fn) in walk.fns.items():
        if not walk.calls[key] & _PG_COMPARE:
            continue
        tables = set()
        for n in ast.walk(fn):
            if isinstance(n, ast.Name):
                if (m.name, n.id) in specs:
                    tables |= specs[(m.name, n.id)]
                elif n.id in m.imports:
                    src, attr = m.imports[n.id]
                    tables |= specs.get((src, attr), set())
        ours = {t for t in tables if t.startswith(("bronze.", "app."))}
        if ours:
            out[key] = ours
    return out


# Comparisons of tables whose writer moved by a switch of its own before the
# registry existed: each stands down on that switch, which it must evaluate.
_OWN_SWITCH_COMPARISONS = {
    ("core/mirror_reconciliation.py", "reconcile_bot_state"): "ENGINE_ENV",
    ("core/mirror_reconciliation.py", "reconcile_sms"): "sms_store_is_postgres",
    ("core/mirror_reconciliation.py", "reconcile_dashboard_users"): "user_store_is_postgres",
}


def _evaluates(walk, key, name) -> bool:
    _module, fn = walk.fns[key]
    return name in ({n.id for n in ast.walk(fn) if isinstance(n, ast.Name)}
                    | _called_names(fn))


class TestEveryComparisonAsksTheRegistry:
    """The other half of the rule: a comparison that went on comparing a
    table its shipper stopped feeding would file every row the chain writes
    as a discrepancy the check itself created. Found by what a function does
    — reads a spec's Postgres copy — and which tables its specs name."""

    def test_each_one_asks(self, walk):
        missing = sorted(key for key in _comparisons(walk)
                         if key not in walk.consulting
                         and key not in _OWN_SWITCH_COMPARISONS)
        assert not missing, f"compares bronze.*/app.* without asking: {missing}"

    def test_the_walk_is_not_vacuous(self, walk):
        found = _comparisons(walk)
        assert {("core/mirror_reconciliation.py", name) for name in (
            "reconcile_mirror", "reconcile_orders", "reconcile_expenses",
            "reconcile_buyers", "reconcile_operational",
        )} <= set(found), sorted(found)
        assert {"bronze.products", "bronze.managers", "bronze.buyers",
                "bronze.expenses", "bronze.orders"} <= set().union(*found.values())

    def test_the_own_switch_exemptions_are_found_and_read_their_switch(self, walk):
        found = _comparisons(walk)
        for key, switch in _OWN_SWITCH_COMPARISONS.items():
            assert key in found, f"stale exemption: {key}"
            assert _evaluates(walk, key, switch), f"{key} never reads {switch}"


class TestTheShippingUnitsAreTheWritersOwn:
    """A shipper stands a unit down whole because it writes the unit in one
    transaction. The units are derived here from what each writer writes, and
    `pg_landing.shipping_units()` must be exactly those — so a writer that
    starts writing a second table in the same transaction becomes a unit the
    stand-down knows about, or this fails."""

    def _derived(self, walk) -> set:
        chains = _chain_modules()
        units = set()
        for key, targets in walk.writers().items():
            if key[0] in chains or key in _DESTINATION:
                continue
            concrete = frozenset(t for t in targets if not t.startswith("{"))
            if len(concrete) > 1:
                units.add(concrete)
        return units

    def test_the_declared_units_are_the_derived_ones(self, walk):
        from core.pg_landing import shipping_units

        derived = self._derived(walk)
        assert len(derived) >= 3, derived
        assert {frozenset(u) for u in shipping_units()} == derived

    def test_no_registered_chain_splits_a_unit(self):
        """DN-22a's order rule, for every unit: whoever takes one table of a
        unit takes the rest of it."""
        from core.pg_landing import shipping_units
        from core.write_chains import WRITE_CHAINS, chain_name

        split = [(chain_name(c), unit) for c in WRITE_CHAINS
                 for unit in shipping_units()
                 if set(unit) & set(c.CHAIN_TABLES)
                 and not set(unit) <= set(c.CHAIN_TABLES)]
        assert split == []


@pytest.fixture
def flags(monkeypatch):
    for env in ("KS_WRITE_INVENTORY", "KS_WRITE_EXPENSES", "KS_WRITE_GOALS",
                "KS_WRITE_EXPENSE_TYPES"):
        monkeypatch.delenv(env, raising=False)
    return monkeypatch


class TestWhatStandsDown:
    def test_nothing_while_every_chain_writes_duckdb(self, flags):
        from core.write_chains import stood_down_tables
        assert stood_down_tables() == frozenset()

    def test_only_the_expenses_table_when_only_chain_8_is_on(self, flags):
        from core.write_chains import stood_down_tables
        flags.setenv("KS_WRITE_EXPENSES", "postgres")
        assert stood_down_tables() == frozenset({"app.manual_expenses"})

    def test_the_union_when_both_are_on(self, flags):
        from core import pg_inventory_write
        from core.write_chains import stood_down_tables
        flags.setenv("KS_WRITE_EXPENSES", "postgres")
        flags.setenv("KS_WRITE_INVENTORY", "postgres")
        assert stood_down_tables() == (
            frozenset(pg_inventory_write.CHAIN_TABLES) | {"app.manual_expenses"})

    def test_an_unknown_expenses_value_raises(self, flags):
        from core.pg_expenses_write import writes_postgres
        flags.setenv("KS_WRITE_EXPENSES", "postgre")
        with pytest.raises(RuntimeError):
            writes_postgres()

    def test_the_expenses_table_is_one_the_shipper_actually_replaces(self):
        """Standing down a table the shipper never touched would be a no-op that
        reads as a guarantee."""
        from core.pg_expenses_write import CHAIN_TABLES
        from core.pg_operational import _FULL_REPLACE
        assert set(CHAIN_TABLES) <= {pg for pg, _d, _c, _o in _FULL_REPLACE}


# ─── DN-22a: the order write path asks the registry ──────────────────────────
#
# No chain declares an order table today, so every assertion below that sees
# the path stand down needs a fake chain 3 — the shape `test_chain_invariants`
# already uses for a third chain. Each "it ships nothing" test has a sibling
# without the fake chain that watches the same recorder fill up: a recorder that
# could not see a write would pass every stand-down test and prove nothing.

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
        if "meta.chain_watermarks" in sql:
            # The audit copy of the latch (`chain_latch.read_owners`).
            if self.recorder.owner_error is not None:
                raise self.recorder.owner_error
            return [{"key": f"owner:{table}", "value": at}
                    for table, at in self.recorder.owner_rows.items()]
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
        self.acquired = 0
        # `{table: latched_at}` as `meta.chain_watermarks` holds it, and what
        # reading it raises instead, when set.
        self.owner_rows = {}
        self.owner_error = None

    def acquire(self):
        self.acquired += 1
        return _Ctx(_Conn(self))

    def wrote(self, table: str) -> bool:
        return any(table in s and "INSERT INTO" in s for s in self.sql)

    def only_asked_who_owns(self) -> bool:
        """Read the owner rows and nothing else: no order table, no watermark,
        no write — and so no `app.order_versions` insert either."""
        return bool(self.sql) and all("meta.chain_watermarks" in s for s in self.sql)


@pytest.fixture
def pool(flags):
    """The recording pool behind `core.pg.get_pool`, with the mirror on."""
    from unittest.mock import AsyncMock

    from core import pg_landing

    flags.delenv(pg_landing.MIRROR_ENV, raising=False)
    pg_landing.reset_health()
    recorder = _RecordingPool()
    flags.setattr("core.pg.get_pool", AsyncMock(return_value=recorder))
    flags.setattr("core.pg.require_revision", AsyncMock())
    yield recorder
    pg_landing.reset_health()


def _never_reached_postgres(pool) -> None:
    """The local stand-down asked Postgres nothing at all. A pool that was
    never acquired is not enough to say so: `get_pool()` hands back the pool
    without acquiring anything, and `require_revision()` is patched out here,
    so either could run before the refusal and leave `acquired` at zero."""
    from core import pg

    assert pool.acquired == 0
    pg.get_pool.assert_not_awaited()
    pg.require_revision.assert_not_awaited()


@pytest.fixture
def order_chain(flags):
    """Register a fake chain that owns `tables`; its flag is `env()`."""
    import types

    from core import write_chains

    def register(tables=(ORDERS, LINES), env=lambda: True):
        fake = types.ModuleType("core.pg_orders_write")
        fake.WRITE_ENV = "KS_WRITE_ORDERS"
        fake.CHAIN_TABLES = tuple(tables)
        fake.env_writes_postgres = env
        flags.setattr(write_chains, "WRITE_CHAINS",
                      write_chains.WRITE_CHAINS + (fake,))
        return fake

    return register


def _rolled_back(flags) -> None:
    """An image rolled back to a build older than the orders chain: no chain
    registered here declares an order table. Made so rather than assumed, so
    the case keeps meaning this once the orders chain is registered for real —
    a precondition on this build's registry would fail those tests then, and a
    test with none would quietly pass through `claimed_tables` instead."""
    from core import write_chains

    flags.setattr(write_chains, "WRITE_CHAINS", tuple(
        c for c in write_chains.WRITE_CHAINS
        if not {ORDERS, LINES} & set(c.CHAIN_TABLES)))


async def _store(tmp_path, ids=()):
    """A DuckDB holding `ids`, written with the mirror patched out."""
    from unittest.mock import AsyncMock, patch

    from core.duckdb_store import DuckDBStore

    store = DuckDBStore(db_path=tmp_path / "dn22a.duckdb")
    await store.connect()
    if ids:
        with patch("core.pg_landing.mirror_orders", new=AsyncMock()):
            await store.upsert_orders([_order(i) for i in ids])
    return store


def _chains_splitting_the_order_tables(chains) -> list:
    """The chains that declare one order table and not the other."""
    from core.write_chains import chain_name

    both = {ORDERS, LINES}
    return [chain_name(c) for c in chains
            if both & set(c.CHAIN_TABLES) and not both <= set(c.CHAIN_TABLES)]


class TestOwnershipOfTheOrderTablesPassesAsAUnit:
    """Why either order table stands both down. Not that they ship together —
    `write_orders(replace_products=False)` ships headers alone every day, from
    the 05:15 status refresh and the comment ship — but that whoever takes the
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


class TestTheOrderTablesAskOnlyTheirOwnChain:
    def test_nothing_stands_down_in_production_today(self, flags):
        """KS_WRITE_EXPENSES=postgres is live; its table is not an order table."""
        from core.pg_landing import order_tables_stood_down

        flags.setenv("KS_WRITE_EXPENSES", "postgres")
        assert order_tables_stood_down() == frozenset()

    def test_a_chain_that_declares_no_order_table_is_not_even_asked(
            self, flags, order_chain, caplog):
        """DN-01's rule, carried from sync keys to tables: an unrelated typo is
        not read, so the sync cannot log it once a minute."""
        from core.pg_landing import order_tables_stood_down

        asked = []
        order_chain(tables=("app.something_else",),
                    env=lambda: asked.append(1) or True)
        flags.setenv("KS_WRITE_EXPENSES", "postgre")
        with caplog.at_level("ERROR", logger="core.write_chains"):
            assert order_tables_stood_down() == frozenset()
        assert asked == []
        assert not caplog.records

    @pytest.mark.parametrize("tables", [(ORDERS,), (LINES,), (ORDERS, LINES)])
    def test_a_chain_on_either_table_is_seen(self, order_chain, tables):
        from core.pg_landing import order_tables_stood_down

        order_chain(tables=tables)
        assert order_tables_stood_down() == frozenset(tables)

    def test_the_owning_chains_typo_stands_it_down_and_does_not_raise(
            self, order_chain, caplog):
        from core.pg_landing import order_tables_stood_down

        def typo():
            raise RuntimeError("KS_WRITE_ORDERS='postgre' is not understood")

        order_chain(env=typo)
        with caplog.at_level("ERROR", logger="core.write_chains"):
            assert order_tables_stood_down() == frozenset({ORDERS, LINES})
        assert "postgre" in caplog.text

    def test_a_latched_owning_chain_stands_down_whatever_its_flag(self, order_chain):
        from core import chain_latch
        from core.pg_landing import order_tables_stood_down

        order_chain(env=lambda: False)
        chain_latch.MARKER_DIR.mkdir(parents=True, exist_ok=True)
        chain_latch.marker_path("pg_orders_write").write_text(
            '{"latched_at": "2026-09-18T12:00:00+00:00"}', encoding="utf-8")
        chain_latch.load()
        assert order_tables_stood_down() == frozenset({ORDERS, LINES})

    @pytest.mark.parametrize("config", ["none", "expenses", "inventory", "orders",
                                        "orders_typo", "orders_off"])
    def test_it_is_the_shippers_answer_narrowed(self, flags, order_chain, config):
        """One rule, two doors: the narrow question may never disagree with
        the whole one about a table they both name."""
        from core import pg_inventory_write
        from core.write_chains import stood_down_among, stood_down_tables

        def typo():
            raise RuntimeError("not understood")

        if config == "expenses":
            flags.setenv("KS_WRITE_EXPENSES", "postgres")
        elif config == "inventory":
            flags.setenv("KS_WRITE_INVENTORY", "postgres")
        elif config == "orders":
            order_chain()
        elif config == "orders_typo":
            order_chain(env=typo)
        elif config == "orders_off":
            order_chain(env=lambda: False)

        for asked in ({ORDERS, LINES}, {"app.manual_expenses", ORDERS},
                      set(pg_inventory_write.CHAIN_TABLES)):
            assert stood_down_among(asked) == stood_down_tables() & asked, asked


class TestTheSyncMirror:
    @pytest.mark.asyncio
    async def test_without_a_chain_the_recorder_sees_the_rows_and_the_archive(
            self, pool, tmp_path):
        """The control. Nothing below means anything unless this recorder can
        see a mirror write and a version capture when they happen."""
        from core.pg_order_versions import TABLE

        store = await _store(tmp_path)
        await store.upsert_orders([_order(1)])
        assert pool.wrote(ORDERS) and pool.wrote(LINES)
        assert pool.wrote(TABLE)

    @pytest.mark.asyncio
    @pytest.mark.parametrize("tables", [(ORDERS,), (LINES,)])
    async def test_either_table_owned_ships_nothing_at_all(
            self, pool, order_chain, tmp_path, tables):
        """Ownership of the order tables passes as a unit, so a chain on either
        one stops both — and DuckDB's own write is untouched. No real chain may
        declare only one (`TestOwnershipOfTheOrderTablesPassesAsAUnit`); this
        is the stand-down holding if one ever did."""
        order_chain(tables=tables)
        store = await _store(tmp_path)
        result = await store.upsert_orders([_order(1)])

        assert result.changed_ids == [1]
        assert pool.acquired == 0 and pool.sql == []
        async with store.connection() as conn:
            assert conn.execute("SELECT count(*) FROM orders").fetchone()[0] == 1

    @pytest.mark.asyncio
    async def test_a_status_refresh_ships_nothing_either(
            self, pool, order_chain, tmp_path):
        """The 05:15 shape: force_update, headers only — the path that would
        otherwise archive 1,400 forced rewrites against the chain's rows."""
        store = await _store(tmp_path, [1])
        order_chain()
        result = await store.upsert_orders(
            [_order(1)], force_update=True, skip_products=True)
        assert result.changed_ids == [1]
        assert pool.acquired == 0


class TestTheBackfillAndItsRepair:
    @pytest.mark.asyncio
    async def test_without_a_chain_it_writes(self, pool, tmp_path):
        from core.pg_backfill import backfill_orders
        from core.pg_order_versions import TABLE

        store = await _store(tmp_path, [1, 2])
        result = await backfill_orders(store)
        assert result["orders_shipped"] == 2
        assert pool.wrote(ORDERS) and pool.wrote(TABLE)

    @pytest.mark.asyncio
    async def test_the_ids_diff_and_the_header_only_repair_refuse_before_postgres(
            self, pool, order_chain, tmp_path):
        from core.pg_backfill import backfill_orders

        store = await _store(tmp_path, [1, 2])
        order_chain()
        with pytest.raises(RuntimeError, match="write chain"):
            await backfill_orders(store)
        _never_reached_postgres(pool)

    @pytest.mark.asyncio
    async def test_the_hourly_diff_stands_down_quietly(
            self, pool, order_chain, tmp_path, caplog):
        """A decision, not a fault: no ERROR every hour."""
        from core.pg_backfill import hourly_orders_ids_diff

        store = await _store(tmp_path, [1])
        order_chain(tables=(LINES,))
        with caplog.at_level("ERROR"):
            result = await hourly_orders_ids_diff(store)
        assert result == {"stood_down": [LINES]}
        _never_reached_postgres(pool)
        assert not [r for r in caplog.records if r.levelname == "ERROR"]


class TestTheCommentShip:
    @pytest.mark.asyncio
    async def test_without_a_chain_it_writes_a_backfill_version(self, pool, tmp_path):
        from core.pg_backfill import ship_orders_by_id
        from core.pg_order_versions import BACKFILL, TABLE

        store = await _store(tmp_path, [1])
        result = await ship_orders_by_id(store, [1], version_kind=BACKFILL)
        assert result["orders_shipped"] == 1
        assert pool.wrote(ORDERS) and pool.wrote(TABLE)

    @pytest.mark.asyncio
    async def test_it_is_skipped_so_the_callers_duckdb_half_still_runs(
            self, pool, order_chain, tmp_path):
        from core.pg_backfill import ship_orders_by_id
        from core.pg_order_versions import BACKFILL

        store = await _store(tmp_path, [1])
        order_chain()
        result = await ship_orders_by_id(store, [1], version_kind=BACKFILL)
        assert result["orders_shipped"] == 0
        assert result["stood_down"] == [LINES, ORDERS]
        assert "write chain" in result["skipped"]
        _never_reached_postgres(pool)


class _NoStore:
    """A DuckDB the comparison must not open once it has stood down."""

    def connection(self):  # pragma: no cover - the assertion is the point
        raise AssertionError("the stood-down comparison read DuckDB")


class TestTheBucketComparison:
    @pytest.mark.asyncio
    async def test_it_files_info_for_both_tables_and_reads_neither_store(
            self, pool, order_chain):
        from core.data_quality import Severity
        from core.mirror_reconciliation import reconcile_orders

        order_chain(tables=(ORDERS,))
        issues = await reconcile_orders(_NoStore())

        assert [(i.check_name, i.table_name, i.severity) for i in issues] == [
            ("mirror_stood_down", ORDERS, Severity.INFO),
            ("mirror_stood_down", LINES, Severity.INFO),
        ]
        assert ORDERS in issues[0].description
        _never_reached_postgres(pool)

    @pytest.mark.asyncio
    async def test_without_a_chain_it_compares(self, pool, tmp_path):
        """The control: the same call reads both stores and files no stand-down."""
        from core.mirror_reconciliation import reconcile_orders

        store = await _store(tmp_path, [1])
        issues = await reconcile_orders(store)
        assert pool.acquired > 0
        assert "mirror_stood_down" not in {i.check_name for i in issues}

    def test_its_lever_is_not_the_generic_mirror_advice(self):
        """`mirror_` says "wait for the re-ship", which is the one thing the
        stand-down exists to prevent."""
        from core.data_quality import remediation_for

        assert remediation_for(["mirror_stood_down"]) != remediation_for(
            ["mirror_missing_rows"])
        assert "never backfill" in remediation_for(["mirror_stood_down"])[0].lower()


class TestTheOwnerRowsStandTheOrderTablesDownToo:
    """DN-06's rule on the order paths that already hold a pool: stand down on
    either copy of the latch. The marker is the copy a lost `./data` loses;
    the owner row in Postgres is the one that survives it, beside the rows the
    chain wrote. Every case below has the flag at `duckdb` and NO marker, so
    the local answer is empty and only the owner row can say the tables moved.
    Each "writes nothing" is watched by the same recorder whose controls above
    see the rows and the version capture when they do happen."""

    @pytest.fixture
    def owned(self, pool, order_chain):
        from core import chain_latch
        from core.pg_landing import order_tables_stood_down

        order_chain(env=lambda: False)
        pool.owner_rows = {ORDERS: "2026-09-20T08:00:00+00:00"}
        assert not chain_latch.latched("pg_orders_write"), "the marker must be absent"
        assert order_tables_stood_down() == frozenset(), "the local answer must be empty"
        return pool

    @pytest.mark.asyncio
    async def test_one_owner_row_holds_both_order_tables(self, owned):
        """Ownership passes for a chain as a unit (`claimed_tables`)."""
        from core.pg_landing import order_tables_stood_down_or_owned

        assert await order_tables_stood_down_or_owned(owned) == frozenset({ORDERS, LINES})
        assert owned.only_asked_who_owns()

    @pytest.mark.asyncio
    async def test_another_chains_owner_row_is_not_an_order_table(self, pool):
        from core.pg_landing import order_tables_stood_down_or_owned

        pool.owner_rows = {"app.manual_expenses": "2026-09-20T08:00:00+00:00"}
        assert await order_tables_stood_down_or_owned(pool) == frozenset()

    @pytest.mark.asyncio
    @pytest.mark.parametrize("row", [ORDERS, LINES])
    async def test_an_owner_row_no_registered_chain_declares_still_holds_both(
            self, flags, pool, row):
        """An image rolled back to a build older than the orders chain: the
        owner row names a table no chain here declares, so `claimed_tables`
        alone would drop it and hand the tables back to DuckDB. Either order
        table owned holds both, the unit rule with no chain left to say it."""
        from core.pg_landing import (
            order_tables_stood_down, order_tables_stood_down_or_owned,
        )

        _rolled_back(flags)
        pool.owner_rows = {row: "2026-09-20T08:00:00+00:00"}
        assert order_tables_stood_down() == frozenset()
        assert await order_tables_stood_down_or_owned(pool) == frozenset({ORDERS, LINES})
        assert pool.only_asked_who_owns()

    @pytest.mark.asyncio
    async def test_after_a_rollback_the_backfill_still_refuses(
            self, flags, pool, tmp_path):
        """The same case through a path: nothing shipped over the chain's rows."""
        from core.pg_backfill import backfill_orders

        _rolled_back(flags)
        pool.owner_rows = {ORDERS: "2026-09-20T08:00:00+00:00"}
        store = await _store(tmp_path, [1, 2])
        with pytest.raises(RuntimeError, match="write chain"):
            await backfill_orders(store)
        assert pool.only_asked_who_owns()

    @pytest.mark.asyncio
    async def test_the_local_answer_is_still_part_of_it(self, pool, order_chain):
        from core.pg_landing import order_tables_stood_down_or_owned

        order_chain()          # the flag says postgres; no owner row yet
        assert await order_tables_stood_down_or_owned(pool) == frozenset({ORDERS, LINES})

    @pytest.mark.asyncio
    async def test_an_unreadable_owner_row_is_not_an_absent_one(self, pool, order_chain):
        """No new failure policy: the read raises through, as it does in
        `replicate_operational` and `reconcile_operational`."""
        from core.pg_landing import order_tables_stood_down_or_owned

        order_chain(env=lambda: False)
        pool.owner_error = RuntimeError("meta.chain_watermarks unreadable")
        with pytest.raises(RuntimeError, match="unreadable"):
            await order_tables_stood_down_or_owned(pool)

    @pytest.mark.asyncio
    async def test_the_backfill_refuses_and_writes_nothing(self, owned, tmp_path):
        from core import pg
        from core.pg_backfill import backfill_orders

        store = await _store(tmp_path, [1, 2])
        with pytest.raises(RuntimeError, match="write chain"):
            await backfill_orders(store)
        assert owned.only_asked_who_owns()
        pg.require_revision.assert_awaited()

    @pytest.mark.asyncio
    async def test_the_hourly_diff_stands_down_quietly(self, owned, tmp_path, caplog):
        from core.pg_backfill import hourly_orders_ids_diff

        store = await _store(tmp_path, [1])
        with caplog.at_level("ERROR"):
            result = await hourly_orders_ids_diff(store)
        assert result == {"stood_down": [LINES, ORDERS]}
        assert owned.only_asked_who_owns()
        assert not [r for r in caplog.records if r.levelname == "ERROR"]

    @pytest.mark.asyncio
    async def test_an_unreadable_owner_row_is_the_hourly_diffs_error_not_a_ship(
            self, pool, order_chain, tmp_path):
        """The job's own contract — returned, never raised — and nothing
        shipped on the strength of a read that failed."""
        from core.pg_backfill import hourly_orders_ids_diff

        order_chain(env=lambda: False)
        pool.owner_error = RuntimeError("meta.chain_watermarks unreadable")
        store = await _store(tmp_path, [1])
        result = await hourly_orders_ids_diff(store)
        assert "unreadable" in result["error"]
        assert pool.only_asked_who_owns()

    @pytest.mark.asyncio
    async def test_the_comment_ship_is_skipped_and_writes_nothing(self, owned, tmp_path):
        from core.pg_backfill import ship_orders_by_id
        from core.pg_order_versions import BACKFILL

        store = await _store(tmp_path, [1])
        result = await ship_orders_by_id(store, [1], version_kind=BACKFILL)
        assert result["orders_shipped"] == 0
        assert result["stood_down"] == [LINES, ORDERS]
        assert "write chain" in result["skipped"]
        assert owned.only_asked_who_owns()

    @pytest.mark.asyncio
    async def test_the_bucket_comparison_pages_and_reads_no_order_table(self, owned):
        """Not the INFO stand-down: on the owner rows alone the sync's mirror
        is still shipping, so this is a CRITICAL — one, for both tables."""
        from core.data_quality import Severity
        from core.mirror_reconciliation import reconcile_orders

        issues = await reconcile_orders(_NoStore())
        assert [(i.check_name, i.table_name, i.severity, i.count)
                for i in issues] == [
            ("order_owner_row_without_marker", f"{LINES}, {ORDERS}",
             Severity.CRITICAL, 2),
        ]
        assert owned.only_asked_who_owns()

    @pytest.mark.asyncio
    @pytest.mark.parametrize("path", [
        "backfill_orders", "hourly_orders_ids_diff", "reconcile_orders",
        "ship_orders_by_id",
    ])
    async def test_a_schema_behind_the_code_breaks_before_the_owner_read(
            self, owned, path):
        """The owner rows live in `meta.chain_watermarks` (revision 0032), so
        each path asks `require_revision()` first and a database behind the
        code breaks as `SchemaVersionError` — never as a missing table, and
        never as a stand-down read off a schema nobody verified. Here the owner
        row is present, so a path that read it first would stand down or skip
        instead of surfacing the error; and a path that dropped the revision
        check altogether would do the same. `assert_awaited` alone could see
        neither: the patched check passes in any position."""
        from core import pg
        from core.mirror_reconciliation import reconcile_orders
        from core.pg_backfill import (
            backfill_orders, hourly_orders_ids_diff, ship_orders_by_id,
        )
        from core.pg_order_versions import BACKFILL

        pg.require_revision.side_effect = pg.SchemaVersionError(
            "database at 0031, code wants 0033")
        calls = {
            "backfill_orders": lambda: backfill_orders(_NoStore()),
            "hourly_orders_ids_diff": lambda: hourly_orders_ids_diff(_NoStore()),
            "reconcile_orders": lambda: reconcile_orders(_NoStore()),
            "ship_orders_by_id": lambda: ship_orders_by_id(
                _NoStore(), [1], version_kind=BACKFILL),
        }
        if path == "hourly_orders_ids_diff":
            # The job's never-raises contract: the error is returned.
            result = await calls[path]()
            assert result.get("error", "").startswith("SchemaVersionError"), result
        else:
            with pytest.raises(pg.SchemaVersionError):
                await calls[path]()
        pg.require_revision.assert_awaited_once()
        assert owned.sql == [] and owned.acquired == 0, owned.sql


class TestTheStandDownFindingSaysWhetherTheSyncStillShips:
    """The finding is read by the one person who could act, so its claim
    about the sync's mirror is parsed out of it and checked against what the
    mirror then does with the same recorder. On the local answer the mirror
    has stopped; on the owner rows alone it has not — it asks only the local
    answer — and a finding saying it had would read as nothing to do."""

    @staticmethod
    def _claim(issues) -> str:
        """The one description the stand-down carries: the INFO on both
        tables on the local answer, the one CRITICAL on the owner rows."""
        from core.data_quality import Severity

        shape = [(i.check_name, i.table_name, i.severity) for i in issues]
        assert shape in (
            [("mirror_stood_down", ORDERS, Severity.INFO),
             ("mirror_stood_down", LINES, Severity.INFO)],
            [("order_owner_row_without_marker", f"{LINES}, {ORDERS}",
              Severity.CRITICAL)],
        ), shape
        (text,) = {i.description for i in issues}
        stopped = "no longer ships DuckDB's copy" in text
        shipping = "the sync mirror is still shipping" in text
        assert stopped != shipping, f"says neither or both: {text}"
        return text

    @pytest.mark.asyncio
    @pytest.mark.parametrize("state", ["flagged", "marker_lost", "rolled_back"])
    async def test_the_claim_is_what_the_sync_mirror_does(
            self, flags, pool, order_chain, tmp_path, state):
        from core.mirror_reconciliation import reconcile_orders

        if state == "flagged":
            order_chain()                       # the local answer names both
        else:
            if state == "marker_lost":
                order_chain(env=lambda: False)  # declared, flag duckdb, no marker
            else:
                _rolled_back(flags)             # nothing here declares them
            pool.owner_rows = {ORDERS: "2026-09-20T08:00:00+00:00"}
        issues = await reconcile_orders(_NoStore())
        text = self._claim(issues)

        pool.sql.clear()
        store = await _store(tmp_path)
        await store.upsert_orders([_order(9)])
        assert pool.wrote(ORDERS) == ("the sync mirror is still shipping" in text), text
        # And the severity says the same thing the text does: a page exactly
        # when the mirror is still writing over the chain's rows.
        assert pool.wrote(ORDERS) == (issues[0].severity.value == "CRITICAL")

        if state == "flagged":
            assert "owner row" not in text
        elif state == "marker_lost":
            assert ("the marker is missing; the sync mirror is still shipping — "
                    "restore data/write-chain-owners or run "
                    "scripts/chain_copy_back.py") in text
        else:
            assert "the marker is missing" not in text
            assert "no chain in this build declares the order tables" in text


async def _persisted(store, issues):
    """The run as the 07:30 job writes it, read back the way the digest and
    `/api/health/data-quality` read it: `(run row, issue rows)`."""
    from datetime import datetime, timezone

    from core.data_quality import fetch_latest_run, fetch_run_issues, persist_run
    from core.mirror_reconciliation import MIRROR_LAYER

    now = datetime.now(timezone.utc)
    async with store.connection() as conn:
        run_id = persist_run(
            conn, started_at=now, ended_at=now, as_of=now,
            window_start=now.date(), window_end=now.date(),
            layer=MIRROR_LAYER, issues=issues, discrepancies=[],
        )
        return fetch_latest_run(conn, MIRROR_LAYER), fetch_run_issues(conn, run_id)


def _what_a_human_reads(run, rows) -> str:
    """Every surface that reaches a person prints the check's label, the count
    and — on a page — the lever, and never the description: the page the job
    sends when the run is CRITICAL, and the digest's one line per finding.
    Rebuilt from the persisted rows, not from the objects that were written."""
    from core.data_quality import (
        IntegrityIssue, Severity, format_alert_message, human_check_name,
    )
    from core.mirror_reconciliation import MIRROR_LAYER

    issues = [IntegrityIssue(
        check_name=r["check_name"], table_name=r["table_name"],
        severity=Severity(r["severity"]), count=r["count"],
        description=r["description"]) for r in rows]
    digest = [f"• {human_check_name(r['check_name'])}: {r['count']}" for r in rows]
    page = (format_alert_message(MIRROR_LAYER, Severity.CRITICAL, issues, [])
            if run["status"] == "CRITICAL" else "")
    return "\n".join([page, *digest])


class TestWhatTheStandDownTellsAHuman:
    """DN-22a's review: with only the owner rows standing the order tables
    down, the finding stayed INFO — and its label, its lever and its registry
    entry all said "not a defect", while the per-tick sync mirror, which asks
    only the local answer, went on writing DuckDB's copy over the chain's rows.
    Pages and the digest print the label and the lever, never the description
    that did say so. Each state is run through `reconcile_orders`, persisted
    as the 07:30 job persists it, and judged from the rows read back."""

    STAND_DOWN = {"mirror_stood_down", "order_owner_row_without_marker"}

    @pytest.mark.asyncio
    async def test_no_chain_owns_the_order_tables_files_nothing_about_them(
            self, pool, tmp_path):
        """Production today: no chain declares an order table and no owner row
        names one. The comparison runs, and no stand-down of either kind and
        no CRITICAL is persisted."""
        from core.mirror_reconciliation import reconcile_orders

        store = await _store(tmp_path, [1])
        run, rows = await _persisted(store, await reconcile_orders(store))

        assert pool.acquired > 0, "the comparison did not run — nothing proved"
        assert not self.STAND_DOWN & {r["check_name"] for r in rows}, rows
        assert run["critical_count"] == 0 and run["status"] != "CRITICAL", run

    @pytest.mark.asyncio
    @pytest.mark.parametrize("state", ["marker_present", "flagged"])
    async def test_on_the_local_answer_it_stays_info(
            self, pool, order_chain, tmp_path, state):
        """The marker is present (flag back at duckdb, both copies agree), or
        the flag says postgres: the sync's mirror has stopped on the same
        answer, and the stand-down is a decision — INFO, no page."""
        from core import chain_latch
        from core.mirror_reconciliation import reconcile_orders

        if state == "marker_present":
            order_chain(env=lambda: False)
            chain_latch.latch("pg_orders_write")
            pool.owner_rows = {ORDERS: "2026-09-20T08:00:00+00:00"}
        else:
            order_chain()
        store = await _store(tmp_path)
        run, rows = await _persisted(store, await reconcile_orders(store))

        assert sorted((r["check_name"], r["table_name"], r["severity"])
                      for r in rows) == [
            ("mirror_stood_down", LINES, "INFO"),
            ("mirror_stood_down", ORDERS, "INFO"),
        ]
        assert run["status"] == "PASS" and run["critical_count"] == 0, run
        assert "order_owner_row_without_marker" not in _what_a_human_reads(run, rows)

    @pytest.mark.asyncio
    @pytest.mark.parametrize("road", ["marker_lost", "rolled_back"])
    async def test_on_the_owner_rows_alone_it_pages_and_never_says_not_a_defect(
            self, flags, pool, order_chain, tmp_path, road):
        from core.mirror_reconciliation import reconcile_orders
        from core.pg_landing import order_tables_stood_down

        if road == "marker_lost":
            order_chain(env=lambda: False)      # declared, flag duckdb, no marker
        else:
            _rolled_back(flags)                 # nothing here declares them
        pool.owner_rows = {ORDERS: "2026-09-20T08:00:00+00:00"}
        assert order_tables_stood_down() == frozenset()
        store = await _store(tmp_path)
        run, rows = await _persisted(store, await reconcile_orders(store))

        assert [(r["check_name"], r["table_name"], r["severity"], r["count"])
                for r in rows] == [
            ("order_owner_row_without_marker", f"{LINES}, {ORDERS}", "CRITICAL", 2),
        ]
        assert run["status"] == "CRITICAL" and run["critical_count"] == 1, run

        read = _what_a_human_reads(run, rows)
        assert "not a defect" not in read.lower(), read
        assert "the sync is overwriting order tables a write chain owns" in read
        (lever,) = [line for line in read.splitlines() if line.startswith("→ ")]
        assert "scripts/chain_copy_back.py" in lever
        assert "data/write-chain-owners" in lever

    def test_the_registry_says_it_does_not_clear_by_itself(self):
        """The REGISTRY comment on `mirror_stood_down` reads "a decision
        somebody took, not a fault"; the owner-rows finding is the fault, and
        its entry must not inherit that — nor clear on a job."""
        from core.alerting import REGISTRY, Kind

        spec = REGISTRY["order_owner_row_without_marker"]
        assert spec.kind is Kind.CONDITION
        assert "a human, not a job" in spec.clears


def _admin_client(flags):
    """A TestClient signed in as a hardcoded admin."""
    import time as _time

    from fastapi.testclient import TestClient

    from core.permissions import ADMIN_USER_IDS
    from web.main import app
    from web.routes.api._deps import limiter
    from web.routes.auth import (
        SESSION_COOKIE, create_session_data, session_serializer,
    )

    limiter.reset()
    admin_id = sorted(ADMIN_USER_IDS)[0]

    async def _resolve(session):
        return {"user_id": admin_id, "role": "admin"}

    flags.setattr("web.routes.auth._resolve_session", _resolve)
    client = TestClient(app)
    client.cookies.set(SESSION_COOKIE, session_serializer.dumps(create_session_data(
        {"id": str(admin_id), "first_name": "T", "last_name": "U",
         "username": "t", "auth_date": str(int(_time.time()))}, role="admin",
    )))
    return client


class TestTheAdminBackfill:
    def _client(self, flags):
        return _admin_client(flags)

    @pytest.fixture
    def backfill(self, flags):
        from unittest.mock import AsyncMock

        run = AsyncMock(return_value={"complete": True})
        flags.setattr("core.pg_backfill.backfill_orders", run)
        flags.setattr("web.routes.api.admin.get_store", AsyncMock(return_value=object()))
        return run

    REFUSAL = (f"{LINES}, {ORDERS} is written by a write chain, not shipped out "
               "of DuckDB")

    def _post(self, flags, background):
        return self._client(flags).post(
            f"/api/mirror/backfill/orders?background={background}")

    @pytest.mark.parametrize("background", ["true", "false"])
    def test_409_before_anything_starts(
            self, flags, pool, order_chain, backfill, background):
        order_chain()
        res = self._post(flags, background)
        assert res.status_code == 409
        assert res.json()["detail"].startswith(self.REFUSAL)
        backfill.assert_not_called()
        _never_reached_postgres(pool)

    @pytest.mark.parametrize("background", ["true", "false"])
    def test_409_on_the_owner_rows_when_the_marker_is_lost(
            self, flags, pool, order_chain, backfill, background):
        """Flag at duckdb, no marker, an owner row in Postgres: the local
        answer is empty, and "started" here was the backfill refusing into a
        log line nobody reads (or a 500 in the foreground)."""
        order_chain(env=lambda: False)
        pool.owner_rows = {ORDERS: "2026-09-20T08:00:00+00:00"}
        res = self._post(flags, background)
        assert res.status_code == 409, res.json()
        assert res.json()["detail"].startswith(self.REFUSAL)
        backfill.assert_not_called()
        assert pool.only_asked_who_owns()

    @pytest.mark.parametrize("background", ["true", "false"])
    def test_an_unreadable_owner_row_is_a_503_never_started(
            self, flags, pool, order_chain, backfill, background):
        order_chain(env=lambda: False)
        pool.owner_error = RuntimeError("meta.chain_watermarks unreadable")
        res = self._post(flags, background)
        assert res.status_code == 503, res.json()
        assert res.json().get("status") != "started"
        assert "unreadable" in res.json()["detail"]
        backfill.assert_not_called()

    @pytest.mark.parametrize("background", ["true", "false"])
    def test_a_schema_behind_the_code_is_a_503_before_the_owner_read(
            self, flags, pool, backfill, background):
        from core import pg

        pg.require_revision.side_effect = pg.SchemaVersionError(
            "database at 0031, code wants 0033")
        pool.owner_rows = {ORDERS: "2026-09-20T08:00:00+00:00"}
        res = self._post(flags, background)
        assert res.status_code == 503, res.json()
        assert "SchemaVersionError" in res.json()["detail"]
        assert pool.sql == [] and pool.acquired == 0
        backfill.assert_not_called()

    @pytest.mark.parametrize("background", ["true", "false"])
    @pytest.mark.parametrize("chain", ["none", "flagged"])
    def test_with_the_mirror_off_it_refuses_and_asks_postgres_nothing(
            self, flags, pool, order_chain, backfill, background, chain):
        """`backfill_orders` refuses a switched-off mirror, so the route says
        so first: a 409, never "started" in the background (the refusal was
        a line in the web log) nor a 500 in the foreground. First means
        before the latch checks: with a chain flagged onto the order tables
        the answer is still the mirror's, and no Postgres read stands in
        front of it either way."""
        from core.pg_landing import MIRROR_ENV

        flags.setenv(MIRROR_ENV, "0")
        if chain == "flagged":
            order_chain()                       # the local answer names both
            pool.owner_rows = {ORDERS: "2026-09-20T08:00:00+00:00"}
        res = self._post(flags, background)
        assert res.status_code == 409, res.json()
        assert res.json() == {"detail": "KS_MIRROR_LANDING is off; nothing was started"}
        assert res.json().get("status") != "started"
        backfill.assert_not_called()
        _never_reached_postgres(pool)

    def test_without_a_chain_it_runs(self, flags, pool, backfill):
        res = self._post(flags, "false")
        assert res.status_code == 200
        backfill.assert_awaited_once()
        assert pool.only_asked_who_owns()


class TestEveryOrderShipperAsksFirst:
    """Walked, not listed. A function that ships DuckDB's orders to Postgres —
    it calls `write_orders` or `mirror_orders` — must ask
    `order_tables_stood_down` itself. `mirror_orders` is the one exemption: it
    is the wrapper every such caller reaches, and each of those is walked."""

    SHIPPERS = {"write_orders", "mirror_orders"}
    EXEMPT = {("core/pg_landing.py", "mirror_orders")}

    @staticmethod
    def _called(fn) -> set:
        names = set()
        for node in ast.walk(fn):
            if isinstance(node, ast.Call):
                f = node.func
                if isinstance(f, ast.Name):
                    names.add(f.id)
                elif isinstance(f, ast.Attribute):
                    names.add(f.attr)
        return names

    def _sites(self):
        root = CORE.parent
        for folder in ("core", "web", "scripts"):
            for path in sorted((root / folder).rglob("*.py")):
                rel = path.relative_to(root).as_posix()
                tree = ast.parse(path.read_text(encoding="utf-8"))
                for fn in ast.walk(tree):
                    if not isinstance(fn, (ast.FunctionDef, ast.AsyncFunctionDef)):
                        continue
                    if (rel, fn.name) in self.EXEMPT:
                        continue
                    called = self._called(fn)
                    if called & self.SHIPPERS:
                        yield rel, fn.name, called

    def test_each_one_asks(self):
        missing = [(rel, name) for rel, name, called in self._sites()
                   if "order_tables_stood_down" not in called]
        assert not missing, f"ships orders without asking the registry: {missing}"

    def test_the_walk_is_not_vacuous(self):
        found = {name for _rel, name, _called in self._sites()}
        assert {"upsert_orders", "backfill_orders", "ship_orders_by_id"} <= found

    def test_one_that_takes_the_pool_asks_the_owner_rows_too(self):
        """DN-06: anything already holding a Postgres connection stands down on
        either copy of the latch. The sync's mirror in `upsert_orders` takes
        no pool — the write path, where that read is the one to avoid — and so
        is not held to it."""
        holding = [(rel, name, called) for rel, name, called in self._sites()
                   if "get_pool" in called]
        assert {"backfill_orders", "ship_orders_by_id"} <= {n for _r, n, _c in holding}
        missing = [(rel, name) for rel, name, called in holding
                   if "order_tables_stood_down_or_owned" not in called]
        assert not missing, f"holds a pool but reads only the local latch: {missing}"


class TestChain7aGoals:
    """DN-25: `app.revenue_goals` joins the registry behind `KS_WRITE_GOALS`,
    off by default — so the default stands down exactly what it did before."""

    def test_off_by_default_stands_nothing_down(self, flags):
        from core.pg_goals_write import writes_postgres
        from core.write_chains import stood_down_sync_keys, stood_down_tables
        assert writes_postgres() is False
        assert stood_down_tables() == frozenset()
        assert stood_down_sync_keys() == frozenset()

    def test_only_the_goals_table_when_only_chain_7a_is_on(self, flags):
        from core.write_chains import stood_down_sync_keys, stood_down_tables
        flags.setenv("KS_WRITE_GOALS", "postgres")
        assert stood_down_tables() == frozenset({"app.revenue_goals"})
        # No `last_sync_*` key: goals are typed, never synced.
        assert stood_down_sync_keys() == frozenset()

    def test_an_unknown_value_raises_in_the_writer_and_stands_down_the_table(self, flags):
        from core.pg_goals_write import writes_postgres
        from core.write_chains import stood_down_tables_checked
        flags.setenv("KS_WRITE_GOALS", "postgre")
        with pytest.raises(RuntimeError, match="KS_WRITE_GOALS"):
            writes_postgres()
        tables, errors = stood_down_tables_checked()
        assert tables == frozenset({"app.revenue_goals"})
        assert list(errors) == ["pg_goals_write"]

    def test_the_goals_table_is_one_the_shipper_actually_replaces(self):
        from core.pg_goals_write import CHAIN_TABLES
        from core.pg_operational import _FULL_REPLACE
        assert set(CHAIN_TABLES) <= {pg for pg, _d, _c, _o in _FULL_REPLACE}


# ─── DN-22b: every other path that ships into bronze.* and app.* asks too ────
#
# The plan's fake chain: one table from each of chains 3–6 that is not an order
# table. Three of the four are one half of a shipping unit — the buyers without
# their contacts, the managers without their classifications — so every test
# that sees a path stand down on it also sees the unit rule hold. Each
# "ships nothing" has its control beside it, on the same recorder.

BUYERS = "bronze.buyers"
CONTACTS = "bronze.buyer_contacts"
MANAGERS = "bronze.managers"
CLASSIFICATIONS = "app.manager_classifications"
EXPENSES = "bronze.expenses"
PRODUCTS = "bronze.products"
CATEGORIES = "bronze.categories"
LANDING = (BUYERS, MANAGERS, EXPENSES, PRODUCTS)


@pytest.fixture
def landing_chain(flags):
    """Register a fake chain owning `tables`; its flag is `env()`."""
    import types

    from core import write_chains

    def register(tables=LANDING, env=lambda: True):
        fake = types.ModuleType("core.pg_landing_write")
        fake.WRITE_ENV = "KS_WRITE_LANDING"
        fake.CHAIN_TABLES = tuple(tables)
        fake.env_writes_postgres = env
        flags.setattr(write_chains, "WRITE_CHAINS",
                      write_chains.WRITE_CHAINS + (fake,))
        return fake

    return register


def _no_landing_chain(flags) -> None:
    """No chain in this build declares any table of the plan's fake chain —
    made so, `_rolled_back`'s reason."""
    from core import write_chains

    units = {BUYERS, CONTACTS, MANAGERS, CLASSIFICATIONS, EXPENSES, PRODUCTS}
    flags.setattr(write_chains, "WRITE_CHAINS", tuple(
        c for c in write_chains.WRITE_CHAINS if not units & set(c.CHAIN_TABLES)))


async def _landing_store(tmp_path):
    """A DuckDB holding two buyers with a contact, one expense and a manager
    with a classification — enough for every backfill to find a row to ship."""
    from core.duckdb_store import DuckDBStore

    store = DuckDBStore(db_path=tmp_path / "dn22b.duckdb")
    await store.connect()
    async with store.connection() as conn:
        conn.execute("INSERT INTO buyers (id, full_name) VALUES (1, 'Anna'), (2, 'Olha')")
        conn.execute("INSERT INTO buyer_contacts (buyer_id, contact_type, value, "
                     "is_primary) VALUES (1, 'phone', '+380500000001', TRUE)")
        conn.execute("INSERT INTO expenses (id, order_id, expense_type_id, amount) "
                     "VALUES (9, 1, 1, 100.00)")
        conn.execute("INSERT OR REPLACE INTO managers (id, name, is_retail) "
                     "VALUES (4, 'Manager', TRUE)")
        conn.execute("INSERT OR REPLACE INTO manager_classifications "
                     "(manager_id, is_retail, valid_from) VALUES (4, TRUE, DATE '1970-01-01')")
    return store


def _buyers():
    from core.models import Buyer

    return [Buyer(id=1, full_name="Anna", phones=["+380500000001"])]


_EXPENSE_ORDERS = [{"id": 1, "expenses": [
    {"id": 9, "expense_type_id": 1, "amount": "100.00", "description": "delivery",
     "status": "paid", "payment_date": None, "created_at": WHEN}]}]


async def _ship(path, store):
    """Run one sync-side shipper the way its caller does."""
    from core import pg_buyers, pg_landing, pg_replication

    if path == "mirror_products":
        return await pg_landing.mirror_products([{"id": 100, "name": "Serum"}])
    if path == "mirror_categories":
        return await pg_landing.mirror_categories([{"id": 7, "name": "Care"}])
    if path == "mirror_expenses":
        return await pg_landing.mirror_expenses(_EXPENSE_ORDERS)
    if path == "mirror_buyers":
        return await pg_buyers.mirror_buyers(_buyers())
    if path == "replicate_managers":
        return await pg_replication.replicate_managers(store)
    raise AssertionError(path)


# What each sync-side shipper writes, for the controls.
_SHIPS = {
    "mirror_products": PRODUCTS,
    "mirror_expenses": EXPENSES,
    "mirror_buyers": BUYERS,
    "replicate_managers": MANAGERS,
}


def _skip_reason(out) -> str:
    return out.skipped if hasattr(out, "skipped") else out.get("skipped")


class TestTheSyncShippersStandDown:
    """The per-tick mirrors and the classification copy: skipped with a
    reason, and Postgres not asked anything — the write path's local answer."""

    @pytest.mark.asyncio
    @pytest.mark.parametrize("path", sorted(_SHIPS))
    async def test_without_a_chain_the_recorder_sees_the_write(
            self, pool, tmp_path, path):
        """The control. A recorder that could not see these writes would pass
        every test below."""
        store = await _landing_store(tmp_path)
        await _ship(path, store)
        assert pool.wrote(_SHIPS[path]), pool.sql

    @pytest.mark.asyncio
    @pytest.mark.parametrize("path", sorted(_SHIPS))
    async def test_with_the_chain_it_reports_stood_down_and_writes_nothing(
            self, pool, landing_chain, path):
        landing_chain()
        out = await _ship(path, _NoStore())
        assert _skip_reason(out).startswith("stood down: a write chain owns "), out
        assert pool.sql == []
        _never_reached_postgres(pool)

    @pytest.mark.asyncio
    async def test_a_table_no_chain_declares_still_ships(self, pool, landing_chain):
        """The question is asked per table, not as an off switch: the
        categories are not the chain's, and they still go."""
        landing_chain()
        out = await _ship("mirror_categories", None)
        assert out.ok and pool.wrote(CATEGORIES), pool.sql

    @pytest.mark.asyncio
    @pytest.mark.parametrize("path,half", [
        ("mirror_buyers", CONTACTS), ("replicate_managers", CLASSIFICATIONS),
    ])
    async def test_either_half_of_a_unit_stands_the_unit_down(
            self, pool, landing_chain, path, half):
        """The other half of each unit: `_write` and `write_managers` write
        both tables in one transaction, so a chain on either stops both."""
        landing_chain(tables=(half,))
        out = await _ship(path, _NoStore())
        assert _skip_reason(out) == f"stood down: a write chain owns {half}", out
        assert pool.sql == []

    @pytest.mark.asyncio
    async def test_a_flag_typo_stands_it_down_too(self, pool, landing_chain):
        """A value no chain understands stands its tables down — the shipper
        must not guess which store the rows belong to."""
        def typo():
            raise RuntimeError("KS_WRITE_LANDING='postgre' is not understood")

        landing_chain(env=typo)
        out = await _ship("mirror_products", None)
        assert out.skipped.startswith("stood down"), out
        assert pool.sql == []

    @pytest.mark.asyncio
    async def test_the_sync_writes_duckdb_and_ships_only_what_is_not_owned(
            self, pool, landing_chain, tmp_path):
        """`SyncService._upsert_orders_with_expenses`, the site that ships the
        expenses every tick: DuckDB gets them, Postgres gets the orders — no
        chain declares those here — and not the expenses."""
        from core.sync_service import SyncService

        landing_chain(tables=(EXPENSES,))
        store = await _store(tmp_path)
        order = dict(_order(1), expenses=_EXPENSE_ORDERS[0]["expenses"])
        await SyncService(store)._upsert_orders_with_expenses([order])

        async with store.connection() as conn:
            assert conn.execute("SELECT count(*) FROM expenses").fetchone()[0] == 1
        assert pool.wrote(ORDERS), pool.sql
        assert not pool.wrote(EXPENSES), pool.sql


# The backfills and the hourly diffs, which hold a pool and so read the owner
# rows too: (the call, what it ships, how it says it stood down).
_POOLED = {
    "backfill_buyers": BUYERS,
    "hourly_ids_diff": BUYERS,
    "backfill_expenses": EXPENSES,
    "hourly_expenses_ids_diff": EXPENSES,
}


async def _run_pooled(path, store):
    from core import pg_buyers, pg_expense_backfill

    return await {
        "backfill_buyers": pg_buyers.backfill_buyers,
        "hourly_ids_diff": pg_buyers.hourly_ids_diff,
        "backfill_expenses": pg_expense_backfill.backfill_expenses,
        "hourly_expenses_ids_diff": pg_expense_backfill.hourly_expenses_ids_diff,
    }[path](store)


# The unit each pooled path ships, for what its stand-down may name.
_UNIT_OF = {BUYERS: {BUYERS, CONTACTS}, EXPENSES: {EXPENSES}}


async def _expect_stood_down(path, store, caplog):
    """A backfill refuses loudly; its hourly diff stands down quietly, naming
    the table it ships and nothing outside that table's unit."""
    table = _POOLED[path]
    if path.startswith("hourly"):
        with caplog.at_level("ERROR"):
            out = await _run_pooled(path, store)
        assert set(out) == {"stood_down"}, out
        assert table in out["stood_down"], out
        assert set(out["stood_down"]) <= _UNIT_OF[table], out
        assert not [r for r in caplog.records if r.levelname == "ERROR"]
        return out
    with pytest.raises(RuntimeError, match=f"{table}.* is written by a write chain"):
        await _run_pooled(path, store)
    return None


class TestTheBackfillsAndTheHourlyDiffs:
    @pytest.mark.asyncio
    @pytest.mark.parametrize("path", sorted(_POOLED))
    async def test_without_a_chain_it_writes(self, pool, tmp_path, path):
        """The control: each ships DuckDB's row into the recorder. Postgres
        holds nothing there, so everything DuckDB has is missing."""
        store = await _landing_store(tmp_path)
        out = await _run_pooled(path, store)
        assert "error" not in out, out
        assert pool.wrote(_POOLED[path]), pool.sql

    @pytest.mark.asyncio
    @pytest.mark.parametrize("path", sorted(_POOLED))
    async def test_the_chain_stands_it_down_before_postgres(
            self, pool, landing_chain, tmp_path, path, caplog):
        landing_chain()
        await _expect_stood_down(path, await _landing_store(tmp_path), caplog)
        _never_reached_postgres(pool)

    @pytest.mark.asyncio
    @pytest.mark.parametrize("path", sorted(_POOLED))
    async def test_the_owner_row_stands_it_down_when_the_marker_is_lost(
            self, pool, landing_chain, tmp_path, path, caplog):
        """Flag at duckdb, no marker: only the owner row in Postgres says the
        table moved, and a path holding a pool reads it — DN-06's rule."""
        landing_chain(env=lambda: False)
        pool.owner_rows = {_POOLED[path]: "2026-09-20T08:00:00+00:00"}
        await _expect_stood_down(path, await _landing_store(tmp_path), caplog)
        assert pool.only_asked_who_owns(), pool.sql

    @pytest.mark.asyncio
    @pytest.mark.parametrize("path", sorted(_POOLED))
    async def test_an_owner_row_no_chain_here_declares_still_holds(
            self, flags, pool, tmp_path, path, caplog):
        """An image older than the chain: the owner row is read as itself."""
        _no_landing_chain(flags)
        pool.owner_rows = {_POOLED[path]: "2026-09-20T08:00:00+00:00"}
        await _expect_stood_down(path, await _landing_store(tmp_path), caplog)
        assert pool.only_asked_who_owns(), pool.sql

    @pytest.mark.asyncio
    async def test_the_buyers_owner_row_holds_their_contacts_too(
            self, flags, pool, tmp_path):
        """An owner row for either half of the unit holds the whole unit."""
        from core.pg_buyers import hourly_ids_diff

        _no_landing_chain(flags)
        pool.owner_rows = {CONTACTS: "2026-09-20T08:00:00+00:00"}
        out = await hourly_ids_diff(await _landing_store(tmp_path))
        assert out == {"stood_down": [CONTACTS, BUYERS]}, out
        assert pool.only_asked_who_owns()

    @pytest.mark.asyncio
    @pytest.mark.parametrize("path", sorted(_POOLED))
    async def test_an_unreadable_owner_row_ships_nothing(
            self, pool, landing_chain, tmp_path, path):
        """Not an absent owner row: the backfill raises it, the hourly diff
        returns it as its error, and neither ships on the strength of it."""
        landing_chain(env=lambda: False)
        pool.owner_error = RuntimeError("meta.chain_watermarks unreadable")
        store = await _landing_store(tmp_path)
        if path.startswith("hourly"):
            out = await _run_pooled(path, store)
            assert "unreadable" in out["error"], out
        else:
            with pytest.raises(RuntimeError, match="unreadable"):
                await _run_pooled(path, store)
        assert pool.only_asked_who_owns(), pool.sql

    @pytest.mark.asyncio
    @pytest.mark.parametrize("path", sorted(_POOLED))
    async def test_a_schema_behind_the_code_breaks_before_the_owner_read(
            self, pool, tmp_path, path):
        """`require_revision()` first, so an old schema is a
        `SchemaVersionError` and never a stand-down read off it — the owner
        row is present here, so a path that read it first would stand down."""
        from core import pg

        pg.require_revision.side_effect = pg.SchemaVersionError(
            "database at 0031, code wants 0033")
        pool.owner_rows = {_POOLED[path]: "2026-09-20T08:00:00+00:00"}
        store = await _landing_store(tmp_path)
        if path.startswith("hourly"):
            out = await _run_pooled(path, store)
            assert out.get("error", "").startswith("SchemaVersionError"), out
        else:
            with pytest.raises(pg.SchemaVersionError):
                await _run_pooled(path, store)
        pg.require_revision.assert_awaited()
        assert pool.sql == [] and pool.acquired == 0, pool.sql


class TestTheAdminExpenseBackfill:
    """`POST /api/mirror/backfill/expenses`: 409 before anything starts, the
    orders route's arrangement. It used to hand the backfill's refusal back as
    a 500 "Backfill failed"."""

    @pytest.fixture
    def backfill(self, flags):
        from unittest.mock import AsyncMock

        run = AsyncMock(return_value={"complete": True})
        flags.setattr("core.pg_expense_backfill.backfill_expenses", run)
        flags.setattr("web.routes.api.admin.get_store", AsyncMock(return_value=object()))
        return run

    def _post(self, flags):
        return _admin_client(flags).post("/api/mirror/backfill/expenses")

    REFUSAL = f"{EXPENSES} is written by a write chain, not shipped out of DuckDB"

    def test_409_on_the_chain_and_postgres_is_not_asked(
            self, flags, pool, landing_chain, backfill):
        landing_chain()
        res = self._post(flags)
        assert res.status_code == 409, res.json()
        assert res.json()["detail"].startswith(self.REFUSAL)
        backfill.assert_not_called()
        _never_reached_postgres(pool)

    def test_409_on_the_owner_row_when_the_marker_is_lost(
            self, flags, pool, landing_chain, backfill):
        landing_chain(env=lambda: False)
        pool.owner_rows = {EXPENSES: "2026-09-20T08:00:00+00:00"}
        res = self._post(flags)
        assert res.status_code == 409, res.json()
        assert res.json()["detail"].startswith(self.REFUSAL)
        backfill.assert_not_called()
        assert pool.only_asked_who_owns()

    def test_an_unreadable_owner_row_is_a_503(
            self, flags, pool, landing_chain, backfill):
        landing_chain(env=lambda: False)
        pool.owner_error = RuntimeError("meta.chain_watermarks unreadable")
        res = self._post(flags)
        assert res.status_code == 503, res.json()
        assert "unreadable" in res.json()["detail"]
        backfill.assert_not_called()

    def test_a_schema_behind_the_code_is_a_503_before_the_owner_read(
            self, flags, pool, backfill):
        from core import pg

        pg.require_revision.side_effect = pg.SchemaVersionError(
            "database at 0031, code wants 0033")
        pool.owner_rows = {EXPENSES: "2026-09-20T08:00:00+00:00"}
        res = self._post(flags)
        assert res.status_code == 503, res.json()
        assert "SchemaVersionError" in res.json()["detail"]
        assert pool.sql == [] and pool.acquired == 0
        backfill.assert_not_called()

    def test_without_a_chain_it_runs(self, flags, pool, backfill):
        res = self._post(flags)
        assert res.status_code == 200, res.json()
        backfill.assert_awaited_once()
        assert pool.only_asked_who_owns()


# ─── DN-22b: the daily comparisons stand down with their shippers ───────────

_COMPARED = {
    # comparison: (the tables it stands down on the plan's fake chain,
    #              the shipper whose claim its finding makes)
    "reconcile_mirror": ((PRODUCTS, MANAGERS, CLASSIFICATIONS), "mirror_products"),
    "reconcile_expenses": ((EXPENSES,), "mirror_expenses"),
    "reconcile_buyers": ((BUYERS, CONTACTS), "mirror_buyers"),
}


async def _compare(name, store):
    """Run one comparison the way the 07:30 job does. `reconcile_mirror` is
    handed a DuckDB side holding the categories alone: a stood-down table it
    went on to compare would fail on the missing key, not pass."""
    from core import mirror_reconciliation as mr

    if name == "reconcile_mirror":
        if store is None:
            dk_side = {CATEGORIES: ({}, {})}
        else:
            async with store.connection() as conn:
                dk_side = mr.read_duckdb_side(conn)
        return await mr.reconcile_mirror(dk_side)
    return await getattr(mr, name)(store if store is not None else _NoStore())


def _read_a_table(pool, tables) -> list:
    """The statements that named one of `tables` — reads and writes alike."""
    return [s for s in pool.sql if any(t in s for t in tables)]


class TestTheComparisonsStandDown:
    @pytest.mark.asyncio
    @pytest.mark.parametrize("name", sorted(_COMPARED))
    async def test_without_a_chain_they_compare(self, pool, tmp_path, name):
        """The control: each reads its tables in Postgres and files no
        stand-down of either kind."""
        store = await _landing_store(tmp_path)
        issues = await _compare(name, store)
        tables = _COMPARED[name][0]
        assert _read_a_table(pool, tables), pool.sql
        assert not {"mirror_stood_down", "owner_row_without_marker"} & {
            i.check_name for i in issues}, issues

    @pytest.mark.asyncio
    @pytest.mark.parametrize("name", sorted(_COMPARED))
    async def test_on_the_local_answer_info_per_table_and_no_read(
            self, pool, landing_chain, name):
        from core.data_quality import Severity

        landing_chain()
        issues = await _compare(name, None)
        tables = _COMPARED[name][0]
        stood = [(i.check_name, i.table_name, i.severity) for i in issues
                 if i.check_name == "mirror_stood_down"]
        assert stood == [("mirror_stood_down", t, Severity.INFO) for t in tables]
        assert "owner_row_without_marker" not in {i.check_name for i in issues}
        assert _read_a_table(pool, tables) == [], pool.sql
        if name != "reconcile_mirror":
            _never_reached_postgres(pool)

    @pytest.mark.asyncio
    @pytest.mark.parametrize("name", sorted(_COMPARED))
    @pytest.mark.parametrize("road", ["marker_lost", "rolled_back"])
    async def test_on_the_owner_rows_alone_one_critical_and_no_read(
            self, flags, pool, landing_chain, name, road):
        from core.data_quality import Severity

        tables = _COMPARED[name][0]
        if road == "marker_lost":
            landing_chain(env=lambda: False)
            # One owner row holds the chain's every table (`claimed_tables`).
            pool.owner_rows = {tables[0]: "2026-09-20T08:00:00+00:00"}
        else:
            _no_landing_chain(flags)
            # No chain to expand a row through: each row holds its own unit,
            # so the catalogue's two units need a row each.
            pool.owner_rows = {t: "2026-09-20T08:00:00+00:00" for t in tables}
        issues = await _compare(name, None)

        (page,) = [i for i in issues if i.check_name == "owner_row_without_marker"]
        assert page.severity is Severity.CRITICAL
        assert page.table_name == ", ".join(sorted(tables))
        assert page.count == len(tables)
        assert "mirror_stood_down" not in {i.check_name for i in issues}
        assert _read_a_table(pool, tables) == [], pool.sql
        if road == "marker_lost":
            assert "the marker is missing" in page.description
        else:
            assert "no chain in this build declares them" in page.description

    @pytest.mark.asyncio
    @pytest.mark.parametrize("name", sorted(_COMPARED))
    async def test_a_schema_behind_the_code_breaks_before_the_owner_read(
            self, pool, name):
        from core import pg

        pg.require_revision.side_effect = pg.SchemaVersionError(
            "database at 0031, code wants 0033")
        pool.owner_rows = {_COMPARED[name][0][0]: "2026-09-20T08:00:00+00:00"}
        with pytest.raises(pg.SchemaVersionError):
            await _compare(name, None)
        assert pool.sql == [] and pool.acquired == 0, pool.sql

    @pytest.mark.asyncio
    @pytest.mark.parametrize("name", sorted(_COMPARED))
    async def test_an_unreadable_owner_row_is_the_comparisons_error(
            self, pool, landing_chain, name):
        """Raised, so the 07:30 job records the check as failed rather than
        comparing — `reconcile_operational`'s contract for the same read."""
        landing_chain(env=lambda: False)
        pool.owner_error = RuntimeError("meta.chain_watermarks unreadable")
        with pytest.raises(RuntimeError, match="unreadable"):
            await _compare(name, None)
        assert pool.only_asked_who_owns(), pool.sql


class TestTheLandingFindingSaysWhetherTheSyncStillShips:
    """`TestTheStandDownFindingSaysWhetherTheSyncStillShips`, for DN-22b: the
    claim each finding makes about the sync's shipper is checked against what
    that shipper then does with the same recorder."""

    @pytest.mark.asyncio
    @pytest.mark.parametrize("name", sorted(_COMPARED))
    @pytest.mark.parametrize("state", ["flagged", "marker_lost", "rolled_back"])
    async def test_the_claim_is_what_the_shipper_does(
            self, flags, pool, landing_chain, tmp_path, name, state):
        tables, shipper = _COMPARED[name]
        if state == "flagged":
            landing_chain()
        else:
            if state == "marker_lost":
                landing_chain(env=lambda: False)
            else:
                _no_landing_chain(flags)
            pool.owner_rows = {t: "2026-09-20T08:00:00+00:00" for t in tables}
        issues = [i for i in await _compare(name, None)
                  if i.check_name in {"mirror_stood_down", "owner_row_without_marker"}]
        assert issues, "no stand-down filed — nothing to check the claim of"
        for text in {i.description for i in issues}:
            stopped = "the sync no longer ships DuckDB's copy" in text
            shipping = "the sync is still shipping" in text
            assert stopped != shipping, f"says neither or both: {text}"
        text = issues[0].description
        shipping = "the sync is still shipping" in text

        # Every sync shipper of the tables the finding covers — the catalogue
        # comparison covers two, and the claim is made about both.
        store = await _landing_store(tmp_path)
        shippers = {"reconcile_mirror": (("mirror_products", PRODUCTS),
                                         ("replicate_managers", MANAGERS))}.get(
            name, ((shipper, tables[0]),))
        for path, table in shippers:
            pool.sql.clear()
            await _ship(path, store)
            assert pool.wrote(table) == shipping, (path, text, pool.sql)
            assert pool.wrote(table) == (issues[0].severity.value == "CRITICAL")


class TestWhatTheLandingStandDownTellsAHuman:
    """Persisted as the 07:30 job persists it, read back as the digest and a
    page read it: the label and the lever, never the description."""

    @pytest.mark.asyncio
    async def test_on_the_local_answer_it_stays_info(
            self, pool, landing_chain, tmp_path):
        from core.mirror_reconciliation import reconcile_buyers

        landing_chain()
        store = await _landing_store(tmp_path)
        run, rows = await _persisted(store, await reconcile_buyers(store))
        assert sorted((r["check_name"], r["table_name"], r["severity"])
                      for r in rows) == [
            ("mirror_stood_down", CONTACTS, "INFO"),
            ("mirror_stood_down", BUYERS, "INFO"),
        ]
        assert run["status"] == "PASS" and run["critical_count"] == 0, run

    @pytest.mark.asyncio
    async def test_on_the_owner_rows_alone_it_pages_and_never_says_not_a_defect(
            self, pool, landing_chain, tmp_path):
        from core.mirror_reconciliation import reconcile_expenses

        landing_chain(env=lambda: False)
        pool.owner_rows = {EXPENSES: "2026-09-20T08:00:00+00:00"}
        store = await _landing_store(tmp_path)
        run, rows = await _persisted(store, await reconcile_expenses(store))

        assert [(r["check_name"], r["table_name"], r["severity"]) for r in rows] == [
            ("owner_row_without_marker", EXPENSES, "CRITICAL")]
        assert run["status"] == "CRITICAL", run
        read = _what_a_human_reads(run, rows)
        assert "not a defect" not in read.lower(), read
        assert "the sync is overwriting tables a write chain owns" in read
        (lever,) = [line for line in read.splitlines() if line.startswith("→ ")]
        assert "scripts/chain_copy_back.py" in lever
        assert "data/write-chain-owners" in lever

    def test_the_registry_says_it_does_not_clear_by_itself(self):
        from core.alerting import REGISTRY, Kind

        spec = REGISTRY["owner_row_without_marker"]
        assert spec.kind is Kind.CONDITION
        assert "a human, not a job" in spec.clears

    def test_the_local_lever_is_not_about_orders_alone(self):
        """`mirror_stood_down` is filed for any table now; its lever must not
        tell the reader of a buyers finding about the order tables."""
        from core.data_quality import remediation_for

        (lever,) = remediation_for(["mirror_stood_down"])
        assert "order" not in lever.lower(), lever
        assert "never backfill" in lever.lower()
