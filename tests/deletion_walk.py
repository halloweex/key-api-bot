"""What the repository's code deletes, renames or replaces, and where (rule 5).

Step 14's rule 5: no line of its code deletes or renames the DuckDB file, its
backups, the export, the Arks, the chain markers or the report spool.
`tests/unit/test_store_without_duckdb.py` holds the rule; this is the walk it
holds it with, kept apart so the mutation cases there run through the very
same code as the tree.

**Python** (`core/`, `web/`, `bot/`, `scripts/`, `deploy/`). A deletion is a
call that ends in a deleting name:

* an attribute — `p.unlink()`, `os.remove(p)`, `_alias_of_shutil.rmtree(p)`;
  `rename` and `replace` only on `os` or with exactly one positional argument
  and no keyword (a path's), so `str.replace(a, b)` and
  `dt.replace(tzinfo=…)` are not;
* a name a from-import of `os` or `shutil` bound (`from os import remove`);
* `subprocess`/`os.system`/`asyncio`'s process runners handed `rm`, `mv`,
  `unlink`, `rmdir`, `shred`, `truncate` or `ln -f`;
* a call of a function or a `self` method that is itself a deletion of one of
  its own parameters, to a fixed point — so a wrapper is seen at its caller.
  A parameter counts when the deleted path takes its directory from it (the
  leftmost part of a `/`, a `+`, an f-string, a `Path(…)`, a method's
  receiver), not when it only fills in a file name: `raise_alert`'s bucket
  names the spooled task's file and points it nowhere else.

What it deletes is read from the deleting call's receiver and arguments,
followed backwards through the function's own assignments, loops, `with`
targets and parameter defaults, then through every module-level constant and
every helper's return value they name — in the module, or in the module an
import names (`from core import chain_latch` … `chain_latch.marker_path`) —
and through `self.<attr>` as the class assigns it. The text so reached is
matched against the places' names. Module-level code is a unit of its own.
Writing a file in place (`open(p, "w")`) is not read as a deletion.

**Shell** (every `*.sh` under the same trees, `nginx/` and `postgres/`). A
deletion is `rm`, `mv`, `unlink`, `rmdir`, `shred`, `truncate`, `ln -f`,
`find … -delete`/`-exec rm` or `xargs rm`, read after the script's own
assignments (and `for` lists) are expanded into it.

Not seen, and said here rather than discovered: a method called on an object
other than `self`, a path built in another process or read from a file, a
name computed at run time.
"""
from __future__ import annotations

import ast
import re
from collections import defaultdict
from functools import lru_cache
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Set, Tuple

REPO = Path(__file__).resolve().parents[1]
PYTHON_TREES = ("core", "web", "bot", "scripts", "deploy")
SHELL_TREES = ("core", "web", "bot", "scripts", "deploy", "nginx", "postgres")
_SKIP_PARTS = {"frontend", "node_modules", "__pycache__", "static", "static-v2"}

# The places, by the names code reaches them through. Whole words: the
# dashboard's host name and `get_traffic_analytics` are not the file, and
# `core.duckdb_store` is not a `.duckdb`.
DUCKDB_PLACES = re.compile(
    r"\banalytics\b|\bDUCKDB_FILE_NAME\b|\bDB_PATH\b|\bDB_DIR\b|export_parquet"
    r"|\bbackups\b|/ark\b|BACKUP_ARK|\.duckdb\b")
KEPT_PLACES = re.compile(r"\bMARKER_DIR\b|write-chain-owners|\bSPOOL_DIR\b"
                         r"|report-ledger-pending")
# In shell Postgres's dumps and WAL share `backups/` (`backups/postgres/…`,
# `backups/pg_wal/…`), and they are not DuckDB's: DuckDB's backups are named
# by `.duckdb`, and the directory counts only as itself or as all of it.
SHELL_PLACES = re.compile(
    r"\.duckdb\b|export_parquet|/root/ark\b|BACKUP_ARK|write-chain-owners"
    r"|report-ledger-pending|\bbackups/?\*?(?=[\"'\s)]|$)")

# On a path or an open file, whatever it is called.
_ANY_RECEIVER = {"unlink", "rmdir", "truncate"}
# A path's, given its target alone: `str.replace(a, b)`, `dt.replace(day=1)`
# and `df.rename(columns=…)` are not.
_ONE_ARGUMENT = {"rename", "replace"}
# Only as these modules' functions — `list.remove(x)` deletes no file.
_FREE_DELETING = {"os": {"remove", "unlink", "rename", "renames", "replace",
                         "rmdir", "removedirs", "truncate"},
                  "shutil": {"rmtree", "move"}}
_RUNNER_MODULES = {"subprocess", "os", "asyncio"}
_RUNNERS = {"run", "call", "check_call", "check_output", "Popen", "system",
            "popen", "create_subprocess_exec", "create_subprocess_shell",
            "getoutput", "getstatusoutput"}
_SHELL_VERBS = {"rm", "mv", "unlink", "rmdir", "shred", "truncate"}
_GROWING = {"append", "extend", "add", "insert", "update", "setdefault", "appendleft"}

Unit = Tuple[str, str]  # (rel, qualname)


# ─── Python ──────────────────────────────────────────────────────────────────

def _names(target: ast.AST) -> List[str]:
    return [n.id for n in ast.walk(target) if isinstance(n, ast.Name)]


def _parameters(fn: ast.AST) -> Set[str]:
    args = getattr(fn, "args", None)
    if args is None:
        return set()
    every = args.posonlyargs + args.args + args.kwonlyargs
    every += [a for a in (args.vararg, args.kwarg) if a is not None]
    return {a.arg for a in every} - {"self", "cls"}


@lru_cache(maxsize=None)
def _sources_of(fn: ast.AST) -> Dict[str, List[ast.AST]]:
    """Every expression a local name of `fn` is given: assignments, loops,
    comprehensions, `with … as`, walruses and parameter defaults."""
    out: Dict[str, List[ast.AST]] = defaultdict(list)
    args = getattr(fn, "args", None)
    if args is not None:
        positional = args.posonlyargs + args.args
        for a, d in zip(positional[len(positional) - len(args.defaults):], args.defaults):
            out[a.arg].append(d)
        for a, d in zip(args.kwonlyargs, args.kw_defaults):
            if d is not None:
                out[a.arg].append(d)
    for node in ast.walk(fn):
        if isinstance(node, ast.Assign):
            for t in node.targets:
                for n in _names(t):
                    out[n].append(node.value)
        elif isinstance(node, (ast.AnnAssign, ast.AugAssign)) and node.value is not None:
            for n in _names(node.target):
                out[n].append(node.value)
        elif isinstance(node, (ast.For, ast.AsyncFor, ast.comprehension)):
            for n in _names(node.target):
                out[n].append(node.iter)
        elif isinstance(node, ast.withitem) and node.optional_vars is not None:
            for n in _names(node.optional_vars):
                out[n].append(node.context_expr)
        elif isinstance(node, ast.NamedExpr):
            out[node.target.id].append(node.value)
        elif (isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
              and isinstance(node.func.value, ast.Name) and node.func.attr in _GROWING):
            # `out.append({"path": path})` gives `out` what it holds.
            out[node.func.value.id].extend(
                list(node.args) + [k.value for k in node.keywords])
    return dict(out)


def _backward(fn: Optional[ast.AST], roots: Iterable[ast.AST]) -> List[ast.AST]:
    """`roots` and every expression of `fn` their local names come from."""
    roots = list(roots)
    if fn is None:
        return roots
    sources = _sources_of(fn)
    out, seen, stack = list(roots), set(), list(roots)
    while stack:
        for n in ast.walk(stack.pop()):
            if isinstance(n, ast.Name) and n.id in sources and n.id not in seen:
                seen.add(n.id)
                out.extend(sources[n.id])
                stack.extend(sources[n.id])
    return out


def _returns(fn: ast.AST) -> List[ast.AST]:
    return [n.value for n in ast.walk(fn)
            if isinstance(n, (ast.Return, ast.Yield, ast.YieldFrom)) and n.value is not None]


class _Module:
    def __init__(self, rel: str, source: str):
        self.rel = rel
        parts = rel[:-3].split("/")
        if parts[-1] == "__init__":
            parts = parts[:-1]
        self.dotted = ".".join(parts)
        self.package = ".".join(parts if rel.endswith("__init__.py") else parts[:-1])
        self.tree = ast.parse(source)
        self.functions: Dict[str, ast.AST] = {}
        self.classes: Dict[str, ast.ClassDef] = {}
        self.constants: Dict[str, List[ast.AST]] = defaultdict(list)
        # local name -> ("module", dotted) | ("name", dotted, attr)
        self.imports: Dict[str, tuple] = {}
        self._top(self.tree.body)
        for node in ast.walk(self.tree):  # lazy imports inside functions too
            if isinstance(node, ast.Import):
                for a in node.names:
                    if a.asname:
                        self.imports[a.asname] = ("module", a.name)
                    else:
                        self.imports.setdefault(a.name.split(".")[0],
                                                ("module", a.name.split(".")[0]))
            elif isinstance(node, ast.ImportFrom):
                base = node.module or ""
                if node.level:
                    up = self.package.split(".") if self.package else []
                    up = up[:len(up) - (node.level - 1)] if node.level > 1 else up
                    base = ".".join([*up, *([base] if base else [])])
                for a in node.names:
                    self.imports[a.asname or a.name] = ("name", base, a.name)
        # Module-level code is a unit of its own.
        self.top = ast.Module(body=[s for s in self.tree.body if not isinstance(
            s, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef))], type_ignores=[])

    def _top(self, body):
        for node in body:
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                self.functions[node.name] = node
            elif isinstance(node, ast.ClassDef):
                self.classes[node.name] = node
            elif isinstance(node, ast.Assign):
                for t in node.targets:
                    for n in _names(t):
                        self.constants[n].append(node.value)
            elif isinstance(node, ast.AnnAssign) and node.value is not None:
                for n in _names(node.target):
                    self.constants[n].append(node.value)
            elif isinstance(node, (ast.If, ast.Try, ast.With)):
                nested = list(node.body) + list(getattr(node, "orelse", []))
                nested += list(getattr(node, "finalbody", []))
                for handler in getattr(node, "handlers", []):
                    nested += handler.body
                self._top(nested)

    def units(self):
        """(qualname, node, class) for every function, method and the
        module-level code."""
        yield "<module>", self.top, None
        for name, fn in self.functions.items():
            yield name, fn, None
        for cname, cls in self.classes.items():
            for node in cls.body:
                if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                    yield f"{cname}.{node.name}", node, cname


class Walk:
    def __init__(self, modules: Dict[str, _Module]):
        self.modules = modules
        self.by_dotted = {m.dotted: m for m in modules.values()}
        # A function that deletes what one of its parameters names → those
        # parameters.
        self.deleters: Dict[tuple, frozenset] = {}
        self._path_memo: Dict[tuple, frozenset] = {}

    # ── what a name names ──
    def _module(self, mod: _Module, name: str) -> Optional[_Module]:
        bound = mod.imports.get(name)
        if not bound:
            return None
        if bound[0] == "module":
            return self.by_dotted.get(bound[1])
        return self.by_dotted.get(f"{bound[1]}.{bound[2]}" if bound[1] else bound[2])

    def _stdlib(self, mod: _Module, name: str) -> Optional[str]:
        bound = mod.imports.get(name)
        if bound and bound[0] == "module" and bound[1] in ("os", "shutil", "subprocess",
                                                          "asyncio", "pathlib"):
            return bound[1]
        return None

    def _lookup(self, mod: _Module, name: str, depth: int = 0) -> List[tuple]:
        if name in mod.constants:
            return [("const", mod.rel, name)]
        if name in mod.functions:
            return [("func", mod.rel, name)]
        bound = mod.imports.get(name)
        if bound and bound[0] == "name" and depth < 4:
            other = self.by_dotted.get(bound[1])
            if other is not None:
                return self._lookup(other, bound[2], depth + 1)
        return []

    def _resolve(self, mod: _Module, node: ast.AST, cls: Optional[str]) -> List[tuple]:
        if isinstance(node, ast.Name):
            return self._lookup(mod, node.id)
        if isinstance(node, ast.Attribute) and isinstance(node.value, ast.Name):
            if node.value.id == "self" and cls is not None:
                return [("self", mod.rel, cls, node.attr)]
            other = self._module(mod, node.value.id)
            if other is not None:
                return self._lookup(other, node.attr)
        return []

    def _expand(self, target: tuple) -> List[tuple]:
        """(module, class, function, expression) to read next."""
        kind, rel = target[0], target[1]
        mod = self.modules[rel]
        if kind == "const":
            return [(mod, None, None, v) for v in mod.constants[target[2]]]
        if kind == "func":
            fn = mod.functions[target[2]]
            return [(mod, None, fn, r) for r in _returns(fn)]
        cls_name, attr = target[2], target[3]
        out = []
        for node in mod.classes[cls_name].body:
            if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                continue
            if node.name == attr:
                out += [(mod, cls_name, node, r) for r in _returns(node)]
            for a in ast.walk(node):
                if isinstance(a, (ast.Assign, ast.AnnAssign)) and a.value is not None:
                    targets = a.targets if isinstance(a, ast.Assign) else [a.target]
                    for t in targets:
                        if (isinstance(t, ast.Attribute) and t.attr == attr
                                and isinstance(t.value, ast.Name) and t.value.id == "self"):
                            out.append((mod, cls_name, node, a.value))
        return out

    def reach(self, mod: _Module, cls: Optional[str], fn: Optional[ast.AST],
              roots: Iterable[ast.AST]) -> str:
        """The text of everything `roots` can name."""
        texts: List[str] = []
        seen: Set[tuple] = set()
        queue = [(mod, cls, fn, list(roots))]
        while queue:
            m, c, f, exprs = queue.pop()
            for e in _backward(f, exprs):
                texts.append(ast.unparse(e))
                for node in ast.walk(e):
                    if not isinstance(node, (ast.Name, ast.Attribute)):
                        continue
                    for target in self._resolve(m, node, c):
                        if target in seen:
                            continue
                        seen.add(target)
                        for mm, cc, ff, ee in self._expand(target):
                            queue.append((mm, cc, ff, [ee]))
        return "\n".join(texts)

    # ── what a call deletes ──
    def _function(self, key: tuple) -> Optional[ast.AST]:
        mod = self.modules.get(key[1])
        if mod is None:
            return None
        if key[0] == "func":
            return mod.functions.get(key[2])
        cls = mod.classes.get(key[2])
        for node in (cls.body if cls is not None else []):
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name == key[3]:
                return node
        return None

    def _callee(self, mod: _Module, call: ast.Call, cls: Optional[str]) -> Optional[tuple]:
        func = call.func
        if isinstance(func, ast.Attribute) and isinstance(func.value, ast.Name) \
                and func.value.id == "self" and cls is not None:
            return ("method", mod.rel, cls, func.attr)
        for target in self._resolve(mod, func, cls):
            if target[0] == "func":
                return target
        return None

    @staticmethod
    def _shell_words(call: ast.Call) -> bool:
        if not call.args:
            return False
        first = call.args[0]
        if isinstance(first, (ast.List, ast.Tuple)):
            words = [e.value if isinstance(e, ast.Constant) and isinstance(e.value, str)
                     else "?" for e in first.elts]
        elif isinstance(first, ast.Constant) and isinstance(first.value, str):
            words = first.value.split()
        elif isinstance(first, ast.JoinedStr):
            words = "".join(v.value if isinstance(v, ast.Constant) else "?"
                            for v in first.values).split()
        else:
            return False
        return bool(words) and _shell_deletes(words)

    def deleting_calls(self, mod: _Module, fn: ast.AST, cls: Optional[str]):
        """(call, roots) for every call of `fn` that deletes."""
        for call in ast.walk(fn):
            if not isinstance(call, ast.Call):
                continue
            func, roots = call.func, list(call.args) + [k.value for k in call.keywords]
            if isinstance(func, ast.Attribute):
                std = self._stdlib(mod, func.value.id) if isinstance(func.value, ast.Name) else None
                if std is not None and func.attr in _FREE_DELETING.get(std, ()):
                    yield call, roots
                    continue
                if std is None and (func.attr in _ANY_RECEIVER or (
                        func.attr in _ONE_ARGUMENT and len(call.args) == 1
                        and not call.keywords)):
                    yield call, roots + [func.value]
                    continue
                if std in _RUNNER_MODULES and func.attr in _RUNNERS and self._shell_words(call):
                    yield call, roots
                    continue
            elif isinstance(func, ast.Name):
                bound = mod.imports.get(func.id)
                if bound and bound[0] == "name":
                    if func.id and bound[2] in _FREE_DELETING.get(bound[1], ()):
                        yield call, roots
                        continue
                    if bound[1] in _RUNNER_MODULES and bound[2] in _RUNNERS \
                            and self._shell_words(call):
                        yield call, roots
                        continue
            callee = self._callee(mod, call, cls)
            if callee is not None and callee in self.deleters:
                yield call, self._arguments_into(callee, call, self.deleters[callee])

    def _path_parameters(self, key: tuple) -> frozenset:
        """The parameters of a function its return value takes its directory
        from (memoised; a cycle reads as none)."""
        memo = self._path_memo
        if key in memo:
            return memo[key]
        memo[key] = frozenset()
        fn = self._function(key)
        if fn is None:
            return memo[key]
        mod, cls = self.modules[key[1]], (key[2] if key[0] == "method" else None)
        memo[key] = frozenset(self._path_names(mod, fn, cls, _returns(fn)) & _parameters(fn))
        return memo[key]

    def _path_names(self, mod: _Module, fn: Optional[ast.AST], cls: Optional[str],
                    roots: Iterable[ast.AST]) -> Set[str]:
        """The local names `roots` can take their directory — the leftmost
        part of a path — from. A name that only fills in a file name
        (`SPOOL / f"{bucket}.json"`, `"canary:" + keys`) cannot point the
        path anywhere else, so a parameter reaching it that way does not
        make the function a deleter of its parameter."""
        sources = _sources_of(fn) if fn is not None else {}
        out: Set[str] = set()
        stack = list(roots)
        while stack:
            e = stack.pop()
            if isinstance(e, ast.Name):
                if e.id not in out:
                    out.add(e.id)
                    stack.extend(sources.get(e.id, []))
            elif isinstance(e, (ast.Attribute, ast.Subscript, ast.Starred, ast.Await,
                                ast.FormattedValue)):
                stack.append(e.value)
            elif isinstance(e, ast.BinOp):
                stack.append(e.left)
            elif isinstance(e, ast.JoinedStr):
                stack.extend(e.values[:1])
            elif isinstance(e, (ast.List, ast.Tuple, ast.Set)):
                stack.extend(e.elts)
            elif isinstance(e, ast.Dict):
                stack.extend(e.values)
            elif isinstance(e, (ast.ListComp, ast.GeneratorExp, ast.SetComp)):
                stack.append(e.elt)
            elif isinstance(e, ast.IfExp):
                stack.extend((e.body, e.orelse))
            elif isinstance(e, ast.BoolOp):
                stack.extend(e.values)
            elif isinstance(e, ast.Call):
                func = e.func
                callee = self._callee(mod, e, cls)
                if callee is not None:
                    stack.extend(self._arguments_into(callee, e, self._path_parameters(callee)))
                elif isinstance(func, ast.Attribute) and isinstance(func.value, ast.Constant):
                    # `"/".join([base, name])` — its first part; `"…".format()` none.
                    if func.attr == "join" and e.args:
                        first = e.args[0]
                        stack.append(first.elts[0] if isinstance(first, (ast.List, ast.Tuple))
                                     and first.elts else first)
                elif isinstance(func, ast.Attribute) and not self._names_a_module(mod, func.value):
                    stack.append(func.value)  # `p.with_suffix(…)`, `folder.glob(…)`
                else:
                    stack.extend(e.args[:1])  # `Path(x)`, `str(x)`, `os.path.join(x, …)`
        return out

    def _names_a_module(self, mod: _Module, node: ast.AST) -> bool:
        while isinstance(node, ast.Attribute):
            node = node.value
        return isinstance(node, ast.Name) and node.id in mod.imports and (
            mod.imports[node.id][0] == "module" or self._module(mod, node.id) is not None)

    def _arguments_into(self, key: tuple, call: ast.Call, params: frozenset) -> List[ast.AST]:
        """The arguments of `call` that land in `params` of the callee."""
        fn = self._function(key)
        if fn is None:
            return list(call.args) + [k.value for k in call.keywords]
        positional = [a.arg for a in fn.args.posonlyargs + fn.args.args]
        if key[0] == "method" and positional and positional[0] in ("self", "cls"):
            positional = positional[1:]
        named = positional + [a.arg for a in fn.args.kwonlyargs]
        out = []
        for i, arg in enumerate(call.args):
            if isinstance(arg, ast.Starred) or i >= len(positional) or positional[i] in params:
                out.append(arg)
        for k in call.keywords:
            if k.arg is None or k.arg in params or k.arg not in named:
                out.append(k.value)
        return out

    def _key(self, mod: _Module, qualname: str, cls: Optional[str]) -> tuple:
        if cls is None:
            return ("func", mod.rel, qualname)
        return ("method", mod.rel, cls, qualname.split(".", 1)[1])

    def settle(self, rels: Iterable[str]) -> None:
        """Every function among `rels` that deletes one of its own parameters,
        to a fixed point."""
        units = [(m, q, f, c) for m in (self.modules[r] for r in rels)
                 for q, f, c in m.units() if q != "<module>"]
        changed = True
        while changed:
            changed = False
            for mod, qualname, fn, cls in units:
                key = self._key(mod, qualname, cls)
                if key in self.deleters:
                    continue
                params = _parameters(fn)
                reached = set()
                for _, roots in self.deleting_calls(mod, fn, cls):
                    reached |= self._path_names(mod, fn, cls, roots) & params
                if reached:
                    self.deleters[key] = frozenset(reached)
                    changed = True

    def hits(self, rels: Iterable[str]) -> Dict[Unit, Tuple[str, ...]]:
        """(rel, qualname) → the places its deletions reach, for `rels`."""
        out: Dict[Unit, Set[str]] = defaultdict(set)
        for rel in rels:
            mod = self.modules[rel]
            for qualname, fn, cls in mod.units():
                for _, roots in self.deleting_calls(mod, fn, cls):
                    text = self.reach(mod, cls, fn, roots)
                    found = {m.group(0) for pattern in (DUCKDB_PLACES, KEPT_PLACES)
                             for m in pattern.finditer(text)}
                    if found:
                        out[(rel, qualname)] |= found
        return {k: tuple(sorted(v)) for k, v in out.items()}


def _python_files(repo: Path) -> Dict[str, str]:
    out = {}
    for top in PYTHON_TREES:
        for path in sorted((repo / top).rglob("*.py")):
            if _SKIP_PARTS & set(path.relative_to(repo).parts):
                continue
            out[path.relative_to(repo).as_posix()] = path.read_text(encoding="utf-8")
    return out


@lru_cache(maxsize=1)
def _tree() -> Tuple[Dict[str, _Module], Dict[tuple, frozenset]]:
    modules = {rel: _Module(rel, src) for rel, src in _python_files(REPO).items()}
    walk = Walk(modules)
    walk.settle(list(modules))
    return modules, dict(walk.deleters)


def python_deletions(overlay: Optional[Dict[str, str]] = None) -> Dict[Unit, Tuple[str, ...]]:
    """The tree's deletions that reach a protected place. With `overlay`
    (rel → source, replacing or adding a file), only the overlaid files'
    deletions, read against the rest of the tree."""
    base, deleters = _tree()
    if not overlay:
        walk = Walk(base)
        walk.deleters = dict(deleters)
        return walk.hits(list(base))
    py = {rel: src for rel, src in overlay.items() if rel.endswith(".py")}
    modules = dict(base)
    modules.update({rel: _Module(rel, src) for rel, src in py.items()})
    walk = Walk(modules)
    walk.deleters = dict(deleters)
    walk.settle(list(py))
    return walk.hits(list(py))


# ─── shell ───────────────────────────────────────────────────────────────────

_ASSIGN = re.compile(r"(?:^|[\s;&|(])(?:local\s+|readonly\s+|export\s+"
                     r"|declare\s+(?:-\w+\s+)*)?([A-Za-z_]\w*)=")
_FOR = re.compile(r"^\s*for\s+([A-Za-z_]\w*)\s+in\s+(.*?)(?:;\s*do\b|$)")
_SPLIT = re.compile(r"&&|\|\||[;|(){}]|\bthen\b|\bdo\b|\belse\b")
_PREFIXES = {"sudo", "command", "exec", "nohup", "env", "time", "!"}


def _shell_deletes(words: List[str]) -> bool:
    i = 0
    while i < len(words) and (words[i] in _PREFIXES or re.match(r"^\w+=", words[i])):
        i += 1
    if i < len(words) and words[i] == "xargs":
        i += 1
        while i < len(words) and words[i].startswith("-"):
            i += 1
    if i >= len(words):
        return False
    verb = words[i].strip("\"'")
    if verb in _SHELL_VERBS:
        return True
    if verb == "ln":
        return any(w.startswith("-") and ("f" in w.lstrip("-") or w == "--force")
                   for w in words[i + 1:])
    if verb == "find":
        rest = words[i + 1:]
        return "-delete" in rest or any(
            w == "-exec" and j + 1 < len(rest) and rest[j + 1] in _SHELL_VERBS
            for j, w in enumerate(rest))
    return False


def _segments(line: str) -> List[str]:
    """The simple commands of a line: split on `;`, `&&`, `||`, `|`, `then`,
    `do`, `else` and bare brackets, never inside a `${…}` or a `$(…)`."""
    held: List[str] = []
    out, i = [], 0
    while i < len(line):
        if line[i] == "$" and i + 1 < len(line) and line[i + 1] in "({":
            opening = line[i + 1]
            closing = ")" if opening == "(" else "}"
            depth, j = 1, i + 2
            while j < len(line) and depth:
                depth += 1 if line[j] == opening else -1 if line[j] == closing else 0
                j += 1
            held.append(line[i:j])
            out.append(f"\x00{len(held) - 1}\x00")
            i = j
            continue
        out.append(line[i])
        i += 1
    parts = _SPLIT.split("".join(out))
    return [re.sub(r"\x00(\d+)\x00", lambda m: held[int(m.group(1))], part) for part in parts]


def _logical_lines(text: str) -> List[Tuple[int, str]]:
    out, buf, start = [], "", 0
    for n, line in enumerate(text.splitlines(), 1):
        if not buf:
            start = n
        if line.rstrip().endswith("\\"):
            buf += line.rstrip()[:-1] + " "
            continue
        out.append((start, buf + line))
        buf = ""
    return out


def _word(text: str, i: int) -> str:
    """The shell word starting at `i`, quotes taken off, `$(…)` and `${…}`
    kept whole."""
    out, quote, depth = [], None, 0
    while i < len(text):
        c = text[i]
        if quote:
            if c == quote:
                quote = None
            else:
                out.append(c)
        elif c in "\"'":
            quote = c
        elif c in "({":
            depth += 1
            out.append(c)
        elif c in ")}":
            if depth == 0:
                break
            depth -= 1
            out.append(c)
        elif depth == 0 and (c.isspace() or c in ";&|"):
            break
        else:
            out.append(c)
        i += 1
    return "".join(out)


def _expand(text: str, values: Dict[str, List[str]]) -> str:
    """`$NAME` and `${NAME…}` replaced by every value the script gives NAME
    (and a `${NAME:-default}`'s default), a few rounds deep."""
    for _ in range(6):
        out, i, changed = [], 0, False
        while i < len(text):
            if text[i] == "$" and i + 1 < len(text) and text[i + 1] == "{":
                depth, j = 1, i + 2
                while j < len(text) and depth:
                    depth += {"{": 1, "}": -1}.get(text[j], 0)
                    j += 1
                inner = text[i + 2:j - 1]
                name = re.match(r"[A-Za-z_]\w*", inner)
                tail = inner[name.end():].lstrip(":?=+-#%") if name else ""
                out.append(" ".join(values.get(name.group(0), []) if name else [])
                           + (" " + tail if tail else ""))
                i, changed = j, True
                continue
            m = re.match(r"\$([A-Za-z_]\w*)", text[i:])
            if m:
                out.append(" ".join(values.get(m.group(1), [])))
                i, changed = i + m.end(), True
                continue
            out.append(text[i])
            i += 1
        text = "".join(out)
        if not changed:
            break
    return text


def shell_deletions_of(rel: str, text: str) -> Dict[Tuple[str, str], Tuple[str, ...]]:
    """(rel, the command as written) → the places it reaches."""
    lines = _logical_lines(text)
    values: Dict[str, List[str]] = defaultdict(list)
    for _, line in lines:
        if line.lstrip().startswith("#"):
            continue
        loop = _FOR.match(line)
        if loop:
            values[loop.group(1)].append(loop.group(2))
        for m in _ASSIGN.finditer(line):
            values[m.group(1)].append(_word(line, m.end()))
    out: Dict[Tuple[str, str], Tuple[str, ...]] = {}
    for _, line in lines:
        if line.lstrip().startswith("#"):
            continue
        for segment in _segments(line):
            words = segment.split()
            if not words or not _shell_deletes(words):
                continue
            # `ls … | xargs rm`: what it deletes comes down the pipe.
            read = line if words[0] == "xargs" or "read" in line.split() else segment
            text_ = read + " " + _expand(read, values)
            found = sorted({m.group(0) for m in SHELL_PLACES.finditer(text_)})
            if found:
                key = (rel, " ".join(words))
                out[key] = tuple(sorted(set(out.get(key, ())) | set(found)))
    return out


def shell_deletions(overlay: Optional[Dict[str, str]] = None
                    ) -> Dict[Tuple[str, str], Tuple[str, ...]]:
    if overlay:
        out = {}
        for rel, text in overlay.items():
            if rel.endswith(".sh"):
                out.update(shell_deletions_of(rel, text))
        return out
    out = {}
    for top in SHELL_TREES:
        for path in sorted((REPO / top).rglob("*.sh")):
            if _SKIP_PARTS & set(path.relative_to(REPO).parts):
                continue
            rel = path.relative_to(REPO).as_posix()
            out.update(shell_deletions_of(rel, path.read_text(encoding="utf-8")))
    return out
