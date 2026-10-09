"""What the migrations leave behind, read by running them against a recorder.

Every revision here is plain `op.execute` — no `get_bind`, no autogenerate —
so a revision's upgrade or downgrade can be run with `op` replaced by an object
that only writes down the SQL it was handed. That is how a test asks "what
comment did table X carry at revision R" without a server, and without a
second, hand-kept copy of the answer: the f-string loops of 0008 and 0009
render exactly as they did when they ran, which no reading of the source text
would.

Only `COMMENT ON TABLE`, `CREATE TABLE` and `DROP TABLE` are followed, which is
all a table's comment depends on (no revision renames a table or moves its
schema). The texts come back unescaped — `''` read as `'` — so they compare
with what `obj_description` returns.
"""
from __future__ import annotations

import importlib.util
import re
from pathlib import Path
from types import ModuleType
from typing import Dict, List, Optional

VERSIONS = Path(__file__).resolve().parents[1] / "migrations" / "versions"

# A string constant, and the ones SQL glues onto it: two literals separated
# only by whitespace containing a newline are one constant (0032 and 0033 write
# their comments that way, and reading only the first piece truncates them).
_LITERAL = r"'(?:[^']|'')*'(?:[ \t]*\n\s*'(?:[^']|'')*')*"

_EVENT = re.compile(
    r"CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?(?P<created>[\w.]+)"
    r"|DROP\s+TABLE\s+(?:IF\s+EXISTS\s+)?(?P<dropped>[\w.]+)"
    r"|COMMENT\s+ON\s+TABLE\s+(?P<commented>[\w.]+)\s+IS\s+"
    rf"(?P<value>NULL|{_LITERAL})",
    re.I,
)

_ONE_COMMENT = re.compile(
    rf"COMMENT ON TABLE (?P<table>[a-z_]+\.[a-z_]+) IS (?P<value>NULL|{_LITERAL})",
    re.S,
)
_PIECE = re.compile(r"'((?:[^']|'')*)'")


class _Recorder:
    """Stands in for `alembic.op`. Anything but `execute` is a revision this
    module was not written for, and says so rather than recording nothing."""

    def __init__(self) -> None:
        self.sql: List[str] = []

    def execute(self, sql, *args, **kwargs) -> None:
        self.sql.append(str(sql))

    def __getattr__(self, name):
        raise AttributeError(f"op.{name}: only op.execute can be replayed")


def load(path: Path) -> ModuleType:
    spec = importlib.util.spec_from_file_location(f"_replay_{path.stem}", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def revisions() -> Dict[str, ModuleType]:
    """{revision id: its module}, every file in `migrations/versions/`."""
    out = {}
    for path in sorted(VERSIONS.glob("*.py")):
        module = load(path)
        out[module.revision] = module
    return out


def lineage(head: str) -> List[ModuleType]:
    """The modules from the base up to and including `head`, in order."""
    mods = revisions()
    out, cursor = [], head
    while cursor:
        module = mods[cursor]
        out.append(module)
        cursor = module.down_revision
    return out[::-1]


def statements(module: ModuleType, function: str) -> List[str]:
    """The SQL `module.<function>()` hands to `op.execute`, in order."""
    recorder, original = _Recorder(), module.op
    module.op = recorder
    try:
        getattr(module, function)()
    finally:
        module.op = original
    return recorder.sql


def _unquote(value: str) -> Optional[str]:
    if value.upper() == "NULL":
        return None
    return "".join(piece.replace("''", "'") for piece in _PIECE.findall(value))


def apply(state: Dict[str, Optional[str]], sql: List[str]) -> Dict[str, Optional[str]]:
    """`state` ({table: comment or None}) after `sql` has run."""
    out = dict(state)
    for text in sql:
        for event in _EVENT.finditer(text):
            if event.group("created"):
                out.setdefault(event.group("created"), None)
            elif event.group("dropped"):
                out.pop(event.group("dropped"), None)
            else:
                out[event.group("commented")] = _unquote(event.group("value"))
    return out


def comments_at(revision: str) -> Dict[str, Optional[str]]:
    """{table: comment or None} for every table the migrations up to and
    including `revision` created or commented."""
    state: Dict[str, Optional[str]] = {}
    for module in lineage(revision):
        state = apply(state, statements(module, "upgrade"))
    return state


def single_comment(sql: str):
    """`(table, text or None)` if `sql` is exactly one `COMMENT ON TABLE`, else
    None — so a revision that is meant to carry nothing else can be held to it."""
    match = _ONE_COMMENT.fullmatch(sql.strip())
    if not match:
        return None
    return match.group("table"), _unquote(match.group("value"))
