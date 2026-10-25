"""What a host script mounts into a container, read the way the shell reads it.

Shared by `tests/unit/test_archive_image.py`, which holds every checkout file
mounted into the pinned image to what that image carries, and
`tests/unit/test_backup_script_guards.py`, which finds the scripts that mount
one. The first walk recognised two spellings — `-v "$PWD/x.py:` and
`-v "$VAR:/x.py:ro"` with `VAR="$REPO/x.py"` — and the same mount written
`${PWD}`, `$(pwd)` or through a variable named anything else went past it:
a file from main running inside the pinned image, held to nothing.

So the walk reads every mount and accounts for each source:

  * every `-v` / `--volume` argument that is a host:container pair, and every
    `--mount` that is not a named volume, wherever it is written — a command,
    a function, the `DOCKER_RUN` array — each read as one shell word, quotes
    and `$( … )` nested in it included;
  * `${NAME}` is `$NAME`; `$PWD`, `$(pwd)`, `$(pwd -P)` and `/opt/key-api-bot`
    are the checkout (each script `cd`s to its root first, the recipes
    `cd /opt/key-api-bot`); a variable is replaced by every value the script
    assigns it, to a fixed point; a positional `$N` inside a function by the
    N-th argument of every call of that function;
  * a source that resolves into the checkout is a file (`found`), a path
    under `data/` (`data`, the runtime directory nothing imports), or a
    problem — the checkout's root or any other directory or file of it;
  * a source that resolves nowhere near it is reported as it is written
    (`outside`), for the caller to hold to a list with reasons. One the walk
    cannot resolve at all is outside too, so a spelling of the checkout this
    walk does not know fails the caller's list rather than passing unseen.
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Callable, Dict, List, Set, Tuple

CHECKOUT = "@checkout"

_VOLUME = re.compile(r"(?<![\w-])(?:-v|--volume)(?=[ \t=])")
_MOUNT = re.compile(r"(?<![\w-])--mount(?=[ \t=])")
_ASSIGN = re.compile(r"(?:^|(?<=[\s;(]))(?:(?:local|export|readonly|declare)[ \t]+)?"
                     r"([A-Za-z_]\w*)=", re.M)
_FUNCTION = re.compile(r"^[ \t]*(?:function[ \t]+)?(\w+)[ \t]*\(\)[ \t]*\{", re.M)
_VAR = re.compile(r"\$([A-Za-z_]\w*)")
_POSITIONAL = re.compile(r"^\$([1-9])(.*)$", re.S)
_ROOT = re.compile(r"\$\(pwd(?:[ \t]+-[LP])?\)|\$PWD\b|/opt/key-api-bot(?![\w.-])")
_ENDS_WORD = " \t\n;|&<>)"


@dataclass
class Mounts:
    found: Dict[str, str] = field(default_factory=dict)    # checkout file -> site
    data: Set[str] = field(default_factory=set)            # checkout paths under data/
    outside: Set[str] = field(default_factory=set)         # sources as written
    problems: List[str] = field(default_factory=list)


def code_of(text: str) -> str:
    """The lines that run, continuations joined: a mount written on the line
    after `docker run \\` is part of that command."""
    lines = [ln for ln in text.splitlines() if not ln.lstrip().startswith("#")]
    return re.sub(r"\\\n\s*", " ", "\n".join(lines))


def normalise(code: str) -> str:
    code = re.sub(r"\$\{([A-Za-z_]\w*|[1-9])\}", r"$\1", code)
    return _ROOT.sub(CHECKOUT, code)


# ── one shell word ────────────────────────────────────────────────────────────

def _close_paren(s: str, i: int) -> int:
    """The index of the `)` closing the `(` at `i`, quotes and nesting
    honoured; the end of `s` when there is none."""
    depth, n = 0, len(s)
    while i < n:
        c = s[i]
        if c == "\\":
            i += 2
            continue
        if c == "'":
            j = s.find("'", i + 1)
            i = (n if j < 0 else j) + 1
            continue
        if c == '"':
            i = _close_quote(s, i) + 1
            continue
        if c == "(":
            depth += 1
        elif c == ")":
            depth -= 1
            if depth == 0:
                return i
        i += 1
    return n - 1


def _close_quote(s: str, i: int) -> int:
    """The index of the `"` closing the one at `i`, `$( … )` inside it
    honoured."""
    i += 1
    n = len(s)
    while i < n and s[i] != '"':
        if s[i] == "\\":
            i += 2
            continue
        if s.startswith("$(", i):
            i = _close_paren(s, i + 1) + 1
            continue
        i += 1
    return i


def read_word(s: str, i: int) -> Tuple[str, int]:
    """The shell word starting at `i` with its quotes removed — a command
    substitution kept as written — and the index after it."""
    out, n = [], len(s)
    while i < n and s[i] not in _ENDS_WORD:
        c = s[i]
        if c == "\\" and i + 1 < n:
            out.append(s[i + 1])
            i += 2
        elif c == "'":
            j = s.find("'", i + 1)
            j = n if j < 0 else j
            out.append(s[i + 1:j])
            i = j + 1
        elif c == '"':
            j = _close_quote(s, i)
            out.append(s[i + 1:j])
            i = j + 1
        elif s.startswith("$(", i):
            j = _close_paren(s, i + 1)
            out.append(s[i:j + 1])
            i = j + 1
        else:
            out.append(c)
            i += 1
    return "".join(out), i


def _argument_after(s: str, end: int) -> str:
    """The word after an option: `-v WORD`, `--volume=WORD`."""
    i = end
    if i < len(s) and s[i] == "=":
        i += 1
    while i < len(s) and s[i] in " \t":
        i += 1
    return read_word(s, i)[0]


def _words_after(s: str, end: int) -> List[str]:
    """The words of a simple command after its name, to the first operator."""
    words, i, n = [], end, len(s)
    while True:
        while i < n and s[i] in " \t":
            i += 1
        if i >= n or s[i] in "\n;|&<>)":
            return words
        word, i = read_word(s, i)
        words.append(word)


# ── the script ────────────────────────────────────────────────────────────────

def _assignments(code: str) -> Dict[str, Set[str]]:
    found: Dict[str, Set[str]] = {}
    for m in _ASSIGN.finditer(code):
        found.setdefault(m.group(1), set()).add(read_word(code, m.end())[0])
    return found


def resolve(expr: str, assigned: Dict[str, Set[str]], _seen: frozenset = frozenset(),
            _budget: List[int] = None) -> Set[str]:
    """Every string `expr` can be once each variable the script assigns is
    replaced by each value it is assigned. A variable inside its own value
    is left as written, and so is one the script never assigns."""
    budget = _budget if _budget is not None else [256]
    for m in _VAR.finditer(expr):
        name = m.group(1)
        if name in assigned and name not in _seen:
            out: Set[str] = set()
            for value in sorted(assigned[name]):
                budget[0] -= 1
                if budget[0] < 0:
                    return out | {expr}
                out |= resolve(expr[:m.start()] + value + expr[m.end():], assigned,
                               _seen | {name}, budget)
            return out
    return {expr}


def _enclosing_function(code: str, pos: int):
    name = None
    for m in _FUNCTION.finditer(code):
        if m.start() > pos:
            break
        depth, end = 0, None
        for i in range(m.end() - 1, len(code)):
            if code[i] == "{":
                depth += 1
            elif code[i] == "}":
                depth -= 1
                if depth == 0:
                    end = i
                    break
        if end is not None and m.end() <= pos <= end:
            name = m.group(1)
    return name


def _calls(code: str, function: str) -> List[List[str]]:
    calls = []
    for m in re.finditer(r"(?<![\w-])" + re.escape(function) + r"(?![\w-])", code):
        if code[m.end():].lstrip(" \t").startswith("()"):     # the definition
            continue
        calls.append(_words_after(code, m.end()))
    return calls


def _sources(code: str) -> List[Tuple[int, str]]:
    """(position, source as written) of every host mount."""
    out = []
    for m in _VOLUME.finditer(code):
        value = _argument_after(code, m.end())
        if ":" in value:                       # host:container, not a bare volume
            out.append((m.start(), value.split(":", 1)[0]))
    for m in _MOUNT.finditer(code):
        spec = dict(part.split("=", 1)
                    for part in _argument_after(code, m.end()).split(",") if "=" in part)
        if spec.get("type") in ("volume", "tmpfs"):   # not a host path
            continue
        out.append((m.start(), spec.get("source", spec.get("src", ""))))
    return out


def walk(text: str, site: str, *, is_checkout_file: Callable[[str], bool]) -> Mounts:
    """The mounts of one script or one recipe, judged. `is_checkout_file`
    answers whether a checkout-relative path is a file of the checkout."""
    code = normalise(code_of(text))
    assigned = _assignments(code)
    result = Mounts()
    for pos, source in _sources(code):
        written = [source]
        positional = _POSITIONAL.match(source)
        if positional:
            function = _enclosing_function(code, pos)
            n, rest = int(positional.group(1)), positional.group(2)
            written = [args[n - 1] + rest for args in (_calls(code, function) if function else [])
                       if len(args) >= n]
            if not written:
                result.problems.append(f"{site}: `{source}` is mounted, and no call "
                                       f"passes that argument")
                continue
        for expr in written:
            for value in resolve(normalise(expr), assigned):
                if value != CHECKOUT and not value.startswith(CHECKOUT + "/"):
                    result.outside.add(expr)
                    continue
                rel = re.sub(r"/+", "/", value[len(CHECKOUT):].strip("/"))
                rel = re.sub(r"(^|/)\./", r"\1", rel)
                if rel == "data" or rel.startswith("data/"):
                    result.data.add(rel)
                elif rel.endswith(".py") and is_checkout_file(rel):
                    result.found[rel] = site
                else:
                    result.problems.append(
                        f"{site}: mounts {rel or 'the checkout itself'} (`{expr}`): code "
                        f"from the checkout that no import list holds")
    return result
