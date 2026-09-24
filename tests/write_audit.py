"""Run a script's `main()` in a fresh interpreter and list everything it wrote.

"Writes nothing" is a claim about a process, so it is checked on one: a child
Python with an audit hook (PEP 578) installed before the script is even
imported, so module import time counts too. The hook records every `open` for
writing, every filesystem mutation, every network connection and every command
started, and the child prints them as JSON after `main()` returns.

A fresh interpreter rather than a hook in the test process, for two reasons: a
hook cannot be removed once added, and the suite's own imports have already
happened in the test process, so an import that wrote a file would be invisible
there. `-B` because the interpreter's bytecode cache is the interpreter writing,
not the script.

The hook sees what Python does, and nothing a C extension does on its own. The
one store client that matters here is DuckDB, which opens and writes its file
from C++ without raising a single audit event; SQLite's `connect` is audited,
and every networked store is a `socket.connect`. So the child also reports the
modules loaded when `main()` returned, and a test asserts DuckDB is not among
them: a library never imported cannot have written anything.
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional, Sequence

REPO = Path(__file__).resolve().parents[1]
MARK = "@@write-audit@@"

_CHILD = r'''
import json, os, sys

_EVENTS = []
_MUTATIONS = {
    "os.remove", "os.rename", "os.mkdir", "os.rmdir", "os.truncate",
    "os.link", "os.symlink", "os.chmod", "os.chown", "os.utime",
    "shutil.copyfile", "shutil.copymode", "shutil.copystat", "shutil.move",
    "shutil.rmtree", "tempfile.mkstemp", "tempfile.mkdtemp", "sqlite3.connect",
}
_COMMANDS = {"subprocess.Popen", "os.system", "os.exec", "os.posix_spawn",
             "os.spawn", "os.fork", "os.forkpty", "pty.spawn"}
_WRITE_FLAGS = os.O_WRONLY | os.O_RDWR | os.O_CREAT | os.O_APPEND | os.O_TRUNC

def _hook(event, args):
    if event == "open":
        path, mode, flags = args
        if isinstance(mode, str):
            writing = any(c in mode for c in "wax+")
        else:
            writing = isinstance(flags, int) and bool(flags & _WRITE_FLAGS)
        if writing:
            _EVENTS.append(["write", str(path)])
    elif event in _MUTATIONS:
        _EVENTS.append(["mutate", event, repr(args)[:200]])
    elif event == "socket.connect":
        _EVENTS.append(["connect", repr(args[1])])
    elif event in _COMMANDS:
        _EVENTS.append(["command", event])

sys.addaudithook(_hook)
sys.path.insert(0, REPO)
import importlib
script = importlib.import_module(MODULE)
SETUP
_code = script.main(ARGV)
sys.stdout.flush()
sys.stderr.write("\n" + MARK + json.dumps(
    {"code": _code, "events": _EVENTS, "modules": sorted(sys.modules)}) + "\n")
'''


@dataclass
class Audited:
    code: int
    stdout: str
    stderr: str
    events: List[list]
    # Every module loaded when `main()` returned.
    modules: List[str]

    @property
    def writes(self) -> List[str]:
        return [e[1] for e in self.events if e[0] == "write"]

    @property
    def mutations(self) -> List[list]:
        return [e for e in self.events if e[0] == "mutate"]

    @property
    def connects(self) -> List[str]:
        return [e[1] for e in self.events if e[0] == "connect"]

    @property
    def commands(self) -> List[str]:
        return [e[1] for e in self.events if e[0] == "command"]


def run_main_audited(
    module: str,
    argv: Sequence[str],
    *,
    setup: str = "",
    env: Optional[dict] = None,
    cwd: Optional[Path] = None,
    timeout: float = 120,
) -> Audited:
    """`module.main(argv)` in a child interpreter under the audit hook.

    `setup` runs after the module is imported and before `main`, with the
    module bound to `script` — where a test swaps in a fake connection.
    """
    code = (
        _CHILD.replace("REPO", repr(str(REPO)))
        .replace("MODULE", repr(module))
        .replace("ARGV", repr(list(argv)))
        .replace("MARK", repr(MARK))
        .replace("SETUP", setup)
    )
    proc = subprocess.run(
        [sys.executable, "-B", "-c", code],
        capture_output=True, text=True, timeout=timeout,
        cwd=str(cwd) if cwd else None,
        env={**os.environ, **(env or {})},
    )
    head, sep, tail = proc.stderr.rpartition(MARK)
    if not sep:
        raise AssertionError(
            f"the child never reached the end of main():\n{proc.stdout}\n{proc.stderr}")
    result = json.loads(tail.strip())
    return Audited(code=result["code"], stdout=proc.stdout, stderr=head,
                   events=result["events"], modules=result["modules"])
