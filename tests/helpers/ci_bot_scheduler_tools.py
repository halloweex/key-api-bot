"""Shell scripts run with the tools they call replaced by recorders.

    tools = FakeTools(tmp_path, ("docker", "flock"))
    tools.answer("docker", "image", "inspect", exit=1)
    result = tools.run(REPO / "deploy" / "quick_gate.sh", "tests/unit",
                       env={"REPO": str(checkout)})
    builds = [c for c in tools.calls("docker") if c.argv[0] == "build"]

Each fake is an executable first on the run's PATH. Every call is appended
to one log, in order, with its argv, and answered by the first `answer` rule
whose arguments are a prefix of the call's (rules are tried in the order they
were added; with none matching, exit 0 and no output). Two fakes do more than
answer: `flock` records which file the descriptor it was handed holds, and
`timeout` runs its command, bounded well below the test's own limit.

The run gets an environment of nothing else: PATH is the fakes and the
system's own bin directories, `DOCKER_HOST` names a socket that does not
exist and every proxy variable a closed port. So a script that reached a real
`docker` or `curl` by an absolute path would reach nothing, and the call its
fake should have recorded is missing from the log; `absolute_calls` says
whether a script names any such path at all.
"""
from __future__ import annotations

import json
import os
import re
import shlex
import shutil
import subprocess
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

BASH = shutil.which("bash") or "/bin/bash"

# The fake itself, one file every tool name links to. Standard library only;
# it finds its rules and its log beside the directory it is called from.
_FAKE = r'''
import json, os, subprocess, sys

HERE = os.path.dirname(os.path.abspath(sys.argv[0]))
ROOT = os.path.dirname(HERE)
TOOL = os.path.basename(sys.argv[0])
ARGS = sys.argv[1:]
CAP_S = 30.0


def record(**extra):
    entry = dict(tool=TOOL, argv=ARGS, **extra)
    with open(os.path.join(ROOT, "calls.jsonl"), "a", encoding="utf-8") as log:
        log.write(json.dumps(entry) + "\n")


def rule():
    with open(os.path.join(ROOT, "rules.json"), encoding="utf-8") as f:
        for r in json.load(f):
            if r["tool"] == TOOL and ARGS[:len(r["args"])] == r["args"]:
                return r
    return {"stdout": "", "stderr": "", "exit": 0}


def reply(r):
    sys.stdout.write(r["stdout"])
    sys.stderr.write(r["stderr"])
    sys.stdout.flush()
    return r["exit"]


def flock():
    """`flock [options] <fd>`, or `flock [options] <file> [-c] <command>`."""
    with_value = {"-w", "--wait", "--timeout", "-E", "--conflict-exit-code"}
    nonblocking, rest, i = False, [], 0
    while i < len(ARGS):
        a = ARGS[i]
        if a in ("-n", "--nb", "--nonblock"):
            nonblocking = True
        elif a in with_value:
            i += 1
        elif a.startswith("-") and not rest and a not in ("-c", "--command"):
            pass
        else:
            rest = ARGS[i:]
            break
        i += 1
    target = rest[0] if rest else ""
    try:
        st = os.fstat(int(target)) if target.isdigit() else os.stat(target)
        held = [st.st_dev, st.st_ino]
        error = None
    except (OSError, ValueError) as exc:
        held, error = None, str(exc) or repr(exc)
    record(lock=held, lock_error=error, nonblocking=nonblocking)
    command = rest[1:]
    if command[:1] in (["-c"], ["--command"]):
        command = ["sh", "-c", command[1]]
    if command:
        return subprocess.call(command)
    return reply(rule())


def timeout():
    """`timeout [options] <duration> <command…>`."""
    with_value = {"-s", "--signal", "-k", "--kill-after"}
    i = 0
    while i < len(ARGS) and ARGS[i].startswith("-"):
        i += 2 if ARGS[i] in with_value else 1
    record()
    duration, command = ARGS[i], ARGS[i + 1:]
    scale = {"s": 1, "m": 60, "h": 3600, "d": 86400}
    seconds = (float(duration[:-1]) * scale[duration[-1]] if duration[-1] in scale
               else float(duration))
    try:
        return subprocess.run(command, timeout=min(seconds, CAP_S)).returncode
    except subprocess.TimeoutExpired:
        return 124


if TOOL == "flock":
    sys.exit(flock())
if TOOL == "timeout":
    sys.exit(timeout())
record()
sys.exit(reply(rule()))
'''


@dataclass(frozen=True)
class Call:
    """One call of a fake: which tool, its arguments, and what the fake
    noted beside them (`lock`, `lock_error`, `nonblocking` for `flock`)."""
    tool: str
    argv: List[str]
    notes: Dict[str, object] = field(default_factory=dict)


class FakeTools:
    """Fakes for `tools` under `root`, which the test owns (a `tmp_path`)."""

    def __init__(self, root: Path, tools: Iterable[str]):
        self.root = Path(root)
        self.bin = self.root / "bin"
        self.bin.mkdir(parents=True, exist_ok=True)
        fake = self.root / "fake_tool.py"
        fake.write_text(f"#!{sys.executable} -I\n{_FAKE}", encoding="utf-8")
        fake.chmod(0o755)
        for name in tools:
            link = self.bin / name
            if not link.exists():
                link.symlink_to(fake)
        self._rules: List[dict] = []
        self._write_rules()
        (self.root / "calls.jsonl").write_text("", encoding="utf-8")
        for d in ("home", "tmp", "docker-config"):
            (self.root / d).mkdir(exist_ok=True)

    def _write_rules(self) -> None:
        (self.root / "rules.json").write_text(json.dumps(self._rules), encoding="utf-8")

    def answer(self, tool: str, *args: str, stdout: str = "", stderr: str = "",
               exit: int = 0) -> None:
        """Answer a call of `tool` whose arguments start with `args`."""
        self._rules.append({"tool": tool, "args": list(args), "stdout": stdout,
                            "stderr": stderr, "exit": exit})
        self._write_rules()

    def env(self, extra: Optional[Dict[str, str]] = None) -> Dict[str, str]:
        closed = "http://127.0.0.1:9"
        env = {
            "PATH": os.pathsep.join((str(self.bin), "/usr/bin", "/bin")),
            "HOME": str(self.root / "home"),
            "TMPDIR": str(self.root / "tmp"),
            "LANG": "C", "LC_ALL": "C",
            # Nothing a real client could reach, should one be called round
            # its fake.
            "DOCKER_HOST": f"unix://{self.root / 'no-such-docker.sock'}",
            "DOCKER_CONFIG": str(self.root / "docker-config"),
            "http_proxy": closed, "https_proxy": closed, "all_proxy": closed,
            "HTTP_PROXY": closed, "HTTPS_PROXY": closed, "ALL_PROXY": closed,
            "no_proxy": "", "NO_PROXY": "",
        }
        env.update(extra or {})
        return env

    def run(self, script: Path, *args: str, env: Optional[Dict[str, str]] = None,
            timeout: float = 90) -> subprocess.CompletedProcess:
        """`bash script args…` under the fakes, from `root`."""
        return subprocess.run(
            [BASH, str(script), *args], env=self.env(env), cwd=self.root,
            capture_output=True, text=True, timeout=timeout)

    def run_text(self, text: str, **kwargs) -> subprocess.CompletedProcess:
        script = self.root / "fragment.sh"
        script.write_text(text, encoding="utf-8")
        return self.run(script, **kwargs)

    def calls(self, tool: Optional[str] = None) -> List[Call]:
        """Every call, in the order made; only `tool`'s when named."""
        out = []
        for line in (self.root / "calls.jsonl").read_text(encoding="utf-8").splitlines():
            entry = json.loads(line)
            name, argv = entry.pop("tool"), entry.pop("argv")
            if tool is None or name == tool:
                out.append(Call(name, argv, entry))
        return out


def absolute_calls(text: str, tools: Sequence[str]) -> List[str]:
    """Every path in `text` that names one of `tools` (`/usr/bin/docker`,
    `$DIR/bin/curl`) — a call through any of them would go round the fake
    of that name, which only a bare name on PATH reaches."""
    names = "|".join(re.escape(t) for t in tools)
    return re.findall(r"(?<![\w.-])((?:[\w.$+{}-]*/)+(?:" + names + r"))(?![\w./-])", text)


# ─── what a recorded call says ───────────────────────────────────────────────

_RUN_VALUE_OPTIONS = {
    "--name", "--network", "--net", "-e", "--env", "--env-file", "-v", "--volume",
    "--mount", "-u", "--user", "--entrypoint", "--pull", "-w", "--workdir", "-l",
    "--label", "-p", "--publish", "--add-host", "-m", "--memory", "--cpus",
    "--tmpfs", "--platform", "-h", "--hostname", "--restart", "--shm-size",
}
_RUN_FLAGS = {"-d", "--detach", "--rm", "-i", "--interactive", "-t", "--tty",
              "-it", "-ti", "--init", "--privileged", "--read-only", "-q", "--quiet"}


@dataclass(frozen=True)
class DockerRun:
    image: str
    command: List[str]
    entrypoint: Optional[str]
    mounts: List[Tuple[str, str]]       # (source on the host, target inside)
    env: Dict[str, str]

    def target_of(self, source: Path) -> Optional[str]:
        """Where `source` is mounted inside the container, if it is."""
        return next((dst for src, dst in self.mounts if Path(src) == Path(source)), None)

    def shell(self) -> Optional[str]:
        """The shell program the container runs, when it runs one."""
        argv = ([self.entrypoint] if self.entrypoint else []) + self.command
        if len(argv) >= 3 and Path(argv[0]).name in ("sh", "bash") and argv[1] == "-c":
            return argv[2]
        return None


def docker_run(argv: Sequence[str]) -> DockerRun:
    """`docker run …` as Docker reads it. An option this does not know is
    an error, so that a value is never mistaken for the image."""
    assert argv[0] == "run", argv
    mounts, env, entrypoint, i = [], {}, None, 1
    while i < len(argv) and argv[i].startswith("-"):
        option, _, inline = argv[i].partition("=")
        if option in _RUN_FLAGS:
            i += 1
            continue
        if option not in _RUN_VALUE_OPTIONS:
            raise ValueError(f"docker run option this parser does not know: {argv[i]}")
        value = inline if inline else argv[i + 1]
        i += 1 if inline else 2
        if option in ("-v", "--volume"):
            source, target = value.split(":")[:2]
            mounts.append((source, target))
        elif option in ("-e", "--env"):
            key, _, val = value.partition("=")
            env[key] = val
        elif option == "--entrypoint":
            entrypoint = value
    return DockerRun(image=argv[i], command=list(argv[i + 1:]), entrypoint=entrypoint,
                     mounts=mounts, env=env)


@dataclass(frozen=True)
class DockerBuild:
    dockerfile: Optional[str]
    tags: List[str]
    context: str


def docker_build(argv: Sequence[str]) -> DockerBuild:
    assert argv[0] == "build", argv
    values = {"-f", "--file", "-t", "--tag", "--build-arg", "--target", "--platform",
              "--progress", "--network", "--label", "--cache-from", "--iidfile"}
    dockerfile, tags, positional, i = None, [], [], 1
    while i < len(argv):
        option, _, inline = argv[i].partition("=")
        if option in values:
            value = inline if inline else argv[i + 1]
            i += 1 if inline else 2
            if option in ("-f", "--file"):
                dockerfile = value
            elif option in ("-t", "--tag"):
                tags.append(value)
            continue
        if not argv[i].startswith("-"):
            positional.append(argv[i])
        i += 1
    return DockerBuild(dockerfile=dockerfile, tags=tags, context=positional[-1])


@dataclass(frozen=True)
class PipInstall:
    requirements: Dict[str, object]      # canonical name -> packaging Requirement
    constraints: List[str]
    requirement_files: List[str]


_CONTROL = {";", "&&", "||", "|", "&", "(", ")", "{", "}"}
_PIP_VALUES = {"-c", "--constraint", "-r", "--requirement", "-i", "--index-url",
               "--extra-index-url", "-t", "--target", "--prefix", "--root", "-e",
               "--editable", "-f", "--find-links", "--log", "--cache-dir",
               "--trusted-host", "--retries", "--timeout", "--proxy", "--report"}


def _simple_commands(shell: str) -> List[List[str]]:
    """The words of each simple command in `shell`, redirections dropped."""
    lexer = shlex.shlex(shell.replace("\\\n", " ").replace("\n", " ; "),
                        posix=True, punctuation_chars=True)
    lexer.whitespace_split = True
    commands: List[List[str]] = [[]]
    redirect = False
    for token in lexer:
        if token in _CONTROL:
            commands.append([])
        elif set(token) <= set("<>&") and ("<" in token or ">" in token):
            if commands[-1] and commands[-1][-1].isdigit():
                commands[-1].pop()           # the `2` of `2>&1`
            redirect = True
        elif redirect:
            redirect = False
        else:
            commands[-1].append(token)
    return [c for c in commands if c]


def pip_installs(shell: str) -> List[PipInstall]:
    """Every `pip install` the shell program runs, as pip reads it."""
    from packaging.requirements import Requirement
    from packaging.utils import canonicalize_name

    found = []
    for words in _simple_commands(shell):
        if Path(words[0]).name in ("pip", "pip3") and words[1:2] == ["install"]:
            args = words[2:]
        elif (Path(words[0]).name.startswith("python") and words[1:3] == ["-m", "pip"]
              and words[3:4] == ["install"]):
            args = words[4:]
        else:
            continue
        reqs, constraints, files, i = {}, [], [], 0
        while i < len(args):
            option, _, inline = args[i].partition("=")
            if option in _PIP_VALUES:
                value = inline if inline else args[i + 1]
                i += 1 if inline else 2
                if option in ("-c", "--constraint"):
                    constraints.append(value)
                elif option in ("-r", "--requirement"):
                    files.append(value)
                continue
            if not args[i].startswith("-"):
                req = Requirement(args[i])
                reqs[canonicalize_name(req.name)] = req
            i += 1
        found.append(PipInstall(reqs, constraints, files))
    return found


__all__ = [
    "BASH", "Call", "DockerBuild", "DockerRun", "FakeTools", "PipInstall",
    "absolute_calls", "docker_build", "docker_run", "pip_installs",
]
