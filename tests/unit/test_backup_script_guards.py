"""The backup family's shell, as facts about the files.

`tests/unit/test_ci_workflow.py` already walks every script for `docker rm -v`
and the build-cache cap, because both are about the *host's disk*. These are
about the copies themselves, and they are the rules this repository has
already paid for once each:

* a backup script that does not stop on the first error carries on to the next
  step and reports the wrong one, or writes a freshness marker after a stage
  that failed;
* retention keyed on age deletes the copy you most want on exactly the week
  the maker was broken — the diagnostic agent recommended that form twice in
  one week and both would have destroyed a recovery point;
* a passphrase on a command line is readable in `ps` by every user on the box
  for as long as the process runs, and in `/proc` if it is in the environment.

**The subjects are derived, not named.** Naming them is the mistake this
repository has now made three times — `gate_with_stores.sh` got `docker rm -v`
and `quick_gate.sh` leaked volumes behind a green suite for five days — so the
set is "every shell file under deploy/ that touches the backups or reads
deploy/backup.env". A fourth backup script inherits all three rules by
existing.

The first rule has one narrower subject set, and only the first: `set -Eeuo
pipefail` is a statement about a process, so a file that is `source`d is
outside it. The other two are statements about what a file may *do*, and being
sourced changes nothing about that — which is why `deploy/pg_offsite_lib.sh`,
where the remote deletion primitive lives, is inside them.
"""
from __future__ import annotations

import re
from pathlib import Path

from tests.archive_mounts import walk as walk_mounts

REPO = Path(__file__).resolve().parents[2]

_ALL_SHELL = sorted(
    p for p in (REPO / "deploy").rglob("*.sh") if "runbooks" not in p.parts
)
_SOURCED = {
    p.name
    for p in _ALL_SHELL
    if any(f"source deploy/{p.name}" in other.read_text() for other in _ALL_SHELL
           if other != p)
}

# Every shell file under deploy/ that touches the backups or reads
# deploy/backup.env — libraries included. Two of the three rules below are
# about what a file may *do*, and a file being sourced rather than executed
# changes nothing about that.
BACKUP_SHELL = [
    p for p in _ALL_SHELL
    if re.search(r"backups/|BACKUP_|backup\.env", p.read_text())
]

# The subset that is *run*. Sourced libraries are excluded here, by behaviour
# and not by name: `set -Eeuo pipefail` inside a file that is `source`d would
# impose errexit on whatever sourced it, which is a decision for the caller.
#
# That exclusion used to apply to all three rules, with this reason given for
# it — and the file it excluded is deploy/pg_offsite_lib.sh, which holds the
# remote deletion primitive. An age-keyed sweep added beside `remote_rm`, the
# natural place for one, would have been the rule this repository has paid for
# twice, reintroduced behind a green suite. An exclusion is a list of subjects
# too; it earns the same suspicion.
BACKUP_SCRIPTS = [p for p in BACKUP_SHELL if p.name not in _SOURCED]


def _calls(text: str) -> str:
    """The code lines with every backslash continuation joined, so a command
    whose arguments start on the next line reads as one command: `gpg \\` then
    `--symmetric ...` is a gpg call like any other."""
    return re.sub(r"\\\n\s*", " ", "\n".join(_code_lines(text)))


def _code_lines(text: str):
    """Lines that run, not lines that explain.

    Every occurrence of `-mtime` and `--delete` in this tree today is inside a
    comment saying why it is not used, so a grep over the raw source would
    match the reasoning and fail the file that documents the rule best. Assert
    on structure, not prose."""
    for line in text.splitlines():
        if line.lstrip().startswith("#"):
            continue
        yield line


def test_the_walk_found_the_family():
    """If either list empties or halves, the derivation has gone wrong and
    every assertion below would pass by looking at nothing."""
    names = {p.name for p in BACKUP_SCRIPTS}
    assert len(names) >= 6, names
    assert {"pg_offsite.sh", "pg_restore_drill.sh", "offsite_check.sh",
            "offsite_parquet.sh"} <= names

    # And the library the two rules below have to reach. It is where
    # `remote_rm` lives, so a retention written by age would be written there.
    shell = {p.name for p in BACKUP_SHELL}
    assert "pg_offsite_lib.sh" in shell, shell
    assert names < shell, "nothing is being excluded; _SOURCED has stopped working"


def test_every_backup_script_stops_on_the_first_error():
    for path in BACKUP_SCRIPTS:
        first = next((ln.strip() for ln in _code_lines(path.read_text())
                      if ln.startswith("set ")), None)
        assert first == "set -Eeuo pipefail", (
            f"{path.relative_to(REPO)}: first `set` is {first!r}. Without -e a "
            f"failed stage runs the next one; without -E the ERR trap is not "
            f"inherited by functions, so a failure inside one aborts the "
            f"shell without ever alerting."
        )


def test_no_backup_retention_is_keyed_on_age():
    """Count, or an anchor that means something — never a date.

    `pg_basebackup.sh` anchors WAL deletion to the oldest retained base
    backup's START WAL; `pg_backup.sh` and `pg_offsite.sh` keep the newest N.
    An age would let a week of failed runs quietly empty the archive."""
    forbidden = re.compile(r"(?<![\w-])-(?:mtime|ctime|atime|newermt)\b")
    for path in BACKUP_SHELL:
        for line in _code_lines(path.read_text()):
            assert not forbidden.search(line), (
                f"{path.relative_to(REPO)}: {line.strip()!r} selects files by "
                f"age. Retention here is by count or by a named anchor."
            )
            assert "--delete" not in line, (
                f"{path.relative_to(REPO)}: {line.strip()!r} — `rsync "
                f"--delete` mirrors a wiped local directory onto the off-site "
                f"copy and erases the history."
            )


def test_a_passphrase_is_never_on_a_command_line():
    """`--passphrase-file`, always, and the file is never opened.

    Two spellings put the escrowed passphrase where the whole box can read it:
    `--passphrase <value>`, which `ps` shows to every user for as long as gpg
    runs, and reading the file into a shell variable, which puts it in this
    process's environment and so in /proc. Handing gpg the path costs nothing
    and does neither.

    This is deliberately about the *passphrase* and not about every string
    containing PASSWORD: the drill starts a throwaway `--network none`
    container with an invented `POSTGRES_PASSWORD`, which is not a secret and
    is exactly how deploy/pg_pitr_drill.sh has always done it."""
    users = []
    for path in BACKUP_SHELL:
        text = path.read_text()
        code = list(_code_lines(text))
        # `gpg\s+-`, not `gpg\b`: deploy/restore-test.sh mentions the filename
        # env.gpg and invokes nothing. `gpg_safe` is gpg (deploy/gpg_safe.sh),
        # and every invocation goes through it — which is what
        # test_gpg_runs_only_through_gpg_safe holds.
        if not re.search(r"(?<![\w./-])gpg(?:_safe)?\s+-", _calls(text)):
            continue
        users.append(path.name)
        for line in code:
            assert not re.search(r"--passphrase(?!-file)", line), (
                f"{path.relative_to(REPO)}: {line.strip()!r} puts the "
                f"passphrase itself on gpg's command line"
            )
            assert not re.search(r"(?:cat|read|<)\s*[\"']?\$\{?\w*PASSFILE", line), (
                f"{path.relative_to(REPO)}: {line.strip()!r} reads the "
                f"passphrase file's contents; pass gpg the path instead"
            )
        assert "--passphrase-file" in text, (
            f"{path.relative_to(REPO)} runs gpg without --passphrase-file"
        )

    assert {"offsite_parquet.sh", "pg_offsite.sh", "pg_restore_drill.sh"} <= set(users), users


def test_gpg_runs_only_through_gpg_safe():
    """Every gpg in deploy/ and scripts/ runs in a home of its own.

    On 2026-10-10 the Postgres off-site copy stopped at `encrypt` because a
    root SSH login had started systemd's user manager, which holds the
    sockets of root's *default* gpg home, eleven seconds before cron's gpg
    went looking for an agent. deploy/gpg_safe.sh gives each call a fresh
    home and asks a call that found no agent again. A script that called gpg
    directly would be back on root's default sockets, so the rule is that
    none does — derived from what each file runs, not from a list of the
    scripts that happened to use gpg the day this was written."""
    shell = sorted(
        p for d in ("deploy", "scripts") for p in (REPO / d).rglob("*.sh")
        if "runbooks" not in p.parts
    )
    definers = [p for p in shell
                if re.search(r"(?m)^gpg_safe\(\)\s*\{", p.read_text())]
    assert [p.relative_to(REPO).as_posix() for p in definers] == ["deploy/gpg_safe.sh"]

    callers = []
    for path in shell:
        if path in definers:
            continue
        code = _calls(path.read_text())
        direct = re.findall(r"(?<![\w./-])gpg\s+-\S*", code)
        assert not direct, (
            f"{path.relative_to(REPO)} runs gpg directly ({direct[0]}); use "
            f"gpg_safe from deploy/gpg_safe.sh"
        )
        if re.search(r"(?<![\w./-])gpg_safe\s+-", code):
            callers.append(path.name)
            assert "source deploy/gpg_safe.sh" in code, (
                f"{path.relative_to(REPO)} calls gpg_safe without sourcing "
                f"deploy/gpg_safe.sh"
            )
    assert {"offsite_parquet.sh", "pg_offsite.sh", "pg_restore_drill.sh",
            "ark_ship.sh"} <= set(callers), callers

    # The walk itself, on the shape a review found it missing: a call whose
    # options start on the continuation line.
    probe = "f() {\n    gpg \\\n        --symmetric --batch -o out in\n}\n"
    assert re.findall(r"(?<![\w./-])gpg\s+-\S*", _calls(probe)), "continuation not joined"

    lib = "\n".join(_code_lines(definers[0].read_text()))
    assert 'home="$(mktemp -d ' in lib, "gpg_safe's home must be made fresh per call"
    assert re.search(r'GNUPGHOME="\$home" gpg --no-keyring "\$@"', lib), (
        "gpg_safe must run gpg in its own home"
    )
    assert not re.search(r"(?m)^\s*set\s+-", lib), (
        "a sourced library sets no shell options; errexit is the caller's"
    )


def test_the_shipper_holds_a_lock_it_does_not_wait_for():
    """Two runs must not interleave — the verification downloads everything it
    uploaded, so a slow night can still be running when the next morning
    fires, and two `rename`s would race for one name. `-n` rather than a wait,
    because a cron entry that queues behind itself turns one slow run into a
    pile of them."""
    text = (REPO / "deploy" / "pg_offsite.sh").read_text()
    assert re.search(r"flock -n\b", text), text[:0] or "pg_offsite.sh takes no non-blocking flock"


def test_the_shipper_writes_its_marker_only_after_verifying():
    """The marker is what deploy/offsite_check.sh reads, so writing it before
    the uploaded bytes have been read back and hashed would make the alarm
    swear to a copy nobody has checked."""
    text = (REPO / "deploy" / "pg_offsite.sh").read_text()
    verify = text.index('step "verify the remote copy"')
    marker = text.index('step "write marker"')
    assert verify < marker, "the marker is written before the verification step"
    # And retention comes last of all: pruning ahead of the marker would delete
    # an old copy on a run that never proved the new one arrived.
    assert marker < text.index('step "prune off-site"')


def test_the_plaintext_the_shipper_makes_to_check_is_removed_where_it_is_made():
    """Verifying that the copy *opens* means decrypting it, which is a second
    plaintext of ~6 000 phone numbers on this disk. The `finish` trap would
    clear it at the end of the run in any case; it goes the moment the
    comparison is served instead, which is the same rule as every other bound
    here — it belongs to whoever created the thing."""
    text = (REPO / "deploy" / "pg_offsite.sh").read_text()
    decrypt = text.index('-o "$plain" -d "$VERIFY/$enc_dump"')
    removed = text.index('rm -f "$plain"')
    marker = text.index('step "write marker"')
    assert decrypt < removed < marker, (
        "the decrypted dump outlives the check it was made for"
    )

    # The same for the configuration that rides with every shipment since
    # OD-S5-6: its plaintext is every secret on the host. Each exit from the
    # comparison — refused, garbled, matched — removes it before going on.
    decrypt = text.index('-o "$plain_env" -d "$VERIFY/$enc_env"')
    body = text[decrypt:marker]
    exits = [m.start() for m in re.finditer(r"return 1|echo \"verified: \$enc_env", body)]
    assert len(exits) == 3, exits
    removals = [m.start() for m in re.finditer(r'rm -f "\$plain_env"', body)]
    assert len(removals) == 3 and all(r < e for r, e in zip(removals, exits)), (
        "the decrypted .env outlives the check it was made for"
    )


def test_the_pitr_drill_vouches_only_after_every_check():
    """Chain 3's flip reads `data/.pg_pitr_drill_last_ok`
    (`core/backup_evidence.py`). Written anywhere but at the end — after the
    last `fail` the drill can reach — it would vouch for a recovery that had
    not been proved. Mutation: move the marker block above the row counts."""
    text = (REPO / "deploy" / "pg_pitr_drill.sh").read_text()
    lines = [(i, line) for i, line in enumerate(text.splitlines())
             if not line.lstrip().startswith("#")]
    write = max(i for i, line in lines if "pg_pitr_drill_last_ok" in line)
    last_fail = max(i for i, line in lines if re.search(r"\bfail \"", line))
    assert last_fail < write
    assert text.index("PITR is real") < text.index("pg_pitr_drill_last_ok")
    # Through a rename, so the web container never reads half a line.
    assert 'mv -f "$MARKER.tmp" "$MARKER"' in text


def test_the_remote_drill_vouches_only_after_pass():
    """Mutation: write the marker before the row comparison's last `fail`."""
    text = (REPO / "deploy" / "pg_restore_drill.sh").read_text()
    body = text[text.index("drill_from_remote() {"):]
    body = body[:body.index("\n}\n")]
    assert body.index("PASS — the copy off this machine") < body.index(
        "pg_restore_drill_remote_last_ok")
    assert body.rindex('fail "') < body.index("pg_restore_drill_remote_last_ok")
    assert 'mv -f "$marker.tmp" "$marker"' in body


# The family's DuckDB archive tools (step 14, S14-02): every backup script
# that mounts a Python file of this checkout into a container — code from main
# running inside an image. Derived by that behaviour, not named: a fourth such
# script inherits the rule by existing. Read two ways: any `-v` of a `.py`, and
# the mount walk of tests/archive_mounts.py, which resolves `${PWD}`, `$(pwd)`,
# variables and function arguments to the checkout file they name — a mount
# spelled so the first regex misses it still makes its script an archive tool.
def _mounts_checkout_python(path: Path) -> bool:
    found = walk_mounts(path.read_text(), path.name,
                        is_checkout_file=lambda rel: (REPO / rel).is_file()).found
    return bool(found)


ARCHIVE_TOOLS = [p for p in BACKUP_SCRIPTS
                 if re.search(r'-v "[^"\s]*\.py:', _calls(p.read_text()))
                 or _mounts_checkout_python(p)]

# A container started by a command: at the start of a line, in `$( … )`, or
# as the first word of an array. Never the words inside a message.
_DOCKER_RUN = re.compile(r"(?:^\s*|\$\(|=\()docker run\b[^\n]*", re.M)


def test_the_archive_tools_run_only_a_pinned_image():
    """Step 14 takes DuckDB out of the images at S14-19, and these three read
    archives DuckDB 1.5.5 wrote. Each takes its image from a variable with no
    default, sources deploy/archive_image.sh and asks the image before it
    uses it, names no image of its own in code, and never pulls. Mutation:
    put `:-halloweex/keycrm-web:latest` back on any of them, or drop a
    `--pull never`."""
    names = {p.name for p in ARCHIVE_TOOLS}
    assert names >= {"restore-test.sh", "daily_offsite.sh", "ark_ship.sh"}, names
    for path in ARCHIVE_TOOLS:
        code = _calls(path.read_text())
        where = path.relative_to(REPO)
        assert re.search(r"^\s*source deploy/archive_image\.sh\s*$", code, re.M), where
        assert re.search(r"\barchive_image_(?:ready|probe) ", code), (
            f"{where}: the image is never asked whether it carries DuckDB 1.5.5")
        defaults = re.findall(r"\$\{BACKUP_\w*IMAGE:-([^}]*)\}", code)
        assert all(d == "" or d.startswith("${BACKUP_") for d in defaults), (
            f"{where}: an image variable defaults to {defaults}; the archive tools "
            f"run only the image backup.env names")
        assert not re.search(r"[\w.-]+/keycrm-[\w-]+", code), (
            f"{where}: names an image in code; it comes from backup.env")
        runs = _DOCKER_RUN.findall(code)
        assert runs, f"{where}: the walk found no container to judge"
        for run in runs:
            assert "--pull never" in run, f"{where}: {run.strip()!r} may pull"
