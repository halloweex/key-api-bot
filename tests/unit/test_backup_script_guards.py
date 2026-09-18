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
        # env.gpg and invokes nothing.
        if not re.search(r"(?<![\w./-])gpg\s+-", "\n".join(code)):
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
