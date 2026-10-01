"""deploy/duckdb_silence_check.sh — the week of silence's file detector, run.

The script is the evidence that nothing outside web touched the DuckDB file
during the week (OD-17 (a)), so what is held here is what that evidence rests
on: it never writes the file it judges; `--peek` and `--status` write nothing
anywhere and `--status` hashes nothing; a change moves `since` and the count; a
vanished file is MISSING and keeps the reference; the `.wal` counts; the record
is parsed, never sourced; and the history it grows is bounded where it grows.
Each guard names the mutation that breaks it.
"""
from __future__ import annotations

import os
import shutil
import stat
import subprocess
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[2]
SCRIPT = REPO / "deploy" / "duckdb_silence_check.sh"

pytestmark = pytest.mark.skipif(not shutil.which("bash"), reason="needs bash")

T0 = 1_900_000_000  # 2030-03-17T17:46:40Z


class Box:
    """A DuckDB stand-in, a state directory and a clock."""

    def __init__(self, root: Path):
        self.root = root
        self.db = root / "data" / "analytics.duckdb"
        self.db.parent.mkdir()
        self.db.write_bytes(b"DUCK" * 1000)
        self.state = root / "silence"
        self.now = T0
        self.env_extra: dict = {}

    def run(self, *args: str, path: str | None = None):
        env = {k: v for k, v in os.environ.items()
               if not k.startswith(("DUCKDB_SILENCE_", "DUCKDB_FILE"))}
        env.update({"DUCKDB_FILE": str(self.db),
                    "DUCKDB_SILENCE_STATE_DIR": str(self.state),
                    "DUCKDB_SILENCE_NOW": str(self.now), **self.env_extra})
        if path is not None:
            env["PATH"] = path
        done = subprocess.run(["bash", str(SCRIPT), *args], env=env,
                              capture_output=True, text=True, timeout=60)
        line = done.stdout.strip()
        verdict, _, rest = line.partition(" ")
        fields = dict(kv.split("=", 1) for kv in rest.split() if "=" in kv)
        return done.returncode, verdict, fields, done

    def record(self) -> dict:
        lines = (self.state / "state").read_text().splitlines()
        return dict(line.split("=", 1) for line in lines)

    def tick(self, hours: float = 1) -> None:
        self.now += int(hours * 3600)


@pytest.fixture
def box(tmp_path):
    return Box(tmp_path)


def _snapshot(path: Path):
    st = path.stat()
    return path.read_bytes(), st.st_mtime_ns, st.st_mode


class TestTheRecord:
    def test_baseline_unchanged_changed_unchanged(self, box):
        code, verdict, f, _ = box.run()
        assert (code, verdict) == (2, "BASELINE")
        assert f["since"] == "2030-03-17T17:46:40Z"
        rec = box.record()
        assert rec["last"] == "BASELINE" and rec["since_reason"] == "baseline"
        assert rec["changes"] == "0" and rec["wal"] == "none"

        box.tick()
        code, verdict, f, _ = box.run()
        assert (code, verdict) == (0, "UNCHANGED")
        assert f["since"] == "2030-03-17T17:46:40Z" and f["age_s"] == "3600"
        assert f["since_reason"] == "baseline"

        box.tick()
        box.db.write_bytes(b"DUCK" * 1001)
        code, verdict, f, _ = box.run()
        assert (code, verdict) == (1, "CHANGED")
        assert f["since"] == "2030-03-17T19:46:40Z"
        assert f["changed_after"] == "2030-03-17T18:46:40Z"
        assert f["was"] != f["sha256"] and f["changes"] == "1"

        box.tick()
        code, verdict, f, _ = box.run()
        assert (code, verdict) == (0, "UNCHANGED")
        assert f["since"] == "2030-03-17T19:46:40Z" and f["since_reason"] == "change"
        assert box.record()["previous_checked_at"] == "2030-03-17T19:46:40Z"

    def test_a_wal_beside_the_file_is_a_change(self, box):
        """An un-checkpointed write lives in the WAL, not in the file.
        Mutation: compare `n_hash` alone."""
        box.run()
        box.tick()
        Path(f"{box.db}.wal").write_bytes(b"a write")
        code, verdict, f, _ = box.run()
        assert (code, verdict) == (1, "CHANGED") and f["wal"] != "none"

    def test_a_missing_file_keeps_the_reference(self, box):
        """Deleting the file is a DROP (OD-11 (a)), and a file that comes back
        is judged against what it was. Mutation: write the record's hash from
        an empty `n_hash` on MISSING."""
        box.run()
        reference = box.record()["hash"]
        saved = box.db.read_bytes()
        box.db.unlink()
        box.tick()
        code, verdict, f, _ = box.run()
        assert (code, verdict) == (3, "MISSING")
        assert f["last_hash"] == reference
        assert box.record()["hash"] == reference and box.record()["last"] == "MISSING"

        box.tick()
        box.db.write_bytes(saved)
        assert box.run()[:2] == (0, "UNCHANGED")
        box.db.write_bytes(saved + b"x")
        assert box.run()[:2] == (1, "CHANGED")

    def test_the_record_is_private(self, box):
        box.run()
        assert stat.S_IMODE((box.state / "state").stat().st_mode) == 0o600
        assert stat.S_IMODE(box.state.stat().st_mode) == 0o700
        assert stat.S_IMODE((box.state / "history").stat().st_mode) == 0o600

    def test_history_is_bounded_where_it_grows(self, box):
        """Mutation: delete the `tail -n "$HISTORY_MAX"` trim."""
        box.env_extra = {"DUCKDB_SILENCE_HISTORY_MAX": "3"}
        for _ in range(6):
            box.run()
            box.tick()
        lines = (box.state / "history").read_text().splitlines()
        assert len(lines) == 3
        assert lines[0].startswith("2030-03-17T20:46:40Z UNCHANGED ")
        assert lines[-1].startswith("2030-03-17T22:46:40Z UNCHANGED ")

    def test_a_record_is_parsed_never_sourced(self, box, tmp_path):
        """A state file edited into a command is an error, not an instruction."""
        box.run()
        canary = tmp_path / "ran"
        (box.state / "state").write_text(
            f"hash=$(touch {canary})\nsince_epoch=1\n")
        code, verdict, _, _ = box.run("--status")
        assert (code, verdict) == (4, "ERROR") and not canary.exists()
        (box.state / "state").write_text("hash=a\nsince_epoch=1\nPATH=/nowhere\n")
        assert box.run()[:2] == (4, "ERROR")

    def test_an_unknown_argument_is_an_error(self, box):
        assert box.run("--force")[:2] == (4, "ERROR")


class TestItWritesNothingItShouldNot:
    @pytest.mark.parametrize("mode", [(), ("--peek",), ("--status",)])
    def test_the_duckdb_file_is_never_written(self, box, mode):
        """Bytes, mtime and mode of the file, and nothing new beside it — in
        every mode, twice, around a change."""
        box.run()
        before = _snapshot(box.db)
        for _ in range(2):
            box.tick()
            box.run(*mode)
        assert _snapshot(box.db) == before
        assert sorted(p.name for p in box.db.parent.iterdir()) == ["analytics.duckdb"]

    def test_peek_writes_nothing_anywhere(self, box):
        """Mutation: make `record` write when MODE=peek."""
        code, verdict, _, _ = box.run("--peek")
        assert (code, verdict) == (2, "BASELINE") and not box.state.exists()
        box.run()
        before = {p.name: p.read_bytes() for p in box.state.iterdir()}
        box.tick()
        box.db.write_bytes(b"other")
        assert box.run("--peek")[:2] == (1, "CHANGED")
        assert {p.name: p.read_bytes() for p in box.state.iterdir()} == before
        assert box.run("--peek")[:2] == (1, "CHANGED"), "still a change: nothing recorded"

    def test_status_hashes_nothing_and_writes_nothing(self, box, tmp_path):
        """What the daily soak calls. With every hashing tool replaced by one
        that leaves a mark, a status read leaves none. Mutation: hash in
        `--status`."""
        box.run()
        box.tick(2)
        before = {p.name: p.read_bytes() for p in box.state.iterdir()}
        bin_dir = tmp_path / "bin"
        bin_dir.mkdir()
        mark = tmp_path / "hashed"
        for tool in ("sha256sum", "shasum"):
            fake = bin_dir / tool
            fake.write_text(f"#!/bin/sh\ntouch {mark}\necho x\n")
            fake.chmod(0o755)
        code, verdict, f, _ = box.run("--status", path=f"{bin_dir}{os.pathsep}/usr/bin:/bin")
        assert (code, verdict) == (0, "STATUS") and not mark.exists()
        assert f == {"last": "BASELINE", "since": "2030-03-17T17:46:40Z",
                     "since_reason": "baseline", "checked_at": "2030-03-17T17:46:40Z",
                     "age_s": "7200", "checked_age_s": "7200", "changes": "0"}
        assert {p.name: p.read_bytes() for p in box.state.iterdir()} == before

    def test_status_without_a_record_says_so(self, box):
        code, verdict, f, _ = box.run("--status")
        assert (code, verdict) == (2, "NORECORD") and not box.state.exists()


class TestItIsAHostTool:
    def test_it_lives_in_no_image(self):
        """Host cron reads the host's file; no image COPYs deploy/."""
        for dockerfile in REPO.glob("Dockerfile*"):
            text = dockerfile.read_text(encoding="utf-8")
            assert "duckdb_silence_check" not in text, dockerfile.name
            assert not any(line.split()[1:2] == ["deploy/"] for line in
                           text.splitlines() if line.startswith("COPY")), dockerfile.name

    def test_its_default_paths_are_the_hosts(self):
        text = SCRIPT.read_text(encoding="utf-8")
        assert 'DUCKDB_FILE="${DUCKDB_FILE:-$HERE/../data/analytics.duckdb}"' in text
        assert 'STATE_DIR="${DUCKDB_SILENCE_STATE_DIR:-/root/duckdb-silence}"' in text
        assert os.access(SCRIPT, os.X_OK)
