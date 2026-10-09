"""deploy/stage4_soak.sh, run against a fake `docker` (DN-03).

The SQL files are tested against a real Postgres in
`tests/integration/test_stage4_soak_sql.py`. What is left is the script's own
contract, and it is the part a daily reader acts on: the exit code, the S0 log
grep, what a check that did not run turns into, and the promises the header
makes — every session read-only, `ks_app` only where a file asks for it, and
the web container's environment asked for by name, never listed.

The fake answers the way the real commands do: `inspect` prints the running
state, `printenv NAME` exits 1 for an unset name, and `psql` reads the file on
stdin. It records every call, which is how the promises are checked. A run
costs about a second, so the healthy run is shared and flag cases are paired.
"""
from __future__ import annotations

import os
import re
import shutil
import subprocess
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[2]
SCRIPT = REPO / "deploy" / "stage4_soak.sh"
SQL_DIR = REPO / "deploy" / "stage4_soak"
FILES = sorted(SQL_DIR.glob("*.sql"))

pytestmark = pytest.mark.skipif(
    not (shutil.which("bash") and shutil.which("awk")), reason="needs bash and awk")

FAKE_DOCKER = r"""#!/usr/bin/env bash
printf '%s\n' "$*" >> "$FAKE_DOCKER_CALLS"
case "$1" in
    inspect)
        case "${FAKE_WEB:-running}" in
            missing) exit 1 ;;
            running) echo true ;;
            *) echo false ;;
        esac
        exit 0 ;;
    logs)
        printf '%s\n' "${FAKE_LOGS:-INFO all quiet}"
        exit 0 ;;
    exec)
        if [ "$2" = "keycrm-web" ]; then
            if [ "$3" = "test" ]; then
                # `test -f <marker>`: the latch, read the way the script reads it.
                case "$5" in
                    */pg_inventory_write) [ -n "${FAKE_INVENTORY_LATCHED:-}" ] && exit 0 ;;
                    */pg_buyers_write) [ -n "${FAKE_BUYERS_LATCHED:-}" ] && exit 0 ;;
                    */pg_managers_write) [ -n "${FAKE_MANAGERS_LATCHED:-}" ] && exit 0 ;;
                    */pg_orders_write) [ -n "${FAKE_ORDERS_LATCHED:-}" ] && exit 0 ;;
                    */pg_dq_journal_write) [ -n "${FAKE_DQ_JOURNAL_LATCHED:-}" ] && exit 0 ;;
                    */pg_watchdog_write) [ -n "${FAKE_WATCHDOGS_LATCHED:-}" ] && exit 0 ;;
                    */pg_weekly_ledger_write) [ -n "${FAKE_WEEKLY_LEDGER_LATCHED:-}" ] && exit 0 ;;
                    */pg_traffic_ledger_write) [ -n "${FAKE_TRAFFIC_LEDGER_LATCHED:-}" ] && exit 0 ;;
                esac
                exit 1
            fi
            case "$4" in
                KS_WRITE_INVENTORY) v="${FAKE_KS_WRITE_INVENTORY-__unset__}" ;;
                KS_DQ_PG_WAREHOUSE) v="${FAKE_KS_DQ_PG_WAREHOUSE-__unset__}" ;;
                KS_WRITE_BUYERS) v="${FAKE_KS_WRITE_BUYERS-__unset__}" ;;
                KS_SMS_STORE) v="${FAKE_KS_SMS_STORE-__unset__}" ;;
                KS_READ_SEARCH_INDEX) v="${FAKE_KS_READ_SEARCH_INDEX-__unset__}" ;;
                KS_READ_DASHBOARD) v="${FAKE_KS_READ_DASHBOARD-__unset__}" ;;
                KS_WRITE_MANAGERS) v="${FAKE_KS_WRITE_MANAGERS-__unset__}" ;;
                KS_WRITE_ORDERS) v="${FAKE_KS_WRITE_ORDERS-__unset__}" ;;
                KS_WRITE_DQ_JOURNAL) v="${FAKE_KS_WRITE_DQ_JOURNAL-__unset__}" ;;
                KS_WRITE_WATCHDOGS) v="${FAKE_KS_WRITE_WATCHDOGS-__unset__}" ;;
                KS_WRITE_WEEKLY_LEDGER) v="${FAKE_KS_WRITE_WEEKLY_LEDGER-__unset__}" ;;
                KS_WRITE_TRAFFIC_LEDGER) v="${FAKE_KS_WRITE_TRAFFIC_LEDGER-__unset__}" ;;
                KS_DUCKDB) v="${FAKE_KS_DUCKDB-__unset__}" ;;
                *) v="__unset__" ;;
            esac
            [ "$v" = "__unset__" ] && exit 1
            printf '%s\n' "$v"
            exit 0
        fi
        sql="$(cat)"
        check="$(printf '%s\n' "$sql" | sed -n "s/^SELECT '\([^']*\)'::text AS \"check\",.*/\1/p" | head -n 1)"
        if [ "$check" = "${FAKE_PSQL_ERROR:-}" ]; then
            echo 'ERROR:  permission denied for sequence' >&2
            exit 3
        fi
        verdict=PASS
        [ "$check" = "${FAKE_FAIL:-}" ] && verdict=FAIL
        [ "$check" = "${FAKE_UNKNOWN:-}" ] && verdict=UNKNOWN
        printf '%s|%s|detail for %s | with a bar\n' "$check" "$verdict" "$check"
        exit 0 ;;
esac
exit 2
"""


def _labels():
    """The check label each file returns, read the way the fake reads it."""
    out = {}
    for path in FILES:
        m = re.search(r"""^SELECT '([^']*)'::text AS "check",""",
                      path.read_text(encoding="utf-8"), flags=re.M)
        assert m, f"{path.name} has no `SELECT '<label>'::text AS \"check\",` line"
        out[path.stem] = m.group(1)
    return out


LABELS = _labels()


class Run:
    def __init__(self, done, calls):
        self.code = done.returncode
        self.out = done.stdout + done.stderr
        self.rows = {}
        for line in done.stdout.splitlines():
            m = re.match(r"^(.+?)\s{2,}(PASS|FAIL|UNKNOWN)\s{2,}(.*)$", line)
            if m:
                self.rows[m.group(1)] = (m.group(2), m.group(3))
        self.calls = calls
        self.psql = [c for c in calls if c.startswith("exec") and " psql " in c]


def _run(workdir: Path, **fake: str) -> Run:
    bin_dir = workdir / "bin"
    bin_dir.mkdir(parents=True, exist_ok=True)
    docker = bin_dir / "docker"
    docker.write_text(FAKE_DOCKER)
    docker.chmod(0o755)
    calls = workdir / "calls.log"
    calls.write_text("")
    env = {k: v for k, v in os.environ.items() if not k.startswith(("FAKE_", "SOAK_"))}
    env.update({"PATH": f"{bin_dir}{os.pathsep}{os.environ.get('PATH', '')}",
                "FAKE_DOCKER_CALLS": str(calls),
                # The file-hash record P1 and P4 read: none, unless a test
                # makes one. Never the host's /root/duckdb-silence.
                "DUCKDB_SILENCE_STATE_DIR": str(workdir / "silence"),
                # The `.env` the host-cron sidecars read (P4): none, unless a
                # test writes one. Never the checkout's.
                "SOAK_ENV_FILE": str(workdir / "dot-env"), **fake})
    done = subprocess.run(["bash", str(SCRIPT)], env=env, capture_output=True,
                          text=True, timeout=120)
    return Run(done, calls.read_text().splitlines())


@pytest.fixture(scope="module")
def healthy(tmp_path_factory):
    return _run(tmp_path_factory.mktemp("healthy"))


class TestExitCode:
    def test_all_pass_is_zero(self, healthy):
        assert healthy.code == 0, healthy.out
        assert set(healthy.rows) == set(LABELS.values()) | {"S0 order intake (log)"}
        assert {v for v, _ in healthy.rows.values()} == {"PASS"}
        assert f"{len(healthy.rows)} PASS, 0 FAIL, 0 UNKNOWN" in healthy.out

    def test_any_fail_is_one(self, tmp_path):
        run = _run(tmp_path, FAKE_FAIL="D4 unmarked writes")
        assert run.code == 1, run.out
        assert run.rows["D4 unmarked writes"][0] == "FAIL"

    def test_unknown_without_fail_is_two(self, tmp_path):
        run = _run(tmp_path, FAKE_UNKNOWN="D8 mirror_landing (derived)")
        assert run.code == 2, run.out
        assert run.rows["D8 mirror_landing (derived)"][0] == "UNKNOWN"

    def test_fail_outranks_unknown(self, tmp_path):
        run = _run(tmp_path, FAKE_FAIL="D1 owed rebuild",
                   FAKE_UNKNOWN="D8 mirror_landing (derived)")
        assert run.code == 1, run.out

    def test_a_query_that_did_not_run_is_unknown_not_pass(self, tmp_path):
        """Named by its file, since it never got as far as naming itself."""
        run = _run(tmp_path, FAKE_PSQL_ERROR="D2 derivation journal")
        assert run.code == 2, run.out
        verdict, detail = run.rows["03_d2_derivation_journal"]
        assert verdict == "UNKNOWN"
        assert "did not run" in detail and "permission denied" in detail
        assert "D2 derivation journal" not in run.rows


class TestS0LogHalf:
    @pytest.mark.parametrize("line", [
        "ERROR core.scheduler: Job incremental_sync failed: RuntimeError",
        "RuntimeError: KS_WRITE_EXPENSES='postgress' is not understood; "
        "expected 'duckdb' or 'postgres'",
    ])
    def test_a_matching_line_fails(self, tmp_path, line):
        run = _run(tmp_path, FAKE_LOGS=f"INFO fine\n{line}\nINFO fine")
        assert run.code == 1, run.out
        verdict, detail = run.rows["S0 order intake (log)"]
        assert verdict == "FAIL" and "1 line(s)" in detail

    def test_a_stopped_web_container_fails_and_its_flags_are_unknown(self, tmp_path):
        run = _run(tmp_path, FAKE_WEB="exited")
        assert run.code == 1, run.out
        assert run.rows["S0 order intake (log)"][0] == "FAIL"
        assert not [c for c in run.calls if c.startswith("logs")]
        assert all("inventory_on=unknown " in c and "dq_pg_warehouse_on=unknown" in c
                   for c in run.psql)

    def test_no_web_container_at_all_is_unknown(self, tmp_path):
        run = _run(tmp_path, FAKE_WEB="missing")
        assert run.rows["S0 order intake (log)"][0] == "UNKNOWN"
        assert run.code == 2, run.out


class TestWhatTheScriptPromises:
    def test_every_file_runs_once_in_a_read_only_session(self, healthy):
        assert len(healthy.psql) == len(FILES)
        for call in healthy.psql:
            assert "PGOPTIONS=-c default_transaction_read_only=on" in call, call
            assert "ON_ERROR_STOP=1" in call, call

    def test_the_owner_only_where_a_file_asks_and_never_the_superuser(self, healthy):
        users = [re.search(r"-U (\S+)", c).group(1) for c in healthy.psql]
        asking = sum(1 for p in FILES if p.read_text(encoding="utf-8").splitlines()[0]
                     == "-- soak:run-as ks_app")
        assert asking == 2
        assert users.count("ks_app") == asking
        assert "postgres" not in users
        assert users.count("ks_readonly") == len(FILES) - asking

    def test_the_environment_is_asked_by_name_and_never_listed(self, healthy):
        """The web container's environment holds every secret on the host."""
        web = [c for c in healthy.calls if c.startswith("exec keycrm-web")]
        assert web, healthy.calls
        assert all(re.fullmatch(
            r"exec keycrm-web (printenv KS_[A-Z_]+|test -f \S+)", c) for c in web), web
        assert not [c for c in healthy.calls if "Config.Env" in c]

    def test_unset_flags_are_zero(self, healthy):
        assert all("inventory_on=0 " in c and "dq_pg_warehouse_on=0" in c for c in healthy.psql)

    def test_set_flags_are_read_the_way_the_application_reads_them(self, tmp_path):
        run = _run(tmp_path, FAKE_KS_WRITE_INVENTORY=" Postgres ", FAKE_KS_DQ_PG_WAREHOUSE="on")
        assert all("inventory_on=1 " in c and "dq_pg_warehouse_on=1" in c for c in run.psql)

    def test_values_nobody_understands_are_invalid(self, tmp_path):
        run = _run(tmp_path, FAKE_KS_WRITE_INVENTORY="postgress", FAKE_KS_DQ_PG_WAREHOUSE="yes")
        assert all("inventory_on=invalid " in c and "dq_pg_warehouse_on=invalid" in c
                   for c in run.psql)

    def test_the_latch_outranks_the_flag(self, tmp_path):
        """The state DN-06 exists for: chain 1 has written Postgres, so it goes
        on writing Postgres, and somebody has put `KS_WRITE_INVENTORY` back
        expecting a rollback. Reading the flag alone made `inventory_on` 0, and
        0 is what turns all five I-checks — I1 included, the soak half of the
        E1 detector that watches for the hourly copy overwriting the six
        inventory tables — into "not applicable" PASSes. The chain would then
        be unwatched in the one state nobody has practised."""
        run = _run(tmp_path, FAKE_KS_WRITE_INVENTORY="duckdb", FAKE_INVENTORY_LATCHED="1")
        assert all("inventory_on=1 " in c for c in run.psql), run.psql
        assert "latched" in run.out, "the report does not say the two disagree"

    def test_a_latched_chain_whose_flag_agrees_reads_as_one_state(self, tmp_path):
        run = _run(tmp_path, FAKE_KS_WRITE_INVENTORY="postgres", FAKE_INVENTORY_LATCHED="1")
        assert all("inventory_on=1 " in c for c in run.psql)
        assert "latched" not in run.out, "nothing disagrees — do not say it does"

    def test_a_typo_on_a_latched_chain_still_routes_to_postgres(self, tmp_path):
        """`writes_postgres()` answers True over an unreadable value too, so
        the stand-down must still be checked. The typo itself stays visible:
        S0 greps the log for the message the shipper writes every hour."""
        run = _run(tmp_path, FAKE_KS_WRITE_INVENTORY="postgress",
                   FAKE_INVENTORY_LATCHED="1")
        assert all("inventory_on=1 " in c for c in run.psql)

    def test_the_marker_path_is_the_one_the_application_writes(self):
        """A path spelled twice drifts once. `/app` is the image's WORKDIR
        (Dockerfile.web) and `./data:/app/data` is the mount both services
        carry, so the container path is the repository path with that prefix.

        Read out of the source rather than from `core.chain_latch.MARKER_DIR`,
        because the suite's own fixture redirects that attribute into a
        tmp_path so no test can latch a chain in the real data directory."""
        latch_src = (REPO / "core" / "chain_latch.py").read_text(encoding="utf-8")
        consts_src = (REPO / "core" / "duckdb_constants.py").read_text(encoding="utf-8")
        marker = re.search(r'MARKER_DIR[^=]*= DB_DIR / "([^"]+)"', latch_src)
        db_dir = re.search(r'DB_DIR = Path\(__file__\)\.parent\.parent / "([^"]+)"',
                           consts_src)
        assert marker and db_dir, "the marker directory is no longer spelled this way"
        expected = f"/app/{db_dir.group(1)}/{marker.group(1)}"
        assert f'MARKER_DIR_IN_CONTAINER="{expected}"' in SCRIPT.read_text()
        assert "./data:/app/data" in (REPO / "docker-compose.yml").read_text()

    def test_a_bar_inside_a_detail_survives(self, healthy):
        assert healthy.rows["D1 owed rebuild"][1] == "detail for D1 owed rebuild | with a bar"

    def test_it_lives_in_no_image(self):
        for dockerfile in REPO.glob("Dockerfile*"):
            copies = re.findall(r"^COPY\s+(?:--\S+\s+)?(\S+)", dockerfile.read_text(), flags=re.M)
            assert not [c for c in copies if c.rstrip("/") in {"deploy", "."}], (
                dockerfile.name, copies)


class TestChain4:
    """`pg_buyers_write` has a state chain 1 has not: its flag moves the writes
    only once every buyer reader reads Postgres. "postgres, unlatched, a reader
    on duckdb" is a chain HELD on DuckDB, and the report must say so rather
    than judge a chain that never moved."""

    READERS = {"FAKE_KS_SMS_STORE": "postgres", "FAKE_KS_READ_SEARCH_INDEX": "postgres",
               "FAKE_KS_READ_DASHBOARD": "postgres"}

    def test_unset_is_zero(self, healthy):
        assert all("buyers_on=0 " in c for c in healthy.psql), healthy.psql

    def test_the_flag_with_every_reader_is_one(self, tmp_path):
        run = _run(tmp_path, FAKE_KS_WRITE_BUYERS=" Postgres ", **self.READERS)
        assert all("buyers_on=1 " in c and "buyers_held_by= " in c for c in run.psql)

    @pytest.mark.parametrize("reader", ["FAKE_KS_SMS_STORE", "FAKE_KS_READ_SEARCH_INDEX",
                                        "FAKE_KS_READ_DASHBOARD"])
    def test_a_reader_on_duckdb_holds_it(self, tmp_path, reader):
        readers = {**self.READERS, reader: "duckdb"}
        run = _run(tmp_path, FAKE_KS_WRITE_BUYERS="postgres", **readers)
        name = reader[len("FAKE_"):]
        assert all("buyers_on=held " in c and f"buyers_held_by={name} " in c
                   for c in run.psql), run.psql
        assert f"held by {name}" in run.out

    def test_an_unset_reader_holds_it_too(self, tmp_path):
        readers = {k: v for k, v in self.READERS.items() if k != "FAKE_KS_READ_DASHBOARD"}
        run = _run(tmp_path, FAKE_KS_WRITE_BUYERS="postgres", **readers)
        assert all("buyers_held_by=KS_READ_DASHBOARD " in c for c in run.psql)

    def test_the_latch_outranks_the_flag_and_the_readers(self, tmp_path):
        """Latched, the chain writes Postgres whatever the flag or the readers
        say (OD-19 (a)), so every B-check must judge it."""
        run = _run(tmp_path, FAKE_KS_WRITE_BUYERS="duckdb", FAKE_BUYERS_LATCHED="1",
                   FAKE_KS_SMS_STORE="duckdb")
        assert all("buyers_on=1 " in c for c in run.psql), run.psql
        assert "latched: chain 4" in run.out

    def test_a_value_nobody_understands_is_invalid(self, tmp_path):
        run = _run(tmp_path, FAKE_KS_WRITE_BUYERS="postgress", **self.READERS)
        assert all("buyers_on=invalid " in c for c in run.psql)

    def test_the_flip_time_and_the_floor_are_passed(self, tmp_path):
        run = _run(tmp_path, SOAK_BUYERS_FLIP_AT="2026-10-01 10:30+03",
                   SOAK_BUYERS_OVERRIDE_FLOOR="3")
        assert all("buyers_flip_at=2026-10-01 10:30+03 " in c
                   and "buyers_override_floor=3" in c for c in run.psql), run.psql


class TestChain5:
    """`managers_on`: 0, 1, pending, invalid, unknown — the latch outranks the
    flag, and a flag the chain has not latched under is pending, because its
    preconditions are only readable in web."""

    def test_unset_is_zero(self, healthy):
        assert all("managers_on=0 " in c and re.search(r"managers_flip_at=(\s|$)", c)
                   for c in healthy.psql), healthy.psql

    def test_the_flag_without_the_latch_is_pending(self, tmp_path):
        run = _run(tmp_path, FAKE_KS_WRITE_MANAGERS=" Postgres ")
        assert all("managers_on=pending " in c for c in run.psql), run.psql

    def test_the_latch_is_one_whatever_the_flag_says(self, tmp_path):
        run = _run(tmp_path, FAKE_KS_WRITE_MANAGERS="duckdb", FAKE_MANAGERS_LATCHED="1")
        assert all("managers_on=1 " in c for c in run.psql), run.psql
        assert "latched: chain 5 owns its tables" in run.out

    def test_the_flag_and_the_latch_agree_on_one(self, tmp_path):
        run = _run(tmp_path, FAKE_KS_WRITE_MANAGERS="postgres", FAKE_MANAGERS_LATCHED="1")
        assert all("managers_on=1 " in c for c in run.psql), run.psql

    def test_a_value_nobody_understands_is_invalid(self, tmp_path):
        run = _run(tmp_path, FAKE_KS_WRITE_MANAGERS="postgress")
        assert all("managers_on=invalid " in c for c in run.psql)

    def test_the_flip_time_is_passed(self, tmp_path):
        run = _run(tmp_path, SOAK_MANAGERS_FLIP_AT="2026-10-08 10:30+03")
        assert all(re.search(r"managers_flip_at=2026-10-08 10:30\+03(\s|$)", c)
                   for c in run.psql), run.psql


class TestChain3:
    """`orders_on`: chain 5's five states, for `pg_orders_write`. Its
    preconditions — the lockout, step 13, chain 1, the backup evidence — are
    web's to judge, so a flag without the latch is pending. Mutation: drop the
    latch from ORDERS_ON, and a chain latched with its flag put back reads
    "not applicable" on all five O-checks."""

    def test_unset_is_zero(self, healthy):
        assert all("orders_on=0 " in c and re.search(r"orders_flip_at=(\s|$)", c)
                   for c in healthy.psql), healthy.psql

    def test_the_flag_without_the_latch_is_pending(self, tmp_path):
        run = _run(tmp_path, FAKE_KS_WRITE_ORDERS=" Postgres ")
        assert all("orders_on=pending " in c for c in run.psql), run.psql

    def test_the_latch_is_one_whatever_the_flag_says(self, tmp_path):
        run = _run(tmp_path, FAKE_KS_WRITE_ORDERS="duckdb", FAKE_ORDERS_LATCHED="1")
        assert all("orders_on=1 " in c for c in run.psql), run.psql
        assert "latched: chain 3 owns its tables" in run.out

    def test_a_typo_on_a_latched_chain_is_still_one(self, tmp_path):
        """`writes_postgres()` answers True over a value nobody can read when
        latched, so the stand-down must still be judged."""
        run = _run(tmp_path, FAKE_KS_WRITE_ORDERS="postgress", FAKE_ORDERS_LATCHED="1")
        assert all("orders_on=1 " in c for c in run.psql), run.psql

    def test_the_flag_and_the_latch_agree_on_one(self, tmp_path):
        run = _run(tmp_path, FAKE_KS_WRITE_ORDERS="postgres", FAKE_ORDERS_LATCHED="1")
        assert all("orders_on=1 " in c for c in run.psql), run.psql
        assert "latched: chain 3" not in run.out, "nothing disagrees — do not say it does"

    def test_a_value_nobody_understands_is_invalid(self, tmp_path):
        run = _run(tmp_path, FAKE_KS_WRITE_ORDERS="postgress")
        assert all("orders_on=invalid " in c for c in run.psql), run.psql

    def test_a_stopped_web_is_unknown(self, tmp_path):
        run = _run(tmp_path, FAKE_WEB="exited")
        assert all("orders_on=unknown " in c for c in run.psql), run.psql

    def test_the_flip_time_is_passed_and_reported(self, tmp_path):
        run = _run(tmp_path, SOAK_ORDERS_FLIP_AT="2026-10-12 10:30+03")
        assert all(re.search(r"orders_flip_at=2026-10-12 10:30\+03(\s|$)", c)
                   for c in run.psql), run.psql
        assert "orders flip at 2026-10-12 10:30+03" in run.out

    def test_the_five_checks_are_in_the_report(self, healthy):
        """O1–O5, each once, in the order a reader works down them."""
        labels = [label for label in healthy.rows if label.startswith("O")]
        assert labels == ["O1 orders copies stood down", "O2 orders watermark",
                          "O3 order versions", "O4 half-written orders",
                          "O5 orders integrity"], labels


class TestChain9:
    """Where the quality journal is written decides whether D8, 20, 21 and 22
    gate on the hourly copy (OD-02 (c)). Mutation: drop the latch from
    `DQ_JOURNAL_DIRECT`, or stop passing it — a latched chain with its flag
    put back would then read UNKNOWN on a copy that stood down."""

    def test_unset_is_zero(self, healthy):
        assert all("dq_journal_direct=0" in c for c in healthy.psql), healthy.psql

    def test_the_flag_is_one(self, tmp_path):
        run = _run(tmp_path, FAKE_KS_WRITE_DQ_JOURNAL=" Postgres ")
        assert all("dq_journal_direct=1" in c for c in run.psql), run.psql

    def test_the_latch_outranks_the_flag(self, tmp_path):
        run = _run(tmp_path, FAKE_KS_WRITE_DQ_JOURNAL="duckdb",
                   FAKE_DQ_JOURNAL_LATCHED="1")
        assert all("dq_journal_direct=1" in c for c in run.psql), run.psql

    def test_a_value_nobody_understands_is_not_direct(self, tmp_path):
        """Unlatched, a typo writes the journal nowhere (OD-18 (a)); the copy
        is what stands, and the checks keep gating on it."""
        run = _run(tmp_path, FAKE_KS_WRITE_DQ_JOURNAL="postgress")
        assert all("dq_journal_direct=invalid" in c for c in run.psql), run.psql


class TestTheOtherShadowChains:
    """Chains 10, 11a and 11b (OD-02 (c)) reach H1 and H2 the way chain 9
    reaches D8: the flag, outranked by the latch. Mutation: stop passing one
    of the three, or drop the latch from `shadow_state` — a latched chain with
    its flag put back would read "not applicable" in the one state the stand-
    down exists for."""

    NAMES = (("watchdogs_on", "KS_WRITE_WATCHDOGS", "WATCHDOGS"),
             ("weekly_ledger_on", "KS_WRITE_WEEKLY_LEDGER", "WEEKLY_LEDGER"),
             ("traffic_ledger_on", "KS_WRITE_TRAFFIC_LEDGER", "TRAFFIC_LEDGER"))

    def test_unset_is_zero(self, healthy):
        for var, _, _ in self.NAMES:
            assert all(f"{var}=0" in c for c in healthy.psql), (var, healthy.psql)

    @pytest.mark.parametrize("var,env,latch", NAMES)
    def test_the_flag_is_one_and_a_typo_invalid(self, tmp_path, var, env, latch):
        run = _run(tmp_path / "on", **{f"FAKE_{env}": " Postgres "})
        assert all(f"{var}=1" in c for c in run.psql), run.psql
        run = _run(tmp_path / "typo", **{f"FAKE_{env}": "postgress"})
        assert all(f"{var}=invalid" in c for c in run.psql), run.psql

    @pytest.mark.parametrize("var,env,latch", NAMES)
    def test_the_latch_outranks_the_flag(self, tmp_path, var, env, latch):
        run = _run(tmp_path, **{f"FAKE_{env}": "duckdb", f"FAKE_{latch}_LATCHED": "1"})
        assert all(f"{var}=1" in c for c in run.psql), run.psql
        assert f"{var}=1 ({env})" in run.out


class TestStage5:
    """The parallel period and the week of silence (OD-17 (a)): KS_DUCKDB read
    by name, the declared start passed through, and the file's hash record
    read with `--status` — which hashes nothing and writes nothing."""

    def test_nothing_set_is_on_undeclared_and_no_record(self, healthy):
        assert all("duckdb_off=0 " in c and "parallel_from= " in c
                   and "duckdb_file_last=none " in c and "duckdb_file_since= " in c
                   for c in healthy.psql), healthy.psql
        assert "duckdb_off=0 (KS_DUCKDB), in .env: unknown, file record: none" in healthy.out

    @pytest.mark.parametrize("value, expected", [
        ("off", "1"), (" OFF ", "1"), ("on", "0"), ("", "0"), ("of", "invalid")])
    def test_ks_duckdb_reads_the_way_web_reads_it(self, tmp_path, value, expected):
        run = _run(tmp_path, FAKE_KS_DUCKDB=value)
        assert all(f"duckdb_off={expected} " in c for c in run.psql), run.psql

    def test_a_stopped_web_is_unknown(self, tmp_path):
        run = _run(tmp_path, FAKE_WEB="exited")
        assert all("duckdb_off=unknown " in c for c in run.psql)

    def test_the_declared_start_is_passed(self, tmp_path):
        run = _run(tmp_path, SOAK_PARALLEL_FROM="2026-11-02 10:00+02")
        assert all("parallel_from=2026-11-02 10:00+02 " in c for c in run.psql)
        assert "parallel period from 2026-11-02 10:00+02" in run.out

    def test_the_record_is_read_and_left_as_it_was(self, tmp_path):
        """The hourly cron's record, made by the real script, then read by the
        report: every field passed through, and the record byte for byte as
        it was. Mutation: call the check without `--status` in the report —
        it would hash the file and write the record."""
        db = tmp_path / "analytics.duckdb"
        db.write_bytes(b"duck")
        state = tmp_path / "silence"
        base = {k: v for k, v in os.environ.items() if not k.startswith("DUCKDB_")}
        made = subprocess.run(
            ["bash", str(REPO / "deploy" / "duckdb_silence_check.sh")],
            env={**base, "DUCKDB_FILE": str(db), "DUCKDB_SILENCE_STATE_DIR": str(state),
                 "DUCKDB_SILENCE_NOW": "1900000000"},
            capture_output=True, text=True, timeout=60)
        assert made.returncode == 2, made.stdout + made.stderr
        before = {p.name: p.read_bytes() for p in state.iterdir()}
        db.write_bytes(b"changed")  # a record-mode run would now say CHANGED

        run = _run(tmp_path, DUCKDB_SILENCE_STATE_DIR=str(state), DUCKDB_FILE=str(db))
        assert all("duckdb_file_last=BASELINE " in c
                   and "duckdb_file_since=2030-03-17T17:46:40Z " in c
                   and "duckdb_file_since_reason=baseline " in c
                   and "duckdb_file_checked_at=2030-03-17T17:46:40Z" in c
                   for c in run.psql), run.psql
        assert {p.name: p.read_bytes() for p in state.iterdir()} == before

    def test_a_missing_episode_reaches_the_checks(self, tmp_path):
        """The review of 02.10's sequence, by the real script: baseline, the
        file moved away for a check, moved back. The record reads UNCHANGED
        since the baseline; `missing_at` is what tells P1 and P4. Mutation:
        drop the `missing_at=*` case from `silence_record`."""
        db = tmp_path / "analytics.duckdb"
        db.write_bytes(b"duck")
        state = tmp_path / "silence"
        base = {k: v for k, v in os.environ.items() if not k.startswith("DUCKDB_")}
        env = {**base, "DUCKDB_FILE": str(db), "DUCKDB_SILENCE_STATE_DIR": str(state)}
        check = ["bash", str(REPO / "deploy" / "duckdb_silence_check.sh")]
        for now, move in ((1900000000, None), (1900003600, "away"), (1900007200, "back")):
            if move == "away":
                db.rename(tmp_path / "away")
            elif move == "back":
                (tmp_path / "away").rename(db)
            subprocess.run(check, env={**env, "DUCKDB_SILENCE_NOW": str(now)},
                           capture_output=True, text=True, timeout=60)
        run = _run(tmp_path, DUCKDB_SILENCE_STATE_DIR=str(state), DUCKDB_FILE=str(db))
        assert all("duckdb_file_last=UNCHANGED " in c
                   and "duckdb_file_since_reason=baseline " in c
                   and "duckdb_file_missing_at=2030-03-17T18:46:40Z" in c
                   for c in run.psql), run.psql
        assert "last missing 2030-03-17T18:46:40Z" in run.out

    def test_an_unreadable_record_is_an_error_not_a_pass(self, tmp_path):
        state = tmp_path / "silence"
        state.mkdir()
        (state / "state").write_text("nonsense=1\n")
        run = _run(tmp_path, DUCKDB_SILENCE_STATE_DIR=str(state))
        assert all("duckdb_file_last=error " in c for c in run.psql)

    @pytest.mark.parametrize("lines, expected", [
        ("KS_DUCKDB=off\n", "1"),
        ("KS_DUCKDB= OFF \r\n", "1"),
        ("KS_DUCKDB=on\nKS_DUCKDB=off\n", "1"),
        ("KS_DUCKDB=off\nKS_DUCKDB=on\n", "0"),
        ("KS_DUCKDB=on\n", "0"),
        ("BOT_TOKEN=x\n", "0"),
        ("# KS_DUCKDB=off\n", "0"),
        ('KS_DUCKDB="off"\n', "invalid"),
        ("KS_DUCKDB=of\n", "invalid"),
    ])
    def test_env_is_read_the_way_the_sidecars_read_it(self, tmp_path, lines, expected):
        """The weekly compaction and the nightly off-site start their sidecars
        with `docker run --env-file .env`, so what refuses them is `.env`, not
        web's environment: the last line naming the key, quotes kept.
        Mutation: strip the quotes, or take the first line."""
        env_file = tmp_path / "dot-env"
        env_file.write_text(lines)
        os.utime(env_file, (1900000000, 1900000000))
        run = _run(tmp_path, SOAK_ENV_FILE=str(env_file))
        assert all(f"duckdb_off_env={expected} " in c
                   and "duckdb_env_changed_at=2030-03-17T17:46:40Z" in c
                   for c in run.psql), run.psql

    def test_no_env_is_unknown_and_nothing_else_is_read_from_it(self, tmp_path):
        run = _run(tmp_path)
        assert all("duckdb_off_env=unknown " in c and "duckdb_env_changed_at= " in c
                   for c in run.psql), run.psql
        env_file = tmp_path / "dot-env"
        env_file.write_text("BOT_TOKEN=123456:never-printed\nKS_DUCKDB=off\n")
        run = _run(tmp_path, SOAK_ENV_FILE=str(env_file))
        assert "never-printed" not in run.out and not any("never-printed" in c
                                                           for c in run.calls)
        assert "in .env: 1 (edited 20" in run.out, run.out

    def test_the_check_is_only_ever_asked_for_its_status(self):
        calls = re.findall(r'"\$SILENCE_CHECK"[^\n]*', SCRIPT.read_text())
        assert calls == ['"$SILENCE_CHECK" --status 2>&1 || true)"'], calls


def test_every_variable_a_check_reads_is_passed():
    """Walked over the checks, not listed: a `:'name'` a file reads that the
    script does not pass reaches the server as psql's literal text, and the
    check judges that. Mutation: drop `-v duckdb_off_env=...` from the
    script."""
    passed = set(re.findall(r"-v (\w+)=", SCRIPT.read_text())) - {"ON_ERROR_STOP"}
    read = set()
    for path in FILES:
        read |= set(re.findall(r":'(\w+)'",
                               re.sub(r"--[^\n]*", "", path.read_text(encoding="utf-8"))))
    assert read <= passed, sorted(read - passed)


# A soak file is named in prose far from where it lives — a module docstring
# telling the reader which check reads its rows, a canary comment naming the
# check that judges its page — and a renumbering leaves those pointing at
# nothing. Found once: three such names survived a renumbering in the change
# that added P1–P4. Walked, not listed: every tree a reader would look in.
_SOAK_NAME = re.compile(r"\b\d{2}_[a-z0-9]+_[a-z0-9_]+\.sql\b")
_PROSE_TREES = ("core", "bot", "web", "scripts", "deploy", "tests")


def test_every_soak_file_named_anywhere_exists():
    """Mutation: put P4's old number back in the canary's comment (52 for 53,
    `p3` for `p4`), a file that does not exist, and this fails naming the
    file and the name."""
    sources = [REPO / ".claude" / "CLAUDE.md"]
    for tree in _PROSE_TREES:
        sources += [p for p in (REPO / tree).rglob("*")
                    if p.is_file() and p.suffix in {".py", ".sh", ".sql", ".md", ".yml"}
                    and "__pycache__" not in p.parts]
    existing = {p.name for p in FILES}
    dangling = sorted(
        f"{path.relative_to(REPO)}: {name}"
        for path in sources if path.exists()
        for name in set(_SOAK_NAME.findall(path.read_text(errors="replace")))
        if name not in existing)
    assert not dangling, dangling
