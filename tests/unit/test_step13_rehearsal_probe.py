"""The step-13 rehearsal's judges (`deploy/step13_rehearsal/probe.py`).

The rehearsal runs once, on the host, for over an hour, and its table is what
somebody reads before deciding a flip. So each judge is held here to its three
outcomes on hand-built evidence — and above all to the one that matters most:
evidence that is missing, or that could not have shown a failure, is UNKNOWN
and never PASS. P1's single resolve proved by a gate with nothing delivered,
P4a's deletion healed by a derivation before the integrity run looked, P8
killed by the memory cap: each would read as a green row if the judge only
counted.

The probe's constants and the log lines the script greps are pinned against
the code they describe, so a rename there fails here instead of turning a
rehearsal row UNKNOWN on the host.
"""
from __future__ import annotations

import importlib.util
import json
import re
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[2]
PROBE_PATH = REPO / "deploy" / "step13_rehearsal" / "probe.py"
SCRIPT = REPO / "deploy" / "step13_rehearsal.sh"


def _load():
    spec = importlib.util.spec_from_file_location("reh_probe", PROBE_PATH)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


probe = _load()
PASS, FAIL, UNKNOWN = probe.PASS, probe.FAIL, probe.UNKNOWN
STOOD = list(probe.STOOD_DOWN)
T1 = "2026-10-01T10:41:07+00:00"


def snap(mode="postgres", *, value="postgres", unmet=(), held=False, reclassify=False,
         jobs=("pg_warehouse_derive", "incremental_sync"), writer=None, cut_unmet=(),
         stood=None, frozen="2026-10-01 07:58:00.123456", last_trigger=None,
         validation_passed=None, code=200):
    return {
        "health_code": code,
        "health": {"warehouse_writer_mode": {
            "mode": mode, "value": value, "error": None,
            "preconditions_unmet": list(unmet), "held": held, "reclassify_needed": reclassify}},
        "status": {
            "writer": "postgres" if mode == "postgres" else None,
            "last_trigger": last_trigger, "validation_passed": validation_passed,
            "frozen_last_refresh": frozen if mode == "postgres" else None,
            "cutover": {
                "mode": mode, "value": value, "preconditions_met": not cut_unmet,
                "unmet": list(cut_unmet),
                "writer": writer if writer is not None else (
                    {"writer": "postgres", "resolved": True, "resolved_at": T1}
                    if mode == "postgres" else None),
                "held": held, "reclassify_needed": reclassify,
                "stood_down_duckdb_checks": STOOD if stood is None else stood},
        },
        "jobs": sorted(jobs),
    }


DUCK0 = {"warehouse_dirty": ["full", "2026-10-01 07:57:00"], "writer": None,
         "refreshes": {"max_refreshed_at": "2026-10-01 07:58:00.123456", "count": 812},
         "orders": 400, "silver_orders": 400, "silver_order_utm": 210}
DUCK1 = {**DUCK0, "writer": {"writer": "postgres", "resolved": True, "resolved_at": T1,
                             "utm_reparsed_at": "2026-10-01T10:50:00+00:00"},
         "gate_delivered": []}


# ─── P7 ──────────────────────────────────────────────────────────────────────

def p7_ev(**over):
    ev = {"snapshot": snap("duckdb", unmet=["read_fallback_off"],
                           jobs=("warehouse_refresh", "pg_warehouse_derive")),
          "canary": {"severity": "critical", "failure_keys": ["warehouse_preconditions_unmet"],
                     "preconditions_keys": ["warehouse_preconditions_unmet"]},
          "log_unmet": True, "d0": DUCK0}
    ev.update(over)
    return ev


def test_p7_passes_when_the_unmet_switch_runs_as_duckdb_and_pages():
    verdict, detail = probe.judge_p7(p7_ev())
    assert verdict == PASS, detail


@pytest.mark.parametrize("change", [
    {"snapshot": snap("postgres", unmet=[])},
    {"snapshot": snap("duckdb", unmet=["read_fallback_off", "ch_url"],
                      jobs=("warehouse_refresh",))},
    {"canary": {"severity": "warn", "failure_keys": ["warehouse_preconditions_unmet"],
                "preconditions_keys": ["warehouse_preconditions_unmet"]}},
    {"snapshot": snap("duckdb", unmet=["read_fallback_off"], jobs=("pg_warehouse_derive",))},
    {"d0": {**DUCK0, "writer": {"writer": "postgres"}}},
    {"log_unmet": False},
])
def test_p7_fails_on_each_broken_half(change):
    assert probe.judge_p7(p7_ev(**change))[0] == FAIL


def test_p7_is_unknown_without_a_process_or_a_canary():
    assert probe.judge_p7(p7_ev(snapshot=None))[0] == UNKNOWN
    assert probe.judge_p7(p7_ev(canary=None))[0] == UNKNOWN


# ─── P1 ──────────────────────────────────────────────────────────────────────

def p1_ev(**over):
    ev = {"snapshot": snap(jobs=("pg_warehouse_derive", "incremental_sync")), "seeded": True,
          "log": {"not_registered": 1, "holds": 1, "recorded": 1},
          "resolved_events": 1, "d1": DUCK1}
    ev.update(over)
    return ev


def test_p1_passes_on_a_complete_flip():
    verdict, detail = probe.judge_p1(p1_ev())
    assert verdict == PASS, detail
    assert T1 in detail


def test_p1_without_a_seeded_page_is_unknown_never_pass():
    """Under KS_ALERTS_DISABLED nothing is ever delivered, so with no seeded
    page `resolve_group` journals nothing and 'one resolve' is unprovable."""
    assert probe.judge_p1(p1_ev(seeded=False))[0] == UNKNOWN


def test_p1_names_the_unmet_keys_when_there_was_no_flip():
    verdict, detail = probe.judge_p1(p1_ev(
        snapshot=snap("duckdb", unmet=["goals_bridge"], cut_unmet=["goals_bridge"])))
    assert verdict == FAIL and "goals_bridge" in detail


@pytest.mark.parametrize("change", [
    {"resolved_events": 2},
    {"resolved_events": 0},
    {"log": {"not_registered": 1, "holds": 1, "recorded": 2}},
    {"snapshot": snap(jobs=("pg_warehouse_derive", "warehouse_refresh"))},
    {"d1": {**DUCK1, "writer": {"writer": "postgres", "resolved": False}}},
])
def test_p1_fails_on_each_broken_half(change):
    assert probe.judge_p1(p1_ev(**change))[0] == FAIL


def test_p1_with_no_duckdb_read_after_the_flip_is_unknown_not_fail():
    """A probe call that died at F7 left `d1.json` empty, and P1 read that as
    the product failing to record the writer. Evidence that is missing is
    UNKNOWN; it is FAIL only beside a failure the evidence does show.
    Kills: "a missing writer read is appended to the failures"."""
    verdict, detail = probe.judge_p1(p1_ev(d1=None))
    assert verdict == UNKNOWN and "not read" in detail
    assert probe.judge_p1(p1_ev(d1=None, resolved_events=2))[0] == FAIL


# ─── P2 ──────────────────────────────────────────────────────────────────────

def p2_ev(**over):
    ev = {"flipped": True, "d0": DUCK0, "d1": DUCK1,
          "frozen_samples": ["2026-10-01T07:58:00.123456"] * 4,
          "runs_ok": 5, "refreshing_lines": 0, "sync_ticks": 9}
    ev.update(over)
    return ev


def test_p2_passes_when_duckdb_is_frozen_and_postgres_derives():
    verdict, detail = probe.judge_p2(p2_ev())
    assert verdict == PASS, detail


@pytest.mark.parametrize("change", [
    {"d1": {**DUCK1, "warehouse_dirty": ["full", "2026-10-01 10:59:00"]}},
    {"d1": {**DUCK1, "refreshes": {"max_refreshed_at": "2026-10-01 10:00:00", "count": 813}}},
    {"frozen_samples": ["2026-10-01T07:58:00.123456", "2026-10-01T10:00:00"]},
    {"frozen_samples": []},
    {"runs_ok": 2},
    {"refreshing_lines": 1},
    {"sync_ticks": 1},
])
def test_p2_fails_when_anything_moved_or_too_little_ran(change):
    assert probe.judge_p2(p2_ev(**change))[0] == FAIL


def test_p2_is_unknown_without_a_flip_or_both_reads():
    assert probe.judge_p2(p2_ev(flipped=False))[0] == UNKNOWN
    assert probe.judge_p2(p2_ev(d1=None))[0] == UNKNOWN


def test_p2_holds_the_stop_before_the_kill_to_the_same_frozen_state():
    assert probe.judge_p2(p2_ev(d_pre=DUCK1))[0] == PASS
    moved = {**DUCK1, "refreshes": {"max_refreshed_at": "2026-10-01 10:00:00", "count": 813}}
    verdict, detail = probe.judge_p2(p2_ev(d_pre=moved))
    assert verdict == FAIL and "before the kill" in detail


def test_a_timestamp_is_compared_as_an_instant_across_renderings():
    assert probe._same_instant("2026-10-01 10:00:00+03", "2026-10-01T07:00:00+00:00") is True
    assert probe._same_instant("2026-10-01 10:00:00", "2026-10-01T10:00:00") is True
    assert probe._same_instant("2026-10-01 10:00:01", "2026-10-01T10:00:00") is False


# ─── P3 ──────────────────────────────────────────────────────────────────────

def stop_state(kind="graceful", **over):
    """The script's `state_of` after a stop, as the container shows it: a
    graceful stop exits 0; the rehearsal's `docker kill` and a `docker stop`
    whose grace ran out both exit 137, told apart by what was asked."""
    code, asked = {"graceful": (0, "stop"), "kill": (137, "kill"),
                   "grace_expired": (137, "stop"), "oom": (137, "kill"),
                   "running": (0, "kill")}[kind]
    state = {"running": kind == "running", "exit_code": code, "oom_killed": kind == "oom",
             "started_at": "2026-10-01T10:50:00.1Z", "finished_at": "2026-10-01T10:55:00.25Z",
             "restart_count": 0, "asked": asked, "was_running": True}
    if asked == "stop":
        state.update(grace_s=10, stop_s=11 if kind == "grace_expired" else 2)
    state.update(over)
    return state


START = {"running": True, "exit_code": 0, "oom_killed": False,
         "started_at": "2026-10-01T10:55:03.5Z", "finished_at": "2026-10-01T10:55:00.25Z",
         "restart_count": 0}


def p3_ev(**over):
    ev = {"flipped": True, "seen_blocked": True, "stop": stop_state("kill"),
          "requested": 41, "built": 40, "silver_version": 4,
          "after_kill": {"requested": 41, "built": 40, "rows_above": 0},
          "first_after_restart": {"id": 77, "trigger": "signal", "requested_seen": 41,
                                  "validation_passed": True, "error": None},
          "built_after": 41}
    ev.update(over)
    return ev


def test_p3_passes_when_the_owed_state_survives_the_kill():
    verdict, detail = probe.judge_p3(p3_ev())
    assert verdict == PASS, detail


@pytest.mark.parametrize("change", [
    {"first_after_restart": {"id": 77, "trigger": "first_tick", "requested_seen": 41,
                             "validation_passed": True, "error": None}},
    {"first_after_restart": {"id": 77, "trigger": "signal", "requested_seen": 40,
                             "validation_passed": True, "error": None}},
    {"first_after_restart": {"id": 77, "trigger": "signal", "requested_seen": 41,
                             "validation_passed": False, "error": None}},
    {"first_after_restart": None},
    {"after_kill": {"requested": 41, "built": 41, "rows_above": 0}},
    {"built_after": 40},
])
def test_p3_fails_when_the_owed_state_is_lost_or_not_rebuilt(change):
    assert probe.judge_p3(p3_ev(**change))[0] == FAIL


def test_p3_is_unknown_when_the_kill_did_not_land_inside_the_derivation():
    assert probe.judge_p3(p3_ev(seen_blocked=False))[0] == UNKNOWN
    # The statement timeout beat the kill: a journal row for the killed run.
    assert probe.judge_p3(p3_ev(after_kill={"requested": 41, "built": 40,
                                            "rows_above": 1}))[0] == UNKNOWN


@pytest.mark.parametrize("stop", [
    stop_state("oom"), stop_state("running"), stop_state("grace_expired"),
    stop_state("kill", was_running=False), None,
])
def test_p3_judges_the_kill_the_container_shows_not_the_one_sent(stop):
    """The TRUNCATE was seen blocked, but the container was not left
    SIGKILLed by the rehearsal — the OOM killer took it first, or it never
    stopped. What followed is no restart after a kill inside the
    derivation, and judging it would blame the product ("no derivation ran
    after the restart") for a kill that never landed.
    Kills: "P3 trusts seen_blocked and never reads the kill's state"."""
    ev = p3_ev(stop=stop, after_kill=None, first_after_restart=None)
    verdict, detail = probe.judge_p3(ev)
    assert verdict == UNKNOWN and "not left SIGKILLed" in detail, detail


def test_a_stops_kind_is_read_from_the_container_state():
    kinds = {k: probe.stop_kind(stop_state(k)) for k in
             ("graceful", "kill", "grace_expired", "oom", "running")}
    assert kinds == {"graceful": "graceful", "kill": "kill", "grace_expired": "grace_expired",
                     "oom": "oom", "running": "running"}
    assert probe.stop_kind(stop_state("kill", was_running=False)) == "not_running"
    assert probe.stop_kind(stop_state("graceful", exit_code=143)) == "exit 143"
    assert probe.stop_kind(None) is None
    assert "ran out" in probe._describe_stop(stop_state("grace_expired"))


def test_docker_times_are_read_whole_whatever_their_digits():
    a = probe._docker_time("2026-10-02T09:27:19.81198843Z")
    b = probe._docker_time("2026-10-02T09:27:19.811988431Z")
    assert a is not None and a == b
    assert probe._docker_time("2026-10-02T09:27:19Z") < a
    assert probe._docker_time("0001-01-01T00:00:00Z").year == 1
    assert probe._docker_time("not a time") is None


# ─── P4 ──────────────────────────────────────────────────────────────────────

def _read_whole(run):
    """The run as the product's reader sees it when DuckDB answers honestly:
    the same names the scan found."""
    return {**run, "reader": {"names": sorted(i["check_name"] for i in run["issues"])}}


def integrity_run(*, missing_ids=(900123,), extra=(), reader=None):
    issues = [{"check_name": "pg_silver_missing_rows", "count": len(missing_ids),
               "sample_ids": list(missing_ids), "severity": "WARN"},
              {"check_name": "pg_twin_pairing", "count": 1, "sample_ids": [],
               "description": json.dumps({
                   "duckdb": {"silver_missing_rows": None, "silver_orphan_rows": None,
                              "headline_vs_line_items": None,
                              "goods_shipped_without_sale": None},
                   "standalone": {"pg_silver_missing_rows": len(missing_ids)}})}]
    run = _read_whole({"run_id": 31, "error_message": None, "issues": issues + list(extra)})
    if reader is not None:
        run["reader"] = reader
    return run


def p4_ev(**over):
    b = [{"k": k, "status": s, "requested_before": 10 + k, "requested_after": 11 + k,
          "silver_status": s, "latency_s": 70.5,
          "run": {"id": 60 + k, "trigger": "signal", "requested_seen": 11 + k,
                  "validation_passed": True, "error": None}}
         for k, s in ((1, 1), (2, 2), (3, 8))]
    ev = {"flipped": True,
          "a": {"deleted_id": 900123, "derivations_between": 0, "restored": True,
                "run": integrity_run()},
          "b": b,
          "c": {"manager_id": 31,
                "with_trigger": {"partition_exhaustive": False,
                                 "unknown_sales_types": [{"sales_type": "reh_unknown",
                                                          "revenue": 2160.0}]},
                "log_alert": 1, "after_drop": {"partition_exhaustive": True}}}
    ev.update(over)
    return ev


def test_p4_passes_when_all_three_mutations_are_caught():
    verdict, detail = probe.judge_p4(p4_ev(), floor_s=120)
    assert verdict == PASS, detail
    assert "4a PASS" in detail and "4b PASS" in detail and "4c PASS" in detail


def test_p4a_fails_when_the_twin_does_not_name_the_row():
    ev = p4_ev()
    ev["a"] = {**ev["a"], "run": integrity_run(missing_ids=(5,))}
    assert probe.judge_p4(ev, floor_s=120)[0] == FAIL


def test_p4a_fails_when_the_products_reader_does_not_see_the_finding():
    ev = p4_ev()
    ev["a"] = {**ev["a"], "run": integrity_run(reader={"names": []})}
    verdict, detail = probe.judge_p4(ev, floor_s=120)
    assert verdict == FAIL and "4a FAIL" in detail and "fetch_run_issues" in detail


def test_p4a_is_unknown_when_the_products_reader_was_not_run():
    ev = p4_ev()
    ev["a"] = {**ev["a"], "run": integrity_run(reader={"error": "CatalogException"})}
    assert probe.judge_p4(ev, floor_s=120)[0] == UNKNOWN


def test_the_reader_judge_says_what_the_live_process_saw():
    run = {**integrity_run(reader={"names": []}), "live_names": ["pg_silver_missing_rows",
                                                                "pg_twin_pairing"]}
    fail, unknown = probe.judge_reader(run)
    assert unknown is None and "the live process showed 2 at F4" in fail


def test_p4a_healed_before_the_check_looked_is_unknown():
    ev = p4_ev()
    ev["a"] = {**ev["a"], "derivations_between": 1}
    assert probe.judge_p4(ev, floor_s=120)[0] == UNKNOWN


def test_p4b_fails_past_the_floor_and_is_unknown_when_a_version_never_landed():
    late = p4_ev()
    late["b"][1] = {**late["b"][1], "latency_s": 400}
    assert probe.judge_p4(late, floor_s=120)[0] == FAIL
    short = p4_ev(b=p4_ev()["b"][:2])
    assert probe.judge_p4(short, floor_s=120)[0] == UNKNOWN


def test_p4b_fails_when_silver_does_not_show_the_version():
    ev = p4_ev()
    ev["b"][2] = {**ev["b"][2], "silver_status": 2}
    assert probe.judge_p4(ev, floor_s=120)[0] == FAIL


def test_p4c_fails_when_the_partition_is_not_caught_or_not_restored():
    missed = p4_ev()
    missed["c"] = {**missed["c"], "with_trigger": {"partition_exhaustive": True,
                                                    "unknown_sales_types": []}}
    assert probe.judge_p4(missed, floor_s=120)[0] == FAIL
    stuck = p4_ev()
    stuck["c"] = {**stuck["c"], "after_drop": {"partition_exhaustive": False}}
    assert probe.judge_p4(stuck, floor_s=120)[0] == FAIL
    silent = p4_ev()
    silent["c"] = {**silent["c"], "log_alert": 0}
    assert probe.judge_p4(silent, floor_s=120)[0] == FAIL


# ─── P5 ──────────────────────────────────────────────────────────────────────

RETIRED = ["silver_arc_mismatch", "gold_cell_values", "attribution_coverage_low"]
TWINS = ["pg_silver_missing_rows", "pg_silver_orphan_rows", "pg_headline_vs_line_items"]


ASKED = ["reconcile_mirror", "reconcile_orders", "reconcile_order_utm_completeness",
         "reconcile_expenses", "pg_gold_internal_check", "reconcile_operational",
         "reconcile_clickhouse", "reconcile_ch_history"]


def mirror_run(issues=None, **over):
    issues = issues if issues is not None else [
        {"check_name": "gold_rollup_mismatch", "table_name": "gold.daily_revenue"},
        {"check_name": "mirror_row_values", "table_name": "bronze.orders"}]
    run = _read_whole({"run_id": 32, "error_message": None, "issues": issues})
    run.update(over)
    return run


def p5_ev(**over):
    ev = {"flipped": True, "mirror": mirror_run(), "integrity": integrity_run(),
          "stood_down": STOOD, "checks_run": list(ASKED)}
    ev.update(over)
    return ev


def test_p5_passes_when_the_stand_down_holds_in_both_jobs():
    verdict, detail = probe.judge_p5(p5_ev(), retired=RETIRED, standalone_twins=TWINS)
    assert verdict == PASS, detail


@pytest.mark.parametrize("mirror_issues", [
    [{"check_name": "gold_cell_values", "table_name": "gold"}],
    [{"check_name": "gold_rollup_mismatch", "table_name": "gold"},
     {"check_name": "mirror_row_values", "table_name": "silver.orders"}],
    [],
])
def test_p5_fails_when_a_retired_comparison_ran_or_the_internal_check_did_not(mirror_issues):
    ev = p5_ev(mirror=mirror_run(mirror_issues))
    assert probe.judge_p5(ev, retired=RETIRED, standalone_twins=TWINS)[0] == FAIL


@pytest.mark.parametrize("retired", list(probe.RETIRED_COMPARISONS))
def test_p5_fails_on_a_retired_comparison_that_ran_and_found_nothing(retired):
    """The review's mutation: `reconcile_gold` run under the stand-down, with
    every cell excused as in flight, files nothing — the run's findings are
    exactly a clean stand-down's. Only the job's own list tells them apart."""
    verdict, detail = probe.judge_p5(p5_ev(checks_run=ASKED + [retired]),
                                     retired=RETIRED, standalone_twins=TWINS)
    assert verdict == FAIL and retired in detail


def test_p5_fails_when_the_internal_check_was_not_asked():
    asked = [c for c in ASKED if c != probe.INTERNAL_CHECK]
    assert probe.judge_p5(p5_ev(checks_run=asked), retired=RETIRED,
                          standalone_twins=TWINS)[0] == FAIL


def test_p5_without_the_jobs_list_is_unknown_never_pass():
    """An image whose completion line carries no `checks_run`: absent
    findings alone cannot have shown a comparison that ran."""
    verdict, detail = probe.judge_p5(p5_ev(checks_run=None), retired=RETIRED,
                                     standalone_twins=TWINS)
    assert verdict == UNKNOWN and "checks_run" in detail


@pytest.mark.parametrize("which", ["mirror", "integrity"])
def test_p5_fails_when_the_products_reader_does_not_see_a_run(which):
    """A kill can leave DuckDB 1.5.5 answering `run_id = ?` with nothing for
    runs a scan returns; `fetch_run_issues` is that query, so the dashboard
    and the digest would show those runs as empty. P5 reads its runs before
    the rehearsal's kill, and still fails if the reader there is blind."""
    run = (mirror_run if which == "mirror" else integrity_run)()
    ev = p5_ev(**{which: {**run, "reader": {"names": []}}})
    verdict, detail = probe.judge_p5(ev, retired=RETIRED, standalone_twins=TWINS)
    assert verdict == FAIL and "fetch_run_issues returns 0 of the" in detail


def test_p5_is_unknown_when_the_products_reader_was_not_run():
    ev = p5_ev(mirror=mirror_run(reader={"error": "ImportError"}))
    assert probe.judge_p5(ev, retired=RETIRED, standalone_twins=TWINS)[0] == UNKNOWN
    bare = mirror_run()
    del bare["reader"]
    assert probe.judge_p5(p5_ev(mirror=bare), retired=RETIRED,
                          standalone_twins=TWINS)[0] == UNKNOWN


def test_p5_fails_on_an_error_or_a_stood_down_check_filing():
    errored = p5_ev()
    errored["mirror"] = {**errored["mirror"], "error_message": "reconcile_gold: boom"}
    assert probe.judge_p5(errored, retired=RETIRED, standalone_twins=TWINS)[0] == FAIL
    filed = p5_ev(integrity=integrity_run(extra=[{"check_name": "silver_arc_mismatch"}]))
    assert probe.judge_p5(filed, retired=RETIRED, standalone_twins=TWINS)[0] == FAIL
    looked = p5_ev()
    run = integrity_run()
    body = json.loads(run["issues"][1]["description"])
    body["duckdb"]["silver_missing_rows"] = 0
    run["issues"][1]["description"] = json.dumps(body)
    looked["integrity"] = run
    assert probe.judge_p5(looked, retired=RETIRED, standalone_twins=TWINS)[0] == FAIL


def test_p5_standalone_counts_must_be_filed_as_counted():
    run = integrity_run()
    body = json.loads(run["issues"][1]["description"])
    body["standalone"]["pg_headline_vs_line_items"] = 3
    run["issues"][1]["description"] = json.dumps(body)
    assert probe.judge_p5(p5_ev(integrity=run), retired=RETIRED,
                          standalone_twins=TWINS)[0] == FAIL


def test_p5_is_unknown_without_both_runs():
    assert probe.judge_p5(p5_ev(mirror=None), retired=RETIRED, standalone_twins=TWINS)[0] == UNKNOWN


# ─── P6 ──────────────────────────────────────────────────────────────────────

def restart(kind="graceful", **over):
    """One P6 record as `restart_record` writes it: the container's state
    after the stop before the restart, and after the start."""
    r = {"stop": stop_state(kind), "start": dict(START), "snapshot": snap(),
         "resolved_events": 1, "recorded_lines": 1, "resolved_lines": 1}
    r.update(over)
    return r


def p6_ev(**over):
    ev = {"flipped": True, "resolved_at": T1,
          "restarts": [restart("graceful"), restart("kill"), restart("graceful")],
          "gate_delivered": [], "kill_stop": stop_state("kill")}
    ev.update(over)
    return ev


def test_p6_passes_on_three_shown_restarts_and_one_resolve():
    verdict, detail = probe.judge_p6(p6_ev())
    assert verdict == PASS, detail
    assert "3 restarts the container shows (graceful,kill,graceful)" in detail


@pytest.mark.parametrize("change", [
    {"restarts": [restart(), restart("kill", resolved_events=2), restart()]},
    {"restarts": [restart(resolved_lines=2), restart("kill"), restart()]},
    {"restarts": [restart(), restart("kill", snapshot=snap(writer={
        "writer": "postgres", "resolved": True, "resolved_at": "2026-10-01T11:30:00+00:00"}))]},
    {"restarts": [restart(snapshot=snap("duckdb")), restart("kill")]},
    {"gate_delivered": ["warehouse:validation_retrying"]},
])
def test_p6_fails_on_a_second_resolve_or_a_lost_flip(change):
    assert probe.judge_p6(p6_ev(**change))[0] == FAIL


def test_p6_is_unknown_with_one_restart():
    assert probe.judge_p6(p6_ev(restarts=[restart("kill")]))[0] == UNKNOWN


def test_p6_judges_all_three():
    three = [restart(), restart("kill"), restart(resolved_events=2)]
    assert probe.judge_p6(p6_ev(restarts=three))[0] == FAIL


def test_p6_without_the_restart_after_the_kill_is_unknown_never_pass():
    """F5s's graceful restart and F7's make two; without the one after F6's
    kill they are not the two P6 is about."""
    verdict, detail = probe.judge_p6(p6_ev(restarts=[restart(), restart()]))
    assert verdict == UNKNOWN and "no restart after the rehearsal's kill" in detail


@pytest.mark.parametrize("second", [
    # A record the script labelled a kill, whose container never stopped.
    {"kind": "kill", "stop": stop_state("running"), "start": dict(START)},
    # The same, with `docker start` on a running container: nothing moved.
    {"kind": "kill", "stop": stop_state("kill"),
     "start": {**START, "started_at": "2026-10-01T10:50:00.1Z"}},
    # The kernel's OOM killer, not the rehearsal's kill.
    {"stop": stop_state("oom")},
    # A record with no container state at all — what the script wrote before.
    {"kind": "kill", "stop": None, "start": None},
])
def test_p6_counts_the_kill_the_container_shows_not_the_label(second):
    """The script once wrote a `kill` record for a restart that never
    happened — no TRUNCATE seen, nothing killed, `docker start` on a running
    container — and P6 passed "graceful,kill,graceful" on it. The kind is
    read from the container's own state now, and a record that shows no
    stop and start is no restart.
    Kills: "P6 reads the record's kind", "P6 counts a record without the
    container's state", "P6 counts a start that did not move"."""
    verdict, detail = probe.judge_p6(p6_ev(restarts=[restart(), restart(**second), restart()]))
    assert verdict == UNKNOWN, detail
    assert "no restart after the rehearsal's kill" in detail


def test_p6_a_graceful_stop_that_outran_the_grace_is_said_and_unknown():
    """`docker stop` past its grace ends exit 137, the state a kill leaves.
    With production's grace that is what a deploy would do to web: the
    restart still counts, and the row says why it is not a PASS.
    Kills: "P6 takes any exit 137 for the kill it asked for", "P6 never
    requires a graceful stop to have exited 0"."""
    verdict, detail = probe.judge_p6(p6_ev(restarts=[
        restart("grace_expired"), restart("kill"), restart()]))
    assert verdict == UNKNOWN and "restart 1:" in detail and "ran out" in detail, detail


def test_p6_with_the_gate_file_unread_is_unknown_not_fail():
    verdict, detail = probe.judge_p6(p6_ev(gate_delivered=None))
    assert verdict == UNKNOWN and "gate file was not read" in detail


@pytest.mark.parametrize("kill_stop, why", [
    (None, "P3 records no kill"),
    (stop_state("oom"), "P3's kill is not one"),
    (stop_state("kill", finished_at="2026-10-01T11:20:00.5Z"), "is not P3's"),
])
def test_p6_counts_only_the_kill_p3_records(kill_stop, why):
    """The restart after the kill is the one P3's evidence shows: the script
    writes one state file at F6 into both, so a `kill` record whose SIGKILL
    P3 does not record — none at all, or another one — proves no restart
    after the rehearsal's kill, however well-formed the record is.
    Kills: "P6 never reads P3's kill", "P6 accepts any SIGKILL"."""
    verdict, detail = probe.judge_p6(p6_ev(kill_stop=kill_stop))
    assert verdict == UNKNOWN, detail
    assert "no restart after the rehearsal's kill" in detail and why in detail, detail


def _write_evidence(ev_dir, files):
    for name, body in files.items():
        (ev_dir / name).write_text(body if isinstance(body, str) else json.dumps(body))


@pytest.mark.parametrize("records", [
    # What the script wrote before it read the container: labels alone.
    [{"kind": k, "snapshot": snap(), "resolved_events": 1, "recorded_lines": 1,
      "resolved_lines": 1} for k in ("graceful", "kill", "graceful")],
    # The same restarts recorded with the containers' states, the kill's
    # well-formed — nothing but P3 says it was never sent.
    [restart("graceful"), restart("kill"), restart("graceful")],
], ids=["labels", "states"])
def test_the_fake_kill_run_does_not_pass_p6(tmp_path, records):
    """The reviewer's repro: F6 saw no blocked TRUNCATE, so nothing was
    killed and `docker start` on the running reh-web did nothing — yet the
    script wrote a `kill` restart record inside `if wait_health`, and P6
    passed "3 restarts (graceful,kill,graceful)". Judged off the evidence
    files as the script leaves them, P6, P3 and D1 must all say no kill.
    Kills: "P6 passes graceful,kill,graceful beside a P3 that saw nothing"."""
    files = {"f1_snapshot.json": snap(), "d1.json": {**DUCK1, "gate_delivered": []},
             "p3.json": {"seen_blocked": False, "stop": None,
                         "why": "no TRUNCATE gold.daily_revenue waited on the lock in time"}}
    files.update({f"p6_restart{n}.json": r for n, r in enumerate(records, start=1)})
    _write_evidence(tmp_path, files)
    rows = {label.split()[0]: (v, d) for label, v, d in probe.judge_all(
        tmp_path, floor_s=60, retired=RETIRED, standalone_twins=TWINS)}
    assert rows["P6"][0] == UNKNOWN, rows["P6"]
    assert "no restart after the rehearsal's kill" in rows["P6"][1] \
        or "of 2 restarts shown" in rows["P6"][1], rows["P6"]
    assert rows["P3"][0] == UNKNOWN and "not seen" in rows["P3"][1], rows["P3"]
    assert rows["D1"] == (UNKNOWN, "no kill landed inside the derivation (see P3)")


# ─── D1 ──────────────────────────────────────────────────────────────────────

SWEEP = {"swept": [{"index": "data_quality_issues.idx_dqi_run", "rows": 9, "missing": 0},
                   {"index": "orders.idx_orders_status", "rows": 401, "missing": 0}],
         "skipped": [{"index": "orders.idx_orders_buyer_date", "why": "composite"},
                     {"index": "gold_daily_revenue.idx_gold_rev_date", "why": "composite"},
                     {"index": "data_quality_diffs.idx_dqd_run", "why": "empty"}]}
WIN, FLOOR, FIX, V4 = 41, 32, 900500, 9


# The fixture order has no buyer and no manager, and DuckDB keeps no index
# entry for a row with a NULL key: these hold none of the window's rows.
NULL_KEYED = ("idx_orders_buyer", "idx_orders_buyer_date", "idx_orders_manager",
              "idx_orders_manager_date")


def _indexes(*names):
    return [{"index": n, "columns": 2 if n.endswith("_date") else 1,
             "held": 0 if n in NULL_KEYED else 1} for n in names]


def window_read(**over):
    """`window_rows` on the copy after the kill: F5w's run, read by a scan
    and by the product's reader, and the window's rows by table."""
    w = {"run_id": WIN, "order_id": FIX, "order_status": V4,
         "run": {**integrity_run(), "run_id": WIN},
         "tables": {
             "data_quality_issues": {"rows": 2, "indexes": _indexes("idx_dqi_run")},
             "data_quality_diffs": {"rows": 0, "indexes": _indexes("idx_dqd_run")},
             "data_quality_runs": {"rows": 1, "indexes": _indexes("idx_dqr_layer",
                                                                  "idx_dqr_started_at")},
             "order_products": {"rows": 1, "indexes": _indexes("idx_order_products_order",
                                                               "idx_order_products_product")},
             "orders": {"rows": 1, "indexes": _indexes(
                 "idx_orders_buyer", "idx_orders_buyer_date", "idx_orders_manager",
                 "idx_orders_manager_date", "idx_orders_ordered_at", "idx_orders_source",
                 "idx_orders_source_date", "idx_orders_status", "idx_orders_status_date")}}}
    w.update(over)
    return w


def deletes(**over):
    """`window_delete` at the end: each window table's rows found by a scan,
    and every one of them given up by every index on it."""
    out = [{"table": table, "scanned": n, "deleted": n, "left": 0}
           for table, n in (("data_quality_issues", 2), ("data_quality_diffs", 0),
                            ("data_quality_runs", 1), ("order_products", 1), ("orders", 1))]
    for d in out:
        if d["table"] in over:
            d.update(over[d["table"]])
            for key in [k for k, v in d.items() if v is None]:
                d.pop(key)
    return {"run_id": WIN, "order_id": FIX, "deletes": out}


def d1_ev(**over):
    ev = {"flipped": True, "seen_blocked": True, "kill_stop": stop_state("kill"),
          "pre": {**DUCK1, "max_dq_run_id": FLOOR, "indexes": SWEEP,
                  "runs": {"31": integrity_run(), "32": mirror_run()}},
          "post": {**DUCK1, "max_dq_run_id": WIN, "indexes": SWEEP, "window": window_read()},
          "f7_stop": stop_state("graceful"), "p0_stop": stop_state("graceful"),
          "window_ids": {"run_id": WIN, "order_id": FIX, "order_status": V4},
          "delete": deletes(),
          "b_pre_stop": stop_state("graceful"), "b_stop": stop_state("graceful")}
    ev.update(over)
    return ev


def test_d1_passes_when_nothing_it_asks_was_lost():
    """The PASS says what was asked and what was not — never that the kill
    cost nothing at all: a table whose DELETE found no row asked none of its
    indexes, an index holding none of the window's rows (a NULL key) was not
    asked whatever its table, and composite indexes outside the window were
    never asked. The fixture order has no buyer and no manager, so of the
    four composite indexes on `orders` the DELETE asks two — the reviewer's
    reading of the 10-07 evidence, which this PASS once called "4 composite
    among them".
    Kills: "D1 counts a table the DELETE found empty as asked", "D1 counts
    an index holding none of the window's rows as asked"."""
    verdict, detail = probe.judge_d1(d1_ev())
    assert verdict == PASS, detail
    assert f"run #{WIN} (2 findings)" in detail and f"order #{FIX}'s (1 lines)" in detail
    assert ("given up at the end by the 10 indexes holding them on the 4 tables they are in, "
            "2 composite among them") in detail
    assert ("Not asked: 4 indexes on those tables that hold none of them, a NULL key in each "
            "[orders.idx_orders_buyer,orders.idx_orders_buyer_date,orders.idx_orders_manager,"
            "orders.idx_orders_manager_date]") in detail
    assert "2 single-column indexes" in detail
    assert "1 composite indexes on tables outside the window, 1 on an empty" in detail
    assert "cost nothing" not in detail and "4 composite" not in detail


def test_d1_without_which_indexes_hold_the_window_cannot_count_them():
    """Evidence read before `held` existed — the 10-07 run's — says which
    indexes a table has, not which of them hold its window row; counting all
    of them is how the PASS claimed four composite indexes where the DELETE
    asked two. An index whose holding was not read leaves the row UNKNOWN.
    Kills: "an index whose holding was not read counts as asked"."""
    ev = d1_ev()
    tables = {t: {**v, "indexes": [{k: x for k, x in i.items() if k != "held"}
                                   for i in v["indexes"]]}
              for t, v in ev["post"]["window"]["tables"].items()}
    ev["post"] = {**ev["post"], "window": window_read(tables=tables)}
    verdict, detail = probe.judge_d1(ev)
    assert verdict == UNKNOWN, detail
    assert ("orders.idx_orders_buyer,orders.idx_orders_buyer_date,orders.idx_orders_manager,"
            "orders.idx_orders_manager_date,orders.idx_orders_ordered_at,orders.idx_orders_source,"
            "orders.idx_orders_source_date,orders.idx_orders_status,orders.idx_orders_status_date] "
            "hold the window's rows was not read") in detail
    assert detail.count("was not read") == 1, detail
    # A table whose DELETE found nothing asked none of its indexes either way.
    assert "data_quality_diffs" not in detail


def test_d1_fails_on_an_index_the_kill_left_short():
    lost = {**SWEEP, "swept": [{**SWEEP["swept"][0], "missing": 7}, SWEEP["swept"][1]]}
    ev = d1_ev()
    ev["post"] = {**ev["post"], "indexes": lost}
    verdict, detail = probe.judge_d1(ev)
    assert verdict == FAIL
    assert "after the kill" in detail and "idx_dqi_run misses 7 of 9" in detail


def test_d1_fails_on_a_loss_already_there_before_the_kill():
    ev = d1_ev(p0_stop=stop_state("grace_expired"))
    ev["pre"] = {**ev["pre"], "indexes": {**SWEEP, "swept": [{**SWEEP["swept"][1], "missing": 1}]}}
    verdict, detail = probe.judge_d1(ev)
    assert verdict == FAIL and "before the kill" in detail and "phase 0's stop" in detail
    # The copy's own loss is not what the rehearsal's kill did.
    assert probe.KNOWN_DEFECT not in detail


@pytest.mark.parametrize("change", [
    {"post": {**d1_ev()["post"], "indexes": {**SWEEP, "swept": [
        {**SWEEP["swept"][0], "missing": 7}, SWEEP["swept"][1]]}}},
    {"post": {**d1_ev()["post"], "window": window_read(
        run={**integrity_run(reader={"names": []}), "run_id": WIN})}},
    {"delete": deletes(orders={"fatal": "FATAL Error: Failed to delete all rows from index",
                               "scanned": None, "deleted": None, "left": None})},
])
def test_a_loss_after_the_kill_is_labelled_the_known_defect(change):
    """What the rehearsal's kill costs DuckDB 1.5.5 is the defect measured
    apart from the rehearsal, which an OOM kill of the live web would cost
    the same way; the switch to Postgres writes no DuckDB index. The row
    stays FAIL — the defect is production's — and says it is that one, not
    a step-13 regression, beside a loss the copy already carried.
    Kills: "D1's FAIL does not say which defect it is", "a pre-kill loss is
    labelled the kill's"."""
    verdict, detail = probe.judge_d1(d1_ev(**change))
    assert verdict == FAIL and detail.startswith(probe.KNOWN_DEFECT), detail
    assert "not a step-13 regression" in detail
    ev = d1_ev(**change)
    ev["pre"] = {**ev["pre"], "indexes": {**SWEEP, "swept": [{**SWEEP["swept"][1], "missing": 1}]}}
    verdict, detail = probe.judge_d1(ev)
    assert verdict == FAIL and detail.startswith("before the kill"), detail
    assert f"; {probe.KNOWN_DEFECT}: after the kill, " in detail


def test_a_shortfall_the_copy_carried_is_not_the_kills():
    """An index already short at F5s is still short after the kill; the
    sweep after it reports only what was lost since, so the copy's own loss
    is not labelled the kill's — and a further loss is.
    Kills: "the post-kill sweep reports the pre-kill loss again as the
    defect"."""
    short = lambda n: {**SWEEP, "swept": [SWEEP["swept"][0], {**SWEEP["swept"][1], "missing": n}]}  # noqa: E731
    ev = d1_ev()
    ev["pre"] = {**ev["pre"], "indexes": short(1)}
    ev["post"] = {**ev["post"], "indexes": short(1)}
    verdict, detail = probe.judge_d1(ev)
    assert verdict == FAIL and detail.startswith("before the kill"), detail
    assert probe.KNOWN_DEFECT not in detail
    ev["post"] = {**ev["post"], "indexes": short(3)}
    verdict, detail = probe.judge_d1(ev)
    assert verdict == FAIL and "idx_orders_status misses 3 of 401 (1 already before the kill)" in detail
    assert f"{probe.KNOWN_DEFECT}: after the kill, " in detail


def test_d1_with_a_delete_that_found_no_window_row_is_unknown():
    """Every DELETE answered and none found a row: no index on the window's
    tables was asked, so nothing is known of what the kill cost them.
    Kills: "a DELETE of nothing passes"."""
    nothing = {t: {"scanned": 0, "deleted": 0, "left": 0} for t in
               ("data_quality_issues", "data_quality_runs", "order_products", "orders")}
    verdict, detail = probe.judge_d1(d1_ev(delete=deletes(**nothing)))
    assert verdict == UNKNOWN and "found none of the window's rows" in detail, detail


def test_d1_fails_when_the_products_reader_is_blind_to_the_windows_run():
    ev = d1_ev(post={**d1_ev()["post"], "window": window_read(
        run={**integrity_run(reader={"names": []}), "run_id": WIN})})
    verdict, detail = probe.judge_d1(ev)
    assert verdict == FAIL and "fetch_run_issues returns 0 of the" in detail


def test_d1_fails_on_a_delete_an_index_could_not_follow():
    """The one question a composite index answers: taking a row out of it.
    Kills: "D1 never reads the end's DELETE"."""
    fatal = "FATAL Error: Invalid Input Error: Failed to delete all rows from index. Only deleted 0 out of 1 rows."
    verdict, detail = probe.judge_d1(d1_ev(delete=deletes(orders={
        "fatal": fatal, "scanned": None, "deleted": None, "left": None})))
    assert verdict == FAIL and "from orders is DuckDB's FATAL" in detail
    assert "idx_orders_status_date" in detail
    # An index holding none of the rows gave none up, so it is no suspect.
    assert "idx_orders_buyer_date" not in detail


@pytest.mark.parametrize("which, what", [
    ("b_pre_stop", "the stop the way back started from"),
    ("b_stop", "the way back's own stop"),
])
@pytest.mark.parametrize("state, why", [
    (stop_state("grace_expired"), "Docker killed it 11 s into a 10 s grace (exit 137)"),
    (stop_state("oom"), "the kernel's OOM killer stopped it"),
    (None, "the container's state after it was not read"),
])
def test_d1_names_a_way_back_stop_that_was_no_deploys_beside_a_delete_loss(which, what,
                                                                            state, why):
    """The end's DELETE runs after the way back, so the copy reaches it
    through two more stops of reh-web than F7's. One that outran the grace
    was a SIGKILL of its own: a loss the DELETE finds is still DuckDB's
    defect and still FAIL, but it may be that kill's and not P3's, and the
    verdict says so instead of laying it at P3's door. A clean DELETE does
    not lean on those stops — F7's checkpoint already wrote down what P3's
    kill cost — so it still passes.
    Kills: "D1 never reads the stops between F7 and the end's DELETE"."""
    fatal = "FATAL Error: Invalid Input Error: Failed to delete all rows from index. Only deleted 0 out of 1 rows."
    loss = deletes(orders={"fatal": fatal, "scanned": None, "deleted": None, "left": None})
    short = deletes(data_quality_issues={"scanned": 2, "deleted": 0, "left": 2})
    for delete in (loss, short):
        verdict, detail = probe.judge_d1(d1_ev(delete=delete, **{which: state}))
        assert verdict == FAIL and detail.startswith(probe.KNOWN_DEFECT), detail
        assert f"the copy reached the DELETE through {what} ({why}" in detail, detail
        assert "a kill after P3's may have cost it" in detail
        verdict, detail = probe.judge_d1(d1_ev(delete=delete))
        assert verdict == FAIL and "reached the DELETE" not in detail, detail
    assert probe.judge_d1(d1_ev(**{which: state}))[0] == PASS


def test_d1_fails_on_a_delete_that_said_ok_and_left_the_rows():
    """DuckDB 1.5.5: a DELETE whose predicate goes through an index that
    lost the rows finds none and reports success. The scan beside it is
    what says they are still there.
    Kills: "D1 trusts the DELETE's own count"."""
    verdict, detail = probe.judge_d1(d1_ev(delete=deletes(data_quality_issues={
        "scanned": 2, "deleted": 0, "left": 2})))
    assert verdict == FAIL and "took 0 of the 2 rows a scan finds, 2 left" in detail


@pytest.mark.parametrize("change", [
    {"flipped": False},
    {"seen_blocked": False},
    {"kill_stop": stop_state("oom")},
    {"kill_stop": stop_state("running")},
    {"kill_stop": None},
    {"pre": None},
    {"post": None},
])
def test_d1_without_a_kill_the_container_shows_or_both_reads_is_unknown(change):
    """Seen blocked is not killed: the reviewer's case had the OOM killer
    take reh-web first, and D1 then judged a kill that never landed.
    Kills: "D1's kill is seen_blocked"."""
    assert probe.judge_d1(d1_ev(**change))[0] == UNKNOWN


@pytest.mark.parametrize("sweep", [
    None,
    {"error": "IOException", "swept": [], "skipped": []},
    {"swept": [], "skipped": [{"index": "t.i", "why": "empty"}]},
])
def test_d1_with_no_sweep_to_judge_is_unknown(sweep):
    ev = d1_ev()
    ev["post"] = {**ev["post"], "indexes": sweep}
    assert probe.judge_d1(ev)[0] == UNKNOWN


@pytest.mark.parametrize("ids, why", [
    ({"run_id": FLOOR, "order_id": FIX, "order_status": V4}, "already at F5s's checkpoint"),
    ({"run_id": 6, "order_id": FIX, "order_status": V4}, "already at F5s's checkpoint"),
    ({"run_id": None, "order_id": FIX, "order_status": V4}, "F5w wrote no integrity run"),
])
def test_d1_asks_only_of_a_run_written_after_the_checkpoint(ids, why):
    """F5w once took F4's run — checkpointed at F5s, out of the kill's
    reach — when the live API's read of the newest run failed and the
    trigger did not run. The run D1 asks of must be above every run F5s's
    read found.
    Kills: "D1 takes whatever run F5w names"."""
    verdict, detail = probe.judge_d1(d1_ev(window_ids=ids))
    assert verdict == UNKNOWN and why in detail, detail


def test_d1_without_the_checkpoints_newest_run_cannot_place_the_window():
    ev = d1_ev()
    ev["pre"] = {k: v for k, v in ev["pre"].items() if k != "max_dq_run_id"}
    verdict, detail = probe.judge_d1(ev)
    assert verdict == UNKNOWN and "not known to have been written after it" in detail


@pytest.mark.parametrize("window, why", [
    (None, "the window was not read"),
    ({"error": "CatalogException"}, "raised CatalogException"),
    (window_read(run=None), "not in the copy after the kill"),
    (window_read(run={**integrity_run(), "run_id": WIN, "issues": [],
                      "reader": {"names": []}}), "filed no finding"),
    (window_read(order_status=8), "did not land before the kill"),
    (window_read(tables={**window_read()["tables"], "orders": {"rows": 0, "indexes": []}}),
     f"order #{FIX} is not in the copy"),
])
def test_d1_with_nothing_in_the_window_to_ask_is_unknown(window, why):
    ev = d1_ev()
    ev["post"] = {**ev["post"], "window": window}
    verdict, detail = probe.judge_d1(ev)
    assert verdict == UNKNOWN and why in detail, detail


@pytest.mark.parametrize("done, why", [
    (None, "not deleted at the end"),
    ({"skipped": "--keep leaves the copy as it is"}, "--keep"),
    (deletes(orders={"error": "IOException", "scanned": None, "deleted": None, "left": None}),
     "raised IOException"),
    ({"run_id": WIN, "order_id": FIX, "deletes": []}, "deleted no window table"),
])
def test_d1_without_the_ends_delete_is_unknown(done, why):
    verdict, detail = probe.judge_d1(d1_ev(delete=done))
    assert verdict == UNKNOWN and why in detail, detail


@pytest.mark.parametrize("f7", [stop_state("grace_expired"), stop_state("kill"), None])
def test_d1_after_a_stop_that_was_no_checkpoint_is_unknown(f7):
    """A read-only open sees every row the WAL holds whatever an index
    lost; only the close of a graceful stop writes the loss down. A read
    after F7's stop says what the kill cost only when that stop was one.
    Kills: "D1 never asks how F7's stop ended"."""
    verdict, detail = probe.judge_d1(d1_ev(f7_stop=f7))
    assert verdict == UNKNOWN and "F7's stop was no checkpoint" in detail, detail
    ev = d1_ev(f7_stop=f7, delete=deletes(orders={"scanned": 1, "deleted": 0, "left": 1}))
    assert probe.judge_d1(ev)[0] == FAIL


_KILLED_WRITER = """
import os, signal, sys
import duckdb
con = duckdb.connect(sys.argv[1])
con.execute("INSERT INTO t SELECT 100000 + r, r % 3, 'late' || r, 1 FROM range(12) x(r)")
os.kill(os.getpid(), signal.SIGKILL)
"""


def _killed_copy(tmp_path, duckdb, *, kill, restart=True):
    """A file whose last twelve rows a SIGKILL caught in the WAL, then
    replayed by a read-write session that touched nothing and closed — the
    shape of an OOM-killed web restarted and then stopped for a deploy. With
    `kill=False`, the same rows written by a clean session instead; with
    `restart=False`, no session after the kill at all."""
    import subprocess
    import sys

    db = str(tmp_path / f"{'killed' if kill else 'clean'}.duckdb")
    con = duckdb.connect(db)
    con.execute("CREATE TABLE t (id BIGINT PRIMARY KEY, k BIGINT, s VARCHAR, one INTEGER)")
    con.execute("CREATE INDEX i_k ON t(k)")
    con.execute("CREATE INDEX i_s ON t(s)")
    con.execute("CREATE INDEX i_one ON t(one)")
    con.execute("CREATE INDEX i_ks ON t(k, s)")
    con.execute("INSERT INTO t SELECT r, r % 5, 's' || (r % 7), 1 FROM range(1, 3001) x(r)")
    con.close()
    if kill:
        proc = subprocess.run([sys.executable, "-c", _KILLED_WRITER, db])
        assert proc.returncode == -9
    else:
        con = duckdb.connect(db)
        con.execute("INSERT INTO t SELECT 100000 + r, r % 3, 'late' || r, 1 FROM range(12) x(r)")
        con.close()
    if restart:
        duckdb.connect(db).close()
    return duckdb.connect(db, read_only=True)


def test_the_sweep_reads_a_clean_file_whole(tmp_path):
    duckdb = pytest.importorskip("duckdb")
    con = _killed_copy(tmp_path, duckdb, kill=False)
    try:
        sweep = probe.index_sweep(con)
    finally:
        con.close()
    assert sweep["swept"] == [{"index": "t.i_k", "rows": 3012, "missing": 0},
                              {"index": "t.i_s", "rows": 3012, "missing": 0}]
    assert sweep["skipped"] == [{"index": "t.i_ks", "why": "composite"},
                                {"index": "t.i_one", "why": "one value"}]


def test_the_sweep_finds_what_a_kill_costs_duckdb_1_5_5(tmp_path):
    """The defect D1 exists to report, on DuckDB itself. If a DuckDB upgrade
    makes `missing` zero here, the loss may be fixed: run the repro again
    and correct CLAUDE.md "The step-13 rehearsal" before changing this."""
    duckdb = pytest.importorskip("duckdb")
    con = _killed_copy(tmp_path, duckdb, kill=True)
    try:
        sweep = probe.index_sweep(con)
        # What a product's `WHERE col = ?` reads, against the scan.
        assert con.execute("SELECT count(*) FROM t WHERE s = 'late3'").fetchone()[0] == 0
        assert con.execute("SELECT count_if(s = 'late3') FROM t").fetchone()[0] == 1
    finally:
        con.close()
    assert {s["index"]: s["missing"] for s in sweep["swept"]} == {"t.i_k": 12, "t.i_s": 12}


def test_a_read_only_read_after_a_kill_sees_every_row(tmp_path):
    """What makes F7's stop matter to D1, and F5s's not to P4a and P5: a
    read-only open replays the WAL in memory and loses nothing; the loss is
    written by the first checkpoint a read-write session takes after the
    restart — the close of a graceful stop. A read after a stop that was no
    checkpoint therefore sees every row, whatever the kill cost."""
    duckdb = pytest.importorskip("duckdb")
    con = _killed_copy(tmp_path, duckdb, kill=True, restart=False)
    try:
        sweep = probe.index_sweep(con)
    finally:
        con.close()
    assert {s["index"]: s["missing"] for s in sweep["swept"]} == {"t.i_k": 0, "t.i_s": 0}
    duckdb.connect(str(tmp_path / "killed.duckdb")).close()
    con = duckdb.connect(str(tmp_path / "killed.duckdb"), read_only=True)
    try:
        sweep = probe.index_sweep(con)
    finally:
        con.close()
    assert {s["index"]: s["missing"] for s in sweep["swept"]} == {"t.i_k": 12, "t.i_s": 12}


_KILLED_TWO = """
import os, signal, sys
import duckdb
con = duckdb.connect(sys.argv[1])
for table in ("c", "s"):
    con.execute(f"INSERT INTO {table} SELECT 100000 + r, 7, 'late' || r FROM range(4) x(r)")
os.kill(os.getpid(), signal.SIGKILL)
"""


def test_a_composite_index_serves_no_read_duckdb_1_5_5(tmp_path):
    """Measured by what reads return, not by EXPLAIN — which prints
    SEQ_SCAN for a lookup that demonstrably goes through a single-column
    index. After a kill costs both kinds their entries, a filter on the
    composite index's columns still returns what a scan does, and the
    single-column index's filter misses the rows: so no read can ask a
    composite index, and D1 asks it with a DELETE instead."""
    duckdb = pytest.importorskip("duckdb")
    import subprocess
    import sys

    db = str(tmp_path / "two.duckdb")
    con = duckdb.connect(db)
    for table, index in (("c", "CREATE INDEX i_cab ON c(a, b)"), ("s", "CREATE INDEX i_sa ON s(a)")):
        con.execute(f"CREATE TABLE {table} (id BIGINT PRIMARY KEY, a BIGINT, b VARCHAR)")
        con.execute(index)
        con.execute(f"INSERT INTO {table} SELECT r, r % 5, 'early' || r FROM range(1, 3001) x(r)")
    con.close()
    assert subprocess.run([sys.executable, "-c", _KILLED_TWO, db]).returncode == -9
    duckdb.connect(db).close()
    con = duckdb.connect(db, read_only=True)
    try:
        for stmt in probe.SWEEP_SETTINGS:
            con.execute(stmt)

        def both(table, where):
            return (con.execute(f"SELECT count(*) FROM {table} WHERE {where}").fetchone()[0],
                    con.execute(f"SELECT count_if({where}) FROM {table}").fetchone()[0])

        assert both("c", "a = 7 AND b = 'late1'") == (1, 1)
        assert both("c", "a = 7") == (4, 4)
        assert both("c", "a >= 6") == (4, 4)
        assert both("s", "a = 7") == (0, 4)
        assert both("s", "a >= 6") == (0, 4)
    finally:
        con.close()


def test_a_delete_through_a_lost_index_finds_nothing_duckdb_1_5_5(tmp_path):
    """Why `window_delete` finds its rows by the id's text with something
    appended: a committed DELETE whose predicate the lost index serves finds
    no row, reports success and leaves them — a bare CAST of a VARCHAR
    column is folded into exactly that — where the same rows found by a
    scan must come out of the index too, and that is DuckDB's FATAL.
    Kills: "the window predicate is `col = ?`" and "…a bare CAST"."""
    duckdb = pytest.importorskip("duckdb")
    db = str(tmp_path / "killed.duckdb")
    _killed_copy(tmp_path, duckdb, kill=True).close()
    con = duckdb.connect(db)
    try:
        assert con.execute("DELETE FROM t WHERE s = 'late3'").fetchone()[0] == 0
        assert con.execute("SELECT count_if(s = 'late3') FROM t").fetchone()[0] == 1
    finally:
        con.close()
    import subprocess
    import sys

    for col, value in (("s", "late3"), ("k", "1")):
        proc = subprocess.run([sys.executable, "-c", probe._DELETE_ONE, db, "t",
                               probe._WINDOW_WHERE.format(col=col), value, str(REPO)],
                              capture_output=True, text=True)
        result = json.loads(proc.stdout.strip().splitlines()[-1])
        assert result.get("fatal", "").startswith(
            "FATAL Error: Invalid Input Error: Failed to delete all rows from index"), result


def test_the_ends_delete_costs_no_index_of_its_own_duckdb_1_5_5(tmp_path, monkeypatch):
    """`window_delete` behind a stop that left a WAL — a grace that ran out
    after P3's kill — asks one table per process, and each opens the copy
    through the product's guard (`open_read_write`), as the product's next
    start would. So the first process's close is not DuckDB's lossy
    checkpoint of the next table's index: every DELETE comes clean, and a
    loss D1 reports is one the product left. A bare read-write connect there
    replayed the WAL and closed lossily in the first process, the second
    table's DELETE was the FATAL, and D1 reported its own loss as the kill's.
    Kills: "_DELETE_ONE opens the copy with a bare duckdb.connect"."""
    duckdb = pytest.importorskip("duckdb")
    import subprocess
    import sys

    monkeypatch.setattr(probe, "APP_DIR", str(REPO))
    db = str(tmp_path / "wal.duckdb")
    con = duckdb.connect(db)
    for table, index in (("c", "CREATE INDEX i_cab ON c(a, b)"), ("s", "CREATE INDEX i_sa ON s(a)")):
        con.execute(f"CREATE TABLE {table} (id BIGINT PRIMARY KEY, a BIGINT, b VARCHAR)")
        con.execute(index)
        con.execute(f"INSERT INTO {table} SELECT r, r % 5, 'early' || r FROM range(1, 3001) x(r)")
    con.close()
    assert subprocess.run([sys.executable, "-c", _KILLED_TWO, db]).returncode == -9
    assert (tmp_path / "wal.duckdb.wal").exists()
    monkeypatch.setattr(probe, "_window_targets",
                        lambda run_id, order_id: [("c", "id", 100001), ("s", "id", 100001)])
    done = probe.window_delete(db, None, 100001)
    assert [(d["table"], d.get("scanned"), d.get("deleted"), d.get("left"))
            for d in done["deletes"]] == [("c", 1, 1, 0), ("s", 1, 1, 0)], done
    assert not any("fatal" in d or "error" in d for d in done["deletes"]), done


_WINDOW_WRITER = """
import os, signal, sys
import duckdb
con = duckdb.connect(sys.argv[1])
rid, oid = int(sys.argv[2]), int(sys.argv[3])
con.execute("INSERT INTO data_quality_runs (run_id, started_at, ended_at, as_of, window_start, "
            "window_end, layer, status) VALUES (?, now(), now(), now(), current_date, "
            "current_date, 'integrity', 'WARN')", [rid])
con.execute("INSERT INTO data_quality_issues VALUES "
            "(?, 'pg_silver_missing_rows', 'silver.orders', 'WARN', 1, '[5]', NULL), "
            "(?, 'pg_twin_pairing', 'silver.orders', 'INFO', 1, '[]', '{}')", [rid, rid])
# No buyer, as the fixture: no index on buyer_id holds the order.
con.execute("INSERT INTO orders VALUES (?, 1, 9, TIMESTAMP '2026-10-01 10:00:00', NULL)", [oid])
con.execute("INSERT INTO order_products VALUES (?, ?, 77)", [oid * 1000 + 1, oid])
if sys.argv[4] == "kill":
    os.kill(os.getpid(), signal.SIGKILL)
con.close()
"""


def _window_copy(tmp_path, duckdb, *, kill):
    """A copy with production's DQ journal (its own migration) and an order
    table whose two indexes are composite — production's
    `idx_orders_source_date` and `idx_orders_buyer_date` — with history
    checkpointed and then D1's window — one run, one order with a line and,
    as the fixture, no buyer — written by a process SIGKILLed before it
    closed (or closing cleanly), and a restart that touched nothing and
    closed: F5w and F6, the kill, F7."""
    import subprocess
    import sys

    from core import migrations

    db = str(tmp_path / f"window-{kill}.duckdb")
    con = duckdb.connect(db)

    class _Store:
        _connection = con

    migrations._m0023_data_quality_tables(_Store())
    con.execute("CREATE TABLE orders (id INTEGER PRIMARY KEY, source_id INTEGER, "
                "status_id INTEGER, ordered_at TIMESTAMP, buyer_id INTEGER)")
    con.execute("CREATE INDEX idx_orders_source_date ON orders(source_id, ordered_at)")
    con.execute("CREATE INDEX idx_orders_buyer_date ON orders(buyer_id, ordered_at)")
    con.execute("CREATE TABLE order_products (id BIGINT PRIMARY KEY, order_id INTEGER, "
                "product_id INTEGER)")
    con.execute("CREATE INDEX idx_order_products_order ON order_products(order_id)")
    con.execute("CREATE TABLE sync_metadata (key VARCHAR, value VARCHAR, updated_at TIMESTAMP)")
    con.execute("CREATE TABLE warehouse_refreshes (id INTEGER, trigger VARCHAR, "
                "validation_passed BOOLEAN, refreshed_at TIMESTAMP)")
    for name in ("silver_orders", "silver_order_utm"):
        con.execute(f"CREATE TABLE {name} (id INTEGER)")
    con.execute("INSERT INTO data_quality_runs (run_id, started_at, ended_at, as_of, window_start, "
                "window_end, layer, status) SELECT r, now(), now(), now(), current_date, "
                "current_date, CASE WHEN r % 2 = 0 THEN 'integrity' ELSE 'mirror_landing' END, "
                "'WARN' FROM range(1, 31) x(r)")
    con.execute("INSERT INTO data_quality_issues SELECT r, 'pg_twin_pairing', 'silver.orders', "
                "'INFO', 1, '[]', '{}' FROM range(1, 31) x(r)")
    con.execute("INSERT INTO orders SELECT r, 1 + r % 3, 1 + r % 4, "
                "TIMESTAMP '2026-01-01' + to_minutes(r), 1 + r % 50 FROM range(1, 3001) x(r)")
    con.execute("INSERT INTO order_products SELECT r * 1000 + 1, r, 7 FROM range(1, 3001) x(r)")
    con.close()
    proc = subprocess.run([sys.executable, "-c", _WINDOW_WRITER, db, str(WIN), str(FIX),
                           "kill" if kill else "close"])
    assert proc.returncode == (-9 if kill else 0)
    duckdb.connect(db).close()
    return db


@pytest.mark.parametrize("kill", [False, True])
def test_the_window_is_asked_of_duckdb_itself(tmp_path, monkeypatch, kill):
    """`window_rows` after the kill and `window_delete` at the end, run on
    DuckDB 1.5.5 itself, judged as D1 judges them. On a clean file every
    index gives every window row up; across a SIGKILL the product's reader
    is blind to the run, and the DELETE of each table that holds the
    window's rows is DuckDB's FATAL — the order's too, whose indexes are
    composite and which no read can ask. The order has no buyer, so
    `idx_orders_buyer_date` holds none of it and is not asked: the PASS
    names it instead of counting it.
    Kills: "window_delete finds its rows through an index" (a DELETE that
    found nothing would report ok, and the FATAL asserted here would not
    come), "window_rows counts a NULL-keyed row as held", and the script's
    flags meeting nothing in the probe."""
    duckdb = pytest.importorskip("duckdb")
    monkeypatch.setattr(probe, "APP_DIR", str(REPO))
    db = _window_copy(tmp_path, duckdb, kill=kill)
    assert probe.main(["duckdb-facts", "--db", db, "--window-run", str(WIN),
                       "--window-order", str(FIX)]) == 0
    facts = probe.duckdb_facts(db, None, [], window_run=WIN, window_order=FIX)
    window = facts["window"]
    assert {t: v["rows"] for t, v in window["tables"].items()} == {
        "data_quality_issues": 2, "data_quality_diffs": 0, "data_quality_runs": 1,
        "order_products": 1, "orders": 1}
    assert window["order_status"] == V4 and facts["max_dq_run_id"] == WIN
    assert window["tables"]["orders"]["indexes"] == [
        {"index": "idx_orders_buyer_date", "columns": 2, "held": 0},
        {"index": "idx_orders_source_date", "columns": 2, "held": 1}]
    done = probe.window_delete(db, WIN, FIX)
    by_table = {d["table"]: d for d in done["deletes"]}
    ev = d1_ev(post={**DUCK1, "indexes": SWEEP, "window": window}, delete=done)
    verdict, detail = probe.judge_d1(ev)
    if not kill:
        assert window["run"]["reader"] == {"names": ["pg_silver_missing_rows", "pg_twin_pairing"]}
        assert all(d.get("deleted") == d.get("scanned") and d.get("left") == 0
                   for d in done["deletes"]), done
        assert verdict == PASS, detail
        assert "1 composite among them" in detail
        assert "a NULL key in each [orders.idx_orders_buyer_date]" in detail
    else:
        assert window["run"]["reader"] == {"names": []}
        for table in ("data_quality_issues", "data_quality_runs", "order_products", "orders"):
            assert "Failed to delete all rows from index" in by_table[table].get("fatal", ""), (
                table, by_table[table])
        assert by_table["data_quality_diffs"] == {
            "table": "data_quality_diffs", "scanned": 0, "deleted": 0, "left": 0}
        assert verdict == FAIL and "from orders is DuckDB's FATAL" in detail
        assert "fetch_run_issues returns 0 of the 2 findings" in detail
        assert detail.startswith(probe.KNOWN_DEFECT), detail


_KILLED_PAIR = """
import os, signal, sys
import duckdb
con = duckdb.connect(sys.argv[1])
con.execute("INSERT INTO orders VALUES (900401, NULL, NULL, 9, TIMESTAMP '2026-10-07 12:46:00')")
con.execute("INSERT INTO orders VALUES (900402, 7, NULL, 9, TIMESTAMP '2026-10-07 12:46:00')")
con.execute("INSERT INTO orders VALUES (900403, 7, 3, 9, NULL)")
os.kill(os.getpid(), signal.SIGKILL)
"""


@pytest.mark.parametrize("cols", [("buyer_id",), ("buyer_id", "ordered_at"),
                                  ("manager_id", "ordered_at"), ("status_id", "ordered_at"),
                                  ("ordered_at", "buyer_id")])
def test_an_index_holds_no_row_with_a_null_key_duckdb_1_5_5(tmp_path, cols):
    """What `held` stands on, on DuckDB itself: after a SIGKILL costs an
    index every entry the WAL held, deleting a row by a scan is the FATAL
    exactly when the index held it — every key column set — and a clean
    delete otherwise, whatever the index lost. So the end's DELETE asks an
    index only of the rows it holds, and `window_rows` must say which: the
    fixture order, with no buyer and no manager, is in `idx_orders_status_date`
    and `idx_orders_source_date`, and in neither the buyer nor the manager
    ones (the reviewer's 10-07 reading: 2 of the 4 composite indexes asked).
    If an upgrade changes which rows an index holds, this fails first.
    Kills: "held counts the window's rows whatever their keys"."""
    duckdb = pytest.importorskip("duckdb")
    import subprocess
    import sys

    db = str(tmp_path / "null.duckdb")
    con = duckdb.connect(db)
    con.execute("CREATE TABLE orders (id INTEGER, buyer_id INTEGER, manager_id INTEGER, "
                "status_id INTEGER, ordered_at TIMESTAMP)")
    con.execute("CREATE TABLE order_products (id BIGINT, order_id INTEGER)")
    con.execute(f"CREATE INDEX i ON orders({', '.join(cols)})")
    con.execute("INSERT INTO orders SELECT r, 1 + r % 50, 1 + r % 9, 1 + r % 4, "
                "TIMESTAMP '2026-01-01' + to_minutes(r) FROM range(1, 3001) x(r)")
    con.close()
    assert subprocess.run([sys.executable, "-c", _KILLED_PAIR, db]).returncode == -9
    duckdb.connect(db).close()
    rows = {900401: {"buyer_id": None, "manager_id": None, "status_id": 9, "ordered_at": 1},
            900402: {"buyer_id": 7, "manager_id": None, "status_id": 9, "ordered_at": 1},
            900403: {"buyer_id": 7, "manager_id": 3, "status_id": 9, "ordered_at": None}}
    for oid, row in rows.items():
        ro = duckdb.connect(db, read_only=True)
        try:
            window = probe.window_rows(ro, None, oid)
        finally:
            ro.close()
        held = window["tables"]["orders"]["indexes"][0]["held"]
        assert held == (1 if all(row[c] is not None for c in cols) else 0), (oid, window)
        proc = subprocess.run([sys.executable, "-c", probe._DELETE_ONE, db, "orders",
                               probe._WINDOW_WHERE.format(col="id"), str(oid), str(REPO)],
                              capture_output=True, text=True)
        result = json.loads(proc.stdout.strip().splitlines()[-1])
        assert ("fatal" in result) is (held == 1), (oid, cols, result)
        if not held:
            assert (result["scanned"], result["deleted"], result["left"]) == (1, 1, 0), result


def test_held_is_none_for_a_key_that_is_no_column():
    """An expression index names no column `held` can test for NULL, so its
    holding is not read rather than guessed — and D1 then cannot count it."""
    duckdb = pytest.importorskip("duckdb")
    con = duckdb.connect()
    con.execute("CREATE TABLE orders (id INTEGER, status_id INTEGER, note VARCHAR)")
    con.execute("CREATE TABLE order_products (id BIGINT, order_id INTEGER)")
    con.execute("CREATE INDEX i_expr ON orders((lower(note)))")
    con.execute("CREATE INDEX i_status ON orders(status_id)")
    con.execute("INSERT INTO orders VALUES (5, 9, 'x')")
    window = probe.window_rows(con, None, 5)
    assert window["tables"]["orders"]["indexes"] == [
        {"index": "i_expr", "columns": 1, "held": None},
        {"index": "i_status", "columns": 1, "held": 1}]


def test_the_sweep_says_why_it_could_not_ask():
    class _Broken:
        def execute(self, *_a, **_k):
            raise RuntimeError("closed")

    assert probe.index_sweep(_Broken()) == {"error": "RuntimeError", "swept": [], "skipped": []}


def test_index_expressions_are_read_as_column_names():
    assert probe._index_columns("[run_id]") == ["run_id"]
    assert probe._index_columns("['\"value\"']") == ["value"]
    assert probe._index_columns("[buyer_id, ordered_at]") == ["buyer_id", "ordered_at"]
    assert probe._index_columns(None) == []


# ─── P8 ──────────────────────────────────────────────────────────────────────

def p8_ev(**over):
    ev = {"flipped": True, "oom": False, "expect_reclassify": True,
          "start": snap("duckdb", value=None, held=True, reclassify=True,
                        jobs=("warehouse_refresh",)),
          "final": snap("duckdb", value=None, held=False, reclassify=False, stood=[],
                        jobs=("warehouse_refresh",), last_trigger="dirty_flag",
                        validation_passed=True),
          "log": {"way_back": 1, "emptied": 1, "full_tick": 1, "released": 1},
          "dk": {**DUCK0, "writer": {"writer": "duckdb", "since": "2026-10-01T12:00:00+00:00"},
                 "last_refresh": {"trigger": "dirty_flag", "validation_passed": True,
                                  "refreshed_at": "2026-10-01 12:04:00"}},
          "r0": "2026-10-01 07:58:00.123456",
          "pre_stop": stop_state("graceful"), "stop": stop_state("graceful")}
    ev.update(over)
    return ev


def test_p8_passes_on_a_held_then_released_way_back():
    verdict, detail = probe.judge_p8(p8_ev())
    assert verdict == PASS, detail


@pytest.mark.parametrize("change", [
    {"start": snap("duckdb", value=None, held=False, jobs=("warehouse_refresh",))},
    {"log": {"way_back": 1, "emptied": 1, "full_tick": 0, "released": 1}},
    {"log": {"way_back": 1, "emptied": 0, "full_tick": 1, "released": 1}},
    {"final": snap("duckdb", value=None, held=True, jobs=("warehouse_refresh",),
                   last_trigger="dirty_flag", validation_passed=True)},
    {"dk": {**DUCK0, "writer": {"writer": "postgres"}}},
    {"dk": {**DUCK0, "writer": {"writer": "duckdb"}, "silver_order_utm": 0,
            "last_refresh": {"trigger": "dirty_flag", "validation_passed": True,
                             "refreshed_at": "2026-10-01 12:04:00"}}},
    {"dk": {**DUCK0, "writer": {"writer": "duckdb"},
            "last_refresh": {"trigger": "dirty_flag", "validation_passed": True,
                             "refreshed_at": "2026-10-01 07:00:00"}}},
])
def test_p8_fails_when_the_way_back_is_not_held_or_not_released(change):
    assert probe.judge_p8(p8_ev(**change))[0] == FAIL


def test_p8_out_of_memory_is_unknown_not_fail():
    assert probe.judge_p8(p8_ev(oom=True))[0] == UNKNOWN


@pytest.mark.parametrize("which", ["pre_stop", "stop"])
@pytest.mark.parametrize("state, why", [
    (stop_state("grace_expired"), "Docker killed it 11 s into a 10 s grace (exit 137)"),
    (stop_state("oom"), "the kernel's OOM killer stopped it"),
    (stop_state("running"), "it never stopped"),
    (None, "the container's state after it was not read"),
])
def test_p8_says_a_stop_around_the_way_back_that_was_no_deploys(which, state, why):
    """The stop before the way back and the way back's own were the two
    stops of reh-web no file recorded: one that outran production's grace
    was a SIGKILL the way back then started from, and nothing said so. Each
    is now read from the container's state, as P6 reads its restarts: said,
    and UNKNOWN until somebody has read why web did not stop in time; a
    FAIL stays a FAIL.
    Kills: "P8 never reads the stop the way back starts from", "…its own
    last stop"."""
    verdict, detail = probe.judge_p8(p8_ev(**{which: state}))
    assert verdict == UNKNOWN, detail
    what = ("the stop the way back started from" if which == "pre_stop"
            else "the way back's own stop")
    assert f"{what}: " in detail and why in detail, detail
    broken = p8_ev(**{which: state}, log={"way_back": 1, "emptied": 1, "full_tick": 0,
                                          "released": 1})
    assert probe.judge_p8(broken)[0] == FAIL


def test_p8_without_the_reparse_does_not_demand_the_reclassify_branch():
    ev = p8_ev(expect_reclassify=False,
               start=snap("duckdb", value=None, held=True, reclassify=False,
                          jobs=("warehouse_refresh",)),
               log={"way_back": 1, "emptied": 0, "full_tick": 1, "released": 1})
    assert probe.judge_p8(ev)[0] == PASS


# ─── K0, Z0 ──────────────────────────────────────────────────────────────────

def test_k0():
    good = {"internal": True, "base_url": "http://reh-keycrm:8080/v1", "stub_requests": 214,
            "keycrm_app_lines": 0}
    assert probe.judge_k0(good)[0] == PASS
    assert probe.judge_k0({**good, "keycrm_app_lines": 1})[0] == FAIL
    assert probe.judge_k0({**good, "internal": False})[0] == FAIL
    assert probe.judge_k0({**good, "base_url": "https://openapi.keycrm.app/v1"})[0] == FAIL
    assert probe.judge_k0({**good, "stub_requests": 0})[0] == FAIL
    assert probe.judge_k0({})[0] == UNKNOWN


WEB_UP = "c1 keycrm-web 2026-10-01T09:00:00.1Z 0 false"
PG_UP = "c2 ks-postgres 2026-09-30T02:00:00.5Z 0 false"


def test_z0_unchanged_is_pass():
    assert probe.judge_z0({"before": [WEB_UP, PG_UP], "after": [PG_UP, WEB_UP],
                           "min_mem_available_mib": 2100})[0] == PASS
    # The id-and-name form an older run wrote still reads.
    assert probe.judge_z0({"before": ["a x"], "after": ["a x"],
                           "min_mem_available_mib": None})[0] == PASS


def test_z0_a_live_container_restarted_in_place_is_fail():
    """A restart policy restarts an OOM-killed web under the same id and name,
    and resets OOMKilled on the way up: only StartedAt and RestartCount say
    it happened."""
    restarted = "c1 keycrm-web 2026-10-01T10:12:44.9Z 1 false"
    verdict, detail = probe.judge_z0({"before": [WEB_UP, PG_UP], "after": [restarted, PG_UP],
                                      "min_mem_available_mib": 900})
    assert verdict == FAIL and "keycrm-web" in detail and "ks-postgres" not in detail
    stopped_oom = "c1 keycrm-web 2026-10-01T09:00:00.1Z 0 true"
    verdict, detail = probe.judge_z0({"before": [WEB_UP], "after": [stopped_oom],
                                      "min_mem_available_mib": 900})
    assert verdict == FAIL and "OOM-killed" in detail


def test_z0_a_container_gone_or_new_is_unknown():
    assert probe.judge_z0({"before": [WEB_UP, PG_UP], "after": [PG_UP],
                           "min_mem_available_mib": None})[0] == UNKNOWN
    assert probe.judge_z0({"before": ["a x"], "after": ["b y"],
                           "min_mem_available_mib": None})[0] == UNKNOWN
    assert probe.judge_z0({"before": None, "after": [WEB_UP]})[0] == UNKNOWN


# ─── assemble ────────────────────────────────────────────────────────────────

def test_an_empty_evidence_directory_passes_nothing(tmp_path):
    rows = probe.judge_all(tmp_path, floor_s=120, retired=RETIRED, standalone_twins=TWINS)
    assert [label for label, _v, _d in rows] == [label for _k, label in probe.LABELS]
    assert all(verdict != PASS for _l, verdict, _d in rows), rows


def test_assemble_reads_frozen_samples_from_the_flipped_snapshots(tmp_path):
    (tmp_path / "f1_snapshot.json").write_text(json.dumps(snap()))
    (tmp_path / "flip_snap_f2_1.json").write_text(json.dumps(snap(frozen="A")))
    (tmp_path / "flip_snap_r1.json").write_text(json.dumps(snap(frozen="B")))
    (tmp_path / "flip_snap_r2.json").write_text(json.dumps({**snap(frozen="C"), "health_code": 0}))
    ev = probe.assemble(tmp_path)
    assert ev["P2"]["frozen_samples"] == ["A", "B"]
    assert ev["P2"]["flipped"] is True


def test_assemble_derives_the_reclassify_branch_from_the_recorded_writer(tmp_path):
    (tmp_path / "d1.json").write_text(json.dumps(DUCK1))
    assert probe.assemble(tmp_path)["P8"]["expect_reclassify"] is True
    (tmp_path / "d1.json").write_text(json.dumps({**DUCK1, "writer": {"writer": "postgres"}}))
    assert probe.assemble(tmp_path)["P8"]["expect_reclassify"] is False
    # F7's read lost: F5s's, also after F3, says it instead.
    (tmp_path / "d1.json").write_text("")
    (tmp_path / "d_pre.json").write_text(json.dumps(DUCK1))
    assert probe.assemble(tmp_path)["P8"]["expect_reclassify"] is True
    # Neither read: not known, which P8 cannot pass on.
    (tmp_path / "d_pre.json").unlink()
    assert probe.assemble(tmp_path)["P8"]["expect_reclassify"] is None
    verdict, detail = probe.judge_p8(p8_ev(expect_reclassify=None))
    assert verdict == UNKNOWN and "reclassify branch is not known" in detail


def test_a_lost_read_at_f7_costs_no_row_a_false_verdict(tmp_path):
    """The reviewer's case: the probe call at F7 died (exit 2, swallowed by
    `|| true`) and left `d1.json` empty. P1 then failed the product for a
    writer record nobody read, P6 failed it for a gate file nobody read, and
    P8 quietly dropped the reclassify branch it read out of the same file.
    A read that did not happen is UNKNOWN where it decides a row, and F5s's
    read — also after the flip and after F3 — answers where it can.
    Kills: "P1 reads F7's copy alone", "P8's branch is read from F7's copy
    alone", "an unread gate file is a FAIL"."""
    for name, body in (("f1_snapshot.json", snap()), ("seed_gate.json", {"seeded": True}),
                       ("f1_log.json", {"not_registered": 1, "holds": 1, "recorded": 1}),
                       ("f1_pg.json", {"resolved_events": 1}),
                       ("d0.json", DUCK0), ("d_pre.json", {**DUCK1, "indexes": SWEEP}),
                       ("p3.json", p3_ev()),
                       ("p6_restart1.json", restart()), ("p6_restart2.json", restart("kill")),
                       ("p6_restart3.json", restart())):
        (tmp_path / name).write_text(json.dumps(body))
    (tmp_path / "d1.json").write_text("")
    rows = {label.split()[0]: (v, d) for label, v, d in probe.judge_all(
        tmp_path, floor_s=60, retired=RETIRED, standalone_twins=TWINS)}
    assert rows["P1"][0] == PASS, rows["P1"]
    assert rows["P6"] == (UNKNOWN, "the gate file was not read after the restarts")
    assert rows["D1"][0] == UNKNOWN and rows["P2"][0] == UNKNOWN
    assert probe.assemble(tmp_path)["P8"]["expect_reclassify"] is True


def test_assemble_reads_the_jobs_list_for_the_mirror_run_and_the_live_view(tmp_path):
    (tmp_path / "f4_runs.json").write_text(json.dumps({"integrity": 31, "mirror_landing": 32}))
    (tmp_path / "d_pre.json").write_text(json.dumps({**DUCK1, "runs": {
        "31": integrity_run(), "32": mirror_run()}}))
    (tmp_path / "mirror_complete.log").write_text("\n".join([
        "x - Mirror reconciliation complete | {'run_id': 9, 'checks_run': ['reconcile_gold']}",
        "x - Mirror reconciliation complete | {'run_id': 32, 'error': None, "
        "'findings': ['WARN:gold_rollup_mismatch:gold.daily_revenue:1'], "
        "'checks_run': ['reconcile_mirror', 'pg_gold_internal_check']}",
    ]))
    (tmp_path / "f4_integrity_live.json").write_text(json.dumps(
        {"run_id": 31, "issue_names": ["pg_silver_missing_rows", "pg_twin_pairing"]}))
    ev = probe.assemble(tmp_path)["P5"]
    assert ev["checks_run"] == ["reconcile_mirror", "pg_gold_internal_check"]
    assert ev["integrity"]["live_names"] == ["pg_silver_missing_rows", "pg_twin_pairing"]
    assert "live_names" not in ev["mirror"]


def test_p4_and_p5_read_their_runs_before_the_kill_and_d1_after_it(tmp_path):
    """The runs F4 wrote are judged as the stop before F6's kill read them;
    what the kill left of them is D1's to report, never P4's or P5's to pass
    or fail on — and a run read only after the kill is no run for them."""
    (tmp_path / "f4_runs.json").write_text(json.dumps({"integrity": 31, "mirror_landing": 32}))
    (tmp_path / "p3.json").write_text(json.dumps({"seen_blocked": True, "stop": stop_state("kill")}))
    blind = {"31": integrity_run(reader={"names": []}), "32": mirror_run(reader={"names": []})}
    (tmp_path / "d1.json").write_text(json.dumps({**DUCK1, "runs": blind}))
    ev = probe.assemble(tmp_path)
    assert ev["P4"]["a"]["run"] is None
    assert ev["P5"]["mirror"] is None and ev["P5"]["integrity"] is None
    assert ev["D1"]["post"]["runs"] == blind and ev["D1"]["pre"] is None
    whole = {"31": integrity_run(), "32": mirror_run()}
    (tmp_path / "d_pre.json").write_text(json.dumps({**DUCK1, "runs": whole}))
    ev = probe.assemble(tmp_path)
    assert ev["P4"]["a"]["run"] == whole["31"]
    assert ev["P5"]["integrity"] == whole["31"] and ev["P5"]["mirror"] == whole["32"]
    assert ev["D1"]["pre"]["runs"] == whole and ev["D1"]["seen_blocked"] is True
    assert probe.stop_kind(ev["D1"]["kill_stop"]) == "kill"


def test_assemble_hands_d1_the_kill_the_window_and_the_stops(tmp_path):
    """D1's kill is the one p3.json's container state shows; its window and
    its stops come from the files the script writes for them."""
    files = {"p3.json": {"seen_blocked": True, "stop": stop_state("oom")},
             "d1_window.json": {"run_id": WIN, "order_id": FIX, "order_status": V4},
             "d1_delete.json": deletes(), "f7_stop_state.json": stop_state("graceful"),
             "p0_stop_state.json": stop_state("grace_expired")}
    for name, body in files.items():
        (tmp_path / name).write_text(json.dumps(body))
    ev = probe.assemble(tmp_path)["D1"]
    assert ev["window_ids"] == files["d1_window.json"] and ev["delete"] == files["d1_delete.json"]
    assert ev["f7_stop"] == files["f7_stop_state.json"]
    assert ev["p0_stop"] == files["p0_stop_state.json"]
    # P8's two stops, by the files the script writes around the way back.
    (tmp_path / "b_pre_stop_state.json").write_text(json.dumps(stop_state("grace_expired")))
    (tmp_path / "b_stop_state.json").write_text(json.dumps(stop_state("graceful")))
    p8 = probe.assemble(tmp_path)["P8"]
    assert probe.stop_kind(p8["pre_stop"]) == "grace_expired"
    assert probe.stop_kind(p8["stop"]) == "graceful"
    # And D1 reads the same two: they stand between F7's read and its DELETE.
    d1 = probe.assemble(tmp_path)["D1"]
    assert (d1["b_pre_stop"], d1["b_stop"]) == (p8["pre_stop"], p8["stop"])
    assert probe.judge_d1({**ev, "flipped": True}) == (
        UNKNOWN, "no kill landed inside the derivation (see P3)")


def test_the_copys_runs_are_read_by_the_products_reader_too(tmp_path, monkeypatch):
    """`duckdb_facts` runs `fetch_run_issues` beside the scan, on production's
    own DDL and writer: where DuckDB answers both honestly they agree, and
    where the reader comes back empty the judge fails."""
    duckdb = pytest.importorskip("duckdb")
    from datetime import date, datetime, timezone

    from core import data_quality, migrations
    from core.data_quality import IntegrityIssue, Severity, persist_run

    monkeypatch.setattr(probe, "APP_DIR", str(REPO))
    db = str(tmp_path / "copy.duckdb")
    con = duckdb.connect(db)

    class _Store:
        _connection = con

    migrations._m0023_data_quality_tables(_Store())
    con.execute("CREATE TABLE sync_metadata (key VARCHAR, value VARCHAR, updated_at TIMESTAMP)")
    con.execute("CREATE TABLE warehouse_refreshes (id INTEGER, trigger VARCHAR, "
                "validation_passed BOOLEAN, refreshed_at TIMESTAMP)")
    for name in ("orders", "silver_orders", "silver_order_utm"):
        con.execute(f"CREATE TABLE {name} (id INTEGER)")
    now = datetime.now(timezone.utc)
    run_id = persist_run(
        con, started_at=now, ended_at=now, as_of=now, window_start=date.today(),
        window_end=date.today(), layer="integrity", discrepancies=[],
        issues=[IntegrityIssue("pg_silver_missing_rows", "silver.orders", Severity.WARN, 1, (5,)),
                IntegrityIssue("pg_twin_pairing", "silver.orders", Severity.INFO, 1, (), "{}")])
    con.close()

    facts = probe.duckdb_facts(db, None, [run_id])
    run = facts["runs"][str(run_id)]
    assert run["reader"] == {"names": ["pg_silver_missing_rows", "pg_twin_pairing"]}
    assert probe.judge_reader(run) == (None, None)

    monkeypatch.setattr(data_quality, "fetch_run_issues", lambda conn, rid, limit=100: [])
    blind = probe.duckdb_facts(db, None, [run_id])["runs"][str(run_id)]
    assert blind["reader"] == {"names": []}
    assert probe.judge_reader(blind)[0].startswith("fetch_run_issues returns 0 of the 2")


def test_a_judge_that_raises_is_unknown(tmp_path, monkeypatch):
    monkeypatch.setattr(probe, "judge_k0", lambda ev: 1 / 0)
    rows = dict((label, verdict) for label, verdict, _d in probe.judge_all(
        tmp_path, floor_s=120, retired=RETIRED, standalone_twins=TWINS))
    assert rows["K0 KeyCRM never called"] == UNKNOWN


# ─── Pinned to the code it describes ─────────────────────────────────────────

def test_the_probe_constants_are_the_codes():
    from core import warehouse_cutover as wc
    from core.alerting import REGISTRY, is_condition
    from core.pg_warehouse_dq import GUARD_CONDITIONS

    assert set(probe.STOOD_DOWN) == set(wc.STOOD_DOWN_WHEN_POSTGRES)
    assert set(probe.RETIRED_GOLD) == set(wc.RETIRED_COMPARISON_CONDITIONS)
    assert wc.RESOLVE_NOTE.startswith(probe.RESOLVE_NOTE_HEAD)
    assert probe.SEEDED_KEY in REGISTRY and is_condition(probe.SEEDED_KEY)
    assert set(probe.STANDALONE_GROUPS) <= set(GUARD_CONDITIONS)
    assert set(probe.RETIRED_COMPARISONS) == set(wc.RETIRED_COMPARISONS)


def test_the_jobs_list_and_its_line_are_the_codes():
    import inspect

    from core.scheduler import BackgroundScheduler

    source = inspect.getsource(BackgroundScheduler._run_dq_mirror_landing)
    assert f'logger.info("{probe.MIRROR_COMPLETE}", extra=result)' in source
    assert '"checks_run": list(checks_run)' in source
    assert f'check("{probe.INTERNAL_CHECK}"' in source
    assert "grep -F 'Mirror reconciliation complete'" in SCRIPT.read_text()


def test_the_fixture_versions_are_known_statuses():
    from core.data_quality import KNOWN_STATUS_IDS

    statuses = [s for s, _g in probe.FIXTURE_VERSIONS.values()]
    assert set(statuses) <= set(KNOWN_STATUS_IDS)
    assert len(set(statuses)) == len(statuses), "every version must move the status"


def _code_text() -> str:
    parts = []
    for folder in ("core", "bot", "web"):
        for path in (REPO / folder).rglob("*.py"):
            parts.append(path.read_text(encoding="utf-8"))
    return "\n".join(parts)


def test_every_log_line_the_script_greps_is_one_the_code_writes():
    """A renamed log line would turn a rehearsal row UNKNOWN or FAIL on the
    host, an hour in. Read straight out of the script's greps."""
    script = SCRIPT.read_text(encoding="utf-8")
    patterns = set(re.findall(r"log_count [^']*'([^']+)'", script))
    patterns |= set(re.findall(r"grep -q?c?F (?:-- )?'([^']+)'", script))
    patterns = {p for p in patterns if not p.startswith("REH-KEYCRM")}
    assert len(patterns) >= 10, patterns
    code = _code_text()
    for pattern in sorted(patterns):
        literal = pattern.replace("(changed_ids=full)", "(changed_ids=")
        assert literal in code, f"the script greps for {pattern!r}, which nothing writes"


def test_the_stub_logs_the_line_k0_counts():
    stub = (REPO / "deploy" / "step13_rehearsal" / "keycrm_stub.py").read_text()
    assert 'print(f"REH-KEYCRM {self.command} {parts.path}?{shown}", flush=True)' in stub
    script = SCRIPT.read_text()
    assert "grep -c '^REH-KEYCRM '" in script
    assert "grep -cF 'REH-KEYCRM GET /v1/order?'" in script


def test_the_stub_never_writes_and_logs_no_contact_value():
    stub = (REPO / "deploy" / "step13_rehearsal" / "keycrm_stub.py").read_text()
    assert 'self._send(405' in stub
    assert "'<redacted>'" in stub


def test_the_session_is_minted_for_a_hardcoded_admin(monkeypatch):
    from core.permissions import ADMIN_USER_IDS

    monkeypatch.setattr(probe, "APP_DIR", str(REPO))
    assert probe._admin_id() in ADMIN_USER_IDS


def test_p5_bump_healed_before_the_run_looked_is_unknown():
    assert probe.judge_p5(p5_ev(derivations_after_bump=1), retired=RETIRED,
                          standalone_twins=TWINS)[0] == UNKNOWN


def test_p5_says_whether_clickhouse_compared():
    ev = p5_ev(mirror=mirror_run(mirror_run()["issues"] + [
        {"check_name": "ch_engines_gold_mismatch", "table_name": "gold.daily_revenue"}]))
    verdict, detail = probe.judge_p5(ev, retired=RETIRED, standalone_twins=TWINS)
    assert verdict == PASS and "ClickHouse caught the bump too" in detail


def test_issues_are_read_by_the_ids_text_not_a_pushed_down_equality():
    """DuckDB 1.5.5 answers `run_id = ?` with no rows, after a SIGKILL, for
    runs a scan returns. The scan is what the judges hold the product's
    reader against (`judge_reader`), so it must not ask the same question."""
    source = PROBE_PATH.read_text()
    assert "FROM data_quality_issues WHERE CAST(run_id AS VARCHAR) = ?" in source
    assert "data_quality_issues WHERE run_id = ?" not in source
