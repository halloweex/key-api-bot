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
    {"d1": None},
])
def test_p1_fails_on_each_broken_half(change):
    assert probe.judge_p1(p1_ev(**change))[0] == FAIL


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


def test_a_timestamp_is_compared_as_an_instant_across_renderings():
    assert probe._same_instant("2026-10-01 10:00:00+03", "2026-10-01T07:00:00+00:00") is True
    assert probe._same_instant("2026-10-01 10:00:00", "2026-10-01T10:00:00") is True
    assert probe._same_instant("2026-10-01 10:00:01", "2026-10-01T10:00:00") is False


# ─── P3 ──────────────────────────────────────────────────────────────────────

def p3_ev(**over):
    ev = {"flipped": True, "seen_blocked": True, "requested": 41, "built": 40,
          "silver_version": 4,
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


# ─── P4 ──────────────────────────────────────────────────────────────────────

def integrity_run(*, missing_ids=(900123,), extra=()):
    issues = [{"check_name": "pg_silver_missing_rows", "count": len(missing_ids),
               "sample_ids": list(missing_ids), "severity": "WARN"},
              {"check_name": "pg_twin_pairing", "count": 1, "sample_ids": [],
               "description": json.dumps({
                   "duckdb": {"silver_missing_rows": None, "silver_orphan_rows": None,
                              "headline_vs_line_items": None,
                              "goods_shipped_without_sale": None},
                   "standalone": {"pg_silver_missing_rows": len(missing_ids)}})}]
    return {"run_id": 31, "error_message": None, "issues": issues + list(extra)}


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


def p5_ev(**over):
    ev = {"flipped": True,
          "mirror": {"run_id": 32, "error_message": None,
                     "issues": [{"check_name": "gold_rollup_mismatch", "table_name": "gold.daily_revenue"},
                                {"check_name": "mirror_row_values", "table_name": "bronze.orders"}]},
          "integrity": integrity_run(),
          "stood_down": STOOD}
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
    ev = p5_ev()
    ev["mirror"] = {**ev["mirror"], "issues": mirror_issues}
    assert probe.judge_p5(ev, retired=RETIRED, standalone_twins=TWINS)[0] == FAIL


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

def restart(**over):
    r = {"snapshot": snap(), "resolved_events": 1, "recorded_lines": 1, "resolved_lines": 1}
    r.update(over)
    return r


def p6_ev(**over):
    ev = {"flipped": True, "resolved_at": T1, "restarts": [restart(), restart()],
          "gate_delivered": []}
    ev.update(over)
    return ev


def test_p6_passes_on_two_restarts_and_one_resolve():
    verdict, detail = probe.judge_p6(p6_ev())
    assert verdict == PASS, detail


@pytest.mark.parametrize("change", [
    {"restarts": [restart(), restart(resolved_events=2)]},
    {"restarts": [restart(resolved_lines=2), restart()]},
    {"restarts": [restart(), restart(snapshot=snap(writer={"writer": "postgres", "resolved": True,
                                                            "resolved_at": "2026-10-01T11:30:00+00:00"}))]},
    {"restarts": [restart(snapshot=snap("duckdb")), restart()]},
    {"gate_delivered": ["warehouse:validation_retrying"]},
])
def test_p6_fails_on_a_second_resolve_or_a_lost_flip(change):
    assert probe.judge_p6(p6_ev(**change))[0] == FAIL


def test_p6_is_unknown_with_one_restart():
    assert probe.judge_p6(p6_ev(restarts=[restart()]))[0] == UNKNOWN


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
          "r0": "2026-10-01 07:58:00.123456"}
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


def test_z0():
    assert probe.judge_z0({"before": ["a x"], "after": ["a x"],
                           "min_mem_available_mib": 2100})[0] == PASS
    assert probe.judge_z0({"before": ["a x"], "after": ["b y"],
                           "min_mem_available_mib": None})[0] == UNKNOWN


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
    ev = p5_ev()
    ev["mirror"] = {**ev["mirror"], "issues": ev["mirror"]["issues"] + [
        {"check_name": "ch_engines_gold_mismatch", "table_name": "gold.daily_revenue"}]}
    verdict, detail = probe.judge_p5(ev, retired=RETIRED, standalone_twins=TWINS)
    assert verdict == PASS and "ClickHouse caught the bump too" in detail


def test_issues_are_read_by_the_ids_text_not_a_pushed_down_equality():
    """DuckDB 1.5.5 answered `run_id = ?` with no rows on the first local
    rehearsal's copy while a scan returned them; the judges then read two DQ
    runs as having filed nothing."""
    source = PROBE_PATH.read_text()
    assert "FROM data_quality_issues WHERE CAST(run_id AS VARCHAR) = ?" in source
    assert "data_quality_issues WHERE run_id = ?" not in source
