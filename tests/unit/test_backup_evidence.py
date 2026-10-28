"""The backup evidence, read from the markers the deploy scripts leave —
chain 3's precondition until step 14's S14-00, the canary's
`backup_evidence_stale` since.

Each test names the mutation it exists to fail on.
"""
from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone

import pytest

from core import backup_evidence as ev

NOW = datetime(2026, 10, 1, 9, 0, tzinfo=timezone.utc)


def _write(root, name, at, **labels):
    text = at.strftime("%Y-%m-%dT%H:%M:%SZ") + "".join(
        f" {k}={v}" for k, v in labels.items())
    (root / name).write_text(text + "\n")


def _all_fresh(root):
    _write(root, ev.PITR_DRILL, NOW - timedelta(days=2), base="20260928T081000Z")
    _write(root, ev.REMOTE_DRILL, NOW - timedelta(days=2))
    _write(root, ev.OFFSITE, NOW - timedelta(hours=2))


def _keys(unmet):
    return sorted(line.split(":", 1)[0] for line in unmet)


def test_everything_fresh_is_met(tmp_path):
    _all_fresh(tmp_path)
    assert ev.unmet(NOW, tmp_path) == []


def test_nothing_on_disk_is_every_piece_missing(tmp_path):
    """Mutation: treat an absent marker as met."""
    assert _keys(ev.unmet(NOW, tmp_path)) == ["pg_offsite", "pitr_drill", "remote_restore"]


def test_one_provider_is_enough(tmp_path):
    """OD-01 (c) was cancelled by the owner on 2026-10-01: one Storage Box,
    risk accepted in writing. Mutation: put the second-provider check back —
    this host, with one fresh shipment, would read as unmet for ever."""
    _all_fresh(tmp_path)
    assert not [line for line in ev.unmet(NOW, tmp_path) if "provider" in line]


@pytest.mark.parametrize("name,key", [
    (ev.PITR_DRILL, "pitr_drill"),
    (ev.REMOTE_DRILL, "remote_restore"),
])
def test_a_drill_older_than_a_week_and_a_day_is_unmet(tmp_path, name, key):
    """Mutation: compare against hours where the limit is days, or drop the
    age check — a drill that passed once would vouch for ever."""
    _all_fresh(tmp_path)
    _write(tmp_path, name, NOW - timedelta(days=9))
    assert _keys(ev.unmet(NOW, tmp_path)) == [key]
    _write(tmp_path, name, NOW - timedelta(days=7, hours=23))
    assert ev.unmet(NOW, tmp_path) == []


def test_a_stale_shipment_is_unmet_at_offsite_checks_limit(tmp_path):
    """Mutation: a limit other than `deploy/offsite_check.sh`'s 36 hours."""
    _all_fresh(tmp_path)
    _write(tmp_path, ev.OFFSITE, NOW - timedelta(hours=37))
    assert _keys(ev.unmet(NOW, tmp_path)) == ["pg_offsite"]
    _write(tmp_path, ev.OFFSITE, NOW - timedelta(hours=35))
    assert ev.unmet(NOW, tmp_path) == []


def test_a_half_written_temporary_file_is_not_a_shipment(tmp_path):
    """The scripts write `<marker>.tmp` and rename it. Mutation: read the
    marker by prefix."""
    _all_fresh(tmp_path)
    (tmp_path / ev.OFFSITE).unlink()
    _write(tmp_path, ev.OFFSITE + ".tmp", NOW)
    assert _keys(ev.unmet(NOW, tmp_path)) == ["pg_offsite"]


def test_an_unreadable_line_falls_back_to_the_file_s_own_time(tmp_path):
    """The file exists only because the script got that far."""
    path = tmp_path / ev.PITR_DRILL
    path.write_text("garbage\n")
    old = (NOW - timedelta(days=20)).timestamp()
    os.utime(path, (old, old))
    marker = ev.read_markers(tmp_path)[0]
    assert marker is not None and marker.age_h(NOW) == pytest.approx(480)


def test_a_missing_directory_is_evidence_missing_not_a_raise(tmp_path):
    assert len(ev.unmet(NOW, tmp_path / "nope")) == 3


def test_the_published_block_carries_ages_only(tmp_path):
    """Public endpoint: no path, host or label leaves this module."""
    _all_fresh(tmp_path)
    out = ev.published(NOW, tmp_path)
    assert out == {"pitr_drill_age_h": 48.0, "remote_restore_age_h": 48.0,
                   "pg_offsite_age_h": 2.0}
    assert not any(isinstance(v, str) for v in out.values())
    assert ev.published(NOW, tmp_path / "nope") == {
        "pitr_drill_age_h": None, "remote_restore_age_h": None,
        "pg_offsite_age_h": None}


# ─── /api/health publishes it ─────────────────────────────────────────────────

def test_the_health_block_reads_the_data_directory(tmp_path, monkeypatch):
    """The route reads the same markers the precondition does, from `./data`.
    Mutation: return a constant, or read another directory."""
    from web.routes.api import health

    _write(tmp_path, ev.OFFSITE, datetime.now(timezone.utc) - timedelta(hours=3))
    monkeypatch.setattr(ev, "DB_DIR", tmp_path)
    block = health._backups()
    assert set(block) == {"pitr_drill_age_h", "remote_restore_age_h", "pg_offsite_age_h"}
    assert block["pitr_drill_age_h"] is None and block["remote_restore_age_h"] is None
    assert block["pg_offsite_age_h"] == pytest.approx(3, abs=0.2)


def test_an_unreadable_block_is_null_not_a_raise(monkeypatch):
    """`/api/health` must answer whatever the disk says. Mutation: drop the
    guard — the whole health check would 500 over one block."""
    from web.routes.api import health

    def boom(*a, **k):
        raise OSError("disk")

    monkeypatch.setattr(ev, "published", boom)
    assert health._backups() is None


def test_the_response_model_keeps_it():
    """`/api/health` has a response model, and a field it does not declare is
    dropped on the way out. Mutation: drop the field from `HealthResponse`."""
    from web.schemas import HealthResponse

    assert "backups" in HealthResponse.model_fields


def test_the_route_returns_it():
    """Read off the route's own return statement, not its prose: the dict
    `health_check` returns names `backups` and fills it from `_backups()`.
    Mutation: drop the key from the returned dict."""
    import ast
    import inspect

    from web.routes.api import health

    tree = ast.parse(inspect.getsource(health))
    fn = next(n for n in ast.walk(tree)
              if isinstance(n, ast.AsyncFunctionDef) and n.name == "health_check")
    returned = [n.value for n in ast.walk(fn)
                if isinstance(n, ast.Return) and isinstance(n.value, ast.Dict)]
    assert returned, "health_check returns no dict literal"
    pairs = {k.value: v for d in returned for k, v in zip(d.keys, d.values)
             if isinstance(k, ast.Constant)}
    value = pairs.get("backups")
    assert isinstance(value, ast.Call) and getattr(value.func, "id", None) == "_backups"


# ─── The canary judges it (step 14's S14-00) ─────────────────────────────────
#
# Until S14-00 the two drills were judged by chain 3's precondition alone,
# which paged `write_chain_precondition_unmet`; the precondition is gone, so
# the canary reads the published block itself, at this module's limits.

def _block(pitr=45.7, remote=46.0, offsite=22.7):
    """H0's ages: what production published on 2026-10-21."""
    return {"backups": {"pitr_drill_age_h": pitr, "remote_restore_age_h": remote,
                        "pg_offsite_age_h": offsite}}


def test_the_canary_is_quiet_on_productions_ages():
    """Mutation: a limit in days compared against hours, or any stale rule
    that fires on fresh evidence."""
    from bot import canary

    assert canary.check_backup_evidence(_block()) == []
    edge = _block(pitr=ev.DRILL_MAX_AGE_H, remote=ev.DRILL_MAX_AGE_H,
                  offsite=ev.OFFSITE_MAX_AGE_H)
    assert canary.check_backup_evidence(edge) == []


@pytest.mark.parametrize("field,age,script", [
    ("pitr", ev.DRILL_MAX_AGE_H + 1, "deploy/pg_pitr_drill.sh"),
    ("remote", ev.DRILL_MAX_AGE_H + 1, "deploy/pg_restore_drill.sh --from-remote"),
    ("offsite", ev.OFFSITE_MAX_AGE_H + 1, "deploy/pg_offsite.sh"),
])
def test_the_canary_warns_on_each_stale_marker_and_names_its_script(field, age, script):
    """Mutation: drop one of the three markers from `BACKUP_EVIDENCE`, or
    judge one at the other's limit (the shipment's 36 h is not a drill's
    eight days)."""
    from bot import canary

    ((key, message),) = canary.check_backup_evidence(_block(**{field: age}))
    assert key == "backup_evidence_stale"
    assert script in message and f"{age:.1f} h old" in message


def test_an_age_just_past_its_limit_reads_as_past_it():
    """192.01 h used to read "192 h old, limit 192 h", which looks fresh."""
    from bot import canary

    ((_, message),) = canary.check_backup_evidence(
        _block(remote=ev.DRILL_MAX_AGE_H + 0.01))
    assert f"over its {ev.DRILL_MAX_AGE_H} h limit" in message


def test_the_canary_warns_on_a_marker_that_never_passed():
    """A null age is a marker that does not exist: the script has never
    passed on this host. Mutation: read None as fresh."""
    from bot import canary

    ((key, message),) = canary.check_backup_evidence(_block(remote=None))
    assert key == "backup_evidence_stale" and "never passed" in message
    assert "pg_restore_drill.sh --from-remote" in message


def test_the_canary_judges_nothing_it_was_not_given():
    """No block, or one that is not an object — an older web, or `_backups()`
    answering None because the directory could not be read — says nothing
    about the drills. Mutation: page a missing block."""
    from bot import canary

    assert canary.check_backup_evidence({}) == []
    assert canary.check_backup_evidence(None) == []
    assert canary.check_backup_evidence({"backups": None}) == []
    assert canary.check_backup_evidence({"backups": {}}) == []


def test_a_probe_that_read_no_block_holds_the_page():
    """`resolve_group` clears every delivered key a probe did not report, and
    a probe that read no `backups` block cannot say the drills ran: the
    drills are weekly, so a blind probe announcing them resolved would page
    the same stale marker again as a new incident. Mutation: drop the key
    from `unjudged_keys`."""
    from bot import canary

    assert "backup_evidence_stale" in canary.unjudged_keys({})
    assert "backup_evidence_stale" in canary.unjudged_keys({"backups": None})
    assert "backup_evidence_stale" not in canary.unjudged_keys(_block())


@pytest.mark.asyncio
async def test_the_probe_warns_on_it():
    """Through `run_canary`, the probe the bot runs every 15 min, on an
    otherwise healthy payload. Mutation: drop `check_backup_evidence` from
    the probe — the function above would still answer, and a drill that
    quietly stopped running would be seen by nobody."""
    from unittest.mock import patch

    import httpx

    from bot import canary
    from tests.unit.test_canary import DASHBOARD, _healthy_payload, _mock_transport

    payload = {**_healthy_payload(), **_block(pitr=9 * 24)}
    future = datetime.now(timezone.utc) + timedelta(days=60)
    cert = {"notAfter": future.strftime("%b %d %H:%M:%S %Y GMT")}
    async with _mock_transport(lambda request: httpx.Response(200, json=payload)) as client:
        with patch.object(canary, "_fetch_peer_cert", return_value=cert):
            result = await canary.run_canary(DASHBOARD, client=client)
    assert "backup_evidence_stale" in result.failure_keys
    assert result.severity == "warn"
    assert "pg_pitr_drill.sh" in canary.format_alert(result, DASHBOARD)

    fresh = {**_healthy_payload(), **_block()}
    async with _mock_transport(lambda request: httpx.Response(200, json=fresh)) as client:
        with patch.object(canary, "_fetch_peer_cert", return_value=cert):
            result = await canary.run_canary(DASHBOARD, client=client)
    assert "backup_evidence_stale" not in result.failure_keys
