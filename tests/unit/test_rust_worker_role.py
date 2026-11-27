"""The core worker's own logins (deploy/rust/ks_rust_worker.sql and
deploy/rust/ks_rust_worker_clickhouse.sql), held to what the worker does.

In Postgres: SELECT on exactly the tables its statements read, INSERT and
UPDATE on exactly the ones they write, the schemas they live in, and the
limits every session carries. In ClickHouse: the shadow's five tables and
nothing live, until the switch's half is uncommented on its own yes. The K1
harness ships the shadow as both logins, so a grant they lack fails there; a
grant nobody needs fails here.
"""
from __future__ import annotations

import re
from pathlib import Path

import yaml

REPO = Path(__file__).resolve().parents[2]
SQL = (REPO / "deploy" / "rust" / "ks_rust_worker.sql").read_text()
STATEMENTS = re.sub(r"--[^\n]*", "", SQL)
CH_SQL = (REPO / "deploy" / "rust" / "ks_rust_worker_clickhouse.sql").read_text()
CH_STATEMENTS = re.sub(r"--[^\n]*", "", CH_SQL)
SOURCES = "\n".join(
    p.read_text()
    for crate in ("ks-core", "ks-worker")
    for p in sorted((REPO / "rust/crates" / crate / "src").rglob("*.rs")))


def _granted(kind: str) -> set:
    found = re.findall(
        rf"GRANT\s+{kind}\s+ON\s+(?:SCHEMA\s+)?(.*?)\s+TO\s+ks_rust_worker\s*;",
        STATEMENTS, re.S)
    return {name.strip() for clause in found for name in clause.split(",")}


def test_select_is_granted_on_what_the_worker_reads_and_the_state_it_updates():
    read = set(re.findall(r"\b(?:FROM|JOIN)\s+([a-z_]+\.[a-z_]+)", SOURCES))
    written = set(re.findall(r"\b(?:INTO|UPDATE)\s+([a-z_]+\.[a-z_]+)", SOURCES))
    assert read and written, "no statement found in the worker's sources"
    # ON CONFLICT … DO UPDATE reads the row it updates.
    assert _granted("SELECT") == read | written


def test_insert_and_update_on_exactly_what_it_writes():
    written = set(re.findall(r"\b(?:INTO|UPDATE)\s+([a-z_]+\.[a-z_]+)", SOURCES))
    assert _granted(r"INSERT,\s*UPDATE") == written == {"meta.mirror_state"}


def test_usage_on_exactly_their_schemas():
    assert _granted("USAGE") == {table.split(".")[0] for table in _granted("SELECT")}


def test_the_limits_every_session_carries():
    for setting in ("statement_timeout = '60s'", "lock_timeout = '5s'",
                    "idle_in_transaction_session_timeout = '60s'"):
        assert f"ALTER ROLE ks_rust_worker SET {setting};" in STATEMENTS, setting
    assert "CONNECTION LIMIT 4" in STATEMENTS


def test_nothing_but_reading_and_the_stamps():
    assert re.search(r"\bNOSUPERUSER\b.*\bNOCREATEDB\b.*\bNOCREATEROLE\b", STATEMENTS, re.S)
    assert not re.search(r"GRANT\s+(?!SELECT\b|USAGE\b|CONNECT\b|INSERT,\s*UPDATE\b)\w+",
                         STATEMENTS)
    assert "TEMPORARY" not in STATEMENTS and "DEFAULT PRIVILEGES" not in STATEMENTS
    assert "DELETE" not in STATEMENTS and "TRUNCATE" not in STATEMENTS


def test_the_password_is_never_written_down():
    assert "PASSWORD :'password'" in STATEMENTS
    assert not re.search(r"PASSWORD\s+'", STATEMENTS)
    assert CH_STATEMENTS.count("BY '@PASSWORD_SHA256@'") == 2


SHADOW_TABLES = {
    "silver.orders_rust", "silver.orders_rust_staging",
    "gold.daily_revenue_rust", "gold.daily_revenue_rust_staging",
    "history.order_versions_rust",
}


def _ch_grants(text: str) -> dict:
    return {table: {p.strip() for p in privileges.split(",")}
            for privileges, table in re.findall(
                r"^\s*GRANT\s+(.*?)\s+ON\s+([a-z_]+\.[a-z_]+)\s+TO\s+ks_rust_worker\s*;",
                text, re.M)}


def test_clickhouse_grants_the_shadow_and_nothing_live():
    granted = _ch_grants(CH_STATEMENTS)
    assert set(granted) == SHADOW_TABLES
    for table, privileges in granted.items():
        assert privileges <= {"SELECT", "INSERT", "CREATE TABLE", "DROP TABLE", "TRUNCATE"}, table
        # The archive is appended to, never swapped or truncated.
        if table.startswith("history."):
            assert privileges == {"SELECT", "INSERT", "CREATE TABLE"}
    assert not re.search(r"\bON\s+\*|\.\*\s+TO", CH_STATEMENTS)


def test_the_switchs_half_is_written_down_and_commented_out():
    """The live tables' grants exist only as comments: a yes of their own."""
    live = _ch_grants("\n".join(line.lstrip("- ") for line in CH_SQL.splitlines()
                                if line.startswith("-- GRANT")))
    assert {t.replace("_rust", "") for t in SHADOW_TABLES} == set(live)


def test_the_names_are_the_ones_the_worker_writes():
    """The shadow's names, as `Names::new(Target::Shadow, "history")` makes them."""
    worker = (REPO / "rust/crates/ks-worker/src/ch_sync.rs").read_text()
    assert 'SHADOW_SUFFIX: &str = "_rust"' in worker
    for name in ("silver.orders{suffix}", "silver.orders{suffix}_staging",
                 "gold.daily_revenue{suffix}", "gold.daily_revenue{suffix}_staging",
                 "{history_db}.order_versions{suffix}"):
        assert f'format!("{name}")' in worker, name


def test_the_service_reads_as_these_logins_and_nothing_starts_it():
    compose = yaml.safe_load((REPO / "docker-compose.yml").read_text())
    service = compose["services"]["rust-worker"]
    env = service["environment"]
    assert env["KS_PG_DSN"].startswith(
        "postgresql://ks_rust_worker:${KS_RUST_WORKER_PASSWORD:-}@postgres:5432/")
    assert env["KS_CH_USER"] == "ks_rust_worker"
    assert env["KS_CH_PASSWORD"] == "${KS_RUST_WORKER_CH_PASSWORD:-}"
    assert env["KS_WORKER_SHADOW"] == "${KS_WORKER_SHADOW:-}"
    assert service["profiles"] == ["rust-worker"]
    assert "ports" not in service
    assert set(service["networks"]) == {"default", "ks-data"}
    assert service["read_only"] is True and service["cap_drop"] == ["ALL"]


def test_a_deploy_updates_a_running_worker_and_starts_none():
    deploy = (REPO / ".github/workflows/deploy.yml").read_text()
    assert "target: worker" in deploy
    assert "keycrm-rust-worker:latest" in deploy
    guard = re.search(r"if \[ -n \"\$\(docker ps -q --filter name='\^ks-rust-worker\$'\)\" \]; then"
                      r"\s+docker compose --profile rust-worker pull rust-worker"
                      r"\s+docker compose --profile rust-worker up -d --no-deps rust-worker"
                      r"\s+fi", deploy)
    assert guard, "the Rust deploy must touch the worker only when it is already running"
    # Nowhere else does a deploy's script name the worker's profile.
    commands = [line for line in deploy.splitlines() if not line.strip().startswith("#")]
    assert sum(line.count("--profile rust-worker") for line in commands) == 2
