"""The Rust pilot's own role (deploy/rust/ks_rust_api.sql), held to what the
pilot reads: SELECT on exactly the tables its statements name, the schemas
they live in, and the limits every session of the role carries.

A table the pilot starts reading without a grant fails its harness in CI,
which reads as this role; one it stops reading would keep a grant nobody
needs. Both are a line in the SQL, and this says which.
"""
from __future__ import annotations

import re
from pathlib import Path

import yaml

REPO = Path(__file__).resolve().parents[2]
SQL = (REPO / "deploy" / "rust" / "ks_rust_api.sql").read_text()
STATEMENTS = re.sub(r"--[^\n]*", "", SQL)
SOURCES = "\n".join(p.read_text() for p in sorted((REPO / "rust/crates/ks-api/src").rglob("*.rs")))


def _granted(kind: str) -> set:
    found = re.findall(rf"GRANT\s+{kind}\s+ON\s+(?:SCHEMA\s+)?(.*?)\s+TO\s+ks_rust_api\s*;",
                       STATEMENTS, re.S)
    return {name.strip() for clause in found for name in clause.split(",")}


def test_select_is_granted_on_exactly_what_the_pilot_reads():
    read = set(re.findall(r"\b(?:FROM|JOIN)\s+([a-z_]+\.[a-z_]+)", SOURCES))
    assert read, "no statement found in the pilot's sources"
    assert _granted("SELECT") == read


def test_usage_on_exactly_their_schemas():
    assert _granted("USAGE") == {table.split(".")[0] for table in _granted("SELECT")}


def test_the_limits_every_session_carries():
    for setting in ("statement_timeout = '3s'", "lock_timeout = '1s'",
                    "idle_in_transaction_session_timeout = '10s'",
                    "default_transaction_read_only = on"):
        assert f"ALTER ROLE ks_rust_api SET {setting};" in STATEMENTS, setting


def test_nothing_but_reading():
    assert re.search(r"\bNOSUPERUSER\b.*\bNOCREATEDB\b.*\bNOCREATEROLE\b", STATEMENTS, re.S)
    assert not re.search(r"GRANT\s+(?!SELECT\b|USAGE\b|CONNECT\b)\w+", STATEMENTS)
    assert "TEMPORARY" not in STATEMENTS and "DEFAULT PRIVILEGES" not in STATEMENTS


def test_the_password_is_never_written_down():
    assert "PASSWORD :'password'" in STATEMENTS
    assert not re.search(r"PASSWORD\s+'", STATEMENTS)


def test_the_service_reads_as_this_role_and_waits_for_a_connection_briefly():
    compose = yaml.safe_load((REPO / "docker-compose.yml").read_text())
    env = compose["services"]["rust-api"]["environment"]
    assert env["KS_PG_DSN"].startswith("postgresql://ks_rust_api:${KS_RUST_API_PASSWORD:-}@")
    # The pool's wait is the pilot's default, 2.5 s, unless compose says otherwise.
    assert 2.0 <= float(env.get("KS_PILOT_POOL_WAIT", "2.5")) <= 3.0
    assert "KS_PILOT_POOL_WAIT\", 2.5" in SOURCES
