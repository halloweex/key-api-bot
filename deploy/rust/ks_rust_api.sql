-- ks_rust_api: the Rust pilot's own login (rust/README.md; the Rust plan,
-- revision 3, step 2).
--
-- NOT CREATED ON PRODUCTION until the owner says yes to it. Nothing runs this
-- file on its own: no migration, no initdb script, no deploy step. CI runs it
-- on its throwaway Postgres (.github/workflows/rust-pilot.yml), so the pilot's
-- harness reads as this role and a grant it lacks fails there.
--
-- On the owner's yes, by hand, as the superuser, after the migrations — the
-- tables must exist to be granted — with the password .env holds as
-- KS_RUST_API_PASSWORD and never typed here:
--
--   docker compose exec -T postgres psql -U postgres -d ks -v ON_ERROR_STOP=1 \
--     -v password="$KS_RUST_API_PASSWORD" -f - < deploy/rust/ks_rust_api.sql
--
-- Re-running it changes nothing but the password.
--
-- WHAT IT HOLDS, AND WHY THAT AND NOTHING ELSE
--
-- * SELECT on the six tables the pilot reads, by name: the trend's Gold and
--   stored forecast, the three tables the session check reads, and the schema
--   stamp every read asks first. `ks_readonly` reads every table in four
--   schemas, the customers' phone numbers among them; a pilot nobody has
--   reviewed for exposure has no reason to. A table the pilot starts reading
--   is a line here, or its harness fails on a missing grant;
--   tests/unit/test_rust_api_role.py holds this list to the tables its
--   statements name.
-- * Limits on every session of the role, whatever the client sets. A
--   statement past `statement_timeout`, or a lock not had within
--   `lock_timeout` (behind a migration's DDL, say), is stopped, and the pilot
--   hands that request to the Python app (`handover::stopped_by_own_limit`):
--   its own pool waits 2.5 s for a connection (`KS_PILOT_POOL_WAIT`), so one
--   slow minute of Postgres costs the pilot its answers and nobody theirs.
--   The trend's statements take milliseconds; three seconds is two orders of
--   magnitude above the slowest.
-- * Read-only by default, no temp tables, five connections: the pool holds
--   three (`KS_PG_POOL_MAX` in docker-compose.yml), and two spare cover a
--   reconnect while an old connection is still closing.

\set ON_ERROR_STOP on

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'ks_rust_api') THEN
        CREATE ROLE ks_rust_api LOGIN;
    END IF;
END
$$;

ALTER ROLE ks_rust_api WITH LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT
    NOREPLICATION NOBYPASSRLS CONNECTION LIMIT 5 PASSWORD :'password';

ALTER ROLE ks_rust_api SET statement_timeout = '3s';
ALTER ROLE ks_rust_api SET lock_timeout = '1s';
ALTER ROLE ks_rust_api SET idle_in_transaction_session_timeout = '10s';
ALTER ROLE ks_rust_api SET default_transaction_read_only = on;

GRANT CONNECT ON DATABASE ks TO ks_rust_api;

GRANT USAGE ON SCHEMA app, gold, meta TO ks_rust_api;

GRANT SELECT ON
    app.authorized_users,
    app.dashboard_users,
    app.revenue_predictions,
    app.role_permissions,
    gold.daily_revenue,
    meta.alembic_version
TO ks_rust_api;
