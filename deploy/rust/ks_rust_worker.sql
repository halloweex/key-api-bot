-- ks_rust_worker: the core's worker's own login in Postgres (the Rust plan,
-- revision 4, К1; docker-compose.yml's `rust-worker`).
--
-- NOT CREATED ON PRODUCTION until the owner says yes to it. Nothing runs this
-- file on its own: no migration, no initdb script, no deploy step. CI runs it
-- on its throwaway Postgres (.github/workflows/rust-pilot.yml, job `k1`), and
-- so does rust/harness/k1-ch-sync.sh, so the shadow ships as this role there
-- and a grant it lacks fails there.
--
-- On the owner's yes, by hand, as the superuser, after the migrations — the
-- tables must exist to be granted — with the password .env holds as
-- KS_RUST_WORKER_PASSWORD and never typed here. It travels as an environment
-- variable named on the command line, so it is in no argv and no output, as
-- deploy/rust/ks_rust_api.sql's does:
--
--   docker compose exec -T -e KS_RUST_WORKER_PASSWORD postgres psql -U postgres \
--     -d ks -v ON_ERROR_STOP=1 -f - < deploy/rust/ks_rust_worker.sql
--
-- `-v password=...` still wins where it is given: CI and the harness pass a
-- throwaway one. Neither given, the script stops before it changes anything.
--
-- Re-running it changes nothing but the password.
--
-- WHAT IT HOLDS, AND WHY THAT AND NOTHING ELSE
--
-- * SELECT on what ch_sync reads: `silver.orders`, shipped whole;
--   `app.order_versions`, the archive's ids-diff; `meta.chain_watermarks`,
--   whose `executor:ch_sync` row says whose job it is (К0). The customers'
--   phone numbers and the rest of `app` stay out of reach.
--   tests/unit/test_rust_worker_role.py holds this list to the tables the
--   worker's statements name.
-- * INSERT and UPDATE on `meta.mirror_state`, the stamp of each ship, and
--   SELECT on it, which `ON CONFLICT ... DO UPDATE SET failures_since_ok =
--   meta.mirror_state.failures_since_ok + 1` needs to read the row it
--   updates. Every row of it: Postgres grants by table. Which rows the worker
--   writes is the code's — `rust_shadow.*` for the shadow, the live
--   `clickhouse.*` only once the switch names Rust — and the nightly judge
--   (core/ch_rust_shadow.py) reads them.
-- * The lease is `pg_try_advisory_lock`, which needs no grant.
-- * Limits on every session of the role, whatever the client sets: a
--   statement past a minute is stopped (the archive's first inheritance reads
--   ~50 000 versions in one statement, which takes about a second), a lock
--   not had within five seconds is given up, and so is a transaction left
--   open for a minute. Four connections: the pool holds two
--   (`KS_PG_POOL_MAX` in docker-compose.yml) and the lease one more.

\set ON_ERROR_STOP on

\if :{?password}
\else
    \set password ''
    \getenv password KS_RUST_WORKER_PASSWORD
\endif
SELECT :'password' <> '' AS password_given \gset
\if :password_given
\else
    DO $$ BEGIN RAISE EXCEPTION 'no password: export KS_RUST_WORKER_PASSWORD, or pass -v password=...'; END $$;
\endif

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'ks_rust_worker') THEN
        CREATE ROLE ks_rust_worker LOGIN;
    END IF;
END
$$;

ALTER ROLE ks_rust_worker WITH LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT
    NOREPLICATION NOBYPASSRLS CONNECTION LIMIT 4 PASSWORD :'password';

ALTER ROLE ks_rust_worker SET statement_timeout = '60s';
ALTER ROLE ks_rust_worker SET lock_timeout = '5s';
ALTER ROLE ks_rust_worker SET idle_in_transaction_session_timeout = '60s';

GRANT CONNECT ON DATABASE ks TO ks_rust_worker;

GRANT USAGE ON SCHEMA app, meta, silver TO ks_rust_worker;

GRANT SELECT ON
    app.order_versions,
    meta.chain_watermarks,
    meta.mirror_state,
    silver.orders
TO ks_rust_worker;

GRANT INSERT, UPDATE ON
    meta.mirror_state
TO ks_rust_worker;
