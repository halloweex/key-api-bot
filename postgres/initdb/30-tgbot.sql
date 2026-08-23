-- The shop's own schema and role. Still no tables — Rule 11 survives the move
-- of the server itself: `ks-tg-bot` owns its twelve tables and declares them in
-- its own Alembic revision 001, not here.
--
-- Why a separate role rather than reusing ks_app: ks_app is key-api-bot's
-- writer. Handing it to the shop would make every shop deploy able to write the
-- warehouse schemas, and "we gave it a role" and "the role is limited" are
-- different statements — the same distinction Rule 12 records about ClickHouse
-- users declared without a <grants> section.
--
-- Idempotent by construction, like its neighbours, because on a live database
-- `/docker-entrypoint-initdb.d` never runs again: it executes only against an
-- empty data directory. The `postgres-bootstrap` one-shot in docker-compose.yml
-- re-applies every *.sql here on each `up -d`, which is how this file reaches a
-- database that already exists.

\set tgbot_password `echo "$TGBOT_APP_PASSWORD"`
\set tgbot_owner_password `echo "$TGBOT_OWNER_PASSWORD"`

-- ── Two roles, and the split is the point ────────────────────────────────────
--
-- `tgbot_owner` owns the schema, the tables and the sequences, and is who
-- Alembic connects as. `tgbot_app` is what the bot connects as and owns
-- nothing.
--
-- Why bother: **an owner bypasses row level security.** Revision 001 of the shop
-- says so itself — "It does not own the tables, which is half of what makes RLS
-- real" — and then does not arrange it: its grant block hardcodes schema
-- `public`, so once the tables landed in `tgbot` every statement in it was a
-- no-op, and its docstring promises a FORCE the code never applies. Found by
-- applying it (2026-08-23), not by reading it.
--
-- Done now because it is nearly free: ownership transfer moves no data, and the
-- rows presently in `tgbot` are a rehearsal load that can be replayed. After the
-- switch it would be a change of ownership under a live bot.
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'tgbot_owner') THEN
        CREATE ROLE tgbot_owner LOGIN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'tgbot_app') THEN
        CREATE ROLE tgbot_app LOGIN;
    END IF;
END
$$;

ALTER ROLE tgbot_owner WITH PASSWORD :'tgbot_owner_password';
ALTER ROLE tgbot_app   WITH PASSWORD :'tgbot_password';

CREATE SCHEMA IF NOT EXISTS tgbot AUTHORIZATION tgbot_owner;
-- Idempotent, and it is what moves an existing schema created before the split.
ALTER SCHEMA tgbot OWNER TO tgbot_owner;

GRANT CONNECT ON DATABASE ks TO tgbot_owner, tgbot_app;

-- Everything already in the schema moves too, or the split only applies to
-- tables created from tomorrow. Sequences included: `users.id` draws from one,
-- and an app that cannot use it cannot register anybody.
DO $$
DECLARE r record;
BEGIN
    FOR r IN SELECT tablename FROM pg_tables WHERE schemaname = 'tgbot' LOOP
        EXECUTE format('ALTER TABLE tgbot.%I OWNER TO tgbot_owner', r.tablename);
    END LOOP;
    FOR r IN SELECT sequencename FROM pg_sequences WHERE schemaname = 'tgbot' LOOP
        EXECUTE format('ALTER SEQUENCE tgbot.%I OWNER TO tgbot_owner', r.sequencename);
    END LOOP;
END
$$;

-- The application's own privileges: everything it needs to run, nothing that
-- lets it change the shape of what it runs on.
GRANT USAGE ON SCHEMA tgbot TO tgbot_app;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA tgbot TO tgbot_app;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA tgbot TO tgbot_app;
-- And for tables a later revision adds, so a new table does not silently become
-- invisible to the bot. Granted FOR the owner, because it is the owner that will
-- create them.
ALTER DEFAULT PRIVILEGES FOR ROLE tgbot_owner IN SCHEMA tgbot
    GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO tgbot_app;
ALTER DEFAULT PRIVILEGES FOR ROLE tgbot_owner IN SCHEMA tgbot
    GRANT USAGE, SELECT ON SEQUENCES TO tgbot_app;

-- Everything this role creates lands in its own schema without anyone having
-- to remember to qualify it. That includes Alembic's `alembic_version` table:
-- `migrations/env.py` sets no schema and no `version_table_schema`, so without
-- this line revision 001 would create twelve tables and a version ledger in
-- `public` — quietly, and correctly by Postgres's rules.
--
-- `public` stays on the path behind it because extensions live there.
ALTER ROLE tgbot_owner SET search_path = tgbot, public;
ALTER ROLE tgbot_app   SET search_path = tgbot, public;

-- The shop reads the CRM side rather than keeping its own copy of it — that is
-- the point of one shared instance. The grant is declared now and lands when
-- the `keycrm` schema arrives at step 05; until then it simply has nothing to
-- cover.
GRANT USAGE ON SCHEMA tgbot TO ks_readonly;
GRANT SELECT ON ALL TABLES IN SCHEMA tgbot TO ks_readonly;
ALTER DEFAULT PRIVILEGES FOR ROLE tgbot_owner IN SCHEMA tgbot
    GRANT SELECT ON TABLES TO ks_readonly;

-- tgbot_app must not reach the warehouse. Stated rather than assumed: it holds
-- no grant on bronze/silver/gold/meta and this line is here so that a future
-- reader sees the omission was deliberate.
REVOKE ALL ON SCHEMA bronze, silver, gold, meta FROM tgbot_app, tgbot_owner;
