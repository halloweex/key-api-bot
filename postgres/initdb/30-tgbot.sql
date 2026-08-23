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

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'tgbot_app') THEN
        CREATE ROLE tgbot_app LOGIN;
    END IF;
END
$$;

ALTER ROLE tgbot_app WITH PASSWORD :'tgbot_password';

CREATE SCHEMA IF NOT EXISTS tgbot AUTHORIZATION tgbot_app;

GRANT CONNECT ON DATABASE ks TO tgbot_app;

-- Everything this role creates lands in its own schema without anyone having
-- to remember to qualify it. That includes Alembic's `alembic_version` table:
-- `migrations/env.py` sets no schema and no `version_table_schema`, so without
-- this line revision 001 would create twelve tables and a version ledger in
-- `public` — quietly, and correctly by Postgres's rules.
--
-- `public` stays on the path behind it because extensions live there.
ALTER ROLE tgbot_app SET search_path = tgbot, public;

-- The shop reads the CRM side rather than keeping its own copy of it — that is
-- the point of one shared instance. The grant is declared now and lands when
-- the `keycrm` schema arrives at step 05; until then it simply has nothing to
-- cover.
GRANT USAGE ON SCHEMA tgbot TO ks_readonly;
ALTER DEFAULT PRIVILEGES FOR ROLE tgbot_app IN SCHEMA tgbot
    GRANT SELECT ON TABLES TO ks_readonly;

-- tgbot_app must not reach the warehouse. Stated rather than assumed: it holds
-- no grant on bronze/silver/gold/meta and this line is here so that a future
-- reader sees the omission was deliberate.
REVOKE ALL ON SCHEMA bronze, silver, gold, meta FROM tgbot_app;
