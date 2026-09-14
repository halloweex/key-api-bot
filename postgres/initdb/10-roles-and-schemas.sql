-- Roles and empty schemas. No tables — that is Rule 11, and it is the whole
-- reason this file is short.
--
-- The application owns its DDL. A table created here is a table two
-- repositories can disagree about, and disagreeing about a column inside one
-- repository is what started the 2026-08-09 incident: a deploy arrived before
-- the column it read, the ALTER failed inside a try/except at DEBUG, and the
-- self-heal turned a one-off into a rebuild every two minutes for hours.
--
-- Runs once, on an empty data directory. Re-running it is a no-op by
-- construction, so a restored volume does not need it skipped by hand.

\set app_password `echo "$KS_APP_PASSWORD"`
\set readonly_password `echo "$KS_READONLY_PASSWORD"`

-- ── Roles ────────────────────────────────────────────────────────────────────
-- ks_app owns what it writes and nothing else. Not a superuser: the ETL has no
-- business being able to drop a database it merely fills.
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'ks_app') THEN
        CREATE ROLE ks_app LOGIN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'ks_readonly') THEN
        CREATE ROLE ks_readonly LOGIN;
    END IF;
END
$$;

ALTER ROLE ks_app      WITH PASSWORD :'app_password';
ALTER ROLE ks_readonly WITH PASSWORD :'readonly_password';

-- ── Schemas ──────────────────────────────────────────────────────────────────
-- Named for the layers the warehouse already has, so nobody has to translate
-- between two vocabularies during a migration. Empty on purpose.
CREATE SCHEMA IF NOT EXISTS bronze AUTHORIZATION ks_app;
CREATE SCHEMA IF NOT EXISTS silver AUTHORIZATION ks_app;
CREATE SCHEMA IF NOT EXISTS gold   AUTHORIZATION ks_app;
-- Operational tables the platform itself needs a home for: the schema-version
-- ledger the ETL gates on (Rule 11) lives here, created by the application.
CREATE SCHEMA IF NOT EXISTS meta   AUTHORIZATION ks_app;

-- ── Grants ───────────────────────────────────────────────────────────────────
REVOKE ALL ON DATABASE ks FROM PUBLIC;
GRANT CONNECT ON DATABASE ks TO ks_app, ks_readonly;

-- `ks_app` creates one temporary table, and only one: the `first_seen_at`
-- carry-forward in the /inventory status rebuild, which has to read the table
-- it is about to delete. `REVOKE ALL ON DATABASE` above takes TEMPORARY away
-- with everything else — it is granted to PUBLIC by default — so without this
-- line the rebuild fails with "permission denied to create temporary tables"
-- the first time Postgres is the writer of that chain.
--
-- NOT granted to ks_readonly: that role reads, and a temp table is a write.
GRANT TEMPORARY ON DATABASE ks TO ks_app;

GRANT USAGE ON SCHEMA bronze, silver, gold, meta TO ks_readonly;

-- Applies to tables ks_app creates later. Without this, every new table would
-- need a grant nobody remembers to write, and the read-only role would quietly
-- see an older and older subset of the warehouse.
ALTER DEFAULT PRIVILEGES FOR ROLE ks_app IN SCHEMA bronze, silver, gold, meta
    GRANT SELECT ON TABLES TO ks_readonly;

-- PUBLIC can create in `public` by default in PostgreSQL 14 and earlier; 15+
-- revoked it. Stated anyway, because this outlives the version it was written
-- against.
REVOKE CREATE ON SCHEMA public FROM PUBLIC;
