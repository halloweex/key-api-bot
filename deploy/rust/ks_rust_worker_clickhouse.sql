-- ks_rust_worker in ClickHouse: the core's worker's own login there (the Rust
-- plan, revision 4, К1; docker-compose.yml's `rust-worker`).
--
-- NOT CREATED ON PRODUCTION until the owner says yes to it, and not by this
-- project alone: ks-clickhouse is the platform's container, and its users are
-- the server's configuration (the databases bronze, silver, gold and history
-- inside it are ours). CI and rust/harness/k1-ch-sync.sh run this file on
-- their throwaway ClickHouse, so the shadow ships as this user there, and a
-- grant it lacks fails there.
--
-- The password is .env's KS_RUST_WORKER_CH_PASSWORD, written here only as its
-- SHA-256, by a login that may manage users:
--
--   hash=$(printf %s "$KS_RUST_WORKER_CH_PASSWORD" | sha256sum | cut -d' ' -f1)
--   sed "s/@PASSWORD_SHA256@/$hash/" deploy/rust/ks_rust_worker_clickhouse.sql \
--     | docker exec -i ks-clickhouse clickhouse-client --multiquery \
--         --user <a login that may manage users> --ask-password
--
-- Re-running it changes nothing but the password. The archive's database is
-- `history`; a host that sets KS_CH_HISTORY_DB names its own here.
--
-- THE SHADOW, AND NOTHING ELSE
--
-- Five tables, each by name: the shadow's Silver and Gold, their staging
-- tables, and its archive. A ship sends CREATE TABLE IF NOT EXISTS, TRUNCATE,
-- INSERT, SELECT, and EXCHANGE TABLES, which swaps the two tables' names and
-- so takes DROP TABLE as well. CREATE DATABASE IF NOT EXISTS is refused and
-- tolerated, as it is for ks_app. The live tables stay Python's:
-- tests/integration/test_rust_k1_ch_sync.py runs the shadow as this user and
-- requires a live run as this user to be refused.

CREATE USER IF NOT EXISTS ks_rust_worker IDENTIFIED WITH sha256_hash BY '@PASSWORD_SHA256@';
ALTER USER ks_rust_worker IDENTIFIED WITH sha256_hash BY '@PASSWORD_SHA256@';

GRANT SELECT, INSERT, CREATE TABLE, DROP TABLE, TRUNCATE ON silver.orders_rust TO ks_rust_worker;
GRANT SELECT, INSERT, CREATE TABLE, DROP TABLE, TRUNCATE ON silver.orders_rust_staging TO ks_rust_worker;
GRANT SELECT, INSERT, CREATE TABLE, DROP TABLE, TRUNCATE ON gold.daily_revenue_rust TO ks_rust_worker;
GRANT SELECT, INSERT, CREATE TABLE, DROP TABLE, TRUNCATE ON gold.daily_revenue_rust_staging TO ks_rust_worker;
GRANT SELECT, INSERT, CREATE TABLE ON history.order_versions_rust TO ks_rust_worker;

-- AT THE SWITCH, AND ONLY ON THE OWNER'S YES TO IT
--
-- Switching ch_sync to Rust (`scripts/job_executor.py set ch_sync rust`, К0)
-- has the worker write the live tables. Until they are granted, that run is
-- refused (Code 497) and counted against the live state rows, which the
-- morning check reads: nothing is written wrong, and the switch goes back.
-- Uncomment and run the same way:
--
-- GRANT SELECT, INSERT, CREATE TABLE, DROP TABLE, TRUNCATE ON silver.orders TO ks_rust_worker;
-- GRANT SELECT, INSERT, CREATE TABLE, DROP TABLE, TRUNCATE ON silver.orders_staging TO ks_rust_worker;
-- GRANT SELECT, INSERT, CREATE TABLE, DROP TABLE, TRUNCATE ON gold.daily_revenue TO ks_rust_worker;
-- GRANT SELECT, INSERT, CREATE TABLE, DROP TABLE, TRUNCATE ON gold.daily_revenue_staging TO ks_rust_worker;
-- GRANT SELECT, INSERT, CREATE TABLE ON history.order_versions TO ks_rust_worker;
