-- The application's own state. Still no tables — Rule 11 holds: the schema is
-- provisioning, the tables are `core/migrations.py`'s to create.
--
-- Why a fourth schema rather than putting these in `meta`. Three different
-- kinds of thing live in this database and they are not interchangeable:
--
--   bronze / silver / gold   the warehouse layers, rebuildable from KeyCRM
--   meta                     the machinery ABOUT the warehouse — the schema
--                            ledger, refresh journal, data-quality runs. Also
--                            rebuildable, and worthless once it is.
--   app                      what the application knows and nothing else does
--
-- Only the third is irreplaceable. `bot.db` today holds 24 authorised users,
-- 25 celebrated milestones and everyone's language choice — about fifty rows
-- that exist nowhere else and that no sync can bring back. Putting them beside
-- a journal that is regenerated every two minutes would mean one backup policy
-- for two very different losses, and the strictest of the two would end up
-- applied to everything or, worse, the mildest.
--
-- The operational tables follow them here — goals, campaigns, opt-outs, manager
-- classifications — for the same reason: a human decided each of those and
-- nothing can decide them again.
--
-- Idempotent, like its neighbours, because on a live database
-- `/docker-entrypoint-initdb.d` never runs again — the `postgres-bootstrap`
-- one-shot re-applies every *.sql here on each `up -d`.

CREATE SCHEMA IF NOT EXISTS app AUTHORIZATION ks_app;

GRANT USAGE ON SCHEMA app TO ks_readonly;
ALTER DEFAULT PRIVILEGES FOR ROLE ks_app IN SCHEMA app
    GRANT SELECT ON TABLES TO ks_readonly;

-- Deliberately NOT split into an owner role and an application role, the way
-- the shop's schema was. That split exists so that row level security can apply
-- at all, because an owner bypasses it; key-api-bot has no RLS in its design
-- and no second tenant to protect a row from — its dashboard gates on
-- `sales_type` in the application layer. Adding the split here would be
-- ceremony imitating a reason that does not exist yet. If RLS is ever wanted,
-- the shop's file is the worked example.
