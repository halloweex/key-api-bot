-- O3 (chain 3) — the order-version archive is still being written.
--
-- Only under chain 3; the variables are O1's.
--
-- WHY IT EXISTS
-- `app.order_versions` is not a chain table and has no copy anywhere: KeyCRM
-- serves current state only, so a transition nobody captured when it happened
-- is gone. Its one writer is `pg_order_versions.capture_versions`, inside
-- `pg_landing._write_order_rows`, which the chain runs exactly as the mirror
-- did — so the archive goes on across the flip only while every write of an
-- order goes through it. A writer that went round it would land every order,
-- archive none, and move no number on any page.
--
-- HOW IT DECIDES
-- `mirror_reconciliation.reconcile_order_versions`' three predicates, asked
-- live rather than read out of the 07:30 run in the journal (D8):
--   - stalled: no version in 24 h but an operator's backfill. Production
--     creates ~48 orders a day and a new order always writes one;
--   - missing: an order in bronze.orders with no version at all. Every write
--     captures in the row's own transaction and revision 0010 seeded the rest;
--   - flooding: over 1 000 of the writer's versions in 24 h (~100 is
--     ordinary) — the content comparison has stopped discriminating, and the
--     05:15 refresh's ~1 400 forced headers are landing as changes.
-- The limits and the kinds left out are that function's, from
-- `pg_order_versions.NOT_THE_WRITER`; tests/integration/test_stage4_soak_sql.py
-- holds them equal.
--
-- WHAT A FAIL MEANS
-- Stalled or missing: something writes bronze.orders round
-- `_write_order_rows`, or the chain is not writing at all (S0, O2). Nothing
-- repairs the archive — the history since is lost, and a human decides what
-- to do about it. Flooding: read the last day's versions by kind and by hour.
WITH clock AS (
    SELECT COALESCE(NULLIF(current_setting('soak.now', true), '')::timestamptz,
                    now()) AS now
),
flag AS (
    SELECT :'orders_on'::text AS state
),
uncovered AS (
    SELECT o.id
    FROM bronze.orders o
    WHERE NOT EXISTS (SELECT 1 FROM app.order_versions v WHERE v.order_id = o.id)
),
facts AS (
    SELECT
        (SELECT max(captured_at) FROM app.order_versions WHERE kind <> 'backfill')
            AS newest,
        (SELECT count(*) FROM app.order_versions v
         WHERE v.captured_at >= clock.now - interval '24 hours'
           AND v.kind NOT IN ('baseline', 'backfill')) AS recent,
        (SELECT count(*) FROM uncovered) AS missing,
        (SELECT (array_agg(id ORDER BY id))[1:10] FROM uncovered) AS missing_sample,
        clock.now
    FROM clock
),
problems AS (
    SELECT array_remove(ARRAY[
        CASE WHEN f.newest IS NULL THEN 'the archive holds no version'
             WHEN f.newest < f.now - interval '24 hours' THEN
            format('no version since %s Kyiv (limit 24 h)',
                   to_char(f.newest AT TIME ZONE 'Europe/Kyiv', 'DD.MM HH24:MI')) END,
        CASE WHEN f.missing > 0 THEN
            format('%s order(s) with no version at all, e.g. %s', f.missing,
                   left(f.missing_sample::text, 120)) END,
        CASE WHEN f.recent > 1000 THEN
            format('%s versions in 24 h (limit 1000): flooding', f.recent) END
    ], NULL) AS listed
    FROM facts f
)
SELECT 'O3 order versions'::text AS "check",
       CASE flag.state
           WHEN '0' THEN 'PASS'
           WHEN 'pending' THEN 'PASS'
           WHEN '1' THEN CASE WHEN cardinality(p.listed) = 0 THEN 'PASS' ELSE 'FAIL' END
           ELSE 'UNKNOWN'
       END AS verdict,
       CASE flag.state
           WHEN '0' THEN 'not applicable: chain 3 still writes DuckDB (KS_WRITE_ORDERS is not postgres and no latch marker)'
           WHEN 'pending' THEN 'not applicable: chain 3 has not latched (see O1)'
           WHEN '1' THEN CASE WHEN cardinality(p.listed) = 0 THEN
               format('newest version %s Kyiv, %s in 24 h, every order covered',
                      to_char(f.newest AT TIME ZONE 'Europe/Kyiv', 'DD.MM HH24:MI'), f.recent)
               ELSE left(array_to_string(p.listed, '; '), 500) END
           ELSE format('orders_on=%s: see O1', flag.state)
       END AS detail
FROM flag CROSS JOIN problems p CROSS JOIN facts f;
