-- UTM on two clocks — orders with no attribution verdict, or a stale one.
--
-- WHY IT EXISTS
-- Under own, Silver is derived on Postgres' signal while `silver.order_utm` is
-- still parsed in DuckDB and shipped on DuckDB's tick (core/scheduler.py says
-- why, and #213 accepted the gap). An order can therefore reach Silver before
-- its UTM row, and /traffic files it through the COALESCE as organic until the
-- row lands. A floor and a tick is the accepted width of that gap; anything
-- older means the copy stalled, or shipped a partial table (DN-04).
--
-- HOW IT DECIDES
-- Every bronze order with a non-empty `manager_comment` is one the parser
-- reads, so it must have a row. `parsed_at` is the order's own `updated_at` at
-- parse time (core/repositories/traffic.py), so a row older than the order is
-- a verdict the parser has not caught up with. Both are forgiven for 15 min
-- after the order was mirrored: the 600 s floor, a tick, and slack.
--
-- WHAT A FAIL MEANS
-- `missing_past_floor` or `stale_verdict` above zero: /traffic is
-- misattributing those orders now. Read the `silver.order_utm` watermark,
-- then DN-04. Inside the host's Sunday compaction window (02:00-03:30 UTC)
-- the same counts read UNKNOWN until L4-10: the compaction stopped the web
-- container, but its cron line was retired on 2026-10-16 and its script
-- deleted in step 14's S14-18, so nothing stops web there any more.
WITH clock AS (
    SELECT COALESCE(NULLIF(current_setting('soak.now', true), '')::timestamptz,
                    now()) AS now
),
counts AS (
    SELECT count(*) FILTER (WHERE u.order_id IS NULL
                              AND b.mirrored_at < clock.now - interval '15 minutes') AS missing_past_floor,
           count(*) FILTER (WHERE u.order_id IS NULL
                              AND b.mirrored_at >= clock.now - interval '15 minutes') AS missing_in_flight,
           count(*) FILTER (WHERE u.order_id IS NOT NULL
                              AND b.updated_at > u.parsed_at
                              AND b.mirrored_at < clock.now - interval '15 minutes') AS stale_verdict,
           min(b.id) FILTER (WHERE u.order_id IS NULL
                               AND b.mirrored_at < clock.now - interval '15 minutes') AS first_missing
    FROM bronze.orders b
    CROSS JOIN clock
    LEFT JOIN silver.order_utm u ON u.order_id = b.id
    WHERE b.manager_comment IS NOT NULL AND b.manager_comment <> ''
)
SELECT 'UTM gap'::text AS "check",
       CASE
           WHEN counts.missing_past_floor = 0 AND counts.stale_verdict = 0 THEN 'PASS'
           ELSE 'FAIL'
       END AS verdict,
       format('missing past the floor %s%s; missing in flight %s; stale verdicts %s',
              counts.missing_past_floor,
              CASE WHEN counts.first_missing IS NOT NULL
                   THEN format(' (lowest order id %s)', counts.first_missing) ELSE '' END,
              counts.missing_in_flight, counts.stale_verdict) AS detail
FROM counts;
