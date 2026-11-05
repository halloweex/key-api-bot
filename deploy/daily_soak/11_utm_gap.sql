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
-- `missing_past_floor` or `stale_verdict` above zero outside the Sunday
-- compaction window: /traffic is misattributing those orders now. Read the
-- `silver.order_utm` watermark, then DN-04. Inside the compaction window the
-- same counts are UNKNOWN — the web container is down and the ship with it.
WITH clock AS (
    SELECT COALESCE(NULLIF(current_setting('soak.now', true), '')::timestamptz,
                    now()) AS now
),
compaction AS (
    -- Host cron, Sunday 02:00 UTC (scripts/weekly_compact.sh), and the restart after it.
    SELECT extract(isodow FROM clock.now AT TIME ZONE 'UTC') = 7
           AND (clock.now AT TIME ZONE 'UTC')::time >= time '02:00'
           AND (clock.now AT TIME ZONE 'UTC')::time < time '03:30' AS inside
    FROM clock
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
           WHEN compaction.inside THEN 'UNKNOWN'
           ELSE 'FAIL'
       END AS verdict,
       format('missing past the floor %s%s; missing in flight %s; stale verdicts %s%s',
              counts.missing_past_floor,
              CASE WHEN counts.first_missing IS NOT NULL
                   THEN format(' (lowest order id %s)', counts.first_missing) ELSE '' END,
              counts.missing_in_flight, counts.stale_verdict,
              CASE WHEN compaction.inside
                        AND (counts.missing_past_floor > 0 OR counts.stale_verdict > 0)
                   THEN ' — inside the Sunday compaction window; re-run after 03:30 UTC'
                   ELSE '' END) AS detail
FROM counts CROSS JOIN compaction;
