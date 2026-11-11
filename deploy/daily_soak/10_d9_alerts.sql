-- D9 — alerts for the derivation and the derived watermarks.
--
-- WHY IT EXISTS
-- Every other check here reads the system's state; this reads what the system
-- itself decided to say about it. A series still firing is a condition nobody
-- has resolved — and the alert journal is fire-and-forget, so a message that
-- never reached Telegram still leaves its row here.
--
-- No freshness precondition: `app.alert_series` is written to Postgres
-- directly by the alert archive (core/alert_archive.py).
--
-- WHAT A FAIL MEANS
-- A named series is in state `firing`. The key says which: `warehouse_pg:*` is
-- the derivation itself, `mirror_stale:`/`mirror_failing:` a derived table's
-- watermark, `dq_stale:mirror_landing` the daily comparison. A series that
-- fired and resolved is not a fail — one `mirror_stale` around a deploy's
-- recreate can be expected — but it is listed, to be recorded.
--
-- `mirror_buckets_disagree` was watched too, until step 14's S14-20: S14-12
-- deleted the comparison against DuckDB that filed it, and a test holds every
-- name listed here to the registry.
--
-- `customer_profile_mismatch` was watched too, until the owner's decision O6
-- (2026-11-11) deleted the check that filed it with the витрина's rebuild.
--
-- `derivation_mode_invalid` was watched too, until step 14's S14-15 deleted
-- it with KS_PG_DERIVE: a step-14 web cannot fire it, and a series still
-- `firing` under it is history an image from before step 14 left.
WITH clock AS (
    SELECT COALESCE(NULLIF(current_setting('soak.now', true), '')::timestamptz,
                    now()) AS now
),
watched AS (
    SELECT a.condition_key, a.state, a.first_fired_at, a.last_fired_at,
           a.fired_count, a.resolved_at
    FROM app.alert_series a
    WHERE left(a.condition_key, 13) = 'warehouse_pg:'
       OR a.condition_key IN ('mirror_stale:silver.orders', 'mirror_stale:gold.daily_revenue',
                              'mirror_failing:silver.orders', 'mirror_failing:gold.daily_revenue',
                              'mirror_block_missing',
                              'integrity_check_raised',
                              'gold_rollup_mismatch',
                              'dq_stale:mirror_landing')
),
agg AS (
    SELECT count(*) FILTER (WHERE w.state = 'firing') AS firing,
           string_agg(format('%s since %s Kyiv (fired %s times)', w.condition_key,
                             to_char(w.first_fired_at AT TIME ZONE 'Europe/Kyiv', 'DD.MM HH24:MI'),
                             w.fired_count),
                      '; ' ORDER BY w.last_fired_at DESC) FILTER (WHERE w.state = 'firing') AS firing_listed,
           string_agg(format('%s resolved %s Kyiv', w.condition_key,
                             to_char(w.resolved_at AT TIME ZONE 'Europe/Kyiv', 'DD.MM HH24:MI')),
                      '; ' ORDER BY w.resolved_at DESC)
               FILTER (WHERE w.state <> 'firing' AND w.resolved_at > clock.now - interval '24 hours')
               AS resolved_listed
    FROM watched w, clock
)
SELECT 'D9 alerts'::text AS "check",
       CASE WHEN COALESCE(agg.firing, 0) > 0 THEN 'FAIL' ELSE 'PASS' END AS verdict,
       CASE
           WHEN COALESCE(agg.firing, 0) > 0 THEN left('firing: ' || agg.firing_listed, 500)
           WHEN agg.resolved_listed IS NOT NULL THEN
               left('nothing firing; resolved in 24 h (record it): ' || agg.resolved_listed, 500)
           ELSE 'nothing firing, nothing resolved in 24 h'
       END AS detail
FROM agg;
