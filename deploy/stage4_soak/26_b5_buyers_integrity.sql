-- B5 (chain 4) — what is true of the three buyer tables on their own.
--
-- Only under chain 4; the variables are B1's.
--
-- WHY IT EXISTS
-- The daily comparison against DuckDB stands down for these tables once the
-- chain has them, so what is left is what holds of the Postgres copy alone —
-- `core/pg_chain_invariants.py`'s buyer group, asked here every morning of the
-- soak rather than waiting for the 07:00 integrity run to reach the digest:
--   - no contact or verdict whose buyer is gone (every writer writes the three
--     together, and nothing deletes a buyer);
--   - no NULL `full_name` (DuckDB's is NOT NULL, so the copy-back could not
--     carry such a buyer back — the one rollback there is);
--   - every buyer's phone and email in its contact list (both come from one
--     KeyCRM list; the SMS audience reads the list).
-- All three read zero on production on 2026-09-30.
--
-- WHAT A FAIL MEANS
-- A write went round the chain's writer. Nothing here repairs: find the
-- writer first (chain_buyer_orphan_rows / chain_required_column_null /
-- chain_buyer_contact_missing in the digest name the ids).
WITH flag AS (
    SELECT :'buyers_on'::text AS state
),
facts AS (
    SELECT
        (SELECT count(*) FROM bronze.buyer_contacts c
         WHERE NOT EXISTS (SELECT 1 FROM bronze.buyers b WHERE b.id = c.buyer_id))
            AS orphan_contacts,
        (SELECT count(*) FROM app.buyer_gender g
         WHERE NOT EXISTS (SELECT 1 FROM bronze.buyers b WHERE b.id = g.buyer_id))
            AS orphan_verdicts,
        (SELECT count(*) FROM bronze.buyers WHERE full_name IS NULL) AS null_names,
        (SELECT count(*) FROM bronze.buyers b
         WHERE (b.phone IS NOT NULL AND NOT EXISTS (
                    SELECT 1 FROM bronze.buyer_contacts c
                    WHERE c.buyer_id = b.id AND c.contact_type = 'phone'
                      AND c.value = b.phone))
            OR (b.email IS NOT NULL AND NOT EXISTS (
                    SELECT 1 FROM bronze.buyer_contacts c
                    WHERE c.buyer_id = b.id AND c.contact_type = 'email'
                      AND c.value = b.email))) AS contact_missing
)
SELECT 'B5 buyers integrity'::text AS "check",
       CASE flag.state
           WHEN '0' THEN 'PASS'
           WHEN 'held' THEN 'PASS'
           WHEN '1' THEN CASE WHEN f.orphan_contacts + f.orphan_verdicts + f.null_names
                                   + f.contact_missing > 0 THEN 'FAIL' ELSE 'PASS' END
           ELSE 'UNKNOWN'
       END AS verdict,
       CASE flag.state
           WHEN '0' THEN 'not applicable: chain 4 still writes DuckDB (KS_WRITE_BUYERS is not postgres and no latch marker)'
           WHEN 'held' THEN 'not applicable: chain 4 is held on DuckDB (see B1)'
           WHEN '1' THEN format(
               '%s orphaned contact(s), %s orphaned verdict(s), %s NULL name(s), '
               '%s buyer(s) whose phone or email is not in their contact list',
               f.orphan_contacts, f.orphan_verdicts, f.null_names, f.contact_missing)
           ELSE format('buyers_on=%s: see B1', flag.state)
       END AS detail
FROM flag CROSS JOIN facts f;
