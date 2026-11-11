-- B5 (chain 4) — what is true of the three buyer tables on their own.
--
-- It judged only under chain 4 (KS_WRITE_BUYERS=postgres with every reader
-- of the buyers on Postgres, or its latch) until step 14's S14-20: every
-- chain writes Postgres since S14-00.
--
-- WHY IT EXISTS
-- The daily comparison against DuckDB stood down for these tables once the
-- chain had them, and step 14 deleted it, so what is left is what holds of
-- the Postgres copy alone —
-- `core/pg_chain_invariants.py`'s buyer group, asked here every morning of the
-- soak rather than waiting for the 07:00 integrity run to reach the digest:
--   - no contact or verdict whose buyer is gone (every writer writes the three
--     together, and nothing deletes a buyer);
--   - no NULL `full_name` (DuckDB's is NOT NULL, so an image from before
--     step 14, whose copy-back is the way back there, could not carry such a
--     buyer back);
--   - every buyer's phone and email in its contact list (both come from one
--     KeyCRM list; the SMS audience reads the list). An empty column is not
--     a value: the parse stores '' when KeyCRM's list starts with one, and
--     writes no contact for it.
-- All three read zero on production on 2026-09-30.
--
-- WHAT A FAIL MEANS
-- A write went round the chain's writer. Nothing here repairs: find the
-- writer first (chain_buyer_orphan_rows / chain_required_column_null /
-- chain_buyer_contact_missing in the digest name the ids).
WITH facts AS (
    SELECT
        (SELECT count(*) FROM bronze.buyer_contacts c
         WHERE NOT EXISTS (SELECT 1 FROM bronze.buyers b WHERE b.id = c.buyer_id))
            AS orphan_contacts,
        (SELECT count(*) FROM app.buyer_gender g
         WHERE NOT EXISTS (SELECT 1 FROM bronze.buyers b WHERE b.id = g.buyer_id))
            AS orphan_verdicts,
        (SELECT count(*) FROM bronze.buyers WHERE full_name IS NULL) AS null_names,
        (SELECT count(*) FROM bronze.buyers b
         WHERE (NULLIF(b.phone, '') IS NOT NULL AND NOT EXISTS (
                    SELECT 1 FROM bronze.buyer_contacts c
                    WHERE c.buyer_id = b.id AND c.contact_type = 'phone'
                      AND c.value = b.phone))
            OR (NULLIF(b.email, '') IS NOT NULL AND NOT EXISTS (
                    SELECT 1 FROM bronze.buyer_contacts c
                    WHERE c.buyer_id = b.id AND c.contact_type = 'email'
                      AND c.value = b.email))) AS contact_missing
)
SELECT 'B5 buyers integrity'::text AS "check",
       CASE WHEN f.orphan_contacts + f.orphan_verdicts + f.null_names
                               + f.contact_missing > 0 THEN 'FAIL' ELSE 'PASS' END AS verdict,
       format(
           '%s orphaned contact(s), %s orphaned verdict(s), %s NULL name(s), '
           '%s buyer(s) whose phone or email is not in their contact list',
           f.orphan_contacts, f.orphan_verdicts, f.null_names, f.contact_missing) AS detail
FROM facts f;
