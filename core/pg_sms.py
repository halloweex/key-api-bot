"""The SMS tab's store switch, a constant.

Until step 14's S14-13 this module copied five of revision 0013's tables —
the opt-outs, the audience presets, the campaigns, their members and the
delivery-report bindings — from DuckDB into Postgres every hour
(`replicate_sms`, riding `replicate_operational`), and stood the copy down
once `KS_SMS_STORE=postgres` made Postgres the writer (2026-09-06). Postgres
has been `/sms`'s only store since, so the copy and its helpers went with
S14-13. What is left is the switch, for its one remaining reader.
"""
from __future__ import annotations


def sms_store_is_postgres() -> bool:
    """Which store answers `/sms`: Postgres, a constant since step 14's
    S14-00 — production since 2026-09-06, with no way back.

    `KS_SMS_STORE` is read for nothing, so no value of it can stop the
    container. Since S14-08 nothing routes on this: the DuckDB branches of
    `_sms_tx`, `_sms_run` and the audience are gone, and with them the
    half-switch guard (`refuse_while_unported`, `UNPORTED`), which had been
    empty since every `/sms` path ran on both engines. Last consumer, by
    rule 16 of the step-14 plan: `core/duckdb_table_fates.py` (S14-18);
    `core/mirror_reconciliation.reconcile_sms` went in S14-12 and
    `replicate_sms` in S14-13. Until S14-00 an unknown value raised —
    `KS_BOT_STORE`'s rule.
    """
    return True
