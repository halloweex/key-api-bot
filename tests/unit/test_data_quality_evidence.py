

class TestEvidenceForAgent:
    """What the alert had no room for, in the shape a SELECT can read.

    The diagnostician reads Postgres; DQ findings live in a DuckDB it cannot
    open. On 2026-09-01 that gap cost two wrong diagnoses on one incident —
    the answer was `Columns: updated_at (891), ...` in a finding nothing could
    reach. This is that sentence, put where `psql` finds it.
    """

    def _issue(self, check, table, sev, count, desc="", samples=()):
        from core.data_quality import IntegrityIssue
        return IntegrityIssue(check, table, sev, count, tuple(samples), desc)

    def test_it_carries_the_sentence_that_names_the_columns(self):
        from core.data_quality import Severity, evidence_for_agent
        detail = ("891 row(s) are present in both stores and disagree. "
                  "Columns: updated_at (891), reserve (3), last_sale_date (3).")
        ev = evidence_for_agent("mirror_landing", [
            self._issue("mirror_row_values", "app.sku_inventory_status",
                        Severity.CRITICAL, 891, detail, (470, 804)),
        ], run_id=449)
        assert ev["layer"] == "mirror_landing"
        assert ev["run_id"] == 449
        one = ev["findings"][0]
        assert one["check"] == "mirror_row_values"
        assert one["table"] == "app.sku_inventory_status"
        assert one["count"] == 891
        assert one["samples"] == [470, 804]
        assert "updated_at (891)" in one["detail"]

    def test_worst_first_so_truncation_drops_the_noise(self):
        """Bounds bite from the tail, so the ordering is what makes them safe."""
        from core.data_quality import Severity, evidence_for_agent
        ev = evidence_for_agent("mirror_landing", [
            self._issue("mirror_retired_rows", "bronze.products", Severity.INFO, 1),
            self._issue("mirror_row_values", "bronze.offer_stocks", Severity.CRITICAL, 3),
            self._issue("mirror_row_values", "app.sku_inventory_status",
                        Severity.CRITICAL, 891),
        ])
        assert [f["count"] for f in ev["findings"]] == [891, 3, 1]

    def test_many_findings_are_capped_and_the_total_is_kept(self):
        from core.data_quality import (
            EVIDENCE_MAX_FINDINGS, Severity, evidence_for_agent,
        )
        issues = [self._issue(f"check_{i}", "t", Severity.WARN, i)
                  for i in range(EVIDENCE_MAX_FINDINGS + 8)]
        ev = evidence_for_agent("integrity", issues)
        assert len(ev["findings"]) == EVIDENCE_MAX_FINDINGS
        assert ev["findings_total"] == EVIDENCE_MAX_FINDINGS + 8

    def test_a_giant_run_cannot_become_a_megabyte_row(self):
        """One INSERT, bounded — the ledger write is fire-and-forget with a
        one-second budget and must not be the thing that blows it."""
        import json
        from core.data_quality import (
            EVIDENCE_MAX_BYTES, Severity, evidence_for_agent,
        )
        issues = [self._issue(f"check_{i}", "some.table", Severity.CRITICAL,
                              9999, "x" * 5000, tuple(range(10)))
                  for i in range(40)]
        ev = evidence_for_agent("integrity", issues)
        size = len(json.dumps(ev, ensure_ascii=False).encode("utf-8"))
        assert size <= EVIDENCE_MAX_BYTES, size
        assert ev["findings_total"] == 40

    def test_descriptions_are_truncated_not_dropped(self):
        from core.data_quality import (
            EVIDENCE_MAX_DETAIL_CHARS, Severity, evidence_for_agent,
        )
        ev = evidence_for_agent("integrity", [
            self._issue("c", "t", Severity.CRITICAL, 1, "y" * 9000),
        ])
        assert len(ev["findings"][0]["detail"]) == EVIDENCE_MAX_DETAIL_CHARS

    def test_discrepancies_are_summarised_not_listed(self):
        """Reconciliation can produce thousands; the agent needs the shape."""
        from core.data_quality import (
            Discrepancy, DiscrepancyClass, Severity, evidence_for_agent,
        )
        ds = [
            Discrepancy("2026-08", 1, DiscrepancyClass.MISSING_IN_DK,
                        "orders", 1.0, 2.0),
            Discrepancy("2026-08", 2, DiscrepancyClass.MISSING_IN_DK,
                        "orders", 1.0, 2.0),
            Discrepancy("2026-08", 1, DiscrepancyClass.VALUE_MISMATCH,
                        "revenue", 1.0, 2.0),
        ]
        ev = evidence_for_agent("reconciliation", [], ds)
        assert ev["discrepancies"]["total"] == 3
        assert ev["discrepancies"]["by_class"]["MISSING_IN_DK"] == 2
        assert ev["discrepancies"]["by_class"]["VALUE_MISMATCH"] == 1

    def test_a_clean_run_produces_no_findings_and_no_crash(self):
        from core.data_quality import evidence_for_agent
        ev = evidence_for_agent("reconciliation", [], [])
        assert ev["findings"] == []
        assert "discrepancies" not in ev
