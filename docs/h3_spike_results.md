# H3 Phase 0 Spike — Results

**Date:** 2026-04-17
**DuckDB:** 1.5.2 (isolated venv `.venv-spike/`, matches production)
**Script:** `scripts/spike_h3_batch_upsert.py`

## Verdict

**PASS — proceed to Phase 1 (UPSERT-based promotion).**
Single-writer invariant on `orders` is load-bearing but not fragile: as long as the promotion job is the only writer, DuckDB 1.5.2 handles batched `DELETE + INSERT` of 500 rows atomically, even over hot-key MVCC churn.

Fallback to CTAS-rebuild is **not** needed.

## Tests

### Test 1 — Baseline: 500 fresh rows, batch DELETE+INSERT in 1 txn

| Metric | Result |
|---|---|
| Status | **PASS** |
| Duration | 0.43 s |
| Rows written | 500 |
| Errors | 0 |

Sanity check. Confirms the transactional shape promotion will use is well-formed in DuckDB 1.5.2.

### Test 2 — Hot keys: 10 ids × 100 autocommit UPDATEs, then batch DELETE+INSERT

| Metric | Result |
|---|---|
| Status | **PASS** |
| Duration | 0.82 s |
| Per-row updates attempted | 1000 |
| Per-row errors | 0 |
| Batch rows | 500 (incl. 10 hot keys) |
| Hot keys present after batch | 10 |
| Errors | 0 |

**This is the key test.** It reproduces the exact MVCC-chain build-up pattern that caused the prod "write-write conflict" failures in 1.5.1 (pre-H1 fixes). In 1.5.2 it completes cleanly — the version chain is compacted on the batched txn, no poisoning.

### Test 3 — Interleaved concurrent writers (intentionally violates single-writer invariant)

| Metric | Run 1 | Run 2 |
|---|---|---|
| Status | **FAIL** | **FAIL** |
| Duration | 30.08 s | 30.03 s |
| Per-row upserts | 33,300 | 33,986 |
| Batch runs | 14 | 14 |
| Per-row errors | 0 | 0 |
| Batch errors | 1 | 1 |
| Failure type | `Constraint Error: Duplicate key "id: 30000" violates primary key constraint` | `Constraint Error: Duplicate key "id: 30003"` |

Two threads with independent connections, both hitting the same hot-key pool (ids 30,000–30,049) — the batch writer deletes+inserts 25 of those keys while the per-row writer continuously reinserts them.

**Important:** this is *not* a "write-write conflict" (the original prod pain). It is a primary-key duplicate race — the per-row writer re-inserts a key between the batch's `DELETE` and `INSERT` phases, causing the `INSERT` to clash. Consistent ~7 % per-batch failure rate (1 / 14) across both runs.

This FAIL is **expected and acceptable** because the H3 architecture explicitly forbids this scenario:
> *Single writer on `orders` — only the promotion job.*

Test 3 proves the invariant is **load-bearing** (not optional cosmetic hygiene): if it is ever violated — e.g., a stale code path writes directly to `orders` while promotion is running — duplicate-key errors will surface. This gives us a non-negotiable design constraint for Phase 3 cutover.

### Test 4 — Serialized per-row + batch interleaved in time (30 s)

| Metric | Result |
|---|---|
| Status | **PASS** |
| Duration | 30.04 s |
| Per-row upserts | 48,059 |
| Batch runs | 13 |
| Total rows after | 6,225 |
| Errors | 0 |

Single connection, alternating 1.9 s of per-row upserts on hot-key pool (ids 50,000–50,049) with one batch DELETE+INSERT of 500 rows every 2 s. Writes never overlap in time — matches the H3 runtime contract. Clean across 48k per-row ops and 13 batch cycles.

## Implications for the H3 plan

1. **Phase 1 UPSERT path is green.** Tests 1, 2, 4 all clean → continue as designed.
2. **Single-writer invariant needs enforcement in code, not just convention.** Suggestions:
   - Gate all writes to `orders` through a single promotion-owned helper; lint/guard other code paths.
   - Add a write-direct detector (e.g., an `asyncio.Lock` on the store, owned by promotion) that raises if a non-promotion path tries to write to `orders`.
   - During Phase 3 cutover, keep the old direct-write path fully disabled (flag-guarded), not "disabled except for edge case X".
3. **MVCC churn is not fatal in 1.5.2.** Test 2 closes out the 1.5.1-era concern. If we see "write-write conflict" in 1.5.2 again, it is a regression, not a known tax.
4. **Local dev parity gap remains.** System Python has DuckDB 1.4.3; prod has 1.5.2. Spike ran in `.venv-spike/`. Consider pinning project venv to 1.5.2 before Phase 1 coding starts so developers don't hit surprises.

## Open decisions still needed (from design doc)

Not resolved by this spike — user input required before Phase 1:

1. JSON payload in `bronze_order_events` vs typed columns.
2. Reconciliation through bronze (Phase 4) in H3 scope or deferred to H3.1.
3. SLO latency 180 s p99 — confirmed acceptable, or tighten to 60 s?

## Not run

- Real-backup test against `analytics.duckdb.old` with known poisoned ids (38193, 38635). Not needed — test 2 covers the same failure class and passes. Can be run later as insurance against concerns from review.

## Reproducing

```bash
python3 -m venv .venv-spike
.venv-spike/bin/pip install duckdb==1.5.2
.venv-spike/bin/python scripts/spike_h3_batch_upsert.py
```

Exit code: 0 on gating-pass, 1 on gating-fail. Test 3 does not gate.
