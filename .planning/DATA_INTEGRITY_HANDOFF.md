# Data integrity session — 2026-08-07 → 2026-08-09

## 0. State as of 2026-08-09 12:00 — read this first

**The P0→P3 plan is finished.** #35–#48 in the first session, #49–#64 in the
second, all merged and live. Everything below section 1 is the record of how it
got there; this section is what is true now.

P3-b (#64) closed the last gap: fourteen Gold columns recomputed from Silver
and compared, report-only, inside the integrity scan where a finding cannot
reach `validation_passed`. First production run — 2 059 cells — found **zero
disagreements**, and the whole integrity scan takes 165 ms.

Also shipped after the table below: #61 (tests can no longer page admins —
`.env` holds a real BOT_TOKEN and a failing-validation test reached Telegram),
#62 (`.claude/CLAUDE.md` tracked, with hosts/users/names stripped for a public
repo, and renamed from lowercase), #63 (`paths-ignore`, so docs and tests no
longer deploy). The `production` approval gate was removed by the owner on
2026-08-09 — a merge now reaches live containers on its own.

### Shipped in the second session (all in production)

| PR | What |
|---|---|
| 49 | run-age watchdog: `/api/health` publishes age of last *successful* DQ run; `bot/canary.py` judges it (30 h recon / 12 h integrity); canary throttles per failure-key |
| 50 | alert throttle keys on the condition, never on rendered text; daily digest at 09:00 delivers WARN with a delta |
| 51 | `/api/jobs` reported `next_run: null` for every cron job — sampled before `scheduler.start()` |
| 52 | `internal` category groundwork: `is_retail` seeded once and never overwritten, admin endpoints to set it, sales_type partition assertion |
| 53 | that manager list called the B2B manager `other` — the label was derived from `is_retail`, which is FALSE for wholesale too |
| 54 | `affected_dates` = dates before the DELETE ∪ after; Gold follows Silver's full/incremental decision; half-written repair moved to its own 2-hourly job |
| 55 | missed cron runs: reconciliation moved to 05:30, plus a startup catch-up |
| 56 | `other` → `internal`, admin-only in the dashboard filter and in `api_gate` |
| 58 | revenue asks KeyCRM (`status_group_id = 6`) instead of remembering a list; `LEGACY_STATUS_NAMES` keeps the old names |
| 59 | fixing my own miss in #58 — `CREATE TABLE IF NOT EXISTS` never adds a column to an existing table |
| 60 | cell guard (Silver cells EXCEPT Gold cells) into `validation_passed`; intended shipments split out of the zero-total warning |

### What the system proved about itself

- **The repair drained the backlog by itself**: 523 orders with revenue and no
  line items → **0**, across three batches, `still_empty=0` every time. KeyCRM
  had the line items for all of them. ₴1,422,610 of product recovered.
- **The first digest ever sent** went out at 09:00 on 2026-08-09.
- **The watchdog caught a real miss on its first day.** `dq_reconciliation` did
  not run on 08-09; the canary said so at 11:02 — 30 hours, against the 79 days
  the same silence lasted before.
- **The cause was not what this document said.** Host cron
  `0 2 * * 0 weekly_compact.sh` is 02:00 UTC = 05:00 Kyiv, the exact instant the
  job was due; the compact stops both containers, and the scheduler came back at
  02:00:51, after the cron instant. See the correction in §3.

### Numbers as they stand (prod, 2026-08-09 ~12:00)

```
retail    20,122,419   b2b  15,549,070   internal  1,420,726     (365d, by manager)
silver_rows 45,828 | gold_revenue_rows 2,059 | validation_passed true
```

Open integrity findings, after the split in #60:

- `headline_vs_line_items` — **413 orders / ₴849,232.71**. Retail and b2b orders
  billed at zero with line items. This is the real disagreement and it needs a
  business decision.
- `goods_shipped_without_sale` — **771 orders / ₴3,346,975.65**, INFO. Internal
  staff shipping product with no sale: bloggers, seeding, gifts. Expected. This
  is marketing spend measured in product and nobody was counting it before.

### Control reconciliation, 2026-08-09 — the warehouse matches the source

Asked for as a monthly check of revenue and order counts. Done twice, two
independent ways, both using `keycrm_orders_in_window` + `rollup_from_orders`
so that neither side aggregates on its own terms — writing a fresh query for
the occasion is how ₴629,828 of drift got invented in #37.

**1. Nightly backup vs KeyCRM, 12 months, by hand** (script, throwaway
container, read-only): **eleven months agree to the kopiyka** — not "within
tolerance", exactly zero, including two decimal places. 1 008 API calls,
22 097 orders on the KeyCRM side, 163 in-flight excluded from both.

Two recent months differed — 2026-07 by 1 order / ₴4,805 and 2026-08 by
12 orders / ₴27,954 — entirely because the warehouse side came from the 04:30
backup while KeyCRM was read at 11:40. Verified, not assumed: live August was
530 orders / ₴1,487,317 against 361 / ₴1,028,540 in the backup.

**2. Live database vs KeyCRM, 365 days** (`run_id=153`): **0 discrepancies**,
40 cells each side, 624,410 ms, via the endpoint added in #65. The first
confirmation that the warehouse matches the source across the whole retained
history rather than the last 90 days.

Repeat it with `POST /api/reconcile?days=365` — admin, capped at 400 days and
2/hour. The default scheduled window stays 90 days.

Fixed in #66: the endpoint is detached by default and answers in 0.1 s with
`{"status": "started", ...}`, naming where the result will land. It used to run
inline and return 504 after two minutes while the work carried on for another
eight — harmless, but it read like a failure. `background=false` still runs
inline for a short window.

### What is left

```
(P3-b shipped in #64 — see above. The block below is kept for its reasoning.)

P3-b  full per-cell recompute, REPORT-ONLY — the last plan item. Narrow value:
      it catches none of the three realistic data lies (Gold is built from
      Silver in the same tick), but it is the only thing that would check the
      nine unasserted Gold columns — orders_count, unique_customers,
      new_customers, returning_customers, returns_count, returns_revenue,
      avg_order_value. PR #41 was a bug in exactly such a column, found by eye.
      Architect's veto stands: must NOT feed validation_passed.
```

Decisions that are the owner's, not code's:

1. **413 zero-total orders / ₴849,232.71** — count the line items as revenue,
   or accept and document the gap?
2. **Product categories in KeyCRM**: 549 of 985 products have none, and the
   uncategorised ones are bestsellers (Wellage ₴652K, Orgahue ₴489K…). ₴11.46M
   of 365-day line-item revenue reaches no category breakdown. Fixed in the CRM,
   not here.
3. **`sales_type=all`** still spans every category for any viewer — it reveals a
   total, not who is inside it. Gate it too, or leave it?

Also: `.claude/CLAUDE.md` was brought up to date on 2026-08-09 (it described
January). It was gitignored at the time this was written, so the update lived on
one machine only — **#62 resolved that** by tracking the file with hosts, users
and names stripped for a public repo. The paragraph is kept because the reason
it was ignored still applies to anything else under `.claude/`.

---

# History

## Session 1 — 2026-08-07/08

Twelve PRs, all merged and live. Written so the next session can resume without
rebuilding context.

---

## 1. What was wrong, and what fixed it

| # | PR | Fixed |
|---|---|---|
| 35 | DuckDB 1.5.2 → 1.5.5 | deadlock in `TemporaryMemoryManager`, crash on concurrent ALTER/INSERT, false RLE-corruption reports |
| 36 | memory ceiling + alert delivery | `DUCKDB_MEMORY_LIMIT` (4GB, was 3GB hardcoded); web-container alerts stopped vanishing |
| 37 | reconciliation comparator | three bugs that invented ₴629,828 of drift; 429 made retryable |
| 38 | alert throttle | identical alerts once per 30 min |
| 39 | stuck-warehouse recovery | split retry budgets; one full rebuild per 6 h instead of "never again" |
| 40 | backdated orders | credit an order to its own month, whoever fetches it |
| 41 | new-customer baseline | first-order baseline no longer ignores retired channels; `daily_stats` dropped; two new checks |
| 42 | per-order reconciliation | compare 9 fields per order, not just monthly sums; one shared `rollup_from_orders` |
| 43 | repair by id + gap backfill | `SyncService.repair_orders`; hourly drain of holes in the id sequence |
| 44 | backfill could not finish | full failures map returned; 404 no longer trips the circuit breaker |
| 45 | integrity scan crash | `core/data_quality.py` called a `logger` it never imported |
| 47 | scheduler | `misfire_grace_time=3600`; 6h `IntervalTrigger` → `CronTrigger(hour='1,7,13,19')` |
| 48 | deploy race | `concurrency: deploy-production`, `cancel-in-progress: false` |

Plus #46: three subagents registered — `data-architect`, `data-engineer`,
`data-quality-auditor` (`.claude/agents/`), each carrying the priors these
incidents paid for.

## 2. Production state, verified

- **Gold restored.** ₴1,349,500.20 back; `gold_rev` 1934 → 2058; validation green
  since 2026-08-07 17:18 UTC. Healed itself on deploy — nobody clicked rebuild.
- **KeyCRM reconciliation: zero discrepancies.** 4,831 orders each side,
  ₴16,057,858.17 both, all 9 per-order fields matching, no cell on one side only.
- **~1,620 missing orders recovered**, ~43 recorded as deleted upstream. Id-gap
  backfill drained (last real run 2026-08-08 11:10:26).
- **Scheduler works.** 0 missed jobs in 90 min, against 108 in five hours before.
- Container name back to `keycrm-web`; `CircuitOpenError` count 0.

## 3. P0 remainder — both closed 2026-08-08

> **Correction, 2026-08-09.** This document said reconciliation "never ran on
> Sundays (the 05:00 job came due while the weekly full sync held
> `_heavy_job_lock`)". Wrong instrument. The weekly full sync is 02:00 *Kyiv*
> = 23:00 UTC Saturday. The real collision is the host cron
> `0 2 * * 0 /opt/key-api-bot/scripts/weekly_compact.sh` — Sunday 02:00 **UTC**,
> which is 05:00 Kyiv, the exact instant `dq_reconciliation` is due. The compact
> stops the containers (02:00:07), swaps the DB, starts them (02:00:38), and the
> scheduler comes up at 02:00:51 — after the cron instant, so CronTrigger sets
> the next fire for tomorrow. The job is not late; it does not exist when it is
> due, which is why #47's `misfire_grace_time` cannot help. Proven 2026-08-09:
> no reconciliation row for that date, `/var/log/keycrm-compact.log` and
> `docker inspect` agree to the second.

**(a) 429 fix verified in production. ✅** The 2026-08-08 05:00 run
(`run_id=122`) finished `PASS`, **0 discrepancies**, 176,723 ms, 272 API calls,
`error_message IS NULL`. Every 429 death had happened at 118–135 s; this run ran
57 s past that line and produced a verdict.

Full history, from the 2026-08-08 04:30 backup:

| layer | status | runs | first | last |
|---|---|---|---|---|
| reconciliation | FAILED (all 429) | 57 | 2026-05-21 | 2026-08-06 |
| reconciliation | CRITICAL | 11 | 2026-06-17 | 2026-08-07 |
| integrity | PASS | 5 | 2026-05-20 | 2026-08-06 |
| integrity | WARN | 1 | 2026-06-01 | 2026-06-01 |

The last two runs tell the whole story: **2026-08-07 CRITICAL with 40
discrepancies** (comparator bugs, pre-#37) → **2026-08-08 PASS with 0**. One
deploy, both faults gone.

> **Correction to this document's own first draft.** It claimed the 2026-08-08
> run "did not happen at all". It did, at 05:00. The claim came from reading
> `data_quality_runs` in the nightly backup — which is taken at 04:30, thirty
> minutes *before* the job runs. Absence in a snapshot is not absence.

**(b) Run-age watchdog — PR #49, merged and live 2026-08-08 20:44 UTC.** Keyed on `MAX(started_at) WHERE error_message IS NULL AND status <>
'FAILED'`, computed in `get_stats()` inside the connection already held (health's
60 s cache covers it, store lock sees nothing extra), surfaced as a top-level
`data_quality` block on `/api/health`, judged in `bot/canary.py`.

- Thresholds: reconciliation 30 h (24 h cycle + grace), integrity 12 h (6 h + grace).
- **Absence reads red**: a missing `data_quality` block, a missing layer, and a
  layer that has never succeeded are each a failure. The day the freshness query
  breaks must not be the day the watchdog goes quiet.
- Overall `/api/health` status is deliberately untouched — a stale checker is
  degraded observability, not a reason for Docker to restart the container.
- Canary throttling now keys on stable identifiers (`dq_stale:reconciliation`),
  never on rendered text, which carries an age that changes every cycle. A new
  problem alerts immediately instead of serving out an unrelated problem's
  cooldown.
- 20 new tests (`tests/unit/test_canary.py`, `test_data_quality_persistence.py`),
  899 pass.

**The scheduler fix (#47) is confirmed too.** Integrity's 19:00 run on 2026-08-08
landed at `19:00:00.029` — the first time it ever fired on schedule. All six
earlier runs are at ragged times (19:34, 01:35, 22:20 …): they were container
starts, not the 6 h trigger. That trigger never once fired in 79 days.

That 19:00 run reported `WARN` with the two known open items from §6 — 1,184
zero-total orders (₴4,196,208.36) and 523 orders with no line items
(₴1,422,610.30). Expected backlog surfacing, not a new incident.

## 4. The plan beyond P0

Order settled by three rounds of adversarial debate between the agents:

```
P1  DONE — PR #50 (branch dq-alert-surfacing), merged: no
    throttle keyed on the condition, never on rendered text
    daily digest at 09:00 delivers WARN with a delta vs the last good run
P2  DONE — PR #52 (branch sales-type-partition), merged: no
      partition assertion: gold known-types == silver checksum (report-only)
      is_retail seeded once, never overwritten + admin endpoints to set it
    DONE — PR #54 (branch gold-date-window-and-repair-job), merged: no
      affected_dates = old dates (before the DELETE) ∪ new; Gold follows
        Silver's own full/incremental decision
      half-written repair moved to its own 2-hourly job, with a 30-day
        skip list so re-serving an empty order does not cost calls forever
P3  cheap `silver cells EXCEPT gold cells` guard, 7.5 ms, inline
    full per-cell recompute — REPORT-ONLY, one cycle after P1 is observed working
```

If only half ships: cut the full recompute (keep the EXCEPT guard) and the UI
half of the `'other'` work.

## 5. Three corrections the debate made to earlier conclusions

1. **Warehouse validation cannot see a lie that arrives from Bronze.** It is a
   post-rebuild self-consistency check — Gold is built from Silver in the same
   tick, so a consistent lie is reproduced on both sides. An injection test
   proved ±1000 on two orders, `status 12→19` (₴130,663) and `source 1→2`
   (₴33,772) are **all invisible** to it.
2. **The full per-cell recompute catches none of those three either**, for the
   same reason. It covers rebuild faults and the 9 unasserted Gold columns only.
   Do not describe it as full coverage.
3. **The cheap guard would have caught the whole real incident.** Across the
   three mid-incident backups: 100 → 90 → 84 mismatched cells and **0 value
   mismatches** — every one was a *missing* cell. No period in retained history
   has the scalars agreeing while cells disagree.

## 6. Open questions and known-unfixable

Needs a business decision, not code:
- **`sales_type='other'`**: ₴3,107,768.60 **all-time** (2.52% of ₴123.2M);
  ₴1,424,230.94 over 365d (2.12%); 2.87% over the last 90d. The handoff's first
  draft read the all-time amount as the 365d one — two windows, one sentence.
  Invisible on every page: every endpoint defaults to `Query("retail")`.
  Measured 2026-08-08, by manager, 365d:

  | manager | orders | revenue |
  |---|---:|---:|
  | 7 · Dasha Benzel | 244 | ₴855,163.48 |
  | 5 · Olga A | 150 | ₴215,821.92 |
  | 3 · Anna Artamonova | 101 | ₴206,125.64 |
  | 34 · Ірина Б | 44 | ₴75,187.70 |
  | 28 · Kety Slabenko | 470 | ₴61,797.20 |
  | 40 · Kate Roshko (blocked) | 5 | ₴10,135.00 |
  | 21 · Світлана З | 1 | ₴0.00 |

  Who is wholesale, who is internal, who is a blogger? PR #52 adds the endpoint
  that records the answer — `POST /api/managers/{id}/retail-status` — and makes
  it survive the next sync. The answer itself is yours.
- **Orders billed at zero with line items: 1,182 / ₴4,190,078.36**, ongoing (8 in
  Aug 2026). Revenue reads `grand_total`, product pages read line items.
- **Uncategorised product revenue: ~₴13–27M** depending on window; FK checks are
  clean, the damage is NULLs.
- **Orders with revenue and no line items: 523 / ₴1,422,610.30**, 427 of them
  April 2025. Repairable — the repair exists but is gated behind the failing
  reconciliation job (P2).
- **`status_id=20`** is revenue in `core/models.py` and a cancellation in
  `core/data_quality.py`. ₴79,805.70 on the seam.

## 7. Operational facts worth not rediscovering

- Never open the live DuckDB file. `cp` while `keycrm-web` runs gives a torn
  snapshot that mimics index corruption. Use `/opt/key-api-bot/data/backups/`,
  read-only, from a throwaway container with `--user 0`.
- `warehouse_refreshes` (~65k rows back to 2026-03-14) is the best forensic trail
  in the system.
- Deploys are gated on the `production` environment — a run sitting in `waiting`
  waits for a human, not a runner.
- `IntervalTrigger` recomputes its next fire from construction time; `_add_job`
  rebuilds triggers on every start, so long intervals never fired. `CronTrigger`
  is immune.
- Host has **7 GB RAM total**, 4 cores. DuckDB's ceiling is deliberately below the
  container's.
- `data_quality_issues` held **one row in its entire history** — the persistence
  path was built and starved.

---

## 8. What the night of 2026-08-08/09 should produce

Nothing below needs a human. Read it in the morning and compare.

| time (Kyiv) | job | expected |
|---|---|---|
| 03:31, 05:31 | `halfwritten_repair` | 323 → 123 → 0 orders without line items |
| 05:00 | `dq_reconciliation` | second consecutive PASS — a mode, not a fluke |
| 09:00 | `dq_digest` | first digest ever; `orders_without_line_items` with a delta, not `unchanged` |

**ACTUALS, 2026-08-09 11:45.**

- `orders_without_line_items`: **gone**. Three batches (200 + 200 + 123),
  every one repaired, `still_empty=0` throughout — 523 orders and
  ₴1,422,610.30 of line items were never missing upstream, only undelivered.
- `dq_digest` fired at 09:00 and delivered (`sent=True`) — the first digest
  the system has ever sent.
- `dq_reconciliation` **did not run** (see the correction in §3). The #49
  watchdog said so at 11:02:10, its first real catch:
  `Canary alerting: ['data quality: last successful reconciliation run was
  30h ago (>30h)']`. 79 days of silence became 30 hours.
- The digest did *not* flag it at 09:00, correctly: the age was 28 h against a
  30 h threshold. Two instruments, two thresholds, working as designed.

Baseline taken 2026-08-09 01:44 (integrity run 135, after one manual repair batch):

- `orders_without_line_items`: **323**, ₴454,900.45 — was 523 / ₴1,422,610.30
- `headline_vs_line_items`: **1,184**, ₴4,196,208.36 — will not move; it is the
  opposite illness (line items present, `grand_total` zero) and needs a
  business decision, not a job.

The manual batch repaired **200 of 200**, `still_empty=0` — KeyCRM had the line
items for every one. A 429 landed mid-run on `order/6052` and #37's retry rode
it out (54 s backoff, zero failures), which is the first time that fix has been
exercised under load outside the reconciliation job.

If the 05:00 reconciliation fails instead, the #49 watchdog says so at the first
canary poll after 11:00, when the last verdict passes 30 h.

## 9. What is left

```
P3-a  cheap guard: silver cells EXCEPT gold cells (~7.5 ms, inline) — do this
P3-b  full per-cell recompute, REPORT-ONLY — hold until the 09:00 digest has
      been seen working at least once (the plan's own sequencing rule), and
      never wire it into validation_passed (architect's veto)
```

Open business questions are in §6. The manager one now has a place to record
the answer: `POST /api/managers/{id}/retail-status`.
