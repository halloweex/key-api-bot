# Redis / cache investigation — handoff

**Date:** 2026-08-10 · **Status:** 4 of 5 PRs deployed to prod, #70 held deliberately
**Visual writeup:** https://claude.ai/code/artifact/b1e61026-dfcd-413e-9c20-074ee4460eb6
**Full agent reports:** `advocate_redis.md`, `advocate_inproc.md`, `skeptic.md` (this directory)

---

## The question and the answer

**Question:** "what's up with Redis, we had some problem."

**Answer: Redis is not needed, a general cache is not needed, and the dead layer has been
deleted.** Three engineering agents with incompatible mandates (pro-Redis, pro-in-process,
sceptic) argued it out under a repro-or-it-doesn't-exist rule and two rounds of
cross-examination. **All three conceded their positions.**

### Why the cache was dead, and why that is not a regression

| Date | Commit | What happened |
|---|---|---|
| 2026-01-13 | `a4f519d` | In-process `AsyncCache` added. Used by `dashboard_service.py`, which fetched from KeyCRM over **HTTP**. |
| 2026-01-14 | `19b91be` | "Core. DuckDB added. **Cache removed**." Removed deliberately, the next day, in the commit that brought DuckDB. The reason for it had vanished. |
| 2026-01-29 | `641952b` | `core/cache.py` rewritten into `RedisCache` (232 → 513 lines), `redis>=5.0.0` added. **A Redis container was never added, ever** — `git log -S redis -- docker-compose.yml` is empty. |

So the Redis layer was designed for a problem that had stopped existing two weeks earlier,
and it never had a backend. A later audit (A8-1) read the module and its docstrings, rated
the missing cache **severity 6**, and named it the standing cause of dashboard latency.
That was a documentation defect, not a performance defect.

### The measurements that settled it

Replaying the real production request stream (nginx access log, 4.2 days):

| Scenario | Hit rate |
|---|---|
| `ttl=60` — what the code actually used | **0.1 %** (1–2 requests in 4 days) |
| `ttl=300` | 29.7 – 37.6 % |
| `ttl=300` with honest invalidation | **2.8 %** — the refresh declared 583 dates affected every 2 min, so overlap matched every dashboard key |
| `ttl=300`, fresh page loads only | 3.2 % (9 hits / 283 over 3.55 days) |
| **The session that produced every slow response** | **0.0 % at any TTL** |

That last row is the one that ends it. The slow session was a person stepping through dates
(`month → yesterday → 08-07 → 08-06 → 08-05 → 08-04`): **59 requests, 56 distinct URLs**, the
only repeats being four `/api/health`. A cache would have hit **none** of the 33 slow
responses. The slow requests are precisely the uncacheable ones.

### The latency plateau, and what it is not

The only user-visible defect anyone demonstrated: seven endpoints finishing within a
**0.61 ms window at 323 ms**, versus 8–148 ms for the same seven run alone. Server-side
data from `duration_ms` in `web/middleware.py` (n=72): p50 171.4 ms, p95 323.7 ms, 46 % over
200 ms, 39 % under 50 ms.

- **Not the DuckDB lock.** A plateau reproduced 3.8 s after a refresh released it
  (refresh ran 17:41:36.5→38.0, plateau at 17:41:41.8). Slow requests overlap a refresh
  19 % of the time; fast ones 21 %. No correlation.
- **Not the `cpus: 1.0` cap.** 1.4 s of throttling per 6.2 h (94 periods of 82 693). And
  raising `cpus` cannot help: uvicorn is one event loop on one thread.
- **Shape rules out a mutex.** `sum/(n×max) = 0.9992` — flat. A mutex releases in a
  staircase summing to 427.8 ms, not a flat 323.

**Still unresolved.** The sceptic concluded "event-loop serialisation of Python, DuckDB is
only ~18 %". **That 18 % does not reproduce.** Profiling the service layer directly (local
dev DB at 8.75 GB = 12× prod, so an upper bound on DuckDB's share) gives **DuckDB 98–99 %**,
Python reshaping 1–2 %, four endpoints totalling ~29 ms of service work. So the missing time
is neither the queries nor the reshaping — it is elsewhere in the request stack (auth,
validation, serialisation, gzip, middleware) or contention. **Do not refactor the 211
blocking call sites until per-stage timing on prod says where the time goes.** #68 is now
collecting the first half of that data.

---

## What shipped (all live in prod, all verified on the running server)

| PR | Change | How it was verified |
|---|---|---|
| **#69** | Truthful cache docs; removed the `core/scheduler.py` docstring line claiming a warming job that never existed | deploy success |
| **#67** | TurboSMS webhook: rate limit 120 → 600/min, per-condition rejection counters + throttled alert, **both signature orders accepted**, `scripts/check_turbosms_signature.py`, index on `sms_campaign_members(message_id)` | synthetic callback against prod with the real secret: both orders → 200, forgery → 401 |
| **#68** | nginx `rt=$request_time urt=$upstream_response_time`; bounded json-file log rotation (50 MB × 5) on all four services | `rt=0.004 urt=0.004` present in live nginx logs |
| **#72** | Deleted the Redis layer entirely | **zero** redis lines in the web container's startup log (was 2), zero errors, dashboard 5xx = 0 |

**#72 removed:** `core/cache.py` (491 lines), the `redis` dependency, the boot-time connection
attempt to `localhost:6379` that failed and logged a warning at every start, the summary
get/set that did nothing, `/cache/stats` and `/cache/invalidate`, the `"redis"` block in
`/api/health/detailed` that reported `not_connected` forever, `CACHE_INVALIDATED` /
`CACHE_WARMED`, and `CacheConfig` (whose `warming_interval_seconds` configured a job that has
never existed).

---

## The three real defects found instead of a cache problem

### 1. Every SMS delivery report was being dropped — FIXED, awaiting real-world confirmation

The 2026-08-05 campaign produced **20 618 callbacks and not one HTTP 200**:
3 655 × 401, 13 211 × 429, 3 317 × 499, 416 × 500. Every delivery result for that send is
gone; the gateway has no replay and stops after 4.5 hours. Nobody knew for four days.

**Two independent faults:**

- **The rate limit was sized for a hand-typed request.** A send of N recipients produces up
  to N reports, ~98 % inside the same minute. At `120/minute` the limiter rejected **3.6×
  more callbacks than the signature check did** — a correct secret alone would still have
  lost most of the campaign. Now 600/min.
- **The signature order is undefined in the docs.** The 401s first looked like a wrong
  secret. The owner then confirmed `.env` matches the panel, and the value is clean (48
  chars, no quotes/whitespace/CR). TurboSMS's docs say only *"SHA1 hash of a string
  consisting of the secret security key and id"* — **no order, no worked example**. This code
  guessed `secret + id`. Both orders are now accepted (each still requires the secret, so
  nothing is widened) and the matching one is logged once as
  `TurboSMS webhook signature scheme confirmed: sha1(...)`.

**Ruled out along the way:** the envelope. Rejections were 401, not 400, so the body always
parsed as JSON and the top-level fields were read. It is the hash that disagreed.

> **NOT PROVEN.** That the concatenation order was the cause is the best remaining
> hypothesis after wrong-secret, dirty-variable and body-format were eliminated. **Only a
> real callback from the gateway proves it.** The next campaign is the test.

### 2. Every DuckDB read blocks the event loop — NOT ACTED ON

```
blocking conn.execute in core/repositories/*.py :  211
run_in_executor there                           :    0
callers of _execute_with_timeout (:242)         :    0   (definition only)
callers of _fetch_df (:343)                     :    0   (definition only)
```

`connection()` (`core/duckdb_store.py:228-238`) takes the lock and yields the **raw**
connection, so all 211 queries run synchronously on the event-loop thread. The
`ThreadPoolExecutor(max_workers=1)` created at `:185` specifically to offload blocking work
sits idle. The non-blocking path is written and has zero callers — the same disease as
`get_or_set`, twice in one codebase.

**Deliberately not fixed.** See the "still unresolved" note above: the share of response time
this would recover is contested, and a 211-site refactor in a system with a history of lock
deadlocks and OOM-truncated Gold needs a measurement first, not a guess.

### 3. The warehouse rebuilt what it looked at, not what changed — FIXED IN #70, NOT DEPLOYED

Gold was rebuilt for **583 dates every two minutes**, around the clock, while the logs
recorded `silver=45834, gold_rev=2059, gold_prod=86963` unchanged **176 times running**.
~1 007 s/day of DuckDB against ~5.6 s/day for every human request combined (**180×**), and
the largest consumer of the lock dashboard reads queue behind.

| Stage | Value | Multiplier |
|---|---|---|
| `changed_ids` marked dirty per cycle | 207 | — |
| `silver_scope` after the buyer cascade | 1 317 | ×6.4 |
| `gold_dates` rebuilt | **583** (~19 months) | ×2.8 |

**Only the first number was wrong.** The buyer cascade is **correct and must stay**:
`is_new_customer` depends on a buyer's whole history, so touching one of their orders really
does mean recomputing all of them, and ×6.4 is just the average history length of a repeat
customer. 583 dates follows honestly from 1 317 orders.

The defect is upstream: the sync window is the trailing 24 h, so `orders` is ~200 rows every
run whether or not anything moved, and the loop marked the **entire fetched window** dirty.
It gated on `stats["orders"]`, which counts a row already in the desired state as a success.
`upsert_orders` already computed the set actually written and threw it away.

**#70:** `upsert_orders` returns `UpsertResult.changed_ids`; only that set is marked dirty;
adaptive backoff follows the same number (keyed on the inflated count it never saw a quiet
period and sat on the 60 s floor all night).

**One thing the permanent dirtiness was hiding:** Silver reads only `orders`, but **Gold
joins products and categories**, so a renamed product changes gold rows with no order
involved — and the hourly catalog sync never marked anything dirty because it never had to.
#70 makes it ask for itself, or a rename made at night would wait for the next order.

---

## Next actions, in order

1. **Deploy #70** (the only open PR). Watch for the signature of success in the web logs:
   `gold_dates=` should disappear from refresh lines, and refreshes should become rare
   instead of ~681/day. Both new sync-cycle tests were confirmed to fail against the old
   gate before being kept.
2. **Next SMS campaign is the webhook's real test.** Success looks like
   `TurboSMS webhook signature scheme confirmed: sha1(...)` in the web log and delivery rows
   filling in. Failure now names its own cause via the per-condition counters instead of
   four days of silence. Once the order is known, drop the other one.
3. **Let #68 collect a week of `rt`/`urt`** before touching defect 2. Then add one per-stage
   timing inside a handler to find where the 323 ms actually goes.
4. **Housekeeping noticed in passing, not acted on:**
   - `/opt/key-api-bot/data/` holds ~13 GB of stale DB copies from the 2026-08-05 compact
     (`analytics.duckdb.old` 4.4 GB + four `.bak*` ~2 GB each) on a 75 GB disk at 61 %.
     Not deleted — the rule is never to remove a DuckDB file without checking what is in it.
   - The DB grew 728 MB → 1 064 MB in a day post-compact (+336 MB/day). That is the MVCC
     churn from the 583-date rebuild, i.e. the cost that #70 removes, measured in megabytes.
   - `tests/unit/test_gold_cell_values.py::TestTheAudit::test_the_validation_scalars_stay_green_meanwhile`
     is **intermittent** — failed twice in full-suite runs, then green six runs straight
     including the same suite on the same branch. No ordering plugin is installed, so
     something stateful leaks between tests. Unrelated to this work; not chased.

---

## Corrections recorded, so they are not re-derived

- **"The webhook secret is wrong."** Wrong. Inferred by elimination, not proven; the owner
  confirmed it matches the panel. The concatenation order replaced it as the hypothesis.
- **"Delivery reports are being lost right now."** Wrong. The storm ended 2026-08-06 17:27
  when the gateway gave up; zero webhook traffic since. The loss is bounded to the Aug 5
  campaign and to every future one until this is confirmed fixed.
- **"DuckDB is ~18 % of response time."** Does not reproduce; measured 98–99 % of the service
  layer. Where the rest of prod's wall clock goes is still unknown.
- **`test_gold_cell_values` is a stable pre-existing failure.** Wrong — it is intermittent.
- **`_key_locks` cleanup can key on `key_lock.locked()`.** Wrong, and this is a subtle one:
  releasing an `asyncio.Lock` wakes a waiter but does not mark the lock held until that
  waiter is scheduled, so `locked()` reads `False` while somebody is still queued. Deleting
  there hands the next arrival a fresh lock and lets two coroutines compute the same key at
  once. (Moot now — the file is deleted — but the trap generalises.)

## Method note

Three agents, incompatible mandates, repro-or-it-doesn't-exist, two rounds (build the case,
then cross-examination with the rivals' strongest evidence). Every headline number was
re-verified independently of the agents before being acted on — which is how the sceptic's
"no direct latency data exists" claim was caught (the data was in the same log he had read,
under a string he never grepped for). Prod was read-only throughout the investigation; the
only writes were the deploys and a synthetic webhook callback with a non-existent
`message_id`, which changes nothing.
