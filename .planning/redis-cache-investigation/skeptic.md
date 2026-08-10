# THE SKEPTIC — verdict on "add Redis" vs "add an in-process cache"

**Both proposals are solutions to a problem production does not have.**
Measured on prod, 2026-08-09/10.

## Headline numbers

| Claim under test | Measured on prod | Source |
|---|---|---|
| Gateway timeouts (504) | **0** in 4.2 days | nginx access log |
| `/api/dashboard/batch` requests | **0** in 4.2 days; **0** references in frontend source | nginx log + `grep -rn dashboard/batch web/frontend/src` |
| Timeout / slow-query lines in web log | **0** of 4,620 lines | `docker logs keycrm-web` |
| Dashboard API traffic | **375 req/day** (1,577 cacheable 200s / 4.2 d) | nginx log |
| Hot query cost @ 45,810 orders | **1.2 – 20.9 ms** | probe on nightly backup |
| Cache hit rate at code's own TTL (60s) | **0.1 % — 2 hits in 4.2 days** | log replay |
| Warehouse refresh lock-hold | p50 **1,479 ms**, 176 rebuilds / 6.2 h | live web log |
| DuckDB time: refresh vs *all* dashboard queries | **~1,007 s/day vs ~5.6 s/day = 180×** | both above |
| keycrm-web CPU / RAM right now | **0.21 %** / 730 MiB of 7 GiB | `docker stats` |

## 1. Does the problem exist in production today? No.

`docker stats`: keycrm-web at **0.21 % CPU**, 730 MiB / 7 GiB. Host load average
**0.43** on 121 days uptime, 6 GB of 7.7 GB available.

**Zero 504s in 4.2 days.** Full status distribution over 34,179 nginx lines:
```
13211 429   6383 200   3660 401   3344 499   3191 302   2893 301   422 500   68 502   0 504
```
Every one of those error classes decomposes to something that is not latency:

- **3,317 of 3,344 499s** and **416 of 422 500s** are `POST /api/webhooks/turbosms`.
- **68 502s**: 43 on `/ws/dashboard`, rest split — all match `connect() failed
  (111: Connection refused)` in the error log, i.e. deploy/restart windows, not timeouts.
- Dashboard endpoints across 4.2 days: **14 total 499s**, **0 500s**, **0 504s**.

`docker logs keycrm-web` (4,620 lines): `grep -icE "timeout|timed out"` → **0**.
`slow` → **0**. `_safe_fetch|section failed` → **0**. The only cache-related lines
in the entire log are the two that confirm Redis is absent:
```
core.cache - Redis connection failed: Error 111 connecting to localhost:6379.
web.main  - Redis cache not available, running without cache
```

**Who uses this thing.** 1,237 dashboard API requests over 4.2 days across ~12
client IPs, clustered in office hours. That is roughly **25 page loads per day**.
Traffic by day: 254 / 736 / 182 / 9 / 56. One busy day, then near-silence.

**The one real load event proves the opposite of the cache case.** On 2026-08-05
the TurboSMS webhook peaked at **3,654 requests in a single minute** (61 req/s)
against the single-worker uvicorn. During that hour the dashboard served **85 API
requests, all HTTP 200** — no 504, no 499, no 502. A single worker with no cache
absorbed a 61 req/s flood while serving users perfectly.

## 2. If there were a latency problem, where would it live? The lock — and it is 1.2 % of the day.

`core/duckdb_store.py:147` `self._lock = asyncio.Lock()  # Serializes all database
access`, and `connection()` does `async with self._lock: yield self._connection`.
So reads **do** block behind `refresh_warehouse_layers`. Confirmed, not assumed.

Live measurement from the running container (6.2 h window, post-compact):
```
SCHEDULED RUNS: 185      ACTUAL REBUILDS: 176      (95 % — the changed_ids gate saves almost nothing)
duration: n=176 min=1400 p50=1479 p95=1563 max=2573 ms
```
Cross-checked against `warehouse_refreshes` in the nightly backup (7 days):
`dirty_flag n=3311, p50=1492, p95=1837, p99=2660, max=3382 ms`; **3,259 of 3,318
refreshes exceed 1 s**; duty cycle 0.78–1.10 % of each day. Two independent
sources, same answer. The 2026-08-09 compact (4.6 GB → 741 MB) did **not** reduce
refresh cost.

**The arithmetic that ends the debate:**
- Refresh: 176 rebuilds / 6.2 h → ~681/day × 1.479 s = **~1,007 s/day** of lock-held DuckDB compute.
- All dashboard traffic: 375 req/day × ~15 ms = **~5.6 s/day**.
- The background job burns **180× more DuckDB time than every human request combined.**

Collision exposure: refresh every ~127 s holding the lock 1.479 s → **1.2 % duty
cycle**. A page load is a burst of ~15 requests spanning ~1 s, so
(1.479 + 1.0) / 127 ≈ **1.95 % of page loads** hit a refresh and wait ≤1.5 s extra.
At 25 page loads/day that is **one slightly-slow page load every two days**. That
is the entire measurable latency problem in this system.

Ranked causes, by evidence:
1. **Warehouse refresh lock-hold** — real, measured, 1.2 % duty cycle, ≤1.5 s penalty. Only real cause.
2. **Frontend fan-out** — ~15 requests per page load, incl. 3 separate
   `/api/summary?...&source_id=1|2|4` calls. Serialized behind one lock:
   ~15 × 15 ms ≈ 200–300 ms. Real but invisible. Only **1 duplicate request in
   1,577** (0.1 %) — there is no double-fire bug worth naming.
3. **Single worker** — `Dockerfile.web:67` `CMD ["uvicorn","web.main:app",...]`,
   no `--workers`. Irrelevant: DuckDB is one serialized connection, so extra
   workers would queue on the same lock. Absorbed 61 req/s anyway.
4. **Per-query cost** — ruled out. 1.2–20.9 ms at prod volume.
5. **Missing cache** — ruled out, see §4.

## 3. Attacking A8-1 directly

**Where A8-1 is right (concede fully):** every structural claim is true and still
true today. No Redis on prod (`docker ps`, `docker-compose.yml` — never had one).
`cache.get/set` no-ops. `get_or_set` and `@cache.cached` appear **only in
`tests/integration/test_cache.py`** — zero route call sites; the sole production
call site is `web/routes/api/analytics.py:338,350`. "Cache warming (every 5
minutes)" at `core/scheduler.py:8` is a lie in a docstring with no job behind it.
CLAUDE.md's "in-memory cache 5-min TTL, background warming every 4 minutes"
describes a system that has never run. **A8-2's three-different-TTLs point is also
correct** (60 s at the call site, 300 s `DEFAULT_TTL`, "5 minutes" in CLAUDE.md).

**Where A8-1 is wrong — its entire impact claim:**

> "This is the *standing* cause of the batch 30s timeouts attributed to A6-5."

The 30 s figure is `web/routes/batch.py:267`
`asyncio.wait_for(fetcher(**params), timeout=30.0)`. That endpoint was requested
**zero times in 4.2 days** and is referenced **nowhere in the React source**. A8-1
names as its impact the timeouts of an endpoint production does not call, and
those timeouts have never appeared in any log I can reach. A8-1 was written from
code reading plus a local snapshot and was never checked against a request log.
Its severity 6 is unsupportable; on measured impact it is **severity 2 — a
documentation defect**.

> "serialized × many widgets × no cache = visible latency"

Measured. "Many widgets" is 15 requests at 1.2–20.9 ms each = 200–300 ms once, by
~12 people, ~25 times a day. Not visible.

A8's scaling note survives and gets better: it projected on 24,794 orders; prod is
45,835 (1.85×) and queries still run in single-digit-to-20 ms. **A8-5's "no time
cliff" conclusion is confirmed at 1.85× real volume.**

**A8-3 is the finding that deserved severity 6, not A8-1.** Its mechanism is
partly fixed — the log now reads `Warehouse dirty — refreshing (changed_ids=208)`,
which is exactly the fix A8-3 proposed — yet **176 of 185 scheduled runs (95 %)
still rebuild**. The gate saves 5 %. That is the ~1,007 s/day and the only thing
that ever holds the lock long enough to be felt.

## 4. The cheapest correct action

**Do not add Redis. Do not add a general in-process cache. Fix the documentation.**

The killer measurement — I replayed all 1,577 cacheable `GET /api/* → 200`
requests from the real access log against a TTL cache:

```
TTL=    5s  hit-rate=  0.1%      TTL=  300s  hit-rate= 29.7%
TTL=   30s  hit-rate=  0.1%      TTL= 3600s  hit-rate= 52.7%
TTL=   60s  hit-rate=  0.1%   <-- the TTL the code actually uses
```

**At the TTL already written into `analytics.py:350`, a working cache would have
served 2 requests in 4.2 days.** The 0.1 %→29.7 % jump at 300 s is one poller: the
most-repeated URL (`/api/summary?period=yesterday&sales_type=retail`, n=126) has a
**p50 inter-arrival gap of 120 s**, so a 60 s TTL misses it by construction — and
120 of its 126 hits landed on a single day (Aug 6) across 11 IPs, i.e. a shared
morning-report link, not a standing load.

Even the generous case: TTL=300 s saves 469 requests over 4.2 days = 112/day ×
~15 ms = **~1.7 seconds of DuckDB per day**, against the refresh's 1,007 s/day.
**0.17 %.** And a 300 s TTL exceeds the 127 s refresh interval, so it buys that
0.17 % by serving numbers up to two rebuilds stale on a dashboard whose purpose is
"is today's revenue right." That is a bad trade at any hit rate.

Against Redis specifically: a fifth container, a network hop, a new failure mode
and a persistence story, to save 1.7 s/day of CPU on a box idling at 0.21 %.

Against a general in-process cache: **the codebase already has four of them**, put
exactly where evidence demanded — `web/routes/api/health.py:24` `_STATS_CACHE_TTL
= 60` (the endpoint UptimeRobot and the 30 s Docker healthcheck actually hammer;
`/api/health` returns `latency_ms: 0.0`, i.e. that cache is hitting), plus
`web/routes/api/admin.py`, `web/services/category_service.py`,
`web/services/brand_service.py:111` `_product_brand_cache`. The pattern is present
and correctly applied. Generalising it to endpoints with a measured 0.1 % hit rate
is cargo cult.

**Do this instead, in order:**

1. **(free, today) Delete the lies.** `core/scheduler.py:8` "Cache warming (every
   5 minutes)"; the CLAUDE.md Caching section ("5-minute TTL", "background warming
   every 4 minutes"). These docs are the *only* reason this debate exists — two
   agents are arguing about restoring a system that never ran. Either delete
   `core/cache.py`'s dead `get_or_set`/`@cache.cached` or mark them unused. This
   dissolves A8-1 and A8-2 at zero risk.
2. **(the actual money) Attack the 1,007 s/day refresh**, not the 5.6 s/day of
   reads. 95 % of scheduled runs still rebuild. Either the changed-set is still
   inflated or 208 orders genuinely change every 2 min — find out which, then
   either widen the interval or shrink the lock's critical section. This is
   180× the prize a cache offers and it is the *only* thing that ever makes a
   user wait.
3. **(bigger than both) Fix the webhook.** 20,618 requests, **zero HTTP 200s**:
   3,655×401 (bad signature), 13,211×429, 3,317×499, 416×500. The handler's own
   docstring says "TurboSMS retries for 4.5 hours on any other status" — the 401
   at `web/routes/api/webhooks.py:63` starts a retry storm that trips the
   `120/minute` limiter at `:34` into 13k 429s. **Every delivery receipt from the
   Aug 5 campaign was dropped**, so campaign delivery measurement is blind. The
   rate limiter did its job and protected the app; the signature check is the bug.
   Out of scope for this debate, but it is worth more than the cache by orders of
   magnitude and no one has reported it.

## 5. Conditions under which I am wrong

- **A user reports the dashboard is slow.** My whole case is that nobody is
  waiting. One credible complaint outranks all of it — but then instrument first:
  add `$request_time $upstream_response_time` to the nginx `log_format` (currently
  plain `combined`, so I have **no direct latency data at all** — see coverage
  below) and find out whether it is the lock, the fan-out, or the browser.
- **Concurrency rises ~50×.** At ~25 page loads/day the lock's 1.2 % duty cycle is
  harmless; if 30 people load dashboards simultaneously, 15 serialized requests
  each behind one connection queues badly. The fix then is still not a cache —
  it is a read-only second DuckDB connection or a shorter lock. A cache would only
  hide it until the TTL expires and everyone stampedes (A8-2's exact point, which
  I concede in full).
- **Traffic becomes periodic under 300 s.** The 29.7 %-at-300 s figure is one
  shared link on one day. If a wall-mounted dashboard polls every 60 s forever,
  hit rate goes high and a **60 s in-process TTL on `/api/summary` and
  `/api/revenue/trend` alone** becomes correct. It is not correct today.
- **A 504 exists outside my window.** nginx has been up since 2026-08-05 13:32,
  `json-file` driver with no rotation, so my access log is exactly 4.2 days.

## Coverage — what I did NOT check

- **No direct latency percentiles.** nginx `log_format` is default `combined`;
  neither `$request_time` nor `$upstream_response_time` is logged. **I cannot
  produce a real p50/p95/p99 for any endpoint.** Every latency number here is
  inferred from (a) DuckDB query cost measured on the backup, (b) measured
  lock-hold duration, (c) absence of 499/504. The one end-to-end figure I have is
  `/api/health` at ~10.5 ms through TLS+nginx — and that is a `_stats_cache` hit
  (`latency_ms: 0.0`), so it measures the stack, not a query.
- **4.2 days of nginx log, 6.2 hours of web log** (container restarted 2026-08-09
  14:15). No visibility into the 2026-08-02 Gold-truncation OOM or any earlier
  incident from logs.
- **No authenticated endpoint timing.** `/api/warehouse/status` returns 401 to me;
  I did not use anyone's session. Per-endpoint server-side latency is unmeasured.
- **No load test**, by instruction. The 61 req/s webhook flood is natural-experiment
  evidence, not a controlled test.
- **Query costs measured on the 2026-08-09 01:30 backup (4.6 GB, pre-compact)**,
  read-only in a throwaway container. Live is 741 MB post-compact, so these are an
  upper bound.
- I did not verify whether the TurboSMS 401s are a wrong secret or a signature-
  format mismatch, and I did not confirm the two source IPs belong to TurboSMS.
- I did not check WebSocket load. 182 `/ws/dashboard` requests, 138 upgraded (101).

---

# CROSS-EXAMINATION

## 1. The coverage gap was my error. Conceded without qualification.

`web/middleware.py:105-113` logs `duration_ms` for every request. I grepped that
exact log for `timeout` and `slow` and never for `duration_ms`. I then wrote "I
cannot produce a real p50/p95/p99" — asserting absence from a search I did not
run. My own notes file carries the rule I broke: *"Don't conclude from absent
data."* Concede fully.

I re-extracted it independently (`/tmp/lat.py`, regex on `Request completed` +
`status_code` + `duration_ms`) and reproduce the coordinator's numbers exactly:

```
GET/POST /api/* 200:  n=72  p50=171.4  p75=209.3  p90=220.1  p95=323.7  max=323.9
<=50ms: 28/72 (39%)   >200ms: 33/72 (46%)
```

**What does not survive.** My sentence *"nobody is waiting"* is too strong and I
withdraw it. People do wait 171–324 ms for a page. Worse, my estimate
"~15 × 15 ms ≈ 200–300 ms, not visible" landed on the right number by wrong
reasoning: I assumed the cost was DuckDB-bound at ~15 ms/request. It is not —
see §2. Right answer, wrong arithmetic, and it should not have been arithmetic
at all when the measurement was one grep away.

**What survives untouched.** Traffic volume (375 req/day, ~25 page loads, ~12
IPs). Cache hit rate. Zero 504s. Zero timeout/slow lines. The 180× refresh-vs-reads
ratio. The webhook finding. And the top-line judgement: 324 ms on a full-page
fan-out is not an outage, and there is still no evidence of one.

**One correction to the framing, in my favour.** This is not "the only direct
latency dataset" in a robust sense: **all 72 requests come from one user session
on one day**, 17:41:36–18:15:16 UTC, inside a 6.2-hour log window. n=72 is thin,
and it is n=1 in sessions. That matters for §2, because every plateau in the
dataset comes from that one session.

## 2. The decisive experiment: neither (a) nor (b). It is (c) — single-threaded event-loop serialization of Python handler work.

### (a) The lock is refuted, four ways

**i. A 207 ms plateau with the lock provably free.** I derived every refresh's
window as `[log_timestamp − duration_ms, log_timestamp]` (177 refreshes parsed):

```
refresh 17:41:36.526 -> 17:41:38  (1474ms)
refresh 17:43:36.535 -> 17:43:38  (1465ms)
```
The 17:41:42 burst — 7 requests, durations 206.9–217.7 ms, spread 10.78 ms —
started at **17:41:41.79**. That is ≥3.8 s after the previous refresh released
and 114 s before the next one acquires. Both log clocks are UTC (verified: the
prefix reads `20:19:37` while APScheduler prints `23:19:37 EEST`, UTC+3). Under
any ±1 s slop the windows do not touch. **The plateau reproduces with the lock
free.**

**ii. No aggregate correlation.**
```
SLOW (>150ms): 7/37 overlap a refresh (19%)
FAST (<=50ms): 6/28 overlap a refresh (21%)
```
Slow requests are, if anything, marginally *less* likely to coincide with a
refresh. If the lock caused the plateaus this ratio would be inverted and stark.

**iii. No request in the dataset has a lock-shaped wait.** The lock is held for
1474 ms in one indivisible block (`refresh_warehouse_layers` holds `self._lock`
across Silver+Gold+validation). A request arriving mid-refresh must wait up to
1474 ms. **The maximum duration anywhere in 72 requests is 323.9 ms.** Waits of
the required magnitude simply do not occur. The coordinator asked whether 207/324
ms is "consistent with arriving mid-refresh" — it is not: the 17:41:37 burst
started at 36.676, i.e. 150 ms into a refresh that ran to ~38.0, so a lock-blocked
request would have shown ~1320 ms. It showed 324 ms.

**iv. The shape is wrong for a mutex.** This is the clue the coordinator pointed
at, and it resolves cleanly. A mutex hands off one waiter at a time, so N queued
requests exit **staggered** at cumulative sums (8, 19, 50, 121, … 428 ms). The
observed plateaus are **flat**:

| burst | n | sum(durations) | n × max | ratio |
|---|---|---|---|---|
| 17:41:37 | 7 | 2265.6 | 2267.3 | **0.9992** |
| 17:41:42 | 7 | 1461.0 | 1523.9 | 0.959 |

A ratio of 0.9992 means all seven progressed *concurrently* and finished within
0.61 ms of each other. That is round-robin scheduling of interleaved coroutines.
**A mutex cannot produce it.** The arithmetic the coordinator flagged says the
same thing: sum of solo costs (~381 ms for those six endpoints, using each
endpoint's observed minimum) **exceeds** the 324 ms wall time. Work that overlaps
is not work behind one mutex.

### (b) The CPU cap is not it either

```
host: 4 cores.  keycrm-web: NanoCpus=1000000000 (1.0 CPU)
cpu.stat: nr_periods 82693   nr_throttled 94 (0.11%)   throttled_usec 1408276
          usage_usec 539467279
```
**1.4 seconds of CFS throttling in total** over the container's 6.2-hour life.
The ten bursts alone would need ~2.5 s. Hard throttling is real but is not the
mechanism.

And the proposed one-line fix does not work: **uvicorn runs one event loop on one
thread.** A single-threaded loop cannot consume more than 1.0 CPU, so raising
`cpus: 1.0 → 2.0` changes nothing for this workload. I took the coordinator's
warning and did not lean on the 0.21 % point-in-time sample — I used the cumulative
cgroup counters instead, and they say the same thing.

### (c) What it actually is

Concurrent handlers time-slicing on one event loop, and the work being sliced is
**Python, not DuckDB**:

| | measured DuckDB cost @ prod volume | observed endpoint minimum |
|---|---|---|
| `/api/customers/insights` | 4.3 ms | 130.0 ms |
| `/api/products/performance` | 17.1 ms | 111.3 ms |
| `/api/products/top` | 20.9 ms | 70.5 ms |
| `/api/brands/analytics` | 19.2 ms | 28.4 ms |
| `/api/sales/by-source` | 2.6 ms | 31.9 ms |
| `/api/revenue/trend` | 3.9 ms | 8.9 ms |
| **sum (6)** | **68.0 ms** | **381.0 ms** |

DuckDB is ~18 % of handler cost. The other ~82 % is Python in the handler body.

### Which side does this favour? Honestly: the cache advocates, on mechanism.

A cache **hit** skips the whole handler body — query *and* the Python
transformation — leaving only routing and serialization (~5–10 ms, cf. `/api/summary`
min 7.8 ms, `/api/brands` 15.1 ms). A fully-warm 7-request burst would cost
~56 ms instead of 324 ms: **an ~83 % cut.** ADVOCATE-REDIS was right that this was
the assumption that could break my case, and on mechanism it does. I concede it.

### And here is what kills it anyway — measured, not inferred

The session that produced **every** 324 ms and 207 ms plateau in the dataset is a
user stepping through dates: `period=month` → `period=yesterday` →
`start_date=2026-08-07` → `08-06` → `08-05` → `08-04`. Joining that window in the
nginx log (which keeps query strings):

```
09/Aug 17:41:30-17:42:40:  requests=54  distinct URLs=54  repeats=0
  -> achievable hit rate at ANY TTL: 0.0%
```

**Fifty-four requests, fifty-four distinct cache keys, zero repeats.** A cache
would have hit **exactly zero** of the 33 responses over 200 ms. The slow requests
are precisely the uncacheable ones — because the parameter being varied *is* the
thing the user is interacting with. Date-stepping is cache-hostile by
construction, and date-stepping is what this dashboard is for.

That is my case restated on measurement instead of inference, and it is stronger
than the version I filed.

## 3. The two counters

### 3a. Scoped invalidation on refresh completion — I concede the freshness point and withdraw my objection, but it costs him a third of his headline.

ADVOCATE-INPROC is right: with invalidation on refresh completion, TTL is a
backstop and the served data is strictly fresher than today's blind 60 s TTL. My
"serves numbers two rebuilds stale" objection is wrong and I withdraw it.

But the same design caps the **effective** TTL at the refresh interval, because
the refresh fires every ~127 s and actually rebuilds on **176 of 185 runs (95 %)**
— so it invalidates nearly every cycle. I replayed real traffic at that ceiling:

```
TTL=  60s  hit=  0.1%  (2/1577)     <- today's hardcoded value
TTL= 120s  hit= 11.6%  (183/1577)
TTL= 127s  hit= 20.2%  (319/1577)   <- the invalidation-capped ceiling
TTL= 180s  hit= 21.2%  (334/1577)
TTL= 300s  hit= 29.7%  (469/1577)
```
His correctness fix is real, and it moves his best case from 29.7 % to **20.2 %**.
And on the only session we have latency for, it is still 0.0 %.

### 3b. The four existing caches — conceded entirely, and then I tested the concession and it failed.

Yes: the mechanism is already in the codebase (`web/routes/api/health.py:24`,
`admin.py`, `category_service.py`, `brand_service.py:111`), it is free, I endorse
it, and the only question left is which endpoints qualify. Full concession.

So I went looking for the strongest remaining candidate class on his behalf —
**parameterless reference endpoints** (`/api/categories`, `/api/brands`,
`/api/promocodes`: 49 requests each, near-static data, one cache key, no key
explosion). If any endpoint qualifies, it is these:

```
parameterless  TTL= 60s  hit= 0.5%  (1/184)
parameterless  TTL=127s  hit= 2.2%  (4/184)
parameterless  TTL=300s  hit=10.3%  (19/184)
```
**2.2 % at the capped TTL.** The concession I was about to make fails its own
test, so I do not make it. I am not handing over an endpoint list, because the
data does not support one.

The qualification rule the codebase already applies is the correct one: cache
where the **same key** recurs faster than the TTL. `/api/health` qualifies —
one key, UptimeRobot plus a 30 s Docker healthcheck, ~3,000 hits/day, and it is
already cached and hitting (`latency_ms: 0.0`). Date-parameterised analytics never
will.

## Revised bottom line

My conclusion is unchanged; one of its supports was rotten and has been replaced,
and the action list gains a new item at the top.

0. **(new, and the real defect) Instrument.** Add `$request_time
   $upstream_response_time` to the nginx `log_format` and retain the middleware
   `duration_ms` beyond a container restart. Seventy-two requests from one session
   in 6.2 hours is not a latency dataset. That A8 argued this from a local
   snapshot, that two advocates argued it from code, and that I argued it from
   inference — for this long, over data this cheap to collect — is the finding.
1. **Delete the lying docs** (`core/scheduler.py:8`, CLAUDE.md Caching section).
2. **Attack the 1,007 s/day refresh**, still 180× the read cost, still rebuilding
   on 95 % of runs.
3. **Fix the TurboSMS webhook** — 20,618 requests, zero HTTP 200s, every Aug-5
   delivery receipt dropped. Still the largest thing in this log by a wide margin.
4. **(new, ranks above any cache) The 324 ms is Python, not DuckDB** — handler
   bodies run 5–30× their query cost. If someone wants that number down, make the
   handlers cheaper or stop the frontend firing 15 requests per page. Both beat
   memoizing keys that never repeat.

**Where I am now wrong** (updated): if a session appears whose URLs *do* repeat
inside ~127 s — a wall-mounted dashboard on a fixed period, or several people on
the same shared morning-report link — then hit rate goes high, the ~83 % burst
reduction I conceded in §2 becomes real, and a 60 s in-process TTL on
`/api/summary` and `/api/revenue/trend` is correct. The 2026-08-06 spike (120 hits
on one URL, 11 IPs) is the closest thing to that in the log, and it lasted a day.
Instrument first (item 0); the answer arrives for free.

**Coverage, updated.** Still no nginx-level latency. Latency now measured but
n=72 from a single session. No authenticated endpoint timing. `cpu.stat` is
cumulative — I could not sample throttling *during* a burst, only bound it over
the container's life. I did not profile the handlers to say what the ~82 %
non-DuckDB time is actually spent on; that is asserted as a residual, not
observed directly.
