# ADVOCATE-REDIS — the case for a real Redis service + routing hot endpoints through it

All numbers below were measured today (2026-08-10) against prod (<vps-host>), read-only.
Raw artifacts: `/Users/vladislav/.claude/jobs/d47a98ec/tmp/nginx_access.log` (34 658 lines,
2026-08-05 13:32 → 2026-08-09 20:17, 4.28 d) and `.../web_timings.log` (288 lines,
web container uptime 6.1 h). Analysis scripts: `.../analyze.py`, `.../hitrate.py`,
`.../interarrival.py`.

**Bottom line up front, stated honestly:** the aggregate-CPU argument for caching is
*dead* — I killed it myself below (§1.5). The surviving, measured argument is
**user-visible burst latency**: a dashboard mount fires 7–21 concurrent queries that
serialize into a 207–324 ms wall-clock stall, and 46 % of all API responses exceed
200 ms while 39 % of them are ≤ 50 ms when run alone. A cache is the only thing that
shortens that chain. My specific claim — that it should be **Redis**, not an
in-process dict — rests on memory isolation against a documented OOM history and on
the fact that the Redis code already exists and the dict does not.

---

## 1. Measurement

### 1.1 Who actually uses this thing

| Metric | Value | Source |
|---|---|---|
| Total `/api` requests, 4.28 d | 24 291 | `analyze.py` |
| …of which **turbosms webhook** | 20 618 (85 %) | `analyze.py` |
| …of which UptimeRobot + canary | 1 769 | `analyze.py` |
| **Real dashboard `/api`** | **1 904 → 445/day** | `analyze.py` |
| Cacheable (200-OK, 15 analytic paths) | 1 455 → 347/day | `hitrate.py` |
| Distinct client IPs | 26 | `interarrival.py` |
| Human sessions (30-min idle split) | 46 → **11/day** | `interarrival.py` |
| Requests/session | median 15, mean 32, max 158 | `interarrival.py` |

This is a **~10-user internal dashboard doing 0.004 req/s**. I am not going to
pretend otherwise, and any argument I make that depends on throughput is invalid.

### 1.2 Latency is bimodal, and the bad mode is bursts

72 API 200-responses with server-measured `duration_ms` (`web/middleware.py:98-113`
logs every request):

```
p50=171.4ms  p75=209.3  p90=220.1  p95=323.7  max=323.9
<=50ms (unblocked): 28/72 = 39%
>100ms:             42/72 = 58%
>200ms:             33/72 = 46%
```

The shape is not noise. Verbatim from `web_timings.log`, one dashboard mount:

```
17:41:42  206.91ms  /api/brands/analytics
17:41:42  207.00ms  /api/products/performance
17:41:42  207.10ms  /api/revenue/trend
17:41:42  207.24ms  /api/summary
17:41:42  207.46ms  /api/customers/insights
17:41:42  207.62ms  /api/products/top
17:41:42  217.69ms  /api/sales/by-source
```

Seven endpoints finishing **within 0.7 ms of each other** at 207 ms. Compare the same
seven, twelve seconds later, when nothing else is in flight:

```
17:42:34    8.13ms  /api/summary
17:42:34   10.62ms  /api/revenue/trend
17:42:34   31.87ms  /api/sales/by-source
17:42:34   70.54ms  /api/products/top
17:42:35   28.38ms  /api/brands/analytics
17:42:35  130.03ms  /api/customers/insights
17:42:35  148.22ms  /api/products/performance
```

Same queries, same data, 8–148 ms solo vs a flat 207 ms in a burst. The burst is not
a staircase (30/60/90/…), it is a **flat plateau**: everyone waits for the whole chain
and then flushes together. Mechanically that is the single DuckDB connection behind
`asyncio.Lock` plus `cpus: 1.0` on the web container (verified:
`NanoCpus=1000000000`, host has `nproc=4`) — the executor thread doing DuckDB work
holds the core, and no coroutine gets resumed until the chain drains.

Burst structure from nginx (`hitrate.py`): **192 bursts of ≥4 concurrent requests**
in 4.28 d; busiest 1-second buckets contain 20, 16, 13, 13, 13, 11 requests.

**This is the entire user-visible problem, and it is the one thing a cache fixes**:
a cache hit takes no DuckDB lock, no executor slot, and no query CPU, so it does not
extend the chain.

### 1.3 Achievable hit rate — simulated on real traffic, not assumed

Replaying the 1 455 cacheable 200-OK requests through a shared server cache keyed on
full URL, with `setex` semantics (expiry not refreshed on hit):

| TTL | hits | hit rate |
|---|---|---|
| 30 s | 1 / 1455 | **0.1 %** |
| 60 s | 1 / 1455 | **0.1 %** |
| 120 s | 182 / 1455 | 12.5 % |
| **300 s** | **460 / 1455** | **31.6 %** |
| 600 s | 589 / 1455 | 40.5 % |

Median gap between two requests for the *same* key = **210 s**. This number is not a
coincidence — `web/frontend/src/hooks/useApi.ts:155-160` sets
`CACHE_TTL.REALTIME = 2*60*1000` for summary/revenue/sales. **TanStack Query already
absorbs every repeat under 2 minutes client-side.** The server cache only ever sees
what leaks past `staleTime`.

Two consequences, and I want the first one on the record because it is the single most
important operational fact in this debate:

> **The TTL hardcoded at `web/routes/api/analytics.py:350` is `ttl=60`. At 60 s the
> measured hit rate is 0.1 %.** If someone adds Redis today and changes nothing else,
> the cache does essentially nothing. `DEFAULT_TTL` is 300 (`core/cache.py:46`),
> CLAUDE.md says 5 minutes — three different numbers, and the one actually in the code
> is the only one that does not work. This is A8-2's "TTL doc is wrong" being worth
> far more than sev4.

Second: **hit rate is a lever, not a constant.** Key concentration is extreme —
186 distinct keys, but:

```
top  5 keys -> 42.3% of requests
top 10 keys -> 57.2%
top 20 keys -> 72.0%
top 30 keys -> 77.5%
```

Warming 20 keys after each warehouse refresh puts the ceiling at **~72 %**, not 31.6 %.
The docstring at `core/scheduler.py:8` claims this warming exists; A8-1 proved it does
not. Warming is ~15 lines against the existing scheduler.

### 1.4 Working-set size — the cache is tiny

Summed max payload over **all 223** distinct 200-OK `/api` URL keys seen in 4.28 days:
**1.08 MB**. Largest single payload 446 KB (`/api/customers/sms-segments/export/csv`,
n=2, which I exclude from caching). Excluding that, the entire hot set is well under
1 MB. A 64 MB Redis is a 60× margin.

### 1.5 The aggregate-CPU argument, which I am conceding before anyone makes it

Scheduler work in the same container, measured over 6.1 h of uptime:

```
Warehouse Refresh: n=176, total 266.0 s, avg 1511 ms   (interval 0:02:00)
Incremental Sync:  n=177, total 661.9 s, avg 3740 ms   (interval 0:01:00)
                   ------------------------------------
                   928 s / 21 964 s = 4.2% duty cycle on the 1.0-CPU budget
```

Dashboard API DuckDB work: 347 req/day × ~30 ms median-solo ≈ **10 s/day**.
Scheduler: ≈ **3 650 s/day**. The dashboard is **0.3 %** of the container's DuckDB
work. Host load average is **0.04 on 4 cores**.

**Caching the dashboard saves no meaningful CPU and no money.** Anyone arguing for
Redis on resource-efficiency grounds is wrong and I will not defend that position.
The value is entirely in *latency of the burst*, §1.2.

I also checked and am conceding two more of A8-1's supporting claims:
- **The "batch 30 s timeouts" are not visible in current prod.** Non-webhook `/api`
  status mix over 4.28 d: 3 345×200, 18×499, 6×500, 6×502. Of the 18 client aborts,
  4 are `/api/revenue/trend`, 2 `/api/summary`, 2 `/api/sales/by-source` — plausible
  but far from a smoking gun. No 504s on dashboard endpoints.
- **`/api/dashboard/batch` does not appear in the traffic at all.** The current
  frontend does not use it. Arguments about the batch endpoint are moot.

---

## 2. The concrete design

### 2.1 docker-compose service

Insert after the `meilisearch` block (`docker-compose.yml:2-25`):

```yaml
  redis:
    image: redis:7.4-alpine
    container_name: keycrm-redis
    restart: unless-stopped
    command: >
      redis-server
      --save ""
      --appendonly no
      --maxmemory 48mb
      --maxmemory-policy allkeys-lru
      --tcp-keepalive 60
    # No ports: — reachable only on the compose network, never from the host.
    mem_limit: 96m
    cpus: 0.25
    healthcheck:
      test: ["CMD", "redis-cli", "ping"]
      interval: 30s
      timeout: 5s
      retries: 3
```

Rationale, each choice tied to evidence:
- **`--save "" --appendonly no`** — persistence OFF. This is a derived cache of DuckDB;
  a cold start after restart is correct behaviour, and RDB forks are the classic way a
  Redis doubles its RSS. Disk is 59 % used (43/75 GB); no reason to spend any.
- **`maxmemory 48mb` + `allkeys-lru`** — measured working set is 1.08 MB (§1.4).
  48 MB is a 44× margin and, critically, a **hard ceiling that Redis enforces itself**.
  `mem_limit: 96m` is the outer backstop.
- **`cpus: 0.25`** — matches the meili/nginx precedent; leaves web's 1.0 untouched.
  Host has 4 cores at load 0.04.
- **No `ports:`** — Redis has no auth here, so it must never be host-reachable. It is
  on the compose bridge only, same posture as meili today (meili has no `ports:` either).
- **`mem_limit: 96m`** — sized against the existing precedent `meilisearch: mem_limit: 256m`
  (`docker-compose.yml:17`). Host free memory now: 6 106 MB available of 7 729 MB.

Then on the `web` service (`docker-compose.yml:50-58`), add to `environment:`:

```yaml
      - REDIS_URL=redis://redis:6379/0
      - CACHE_DEFAULT_TTL=300
```

and to `depends_on:` (currently `docker-compose.yml:60-62`):

```yaml
      redis:
        condition: service_healthy
```

`redis>=5.0.0` is **already in `requirements.txt:33`** — the client library ships in
the image today. No image rebuild is needed for the dependency; only the compose file
and `.env`/environment change.

### 2.2 Code changes — file:line

**(a) The call site must use `get_or_set`, and the TTL must be 300 not 60.**
`web/routes/api/analytics.py:337-352` is today a raw get→compute→set with `ttl=60`.
Replace with:

```python
cache_key = f"summary:{start}:{end}:{source_id or ''}:{category_id or ''}:{brand or ''}:{sales_type}:{promocode or ''}"
return await cache.get_or_set(
    cache_key,
    lambda: dashboard_service.get_summary_stats(
        start, end, category_id, brand=brand, source_id=source_id,
        sales_type=sales_type, promocode=promocode,
    ),
    ttl=CACHE_TTL_ANALYTIC,   # 300, from core/cache.py DEFAULT_TTL
)
```

`get_or_set` already exists with per-key `asyncio.Lock` (`core/cache.py:291-337`) and
is called from nowhere. This is A8-2's fix and it is already written.

**(b) Extend to the other five hot endpoints.** By measured traffic the set is exactly:
`/api/revenue/trend` (402 req), `/api/summary` (338), `/api/sales/by-source` (201),
`/api/products/top` (75), `/api/products/performance` (75), `/api/customers/insights`
(70), `/api/brands/analytics` (70) — these seven are 1 231 of the 1 455 cacheable
requests (**85 %**) and are exactly the seven that appear together in the 207 ms bursts.
Plus the three near-static ones: `/api/categories` (50), `/api/brands` (50),
`/api/promocodes` (50) at TTL 900.

**Key schema** — one helper, not per-endpoint string building:
`v{SCHEMA_VER}:{endpoint}:{start}:{end}:{sales_type}:{source_id}:{category_id}:{brand}:{promocode}`
with `SCHEMA_VER` bumped on any response-shape change, so a deploy cannot serve a
stale-shaped payload to a new frontend. `_build_key` (`core/cache.py:387-411`) already
hashes over 200 chars; reuse it.

Do **not** cache: anything admin/PII-bearing (`/api/customers/sms-segments`,
`/export/csv`, `/api/me`) — the key schema has no user dimension and `sales_type=internal`
is admin-gated in `api_gate`. Caching those risks cross-user leakage. This is a
correctness constraint, not a preference.

**(c) Invalidation — and here is the trap that must be fixed or Redis is worthless.**

`register_cache_invalidation_handlers()` (`core/cache.py:436-466`) hooks
`SyncEvent.ORDERS_SYNCED` and flushes `summary:*`, `revenue:*`, `sales:*`.
`ORDERS_SYNCED` is emitted at `core/sync_service.py:784` whenever the sync block runs
with orders. Per A8-3, `stats["orders"]` counts skip-unchanged rows
(`core/duckdb_store.py:2598-2599, 2659`). **Prod proves it**: every single incremental
sync for hours logs the identical constant

```
Incremental sync completed | {'duration_ms': 3196.11, 'stats': {'orders': 207, ...}}
Incremental sync completed | {'duration_ms': 3245.25, 'stats': {'orders': 207, ...}}
Incremental sync completed | {'duration_ms': 3413.19, 'stats': {'orders': 207, ...}}
```

and every refresh logs `silver=45834 (incremental_1317)` unchanged. Nothing is
changing; the machinery fires anyway. **Wiring Redis with the invalidation handlers
as they stand would flush the hot keys every 60 s**, which on top of `ttl=60` gives a
guaranteed 0 % hit rate. So:

1. Re-hook invalidation from `ORDERS_SYNCED` to **warehouse-refresh completion** —
   `core/duckdb_store.py:2575` is where the refresh logs its result. Emit a new event
   there carrying `affected_dates` and `validation_passed`.
2. **Invalidate by date scope, not `summary:*`.** The refresh already computes
   `affected_dates` (logged as `gold_dates=583`); only keys whose `[start,end]`
   intersects an affected date need dropping. Keys for closed historical periods
   (`period=last_month`) survive indefinitely.
3. **Never invalidate on `validation_passed=False`.** If the rebuild failed
   validation, the *cache holds the last good answer* and should keep serving it. This
   is a genuine reliability gain, not just speed — cf. the 2026-08-02 OOM storm that
   left Gold truncated for five days while the dashboard cheerfully served the
   truncated numbers.

**(d) Warming.** Register a job on warehouse-refresh completion that populates the
top-20 key set (72 % of traffic, §1.3) via the same `get_or_set` factories. This must
share `scheduler.py`'s `_heavy_job_lock` so it never runs concurrently with a refresh.
It also finally makes `core/scheduler.py:8`'s docstring true instead of deleting it.

**(e) Observability.** `cache.get_stats()` (`core/cache.py:413-420`) already returns
hits/misses/hit_rate. Surface it in `/api/health` — the endpoint currently returns
`{"status","version","uptime_seconds","duckdb","sync","data_quality"}` with **no cache
block**. Without this, nobody will ever know whether the hit rate is 31 % or 0 %, which
is precisely how the system ended up believing it had a cache for months.

### 2.3 Why the stampede matters even at this traffic
Measured duplicate-identical-request rate *within* a 5 s burst is **1/1455 = 0.1 %** —
so a classic thundering herd is not happening today, and I concede A8-2's live severity
is low. `get_or_set` is still the right call because it costs nothing (it is written
and tested) and because warming + a 300 s TTL creates synchronised expiry across the
top-20 keys, which is exactly the condition that manufactures a herd where none exists
now.

---

## 3. The strongest arguments against me, and my answers

### 3.1 "One container, one process — an in-process dict is strictly simpler and faster."
**This is the best argument against me and it is substantially correct.** A dict lookup
is ~0.001 ms; a Redis GET is a socket round-trip plus `json.loads`, call it 0.2–0.5 ms
on the loopback bridge. On a `cpus: 1.0` budget, Redis actively costs CPU that a dict
does not. There is exactly one web container (verified `docker ps`), so there is no
cross-process sharing to win. **I concede the entire performance-of-the-cache-hit axis.**

What I do not concede:

1. **Memory isolation is the point, and it is not rhetorical here.** This container has
   `DUCKDB_MEMORY_LIMIT=4GB` inside `mem_limit: 7g` on a **7 729 MB host** — the compose
   file itself carries the comment "3GB is what OOM'd on 2026-08-02 and truncated the
   Gold layer" (`docker-compose.yml:54-57`). An in-process cache is unbounded Python
   heap in the same address space DuckDB is already budgeted against. Redis at
   `maxmemory 48mb / mem_limit 96m` has a ceiling **the kernel and Redis both enforce**,
   in a budget that is separately accounted. Given this system's specific history —
   seven consecutive OOM'd refreshes, Gold truncated five days — putting new
   unbounded allocation inside the web container is the one thing I would refuse.
   Counter-counter I accept: a *bounded* LRU dict (`cachetools.TTLCache(maxsize=500)`)
   also has a ceiling. True. But then see (2).

2. **The Redis code exists; the dict code does not.** `core/cache.py` is 467 lines
   already written: TTL, JSON serialisation, per-key-locked `get_or_set`
   (`:291-337`), SCAN-based pattern invalidation (`:243-289`), stats (`:413-420`),
   graceful degradation (`:166-168`, `:203-204`), event-driven invalidation handlers
   (`:436-466`), a startup path already wired at `web/main.py:206-213`, a call site
   already written at `analytics.py:337-352`, an integration test suite at
   `tests/integration/test_cache.py`, and `redis>=5.0.0` already in
   `requirements.txt:33`. The "simpler" in-process alternative requires **writing a new
   component**: TTL expiry sweeping, bounded eviction, and — the part people forget —
   thread-safety, because invalidation would be triggered from APScheduler's internal
   thread (documented gotcha: "APScheduler thread-safety: event listeners run from
   APScheduler's internal thread. Use `threading.Lock`"). "Simpler" is doing a lot of
   work in that sentence when one option is ~8 lines of YAML against an already-tested
   library and the other is a new concurrency-sensitive module.

3. **An in-process cache dies on every deploy.** Deploys restart web (`RestartCount=0`,
   `StartedAt=2026-08-09T14:15:29Z`, image `Created=2026-08-09T14:14:50Z` — the
   container is exactly as old as the image, i.e. every push replaces it). Redis
   survives. Minor, but free.

**Honest verdict on 3.1:** if the counter-proposal is a *bounded* `TTLCache` with an
explicit `maxsize`, plus the same TTL/invalidation/warming/observability fixes, it is a
defensible position and the gap between us is small. I would still take Redis for the
enforced ceiling, but I would not call the dict wrong. **What is wrong is doing neither.**

### 3.2 "You're adding an operational component to a system that already OOMs."
Answered by the numbers rather than by assurance. Host: 7 729 MB total, **6 106 MB
available**, 866 MB free, 5 298 MB buff/cache. Current container RSS: web 727.7 MB /
7 GiB, bot 64 MiB, meili 41 MiB, nginx 13.8 MiB. A Redis holding a 1.08 MB working set
runs at ~10–15 MB RSS; capped at 96 MB it is **1.2 % of host RAM**, and it is the *only*
component in the stack whose memory is bounded by two independent mechanisms.

The sharper form of this objection is "one more thing to fail." Answer: `cache.get()`
returns `None` on any exception (`core/cache.py:181-184`) and `set` returns `False`
(`:216-219`); `connect()` failure is already non-fatal (`web/main.py:206-213`, and the
live log line "Redis cache not available, running without cache" is the proof that the
degradation path is *exercised in production right now*). Redis dying returns the system
to exactly today's behaviour. That is a genuinely unusual property and it is the reason
this is a low-risk change: **the failure mode is the current state.**

### 3.3 "The warehouse refreshes every 2 minutes, so cached data is either stale or constantly invalidated."
This is the objection I take most seriously, and §2.2(c) is my answer — but note it
attacks the *naive* wiring, which I am explicitly not proposing.

Measured facts: the refresh runs every 2 min at avg 1 511 ms and **the data does not
change** — 176 consecutive refreshes logged `silver=45834 (incremental_1317)`,
`gold_rev=2059`, `gold_prod=86963`, identical every time; 177 syncs logged the identical
`'orders': 207`. So "constantly invalidated" is an artefact of A8-3's skip-inflated
counter, not of real data churn. Invalidating on **refresh completion scoped to
`affected_dates`** means a key for `period=last_month` is never touched, and a key for
`period=today` is dropped only when today's cell actually moved.

On staleness: the ceiling on acceptable TTL is already set *by the frontend*, which
serves 2-minute-old data from browser memory (`useApi.ts:156`). A 300 s server TTL
means worst-case 5-minute-old revenue on a dashboard where the underlying sync itself
runs on a 60 s interval with adaptive backoff. If 5 minutes is unacceptable, the
correct response is scoped invalidation (which makes TTL a backstop rather than the
freshness contract), not no cache.

### 3.4 "Hit rate will be near zero because every user picks different date filters."
Tested, not assumed, and the objection is **quantitatively wrong**: 186 distinct keys
over 1 455 requests, with the **top 5 keys covering 42 % and the top 20 covering 72 %**.
The single most requested key —
`/api/summary?start_date=2026-08-04&end_date=2026-08-04&sales_type=retail` — appears
131 times. Users are not picking arbitrary ranges; they are opening the default view
(`period=yesterday`, `sales_type=retail`) over and over.

Where the objection *is* right: at TTL=60 the hit rate really is 0.1 %, because
TanStack already ate the tight repeats. That is an argument for **TTL=300 + warming**,
which is what I am proposing, not an argument against caching.

### 3.5 "445 requests/day. This is a non-problem. Do nothing."
The strongest *strategic* objection, and I will not overclaim against it. My honest
position: the win is a p50 of 171 ms → ~15 ms and a p95 of 324 ms → ~15 ms on the
owner's daily surface, for ~8 lines of YAML plus using functions that are already
written. It is a small, cheap, reversible win, not an incident. If the team's bar is
"only fix things that page someone," this does not clear it, and I would rather concede
that than inflate it. What I *will* argue is that leaving it as-is is the worst option,
because the codebase, the scheduler docstring (`core/scheduler.py:8`) and CLAUDE.md all
currently assert a cache that does not exist — so the next person to reason about
dashboard performance starts from a false premise. Either turn it on or delete it.

---

## 4. Conditions under which I am wrong

I lose this debate if any of the following holds:

1. **A bounded in-process TTL cache is on the table.** If the rival proposal is
   `cachetools.TTLCache(maxsize=500, ttl=300)` *plus* the same TTL fix, scoped
   invalidation, warming and hit-rate observability, then it captures ~95 % of the
   measured benefit with strictly less operational surface, and my remaining advantages
   (enforced external ceiling, survives restart) are thin. **I would concede.** My
   position only clearly wins against "no cache" or against an *unbounded* dict.

2. **The 207 ms plateau turns out to be an event-loop freeze rather than DuckDB lock
   queueing.** I have shown the plateau is real and correlates with concurrency, and I
   have shown the same queries run at 8–148 ms solo — but I have **not** isolated the
   mechanism to the `asyncio.Lock` as opposed to GIL/CPU starvation under `cpus: 1.0`.
   *If it is a whole-event-loop freeze, a Redis GET is also an `await` on that same
   frozen loop and buys nothing.* This is my single biggest unproven assumption and I
   flag it as such. The falsifying experiment: instrument `refresh_warehouse_layers`
   lock acquire/release and correlate with request start/finish timestamps, or simply
   ship the cache behind a flag and measure p50/p95 before and after.

3. **Raising `cpus` on web, or moving the scheduler out of the web container, removes
   the plateau.** The host is 4 cores at load 0.04; web is capped at 1.0. If bumping to
   `cpus: 2.0` collapses the 207 ms burst to ~40 ms, that is a one-line change that
   beats a cache on every axis and I should lose. **This should be tested first — it is
   cheaper than my proposal.** I would rather be right about the system than right about
   my position.

4. **The 46 % of requests over 200 ms is a sampling artefact.** My latency dataset is
   72 samples from 6.1 h of one evening. It is the only server-side timing data that
   exists (the container was restarted 6 h ago and nginx logs `combined` format with no
   `$request_time` — verified: no `log_format` directive in
   `/opt/key-api-bot/nginx/nginx.conf`). A week of data could show a very different
   distribution. **Assumption, explicitly labelled.**

5. **Caching is judged to endanger correctness.** This system's entire assurance
   apparatus — cell guard, reconciliation, data-quality digest — exists because wrong
   numbers reached humans. A cache is a second place for a number to be wrong. If the
   team judges that any serving path not backed by a live DuckDB read is unacceptable
   on a revenue dashboard, that is a coherent position and it beats mine on values, not
   on measurement. (My §2.2(c)(3) — don't invalidate when `validation_passed=False` —
   is an attempt to turn the cache into a *correctness asset*, but it is a mitigation,
   not a refutation.)

---

## 5. What I would actually ship, in order

1. `analytics.py:350` `ttl=60` → 300, and switch to `get_or_set`. **Zero new
   infrastructure, and it is the change with the highest ratio of benefit to risk in
   this whole debate** — because at 60 s no cache of any kind can work.
2. Re-hook invalidation off `ORDERS_SYNCED` (fires every 60 s on a constant
   `orders: 207`) onto scoped warehouse-refresh completion.
3. Test `cpus: 2.0` on web first (falsification #3). If it kills the plateau, stop here.
4. Redis service + `REDIS_URL` as specified in §2.1.
5. Warm the top-20 keys after each refresh; expose `cache.get_stats()` in
   `/api/health`.
6. Fix `core/scheduler.py:8` and the CLAUDE.md caching section to describe what exists.

Steps 1, 2 and 6 are correct **regardless of who wins this debate.**

---

# CROSS-EXAMINATION

**Verdict up front: I lose on the backend question. I concede to ADVOCATE-INPROC.**
Points 1 and 5 are correct and I have no answer to either. Point 4's arithmetic
falsifies my stated mechanism and I withdraw the claim it supported. What survives is
not mine-versus-his: it is three findings that break *both* designs equally, and one
of them (§X2) breaks his proposed invalidation strategy as thoroughly as mine.

---

## X1. The `_key_locks` leak — conceded, and it is worse rhetorically than materially

**Confirmed, repro:** `core/cache.py:104` — `self._key_locks: dict[str, asyncio.Lock] = {}`;
`core/cache.py:317-320`:

```python
async with self._lock:
    if key not in self._key_locks:
        self._key_locks[key] = asyncio.Lock()
    key_lock = self._key_locks[key]
```

No `del`, no eviction, no bound anywhere in the file. Every distinct cache key ever
requested leaves a permanent `asyncio.Lock` behind. INPROC is right, and the irony is
fair: I argued for Redis on the grounds of bounded memory and my headline change calls
the one unbounded function in the module. **Conceded without qualification.**

I will size it honestly rather than let it be either dismissed or inflated. Measured
distinct-key growth on the real traffic:

```
2026-08-05  new_today= 72  cumulative=  72
2026-08-06  new_today=136  cumulative= 162
2026-08-07  new_today= 45  cumulative= 171
2026-08-08  new_today= 18  cumulative= 171
2026-08-09  new_today= 56  cumulative= 186
```

Steady state ≈ **40 genuinely new keys/day** (the churn is date ranges rolling forward).
At ~90 B key string + ~350 B for an `asyncio.Lock` object ≈ 450 B/entry, that is
**~6.6 MB/year**, unbounded. So: a real leak, a slow one, and one that would never
have been the thing that OOMs this container — but "slow" is not a defence when the
whole argument was about enforced ceilings.

Fix, folded in (3 lines, in `get_or_set` after the value is set):

```python
finally:
    # Drop the per-key lock once nobody is waiting on it.
    async with self._lock:
        lk = self._key_locks.get(key)
        if lk is not None and not lk.locked():
            del self._key_locks[key]
```

This must land **before** ship-list item #1, not after it. Note it is a fix to shared
code that INPROC's design needs too if he reuses `get_or_set`'s stampede logic — but he
is not obliged to, so I do not get to spread the blame.

---

## X2. Date-scoped invalidation is un-buildable — **in both backends**, and this is my
one real counter-punch

INPROC's attack is that a Redis glob cannot express "drop entries whose span overlaps
`affected_dates`," while an in-process loop over ≤512 tagged entries can. The premise
is correct. The conclusion does not follow, because of a number neither of us checked:

**`affected_dates` is 582–596 dates on every single refresh.** Repro — `gold_dates` in
the refresh log is literally `len(affected_dates)` (`core/duckdb_store.py:2571-2572`),
and across 176 consecutive prod refreshes:

```
  66 gold_dates=584
  39 gold_dates=583
  29 gold_dates=582
  16 gold_dates=595
  12 gold_dates=594
   7 gold_dates=587
   6 gold_dates=596
   3 gold_dates=592
```

~19 months of dates declared affected, every 2 minutes, while `silver=45834
(incremental_1317)` and `gold_rev=2059` and `gold_prod=86963` never move. It comes from
`affected_dates = silver_old_dates | new_dates` (`:2267`) over the dirty id set, which
per A8-3 is the whole skip-inflated 24 h fetch window, not the changed rows.

**Consequence: an overlap test against 583 dates matches essentially every dashboard
key there is** — `yesterday`, `last_month`, `month`, any explicit range in the last 19
months. INPROC's exact loop over ≤512 structured tags returns "invalidate" for
substantially all of them. His scoped design degenerates to a global flush on current
data. His own simulator already tells him what that costs; mine is harsher than his:

```
TTL=300, no invalidation                              460/1455 = 31.6%
TTL=300 + GLOBAL invalidate every 120s                 41/1455 =  2.8%   <-- not 15.8%
```

I report the number that hurts me more, because it is what my replay produces.

So the honest position is: **scoped invalidation is not a Redis weakness, it is an
upstream data-quality bug (A8-3), and until `changed_ids` is computed properly no
invalidation strategy works in any backend.** That materially changes the shape of the
debate — it moves the blocking work out of the cache layer entirely.

And to answer the question directly, because it deserves a direct answer: **yes, in
Redis I would keep the span index in the web process.** Tag-sets are the alternative
and they are worse (a `period=year` key joins 583 tag sets; tag sets drift from keys and
need their own TTL and reconciliation; and the index is empty after a restart while the
Redis values are not, which is a *fresh* consistency bug that pure in-process simply
does not have). So yes — **the interesting state lives in-process either way, and that
is a concession, not a dodge.** Once the index is in-process, Redis is a network hop to
a dumb byte store, and INPROC is right that this is the wrong shape.

---

## X3. Staleness — I withdraw plain TTL=300, and the replacement is better than what
either of us proposed

The skeptic is right that a flat 300 s TTL on a "is today's revenue right" dashboard,
in a system whose cell guard and reconciliation exist *because* wrong numbers reached
humans, is not defensible — especially once §X2 removes scoped invalidation as the
escape hatch. **I withdraw TTL=300 as a uniform value.**

The replacement comes from a split neither report measured. Classifying all 1 455
cacheable requests by whether the query window includes the current date:

| query window | requests | share |
|---|---|---|
| **CLOSED** (`yesterday`, `last_week`, `last_month`, explicit past ranges) | 731 | **50.2 %** |
| OPEN (includes today: `today`, `week`, `month`) | 518 | 35.6 % |
| NO-DATE (`/api/categories`, `/api/brands`, `/api/promocodes`) | 198 | 13.6 % |
| unknown | 8 | 0.5 % |

**63.8 % of dashboard traffic asks about data that a 2-minute refresh cannot legitimately
change.** The top key overall is `/api/summary?start_date=2026-08-04&end_date=2026-08-04`
(131 hits) — a single closed day. The staleness objection applies to 35.6 % of traffic,
not to all of it.

So the answer to "what do you say to a user seeing a 5-minute-old number" is: they never
see one, because open-window keys get a short TTL and closed-window keys — which cannot
move — get a long one. Simulated on the real traffic:

```
tiered TTL (open=120s, closed=3600s, static=3600s), no invalidation   689/1455 = 47.4%
tiered TTL + invalidate every 120s, CLOSED/static exempt              589/1455 = 40.5%
TTL=300 flat, no invalidation                                         460/1455 = 31.6%
TTL=300 flat + global invalidate every 120s                            41/1455 =  2.8%
```

**40.5 % under realistic every-2-minute invalidation, with worst-case staleness on
today's numbers of 120 s — below the 127 s refresh interval, so never more than one
rebuild old.** That is strictly better than my original design on both freshness and
hit rate.

This is backend-agnostic. It is INPROC's to implement, and he should.

---

## X4. The arithmetic — pre-committed answer, and my reading

### Pre-commitment, plainly
**Yes. If the plateau is CPU/event-loop starvation rather than DuckDB lock queueing, I
withdraw** — not just the Redis position but the headline latency argument for caching
of any kind. A cache hit is still an `await` on the same event loop; if the loop is
starved for 207 ms, `cache.get()` returns at 207 ms exactly like a DuckDB query does.
Under that mechanism a cache reduces only the *quantity of GIL-heavy work feeding the
starvation*, proportional to hit rate, which at 40 % is a modest shortening of the
freeze — not the 10-20× I claimed. The correct fix would be `cpus: 2.0` (host has
`nproc=4` at load 0.04, web is capped at `NanoCpus=1000000000`) and/or moving the
scheduler out of the web process. I said in §4 of my report that I would rather be right
about the system than right about my position; this is where that gets paid.

### My reading of 427.8 ms vs 207 ms — and the flatness is the real clue

The coordinator is right that this is the most informative number available. My analysis:

**The magnitude discrepancy is suggestive; the *flatness* is decisive, and it points
away from my mechanism.**

Strict serialization behind one lock produces a **staircase**: the first waiter to
acquire finishes at t₁, the second at t₁+t₂, and so on. It cannot produce seven
completions inside a 0.61 ms window, no matter what the individual durations are. That
is not a matter of whether 428 ms is the right work estimate — no queue discipline
produces simultaneous completion. Yet across every plateau I classified, the tight
cluster is real and there is *also* a fast tail in the same burst:

```
burst  n  spread  min     max     verdict-if-treated-as-one-group
   2  14  312.5    11.4   323.9   (7 clustered at 323.3-323.9, rest at 8-43)
   3   8  208.8     8.9   217.7   (7 clustered at 206.9-217.7, one at 8.86)
   5   8  210.1     9.9   220.0   (7 clustered at 211.1-220.0, one at 9.9)
   6   8  193.8     8.7   202.5   (5 clustered at 202.1-202.5, three at 8.7-10.6)
   7   8  208.4    11.6   220.1   (6 clustered at 208.9-209.5, two at 11.6/220.1)
   8   8  140.1     8.1   148.2   (staggered 8/10/28/31/70/130/148 — NO plateau)
```

Two things fall out:

1. **Burst 8 has the same eight endpoints and does not plateau at all** — it staggers
   8.13 / 10.62 / 28.38 / 31.87 / 70.54 / 130.03 / 148.22. Identical concurrency,
   identical queries, no plateau. So the plateau is **not** produced by the requests
   contending with each other. It is produced by an *intermittent external blocker*.
   That kills my "7 concurrent widget queries serialize on one connection" story
   outright.

2. **Within a plateau burst, some requests return in 8–11 ms while others sit at 207 ms.**
   The fast ones are the ones that arrived *after* the blocker cleared (e.g. the
   `/api/revenue/trend` at 8.86 ms completing at 17:41:43, one second after the 17:41:42
   plateau). So the blocker has a sharp edge: everything in flight when it starts is
   held until it ends; everything arriving after runs at full speed.

Simultaneous release + sharp edge + independent of concurrency level = **a single shared
blocking event, released at one instant.** The two candidates are (a) the event loop /
GIL being held by synchronous work, or (b) a single long lock hold released once.
Under (a), caching does not fix it. Under (b) — note that the queries' own work must
then have completed *during* the hold, i.e. concurrently in the executor, which means
reads are **not** serialized behind one connection, which independently falsifies the
"single serialized DuckDB connection" premise that A8-1 and I both leaned on.

**Both branches cost me the argument.** The most likely blocker in either case is the
scheduler in the same process — refresh avg 1511 ms every 2 min, sync avg 3740 ms every
1–2 min, 4.2 % duty cycle on a 1.0-CPU budget. I could not discriminate (a) from (b)
read-only; the discriminating experiment is instrumenting lock acquire/release in
`refresh_warehouse_layers` against request start/finish, and I endorse the skeptic
running it.

---

## X5. The memory-pressure hook — conceded; nothing survives that matters at 445 req/day

**Verified it exists:** `core/scheduler.py:1497-1499` — `_MEM_WARN_THRESHOLD = 0.75`,
`_MEM_CRITICAL_THRESHOLD = 0.90`, with `_run_memory_monitor` at `:1588` reading the
cgroup (`_read_cgroup_memory`, current/peak/max). INPROC's proposal to shrink the cache
at 75 % and clear it at 90 % is sound and it attacks my single strongest differentiator
on its own ground — correctly.

My argument was that a separate cgroup gives an *enforced* ceiling. His is that an
in-process cache can **yield**, which a separate cgroup structurally cannot. For **this**
system that is the better property, and I should have seen it: the failure this
container actually had (2026-08-02, seven consecutive OOM'd refreshes, Gold truncated
five days) was DuckDB needing more memory than it could get. A Redis holding 1 MB in its
own 96 MB cgroup would have sat there being irrelevant. A cache that hands memory back
to DuckDB at 75 % is strictly more useful in the exact incident class I cited as my
justification. **Conceded.**

Going through what Redis could still win, honestly:

- **Multi-process sharing** — there is one web container (`docker ps` verified). The bot
  containers do not consume analytic endpoints; `bot/canary.py` hits `/api/health`,
  which is not cacheable. **No win.**
- **Survives restart** — true (image `Created` == container `StartedAt`, so every deploy
  replaces it), but at 11 sessions/day a cold cache after a deploy costs one slow page
  load. **Negligible.**
- **The code already exists** — this was my best remaining point and §X1 spends it: the
  code that exists contains an unbounded dict, so "already written and tested" was
  overstated. `cachetools` gives him TTL + `maxsize` eviction off the shelf with no new
  concurrency code. **No win.**
- **Enforced ceiling** — beaten by the yield behaviour above. **No win.**
- **Observability** — `cache.get_stats()` (`core/cache.py:413-420`) is backend-agnostic.
  **No win.**

At 445 dashboard requests/day, 26 IPs and 11 sessions/day, **there is no remaining
argument for adding a network service.** I concede the backend question to INPROC.

---

## X6. What I still hold, and what I would have the coordinator take away

I am not retreating on these, and none of them favour a backend:

1. **`ttl=60` at `web/routes/api/analytics.py:350` yields a measured 0.1 % hit rate.**
   Whatever is built, this number kills it. Highest benefit-to-risk change in the debate
   and it is a one-token edit.
2. **`affected_dates` is 582–596 every refresh** (`gold_dates` in 176 consecutive prod
   refreshes, `core/duckdb_store.py:2571-2572`). Scoped invalidation is impossible in
   *any* backend until A8-3's `changed_ids` fix lands. **This is the blocking dependency
   for the whole feature and it is not in the cache layer.** If one thing comes out of
   this debate, it should be this.
3. **63.8 % of traffic is closed-window or date-free.** Tiered TTL (open 120 s / closed
   3600 s) simulates to **40.5 % under every-2-minute invalidation** with worst-case
   staleness of 120 s — better hit rate *and* better freshness than my flat TTL=300, and
   it is the answer to the staleness objection.
4. **The plateau is caused by an intermittent external blocker, not by request
   self-contention** — proven by burst 8, which has identical concurrency and staggers
   normally. Whatever the fix is, it is upstream of the cache: `cpus: 2.0`, or getting
   the scheduler out of the web process.
5. **The aggregate-CPU argument remains dead** (dashboard = 0.3 % of container DuckDB
   work) — I conceded that unprompted and it still stands.

**Revised ship-list, with my own position removed from it:**

1. Fix `_key_locks` unbounded growth (`core/cache.py:317-320`) — required by any design
   reusing `get_or_set`.
2. `analytics.py:350` `ttl=60` → tiered TTL keyed on whether the window includes today.
3. Fix `changed_ids` upstream (A8-3) so `affected_dates` means something. **Blocker for
   invalidation of any kind.**
4. Determine the plateau mechanism before building a cache to fix it; test `cpus: 2.0`
   first — it is one line and it may end the discussion.
5. Then, if steps 3–4 leave a real problem: **INPROC's bounded in-process `TTLCache`
   wired to the memory monitor at `core/scheduler.py:1497-1499`.** Not Redis.
6. Fix `core/scheduler.py:8` and the CLAUDE.md caching section either way — they
   currently assert a cache and a warming job that do not exist, which is how this
   became a debate instead of a ticket.

Items 1, 2, 3 and 6 are correct regardless of who won. Item 5 is INPROC's.
