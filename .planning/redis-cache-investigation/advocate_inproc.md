# ADVOCATE-INPROC — Rip out Redis, ship a bounded in-process TTL cache

**Thesis, in one line:** on this workload the cache *backend* is not the variable that
matters — the TTL is. I can prove that with the production request stream, and the proof
is backend-agnostic, which is exactly why paying for a network-attached datastore is
the wrong trade.

---

## 0. The measurement that decides the debate

I replayed 86 h of the real production request stream (nginx access log,
`docker logs --since 96h keycrm-nginx`, 1 073 dashboard API requests,
2026-08-06 05:01:35 → 2026-08-09 18:15:24) through a cache simulator
(`/tmp/inproc/sim.py`, `/tmp/inproc/reqs.txt`):

| Policy | hits | misses | hit rate |
|---|---|---|---|
| **TTL=60 s — what `analytics.py:350` actually hardcodes today** | **1** | **1 072** | **0.1 %** |
| TTL=120 s | 169 | 904 | 15.8 % |
| TTL=300 s (`core/cache.py:46` `DEFAULT_TTL`) | 403 | 670 | 37.6 % |
| TTL=300 s + global invalidation every 120 s (warehouse refresh) | 169 | 904 | 15.8 % |
| TTL=3600 s (upper bound on key reuse) | 653 | 420 | 60.9 % |

**Deploying Redis today, against the code exactly as written, buys one cache hit per
four days.** Not "a small win" — one hit, n=1073.

Why: the gap distribution between two requests for the *same* key
(`/tmp/inproc/gaps.py`, n=920 gaps) is p5=116 s, p25=120 s, **p50=122 s**, and
38.2 % of all repeats land in the 60–120 s bucket. That is the frontend polling
interval: `web/frontend/src/hooks/useApi.ts:156` — `REALTIME: 2 * 60 * 1000`,
TanStack Query `staleTime` on summary/revenue/sales. The server TTL
(`web/routes/api/analytics.py:350`, `ttl=60`) is **exactly half the client's refetch
cadence**, so it expires immediately before every repeat. Zero requests fall in the
10–60 s bucket. One falls under 10 s.

So the entire deliverable value of "add a cache" on this system is unlocked by changing
one integer. Redis contributes 0 of the 403 hits at TTL=300 s. A dict contributes 403 of
them. That is the whole argument, and everything below is why the dict is also the
cheaper, safer container for those 403 hits.

---

## 1. What the workload actually is (prod, read-only)

| Fact | Value | Source |
|---|---|---|
| Dashboard API requests / 96 h | **1 141** (1 073 in the analysed window) | nginx log |
| Average rate | **~12 req/h** | derived |
| Busiest hour | 196 req | `2026-08-06:05` |
| Peak requests in any one second | **14** | log, grouped by `$time_local` |
| **Distinct cache keys across all endpoints / 96 h** | **153** | log |
| Distinct keys, `/api/summary` alone | **18** (299 requests) | log |
| Distinct client IPs on `/api/summary` | 18 | log |
| `POST /api/dashboard/batch` requests | **0** | log — the batch endpoint is unused in prod |

The two hottest summary keys are 131 + 126 of the 299 requests — one pinned date
(`start_date=2026-08-04&end_date=2026-08-04`) and `period=yesterday`. Working set: a
couple of hundred small JSON blobs.

**Query cost being avoided** (`/tmp/inproc/bench3.py`, `data/analytics.duckdb`
read-only, `memory_limit=1GB`, `threads=2`, warm, 7 iters, p50):

| Query (30-day range) | p50 |
|---|---|
| summary — gold path | **0.43 ms** |
| summary — silver+order_products+products, brand filter | 1.10 ms |
| revenue trend | 0.37 ms |
| products/top (gold_daily_products GROUP BY) | 8.46 ms |
| products/performance by category | 7.99 ms |

I will state this plainly because my opponents will find it anyway: **the compute a cache
saves here is single-digit milliseconds.** The cache is not buying CPU. It is buying
*queue position* — every one of these has to acquire `DuckDBStore._lock`
(`core/duckdb_store.py:237`, single connection, `asyncio.Lock`, "Serializes all
database access"), which `refresh_warehouse_layers` holds for its entire
Silver+Gold+validation pipeline. A8 measured that hold at p50 294 ms / p99 1058 ms /
max 9489 ms over 16 725 refreshes. Prod runs **176 refreshes in the ~6 h since the
last restart** (`grep -c "Warehouse dirty"`, ~29/h ≈ every 2 min), always with
`changed_ids≈205-216` even though the same log line says
`written=0, skipped_unchanged=217`.

That queue-position benefit is **identical** for Redis and for a dict. Neither backend
is better at it. So the tiebreak is cost and failure modes.

---

## 2. Payload sizes and the cost of the two backends

Payloads reconstructed from the same gold tables the endpoints read
(`/tmp/inproc/ser.py`). Cross-check: my gzip sizes match the nginx `$body_bytes_sent`
distribution almost exactly, which validates that these are the right size class as
prod (summary gzip 133 B vs observed p50 126 B; trend gzip 376 B vs observed p50 329 B).

| Payload | JSON bytes | gzip | `json.dumps` | `json.loads` |
|---|---|---|---|---|
| summary | 169 | 133 | 2.3 µs | 1.4 µs |
| revenue/trend (30 d) | 1 777 | 376 | 15.8 µs | 9.4 µs |
| products/top | 3 287 | 842 | 8.8 µs | 8.6 µs |
| products/performance | 1 393 | 443 | 11.5 µs | 6.9 µs |
| brands/analytics | 1 214 | 419 | 10.7 µs | 6.4 µs |
| revenue/trend, 730 d (worst realistic) | 37 153 | — | — | — |

Round-trip cost, measured (`/tmp/inproc/rtt.py`, asyncio client, 3 000 samples):

| | p50 | p99 |
|---|---|---|
| loopback async socket round-trip + `json.loads` (**floor** for any Redis GET) | **69.5 µs** | 113.5 µs |
| in-process `dict.get()` | **0.083 µs** | 0.125 µs |

The 69.5 µs is a *floor*: a real Redis adds RESP encoding in redis-py, the Docker bridge
hop, and server processing. Honest framing: **70 µs is not a user-visible latency
difference.** I am not going to pretend it is. What it does mean is that the network hop
buys nothing measurable, so it has to justify itself entirely on operational grounds —
and it can't (§4).

The app already uses `ORJSONResponse` as the default response class
(`web/main.py:43`), and `orjson.dumps` on the 30-day trend is 1.7 µs. So the right
in-process design caches the **encoded bytes**, and a hit costs one dict lookup plus a
`Response(content=bytes)` — zero encode, zero decode, zero copy, and no aliasing hazard
from handing a shared mutable dict to a caller. Redis-with-JSON costs decode-on-read
plus re-encode-on-response for every hit.

---

## 3. RAM — the objection I take most seriously, answered with numbers

Prod `keycrm-web`, read from cgroup v2 right now:

```
memory.current  1 146 703 872   (1.147 GB)
memory.peak     1 872 932 864   (1.873 GB, since restart 6 h ago)
memory.max      7 516 192 768   (7.0 GiB)
memory.events   oom_kill 0
```

`docker stats`: 727.7 MiB / 7 GiB. Host `free -m`: 7729 total, 6060 available. Prod
`analytics.duckdb` is **777 MB** (post-compact), not the 9 GB of the OOM era.

Measured footprint of a hard-bounded byte cache (`tracemalloc`, distinct objects):

| Bound | RAM |
|---|---|
| 512 entries × realistic 30-day payload | **0.99 MiB** |
| 512 entries × the *worst realistic* payload (730-day trend, 37 KB) | **18.31 MiB** |
| 144 entries as live Python dicts (not bytes), 30-day trend | 1.16 MiB |

18.31 MiB is **0.98 % of the current peak** and **0.24 % of the 7 GiB ceiling**. The
2026-08-02 OOM was DuckDB's arena during a warehouse rebuild against a 9 GB file at a
3 GB `memory_limit` — it was not Python heap pressure, and an 18 MiB ceiling does not
participate in that failure mode. I still bound it, and I still wire it to the existing
pressure signal (§4), because "it's only 18 MiB" is how unbounded caches get shipped.

---

## 4. The concrete design

**Adapt `core/cache.py`, do not replace it.** Keep the public surface
(`get/set/delete/invalidate_pattern/get_or_set/cached/get_stats/is_connected`) so
`web/routes/api/health.py:137-143`, `web/routes/api/admin.py:379-401`,
`web/main.py:301-305` and `tests/integration/test_cache.py` (276 lines, 5 classes)
keep working. Swap the backend beneath it.

### 4.1 Changes, file:line

| Location | Change |
|---|---|
| `core/cache.py:1-26` | Module docstring: it currently documents a Redis layer that has never run in prod. Rewrite for the in-process backend. |
| `core/cache.py:44` | Delete `REDIS_URL`. Replace with `CACHE_MAX_ENTRIES` (default 512) and `CACHE_MAX_BYTES` (default 32 MiB). |
| `core/cache.py:46` | `DEFAULT_TTL` stays 300 — and becomes the value actually used (see `analytics.py:350`). |
| `core/cache.py:85-141` | `RedisCache` → `InProcessCache`. `connect()` becomes a no-op returning `True` (so `web/main.py:207` registers the invalidation handlers, which today never register at all). Delete the `import redis.asyncio` block at `:118`. |
| `core/cache.py:100-101` | `_client`/`_connected` → `_entries: OrderedDict[str, Entry]`, `_bytes: int`. `is_connected` (`:151-154`) returns `self.enabled` — health/admin endpoints keep reading it. |
| `core/cache.py:156-184` `get()` | `OrderedDict` lookup + expiry check + `move_to_end` (LRU). Return a **sentinel** on miss, not `None`: today `analytics.py:339` tests `if cached is not None`, so a legitimately-cached `None`/empty result is indistinguishable from a miss. |
| `core/cache.py:186-219` `set()` | Store `(payload_bytes, expires_at, tags)`. Evict LRU while `len > CACHE_MAX_ENTRIES` **or** `_bytes > CACHE_MAX_BYTES`. Reject any single payload > 1 MiB (cache-bypass, counted in stats) so one pathological range query can't own the budget. |
| `core/cache.py:243-289` `invalidate_pattern()` | Keep the signature; implement with `fnmatch` over the key set. Add `invalidate_dates(start, end)` — see 4.3. |
| `core/cache.py:104, 317-320` | **Existing bug, inherited by both positions:** `_key_locks` is an unbounded `dict[str, asyncio.Lock]` — entries are created and *never removed*. Keys embed date ranges, so it grows without bound. Fix: `finally: if not key_lock.locked(): self._key_locks.pop(key, None)`. |
| `core/cache.py:291-337` `get_or_set()` | Keep as the single stampede-safe entry point. It is already correct apart from the leak. |
| `core/cache.py:339-385` `cached()` decorator | Reroute through `get_or_set` so the decorator is stampede-safe too, or delete it — it is applied to zero endpoints today. |
| `core/cache.py:436-466` | Handlers stay as written; they simply start running, because `connect()` now returns `True`. |
| `web/routes/api/analytics.py:337-352` | Replace the manual get→compute→set with a single `await cache.get_or_set(cache_key, factory, ttl=CACHE_TTL_SUMMARY)`. **This is the change that turns 0.1 % into 37.6 %.** |
| `web/routes/api/analytics.py:350` | `ttl=60` → `ttl=DEFAULT_TTL` (300). |
| `core/scheduler.py:784` | After `refresh_warehouse_layers` returns, call `await cache.invalidate_dates(...)` using the refresh result's date span. Direct function call, same process, cannot fail over a wire. |
| `core/scheduler.py:1588-1607` | `_run_memory_monitor` already reads cgroup `current/peak/max`. When `usage_pct >= _MEM_WARN_THRESHOLD` (`:1498`, 0.75) call `cache.shrink(0.5)`; at `_MEM_CRITICAL_THRESHOLD` (`:1499`, 0.90) call `cache.clear()`. A cache that yields under pressure cannot be the cause of an OOM. |
| `web/routes/api/health.py:135-143` | Rename the `"redis"` component key to `"cache"`, report `entries`, `bytes`, `hit_rate_percent`, `evictions`. Today it prints `not_connected` forever. |
| `requirements.txt:33` | Delete `redis>=5.0.0`. |
| `tests/integration/test_cache.py` | `TestRedisCacheWithMock` (`:173`) loses its reason to exist; the other four classes become real tests instead of tests of the degraded path. Add: eviction at bound, byte-bound eviction, TTL expiry, `get_or_set` stampede (N concurrent → factory called once), `_key_locks` does not grow, date-overlap invalidation. |
| `.claude/CLAUDE.md` | "In-memory cache — 5-minute TTL / Background warming every 4 minutes / Gzip ~70 %" currently describes a system that has never run. Two of those three become true; delete the warming claim (`core/scheduler.py:8` docstring also lies about it — no warming job is registered). |

### 4.2 Stampede protection

`get_or_set` (`core/cache.py:291-337`) already does double-checked per-key locking.
Route the hot endpoints through it. Honest calibration: **peak observed concurrency is
14 req/s and those 14 are 14 *different* keys** (one dashboard mount fanning out across
endpoints), so a stampede is not a live production problem today. It is cheap insurance
that costs one function call, and it is the thing A8-2 (sev4) asked for.

### 4.3 Interaction with the 2-minute warehouse refresh

This is where in-process wins on capability, not just on cost. The refresh knows exactly
which orders changed — `consume_warehouse_dirty()` returns `changed_ids`
(`core/scheduler.py:774-783`), and `refresh_warehouse_layers` knows the affected date
span. In-process, invalidation can be a predicate over structured key metadata:

```
cache.set(key, payload, ttl, tags={"span": (start, end), "sales_type": st})
cache.invalidate_dates(changed_start, changed_end)   # drop entries whose span overlaps
```

Redis's native tool is a glob (`invalidate_pattern("summary:*")`,
`core/cache.py:444-446`, `web/main.py:303`) — it cannot express "date range overlaps
[2026-08-08, 2026-08-10]", so every 2-minute refresh nukes the whole namespace including
the January cells that provably did not change. You can build tag-sets in Redis; it is
three extra round trips and a second consistency problem. In-process it is a loop over
≤512 entries.

That matters because of the simulator row `TTL=300 s + invalidate every 120 s → 15.8 %`:
naive global invalidation on every refresh **halves** the achievable hit rate. Overlap-
scoped invalidation keeps the long-tail historical keys (period=last_month, fixed date
ranges — 20.9 % of repeats have gaps > 2 h) alive while still guaranteeing that
today's numbers are never stale by more than one refresh cycle. **That is strictly
fresher than the 60 s TTL shipped today, not looser.**

### 4.4 Cold start

Deploy or restart → empty cache. Refill cost = one miss per key = 0.4–8.5 ms of DuckDB
compute (§1), ~40 ms total for a full dashboard mount. Also: a deploy is immediately
followed by a warehouse refresh, which would invalidate the cache anyway. There is no
cold-start problem to solve here; see §5.1 for the one case where there is.

### 4.5 Why adapt, not replace

`core/cache.py` is 467 lines of which the stats dataclass (`:49-82`), the key builder
(`:387-411`), `get_or_set`'s locking (`:291-337`) and the invalidation handlers
(`:436-466`) are all backend-agnostic and already written and already tested. Only
`connect/disconnect/get/set/delete/invalidate_pattern` touch Redis. Swapping the backend
is ~120 lines changed in one file; three call sites keep compiling unchanged.

---

## 5. The strongest arguments against me, and my answers

### 5.1 "An in-process cache dies on every deploy/restart; Redis survives."
**Partly conceded, and it is my opponents' best point.** The one scenario with teeth:
the web process restarts, a user's dashboard mounts cold, and the very first warehouse
refresh is holding `DuckDBStore._lock` for up to 9.5 s (A8: max 9489 ms) — with a warm
Redis those requests would have been served without touching the lock.

Answers: (a) the window is seconds, once per deploy, and deploys are human-gated on the
`production` environment so they are not frequent or surprising; (b) that first refresh
would have invalidated the cache anyway, so a surviving Redis would have been *wrong*,
not fast; (c) the correct fix for lock contention is not a cache in front of the lock —
it is a second DuckDB cursor off the same in-process database instance for the read path,
or a shorter lock hold in `refresh_warehouse_layers`. If we accept "the cache exists to
paper over a 9.5 s lock", we have chosen a datastore to avoid fixing a mutex.

### 5.2 "You can't invalidate across processes when they add a second worker or scale out."
**Rejected on the facts.** A second worker is *impossible* here, and not because of the
cache: DuckDB is single-writer and its connection is explicitly documented as not
thread-safe (`core/duckdb_store.py:228-238`). Two uvicorn workers = two processes trying
to open the same `analytics.duckdb` read-write = the exact "can't open a second
connection" failure the ops notes already record. `Dockerfile.web:67` starts uvicorn with
no `--workers`. `docker-compose.yml` caps `web` at `cpus: 1.0` — one core.

So the day someone wants two web processes, DuckDB has to be replaced or fronted first;
the cache is not on the critical path of that migration, and at that point you have
re-architected the storage layer anyway. **Buying Redis today to hedge against a
scale-out that DuckDB structurally forbids is paying rent on a house you cannot enter.**

Also note the *converse* risk, which is real: Redis introduces cross-process cache
coherence as a *new* problem the system does not have today. Right now the writer
(`_run_warehouse_refresh`, `core/scheduler.py:768`) and the readers (the API handlers)
are the **same process**, started from the same `web/main.py` lifespan (`:183`
`start_scheduler()`). A cross-process cache is solving a problem that does not exist,
and creating a distributed-invalidation problem that currently does not exist.

### 5.3 "The bot container can't share it."
**Moot, verified.** `grep -rn "duckdb" bot/` → the bot never opens the warehouse. The bot
talks to KeyCRM and to the dashboard over HTTP (`bot/main.py:265` `run_canary(DASHBOARD_URL)`).
There is no cache call site in `bot/` at all (`grep -rn "core.cache" bot/` → 0). If the
bot ever wanted cached analytics, it would call the web API — and hit the in-process
cache on the way through. `ks-tg-bot` is a separate image and a separate concern.

### 5.4 "RAM in a container with an OOM history is the worst place for a cache."
**Answered with the cgroup numbers** (§3): 18.31 MiB worst-case bound = 0.98 % of the
container's current peak, 0.24 % of its limit; `oom_kill 0`; the DB is 777 MB now, not
9 GB. The 2026-08-02 OOM was DuckDB's arena at a 3 GB `memory_limit`, not Python heap.
And unlike Redis, an in-process cache can *participate* in the mitigation: hooked into
`_run_memory_monitor` (`core/scheduler.py:1588`, thresholds `:1498-1499`) it releases
memory at 75 % and empties at 90 %. Redis, sitting in its own cgroup, cannot — it will
happily hold its `maxmemory` while the web container is being OOM-killed. **On the OOM
axis, in-process is strictly better, not worse.**

### 5.5 "Redis is 30 lines of YAML and battle-tested; you're hand-rolling a cache in 2026."
Three answers.

1. **The 30 lines of YAML buy 1 hit per 1 073 requests** (§0) unless you also change
   `analytics.py:350`. If you change `analytics.py:350`, you get 403 hits with or
   without the YAML. The YAML is not what is being purchased.
2. **The hand-rolling is not avoided by choosing Redis.** `core/cache.py` is already
   hand-rolled — the stats, key builder, per-key lock, TTL policy, invalidation handlers.
   Choosing Redis means keeping all of that *and* adding a client, a container, a URL, a
   healthcheck, a `maxmemory` policy, an eviction policy, a persistence decision, and a
   sixth thing that the weekly `weekly_compact.sh` stop/start dance and `bot/canary.py`
   have to know about. What I am proposing deletes code (`redis>=5.0.0`, the
   `import redis.asyncio` block, `TestRedisCacheWithMock`) and adds an `OrderedDict` with
   an LRU bound — that is not exotic, it is `functools.lru_cache` with expiry.
3. **Redis fails silently in the direction that costs money.** `invalidate_pattern`
   swallows every exception and returns `0` (`core/cache.py:286-289`). A blip during the
   post-refresh invalidation → the dashboard shows revenue that is up to TTL seconds
   stale and *nothing logs above DEBUG* (`:288` is `logger.debug`). Nothing in
   `bot/canary.py` or `/api/health` would catch it — `health.py:140-141` reports
   `not_connected` without degrading overall status. An in-process `dict` invalidation
   cannot half-fail. On a dashboard whose entire purpose is correct revenue, that
   asymmetry is the argument.

### 5.6 "Prod already degrades gracefully with no cache — do nothing."
Rejected, and this is where I agree with A8-1: graceful is *correct*, not *fast*, and
the documented architecture ("5-minute cache + background warming") does not exist. But
note this objection lands harder on the Redis proposal than on mine: doing nothing and
deploying Redis-at-TTL-60 are empirically the same system (0.1 % hit rate).

### 5.7 "153 keys today, but keys explode with arbitrary date pickers."
Real, and it is why the design has a hard entry bound **and** a byte bound **and** a
per-payload cap (§4.1). The observed key space is 153 over 96 h; the bound is 512 with
LRU. If the key space genuinely exploded past that, the hit rate collapses toward zero
for *any* backend — Redis included — and the answer is key normalisation, not more RAM
somewhere else.

---

## 6. When I am wrong

I concede the position, immediately and without argument, if any of these becomes true:

1. **A second process needs to read the cache.** Concretely: uvicorn `--workers>1`,
   a separate API container, or the bot reading warehouse analytics directly. All three
   are currently blocked by DuckDB's single-writer constraint — but if the storage layer
   moves off DuckDB (or to a read-replica/`ATTACH`-read-only topology), a shared cache
   becomes the correct answer and my argument evaporates the same day.
2. **Deploy frequency rises to where cold-start cost is measurable.** If the web
   container restarted, say, hourly, the cold-cache penalty during a lock-held refresh
   window stops being a rounding error.
3. **The working set stops fitting a hard bound.** If distinct keys go from 153 to
   ~10⁴–10⁵ per hour (real multi-tenant traffic, per-customer keys), a bounded
   in-process LRU thrashes and a dedicated cache tier with real eviction telemetry is
   right.
4. **Something else in the stack needs Redis anyway** — rate limiting shared across
   processes, Celery/RQ, pub/sub, distributed locks, session storage. If Redis has to
   exist for another reason, the marginal cost of using it for the cache is genuinely
   near zero and I would not argue against it. (Today: `slowapi`'s limiter is in-memory,
   the scheduler is APScheduler in-process, sessions are cookie/JWT — nothing needs it.)
5. **Someone measures a real Redis GET on this host at a latency that changes a user-
   visible number.** It won't — but the 69.5 µs figure is a loopback *floor* I measured
   with a Python echo server, not a real Redis benchmark, and I label it as such. If the
   real number were somehow *lower* than an in-process dict lookup, physics has changed
   and so should I.

**What I will not concede:** that the choice of backend explains the hit rate. The
simulator runs on the real request stream and is backend-agnostic. Whoever argues for
Redis has to explain how a network hop turns 1 hit into 403 — it doesn't;
`web/routes/api/analytics.py:350` does.

---

## Appendix — commands and artefacts

Prod (read-only):
```
ssh <vps> 'docker ps; docker stats --no-stream; free -m'
ssh <vps> 'docker logs --since 96h keycrm-nginx > /tmp/nx.log 2>&1'
ssh <vps> 'docker logs --since 24h keycrm-web  > /tmp/web.log 2>&1'
ssh <vps> 'docker exec keycrm-web cat /sys/fs/cgroup/memory.{current,peak,max}'
ssh <vps> 'docker exec keycrm-web cat /sys/fs/cgroup/memory.events'   # oom_kill 0
ssh <vps> 'grep -in "redis\|CACHE" /opt/key-api-bot/docker-compose.yml /opt/key-api-bot/.env'  # 0 hits
```
Live log line confirming the premise:
`core.cache - Redis connection failed: Error 111 connecting to localhost:6379. Connection refused.`
→ `web.main - Redis cache not available, running without cache`

Local (all in `/tmp/inproc/`, nothing written to the repo):
- `bench3.py` — DuckDB query timings (`data/analytics.duckdb`, `read_only=True`)
- `ser.py` — payload sizes, gzip, serialisation cost, RAM of 144 held payloads
- `rtt.py` — loopback async round-trip vs `dict.get()`
- `sim.py` — **the hit-rate simulation over the real request stream**
- `gaps.py` — inter-arrival gaps for identical keys (the 122 s poll interval)
- `reqs.txt` — 1 073 `(timestamp, path+querystring)` pairs extracted from prod nginx

Local `data/analytics.duckdb` is a dev copy (gold data ends 2026-05-30, prod ends
today); query timings are therefore indicative of shape and magnitude, not of prod
wall-clock. Prod's DB is 777 MB vs local 8.75 GB, so prod is if anything faster. All
traffic, key-count, hit-rate and memory numbers are from **prod**.

---

# CROSS-EXAMINATION

## CX-0. The concession, up front

**THE SKEPTIC wins the performance axis. I concede it without reservation, and my own
new measurement is worse for me than his.**

He asked what a user gets on a real page load. I went and separated user-visible
requests from invisible ones (`/tmp/inproc/mounts2.py`). I clustered the 1 073 prod
requests into bursts (gap ≤ 5 s), then split the bursts by the gap that *preceded*
them: a burst arriving < 240 s after the last one means the tab was already open and
TanStack was refetching on its 120 s cadence (**invisible**); a burst after a longer gap
is a genuinely cold client — new tab, reload, phone unlocked (**this is the only thing a
user experiences**).

| TTL | fresh page load (user-visible) | background refetch storm | lone request |
|---|---|---|---|
| 60 s (shipped) | **1 / 283 = 0.4 %** | 0 / 643 = 0.0 % | 0 / 147 = 0.0 % |
| 120 s | 1 / 283 = 0.4 % | 136 / 643 = 21.2 % | 32 / 147 = 21.8 % |
| **300 s (my proposal)** | **9 / 283 = 3.2 %** | 307 / 643 = 47.7 % | 87 / 147 = 59.2 % |
| 600 s | 28 / 283 = 9.9 % | 371 / 643 = 57.7 % | 111 / 147 = 75.5 % |
| 1800 s | 54 / 283 = 19.1 % | 432 / 643 = 67.2 % | 127 / 147 = 86.4 % |

35 fresh loads and 115 refetch storms in 3.55 days → **9.9 fresh page loads/day.**

**So: of the 403 hits I paraded in §0, 307 are background refetches the user never sees
and 87 are lone requests. Nine are on a fresh page load. Over 3.55 days.** At my own
proposed TTL, a genuine page load has a **3.2 % chance** that any *one* of its eight
endpoints is served from cache. Expressed the way the coordinator asked: the honest
number is not "p50 171 ms → 15 ms on repeat mounts." It is **"~0.97 of a page load in ten
sees one endpoint out of eight come back in ~0.1 ms instead of ~50 ms"** — a few
milliseconds off one page load per day. TanStack's 120 s `staleTime`
(`web/frontend/src/hooks/useApi.ts:156`) has already eaten everything else.

**My change is a code-hygiene and correctness change, not a performance change.**
I am striking the word "performance" from my case. THE SKEPTIC's 180× arithmetic
(1 007 s/day refresh vs ~5.6 s/day human) is right, his 0.5 s/day figure for my best
case is right, and my mount analysis independently confirms it from a second direction.

**But note what that concession does to the other side of the table.** Every number
above is an *a fortiori* argument against Redis. If the prize is ~0.5 s/day of DuckDB
and nine cached responses per 3.55 days, then:

- my proposal costs ~120 lines in a file that already exists and **deletes** a
  dependency (`requirements.txt:33`), and
- the Redis proposal costs the same ~120 lines of glue **plus** a sixth container, a
  `maxmemory`/eviction policy, a healthcheck, a `REDIS_URL`, a client dependency, a new
  entry in the `weekly_compact.sh` stop/start dance, and a silent-staleness failure mode
  (`core/cache.py:286-289`, exception swallowed, returns 0, `logger.debug`).

THE SKEPTIC's demolition of my cache is a *nuclear* demolition of Redis. At a prize of
0.5 s/day, "battle-tested" is not a virtue, it is an overpayment. **The skeptic and I
agree on the first and largest action: `redis>=5.0.0` and the Redis client come out.**
Our residual disagreement is over ~40 lines, and I state below where I now think he is
right about those too.

---

## CX-1. Revised proposal (narrowed)

Ranked by evidence, not by my prior position:

1. **Delete the Redis dependency** — `requirements.txt:33`, `core/cache.py:44`
   (`REDIS_URL`), `core/cache.py:117-141` (the `import redis.asyncio` block),
   `tests/integration/test_cache.py:173` (`TestRedisCacheWithMock`). Unanimous across
   me and the skeptic. This is the whole of my original mandate and it is unchallenged.
2. **Delete the lying docs** — `core/scheduler.py:8` ("Cache warming (every 5 minutes)":
   no warming job is registered anywhere), the CLAUDE.md Caching section
   ("5-minute TTL / background warming every 4 minutes"). Skeptic's item; I adopt it.
3. **Attack the refresh** — 95 % of scheduled runs still rebuild; prod shows
   `changed_ids≈205-216` on every tick while the same log line reads
   `written=0, skipped_unchanged=217`. This is A8-3, it is 1 007 s/day, and it dwarfs
   everything either of us proposed. Skeptic's item; I adopt it and rank it above my own.
4. **Fix `_key_locks`** (`core/cache.py:104, 317-320`) — unbounded, keyed on date-range
   strings, never popped. Survives in *both* ship-lists. Must be fixed or deleted.
5. **The summary cache itself** — either delete `web/routes/api/analytics.py:337-352`
   outright, or reduce it to ~40 lines: `get_or_set` at `DEFAULT_TTL`, `OrderedDict` LRU,
   hard byte bound. **On the evidence I just produced, I no longer think this is worth
   120 lines.** If the choice is Redis vs delete-it-all, I now prefer delete-it-all over
   my own original proposal. I keep item 5 as *optional*, argued below on correctness
   grounds only, and I will not defend it on latency.

My original 120-line figure included the invalidation plumbing, the date-overlap
tagging, and the memory hook. Item 5 in its minimal form is ~40 lines and does not need
the tagging. I am revising my own scope down.

---

## CX-2. The tie-break he asked for — evidence, not rhetoric

First, a correction to my own record: **there are three in-process caches, not four.**
I mis-cited `admin.py`; grepping it (`web/routes/api/admin.py:379-401`) shows only the
two *Redis* endpoints (`/cache/stats`, `/cache/invalidate`), no in-process cache. My
error, withdrawn.

Now the tie-break. Both readings ("the pattern belongs here" vs "the pattern was already
applied exactly where evidence demanded") are compatible with the *existence* of those
caches. They are not compatible with their **TTLs**. The discriminating variable is
whether each cache's TTL exceeds its consumer's polling cadence — because that, and only
that, is what determines whether a cache can ever hit:

| Cache | TTL | Consumer's poll cadence | TTL > cadence? | Can hit? |
|---|---|---|---|---|
| `web/routes/api/health.py:24` `_STATS_CACHE_TTL` | 60 s | Docker `healthcheck interval: 30s` (docker-compose.yml) | ✅ 60 > 30 | yes |
| `web/services/category_service.py:22` `CACHE_TTL` | 3600 s | frontend `STATIC: 10*60*1000` (`useApi.ts:158`) | ✅ 3600 > 600 | yes |
| `web/services/brand_service.py` | none (warm-once via `_brands_loaded`) | any | ✅ ∞ | yes |
| **`web/routes/api/analytics.py:350`** | **60 s** | frontend `REALTIME: 2*60*1000` (`useApi.ts:156`) | ❌ **60 < 120** | **no** |

**Three for three on the in-process caches. Zero for one on the Redis-routed one.**

That breaks the tie, and it breaks it in a way that is bad for *both* of us and worth
stating precisely: the distinguishing variable is not "was there evidence" and not
"in-process vs Redis" — it is **"did anyone check the TTL against the consumer's
cadence."** Whoever wrote the three in-process caches did. Whoever wired the summary
endpoint to Redis did not, and it is the only one that has never worked.

So THE SKEPTIC's reading is *more* right than mine: the pattern was applied where
someone had looked at the access pattern. The summary cache is not evidence that the
pattern generalises — it is evidence of a cache written without looking. His conclusion
follows: don't generalise it. My residual point is narrower and I'll state it once:
that cache is **already in the codebase**; the live choice is delete-it or fix-it, not
add-it-or-not. Neither of those is "add a container."

---

## CX-3. The mechanism — and it moots part of the framing

I think the (a)-lock vs (b)-CPU dichotomy is **false for this codebase**, and the
plateau arithmetic is what shows it.

### The arithmetic

Solo: 8.13 + 10.62 + 31.87 + 70.54 + 28.38 + 130.03 + 148.22 = **427.79 ms**.
Concurrent: seven completions inside **0.61 ms** at 323.3–323.9 ms.

- **Model A — strict serialization behind one mutex, nothing else.** Completions are the
  cumulative sums: 8.1, 18.8, 50.6, 121.2, 149.5, 279.6, 427.8. Spread **419.7 ms**,
  last at 427.8 ms. Observed spread 0.61 ms. **Refuted by three orders of magnitude.**
- **Model B — loop blocked for D, then the same serialized drain.** Identical staircase
  shifted by D. Same 419.7 ms spread. **Also refuted.** So neither pure story works.
- **Model C — loop blocked for D ≈ 323 ms, then a drain in which each request costs
  ~0.09 ms.** 7 × 0.09 ≈ 0.61 ms. **Fits exactly.**

Model C requires the marginal cost of one of these requests to be **sub-millisecond**,
and I have that independently: I timed these query shapes warm at **0.37–8.46 ms p50**
(`/tmp/inproc/bench3.py`) on a **8.75 GB** dev DB, against prod's **777 MB**.

That forces an inference that is bad for my own case and I'll state it anyway: **the
8–148 ms solo figures are not compute.** If seven concurrent requests can finish within
0.61 ms of each other, a *solo* request measured at 148 ms spent ~148 ms waiting, not
working. Which means **a cache cannot remove that 148 ms** — the cache removes the
~0.09 ms of work, not the wait. The wait is removed only by removing the blocker.

### What the blocker is (verified in code, not inferred)

```
core/repositories/*.py :  211 blocking  conn.execute(...)   across 9 files
core/repositories/*.py :    0 run_in_executor              (grep -rc → no non-zero lines)
core/duckdb_store.py:228-238 : connection() takes self._lock, yields the RAW connection
core/duckdb_store.py:2036+   : refresh_warehouse_layers issues 32+ blocking conn.execute
```

Every DuckDB touch — read **and** the whole warehouse refresh — is a synchronous C call
executed *on the event loop thread* while holding `self._lock` (`:147`,
"Serializes all database access"). **The mutex and the event loop are held by the same
call.** (a) and (b) are not competing hypotheses here; they are one event described
twice.

And the punchline: the **non-blocking path already exists and has zero callers.**

| Helper | Line | Callers |
|---|---|---|
| `_execute_with_timeout` (executor + timeout) | `duckdb_store.py:242` | **0** |
| `_fetch_one` | `:269` | 3 (all internal to `duckdb_store`) |
| `_fetch_all` | `:306` | 6 (all internal) |
| `_fetch_df` | `:343` | **0** |

`ThreadPoolExecutor(max_workers=1)` is constructed at `:185` and, apart from the backup
path (`:2905`, `:2918`), sits idle. **This is the identical failure mode as
`get_or_set`: the correct primitive is written, tested-shaped, and called from nowhere.
Twice in one codebase.** That, not the cache, is the finding worth the coordinator's
attention.

### Consequences

1. **`cpus: 2.0` does not fix the plateau.** The stall is one Python thread parked
   inside a C call; the event loop *lives in that thread*. A second core cannot run it.
   What a second core *would* do is shorten D, because there is no `SET threads=`
   anywhere — connect only sets `memory_limit`, `preserve_insertion_order`,
   `wal_autocheckpoint`, `temp_directory` (`core/duckdb_store.py:170-180`), so DuckDB
   takes the host's 4 cores as its default and then gets throttled to a 1.0 CPU quota.
   `cpus: 2.0` is a cheap, real, partial mitigation of *duration*. It does nothing about
   head-of-line blocking.
2. **The actual fix is `run_in_executor` on the read path** — using code that is already
   written. The loop stays responsive; requests still serialize on the single connection,
   but they serialize without freezing the accept loop, the WebSocket broadcasts, or the
   health endpoint. That converts the flat 323 ms plateau into a staircase whose *first*
   response lands at ~8 ms. **This is worth more than every cache proposal in this
   debate combined**, and neither advocate proposed it.
3. **D ≈ 207–324 ms has an obvious candidate**: `refresh_warehouse_layers`, which A8
   measured at p50 294 ms / p90 683 ms / max 9 489 ms and which prod runs **176 times in
   ~6 h** (`grep -c "Warehouse dirty"`, ≈ every 2 min). The observed plateau sits inside
   that distribution. Caveat, labelled: I did not capture a refresh and a mount in the
   same trace — nginx's log format has no `$request_time` (`nginx/nginx.conf` has no
   `log_format` directive at all, so it is the stock combined format at 1-second
   resolution) and the API endpoints return 401 without a token I am not going to mint on
   a read-only engagement. **The correlation is a hypothesis, well-supported by the
   arithmetic; the blocking-call mechanism is a verified fact.**

### "Does your proposal survive if the answer is (b)?"

**Direct answer: yes on mechanism, no on magnitude — and the second half is what matters.**

- The skeptic's premise ("a cache lookup is an `await` on the same starved loop, so it
  buys less than you think") is **wrong on the mechanism**. A cache *hit* returns from
  `analytics.py` without ever entering `connection()`: it takes neither the mutex nor a
  blocking C call, so it does not merely serve one request fast — it **removes a
  starvation event from the shared loop**, helping every concurrent request. On a
  blocking-loop architecture a cache hit is worth *more* than naive accounting suggests,
  not less.
- And it does not matter, because the quantity is ~86 avoided blocking entries per day
  × ~0.09–5 ms ≈ **0.4–0.5 s/day**, against the refresh's **1 007 s/day**. Directionally
  right, quantitatively irrelevant. **I do not get to keep the performance claim by
  winning the mechanism argument.**

---

## CX-4. The memory hook — how much of it was real

**Concession: at a 30-minute tick the hook is decorative, and I should not have
presented it as a safety property.**

`core/scheduler.py:525-532` — `memory_monitor` is `IntervalTrigger(minutes=30)`.
Between ticks, nothing shrinks. A 30-minute reaction time is not a memory guard; it is
a Telegram alert with a cache-shrink bolted on. THE SKEPTIC and ADVOCATE-REDIS are both
entitled to that point and I withdraw the framing.

**What I would actually build**, in order of what carries the guarantee:

1. **The hard bounds, enforced synchronously inside `set()`** — `CACHE_MAX_ENTRIES`
   (512) and `CACHE_MAX_BYTES` (32 MiB), with LRU eviction in the same call, plus a
   1 MiB per-payload reject. This is the guarantee. It is not best-effort and it does not
   depend on any scheduler tick. Measured worst case: **18.31 MiB** for 512 × the
   largest realistic payload (730-day trend, 37 KB); **0.99 MiB** at realistic sizes.
2. **An inline pressure check, amortised** — I measured the cost on prod:
   ```
   docker exec keycrm-web python3 -c "...open('/sys/fs/cgroup/memory.current')..."
   → inline cgroup memory.current read: 17.4 us
   ```
   17.4 µs. At ~12 requests/hour it is free even unamortised; amortise to once per 5 s
   and it is unmeasurable. That gives a **5-second** reaction time instead of 30 minutes,
   and it is ~6 lines. This is what I would build, and it is a straight upgrade on my
   original sketch — I got the mechanism right and the trigger wrong.
3. The 30-minute `_run_memory_monitor` hook stays as belt-and-braces, ~5 lines,
   advertised as nothing more.

**Sharpening the counter to Redis's external ceiling.** ADVOCATE-REDIS is right that
`maxmemory 48mb` + `mem_limit 96m` is a stronger *kind* of guarantee — kernel-enforced,
independent of Python, and it cannot be defeated by a bug in my eviction loop. I concede
that cleanly. My counter is that it guards the wrong object:

- **The thing that OOM'd was not a cache.** The compose comment he cites points at
  `core/duckdb_store.py:165-169`, which records the actual event: seven consecutive
  refreshes "died at 2.7/2.7 GiB **while the container sat at ~950 MiB of its 7 g
  budget**." That was DuckDB's arena hitting `memory_limit=3GB` against a 9 GB file.
  A cache ceiling — inside or outside the process — would not have moved that outcome by
  one byte.
- **Today's numbers**: `memory.current` 1.147 GB, `memory.peak` 1.873 GB,
  `memory.max` 7.516 GB, `memory.events oom_kill 0`, DB now **777 MB**. An 18 MiB
  worst-case bound is 0.98 % of peak, 0.24 % of the limit.
- **Externalising the ceiling costs fungibility.** 48 MB pinned in a separate cgroup is
  48 MB the web container cannot reclaim when DuckDB is the thing under pressure — and
  DuckDB is always the thing under pressure here. In-process memory is fungible with the
  process that actually needs it; a sibling container's is not.
- So: stronger guarantee, wrong object, small negative side-effect. I'd take the
  synchronous byte bound plus a 5-second inline check.

---

## CX-5. Revised statement of when I am wrong

Superseding §6. I have already conceded the largest one in CX-0.

1. **Already conceded, unprompted:** the performance case. 3.2 % fresh-load hit rate,
   ~0.5 s/day of DuckDB, against 1 007 s/day burned by the refresh. My change is
   hygiene and correctness. If the coordinator's question is "does the user get faster
   pages," the answer is **no, measurably no**, and the skeptic proved it before I
   confirmed it from a second angle.
2. **Already conceded, unprompted:** if the choice is Redis vs delete-the-cache-entirely,
   I prefer **delete**, over my own original 120-line proposal. My mandate was "Redis is
   the wrong answer"; that is satisfied by deletion just as well as by replacement, and
   deletion is cheaper.
3. **Conceded on the tie-break:** THE SKEPTIC's reading of the three in-process caches is
   better supported than mine. They were tuned against their consumers' cadences; the
   summary cache was not. That is an argument against generalising, and I withdraw the
   "the pattern belongs here" framing in favour of "this specific cache is already here
   and is broken."
4. **Conceded on the memory hook:** 30-minute tick, decorative as presented. The bound
   in `set()` is the real guarantee; the inline 17.4 µs check is the real hook.
5. **Conceded on my own citation:** three in-process caches, not four. `admin.py` has
   none.
6. **Still standing, and I will defend these against anyone:**
   - Redis is the wrong answer. Nothing in cross-examination touched this; the skeptic's
     numbers strengthen it.
   - `_key_locks` (`core/cache.py:104, 317-320`) is an unbounded leak in *both* ship-lists.
   - Redis's glob invalidation cannot express date-range overlap; in-process can.
   - **The plateau is not a mutex staircase and not a CPU shortage** — 211 blocking
     `conn.execute` on the loop thread, 0 `run_in_executor` in any repository, and four
     executor-backed helpers with 0–6 internal callers and none from the read path. The
     lock and the loop are the same event. `cpus: 2.0` shortens D but does not remove
     head-of-line blocking; `run_in_executor` on the read path does, using code that is
     already written.
   - **The highest-value action in this entire debate belongs to neither advocate:** it
     is the skeptic's refresh attack (1 007 s/day) followed by moving reads off the event
     loop. Both dwarf the cache by two to three orders of magnitude. If the coordinator
     ships one thing, it should not be a cache and it should certainly not be Redis.

### Cross-examination artefacts
- `/tmp/inproc/mounts.py`, `/tmp/inproc/mounts2.py` — mount-vs-poll hit-rate split (CX-0)
- `grep -rc "run_in_executor" core/repositories/*.py` → no non-zero lines (CX-3)
- `grep -rc "conn.execute" core/repositories/*.py` → 211 across 9 files (CX-3)
- caller counts for `_execute_with_timeout` / `_fetch_one` / `_fetch_all` / `_fetch_df` (CX-3)
- `ssh <vps> 'docker exec keycrm-web python3 -c "...memory.current..."'` → 17.4 µs (CX-4)
- `core/scheduler.py:525-532` → `IntervalTrigger(minutes=30)` (CX-4)
