# Moving Postgres from `ks-data-platform` into `key-api-bot`

Owner's decision, 2026-08-23. This runbook exists because **merging the compose
change does not perform the move — it detonates it.** The deploy job runs

```
cd /opt/key-api-bot && git reset --hard "$GH_SHA" && docker compose pull && docker compose up -d
```

so the compose file on production is whatever the merged commit says, applied
immediately, with `set -e`. Two things in it fail hard if the host is not
prepared first:

* `container_name: ks-postgres` collides with the platform's running container
  → `up -d` fails → the deploy goes red.
* `${POSTGRES_PASSWORD:?set it in .env}` aborts compose outright if the variable
  is absent from `/opt/key-api-bot/.env`.

Neither takes the site down (web, bot and nginx are unchanged and keep running),
but the release fails and every subsequent deploy keeps failing until the host
is fixed by hand.

## Why the volume is declared external

`pgdata` is declared `external: true, name: ks-data-platform_pgdata` — the swap
therefore **moves no bytes**. That volume already holds the state a restore
drill was rehearsed against (roles `ks_app`/`ks_readonly`, empty schemas
bronze/silver/gold/meta, `btree_gist` 1.7, replication slot `ks_pitr`), and
charter rule 2 says nothing moves before a rehearsed restore. Renaming it to
`key-api-bot_pgdata` is tidier and is a **separate** window: doing both at once
means one rollback has to undo two things.

## Do this while the database still holds no application data

Step 03 of the migration plan has not run, so `ks` contains roles and empty
schemas and nothing else. Today the swap is a compose edit. After step 03 it is
an outage for both bots and the dashboard at once — the same asymmetry the plan
cites for `archive_mode`.

## Sequence

Everything in **part 1** happens on the host, by hand, before the PR is merged.

### 1. Prepare the host

```bash
ssh <vps>

# 1.1  Secrets first — compose aborts without them, and that fails the deploy.
#      Copy the three values out of /opt/ks-data-platform/.env.
grep -E '^(POSTGRES_PASSWORD|KS_APP_PASSWORD|KS_READONLY_PASSWORD)=' \
  /opt/ks-data-platform/.env >> /opt/key-api-bot/.env

# 1.2  Stop the platform's copy and remove the containers, so the names free up.
#      `stop` before `rm` so pg_receivewal flushes and releases the slot.
cd /opt/ks-data-platform
docker compose stop pg-receivewal postgres
docker compose rm -f pg-receivewal postgres

# 1.3  Move the WAL stream and the base backups WITH the service. The chain of
#      base backup + WAL segments is only a recovery point while it stays
#      together; splitting it across two directories silently ends PITR.
mkdir -p /opt/key-api-bot/backups
mv /opt/ks-data-platform/backups/pg_wal    /opt/key-api-bot/backups/pg_wal
mv /opt/ks-data-platform/backups/postgres  /opt/key-api-bot/backups/postgres

# 1.4  Confirm the external network the compose file expects is present.
docker network ls | grep -w ks-data
```

`postgres/initdb/` ships in the repo, so the merge brings it. It will not
re-run: `initdb` executes only on an empty data directory, and this one is not.

### 2. Merge

Merging deploys. Watch the run; the health gate at the end of the job proves
nginx and web survived, not that Postgres did — check that separately in part 3.

### 3. Verify on the host

```bash
docker compose -f /opt/key-api-bot/docker-compose.yml ps postgres pg-receivewal
docker exec ks-postgres psql -U postgres -d ks -tAc \
  "SELECT slot_name, active FROM pg_replication_slots"   # expect: ks_pitr|t
ls -t /opt/key-api-bot/backups/pg_wal | head -3           # segments still arriving

# AND THE ONE THAT ACTUALLY BIT — see below. Not the deploy's own gate.
curl -fsS -o /dev/null -w '%{http_code}\n' https://ksanalytics.duckdns.org/api/health
```

The slot reading `active = t` is the one that matters for Postgres: it proves
`pg_receivewal` reconnected to the same slot rather than silently starting a new
recovery point.

There is deliberately **no** `ks-tg-bot` reachability check here. That container
does not join `ks-data` until step 03 of the migration plan; testing it now
fails for the wrong reason.

### The four-minute outage this caused on the first run, and how to avoid it

Adding two services to `default` **shifts IP allocation**. On 2026-08-23
`keycrm-web` came back on `172.18.0.6` instead of `172.18.0.4`, and
`keycrm-nginx` — up for two days, not recreated by the deploy — was still
holding `172.18.0.4`, because nginx resolves an upstream hostname **once, at its
own startup**, and this config has no `resolver` directive. Every request got
502 for about four minutes.

The cure is one command, and it belongs in part 2 of any deploy that changes
network membership:

```bash
cd /opt/key-api-bot && docker compose restart nginx
```

Three defects were found underneath this. **All three are now fixed** — the
history is kept because each one hid the next.

* **The deploy's health gate did not gate anything** (#117). It ran
  `curl -fsS http://127.0.0.1/api/health`, which nginx answers with a **301** to
  HTTPS. `curl -f` does not fail on a 3xx and there was no `-L`, so the gate
  exited 0 without ever reaching `web`. It reported success throughout the
  outage. Now HTTPS on loopback via `--resolve`, with `= 200` asserted.
* **nginx never re-resolved its upstream** (#118). An `upstream` block resolves
  the name once, at config load, and open-source nginx cannot re-resolve it.
  Now `resolver 127.0.0.11 valid=10s` with the host in a variable. The
  keepalive pool was the price, measured at ~0.3 ms/request — inside the
  run-to-run spread at this traffic level.
* **A single-file bind mount does not see a `git reset --hard`.** Found while
  deploying the fix above, and it is the most transferable of the three:

  ```
  host   nginx/nginx.conf   6 occurrences of web_upstream
  in the container          0
  ```

  `git reset --hard` **replaces** a file — new inode — and a bind mount of a
  single *file* is bound to the inode it saw at container creation. So the host
  file changes and the container keeps the old one. `docker compose restart`
  does not help, and `nginx -s reload` re-reads the *old* file and reports
  success.

  **`docker compose up -d --force-recreate <service>` is the only thing that
  works.** Three mounts in this compose file have this property —
  `nginx/nginx.conf`, `nginx/security-headers.conf`, `nginx/error.html`.
  Directory mounts such as `postgres/initdb` do not: names are resolved inside
  the directory on each open.

  Corollary worth holding onto: **any nginx config change needs a recreate, and
  a deploy alone will silently not apply it.**

### 4. Finish the platform side

`ks-data-platform` still *declares* both services. Delete them from its compose
file, or the next `docker compose up -d` in that directory recreates them and
collides on the container names. ClickHouse stays.

Also still to port, and deliberately **not** in this change — name them so they
are not discovered later by their absence:

* `scripts/backup.sh` and `scripts/restore-drill.sh` in the platform cover both
  engines. The Postgres halves belong in `deploy/` here; the ClickHouse halves
  stay there. Until that is done, **backups and the drill still run from
  `/opt/ks-data-platform`** and will not find the moved directories.
* Whatever cron entry invokes them points at the old paths.

## Rollback

One revert, one manual step, and no data is at risk — the volume is external and
untouched by either direction:

1. Revert the merge (this deploys, removing both services from this project).
2. Restore the two service definitions in the platform's compose and
   `docker compose up -d postgres pg-receivewal` there.
3. Move `backups/pg_wal` and `backups/postgres` back.

## One change that is not a move

The healthcheck is no longer `pg_isready`. Charter rule 13, added 2026-08-22,
requires a check to perform the system's work: `pg_isready` executes no query,
which is exactly why nine connection failures out of twenty at concurrency 20 —
caused by a 64 MB `/dev/shm` — were invisible to it. The replacement
(`psql -tAc 'SELECT 1'`) was run against the live container before it was
written into the file. It is called out here rather than buried in the diff
because it is a behaviour change riding along with a relocation.
