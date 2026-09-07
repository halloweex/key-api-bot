# Changelog

All notable changes to this project will be documented in this file.

## 3.0.143

- The expense mirror failed on every row: KeyCRM sends ISO strings


## 3.0.142

- Document the /expenses port
- Test the expense mirror, its backfill, and the comparison
- The expense comparison was reading the wrong clock
- Give the expense backfill a way to be run
- Fix the expenses fixture, the wiring pin, and add a two-minute gate
- Read /expenses from Postgres, and fix a fan-out worth 314K


## 3.0.141

- Two defects in the review's own tests
- Code review of the /traffic port: seven findings, all fixed


## 3.0.140

- Make the layer-sequence test run the whole sequence
- Ship the UTM last, and the five contracts the gate made me move
- Read /traffic from Postgres, and delete a Gold instead of copying it


## 3.0.139

- Two portability traps the gate caught in products_intel
- Let the product-intelligence reads use Postgres


## 3.0.138

- Make a silent fallback fail the differential tests


## 3.0.137

- Close the margin ties the gate found
- Let /margin read Postgres


## 3.0.136

- Let /marketing read Postgres


## 3.0.135

- Derive the reports fixture's expectations from the fixture
- Make the b2b order in the reports fixture actually b2b
- Let /reports read Postgres


## 3.0.134

- Stop the write comparison from comparing two clocks
- Two defects the gate found, one of them a nightly flake
- Give the dashboard's user list a Postgres home


## 3.0.133

- Say what no-cache actually costs here


## 3.0.132

- Mount nginx/ in the gate, where its new reader needs it
- Stop a deploy from breaking the tabs that are already open


## 3.0.131

- Send only the stock figures the page draws


## 3.0.130

- Stop serving a stock list nothing renders


## 3.0.129

- Take /inventory's last read off DuckDB — the blocker was a belief


## 3.0.128

- Keep shipping the one SMS table KeyCRM still fills


## 3.0.127

- Give the disk-resolve test the ledger stub its siblings already use
- Close the remaining LOW findings of the race audit
- Record a resolution or an escalation before announcing it
- Close the three Postgres-only gaps in the SMS state
- Stop the throughput benchmark from being able to destroy anything
- Write permission toggles column by column, and let a promotion to admin stick
- Make the bot's verdict buttons answer the pending request
- Stop the read paths and the b2b pass from rewriting the goals tables
- Keep the dirty flag until the refresh has run, and defer the Postgres rebuild
- Hold the heavy-job lock on every mirror writer and re-ship lost orders hourly
- Write a manager's classification in one transaction
- Admit one send per alert bucket at a time
- Make force_resync rewrite the window in place
- Read the weekly report's audience from the store the bot writes
- Write an order's line items before its header
- Rebuild sku_inventory_status in one transaction
- Refuse a delivery report for an id we do not hold yet
- Keep the SMS claim when the gateway's answer is unknown


## 3.0.126

- Name the mechanism behind the lost delivery reports, and guard against it
- Give the surface tests a frontend to serve when the checkout has none
- Give the diagnostician the sentence the alert had no room for
- Make the next dual-role stamp declare which kind it is
- Stop comparing the stamp that says when the copy was taken
- Guard the class of bug that shipped twice
- Stop the cohort and SMS bodies inheriting a timezone
- Tell both engines whose day today is, and stop two more ties
- Take the trend chart and the average off DuckDB too
- Prove the two engines agree, and fix the two places they did not
- Let KS_READ_INVENTORY choose which engine answers the tab
- Put the eleven inventory views in Postgres as revision 0014
- Give the inventory views one body instead of a DuckDB-only one
- Replace a WAL figure that was 65x out with what the server says
- Give the WAL archive a base backup to be a recovery point for
- Let the disk watchdog see the disk, not just its own directory
- Make the deploy gate run the checks it was skipping
- Let the archive's real-Postgres test run under production's privileges
- Give the analytics column types one home, beside the bodies they describe
- Move the other four cohort bodies onto ClickHouse too
- Give the analytics reads one router, and catch the deadlock it caused
- Let the cohort matrix be read from ClickHouse
- Make the cohort queries sayable to ClickHouse
- Let the cohort tab mean what every other tab means by retail
- Answer the whole SMS tab from either engine
- Move the roster and the delivery binding onto either engine
- Make the send-once claim atomic, not merely serialised
- Move the SMS tab's short statements onto either engine
- Make the half-switched /sms refuse instead of drifting
- Let the audience be read from Postgres, behind the writer's own flag
- Give the SMS audience one body, and prove it means one thing
- Decide the control group in Python, where both engines can agree
- Close the six the IDOR sweep actually found
- Ask the store which SMS tables it has, instead of assuming six
- Put the SMS tab's state where it can be answered without DuckDB
- Close the book on the two-day alerts rework
- Admins only, and three fewer reasons to swipe
- Keep the brevity, drop the translation nobody asked for
- Часы и дни говорят на языке остального предложения
- Три строки, по-русски: что случилось, сколько, что делать
- Let the page speak only of what pages, and teach the agent our own accent
- Catch the book up with the evening: third arm, agent tier, per-container memory
- Watch the container with the smallest limit, and let memory summon its agent
- Ask ClickHouse the question the other two stores already answer daily
- Let the containers write into the spool root created
- Give every fresh incident a diagnostician that can look but never touch
- The offsite channel was dead on arrival, and the archive proved it in a minute
- Six throttles become one Gate, and "every outbound message" becomes true
- Let the digest carry the ledger's tail, and never be summoned by it
- Press once on what nobody resolved, from the process that watches from outside
- Say when it stops hurting — once, and only to those who heard it hurt
- Remember what the alerting did, and watch the thing that watches
- Take back the tests that arrived before their code
- One door for the web emitters, and a cooldown that knows how old the news is
- Let the registry wait for an emitter that lives on the shared branch
- Give every alert condition one name, and make the list unable to lie
- Deliver the message before admiring its formatting
- Write down what the three rules cost, and the number that was measured
- Say who is speaking, what to do, and whether the copy is still moving
- Document the dev alerts kill switch
- Let a dev instance run everything without reaching anyone's phone
- One rhythm between blocks, one grid for tiles
- Finish what the re-review said was half-finished
- Give the ClickHouse plumbing one home, and the copies one column list
- Give Step 1 a standing place, and its button a home
- Give the archive copy a repair path, and the self-heals a paper trail
- Close the three races and get the store lock off the network
- Own every style inside its component, and give the library a Storybook
- Count the steps once
- Use the page's own components, and one panel instead of four
- Say each thing once, in one voice
- Take the null the Select actually hands back
- Say what the campaign did before showing how it was measured
- Say in the doc what production actually reads
- Make a WARN verdict name its findings in the log
- Let on-call see the mirror-landing findings without opening the database
- Run the buyers ids-diff hourly, because a one-shot leaves loss unrepairable
- Give Postgres a working set that fits again
- Page when the mirror-landing сверка goes stale, not just the digest
- Let the buyers backfill run itself, because nobody can run it from outside
- Wire the third engine into the scheduler, the network and the docs
- Give ClickHouse silver, a Gold it computes itself, and the archive
- Land the buyers, the line level, and a customer row built from facts
- Let the Gold-only reads come from Postgres, behind a flag
- Say when this file was last true
- Stop the archive's own seed from reading as a flood
- Write down why the order archive is a side branch built first
- Archive every order version, in the transaction that writes it
- Record the engine switch in the file that performs it
- Make the bot's healthcheck say something true
- Give the port a Postgres adapter, and a switch that cannot half-happen
- Put a port in front of the bot's state so the engine can change
- Put the access-control logic under test, and fix the clock it reads by
- Copy the bot's own state, the only store that is in no backup
- Replicate the five tables that have no source to be rebuilt from
- Compute Gold inside Postgres, and compare it against DuckDB's
- Give the Gold measures one home before Postgres needs them too
- Tell the gateway's hang-ups apart from payloads it never finished sending
- Replicate the manager numbers that are recomputed after the copy
- Restore the August campaign's message and bill, and say it was restored
- Open a campaign and see what it actually was
- Send the text that was written, not a second one typed at the door
- Raise the delivery-report ceiling to clear a whole send
- Say the recency window in dates, not only in days
- Stop the live preview firing a request per keystroke
- Take the statistics off the manager's path
- Separate the value level from the way the result is measured
- Say that the arms are one audience cut up, not three audiences
- Stop splitting into value tiers by default, and show their cut-offs
- Keep the draft, name the save button, and price the campaign on screen
- Make the filters legible: bigger controls, an icon and an example each
- Put each explanation under the control it explains
- Filters first, then how the audience is split
- Say what a saved audience actually contains
- Make saved audiences a list you choose from
- Name the campaign first, and keep saving an audience with loading one
- Put the step action in the same place on every step
- Rehearse the text that is about to go out
- Give the base window a control, and group the filters by what they ask
- Name the tiers, stop numbering them
- Open the filter panel by default
- Gate the wizard's endpoints on the sms permission too
- Build the campaign audience from filters, not from one fixed cohort
- Let a marketer run SMS campaigns without being an admin


## 3.0.125

- Reconcile the two computations of Silver (#146)


## 3.0.124

- Rebuild Postgres Silver after the warehouse refresh (#145)


## 3.0.123

- Compute Silver inside Postgres (#144)


## 3.0.122

- silver.orders — the first table Postgres computes rather than receives (#143)


## 3.0.121

- One Silver projection, rendered for two engines (#142)


## 3.0.120



## 3.0.119

- Replicate the classification at startup, not only once a day (#140)
- Pin the build-time base images by digest (#141)


## 3.0.118

- Make the suite run in a clean checkout (#139)


## 3.0.117

- Run the suite on every pull request (#138)


## 3.0.116

- Replicate the manager classification into Postgres (#137)


## 3.0.115

- Reconcile Postgres against KeyCRM, on the fetch we already paid for (#136)
- Record that the Ark left the machine


## 3.0.114

- Compare orders between the two stores, at a size that forbids the obvious method (#135)


## 3.0.113

- Mirror orders and their line items into Postgres (#134)


## 3.0.112

- Check that Postgres holds what DuckDB holds (#133)


## 3.0.111



## 3.0.110

- Write the catalogue to Postgres as well (#132)


## 3.0.109

- Give the catalogue a home in Postgres (#131)


## 3.0.108

- Read a KeyCRM payload once, for however many stores want it (#130)


## 3.0.107

- Write down the two preflight steps that only the host can do (#129)


## 3.0.106

- Read what the migration exited with, instead of assuming (#128)


## 3.0.105

- Apply the schema on the deploy that carries it (#127)


## 3.0.104

- Give this repository a Postgres connection and a schema it owns (#126)


## 3.0.103

- Let the Unknown brand be selected, not just seen (#125)


## 3.0.102

- Say which measure the revenue trend is showing, and stop a filter changing it (#124)


## 3.0.101

- Watch the landing→Silver arc, which only a row count watched (#123)


## 3.0.100

- Hand the shop its own provisioning back, and give this application a schema of its own (#122)


## 3.0.99

- Stop the shop from owning its own tables, so row level security can ever apply (#121)


## 3.0.98

- Send the shop's tables to the shop's schema, not to public (#120)


## 3.0.97

- Give the shop a schema of its own, and a way for provisioning to reach a live database (#119)
- Record the third defect, the one that hid while fixing the second


## 3.0.96

- Let nginx find web again after it moves (#118)


## 3.0.95

- Make the deploy's health gate reach web, which it never did (#117)


## 3.0.94

- The Postgres backup and its drill follow the server that they back up (#116)
- Record what the Postgres move actually cost, in the runbook that led it


## 3.0.93

- Bring Postgres home: the store lives beside the app that writes it (#115)


## 3.0.92

- The Ark: freeze the warehouse so it outlives the code that wrote it


## 3.0.91

- Drop the DuckDB copies of three tables the bot owns (#114)


## 3.0.90

- Back up the bot's database, and give the language one home (#113)


## 3.0.89

- Let the schema ledger reach /api/health (#112)


## 3.0.88

- Give the schema a ledger: migrations run once, and cannot fail in silence (#111)


## 3.0.87

- Move revenue, SMS segments and campaign results onto the level (#110)


## 3.0.86

- Move basket intelligence, chat tools and search onto the level (#109)


## 3.0.85

- Name the order line: silver_order_lines (#108)


## 3.0.84

- Resolve a manager's classification as of the order's date (#107)


## 3.0.83

- Give the Silver row one definition (#106)


## 3.0.82

- Move order_status_refresh off the compaction instant, and mark dirty in the method (#105)


## 3.0.81

- Drop every index on gold_daily_products (#104)


## 3.0.80

- A catalog change widens one scope, not four (#103)


## 3.0.79

- Rebuild gold_daily_traffic for the dates that moved (#102)


## 3.0.78

- Name KeyCRM source 5 «Виставка» and give it its own sales_type (#101)


## 3.0.77

- Bump the version again, after four months of green no-ops (#100)
- Say a standing DQ finding once, not every morning (#99)
- Fix the two defects the cleanup listed and left (#98)
- Remove 6151 lines of dead code, behind a suite that now runs (#97)
- Send technical alerts to every admin, not the first one (#96)
- Do not call a restarted watchdog a stopped one (#95)
- Let something outside the process say the watchdog is alive (#94)
- Watch the floor the compact leaves behind (#93)
- Watch the whole directory, and name what grew (#92)
- Document how a sub-account key actually gets installed (#91)
- Stop warning about the layers the snapshot deliberately omits (#90)
- Let the snapshot container write to its own work directory (#89)
- Take the off-site copy nightly instead of weekly (#88)
- Measure memory that has to fit, not memory the kernel lends back (#87)
- Keep two local backups, not seven (#86)
- Let the import finish when there is no source database (#85)
- Give the restore drill write access, not just read (#84)
- Let a non-root container read the restored archive (#83)
- Mount the drill script instead of baking it into the image (#82)
- Give sftp its own port flag (#81)
- Ship the warehouse export off the box every week (#78)
- Notice when a day of stock history goes missing (#79)
- Stop paging on a percentage that resets every compaction (#77)
- Stop publishing the session handoffs (#80)
- Put the two handoffs where the next session will find them
- Rebuild what changed, not what was looked at (#70)
- Speak the whole bot in the reader's language, not just the report (#76)
- Send the week to everyone, in the language each of them reads (#74)
- Weekly report, three languages, and a menu that stays reachable (#73)
- Record the Redis investigation and what came of it
- Delete the cache that was never there (#72)
- Log how long a request actually took (#68)
- Stop dropping every SMS delivery report (#67)
- Stop documenting a cache that has never run (#69)
- Do not hold a request open for ten minutes (#66)
- Let the reconciliation look further back than ninety days (#65)
- Check the thirteen columns nothing was checking (#64)
- Do not deploy a paragraph (#63)
- Track the project doc, minus the things a public repo should not carry (#62)
- The test suite must not be able to page anyone (#61)
- Compare the cells, and stop calling a job a defect (#60)
- Add the column to the table that already exists (#59)
- Ask KeyCRM what counts as a sale, instead of remembering (#58)
- Give the third category a name, and a door with a lock (#56)
- Run the check we were not alive for (#55)
- Recompute the date an order left, not only the one it arrived at (#54)
- Do not call the wholesale manager unclassified (#53)
- Let a human say who is retail, and notice revenue that is neither (#52)
- Report the next run APScheduler actually has (#51)
- Say the findings out loud, and say each one once (#50)
- Notice when a check stops producing verdicts (#49)
- Queue deploys instead of letting them fight over a container (#48)
- Let jobs run late instead of not at all (#47)
- Register three data agents, and give them what the incidents cost (#46)
- Give the integrity module the logger it was already calling (#45)
- Let the backfill finish: stop hiding failures and stop fearing a 404 (#44)
- Fetch back the orders no date-window sync can reach (#43)
- Compare orders, not just their monthly sums (#42)
- Count a customer as new only if they have never bought before (#41)
- Credit an order to the month it was ordered in, whoever finds it (#40)
- Let a stuck warehouse try again instead of waiting for a human (#39)
- Say it once — an alarm that repeats every two minutes is furniture (#38)
- Stop blaming the warehouse for the ruler's own bends (#37)
- Give the warehouse room to breathe, and a voice when it cannot (#36)
- Take the DuckDB patches that fix our own failure modes (#35)
- Take 5 550 customers' phone numbers out of a public repository (#34)
- Let the money look as unsettled as it is (#33)
- Stop certifying a campaign nobody has measured yet (#31) (#32)
- Read a campaign down a column, not across four cards (#30)
- Let the window match an offer that lasts two days (#29)
- Write the status the results already read (#28)
- Put every arm of a campaign on one axis (#27)
- Say what the result figures mean, and show revenue beside margin (#26)
- Keep the never-messaged out of the arm they never joined (#25)
- Measure from the moment the message went out (#24)
- Claim a campaign before sending it, not after (#23)
- Split a roster the gateway will not take in one call (#22)
- Count an emoji as the two units the operator bills (#21)
- Stop recreating containers the deploy did not change (#20)
- Let a campaign choose which tiers it is for (#19)
- Treat every TurboSMS success code as a success (#18)
- Send over Viber, with SMS as the fallback (#17)
- Report what the server said, not what the status code implies (#16)
- Rehearse an SMS before it becomes a campaign (#15)
- Say why the SMS list is the size it is (#14)
- Send a campaign from the dashboard (#12)
- Migrate delivery columns onto an existing sms_campaign_members (#10)
- TurboSMS integration: send, delivery reports, and opt-outs (#9)
- SMS campaigns UI: tiers, export, and results read against the control (#8)
- Measure a campaign against its holdout, with an interval (#6)
- Freeze the SMS campaign roster, and carry the last order into the export (#5)
- Data platform fixes (#3)
- docs: ultra-deep audit findings (parser, NULL-overwrite, 'other' bucket)
- upsert_orders: never overwrite manager_comment with NULL
- Harden UTM parser: recover metadata the strict format was dropping
- UTM Campaigns table: sortable columns + traffic type / platform filters
- docs: Traffic Attribution deep-dive (pipeline, classification, gaps)
- Traffic: UTM campaigns table, Google ads/organic split, fix TOF/MOF misattribution
- Disk growth watchdog: alert on capacity and 24h growth-rate breaches
- compact: auto-swap inside sidecar removes "operator died" outage class
- upsert_orders: skip-if-unchanged eliminates 1440x write amplification
- Data Quality framework: orchestrator + scheduler jobs + endpoint
- Data Quality framework: Layer 1 + Layer 2 foundations
- Bronze invariant watchdog: catch config drift in 6h, not 30 days
- Bronze alert: defense-in-depth mode gate
- Bronze shadow-write becomes opt-in in legacy mode
- Bronze prune: mode-aware retention policy
- Fix compact preflight: account for source DB already on disk
- Fix: UserRow falls back to generated avatar on photo_url 404
- Security followup 2 (commit D2): robustness — case-insensitive prod detection, clean stale defaults, surface dirty server tree before reset
- Security followup 2 (commit C2): close audit-invariant escape hatches
- Security followup 2 (commit B2): delete dead /v1 dashboard
- Security followup 2 (commit A2): fix WebApp auth — server-side HttpOnly cookie
- Security followup (commit I): drop brittle spelling-grep assertion
- Security followup (commit H): advisory pip-audit in build job
- Security followup (commit G): tighten rate-limit key, trim docstring
- Security followup (commit F): deploy pinned to approved commit SHA
- Security followup (commit E): chat router gated once at include level
- Security followup (commit D): tighten CSP — drop 'unsafe-inline' from script-src
- Security followup (commit C): nginx headers via include + expires
- Security followup (commit B): unify /api lockdown behind a single api_gate
- Security followup (commit A): Tier 1 bug fixes from review
- CI/CD hardening: split build/deploy, manual-approval gate, SHA-pin actions
- Fix Dockerfile.web: glob web/*.py so new modules are included
- Add security-hardening regression tests
- Security hardening: medium/low findings (#10-#16)
- Security hardening: lock down API surface, fix auth gaps (#1-#9)
- Refactor UI: enforce component visual boundaries (#1)
- Lower compact preflight margin 1.5x → 0.8x source size
- Add internal canary: HTTPS health probe + cert expiry watcher
- Add weekly_compact.sh: host-cron wrapper for automated compaction
- Reallocate memory budget: 7g container + 3g DuckDB buffer
- Traffic analytics: detect AI assistants (ChatGPT) as new 'ai' platform
- Hide cost basis from SKU table and Brand rotation reports
- SkuRotationTable: show retail value alongside cost basis
- SkuRotationTable: add Last sale column
- SKU rotation table: 4 action presets, smart suggestions, CSV export
- Dead stock deep analytics: cost basis, GMROI, NPV decision, brand rotation
- Add unit tests for Silver incremental refresh
- Silver incremental: clean orphan rows by including silver-side scope
- Silver incremental refresh: scope DELETE+INSERT to changed_ids + cascade
- Support automated compact: ship scripts/ + register bronze sequence
- Fix stale buyer_name in Meili orders index
- Reduce memory pressure: incremental Meili sync, hourly checkpoint, fix alert
- Add sync_mode field to HealthResponse schema
- H3 Phase 3-5: SYNC_MODE cutover, backfill, prune, replay, alerts
- H3 Phase 2: promotion job bronze → orders_v2 shadow table with diff
- H3 Phase 1: bronze_order_events audit log with dual-write shadow path
- Make reconciliation endpoint non-blocking by default
- Add compact_duckdb.py maintenance script
- Expand UTM classifier: Advantage+, fbsales, telegram, cpc* prefixes
- Add memory monitor job with Telegram alerts at 75%/90%
- Rewrite reconciliation to compare per-order, not counts
- Replace ON CONFLICT with explicit SELECT→UPDATE/INSERT in upsert_orders
- Fix gold_daily_traffic GROUP BY: repeat COALESCE expressions explicitly
- Fix gold_daily_traffic PK violation: GROUP BY COALESCE'd aliases, not raw NULLs
- Fix gold_daily_traffic PK conflict on incremental UTM rebuild
- Fix UTM refresh holding DB lock for 37K rows, blocking health checks
- Fix deadlock: mark_warehouse_dirty called inside connection lock
- Fix warehouse refresh destroying silver: always full rebuild + fix sequences
- Fix ImportError in rebuild-silver: import constants from duckdb_constants, not config
- Add /warehouse/rebuild-silver: DROP+CREATE+INSERT to bypass MVCC corruption
- Bump DuckDB 1.5.1 → 1.5.2 for race condition and index bugfixes
- Add admin endpoint to purge poisoned orders and CHECKPOINT
- Catch ConstraintException in upsert_orders, fallback to UPDATE
- Isolate poisoned rows in orders upsert: skip, don't block batch
- Stabilize DuckDB writes: drop RMW, raise WAL threshold, serialize warehouse refresh
- Fix orders upsert for DuckDB 1.5: autocommit per-row ON CONFLICT
- Fix PK violation: fallback to UPDATE when ON CONFLICT fails
- Fix orders upsert: use per-row execute instead of broken executemany
- Fix orders upsert: use ON CONFLICT instead of INSERT OR REPLACE
- Fix PK violation in upsert_orders: use INSERT OR REPLACE
- Serve React SPA at root (/) instead of /v2
- Add date filter support to marketing report (any period, not just months)
- Pin DuckDB <1.6.0 to prevent unexpected major version upgrades
- Fix DuckDB 1.5 NaN→INT32 conversion error breaking sync
- Fix login 500 error: update TemplateResponse for Starlette 1.0
- Add same month previous year (YoY) comparison to marketing report
- Fix SyntaxError: use list comprehension instead of generator for unpacking
- Add CSV export for marketing report (3 months side by side)
- Add monthly marketing report to Marketing tab
- Fix DuckDB upsert_orders write-write conflict and PK violations
- Fix CI version bump race condition on concurrent pushes


## 3.0.76

- Fix silver_orders OOM: two-pass rebuild, skip warehouse refresh on non-order changes
- Add all-products breakdown with search to Reports tab


## 3.0.75

- Increase DuckDB memory limit to 4GB (checkpoint OOMs at 2GB on 9GB DB)


## 3.0.74

- Fix OOM on startup: remove DEFAULT from ALTER TABLE migration


## 3.0.73

- Add Tier 3: validation retry, auto-resync on drift, dead code cleanup, reconciliation API


## 3.0.72

- Add warehouse robustness Tier 2: audit columns, reconciliation, dirty flag decoupling
- Fix data consistency: use UPDATE+INSERT for all order upserts, stop advancing empty checkpoint


## 3.0.71

- Prevent DuckDB OOM: skip unused self-join, enable disk spilling, serialize heavy jobs


## 3.0.70

- Fix stale returns: use UPDATE instead of INSERT OR REPLACE for status refresh


## 3.0.69

- Fix stale returns: skip product re-insertion during status refresh


## 3.0.68

- Add stale returns diagnostics and bump container mem_limit to 6g


## 3.0.67

- Bump container mem_limit to 5g — 4g OOM-killed during status refresh


## 3.0.66

- Fix margin page: render AppShell outside AdminGuard so sidebar shows


## 3.0.65

- Bump DuckDB memory_limit to 3GB — 2.5GB still OOMs on 60-day status refresh


## 3.0.64

- Fix status refresh OOM: incremental Silver rebuild + memory bump


## 3.0.63

- Restrict Margin Analysis page to admin users only
- Add Margin Analysis page with full frontend and backend


## 3.0.62

- Add /admin/* SPA route so admin page links work on direct access


## 3.0.61

- Fix user management: enforce viewer default, use DB-backed permissions


## 3.0.60

- Fix offer_stocks: add PK migration, switch to INSERT OR REPLACE


## 3.0.59

- Harden DuckDB: safe ROLLBACK, fix sync gaps, fix customer metrics


## 3.0.58

- Fix DuckDB write-write conflict: use INSERT OR REPLACE for order upsert


## 3.0.57

- Fix Today/Yesterday showing no data: use Kyiv timezone for date calculations


## 3.0.56

- Fix double-slash paths in sidebar navigation


## 3.0.55

- Fix DuckDB upsert: use temp table instead of registered DataFrame view


## 3.0.54

- Add INSERT OR IGNORE as safety net for duplicate order inserts


## 3.0.53

- Fix duplicate key crash in upsert_orders during startup sync


## 3.0.52

- Remove /v2 prefix from all navigation links


## 3.0.51

- Move Promocode Analytics from main dashboard to Marketing section


## 3.0.50

- Make User Management table header sticky


## 3.0.49

- Migrate SQLite users to DuckDB on startup


## 3.0.48

- Fix login denied for users not yet in DuckDB


## 3.0.47

- Fix revenue trend showing wrong data for sales_type=all


## 3.0.46

- Fix ML forecast total using actual + predicted revenue


## 3.0.45

- Fix NameError in get_product_performance — build top_by_revenue dict


## 3.0.44

- Add promocode performance overview to dashboard


## 3.0.43

- Fix hardcoded 365-day sync in web/main.py — change to 730 days


## 3.0.42

- Extend default sync to 730 days for accurate YoY comparisons


## 3.0.41

- Fix OOM during DuckDB sync — limit memory, reduce chunk size, increase container limit


## 3.0.40

- Fix DuckDB WAL corruption on aarch64 — add checkpoint after each sync chunk


## 3.0.39

- Add promocode filter to dashboard — full stack (API → DuckDB → React)


## 3.0.38

- Fix sku_inventory_status same PK issue — DELETE+INSERT with temp table


## 3.0.37

- Fix stale stock data — use DELETE+INSERT instead of INSERT OR REPLACE


## 3.0.36

- Update milestones — daily 300K, remove weekly 800K, add YoY comparison


## 3.0.35

- Fix DuckDB deadlock — pass conn to helper methods instead of re-acquiring lock


## 3.0.34

- Improve monthly goal algorithm — weighted blend, ML signal, dynamic caps


## 3.0.33

- Fix monthly goal using wrong YoY growth — exclude incomplete months/years


## 3.0.32

- Fix inflated avg check in Reports — grand_total was summed per product row


## 3.0.31

- Add sell-through velocity section title and description (i18n)


## 3.0.30

- Fix days-of-supply: use 90-day velocity instead of 30-day


## 3.0.29

- Add ABC classification descriptions


## 3.0.28

- Add overstocked status to Inventory Health


## 3.0.27

- Add clickable ABC cards with expandable SKU list


## 3.0.26

- Show current stock value in gauge labels


## 3.0.25

- Remove grey gap from stock gauge and add color legend


## 3.0.24

- Make optimal stock params configurable via API and UI


## 3.0.23

- Fix TS build errors in InventoryTurnoverChart
- Add new inventory
- Add inventory turnover & optimal stock analytics


## 3.0.22

- Fix Decimal TypeError in cohort insights and 429 retry storm


## 3.0.21

- Add cohort analysis enhancements: insights, skeletons, i18n, and controls


## 3.0.20

- Add SVG skeleton loaders and improve MilestoneProgress visuals


## 3.0.19

- Fix date filtering for product pairs & brand affinity endpoints


## 3.0.18

- Upgrade MilestoneProgress with rich SVG animations and Lottie confetti


## 3.0.17

- Add Vector


## 3.0.16

- Show user name and username next to avatar in expanded sidebar


## 3.0.15

- Fix i18n: hardcoded qty strings, fix 60+ bad DeepL translations


## 3.0.14

- i18n: translate MilestoneProgress, rename to Main Dashboard


## 3.0.13

- Move ROI Calculator to new Marketing page, reorder sidebar tabs


## 3.0.12

- Fix i18n: replace hardcoded English strings with t() calls in charts


## 3.0.11

- Sidebar UX: push content with smart formula, collapsed nav icons, language accordion


## 3.0.10

- Restyle language selector as vertical dropdown list with checkmark


## 3.0.9

- Change language selector from cycle to dropdown with all options


## 3.0.8

- Add multi-language support (EN/UK/RU) with react-i18next


## 3.0.7

- Add info popovers to Product Intelligence page metrics


## 3.0.6

- Bump web container memory to 2GB, remove DuckDB memory limit


## 3.0.5

- Fix OOM: use staged temp tables for gold_product_pairs, limit memory to 400MB


## 3.0.4

- Fix OOM crash: limit DuckDB memory + optimize gold_product_pairs query


## 3.0.3

- Add Product Intelligence page with basket analysis, pairs, and momentum


## 3.0.2

- Add winsorized LightGBM training to reduce promo spike distortion


## 3.0.1

- Bump major version to 3.0.0


## 2.0.2

- Fix CI bump: disable checkout credential helper override
- Fix VERSION file missing from Docker images
- Add WAPE metric to predictions and improve CSV export format
- Add auto-versioning with git tags, Docker tags & changelog
- Remove zero-gain dow_event_interaction feature (32→31)
- Fix undefined val_dows after DOW correction refactor
- Add DOW-specific features and expand DOW correction window
- Remove 2 zero-gain features: is_weekend, log_trend_index (31→29)
- Improve revenue prediction: expand to 31 features, widen DOW correction
- export csv added
- Fix inventory queries: convert to f-strings for INTERVAL interpolation
- Fix INTERVAL parameterization: DuckDB rejects ? for all INTERVAL types
- Fix cohort analysis: DuckDB INTERVAL parameterization for months
- Fix 4 critical ML prediction bugs causing wrong forecasts
- Fix goals crash + SQL parameterization + thread-safety improvements
- extra changes
- Fix bot report_service injection into correct module
- Add stock movement tracking and fix inventory data accuracy
- fonts fixed
- Fix data layer: OrderStatus import, BIGINT migration, validation, UTM & traffic


## 2.0.1

- Initial versioned release
- Auto-versioning with git tags and Docker image tagging
