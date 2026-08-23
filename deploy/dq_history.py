import glob, duckdb
f = sorted(glob.glob("/data/backups/analytics-*.duckdb"))[-1]
print("reading", f, "\n")
c = duckdb.connect(f, read_only=True)

print("== reconciliation runs by month ==")
for r in c.execute("""
    SELECT strftime(started_at,'%Y-%m') AS month, COUNT(*) AS runs,
           COUNT(*) FILTER (WHERE error_message IS NULL AND status <> 'FAILED') AS ok,
           COUNT(*) FILTER (WHERE error_message ILIKE '%429%')                  AS rate_limited,
           COUNT(*) FILTER (WHERE error_message IS NOT NULL
                              AND error_message NOT ILIKE '%429%')              AS other_err
    FROM data_quality_runs WHERE layer='reconciliation' GROUP BY 1 ORDER BY 1
""").fetchall():
    print("  %s  runs %3d  ok %3d  429 %3d  other %3d" % r)

rows = c.execute("""
    SELECT strftime(started_at,'%Y-%m-%d %H:%M'), status,
           COALESCE(substr(error_message,1,60),'')
    FROM data_quality_runs
    WHERE layer='reconciliation' AND started_at > now() - INTERVAL '30 days'
    ORDER BY started_at DESC
""").fetchall()
print("\n== last 30 days: %d runs ==" % len(rows))
for r in rows:
    print("  %s  %-9s %s" % r)

print("\n== landing->Silver arc findings, if any ==")
arc = c.execute("""
    SELECT strftime(r.started_at,'%Y-%m-%d %H:%M'), i.check_name, i.severity,
           i.count, substr(i.description,1,80)
    FROM data_quality_issues i JOIN data_quality_runs r USING (run_id)
    WHERE i.check_name LIKE 'silver_%' ORDER BY r.started_at DESC LIMIT 20
""").fetchall()
print("  (none — the arc is clean)" if not arc else "")
for r in arc:
    print("  %s  %-22s %-8s %6d  %s" % r)
