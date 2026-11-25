//! `ks-worker`: the core's jobs in Rust (plan, revision 4). К1 runs one,
//! `ch_sync`, the hourly ClickHouse copy.
//!
//! ```text
//! ks-worker ch-sync --target shadow|live [--history-chunk N] [--dump DIR]
//! ks-worker run
//! ```
//!
//! `ch-sync` is one run, its answer on stdout in Python's keys
//! (`_run_ch_sync`'s result), exit 1 when either half failed. `--dump`
//! writes every statement sent to ClickHouse (`statements.jsonl`) and every
//! finding (`findings.jsonl`): what the harness compares with Python's.
//!
//! `run` is the long-running worker: an hourly tick, as `IntervalTrigger(hours=1)`
//! fires — the first an hour after the start. Each tick runs the live job
//! only if the switch names Rust (`KS_EXECUTOR_CH_SYNC=rust`, the stand-in
//! until К0's contract), and the shadow if `KS_WORKER_SHADOW` lists
//! `ch_sync`; each under its own lease, a tick skipped when another process
//! holds it. Nothing in К1 starts this process anywhere.

use std::path::PathBuf;
use std::process::ExitCode;
use std::time::Duration;

use ks_core::alert::{AlertSink, LogSink, MemorySink, Severity};
use ks_core::ch::{ClickHouse, Recorder};
use ks_core::lease::{EnvSwitch, Executor, ExecutorSwitch, JobLease, PgAdvisoryLease, lease_name};
use ks_core::pg::PgSettings;
use ks_core::{ch, config};
use ks_worker::ch_sync::{self, ChSync, HISTORY_CHUNK, JOB_ID, Names, Target};
use serde_json::{Value, json};
use sqlx::PgPool;

const USAGE: &str = "usage:
  ks-worker ch-sync --target shadow|live [--history-chunk N] [--dump DIR]
  ks-worker run";

/// Which jobs the long-running worker shadows.
const SHADOW_ENV: &str = "KS_WORKER_SHADOW";
const PERIOD: Duration = Duration::from_secs(3600);

struct Once {
    target: Target,
    chunk: usize,
    dump: Option<PathBuf>,
}

fn parse(args: &[String]) -> Result<Once, String> {
    let mut target = None;
    let mut chunk = HISTORY_CHUNK;
    let mut dump = None;
    let mut args = args.iter();
    while let Some(arg) = args.next() {
        let mut value = || args.next().ok_or(format!("{arg} needs a value"));
        match arg.as_str() {
            "--target" => {
                target = Some(match value()?.as_str() {
                    "live" => Target::Live,
                    "shadow" => Target::Shadow,
                    other => return Err(format!("--target is live or shadow, not {other}")),
                });
            }
            "--history-chunk" => {
                chunk = value()?
                    .parse()
                    .ok()
                    .filter(|n: &usize| *n > 0)
                    .ok_or("--history-chunk is a positive number")?;
            }
            "--dump" => dump = Some(PathBuf::from(value()?)),
            other => return Err(format!("unknown argument {other}")),
        }
    }
    Ok(Once {
        target: target.ok_or("--target is required: live or shadow")?,
        chunk,
        dump,
    })
}

fn stood_down(reason: &str) -> Value {
    json!({ "skipped": reason, "history": { "skipped": reason } })
}

fn failed(reason: &str) -> Value {
    json!({ "error": reason, "history": { "error": reason } })
}

fn has_error(result: &Value) -> bool {
    result.get("error").is_some() || result["history"].get("error").is_some()
}

/// The job once, under its lease. `None` from `ClickHouse::from_env` is
/// Python's stand-down: no `KS_CH_URL`, nothing touched.
async fn run_once(
    settings: &PgSettings,
    pool: &PgPool,
    clickhouse: &ClickHouse,
    target: Target,
    chunk: usize,
    alerts: &dyn AlertSink,
) -> Value {
    if target == Target::Live {
        match EnvSwitch.executor(JOB_ID).await {
            Ok(Executor::Rust) => {}
            Ok(Executor::Python) => {
                return stood_down(&format!(
                    "{} says python runs {JOB_ID}",
                    EnvSwitch::variable(JOB_ID)
                ));
            }
            Err(error) => return stood_down(&error.to_string()),
        }
    }
    let name = lease_name(JOB_ID, target == Target::Shadow);
    let lease = match PgAdvisoryLease::new(settings.options.clone(), settings.timeout)
        .try_acquire(&name)
        .await
    {
        Ok(Some(lease)) => lease,
        Ok(None) => return stood_down(&format!("another process holds {name}")),
        Err(error) => return failed(&error.to_string()),
    };
    let result = ChSync {
        pg: pool,
        timeout: settings.timeout,
        ch: clickhouse,
        names: Names::new(target, &ch_sync::history_db()),
        history_chunk: chunk,
        alerts,
    }
    .run()
    .await;
    if let Err(error) = lease.release().await {
        tracing::warn!("{name}: {error}");
    }
    result
}

async fn once(args: &[String]) -> ExitCode {
    let options = match parse(args) {
        Ok(options) => options,
        Err(error) => {
            eprintln!("{error}\n{USAGE}");
            return ExitCode::from(2);
        }
    };
    let recorder = Recorder::default();
    let alerts = MemorySink::default();
    let result = match (ClickHouse::from_env(), PgSettings::from_env()) {
        (Ok(None), _) => stood_down(&format!("{} is not set", ch::URL_ENV)),
        (Err(error), _) => failed(&error.to_string()),
        (_, Err(error)) => failed(&error.to_string()),
        (Ok(Some(clickhouse)), Ok(settings)) => {
            let clickhouse = clickhouse.recording(recorder.clone());
            let pool = settings.pool();
            let result = run_once(
                &settings,
                &pool,
                &clickhouse,
                options.target,
                options.chunk,
                &alerts,
            )
            .await;
            pool.close().await;
            result
        }
    };
    if let Some(dir) = &options.dump
        && let Err(error) = dump(dir, &recorder, &alerts)
    {
        eprintln!("--dump {}: {error}", dir.display());
        return ExitCode::from(2);
    }
    println!("{result}");
    if has_error(&result) {
        ExitCode::from(1)
    } else {
        ExitCode::SUCCESS
    }
}

fn dump(dir: &std::path::Path, recorder: &Recorder, alerts: &MemorySink) -> std::io::Result<()> {
    std::fs::create_dir_all(dir)?;
    let mut statements = String::new();
    for sent in recorder.sent() {
        let body = sent.body.map(|b| String::from_utf8_lossy(&b).into_owned());
        statements.push_str(&json!({ "sql": sent.sql, "body": body }).to_string());
        statements.push('\n');
    }
    std::fs::write(dir.join("statements.jsonl"), statements)?;
    let mut findings = String::new();
    for finding in alerts.findings() {
        let severity = match finding.severity {
            Severity::Info => "INFO",
            Severity::Warn => "WARN",
            Severity::Critical => "CRITICAL",
        };
        findings.push_str(
            &json!({
                "key": finding.key,
                "severity": severity,
                "count": finding.count,
                "message": finding.message,
            })
            .to_string(),
        );
        findings.push('\n');
    }
    std::fs::write(dir.join("findings.jsonl"), findings)
}

async fn shutdown() {
    let mut terminate =
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(signal) => signal,
            Err(_) => {
                let _ = tokio::signal::ctrl_c().await;
                return;
            }
        };
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {}
        _ = terminate.recv() => {}
    }
}

async fn run() -> ExitCode {
    let settings = match PgSettings::from_env() {
        Ok(settings) => settings,
        Err(error) => {
            eprintln!("{error}");
            return ExitCode::from(2);
        }
    };
    let shadow = config::trimmed(SHADOW_ENV)
        .is_some_and(|jobs| jobs.split(',').any(|job| job.trim() == JOB_ID));
    let pool = settings.pool();
    let mut ticks = tokio::time::interval_at(tokio::time::Instant::now() + PERIOD, PERIOD);
    ticks.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    tracing::info!(shadow, "ks-worker: {JOB_ID} every {} s", PERIOD.as_secs());
    let stop = shutdown();
    tokio::pin!(stop);
    loop {
        tokio::select! {
            _ = ticks.tick() => {}
            _ = &mut stop => break,
        }
        let clickhouse = match ClickHouse::from_env() {
            Ok(Some(clickhouse)) => clickhouse,
            Ok(None) => continue,
            Err(error) => {
                tracing::error!("{error}");
                continue;
            }
        };
        let targets = [Target::Live, Target::Shadow];
        for target in targets {
            if target == Target::Shadow && !shadow {
                continue;
            }
            let result = run_once(
                &settings,
                &pool,
                &clickhouse,
                target,
                HISTORY_CHUNK,
                &LogSink,
            )
            .await;
            if result.get("skipped").is_none() {
                tracing::info!(?target, "ClickHouse sync: {result}");
            }
        }
    }
    pool.close().await;
    ExitCode::SUCCESS
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> ExitCode {
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_target(false)
        .init();
    let args: Vec<String> = std::env::args().skip(1).collect();
    match args.first().map(String::as_str) {
        Some("ch-sync") => once(&args[1..]).await,
        Some("run") => run().await,
        _ => {
            eprintln!("{USAGE}");
            ExitCode::from(2)
        }
    }
}
