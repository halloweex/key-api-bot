//! `ks-api`, the pilot service.
//!
//! Configuration uses the Python app's names where they mean the same
//! thing: `KS_PG_DSN`, `KS_PG_POOL_MIN` (1), `KS_PG_POOL_MAX` (5) and
//! `KS_PG_TIMEOUT` (30 s). Its own are `KS_PILOT_LISTEN` (127.0.0.1:8090 —
//! loopback, because nothing here checks who is asking) and
//! `KS_PILOT_TEST_CLOCK=1`, which lets the harness freeze "now".

use std::error::Error;
use std::net::SocketAddr;
use std::process::ExitCode;
use std::str::FromStr;
use std::time::Duration;

use ks_api::gold::PgGold;
use sqlx::postgres::PgPoolOptions;
use tokio::net::TcpListener;

struct Config {
    dsn: String,
    listen: SocketAddr,
    pool_min: u32,
    pool_max: u32,
    timeout: Duration,
    test_clock: bool,
}

fn env_or<T: FromStr>(name: &str, default: T) -> Result<T, String> {
    match std::env::var(name) {
        Ok(value) if !value.trim().is_empty() => value
            .trim()
            .parse()
            .map_err(|_| format!("{name}={value:?} does not parse")),
        _ => Ok(default),
    }
}

impl Config {
    fn from_env() -> Result<Self, String> {
        let dsn = std::env::var("KS_PG_DSN").unwrap_or_default();
        if dsn.trim().is_empty() {
            return Err("KS_PG_DSN is not set; the pilot has no other store".into());
        }
        let timeout: f64 = env_or("KS_PG_TIMEOUT", 30.0)?;
        Ok(Self {
            dsn: dsn.trim().to_owned(),
            listen: env_or("KS_PILOT_LISTEN", SocketAddr::from(([127, 0, 0, 1], 8090)))?,
            pool_min: env_or("KS_PG_POOL_MIN", 1)?,
            pool_max: env_or("KS_PG_POOL_MAX", 5)?,
            timeout: Duration::try_from_secs_f64(timeout)
                .map_err(|_| format!("KS_PG_TIMEOUT={timeout} is not a duration"))?,
            test_clock: std::env::var("KS_PILOT_TEST_CLOCK").is_ok_and(|v| v == "1"),
        })
    }
}

async fn shutdown() {
    let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        .expect("a SIGTERM handler can be installed");
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {}
        _ = terminate.recv() => {}
    }
}

async fn run() -> Result<(), Box<dyn Error>> {
    let config = Config::from_env()?;
    // Lazy, like `core.pg.get_pool`: the first request opens the pool.
    let pool = PgPoolOptions::new()
        .min_connections(config.pool_min)
        .max_connections(config.pool_max)
        .acquire_timeout(config.timeout)
        .connect_lazy(&config.dsn)?;
    if config.test_clock {
        tracing::warn!(
            "KS_PILOT_TEST_CLOCK=1: the X-Pilot-Now header sets the clock. \
             A test hook — never where a person reads the answer."
        );
    }
    let app = ks_api::router(ks_api::AppState::new(
        PgGold::new(pool, config.timeout),
        config.test_clock,
    ));
    let listener = TcpListener::bind(config.listen).await?;
    tracing::info!(listen = %config.listen, "ks-api listening");
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown())
        .await?;
    Ok(())
}

#[tokio::main]
async fn main() -> ExitCode {
    tracing_subscriber::fmt().with_target(false).init();
    match run().await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            tracing::error!("{error}");
            ExitCode::FAILURE
        }
    }
}
