//! `ks-api`, the pilot service.
//!
//! Configuration uses the Python app's names where they mean the same
//! thing: `KS_PG_DSN`, `KS_PG_POOL_MIN` (1), `KS_PG_POOL_MAX` (5),
//! `KS_PG_TIMEOUT` (30 s), and the session's signing key — `DASHBOARD_SECRET_KEY`,
//! or `BOT_TOKEN` when that is empty, as `web/routes/auth.py` falls back.
//! Access is read where production keeps it, so `KS_USER_STORE` and
//! `KS_BOT_STORE` must both say `postgres`; the pilot reads no other store.
//!
//! Its own: `KS_PILOT_LISTEN` (127.0.0.1:8090); `KS_HARDCODED_ADMIN_IDS`, the
//! ids `core.permissions.ADMIN_USER_IDS` holds — given at start rather than
//! written down again, and refused unless they hash to what the contract
//! says the code holds; and, in a `test-clock` build only,
//! `KS_PILOT_TEST_CLOCK=1`, which lets the harness freeze the trend's "now".
//!
//! `ks-api healthcheck` is the container's healthcheck: it asks the running
//! service's `/readyz` and exits 0 or 1, since the image has no shell.

use std::collections::BTreeSet;
use std::error::Error;
use std::net::SocketAddr;
use std::process::ExitCode;
use std::str::FromStr;
use std::time::Duration;

use ks_api::access::contract::Contract;
use ks_api::access::gate::Access;
use ks_api::access::routing::Router;
use ks_api::access::signer::SessionKey;
use ks_api::access::store::PgAccess;
use ks_api::gold::PgGold;
use sha2::{Digest, Sha256};
use sqlx::postgres::PgPoolOptions;
use tokio::net::TcpListener;

struct Config {
    dsn: String,
    listen: SocketAddr,
    pool_min: u32,
    pool_max: u32,
    timeout: Duration,
    secret: Vec<u8>,
    admins: BTreeSet<i64>,
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

/// `os.getenv(name, default).strip().lower()`, which both store switches read.
fn store_switch(name: &str, default: &str) -> String {
    let value = std::env::var(name).unwrap_or_default();
    let value = value.trim().to_lowercase();
    if value.is_empty() {
        default.to_owned()
    } else {
        value
    }
}

fn hardcoded_admins(contract: &Contract) -> Result<BTreeSet<i64>, String> {
    let raw = std::env::var("KS_HARDCODED_ADMIN_IDS").unwrap_or_default();
    let admins = raw
        .split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(|s| {
            s.parse::<i64>()
                .map_err(|_| format!("KS_HARDCODED_ADMIN_IDS: {s:?} is not an id"))
        })
        .collect::<Result<BTreeSet<i64>, String>>()?;
    let joined = admins
        .iter()
        .map(i64::to_string)
        .collect::<Vec<_>>()
        .join(",");
    let digest: String = Sha256::digest(joined.as_bytes())
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect();
    if digest != contract.hardcoded_admins_sha256 {
        return Err(
            "KS_HARDCODED_ADMIN_IDS are not core.permissions.ADMIN_USER_IDS \
                    (their hash is not the contract's)"
                .into(),
        );
    }
    Ok(admins)
}

impl Config {
    fn from_env(contract: &Contract) -> Result<Self, String> {
        let dsn = std::env::var("KS_PG_DSN").unwrap_or_default();
        if dsn.trim().is_empty() {
            return Err("KS_PG_DSN is not set; the pilot has no other store".into());
        }
        for (name, default) in [("KS_USER_STORE", "duckdb"), ("KS_BOT_STORE", "sqlite")] {
            let value = store_switch(name, default);
            if value != "postgres" {
                return Err(format!(
                    "{name}={value:?}: the pilot reads access from Postgres only, \
                     where production keeps it"
                ));
            }
        }
        // `config.web.secret_key or config.bot.token`: unset and empty alike.
        let secret = [
            std::env::var("DASHBOARD_SECRET_KEY"),
            std::env::var("BOT_TOKEN"),
        ]
        .into_iter()
        .flatten()
        .find(|v| !v.is_empty())
        .ok_or("DASHBOARD_SECRET_KEY or BOT_TOKEN must be set")?;
        let timeout: f64 = env_or("KS_PG_TIMEOUT", 30.0)?;
        Ok(Self {
            dsn: dsn.trim().to_owned(),
            listen: env_or("KS_PILOT_LISTEN", SocketAddr::from(([127, 0, 0, 1], 8090)))?,
            pool_min: env_or("KS_PG_POOL_MIN", 1)?,
            pool_max: env_or("KS_PG_POOL_MAX", 5)?,
            timeout: Duration::try_from_secs_f64(timeout)
                .map_err(|_| format!("KS_PG_TIMEOUT={timeout} is not a duration"))?,
            secret: secret.into_bytes(),
            admins: hardcoded_admins(contract)?,
            test_clock: cfg!(feature = "test-clock")
                && std::env::var("KS_PILOT_TEST_CLOCK").is_ok_and(|v| v == "1"),
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
    let contract = Contract::load()?;
    let router = Router::new(&contract)?;
    let config = Config::from_env(&contract)?;
    // Lazy, like `core.pg.get_pool`: the first request opens the pool.
    let pool = PgPoolOptions::new()
        .min_connections(config.pool_min)
        .max_connections(config.pool_max)
        .acquire_timeout(config.timeout)
        .connect_lazy(&config.dsn)?;
    if config.test_clock {
        tracing::warn!(
            "KS_PILOT_TEST_CLOCK=1: the X-Pilot-Now header sets the trend's clock. \
             A harness build — never where a person reads the answer."
        );
    }
    let access = Access {
        key: SessionKey::derive(&config.secret, contract.session.salt.as_bytes()),
        store: PgAccess::new(
            pool.clone(),
            config.timeout,
            contract.required_revision.clone(),
        ),
        admins: config.admins,
        contract,
    };
    let app = ks_api::router(ks_api::AppState::new(
        PgGold::new(pool, config.timeout),
        access,
        router,
        config.listen,
        config.test_clock,
    ));
    let listener = TcpListener::bind(config.listen).await?;
    tracing::info!(listen = %config.listen, "ks-api listening");
    axum::serve(
        listener,
        app.into_make_service_with_connect_info::<SocketAddr>(),
    )
    .with_graceful_shutdown(shutdown())
    .await?;
    Ok(())
}

/// `ks-api healthcheck`, the container's healthcheck: this process's own
/// `/readyz`, on the address `KS_PILOT_LISTEN` names, within four seconds.
async fn healthcheck() -> ExitCode {
    let Ok(listen) = env_or("KS_PILOT_LISTEN", SocketAddr::from(([127, 0, 0, 1], 8090))) else {
        return ExitCode::FAILURE;
    };
    let target = ks_api::health::loopback(listen);
    match tokio::time::timeout(
        Duration::from_secs(4),
        ks_api::health::probe(target, "/readyz"),
    )
    .await
    {
        Ok(Ok(true)) => ExitCode::SUCCESS,
        _ => ExitCode::FAILURE,
    }
}

#[tokio::main]
async fn main() -> ExitCode {
    if std::env::args().nth(1).as_deref() == Some("healthcheck") {
        return healthcheck().await;
    }
    tracing_subscriber::fmt().with_target(false).init();
    match run().await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            tracing::error!("{error}");
            ExitCode::FAILURE
        }
    }
}
