//! Where the pilot steps aside. A 501 is the one answer nginx does not pass
//! on: `error_page 501 502` in `nginx/rust/proxy.conf` sends the request to
//! the Python app, and the person reads Python's answer. So the pilot answers
//! 501 wherever its own answer could be wrong for a reason the request does
//! not carry — the schema at a revision this build does not know, Postgres or
//! the session's stores not answering before the first read of data, no
//! connection within the pool's wait — and, by the owner's decision 5 of
//! 2026-10-29, for input the page never sends, whose parsing the pilot does
//! not emulate. Every route it answers is a GET: a request handed over has
//! changed nothing.

/// Why a request was handed to the Python app. `as_str` is the `branch` its
/// 501 names, which the harnesses count and the log carries.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Reason {
    /// The schema is not at this build's revision: a migration window, or an
    /// image behind the schema.
    Revision,
    /// Postgres did not answer the revision check, the first thing every read asks.
    Database,
    /// No connection within the pool's wait.
    Pool,
    /// The session, or the permissions behind it, could not be read.
    Session,
    /// Input the page never sends: Python's parsing answers it.
    Input,
}

impl Reason {
    pub const ALL: [Reason; 5] = [
        Reason::Revision,
        Reason::Database,
        Reason::Pool,
        Reason::Session,
        Reason::Input,
    ];

    pub fn as_str(self) -> &'static str {
        match self {
            Reason::Revision => "revision",
            Reason::Database => "database",
            Reason::Pool => "pool",
            Reason::Session => "session",
            Reason::Input => "input",
        }
    }

    pub fn sentence(self) -> &'static str {
        match self {
            Reason::Revision => "the schema is not at this build's revision",
            Reason::Database => "Postgres did not answer before the first read",
            Reason::Pool => "no connection came within the pool's wait",
            Reason::Session => "the session could not be read",
            Reason::Input => "the page never sends this input",
        }
    }
}

/// The reason a database error hands a request over: a pool that gave no
/// connection in time is `Pool`, anything else `otherwise`.
pub fn of(error: &sqlx::Error, otherwise: Reason) -> Reason {
    match error {
        sqlx::Error::PoolTimedOut => Reason::Pool,
        _ => otherwise,
    }
}
