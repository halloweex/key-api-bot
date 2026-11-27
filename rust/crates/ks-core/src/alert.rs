//! Where a job's finding goes.
//!
//! An interface, and only that in К1: the production path — the alert
//! journal (`app.alert_series`, `app.alert_events`) behind the Gate that
//! decides what reaches Telegram (`core/alerting.py`) — is Python's, and
//! nothing here writes to it or to Telegram. A sink that logs is what the
//! worker runs with; a sink that keeps what it was given is what a harness
//! reads.
//!
//! The one finding `ch_sync` makes is a heal: the archive's ids-diff
//! re-shipping versions below the copy's own high-water mark. Python keeps
//! that in process memory (`core.ch_history.last_heal`) for the daily check
//! to file as `ch_history_selfhealed`; a Rust process's memory is not the
//! daily check's, so the finding leaves through here.

use std::sync::{Arc, Mutex, PoisonError};

/// How loud a finding is: `core.data_quality.Severity`'s three.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Severity {
    Info,
    Warn,
    Critical,
}

/// One finding: the condition's name (`core/alerting.py`'s vocabulary), how
/// loud, and what a reader needs.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Finding {
    pub key: String,
    pub severity: Severity,
    pub count: u64,
    pub message: String,
}

/// Something that takes findings.
pub trait AlertSink: Send + Sync {
    fn raise(&self, finding: &Finding);
}

/// The worker's sink in К1: the log, at the finding's level.
#[derive(Clone, Copy, Debug, Default)]
pub struct LogSink;

impl AlertSink for LogSink {
    fn raise(&self, finding: &Finding) {
        let Finding {
            key,
            count,
            message,
            ..
        } = finding;
        match finding.severity {
            Severity::Info => tracing::info!(%key, count, "{message}"),
            Severity::Warn => tracing::warn!(%key, count, "{message}"),
            Severity::Critical => tracing::error!(%key, count, "{message}"),
        }
    }
}

/// A sink that logs and keeps what it was given.
#[derive(Clone, Debug, Default)]
pub struct MemorySink(Arc<Mutex<Vec<Finding>>>);

impl MemorySink {
    /// Everything raised so far.
    pub fn findings(&self) -> Vec<Finding> {
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }
}

impl AlertSink for MemorySink {
    fn raise(&self, finding: &Finding) {
        LogSink.raise(finding);
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(finding.clone());
    }
}
