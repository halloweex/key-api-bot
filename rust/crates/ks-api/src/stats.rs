//! What the pilot answered, for its watch (`core/rust_api_watch.py`, a line
//! in the 09:00 digest). Every request to the app's table is counted once, and
//! each 501 by the branch its body names: a hand-over by its reason
//! (`crate::handover`), a route or a filter the pilot leaves to Python by its
//! name. `GET /stats` publishes the last `WINDOW_MINUTES` minutes and the
//! totals since the process started. It is the pilot's own path, outside the
//! app's table as `/healthz` is, and nginx routes no client to it.
//!
//! The window is the watch's, so that it can judge a share without keeping a
//! sample of its own between runs: the integrity job asks four times a day,
//! and the morning's run, which the digest reports, must see the night before
//! it. A restart costs the hours this process has not lived yet, which
//! `window_s` says.

use std::collections::{BTreeMap, VecDeque};
use std::sync::Mutex;
use std::time::Instant;

use serde::Serialize;

/// How far back `window` reaches: a day, in minutes.
pub const WINDOW_MINUTES: u64 = 24 * 60;

/// What one answer was, as the counters see it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Outcome {
    /// Any answer but a 501.
    Answered,
    /// A 501, which nginx turns into Python's answer, by the branch it names.
    HandedOver(&'static str),
}

/// A 501's branch, put on the response by whoever built it.
#[derive(Clone, Copy, Debug)]
pub struct Branch(pub &'static str);

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize)]
pub struct Counts {
    pub requests: u64,
    /// 501s by branch; a branch that never occurred is absent.
    pub handed_over: BTreeMap<&'static str, u64>,
}

impl Counts {
    fn add(&mut self, outcome: Outcome) {
        self.requests += 1;
        if let Outcome::HandedOver(branch) = outcome {
            *self.handed_over.entry(branch).or_default() += 1;
        }
    }

    fn merge(&mut self, other: &Counts) {
        self.requests += other.requests;
        for (branch, n) in &other.handed_over {
            *self.handed_over.entry(branch).or_default() += n;
        }
    }
}

#[derive(Debug, PartialEq, Eq, Serialize)]
pub struct Snapshot {
    pub uptime_s: u64,
    /// The seconds `window` covers: `WINDOW_MINUTES` once the process has
    /// lived that long, its age until then.
    pub window_s: u64,
    pub window: Counts,
    pub since_start: Counts,
}

#[derive(Default)]
struct Inner {
    total: Counts,
    /// (minute since start, that minute's counts), oldest first.
    minutes: VecDeque<(u64, Counts)>,
}

pub struct Stats {
    started: Instant,
    inner: Mutex<Inner>,
}

impl Default for Stats {
    fn default() -> Self {
        Self {
            started: Instant::now(),
            inner: Mutex::default(),
        }
    }
}

impl Stats {
    pub fn record(&self, outcome: Outcome) {
        self.record_at(self.started.elapsed().as_secs(), outcome);
    }

    pub fn snapshot(&self) -> Snapshot {
        self.snapshot_at(self.started.elapsed().as_secs())
    }

    fn record_at(&self, elapsed_s: u64, outcome: Outcome) {
        let minute = elapsed_s / 60;
        let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        inner.total.add(outcome);
        if inner.minutes.back().is_none_or(|(m, _)| *m != minute) {
            inner.minutes.push_back((minute, Counts::default()));
        }
        if let Some((_, counts)) = inner.minutes.back_mut() {
            counts.add(outcome);
        }
        while inner
            .minutes
            .front()
            .is_some_and(|(m, _)| m + WINDOW_MINUTES <= minute)
        {
            inner.minutes.pop_front();
        }
    }

    fn snapshot_at(&self, elapsed_s: u64) -> Snapshot {
        let minute = elapsed_s / 60;
        let inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        let mut window = Counts::default();
        for (m, counts) in &inner.minutes {
            if m + WINDOW_MINUTES > minute {
                window.merge(counts);
            }
        }
        Snapshot {
            uptime_s: elapsed_s,
            window_s: elapsed_s.min(WINDOW_MINUTES * 60),
            window,
            since_start: inner.total.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{Outcome, Stats, WINDOW_MINUTES};

    #[test]
    fn a_501_is_counted_by_its_branch_and_every_answer_as_a_request() {
        let stats = Stats::default();
        stats.record_at(0, Outcome::Answered);
        stats.record_at(1, Outcome::HandedOver("pool"));
        stats.record_at(2, Outcome::HandedOver("pool"));
        stats.record_at(3, Outcome::HandedOver("line_level_filter"));
        let snap = stats.snapshot_at(10);
        assert_eq!(snap.window.requests, 4);
        assert_eq!(snap.window.handed_over["pool"], 2);
        assert_eq!(snap.window.handed_over["line_level_filter"], 1);
        assert!(!snap.window.handed_over.contains_key("revision"));
        assert_eq!(snap.window, snap.since_start);
        assert_eq!(snap.window_s, 10);
    }

    #[test]
    fn the_window_forgets_what_is_older_than_its_minutes() {
        let stats = Stats::default();
        stats.record_at(0, Outcome::HandedOver("revision"));
        stats.record_at(60 * (WINDOW_MINUTES - 1), Outcome::Answered);
        // Minute 14: both minutes still inside.
        assert_eq!(
            stats.snapshot_at(60 * (WINDOW_MINUTES - 1)).window.requests,
            2
        );
        // Minute 15: minute 0 has left the window, the totals keep it.
        let later = stats.snapshot_at(60 * WINDOW_MINUTES);
        assert_eq!(later.window.requests, 1);
        assert!(later.window.handed_over.is_empty());
        assert_eq!(later.since_start.requests, 2);
        assert_eq!(later.since_start.handed_over["revision"], 1);
        assert_eq!(later.window_s, 60 * WINDOW_MINUTES);
        // A quiet window later the window holds only a new answer, which
        // prunes what is older.
        let quiet_after = 60 * (2 * WINDOW_MINUTES + 60);
        stats.record_at(quiet_after, Outcome::Answered);
        let quiet = stats.snapshot_at(quiet_after);
        assert_eq!(quiet.window.requests, 1);
        assert_eq!(quiet.since_start.requests, 3);
    }
}
