//! slowapi's per-route limits, with the `limits` library's in-memory fixed
//! window behind them (`web/ratelimit.py`).
//!
//! A window opens at its first hit and lasts the limit's period; the hit
//! that passes the amount is refused and still counted. A window is per
//! limit, per client key, per endpoint. The key is `X-Real-IP` — the first
//! such header, `str.strip()`ped — or the peer's address; a header that
//! strips to nothing makes slowapi skip the limit altogether ("empty value
//! found in parameters"), which is how it behaves, not how it should.
//!
//! The check runs inside the endpoint, so after the gates and after FastAPI
//! has parsed the query: a refused or malformed request costs nothing.

use std::collections::HashMap;
use std::sync::Mutex;

use super::contract::RateLimit;
use crate::pyrt::py_strip;

struct Window {
    count: u64,
    expires_at: f64,
}

/// Windows kept before the expired ones are swept, at most once a second:
/// the bound on memory is a period's worth of clients, as in `limits`.
const SWEEP_ABOVE: usize = 10_000;

/// Endpoint → limit → client: the contract's handful of limits each hold
/// their clients, so a window costs its client's key and nothing else.
type Clients = HashMap<String, Window>;

#[derive(Default)]
struct Windows {
    by_scope: HashMap<String, HashMap<String, Clients>>,
    swept_at: f64,
}

#[derive(Default)]
pub struct Limiter {
    windows: Mutex<Windows>,
}

/// `map[key]`, inserting the default first — allocating only on a miss.
fn slot<'m, V: Default>(map: &'m mut HashMap<String, V>, key: &str) -> &'m mut V {
    if !map.contains_key(key) {
        map.insert(key.to_owned(), V::default());
    }
    map.get_mut(key).expect("present: inserted just above")
}

impl Default for Window {
    fn default() -> Self {
        Window {
            count: 0,
            expires_at: 0.0,
        }
    }
}

impl Limiter {
    /// One hit at `now` (Unix seconds); whether it is within the limit.
    pub fn hit(&self, limit: &RateLimit, key: &str, now: f64) -> bool {
        let mut windows = self.windows.lock().unwrap_or_else(|e| e.into_inner());
        let open: usize = windows
            .by_scope
            .values()
            .flat_map(HashMap::values)
            .map(HashMap::len)
            .sum();
        if open > SWEEP_ABOVE && now - windows.swept_at >= 1.0 {
            for clients in windows.by_scope.values_mut().flat_map(HashMap::values_mut) {
                clients.retain(|_, w| w.expires_at > now);
            }
            windows.swept_at = now;
        }
        let limits = slot(&mut windows.by_scope, &limit.scope);
        let window = slot(slot(limits, &limit.limit), key);
        if window.expires_at <= now {
            window.count = 0;
        }
        window.count += 1;
        if window.count == 1 {
            window.expires_at = now + limit.expiry_s as f64;
        }
        window.count <= limit.amount
    }
}

/// `client_key(request)`, or `None` where slowapi skips the limit.
pub fn client_key(real_ip: Option<&[u8]>, peer: &str) -> Option<String> {
    match real_ip.filter(|v| !v.is_empty()) {
        Some(raw) => {
            let text: String = raw.iter().map(|&b| char::from(b)).collect();
            let key = py_strip(&text);
            (!key.is_empty()).then(|| key.to_owned())
        }
        None => Some(peer.to_owned()),
    }
}

#[cfg(test)]
mod tests {
    use super::{Limiter, client_key};
    use crate::access::contract::RateLimit;

    #[test]
    fn the_thirty_first_hit_is_refused_until_the_window_ends() {
        let limit = RateLimit {
            limit: "30 per 1 minute".into(),
            amount: 30,
            expiry_s: 60,
            scope: "m.f".into(),
        };
        let limiter = Limiter::default();
        assert!((0..30).all(|i| limiter.hit(&limit, "k", 100.0 + f64::from(i))));
        assert!(!limiter.hit(&limit, "k", 130.0));
        assert!(limiter.hit(&limit, "other", 130.0));
        assert!(limiter.hit(&limit, "k", 160.0));
    }

    #[test]
    fn the_key_is_the_header_stripped_or_the_peer() {
        assert_eq!(
            client_key(Some(b" 1.2.3.4 "), "9.9.9.9").as_deref(),
            Some("1.2.3.4")
        );
        assert_eq!(client_key(Some(b""), "9.9.9.9").as_deref(), Some("9.9.9.9"));
        assert_eq!(client_key(None, "9.9.9.9").as_deref(), Some("9.9.9.9"));
        assert_eq!(client_key(Some(b"  \x1f"), "9.9.9.9"), None);
    }
}
