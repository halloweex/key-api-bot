//! `_resolve_session`: who a cookie says is asking, read again every request.
//!
//! The cookie is checked as itsdangerous checks it, and a refusal is no
//! session. The payload must be a dict — `.get` on anything else raises — and
//! its `user_id` is Python's value with Python's rules: falsy is no session;
//! `user_id in ADMIN_USER_IDS` is a hardcoded admin, who needs no row and
//! whose cookie role does not matter (`5.0` is the int `5` there, a list
//! raises); anything else goes to Postgres the way asyncpg would send it as a
//! BIGINT — `int()` of a float truncates, `True` is `1`, a string is refused,
//! and a refused parameter is an error like any other read error: no session.
//!
//! A row that is not `approved` is no session; one that is carries its own
//! role and tab set, never the cookie's. No row falls back to the bot's list,
//! as a `viewer` with no tab set. A read of either list that fails hands the
//! request to Python (`crate::handover`), which answers as it would — its 401
//! for the user list, its 500 for the bot's — or reads what this could not.

use std::collections::BTreeSet;

use super::contract::Contract;
use super::permissions::parse_features;
use super::pyjson::PyValue;
use super::signer::{self, LoadError, SessionKey};
use super::store::AccessStore;
use crate::handover::Reason;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Caller {
    Anonymous,
    /// `is_hardcoded_admin(user_id)`: every gate passes.
    HardcodedAdmin,
    User {
        role: String,
        /// `None` is "as the level"; an empty list is "no tabs".
        tabs: Option<Vec<String>>,
    },
}

/// What `_resolve_session` raises instead of answering: the app's 500.
#[derive(Debug, PartialEq, Eq)]
pub struct Raised;

/// What the session check ends in, other than a caller.
#[derive(Debug, PartialEq, Eq)]
pub enum Unresolved {
    /// Python raises: the app's 500.
    Raised,
    /// A read it needs failed here: the request goes to Python.
    HandOver(Reason),
}

impl From<Raised> for Unresolved {
    fn from(_: Raised) -> Self {
        Unresolved::Raised
    }
}

/// `user_id in ADMIN_USER_IDS`.
fn hardcoded(user_id: &PyValue, admins: &BTreeSet<i64>) -> Result<bool, Raised> {
    Ok(match user_id {
        PyValue::Int(digits) => digits.parse().is_ok_and(|id: i64| admins.contains(&id)),
        PyValue::Bool(b) => admins.contains(&i64::from(*b)),
        PyValue::Float(f) => to_i64(*f).is_some_and(|id| id as f64 == *f && admins.contains(&id)),
        PyValue::Str(_) | PyValue::None => false,
        // `[5] in {…}`: TypeError, unhashable.
        PyValue::List(_) | PyValue::Dict(_) => return Err(Raised),
    })
}

/// `int(f)` within BIGINT, or `None` where `int()` or the range refuses.
fn to_i64(f: f64) -> Option<i64> {
    let t = f.trunc();
    (t.is_finite() && (-9_223_372_036_854_775_808.0..9_223_372_036_854_775_808.0).contains(&t))
        .then_some(t as i64)
}

/// The BIGINT asyncpg would send for `user_id`, or `None` where it refuses.
fn bigint(user_id: &PyValue) -> Option<i64> {
    match user_id {
        PyValue::Int(digits) => digits.parse().ok(),
        PyValue::Bool(b) => Some(i64::from(*b)),
        PyValue::Float(f) => to_i64(*f),
        _ => None,
    }
}

pub struct Resolver<'a, S> {
    pub contract: &'a Contract,
    pub key: &'a SessionKey,
    pub admins: &'a BTreeSet<i64>,
    pub store: &'a S,
}

impl<S: AccessStore> Resolver<'_, S> {
    /// `_resolve_session(cookie)` at the instant `now`, in Unix seconds.
    pub async fn resolve(&self, cookie: Option<&str>, now: i64) -> Result<Caller, Unresolved> {
        let Some(cookie) = cookie.filter(|c| !c.is_empty()) else {
            return Ok(Caller::Anonymous);
        };
        let max_age = self.contract.session.max_age_s;
        let data = match signer::loads(cookie, self.key, now, max_age) {
            Ok(data) => data,
            Err(LoadError::Refused) => return Ok(Caller::Anonymous),
            Err(LoadError::Payload | LoadError::Raised) => return Err(Unresolved::Raised),
        };
        if !matches!(data, PyValue::Dict(_)) {
            return Err(Unresolved::Raised);
        }
        let missing = PyValue::None;
        let user_id = data.get("user_id").unwrap_or(&missing);
        if !user_id.truthy() {
            return Ok(Caller::Anonymous);
        }
        if hardcoded(user_id, self.admins)? {
            return Ok(Caller::HardcodedAdmin);
        }
        let Some(id) = bigint(user_id) else {
            return Ok(Caller::Anonymous);
        };
        match self.store.dashboard_user(id).await {
            Err(error) => {
                tracing::warn!(%id, reason = error.reason.as_str(), error = %error.message,
                    "the user list did not answer; handing the request to Python");
                Err(Unresolved::HandOver(error.reason))
            }
            Ok(Some(row)) if row.status == "approved" => Ok(Caller::User {
                role: row.role,
                tabs: parse_features(self.contract, row.allowed_features.as_deref()),
            }),
            Ok(Some(_)) => Ok(Caller::Anonymous),
            Ok(None) => match self.store.bot_status(id).await {
                Err(error) => {
                    tracing::warn!(%id, reason = error.reason.as_str(), error = %error.message,
                        "the bot's list did not answer; handing the request to Python");
                    Err(Unresolved::HandOver(error.reason))
                }
                Ok(Some(Some(status))) if status == "approved" => Ok(Caller::User {
                    role: "viewer".into(),
                    tabs: None,
                }),
                Ok(_) => Ok(Caller::Anonymous),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{PyValue, Raised, bigint, hardcoded};
    use std::collections::BTreeSet;

    #[test]
    fn a_user_id_is_read_as_python_and_asyncpg_read_it() {
        let admins: BTreeSet<i64> = [7].into();
        assert_eq!(hardcoded(&PyValue::Float(7.0), &admins), Ok(true));
        assert_eq!(hardcoded(&PyValue::Float(7.5), &admins), Ok(false));
        assert_eq!(hardcoded(&PyValue::Str(vec![55]), &admins), Ok(false));
        assert_eq!(
            hardcoded(&PyValue::List(vec![PyValue::None]), &admins),
            Err(Raised)
        );
        assert_eq!(bigint(&PyValue::Float(5.7)), Some(5));
        assert_eq!(bigint(&PyValue::Float(f64::NAN)), None);
        assert_eq!(bigint(&PyValue::Bool(true)), Some(1));
        assert_eq!(bigint(&PyValue::Int("9223372036854775808".into())), None);
    }
}
