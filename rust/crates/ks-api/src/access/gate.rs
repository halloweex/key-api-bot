//! The gates a route runs, in its order, and the words each refuses in.
//!
//! `api_gate` lets `PUBLIC_API_PATHS` through without asking who is there —
//! judged on `request.url.path`, not the routed path — then needs a session
//! (401) and keeps `sales_type=internal` for admins (403). `require_user`
//! needs a session; `require_admin` an admin role or a hardcoded admin;
//! `require_permission` and `require_any_permission` the action on the
//! feature after the tab override, a hardcoded admin passing all of them.
//! The session is resolved once, by the first gate that asks, as
//! `get_current_user` caches it on the request; and the first dependency
//! that is not a gate ends the gates — what follows is the endpoint's.

use std::collections::BTreeSet;

use super::contract::{ApiRoute, Contract, Dependency, Gate};
use super::paths::url_path;
use super::permissions::{self, Matrix};
use super::session::{Caller, Raised, Resolver};
use super::signer::SessionKey;
use super::store::AccessStore;
use crate::pyrt::py_lower_strip;

#[derive(Debug, PartialEq, Eq)]
pub enum Refusal {
    /// 401 `{"detail":"Authentication required"}`.
    Unauthenticated,
    /// 403 with Python's detail.
    Forbidden(String),
    /// The app's 500.
    Raised,
}

#[derive(Debug, PartialEq, Eq)]
pub enum Verdict {
    /// Every gate passed: the request reaches the endpoint's own work.
    Reached,
    Refused(Refusal),
}

/// What the gates read of a request.
pub struct Ask<'a> {
    /// The path as uvicorn decoded it.
    pub path: &'a str,
    /// The raw query string.
    pub query: &'a str,
    /// The `dashboard_session` cookie, as Starlette parsed it.
    pub cookie: Option<&'a str>,
    /// Unix seconds, from the real clock.
    pub now: i64,
}

pub struct Access<S> {
    pub contract: Contract,
    pub key: SessionKey,
    pub admins: BTreeSet<i64>,
    pub store: S,
}

/// `request.query_params.get(name)`: the last value given.
fn query_param(query: &str, name: &str) -> Option<String> {
    form_urlencoded::parse(query.as_bytes())
        .filter(|(k, _)| k == name)
        .last()
        .map(|(_, v)| v.into_owned())
}

fn is_admin(caller: &Caller) -> bool {
    match caller {
        Caller::HardcodedAdmin => true,
        Caller::User { role, .. } => role == "admin",
        Caller::Anonymous => false,
    }
}

impl<S: AccessStore> Access<S> {
    fn resolver(&self) -> Resolver<'_, S> {
        Resolver {
            contract: &self.contract,
            key: &self.key,
            admins: &self.admins,
            store: &self.store,
        }
    }

    /// `effective_permissions(user)`.
    async fn matrix(&self, role: &str, tabs: Option<&[String]>) -> Matrix {
        let base = match self.store.role_permissions(role).await {
            Ok(rows) => permissions::stored(&self.contract, role, &rows),
            Err(error) => {
                tracing::warn!(role, error = %error.0, "permissions unreadable; the code's matrix stands");
                permissions::coded(&self.contract, role)
            }
        };
        permissions::apply_feature_override(&self.contract, base, tabs, role)
    }

    /// Run `route`'s gates for `ask`.
    pub async fn gates(&self, route: &ApiRoute, ask: &Ask<'_>) -> Verdict {
        let mut caller: Option<Caller> = None;
        let mut matrix: Option<Matrix> = None;
        for dependency in &route.dependencies {
            let gate = match dependency {
                Dependency::Gate(gate) => gate,
                Dependency::Opaque { .. } => return Verdict::Reached,
            };
            if matches!(gate, Gate::ApiGate)
                && self.contract.public_api_paths.contains(&url_path(ask.path))
            {
                continue;
            }
            if caller.is_none() {
                match self.resolver().resolve(ask.cookie, ask.now).await {
                    Ok(resolved) => caller = Some(resolved),
                    Err(Raised) => return Verdict::Refused(Refusal::Raised),
                }
            }
            let Some(who) = caller.as_ref() else {
                unreachable!("resolved just above");
            };
            if *who == Caller::Anonymous {
                return Verdict::Refused(Refusal::Unauthenticated);
            }
            let refused = |detail: String| Verdict::Refused(Refusal::Forbidden(detail));
            match gate {
                Gate::ApiGate => {
                    let internal = query_param(ask.query, "sales_type")
                        .is_some_and(|v| py_lower_strip(&v) == "internal");
                    if internal && !is_admin(who) {
                        return refused("sales_type=internal is admin-only".into());
                    }
                }
                Gate::RequireUser => {}
                Gate::RequireAdmin => {
                    if !is_admin(who) {
                        return refused("Admin access required".into());
                    }
                }
                Gate::RequirePermission { feature, action } => {
                    let Caller::User { role, tabs } = who else {
                        continue;
                    };
                    if matrix.is_none() {
                        matrix = Some(self.matrix(role, tabs.as_deref()).await);
                    }
                    let allowed = matrix
                        .as_ref()
                        .and_then(|m| m.get(feature))
                        .is_some_and(|a| a.allows(action));
                    if !allowed {
                        return refused(format!("No {action} access to {feature}"));
                    }
                }
                Gate::RequireAnyPermission { features, action } => {
                    let Caller::User { role, tabs } = who else {
                        continue;
                    };
                    if matrix.is_none() {
                        matrix = Some(self.matrix(role, tabs.as_deref()).await);
                    }
                    let any = features.iter().any(|f| {
                        matrix
                            .as_ref()
                            .and_then(|m| m.get(f))
                            .is_some_and(|a| a.allows(action))
                    });
                    if !any {
                        return refused(format!("No {action} access to {}", features.join(" or ")));
                    }
                }
            }
        }
        Verdict::Reached
    }
}

/// The contract's own test: what it promises the pilot can keep.
#[cfg(test)]
mod tests {
    use super::super::contract::{Contract, RouteSpec};

    #[test]
    fn the_embedded_contract_loads_and_every_pattern_compiles() {
        let contract = Contract::load().expect("the embedded contract");
        contract.compile().expect("every pattern");
        let trend = contract.routes.iter().find_map(|r| match r {
            RouteSpec::Api(a) if a.path == "/api/revenue/trend" => Some(a),
            _ => None,
        });
        assert!(trend.is_some_and(|t| !t.rate_limits.is_empty()));
    }
}
