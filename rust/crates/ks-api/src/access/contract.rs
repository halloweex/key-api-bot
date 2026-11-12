//! `rust/contract/access.json`, embedded when the pilot is built.
//!
//! `tests/rust_access_contract.py` writes it from the running app and a test
//! fails when the app and the file disagree, so nothing here is a second
//! opinion about a route: the order, the patterns, the methods, the gates and
//! the limits are the app's. What the pilot adds is refusing to start on a
//! contract it cannot honour — a session shape it does not verify, a pattern
//! the regex engine will not compile, a dependency it does not know.

use std::collections::BTreeMap;

use regex::Regex;
use serde::Deserialize;

pub const CONTRACT_JSON: &str = include_str!("../../../../contract/access.json");

#[derive(Debug, Deserialize)]
pub struct Contract {
    pub session: SessionSpec,
    pub required_revision: String,
    pub public_api_paths: Vec<String>,
    pub redirect_slashes: bool,
    /// `Feature`, in enum order.
    pub features: Vec<String>,
    /// `TAB_FEATURE_KEYS`: the features a person's tab set narrows.
    pub tab_features: Vec<String>,
    /// `ROLE_PERMISSIONS`: role → feature → the actions it grants.
    pub role_permissions: BTreeMap<String, BTreeMap<String, Vec<String>>>,
    /// `DEFAULT_TABS`: what "no tab set" means per level; `None`, not narrowed.
    pub default_tabs: BTreeMap<String, Option<Vec<String>>>,
    pub hardcoded_admins_sha256: String,
    pub routes: Vec<RouteSpec>,
}

#[derive(Debug, Deserialize)]
pub struct SessionSpec {
    pub cookie: String,
    pub max_age_s: i64,
    pub salt: String,
    pub separator: String,
    pub key_derivation: String,
    pub key_digest: String,
    pub mac_digest: String,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "kind", rename_all = "lowercase")]
pub enum RouteSpec {
    Api(ApiRoute),
    Route(PlainRoute),
    Mount(MountRoute),
    Websocket(WebSocketRoute),
}

#[derive(Debug, Deserialize)]
pub struct ApiRoute {
    pub path: String,
    pub pattern: String,
    pub methods: Vec<String>,
    pub endpoint: String,
    /// FastAPI reads the body before any dependency runs.
    pub body: bool,
    pub dependencies: Vec<Dependency>,
    pub rate_limits: Vec<RateLimit>,
}

#[derive(Debug, Deserialize)]
pub struct PlainRoute {
    pub path: String,
    pub pattern: String,
    pub methods: Vec<String>,
}

#[derive(Debug, Deserialize)]
pub struct MountRoute {
    pub path: String,
    pub pattern: String,
}

#[derive(Debug, Deserialize)]
pub struct WebSocketRoute {
    pub path: String,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
pub enum Dependency {
    Gate(Gate),
    /// Not a gate: the endpoint's own work, where comparing stops.
    Opaque {
        opaque: String,
    },
}

#[derive(Debug, Deserialize)]
#[serde(tag = "gate", rename_all = "snake_case")]
pub enum Gate {
    ApiGate,
    RequireUser,
    RequireAdmin,
    RequirePermission {
        feature: String,
        action: String,
    },
    RequireAnyPermission {
        features: Vec<String>,
        action: String,
    },
}

#[derive(Debug, Deserialize)]
pub struct RateLimit {
    /// `str(limit)`, which the 429 says back: "30 per 1 minute".
    pub limit: String,
    pub amount: u64,
    pub expiry_s: u64,
    /// slowapi scopes a limit by endpoint, `module.function`.
    pub scope: String,
}

impl RouteSpec {
    pub fn pattern(&self) -> Option<&str> {
        match self {
            RouteSpec::Api(r) => Some(&r.pattern),
            RouteSpec::Route(r) => Some(&r.pattern),
            RouteSpec::Mount(r) => Some(&r.pattern),
            RouteSpec::Websocket(_) => None,
        }
    }

    pub fn methods(&self) -> &[String] {
        match self {
            RouteSpec::Api(r) => &r.methods,
            RouteSpec::Route(r) => &r.methods,
            RouteSpec::Mount(_) | RouteSpec::Websocket(_) => &[],
        }
    }
}

impl Contract {
    /// The embedded contract, refused if the pilot cannot honour it.
    pub fn load() -> Result<Self, String> {
        let contract: Contract = serde_json::from_str(CONTRACT_JSON)
            .map_err(|e| format!("rust/contract/access.json does not parse: {e}"))?;
        let s = &contract.session;
        let shape = (
            s.salt.as_str(),
            s.separator.as_str(),
            s.key_derivation.as_str(),
            s.key_digest.as_str(),
            s.mac_digest.as_str(),
        );
        if shape != ("itsdangerous", ".", "django-concat", "sha1", "sha1") {
            return Err(format!(
                "the session is signed in a shape the pilot does not check: {shape:?}"
            ));
        }
        Ok(contract)
    }

    /// Every route's pattern, compiled; a WebSocket route has none.
    pub fn compile(&self) -> Result<Vec<Option<Regex>>, String> {
        self.routes
            .iter()
            .map(|route| {
                route
                    .pattern()
                    .map(|p| Regex::new(p).map_err(|e| format!("{p}: {e}")))
                    .transpose()
            })
            .collect()
    }
}
