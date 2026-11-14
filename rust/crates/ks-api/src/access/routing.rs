//! Which route a request reaches, as Starlette decides it.
//!
//! The routes are tried in the app's order. The first whose pattern matches
//! the decoded path and whose methods hold the request's is the one; failing
//! that, the first whose pattern matched is a 405 — before any gate runs, so
//! an unauthenticated `POST` to a `GET` route is told the method, not to sign
//! in. A Mount matches by path alone, a WebSocket route never matches HTTP.
//! With nothing matching, Starlette tries the path with its trailing slash
//! toggled and redirects if that would match, and only then answers 404.
//!
//! A pattern is Python's: `$` matches at the end or before a final newline,
//! so `/api/health%0A` is `/api/health`.

use std::collections::BTreeMap;

use regex::Regex;

use super::contract::{Contract, RouteSpec};

/// A route's path parameters, by name.
pub type Captures = BTreeMap<String, String>;

pub enum Resolution<'a> {
    /// Routed, with its path parameters.
    Full(&'a RouteSpec, Captures),
    /// The path is a route's; the method is not.
    MethodNotAllowed(&'a RouteSpec),
    /// `redirect_slashes`: the same request at this path would route.
    Redirect(String),
    NotFound,
}

/// `re.match(pattern, s)`.
pub fn py_match(regex: &Regex, s: &str) -> Option<Captures> {
    let caps = regex
        .captures(s)
        .or_else(|| s.strip_suffix('\n').and_then(|t| regex.captures(t)))?;
    Some(
        regex
            .capture_names()
            .flatten()
            .filter_map(|name| {
                caps.name(name)
                    .map(|m| (name.to_owned(), m.as_str().to_owned()))
            })
            .collect(),
    )
}

pub struct Router {
    regexes: Vec<Option<Regex>>,
}

impl Router {
    pub fn new(contract: &Contract) -> Result<Self, String> {
        Ok(Self {
            regexes: contract.compile()?,
        })
    }

    pub fn resolve<'a>(&self, contract: &'a Contract, method: &str, path: &str) -> Resolution<'a> {
        let mut partial = None;
        for (route, regex) in contract.routes.iter().zip(&self.regexes) {
            let Some(regex) = regex else { continue };
            let Some(captures) = py_match(regex, path) else {
                continue;
            };
            if matches!(route, RouteSpec::Mount(_)) || route.methods().iter().any(|m| m == method) {
                return Resolution::Full(route, captures);
            }
            partial.get_or_insert(route);
        }
        if let Some(route) = partial {
            return Resolution::MethodNotAllowed(route);
        }
        if contract.redirect_slashes && path != "/" {
            let toggled = match path.strip_suffix('/') {
                Some(_) => path.trim_end_matches('/').to_owned(),
                None => format!("{path}/"),
            };
            if self
                .regexes
                .iter()
                .flatten()
                .any(|rx| py_match(rx, &toggled).is_some())
            {
                return Resolution::Redirect(toggled);
            }
        }
        Resolution::NotFound
    }
}

#[cfg(test)]
mod tests {
    use super::py_match;
    use regex::Regex;

    #[test]
    fn a_dollar_matches_before_a_final_newline() {
        let rx = Regex::new(r"^/api/health$").unwrap();
        assert!(py_match(&rx, "/api/health\n").is_some());
        assert!(py_match(&rx, "/api/health\n\n").is_none());
        let rx = Regex::new(r"^/(?P<path>.*)$").unwrap();
        assert_eq!(py_match(&rx, "/a\n").unwrap()["path"], "a");
    }
}
