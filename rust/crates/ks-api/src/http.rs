//! The HTTP shell: every request routed as the Python app routes it, its
//! gates run, and only then an answer — the bodies the app answers with, and
//! the bookkeeping `web/middleware.py` does for every request: a correlation
//! id, a start and an end log line, a 30-second timeout.
//!
//! Routing and the gates cover every route the app has
//! (`rust/contract/access.json`). Past the gates, the pilot answers
//! `GET /api/revenue/trend` and wave 1's tabs — /margin, /products' intel,
//! /reports and /marketing, the expenses reads — each with its rate limit,
//! and the SPA
//! catch-all's JSON 404 for an unmatched `/api` path. Every other route that
//! a request is let through to is a 501 naming it — the Python app serves it,
//! and nginx hands it there (`nginx/rust/proxy.conf`). Two paths of its own,
//! outside the app's table: `/healthz` (alive) and `/readyz` (able to answer).

use std::any::Any;
use std::collections::hash_map::RandomState;
use std::hash::BuildHasher;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use axum::extract::{ConnectInfo, Request, State};
use axum::http::{HeaderMap, HeaderValue, Method, StatusCode, Uri, header};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use chrono::{DateTime, Utc};
use regex::Regex;
use serde::Serialize;
use tower_http::catch_panic::CatchPanicLayer;

use crate::access::contract::{ApiRoute, RouteSpec};
use crate::access::cookie;
use crate::access::gate::{Access, Ask, Refusal, Verdict};
use crate::access::paths::uvicorn_path;
use crate::access::ratelimit::{Limiter, client_key};
use crate::access::routing::{Captures, Resolution, Router};
use crate::access::store::PgAccess;
use crate::expenses;
use crate::goals;
use crate::gold::PgGold;
use crate::inventory;
use crate::margin::{self, PgMargin};
use crate::pgread::PgReads;
use crate::products_intel;
use crate::reports;
use crate::traffic;
use crate::trend::{self, Failure};

/// `DEFAULT_REQUEST_TIMEOUT` in `web/middleware.py`.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);
/// `SLOW_ENDPOINT_TIMEOUT`, for the paths in `SLOW_ENDPOINTS`.
const SLOW_REQUEST_TIMEOUT: Duration = Duration::from_secs(300);
/// `SLOW_ENDPOINTS`, matched against the decoded path as
/// `request.url.path` is.
const SLOW_ENDPOINTS: [&str; 8] = [
    "/api/duckdb/resync",
    "/api/duckdb/refresh-statuses",
    "/api/goals/recalculate",
    "/api/stocks/analysis",
    "/api/revenue/forecast/train",
    "/api/revenue/forecast/evaluate",
    "/api/revenue/forecast/tune",
    "/api/traffic/reclassify",
];
/// The endpoints the pilot answers past the gates, by the contract's name.
const TREND: &str = "web.routes.api.analytics.get_revenue_trend";
const SPA_CATCH_ALL: &str = "web.routes.pages.catch_all_spa";

pub struct AppState {
    gold: PgGold,
    margin: PgMargin,
    /// Wave 1's tabs past margin, each read as its Python reader reads.
    reads: PgReads,
    access: Access<PgAccess>,
    router: Router,
    limiter: Limiter,
    /// What `scope["server"]` would say, for a redirect without a usable Host.
    listen: SocketAddr,
    #[cfg(feature = "test-clock")]
    test_clock: bool,
}

impl AppState {
    pub fn new(
        gold: PgGold,
        margin: PgMargin,
        reads: PgReads,
        access: Access<PgAccess>,
        router: Router,
        listen: SocketAddr,
        #[cfg_attr(not(feature = "test-clock"), allow(unused_variables))] test_clock: bool,
    ) -> Self {
        Self {
            gold,
            margin,
            reads,
            access,
            router,
            limiter: Limiter::default(),
            listen,
            #[cfg(feature = "test-clock")]
            test_clock,
        }
    }

    /// The trend's "now": the real clock, or — in a harness build, switched
    /// on — the instant `X-Pilot-Now` names. Sessions never read this.
    fn trend_now(&self, headers: &HeaderMap) -> Option<DateTime<Utc>> {
        #[cfg(feature = "test-clock")]
        if self.test_clock
            && let Some(value) = headers.get("x-pilot-now")
        {
            let instant = DateTime::parse_from_rfc3339(value.to_str().ok()?).ok()?;
            return Some(instant.with_timezone(&Utc));
        }
        let _ = headers;
        Some(Utc::now())
    }
}

pub fn router(state: AppState) -> axum::Router {
    axum::Router::new()
        .route("/healthz", get(healthz))
        .route("/readyz", get(readyz))
        .fallback(dispatch)
        .layer(middleware::from_fn(reject_nul))
        .layer(middleware::from_fn(observe))
        .layer(CatchPanicLayer::custom(panicked))
        .with_state(Arc::new(state))
}

#[derive(Serialize)]
struct Detail<T: Serialize> {
    detail: T,
}

/// Field order is the Python dicts', so the bytes match too.
#[derive(Serialize)]
struct ErrorBody<'a> {
    error: &'a str,
    detail: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    surface: Option<&'a str>,
}

#[derive(Serialize)]
struct NotInPilot<'a> {
    error: &'a str,
    detail: String,
    branch: &'a str,
}

#[derive(Serialize)]
struct TooMany<'a> {
    error: &'a str,
    detail: &'a str,
    retry_after: &'a str,
}

#[derive(Serialize)]
struct Timeout<'a> {
    error: &'a str,
    detail: &'a str,
    path: &'a str,
    correlation_id: &'a str,
}

fn json<T: Serialize>(status: StatusCode, body: &T) -> Response {
    raw_json(
        status,
        serde_json::to_vec(body).expect("error bodies are plain data"),
    )
}

fn raw_json(status: StatusCode, body: Vec<u8>) -> Response {
    (status, [(header::CONTENT_TYPE, "application/json")], body).into_response()
}

fn detail(status: StatusCode, detail: &str) -> Response {
    json(status, &Detail { detail })
}

const INTERNAL: ErrorBody<'static> = ErrorBody {
    error: "Internal Server Error",
    detail: "An unexpected error occurred.",
    surface: None,
};

fn not_in_pilot(what: String, branch: &str) -> Response {
    json(
        StatusCode::NOT_IMPLEMENTED,
        &NotInPilot {
            error: "Not Implemented",
            detail: format!("{what} is not in the Rust pilot; the Python app serves it"),
            branch,
        },
    )
}

impl IntoResponse for Failure {
    fn into_response(self) -> Response {
        match self {
            Failure::Unprocessable(errors) => {
                json(StatusCode::UNPROCESSABLE_ENTITY, &Detail { detail: errors })
            }
            Failure::BadRequest(message) => detail(StatusCode::BAD_REQUEST, &message),
            Failure::Raised(why) => {
                tracing::error!(why, "the Python app raises here; answering its 500");
                json(StatusCode::INTERNAL_SERVER_ERROR, &INTERNAL)
            }
            Failure::Unavailable(surface, error) => {
                tracing::error!(surface, error = %error.0, "read failed");
                let detail = format!(
                    "The data behind '{surface}' cannot be read right now. Try again shortly."
                );
                json(
                    StatusCode::SERVICE_UNAVAILABLE,
                    &ErrorBody {
                        error: "Service Unavailable",
                        detail: &detail,
                        surface: Some(surface),
                    },
                )
            }
            Failure::NotInPilot(branch) => not_in_pilot(branch.to_owned(), branch),
            Failure::RateLimited(limit) => json(
                StatusCode::TOO_MANY_REQUESTS,
                &TooMany {
                    error: "Rate limit exceeded",
                    detail: "Too many requests. Please try again later.",
                    retry_after: &limit,
                },
            ),
        }
    }
}

fn refused(refusal: Refusal) -> Response {
    match refusal {
        Refusal::Unauthenticated => detail(StatusCode::UNAUTHORIZED, "Authentication required"),
        Refusal::Forbidden(why) => {
            tracing::info!(wanted = %why, "Authorization refused");
            detail(StatusCode::FORBIDDEN, &why)
        }
        Refusal::Raised => {
            tracing::error!("the session check raises here in Python; answering its 500");
            json(StatusCode::INTERNAL_SERVER_ERROR, &INTERNAL)
        }
    }
}

fn unix_now() -> Duration {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
}

/// `^([a-z0-9.-]+|\[[a-f0-9]*:[a-f0-9.:]+\])(?::[0-9]+)?$`: a Host Starlette
/// puts in a URL; any other and it uses the server's address instead.
fn usable_host(host: &str) -> bool {
    static HOST: OnceLock<Regex> = OnceLock::new();
    HOST.get_or_init(|| {
        Regex::new(r"^([a-z0-9.-]+|\[[a-f0-9]*:[a-f0-9.:]+\])(?::[0-9]+)?$").expect("a literal")
    })
    .is_match(host)
}

/// `quote(url, safe=":/%#?=@[]!$&'()*+,;")`, which `RedirectResponse` applies.
fn quote(url: &str) -> String {
    const SAFE: &[u8] = b":/%#?=@[]!$&'()*+,;_.-~";
    let mut out = String::with_capacity(url.len());
    for b in url.bytes() {
        if b.is_ascii_alphanumeric() || SAFE.contains(&b) {
            out.push(char::from(b));
        } else {
            out.push_str(&format!("%{b:02X}"));
        }
    }
    out
}

fn redirect(state: &AppState, headers: &HeaderMap, path: &str, query: &str) -> Response {
    let host = headers
        .get(header::HOST)
        .map(|v| {
            v.as_bytes()
                .iter()
                .map(|&b| char::from(b))
                .collect::<String>()
        })
        .filter(|h| usable_host(h));
    let netloc = host.unwrap_or_else(|| match state.listen.port() {
        80 => state.listen.ip().to_string(),
        port => format!("{}:{port}", state.listen.ip()),
    });
    let mut url = format!("http://{netloc}{path}");
    if !query.is_empty() {
        url.push('?');
        url.push_str(query);
    }
    let location = HeaderValue::from_str(&quote(&url)).unwrap_or(HeaderValue::from_static("/"));
    (
        StatusCode::TEMPORARY_REDIRECT,
        [(header::LOCATION, location)],
    )
        .into_response()
}

async fn dispatch(
    State(state): State<Arc<AppState>>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
) -> Response {
    let path = uvicorn_path(uri.path());
    let query = uri.query().unwrap_or("");
    let contract = &state.access.contract;
    match state.router.resolve(contract, method.as_str(), &path) {
        Resolution::NotFound => detail(StatusCode::NOT_FOUND, "Not Found"),
        Resolution::MethodNotAllowed(route) => {
            let allow = route.methods().join(", ");
            let mut response = detail(StatusCode::METHOD_NOT_ALLOWED, "Method Not Allowed");
            if let Ok(value) = HeaderValue::from_str(&allow) {
                response.headers_mut().insert(header::ALLOW, value);
            }
            response
        }
        Resolution::Redirect(target) => redirect(&state, &headers, &target, query),
        Resolution::Full(RouteSpec::Api(route), captures) => {
            let session = cookie::cookie(
                headers
                    .get_all(header::COOKIE)
                    .iter()
                    .map(HeaderValue::as_bytes),
                &contract.session.cookie,
            );
            let ask = Ask {
                path: &path,
                query,
                cookie: session.as_deref(),
                now: i64::try_from(unix_now().as_secs()).unwrap_or(i64::MAX),
            };
            match state.access.gates(route, &ask).await {
                Verdict::Refused(refusal) => refused(refusal),
                Verdict::Reached => endpoint(&state, route, &captures, query, &headers, peer).await,
            }
        }
        Resolution::Full(other, _) => {
            let path = match other {
                RouteSpec::Route(r) => &r.path,
                RouteSpec::Mount(r) => &r.path,
                RouteSpec::Api(r) => &r.path,
                RouteSpec::Websocket(r) => &r.path,
            };
            not_in_pilot(format!("{method} {path}"), "route")
        }
    }
}

/// Past the gates: an endpoint the pilot answers, or a 501.
async fn endpoint(
    state: &AppState,
    route: &ApiRoute,
    captures: &Captures,
    query: &str,
    headers: &HeaderMap,
    peer: SocketAddr,
) -> Response {
    match route.endpoint.as_str() {
        TREND => revenue_trend(state, route, query, headers, peer).await,
        name if margin::Route::from_endpoint(name).is_some() => {
            let which = margin::Route::from_endpoint(name).expect("matched above");
            margin_route(state, which, route, query, headers, peer).await
        }
        name if products_intel::Route::from_endpoint(name).is_some() => {
            let which = products_intel::Route::from_endpoint(name).expect("matched above");
            products_route(state, which, route, query, headers, peer).await
        }
        name if reports::Route::from_endpoint(name).is_some() => {
            let which = reports::Route::from_endpoint(name).expect("matched above");
            reports_route(state, which, route, query, headers, peer).await
        }
        name if expenses::Route::from_endpoint(name).is_some() => {
            let which = expenses::Route::from_endpoint(name).expect("matched above");
            expenses_route(state, which, route, query, headers, peer).await
        }
        name if goals::Route::from_endpoint(name).is_some() => {
            let which = goals::Route::from_endpoint(name).expect("matched above");
            goals_route(state, which, route, query, headers, peer).await
        }
        name if traffic::Route::from_endpoint(name).is_some() => {
            let which = traffic::Route::from_endpoint(name).expect("matched above");
            let Some(now) = state.trend_now(headers) else {
                return detail(
                    StatusCode::BAD_REQUEST,
                    "X-Pilot-Now must be an RFC 3339 instant",
                );
            };
            let admit = admitter(state, route, headers, peer);
            match traffic::answer(which, &state.reads, query, now, admit).await {
                Ok(body) => raw_json(StatusCode::OK, body),
                Err(failure) => failure.into_response(),
            }
        }
        name if inventory::Route::from_endpoint(name).is_some() => {
            let which = inventory::Route::from_endpoint(name).expect("matched above");
            let admit = admitter(state, route, headers, peer);
            let abc_class = captures.get("abc_class").map(String::as_str);
            match inventory::answer(which, &state.reads, query, abc_class, admit).await {
                Ok(body) => raw_json(StatusCode::OK, body),
                Err(failure) => failure.into_response(),
            }
        }
        SPA_CATCH_ALL => {
            // `path == "api" or path.startswith("api/")`, before any session check.
            let spa = captures.get("path").map_or("", String::as_str);
            if spa == "api" || spa.starts_with("api/") {
                detail(StatusCode::NOT_FOUND, "Not Found")
            } else {
                not_in_pilot(format!("GET {}", route.path), "route")
            }
        }
        _ => not_in_pilot(
            format!("{} {}", route.methods.join(","), route.path),
            "route",
        ),
    }
}

async fn revenue_trend(
    state: &AppState,
    route: &ApiRoute,
    query: &str,
    headers: &HeaderMap,
    peer: SocketAddr,
) -> Response {
    let Some(now) = state.trend_now(headers) else {
        return detail(
            StatusCode::BAD_REQUEST,
            "X-Pilot-Now must be an RFC 3339 instant",
        );
    };
    match trend::revenue_trend(
        &state.gold,
        query,
        now,
        admitter(state, route, headers, peer),
    )
    .await
    {
        Ok(body) => raw_json(StatusCode::OK, body),
        Err(failure) => failure.into_response(),
    }
}

/// The route's slowapi limits, checked when the endpoint is called: once the
/// parameters parse, before the route's own validators.
fn admitter<'a>(
    state: &'a AppState,
    route: &'a ApiRoute,
    headers: &HeaderMap,
    peer: SocketAddr,
) -> impl FnOnce() -> Result<(), Failure> + 'a {
    let key = client_key(
        headers.get("x-real-ip").map(HeaderValue::as_bytes),
        &peer.ip().to_string(),
    );
    move || {
        let Some(key) = key.as_deref() else {
            return Ok(());
        };
        let at = unix_now().as_secs_f64();
        match route
            .rate_limits
            .iter()
            .find(|l| !state.limiter.hit(l, key, at))
        {
            Some(limit) => Err(Failure::RateLimited(limit.limit.clone())),
            None => Ok(()),
        }
    }
}

/// One of `/api/margin/*`, past the gates.
async fn margin_route(
    state: &AppState,
    which: margin::Route,
    route: &ApiRoute,
    query: &str,
    headers: &HeaderMap,
    peer: SocketAddr,
) -> Response {
    let Some(now) = state.trend_now(headers) else {
        return detail(
            StatusCode::BAD_REQUEST,
            "X-Pilot-Now must be an RFC 3339 instant",
        );
    };
    let admit = admitter(state, route, headers, peer);
    match margin::answer(which, &state.margin, query, now, admit).await {
        Ok(body) => raw_json(StatusCode::OK, body),
        Err(failure) => failure.into_response(),
    }
}

/// One of `/api/products/intel/*`, past the gates.
async fn products_route(
    state: &AppState,
    which: products_intel::Route,
    route: &ApiRoute,
    query: &str,
    headers: &HeaderMap,
    peer: SocketAddr,
) -> Response {
    let Some(now) = state.trend_now(headers) else {
        return detail(
            StatusCode::BAD_REQUEST,
            "X-Pilot-Now must be an RFC 3339 instant",
        );
    };
    let admit = admitter(state, route, headers, peer);
    match products_intel::answer(which, &state.reads, query, now, admit).await {
        Ok(body) => raw_json(StatusCode::OK, body),
        Err(failure) => failure.into_response(),
    }
}

/// One of `/api/reports/*`, past the gates: JSON, or a CSV download.
async fn reports_route(
    state: &AppState,
    which: reports::Route,
    route: &ApiRoute,
    query: &str,
    headers: &HeaderMap,
    peer: SocketAddr,
) -> Response {
    let Some(now) = state.trend_now(headers) else {
        return detail(
            StatusCode::BAD_REQUEST,
            "X-Pilot-Now must be an RFC 3339 instant",
        );
    };
    let admit = admitter(state, route, headers, peer);
    match reports::answer(which, &state.reads, query, now, admit).await {
        Ok(reports::Answer::Json(body)) => raw_json(StatusCode::OK, body),
        Ok(reports::Answer::Csv { body, filename }) => csv(body, &filename),
        Err(failure) => failure.into_response(),
    }
}

/// One of the goal routes' seven GETs, past the gates.
async fn goals_route(
    state: &AppState,
    which: goals::Route,
    route: &ApiRoute,
    query: &str,
    headers: &HeaderMap,
    peer: SocketAddr,
) -> Response {
    let Some(now) = state.trend_now(headers) else {
        return detail(
            StatusCode::BAD_REQUEST,
            "X-Pilot-Now must be an RFC 3339 instant",
        );
    };
    let admit = admitter(state, route, headers, peer);
    match goals::answer(which, &state.reads, query, now, admit).await {
        Ok(body) => raw_json(StatusCode::OK, body),
        Err(failure) => failure.into_response(),
    }
}

/// One of the expenses router's four GET routes, past the gates.
async fn expenses_route(
    state: &AppState,
    which: expenses::Route,
    route: &ApiRoute,
    query: &str,
    headers: &HeaderMap,
    peer: SocketAddr,
) -> Response {
    let Some(now) = state.trend_now(headers) else {
        return detail(
            StatusCode::BAD_REQUEST,
            "X-Pilot-Now must be an RFC 3339 instant",
        );
    };
    let admit = admitter(state, route, headers, peer);
    match expenses::answer(which, &state.reads, query, now, admit).await {
        Ok(body) => raw_json(StatusCode::OK, body),
        Err(failure) => failure.into_response(),
    }
}

/// `StreamingResponse(…, media_type="text/csv; charset=utf-8",
/// headers={"Content-Disposition": f"attachment; filename={filename}"})`.
fn csv(body: Vec<u8>, filename: &str) -> Response {
    match HeaderValue::from_str(&format!("attachment; filename={filename}")) {
        Ok(disposition) => (
            StatusCode::OK,
            [
                (
                    header::CONTENT_TYPE,
                    HeaderValue::from_static("text/csv; charset=utf-8"),
                ),
                (header::CONTENT_DISPOSITION, disposition),
            ],
            body,
        )
            .into_response(),
        // A name made of dates is always a header value; Starlette would
        // have refused anything else while encoding it.
        Err(_) => json(StatusCode::INTERNAL_SERVER_ERROR, &INTERNAL),
    }
}

/// Liveness only: it asks Postgres nothing. The pilot's, not the app's.
async fn healthz() -> Response {
    raw_json(StatusCode::OK, br#"{"status":"ok"}"#.to_vec())
}

/// Readiness, which the container's healthcheck asks (`ks-api healthcheck`):
/// Postgres answers this service's role, and the schema is at the contract's
/// revision. Not the app's either — nginx routes no client to it.
async fn readyz(State(state): State<Arc<AppState>>) -> Response {
    match state.access.store.ready().await {
        Ok(()) => raw_json(StatusCode::OK, br#"{"status":"ok"}"#.to_vec()),
        Err(error) => {
            tracing::warn!("not ready: {}", error.0);
            raw_json(
                StatusCode::SERVICE_UNAVAILABLE,
                br#"{"status":"unavailable"}"#.to_vec(),
            )
        }
    }
}

fn panicked(_payload: Box<dyn Any + Send + 'static>) -> Response {
    tracing::error!("handler panicked; answering the Python app's 500");
    json(StatusCode::INTERNAL_SERVER_ERROR, &INTERNAL)
}

/// Eight hex digits, unique enough to find one request in a log.
fn correlation_id() -> String {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    static KEYS: OnceLock<RandomState> = OnceLock::new();
    let n = NEXT.fetch_add(1, Ordering::Relaxed);
    let hash = KEYS.get_or_init(RandomState::new).hash_one(n);
    format!("{:08x}", hash >> 32)
}

/// `RejectNulMiddleware`: an `/api/` request with a NUL in its path or its
/// query string is a 400 before routing, before anything reads a session.
/// It sits inside `observe`, as Python's sits inside the logging middleware.
async fn reject_nul(request: Request, next: Next) -> Response {
    if let Some(at) = nul_in(request.uri()) {
        let detail = format!("{at}: Must not contain a NUL character");
        return json(
            StatusCode::BAD_REQUEST,
            &ErrorBody {
                error: "Validation Error",
                detail: &detail,
                surface: None,
            },
        );
    }
    next.run(request).await
}

/// `_nul_in`: `path` when the decoded path holds a NUL, `query` when the raw
/// query string holds `%00` or a NUL — for a decoded path under `/api/`.
fn nul_in(uri: &Uri) -> Option<&'static str> {
    let path = unquoted(uri.path());
    if !path.starts_with(b"/api/") {
        return None;
    }
    if path.contains(&0) {
        return Some("path");
    }
    let query = uri.query().unwrap_or("");
    (query.contains("%00") || query.contains('\0')).then_some("query")
}

/// The raw path as uvicorn's `unquote` reads it, in bytes: each `%XX` one
/// byte, anything else as it stands.
fn unquoted(raw: &str) -> Vec<u8> {
    let bytes = raw.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        let hex = |b: u8| (b as char).to_digit(16);
        if bytes[i] == b'%'
            && let (Some(hi), Some(lo)) = (
                bytes.get(i + 1).copied().and_then(hex),
                bytes.get(i + 2).copied().and_then(hex),
            )
        {
            out.push((hi * 16 + lo) as u8);
            i += 3;
        } else {
            out.push(bytes[i]);
            i += 1;
        }
    }
    out
}

/// `RequestLoggingMiddleware` and `RequestTimeoutMiddleware` in one.
async fn observe(request: Request, next: Next) -> Response {
    let id = request
        .headers()
        .get("x-request-id")
        .and_then(|v| v.to_str().ok())
        .map_or_else(correlation_id, str::to_owned);
    let method = request.method().clone();
    let path = request.uri().path().to_owned();
    let quiet = path == "/healthz" || path == "/readyz";
    let started = Instant::now();
    if !quiet {
        tracing::info!(%method, %path, request_id = %id, "Request started");
    }

    let (limit, detail) = if SLOW_ENDPOINTS.contains(&uvicorn_path(&path).as_str()) {
        (SLOW_REQUEST_TIMEOUT, "Request exceeded 300.0s timeout")
    } else {
        (REQUEST_TIMEOUT, "Request exceeded 30.0s timeout")
    };
    let mut response = match tokio::time::timeout(limit, next.run(request)).await {
        Ok(response) => response,
        Err(_) => {
            tracing::warn!(%method, %path, request_id = %id, "Request timeout");
            json(
                StatusCode::GATEWAY_TIMEOUT,
                &Timeout {
                    error: "Request Timeout",
                    detail,
                    path: &path,
                    correlation_id: &id,
                },
            )
        }
    };

    let elapsed_ms = started.elapsed().as_secs_f64() * 1000.0;
    let headers = response.headers_mut();
    if let Ok(value) = HeaderValue::from_str(&id) {
        headers.insert("x-request-id", value);
    }
    if let Ok(value) = HeaderValue::from_str(&format!("{elapsed_ms:.2}ms")) {
        headers.insert("x-response-time", value);
    }
    if !quiet {
        let status = response.status().as_u16();
        if status < 400 {
            tracing::info!(%method, %path, status, elapsed_ms, "Request completed");
        } else {
            tracing::warn!(%method, %path, status, elapsed_ms, "Request completed");
        }
    }
    response
}
