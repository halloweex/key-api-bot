//! The HTTP shell: the route, the bodies the Python app answers errors
//! with, and the bookkeeping `web/middleware.py` does for every request — a
//! correlation id, a start and an end log line, a 30-second timeout — so the
//! benchmark compares like with like.
//!
//! There is no authentication here, by design of the pilot: it listens on
//! loopback unless told otherwise, and must stay unreachable from anywhere a
//! person is. `sales_type=internal` is admin-only behind `api_gate` in the
//! Python app and is served to anyone who can reach this one.

use std::any::Any;
use std::collections::hash_map::RandomState;
use std::hash::BuildHasher;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

use axum::Router;
use axum::extract::{Request, State};
use axum::http::{HeaderMap, HeaderValue, StatusCode, Uri, header};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use chrono::{DateTime, Utc};
use serde::Serialize;
use tower_http::catch_panic::CatchPanicLayer;

use crate::gold::PgGold;
use crate::trend::{self, Failure};

/// `DEFAULT_REQUEST_TIMEOUT` in `web/middleware.py`.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);
/// Honoured only under `KS_PILOT_TEST_CLOCK=1`: the harness's frozen instant.
const TEST_CLOCK_HEADER: &str = "x-pilot-now";

pub struct AppState {
    gold: PgGold,
    test_clock: bool,
}

impl AppState {
    pub fn new(gold: PgGold, test_clock: bool) -> Self {
        Self { gold, test_clock }
    }

    /// The real clock, or the harness's instant when the hook is on.
    fn now(&self, headers: &HeaderMap) -> Option<DateTime<Utc>> {
        let Some(value) = headers.get(TEST_CLOCK_HEADER).filter(|_| self.test_clock) else {
            return Some(Utc::now());
        };
        let instant = DateTime::parse_from_rfc3339(value.to_str().ok()?).ok()?;
        Some(instant.with_timezone(&Utc))
    }
}

pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/api/revenue/trend", get(revenue_trend))
        .route("/healthz", get(healthz))
        .fallback(not_found)
        .method_not_allowed_fallback(method_not_allowed)
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

const INTERNAL: ErrorBody<'static> = ErrorBody {
    error: "Internal Server Error",
    detail: "An unexpected error occurred.",
    surface: None,
};

impl IntoResponse for Failure {
    fn into_response(self) -> Response {
        match self {
            Failure::Unprocessable(errors) => {
                json(StatusCode::UNPROCESSABLE_ENTITY, &Detail { detail: errors })
            }
            Failure::BadRequest(detail) => json(StatusCode::BAD_REQUEST, &Detail { detail }),
            Failure::Raised(why) => {
                tracing::error!(why, "the Python app raises here; answering its 500");
                json(StatusCode::INTERNAL_SERVER_ERROR, &INTERNAL)
            }
            Failure::Unavailable(error) => {
                tracing::error!(error = %error.0, "Gold read failed");
                json(
                    StatusCode::SERVICE_UNAVAILABLE,
                    &ErrorBody {
                        error: "Service Unavailable",
                        detail: "The data behind 'dashboard' cannot be read right now. \
                                 Try again shortly.",
                        surface: Some("dashboard"),
                    },
                )
            }
            Failure::NotInPilot(branch) => json(
                StatusCode::NOT_IMPLEMENTED,
                &NotInPilot {
                    error: "Not Implemented",
                    detail: format!("{branch} is not in the Rust pilot; the Python app serves it"),
                    branch,
                },
            ),
        }
    }
}

async fn revenue_trend(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    uri: Uri,
) -> Response {
    let Some(now) = state.now(&headers) else {
        return json(
            StatusCode::BAD_REQUEST,
            &Detail {
                detail: "X-Pilot-Now must be an RFC 3339 instant",
            },
        );
    };
    match trend::revenue_trend(&state.gold, uri.query().unwrap_or(""), now).await {
        Ok(body) => raw_json(StatusCode::OK, body),
        Err(failure) => failure.into_response(),
    }
}

/// Liveness only: it asks Postgres nothing.
async fn healthz() -> Response {
    raw_json(StatusCode::OK, br#"{"status":"ok"}"#.to_vec())
}

async fn not_found() -> Response {
    json(
        StatusCode::NOT_FOUND,
        &Detail {
            detail: "Not Found",
        },
    )
}

async fn method_not_allowed() -> Response {
    json(
        StatusCode::METHOD_NOT_ALLOWED,
        &Detail {
            detail: "Method Not Allowed",
        },
    )
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
    let quiet = path == "/healthz";
    let started = Instant::now();
    if !quiet {
        tracing::info!(%method, %path, request_id = %id, "Request started");
    }

    let mut response = match tokio::time::timeout(REQUEST_TIMEOUT, next.run(request)).await {
        Ok(response) => response,
        Err(_) => {
            tracing::warn!(%method, %path, request_id = %id, "Request timeout");
            json(
                StatusCode::GATEWAY_TIMEOUT,
                &Timeout {
                    error: "Request Timeout",
                    detail: "Request exceeded 30.0s timeout",
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
