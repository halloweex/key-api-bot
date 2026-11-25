//! ClickHouse over HTTP — `core/ch_common.py`'s `execute` and
//! `ensure_database`.
//!
//! One POST per statement. DDL and SELECT travel as the body; an INSERT
//! travels as `?query=` with its TabSeparated rows as the body, ClickHouse's
//! documented way to stream data. The credentials are headers
//! (`X-ClickHouse-User`, `X-ClickHouse-Key`), never the address, so a logged
//! request line cannot carry them. Thirty seconds a statement, Python's
//! `TIMEOUT_S`, sized for today's 47 000-row INSERTs.
//!
//! A fresh connection per statement, as Python's client-per-call is: at ten
//! statements an hour keep-alive buys nothing, and a pooled connection
//! ClickHouse has already closed is a failure it would add.
//!
//! `http://` only: production reaches ClickHouse inside a Docker network,
//! and this build speaks no TLS. An `https://` address is refused when the
//! client is made, not when a statement fails.

use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use http_body_util::{BodyExt, Full};
use hyper::body::Bytes;
use hyper::{Method, Request, StatusCode, Uri};
use hyper_util::client::legacy::Client;
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::rt::TokioExecutor;

use crate::config;

pub const URL_ENV: &str = "KS_CH_URL";
pub const USER_ENV: &str = "KS_CH_USER";
pub const PASSWORD_ENV: &str = "KS_CH_PASSWORD";
/// The user when `KS_CH_USER` is unset: `core/ch_common.py`'s default.
pub const DEFAULT_USER: &str = "ks_app";
pub const TIMEOUT: Duration = Duration::from_secs(30);
/// How much of an error answer a message carries: Python's `text[:300]`.
pub const ERROR_TEXT_CHARS: usize = 300;

/// What a statement can fail with.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ChError {
    /// The address or a header cannot be used.
    Config(String),
    /// ClickHouse answered, and not with 200: Python's
    /// `RuntimeError(f"ClickHouse HTTP {status}: {text[:300]}")`.
    Http { status: u16, text: String },
    /// No answer: the connection failed or broke.
    Transport(String),
    /// No answer within the timeout.
    Timeout(Duration),
}

impl std::fmt::Display for ChError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ChError::Config(text) => f.write_str(text),
            ChError::Http { status, text } => write!(f, "ClickHouse HTTP {status}: {text}"),
            ChError::Transport(text) => write!(f, "ClickHouse unreachable: {text}"),
            ChError::Timeout(after) => {
                write!(f, "ClickHouse: no answer within {} s", after.as_secs_f64())
            }
        }
    }
}

impl std::error::Error for ChError {}

/// One statement as it was sent: its text, and the rows of an INSERT.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Sent {
    pub sql: String,
    pub body: Option<Vec<u8>>,
}

/// Every statement a client sends, in order — what a harness compares with
/// the statements Python sends.
#[derive(Clone, Debug, Default)]
pub struct Recorder(Arc<Mutex<Vec<Sent>>>);

impl Recorder {
    fn push(&self, sent: Sent) {
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(sent);
    }

    /// What was sent so far.
    pub fn sent(&self) -> Vec<Sent> {
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }
}

/// A ClickHouse server, reached over HTTP.
#[derive(Clone)]
pub struct ClickHouse {
    client: Client<HttpConnector, Full<Bytes>>,
    endpoint: String,
    user: String,
    key: Option<String>,
    timeout: Duration,
    recorder: Option<Recorder>,
}

impl std::fmt::Debug for ClickHouse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // The key is never printed.
        f.debug_struct("ClickHouse")
            .field("endpoint", &self.endpoint)
            .field("user", &self.user)
            .finish_non_exhaustive()
    }
}

impl ClickHouse {
    /// The server `KS_CH_URL` names, as `KS_CH_USER` (default `ks_app`) with
    /// `KS_CH_PASSWORD`; `None` when `KS_CH_URL` is unset — the copy then
    /// stands down, as Python's does.
    pub fn from_env() -> Result<Option<Self>, ChError> {
        let Some(url) = config::trimmed(URL_ENV) else {
            return Ok(None);
        };
        let user = config::trimmed(USER_ENV).unwrap_or_else(|| DEFAULT_USER.to_owned());
        let key = config::raw(PASSWORD_ENV);
        Self::new(&url, &user, key.as_deref()).map(Some)
    }

    /// A client for `url`: `url.strip().rstrip("/") + "/"` is where every
    /// statement goes, as in Python.
    pub fn new(url: &str, user: &str, key: Option<&str>) -> Result<Self, ChError> {
        let endpoint = format!("{}/", url.trim().trim_end_matches('/'));
        if !endpoint.to_ascii_lowercase().starts_with("http://") {
            return Err(ChError::Config(format!(
                "{URL_ENV} must be an http:// address: this build speaks no TLS"
            )));
        }
        endpoint
            .parse::<Uri>()
            .map_err(|e| ChError::Config(format!("{URL_ENV} is not an address: {e}")))?;
        let client = Client::builder(TokioExecutor::new())
            .pool_max_idle_per_host(0)
            .build_http();
        Ok(ClickHouse {
            client,
            endpoint,
            user: user.to_owned(),
            key: key.filter(|k| !k.is_empty()).map(str::to_owned),
            timeout: TIMEOUT,
            recorder: None,
        })
    }

    /// The same client, recording every statement it sends.
    pub fn recording(mut self, recorder: Recorder) -> Self {
        self.recorder = Some(recorder);
        self
    }

    /// A statement whose text is the body: DDL, SELECT, EXCHANGE.
    pub async fn execute(&self, sql: &str) -> Result<String, ChError> {
        self.post(sql, None).await
    }

    /// An INSERT: the statement as `?query=`, its rows as the body.
    pub async fn insert(&self, sql: &str, rows: Vec<u8>) -> Result<String, ChError> {
        self.post(sql, Some(rows)).await
    }

    async fn post(&self, sql: &str, rows: Option<Vec<u8>>) -> Result<String, ChError> {
        if let Some(recorder) = &self.recorder {
            recorder.push(Sent {
                sql: sql.to_owned(),
                body: rows.clone(),
            });
        }
        let (uri, content) = match rows {
            None => (self.endpoint.clone(), sql.as_bytes().to_vec()),
            Some(rows) => {
                let query = form_urlencoded::Serializer::new(String::new())
                    .append_pair("query", sql)
                    .finish();
                (format!("{}?{query}", self.endpoint), rows)
            }
        };
        let mut request = Request::builder()
            .method(Method::POST)
            .uri(uri)
            .header("X-ClickHouse-User", &self.user);
        if let Some(key) = &self.key {
            request = request.header("X-ClickHouse-Key", key);
        }
        let request = request
            .body(Full::new(Bytes::from(content)))
            .map_err(|_| ChError::Config("a ClickHouse header is not a valid value".into()))?;
        let exchange = async {
            let response = self
                .client
                .request(request)
                .await
                .map_err(|e| ChError::Transport(chain(&e)))?;
            let status = response.status();
            let body = response
                .into_body()
                .collect()
                .await
                .map_err(|e| ChError::Transport(chain(&e)))?
                .to_bytes();
            Ok::<_, ChError>((status, body))
        };
        let (status, body) = tokio::time::timeout(self.timeout, exchange)
            .await
            .map_err(|_| ChError::Timeout(self.timeout))??;
        let text = String::from_utf8_lossy(&body).into_owned();
        if status != StatusCode::OK {
            return Err(ChError::Http {
                status: status.as_u16(),
                text: text.chars().take(ERROR_TEXT_CHARS).collect(),
            });
        }
        Ok(text)
    }

    /// `CREATE DATABASE IF NOT EXISTS`, tolerating a denied global right.
    ///
    /// On production the platform pre-creates the databases and `ks_app`
    /// holds no global CREATE DATABASE, so the statement is denied even when
    /// the database exists; the CREATE TABLE that follows is the real probe.
    /// What is tolerated is Python's test, on the same text: an answer that
    /// says `ACCESS_DENIED` or `Code: 497` within its first 300 characters.
    /// A connection that fails is not tolerated.
    pub async fn ensure_database(&self, name: &str) -> Result<(), ChError> {
        match self
            .execute(&format!("CREATE DATABASE IF NOT EXISTS {name}"))
            .await
        {
            Ok(_) => Ok(()),
            Err(error @ ChError::Http { .. }) => {
                let text = error.to_string();
                if text.contains("ACCESS_DENIED") || text.contains("Code: 497") {
                    Ok(())
                } else {
                    Err(error)
                }
            }
            Err(error) => Err(error),
        }
    }
}

/// An error and its causes, one line: hyper's own message is "client error
/// (Connect)", and the cause is what a reader needs.
fn chain(error: &(dyn std::error::Error + 'static)) -> String {
    let mut text = error.to_string();
    let mut source = error.source();
    while let Some(cause) = source {
        let cause_text = cause.to_string();
        if !text.contains(&cause_text) {
            text.push_str(": ");
            text.push_str(&cause_text);
        }
        source = cause.source();
    }
    text
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_endpoint_is_the_address_with_one_slash() {
        for url in ["http://ch:8123", "http://ch:8123/", " http://ch:8123// "] {
            assert_eq!(
                ClickHouse::new(url, "u", None).unwrap().endpoint,
                "http://ch:8123/"
            );
        }
    }

    #[test]
    fn https_and_nonsense_are_refused_up_front() {
        assert!(matches!(
            ClickHouse::new("https://ch:8443", "u", None),
            Err(ChError::Config(_))
        ));
        assert!(matches!(
            ClickHouse::new("not an address", "u", None),
            Err(ChError::Config(_))
        ));
    }

    #[test]
    fn an_empty_key_sends_no_key_header() {
        assert_eq!(
            ClickHouse::new("http://ch:8123", "u", Some(""))
                .unwrap()
                .key,
            None
        );
    }

    #[test]
    fn the_message_is_pythons() {
        let error = ChError::Http {
            status: 500,
            text: "Code: 60. DB::Exception: Table silver.x does not exist.".into(),
        };
        assert_eq!(
            error.to_string(),
            "ClickHouse HTTP 500: Code: 60. DB::Exception: Table silver.x does not exist."
        );
    }

    /// A one-shot HTTP server on loopback: answers `status` with `body` and
    /// hands back the request it read.
    async fn serve_once(
        status: u16,
        body: &'static str,
    ) -> (String, tokio::task::JoinHandle<String>) {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = format!("http://{}", listener.local_addr().unwrap());
        let handle = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut request = Vec::new();
            let mut buffer = [0u8; 4096];
            loop {
                let n = socket.read(&mut buffer).await.unwrap();
                request.extend_from_slice(&buffer[..n]);
                let text = String::from_utf8_lossy(&request);
                if let Some(end) = text.find("\r\n\r\n") {
                    let length = text[..end]
                        .lines()
                        .find_map(|l| {
                            l.to_ascii_lowercase()
                                .strip_prefix("content-length: ")
                                .map(str::to_owned)
                        })
                        .and_then(|v| v.trim().parse::<usize>().ok())
                        .unwrap_or(0);
                    if request.len() >= end + 4 + length {
                        break;
                    }
                }
                if n == 0 {
                    break;
                }
            }
            let answer = format!(
                "HTTP/1.1 {status} X\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                body.len()
            );
            socket.write_all(answer.as_bytes()).await.unwrap();
            String::from_utf8_lossy(&request).into_owned()
        });
        (address, handle)
    }

    #[tokio::test]
    async fn an_insert_carries_its_statement_in_the_query_and_the_key_in_a_header() {
        let (address, server) = serve_once(200, "").await;
        let recorder = Recorder::default();
        let ch = ClickHouse::new(&address, "ks_app", Some("secret"))
            .unwrap()
            .recording(recorder.clone());
        ch.insert("INSERT INTO t (a) FORMAT TabSeparated", b"1\n".to_vec())
            .await
            .unwrap();
        let request = server.await.unwrap();
        let head = request.lines().next().unwrap();
        assert_eq!(
            head,
            "POST /?query=INSERT+INTO+t+%28a%29+FORMAT+TabSeparated HTTP/1.1"
        );
        let lower = request.to_ascii_lowercase();
        assert!(lower.contains("x-clickhouse-user: ks_app"));
        assert!(lower.contains("x-clickhouse-key: secret"));
        assert!(request.ends_with("\r\n\r\n1\n"));
        assert!(!head.contains("secret"));
        assert_eq!(
            recorder.sent(),
            vec![Sent {
                sql: "INSERT INTO t (a) FORMAT TabSeparated".into(),
                body: Some(b"1\n".to_vec()),
            }]
        );
    }

    #[tokio::test]
    async fn a_denied_create_database_is_tolerated_and_another_error_is_not() {
        let (address, _server) = serve_once(
            500,
            "Code: 497. DB::Exception: ks_app: Not enough privileges. (ACCESS_DENIED)",
        )
        .await;
        let ch = ClickHouse::new(&address, "ks_app", None).unwrap();
        assert_eq!(ch.ensure_database("history").await, Ok(()));

        let (address, _server) = serve_once(500, "Code: 1000. DB::Exception: something else").await;
        let ch = ClickHouse::new(&address, "ks_app", None).unwrap();
        assert!(matches!(
            ch.ensure_database("history").await,
            Err(ChError::Http { status: 500, .. })
        ));
    }

    #[tokio::test]
    async fn a_closed_port_is_a_transport_error() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let address = format!("http://{}", listener.local_addr().unwrap());
        drop(listener);
        let ch = ClickHouse::new(&address, "ks_app", None).unwrap();
        assert!(matches!(
            ch.execute("SELECT 1").await,
            Err(ChError::Transport(_))
        ));
        assert!(matches!(
            ch.ensure_database("x").await,
            Err(ChError::Transport(_))
        ));
    }
}
