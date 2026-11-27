//! Two readings of a request's path, each where the Python app makes it.
//!
//! uvicorn decodes the request target before anything routes on it: the
//! path, if it holds a `%`, through `urllib.parse.unquote` — UTF-8, a
//! replacement character for every invalid sequence, a `%` not followed by
//! two hex digits kept as it is. So `%2F` is a separator by the time a route
//! sees it, and that decoded path is what Starlette matches.
//!
//! `api_gate` asks `request.url.path` whether the path is public, and that is
//! not the routed path: Starlette rebuilds a URL around it and `urlsplit`
//! reads it back, which drops every tab, CR and LF and ends the path at the
//! first `?` or `#` a decoded `%3F` or `%23` put there.

/// `scope["path"]` as uvicorn sets it from the raw path.
pub fn uvicorn_path(raw: &str) -> String {
    if !raw.contains('%') {
        return raw.to_owned();
    }
    // `unquote` decodes each run of ASCII on its own and keeps the rest as is.
    let mut out = String::new();
    let mut rest = raw;
    while !rest.is_empty() {
        let ascii_len = rest.bytes().take_while(u8::is_ascii).count();
        let (ascii, tail) = rest.split_at(ascii_len);
        out.push_str(&String::from_utf8_lossy(&unquote_to_bytes(
            ascii.as_bytes(),
        )));
        let other_len = tail
            .char_indices()
            .find(|(_, c)| c.is_ascii())
            .map_or(tail.len(), |(at, _)| at);
        let (other, next) = tail.split_at(other_len);
        out.push_str(other);
        rest = next;
    }
    out
}

/// `urllib.parse._unquote_impl`: split on `%`; two hex digits after one are a byte.
fn unquote_to_bytes(s: &[u8]) -> Vec<u8> {
    let mut parts = s.split(|&b| b == b'%');
    let mut out = parts.next().unwrap_or_default().to_vec();
    for part in parts {
        let byte = part
            .get(..2)
            .and_then(|h| std::str::from_utf8(h).ok())
            .filter(|h| h.bytes().all(|b| b.is_ascii_hexdigit()))
            .and_then(|h| u8::from_str_radix(h, 16).ok());
        match byte {
            Some(b) => {
                out.push(b);
                out.extend_from_slice(&part[2..]);
            }
            None => {
                out.push(b'%');
                out.extend_from_slice(part);
            }
        }
    }
    out
}

/// `request.url.path` for a decoded path. `urlunsplit` puts a `/` before a
/// path that lacks one, since the URL has a host — a real request path
/// always has one, and the rule is kept anyway.
pub fn url_path(path: &str) -> String {
    let slash = (!path.is_empty() && !path.starts_with('/')).then_some('/');
    slash
        .into_iter()
        .chain(path.chars())
        .filter(|c| !matches!(c, '\t' | '\r' | '\n'))
        .take_while(|c| !matches!(c, '?' | '#'))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::{url_path, uvicorn_path};

    #[test]
    fn decoding_and_rereading() {
        assert_eq!(uvicorn_path("/api/revenue%2Ftrend"), "/api/revenue/trend");
        assert_eq!(uvicorn_path("/a%zz%4"), "/a%zz%4");
        assert_eq!(uvicorn_path("/%C3"), "/\u{fffd}");
        assert_eq!(url_path("/api/health\n"), "/api/health");
        assert_eq!(url_path("/api/health?x#y"), "/api/health");
    }
}
