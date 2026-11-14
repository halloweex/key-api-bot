//! `request.cookies` in Starlette 1.6: every `Cookie` header in order, each
//! through `cookie_parser`, merged into one dict — so a later header's value
//! wins, and within a header a later pair's.
//!
//! `cookie_parser` is Django's, not RFC 6265's: split on `;`, then on the
//! first `=` (no `=` is a value with an empty name), `str.strip()` both —
//! Python's whitespace, U+001C..U+001F and U+00A0 among it — keep the pair
//! unless both are empty, and unquote the value with `http.cookies._unquote`.
//! A header is latin-1, so every byte is one character.

use crate::pyrt::py_strip;

/// The value Starlette would hand `_resolve_session` for `name`.
pub fn cookie<'a>(headers: impl IntoIterator<Item = &'a [u8]>, name: &str) -> Option<String> {
    request_cookies(headers)
        .into_iter()
        .find(|(k, _)| k == name)
        .map(|(_, v)| v)
}

/// The whole dict, in its insertion order.
pub fn request_cookies<'a>(headers: impl IntoIterator<Item = &'a [u8]>) -> Vec<(String, String)> {
    let mut cookies: Vec<(String, String)> = Vec::new();
    for header in headers {
        let text: String = header.iter().map(|&b| char::from(b)).collect();
        for chunk in text.split(';') {
            let (key, value) = chunk.split_once('=').unwrap_or(("", chunk));
            let (key, value) = (py_strip(key), py_strip(value));
            if key.is_empty() && value.is_empty() {
                continue;
            }
            let value = unquote(value);
            match cookies.iter_mut().find(|(k, _)| k == key) {
                Some(slot) => slot.1 = value,
                None => cookies.push((key.to_owned(), value)),
            }
        }
    }
    cookies
}

/// `http.cookies._unquote`: inside a quoted value, `\NNN` (octal, first digit
/// 0..3) is that character and a backslash before anything but a newline is
/// what follows it.
fn unquote(value: &str) -> String {
    let chars: Vec<char> = value.chars().collect();
    if chars.len() < 2 || chars[0] != '"' || chars[chars.len() - 1] != '"' {
        return value.to_owned();
    }
    let inner = &chars[1..chars.len() - 1];
    let mut out = String::with_capacity(inner.len());
    let mut i = 0;
    while i < inner.len() {
        if inner[i] == '\\' {
            let octal = inner.get(i + 1..i + 4).filter(|d| {
                matches!(d[0], '0'..='3') && matches!(d[1], '0'..='7') && matches!(d[2], '0'..='7')
            });
            if let Some(d) = octal {
                let code = d.iter().fold(0, |n, c| n * 8 + c.to_digit(8).unwrap_or(0));
                out.push(char::from_u32(code).unwrap_or('\u{fffd}'));
                i += 4;
                continue;
            }
            if let Some(&next) = inner.get(i + 1).filter(|&&c| c != '\n') {
                out.push(next);
                i += 2;
                continue;
            }
        }
        out.push(inner[i]);
        i += 1;
    }
    out
}

#[cfg(test)]
mod tests {
    use super::cookie;

    #[test]
    fn a_later_header_wins_and_quotes_come_off() {
        let headers: [&[u8]; 2] = [b"dashboard_session=a", b" dashboard_session = \"b\\\"c\" "];
        assert_eq!(
            cookie(headers, "dashboard_session").as_deref(),
            Some("b\"c")
        );
        assert_eq!(cookie([b"x=1".as_slice()], "dashboard_session"), None);
    }
}
