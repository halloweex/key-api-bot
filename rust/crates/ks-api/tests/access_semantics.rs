//! The access layer's CPython semantics, case by case, against CPython.
//!
//! `fixtures/access_semantics.jsonl` holds inputs and what CPython 3.14 with
//! the locked itsdangerous and Starlette made of them (`gen_access_fixtures.py`
//! writes it): base64, zlib, `json.loads`, Starlette's cookie parsing,
//! uvicorn's path decoding, `request.url.path`, every route pattern under
//! `re.match`, and `URLSafeTimedSerializer.loads` on signed and forged
//! cookies at a fixed instant.

use std::collections::BTreeMap;

use ks_api::access::pybase64::itsdangerous_b64decode;
use ks_api::access::pyjson::{self, PyValue};
use ks_api::access::signer::{self, LoadError, SessionKey};
use ks_api::access::{cookie, paths, routing};
use regex::Regex;
use serde_json::{Value, json};

const FIXTURES: &str = include_str!("fixtures/access_semantics.jsonl");

fn hex(v: &Value) -> Vec<u8> {
    let s = v.as_str().expect("hex");
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).expect("hex digits"))
        .collect()
}

fn to_hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// A str from the fixture: itself, or its code points; `None` when one of
/// them is a surrogate, which no Rust string — and no decoded payload — holds.
fn text(v: &Value) -> Option<String> {
    match v {
        Value::String(s) => Some(s.clone()),
        Value::Array(cps) => cps
            .iter()
            .map(|c| char::from_u32(u32::try_from(c.as_u64()?).ok()?))
            .collect(),
        other => panic!("not a text: {other}"),
    }
}

fn tag(v: &PyValue) -> Value {
    match v {
        PyValue::None => json!(["n"]),
        PyValue::Bool(b) => json!(["b", b]),
        PyValue::Int(digits) => json!(["i", digits]),
        PyValue::Float(f) => json!(["f", format!("{:016x}", f.to_bits())]),
        PyValue::Str(cps) => json!(["s", cps]),
        PyValue::List(items) => json!(["l", items.iter().map(tag).collect::<Vec<_>>()]),
        PyValue::Dict(pairs) => json!([
            "d",
            pairs
                .iter()
                .map(|(k, v)| json!([k, tag(v)]))
                .collect::<Vec<_>>()
        ]),
    }
}

fn nested_at_least(mut v: &PyValue, depth: usize) -> bool {
    for _ in 0..depth {
        match v {
            PyValue::List(items) if items.len() == 1 => v = &items[0],
            _ => return false,
        }
    }
    true
}

struct Made {
    now: i64,
    max_age: i64,
    key: SessionKey,
}

/// `Some(true)` agrees, `Some(false)` does not, `None` cannot be asked here.
fn agrees(case: &Value, made: &Made) -> Option<bool> {
    let kind = case["kind"].as_str().expect("a kind");
    Some(match kind {
        "b64" => {
            let out = itsdangerous_b64decode(&hex(&case["in"])).map(|b| to_hex(&b));
            out.as_deref() == case["out"].as_str()
        }
        "zlib" => {
            let out = signer::zlib_decompress(&hex(&case["in"])).map(|b| to_hex(&b));
            out.as_deref() == case["out"].as_str()
        }
        "json" => {
            let out = pyjson::loads(&text(&case["in"])?).map(|v| tag(&v));
            out.unwrap_or(Value::Null) == case["out"]
        }
        "json_nest" => {
            let depth = usize::try_from(case["depth"].as_u64().expect("a depth")).expect("fits");
            let text = format!("{}{}", "[".repeat(depth), "]".repeat(depth));
            pyjson::loads(&text).is_some() == case["ok"].as_bool().expect("ok")
        }
        "cookie" => {
            let headers: Vec<Vec<u8>> = case["in"]
                .as_array()
                .expect("headers")
                .iter()
                .map(|h| {
                    h.as_str()
                        .expect("a header")
                        .chars()
                        .map(|c| u8::try_from(u32::from(c)).expect("latin-1"))
                        .collect()
                })
                .collect();
            let got = cookie::request_cookies(headers.iter().map(Vec::as_slice));
            json!(got) == case["out"]
        }
        "unquote" => {
            let raw = case["in"].as_str().expect("a raw path");
            Some(paths::uvicorn_path(raw)) == text(&case["out"])
        }
        "url_path" => Some(paths::url_path(&text(&case["in"])?)) == text(&case["out"]),
        "route" => {
            let regex = Regex::new(case["pattern"].as_str().expect("a pattern")).expect("compiles");
            let got = routing::py_match(&regex, &text(&case["in"])?);
            let want: Option<BTreeMap<String, String>> = match &case["out"] {
                Value::Null => None,
                Value::Object(groups) => Some(
                    groups
                        .iter()
                        .map(|(k, v)| (k.clone(), text(v).expect("a group")))
                        .collect(),
                ),
                other => panic!("route out: {other}"),
            };
            got == want
        }
        "itsd" => {
            let cookie = case["in"].as_str().expect("a cookie");
            let got = signer::loads(cookie, &made.key, made.now, made.max_age);
            match (&got, case.get("ok"), case.get("ok_nest"), case.get("err")) {
                (Ok(v), Some(want), _, _) => tag(v) == *want,
                (Ok(v), None, Some(_), _) => nested_at_least(v, 100),
                (Err(LoadError::Refused), _, _, Some(e)) => e == "refused",
                (Err(LoadError::Payload), _, _, Some(e)) => e == "payload",
                (Err(LoadError::Raised), _, _, Some(e)) => e == "raised",
                _ => false,
            }
        }
        other => panic!("a fixture kind this test does not know: {other}"),
    })
}

#[test]
fn every_case_answers_as_cpython_did() {
    let mut lines = FIXTURES.lines();
    let header: Value = serde_json::from_str(lines.next().expect("a header")).expect("JSON");
    assert_eq!(header["kind"], "made_by");
    let made = Made {
        now: header["now"].as_i64().expect("now"),
        max_age: header["max_age"].as_i64().expect("max_age"),
        key: SessionKey::derive(
            header["secret"].as_str().expect("secret").as_bytes(),
            b"itsdangerous",
        ),
    };
    let mut seen: BTreeMap<String, usize> = BTreeMap::new();
    let mut wrong: BTreeMap<String, usize> = BTreeMap::new();
    let mut not_asked = 0;
    let mut disagree = Vec::new();
    for line in lines {
        let case: Value = serde_json::from_str(line).expect("a JSON line");
        let kind = case["kind"].as_str().unwrap_or("?").to_owned();
        *seen.entry(kind.clone()).or_default() += 1;
        match agrees(&case, &made) {
            Some(true) => {}
            Some(false) => {
                *wrong.entry(kind).or_default() += 1;
                disagree.push(line.chars().take(300).collect::<String>());
            }
            None => not_asked += 1,
        }
    }
    assert!(
        disagree.is_empty(),
        "{} case(s) answer differently from CPython ({wrong:?}), first ones:\n{}",
        disagree.len(),
        disagree
            .iter()
            .take(10)
            .cloned()
            .collect::<Vec<_>>()
            .join("\n")
    );
    // Only JSON texts holding a raw surrogate, which a UTF-8 payload cannot.
    assert!(not_asked <= 40, "{not_asked} cases could not be asked");
    for kind in [
        "b64", "zlib", "json", "cookie", "unquote", "url_path", "route", "itsd",
    ] {
        assert!(
            seen.get(kind).copied().unwrap_or(0) >= 100,
            "too few {kind} cases: {seen:?}"
        );
    }
    assert_eq!(seen.get("json_nest"), Some(&4));
}
