//! `session_serializer.loads(cookie, max_age=SESSION_MAX_AGE)`: itsdangerous
//! 2.2's check of a dashboard session, step for step, for the one shape
//! `web/routes/auth.py` builds — `URLSafeTimedSerializer(SECRET_KEY)`.
//!
//! A cookie is `payload.timestamp.signature`. The signature is HMAC-SHA1 under
//! `sha1(salt + b"signer" + secret)` (itsdangerous's django-concat derivation,
//! salt `itsdangerous`) over everything before its dot; every part is
//! itsdangerous's lenient base64. The timestamp is at most eight bytes,
//! big-endian. A payload beginning with a dot is zlib-compressed — the
//! serializer compresses whenever that is shorter, which a real session
//! (names, a photo URL) usually is — and then it is JSON, `ensure_ascii=False`.
//!
//! How each failure lands matters as much as the bytes: `_resolve_session`
//! catches `BadSignature` (a forgery, an expired or future cookie, a broken
//! timestamp) and answers 401, and catches nothing else. A payload that is
//! not one, or a future timestamp past what `datetime` holds — both only
//! possible with the key — is the app's 500.

use hmac::{Hmac, Mac};
use sha1::{Digest, Sha1};

use super::pybase64::itsdangerous_b64decode;
use super::pyjson::{self, PyValue};

#[derive(Debug, PartialEq, Eq)]
pub enum LoadError {
    /// `BadSignature` and its subclasses: `_resolve_session` answers None.
    Refused,
    /// `BadPayload`: signed with the key, and not a payload. Uncaught.
    Payload,
    /// Anything else `loads` raises: `datetime.fromtimestamp` past 9999.
    Raised,
}

/// `datetime.fromtimestamp(ts, tz=timezone.utc)` holds up to 9999-12-31T23:59:59.
const MAX_DATETIME_TS: u64 = 253_402_300_799;

/// The HMAC key itsdangerous derives from the secret and the salt.
pub struct SessionKey([u8; 20]);

impl SessionKey {
    pub fn derive(secret: &[u8], salt: &[u8]) -> Self {
        let mut digest = Sha1::new();
        digest.update(salt);
        digest.update(b"signer");
        digest.update(secret);
        Self(digest.finalize().into())
    }
}

fn split_last_dot(bytes: &[u8]) -> Option<(&[u8], &[u8])> {
    let at = bytes.iter().rposition(|&b| b == b'.')?;
    Some((&bytes[..at], &bytes[at + 1..]))
}

fn genuine(key: &SessionKey, value: &[u8], signature: &[u8]) -> bool {
    let Some(signature) = itsdangerous_b64decode(signature) else {
        return false;
    };
    let mut mac = Hmac::<Sha1>::new_from_slice(&key.0).expect("HMAC takes any key length");
    mac.update(value);
    mac.verify_slice(&signature).is_ok()
}

/// `bytes_to_int`: `struct.unpack(">Q", b.rjust(8, b"\0"))`, so nine bytes fail.
fn bytes_to_int(bytes: &[u8]) -> Option<u64> {
    let mut be = [0u8; 8];
    be.get_mut(8usize.checked_sub(bytes.len())?..)?
        .copy_from_slice(bytes);
    Some(u64::from_be_bytes(be))
}

/// `URLSafeTimedSerializer.loads(cookie, max_age)` at the instant `now`.
///
/// `cookie` is the value as Starlette hands it over — the header decoded as
/// latin-1 — and is signed and checked as its UTF-8, as `want_bytes` does.
pub fn loads(cookie: &str, key: &SessionKey, now: i64, max_age: i64) -> Result<PyValue, LoadError> {
    let Some((value, signature)) = split_last_dot(cookie.as_bytes()) else {
        return Err(LoadError::Refused);
    };
    let signed = genuine(key, value, signature);
    // `TimestampSigner.unsign` splits the timestamp off whether or not the
    // signature held; with no dot left, both branches are a BadSignature.
    let Some((payload, stamp)) = split_last_dot(value) else {
        return Err(LoadError::Refused);
    };
    if !signed {
        return Err(LoadError::Refused);
    }
    let Some(ts) = itsdangerous_b64decode(stamp).and_then(|b| bytes_to_int(&b)) else {
        return Err(LoadError::Refused);
    };
    let age = i128::from(now) - i128::from(ts);
    if age > i128::from(max_age) {
        return Err(LoadError::Refused);
    }
    if age < 0 {
        // `SignatureExpired(..., date_signed=timestamp_to_datetime(ts))` —
        // which raises its own error first when `ts` is past year 9999.
        return Err(if ts > MAX_DATETIME_TS {
            LoadError::Raised
        } else {
            LoadError::Refused
        });
    }
    load_payload(payload)
}

/// `URLSafeSerializerMixin.load_payload`, then `json.loads` of its UTF-8.
fn load_payload(payload: &[u8]) -> Result<PyValue, LoadError> {
    let (compressed, payload) = match payload.strip_prefix(b".") {
        Some(rest) => (true, rest),
        None => (false, payload),
    };
    let mut json = itsdangerous_b64decode(payload).ok_or(LoadError::Payload)?;
    if compressed {
        json = zlib_decompress(&json).ok_or(LoadError::Payload)?;
    }
    let text = std::str::from_utf8(&json).map_err(|_| LoadError::Payload)?;
    pyjson::loads(text).ok_or(LoadError::Payload)
}

/// `zlib.decompress(data)`: a zlib header and checksum, and whatever follows
/// the end of the stream ignored.
pub fn zlib_decompress(data: &[u8]) -> Option<Vec<u8>> {
    miniz_oxide::inflate::decompress_to_vec_zlib(data).ok()
}
