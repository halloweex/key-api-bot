//! itsdangerous's `base64_decode`, which CPython's decoder makes more
//! forgiving than base64.
//!
//! `itsdangerous.encoding.base64_decode` pads its input with `=` to a
//! multiple of four — counting every byte, junk included — and hands it to
//! `base64.urlsafe_b64decode`, which maps `-` and `_` to `+` and `/` (and
//! leaves `+` and `/` as they are) and calls `binascii.a2b_base64` without
//! strict mode. That skips every byte outside the alphabet, and in CPython
//! 3.14 padding does not end the input: `YQ==YQ==` is three bytes, not one.
//! So a signature can carry junk and still verify in Python, and here.

const PAD: u8 = b'=';

fn sextet(b: u8) -> Option<u8> {
    match b {
        b'A'..=b'Z' => Some(b - b'A'),
        b'a'..=b'z' => Some(b - b'a' + 26),
        b'0'..=b'9' => Some(b - b'0' + 52),
        b'+' | b'-' => Some(62),
        b'/' | b'_' => Some(63),
        _ => None,
    }
}

/// `itsdangerous.encoding.base64_decode(data)`; `None` where it raises.
pub fn itsdangerous_b64decode(data: &[u8]) -> Option<Vec<u8>> {
    let pad = (4 - data.len() % 4) % 4;
    a2b_base64(data.iter().copied().chain(std::iter::repeat_n(PAD, pad)))
}

/// `binascii.a2b_base64(data, strict_mode=False)`.
fn a2b_base64(data: impl Iterator<Item = u8>) -> Option<Vec<u8>> {
    let mut out = Vec::new();
    let (mut quad, mut left, mut pads) = (0u8, 0u8, 0usize);
    for b in data {
        if b == PAD {
            pads += 1;
            continue;
        }
        let Some(v) = sextet(b) else { continue };
        pads = 0;
        match quad {
            0 => left = v,
            1 => {
                out.push((left << 2) | (v >> 4));
                left = v & 0x0f;
            }
            2 => {
                out.push((left << 4) | (v >> 2));
                left = v & 0x03;
            }
            _ => out.push((left << 6) | v),
        }
        quad = (quad + 1) % 4;
    }
    // A quad left open is an error unless the padding after it closes it.
    match quad {
        0 => Some(out),
        1 => None,
        q => (usize::from(q) + pads >= 4).then_some(out),
    }
}

#[cfg(test)]
mod tests {
    use super::itsdangerous_b64decode as decode;

    #[test]
    fn padding_does_not_end_the_input() {
        assert_eq!(decode(b"YQ==YQ=="), Some(b"a\x06\x10".to_vec()));
        assert_eq!(decode(b"YQ"), Some(b"a".to_vec()));
        // Padded by length, junk included: `Y!Q` gets one `=` and stays open.
        assert_eq!(decode(b"Y!Q"), None);
        assert_eq!(decode(b"Y!Q=="), Some(b"a".to_vec()));
        assert_eq!(decode(b"Y"), None);
    }
}
