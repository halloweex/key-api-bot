//! `json.loads` as CPython's C scanner reads it: what itsdangerous hands a
//! session cookie's payload to.
//!
//! Wider than JSON. `NaN`, `Infinity` and `-Infinity` are numbers; a
//! `\ud800` escape stays a lone surrogate (so strings are code points, not
//! `String`s); a repeated key keeps its first place and its last value; an
//! int has any number of digits up to the interpreter's 4300; a float rounds
//! correctly and overflows to infinity. Nesting ends where Python's C stack
//! does, which is no number: 20 000 levels load and 100 000 do not, and
//! `MAX_DEPTH` sits between. It is iterative, so depth costs heap, not stack.

use std::collections::HashMap;

/// A Python value as `json.loads` returns it.
#[derive(Clone, Debug, PartialEq)]
pub enum PyValue {
    None,
    Bool(bool),
    /// Decimal digits, as `str(int)` writes them.
    Int(String),
    Float(f64),
    /// Code points: a lone surrogate is one too.
    Str(Vec<u32>),
    List(Vec<PyValue>),
    /// In insertion order, as a `dict` keeps them.
    Dict(Vec<(Vec<u32>, PyValue)>),
}

/// Iterative, because the derived drop recurses: twenty thousand nested lists
/// — a payload Python loads without complaint — would overflow a thread's
/// stack on the way out, and abort the process with it.
impl Drop for PyValue {
    fn drop(&mut self) {
        let mut pending: Vec<PyValue> = Vec::new();
        let take = |value: &mut PyValue, pending: &mut Vec<PyValue>| match value {
            PyValue::List(items) => pending.append(items),
            PyValue::Dict(pairs) => pending.extend(pairs.drain(..).map(|(_, v)| v)),
            _ => {}
        };
        take(self, &mut pending);
        while let Some(mut value) = pending.pop() {
            take(&mut value, &mut pending);
        }
    }
}

impl PyValue {
    /// `bool(value)`.
    pub fn truthy(&self) -> bool {
        match self {
            PyValue::None => false,
            PyValue::Bool(b) => *b,
            PyValue::Int(digits) => digits != "0",
            PyValue::Float(f) => *f != 0.0,
            PyValue::Str(s) => !s.is_empty(),
            PyValue::List(items) => !items.is_empty(),
            PyValue::Dict(pairs) => !pairs.is_empty(),
        }
    }

    /// `value.get(key)` on a dict; `None` on a missing key or a non-dict.
    pub fn get(&self, key: &str) -> Option<&PyValue> {
        let PyValue::Dict(pairs) = self else {
            return None;
        };
        pairs
            .iter()
            .find(|(k, _)| k.iter().copied().eq(key.chars().map(u32::from)))
            .map(|(_, v)| v)
    }
}

/// Deeper than this is refused, as a `RecursionError` would be.
pub const MAX_DEPTH: usize = 50_000;
/// `sys.get_int_max_str_digits()`: a longer int is a `ValueError`.
const INT_MAX_DIGITS: usize = 4300;

/// `json.loads(text)`; `None` where it raises.
pub fn loads(text: &str) -> Option<PyValue> {
    let chars: Vec<char> = text.chars().collect();
    let mut parser = Parser { s: &chars, i: 0 };
    parser.ws();
    let value = parser.value()?;
    parser.ws();
    (parser.i == chars.len()).then_some(value)
}

enum Frame {
    List(Vec<PyValue>),
    Dict {
        pairs: Vec<(Vec<u32>, PyValue)>,
        index: HashMap<Vec<u32>, usize>,
        key: Vec<u32>,
    },
}

struct Parser<'a> {
    s: &'a [char],
    i: usize,
}

fn is_digit(c: Option<&char>) -> bool {
    matches!(c, Some('0'..='9'))
}

impl Parser<'_> {
    fn peek(&self) -> Option<char> {
        self.s.get(self.i).copied()
    }

    /// `[ \t\n\r]*` — JSON's whitespace, and nothing `str.isspace` adds.
    fn ws(&mut self) {
        while matches!(self.peek(), Some(' ' | '\t' | '\n' | '\r')) {
            self.i += 1;
        }
    }

    fn literal(&mut self, word: &str) -> bool {
        let n = word.chars().count();
        let fits = self
            .s
            .get(self.i..self.i + n)
            .is_some_and(|w| w.iter().copied().eq(word.chars()));
        if fits {
            self.i += n;
        }
        fits
    }

    fn value(&mut self) -> Option<PyValue> {
        let mut stack: Vec<Frame> = Vec::new();
        'value: loop {
            let mut done = match self.peek()? {
                '[' => {
                    self.i += 1;
                    self.ws();
                    if self.peek() == Some(']') {
                        self.i += 1;
                        PyValue::List(Vec::new())
                    } else {
                        if stack.len() >= MAX_DEPTH {
                            return None;
                        }
                        stack.push(Frame::List(Vec::new()));
                        continue 'value;
                    }
                }
                '{' => {
                    self.i += 1;
                    self.ws();
                    if self.peek() == Some('}') {
                        self.i += 1;
                        PyValue::Dict(Vec::new())
                    } else {
                        if stack.len() >= MAX_DEPTH {
                            return None;
                        }
                        let key = self.key()?;
                        stack.push(Frame::Dict {
                            pairs: Vec::new(),
                            index: HashMap::new(),
                            key,
                        });
                        continue 'value;
                    }
                }
                _ => self.scalar()?,
            };
            loop {
                let Some(frame) = stack.last_mut() else {
                    return Some(done);
                };
                match frame {
                    Frame::List(items) => {
                        items.push(done);
                        self.ws();
                        match self.peek() {
                            Some(',') => {
                                self.i += 1;
                                self.ws();
                                continue 'value;
                            }
                            Some(']') => {
                                self.i += 1;
                                let Some(Frame::List(items)) = stack.pop() else {
                                    unreachable!("the frame just matched");
                                };
                                done = PyValue::List(items);
                            }
                            _ => return None,
                        }
                    }
                    Frame::Dict { pairs, index, key } => {
                        let key_now = std::mem::take(key);
                        match index.get(&key_now) {
                            Some(&at) => pairs[at].1 = done,
                            None => {
                                index.insert(key_now.clone(), pairs.len());
                                pairs.push((key_now, done));
                            }
                        }
                        self.ws();
                        match self.peek() {
                            Some(',') => {
                                self.i += 1;
                                self.ws();
                                *key = self.key()?;
                                continue 'value;
                            }
                            Some('}') => {
                                self.i += 1;
                                let Some(Frame::Dict { pairs, .. }) = stack.pop() else {
                                    unreachable!("the frame just matched");
                                };
                                done = PyValue::Dict(pairs);
                            }
                            _ => return None,
                        }
                    }
                }
            }
        }
    }

    /// A key, its colon, and the whitespace either side.
    fn key(&mut self) -> Option<Vec<u32>> {
        if self.peek()? != '"' {
            return None;
        }
        self.i += 1;
        let key = self.string()?;
        self.ws();
        if self.peek()? != ':' {
            return None;
        }
        self.i += 1;
        self.ws();
        Some(key)
    }

    fn scalar(&mut self) -> Option<PyValue> {
        match self.peek()? {
            '"' => {
                self.i += 1;
                self.string().map(PyValue::Str)
            }
            'n' if self.literal("null") => Some(PyValue::None),
            't' if self.literal("true") => Some(PyValue::Bool(true)),
            'f' if self.literal("false") => Some(PyValue::Bool(false)),
            'N' if self.literal("NaN") => Some(PyValue::Float(f64::NAN)),
            'I' if self.literal("Infinity") => Some(PyValue::Float(f64::INFINITY)),
            '-' if self.literal("-Infinity") => Some(PyValue::Float(f64::NEG_INFINITY)),
            '-' | '0'..='9' => self.number(),
            _ => None,
        }
    }

    /// `_match_number_unicode`: an int, or a float when a fraction or an
    /// exponent follows — and an `e` without digits is left unread.
    fn number(&mut self) -> Option<PyValue> {
        let start = self.i;
        if self.peek() == Some('-') {
            self.i += 1;
        }
        match self.peek()? {
            '1'..='9' => {
                self.i += 1;
                while is_digit(self.s.get(self.i)) {
                    self.i += 1;
                }
            }
            '0' => self.i += 1,
            _ => return None,
        }
        let mut float = false;
        if self.peek() == Some('.') && is_digit(self.s.get(self.i + 1)) {
            float = true;
            self.i += 2;
            while is_digit(self.s.get(self.i)) {
                self.i += 1;
            }
        }
        if matches!(self.peek(), Some('e' | 'E')) {
            let mark = self.i;
            self.i += 1;
            if matches!(self.peek(), Some('-' | '+')) {
                self.i += 1;
            }
            while is_digit(self.s.get(self.i)) {
                self.i += 1;
            }
            if is_digit(self.s.get(self.i - 1)) {
                float = true;
            } else {
                self.i = mark;
            }
        }
        let text: String = self.s[start..self.i].iter().collect();
        if float {
            return text.parse().ok().map(PyValue::Float);
        }
        let digits = text.strip_prefix('-').unwrap_or(&text);
        if digits.len() > INT_MAX_DIGITS {
            return None;
        }
        Some(PyValue::Int(if digits == "0" {
            digits.into()
        } else {
            text
        }))
    }

    fn hex4(&mut self) -> Option<u32> {
        let mut c = 0;
        for _ in 0..4 {
            c = (c << 4) | self.peek()?.to_digit(16)?;
            self.i += 1;
        }
        Some(c)
    }

    /// `scanstring_unicode`, from just after the opening quote.
    fn string(&mut self) -> Option<Vec<u32>> {
        let mut out = Vec::new();
        loop {
            let c = self.peek()?;
            self.i += 1;
            match c {
                '"' => return Some(out),
                '\\' => {
                    let escape = self.peek()?;
                    self.i += 1;
                    let decoded = match escape {
                        '"' | '\\' | '/' => u32::from(escape),
                        'b' => 0x08,
                        'f' => 0x0c,
                        'n' => 0x0a,
                        'r' => 0x0d,
                        't' => 0x09,
                        'u' => self.unicode_escape()?,
                        _ => return None,
                    };
                    out.push(decoded);
                }
                c if u32::from(c) <= 0x1f => return None,
                c => out.push(u32::from(c)),
            }
        }
    }

    /// `\uXXXX`, joined with a following `\uXXXX` only when the pair is a
    /// high and a low surrogate; otherwise each stands alone.
    fn unicode_escape(&mut self) -> Option<u32> {
        let c = self.hex4()?;
        let high = (0xD800..0xDC00).contains(&c);
        let room = self.i + 6 < self.s.len();
        if high && room && self.s[self.i] == '\\' && self.s[self.i + 1] == 'u' {
            let mark = self.i;
            self.i += 2;
            let low = self.hex4()?;
            if (0xDC00..0xE000).contains(&low) {
                return Some(0x10000 + ((c - 0xD800) << 10) + (low - 0xDC00));
            }
            self.i = mark;
        }
        Some(c)
    }
}

#[cfg(test)]
mod tests {
    use super::{PyValue, loads};

    #[test]
    fn what_python_takes_and_json_does_not() {
        assert!(matches!(loads("NaN"), Some(PyValue::Float(f)) if f.is_nan()));
        assert_eq!(loads("-Infinity"), Some(PyValue::Float(f64::NEG_INFINITY)));
        assert_eq!(loads(r#""\ud800""#), Some(PyValue::Str(vec![0xD800])));
        assert_eq!(loads(r#""😀""#), Some(PyValue::Str(vec![0x1F600])));
        assert_eq!(loads("1E400"), Some(PyValue::Float(f64::INFINITY)));
        assert_eq!(loads("-0"), Some(PyValue::Int("0".into())));
        let dict = loads(r#"{"a":1,"b":2,"a":3}"#).unwrap();
        assert_eq!(dict.get("a"), Some(&PyValue::Int("3".into())));
        let PyValue::Dict(pairs) = &dict else {
            panic!()
        };
        assert_eq!(pairs[0].0, vec![u32::from('a')]);
    }

    #[test]
    fn deep_nesting_loads_and_drops_without_the_stack() {
        let depth = 20_000;
        let text = format!("{}{}", "[".repeat(depth), "]".repeat(depth));
        assert!(loads(&text).is_some());
        let too_deep = format!("{}{}", "[".repeat(100_000), "]".repeat(100_000));
        assert!(loads(&too_deep).is_none());
    }

    #[test]
    fn what_python_refuses() {
        for text in [
            "",
            "[1,]",
            "{\"a\":1,}",
            "01",
            "1.",
            "[1]\x0c",
            "\u{feff}{}",
            "\"a\nb\"",
        ] {
            assert_eq!(loads(text), None, "{text:?}");
        }
        assert_eq!(loads(&"1".repeat(4301)), None);
    }
}
