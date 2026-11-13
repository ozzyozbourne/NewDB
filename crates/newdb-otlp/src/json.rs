//! A small JSON reader and writer, enough for OTLP/JSON, the data files and the payloads this
//! crate builds. Objects keep their key order and numbers keep their text, so writing a
//! parsed value back changes only whitespace and string escapes.

#[derive(Clone, Debug, PartialEq)]
pub enum Json {
    Null,
    Bool(bool),
    /// The number as written, checked against the JSON number grammar.
    Number(String),
    String(String),
    Array(Vec<Json>),
    /// Keys in input order. Duplicate keys are kept; [`Json::get`] returns the last.
    Object(Vec<(String, Json)>),
}

/// Nesting deeper than this is rejected, so hostile input cannot overflow the stack.
pub const MAX_DEPTH: usize = 128;

impl Json {
    /// Parses one JSON value; only whitespace may follow it.
    pub fn parse(bytes: &[u8]) -> Result<Json, String> {
        let text = std::str::from_utf8(bytes).map_err(|e| format!("not UTF-8: {e}"))?;
        let mut p = Parser {
            s: text.as_bytes(),
            text,
            i: 0,
        };
        let v = p.value(0)?;
        p.ws();
        if p.i != p.s.len() {
            return Err(p.err("trailing characters"));
        }
        Ok(v)
    }

    pub fn get(&self, key: &str) -> Option<&Json> {
        match self {
            Json::Object(m) => m.iter().rev().find(|(k, _)| k == key).map(|(_, v)| v),
            _ => None,
        }
    }

    pub fn as_str(&self) -> Option<&str> {
        match self {
            Json::String(s) => Some(s),
            _ => None,
        }
    }

    pub fn as_array(&self) -> Option<&[Json]> {
        match self {
            Json::Array(a) => Some(a),
            _ => None,
        }
    }

    pub fn as_object(&self) -> Option<&[(String, Json)]> {
        match self {
            Json::Object(m) => Some(m),
            _ => None,
        }
    }

    pub fn as_bool(&self) -> Option<bool> {
        match self {
            Json::Bool(b) => Some(*b),
            _ => None,
        }
    }

    /// An integer-valued number (no fraction or exponent) that fits in `i64`.
    pub fn as_i64(&self) -> Option<i64> {
        match self {
            Json::Number(n) => n.parse().ok(),
            _ => None,
        }
    }

    pub fn as_u64(&self) -> Option<u64> {
        match self {
            Json::Number(n) => n.parse().ok(),
            _ => None,
        }
    }

    pub fn as_f64(&self) -> Option<f64> {
        match self {
            Json::Number(n) => n.parse().ok(),
            _ => None,
        }
    }

    /// A number from an `f64` (shortest round-trip text, as `{:?}` prints it); NaN and
    /// infinities have no JSON form and become `null`.
    pub fn from_f64(f: f64) -> Json {
        if f.is_finite() {
            Json::Number(format!("{f:?}"))
        } else {
            Json::Null
        }
    }

    /// Appends compact JSON.
    pub fn write(&self, out: &mut Vec<u8>) {
        match self {
            Json::Null => out.extend_from_slice(b"null"),
            Json::Bool(true) => out.extend_from_slice(b"true"),
            Json::Bool(false) => out.extend_from_slice(b"false"),
            Json::Number(n) => out.extend_from_slice(n.as_bytes()),
            Json::String(s) => write_str(out, s),
            Json::Array(a) => {
                out.push(b'[');
                for (i, v) in a.iter().enumerate() {
                    if i > 0 {
                        out.push(b',');
                    }
                    v.write(out);
                }
                out.push(b']');
            }
            Json::Object(m) => {
                out.push(b'{');
                for (i, (k, v)) in m.iter().enumerate() {
                    if i > 0 {
                        out.push(b',');
                    }
                    write_str(out, k);
                    out.push(b':');
                    v.write(out);
                }
                out.push(b'}');
            }
        }
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        let mut out = Vec::new();
        self.write(&mut out);
        out
    }
}

/// Inserts or replaces `key` in an object's entries.
pub fn insert(obj: &mut Vec<(String, Json)>, key: String, value: Json) {
    match obj.iter_mut().find(|(k, _)| *k == key) {
        Some(slot) => slot.1 = value,
        None => obj.push((key, value)),
    }
}

/// Checks that `v` is an object whose keys are all in `allowed` (or start with `_`, for
/// comments), so a typo in a data file is an error instead of a silently ignored setting.
pub(crate) fn check_keys(v: &Json, allowed: &[&str]) -> Result<(), String> {
    let obj = v.as_object().ok_or("expected a JSON object")?;
    match obj
        .iter()
        .find(|(k, _)| !k.starts_with('_') && !allowed.contains(&k.as_str()))
    {
        Some((k, _)) => Err(format!("unknown key {k:?} (expected one of {allowed:?})")),
        None => Ok(()),
    }
}

/// Appends `s` as a JSON string literal.
pub fn write_str(out: &mut Vec<u8>, s: &str) {
    out.push(b'"');
    let b = s.as_bytes();
    let mut start = 0;
    for (i, &c) in b.iter().enumerate() {
        let esc: &[u8] = match c {
            b'"' => b"\\\"",
            b'\\' => b"\\\\",
            b'\n' => b"\\n",
            b'\r' => b"\\r",
            b'\t' => b"\\t",
            0x08 => b"\\b",
            0x0c => b"\\f",
            0..=0x1f => b"",
            _ => continue,
        };
        out.extend_from_slice(&b[start..i]);
        if esc.is_empty() {
            const HEX: &[u8; 16] = b"0123456789abcdef";
            out.extend_from_slice(b"\\u00");
            out.push(HEX[(c >> 4) as usize]);
            out.push(HEX[(c & 15) as usize]);
        } else {
            out.extend_from_slice(esc);
        }
        start = i + 1;
    }
    out.extend_from_slice(&b[start..]);
    out.push(b'"');
}

struct Parser<'a> {
    s: &'a [u8],
    text: &'a str,
    i: usize,
}

impl Parser<'_> {
    fn err(&self, what: &str) -> String {
        format!("JSON: {what} at byte {}", self.i)
    }

    fn ws(&mut self) {
        while let Some(b' ' | b'\t' | b'\n' | b'\r') = self.s.get(self.i) {
            self.i += 1;
        }
    }

    fn eat(&mut self, c: u8) -> bool {
        self.ws();
        if self.s.get(self.i) == Some(&c) {
            self.i += 1;
            true
        } else {
            false
        }
    }

    fn lit(&mut self, word: &str, v: Json) -> Result<Json, String> {
        if self.s[self.i..].starts_with(word.as_bytes()) {
            self.i += word.len();
            Ok(v)
        } else {
            Err(self.err("invalid literal"))
        }
    }

    fn value(&mut self, depth: usize) -> Result<Json, String> {
        if depth > MAX_DEPTH {
            return Err(self.err("nesting too deep"));
        }
        self.ws();
        match self.s.get(self.i) {
            None => Err(self.err("unexpected end")),
            Some(b'n') => self.lit("null", Json::Null),
            Some(b't') => self.lit("true", Json::Bool(true)),
            Some(b'f') => self.lit("false", Json::Bool(false)),
            Some(b'"') => Ok(Json::String(self.string()?)),
            Some(b'[') => {
                self.i += 1;
                let mut a = Vec::new();
                if self.eat(b']') {
                    return Ok(Json::Array(a));
                }
                loop {
                    a.push(self.value(depth + 1)?);
                    if self.eat(b']') {
                        return Ok(Json::Array(a));
                    }
                    if !self.eat(b',') {
                        return Err(self.err("expected ',' or ']'"));
                    }
                }
            }
            Some(b'{') => {
                self.i += 1;
                let mut m = Vec::new();
                if self.eat(b'}') {
                    return Ok(Json::Object(m));
                }
                loop {
                    self.ws();
                    if self.s.get(self.i) != Some(&b'"') {
                        return Err(self.err("expected a key"));
                    }
                    let k = self.string()?;
                    if !self.eat(b':') {
                        return Err(self.err("expected ':'"));
                    }
                    m.push((k, self.value(depth + 1)?));
                    if self.eat(b'}') {
                        return Ok(Json::Object(m));
                    }
                    if !self.eat(b',') {
                        return Err(self.err("expected ',' or '}'"));
                    }
                }
            }
            Some(b'-' | b'0'..=b'9') => self.number(),
            Some(_) => Err(self.err("unexpected character")),
        }
    }

    fn digits(&mut self) -> usize {
        let start = self.i;
        while self.s.get(self.i).is_some_and(u8::is_ascii_digit) {
            self.i += 1;
        }
        self.i - start
    }

    fn number(&mut self) -> Result<Json, String> {
        let start = self.i;
        if self.s[self.i] == b'-' {
            self.i += 1;
        }
        match self.s.get(self.i) {
            Some(b'0') => self.i += 1,
            Some(b'1'..=b'9') => {
                self.digits();
            }
            _ => return Err(self.err("invalid number")),
        }
        if self.s.get(self.i) == Some(&b'.') {
            self.i += 1;
            if self.digits() == 0 {
                return Err(self.err("invalid number"));
            }
        }
        if let Some(b'e' | b'E') = self.s.get(self.i) {
            self.i += 1;
            if let Some(b'+' | b'-') = self.s.get(self.i) {
                self.i += 1;
            }
            if self.digits() == 0 {
                return Err(self.err("invalid number"));
            }
        }
        Ok(Json::Number(self.text[start..self.i].to_string()))
    }

    fn hex4(&mut self) -> Result<u32, String> {
        let h = self
            .s
            .get(self.i..self.i + 4)
            .and_then(|h| std::str::from_utf8(h).ok())
            .and_then(|h| u32::from_str_radix(h, 16).ok())
            .ok_or_else(|| self.err("invalid \\u escape"))?;
        self.i += 4;
        Ok(h)
    }

    /// Reads a string starting at its opening quote. Lone surrogates become U+FFFD.
    fn string(&mut self) -> Result<String, String> {
        self.i += 1;
        let mut out = String::new();
        loop {
            let start = self.i;
            while let Some(&c) = self.s.get(self.i) {
                if c == b'"' || c == b'\\' || c < 0x20 {
                    break;
                }
                self.i += 1;
            }
            // Only ASCII bytes stop the scan, so the slice is on char boundaries.
            out.push_str(&self.text[start..self.i]);
            match self.s.get(self.i) {
                Some(b'"') => {
                    self.i += 1;
                    return Ok(out);
                }
                Some(b'\\') => {
                    self.i += 1;
                    let Some(&e) = self.s.get(self.i) else {
                        return Err(self.err("unexpected end"));
                    };
                    self.i += 1;
                    match e {
                        b'"' => out.push('"'),
                        b'\\' => out.push('\\'),
                        b'/' => out.push('/'),
                        b'b' => out.push('\u{8}'),
                        b'f' => out.push('\u{c}'),
                        b'n' => out.push('\n'),
                        b'r' => out.push('\r'),
                        b't' => out.push('\t'),
                        b'u' => {
                            let hi = self.hex4()?;
                            let c = if (0xd800..0xdc00).contains(&hi)
                                && self.s[self.i..].starts_with(b"\\u")
                            {
                                let save = self.i;
                                self.i += 2;
                                let lo = self.hex4()?;
                                if (0xdc00..0xe000).contains(&lo) {
                                    char::from_u32(0x10000 + ((hi - 0xd800) << 10) + (lo - 0xdc00))
                                } else {
                                    self.i = save;
                                    None
                                }
                            } else {
                                char::from_u32(hi)
                            };
                            out.push(c.unwrap_or('\u{fffd}'));
                        }
                        _ => return Err(self.err("invalid escape")),
                    }
                }
                Some(_) => return Err(self.err("control character in string")),
                None => return Err(self.err("unterminated string")),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rt(s: &str) -> String {
        String::from_utf8(Json::parse(s.as_bytes()).unwrap().to_bytes()).unwrap()
    }

    #[test]
    fn round_trips_keep_order_and_number_text() {
        assert_eq!(
            rt(r#" { "b" : 1.50e+3 , "a" : [true,false,null,-0,{}] , "c":[] } "#),
            r#"{"b":1.50e+3,"a":[true,false,null,-0,{}],"c":[]}"#
        );
    }

    #[test]
    fn string_escapes() {
        let v = Json::parse(br#""a\"b\\c\/d\n\t\u00e9\ud83d\ude00\ud800x\u0001""#).unwrap();
        assert_eq!(v.as_str().unwrap(), "a\"b\\c/d\n\té😀\u{fffd}x\u{1}");
        assert_eq!(
            String::from_utf8(v.to_bytes()).unwrap(),
            r#""a\"b\\c/d\n\té😀�x\u0001""#
        );
    }

    #[test]
    fn rejects_malformed() {
        for bad in [
            "", "{", "[1,]", "{\"a\"}", "01", "1.", "-", "1e", "\"\n\"", "tru", "{} x", "\"\\x\"",
            "[1 2]", "{1:2}",
        ] {
            assert!(Json::parse(bad.as_bytes()).is_err(), "accepted {bad:?}");
        }
        assert!(Json::parse(b"\"\xff\"").is_err());
        let deep = "[".repeat(MAX_DEPTH + 2) + &"]".repeat(MAX_DEPTH + 2);
        assert!(Json::parse(deep.as_bytes()).is_err());
        let ok = "[".repeat(MAX_DEPTH) + &"]".repeat(MAX_DEPTH);
        assert!(Json::parse(ok.as_bytes()).is_ok());
    }

    #[test]
    fn numbers_and_lookup() {
        let v = Json::parse(br#"{"a":1,"a":"18446744073709551615","n":-3,"f":0.25}"#).unwrap();
        assert_eq!(
            v.get("a").and_then(Json::as_str),
            Some("18446744073709551615")
        );
        assert_eq!(v.get("n").and_then(Json::as_i64), Some(-3));
        assert_eq!(v.get("n").and_then(Json::as_u64), None);
        assert_eq!(v.get("f").and_then(Json::as_f64), Some(0.25));
        assert_eq!(Json::from_f64(0.2).to_bytes(), b"0.2");
        assert_eq!(Json::from_f64(3.0).to_bytes(), b"3.0");
        assert_eq!(Json::from_f64(f64::NAN), Json::Null);
    }
}
