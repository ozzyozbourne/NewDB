//! The tokenizer. It defines what full-text search matches, so the oracle and the `fts`
//! layout must both use it (or produce exactly the same tokens).
//!
//! Rules, applied to raw payload bytes (usually JSON):
//! - A token is a maximal run of ASCII letters, ASCII digits and bytes >= 0x80 (so UTF-8
//!   text stays inside tokens). Everything else separates tokens.
//! - ASCII letters are lowercased. Other bytes are kept as they are.
//! - JSON escapes separate tokens and are skipped: `\n \t \r \b \f \" \\ \/` (2 bytes)
//!   and `\uXXXX` (6 bytes), so `"a\nb"` gives `a`, `b`, not `a`, `nb`.
//! - Tokens longer than [`MAX_TOKEN_LEN`] bytes are dropped entirely.

pub const MAX_TOKEN_LEN: usize = 64;

/// Bytes that belong to tokens: ASCII letters, ASCII digits and every byte >= 0x80.
const TOKEN_BYTE: [bool; 256] = {
    let mut t = [false; 256];
    let mut i = 0;
    while i < 256 {
        let b = i as u8;
        t[i] = b.is_ascii_alphanumeric() || b >= 0x80;
        i += 1;
    }
    t
};

/// Calls `f` with each token of `text`, in order, duplicates included.
///
/// Tokens without ASCII uppercase letters are passed as slices of `text`; only tokens that
/// need lowercasing are copied (into a stack buffer).
pub fn for_each_token(text: &[u8], mut f: impl FnMut(&[u8])) {
    let mut buf = [0u8; MAX_TOKEN_LEN];
    let n = text.len();
    let mut i = 0;
    while i < n {
        let b = text[i];
        if TOKEN_BYTE[b as usize] {
            let start = i;
            let mut upper = false;
            while i < n && TOKEN_BYTE[text[i] as usize] {
                upper |= text[i].is_ascii_uppercase();
                i += 1;
            }
            let tok = &text[start..i];
            if tok.len() > MAX_TOKEN_LEN {
                continue;
            }
            if upper {
                let out = &mut buf[..tok.len()];
                out.copy_from_slice(tok);
                out.make_ascii_lowercase();
                f(out);
            } else {
                f(tok);
            }
        } else if b == b'\\' && i + 1 < n {
            i += match text[i + 1] {
                b'u' => 6,
                b'n' | b't' | b'r' | b'b' | b'f' | b'"' | b'\\' | b'/' => 2,
                _ => 1,
            };
        } else {
            i += 1;
        }
    }
}

/// The distinct tokens of `text`, sorted.
pub fn tokens(text: &[u8]) -> Vec<Vec<u8>> {
    let mut out = Vec::new();
    for_each_token(text, |t| out.push(t.to_vec()));
    out.sort_unstable();
    out.dedup();
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn toks(s: &str) -> Vec<String> {
        let mut v = Vec::new();
        for_each_token(s.as_bytes(), |t| {
            v.push(String::from_utf8(t.to_vec()).unwrap())
        });
        v
    }

    #[test]
    fn splits_lowercases_and_skips_json_escapes() {
        // `\u00e9` escapes are skipped, so the escaped word "été" leaves only "t".
        assert_eq!(
            toks(r#"{"role":"user","content":"Refund\nPolicy for order #42 \u00e9t\u00e9"}"#),
            [
                "role", "user", "content", "refund", "policy", "for", "order", "42", "t"
            ]
        );
        assert_eq!(toks(r#"a\\nb"#), ["a", "nb"]);
        // Non-ASCII bytes stay inside tokens and are not lowercased.
        assert_eq!(toks("Café ÜBER"), ["café", "Über"]);
    }

    #[test]
    fn drops_overlong_tokens_only() {
        let long = "x".repeat(MAX_TOKEN_LEN + 1);
        let exact = "y".repeat(MAX_TOKEN_LEN);
        assert_eq!(
            toks(&format!("a {long} {exact} b")),
            ["a".to_string(), exact, "b".into()]
        );
    }

    #[test]
    fn tokens_are_sorted_and_distinct() {
        assert_eq!(tokens(b"b a B a"), [b"a".to_vec(), b"b".to_vec()]);
    }

    /// The original byte-at-a-time implementation, kept as the reference.
    fn reference(text: &[u8]) -> Vec<Vec<u8>> {
        let mut out = Vec::new();
        let mut buf: Vec<u8> = Vec::new();
        let mut too_long = false;
        let mut flush = |buf: &mut Vec<u8>, too_long: &mut bool| {
            if !buf.is_empty() && !*too_long {
                out.push(buf.clone());
            }
            buf.clear();
            *too_long = false;
        };
        let mut i = 0;
        while i < text.len() {
            let b = text[i];
            if b == b'\\' && i + 1 < text.len() {
                flush(&mut buf, &mut too_long);
                i += match text[i + 1] {
                    b'u' => 6,
                    b'n' | b't' | b'r' | b'b' | b'f' | b'"' | b'\\' | b'/' => 2,
                    _ => 1,
                };
                continue;
            }
            if b.is_ascii_alphanumeric() || b >= 0x80 {
                if buf.len() < MAX_TOKEN_LEN {
                    buf.push(b.to_ascii_lowercase());
                } else {
                    too_long = true;
                }
            } else {
                flush(&mut buf, &mut too_long);
            }
            i += 1;
        }
        flush(&mut buf, &mut too_long);
        out
    }

    #[test]
    fn matches_the_reference_on_random_inputs() {
        // Bytes chosen to hit every branch: case, digits, UTF-8, escapes, separators, and
        // long runs around MAX_TOKEN_LEN.
        const ALPHABET: &[u8] = b"aZu9n\\\\\\\\\" /tX\xc3\xa9\x80 ,:{}";
        let mut x: u64 = 0x2545_f491_4f6c_dd1d;
        let mut next = || {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            x
        };
        for case in 0..20_000 {
            let len = (next() % 200) as usize;
            let mut text = Vec::with_capacity(len);
            while text.len() < len {
                if next() % 16 == 0 {
                    let run = 60 + (next() % 8) as usize;
                    let c = if next() % 2 == 0 { b'a' } else { b'Q' };
                    text.extend(std::iter::repeat_n(c, run));
                } else {
                    text.push(ALPHABET[(next() % ALPHABET.len() as u64) as usize]);
                }
            }
            let mut got = Vec::new();
            for_each_token(&text, |t| got.push(t.to_vec()));
            assert_eq!(
                got,
                reference(&text),
                "case {case}: {:?}",
                String::from_utf8_lossy(&text)
            );
        }
    }
}
