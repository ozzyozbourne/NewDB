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

/// Calls `f` with each token of `text`, in order, duplicates included.
pub fn for_each_token(text: &[u8], mut f: impl FnMut(&[u8])) {
    let mut buf: Vec<u8> = Vec::with_capacity(MAX_TOKEN_LEN);
    let mut too_long = false;
    let mut flush = |buf: &mut Vec<u8>, too_long: &mut bool| {
        if !buf.is_empty() && !*too_long {
            f(buf);
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
}
