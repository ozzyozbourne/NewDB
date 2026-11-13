//! Query-string parsing: `application/x-www-form-urlencoded` pairs, with each endpoint's
//! parameter names checked so a typo is a 400 instead of a silently ignored filter.

pub(crate) struct Params(Vec<(String, String)>);

impl Params {
    /// Parses `a=1&b=x%20y`. A repeated name keeps its last value.
    pub fn parse(query: Option<&str>, allowed: &[&str]) -> Result<Params, String> {
        let mut out: Vec<(String, String)> = Vec::new();
        for pair in query.unwrap_or("").split('&').filter(|p| !p.is_empty()) {
            let (k, v) = pair.split_once('=').unwrap_or((pair, ""));
            let (k, v) = (decode(k)?, decode(v)?);
            if !allowed.contains(&k.as_str()) {
                return Err(format!(
                    "unknown parameter {k:?} (expected one of {})",
                    allowed.join(", ")
                ));
            }
            out.retain(|(o, _)| *o != k);
            out.push((k, v));
        }
        Ok(Params(out))
    }

    /// The value, or `None` when absent or empty.
    pub fn str(&self, name: &str) -> Option<&str> {
        self.0
            .iter()
            .find(|(k, _)| k == name)
            .map(|(_, v)| v.as_str())
            .filter(|v| !v.is_empty())
    }

    pub fn parsed<T>(
        &self,
        name: &str,
        f: impl FnOnce(&str) -> Option<T>,
    ) -> Result<Option<T>, String> {
        match self.str(name) {
            None => Ok(None),
            Some(v) => f(v)
                .map(Some)
                .ok_or_else(|| format!("invalid value for {name}: {v:?}")),
        }
    }

    pub fn u64(&self, name: &str) -> Result<Option<u64>, String> {
        self.parsed(name, |v| v.parse().ok())
    }

    /// `true`/`false`, also `1`/`0`.
    pub fn bool(&self, name: &str) -> Result<Option<bool>, String> {
        self.parsed(name, |v| match v {
            "true" | "1" => Some(true),
            "false" | "0" => Some(false),
            _ => None,
        })
    }

    pub fn limit(&self, default: usize, max: usize) -> Result<usize, String> {
        Ok(self
            .parsed("limit", |v| v.parse::<usize>().ok())?
            .unwrap_or(default)
            .min(max))
    }
}

fn decode(s: &str) -> Result<String, String> {
    let b = s.as_bytes();
    let mut out = Vec::with_capacity(b.len());
    let mut i = 0;
    while i < b.len() {
        match b[i] {
            b'+' => out.push(b' '),
            b'%' => {
                let hex = b
                    .get(i + 1..i + 3)
                    .and_then(|h| std::str::from_utf8(h).ok())
                    .and_then(|h| u8::from_str_radix(h, 16).ok())
                    .ok_or_else(|| format!("bad percent-escape in {s:?}"))?;
                out.push(hex);
                i += 2;
            }
            c => out.push(c),
        }
        i += 1;
    }
    String::from_utf8(out).map_err(|_| format!("parameter is not UTF-8: {s:?}"))
}

/// A non-negative decimal with at most six fraction digits, scaled by 10^6 exactly
/// (`"0.25"` USD is 250000 micro-USD).
pub(crate) fn micro(text: &str) -> Option<u64> {
    let (int, frac) = text.split_once('.').unwrap_or((text, ""));
    if int.is_empty()
        || frac.len() > 6
        || !int.bytes().chain(frac.bytes()).all(|b| b.is_ascii_digit())
    {
        return None;
    }
    let frac: u64 = format!("{frac:0<6}").parse().ok()?;
    int.parse::<u64>()
        .ok()?
        .checked_mul(1_000_000)?
        .checked_add(frac)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_and_decodes() {
        let p = Params::parse(
            Some("q=refund%20order+12&limit=5&q=last&empty="),
            &["q", "limit", "empty"],
        )
        .unwrap();
        assert_eq!(p.str("q"), Some("last"));
        assert_eq!(p.limit(50, 3).unwrap(), 3);
        assert_eq!(p.str("empty"), None);
        let p = Params::parse(Some("q=refund%20order+12%C3%A9"), &["q"]).unwrap();
        assert_eq!(p.str("q"), Some("refund order 12é"));
        assert!(Params::parse(Some("stauts=ok"), &["status"]).is_err());
        assert!(Params::parse(Some("q=%zz"), &["q"]).is_err());
        assert!(Params::parse(Some("q=%ff"), &["q"]).is_err());
        assert!(
            Params::parse(Some("limit=-1"), &["limit"])
                .unwrap()
                .limit(50, 100)
                .is_err()
        );
    }

    #[test]
    fn micro_is_exact() {
        assert_eq!(micro("0.01"), Some(10_000));
        assert_eq!(micro("3"), Some(3_000_000));
        assert_eq!(micro("0.000001"), Some(1));
        assert_eq!(micro("0.0000001"), None);
        assert_eq!(micro(".5"), None);
        assert_eq!(micro("-1"), None);
    }
}
