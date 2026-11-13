//! The model pricing table (`data/pricing.json`), ported from Forge Trace DB's
//! `forge-model/src/pricing.rs`. Prices are USD per million tokens, which is the same number
//! as micro-USD per token. They are parsed exactly from their decimal text (at most six
//! decimals) into pico-USD per token, so costs are integer arithmetic and round down exactly.

use crate::json::Json;

pub(crate) struct Pricing {
    strip_prefixes: Vec<String>,
    /// (lowercased model prefix, input, output), prices in pico-USD per token.
    models: Vec<(String, u64, u64)>,
}

/// Parses a non-negative decimal with at most six fraction digits, scaled by 10^6.
fn micro(text: &str) -> Option<u64> {
    let (int, frac) = text.split_once('.').unwrap_or((text, ""));
    if int.is_empty()
        || frac.len() > 6
        || !(int.bytes().chain(frac.bytes())).all(|b| b.is_ascii_digit())
    {
        return None;
    }
    let mut f = frac.to_string();
    f.extend(std::iter::repeat_n('0', 6 - frac.len()));
    int.parse::<u64>()
        .ok()?
        .checked_mul(1_000_000)?
        .checked_add(f.parse().ok()?)
}

impl Pricing {
    pub fn parse(bytes: &[u8]) -> Result<Pricing, String> {
        let v = Json::parse(bytes)?;
        crate::json::check_keys(&v, &["strip_prefixes", "models"])?;
        let strip_prefixes = match v.get("strip_prefixes") {
            None => Vec::new(),
            Some(Json::Array(a)) => a
                .iter()
                .map(|s| s.as_str().map(str::to_ascii_lowercase))
                .collect::<Option<_>>()
                .ok_or("`strip_prefixes` must hold strings")?,
            Some(_) => return Err("`strip_prefixes` must be a list".into()),
        };
        let models = v
            .get("models")
            .and_then(Json::as_object)
            .ok_or("pricing needs a `models` object")?;
        let price = |model: &str, p: &Json, side: &str| -> Result<u64, String> {
            match p.get(side) {
                Some(Json::Number(n)) => micro(n),
                _ => None,
            }
            .ok_or_else(|| {
                format!("model {model:?}: `{side}` must be a non-negative number with at most 6 decimals")
            })
        };
        let models = models
            .iter()
            .map(|(m, p)| {
                Ok((
                    m.to_ascii_lowercase(),
                    price(m, p, "input")?,
                    price(m, p, "output")?,
                ))
            })
            .collect::<Result<_, String>>()?;
        Ok(Pricing {
            strip_prefixes,
            models,
        })
    }

    /// (input, output) in pico-USD per token for the longest matching model prefix.
    fn price(&self, model: &str) -> Option<(u64, u64)> {
        let lower = model.to_ascii_lowercase();
        let mut m = lower.rsplit('/').next().unwrap_or(&lower);
        for p in &self.strip_prefixes {
            m = m.strip_prefix(p.as_str()).unwrap_or(m);
        }
        self.models
            .iter()
            .filter(|(k, _, _)| m.starts_with(k.as_str()))
            .max_by_key(|(k, _, _)| k.len())
            .map(|(_, i, o)| (*i, *o))
    }

    /// Cost in micro-USD, rounded down. `None` without a priced model or any token count.
    pub fn cost_micro_usd(
        &self,
        model: Option<&str>,
        prompt: Option<u32>,
        completion: Option<u32>,
    ) -> Option<u64> {
        let (i, o) = self.price(model?)?;
        if prompt.is_none() && completion.is_none() {
            return None;
        }
        let pico =
            prompt.unwrap_or(0) as u128 * i as u128 + completion.unwrap_or(0) as u128 * o as u128;
        Some(u64::try_from(pico / 1_000_000).unwrap_or(u64::MAX))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn builtin() -> Pricing {
        Pricing::parse(include_bytes!("../data/pricing.json")).unwrap()
    }

    #[test]
    fn longest_prefix_and_provider_prefixes() {
        let p = builtin();
        assert_eq!(p.price("gpt-4o-mini-2024-07-18"), Some((150_000, 600_000)));
        assert_eq!(p.price("GPT-4o-2024-08-06"), Some((2_500_000, 10_000_000)));
        assert_eq!(p.price("openai/gpt-4o"), Some((2_500_000, 10_000_000)));
        assert_eq!(
            p.price("anthropic.claude-3-haiku-20240307-v1:0"),
            Some((250_000, 1_250_000))
        );
        assert_eq!(p.price("my-local-llama"), None);
    }

    #[test]
    fn costs_are_exact_and_round_down() {
        let p = builtin();
        // 1200 × 3.00 + 5 × 15.00 micro-USD.
        assert_eq!(
            p.cost_micro_usd(Some("claude-sonnet-4-20250514"), Some(1200), Some(5)),
            Some(3675)
        );
        // 0.075 × 13 = 0.975 rounds down to 0; 0.075 × 14 = 1.05 to 1. Float math gets
        // 0.075 * 1e6 * 14 / 1e6 = 1.0499999 right but similar sums wrong; integers do not.
        assert_eq!(
            p.cost_micro_usd(Some("gemini-1.5-flash"), Some(13), None),
            Some(0)
        );
        assert_eq!(
            p.cost_micro_usd(Some("gemini-1.5-flash"), Some(14), None),
            Some(1)
        );
        assert_eq!(
            p.cost_micro_usd(Some("gpt-4o"), Some(1_000_000), Some(1_000_000)),
            Some(12_500_000)
        );
        assert_eq!(p.cost_micro_usd(Some("gpt-4o"), None, None), None);
        assert_eq!(p.cost_micro_usd(Some("unknown"), Some(5), None), None);
        assert_eq!(p.cost_micro_usd(None, Some(5), None), None);
        let max = p
            .cost_micro_usd(Some("o1"), Some(u32::MAX), Some(u32::MAX))
            .unwrap();
        assert_eq!(max, (u32::MAX as u64) * 75);
    }

    #[test]
    fn parses_decimals_exactly() {
        assert_eq!(micro("0.075"), Some(75_000));
        assert_eq!(micro("10"), Some(10_000_000));
        assert_eq!(micro("0.0000001"), None);
        assert_eq!(micro("-1"), None);
        assert_eq!(micro("1e3"), None);
        assert!(Pricing::parse(br#"{"models":{"x":{"input":"1","output":1}}}"#).is_err());
        assert!(Pricing::parse(br#"{}"#).is_err());
    }
}
