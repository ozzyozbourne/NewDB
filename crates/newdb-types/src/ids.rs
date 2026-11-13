//! Trace and span IDs, as in OpenTelemetry. Ordering is plain byte order.

use std::fmt;

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
#[repr(transparent)]
pub struct TraceId(pub [u8; 16]);

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
#[repr(transparent)]
pub struct SpanId(pub [u8; 8]);

macro_rules! hex_id {
    ($t:ident, $n:literal) => {
        impl $t {
            /// Parses lowercase or uppercase hex of exactly the right length.
            pub fn from_hex(s: &str) -> Option<Self> {
                let s = s.as_bytes();
                if s.len() != $n * 2 {
                    return None;
                }
                let mut out = [0u8; $n];
                for (i, b) in out.iter_mut().enumerate() {
                    *b = (hex_val(s[2 * i])? << 4) | hex_val(s[2 * i + 1])?;
                }
                Some($t(out))
            }

            pub fn to_hex(&self) -> String {
                let mut s = String::with_capacity($n * 2);
                for b in self.0 {
                    s.push(HEX[(b >> 4) as usize] as char);
                    s.push(HEX[(b & 15) as usize] as char);
                }
                s
            }

            pub fn is_zero(&self) -> bool {
                self.0 == [0u8; $n]
            }
        }

        impl fmt::Debug for $t {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str(&self.to_hex())
            }
        }

        impl fmt::Display for $t {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str(&self.to_hex())
            }
        }
    };
}

hex_id!(TraceId, 16);
hex_id!(SpanId, 8);

const HEX: &[u8; 16] = b"0123456789abcdef";

fn hex_val(c: u8) -> Option<u8> {
    match c {
        b'0'..=b'9' => Some(c - b'0'),
        b'a'..=b'f' => Some(c - b'a' + 10),
        b'A'..=b'F' => Some(c - b'A' + 10),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hex_round_trip() {
        let t = TraceId::from_hex("0af7651916cd43dd8448eb211c80319c").unwrap();
        assert_eq!(t.0[0], 0x0a);
        assert_eq!(t.0[15], 0x9c);
        assert_eq!(t.to_hex(), "0af7651916cd43dd8448eb211c80319c");
        assert_eq!(
            SpanId::from_hex("B7AD6B7169203331").unwrap().to_hex(),
            "b7ad6b7169203331"
        );
        assert!(SpanId::from_hex("b7ad6b716920333").is_none());
        assert!(SpanId::from_hex("b7ad6b716920333g").is_none());
    }
}
