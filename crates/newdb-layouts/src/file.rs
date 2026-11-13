//! The framing every layout file uses, so readers can cast sections straight from mapped
//! bytes:
//!
//! ```text
//! magic        [u8; 8]
//! version      u32 (the layout's VERSION)
//! sections     u32 (n)
//! table        n × (offset u64, len u64)
//! sections     each at an offset that is a multiple of 8, zero padding between
//! ```
//!
//! All integers are little-endian. The bytes are a pure function of the sections, so
//! deterministic sections give deterministic files.

use bytemuck::Pod;
use newdb_types::{Error, Result};

pub(crate) fn write(magic: &[u8; 8], version: u32, sections: &[&[u8]]) -> Vec<u8> {
    let table_end = 16 + 16 * sections.len();
    let mut out =
        Vec::with_capacity(table_end + sections.iter().map(|s| s.len() + 7).sum::<usize>());
    out.extend_from_slice(magic);
    out.extend_from_slice(&version.to_le_bytes());
    out.extend_from_slice(&(sections.len() as u32).to_le_bytes());
    let mut offset = table_end.next_multiple_of(8);
    for s in sections {
        out.extend_from_slice(&(offset as u64).to_le_bytes());
        out.extend_from_slice(&(s.len() as u64).to_le_bytes());
        offset = (offset + s.len()).next_multiple_of(8);
    }
    for s in sections {
        out.resize(out.len().next_multiple_of(8), 0);
        out.extend_from_slice(s);
    }
    out
}

/// Splits a file written by [`write`] into its `N` sections, checking magic, version and
/// bounds. `name` is for error messages.
pub(crate) fn read<'a, const N: usize>(
    bytes: &'a [u8],
    magic: &[u8; 8],
    version: u32,
    name: &str,
) -> Result<[&'a [u8]; N]> {
    let corrupt = |what: &str| Error::Corrupt(format!("{name}: {what}"));
    let u32_at = |i: usize| -> Option<u32> {
        Some(u32::from_le_bytes(bytes.get(i..i + 4)?.try_into().ok()?))
    };
    let u64_at = |i: usize| -> Option<u64> {
        Some(u64::from_le_bytes(bytes.get(i..i + 8)?.try_into().ok()?))
    };
    if bytes.get(..8) != Some(magic.as_slice()) {
        return Err(corrupt("bad magic"));
    }
    match u32_at(8) {
        Some(v) if v == version => {}
        Some(v) => return Err(corrupt(&format!("version {v}, expected {version}"))),
        None => return Err(corrupt("truncated header")),
    }
    if u32_at(12) != Some(N as u32) {
        return Err(corrupt("unexpected section count"));
    }
    let mut out = [&bytes[..0]; N];
    for (i, slot) in out.iter_mut().enumerate() {
        let (Some(off), Some(len)) = (u64_at(16 + 16 * i), u64_at(24 + 16 * i)) else {
            return Err(corrupt("truncated section table"));
        };
        if off % 8 != 0 {
            return Err(corrupt("misaligned section"));
        }
        *slot = usize::try_from(off)
            .ok()
            .zip(usize::try_from(len).ok())
            .and_then(|(o, l)| bytes.get(o..o.checked_add(l)?))
            .ok_or_else(|| corrupt("section out of bounds"))?;
    }
    Ok(out)
}

/// Casts a section to a slice of `T`. Fails if the length is not a multiple of `T`'s size or
/// if the bytes are not aligned for `T` (files must be mapped at page-aligned addresses).
pub(crate) fn cast<'a, T: Pod>(bytes: &'a [u8], name: &str) -> Result<&'a [T]> {
    bytemuck::try_cast_slice(bytes)
        .map_err(|e| Error::Corrupt(format!("{name}: cannot read section as records: {e:?}")))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sections_round_trip_at_aligned_offsets() {
        let bytes = write(b"TESTFILE", 3, &[b"abc", b"", b"0123456789"]);
        let buf: Vec<u64> = bytes
            .chunks(8)
            .map(|c| {
                let mut w = [0u8; 8];
                w[..c.len()].copy_from_slice(c);
                u64::from_le_bytes(w)
            })
            .collect();
        let aligned = &bytemuck::cast_slice::<u64, u8>(&buf)[..bytes.len()];
        let [a, b, c] = read(aligned, b"TESTFILE", 3, "t").unwrap();
        assert_eq!((a, b, c), (&b"abc"[..], &b""[..], &b"0123456789"[..]));
        for s in [a, b, c] {
            assert_eq!((s.as_ptr() as usize - aligned.as_ptr() as usize) % 8, 0);
        }
        assert!(read::<3>(aligned, b"TESTFILE", 4, "t").is_err());
        assert!(read::<2>(aligned, b"TESTFILE", 3, "t").is_err());
        assert!(read::<3>(&aligned[..aligned.len() - 1], b"TESTFILE", 3, "t").is_err());
        assert!(read::<3>(&aligned[..20], b"TESTFILE", 3, "t").is_err());
    }
}
