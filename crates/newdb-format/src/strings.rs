//! `strings.tbl`: a sealed partition's string table. String IDs start at 1 (0 means "no
//! string") and are assigned in log order, so the table can be rebuilt from the log.
//!
//! ```text
//! magic "NEWDBSTR" | FORMAT_VERSION u32 | count u32
//! offsets [count + 1] u32     string i (ID i + 1) is bytes[offsets[i]..offsets[i + 1]]
//! sorted  [count] u32         IDs ordered by string bytes, for lookups by value
//! bytes
//! ```

use newdb_types::Result;

use crate::{FILE_HEADER_LEN, MAGIC_STRINGS, check_file_header, corrupt, file_header};

/// Encodes the table whose ID `i + 1` is `strings[i]`.
pub fn encode<S: AsRef<str>>(strings: &[S]) -> Vec<u8> {
    let n = strings.len();
    let mut out = file_header(MAGIC_STRINGS, n as u32).to_vec();
    let mut off = 0u32;
    out.extend_from_slice(&off.to_le_bytes());
    for s in strings {
        off += s.as_ref().len() as u32;
        out.extend_from_slice(&off.to_le_bytes());
    }
    let mut sorted: Vec<u32> = (1..=n as u32).collect();
    sorted.sort_by_key(|&id| strings[id as usize - 1].as_ref());
    for id in sorted {
        out.extend_from_slice(&id.to_le_bytes());
    }
    for s in strings {
        out.extend_from_slice(s.as_ref().as_bytes());
    }
    out
}

/// A validated view of an encoded table.
#[derive(Clone, Copy)]
pub struct Table<'a> {
    bytes: &'a [u8],
    count: usize,
    data_start: usize,
}

impl<'a> Table<'a> {
    /// Checks the structure and that every string is UTF-8.
    pub fn open(bytes: &'a [u8]) -> Result<Table<'a>> {
        let count = check_file_header(bytes, MAGIC_STRINGS, "strings")? as usize;
        let offsets_len = count
            .checked_add(1)
            .and_then(|n| n.checked_mul(4))
            .ok_or_else(|| corrupt("strings: bad count"))?;
        let data_start = FILE_HEADER_LEN + offsets_len + 4 * count;
        if data_start > bytes.len() {
            return Err(corrupt("strings: truncated"));
        }
        let t = Table {
            bytes,
            count,
            data_start,
        };
        let data_len = bytes.len() - data_start;
        let mut prev = 0;
        for i in 0..=count {
            let o = t.word(FILE_HEADER_LEN + 4 * i) as usize;
            if o < prev || o > data_len || (i == 0 && o != 0) {
                return Err(corrupt("strings: bad offsets"));
            }
            prev = o;
        }
        if prev != data_len {
            return Err(corrupt("strings: trailing bytes"));
        }
        for id in 1..=count as u32 {
            std::str::from_utf8(t.raw(id)).map_err(|_| corrupt("strings: not UTF-8"))?;
        }
        let mut seen = vec![false; count];
        let mut last: Option<&[u8]> = None;
        for i in 0..count {
            let id = t.sorted_id(i);
            if id == 0 || id as usize > count || std::mem::replace(&mut seen[id as usize - 1], true)
            {
                return Err(corrupt("strings: bad sorted index"));
            }
            let s = t.raw(id);
            if last.is_some_and(|l| l > s) {
                return Err(corrupt("strings: sorted index out of order"));
            }
            last = Some(s);
        }
        Ok(t)
    }

    fn word(&self, at: usize) -> u32 {
        u32::from_le_bytes([
            self.bytes[at],
            self.bytes[at + 1],
            self.bytes[at + 2],
            self.bytes[at + 3],
        ])
    }

    fn sorted_id(&self, i: usize) -> u32 {
        self.word(FILE_HEADER_LEN + 4 * (self.count + 1) + 4 * i)
    }

    /// `id` must be in `1..=count` (checked by callers).
    fn raw(&self, id: u32) -> &'a [u8] {
        let i = id as usize - 1;
        let a = self.word(FILE_HEADER_LEN + 4 * i) as usize;
        let b = self.word(FILE_HEADER_LEN + 4 * (i + 1)) as usize;
        &self.bytes[self.data_start + a..self.data_start + b]
    }

    pub fn len(&self) -> usize {
        self.count
    }

    pub fn is_empty(&self) -> bool {
        self.count == 0
    }

    /// The string with `id`; `None` for 0 and for IDs past the end.
    pub fn get(&self, id: u32) -> Option<&'a str> {
        if id == 0 || id as usize > self.count {
            return None;
        }
        // Validated as UTF-8 in `open`.
        std::str::from_utf8(self.raw(id)).ok()
    }

    /// The ID of `s`, by binary search over the sorted index.
    pub fn find(&self, s: &str) -> Option<u32> {
        let (mut lo, mut hi) = (0, self.count);
        while lo < hi {
            let mid = (lo + hi) / 2;
            let id = self.sorted_id(mid);
            match self.raw(id).cmp(s.as_bytes()) {
                std::cmp::Ordering::Less => lo = mid + 1,
                std::cmp::Ordering::Greater => hi = mid,
                std::cmp::Ordering::Equal => return Some(id),
            }
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trips_and_finds_by_value() {
        let strs = ["project-1", "agent", "", "Ünïcode", "a"];
        let bytes = encode(&strs);
        let t = Table::open(&bytes).unwrap();
        assert_eq!(t.len(), 5);
        for (i, s) in strs.iter().enumerate() {
            assert_eq!(t.get(i as u32 + 1), Some(*s));
            assert_eq!(t.find(s), Some(i as u32 + 1));
        }
        assert_eq!(t.get(0), None);
        assert_eq!(t.get(6), None);
        assert_eq!(t.find("missing"), None);
        let empty = encode::<&str>(&[]);
        assert!(Table::open(&empty).unwrap().find("x").is_none());
    }

    #[test]
    fn damage_is_corrupt_not_a_panic() {
        let bytes = encode(&["project-1", "agent", "chat"]);
        for cut in 0..bytes.len() {
            assert!(Table::open(&bytes[..cut]).is_err(), "cut {cut}");
        }
        for i in FILE_HEADER_LEN..bytes.len() {
            let mut bad = bytes.clone();
            bad[i] ^= 0x80;
            // Either rejected or still a structurally valid table.
            if let Ok(t) = Table::open(&bad) {
                for id in 0..=4 {
                    let _ = t.get(id);
                }
            }
        }
    }
}
