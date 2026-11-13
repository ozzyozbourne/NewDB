//! `log.seg`: the raw, append-only record of every post in a partition.
//!
//! ```text
//! file:    magic "NEWDBLOG" | FORMAT_VERSION u32 | HEADER_FORMAT_VERSION u32 | hour u64
//! record:  len u32 | crc32c u32 | kind u8 | seq u64 | body
//! ```
//!
//! `len` counts `kind | seq | body`; the CRC covers the same bytes. Kinds:
//! - [`kind::SPAN_POST`]: body is one `SpanHeader` (184 bytes); `seq` is its version.
//! - [`kind::STRING`]: body is `id u32 | UTF-8 bytes`, defining the next string-table ID.
//!   The writer emits it before the first post that uses the string; `seq` is 0.
//!
//! A reader stops at the first record with a bad length or CRC: that is the torn tail of
//! a write the process did not finish, and everything after it is discarded.

use newdb_types::header::HEADER_FORMAT_VERSION;
use newdb_types::{Result, SpanHeader};

use crate::{Cursor, FILE_HEADER_LEN, MAGIC_LOG, check_file_header, corrupt, file_header};

pub const HEADER_LEN: usize = FILE_HEADER_LEN + 8;
/// `len u32 | crc u32`.
pub const RECORD_PREFIX_LEN: usize = 8;
/// `kind u8 | seq u64`.
const RECORD_META_LEN: usize = 9;
/// Largest record a reader accepts; anything larger is a torn or corrupt length.
pub const MAX_RECORD_LEN: usize = 16 << 20;

pub mod kind {
    pub const SPAN_POST: u8 = 1;
    pub const STRING: u8 = 2;
}

/// The file header of a partition's log; `hour` is hours since the Unix epoch.
pub fn file_header_bytes(hour: u64) -> [u8; HEADER_LEN] {
    let mut h = [0u8; HEADER_LEN];
    h[..FILE_HEADER_LEN].copy_from_slice(&file_header(MAGIC_LOG, HEADER_FORMAT_VERSION));
    h[FILE_HEADER_LEN..].copy_from_slice(&hour.to_le_bytes());
    h
}

/// Appends one record.
pub fn append_record(out: &mut Vec<u8>, kind: u8, seq: u64, body: &[u8]) {
    let len = (RECORD_META_LEN + body.len()) as u32;
    let start = out.len();
    out.extend_from_slice(&len.to_le_bytes());
    out.extend_from_slice(&[0; 4]);
    out.push(kind);
    out.extend_from_slice(&seq.to_le_bytes());
    out.extend_from_slice(body);
    let crc = crc32c::crc32c(&out[start + RECORD_PREFIX_LEN..]);
    out[start + 4..start + 8].copy_from_slice(&crc.to_le_bytes());
}

pub fn append_post(out: &mut Vec<u8>, header: &SpanHeader) {
    append_record(
        out,
        kind::SPAN_POST,
        header.version,
        bytemuck::bytes_of(header),
    );
}

pub fn append_string(out: &mut Vec<u8>, id: u32, s: &str) {
    let mut body = Vec::with_capacity(4 + s.len());
    body.extend_from_slice(&id.to_le_bytes());
    body.extend_from_slice(s.as_bytes());
    append_record(out, kind::STRING, 0, &body);
}

/// One record read back. `end` is the byte offset just past it in the file.
#[derive(Clone, Copy, Debug)]
pub struct Record<'a> {
    pub kind: u8,
    pub seq: u64,
    pub body: &'a [u8],
    pub end: usize,
}

impl<'a> Record<'a> {
    pub fn post(&self) -> Result<SpanHeader> {
        if self.kind != kind::SPAN_POST || self.body.len() != size_of::<SpanHeader>() {
            return Err(corrupt("log: not a span post record"));
        }
        Ok(bytemuck::pod_read_unaligned(self.body))
    }

    pub fn string(&self) -> Result<(u32, &'a str)> {
        if self.kind != kind::STRING {
            return Err(corrupt("log: not a string record"));
        }
        let mut c = Cursor::new(self.body, "log string record");
        let id = c.u32()?;
        let s = c.str(c.remaining())?;
        Ok((id, s))
    }
}

/// Iterates the valid records of a log file, stopping at the end or at the torn tail.
pub struct Reader<'a> {
    bytes: &'a [u8],
    pos: usize,
    pub hour: u64,
    torn: bool,
}

impl<'a> Reader<'a> {
    /// Fails only if the file header itself is bad (a log always has its header synced
    /// before any record is written).
    pub fn new(bytes: &'a [u8]) -> Result<Reader<'a>> {
        let header_version = check_file_header(bytes, MAGIC_LOG, "log")?;
        if header_version != HEADER_FORMAT_VERSION {
            return Err(corrupt(format!(
                "log: span header version {header_version}, this build reads {HEADER_FORMAT_VERSION}"
            )));
        }
        let mut c = Cursor::new(&bytes[FILE_HEADER_LEN..], "log");
        let hour = c.u64()?;
        Ok(Reader {
            bytes,
            pos: HEADER_LEN,
            hour,
            torn: false,
        })
    }

    /// Bytes of the file that hold the header and valid records so far.
    pub fn valid_len(&self) -> usize {
        self.pos
    }

    /// Whether reading stopped at a bad record rather than at the end of the file.
    pub fn torn(&self) -> bool {
        self.torn
    }
}

impl<'a> Iterator for Reader<'a> {
    type Item = Record<'a>;

    fn next(&mut self) -> Option<Record<'a>> {
        if self.torn || self.pos == self.bytes.len() {
            return None;
        }
        let rest = &self.bytes[self.pos..];
        let parsed = (|| {
            let len = u32::from_le_bytes(rest.get(0..4)?.try_into().ok()?) as usize;
            let crc = u32::from_le_bytes(rest.get(4..8)?.try_into().ok()?);
            if !(RECORD_META_LEN..=MAX_RECORD_LEN).contains(&len) {
                return None;
            }
            let data = rest.get(RECORD_PREFIX_LEN..RECORD_PREFIX_LEN + len)?;
            if crc32c::crc32c(data) != crc {
                return None;
            }
            Some(Record {
                kind: data[0],
                seq: u64::from_le_bytes(data[1..9].try_into().ok()?),
                body: &data[RECORD_META_LEN..],
                end: self.pos + RECORD_PREFIX_LEN + len,
            })
        })();
        match parsed {
            Some(r) => {
                self.pos = r.end;
                Some(r)
            }
            None => {
                self.torn = true;
                None
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use newdb_types::{SpanId, TraceId};

    fn sample() -> (Vec<u8>, SpanHeader) {
        let h = SpanHeader {
            trace_id: TraceId([7; 16]),
            span_id: SpanId([3; 8]),
            version: 42,
            name: 1,
            fields: newdb_types::header::field::NAME,
            ..SpanHeader::default()
        };
        let mut log = file_header_bytes(491_234).to_vec();
        append_string(&mut log, 1, "agent");
        append_post(&mut log, &h);
        append_post(&mut log, &SpanHeader { version: 43, ..h });
        (log, h)
    }

    #[test]
    fn round_trips_records() {
        let (log, h) = sample();
        let mut r = Reader::new(&log).unwrap();
        assert_eq!(r.hour, 491_234);
        let recs: Vec<_> = r.by_ref().collect();
        assert_eq!(recs.len(), 3);
        assert_eq!(recs[0].string().unwrap(), (1, "agent"));
        assert_eq!(recs[1].post().unwrap(), h);
        assert_eq!(recs[2].seq, 43);
        assert!(recs[1].string().is_err());
        assert!(!r.torn());
        assert_eq!(r.valid_len(), log.len());
    }

    #[test]
    fn every_truncation_and_every_flipped_byte_cuts_at_the_damaged_record() {
        let (log, _) = sample();
        let ends: Vec<usize> = Reader::new(&log).unwrap().map(|r| r.end).collect();
        let valid_before = |at: usize| {
            ends.iter()
                .copied()
                .filter(|&e| e <= at)
                .max()
                .unwrap_or(HEADER_LEN)
        };
        for cut in HEADER_LEN..log.len() {
            let mut r = Reader::new(&log[..cut]).unwrap();
            let n = r.by_ref().count();
            assert_eq!(r.valid_len(), valid_before(cut), "cut at {cut}");
            assert_eq!(n, ends.iter().filter(|&&e| e <= cut).count());
            assert_eq!(r.torn(), cut != HEADER_LEN && !ends.contains(&cut));
        }
        for i in HEADER_LEN..log.len() {
            let mut bad = log.clone();
            bad[i] ^= 0x40;
            let mut r = Reader::new(&bad).unwrap();
            r.by_ref().for_each(drop);
            // The damaged record and everything after it are gone.
            assert_eq!(r.valid_len(), valid_before(i), "flip at {i}");
            assert!(r.torn());
        }
    }

    #[test]
    fn bad_header_is_corrupt() {
        let (mut log, _) = sample();
        assert!(Reader::new(&log[..10]).is_err());
        log[0] = b'X';
        assert!(Reader::new(&log).is_err());
    }
}
