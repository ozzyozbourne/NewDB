//! `payloads.blob`: the payload bytes of a partition, each stored once.
//!
//! ```text
//! file:   magic "NEWDBPAY" | FORMAT_VERSION u32 | 0 u32
//! entry:  xxh3-128 hash [16] | raw_len u32 | len u32 | codec u8 | 0 [3] | crc32c(data) u32
//!         data [len]
//! ```
//!
//! A [`PayloadRef`] points at `data` (`offset` is just past the 32-byte entry header), so a
//! read finds the entry header at `offset - 32` and checks it against the reference.
//! Payloads of at least [`COMPRESS_MIN`] bytes are zstd-compressed when that makes them
//! smaller; the rest are stored raw. The hash is of the raw bytes.

use newdb_types::{PayloadRef, Result};

use crate::{Cursor, FILE_HEADER_LEN, MAGIC_PAYLOADS, check_file_header, corrupt, file_header};

pub const ENTRY_HEADER_LEN: usize = 32;
pub const COMPRESS_MIN: usize = 64;
pub const ZSTD_LEVEL: i32 = 1;

pub mod codec {
    pub const RAW: u8 = 0;
    pub const ZSTD: u8 = 1;
}

pub type Hash = u128;

pub fn hash(raw: &[u8]) -> Hash {
    xxhash_rust::xxh3::xxh3_128(raw)
}

pub fn file_header_bytes() -> [u8; FILE_HEADER_LEN] {
    file_header(MAGIC_PAYLOADS, 0)
}

pub fn check_header(bytes: &[u8]) -> Result<()> {
    check_file_header(bytes, MAGIC_PAYLOADS, "payloads").map(drop)
}

/// The decoded entry header in front of each payload.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Entry {
    pub hash: Hash,
    pub codec: u8,
    pub crc: u32,
    /// Reference to this entry's data.
    pub r: PayloadRef,
}

/// Compresses and frames payloads. Keeps one zstd context for the writer's lifetime.
pub struct Encoder {
    zstd: zstd::bulk::Compressor<'static>,
}

impl Encoder {
    pub fn new() -> Result<Encoder> {
        Ok(Encoder {
            zstd: zstd::bulk::Compressor::new(ZSTD_LEVEL)?,
        })
    }

    /// The stored form of `raw`: `Some(zstd frame)` when compressing makes it smaller,
    /// `None` to store it raw.
    pub fn compress(&mut self, raw: &[u8]) -> Result<Option<Vec<u8>>> {
        if raw.len() < COMPRESS_MIN {
            return Ok(None);
        }
        Ok(Some(self.zstd.compress(raw)?).filter(|c| c.len() < raw.len()))
    }

    /// Appends the entry for `raw` to `out`, where `out[0]` will sit at `base` in the
    /// file, and returns its reference.
    pub fn append(
        &mut self,
        raw: &[u8],
        h: Hash,
        base: u64,
        out: &mut Vec<u8>,
    ) -> Result<PayloadRef> {
        let compressed = self.compress(raw)?;
        append_entry(raw, compressed.as_deref(), h, base, out)
    }
}

/// Appends the entry for `raw`, whose stored form [`Encoder::compress`] returned as
/// `compressed`, to `out`, where `out[0]` will sit at `base` in the file.
pub fn append_entry(
    raw: &[u8],
    compressed: Option<&[u8]>,
    h: Hash,
    base: u64,
    out: &mut Vec<u8>,
) -> Result<PayloadRef> {
    let raw_len = u32::try_from(raw.len())
        .map_err(|_| newdb_types::Error::InvalidInput("payload larger than 4 GiB".into()))?;
    let (codec, data) = match compressed {
        Some(c) => (codec::ZSTD, c),
        None => (codec::RAW, raw),
    };
    let r = PayloadRef {
        offset: base + (out.len() + ENTRY_HEADER_LEN) as u64,
        len: data.len() as u32,
        raw_len,
    };
    out.extend_from_slice(&h.to_le_bytes());
    out.extend_from_slice(&raw_len.to_le_bytes());
    out.extend_from_slice(&r.len.to_le_bytes());
    out.extend_from_slice(&[codec, 0, 0, 0]);
    out.extend_from_slice(&crc32c::crc32c(data).to_le_bytes());
    out.extend_from_slice(data);
    Ok(r)
}

fn parse_entry_header(bytes: &[u8], data_offset: u64) -> Result<Entry> {
    let mut c = Cursor::new(bytes, "payload entry");
    let hash = u128::from_le_bytes(
        c.take(16)?
            .try_into()
            .map_err(|_| corrupt("payload entry: short hash"))?,
    );
    let raw_len = c.u32()?;
    let len = c.u32()?;
    let codec = c.u8()?;
    c.take(3)?;
    let crc = c.u32()?;
    if codec > codec::ZSTD || (codec == codec::RAW && len != raw_len) {
        return Err(corrupt("payload entry: bad codec"));
    }
    Ok(Entry {
        hash,
        codec,
        crc,
        r: PayloadRef {
            offset: data_offset,
            len,
            raw_len,
        },
    })
}

/// Decodes the payload `r` from `entry`, the bytes `[r.offset - 32, r.offset + r.len)` of
/// the file. Checks the entry header against `r`, but not the CRC (see [`check`]).
pub fn decode(entry: &[u8], r: PayloadRef) -> Result<Vec<u8>> {
    if entry.len() != ENTRY_HEADER_LEN + r.len as usize {
        return Err(corrupt("payload: entry length does not match reference"));
    }
    let e = parse_entry_header(&entry[..ENTRY_HEADER_LEN], r.offset)?;
    if e.r != r {
        return Err(corrupt("payload: reference does not match entry header"));
    }
    let data = &entry[ENTRY_HEADER_LEN..];
    match e.codec {
        codec::RAW => Ok(data.to_vec()),
        _ => {
            let out = decompress(data, r.raw_len as usize)
                .map_err(|e| corrupt(format!("payload: zstd: {e}")))?;
            if out.len() != r.raw_len as usize {
                return Err(corrupt("payload: decompressed length mismatch"));
            }
            Ok(out)
        }
    }
}

thread_local! {
    /// One decompression context per thread: creating one per payload showed in profiles.
    static DCTX: std::cell::RefCell<Option<zstd::bulk::Decompressor<'static>>> =
        const { std::cell::RefCell::new(None) };
}

pub(crate) fn decompress(data: &[u8], capacity: usize) -> std::io::Result<Vec<u8>> {
    DCTX.with(|cell| {
        let mut slot = cell.borrow_mut();
        let d = match slot.as_mut() {
            Some(d) => d,
            None => slot.insert(zstd::bulk::Decompressor::new()?),
        };
        d.decompress(data, capacity)
    })
}

/// The byte range of `r`'s entry in the file, or an error if `r` cannot be a valid
/// reference into a file of `file_len` bytes.
pub fn entry_range(r: PayloadRef, file_len: u64) -> Result<std::ops::Range<usize>> {
    let start = r.offset.checked_sub(ENTRY_HEADER_LEN as u64);
    let end = r.offset.checked_add(r.len as u64);
    match (start, end) {
        (Some(s), Some(e)) if s >= FILE_HEADER_LEN as u64 && e <= file_len => {
            Ok(s as usize..e as usize)
        }
        _ => Err(corrupt(format!(
            "payload reference {}+{} outside payloads.blob ({file_len} bytes)",
            r.offset, r.len
        ))),
    }
}

/// Reads payload `r` from the whole file's bytes.
pub fn read(blob: &[u8], r: PayloadRef) -> Result<Vec<u8>> {
    decode(&blob[entry_range(r, blob.len() as u64)?], r)
}

/// Full check of one stored payload: CRC, decompression and hash.
pub fn check(blob: &[u8], r: PayloadRef) -> Result<()> {
    let range = entry_range(r, blob.len() as u64)?;
    let e = parse_entry_header(&blob[range.start..range.start + ENTRY_HEADER_LEN], r.offset)?;
    if crc32c::crc32c(&blob[range.start + ENTRY_HEADER_LEN..range.end]) != e.crc {
        return Err(corrupt(format!("payload at {}: bad CRC", r.offset)));
    }
    if hash(&decode(&blob[range], r)?) != e.hash {
        return Err(corrupt(format!("payload at {}: hash mismatch", r.offset)));
    }
    Ok(())
}

/// Iterates the entries of a payload file, stopping at the end or at the first entry that
/// is truncated or fails its CRC (the torn tail).
pub struct Scan<'a> {
    bytes: &'a [u8],
    pos: usize,
    torn: bool,
}

impl<'a> Scan<'a> {
    pub fn new(bytes: &'a [u8]) -> Result<Scan<'a>> {
        check_header(bytes)?;
        Ok(Scan {
            bytes,
            pos: FILE_HEADER_LEN,
            torn: false,
        })
    }

    pub fn valid_len(&self) -> usize {
        self.pos
    }

    pub fn torn(&self) -> bool {
        self.torn
    }
}

impl Iterator for Scan<'_> {
    type Item = Entry;

    fn next(&mut self) -> Option<Entry> {
        if self.torn || self.pos == self.bytes.len() {
            return None;
        }
        let parsed = (|| {
            let head = self.bytes.get(self.pos..self.pos + ENTRY_HEADER_LEN)?;
            let e = parse_entry_header(head, (self.pos + ENTRY_HEADER_LEN) as u64).ok()?;
            let data_start = self.pos + ENTRY_HEADER_LEN;
            let data = self.bytes.get(data_start..data_start + e.r.len as usize)?;
            (crc32c::crc32c(data) == e.crc).then_some(e)
        })();
        match parsed {
            Some(e) => {
                self.pos = (e.r.offset + e.r.len as u64) as usize;
                Some(e)
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

    #[test]
    fn round_trips_raw_and_compressed_and_scans_back() {
        let mut enc = Encoder::new().unwrap();
        let mut blob = file_header_bytes().to_vec();
        let small = b"{\"a\":1}".to_vec();
        let big = b"refund policy ".repeat(100);
        let empty = Vec::new();
        let mut refs = Vec::new();
        for p in [&small, &big, &empty] {
            let base = blob.len() as u64;
            let mut out = Vec::new();
            refs.push(enc.append(p, hash(p), base, &mut out).unwrap());
            blob.extend_from_slice(&out);
        }
        assert_eq!(refs[0].len, refs[0].raw_len);
        assert!(refs[1].len < refs[1].raw_len);
        assert_eq!(read(&blob, refs[0]).unwrap(), small);
        assert_eq!(read(&blob, refs[1]).unwrap(), big);
        assert_eq!(read(&blob, refs[2]).unwrap(), empty);
        for r in &refs {
            check(&blob, *r).unwrap();
        }
        let mut scan = Scan::new(&blob).unwrap();
        let found: Vec<_> = scan.by_ref().map(|e| (e.r, e.hash)).collect();
        assert_eq!(
            found,
            vec![
                (refs[0], hash(&small)),
                (refs[1], hash(&big)),
                (refs[2], hash(&empty))
            ]
        );
        assert_eq!(scan.valid_len(), blob.len());

        // A torn tail stops the scan at the last whole entry.
        let cut = blob.len() - 1;
        let mut scan = Scan::new(&blob[..cut - ENTRY_HEADER_LEN]).unwrap();
        assert_eq!(scan.by_ref().count(), 1);
        assert!(scan.torn());

        // References that do not match the stored entry are corrupt, not a panic.
        let wrong = PayloadRef {
            len: refs[1].len + 1,
            ..refs[1]
        };
        assert!(read(&blob, wrong).is_err());
        let past = PayloadRef {
            offset: blob.len() as u64,
            ..refs[0]
        };
        assert!(read(&blob, past).is_err());
        let mut flipped = blob.clone();
        let at = refs[1].offset as usize + 3;
        flipped[at] ^= 1;
        assert!(check(&flipped, refs[1]).is_err());
    }
}
