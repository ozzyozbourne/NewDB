//! `payloads.sealed`: a sealed partition's payloads, re-framed for reading by trace.
//!
//! The active partition stores each payload as its own zstd frame in `payloads.blob`, in
//! arrival order. Small frames decompress slowly and compress poorly, so at seal time the
//! engine copies every payload into larger frames filled in trace order, and maps each
//! payload's `payloads.blob` offset (the `PayloadRef::offset` the layouts keep) to its
//! place in a frame. Layouts and `PayloadRef`s do not change.
//!
//! ```text
//! magic "NEWDBPSL" | FORMAT_VERSION u32 | 0 u32
//! frame count u32 | entry count u32 | crc32c of the two tables u32 | 0 u32
//! frames  [count] { data offset u64 | len u32 | raw len u32 | crc32c of data u32 | codec u32 }
//! entries [count] { blob offset u64 | frame u32 | offset in frame u32 | len u32 | crc32c u32 }
//! frame data
//! ```
//!
//! Entries are sorted by blob offset and unique; a frame's payloads are its raw bytes
//! `offset..offset + len`, and the entry's CRC covers those raw bytes. Frame data offsets
//! count from the start of the frame data and are contiguous. A payload of at least half
//! the frame size gets a frame of its own, so a small read never decompresses a large
//! neighbour.

use newdb_types::Result;

use crate::payload::{ZSTD_LEVEL, codec, decompress};
use crate::{FILE_HEADER_LEN, MAGIC_SEALED, check_file_header, corrupt, file_header};

/// Raw bytes a frame collects before it is closed.
pub const FRAME_TARGET: usize = 64 << 10;

const FRAME_LEN: usize = 24;
const ENTRY_LEN: usize = 24;
const TABLES_AT: usize = FILE_HEADER_LEN + 16;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Frame {
    pub offset: u64,
    pub len: u32,
    pub raw_len: u32,
    pub crc: u32,
    pub codec: u32,
}

/// Where one payload lives.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Loc {
    pub frame: u32,
    pub at: u32,
    pub len: u32,
    pub crc: u32,
}

/// Builds a file from payloads added in the order they should be laid out.
pub struct Builder {
    target: usize,
    zstd: zstd::bulk::Compressor<'static>,
    frames: Vec<Frame>,
    entries: Vec<(u64, Loc)>,
    data: Vec<u8>,
    cur: Vec<u8>,
}

impl Builder {
    pub fn new(target: usize) -> Result<Builder> {
        Ok(Builder {
            target: target.max(1),
            zstd: zstd::bulk::Compressor::new(ZSTD_LEVEL)?,
            frames: Vec::new(),
            entries: Vec::new(),
            data: Vec::new(),
            cur: Vec::new(),
        })
    }

    /// Starts a group of payloads (one trace) totalling `raw_len` bytes: a group that fits
    /// in a frame is not split across two, so reading a small trace decompresses one frame.
    pub fn start_group(&mut self, raw_len: usize) -> Result<()> {
        if !self.cur.is_empty() && self.cur.len() + raw_len > self.target {
            self.flush()?;
        }
        Ok(())
    }

    /// Adds the payload stored at `blob_offset` in `payloads.blob`. Each offset once.
    pub fn add(&mut self, blob_offset: u64, raw: &[u8]) -> Result<()> {
        let big = raw.len() >= self.target / 2;
        if !self.cur.is_empty() && (big || self.cur.len() + raw.len() > self.target) {
            self.flush()?;
        }
        let too_big = || corrupt("sealed payloads: frame larger than 4 GiB");
        self.entries.push((
            blob_offset,
            Loc {
                frame: self.frames.len() as u32,
                at: u32::try_from(self.cur.len()).map_err(|_| too_big())?,
                len: u32::try_from(raw.len()).map_err(|_| too_big())?,
                crc: crc32c::crc32c(raw),
            },
        ));
        self.cur.extend_from_slice(raw);
        if big {
            self.flush()?;
        }
        Ok(())
    }

    fn flush(&mut self) -> Result<()> {
        let c = self.zstd.compress(&self.cur)?;
        let (codec, data) = if c.len() < self.cur.len() {
            (codec::ZSTD as u32, &c[..])
        } else {
            (codec::RAW as u32, &self.cur[..])
        };
        self.frames.push(Frame {
            offset: self.data.len() as u64,
            len: data.len() as u32,
            raw_len: self.cur.len() as u32,
            crc: crc32c::crc32c(data),
            codec,
        });
        self.data.extend_from_slice(data);
        self.cur.clear();
        Ok(())
    }

    pub fn finish(mut self) -> Result<Vec<u8>> {
        // Also flush a frame holding only empty payloads, so their entries point at one.
        let pending = self
            .entries
            .last()
            .is_some_and(|e| e.1.frame as usize == self.frames.len());
        if pending {
            self.flush()?;
        }
        self.entries.sort_unstable_by_key(|e| e.0);
        if self.entries.windows(2).any(|w| w[0].0 == w[1].0) {
            return Err(newdb_types::Error::InvalidInput(
                "sealed payloads: an offset was added twice".into(),
            ));
        }
        let mut tables =
            Vec::with_capacity(self.frames.len() * FRAME_LEN + self.entries.len() * ENTRY_LEN);
        for f in &self.frames {
            tables.extend_from_slice(&f.offset.to_le_bytes());
            for v in [f.len, f.raw_len, f.crc, f.codec] {
                tables.extend_from_slice(&v.to_le_bytes());
            }
        }
        for (off, l) in &self.entries {
            tables.extend_from_slice(&off.to_le_bytes());
            for v in [l.frame, l.at, l.len, l.crc] {
                tables.extend_from_slice(&v.to_le_bytes());
            }
        }
        let mut out = file_header(MAGIC_SEALED, 0).to_vec();
        for v in [
            self.frames.len() as u32,
            self.entries.len() as u32,
            crc32c::crc32c(&tables),
            0,
        ] {
            out.extend_from_slice(&v.to_le_bytes());
        }
        out.extend_from_slice(&tables);
        out.extend_from_slice(&self.data);
        Ok(out)
    }
}

fn le32(b: &[u8], at: usize) -> u32 {
    u32::from_le_bytes([b[at], b[at + 1], b[at + 2], b[at + 3]])
}

fn le64(b: &[u8], at: usize) -> u64 {
    u64::from(le32(b, at)) | (u64::from(le32(b, at + 4)) << 32)
}

/// A validated view of a `payloads.sealed` file.
#[derive(Clone, Copy)]
pub struct Sealed<'a> {
    bytes: &'a [u8],
    layout: Layout,
}

/// What [`Sealed::open`] learned about a file, to view the same bytes again without
/// re-validating them ([`Sealed::with_layout`]).
#[derive(Clone, Copy, Debug)]
pub struct Layout {
    frames: usize,
    entries: usize,
    data_start: usize,
}

impl<'a> Sealed<'a> {
    /// Checks the tables completely (CRC, order, bounds), so later lookups cannot fail on
    /// structure; frame data is checked when read ([`Sealed::check_frame`] for verify).
    pub fn open(bytes: &'a [u8]) -> Result<Sealed<'a>> {
        let extra = check_file_header(bytes, MAGIC_SEALED, "payloads.sealed")?;
        if bytes.len() < TABLES_AT {
            return Err(corrupt("payloads.sealed: truncated"));
        }
        if extra != 0 || le32(bytes, FILE_HEADER_LEN + 12) != 0 {
            return Err(corrupt("payloads.sealed: reserved fields set"));
        }
        let (frames, entries) = (
            le32(bytes, FILE_HEADER_LEN) as usize,
            le32(bytes, FILE_HEADER_LEN + 4) as usize,
        );
        let data_start = frames
            .checked_mul(FRAME_LEN)
            .and_then(|f| f.checked_add(entries.checked_mul(ENTRY_LEN)?))
            .and_then(|t| t.checked_add(TABLES_AT))
            .filter(|&d| d <= bytes.len())
            .ok_or_else(|| corrupt("payloads.sealed: truncated tables"))?;
        if crc32c::crc32c(&bytes[TABLES_AT..data_start]) != le32(bytes, FILE_HEADER_LEN + 8) {
            return Err(corrupt("payloads.sealed: bad table CRC"));
        }
        let s = Sealed {
            bytes,
            layout: Layout {
                frames,
                entries,
                data_start,
            },
        };
        let mut next = 0u64;
        for i in 0..frames {
            let f = s.frame(i);
            if f.offset != next || f.codec > codec::ZSTD as u32 {
                return Err(corrupt("payloads.sealed: bad frame table"));
            }
            next += f.len as u64;
        }
        if data_start as u64 + next != bytes.len() as u64 {
            return Err(corrupt("payloads.sealed: frame data length"));
        }
        let mut last = None;
        for i in 0..entries {
            let (off, l) = s.entry(i);
            let fits = (l.frame as usize) < frames
                && l.at as u64 + l.len as u64 <= s.frame(l.frame as usize).raw_len as u64;
            if !fits || last.is_some_and(|p| p >= off) {
                return Err(corrupt("payloads.sealed: bad entry table"));
            }
            last = Some(off);
        }
        Ok(s)
    }

    pub fn layout(&self) -> Layout {
        self.layout
    }

    /// Views `bytes` with the layout [`Sealed::open`] returned for exactly these bytes.
    /// Other bytes are an error.
    pub fn with_layout(bytes: &'a [u8], layout: Layout) -> Result<Sealed<'a>> {
        if bytes.len() < layout.data_start
            || le32(bytes, FILE_HEADER_LEN) as usize != layout.frames
            || le32(bytes, FILE_HEADER_LEN + 4) as usize != layout.entries
        {
            return Err(corrupt("payloads.sealed: layout does not match the bytes"));
        }
        Ok(Sealed { bytes, layout })
    }

    pub fn frame_count(&self) -> usize {
        self.layout.frames
    }

    pub fn entry_count(&self) -> usize {
        self.layout.entries
    }

    /// Frame `i` (`i < frame_count()`).
    pub fn frame(&self, i: usize) -> Frame {
        let at = TABLES_AT + i * FRAME_LEN;
        let b = self.bytes;
        Frame {
            offset: le64(b, at),
            len: le32(b, at + 8),
            raw_len: le32(b, at + 12),
            crc: le32(b, at + 16),
            codec: le32(b, at + 20),
        }
    }

    /// Entry `i` (`i < entry_count()`): `(blob offset, location)`.
    pub fn entry(&self, i: usize) -> (u64, Loc) {
        let at = TABLES_AT + self.layout.frames * FRAME_LEN + i * ENTRY_LEN;
        let b = self.bytes;
        let loc = Loc {
            frame: le32(b, at + 8),
            at: le32(b, at + 12),
            len: le32(b, at + 16),
            crc: le32(b, at + 20),
        };
        (le64(b, at), loc)
    }

    /// Where the payload stored at `blob_offset` in `payloads.blob` lives.
    pub fn find(&self, blob_offset: u64) -> Option<Loc> {
        let (mut lo, mut hi) = (0, self.layout.entries);
        while lo < hi {
            let mid = (lo + hi) / 2;
            let (off, loc) = self.entry(mid);
            match off.cmp(&blob_offset) {
                std::cmp::Ordering::Less => lo = mid + 1,
                std::cmp::Ordering::Greater => hi = mid,
                std::cmp::Ordering::Equal => return Some(loc),
            }
        }
        None
    }

    fn frame_data(&self, f: &Frame) -> &'a [u8] {
        let s = self.layout.data_start + f.offset as usize;
        &self.bytes[s..s + f.len as usize]
    }

    /// The raw bytes of frame `i`.
    pub fn decompress(&self, i: usize) -> Result<Vec<u8>> {
        let f = self.frame(i);
        let data = self.frame_data(&f);
        if f.codec == codec::RAW as u32 {
            if data.len() != f.raw_len as usize {
                return Err(corrupt("payloads.sealed: raw frame length"));
            }
            return Ok(data.to_vec());
        }
        let out = decompress(data, f.raw_len as usize)
            .map_err(|e| corrupt(format!("payloads.sealed: zstd: {e}")))?;
        if out.len() != f.raw_len as usize {
            return Err(corrupt("payloads.sealed: decompressed frame length"));
        }
        Ok(out)
    }

    /// Checks frame `i`'s CRC, decompression and the CRC of every payload in it.
    pub fn check_frame(&self, i: usize, raw: &[u8]) -> Result<()> {
        let f = self.frame(i);
        if crc32c::crc32c(self.frame_data(&f)) != f.crc {
            return Err(corrupt(format!("payloads.sealed: frame {i}: bad CRC")));
        }
        if raw.len() != f.raw_len as usize {
            return Err(corrupt(format!("payloads.sealed: frame {i}: length")));
        }
        Ok(())
    }

    /// The payload at `loc` within its frame's raw bytes, CRC-checked.
    pub fn slice<'r>(raw: &'r [u8], loc: Loc) -> Result<&'r [u8]> {
        let p = raw
            .get(loc.at as usize..loc.at as usize + loc.len as usize)
            .ok_or_else(|| corrupt("payloads.sealed: payload outside its frame"))?;
        if crc32c::crc32c(p) != loc.crc {
            return Err(corrupt("payloads.sealed: payload CRC"));
        }
        Ok(p)
    }

    /// Reads one payload (decompressing its whole frame).
    pub fn read(&self, blob_offset: u64) -> Result<Vec<u8>> {
        let loc = self.find(blob_offset).ok_or_else(|| {
            corrupt(format!(
                "payloads.sealed: no payload for offset {blob_offset}"
            ))
        })?;
        let raw = self.decompress(loc.frame as usize)?;
        Ok(Sealed::slice(&raw, loc)?.to_vec())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> (Vec<u8>, Vec<(u64, Vec<u8>)>) {
        let mut b = Builder::new(1024).unwrap();
        let mut payloads = Vec::new();
        for i in 0..40u64 {
            let p = format!("{{\"content\":\"refund policy {i}\"}}").repeat(1 + i as usize % 7);
            payloads.push((1000 - i * 10, p.into_bytes()));
        }
        payloads.push((5, b"x".repeat(5000))); // its own frame
        payloads.push((7, Vec::new()));
        for (o, p) in &payloads {
            b.add(*o, p).unwrap();
        }
        (b.finish().unwrap(), payloads)
    }

    #[test]
    fn round_trips_every_payload_and_frames_large_ones_alone() {
        let (file, payloads) = sample();
        let s = Sealed::open(&file).unwrap();
        assert_eq!(s.entry_count(), payloads.len());
        assert!(s.frame_count() > 3);
        for (o, p) in &payloads {
            assert_eq!(&s.read(*o).unwrap(), p, "offset {o}");
        }
        let big = s.find(5).unwrap();
        assert_eq!(s.frame(big.frame as usize).raw_len, 5000);
        assert!(s.find(6).is_none() && s.read(6).is_err());
        for i in 0..s.frame_count() {
            s.check_frame(i, &s.decompress(i).unwrap()).unwrap();
        }
        assert!(
            Builder::new(64)
                .and_then(|mut b| {
                    b.add(1, b"a")?;
                    b.add(1, b"b")?;
                    b.finish()
                })
                .is_err()
        );
    }

    #[test]
    fn damage_is_an_error_not_a_panic() {
        let (file, payloads) = sample();
        let tables_end = Sealed::open(&file).unwrap().layout.data_start;
        for cut in 0..file.len() {
            assert!(Sealed::open(&file[..cut]).is_err(), "cut {cut}");
        }
        for i in 0..file.len() {
            let mut bad = file.clone();
            bad[i] ^= 0x10;
            match Sealed::open(&bad) {
                // Damage in the header or tables is caught on open.
                Err(_) => assert!(i < tables_end || i >= file.len(), "{i}"),
                // Damage in frame data is caught when the frame is read or checked.
                Ok(s) => {
                    assert!(i >= tables_end, "flip {i} in the tables went unnoticed");
                    let caught = (0..s.frame_count()).any(|f| {
                        s.decompress(f)
                            .and_then(|raw| s.check_frame(f, &raw))
                            .is_err()
                    }) || payloads
                        .iter()
                        .any(|(o, p)| s.read(*o).ok().as_ref() != Some(p));
                    assert!(caught, "flip {i} in frame data went unnoticed");
                }
            }
        }
    }
}
