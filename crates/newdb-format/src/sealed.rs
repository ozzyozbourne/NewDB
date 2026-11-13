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

use std::io::{Seek, SeekFrom, Write};

use newdb_types::{PayloadRef, Result};

use crate::payload::{codec, compress, decompress_into};
use crate::{FILE_HEADER_LEN, MAGIC_SEALED, check_file_header, corrupt, file_header};

const FRAME_LEN: usize = 24;
const ENTRY_LEN: usize = 24;
const TABLES_AT: usize = FILE_HEADER_LEN + 16;

/// Bytes of the file header, before the tables.
pub const HEADER_LEN: usize = TABLES_AT;

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

/// Which payloads go in which frame, decided from their lengths alone so that frames can
/// then be filled and compressed in any order, on any number of threads, with identical
/// output.
pub struct Plan {
    target: usize,
    /// Payloads in layout order.
    items: Vec<PayloadRef>,
    /// `items` index where each closed frame ends.
    ends: Vec<usize>,
    cur: usize,
}

impl Plan {
    pub fn new(target: usize) -> Plan {
        Plan {
            target: target.max(1),
            items: Vec::new(),
            ends: Vec::new(),
            cur: 0,
        }
    }

    fn open(&self) -> bool {
        self.ends.last().copied().unwrap_or(0) < self.items.len()
    }

    fn close(&mut self) {
        if self.open() {
            self.ends.push(self.items.len());
            self.cur = 0;
        }
    }

    /// Starts a group of payloads (one trace) totalling `raw_len` bytes. A group that fits
    /// in a frame is not split across two, and a group starts a new frame once the current
    /// one holds an eighth of the target, so a small trace decompresses about its own
    /// bytes while a large one fills whole frames.
    pub fn start_group(&mut self, raw_len: usize) {
        if self.cur >= self.target / 8 || self.cur + raw_len > self.target {
            self.close();
        }
    }

    /// Adds a payload of `payloads.blob`. Each offset at most once.
    pub fn add(&mut self, r: PayloadRef) {
        let len = r.raw_len as usize;
        let big = len >= self.target / 2;
        if big || self.cur + len > self.target {
            self.close();
        }
        self.items.push(r);
        self.cur += len;
        if big {
            self.close();
        }
    }

    /// Adds a payload in a frame of its own.
    pub fn add_alone(&mut self, r: PayloadRef) {
        self.close();
        self.items.push(r);
        self.close();
    }

    pub fn finish(mut self) -> Plan {
        self.close();
        self
    }

    pub fn frame_count(&self) -> usize {
        self.ends.len()
    }

    /// The payloads of frame `i`, in order.
    pub fn frame(&self, i: usize) -> &[PayloadRef] {
        let start = if i == 0 { 0 } else { self.ends[i - 1] };
        &self.items[start..self.ends[i]]
    }
}

/// One frame, compressed, with the CRC of each payload in it.
pub struct FrameData {
    codec: u32,
    raw_len: u32,
    data: Vec<u8>,
    crcs: Vec<u32>,
}

impl Plan {
    /// Bytes of the header and tables, which come before the frame data.
    pub fn tables_len(&self) -> usize {
        TABLES_AT + self.frame_count() * FRAME_LEN + self.items.len() * ENTRY_LEN
    }
}

/// Fills and compresses frame `i` of `plan`. `read` appends one payload's raw bytes to
/// the buffer it is given. Frames are independent, so any thread may build any frame.
pub fn build_frame(
    plan: &Plan,
    i: usize,
    read: &mut dyn FnMut(PayloadRef, &mut Vec<u8>) -> Result<()>,
) -> Result<FrameData> {
    let refs = plan.frame(i);
    let raw_len: u64 = refs.iter().map(|r| r.raw_len as u64).sum();
    let raw_len =
        u32::try_from(raw_len).map_err(|_| corrupt("sealed payloads: frame larger than 4 GiB"))?;
    let mut buf = Vec::with_capacity(raw_len as usize);
    let mut crcs = Vec::with_capacity(refs.len());
    for r in refs {
        let at = buf.len();
        read(*r, &mut buf)?;
        if buf.len() - at != r.raw_len as usize {
            return Err(corrupt("sealed payloads: payload length differs"));
        }
        crcs.push(crc32c::crc32c(&buf[at..]));
    }
    let c = compress(&buf)?;
    let (codec, data) = match c.len() < buf.len() {
        true => (codec::ZSTD as u32, c),
        false => (codec::RAW as u32, buf),
    };
    Ok(FrameData {
        codec,
        raw_len,
        data,
        crcs,
    })
}

/// Writes the file for `plan` to `w`, taking the built frames in order: their data as
/// they arrive, then the header and tables, which go first in the file, by seeking back.
/// Only the tables and one frame at a time are held in memory.
pub fn write<W: Write + Seek>(
    plan: &Plan,
    frames: impl IntoIterator<Item = Result<FrameData>>,
    w: &mut W,
) -> Result<()> {
    let mismatch =
        || newdb_types::Error::InvalidInput("sealed payloads: frames do not match the plan".into());
    let mut table = Vec::with_capacity(plan.frame_count() * FRAME_LEN);
    let mut entries: Vec<(u64, Loc)> = Vec::with_capacity(plan.items.len());
    w.seek(SeekFrom::Start(plan.tables_len() as u64))?;
    let mut offset = 0u64;
    for (i, f) in frames.into_iter().enumerate() {
        let f = f?;
        if i >= plan.frame_count() || f.crcs.len() != plan.frame(i).len() {
            return Err(mismatch());
        }
        let mut at = 0u32;
        for (r, &crc) in plan.frame(i).iter().zip(&f.crcs) {
            let loc = Loc {
                frame: i as u32,
                at,
                len: r.raw_len,
                crc,
            };
            entries.push((r.offset, loc));
            at += r.raw_len;
        }
        let len = u32::try_from(f.data.len())
            .map_err(|_| corrupt("sealed payloads: frame larger than 4 GiB"))?;
        table.extend_from_slice(&offset.to_le_bytes());
        for v in [len, f.raw_len, crc32c::crc32c(&f.data), f.codec] {
            table.extend_from_slice(&v.to_le_bytes());
        }
        w.write_all(&f.data)?;
        offset += f.data.len() as u64;
    }
    if table.len() != plan.frame_count() * FRAME_LEN {
        return Err(mismatch());
    }
    entries.sort_unstable_by_key(|e| e.0);
    if entries.windows(2).any(|w| w[0].0 == w[1].0) {
        return Err(newdb_types::Error::InvalidInput(
            "sealed payloads: an offset was added twice".into(),
        ));
    }
    for (off, l) in &entries {
        table.extend_from_slice(&off.to_le_bytes());
        for v in [l.frame, l.at, l.len, l.crc] {
            table.extend_from_slice(&v.to_le_bytes());
        }
    }
    let mut head = file_header(MAGIC_SEALED, 0).to_vec();
    for v in [
        plan.frame_count() as u32,
        entries.len() as u32,
        crc32c::crc32c(&table),
        0,
    ] {
        head.extend_from_slice(&v.to_le_bytes());
    }
    w.seek(SeekFrom::Start(0))?;
    w.write_all(&head)?;
    w.write_all(&table)?;
    w.seek(SeekFrom::End(0))?;
    Ok(())
}

#[inline]
fn le32(b: &[u8], at: usize) -> u32 {
    let mut v = [0; 4];
    v.copy_from_slice(&b[at..at + 4]);
    u32::from_le_bytes(v)
}

#[inline]
fn le64(b: &[u8], at: usize) -> u64 {
    let mut v = [0; 8];
    v.copy_from_slice(&b[at..at + 8]);
    u64::from_le_bytes(v)
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
    file_len: u64,
}

impl<'a> Sealed<'a> {
    /// Checks the tables completely (CRC, order, bounds), so later lookups cannot fail on
    /// structure; frame data is checked when read ([`Sealed::verify`] checks all of it).
    pub fn open(bytes: &'a [u8]) -> Result<Sealed<'a>> {
        let layout = Sealed::check_tables(bytes, bytes.len() as u64)?;
        Ok(Sealed { bytes, layout })
    }

    /// Where the frame data starts (the length of the header and tables) of a file whose
    /// first [`HEADER_LEN`] bytes are `head`.
    pub fn tables_end(head: &[u8]) -> Result<usize> {
        let extra = check_file_header(head, MAGIC_SEALED, "payloads.sealed")?;
        if head.len() < TABLES_AT {
            return Err(corrupt("payloads.sealed: truncated"));
        }
        if extra != 0 || le32(head, FILE_HEADER_LEN + 12) != 0 {
            return Err(corrupt("payloads.sealed: reserved fields set"));
        }
        let (frames, entries) = (
            le32(head, FILE_HEADER_LEN) as usize,
            le32(head, FILE_HEADER_LEN + 4) as usize,
        );
        frames
            .checked_mul(FRAME_LEN)
            .and_then(|f| f.checked_add(entries.checked_mul(ENTRY_LEN)?))
            .and_then(|t| t.checked_add(TABLES_AT))
            .ok_or_else(|| corrupt("payloads.sealed: truncated tables"))
    }

    /// Validates a file of `file_len` bytes from its first bytes `prefix`, which must hold
    /// the header and tables ([`Sealed::tables_end`] bytes); the frame data is not needed.
    /// The engine reads this prefix with a plain read, so validating does not keep the
    /// tables resident in a mapping. The layout is for [`Sealed::with_layout`].
    pub fn check_tables(prefix: &[u8], file_len: u64) -> Result<Layout> {
        let data_start = Sealed::tables_end(prefix)?;
        if data_start > prefix.len() || data_start as u64 > file_len {
            return Err(corrupt("payloads.sealed: truncated tables"));
        }
        if crc32c::crc32c(&prefix[TABLES_AT..data_start]) != le32(prefix, FILE_HEADER_LEN + 8) {
            return Err(corrupt("payloads.sealed: bad table CRC"));
        }
        let layout = Layout {
            frames: le32(prefix, FILE_HEADER_LEN) as usize,
            entries: le32(prefix, FILE_HEADER_LEN + 4) as usize,
            data_start,
            file_len,
        };
        // One pass over each table as fixed-size records. `tables_end` computed
        // `data_start` from the two counts, so the slices are exactly the tables.
        let frames_end = TABLES_AT + layout.frames * FRAME_LEN;
        let (frames, _) = prefix[TABLES_AT..frames_end].as_chunks::<FRAME_LEN>();
        let (entries, _) = prefix[frames_end..data_start].as_chunks::<ENTRY_LEN>();
        let mut next = 0u64;
        for f in frames {
            if le64(f, 0) != next || le32(f, 20) > codec::ZSTD as u32 {
                return Err(corrupt("payloads.sealed: bad frame table"));
            }
            next += le32(f, 8) as u64;
        }
        if data_start as u64 + next != file_len {
            return Err(corrupt("payloads.sealed: frame data length"));
        }
        let mut last = None;
        for e in entries {
            let (off, frame, at, len) = (le64(e, 0), le32(e, 8), le32(e, 12), le32(e, 16));
            let fits = frames
                .get(frame as usize)
                .is_some_and(|f| at as u64 + len as u64 <= le32(f, 12) as u64);
            if !fits || last.is_some_and(|p| p >= off) {
                return Err(corrupt("payloads.sealed: bad entry table"));
            }
            last = Some(off);
        }
        Ok(layout)
    }

    pub fn layout(&self) -> Layout {
        self.layout
    }

    /// Views `bytes` with the layout [`Sealed::open`] returned for exactly these bytes.
    /// Other bytes are an error.
    pub fn with_layout(bytes: &'a [u8], layout: Layout) -> Result<Sealed<'a>> {
        if bytes.len() as u64 != layout.file_len
            || bytes.len() < layout.data_start
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
    #[inline]
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
    #[inline]
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

    /// Where the payload stored at `blob_offset` in `payloads.blob` lives: a binary
    /// search over the entries' offsets.
    pub fn find(&self, blob_offset: u64) -> Option<Loc> {
        self.find_from(0, blob_offset).1
    }

    /// [`Sealed::find`] searching only entries from index `from`, which must not be past
    /// the entry for `blob_offset`. Returns where the search ended, to pass as `from` for
    /// the next larger offset: looking up offsets in ascending order this way gallops
    /// forward instead of searching the whole table each time.
    pub fn find_from(&self, from: usize, blob_offset: u64) -> (usize, Option<Loc>) {
        let base = TABLES_AT + self.layout.frames * FRAME_LEN;
        let off = |i: usize| le64(self.bytes, base + i * ENTRY_LEN);
        let n = self.layout.entries;
        let mut lo = from.min(n);
        let mut step = 1;
        let mut hi = lo;
        while hi < n && off(hi) < blob_offset {
            lo = hi + 1;
            hi = (hi + step).min(n);
            step *= 2;
        }
        let mut hi = (hi + 1).min(n);
        while lo < hi {
            let mid = (lo + hi) / 2;
            if off(mid) < blob_offset {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        (
            lo,
            (lo < n && off(lo) == blob_offset).then(|| self.entry(lo).1),
        )
    }

    fn frame_data(&self, f: &Frame) -> &'a [u8] {
        let s = self.layout.data_start + f.offset as usize;
        &self.bytes[s..s + f.len as usize]
    }

    /// The raw bytes of frame `i`, after checking the CRC of its stored bytes.
    pub fn decompress(&self, i: usize) -> Result<Vec<u8>> {
        let mut out = Vec::new();
        self.decompress_into(i, &mut out)?;
        Ok(out)
    }

    /// [`Sealed::decompress`] into `out`, replacing its contents and reusing its capacity.
    pub fn decompress_into(&self, i: usize, out: &mut Vec<u8>) -> Result<()> {
        let f = self.frame(i);
        let data = self.frame_data(&f);
        if crc32c::crc32c(data) != f.crc {
            return Err(corrupt(format!("payloads.sealed: frame {i}: bad CRC")));
        }
        out.clear();
        if f.codec == codec::RAW as u32 {
            if data.len() != f.raw_len as usize {
                return Err(corrupt("payloads.sealed: raw frame length"));
            }
            out.extend_from_slice(data);
            return Ok(());
        }
        out.resize(f.raw_len as usize, 0);
        decompress_into(data, out).map_err(|e| {
            out.clear();
            corrupt(format!("payloads.sealed: zstd: {e}"))
        })
    }

    /// The payload at `loc` within its frame's raw bytes. The frame's CRC was checked when
    /// it was decompressed; [`Sealed::verify`] also checks each payload's own CRC.
    pub fn get(raw: &[u8], loc: Loc) -> Result<&[u8]> {
        raw.get(loc.at as usize..loc.at as usize + loc.len as usize)
            .ok_or_else(|| corrupt("payloads.sealed: payload outside its frame"))
    }

    /// The payload at `loc`, with its CRC checked.
    pub fn slice(raw: &[u8], loc: Loc) -> Result<&[u8]> {
        let p = Sealed::get(raw, loc)?;
        if crc32c::crc32c(p) != loc.crc {
            return Err(corrupt("payloads.sealed: payload CRC"));
        }
        Ok(p)
    }

    /// Decompresses every frame once and checks every CRC.
    pub fn verify(&self) -> Result<()> {
        let mut by_frame: Vec<(u32, usize)> = (0..self.entry_count())
            .map(|i| (self.entry(i).1.frame, i))
            .collect();
        by_frame.sort_unstable();
        let mut next = by_frame.iter().peekable();
        for f in 0..self.frame_count() {
            let raw = self.decompress(f)?;
            while let Some(&(_, i)) = next.next_if(|e| e.0 as usize == f) {
                Sealed::slice(&raw, self.entry(i).1)?;
            }
        }
        Ok(())
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

    fn r(offset: u64, raw_len: usize) -> PayloadRef {
        PayloadRef {
            offset,
            len: 1,
            raw_len: raw_len as u32,
        }
    }

    /// Payloads keyed by their (fake) blob offset, with groups, a large payload and an
    /// empty one; frames built in `order`.
    fn sample(reverse: bool) -> (Vec<u8>, Vec<(u64, Vec<u8>)>) {
        let mut payloads = Vec::new();
        for i in 0..40u64 {
            let p = format!("{{\"content\":\"refund policy {i}\"}}").repeat(1 + i as usize % 7);
            payloads.push((1000 - i * 10, p.into_bytes()));
        }
        payloads.push((5, b"x".repeat(5000))); // its own frame
        payloads.push((7, Vec::new()));
        let mut plan = Plan::new(1024);
        for (k, (o, p)) in payloads.iter().enumerate() {
            if k % 5 == 0 {
                let group: usize = payloads[k..(k + 5).min(payloads.len())]
                    .iter()
                    .map(|p| p.1.len())
                    .sum();
                plan.start_group(group);
            }
            plan.add(r(*o, p.len()));
        }
        let plan = plan.finish();
        let by_offset: std::collections::HashMap<u64, Vec<u8>> = payloads.iter().cloned().collect();
        let mut read = |r: PayloadRef, out: &mut Vec<u8>| {
            out.extend_from_slice(&by_offset[&r.offset]);
            Ok(())
        };
        // Frames are independent: building them in another order changes nothing.
        let mut idx: Vec<usize> = (0..plan.frame_count()).collect();
        if reverse {
            idx.reverse();
        }
        let mut built: Vec<(usize, FrameData)> = idx
            .into_iter()
            .map(|i| (i, build_frame(&plan, i, &mut read).unwrap()))
            .collect();
        built.sort_by_key(|b| b.0);
        let mut file = std::io::Cursor::new(Vec::new());
        write(&plan, built.into_iter().map(|b| Ok(b.1)), &mut file).unwrap();
        (file.into_inner(), payloads)
    }

    #[test]
    fn round_trips_every_payload() {
        let (file, payloads) = sample(false);
        assert!(sample(true).0 == file);
        let s = Sealed::open(&file).unwrap();
        assert_eq!(s.entry_count(), payloads.len());
        assert!(s.frame_count() > 3);
        for (o, p) in &payloads {
            assert_eq!(&s.read(*o).unwrap(), p, "offset {o}");
        }
        // A large payload has a frame to itself.
        let big = s.find(5).unwrap();
        assert_eq!(s.frame(big.frame as usize).raw_len, 5000);
        for missing in [0, 6, 8, 999, 1001, u64::MAX] {
            assert!(s.find(missing).is_none() && s.read(missing).is_err());
        }
        // Ascending lookups from the previous position find the same entries.
        let mut offsets: Vec<u64> = payloads.iter().map(|p| p.0).chain([6, 999, 2000]).collect();
        offsets.sort();
        let mut at = 0;
        for o in offsets {
            let (next, loc) = s.find_from(at, o);
            assert_eq!(loc, s.find(o), "offset {o}");
            assert!(next >= at);
            at = next;
        }
        s.verify().unwrap();
        // A group that fits in a frame is not split: payloads 1000..=960 share a frame.
        let f = s.find(1000).unwrap().frame;
        assert!(
            [990, 980, 970, 960]
                .iter()
                .all(|&o| s.find(o).unwrap().frame == f)
        );

        let mut plan = Plan::new(64);
        plan.add(r(1, 1));
        plan.add(r(1, 1));
        let plan = plan.finish();
        let mut one = |_: PayloadRef, out: &mut Vec<u8>| {
            out.push(0);
            Ok(())
        };
        let frames = (0..plan.frame_count()).map(|i| build_frame(&plan, i, &mut one));
        let frames: Vec<_> = frames.collect();
        let mut out = std::io::Cursor::new(Vec::new());
        assert!(write(&plan, frames, &mut out).is_err());
        let mut two = |_: PayloadRef, out: &mut Vec<u8>| {
            out.extend_from_slice(&[0, 0]);
            Ok(())
        };
        assert!(build_frame(&plan, 0, &mut two).is_err());
        assert!(write(&plan, std::iter::empty(), &mut out).is_err());
    }

    #[test]
    fn damage_is_an_error_not_a_panic() {
        let (file, payloads) = sample(false);
        // Validating from just the header and tables gives the same layout.
        let end = Sealed::tables_end(&file[..HEADER_LEN]).unwrap();
        let l = Sealed::check_tables(&file[..end], file.len() as u64).unwrap();
        assert!(Sealed::with_layout(&file, l).is_ok());
        assert!(Sealed::with_layout(&file[..file.len() - 1], l).is_err());
        assert!(Sealed::check_tables(&file[..end - 1], file.len() as u64).is_err());
        assert!(Sealed::check_tables(&file[..end], file.len() as u64 + 1).is_err());
        let tables_end = Sealed::open(&file).unwrap().layout.data_start;
        for cut in 0..file.len() {
            assert!(Sealed::open(&file[..cut]).is_err(), "cut {cut}");
        }
        for i in 0..file.len() {
            let mut bad = file.clone();
            bad[i] ^= 0x10;
            match Sealed::open(&bad) {
                // Damage in the header or tables is caught on open.
                Err(_) => assert!(i < tables_end, "{i}"),
                // Damage in frame data is caught when the frame is read.
                Ok(s) => {
                    assert!(i >= tables_end, "flip {i} in the tables went unnoticed");
                    assert!(s.verify().is_err(), "flip {i}: verify");
                    let caught = payloads
                        .iter()
                        .any(|(o, p)| s.read(*o).ok().as_ref() != Some(p));
                    assert!(caught, "flip {i} in frame data went unnoticed");
                }
            }
        }
    }
}
