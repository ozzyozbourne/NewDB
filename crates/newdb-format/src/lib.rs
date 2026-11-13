//! Engine file formats (DESIGN.md sections 6.1–6.3). Only `newdb-engine` uses this crate,
//! and both belong to lane 1, so lane 1 may change anything here. Format changes bump
//! [`FORMAT_VERSION`].
//!
//! - [`log`]: `log.seg` records: `len u32 | crc32c u32 | kind u8 | seq u64 | body`. A span
//!   post's body is its `SpanHeader`; strings are defined by string records before the
//!   first post that uses them, and payloads are written to the payload store first and
//!   referenced. Torn-tail detection on open (first bad len or CRC ends the log).
//! - [`payload`]: `payloads.blob`: zstd frames addressed by xxh3-128, deduplicated within a
//!   partition with a byte comparison on hash hits. Reads return the original bytes.
//! - [`sealed`]: `payloads.sealed`: a sealed partition's payloads in larger frames, in
//!   trace order, with a map from `payloads.blob` offsets.
//! - [`strings`]: `strings.tbl`: partition-local string table; ID 0 is "no string".
//! - [`overlap`]: `overlap.bin`: traces of a sealed partition that earlier partitions share.
//! - [`manifest`]: `MANIFEST`: format version, partitions with min/max span start, layout
//!   versions per partition. Replaced atomically (write temp, fsync, rename, fsync dir).
//!
//! No serde, no proc-macros: encoding is written by hand, little-endian.

pub mod log;
pub mod manifest;
pub mod overlap;
pub mod payload;
pub mod sealed;
pub mod strings;

use newdb_types::{Error, Result};

pub const FORMAT_VERSION: u32 = 1;

/// First 8 bytes of every engine file.
pub const MAGIC_LOG: [u8; 8] = *b"NEWDBLOG";
pub const MAGIC_PAYLOADS: [u8; 8] = *b"NEWDBPAY";
pub const MAGIC_STRINGS: [u8; 8] = *b"NEWDBSTR";
pub const MAGIC_MANIFEST: [u8; 8] = *b"NEWDBMAN";
pub const MAGIC_OVERLAP: [u8; 8] = *b"NEWDBOVL";
pub const MAGIC_SEALED: [u8; 8] = *b"NEWDBPSL";

/// Every engine file starts with `magic [8] | FORMAT_VERSION u32 | extra u32`.
pub const FILE_HEADER_LEN: usize = 16;

pub(crate) fn file_header(magic: [u8; 8], extra: u32) -> [u8; FILE_HEADER_LEN] {
    let mut h = [0u8; FILE_HEADER_LEN];
    h[..8].copy_from_slice(&magic);
    h[8..12].copy_from_slice(&FORMAT_VERSION.to_le_bytes());
    h[12..].copy_from_slice(&extra.to_le_bytes());
    h
}

/// Checks magic and version; returns the `extra` field.
pub(crate) fn check_file_header(bytes: &[u8], magic: [u8; 8], what: &str) -> Result<u32> {
    let mut c = Cursor::new(bytes, what);
    if c.take(8)? != magic {
        return Err(corrupt(format!("{what}: bad magic")));
    }
    let version = c.u32()?;
    if version != FORMAT_VERSION {
        return Err(corrupt(format!(
            "{what}: format version {version}, this build reads {FORMAT_VERSION}"
        )));
    }
    c.u32()
}

pub(crate) fn corrupt(msg: impl Into<String>) -> Error {
    Error::Corrupt(msg.into())
}

/// Bounds-checked little-endian reader over stored bytes.
pub(crate) struct Cursor<'a> {
    bytes: &'a [u8],
    pos: usize,
    what: &'a str,
}

impl<'a> Cursor<'a> {
    pub(crate) fn new(bytes: &'a [u8], what: &'a str) -> Cursor<'a> {
        Cursor {
            bytes,
            pos: 0,
            what,
        }
    }

    pub(crate) fn take(&mut self, n: usize) -> Result<&'a [u8]> {
        let end = self
            .pos
            .checked_add(n)
            .filter(|&e| e <= self.bytes.len())
            .ok_or_else(|| corrupt(format!("{}: truncated at byte {}", self.what, self.pos)))?;
        let out = &self.bytes[self.pos..end];
        self.pos = end;
        Ok(out)
    }

    fn array<const N: usize>(&mut self) -> Result<[u8; N]> {
        let mut out = [0u8; N];
        out.copy_from_slice(self.take(N)?);
        Ok(out)
    }

    pub(crate) fn u8(&mut self) -> Result<u8> {
        Ok(self.array::<1>()?[0])
    }

    pub(crate) fn u16(&mut self) -> Result<u16> {
        Ok(u16::from_le_bytes(self.array()?))
    }

    pub(crate) fn u32(&mut self) -> Result<u32> {
        Ok(u32::from_le_bytes(self.array()?))
    }

    pub(crate) fn u64(&mut self) -> Result<u64> {
        Ok(u64::from_le_bytes(self.array()?))
    }

    pub(crate) fn str(&mut self, n: usize) -> Result<&'a str> {
        let what = self.what;
        std::str::from_utf8(self.take(n)?)
            .map_err(|_| corrupt(format!("{what}: string is not UTF-8")))
    }

    pub(crate) fn remaining(&self) -> usize {
        self.bytes.len() - self.pos
    }
}
