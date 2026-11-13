//! Engine file formats (DESIGN.md sections 6.1–6.3). Only `newdb-engine` uses this crate,
//! and both belong to lane 1, so lane 1 may change anything here. Format changes bump
//! [`FORMAT_VERSION`].
//!
//! Planned modules:
//! - `log`: `log.seg` records: `len u32 | crc32c u32 | kind u8 | seq u64 | body`. Body of a
//!   span post: the `SpanHeader` plus inline strings; payloads are written to the payload
//!   store first and referenced. Torn-tail detection on open (first bad len or CRC ends
//!   the log).
//! - `payload`: `payloads.blob`: zstd frames addressed by xxh3-128, deduplicated within a
//!   partition with a byte comparison on hash hits. Reads return the original bytes.
//! - `strings`: `strings.tbl`: partition-local string table; ID 0 is "no string".
//! - `manifest`: `MANIFEST`: format version, partitions with min/max span start, layout
//!   versions per partition. Replaced atomically (write temp, fsync, rename, fsync dir).
//!
//! No serde, no proc-macros: encoding is written by hand.

pub const FORMAT_VERSION: u32 = 1;

/// First 8 bytes of every engine file.
pub const MAGIC_LOG: [u8; 8] = *b"NEWDBLOG";
pub const MAGIC_PAYLOADS: [u8; 8] = *b"NEWDBPAY";
pub const MAGIC_STRINGS: [u8; 8] = *b"NEWDBSTR";
pub const MAGIC_MANIFEST: [u8; 8] = *b"NEWDBMAN";
