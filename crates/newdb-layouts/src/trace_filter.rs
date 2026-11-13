//! `trace_filter`: a binary fuse filter (xorf `BinaryFuse8`) over the partition's trace IDs.
//! The engine keeps every sealed partition's filter in memory and opens only partitions
//! whose filter may contain the trace. No false negatives; about 0.4% false positives.
//!
//! Key: the first 8 bytes of the trace ID as a little-endian u64, XOR the last 8.
//!
//! File `trace_filter.bin` (framing in `file.rs`), two sections: the filter descriptor
//! (`seed` u64, `segment_length`, `segment_length_mask`, `segment_count_length` u32, 4 zero
//! bytes) and the fingerprints. A partition without traces has an empty descriptor and no
//! fingerprints. xorf builds from a fixed seed, so the same keys give the same bytes.

use std::collections::HashSet;

use newdb_types::{Error, Result, TraceId};
use xorf::{BinaryFuse8, Filter};

use crate::{Batch, FileSet, LayoutBuilder, LayoutFiles, file};

pub const NAME: &str = "trace_filter";
pub const VERSION: u32 = 1;

const FILE: &str = "trace_filter.bin";
const MAGIC: &[u8; 8] = b"NDBTFLT\0";

fn key(t: &TraceId) -> u64 {
    let (a, b) = t.0.split_at(8);
    let word = |s: &[u8]| s.iter().rev().fold(0u64, |acc, &x| (acc << 8) | x as u64);
    word(a) ^ word(b)
}

#[derive(Default)]
pub struct Builder {
    traces: HashSet<TraceId>,
}

impl Builder {
    /// Exact answer while active (the builder keeps the set itself).
    pub fn may_contain(&self, trace_id: TraceId) -> bool {
        self.traces.contains(&trace_id)
    }
}

impl LayoutBuilder for Builder {
    fn apply(&mut self, batch: &Batch<'_>) {
        self.traces.extend(batch.headers.iter().map(|h| h.trace_id));
    }

    fn seal(&self, out: &mut LayoutFiles) -> Result<()> {
        let mut keys: Vec<u64> = self.traces.iter().map(key).collect();
        keys.sort_unstable();
        keys.dedup();
        let mut descriptor = Vec::with_capacity(24);
        let mut fingerprints: &[u8] = &[];
        let filter;
        if !keys.is_empty() {
            filter = BinaryFuse8::try_from(&keys)
                .map_err(|e| Error::InvalidInput(format!("{FILE}: {e}")))?;
            let d = &filter.descriptor;
            descriptor.extend_from_slice(&d.seed.to_le_bytes());
            for v in [
                d.segment_length,
                d.segment_length_mask,
                d.segment_count_length,
                0,
            ] {
                descriptor.extend_from_slice(&v.to_le_bytes());
            }
            fingerprints = &filter.fingerprints;
        }
        out.add(
            FILE,
            file::write(MAGIC, VERSION, &[&descriptor, fingerprints]),
        );
        Ok(())
    }
}

/// Owns its filter (small), so the engine can keep it after unmapping the files.
pub struct Reader {
    /// `None` for a partition without traces.
    filter: Option<BinaryFuse8>,
}

impl Reader {
    pub fn open(files: FileSet<'_>) -> Result<Reader> {
        let [d, fingerprints] = file::read(files.get(FILE)?, MAGIC, VERSION, FILE)?;
        if d.is_empty() && fingerprints.is_empty() {
            return Ok(Reader { filter: None });
        }
        let corrupt = || Error::Corrupt(format!("{FILE}: bad filter descriptor"));
        let d: &[u8; 24] = d.try_into().map_err(|_| corrupt())?;
        let u32_at = |i: usize| u32::from_le_bytes([d[i], d[i + 1], d[i + 2], d[i + 3]]);
        let mut seed = [0u8; 8];
        seed.copy_from_slice(&d[..8]);
        let descriptor = xorf::Descriptor {
            seed: u64::from_le_bytes(seed),
            segment_length: u32_at(8),
            segment_length_mask: u32_at(12),
            segment_count_length: u32_at(16),
        };
        // `contains` indexes up to `segment_count_length + 2 * segment_length` (exclusive);
        // check it here so corrupt bytes cannot make it index out of bounds.
        let (len, count) = (descriptor.segment_length, descriptor.segment_count_length);
        let ok = len.is_power_of_two()
            && descriptor.segment_length_mask == len - 1
            && count % len == 0
            && (count as u64 + 2 * len as u64) <= fingerprints.len() as u64
            && fingerprints.len() <= u32::MAX as usize;
        if !ok {
            return Err(corrupt());
        }
        Ok(Reader {
            filter: Some(BinaryFuse8 {
                descriptor,
                fingerprints: fingerprints.into(),
            }),
        })
    }

    pub fn may_contain(&self, trace_id: TraceId) -> bool {
        self.filter
            .as_ref()
            .is_some_and(|f| f.contains(&key(&trace_id)))
    }
}
