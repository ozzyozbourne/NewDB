//! `trace_filter`: a binary fuse filter (xorf `BinaryFuse8`) over the partition's trace IDs.
//! The engine keeps every sealed partition's filter in memory and opens only partitions
//! whose filter may contain the trace. No false negatives; about 0.4% false positives.
//!
//! Key: the first 8 bytes of the trace ID as a little-endian u64, XOR the last 8.

use newdb_types::{Result, TraceId};

use crate::{Batch, FileSet, LayoutBuilder, LayoutFiles};

pub const NAME: &str = "trace_filter";
pub const VERSION: u32 = 1;

#[derive(Default)]
pub struct Builder {
    _todo: (),
}

impl Builder {
    /// Exact answer while active (the builder keeps the set itself).
    pub fn may_contain(&self, trace_id: TraceId) -> bool {
        let _ = trace_id;
        todo!("lane 2")
    }
}

impl LayoutBuilder for Builder {
    fn apply(&mut self, batch: &Batch<'_>) {
        let _ = batch;
        todo!("lane 2")
    }

    fn seal(&self, out: &mut LayoutFiles) -> Result<()> {
        let _ = out;
        todo!("lane 2")
    }
}

/// Owns its filter (small), so the engine can keep it after unmapping the files.
pub struct Reader {
    _todo: (),
}

impl Reader {
    pub fn open(files: FileSet<'_>) -> Result<Reader> {
        let _ = files;
        todo!("lane 2")
    }

    pub fn may_contain(&self, trace_id: TraceId) -> bool {
        let _ = trace_id;
        todo!("lane 2")
    }
}
