//! `runs_by_time`: one [`RunSummary`] per run candidate, newest first, plus the positions
//! of error runs (the `errors` index from DESIGN.md, stored in the same layout).
//!
//! A **run candidate** is a span whose merged header in this partition is a root
//! (`SpanHeader::is_root`). Because merging is per partition, a span's closing post can
//! arrive in a later partition without its parent field; it then appears here as a
//! candidate. The engine drops candidates whose cross-partition merge has a parent, and
//! merges a run's summaries across partitions using `RunSummary::fields`.
//!
//! Order: descending by `(start_ns, trace_id, span_id)`, matching `Store::runs`.

use newdb_types::{Result, RunSummary};

use crate::{Batch, Cost, FileSet, LayoutBuilder, LayoutFiles};

pub const NAME: &str = "runs_by_time";
pub const VERSION: u32 = 1;

#[derive(Default)]
pub struct Builder {
    _todo: (),
}

impl Builder {
    /// All run candidates of the partition so far, newest first.
    pub fn runs(&self) -> (Vec<RunSummary>, Cost) {
        todo!("lane 2")
    }

    /// Positions in [`Builder::runs`] of summaries with status error, ascending.
    pub fn error_positions(&self) -> (Vec<u32>, Cost) {
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

pub struct Reader<'a> {
    _files: FileSet<'a>,
}

impl<'a> Reader<'a> {
    pub fn open(files: FileSet<'a>) -> Result<Reader<'a>> {
        let _ = files;
        todo!("lane 2")
    }

    /// Same as [`Builder::runs`], borrowed from the mapped file.
    pub fn runs(&self) -> &'a [RunSummary] {
        todo!("lane 2")
    }

    /// Same as [`Builder::error_positions`].
    pub fn error_positions(&self) -> &'a [u32] {
        todo!("lane 2")
    }

    /// Cost of reading `n` summaries, for `ReadStats`.
    pub fn cost_of(&self, n: usize) -> Cost {
        Cost {
            records_visited: n as u64,
            bytes_read: (n * size_of::<RunSummary>()) as u64,
        }
    }
}
