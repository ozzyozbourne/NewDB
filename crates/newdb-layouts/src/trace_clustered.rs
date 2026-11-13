//! `trace_clustered`: every span of the partition, merged within the partition, sorted by
//! `(trace_id, start_ns, span_id)`, plus a sorted index from trace_id to its range. A trace
//! load is one binary search and one sequential read.
//!
//! Merging: posts of the same `(project, trace_id, span_id)` within the partition are
//! merged with `SpanHeader::merge_from` in log order. The sort uses the merged `start_ns`
//! (0 when no post in this partition set it).

use newdb_types::{Result, SpanHeader, TraceId};

use crate::{Batch, Cost, FileSet, LayoutBuilder, LayoutFiles};

pub const NAME: &str = "trace_clustered";
pub const VERSION: u32 = 1;

#[derive(Default)]
pub struct Builder {
    _todo: (),
}

impl Builder {
    /// Merged headers of `trace_id` in this partition, sorted by `(start_ns, span_id)`.
    /// All projects; the engine filters by project string ID.
    pub fn trace(&self, trace_id: TraceId) -> (Vec<SpanHeader>, Cost) {
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

pub struct Reader<'a> {
    _files: FileSet<'a>,
}

impl<'a> Reader<'a> {
    pub fn open(files: FileSet<'a>) -> Result<Reader<'a>> {
        let _ = files;
        todo!("lane 2")
    }

    /// Same answer as [`Builder::trace`], borrowed from the mapped file.
    pub fn trace(&self, trace_id: TraceId) -> (&'a [SpanHeader], Cost) {
        let _ = trace_id;
        todo!("lane 2")
    }

    /// Number of spans in the partition.
    pub fn len(&self) -> usize {
        todo!("lane 2")
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}
