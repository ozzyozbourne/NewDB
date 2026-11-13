//! `fts`: full-text index over span `inputs` and `outputs`, per partition.
//!
//! Tokens come from `newdb_types::text::for_each_token` (the same tokenizer the oracle
//! uses). For each token, the index stores the spans of this partition that have at least
//! one post whose `inputs` or `outputs` contain it.
//!
//! Sealed form: an FST term dictionary mapping each token to a postings list, and postings
//! as roaring bitmaps over span positions in a sorted `(trace_id, span_id)` table.
//! Port the tokenizer-independent parts from Forge (`crates/forge-index`), and first find
//! why Forge's search on compacted segments took 765 ms against 19 ms on uncompacted ones.
//!
//! The index answers "which spans in this partition mention token T". The engine turns
//! that into search results: union per token across partitions, intersect across tokens,
//! then confirm each candidate against its merged inputs/outputs, because a later post can
//! replace a payload.

use newdb_types::{Result, SpanId, TraceId};

use crate::{Batch, Cost, FileSet, LayoutBuilder, LayoutFiles};

pub const NAME: &str = "fts";
pub const VERSION: u32 = 1;

#[derive(Default)]
pub struct Builder {
    _todo: (),
}

impl Builder {
    /// Spans of this partition mentioning `token` (already tokenized: lowercase ASCII),
    /// sorted ascending, without duplicates.
    pub fn postings(&self, token: &[u8]) -> (Vec<(TraceId, SpanId)>, Cost) {
        let _ = token;
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

    /// Same as [`Builder::postings`].
    pub fn postings(&self, token: &[u8]) -> (Vec<(TraceId, SpanId)>, Cost) {
        let _ = token;
        todo!("lane 2")
    }
}
