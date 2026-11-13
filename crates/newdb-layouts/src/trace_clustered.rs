//! `trace_clustered`: every span of the partition, merged within the partition, sorted by
//! `(trace_id, start_ns, span_id)`, plus a sorted index from trace_id to its range. A trace
//! load is one binary search and one sequential read.
//!
//! Merging: posts of the same `(project, trace_id, span_id)` within the partition are
//! merged with `SpanHeader::merge_from` in log order. The sort uses the merged `start_ns`
//! (0 when no post in this partition set it). Spans of two projects that share a trace and
//! span ID stay separate and sort by project last.
//!
//! File `trace_clustered.bin` (framing in `file.rs`), three sections:
//! 1. `[SpanHeader]`, sorted by `(trace_id, start_ns, span_id, project)`;
//! 2. `[TraceId]`, the distinct trace IDs, ascending;
//! 3. `[u32]`, one more than section 2: trace `i` is spans `starts[i]..starts[i + 1]`.

use std::collections::HashMap;
use std::collections::hash_map::Entry;

use newdb_types::{Error, Result, SpanHeader, SpanId, TraceId};

use crate::{Batch, Cost, FileSet, LayoutBuilder, LayoutFiles, file};

pub const NAME: &str = "trace_clustered";
pub const VERSION: u32 = 1;

const FILE: &str = "trace_clustered.bin";
const MAGIC: &[u8; 8] = b"NDBTRCL\0";

#[derive(Default)]
pub struct Builder {
    /// Merged headers in order of first arrival.
    spans: Vec<SpanHeader>,
    by_key: HashMap<(u32, TraceId, SpanId), u32>,
    by_trace: HashMap<TraceId, Vec<u32>>,
}

/// Order of spans within a trace: `(start_ns, span_id)`, then project for ties.
fn in_trace_order(h: &SpanHeader) -> (u64, SpanId, u32) {
    (h.start_ns, h.span_id, h.project)
}

fn cost_of(spans: usize) -> Cost {
    Cost {
        records_visited: spans as u64,
        bytes_read: (spans * size_of::<SpanHeader>()) as u64,
    }
}

impl Builder {
    /// Merged headers of `trace_id` in this partition, sorted by `(start_ns, span_id)`.
    /// All projects; the engine filters by project string ID.
    pub fn trace(&self, trace_id: TraceId) -> (Vec<SpanHeader>, Cost) {
        let mut out: Vec<SpanHeader> = self
            .by_trace
            .get(&trace_id)
            .map(|ix| ix.iter().map(|&i| self.spans[i as usize]).collect())
            .unwrap_or_default();
        out.sort_unstable_by_key(in_trace_order);
        let cost = cost_of(out.len());
        (out, cost)
    }
}

impl LayoutBuilder for Builder {
    fn apply(&mut self, batch: &Batch<'_>) {
        for h in batch.headers {
            match self.by_key.entry((h.project, h.trace_id, h.span_id)) {
                Entry::Occupied(e) => self.spans[*e.get() as usize].merge_from(h),
                Entry::Vacant(e) => {
                    // Start from the identity only, so fields no post set read as zero.
                    let mut merged = SpanHeader {
                        trace_id: h.trace_id,
                        span_id: h.span_id,
                        project: h.project,
                        ..SpanHeader::default()
                    };
                    merged.merge_from(h);
                    let i = self.spans.len() as u32;
                    self.spans.push(merged);
                    e.insert(i);
                    self.by_trace.entry(h.trace_id).or_default().push(i);
                }
            }
        }
    }

    fn seal(&self, out: &mut LayoutFiles) -> Result<()> {
        let mut spans = self.spans.clone();
        spans.sort_unstable_by_key(|h| (h.trace_id, in_trace_order(h)));
        let mut traces: Vec<TraceId> = Vec::with_capacity(self.by_trace.len());
        let mut starts: Vec<u32> = Vec::with_capacity(self.by_trace.len() + 1);
        for (i, h) in spans.iter().enumerate() {
            if traces.last() != Some(&h.trace_id) {
                traces.push(h.trace_id);
                starts.push(i as u32);
            }
        }
        starts.push(spans.len() as u32);
        out.add(
            FILE,
            file::write(
                MAGIC,
                VERSION,
                &[
                    bytemuck::cast_slice(&spans),
                    bytemuck::cast_slice(&traces),
                    bytemuck::cast_slice(&starts),
                ],
            ),
        );
        Ok(())
    }
}

pub struct Reader<'a> {
    spans: &'a [SpanHeader],
    traces: &'a [TraceId],
    starts: &'a [u32],
}

impl<'a> Reader<'a> {
    pub fn open(files: FileSet<'a>) -> Result<Reader<'a>> {
        let [spans, traces, starts] = file::read(files.get(FILE)?, MAGIC, VERSION, FILE)?;
        let r = Reader {
            spans: file::cast(spans, FILE)?,
            traces: file::cast(traces, FILE)?,
            starts: file::cast(starts, FILE)?,
        };
        let ok = r.starts.len() == r.traces.len() + 1
            && r.starts.first() == Some(&0)
            && r.starts.last() == Some(&(r.spans.len() as u32))
            && r.starts.windows(2).all(|w| w[0] < w[1]);
        if !ok {
            return Err(Error::Corrupt(format!("{FILE}: bad trace index")));
        }
        Ok(r)
    }

    /// Same answer as [`Builder::trace`], borrowed from the mapped file.
    pub fn trace(&self, trace_id: TraceId) -> (&'a [SpanHeader], Cost) {
        let probes = usize::BITS - self.traces.len().leading_zeros();
        let index_bytes = (probes as usize * size_of::<TraceId>()) as u64;
        let Ok(i) = self.traces.binary_search(&trace_id) else {
            let cost = Cost {
                records_visited: 0,
                bytes_read: index_bytes,
            };
            return (&[], cost);
        };
        // `open` checked that `starts` is increasing and ends at `spans.len()`.
        let spans = &self.spans[self.starts[i] as usize..self.starts[i + 1] as usize];
        let mut cost = cost_of(spans.len());
        cost.bytes_read += index_bytes + 8;
        (spans, cost)
    }

    /// Number of spans in the partition.
    pub fn len(&self) -> usize {
        self.spans.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Every merged span, sorted by `(trace_id, start_ns, span_id)`.
    pub fn spans(&self) -> &'a [SpanHeader] {
        self.spans
    }
}
