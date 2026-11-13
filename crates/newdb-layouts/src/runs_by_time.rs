//! `runs_by_time`: one [`RunSummary`] per run candidate, newest first, plus the positions
//! of error runs (the `errors` index from DESIGN.md, stored in the same layout).
//!
//! A **run candidate** is a span whose merged header in this partition is a root
//! (`SpanHeader::is_root`). Because merging is per partition, a span's closing post can
//! arrive in a later partition without its parent field; it then appears here as a
//! candidate. The engine drops candidates whose cross-partition merge has a parent, and
//! merges a run's summaries across partitions using `RunSummary::fields`.
//!
//! Order: descending by `(start_ns, trace_id, span_id)`, matching `Store::runs`, then by
//! project (descending) when two projects share a trace and span ID.
//!
//! File `runs_by_time.bin` (framing in `file.rs`), two sections: `[RunSummary]` newest
//! first, and `[u32]` positions of error runs, ascending.

use std::cmp::Reverse;
use std::collections::{BTreeSet, HashMap};

use newdb_types::header::field;
use newdb_types::{Result, RunSummary, SpanHeader, SpanId, Status, TraceId};

use crate::{Batch, Cost, FileSet, LayoutBuilder, LayoutFiles, file};

pub const NAME: &str = "runs_by_time";
pub const VERSION: u32 = 1;

const FILE: &str = "runs_by_time.bin";
const MAGIC: &[u8; 8] = b"NDBRUNS\0";

type Key = (u32, TraceId, SpanId);
/// Sorts newest first in a `BTreeSet`.
type Order = Reverse<(u64, TraceId, SpanId, u32)>;

/// A span merged within the partition, reduced to what a run needs.
struct Merged {
    sum: RunSummary,
    /// The latest post that set a parent set a non-zero one.
    child: bool,
}

impl Merged {
    fn order(&self) -> Order {
        let s = &self.sum;
        Reverse((s.start_ns, s.trace_id, s.span_id, s.project))
    }

    /// [`SpanHeader::merge_from`], restricted to the fields a [`RunSummary`] keeps.
    fn merge_from(&mut self, h: &SpanHeader) {
        use field::*;
        let s = &mut self.sum;
        macro_rules! take {
            ($($bit:ident => $f:ident),*) => { $( if h.has($bit) { s.$f = h.$f; } )* };
        }
        take!(
            NAME => name, START => start_ns, END => end_ns, STATUS => status,
            RUN_TYPE => run_type, MODEL => model, SESSION => session,
            PROMPT_TOKENS => prompt_tokens, COMPLETION_TOKENS => completion_tokens,
            COST => cost_micro_usd
        );
        if h.has(PARENT) {
            self.child = !h.parent_span_id.is_zero();
        }
        s.fields |= h.fields as u16;
    }
}

#[derive(Default)]
pub struct Builder {
    spans: HashMap<Key, Merged>,
    /// Run candidates, newest first.
    runs: BTreeSet<Order>,
}

fn is_error(s: &RunSummary) -> bool {
    s.status == Status::Error as u8
}

fn cost_of(n: usize) -> Cost {
    Cost {
        records_visited: n as u64,
        bytes_read: (n * size_of::<RunSummary>()) as u64,
    }
}

impl Builder {
    /// All run candidates of the partition so far, newest first.
    pub fn runs(&self) -> (Vec<RunSummary>, Cost) {
        let out: Vec<RunSummary> = self.runs_newest().copied().collect();
        let cost = cost_of(out.len());
        (out, cost)
    }

    /// The same runs as [`Builder::runs`], in the same order, without copying: each item
    /// costs one ordered-set step and one hash lookup, so newest-N reads N runs however
    /// many the partition holds. Charge `cost_of(n)` for the `n` taken.
    pub fn runs_newest(&self) -> impl Iterator<Item = &RunSummary> + '_ {
        self.runs
            .iter()
            .map(|Reverse((_, t, s, p))| &self.spans[&(*p, *t, *s)].sum)
    }

    /// Positions in [`Builder::runs`] of summaries with status error, ascending.
    pub fn error_positions(&self) -> (Vec<u32>, Cost) {
        let (runs, cost) = self.runs();
        (error_positions(&runs), cost)
    }

    /// Cost of reading `n` summaries, for `ReadStats`.
    pub fn cost_of(&self, n: usize) -> Cost {
        cost_of(n)
    }
}

fn error_positions(runs: &[RunSummary]) -> Vec<u32> {
    (0..runs.len() as u32)
        .filter(|&i| is_error(&runs[i as usize]))
        .collect()
}

impl LayoutBuilder for Builder {
    fn apply(&mut self, batch: &Batch<'_>) {
        for h in batch.headers {
            let key = (h.project, h.trace_id, h.span_id);
            let m = self.spans.entry(key).or_insert_with(|| Merged {
                sum: RunSummary {
                    trace_id: h.trace_id,
                    span_id: h.span_id,
                    project: h.project,
                    ..RunSummary::default()
                },
                child: false,
            });
            if !m.child {
                self.runs.remove(&m.order());
            }
            m.merge_from(h);
            if !m.child {
                self.runs.insert(m.order());
            }
        }
    }

    fn seal(&self, out: &mut LayoutFiles) -> Result<()> {
        let (runs, _) = self.runs();
        let errors = error_positions(&runs);
        out.add(
            FILE,
            file::write(
                MAGIC,
                VERSION,
                &[bytemuck::cast_slice(&runs), bytemuck::cast_slice(&errors)],
            ),
        );
        Ok(())
    }
}

pub struct Reader<'a> {
    runs: &'a [RunSummary],
    errors: &'a [u32],
}

impl<'a> Reader<'a> {
    pub fn open(files: FileSet<'a>) -> Result<Reader<'a>> {
        let [runs, errors] = file::read(files.get(FILE)?, MAGIC, VERSION, FILE)?;
        let r = Reader {
            runs: file::cast(runs, FILE)?,
            errors: file::cast(errors, FILE)?,
        };
        if r.errors.iter().any(|&p| p as usize >= r.runs.len()) {
            return Err(newdb_types::Error::Corrupt(format!(
                "{FILE}: error position out of range"
            )));
        }
        Ok(r)
    }

    /// Same as [`Builder::runs`], borrowed from the mapped file.
    pub fn runs(&self) -> &'a [RunSummary] {
        self.runs
    }

    /// Same as [`Builder::error_positions`].
    pub fn error_positions(&self) -> &'a [u32] {
        self.errors
    }

    /// Cost of reading `n` summaries, for `ReadStats`.
    pub fn cost_of(&self, n: usize) -> Cost {
        cost_of(n)
    }
}
