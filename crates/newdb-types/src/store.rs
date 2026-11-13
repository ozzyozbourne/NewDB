//! The query surface every store implements: the real engine (`newdb-engine::Db`) and the
//! brute-force reference (`newdb-testkit::Oracle`). The server, the Python binding and the
//! tools take `&dyn Store`, so they work against the oracle before the engine exists, and
//! differential tests compare the two answer for answer.
//!
//! The semantics are defined here, by the doc comments and by [`Run::from_root`],
//! [`RunQuery::matches`] and [`crate::text`]. Both implementations must follow them exactly.

use std::cmp::Ordering;

use crate::Result;
use crate::ids::{SpanId, TraceId};
use crate::span::{RunType, Span, Status};

/// Cost of one read. Counters are always filled; `phases_ns` only when the query asked for
/// timings; `allocations` only in bench builds with a counting allocator.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ReadStats {
    pub partitions_touched: u64,
    /// Bytes read from mapped or in-memory layout files (not counting payloads).
    pub bytes_read: u64,
    /// Uncompressed bytes of payloads returned or checked.
    pub payload_bytes_decompressed: u64,
    /// Records (span headers, run summaries, postings) looked at.
    pub records_visited: u64,
    /// Records in the result.
    pub records_returned: u64,
    /// Wall time per named phase, in the order the phases ran.
    pub phases_ns: Vec<(&'static str, u64)>,
    pub allocations: u64,
}

impl ReadStats {
    pub fn add(&mut self, other: &ReadStats) {
        self.partitions_touched += other.partitions_touched;
        self.bytes_read += other.bytes_read;
        self.payload_bytes_decompressed += other.payload_bytes_decompressed;
        self.records_visited += other.records_visited;
        self.records_returned += other.records_returned;
        self.phases_ns.extend_from_slice(&other.phases_ns);
        self.allocations += other.allocations;
    }
}

/// A result plus what it cost.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Read<T> {
    pub value: T,
    pub stats: ReadStats,
}

/// Load every span of one trace.
#[derive(Clone, Debug, Default)]
pub struct TraceQuery {
    pub project: String,
    pub trace_id: TraceId,
    /// When false, spans come back with [`Span::strip_payloads`] applied.
    pub payloads: bool,
    pub timings: bool,
}

/// Sort and cursor key of a run. Runs are returned newest first: descending by
/// `(start_ns, trace_id, span_id)`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Default)]
pub struct RunKey {
    pub start_ns: u64,
    pub trace_id: TraceId,
    pub span_id: SpanId,
}

/// A run: one root span (merged `parent_span_id` is `None`). Its fields are the root
/// span's own values; trace-wide totals are not computed in v1.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Run {
    pub project: String,
    pub trace_id: TraceId,
    pub span_id: SpanId,
    pub name: Option<String>,
    /// 0 when the root span never sent a start time.
    pub start_ns: u64,
    pub end_ns: Option<u64>,
    pub status: Status,
    pub run_type: RunType,
    pub model: Option<String>,
    pub session: Option<String>,
    pub prompt_tokens: u32,
    pub completion_tokens: u32,
    pub cost_micro_usd: u64,
}

impl Run {
    /// The run for a merged root span. Returns `None` for non-root spans.
    pub fn from_root(span: &Span) -> Option<Run> {
        if !span.is_root() {
            return None;
        }
        Some(Run {
            project: span.project.clone(),
            trace_id: span.trace_id,
            span_id: span.span_id,
            name: span.name.clone(),
            start_ns: span.start_ns.unwrap_or(0),
            end_ns: span.end_ns,
            status: span.status.unwrap_or_default(),
            run_type: span.run_type.unwrap_or_default(),
            model: span.model.clone(),
            session: span.session.clone(),
            prompt_tokens: span.prompt_tokens.unwrap_or(0),
            completion_tokens: span.completion_tokens.unwrap_or(0),
            cost_micro_usd: span.cost_micro_usd.unwrap_or(0),
        })
    }

    pub fn key(&self) -> RunKey {
        RunKey {
            start_ns: self.start_ns,
            trace_id: self.trace_id,
            span_id: self.span_id,
        }
    }

    /// `end_ns - start_ns`, or `None` while open. Saturates at 0 if end is before start.
    pub fn latency_ns(&self) -> Option<u64> {
        self.end_ns.map(|e| e.saturating_sub(self.start_ns))
    }

    pub fn total_tokens(&self) -> u64 {
        self.prompt_tokens as u64 + self.completion_tokens as u64
    }
}

/// Orders runs newest first, the order every runs query returns.
pub fn newest_first(a: &RunKey, b: &RunKey) -> Ordering {
    b.cmp(a)
}

/// List runs newest first. Every `Some` filter must match; `limit` caps the result.
#[derive(Clone, Debug, Default)]
pub struct RunQuery {
    pub project: Option<String>,
    pub status: Option<Status>,
    pub run_type: Option<RunType>,
    pub model: Option<String>,
    pub session: Option<String>,
    /// Open runs never match a latency filter.
    pub min_latency_ns: Option<u64>,
    pub min_total_tokens: Option<u64>,
    pub min_cost_micro_usd: Option<u64>,
    /// Only runs with `start_ns >= start_after_ns`.
    pub start_after_ns: Option<u64>,
    /// Pagination: only runs strictly after this key in newest-first order
    /// (that is, with a smaller key). Pass the last run's key from the previous page.
    pub before: Option<RunKey>,
    pub limit: usize,
    pub timings: bool,
}

impl RunQuery {
    /// Whether `run` passes every filter (ignores `limit`).
    pub fn matches(&self, run: &Run) -> bool {
        fn eq<T: PartialEq>(want: &Option<T>, have: T) -> bool {
            want.as_ref().is_none_or(|w| *w == have)
        }
        fn eq_str(want: &Option<String>, have: &Option<String>) -> bool {
            want.as_ref()
                .is_none_or(|w| have.as_deref() == Some(w.as_str()))
        }
        eq(&self.project.as_deref(), run.project.as_str())
            && eq(&self.status, run.status)
            && eq(&self.run_type, run.run_type)
            && eq_str(&self.model, &run.model)
            && eq_str(&self.session, &run.session)
            && self
                .min_latency_ns
                .is_none_or(|m| run.latency_ns().is_some_and(|l| l >= m))
            && self
                .min_total_tokens
                .is_none_or(|m| run.total_tokens() >= m)
            && self
                .min_cost_micro_usd
                .is_none_or(|m| run.cost_micro_usd >= m)
            && self.start_after_ns.is_none_or(|s| run.start_ns >= s)
            && self.before.is_none_or(|b| run.key() < b)
    }
}

/// Full-text search over span `inputs` and `outputs`.
///
/// A span matches when every token of `text` (see [`crate::text::tokens`]) appears among the
/// tokens of its merged `inputs` or `outputs`. A query with no tokens matches nothing. Hits
/// are newest first by `(start_ns, trace_id, span_id)` descending, like runs.
#[derive(Clone, Debug, Default)]
pub struct SearchQuery {
    pub project: Option<String>,
    pub text: String,
    pub limit: usize,
    pub timings: bool,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SearchHit {
    pub project: String,
    pub trace_id: TraceId,
    pub span_id: SpanId,
    /// 0 when the span never sent a start time.
    pub start_ns: u64,
}

impl SearchHit {
    pub fn key(&self) -> RunKey {
        RunKey {
            start_ns: self.start_ns,
            trace_id: self.trace_id,
            span_id: self.span_id,
        }
    }
}

/// What every store answers. All methods are safe to call from many threads at once.
pub trait Store: Send + Sync {
    /// Appends posts in order, assigning each the next version. Returns once they are
    /// durable (per the store's fsync setting) and visible to every read that starts after.
    fn ingest(&self, spans: Vec<Span>) -> Result<()>;

    /// Every span of `(project, trace_id)`, each merged across all its posts, sorted by
    /// `(start_ns or 0, span_id)` ascending. Empty if the trace is unknown.
    fn trace(&self, query: &TraceQuery) -> Result<Read<Vec<Span>>>;

    /// Runs passing the filters, newest first, at most `limit`.
    fn runs(&self, query: &RunQuery) -> Result<Read<Vec<Run>>>;

    /// Spans matching the search, newest first, at most `limit`.
    fn search(&self, query: &SearchQuery) -> Result<Read<Vec<SearchHit>>>;
}
