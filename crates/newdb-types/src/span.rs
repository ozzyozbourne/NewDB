//! The owned span record and the merge rule.
//!
//! A span may be posted several times (typically once at start and once at end). Each post
//! is a [`Span`] whose `None` fields mean "not sent in this post". The stored span is the
//! merge of all posts in version order: for every field, the latest post that set it wins.

use crate::ids::{SpanId, TraceId};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Default)]
#[repr(u8)]
pub enum Status {
    #[default]
    Unset = 0,
    Ok = 1,
    Error = 2,
}

impl Status {
    pub fn from_u8(v: u8) -> Option<Status> {
        Some(match v {
            0 => Status::Unset,
            1 => Status::Ok,
            2 => Status::Error,
            _ => return None,
        })
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Status::Unset => "unset",
            Status::Ok => "ok",
            Status::Error => "error",
        }
    }
}

/// What kind of work a span did, derived from GenAI / OpenInference / OpenLLMetry attributes.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Default)]
#[repr(u8)]
pub enum RunType {
    #[default]
    Other = 0,
    Llm = 1,
    ChatModel = 2,
    Tool = 3,
    Chain = 4,
    Agent = 5,
    Retriever = 6,
    Embedding = 7,
}

impl RunType {
    pub const ALL: [RunType; 8] = [
        RunType::Other,
        RunType::Llm,
        RunType::ChatModel,
        RunType::Tool,
        RunType::Chain,
        RunType::Agent,
        RunType::Retriever,
        RunType::Embedding,
    ];

    pub fn from_u8(v: u8) -> Option<RunType> {
        RunType::ALL.get(v as usize).copied()
    }

    pub fn as_str(self) -> &'static str {
        match self {
            RunType::Other => "other",
            RunType::Llm => "llm",
            RunType::ChatModel => "chat_model",
            RunType::Tool => "tool",
            RunType::Chain => "chain",
            RunType::Agent => "agent",
            RunType::Retriever => "retriever",
            RunType::Embedding => "embedding",
        }
    }

    pub fn parse(s: &str) -> Option<RunType> {
        RunType::ALL.into_iter().find(|t| t.as_str() == s)
    }
}

/// One post of a span (on ingest), or the merged span (on read).
///
/// `project`, `trace_id` and `span_id` identify the span and are always set.
/// `version` is assigned by the store on ingest (increasing in ingest order) and ignored on
/// input. Payload fields hold the JSON bytes exactly as sent; the store never parses them.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Span {
    pub project: String,
    pub trace_id: TraceId,
    pub span_id: SpanId,
    /// `None` after merging means a root span: a run.
    pub parent_span_id: Option<SpanId>,
    pub name: Option<String>,
    pub start_ns: Option<u64>,
    /// `None` while the span is open.
    pub end_ns: Option<u64>,
    pub status: Option<Status>,
    pub run_type: Option<RunType>,
    pub model: Option<String>,
    pub provider: Option<String>,
    pub session: Option<String>,
    pub prompt_tokens: Option<u32>,
    pub completion_tokens: Option<u32>,
    pub cost_micro_usd: Option<u64>,
    pub inputs: Option<Vec<u8>>,
    pub outputs: Option<Vec<u8>>,
    pub attributes: Option<Vec<u8>>,
    pub events: Option<Vec<u8>>,
    pub error: Option<String>,
    pub version: u64,
}

impl Span {
    /// Applies a newer post on top of `self`: every field `newer` sets replaces the current
    /// value; fields it leaves `None` keep theirs. `version` becomes the larger of the two.
    ///
    /// The caller guarantees both posts are the same span (same project, trace and span ID)
    /// and that `newer.version > self.version`. Merging posts in ascending version order
    /// gives the same result regardless of how they are grouped.
    pub fn merge_from(&mut self, newer: &Span) {
        debug_assert_eq!(self.trace_id, newer.trace_id);
        debug_assert_eq!(self.span_id, newer.span_id);
        debug_assert_eq!(self.project, newer.project);
        macro_rules! take {
            ($($f:ident),*) => { $( if newer.$f.is_some() { self.$f = newer.$f.clone(); } )* };
        }
        take!(
            parent_span_id,
            name,
            start_ns,
            end_ns,
            status,
            run_type,
            model,
            provider,
            session,
            prompt_tokens,
            completion_tokens,
            cost_micro_usd,
            inputs,
            outputs,
            attributes,
            events,
            error
        );
        self.version = self.version.max(newer.version);
    }

    pub fn is_root(&self) -> bool {
        self.parent_span_id.is_none()
    }

    /// Drops the large payload fields (`inputs`, `outputs`, `attributes`, `events`).
    /// `error` is kept. Used for reads with `payloads: false`.
    pub fn strip_payloads(&mut self) {
        self.inputs = None;
        self.outputs = None;
        self.attributes = None;
        self.events = None;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn post(version: u64) -> Span {
        Span {
            project: "p".into(),
            trace_id: TraceId([1; 16]),
            span_id: SpanId([2; 8]),
            version,
            ..Span::default()
        }
    }

    #[test]
    fn later_set_fields_win_and_unset_fields_keep_their_value() {
        let mut start = post(1);
        start.name = Some("agent".into());
        start.start_ns = Some(100);
        start.status = Some(Status::Unset);
        start.inputs = Some(b"{\"q\":1}".to_vec());

        let mut end = post(5);
        end.end_ns = Some(900);
        end.status = Some(Status::Error);
        end.outputs = Some(b"{}".to_vec());

        let mut merged = start.clone();
        merged.merge_from(&end);
        assert_eq!(merged.name.as_deref(), Some("agent"));
        assert_eq!(merged.start_ns, Some(100));
        assert_eq!(merged.end_ns, Some(900));
        assert_eq!(merged.status, Some(Status::Error));
        assert_eq!(merged.inputs.as_deref(), Some(&b"{\"q\":1}"[..]));
        assert_eq!(merged.outputs.as_deref(), Some(&b"{}"[..]));
        assert_eq!(merged.version, 5);
    }

    #[test]
    fn a_later_parent_makes_a_root_into_a_child() {
        let mut a = post(1);
        let mut b = post(2);
        b.parent_span_id = Some(SpanId([9; 8]));
        assert!(a.is_root());
        a.merge_from(&b);
        assert!(!a.is_root());
    }
}
