use std::collections::HashMap;
use std::sync::Mutex;

use newdb_types::text::tokens;
use newdb_types::{
    Read, ReadStats, Result, Run, RunQuery, SearchHit, SearchQuery, Span, SpanId, Store, TraceId,
    TraceQuery,
};

/// Brute-force reference store. Keeps every merged span in a hash map.
#[derive(Default)]
pub struct Oracle {
    state: Mutex<State>,
}

#[derive(Default)]
struct State {
    next_version: u64,
    spans: HashMap<(String, TraceId, SpanId), Span>,
}

impl Oracle {
    pub fn new() -> Oracle {
        Oracle::default()
    }

    /// Number of distinct spans stored.
    pub fn len(&self) -> usize {
        self.state.lock().unwrap().spans.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl Store for Oracle {
    fn ingest(&self, spans: Vec<Span>) -> Result<()> {
        let mut st = self.state.lock().unwrap();
        for mut post in spans {
            st.next_version += 1;
            post.version = st.next_version;
            let key = (post.project.clone(), post.trace_id, post.span_id);
            match st.spans.get_mut(&key) {
                Some(cur) => cur.merge_from(&post),
                None => {
                    st.spans.insert(key, post);
                }
            }
        }
        Ok(())
    }

    fn trace(&self, q: &TraceQuery) -> Result<Read<Vec<Span>>> {
        let st = self.state.lock().unwrap();
        let mut out: Vec<Span> = st
            .spans
            .values()
            .filter(|s| s.trace_id == q.trace_id && s.project == q.project)
            .cloned()
            .collect();
        if !q.payloads {
            out.iter_mut().for_each(Span::strip_payloads);
        }
        out.sort_by_key(|s| (s.start_ns.unwrap_or(0), s.span_id));
        Ok(with_stats(out, st.spans.len()))
    }

    fn runs(&self, q: &RunQuery) -> Result<Read<Vec<Run>>> {
        let st = self.state.lock().unwrap();
        let mut out: Vec<Run> = st
            .spans
            .values()
            .filter_map(Run::from_root)
            .filter(|r| q.matches(r))
            .collect();
        out.sort_by_key(|a| std::cmp::Reverse(a.key()));
        out.truncate(q.limit);
        Ok(with_stats(out, st.spans.len()))
    }

    fn search(&self, q: &SearchQuery) -> Result<Read<Vec<SearchHit>>> {
        let st = self.state.lock().unwrap();
        let want = tokens(q.text.as_bytes());
        let mut out = Vec::new();
        if !want.is_empty() {
            for s in st.spans.values() {
                if q.project.as_ref().is_some_and(|p| *p != s.project) {
                    continue;
                }
                let mut have = tokens(s.inputs.as_deref().unwrap_or_default());
                have.extend(tokens(s.outputs.as_deref().unwrap_or_default()));
                if want.iter().all(|t| have.contains(t)) {
                    out.push(SearchHit {
                        project: s.project.clone(),
                        trace_id: s.trace_id,
                        span_id: s.span_id,
                        start_ns: s.start_ns.unwrap_or(0),
                    });
                }
            }
        }
        out.sort_by_key(|a| std::cmp::Reverse(a.key()));
        out.truncate(q.limit);
        Ok(with_stats(out, st.spans.len()))
    }
}

fn with_stats<T>(value: Vec<T>, visited: usize) -> Read<Vec<T>> {
    let stats = ReadStats {
        partitions_touched: 1,
        records_visited: visited as u64,
        records_returned: value.len() as u64,
        ..ReadStats::default()
    };
    Read { value, stats }
}
