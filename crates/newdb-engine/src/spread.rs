//! Search's spans of spread traces (traces with posts in more than one partition), kept
//! across queries.
//!
//! Search must merge a spread trace's spans across partitions before it can match or
//! order them, and loading every spread trace from every partition on every query grows
//! with partitions times spread traces (82 ms at about 700 partitions). Sealed partitions
//! never change, so [`Cache`] keeps each spread trace's spans merged over the leading
//! sealed partitions of the snapshot it was built for, plus their newest-first order.
//! A query then only merges in the partitions after that prefix (the active one), for
//! the traces that have posts there.
//!
//! The cache follows the sealed prefix: a seal appends a partition, which is merged into
//! the cached traces; anything else (retention) rebuilds it. A query on an older snapshot
//! than the cache's builds a private copy instead of moving the cache back.

use std::borrow::Cow;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

use newdb_types::header::field::{INPUTS, OUTPUTS};
use newdb_types::{ReadStats, Result, RunKey, Span, SpanHeader, SpanId, TraceId};

use crate::part::{View, to_span};

/// One span merged over some partitions, without payloads, with the pieces it came
/// from in partition order: `(partition ID, header merged within that partition)`.
#[derive(Clone)]
pub(crate) struct SpreadSpan {
    pub span: Span,
    pub pieces: Vec<(u64, SpanHeader)>,
}

impl SpreadSpan {
    pub fn key(&self) -> RunKey {
        RunKey {
            start_ns: self.span.start_ns.unwrap_or(0),
            trace_id: self.span.trace_id,
            span_id: self.span.span_id,
        }
    }

    /// Whether search looks at it: some piece has inputs or outputs.
    pub fn searchable(&self) -> bool {
        self.pieces
            .iter()
            .any(|(_, h)| h.has(INPUTS) || h.has(OUTPUTS))
    }
}

/// Spans of spread traces merged over a run of sealed partitions.
#[derive(Clone, Default)]
pub(crate) struct State {
    /// The sealed partitions covered, in partition order.
    ids: Vec<u64>,
    /// Every spread trace the state has seen: its spans over `ids`.
    traces: HashMap<TraceId, Arc<Vec<SpreadSpan>>>,
    /// Searchable spans of the traces in `indexed`, ascending by `(key, project)`:
    /// `(key, index in traces[key.trace_id])`. Traces added since are not in it.
    sorted: Arc<Vec<(RunKey, u32)>>,
    indexed: Arc<HashSet<TraceId>>,
    built: bool,
}

impl State {
    pub fn span(&self, key: &RunKey, i: u32) -> &SpreadSpan {
        &self.traces[&key.trace_id][i as usize]
    }

    /// The spans of `t` over the cached partitions (empty if none).
    pub fn spans(&self, t: &TraceId) -> &[SpreadSpan] {
        self.traces.get(t).map_or(&[], |v| v.as_slice())
    }

    pub fn sorted(&self) -> &[(RunKey, u32)] {
        &self.sorted
    }

    pub fn indexed(&self, t: &TraceId) -> bool {
        self.indexed.contains(t)
    }

    /// Adds the pieces in `prefix[from..]` to the cached traces and loads the traces of
    /// `multi` it does not have yet from all of `prefix`.
    fn update(
        &mut self,
        prefix: &[View<'_>],
        multi: &HashSet<TraceId>,
        st: &mut ReadStats,
    ) -> Result<()> {
        let from = self.ids.len();
        if from < prefix.len() {
            for spans in self.traces.values_mut() {
                let t = spans[0].span.trace_id;
                let mut merged: Vec<Cow<'_, SpreadSpan>> = Vec::new();
                let mut touched = false;
                for v in &prefix[from..] {
                    if v.may_contain(t) {
                        if merged.is_empty() {
                            merged = spans.iter().map(Cow::Borrowed).collect();
                        }
                        touched |= merge_pieces(&mut merged, v, t, st)?;
                    }
                }
                if touched {
                    let owned = merged.into_iter().map(Cow::into_owned).collect();
                    *spans = Arc::new(owned);
                }
            }
            self.ids = prefix.iter().map(|v| v.id).collect();
        }
        let missing: Vec<TraceId> = multi
            .iter()
            .filter(|t| !self.traces.contains_key(t))
            .copied()
            .collect();
        for t in missing {
            let mut merged: Vec<Cow<'_, SpreadSpan>> = Vec::new();
            for v in prefix {
                merge_pieces(&mut merged, v, t, st)?;
            }
            if !merged.is_empty() {
                let owned = merged.into_iter().map(Cow::into_owned).collect();
                self.traces.insert(t, Arc::new(owned));
            }
        }
        if from < prefix.len() || !self.built {
            self.reindex();
            self.built = true;
        }
        Ok(())
    }

    fn reindex(&mut self) {
        let mut sorted: Vec<(RunKey, u32)> = Vec::new();
        for spans in self.traces.values() {
            for (i, s) in spans.iter().enumerate() {
                if s.searchable() {
                    sorted.push((s.key(), i as u32));
                }
            }
        }
        sorted.sort_unstable_by(|a, b| {
            let pa = &self.span(&a.0, a.1).span.project;
            let pb = &self.span(&b.0, b.1).span.project;
            (a.0, pa).cmp(&(b.0, pb))
        });
        self.sorted = Arc::new(sorted);
        self.indexed = Arc::new(self.traces.keys().copied().collect());
    }
}

/// The engine's cache of spread traces for search.
#[derive(Default)]
pub(crate) struct Cache {
    state: Mutex<Arc<State>>,
}

impl Cache {
    /// The spread spans of every trace in `multi` over `prefix`, the leading sealed
    /// partitions of a query's snapshot.
    pub fn get(
        &self,
        prefix: &[View<'_>],
        multi: &HashSet<TraceId>,
        st: &mut ReadStats,
    ) -> Result<Arc<State>> {
        let ids: Vec<u64> = prefix.iter().map(|v| v.id).collect();
        let mut g = self.state.lock().unwrap_or_else(|e| e.into_inner());
        if g.ids.len() > ids.len() && g.ids.starts_with(&ids) {
            // An older snapshot than the cache's: answer from a private state.
            let mut local = State::default();
            local.update(prefix, multi, st)?;
            return Ok(Arc::new(local));
        }
        if !ids.starts_with(&g.ids) {
            *g = Arc::new(State::default());
        }
        let stale =
            g.ids.len() < ids.len() || !g.built || multi.iter().any(|t| !g.traces.contains_key(t));
        if stale {
            Arc::make_mut(&mut g).update(prefix, multi, st)?;
        }
        Ok(g.clone())
    }
}

/// Merges the pieces of trace `t` in partition `v` into `spans` (partition order:
/// `v` comes after every partition already merged). Returns whether `v` had any.
pub(crate) fn merge_pieces<'c>(
    spans: &mut Vec<Cow<'c, SpreadSpan>>,
    v: &View<'_>,
    t: TraceId,
    st: &mut ReadStats,
) -> Result<bool> {
    if !v.may_contain(t) {
        return Ok(false);
    }
    let (headers, cost) = v.trace(t)?;
    st.partitions_touched += 1;
    st.records_visited += cost.records_visited;
    st.bytes_read += cost.bytes_read;
    if headers.is_empty() {
        return Ok(false);
    }
    // Within one partition every span appears once, so the index needs only the spans
    // merged before `v`.
    let index: HashMap<SpanId, usize> = match spans.len() > 8 {
        true => spans
            .iter()
            .enumerate()
            .map(|(i, s)| (s.span.span_id, i))
            .collect(),
        false => HashMap::new(),
    };
    for h in headers {
        let s = to_span(&h, v.strings(), v.blob(), 0, &mut 0)?;
        let same = |x: &SpreadSpan| x.span.span_id == s.span_id && x.span.project == s.project;
        let found = match index.get(&s.span_id) {
            Some(&i) if same(&spans[i]) => Some(i),
            None if !index.is_empty() => None,
            _ => spans.iter().position(|x| same(x)),
        };
        match found {
            Some(i) => {
                let m = spans[i].to_mut();
                m.span.merge_from(&s);
                m.pieces.push((v.id, h));
            }
            None => spans.push(Cow::Owned(SpreadSpan {
                span: s,
                pieces: vec![(v.id, h)],
            })),
        }
    }
    Ok(true)
}

/// One query's stream of spread spans, newest first by `(key, project)`: the cached
/// order, minus the traces that have posts after the cached prefix, which are merged
/// again with those posts.
pub(crate) struct Spread<'c> {
    state: &'c State,
    /// Not yet taken, ascending; taken from the back.
    sorted: &'c [(RunKey, u32)],
    /// Traces in `recent` (or with posts after the prefix): skipped in `sorted`.
    skip: HashSet<TraceId>,
    project: Option<String>,
    /// Ascending; taken from the back.
    recent: Vec<(RunKey, Cow<'c, SpreadSpan>)>,
}

impl<'c> Spread<'c> {
    /// `tail` is the query's partitions after the cached prefix.
    pub fn new(
        state: &'c State,
        tail: &[View<'_>],
        multi: &HashSet<TraceId>,
        project: Option<&str>,
        st: &mut ReadStats,
    ) -> Result<Spread<'c>> {
        let mut skip = HashSet::new();
        let mut recent = Vec::new();
        for &t in multi {
            let indexed = state.indexed(&t);
            if indexed && !tail.iter().any(|v| v.may_contain(t)) {
                continue;
            }
            let mut spans: Vec<Cow<'c, SpreadSpan>> =
                state.spans(&t).iter().map(Cow::Borrowed).collect();
            let mut touched = false;
            for v in tail {
                touched |= merge_pieces(&mut spans, v, t, st)?;
            }
            if indexed && !touched {
                continue;
            }
            skip.insert(t);
            for s in spans {
                if s.searchable() && project.is_none_or(|p| p == s.span.project) {
                    recent.push((s.key(), s));
                }
            }
        }
        recent.sort_unstable_by(|a, b| (a.0, &a.1.span.project).cmp(&(b.0, &b.1.span.project)));
        Ok(Spread {
            state,
            sorted: state.sorted(),
            skip,
            project: project.map(str::to_owned),
            recent,
        })
    }

    /// Drops spans at the back of `sorted` that this query does not take from it.
    fn settle(&mut self) {
        while let Some((rest, (k, i))) = self.sorted.split_last().map(|(l, r)| (r, l)) {
            let s = self.state.span(k, *i);
            let wanted = !self.skip.contains(&k.trace_id)
                && self.project.as_deref().is_none_or(|p| p == s.span.project);
            if wanted {
                break;
            }
            self.sorted = rest;
        }
    }

    /// Whether the next span comes from `sorted` (`Some(true)`) or `recent`.
    fn next_source(&mut self) -> Option<bool> {
        self.settle();
        match (self.sorted.last(), self.recent.last()) {
            (None, None) => None,
            (Some(_), None) => Some(true),
            (None, Some(_)) => Some(false),
            (Some((k, i)), Some((rk, rs))) => {
                let s = self.state.span(k, *i);
                Some((*k, &s.span.project) > (*rk, &rs.span.project))
            }
        }
    }

    pub fn peek(&mut self) -> Option<RunKey> {
        match self.next_source()? {
            true => self.sorted.last().map(|e| e.0),
            false => self.recent.last().map(|e| e.0),
        }
    }

    pub fn pop(&mut self) -> Option<Cow<'c, SpreadSpan>> {
        match self.next_source()? {
            true => {
                let ((k, i), rest) = self.sorted.split_last()?;
                self.sorted = rest;
                Some(Cow::Borrowed(self.state.span(k, *i)))
            }
            false => self.recent.pop().map(|e| e.1),
        }
    }
}
