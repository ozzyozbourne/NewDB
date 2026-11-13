//! The `Store` queries over one snapshot of partitions (`views`, in creation order, which
//! is also version order). Layouts answer within a partition; this module merges a span's
//! pieces across partitions with `Span::merge_from`, in partition order.

use std::cmp::Reverse;
use std::collections::hash_map::Entry;
use std::collections::{BTreeMap, BinaryHeap, HashMap, HashSet};
use std::time::Instant;

use newdb_layouts::{Cost, fts};
use newdb_types::header::field::{END, ERROR, INPUTS, MODEL, NAME, OUTPUTS, PARENT, SESSION};
use newdb_types::text::tokens;
use newdb_types::{
    Read, ReadStats, Result, Run, RunKey, RunQuery, RunSummary, RunType, SearchHit, SearchQuery,
    Span, SpanHeader, Status, TraceId, TraceQuery,
};

use crate::part::{
    ALL_PAYLOADS, PAYLOAD_FIELDS, View, corrupt, payload_ref, set_payload, summary_key, to_span,
};

/// One span merged across partitions, without payloads until [`load_payloads`].
pub(crate) struct Merged {
    pub span: Span,
    /// `(partition index, header merged within that partition)`, in partition order.
    pieces: Vec<(usize, SpanHeader)>,
    /// The partition that lists this span's run summary under its true start: the last
    /// one whose piece sets `start_ns`, or the last piece when none does.
    owner: usize,
    owner_has_start: bool,
}

fn add_cost(st: &mut ReadStats, c: Cost) {
    st.records_visited += c.records_visited;
    st.bytes_read += c.bytes_read;
}

/// Every span of trace `t` (of `project`, or of all projects), merged across partitions.
pub(crate) fn load_trace(
    views: &[View<'_>],
    t: TraceId,
    project: Option<&str>,
    st: &mut ReadStats,
) -> Result<Vec<Merged>> {
    let mut out: Vec<Merged> = Vec::new();
    let mut index = HashMap::new();
    for (i, v) in views.iter().enumerate() {
        if !v.may_contain(t) {
            continue;
        }
        let pid = match project {
            Some(p) => match v.strings().id(p) {
                Some(id) => Some(id),
                None => continue,
            },
            None => None,
        };
        let (headers, cost) = v.trace(t)?;
        st.partitions_touched += 1;
        add_cost(st, cost);
        for h in headers {
            if pid.is_some_and(|id| h.project != id) {
                continue;
            }
            let s = to_span(&h, v.strings(), v.blob(), 0, &mut 0)?;
            let has_start = s.start_ns.is_some();
            match index.get(&(s.project.clone(), s.span_id)) {
                Some(&k) => {
                    let m: &mut Merged = &mut out[k];
                    m.span.merge_from(&s);
                    m.pieces.push((i, h));
                    if has_start || !m.owner_has_start {
                        m.owner = i;
                        m.owner_has_start |= has_start;
                    }
                }
                None => {
                    index.insert((s.project.clone(), s.span_id), out.len());
                    out.push(Merged {
                        span: s,
                        pieces: vec![(i, h)],
                        owner: i,
                        owner_has_start: has_start,
                    });
                }
            }
        }
    }
    Ok(out)
}

/// Fills the payload fields in `mask`: each comes from the last piece that set it.
fn load_payloads(views: &[View<'_>], m: &mut Merged, mask: u32, st: &mut ReadStats) -> Result<()> {
    for bit in PAYLOAD_FIELDS {
        if mask & bit == 0 {
            continue;
        }
        if let Some((pi, h)) = m.pieces.iter().rev().find(|(_, h)| h.has(bit)) {
            let bytes = views[*pi].blob().read(payload_ref(h, bit))?;
            st.payload_bytes_decompressed += bytes.len() as u64;
            set_payload(&mut m.span, bit, bytes)?;
        }
    }
    Ok(())
}

struct Phases {
    on: bool,
    last: Instant,
    out: Vec<(&'static str, u64)>,
}

impl Phases {
    fn new(on: bool) -> Phases {
        Phases {
            on,
            last: Instant::now(),
            out: Vec::new(),
        }
    }

    fn mark(&mut self, name: &'static str) {
        if self.on {
            let now = Instant::now();
            self.out.push((name, (now - self.last).as_nanos() as u64));
            self.last = now;
        }
    }
}

fn done<T>(value: Vec<T>, mut stats: ReadStats, phases: Phases) -> Read<Vec<T>> {
    stats.records_returned = value.len() as u64;
    stats.phases_ns = phases.out;
    Read { value, stats }
}

pub(crate) fn trace(views: &[View<'_>], q: &TraceQuery) -> Result<Read<Vec<Span>>> {
    let mut ph = Phases::new(q.timings);
    let mut st = ReadStats::default();
    let mut merged = load_trace(views, q.trace_id, Some(&q.project), &mut st)?;
    ph.mark("headers");
    let mask = if q.payloads { ALL_PAYLOADS } else { ERROR };
    for m in &mut merged {
        load_payloads(views, m, mask, &mut st)?;
    }
    let mut out: Vec<Span> = merged.into_iter().map(|m| m.span).collect();
    out.sort_by_key(|s| (s.start_ns.unwrap_or(0), s.span_id));
    ph.mark("payloads");
    Ok(done(out, st, ph))
}

/// Newest-first runs. Partitions are visited by their newest start time, and each lists
/// its run candidates newest first by the start time it saw. A run is taken only from its
/// owner partition (see [`Merged::owner`]), where that start time is the true one, so the
/// walk can stop as soon as a candidate is older than the `limit`-th result.
pub(crate) fn runs(
    views: &[View<'_>],
    multi: &HashSet<TraceId>,
    q: &RunQuery,
) -> Result<Read<Vec<Run>>> {
    let mut ph = Phases::new(q.timings);
    let mut st = ReadStats::default();
    // Ascending by (key, project): the first entry is the oldest kept.
    let mut best: BTreeMap<(RunKey, String), Run> = BTreeMap::new();
    let mut cache: HashMap<TraceId, Vec<Merged>> = HashMap::new();
    let mut order: Vec<usize> = (0..views.len()).collect();
    order.sort_by_key(|&i| Reverse(views[i].stats().max_start()));
    let floor = |best: &BTreeMap<(RunKey, String), Run>| -> Option<RunKey> {
        (best.len() == q.limit).then(|| best.first_key_value().map(|(k, _)| k.0))?
    };
    for &pi in order.iter().filter(|_| q.limit > 0) {
        let v = &views[pi];
        let max_start = v.stats().max_start();
        if floor(&best).is_some_and(|f| max_start < f.start_ns)
            || q.start_after_ns.is_some_and(|s| max_start < s)
        {
            break;
        }
        let pid = match &q.project {
            Some(p) => match v.strings().id(p) {
                Some(id) => Some(id),
                None => continue,
            },
            None => None,
        };
        st.partitions_touched += 1;
        let cost = v.runs(q.before, &mut |s| {
            if floor(&best).is_some_and(|f| summary_key(s) < f)
                || q.start_after_ns.is_some_and(|a| s.start_ns < a)
            {
                return Ok(false);
            }
            if pid.is_some_and(|id| s.project != id) {
                return Ok(true);
            }
            // A run whose trace has no posts in another partition is complete here: filter
            // on its summary, and build the Run (strings) only for those that pass.
            if !multi.contains(&s.trace_id) {
                if !may_match(q, s) {
                    return Ok(true);
                }
                if let Some(run) = summary_run(v, s)?.filter(|r| q.matches(r)) {
                    best.insert((run.key(), run.project.clone()), run);
                    if best.len() > q.limit {
                        best.pop_first();
                    }
                }
                return Ok(true);
            }
            let project = v
                .strings()
                .get(s.project)
                .ok_or_else(|| corrupt("run summary project is not in the partition"))?;
            if let Entry::Vacant(e) = cache.entry(s.trace_id) {
                e.insert(load_trace(views, s.trace_id, None, &mut st)?);
            }
            let m = cache[&s.trace_id]
                .iter()
                .find(|m| m.span.span_id == s.span_id && m.span.project == project);
            let Some(m) = m.filter(|m| m.owner == pi) else {
                return Ok(true);
            };
            if let Some(run) = Run::from_root(&m.span).filter(|r| q.matches(r)) {
                best.insert((run.key(), run.project.clone()), run);
                if best.len() > q.limit {
                    best.pop_first();
                }
            }
            Ok(true)
        })?;
        add_cost(&mut st, cost);
    }
    ph.mark("scan");
    Ok(done(best.into_values().rev().collect(), st, ph))
}

/// The numeric filters of `q` on a summary, without building the run. Only rejects runs
/// `q.matches` would reject too.
fn may_match(q: &RunQuery, s: &RunSummary) -> bool {
    let has = |bit: u32| s.fields as u32 & bit != 0;
    let latency = has(END).then(|| s.end_ns.saturating_sub(s.start_ns));
    !has(PARENT)
        && q.status.is_none_or(|x| x as u8 == s.status)
        && q.run_type.is_none_or(|x| x as u8 == s.run_type)
        && q.min_latency_ns
            .is_none_or(|m| latency.is_some_and(|l| l >= m))
        && q.min_total_tokens
            .is_none_or(|m| s.prompt_tokens as u64 + s.completion_tokens as u64 >= m)
        && q.min_cost_micro_usd.is_none_or(|m| s.cost_micro_usd >= m)
}

/// The run of a summary merged within one partition, as `Run::from_root` would build it
/// from the merged span.
fn summary_run(v: &View<'_>, s: &RunSummary) -> Result<Option<Run>> {
    let has = |bit: u32| s.fields as u32 & bit != 0;
    if has(PARENT) {
        return Ok(None);
    }
    let strings = v.strings();
    let get = |bit: u32, id: u32| -> Result<Option<String>> {
        if !has(bit) {
            return Ok(None);
        }
        strings
            .get(id)
            .map(|x| Some(x.to_string()))
            .ok_or_else(|| corrupt("run summary string is not in the partition"))
    };
    let status = Status::from_u8(s.status).ok_or_else(|| corrupt("bad status"))?;
    let run_type = RunType::from_u8(s.run_type).ok_or_else(|| corrupt("bad run type"))?;
    Ok(Some(Run {
        project: strings
            .get(s.project)
            .ok_or_else(|| corrupt("run summary project is not in the partition"))?
            .to_string(),
        trace_id: s.trace_id,
        span_id: s.span_id,
        name: get(NAME, s.name)?,
        start_ns: s.start_ns,
        end_ns: has(END).then_some(s.end_ns),
        status,
        run_type,
        model: get(MODEL, s.model)?,
        session: get(SESSION, s.session)?,
        prompt_tokens: s.prompt_tokens,
        completion_tokens: s.completion_tokens,
        cost_micro_usd: s.cost_micro_usd,
    }))
}

/// Newest-first search, exact, stopping at `limit`.
///
/// A span of a trace that lives in one partition matches only if that partition's
/// postings hold every token, and its start there is its true start: such spans come
/// from each partition's `intersect_newest` stream. Spans of traces spread over several
/// partitions (the engine's `multi` set) can have their tokens split across partitions
/// and their start set elsewhere, so they are merged across partitions first and form one
/// more stream, sorted by merged key. A heap merges the streams newest first; partitions
/// open in order of their newest start, only once they could hold the next hit. Each
/// candidate is confirmed against its merged `inputs` and `outputs`.
pub(crate) fn search(
    views: &[View<'_>],
    multi: &HashSet<TraceId>,
    q: &SearchQuery,
) -> Result<Read<Vec<SearchHit>>> {
    let mut ph = Phases::new(q.timings);
    let mut st = ReadStats::default();
    let want = tokens(q.text.as_bytes());
    if want.is_empty() || q.limit == 0 {
        return Ok(done(Vec::new(), st, ph));
    }
    let toks: Vec<&[u8]> = want.iter().map(Vec::as_slice).collect();

    // Spans of spread traces, merged, newest first (popped from the back).
    let mut spread: Vec<(RunKey, Merged)> = Vec::new();
    let mut traces: Vec<TraceId> = multi.iter().copied().collect();
    traces.sort_unstable();
    for t in traces {
        for m in load_trace(views, t, q.project.as_deref(), &mut st)? {
            if m.pieces
                .iter()
                .any(|(_, h)| h.has(INPUTS) || h.has(OUTPUTS))
            {
                let key = RunKey {
                    start_ns: m.span.start_ns.unwrap_or(0),
                    trace_id: t,
                    span_id: m.span.span_id,
                };
                spread.push((key, m));
            }
        }
    }
    spread.sort_unstable_by(|a, b| (a.0, &a.1.span.project).cmp(&(b.0, &b.1.span.project)));
    ph.mark("spread");

    let mut order: Vec<usize> = (0..views.len())
        .filter(|&i| {
            q.project
                .as_ref()
                .is_none_or(|p| views[i].strings().id(p).is_some())
        })
        .collect();
    order.sort_by_key(|&i| Reverse(views[i].stats().max_start()));
    let mut opened = 0;
    let mut streams: Vec<(usize, Box<dyn Iterator<Item = fts::Hit> + '_>)> = Vec::new();
    // (key, stream index); usize::MAX is the spread stream.
    let mut heap: BinaryHeap<(RunKey, usize)> = BinaryHeap::new();
    if let Some((k, _)) = spread.last() {
        heap.push((*k, usize::MAX));
    }
    let key = |(start_ns, trace_id, span_id): fts::Hit| RunKey {
        start_ns,
        trace_id,
        span_id,
    };
    let mut headers: HashMap<(usize, TraceId), Vec<SpanHeader>> = HashMap::new();
    let mut hits = Vec::new();
    'outer: loop {
        while opened < order.len()
            && heap
                .peek()
                .is_none_or(|(k, _)| k.start_ns <= views[order[opened]].stats().max_start())
        {
            let pi = order[opened];
            opened += 1;
            st.partitions_touched += 1;
            let (mut it, cost) = views[pi].intersect_newest(&toks)?;
            add_cost(&mut st, cost);
            if let Some(h) = it.next() {
                heap.push((key(h), streams.len()));
                streams.push((pi, it));
            }
        }
        let Some((k, src)) = heap.pop() else {
            break;
        };
        st.records_visited += 1;
        if src == usize::MAX {
            let Some((_, mut m)) = spread.pop() else {
                break;
            };
            if let Some((k2, _)) = spread.last() {
                heap.push((*k2, usize::MAX));
            }
            load_payloads(views, &mut m, INPUTS | OUTPUTS, &mut st)?;
            if matches(&want, m.span.inputs.as_deref(), m.span.outputs.as_deref()) {
                hits.push(hit(&m.span.project, k));
            }
        } else {
            let (pi, it) = &mut streams[src];
            let pi = *pi;
            if let Some(h) = it.next() {
                heap.push((key(h), src));
            }
            if multi.contains(&k.trace_id) {
                continue;
            }
            let v = &views[pi];
            let pid = q.project.as_ref().and_then(|p| v.strings().id(p));
            if let Entry::Vacant(e) = headers.entry((pi, k.trace_id)) {
                let (hs, cost) = v.trace(k.trace_id)?;
                add_cost(&mut st, cost);
                e.insert(hs);
            }
            for h in &headers[&(pi, k.trace_id)] {
                if h.span_id != k.span_id || pid.is_some_and(|id| h.project != id) {
                    continue;
                }
                let mut read = |bit: u32| -> Result<Option<Vec<u8>>> {
                    if !h.has(bit) {
                        return Ok(None);
                    }
                    let bytes = v.blob().read(payload_ref(h, bit))?;
                    st.payload_bytes_decompressed += bytes.len() as u64;
                    Ok(Some(bytes))
                };
                let (i, o) = (read(INPUTS)?, read(OUTPUTS)?);
                if matches(&want, i.as_deref(), o.as_deref()) {
                    let project = v
                        .strings()
                        .get(h.project)
                        .ok_or_else(|| corrupt("project is not in the partition"))?;
                    hits.push(hit(
                        project,
                        RunKey {
                            start_ns: h.start_ns,
                            ..k
                        },
                    ));
                    if hits.len() == q.limit {
                        break 'outer;
                    }
                }
            }
        }
        if hits.len() >= q.limit {
            break;
        }
    }
    hits.truncate(q.limit);
    ph.mark("streams");
    Ok(done(hits, st, ph))
}

fn matches(want: &[Vec<u8>], inputs: Option<&[u8]>, outputs: Option<&[u8]>) -> bool {
    let mut have = tokens(inputs.unwrap_or_default());
    have.extend(tokens(outputs.unwrap_or_default()));
    want.iter().all(|w| have.contains(w))
}

fn hit(project: &str, k: RunKey) -> SearchHit {
    SearchHit {
        project: project.to_string(),
        trace_id: k.trace_id,
        span_id: k.span_id,
        start_ns: k.start_ns,
    }
}
