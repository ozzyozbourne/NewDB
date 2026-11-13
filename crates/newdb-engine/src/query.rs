//! The `Store` queries over one snapshot of partitions (`views`, in creation order, which
//! is also version order). Layouts answer within a partition; this module merges a span's
//! pieces across partitions with `Span::merge_from`, in partition order.

use std::cmp::Reverse;
use std::collections::hash_map::Entry;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::time::Instant;

use newdb_layouts::Cost;
use newdb_types::header::field::{END, ERROR, INPUTS, MODEL, NAME, OUTPUTS, PARENT, SESSION};
use newdb_types::text::tokens;
use newdb_types::{
    Read, ReadStats, Result, Run, RunKey, RunQuery, RunSummary, RunType, SearchHit, SearchQuery,
    Span, SpanHeader, SpanId, Status, TraceId, TraceQuery,
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
pub(crate) fn runs(views: &[View<'_>], q: &RunQuery) -> Result<Read<Vec<Run>>> {
    let mut ph = Phases::new(q.timings);
    let mut st = ReadStats::default();
    // Ascending by (key, project): the first entry is the oldest kept.
    let mut best: BTreeMap<(RunKey, String), Run> = BTreeMap::new();
    let mut cache: HashMap<TraceId, Vec<Merged>> = HashMap::new();
    let mut multis: HashMap<TraceId, bool> = HashMap::new();
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
            if !multi(views, pi, s.trace_id, &mut multis, &mut st)? {
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

/// Whether trace `t` has posts in a partition other than `pi` (cached per query). The
/// in-memory filters rule out most partitions; a filter hit is confirmed by a lookup.
fn multi(
    views: &[View<'_>],
    pi: usize,
    t: TraceId,
    cache: &mut HashMap<TraceId, bool>,
    st: &mut ReadStats,
) -> Result<bool> {
    if let Some(&m) = cache.get(&t) {
        return Ok(m);
    }
    let mut m = false;
    for (j, w) in views.iter().enumerate() {
        if j != pi && w.may_contain(t) {
            let (h, cost) = w.trace(t)?;
            add_cost(st, cost);
            if !h.is_empty() {
                m = true;
                break;
            }
        }
    }
    cache.insert(t, m);
    Ok(m)
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

/// A search candidate: a span complete in one partition (`Local`, its merged header
/// there), or a span merged across partitions (`Merged`, index into the loaded traces).
enum Cand {
    Local(usize, SpanHeader),
    Merged(usize, usize),
}

/// Newest-first search, exact. A span whose trace lives in one partition matches only if
/// that partition's postings hold every token, so it comes from the per-partition
/// intersection and is keyed and confirmed from that partition alone. A span of a trace
/// with posts in several partitions can match with its tokens split across them, so it
/// becomes a candidate when any partition lists it for any token, and is merged. All
/// candidates are sorted by key, then confirmed against the merged `inputs` and `outputs`
/// until `limit`.
pub(crate) fn search(views: &[View<'_>], q: &SearchQuery) -> Result<Read<Vec<SearchHit>>> {
    let mut ph = Phases::new(q.timings);
    let mut st = ReadStats::default();
    let want = tokens(q.text.as_bytes());
    if want.is_empty() || q.limit == 0 {
        return Ok(done(Vec::new(), st, ph));
    }
    let mut multis = HashMap::new();
    let mut spread: HashSet<(TraceId, SpanId)> = HashSet::new();
    let mut keyed: Vec<(Reverse<(RunKey, &str)>, Cand)> = Vec::new();
    for (pi, v) in views.iter().enumerate() {
        let pid = match &q.project {
            Some(p) => match v.strings().id(p) {
                Some(id) => Some(id),
                None => continue,
            },
            None => None,
        };
        st.partitions_touched += 1;
        // Every (trace, span) listed for a query token here, sorted, one entry per token.
        let mut listed: Vec<(TraceId, SpanId)> = Vec::new();
        for tok in &want {
            let (ps, cost) = v.postings(tok)?;
            add_cost(&mut st, cost);
            listed.extend(ps);
        }
        listed.sort_unstable();
        let mut at = 0;
        while at < listed.len() {
            let t = listed[at].0;
            let mut spans = Vec::new();
            while at < listed.len() && listed[at].0 == t {
                let s = listed[at].1;
                let n = listed[at..]
                    .iter()
                    .take_while(|x| x.1 == s && x.0 == t)
                    .count();
                spans.push((s, n == want.len()));
                at += n;
            }
            if multi(views, pi, t, &mut multis, &mut st)? {
                spread.extend(spans.iter().map(|(s, _)| (t, *s)));
                continue;
            }
            if !spans.iter().any(|(_, all)| *all) {
                continue;
            }
            let (headers, cost) = v.trace(t)?;
            add_cost(&mut st, cost);
            for h in headers {
                let full = spans.iter().any(|(s, all)| *all && *s == h.span_id);
                if full && pid.is_none_or(|id| h.project == id) {
                    let project = v
                        .strings()
                        .get(h.project)
                        .ok_or_else(|| corrupt("project is not in the partition"))?;
                    let key = RunKey {
                        start_ns: h.start_ns,
                        trace_id: t,
                        span_id: h.span_id,
                    };
                    keyed.push((Reverse((key, project)), Cand::Local(pi, h)));
                }
            }
        }
    }
    ph.mark("postings");

    let mut traces: Vec<TraceId> = spread.iter().map(|(t, _)| *t).collect();
    traces.sort_unstable();
    traces.dedup();
    let mut loaded = Vec::with_capacity(traces.len());
    for t in traces {
        loaded.push(load_trace(views, t, q.project.as_deref(), &mut st)?);
    }
    for (ti, merged) in loaded.iter().enumerate() {
        for (j, m) in merged.iter().enumerate() {
            let (t, s) = (m.span.trace_id, m.span.span_id);
            if spread.contains(&(t, s)) {
                let key = RunKey {
                    start_ns: m.span.start_ns.unwrap_or(0),
                    trace_id: t,
                    span_id: s,
                };
                keyed.push((Reverse((key, m.span.project.as_str())), Cand::Merged(ti, j)));
            }
        }
    }
    keyed.sort_unstable_by(|a, b| a.0.cmp(&b.0));
    ph.mark("candidates");

    let mut hits = Vec::new();
    for (Reverse((key, project)), cand) in &keyed {
        let (inputs, outputs) = match cand {
            Cand::Local(pi, h) => {
                let b = views[*pi].blob();
                let mut read = |bit: u32| -> Result<Option<Vec<u8>>> {
                    if !h.has(bit) {
                        return Ok(None);
                    }
                    let bytes = b.read(payload_ref(h, bit))?;
                    st.payload_bytes_decompressed += bytes.len() as u64;
                    Ok(Some(bytes))
                };
                (read(INPUTS)?, read(OUTPUTS)?)
            }
            Cand::Merged(ti, j) => {
                let mut m = Merged {
                    span: loaded[*ti][*j].span.clone(),
                    pieces: loaded[*ti][*j].pieces.clone(),
                    owner: 0,
                    owner_has_start: false,
                };
                load_payloads(views, &mut m, INPUTS | OUTPUTS, &mut st)?;
                (m.span.inputs, m.span.outputs)
            }
        };
        let mut have = tokens(inputs.as_deref().unwrap_or_default());
        have.extend(tokens(outputs.as_deref().unwrap_or_default()));
        if want.iter().all(|w| have.contains(w)) {
            hits.push(SearchHit {
                project: project.to_string(),
                trace_id: key.trace_id,
                span_id: key.span_id,
                start_ns: key.start_ns,
            });
            if hits.len() == q.limit {
                break;
            }
        }
    }
    ph.mark("confirm");
    Ok(done(hits, st, ph))
}
