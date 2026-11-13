//! The `Store` queries over one snapshot of partitions (`views`, in creation order, which
//! is also version order). Layouts answer within a partition; this module merges a span's
//! pieces across partitions with `Span::merge_from`, in partition order.

use std::cmp::Reverse;
use std::collections::hash_map::Entry;
use std::collections::{BTreeMap, BinaryHeap, HashMap, HashSet};
use std::time::Instant;

use newdb_format::sealed::{Loc, Sealed};
use newdb_layouts::{Cost, fts};
use newdb_types::header::field::{END, ERROR, INPUTS, MODEL, NAME, OUTPUTS, PARENT, SESSION};
use newdb_types::text::{for_each_token, tokens};
use newdb_types::{
    PayloadRef, Read, ReadStats, Result, Run, RunKey, RunQuery, RunSummary, RunType, SearchHit,
    SearchQuery, Span, SpanHeader, Status, TraceId, TraceQuery,
};

use crate::part::{
    ALL_PAYLOADS, BlobRef, PAYLOAD_FIELDS, Payloads, View, corrupt, payload_ref, set_payload,
    summary_key, to_span,
};
use crate::spread::{self, Spread};

/// One span merged across partitions, without payloads until [`load_payloads`].
pub(crate) struct Merged {
    pub span: Span,
    /// `(partition index, header merged within that partition)`, in partition order:
    /// the first piece, then any others (most spans have one, so no allocation).
    first: (usize, SpanHeader),
    rest: Vec<(usize, SpanHeader)>,
    /// The partition that lists this span's run summary under its true start: the last
    /// one whose piece sets `start_ns`, or the last piece when none does.
    owner: usize,
    owner_has_start: bool,
}

impl Merged {
    fn pieces(&self) -> impl DoubleEndedIterator<Item = &(usize, SpanHeader)> {
        std::iter::once(&self.first).chain(&self.rest)
    }
}

fn add_cost(st: &mut ReadStats, c: Cost) {
    st.records_visited += c.records_visited;
    st.bytes_read += c.bytes_read;
}

/// Every span of trace `t` (of `project`, or of all projects), merged across partitions.
/// With `wake`, wakes the read pool once the payloads in the headers reach
/// [`PARALLEL_PAYLOAD_BYTES`], so its threads are up when the payloads are read.
pub(crate) fn load_trace(
    views: &[View<'_>],
    t: TraceId,
    project: Option<&str>,
    wake: bool,
    st: &mut ReadStats,
) -> Result<Vec<Merged>> {
    let mut out: Vec<Merged> = Vec::new();
    let mut woken = !wake;
    let mut raw = 0u64;
    // (project, span) -> index in `out`, built when a second partition has the trace:
    // within one partition every span appears once.
    let mut index: Option<HashMap<(&str, newdb_types::SpanId), usize>> = None;
    let mut first: Option<usize> = None;
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
        if headers.is_empty() {
            continue;
        }
        if !woken {
            raw += headers
                .iter()
                .flat_map(|h| PAYLOAD_FIELDS.map(|b| (h.has(b), payload_ref(h, b).raw_len)))
                .filter_map(|(has, n)| has.then_some(n as u64))
                .sum::<u64>();
            if raw >= PARALLEL_PAYLOAD_BYTES {
                crate::pool::wake();
                woken = true;
            }
        }
        match first {
            None => {
                first = Some(i);
                out.reserve(headers.len());
            }
            Some(f) if index.is_none() => {
                let mut ix = HashMap::with_capacity(out.len() + headers.len());
                for (k, m) in out.iter().enumerate() {
                    let p = views[f]
                        .strings()
                        .get(m.first.1.project)
                        .ok_or_else(|| corrupt("project is not in the partition"))?;
                    ix.insert((p, m.span.span_id), k);
                }
                index = Some(ix);
            }
            Some(_) => {}
        }
        let mut headers = headers;
        if let Some(id) = pid {
            headers.retain(|h| h.project == id);
        }
        // Only once the pool is awake for a large payload read: waking it just to build
        // spans costs more than it saves.
        let pooled = wake && woken;
        let mut built = build_spans(&headers, v, pooled)?.map(Vec::into_iter);
        for h in headers {
            let s = match built.as_mut().and_then(|b| b.next()) {
                Some(s) => s,
                None => to_span(&h, v.strings(), v.blob(), 0, &mut 0)?,
            };
            let has_start = s.start_ns.is_some();
            let project = v
                .strings()
                .get(h.project)
                .ok_or_else(|| corrupt("project is not in the partition"))?;
            match index.as_ref().and_then(|ix| ix.get(&(project, s.span_id))) {
                Some(&k) => {
                    let m: &mut Merged = &mut out[k];
                    m.span.merge_from(&s);
                    m.rest.push((i, h));
                    if has_start || !m.owner_has_start {
                        m.owner = i;
                        m.owner_has_start |= has_start;
                    }
                }
                None => {
                    if let Some(ix) = index.as_mut() {
                        ix.insert((project, s.span_id), out.len());
                    }
                    out.push(Merged {
                        span: s,
                        first: (i, h),
                        rest: Vec::new(),
                        owner: i,
                        owner_has_start: has_start,
                    });
                }
            }
        }
    }
    Ok(out)
}

/// From this many headers on, a partition's spans are built on the read pool.
const PARALLEL_SPANS: usize = 512;

/// `headers` of partition `v` as spans, without payloads, in order, when `pooled` and
/// there are enough to build them on the read pool; `None` to build them one by one in
/// place.
fn build_spans(headers: &[SpanHeader], v: &View<'_>, pooled: bool) -> Result<Option<Vec<Span>>> {
    let (strings, blob) = (v.strings(), v.blob());
    let build = |hs: &[SpanHeader]| -> Result<Vec<Span>> {
        hs.iter()
            .map(|h| to_span(h, strings, blob, 0, &mut 0))
            .collect()
    };
    let threads = crate::pool::workers() + 1;
    if !pooled || headers.len() < PARALLEL_SPANS || threads == 1 {
        return Ok(None);
    }
    let size = headers.len().div_ceil(threads);
    let chunks: Vec<&[SpanHeader]> = headers.chunks(size).collect();
    let mut out = Vec::with_capacity(headers.len());
    for r in crate::pool::each(chunks.len(), &|k| build(chunks[k])) {
        out.extend(r.unwrap_or_else(|| Err(corrupt("span task panicked")))?);
    }
    Ok(Some(out))
}

/// Decompressed sealed frames one query keeps for its single payload reads.
const FRAME_CACHE: usize = 16 << 20;

fn payloads<'a>(views: &'a [View<'_>]) -> Payloads<'a> {
    Payloads::new(views.iter().map(|v| v.blob()).collect(), FRAME_CACHE)
}

/// Below this many raw payload bytes, a trace load decodes on the calling thread only.
pub(crate) const PARALLEL_PAYLOAD_BYTES: u64 = 256 << 10;

/// The piece that sets each payload field in `mask` of `m` (the last piece that has it):
/// `(field, partition, reference)`.
fn payload_jobs(m: &Merged, mask: u32) -> impl Iterator<Item = (u32, usize, PayloadRef)> + '_ {
    PAYLOAD_FIELDS.into_iter().filter_map(move |bit| {
        if mask & bit == 0 {
            return None;
        }
        let (pi, h) = m.pieces().rev().find(|(_, h)| h.has(bit))?;
        Some((bit, *pi, payload_ref(h, bit)))
    })
}

/// Fills the payload fields in `mask` of every span. Large traces are split into
/// contiguous runs of spans of about equal payload bytes, one task each on the read pool
/// (the calling thread takes part); each task finds, decodes and places its own payloads.
/// Spans are in trace order, which is the order their payloads were framed in, so a frame
/// is decoded by one task, or by two when it straddles a run boundary.
fn fill_payloads(
    blobs: &[BlobRef<'_>],
    merged: &mut [Merged],
    mask: u32,
    st: &mut ReadStats,
) -> Result<()> {
    let raws: Vec<u64> = merged
        .iter()
        .map(|m| payload_jobs(m, mask).map(|j| j.2.raw_len as u64).sum())
        .collect();
    let total: u64 = raws.iter().sum();
    let threads = crate::pool::workers() + 1;
    if total < PARALLEL_PAYLOAD_BYTES || threads == 1 || merged.len() < 2 {
        st.payload_bytes_decompressed += fill_range(blobs, merged, mask)?;
        return Ok(());
    }
    let tasks = (2 * threads).min(merged.len());
    let mut chunks: Vec<std::sync::Mutex<&mut [Merged]>> = Vec::with_capacity(tasks);
    let (mut rest, mut acc, mut taken) = (merged, 0u64, 0usize);
    for (i, r) in raws.iter().enumerate() {
        acc += r;
        let k = chunks.len() as u64 + 1;
        if k < tasks as u64 && acc * tasks as u64 >= total * k {
            let (head, tail) = std::mem::take(&mut rest).split_at_mut(i + 1 - taken);
            chunks.push(std::sync::Mutex::new(head));
            rest = tail;
            taken = i + 1;
        }
    }
    if !rest.is_empty() {
        chunks.push(std::sync::Mutex::new(rest));
    }
    let done = crate::pool::each(chunks.len(), &|k| {
        let mut g = chunks[k].lock().unwrap_or_else(|e| e.into_inner());
        fill_range(blobs, &mut g, mask)
    });
    for r in done {
        st.payload_bytes_decompressed +=
            r.unwrap_or_else(|| Err(corrupt("payload task panicked")))?;
    }
    Ok(())
}

/// Fills the payload fields in `mask` of `spans` on this thread and returns the bytes
/// read. Sealed payloads are looked up in ascending offset order (galloping through each
/// partition's entry table) and grouped by frame, so each frame is decompressed once,
/// into a buffer reused across frames; a payload that is its whole frame takes the buffer.
fn fill_range(blobs: &[BlobRef<'_>], spans: &mut [Merged], mask: u32) -> Result<u64> {
    // (partition, offset, span, field, reference) of sealed payloads; others read now.
    let mut lookups: Vec<(usize, u64, usize, u32, PayloadRef)> = Vec::new();
    let mut bytes = 0u64;
    for (i, m) in spans.iter_mut().enumerate() {
        let mut js = [(0, 0, PayloadRef::NONE); PAYLOAD_FIELDS.len()];
        let mut n = 0;
        for j in payload_jobs(m, mask) {
            js[n] = j;
            n += 1;
        }
        for &(bit, pi, r) in &js[..n] {
            match blobs[pi] {
                BlobRef::Sealed(_) => lookups.push((pi, r.offset, i, bit, r)),
                b => {
                    let p = b.read(r)?;
                    bytes += p.len() as u64;
                    set_payload(&mut m.span, bit, p)?;
                }
            }
        }
    }
    lookups.sort_unstable_by_key(|l| (l.0, l.1));
    // (partition, frame, location, span, field), then sorted by frame.
    let mut located: Vec<(usize, u32, Loc, usize, u32)> = Vec::with_capacity(lookups.len());
    let mut at = (usize::MAX, 0);
    for &(pi, offset, i, bit, r) in &lookups {
        let BlobRef::Sealed(s) = blobs[pi] else {
            continue;
        };
        let s = s.get()?;
        if at.0 != pi {
            at = (pi, 0);
        }
        let (next, loc) = s.find_from(at.1, offset);
        at.1 = next;
        match loc {
            Some(loc) if loc.len == r.raw_len => located.push((pi, loc.frame, loc, i, bit)),
            _ => {
                return Err(corrupt(format!(
                    "payloads.sealed: no payload of {} bytes for offset {offset}",
                    r.raw_len
                )));
            }
        }
    }
    located.sort_unstable_by_key(|l| (l.0, l.1, l.2.at));
    let mut buf = Vec::new();
    for run in located.chunk_by(|a, b| (a.0, a.1) == (b.0, b.1)) {
        let BlobRef::Sealed(s) = blobs[run[0].0] else {
            continue;
        };
        let s = s.get()?;
        s.decompress_into(run[0].1 as usize, &mut buf)?;
        for (k, &(_, _, loc, i, bit)) in run.iter().enumerate() {
            let whole = loc.at == 0 && loc.len as usize == buf.len();
            let p = match whole && k + 1 == run.len() {
                true => std::mem::take(&mut buf),
                false => Sealed::get(&buf, loc)?.to_vec(),
            };
            bytes += p.len() as u64;
            set_payload(&mut spans[i].span, bit, p)?;
        }
    }
    Ok(bytes)
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
    let mut merged = load_trace(views, q.trace_id, Some(&q.project), q.payloads, &mut st)?;
    ph.mark("headers");
    let mask = if q.payloads { ALL_PAYLOADS } else { ERROR };
    let blobs: Vec<BlobRef<'_>> = views.iter().map(|v| v.blob()).collect();
    fill_payloads(&blobs, &mut merged, mask, &mut st)?;
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
                e.insert(load_trace(views, s.trace_id, None, false, &mut st)?);
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
/// more stream, sorted by merged key ([`spread`], which keeps the merge over sealed
/// partitions between queries). A heap merges the streams newest first; partitions
/// open in order of their newest start, only once they could hold the next hit. Each
/// candidate is confirmed against its merged `inputs` and `outputs`.
pub(crate) fn search(
    views: &[View<'_>],
    multi: &HashSet<TraceId>,
    cache: &spread::Cache,
    q: &SearchQuery,
) -> Result<Read<Vec<SearchHit>>> {
    let mut ph = Phases::new(q.timings);
    let mut st = ReadStats::default();
    let want = tokens(q.text.as_bytes());
    if want.is_empty() || q.limit == 0 {
        return Ok(done(Vec::new(), st, ph));
    }
    let toks: Vec<&[u8]> = want.iter().map(Vec::as_slice).collect();
    let mut pay = payloads(views);

    // Spans of spread traces, merged, newest first.
    let sealed = views.iter().take_while(|v| v.sealed).count();
    let state = cache.get(&views[..sealed], multi, &mut st)?;
    let mut spread = Spread::new(
        &state,
        &views[sealed..],
        multi,
        q.project.as_deref(),
        &mut st,
    )?;
    let view_of: HashMap<u64, usize> = views.iter().enumerate().map(|(i, v)| (v.id, i)).collect();
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
    if let Some(k) = spread.peek() {
        heap.push((k, usize::MAX));
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
            let Some(s) = spread.pop() else {
                break;
            };
            if let Some(k2) = spread.peek() {
                heap.push((k2, usize::MAX));
            }
            // Each payload field comes from the last piece that set it.
            let mut read = |bit: u32| -> Result<Option<Vec<u8>>> {
                let Some((id, h)) = s.pieces.iter().rev().find(|(_, h)| h.has(bit)) else {
                    return Ok(None);
                };
                let pi = *view_of
                    .get(id)
                    .ok_or_else(|| corrupt("spread span from a partition not in the snapshot"))?;
                let bytes = pay.read(pi, payload_ref(h, bit))?;
                st.payload_bytes_decompressed += bytes.len() as u64;
                Ok(Some(bytes))
            };
            let (i, o) = (read(INPUTS)?, read(OUTPUTS)?);
            if matches(&want, i.as_deref(), o.as_deref()) {
                hits.push(hit(&s.span.project, k));
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
                    let bytes = pay.read(pi, payload_ref(h, bit))?;
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

/// Whether every token of `want` (sorted and deduplicated, as [`tokens`] returns them)
/// appears in `inputs` or `outputs`. Scans the payloads without collecting their tokens.
fn matches(want: &[Vec<u8>], inputs: Option<&[u8]>, outputs: Option<&[u8]>) -> bool {
    let mut found = vec![false; want.len()];
    let mut left = want.len();
    for text in [inputs, outputs].into_iter().flatten() {
        for_each_token(text, |t| {
            if let Ok(i) = want.binary_search_by(|w| w.as_slice().cmp(t))
                && !found[i]
            {
                found[i] = true;
                left -= 1;
            }
        });
        if left == 0 {
            return true;
        }
    }
    left == 0
}

fn hit(project: &str, k: RunKey) -> SearchHit {
    SearchHit {
        project: project.to_string(),
        trace_id: k.trace_id,
        span_id: k.span_id,
        start_ns: k.start_ns,
    }
}
