//! Zero-copy trace reads, `Store::with_trace` for [`crate::Db`]. A [`SpanRef`] borrows
//! its strings from the partitions' string tables and its payloads from the sealed frames
//! this one query decompressed, so a trace load allocates per frame instead of per
//! payload and per string, and copies no payload out of its frame.

use std::collections::HashMap;

use newdb_format::sealed::Loc;
use newdb_types::header::field::{
    COMPLETION_TOKENS, COST, END, ERROR, MODEL, NAME, PARENT, PROMPT_TOKENS, PROVIDER, RUN_TYPE,
    SESSION, START, STATUS,
};
use std::time::Instant;

use newdb_types::{
    PayloadRef, ReadStats, Result, RunType, SpanHeader, SpanId, SpanRef, Status, TraceQuery,
};

use crate::part::{ALL_PAYLOADS, BlobRef, PAYLOAD_FIELDS, Strings, View, corrupt, payload_ref};
use crate::query::PARALLEL_PAYLOAD_BYTES;

/// Where one payload field's bytes are after decoding.
#[derive(Clone, Copy)]
enum Bytes {
    None,
    /// In decompressed frame `frames[k]`.
    Frame(usize, Loc),
    /// In `owned[k]` (payloads of partitions without `payloads.sealed`).
    Owned(usize),
}

/// Applies the merged header `h` of partition `vi` on top of `s`, as `Span::merge_from`
/// does, and records which piece sets each payload field in `mask`.
fn apply<'a>(
    s: &mut SpanRef<'a>,
    pay: &mut [Option<(usize, PayloadRef)>; PAYLOAD_FIELDS.len()],
    h: &SpanHeader,
    vi: usize,
    strings: &'a Strings,
    mask: u32,
) -> Result<()> {
    let st = |id: u32| {
        strings
            .get(id)
            .ok_or_else(|| corrupt(format!("string ID {id} is not in the partition")))
    };
    macro_rules! set {
        ($bit:expr, $f:ident, $v:expr) => {
            if h.has($bit) {
                s.$f = Some($v);
            }
        };
    }
    set!(PARENT, parent_span_id, h.parent_span_id);
    set!(NAME, name, st(h.name)?);
    set!(START, start_ns, h.start_ns);
    set!(END, end_ns, h.end_ns);
    set!(
        STATUS,
        status,
        Status::from_u8(h.status).ok_or_else(|| corrupt("bad status"))?
    );
    set!(
        RUN_TYPE,
        run_type,
        RunType::from_u8(h.run_type).ok_or_else(|| corrupt("bad run type"))?
    );
    set!(MODEL, model, st(h.model)?);
    set!(PROVIDER, provider, st(h.provider)?);
    set!(SESSION, session, st(h.session)?);
    set!(PROMPT_TOKENS, prompt_tokens, h.prompt_tokens);
    set!(COMPLETION_TOKENS, completion_tokens, h.completion_tokens);
    set!(COST, cost_micro_usd, h.cost_micro_usd);
    s.version = s.version.max(h.version);
    for (k, bit) in PAYLOAD_FIELDS.into_iter().enumerate() {
        if mask & bit != 0 && h.has(bit) {
            pay[k] = Some((vi, payload_ref(h, bit)));
        }
    }
    Ok(())
}

/// The spans of `q`'s trace, as `query::trace` returns them, passed borrowed to `f`.
pub(crate) fn with_trace(
    views: &[View<'_>],
    q: &TraceQuery,
    f: &mut dyn FnMut(&[SpanRef<'_>]),
) -> Result<ReadStats> {
    let mut stats = ReadStats::default();
    let mut last = Instant::now();
    let mut mark = |stats: &mut ReadStats, name: &'static str| {
        if q.timings {
            let now = Instant::now();
            stats.phases_ns.push((name, (now - last).as_nanos() as u64));
            last = now;
        }
    };
    let mask = if q.payloads { ALL_PAYLOADS } else { ERROR };

    // Headers of every partition that has the trace in this project.
    let mut pieces: Vec<(usize, Vec<SpanHeader>)> = Vec::new();
    let (mut raw, mut woken) = (0u64, !q.payloads);
    for (i, v) in views.iter().enumerate() {
        if !v.may_contain(q.trace_id) {
            continue;
        }
        let Some(pid) = v.strings().id(&q.project) else {
            continue;
        };
        let (mut hs, cost) = v.trace(q.trace_id)?;
        stats.partitions_touched += 1;
        stats.records_visited += cost.records_visited;
        stats.bytes_read += cost.bytes_read;
        hs.retain(|h| h.project == pid);
        if hs.is_empty() {
            continue;
        }
        if !woken {
            raw += hs
                .iter()
                .flat_map(|h| PAYLOAD_FIELDS.map(|b| (h.has(b), payload_ref(h, b).raw_len)))
                .filter_map(|(has, n)| has.then_some(n as u64))
                .sum::<u64>();
            if raw >= PARALLEL_PAYLOAD_BYTES {
                crate::pool::wake();
                woken = true;
            }
        }
        pieces.push((i, hs));
    }

    // Merge in partition order. Within one partition every span appears once, so the
    // index is needed only when a second partition has the trace.
    let mut spans: Vec<SpanRef<'_>> = Vec::new();
    let mut pay: Vec<[Option<(usize, PayloadRef)>; PAYLOAD_FIELDS.len()]> = Vec::new();
    let mut index: HashMap<SpanId, usize> = HashMap::new();
    let multi = pieces.len() > 1;
    for (vi, hs) in &pieces {
        let strings = views[*vi].strings();
        let project = strings
            .get(hs[0].project)
            .ok_or_else(|| corrupt("project is not in the partition"))?;
        spans.reserve(hs.len());
        for h in hs {
            let k = match multi.then(|| index.get(&h.span_id).copied()).flatten() {
                Some(k) => k,
                None => {
                    if multi {
                        index.insert(h.span_id, spans.len());
                    }
                    spans.push(SpanRef {
                        project,
                        trace_id: h.trace_id,
                        span_id: h.span_id,
                        ..SpanRef::default()
                    });
                    pay.push([None; PAYLOAD_FIELDS.len()]);
                    spans.len() - 1
                }
            };
            apply(&mut spans[k], &mut pay[k], h, *vi, strings, mask)?;
        }
    }

    mark(&mut stats, "headers");

    // Find every payload: sealed ones by frame, others read now.
    let blobs: Vec<BlobRef<'_>> = views.iter().map(|v| v.blob()).collect();
    let mut owned_jobs: Vec<(usize, PayloadRef)> = Vec::new();
    let mut at: Vec<[Bytes; PAYLOAD_FIELDS.len()]> =
        vec![[Bytes::None; PAYLOAD_FIELDS.len()]; spans.len()];
    // (partition, blob offset, span, field, reference)
    let mut lookups: Vec<(usize, u64, usize, usize, PayloadRef)> = Vec::new();
    for (si, slots) in pay.iter().enumerate() {
        for (k, slot) in slots.iter().enumerate() {
            let Some((vi, r)) = *slot else { continue };
            match blobs[vi] {
                BlobRef::Sealed(_) => lookups.push((vi, r.offset, si, k, r)),
                _ => {
                    at[si][k] = Bytes::Owned(owned_jobs.len());
                    owned_jobs.push((vi, r));
                }
            }
        }
    }
    // Payloads of partitions without payloads.sealed: one buffer each, large sets on the
    // read pool like `query::fill_payloads`.
    let read_owned = |jobs: &[(usize, PayloadRef)]| -> Result<Vec<Vec<u8>>> {
        jobs.iter().map(|&(vi, r)| blobs[vi].read(r)).collect()
    };
    let owned_total: u64 = owned_jobs.iter().map(|j| j.1.raw_len as u64).sum();
    let threads = crate::pool::workers() + 1;
    let owned: Vec<Vec<u8>> =
        if owned_total < PARALLEL_PAYLOAD_BYTES || threads == 1 || owned_jobs.len() < 2 {
            read_owned(&owned_jobs)?
        } else {
            let tasks = (2 * threads).min(owned_jobs.len());
            let mut runs: Vec<&[(usize, PayloadRef)]> = Vec::with_capacity(tasks);
            let (mut start, mut acc) = (0usize, 0u64);
            for (i, j) in owned_jobs.iter().enumerate() {
                acc += j.1.raw_len as u64;
                let k = runs.len() as u64 + 1;
                if k < tasks as u64 && acc * tasks as u64 >= owned_total * k {
                    runs.push(&owned_jobs[start..=i]);
                    start = i + 1;
                }
            }
            if start < owned_jobs.len() {
                runs.push(&owned_jobs[start..]);
            }
            let mut out = Vec::with_capacity(owned_jobs.len());
            for r in crate::pool::each(runs.len(), &|k| read_owned(runs[k])) {
                out.extend(r.unwrap_or_else(|| Err(corrupt("payload task panicked")))?);
            }
            out
        };
    lookups.sort_unstable_by_key(|l| (l.0, l.1));
    // (partition, frame) to decompress, and which payloads sit in each.
    let mut frames_needed: Vec<(usize, u32)> = Vec::new();
    let mut located: Vec<(usize, u32, Loc, usize, usize)> = Vec::with_capacity(lookups.len());
    let mut cursor = (usize::MAX, 0usize);
    for &(vi, offset, si, k, r) in &lookups {
        let BlobRef::Sealed(s) = blobs[vi] else {
            continue;
        };
        if cursor.0 != vi {
            cursor = (vi, 0);
        }
        let (next, loc) = s.find_from(cursor.1, offset);
        cursor.1 = next;
        match loc {
            Some(loc) if loc.len == r.raw_len => located.push((vi, loc.frame, loc, si, k)),
            _ => {
                return Err(corrupt(format!(
                    "payloads.sealed: no payload of {} bytes for offset {offset}",
                    r.raw_len
                )));
            }
        }
    }
    located.sort_unstable_by_key(|l| (l.0, l.1));
    for &(vi, frame, loc, si, k) in &located {
        if frames_needed.last() != Some(&(vi, frame)) {
            frames_needed.push((vi, frame));
        }
        at[si][k] = Bytes::Frame(frames_needed.len() - 1, loc);
    }

    // Decompress each needed frame once; large traces on the read pool, in contiguous
    // runs of about equal raw bytes.
    let frame_raw = |&(vi, fr): &(usize, u32)| -> u64 {
        match blobs[vi] {
            BlobRef::Sealed(s) => s.frame(fr as usize).raw_len as u64,
            _ => 0,
        }
    };
    let decode = |run: &[(usize, u32)]| -> Result<Vec<Vec<u8>>> {
        run.iter()
            .map(|&(vi, fr)| match blobs[vi] {
                BlobRef::Sealed(s) => s.decompress(fr as usize),
                _ => Err(corrupt("payloads.sealed: lookup")),
            })
            .collect()
    };
    let total: u64 = frames_needed.iter().map(frame_raw).sum();
    let frames: Vec<Vec<u8>> =
        if total < PARALLEL_PAYLOAD_BYTES || threads == 1 || frames_needed.len() < 2 {
            decode(&frames_needed)?
        } else {
            let tasks = threads.min(frames_needed.len());
            let mut runs: Vec<&[(usize, u32)]> = Vec::with_capacity(tasks);
            let (mut start, mut acc) = (0usize, 0u64);
            for (i, fr) in frames_needed.iter().enumerate() {
                acc += frame_raw(fr);
                let k = runs.len() as u64 + 1;
                if k < tasks as u64 && acc * tasks as u64 >= total * k {
                    runs.push(&frames_needed[start..=i]);
                    start = i + 1;
                }
            }
            if start < frames_needed.len() {
                runs.push(&frames_needed[start..]);
            }
            let mut out = Vec::with_capacity(frames_needed.len());
            for r in crate::pool::each(runs.len(), &|k| decode(runs[k])) {
                out.extend(r.unwrap_or_else(|| Err(corrupt("payload task panicked")))?);
            }
            out
        };

    // Point every payload field at its bytes.
    for (s, places) in spans.iter_mut().zip(&at) {
        for (k, place) in places.iter().enumerate() {
            let bytes: &[u8] = match *place {
                Bytes::None => continue,
                Bytes::Frame(fi, loc) => newdb_format::sealed::Sealed::get(&frames[fi], loc)?,
                Bytes::Owned(oi) => &owned[oi],
            };
            stats.payload_bytes_decompressed += bytes.len() as u64;
            match PAYLOAD_FIELDS[k] {
                ERROR => {
                    s.error = Some(
                        std::str::from_utf8(bytes)
                            .map_err(|_| corrupt("error payload is not UTF-8"))?,
                    )
                }
                bit => {
                    let field = match bit {
                        newdb_types::header::field::INPUTS => &mut s.inputs,
                        newdb_types::header::field::OUTPUTS => &mut s.outputs,
                        newdb_types::header::field::ATTRIBUTES => &mut s.attributes,
                        _ => &mut s.events,
                    };
                    *field = Some(bytes);
                }
            }
        }
    }
    spans.sort_by_key(|s| (s.start_ns.unwrap_or(0), s.span_id));
    mark(&mut stats, "payloads");
    stats.records_returned = spans.len() as u64;
    f(&spans);
    Ok(stats)
}
