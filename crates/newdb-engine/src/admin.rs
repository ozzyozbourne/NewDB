//! `verify` and `inspect`: read-only checks and facts about the stored data.

use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::Path;

use newdb_format::manifest::{self, PartitionEntry};
use newdb_format::{log, overlap, payload, strings as strtbl};
use newdb_layouts::{LAYOUTS, LayoutFiles};
use newdb_types::{Result, SpanId, TraceId};

use crate::files::{self, LAYOUTS_DIR, LOG, OVERLAP, PAYLOADS, STRINGS, TRASH, layout_dir_name};
use crate::part::{Mode, log_traces, present, replay};
use crate::{Inner, InspectReport, PartitionInfo, VerifyReport};

pub(crate) fn verify(inner: &Inner) -> Result<VerifyReport> {
    let mut rep = VerifyReport::default();
    let disk = match manifest::read(&inner.path) {
        Ok(m) => m.unwrap_or_default(),
        Err(e) => {
            rep.problems.push(format!("MANIFEST: {e}"));
            return Ok(rep);
        }
    };
    let parts = inner.parts();
    let in_memory: HashSet<&str> = parts
        .iter()
        .filter(|p| p.sealed)
        .map(|p| p.name.as_str())
        .collect();
    let on_disk: HashSet<&str> = disk.partitions.iter().map(|e| e.name.as_str()).collect();
    for n in in_memory.symmetric_difference(&on_disk) {
        rep.problems.push(format!(
            "partition {n}: sealed in the open database but not in MANIFEST, or the reverse"
        ));
    }
    for e in &disk.partitions {
        let dir = inner.path.join(&e.name);
        if !dir.is_dir() {
            rep.problems.push(format!(
                "partition {}: listed in MANIFEST but missing",
                e.name
            ));
            continue;
        }
        rep.partitions_checked += 1;
        check_partition(&dir, e, &mut rep);
        let earlier: Vec<_> = parts
            .iter()
            .filter(|p| p.sealed && p.id < e.id)
            .cloned()
            .collect();
        if let Err(m) = check_overlap(&dir, &earlier) {
            rep.problems
                .push(format!("partition {}: overlap.bin: {m}", e.name));
        }
    }
    let unsealed: HashSet<&str> = parts
        .iter()
        .filter(|p| !p.sealed)
        .map(|p| p.name.as_str())
        .collect();
    for d in fs::read_dir(&inner.path)?.flatten() {
        let name = d.file_name().to_string_lossy().into_owned();
        if d.file_type().is_ok_and(|t| t.is_dir())
            && !name.starts_with('.')
            && name != TRASH
            && !on_disk.contains(name.as_str())
            && !unsealed.contains(name.as_str())
        {
            rep.problems.push(format!(
                "directory {name} is neither a sealed nor the active partition"
            ));
        }
    }
    Ok(rep)
}

/// `overlap.bin` lists every trace this partition shares with an earlier partition (it may
/// list more, for earlier partitions that retention deleted).
fn check_overlap(
    dir: &Path,
    earlier: &[std::sync::Arc<crate::part::Part>],
) -> std::result::Result<(), String> {
    let stored = overlap::decode(&fs::read(dir.join(OVERLAP)).map_err(|e| e.to_string())?)
        .map_err(|e| e.to_string())?;
    let stored: HashSet<TraceId> = stored.into_iter().collect();
    let traces = log_traces(dir).map_err(|e| e.to_string())?;
    let shared = present(earlier, &traces).map_err(|e| e.to_string())?;
    match shared.iter().filter(|t| !stored.contains(t)).count() {
        0 => Ok(()),
        n => Err(format!("{n} shared traces are not listed")),
    }
}

fn check_partition(dir: &Path, e: &PartitionEntry, rep: &mut VerifyReport) {
    let mut problem = |m: String| rep.problems.push(format!("partition {}: {m}", e.name));
    let mut builders: Vec<_> = LAYOUTS.iter().map(|d| (d, (d.new_builder)())).collect();
    let r = match replay(dir, Mode::Verify, &mut |b| {
        builders.iter_mut().for_each(|(_, x)| x.apply(b))
    }) {
        Ok(r) => r,
        Err(err) => return problem(format!("{err}")),
    };
    let s = &r.stats;
    let got = (
        r.id,
        r.hour,
        s.posts,
        s.spans,
        s.min_start(),
        s.max_start(),
        s.version.map_or(0, |v| v.0),
        s.max_version(),
    );
    let want = (
        e.id,
        e.hour,
        e.posts,
        e.spans,
        e.min_start_ns,
        e.max_start_ns,
        e.min_version,
        e.max_version,
    );
    if got != want {
        problem(format!(
            "log says (id, hour, posts, spans, start, versions) = {got:?}, MANIFEST says {want:?}"
        ));
    }
    match fs::read(dir.join(STRINGS)) {
        Ok(b) if b == strtbl::encode(&r.strings.list) => {}
        Ok(_) => problem("strings.tbl differs from the strings in the log".into()),
        Err(err) => problem(format!("strings.tbl: {err}")),
    }
    for (def, b) in &builders {
        rep.layouts_checked += 1;
        let mut problem = |m: String| {
            rep.problems.push(format!(
                "partition {}: layout {} v{}: {m}",
                e.name, def.name, def.version
            ))
        };
        if !e
            .layouts
            .iter()
            .any(|(n, v)| n == def.name && *v == def.version)
        {
            problem("not built for this version (run rebuild)".into());
            continue;
        }
        let mut files = LayoutFiles::default();
        if let Err(err) = b.seal(&mut files) {
            problem(format!("rebuild failed: {err}"));
            continue;
        }
        let mut want: Vec<(String, u128)> = files
            .files
            .iter()
            .map(|(n, b)| (n.clone(), payload::hash(b)))
            .collect();
        want.sort();
        let ld = dir
            .join(LAYOUTS_DIR)
            .join(layout_dir_name(def.name, def.version));
        let mut have = Vec::new();
        match fs::read_dir(&ld) {
            Ok(rd) => {
                for f in rd.flatten() {
                    let n = f.file_name().to_string_lossy().into_owned();
                    match fs::read(f.path()) {
                        Ok(b) => have.push((n, payload::hash(&b))),
                        Err(err) => problem(format!("{n}: {err}")),
                    }
                }
            }
            Err(err) => problem(format!("{}: {err}", ld.display())),
        }
        have.sort();
        if want != have {
            problem("files on disk differ from a rebuild from the log".into());
        }
    }
}

/// `(p50, p99, max)`, 0s when empty.
fn percentiles(mut v: Vec<u64>) -> (u64, u64, u64) {
    if v.is_empty() {
        return (0, 0, 0);
    }
    v.sort_unstable();
    let at = |p: usize| v[(v.len() - 1) * p / 100];
    (at(50), at(99), v[v.len() - 1])
}

pub(crate) fn inspect(inner: &Inner, partitions: Vec<PartitionInfo>) -> Result<InspectReport> {
    // Number of partitions holding posts of each span.
    let mut span_parts: HashMap<(String, TraceId, SpanId), u32> = HashMap::new();
    let mut sizes = Vec::new();
    for p in inner.parts() {
        let bytes = files::map(&p.dir.join(LOG))?;
        let mut strings: Vec<String> = Vec::new();
        let mut seen = HashSet::new();
        for rec in log::Reader::new(&bytes)? {
            match rec.kind {
                log::kind::STRING => strings.push(rec.string()?.1.to_string()),
                log::kind::SPAN_POST => {
                    let h = rec.post()?;
                    if seen.insert((h.project, h.trace_id, h.span_id)) {
                        let project = strings
                            .get((h.project as usize).wrapping_sub(1))
                            .cloned()
                            .unwrap_or_default();
                        *span_parts
                            .entry((project, h.trace_id, h.span_id))
                            .or_default() += 1;
                    }
                }
                _ => {}
            }
        }
        let pay = files::map(&p.dir.join(PAYLOADS))?;
        sizes.extend(payload::Scan::new(&pay)?.map(|e| e.r.raw_len as u64));
    }
    let mut per_trace: HashMap<(&str, TraceId), u64> = HashMap::new();
    for (project, t, _) in span_parts.keys() {
        *per_trace.entry((project.as_str(), *t)).or_default() += 1;
    }
    let cross = span_parts.values().filter(|&&n| n > 1).count();
    let reads = inner.reads.lock().unwrap_or_else(|e| e.into_inner());
    let mut ops: Vec<_> = reads
        .iter()
        .map(|(op, d)| {
            let (p50, p99, _) = percentiles(d.iter().copied().collect());
            (op.to_string(), d.len() as u64, p50, p99)
        })
        .collect();
    ops.sort();
    Ok(InspectReport {
        partitions,
        spans_per_trace: percentiles(per_trace.into_values().collect()),
        payload_bytes: percentiles(sizes),
        cross_partition_span_share: if span_parts.is_empty() {
            0.0
        } else {
            cross as f64 / span_parts.len() as f64
        },
        reads: ops,
    })
}
