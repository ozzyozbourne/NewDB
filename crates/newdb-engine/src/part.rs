//! Partitions: one directory per arrival hour. The active partition (`Live`) answers from
//! its in-memory layout builders; a sealed one (`Fixed`) from its mapped layout files, or
//! from builders replayed from its log when a layout's current version is not on disk.

use std::cell::OnceCell;
use std::collections::{HashMap, HashSet, VecDeque};
use std::fs::{self, File};
use std::io::Read;
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock, RwLock, RwLockReadGuard};

use newdb_format::manifest::PartitionEntry;
use newdb_format::{log, overlap, payload, sealed, strings as strtbl};
use newdb_layouts::{
    Batch, Cost, FileSet, LAYOUTS, LayoutBuilder, LayoutDef, LayoutFiles, fts, runs_by_time,
    trace_clustered, trace_filter,
};
use newdb_types::header::field::*;
use newdb_types::{
    Error, PayloadRef, Result, RunKey, RunSummary, RunType, Span, SpanHeader, SpanId, Status,
    TraceId,
};

use crate::files::{
    self, Bytes, LAYOUTS_DIR, LOG, OVERLAP, PAYLOADS, SEALED_PAYLOADS, STRINGS, layout_dir_name,
};

pub(crate) fn corrupt(msg: impl Into<String>) -> Error {
    Error::Corrupt(msg.into())
}

/// `(project string ID, trace, span)`: one span within a partition.
pub(crate) type Key = (u32, TraceId, SpanId);

pub(crate) const PAYLOAD_FIELDS: [u32; 5] = [INPUTS, OUTPUTS, ATTRIBUTES, EVENTS, ERROR];
pub(crate) const ALL_PAYLOADS: u32 = INPUTS | OUTPUTS | ATTRIBUTES | EVENTS | ERROR;

/// A partition's string table in memory. ID `i + 1` is `list[i]`.
#[derive(Default)]
pub(crate) struct Strings {
    pub list: Vec<String>,
    ids: HashMap<String, u32>,
}

impl Strings {
    pub fn get(&self, id: u32) -> Option<&str> {
        self.list
            .get((id as usize).checked_sub(1)?)
            .map(String::as_str)
    }

    pub fn id(&self, s: &str) -> Option<u32> {
        self.ids.get(s).copied()
    }

    pub fn push(&mut self, s: String) -> u32 {
        self.list.push(s.clone());
        let id = self.list.len() as u32;
        self.ids.insert(s, id);
        id
    }

    pub fn len(&self) -> usize {
        self.list.len()
    }

    fn load(bytes: &[u8]) -> Result<Strings> {
        let t = strtbl::Table::open(bytes)?;
        let mut s = Strings::default();
        for id in 1..=t.len() as u32 {
            s.push(t.get(id).ok_or_else(|| corrupt("strings.tbl"))?.to_string());
        }
        Ok(s)
    }
}

/// Counters a partition keeps about its posts.
#[derive(Clone, Default, Debug)]
pub(crate) struct Stats {
    pub posts: u64,
    pub spans: u64,
    /// Oldest and newest `start_ns` among posts that set it.
    pub start: Option<(u64, u64)>,
    pub version: Option<(u64, u64)>,
}

impl Stats {
    pub fn add(&mut self, h: &SpanHeader) {
        self.posts += 1;
        if h.has(START) {
            let (lo, hi) = self.start.unwrap_or((h.start_ns, h.start_ns));
            self.start = Some((lo.min(h.start_ns), hi.max(h.start_ns)));
        }
        let (lo, hi) = self.version.unwrap_or((h.version, h.version));
        self.version = Some((lo.min(h.version), hi.max(h.version)));
    }

    pub fn min_start(&self) -> u64 {
        self.start.map_or(0, |s| s.0)
    }

    pub fn max_start(&self) -> u64 {
        self.start.map_or(0, |s| s.1)
    }

    pub fn max_version(&self) -> u64 {
        self.version.map_or(0, |v| v.1)
    }

    fn from_entry(e: &PartitionEntry) -> Stats {
        Stats {
            posts: e.posts,
            spans: e.spans,
            start: Some((e.min_start_ns, e.max_start_ns)),
            version: Some((e.min_version, e.max_version)),
        }
    }
}

/// One builder per registered layout. The four built-in layouts are kept as concrete types
/// for their query methods; any other registered layout is only built and sealed.
pub(crate) struct Layouts {
    pub tc: trace_clustered::Builder,
    pub runs: runs_by_time::Builder,
    pub filter: trace_filter::Builder,
    pub fts: fts::Builder,
    /// `LayoutBuilder` is only `Send`; the mutex makes `Layouts` shareable with readers.
    extra: Vec<Extra>,
}

/// Tests set this to run the multi-threaded paths on any machine (nextest runs each test
/// in its own process).
#[cfg(test)]
pub(crate) static FORCE_CORES: std::sync::atomic::AtomicUsize =
    std::sync::atomic::AtomicUsize::new(0);

/// CPUs this process may use.
pub(crate) fn cores() -> usize {
    #[cfg(test)]
    {
        let forced = FORCE_CORES.load(std::sync::atomic::Ordering::Relaxed);
        if forced > 0 {
            return forced;
        }
    }
    static CORES: OnceLock<usize> = OnceLock::new();
    *CORES.get_or_init(|| std::thread::available_parallelism().map_or(1, |n| n.get()))
}

/// Shards of every fts builder this process makes: each core but the writer's and the
/// applier's, at most 8. Fixed per process, so the tokens `writer::prepare` routes on the
/// ingesting threads always fit the builder they reach.
pub(crate) fn fts_shards() -> usize {
    static N: OnceLock<usize> = OnceLock::new();
    *N.get_or_init(|| cores().saturating_sub(2).clamp(1, 8))
}

/// Smaller batches apply faster on one thread than the threads cost to start.
pub(crate) const PARALLEL_APPLY: usize = 256;

/// Builders of [`Layouts`] that apply a batch independently of the other groups.
pub(crate) enum Group {
    Fts(fts::Builder),
    Tc(trace_clustered::Builder),
    Rest(runs_by_time::Builder, trace_filter::Builder, Vec<Extra>),
}

type Extra = (&'static LayoutDef, Mutex<Box<dyn LayoutBuilder>>);

impl Group {
    pub fn apply(&mut self, b: &Batch<'_>) {
        match self {
            Group::Fts(x) => x.apply(b),
            Group::Tc(x) => x.apply(b),
            Group::Rest(runs, filter, extra) => {
                runs.apply(b);
                filter.apply(b);
                for (_, x) in extra.iter_mut() {
                    x.get_mut().unwrap_or_else(|e| e.into_inner()).apply(b);
                }
            }
        }
    }
}

const BUILT_IN: [&str; 4] = [
    trace_clustered::NAME,
    runs_by_time::NAME,
    trace_filter::NAME,
    fts::NAME,
];

impl Layouts {
    pub fn new() -> Layouts {
        Layouts {
            tc: Default::default(),
            runs: Default::default(),
            filter: Default::default(),
            fts: fts::Builder::with_shards(fts_shards()),
            extra: LAYOUTS
                .iter()
                .filter(|d| !BUILT_IN.contains(&d.name))
                .map(|d| (d, Mutex::new((d.new_builder)())))
                .collect(),
        }
    }

    /// Applies a batch to every builder: `fts` and `trace_clustered` on their own threads
    /// when the batch is large and the machine has more than one core.
    pub fn apply(&mut self, b: &Batch<'_>) {
        self.apply_on(b, cores());
    }

    pub fn apply_on(&mut self, b: &Batch<'_>, cores: usize) {
        let mut groups = self.take();
        if cores > 1 && b.headers.len() >= PARALLEL_APPLY {
            let [fts, tc, rest] = &mut groups;
            std::thread::scope(|s| {
                s.spawn(|| fts.apply(b));
                if cores > 2 {
                    s.spawn(|| tc.apply(b));
                } else {
                    tc.apply(b);
                }
                rest.apply(b);
            });
        } else {
            groups.iter_mut().for_each(|g| g.apply(b));
        }
        self.put(groups);
    }

    /// Moves the builders out, in groups that apply independently (and so may apply on
    /// other threads), leaving empty builders until [`Layouts::put`].
    pub fn take(&mut self) -> [Group; 3] {
        use std::mem::take;
        [
            Group::Fts(take(&mut self.fts)),
            Group::Tc(take(&mut self.tc)),
            Group::Rest(
                take(&mut self.runs),
                take(&mut self.filter),
                take(&mut self.extra),
            ),
        ]
    }

    /// Puts back the groups [`Layouts::take`] moved out, in any order.
    pub fn put(&mut self, groups: impl IntoIterator<Item = Group>) {
        for g in groups {
            match g {
                Group::Fts(x) => self.fts = x,
                Group::Tc(x) => self.tc = x,
                Group::Rest(runs, filter, extra) => {
                    self.runs = runs;
                    self.filter = filter;
                    self.extra = extra;
                }
            }
        }
    }

    /// Every registered layout's files, in `LAYOUTS` order.
    pub fn seal(&self) -> Result<Vec<(&'static LayoutDef, LayoutFiles)>> {
        let mut out = Vec::new();
        for def in LAYOUTS {
            let mut f = LayoutFiles::default();
            match def.name {
                trace_clustered::NAME => self.tc.seal(&mut f)?,
                runs_by_time::NAME => self.runs.seal(&mut f)?,
                trace_filter::NAME => self.filter.seal(&mut f)?,
                fts::NAME => self.fts.seal(&mut f)?,
                _ => match self.extra.iter().find(|(d, _)| d.name == def.name) {
                    Some((_, b)) => b.lock().unwrap_or_else(|e| e.into_inner()).seal(&mut f)?,
                    None => continue,
                },
            }
            out.push((def, f));
        }
        Ok(out)
    }
}

/// Where a partition's payload bytes are read from.
pub(crate) enum Blob {
    Map(Bytes),
    /// A sealed partition's `payloads.sealed`.
    Sealed(SealedBlob),
    /// The active partition's file, of which the first `len` bytes are committed.
    File {
        file: File,
        len: u64,
    },
}

#[derive(Clone, Copy)]
pub(crate) enum BlobRef<'a> {
    Slice(&'a [u8]),
    File(&'a File, u64),
    Sealed(SealedRef<'a>),
}

/// A sealed partition's `payloads.sealed`. Opening checks only its fixed header. The
/// tables (megabytes per partition) are validated the first time a query reads a payload
/// from the partition, so opening a database does not walk the tables of every retained
/// partition. A table that fails validation is `Error::Corrupt` on every read that needs
/// it; `verify` checks all of them.
pub(crate) struct SealedBlob {
    bytes: Bytes,
    path: PathBuf,
    layout: OnceLock<sealed::Layout>,
}

impl SealedBlob {
    fn open(path: PathBuf, bytes: Bytes) -> Result<SealedBlob> {
        let head = &bytes[..sealed::HEADER_LEN.min(bytes.len())];
        if sealed::Sealed::tables_end(head)? > bytes.len() {
            return Err(corrupt("payloads.sealed: truncated tables"));
        }
        Ok(SealedBlob {
            bytes,
            path,
            layout: OnceLock::new(),
        })
    }

    /// The validated view. Threads that race to validate each do the work once; the
    /// file is immutable, so they agree.
    pub fn get(&self) -> Result<sealed::Sealed<'_>> {
        let layout = match self.layout.get() {
            Some(l) => *l,
            None => {
                let l = self.check()?;
                *self.layout.get_or_init(|| l)
            }
        };
        sealed::Sealed::with_layout(&self.bytes, layout)
    }

    fn check(&self) -> Result<sealed::Layout> {
        let len = self.bytes.len();
        let head = &self.bytes[..sealed::HEADER_LEN.min(len)];
        let end = sealed::Sealed::tables_end(head)?.min(len);
        // Validate the tables from a plain read: validating through the mapping would
        // keep all of them resident for good. A snapshot can outlive its partition's
        // directory (retention), and then the mapping is all there is.
        match File::open(&self.path) {
            Ok(f) => {
                let mut prefix = Vec::with_capacity(end);
                f.take(end as u64).read_to_end(&mut prefix)?;
                sealed::Sealed::check_tables(&prefix, len as u64)
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                sealed::Sealed::check_tables(&self.bytes[..end], len as u64)
            }
            Err(e) => Err(e.into()),
        }
    }
}

/// A sealed partition's payloads as a query sees them: already validated (replay), or
/// validated when first needed.
#[derive(Clone, Copy)]
pub(crate) enum SealedRef<'a> {
    Checked(sealed::Sealed<'a>),
    Lazy(&'a SealedBlob),
}

impl<'a> SealedRef<'a> {
    pub fn get(self) -> Result<sealed::Sealed<'a>> {
        match self {
            SealedRef::Checked(s) => Ok(s),
            SealedRef::Lazy(b) => b.get(),
        }
    }
}

impl Blob {
    pub fn as_ref(&self) -> BlobRef<'_> {
        match self {
            Blob::Map(b) => BlobRef::Slice(b),
            Blob::File { file, len } => BlobRef::File(file, *len),
            Blob::Sealed(s) => BlobRef::Sealed(SealedRef::Lazy(s)),
        }
    }
}

impl<'a> BlobRef<'a> {
    /// Where `r` lives in a sealed partition's frames; `None` for other partitions.
    pub fn locate(&self, r: PayloadRef) -> Result<Option<(sealed::Sealed<'a>, sealed::Loc)>> {
        let BlobRef::Sealed(s) = *self else {
            return Ok(None);
        };
        let s = s.get()?;
        match s.find(r.offset) {
            Some(loc) if loc.len == r.raw_len => Ok(Some((s, loc))),
            _ => Err(corrupt(format!(
                "payloads.sealed: no payload of {} bytes for offset {}",
                r.raw_len, r.offset
            ))),
        }
    }

    /// Reads one payload. On a sealed partition this decompresses its whole frame; reads
    /// of many payloads go through `query::Frames` or `fill_payloads`, which share frames.
    pub fn read(&self, r: PayloadRef) -> Result<Vec<u8>> {
        match *self {
            BlobRef::Slice(b) => payload::read(b, r),
            BlobRef::Sealed(_) => match self.locate(r)? {
                Some((s, loc)) => {
                    let raw = s.decompress(loc.frame as usize)?;
                    Ok(sealed::Sealed::get(&raw, loc)?.to_vec())
                }
                None => Err(corrupt("payloads.sealed: lookup")),
            },
            BlobRef::File(f, len) => {
                let range = payload::entry_range(r, len)?;
                let mut buf = vec![0u8; range.len()];
                f.read_exact_at(&mut buf, range.start as u64)?;
                payload::decode(&buf, r)
            }
        }
    }
}

/// Payload reads over some partitions' blobs. A sealed partition decompresses a whole
/// frame to read one payload, so the reader keeps recently decompressed frames, up to
/// `budget` bytes, for the payloads that share them (the fields of one span, the spans of
/// one trace).
pub(crate) struct Payloads<'a> {
    blobs: Vec<BlobRef<'a>>,
    frames: HashMap<(usize, u32), Vec<u8>>,
    order: VecDeque<(usize, u32)>,
    cached: usize,
    budget: usize,
}

impl<'a> Payloads<'a> {
    pub fn new(blobs: Vec<BlobRef<'a>>, budget: usize) -> Payloads<'a> {
        Payloads {
            blobs,
            frames: HashMap::new(),
            order: VecDeque::new(),
            cached: 0,
            budget,
        }
    }

    /// The payload `r` of blob `pi`.
    pub fn read(&mut self, pi: usize, r: PayloadRef) -> Result<Vec<u8>> {
        let Some((s, loc)) = self.blobs[pi].locate(r)? else {
            return self.blobs[pi].read(r);
        };
        let key = (pi, loc.frame);
        if !self.frames.contains_key(&key) {
            let raw = s.decompress(loc.frame as usize)?;
            self.cached += raw.len();
            self.frames.insert(key, raw);
            self.order.push_back(key);
            while self.cached > self.budget && self.order.len() > 1 {
                if let Some(old) = self.order.pop_front().and_then(|k| self.frames.remove(&k)) {
                    self.cached -= old.len();
                }
            }
        }
        Ok(sealed::Sealed::get(&self.frames[&key], loc)?.to_vec())
    }
}

pub(crate) fn payload_ref(h: &SpanHeader, bit: u32) -> PayloadRef {
    match bit {
        INPUTS => h.inputs,
        OUTPUTS => h.outputs,
        ATTRIBUTES => h.attributes,
        EVENTS => h.events,
        _ => h.error,
    }
}

pub(crate) fn set_payload(span: &mut Span, bit: u32, bytes: Vec<u8>) -> Result<()> {
    match bit {
        INPUTS => span.inputs = Some(bytes),
        OUTPUTS => span.outputs = Some(bytes),
        ATTRIBUTES => span.attributes = Some(bytes),
        EVENTS => span.events = Some(bytes),
        _ => {
            span.error =
                Some(String::from_utf8(bytes).map_err(|_| corrupt("error payload is not UTF-8"))?)
        }
    }
    Ok(())
}

/// The post (or in-partition merge) `h` as a [`Span`], with the payload fields in `mask`.
/// `decompressed` counts the payload bytes read.
pub(crate) fn to_span(
    h: &SpanHeader,
    strings: &Strings,
    blob: BlobRef<'_>,
    mask: u32,
    decompressed: &mut u64,
) -> Result<Span> {
    let s = |id: u32| {
        strings
            .get(id)
            .map(str::to_owned)
            .ok_or_else(|| corrupt(format!("string ID {id} is not in the partition")))
    };
    let opt = |bit: u32, id: u32| -> Result<Option<String>> {
        if h.has(bit) {
            s(id).map(Some)
        } else {
            Ok(None)
        }
    };
    let status = match h.has(STATUS) {
        true => Some(Status::from_u8(h.status).ok_or_else(|| corrupt("bad status"))?),
        false => None,
    };
    let run_type = match h.has(RUN_TYPE) {
        true => Some(RunType::from_u8(h.run_type).ok_or_else(|| corrupt("bad run type"))?),
        false => None,
    };
    let mut span = Span {
        project: s(h.project)?,
        trace_id: h.trace_id,
        span_id: h.span_id,
        parent_span_id: h.has(PARENT).then_some(h.parent_span_id),
        name: opt(NAME, h.name)?,
        start_ns: h.has(START).then_some(h.start_ns),
        end_ns: h.has(END).then_some(h.end_ns),
        status,
        run_type,
        model: opt(MODEL, h.model)?,
        provider: opt(PROVIDER, h.provider)?,
        session: opt(SESSION, h.session)?,
        prompt_tokens: h.has(PROMPT_TOKENS).then_some(h.prompt_tokens),
        completion_tokens: h.has(COMPLETION_TOKENS).then_some(h.completion_tokens),
        cost_micro_usd: h.has(COST).then_some(h.cost_micro_usd),
        version: h.version,
        ..Span::default()
    };
    for bit in PAYLOAD_FIELDS {
        if mask & bit != 0 && h.has(bit) {
            let bytes = blob.read(payload_ref(h, bit))?;
            *decompressed += bytes.len() as u64;
            set_payload(&mut span, bit, bytes)?;
        }
    }
    Ok(span)
}

/// Sealed layout files of a partition, mapped: `(layout name, [(file name, bytes)])`.
type FileSets = Vec<(&'static str, Vec<(String, Bytes)>)>;

fn with_files<R>(
    sets: &FileSets,
    name: &str,
    f: impl FnOnce(FileSet<'_>) -> Result<R>,
) -> Result<R> {
    let (_, set) = sets
        .iter()
        .find(|(n, _)| *n == name)
        .ok_or_else(|| corrupt(format!("layout {name} is not mapped")))?;
    let v: Vec<(&str, &[u8])> = set.iter().map(|(n, b)| (n.as_str(), &b[..])).collect();
    f(FileSet { files: &v })
}

pub(crate) enum FixedLayouts {
    Files {
        sets: FileSets,
        filter: trace_filter::Reader,
    },
    Memory(Box<Layouts>),
}

/// A partition that no longer changes.
pub(crate) struct Fixed {
    pub strings: Strings,
    pub blob: Blob,
    pub stats: Stats,
    pub layouts: FixedLayouts,
    /// `(layout, version)` directories on disk, from MANIFEST.
    pub versions: Vec<(String, u32)>,
    /// Traces shared with earlier partitions (`overlap.bin`); `None` when not on disk.
    pub overlap: Option<Vec<TraceId>>,
}

/// The active partition, appended to by the writer.
pub(crate) struct Live {
    pub strings: Strings,
    pub blob: Blob,
    pub stats: Stats,
    pub layouts: Layouts,
    pub keys: HashSet<Key>,
}

// One per partition, so the size difference between the variants does not matter.
#[allow(clippy::large_enum_variant)]
pub(crate) enum Body {
    Fixed(Fixed),
    Live(RwLock<Live>),
}

pub(crate) struct Part {
    pub name: String,
    pub id: u64,
    pub hour: u64,
    pub dir: PathBuf,
    /// Listed in MANIFEST.
    pub sealed: bool,
    pub body: Body,
}

/// A partition's mapped layout files as `FileSet` tables, built once per query so the
/// query's readers can borrow them. Empty for partitions answered from memory.
pub(crate) type Tables<'a> = Vec<(&'static str, Vec<(&'a str, &'a [u8])>)>;

impl Part {
    pub fn tables(&self) -> Tables<'_> {
        match &self.body {
            Body::Fixed(Fixed {
                layouts: FixedLayouts::Files { sets, .. },
                ..
            }) => sets
                .iter()
                .map(|(l, set)| (*l, set.iter().map(|(n, b)| (n.as_str(), &b[..])).collect()))
                .collect(),
            _ => Vec::new(),
        }
    }

    pub fn view<'a>(&'a self, tables: &'a Tables<'a>) -> View<'a> {
        let src = match &self.body {
            Body::Fixed(f) => Src::Fixed(f, tables),
            Body::Live(l) => Src::Live(l.read().unwrap_or_else(|e| e.into_inner())),
        };
        View {
            id: self.id,
            sealed: self.sealed,
            src,
            tc: OnceCell::new(),
            runs: OnceCell::new(),
            fts: OnceCell::new(),
        }
    }

    pub fn live(&self) -> Option<&RwLock<Live>> {
        match &self.body {
            Body::Live(l) => Some(l),
            Body::Fixed(_) => None,
        }
    }
}

enum Src<'a> {
    Fixed(&'a Fixed, &'a Tables<'a>),
    Live(RwLockReadGuard<'a, Live>),
}

/// A read-only view of one partition for the duration of one query. Readers of sealed
/// layout files are opened (and validated) at most once per query.
pub(crate) struct View<'a> {
    /// The partition's ID and whether it is listed in MANIFEST (and so immutable).
    pub id: u64,
    pub sealed: bool,
    src: Src<'a>,
    tc: OnceCell<trace_clustered::Reader<'a>>,
    runs: OnceCell<runs_by_time::Reader<'a>>,
    fts: OnceCell<fts::Reader<'a>>,
}

impl<'a> View<'a> {
    pub fn strings(&self) -> &Strings {
        match &self.src {
            Src::Fixed(f, _) => &f.strings,
            Src::Live(l) => &l.strings,
        }
    }

    pub fn blob(&self) -> BlobRef<'_> {
        match &self.src {
            Src::Fixed(f, _) => f.blob.as_ref(),
            Src::Live(l) => l.blob.as_ref(),
        }
    }

    pub fn stats(&self) -> &Stats {
        match &self.src {
            Src::Fixed(f, _) => &f.stats,
            Src::Live(l) => &l.stats,
        }
    }

    /// The builders answering for this partition, or `None` when it has sealed files.
    fn memory(&self) -> Option<&Layouts> {
        match &self.src {
            Src::Live(l) => Some(&l.layouts),
            Src::Fixed(f, _) => match &f.layouts {
                FixedLayouts::Memory(m) => Some(m),
                FixedLayouts::Files { .. } => None,
            },
        }
    }

    fn reader<'s, R>(
        &'s self,
        cell: &'s OnceCell<R>,
        name: &str,
        open: impl FnOnce(FileSet<'a>) -> Result<R>,
    ) -> Result<&'s R> {
        if let Some(r) = cell.get() {
            return Ok(r);
        }
        let Src::Fixed(_, tables) = &self.src else {
            return Err(corrupt("no layout files for an in-memory partition"));
        };
        let (_, files) = tables
            .iter()
            .find(|(n, _)| *n == name)
            .ok_or_else(|| corrupt(format!("layout {name} is not mapped")))?;
        let _ = cell.set(open(FileSet { files })?);
        cell.get().ok_or_else(|| corrupt("layout reader"))
    }

    pub fn may_contain(&self, t: TraceId) -> bool {
        match (&self.src, self.memory()) {
            (_, Some(m)) => m.filter.may_contain(t),
            (
                Src::Fixed(
                    Fixed {
                        layouts: FixedLayouts::Files { filter, .. },
                        ..
                    },
                    _,
                ),
                None,
            ) => filter.may_contain(t),
            _ => true,
        }
    }

    /// Merged headers of `t` in this partition, all projects.
    pub fn trace(&self, t: TraceId) -> Result<(Vec<SpanHeader>, Cost)> {
        if let Some(m) = self.memory() {
            return Ok(m.tc.trace(t));
        }
        let r = self.reader(
            &self.tc,
            trace_clustered::NAME,
            trace_clustered::Reader::open,
        )?;
        let (h, c) = r.trace(t);
        Ok((h.to_vec(), c))
    }

    /// Spans of this partition mentioning every token, newest first by their start here.
    pub fn intersect_newest(
        &self,
        tokens: &[&[u8]],
    ) -> Result<(Box<dyn Iterator<Item = fts::Hit> + '_>, Cost)> {
        if let Some(m) = self.memory() {
            let (it, c) = m.fts.intersect_newest(tokens);
            return Ok((Box::new(it), c));
        }
        let (it, c) = self
            .reader(&self.fts, fts::NAME, fts::Reader::open)?
            .intersect_newest(tokens);
        Ok((Box::new(it), c))
    }

    /// Calls `f` with run candidates newest first, starting after those with a key at or
    /// above `before`, until `f` returns false. Returns the cost.
    pub fn runs(
        &self,
        before: Option<RunKey>,
        f: &mut dyn FnMut(&RunSummary) -> Result<bool>,
    ) -> Result<Cost> {
        let at_or_above = |s: &RunSummary| before.is_some_and(|b| summary_key(s) >= b);
        let mut n = 0;
        if let Some(m) = self.memory() {
            for s in m.runs.runs_newest().skip_while(|s| at_or_above(s)) {
                n += 1;
                if !f(s)? {
                    break;
                }
            }
            return Ok(m.runs.cost_of(n));
        }
        let r = self.reader(&self.runs, runs_by_time::NAME, runs_by_time::Reader::open)?;
        let all = r.runs();
        let from = all.partition_point(at_or_above);
        for s in &all[from..] {
            n += 1;
            if !f(s)? {
                break;
            }
        }
        // The binary search touches about log2(n) summaries.
        let mut c = r.cost_of(n);
        c.records_visited += (usize::BITS - all.len().leading_zeros()) as u64;
        Ok(c)
    }
}

pub(crate) fn summary_key(s: &RunSummary) -> RunKey {
    RunKey {
        start_ns: s.start_ns,
        trace_id: s.trace_id,
        span_id: s.span_id,
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum Mode {
    /// The active partition after a crash: stop at the first bad record.
    Recover,
    /// A sealed partition: any damage is an error.
    Strict,
    /// Strict, and also check every payload's CRC and hash.
    Verify,
}

/// What replaying a partition's log found.
pub(crate) struct Replay {
    pub id: u64,
    pub hour: u64,
    pub strings: Strings,
    pub stats: Stats,
    pub keys: HashSet<Key>,
    pub valid_log: u64,
    pub valid_payloads: u64,
    /// Every intact payload entry, by hash, for the writer's deduplication.
    pub dedup: HashMap<payload::Hash, Vec<PayloadRef>>,
}

const REPLAY_CHUNK: usize = 1024;
/// Decompressed sealed frames a replay keeps; posts arrive in log order, not trace order.
const REPLAY_FRAME_CACHE: usize = 64 << 20;

/// Reads a partition's log and payloads and hands every post to `sink`, in log order.
pub(crate) fn replay(dir: &Path, mode: Mode, sink: &mut dyn FnMut(&Batch<'_>)) -> Result<Replay> {
    let strict = mode != Mode::Recover;
    let log_bytes = files::map(&dir.join(LOG))?;
    // A sealed partition reads its payloads from `payloads.sealed` once re-framing wrote
    // it (its `payloads.blob` may be gone). The active partition never has one.
    let (sealed_bytes, pay_bytes) = match strict {
        true => payload_files(dir)?,
        false => (None, files::map(&dir.join(PAYLOADS))?),
    };
    let mut dedup: HashMap<_, Vec<_>> = HashMap::new();
    let (blob, source) = match &sealed_bytes {
        Some(b) => {
            let s = sealed::Sealed::open(b)?;
            if mode == Mode::Verify {
                s.verify()?;
            }
            (&[][..], BlobRef::Sealed(SealedRef::Checked(s)))
        }
        None => {
            let mut scan = payload::Scan::new(&pay_bytes)?;
            for e in scan.by_ref() {
                dedup.entry(e.hash).or_default().push(e.r);
            }
            if strict && scan.torn() {
                return Err(corrupt(format!("{}: payloads.blob is torn", dir.display())));
            }
            let blob = &pay_bytes[..scan.valid_len()];
            (blob, BlobRef::Slice(blob))
        }
    };
    let mut pay = Payloads::new(vec![source], REPLAY_FRAME_CACHE);
    let mut reader = log::Reader::new(&log_bytes)?;
    let mut rep = Replay {
        id: reader.id,
        hour: reader.hour,
        strings: Strings::default(),
        stats: Stats::default(),
        keys: HashSet::new(),
        valid_log: log::HEADER_LEN as u64,
        valid_payloads: blob.len() as u64,
        dedup,
    };
    let mut checked = HashSet::new();
    let (mut headers, mut spans) = (Vec::new(), Vec::new());
    let mut first_post = 0u32;
    let mut flush = |headers: &mut Vec<SpanHeader>, spans: &mut Vec<Span>, first: &mut u32| {
        if !headers.is_empty() {
            sink(&Batch {
                first_post: *first,
                headers,
                spans,
            });
            *first += headers.len() as u32;
            headers.clear();
            spans.clear();
        }
    };
    for rec in reader.by_ref() {
        let step = (|| -> Result<Option<(SpanHeader, Span)>> {
            match rec.kind {
                log::kind::STRING => {
                    let (id, s) = rec.string()?;
                    if id as usize != rep.strings.len() + 1 {
                        return Err(corrupt(format!("log: string ID {id} out of sequence")));
                    }
                    rep.strings.push(s.to_string());
                    Ok(None)
                }
                log::kind::SPAN_POST => {
                    let h = rec.post()?;
                    if rec.seq != h.version || rep.stats.version.is_some_and(|v| h.version <= v.1) {
                        return Err(corrupt(format!("log: version {} out of order", h.version)));
                    }
                    if mode == Mode::Verify && sealed_bytes.is_none() {
                        for bit in PAYLOAD_FIELDS {
                            let r = payload_ref(&h, bit);
                            if h.has(bit) && checked.insert(r.offset) {
                                payload::check(blob, r)?;
                            }
                        }
                    }
                    let mut span = to_span(&h, &rep.strings, source, 0, &mut 0)?;
                    for bit in PAYLOAD_FIELDS {
                        if h.has(bit) {
                            set_payload(&mut span, bit, pay.read(0, payload_ref(&h, bit))?)?;
                        }
                    }
                    Ok(Some((h, span)))
                }
                k => Err(corrupt(format!("log: unknown record kind {k}"))),
            }
        })();
        match step {
            Ok(None) => {}
            Ok(Some((h, span))) => {
                rep.stats.add(&h);
                rep.keys.insert((h.project, h.trace_id, h.span_id));
                headers.push(h);
                spans.push(span);
                if headers.len() == REPLAY_CHUNK {
                    flush(&mut headers, &mut spans, &mut first_post);
                }
            }
            Err(e) if strict => return Err(e),
            Err(_) => break,
        }
        rep.valid_log = rec.end as u64;
    }
    if strict && (reader.torn() || rep.valid_log != log_bytes.len() as u64) {
        return Err(corrupt(format!("{}: log.seg is torn", dir.display())));
    }
    flush(&mut headers, &mut spans, &mut first_post);
    rep.stats.spans = rep.keys.len() as u64;
    Ok(rep)
}

/// Frames of `payloads.sealed` collect about this many raw bytes.
pub(crate) const FRAME_TARGET: usize = 64 << 10;

/// Plans `payloads.sealed` from a partition's merged headers in `trace_clustered` order
/// (`spans`) and every payload its log refers to (`all`, sorted by offset, unique).
/// Deterministic. Frames hold, in order:
/// - each trace's own payloads, trace by trace;
/// - payloads several traces share (deduplicated, such as a common prompt), one per
///   frame, so a small trace never decompresses another trace's frame to read one;
/// - the other payloads of the log (ones a later post replaced, which replaying the log
///   still reads), in offset order.
pub(crate) fn plan_sealed(spans: &[SpanHeader], all: &[PayloadRef], target: usize) -> sealed::Plan {
    let refs_of = |trace: &[SpanHeader], out: &mut Vec<PayloadRef>| {
        out.clear();
        for h in trace {
            for bit in PAYLOAD_FIELDS {
                let r = payload_ref(h, bit);
                if h.has(bit) && !out.iter().any(|o| o.offset == r.offset) {
                    out.push(r);
                }
            }
        }
    };
    let traces = || spans.chunk_by(|a, b| a.trace_id == b.trace_id);
    // Traces using each payload of `all`, by position; 0 once a trace has claimed it.
    let pos = |r: &PayloadRef| all.binary_search_by_key(&r.offset, |a| a.offset).ok();
    let mut refs = Vec::new();
    let mut uses = vec![0u32; all.len()];
    for trace in traces() {
        refs_of(trace, &mut refs);
        for k in refs.iter().filter_map(pos) {
            uses[k] += 1;
        }
    }
    let used: Vec<bool> = uses.iter().map(|&n| n > 0).collect();
    let mut plan = sealed::Plan::new(target);
    let mut shared = Vec::new();
    let mut own = Vec::new();
    for trace in traces() {
        refs_of(trace, &mut refs);
        own.clear();
        for r in &refs {
            match pos(r).map(|k| &mut uses[k]) {
                // Not in the log: cannot happen, as every header comes from logged posts.
                None | Some(1) => own.push(*r),
                // The first trace using a shared payload claims it.
                Some(n) if *n > 1 => {
                    *n = 0;
                    shared.push(*r);
                }
                _ => {}
            }
        }
        plan.start_group(own.iter().map(|r| r.raw_len as usize).sum());
        own.iter().for_each(|r| plan.add(*r));
    }
    shared.into_iter().for_each(|r| plan.add_alone(r));
    for (r, used) in all.iter().zip(used) {
        if !used {
            plan.add(*r);
        }
    }
    plan.finish()
}

/// Every payload a partition's log refers to, sorted by offset, unique.
fn log_payloads(dir: &Path) -> Result<Vec<PayloadRef>> {
    let bytes = files::map(&dir.join(LOG))?;
    // Payloads are appended in log order, so a new one has the highest offset yet; a
    // lower offset is a deduplicated reference to an earlier one (kept aside, merged).
    let (mut out, mut late): (Vec<PayloadRef>, Vec<PayloadRef>) = (Vec::new(), Vec::new());
    for rec in log::Reader::new(&bytes)? {
        if rec.kind == log::kind::SPAN_POST {
            let h = rec.post()?;
            for bit in PAYLOAD_FIELDS {
                if h.has(bit) {
                    let r = payload_ref(&h, bit);
                    match out.last() {
                        Some(l) if l.offset >= r.offset => late.push(r),
                        _ => out.push(r),
                    }
                }
            }
        }
    }
    late.sort_unstable_by_key(|r| r.offset);
    late.dedup_by_key(|r| r.offset);
    late.retain(|r| out.binary_search_by_key(&r.offset, |o| o.offset).is_err());
    if !late.is_empty() {
        out.extend(late);
        out.sort_unstable_by_key(|r| r.offset);
    }
    Ok(out)
}

/// Writes `payloads.sealed` for a sealed partition to `out` and fsyncs it (not the
/// directory). Payloads are read from `payloads.blob` with `pread`, so the blob is not
/// mapped, and frames are built on the read pool a few at a time and written in order,
/// so memory stays bounded. Stops with an error when `stop` returns true.
pub(crate) fn write_sealed_payloads(dir: &Path, out: &Path, stop: &dyn Fn() -> bool) -> Result<()> {
    let set = map_layout(dir, trace_clustered::NAME, trace_clustered::VERSION)?;
    let files: Vec<(&str, &[u8])> = set.iter().map(|(n, b)| (n.as_str(), &b[..])).collect();
    let tc = trace_clustered::Reader::open(FileSet { files: &files })?;
    let plan = plan_sealed(tc.spans(), &log_payloads(dir)?, FRAME_TARGET);
    drop(set);
    let blob = File::open(dir.join(PAYLOADS))?;
    let blob_len = blob.metadata()?.len();
    let build = |i: usize| {
        let mut entry = Vec::new();
        sealed::build_frame(&plan, i, &mut |r, out| {
            let range = payload::entry_range(r, blob_len)?;
            entry.resize(range.len(), 0);
            blob.read_exact_at(&mut entry, range.start as u64)?;
            payload::decode_append(&entry, r, out)
        })
    };
    let chunk = 4 * (crate::pool::workers() + 1);
    let n = plan.frame_count();
    let frames = (0..n.div_ceil(chunk)).flat_map(|c| {
        let from = c * chunk;
        let built: Vec<Result<sealed::FrameData>> = match stop() {
            true => vec![Err(Error::Io(std::io::Error::other("re-framing stopped")))],
            false => crate::pool::each((n - from).min(chunk), &|k| build(from + k))
                .into_iter()
                .map(|f| f.unwrap_or_else(|| Err(corrupt("re-framing thread panicked"))))
                .collect(),
        };
        built
    });
    let mut w = std::io::BufWriter::with_capacity(1 << 20, File::create(out)?);
    sealed::write(&plan, frames, &mut w)?;
    w.into_inner().map_err(|e| e.into_error())?.sync_all()?;
    Ok(())
}

/// Maps `path`, or `None` when it does not exist.
fn map_existing(path: &Path) -> Result<Option<Bytes>> {
    match files::map(path) {
        Ok(b) => Ok(Some(b)),
        Err(Error::Io(e)) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e),
    }
}

/// A sealed partition's payload files: `(payloads.sealed, payloads.blob)`, the blob empty
/// when the sealed file exists. Re-framing renames payloads.sealed into place before it
/// deletes the blob, so a blob that vanished means payloads.sealed is there now.
fn payload_files(dir: &Path) -> Result<(Option<Bytes>, Bytes)> {
    for _ in 0..2 {
        if let Some(b) = map_existing(&dir.join(SEALED_PAYLOADS))? {
            return Ok((Some(b), Bytes::Owned(Vec::new())));
        }
        if let Some(b) = map_existing(&dir.join(PAYLOADS))? {
            return Ok((None, b));
        }
    }
    Err(corrupt(format!("{}: no payload file", dir.display())))
}

/// A sealed partition's payloads: `payloads.sealed` once re-framing has written it, else
/// `payloads.blob`.
fn open_payloads(dir: &Path) -> Result<Blob> {
    match payload_files(dir)? {
        (Some(b), _) => {
            let s = SealedBlob::open(dir.join(SEALED_PAYLOADS), b)?;
            Ok(Blob::Sealed(s))
        }
        (None, blob) => {
            payload::check_header(&blob)?;
            Ok(Blob::Map(blob))
        }
    }
}

/// Uncompressed size of every stored payload of a partition. Only a partition listed in
/// MANIFEST (`sealed`) can have `payloads.sealed`.
pub(crate) fn payload_sizes(dir: &Path, sealed: bool) -> Result<Vec<u64>> {
    let (file, blob) = match sealed {
        true => payload_files(dir)?,
        false => (None, files::map(&dir.join(PAYLOADS))?),
    };
    if let Some(b) = file {
        let s = sealed::Sealed::open(&b)?;
        return Ok((0..s.entry_count())
            .map(|i| s.entry(i).1.len as u64)
            .collect());
    }
    Ok(payload::Scan::new(&blob)?
        .map(|e| e.r.raw_len as u64)
        .collect())
}

/// A layout version's files in a partition, mapped and sorted by name.
fn map_layout(dir: &Path, name: &str, version: u32) -> Result<Vec<(String, Bytes)>> {
    let ld = dir.join(LAYOUTS_DIR).join(layout_dir_name(name, version));
    let mut set = Vec::new();
    for f in fs::read_dir(&ld)? {
        let f = f?;
        if f.file_type()?.is_file() {
            let name = f.file_name().to_string_lossy().into_owned();
            set.push((name, files::map(&f.path())?));
        }
    }
    set.sort_by(|a, b| a.0.cmp(&b.0));
    Ok(set)
}

/// Opens a sealed partition listed in MANIFEST.
pub(crate) fn open_sealed(dir: &Path, e: &PartitionEntry) -> Result<Fixed> {
    open_fixed(dir, &e.layouts, Stats::from_entry(e))
}

/// Opens a sealed partition again, as after re-framing: the same layout versions and
/// counters, and the payload files on disk now.
pub(crate) fn reopen_sealed(dir: &Path, f: &Fixed) -> Result<Fixed> {
    open_fixed(dir, &f.versions, f.stats.clone())
}

fn open_fixed(dir: &Path, versions: &[(String, u32)], stats: Stats) -> Result<Fixed> {
    let strings = Strings::load(&files::map(&dir.join(STRINGS))?)?;
    let blob = open_payloads(dir)?;
    let mut sets: FileSets = Vec::new();
    for def in LAYOUTS {
        if !versions
            .iter()
            .any(|(n, v)| n == def.name && *v == def.version)
        {
            break;
        }
        sets.push((def.name, map_layout(dir, def.name, def.version)?));
    }
    let layouts = if sets.len() == LAYOUTS.len() {
        let filter = with_files(&sets, trace_filter::NAME, trace_filter::Reader::open)?;
        FixedLayouts::Files { sets, filter }
    } else {
        // A layout's current version is not on disk (a layout changed since this
        // partition sealed): answer from builders replayed from the log until `rebuild`.
        let mut l = Layouts::new();
        replay(dir, Mode::Strict, &mut |b| l.apply(b))?;
        FixedLayouts::Memory(Box::new(l))
    };
    Ok(Fixed {
        strings,
        blob,
        stats,
        layouts,
        versions: versions.to_vec(),
        overlap: match fs::read(dir.join(OVERLAP)) {
            Ok(b) => Some(overlap::decode(&b)?),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => None,
            Err(err) => return Err(err.into()),
        },
    })
}

/// Opens an unsealed partition without changing it (read-only databases).
pub(crate) fn open_unsealed(dir: &Path) -> Result<(Fixed, u64, u64)> {
    let mut l = Layouts::new();
    let rep = replay(dir, Mode::Recover, &mut |b| l.apply(b))?;
    let fixed = Fixed {
        strings: rep.strings,
        blob: Blob::Map(files::map(&dir.join(PAYLOADS))?),
        stats: rep.stats,
        layouts: FixedLayouts::Memory(Box::new(l)),
        versions: Vec::new(),
        overlap: None,
    };
    Ok((fixed, rep.id, rep.hour))
}

/// Reads just the log header: `(id, hour)`.
pub(crate) fn log_id(dir: &Path) -> Result<(u64, u64)> {
    let bytes = files::map(&dir.join(LOG))?;
    let r = log::Reader::new(&bytes)?;
    Ok((r.id, r.hour))
}

/// Which of `traces` have posts in any of `parts`. The filters rule out most partitions;
/// a filter hit is confirmed by a lookup. `parts` must not include the writer's active
/// partition while the writer calls this.
pub(crate) fn present(parts: &[Arc<Part>], traces: &[TraceId]) -> Result<Vec<TraceId>> {
    let tables: Vec<_> = parts.iter().map(|p| p.tables()).collect();
    let views: Vec<_> = parts.iter().zip(&tables).map(|(p, t)| p.view(t)).collect();
    let mut out = Vec::new();
    for &t in traces {
        for v in &views {
            if v.may_contain(t) && !v.trace(t)?.0.is_empty() {
                out.push(t);
                break;
            }
        }
    }
    Ok(out)
}

/// Distinct trace IDs of a partition's log, sorted.
pub(crate) fn log_traces(dir: &Path) -> Result<Vec<TraceId>> {
    let bytes = files::map(&dir.join(LOG))?;
    let mut out: Vec<TraceId> = log::Reader::new(&bytes)?
        .filter(|r| r.kind == log::kind::SPAN_POST)
        .map(|r| r.post().map(|h| h.trace_id))
        .collect::<Result<_>>()?;
    out.sort_unstable();
    out.dedup();
    Ok(out)
}
