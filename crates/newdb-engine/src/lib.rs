//! The NewDB storage engine (DESIGN.md section 6).
//!
//! **Shared contract:** the public items of this file ([`Db`], [`Config`], [`Clock`], the
//! report types) are used by lanes 3 and 4. Lane 1 implements them and may add items, but
//! changes to these signatures go through the integrator. Everything behind them
//! (partitions, writer thread, recovery, file system trait for simulation tests) is lane 1's
//! to design.
//!
//! [`Db`] implements [`newdb_types::Store`]; its answers must equal
//! `newdb_testkit::Oracle`'s for the same posts (`newdb_testkit::diff::compare`).

mod admin;
mod files;
mod part;
mod pool;
mod query;
mod reframe;
#[cfg(test)]
mod tests;
mod writer;

use std::collections::{HashMap, HashSet, VecDeque};
use std::fs::{self, File, OpenOptions, TryLockError};
use std::path::{Path, PathBuf};
use std::sync::mpsc::{Sender, channel};
use std::sync::{Arc, Mutex, RwLock, RwLockWriteGuard};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use newdb_format::{log, manifest};
use newdb_layouts::LAYOUTS;
use newdb_types::{
    Error, Read, Result, Run, RunQuery, SearchHit, SearchQuery, Span, Store, TraceId, TraceQuery,
};

use files::{LAYOUTS_DIR, LOCK, TRASH, dir_bytes, file_len, layout_dir_name, remove_dir_if_exists};
use part::{Body, Fixed, Part, View, log_id, log_traces, open_sealed, open_unsealed, present};
use writer::{Ack, Msg, Writer};

/// Time source. Partitions are hours of `now_ns()` at commit time.
pub trait Clock: Send + Sync {
    fn now_ns(&self) -> u64;
}

/// The wall clock.
pub struct SystemClock;

impl Clock for SystemClock {
    fn now_ns(&self) -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos() as u64)
            .unwrap_or(0)
    }
}

/// A clock tests move by hand, to cross hour boundaries and trigger sealing.
#[derive(Default)]
pub struct ManualClock(pub std::sync::atomic::AtomicU64);

impl ManualClock {
    pub fn new(ns: u64) -> ManualClock {
        ManualClock(ns.into())
    }

    pub fn set(&self, ns: u64) {
        self.0.store(ns, std::sync::atomic::Ordering::SeqCst);
    }

    pub fn advance(&self, by: Duration) {
        self.0
            .fetch_add(by.as_nanos() as u64, std::sync::atomic::Ordering::SeqCst);
    }
}

impl Clock for ManualClock {
    fn now_ns(&self) -> u64 {
        self.0.load(std::sync::atomic::Ordering::SeqCst)
    }
}

#[derive(Clone)]
pub struct Config {
    /// `fdatasync` before acknowledging a batch. Tests and benches may turn it off.
    pub fsync: bool,
    /// Group commit: flush when the pending batch is this old...
    pub commit_interval: Duration,
    /// ...or this large.
    pub commit_bytes: usize,
    /// Delete sealed partitions older than this many days; 0 keeps everything.
    pub retention_days: u32,
    /// Open without the writer: no ingest, no sealing, no retention.
    pub read_only: bool,
    pub clock: Arc<dyn Clock>,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            fsync: true,
            commit_interval: Duration::from_millis(10),
            commit_bytes: 1 << 20,
            retention_days: 30,
            read_only: false,
            clock: Arc::new(SystemClock),
        }
    }
}

/// One partition as `inspect` and the tools see it.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct PartitionInfo {
    /// Directory name, e.g. `2026-11-13T01`.
    pub name: String,
    pub sealed: bool,
    pub posts: u64,
    pub spans: u64,
    pub log_bytes: u64,
    pub payload_bytes: u64,
    /// Oldest and newest span `start_ns` in the partition (0 when empty).
    pub min_start_ns: u64,
    pub max_start_ns: u64,
    /// `(layout name, version, bytes on disk)`.
    pub layouts: Vec<(String, u32, u64)>,
}

/// Result of [`Db::verify`]. Empty `problems` means every check passed.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct VerifyReport {
    pub partitions_checked: u64,
    pub layouts_checked: u64,
    pub problems: Vec<String>,
}

/// Result of [`Db::inspect`]: facts about the data, for choosing layouts.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct InspectReport {
    pub partitions: Vec<PartitionInfo>,
    /// Spans per trace: `(p50, p99, max)`.
    pub spans_per_trace: (u64, u64, u64),
    /// Payload sizes in bytes: `(p50, p99, max)`.
    pub payload_bytes: (u64, u64, u64),
    /// Share of spans whose posts landed in more than one partition, 0..=1.
    pub cross_partition_span_share: f64,
    /// Recent reads: `(operation, count, p50 ns, p99 ns)`.
    pub reads: Vec<(String, u64, u64, u64)>,
}

/// An open database directory.
pub struct Db {
    inner: Arc<Inner>,
    /// `None` when read-only.
    tx: Option<Sender<Msg>>,
    thread: Mutex<Option<JoinHandle<()>>>,
    /// The background re-framing thread; `None` when read-only.
    reframer: Mutex<Option<JoinHandle<()>>>,
}

/// State shared by the handle, the readers and the writer thread.
pub(crate) struct Inner {
    pub path: PathBuf,
    pub config: Config,
    /// Every partition, in creation order (which is also version order).
    parts: RwLock<Vec<Arc<Part>>>,
    /// Traces with posts in more than one partition (a superset after retention): reads
    /// merge their spans across partitions, every other span is complete in one.
    pub multi: RwLock<HashSet<TraceId>>,
    /// Acknowledged batches whose layouts the applier has not caught up with yet.
    pub visible: writer::Visible,
    /// Recent read durations per operation, for `inspect`.
    reads: Mutex<HashMap<&'static str, VecDeque<u64>>>,
    /// Sealed partitions whose payloads are still to move into payloads.sealed.
    pub reframe: reframe::Queue,
    /// Held for the database's lifetime so only one process writes.
    _lock: Option<File>,
}

const READ_LOG_LEN: usize = 1024;

impl Inner {
    pub fn parts(&self) -> Vec<Arc<Part>> {
        self.parts.read().unwrap_or_else(|e| e.into_inner()).clone()
    }

    pub fn parts_mut(&self) -> RwLockWriteGuard<'_, Vec<Arc<Part>>> {
        self.parts.write().unwrap_or_else(|e| e.into_inner())
    }

    /// Swaps in a new version of the partition with the same ID.
    pub fn replace(&self, p: Arc<Part>) {
        for slot in self.parts_mut().iter_mut() {
            if slot.id == p.id {
                *slot = p.clone();
            }
        }
    }

    /// Taken after the query's partition views, so it covers every post they show.
    pub fn multi(&self) -> std::sync::RwLockReadGuard<'_, HashSet<TraceId>> {
        self.multi.read().unwrap_or_else(|e| e.into_inner())
    }

    pub fn add_multi(&self, traces: &[TraceId]) {
        if !traces.is_empty() {
            let mut m = self.multi.write().unwrap_or_else(|e| e.into_inner());
            m.extend(traces.iter().copied());
        }
    }

    fn record(&self, op: &'static str, d: Duration) {
        let mut r = self.reads.lock().unwrap_or_else(|e| e.into_inner());
        let q = r.entry(op).or_default();
        if q.len() == READ_LOG_LEN {
            q.pop_front();
        }
        q.push_back(d.as_nanos() as u64);
    }
}

impl Db {
    /// Opens or creates the database at `path` and recovers the active partition.
    pub fn open(path: impl AsRef<Path>, config: Config) -> Result<Db> {
        let path = path.as_ref().to_path_buf();
        let ro = config.read_only;
        let lock = if ro {
            None
        } else {
            fs::create_dir_all(&path)?;
            let f = OpenOptions::new()
                .create(true)
                .truncate(false)
                .write(true)
                .open(path.join(LOCK))?;
            match f.try_lock() {
                Ok(()) => Some(f),
                Err(TryLockError::WouldBlock) => {
                    return Err(Error::InvalidInput(format!(
                        "{} is open for writing by another process",
                        path.display()
                    )));
                }
                Err(TryLockError::Error(e)) => return Err(e.into()),
            }
        };

        let mut manifest = manifest::read(&path)?.unwrap_or_default();
        // A listed partition whose directory is in trash/ was being deleted by retention.
        let trash = path.join(TRASH);
        let listed = manifest.partitions.len();
        let mut missing = Vec::new();
        manifest.partitions.retain(|e| {
            let here = path.join(&e.name).is_dir();
            if !here && !trash.join(&e.name).exists() {
                missing.push(e.name.clone());
            }
            here
        });
        if !missing.is_empty() {
            return Err(Error::Corrupt(format!(
                "partitions listed in MANIFEST are missing: {missing:?}"
            )));
        }
        if !ro {
            if manifest.partitions.len() != listed {
                manifest::write(&path, &manifest)?;
            }
            remove_dir_if_exists(&trash)?;
            // Re-framing that crashed: before renaming payloads.sealed into place (its
            // .tmp), or after but before deleting the payloads.blob it replaces.
            for e in &manifest.partitions {
                let dir = path.join(&e.name);
                let tmp = dir.join(files::SEALED_PAYLOADS_TMP);
                let blob = dir.join(files::PAYLOADS);
                let done = dir.join(files::SEALED_PAYLOADS).exists();
                if tmp.exists() || (done && blob.exists()) {
                    files::remove_file_if_exists(&tmp)?;
                    if done {
                        files::remove_file_if_exists(&blob)?;
                    }
                    files::sync_dir(&dir)?;
                }
            }
        }

        let mut parts = Vec::new();
        for e in &manifest.partitions {
            let dir = path.join(&e.name);
            parts.push(Arc::new(Part {
                name: e.name.clone(),
                id: e.id,
                hour: e.hour,
                body: Body::Fixed(open_sealed(&dir, e)?),
                dir,
                sealed: true,
            }));
        }

        let names: HashSet<&str> = manifest
            .partitions
            .iter()
            .map(|e| e.name.as_str())
            .collect();
        let mut unsealed = Vec::new();
        for d in fs::read_dir(&path)? {
            let d = d?;
            let name = d.file_name().to_string_lossy().into_owned();
            let dir = d.path();
            if !d.file_type()?.is_dir()
                || name.starts_with('.')
                || name == TRASH
                || names.contains(name.as_str())
                || !dir.join(files::LOG).exists()
            {
                continue;
            }
            match log_id(&dir) {
                Ok((id, _)) => unsealed.push((id, dir)),
                // The log header is synced before any post is written, so a partition
                // without a valid header never held an acknowledged post.
                Err(_) if !ro => remove_dir_if_exists(&dir)?,
                Err(_) => {}
            }
        }
        unsealed.sort();

        let inner = Arc::new(Inner {
            path,
            config,
            parts: RwLock::new(parts),
            multi: RwLock::default(),
            visible: Default::default(),
            reads: Mutex::default(),
            reframe: Default::default(),
            _lock: lock,
        });
        let sealed = inner.parts();
        for (i, p) in sealed.iter().enumerate() {
            match &p.body {
                Body::Fixed(Fixed {
                    overlap: Some(o), ..
                }) => inner.add_multi(o),
                // Sealed before overlap.bin existed: compare with the earlier partitions.
                _ => inner.add_multi(&present(&sealed[..i], &log_traces(&p.dir)?)?),
            }
        }
        if ro {
            for (_, dir) in unsealed {
                inner.add_multi(&present(&inner.parts(), &log_traces(&dir)?)?);
                let (fixed, id, hour) = open_unsealed(&dir)?;
                let name = dir.file_name().map(|n| n.to_string_lossy().into_owned());
                inner.parts_mut().push(Arc::new(Part {
                    name: name.unwrap_or_default(),
                    id,
                    hour,
                    dir,
                    sealed: false,
                    body: Body::Fixed(fixed),
                }));
            }
            return Ok(Db {
                inner,
                tx: None,
                thread: Mutex::new(None),
                reframer: Mutex::new(None),
            });
        }
        let mut w = Writer::open(
            inner.clone(),
            manifest,
            unsealed.into_iter().map(|(_, d)| d).collect(),
        )?;
        w.roll()?;
        let (tx, rx) = channel();
        let thread = std::thread::Builder::new()
            .name("newdb-writer".into())
            .spawn(move || w.run(rx))?;
        for p in inner.parts() {
            if p.sealed && !p.dir.join(files::SEALED_PAYLOADS).exists() {
                inner.reframe.push(p.id);
            }
        }
        let bg = inner.clone();
        let reframer = std::thread::Builder::new()
            .name("newdb-reframe".into())
            .spawn(move || reframe::run(bg))?;
        Ok(Db {
            inner,
            tx: Some(tx),
            thread: Mutex::new(Some(thread)),
            reframer: Mutex::new(Some(reframer)),
        })
    }

    /// Sends one message to the writer and waits for its answer.
    fn call(&self, make: impl FnOnce(Ack) -> Msg) -> Result<()> {
        let tx = self.tx.as_ref().ok_or(Error::ReadOnly)?;
        let (ack, rx) = channel();
        tx.send(make(ack)).map_err(|_| Error::ReadOnly)?;
        rx.recv().map_err(|_| Error::ReadOnly)?
    }

    /// Runs a query over a view of every partition.
    fn read<T>(&self, op: &'static str, f: impl FnOnce(&[View<'_>]) -> Result<T>) -> Result<T> {
        let t0 = Instant::now();
        self.inner.visible.wait();
        let parts = self.inner.parts();
        let tables: Vec<_> = parts.iter().map(|p| p.tables()).collect();
        let views: Vec<View<'_>> = parts.iter().zip(&tables).map(|(p, t)| p.view(t)).collect();
        let r = f(&views);
        drop(views);
        self.inner.record(op, t0.elapsed());
        r
    }

    /// Seals the active partition now, as if its hour had ended. For tests and tools.
    /// Unlike the hourly seal, also waits for the partition's payloads to be re-framed
    /// (and any re-framing queued before), so reads afterwards see the steady state; the
    /// writer keeps taking ingests meanwhile.
    pub fn seal_active(&self) -> Result<()> {
        self.call(Msg::Seal)?;
        self.inner.reframe.wait_idle();
        Ok(())
    }

    /// Applies retention now instead of waiting for the next hourly check.
    pub fn apply_retention(&self) -> Result<()> {
        self.call(Msg::Retention)
    }

    pub fn partitions(&self) -> Result<Vec<PartitionInfo>> {
        self.inner.visible.wait();
        Ok(self.inner.parts().iter().map(|p| info(p)).collect())
    }

    /// Rebuilds every layout of every sealed partition from its log into a temporary
    /// directory and compares checksums with the stored files; checks log CRCs, payload
    /// refs and MANIFEST against the disk.
    pub fn verify(&self) -> Result<VerifyReport> {
        admin::verify(&self.inner)
    }

    pub fn inspect(&self) -> Result<InspectReport> {
        admin::inspect(&self.inner, self.partitions()?)
    }

    /// Rebuilds `layout` (or every layout when `None`) for each sealed partition from its
    /// log into the layout's current version directory, then switches MANIFEST over.
    pub fn rebuild(&self, layout: Option<&str>) -> Result<()> {
        let layout = layout.map(str::to_string);
        self.call(|ack| Msg::Rebuild(layout, ack))
    }

    /// Flushes the pending batch and stops the writer. Dropping a `Db` does the same.
    pub fn close(self) -> Result<()> {
        self.stop()
    }

    fn stop(&self) -> Result<()> {
        let thread = self.thread.lock().unwrap_or_else(|e| e.into_inner()).take();
        if let Some(t) = thread {
            let r = self.call(Msg::Stop);
            t.join()
                .map_err(|_| Error::Io(std::io::Error::other("writer thread panicked")))?;
            r?;
        }
        // Re-framing still running is abandoned; the next open queues it again.
        let reframer = self
            .reframer
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .take();
        if let Some(t) = reframer {
            self.inner.reframe.stop();
            t.join()
                .map_err(|_| Error::Io(std::io::Error::other("re-framing thread panicked")))?;
        }
        Ok(())
    }
}

impl Drop for Db {
    fn drop(&mut self) {
        let _ = self.stop();
    }
}

fn info(p: &Part) -> PartitionInfo {
    let tables = Vec::new();
    let v = p.view(&tables);
    let st = v.stats();
    let layouts = match &p.body {
        Body::Fixed(f) => f
            .versions
            .iter()
            .map(|(n, ver)| {
                let d = p.dir.join(LAYOUTS_DIR).join(layout_dir_name(n, *ver));
                (n.clone(), *ver, dir_bytes(&d))
            })
            .collect(),
        Body::Live(_) => LAYOUTS
            .iter()
            .map(|d| (d.name.to_string(), d.version, 0))
            .collect(),
    };
    PartitionInfo {
        name: p.name.clone(),
        sealed: p.sealed,
        posts: st.posts,
        spans: st.spans,
        log_bytes: file_len(&p.dir.join(files::LOG)),
        payload_bytes: match p.sealed {
            true => {
                file_len(&p.dir.join(files::PAYLOADS))
                    + file_len(&p.dir.join(files::SEALED_PAYLOADS))
            }
            false => file_len(&p.dir.join(files::PAYLOADS)),
        },
        min_start_ns: st.min_start(),
        max_start_ns: st.max_start(),
        layouts,
    }
}

/// Rejects posts the log cannot hold, before they reach the writer.
fn validate(spans: &[Span]) -> Result<()> {
    let max_str = log::MAX_RECORD_LEN - 64;
    for s in spans {
        let strs = [&s.project].into_iter().chain(
            [&s.name, &s.model, &s.provider, &s.session]
                .into_iter()
                .flatten(),
        );
        if strs.into_iter().any(|x| x.len() > max_str) {
            return Err(Error::InvalidInput(format!(
                "span {}: a string field is longer than {max_str} bytes",
                s.span_id
            )));
        }
        let pays = [&s.inputs, &s.outputs, &s.attributes, &s.events];
        if pays
            .into_iter()
            .flatten()
            .any(|p| p.len() > u32::MAX as usize)
            || s.error
                .as_ref()
                .is_some_and(|e| e.len() > u32::MAX as usize)
        {
            return Err(Error::InvalidInput(format!(
                "span {}: a payload is larger than 4 GiB",
                s.span_id
            )));
        }
    }
    Ok(())
}

impl Store for Db {
    fn ingest(&self, spans: Vec<Span>) -> Result<()> {
        if self.tx.is_none() {
            return Err(Error::ReadOnly);
        }
        if spans.is_empty() {
            return Ok(());
        }
        validate(&spans)?;
        let pre = writer::prepare(&spans)?;
        self.call(|ack| Msg::Ingest(spans, pre, ack))
    }

    fn trace(&self, query: &TraceQuery) -> Result<Read<Vec<Span>>> {
        self.read("trace", |v| query::trace(v, query))
    }

    fn runs(&self, query: &RunQuery) -> Result<Read<Vec<Run>>> {
        self.read("runs", |v| query::runs(v, &self.inner.multi(), query))
    }

    fn search(&self, query: &SearchQuery) -> Result<Read<Vec<SearchHit>>> {
        self.read("search", |v| query::search(v, &self.inner.multi(), query))
    }
}
