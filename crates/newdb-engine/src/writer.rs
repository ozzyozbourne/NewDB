//! The one writer thread: group commit, partition lifecycle (create, recover, seal),
//! retention and layout rebuilds. Everything that changes files runs here, in order.
//!
//! A batch moves through three stages, each on its own threads, so consecutive batches
//! overlap:
//!
//! 1. [`prepare`], on the ingesting thread: hash and compress the payloads.
//! 2. The writer: deduplicate, append payloads, `fdatasync`, append the log, `fdatasync`,
//!    acknowledge. The batch is durable now but not yet visible.
//! 3. The applier: apply the batch to the active partition's layouts, in log order, on
//!    persistent worker threads, then publish it.
//!
//! [`Visible::wait`] bridges 2 and 3: every read first waits until each batch
//! acknowledged before it started is applied, so `ingest` returning still means every
//! later read sees the batch.

use std::collections::{HashMap, HashSet};
use std::fs::{self, File, OpenOptions};
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{Receiver, RecvTimeoutError, Sender, SyncSender, channel, sync_channel};
use std::sync::{Arc, Condvar, Mutex, RwLock};
use std::thread::JoinHandle;
use std::time::Duration;

use newdb_format::manifest::{self, Manifest, PartitionEntry};
use newdb_format::{log, overlap, payload, strings as strtbl};
use newdb_layouts::{Batch, LAYOUTS, LayoutDef, LayoutFiles};
use newdb_types::header::field::*;
use newdb_types::{Error, PayloadRef, Result, Span, SpanHeader, TraceId};

use crate::Inner;
use crate::files::{
    self, HOUR_NS, LAYOUTS_DIR, LOG, OVERLAP, PAYLOADS, STRINGS, TRASH, layout_dir_name,
    remove_dir_if_exists, sync_dir, write_synced,
};
use crate::part::{
    Blob, Body, Group, Layouts, Live, Mode, PARALLEL_APPLY, PAYLOAD_FIELDS, Part, Stats, Strings,
    cores, corrupt, open_sealed, present, replay,
};

pub(crate) type Ack = Sender<Result<()>>;

pub(crate) enum Msg {
    /// Posts and their payloads as [`prepare`] stored them, in the same order.
    Ingest(Vec<Span>, Vec<Prepared>, Ack),
    Seal(Ack),
    Retention(Ack),
    Rebuild(Option<String>, Ack),
    Stop(Ack),
}

/// How often the idle writer checks for a new hour (sealing, retention).
const TICK: Duration = Duration::from_secs(1);

/// The active partition's files, owned by the writer.
struct Active {
    part: Arc<Part>,
    log: File,
    pay: File,
    log_len: u64,
    pay_len: u64,
    dedup: HashMap<payload::Hash, Vec<PayloadRef>>,
    posts: u32,
    /// The string table including batches not yet applied (the live one lags).
    strings: Strings,
    traces: HashSet<TraceId>,
    /// Traces of this partition that sealed partitions have too, for `overlap.bin`.
    overlap: Vec<TraceId>,
}

pub(crate) struct Writer {
    inner: Arc<Inner>,
    manifest: Manifest,
    active: Option<Active>,
    applier: Applier,
    /// Batches written since open; the last one's number.
    seq: u64,
    next_version: u64,
    next_id: u64,
    retention_hour: u64,
}

/// A copy of an error for each waiter of a failed batch.
pub(crate) fn dup(e: &Error) -> Error {
    match e {
        Error::Io(io) => Error::Io(std::io::Error::new(io.kind(), io.to_string())),
        Error::Corrupt(m) => Error::Corrupt(m.clone()),
        Error::InvalidInput(m) => Error::InvalidInput(m.clone()),
        Error::ReadOnly => Error::ReadOnly,
    }
}

impl Writer {
    /// Recovers the unsealed partitions found on open: every one but the newest is
    /// sealed now; the newest becomes the active partition.
    pub fn open(inner: Arc<Inner>, manifest: Manifest, unsealed: Vec<PathBuf>) -> Result<Writer> {
        let max_version = manifest
            .partitions
            .iter()
            .map(|p| p.max_version)
            .max()
            .unwrap_or(0);
        let max_id = manifest.partitions.iter().map(|p| p.id).max().unwrap_or(0);
        let mut w = Writer {
            applier: Applier::start(inner.clone())?,
            inner,
            manifest,
            active: None,
            seq: 0,
            next_version: max_version + 1,
            next_id: max_id + 1,
            retention_hour: 0,
        };
        let n = unsealed.len();
        for (i, dir) in unsealed.into_iter().enumerate() {
            w.recover(dir)?;
            if i + 1 < n {
                w.seal()?;
            }
        }
        Ok(w)
    }

    fn now_hour(&self) -> u64 {
        self.inner.config.clock.now_ns() / HOUR_NS
    }

    pub fn run(mut self, rx: Receiver<Msg>) {
        let mut stashed: Option<Msg> = None;
        loop {
            let msg = match stashed.take() {
                Some(m) => m,
                None => match rx.recv_timeout(TICK) {
                    Ok(m) => m,
                    Err(RecvTimeoutError::Timeout) => {
                        let _ = self.roll();
                        continue;
                    }
                    Err(RecvTimeoutError::Disconnected) => return,
                },
            };
            match msg {
                Msg::Ingest(spans, pre, ack) => {
                    // Group commit: take what queued up while the last batch was being
                    // written and synced, without waiting for more. An idle writer
                    // commits a lone request at once; `commit_interval` is only an
                    // upper bound on the wait.
                    let mut batch = vec![(spans, pre, ack)];
                    let mut bytes = size(&batch[0].0);
                    while bytes < self.inner.config.commit_bytes {
                        match rx.try_recv() {
                            Ok(Msg::Ingest(s, p, a)) => {
                                bytes += size(&s);
                                batch.push((s, p, a));
                            }
                            Ok(other) => {
                                stashed = Some(other);
                                break;
                            }
                            Err(_) => break,
                        }
                    }
                    if !self.commit(batch) {
                        return;
                    }
                }
                Msg::Seal(ack) => {
                    let _ = ack.send(self.seal());
                }
                Msg::Retention(ack) => {
                    let _ = ack.send(self.retention());
                }
                Msg::Rebuild(layout, ack) => {
                    let _ = ack.send(self.rebuild(layout.as_deref()));
                }
                Msg::Stop(ack) => {
                    let _ = ack.send(self.applier.drain());
                    return;
                }
            }
        }
    }

    /// Seals the active partition when its hour has passed, and applies retention once
    /// per hour.
    pub fn roll(&mut self) -> Result<()> {
        let hour = self.now_hour();
        if self.active.as_ref().is_some_and(|a| a.part.hour < hour) {
            self.seal()?;
        }
        if self.retention_hour != hour {
            self.retention()?;
            self.retention_hour = hour;
        }
        Ok(())
    }

    /// Writes and acknowledges a group of ingests and queues it for the applier. False when
    /// the applier is gone, which stops the writer.
    fn commit(&mut self, batch: Vec<(Vec<Span>, Vec<Prepared>, Ack)>) -> bool {
        let (mut spans, mut pre, mut acks) = (Vec::new(), Vec::new(), Vec::new());
        for (s, p, a) in batch {
            spans.extend(s);
            pre.extend(p);
            acks.push(a);
        }
        let mut res = self.roll().and_then(|_| self.write(spans, pre));
        if let Ok(Some(job)) = &mut res {
            self.seq += 1;
            job.seq = self.seq;
            self.inner.visible.acked.store(self.seq, Ordering::SeqCst);
        }
        for ack in acks {
            let _ = ack.send(res.as_ref().map(drop).map_err(dup));
        }
        match res {
            Ok(Some(job)) => self.applier.send(Apply::Batch(job)),
            _ => true,
        }
    }

    fn create_active(&mut self, hour: u64) -> Result<()> {
        let base = files::hour_name(hour);
        let mut name = base.clone();
        let mut n = 0;
        while self.inner.path.join(&name).exists() {
            n += 1;
            name = format!("{base}.{n}");
        }
        let dir = self.inner.path.join(&name);
        let id = self.next_id;
        fs::create_dir(&dir)?;
        write_synced(&dir.join(LOG), &log::file_header_bytes(id, hour))?;
        write_synced(&dir.join(PAYLOADS), &payload::file_header_bytes())?;
        sync_dir(&dir)?;
        sync_dir(&self.inner.path)?;
        self.next_id += 1;
        self.attach(dir, name, id, hour, Replayed::empty())
    }

    /// Opens an unsealed partition directory as the active partition, cutting the torn
    /// tails of its log and payload file.
    fn recover(&mut self, dir: PathBuf) -> Result<()> {
        let mut layouts = Layouts::new();
        let rep = replay(&dir, Mode::Recover, &mut |b| layouts.apply(b))?;
        for (f, len) in [(LOG, rep.valid_log), (PAYLOADS, rep.valid_payloads)] {
            let file = OpenOptions::new().write(true).open(dir.join(f))?;
            if file.metadata()?.len() != len {
                file.set_len(len)?;
                file.sync_all()?;
            }
        }
        self.next_version = self.next_version.max(rep.stats.max_version() + 1);
        self.next_id = self.next_id.max(rep.id + 1);
        let name = dir
            .file_name()
            .map(|n| n.to_string_lossy().into_owned())
            .unwrap_or_default();
        let (id, hour) = (rep.id, rep.hour);
        let r = Replayed {
            strings: rep.strings,
            stats: rep.stats,
            layouts,
            keys: rep.keys,
            dedup: rep.dedup,
            log_len: rep.valid_log,
            pay_len: rep.valid_payloads,
        };
        self.attach(dir, name, id, hour, r)
    }

    fn attach(
        &mut self,
        dir: PathBuf,
        name: String,
        id: u64,
        hour: u64,
        r: Replayed,
    ) -> Result<()> {
        let log = OpenOptions::new()
            .read(true)
            .write(true)
            .open(dir.join(LOG))?;
        let pay = OpenOptions::new()
            .read(true)
            .write(true)
            .open(dir.join(PAYLOADS))?;
        let mut traces: Vec<TraceId> = r.keys.iter().map(|k| k.1).collect();
        traces.sort_unstable();
        traces.dedup();
        let overlap = present(&self.inner.parts(), &traces)?;
        self.inner.add_multi(&overlap);
        let mut strings = Strings::default();
        for s in &r.strings.list {
            strings.push(s.clone());
        }
        let live = Live {
            strings: r.strings,
            blob: Blob::File {
                file: File::open(dir.join(PAYLOADS))?,
                len: r.pay_len,
            },
            stats: r.stats,
            layouts: r.layouts,
            keys: r.keys,
        };
        let posts = live.stats.posts as u32;
        let part = Arc::new(Part {
            name,
            id,
            hour,
            dir,
            sealed: false,
            body: Body::Live(RwLock::new(live)),
        });
        self.inner.parts_mut().push(part.clone());
        self.active = Some(Active {
            part,
            log,
            pay,
            log_len: r.log_len,
            pay_len: r.pay_len,
            dedup: r.dedup,
            posts,
            strings,
            traces: traces.into_iter().collect(),
            overlap,
        });
        Ok(())
    }

    /// Appends `spans` to the active partition (creating it if needed) and syncs. Returns
    /// the batch for the applier, which makes it visible, or `None` when there was nothing.
    fn write(&mut self, spans: Vec<Span>, pre: Vec<Prepared>) -> Result<Option<Job>> {
        if spans.is_empty() {
            return Ok(None);
        }
        if self.active.is_none() {
            let hour = self.now_hour();
            self.create_active(hour)?;
        }
        let Some(a) = self.active.as_mut() else {
            return Err(corrupt("no active partition"));
        };
        let mut enc = Encoding {
            log: Vec::new(),
            pay: Vec::new(),
            pay_base: a.pay_len,
            new_strings: Strings::default(),
            new_dedup: HashMap::new(),
        };
        let mut spans = spans;
        for (i, span) in spans.iter_mut().enumerate() {
            span.version = self.next_version + i as u64;
        }
        let headers = enc.encode(&spans, &pre, &a.strings, &a.dedup, &a.pay)?;
        let mut new_traces: Vec<TraceId> = headers
            .iter()
            .map(|h| h.trace_id)
            .filter(|t| !a.traces.contains(t))
            .collect();
        new_traces.sort_unstable();
        new_traces.dedup();
        let sync = self.inner.config.fsync;
        let written = (|| -> Result<()> {
            // Payloads first, so a durable log record never points at missing payloads.
            a.pay.write_all_at(&enc.pay, a.pay_len)?;
            if sync && !enc.pay.is_empty() {
                a.pay.sync_data()?;
            }
            a.log.write_all_at(&enc.log, a.log_len)?;
            if sync {
                a.log.sync_data()?;
            }
            Ok(())
        })();
        if let Err(e) = written {
            let _ = a.log.set_len(a.log_len);
            let _ = a.pay.set_len(a.pay_len);
            return Err(e);
        }
        // Traces that sealed partitions share are marked before the batch is visible, so
        // a read that sees the batch also sees the mark.
        let id = a.part.id;
        let sealed: Vec<_> = self
            .inner
            .parts()
            .into_iter()
            .filter(|p| p.id != id)
            .collect();
        let shared = present(&sealed, &new_traces)?;
        self.inner.add_multi(&shared);
        a.log_len += enc.log.len() as u64;
        a.pay_len += enc.pay.len() as u64;
        for (h, refs) in enc.new_dedup {
            a.dedup.entry(h).or_default().extend(refs);
        }
        a.traces.extend(new_traces);
        a.overlap.extend(shared);
        for s in &enc.new_strings.list {
            a.strings.push(s.clone());
        }
        let posts = Posts {
            first_post: a.posts,
            headers,
            spans,
        };
        a.posts += posts.headers.len() as u32;
        self.next_version += posts.headers.len() as u64;
        Ok(Some(Job {
            part: a.part.clone(),
            seq: 0,
            posts,
            strings: enc.new_strings.list,
            pay_len: a.pay_len,
        }))
    }

    /// Writes the active partition's layouts and string table, lists it in MANIFEST and
    /// reopens it from its files. An empty active partition is removed instead.
    pub fn seal(&mut self) -> Result<()> {
        let Some(a) = self.active.as_ref() else {
            return Ok(());
        };
        self.applier.drain()?;
        let Some(live) = a.part.live() else {
            return Err(corrupt("active partition is sealed"));
        };
        let dir = a.part.dir.clone();
        let entry = {
            let g = live.read().unwrap_or_else(|e| e.into_inner());
            if g.stats.posts == 0 {
                drop(g);
                let id = a.part.id;
                self.active = None;
                self.inner.parts_mut().retain(|p| p.id != id);
                return remove_dir_if_exists(&dir);
            }
            a.log.sync_all()?;
            a.pay.sync_all()?;
            for (def, files) in g.layouts.seal()? {
                write_layout(&dir, def, &files)?;
            }
            write_synced(&dir.join(STRINGS), &strtbl::encode(&g.strings.list))?;
            let mut shared = a.overlap.clone();
            shared.sort_unstable();
            write_synced(&dir.join(OVERLAP), &overlap::encode(&shared))?;
            sync_dir(&dir)?;
            entry(&a.part, &g.stats)
        };
        self.manifest.partitions.push(entry.clone());
        if let Err(e) = manifest::write(&self.inner.path, &self.manifest) {
            self.manifest.partitions.pop();
            return Err(e);
        }
        let a = self.active.take();
        let fixed = open_sealed(&dir, &entry)?;
        if let Some(a) = a {
            self.inner.replace(sealed_part(&a.part, fixed));
        }
        Ok(())
    }

    /// Deletes sealed partitions older than `retention_days`: rename into `trash/`
    /// (the commit point), update MANIFEST, then delete.
    pub fn retention(&mut self) -> Result<()> {
        let days = self.inner.config.retention_days as u64;
        if days == 0 {
            return Ok(());
        }
        let cutoff = self.now_hour().saturating_sub(days * 24);
        let old: HashSet<u64> = self
            .manifest
            .partitions
            .iter()
            .filter(|p| p.hour < cutoff)
            .map(|p| p.id)
            .collect();
        if old.is_empty() {
            return Ok(());
        }
        let trash = self.inner.path.join(TRASH);
        fs::create_dir_all(&trash)?;
        let names: Vec<String> = self
            .manifest
            .partitions
            .iter()
            .filter(|p| old.contains(&p.id))
            .map(|p| p.name.clone())
            .collect();
        for n in &names {
            remove_dir_if_exists(&trash.join(n))?;
            fs::rename(self.inner.path.join(n), trash.join(n))?;
        }
        sync_dir(&self.inner.path)?;
        self.manifest.partitions.retain(|p| !old.contains(&p.id));
        manifest::write(&self.inner.path, &self.manifest)?;
        self.inner.parts_mut().retain(|p| !old.contains(&p.id));
        remove_dir_if_exists(&trash)
    }

    /// Rebuilds layouts of every sealed partition from its log into their current version
    /// directories, then switches MANIFEST over and deletes older versions.
    pub fn rebuild(&mut self, layout: Option<&str>) -> Result<()> {
        let defs: Vec<&'static LayoutDef> = LAYOUTS
            .iter()
            .filter(|d| layout.is_none_or(|l| l == d.name))
            .collect();
        if defs.is_empty() {
            return Err(Error::InvalidInput(format!("unknown layout {layout:?}")));
        }
        let mut next = self.manifest.clone();
        for e in &mut next.partitions {
            let dir = self.inner.path.join(&e.name);
            let mut builders: Vec<_> = defs.iter().map(|d| (*d, (d.new_builder)())).collect();
            replay(&dir, Mode::Strict, &mut |b| {
                builders.iter_mut().for_each(|(_, x)| x.apply(b))
            })?;
            for (def, b) in &builders {
                let mut files = LayoutFiles::default();
                b.seal(&mut files)?;
                write_layout(&dir, def, &files)?;
                e.layouts.retain(|(n, _)| n != def.name);
                e.layouts.push((def.name.to_string(), def.version));
            }
            e.layouts
                .sort_by_key(|(n, _)| LAYOUTS.iter().position(|d| d.name == n));
        }
        manifest::write(&self.inner.path, &next)?;
        self.manifest = next;
        for e in &self.manifest.partitions {
            let dir = self.inner.path.join(&e.name);
            if let Ok(rd) = fs::read_dir(dir.join(LAYOUTS_DIR)) {
                for d in rd.flatten() {
                    let n = d.file_name().to_string_lossy().into_owned();
                    if !e.layouts.iter().any(|(l, v)| layout_dir_name(l, *v) == n) {
                        remove_dir_if_exists(&d.path())?;
                    }
                }
            }
            let fixed = open_sealed(&dir, e)?;
            let old = self.inner.parts().into_iter().find(|p| p.id == e.id);
            if let Some(old) = old {
                self.inner.replace(sealed_part(&old, fixed));
            }
        }
        Ok(())
    }
}

/// Which acknowledged batches readers can see yet.
#[derive(Default)]
pub(crate) struct Visible {
    /// The last acknowledged batch.
    acked: AtomicU64,
    shown: Mutex<Shown>,
    changed: Condvar,
}

#[derive(Default)]
struct Shown {
    /// The last batch the applier made visible.
    applied: u64,
    /// The applier has exited; nothing more becomes visible.
    stopped: bool,
}

impl Visible {
    /// Waits until every batch acknowledged before this call is visible.
    pub fn wait(&self) {
        let target = self.acked.load(Ordering::SeqCst);
        let mut s = self.shown.lock().unwrap_or_else(|e| e.into_inner());
        while s.applied < target && !s.stopped {
            s = self.changed.wait(s).unwrap_or_else(|e| e.into_inner());
        }
    }

    fn update(&self, f: impl FnOnce(&mut Shown)) {
        f(&mut self.shown.lock().unwrap_or_else(|e| e.into_inner()));
        self.changed.notify_all();
    }
}

/// A written batch on its way to the layouts.
struct Job {
    part: Arc<Part>,
    seq: u64,
    posts: Posts,
    /// Strings the batch added to the partition's table, in ID order.
    strings: Vec<String>,
    /// Committed length of the payload file after the batch.
    pay_len: u64,
}

/// The posts of a written batch, as the layouts receive them.
struct Posts {
    first_post: u32,
    headers: Vec<SpanHeader>,
    spans: Vec<Span>,
}

impl Posts {
    fn batch(&self) -> Batch<'_> {
        Batch {
            first_post: self.first_post,
            headers: &self.headers,
            spans: &self.spans,
        }
    }
}

impl Job {
    /// Appends the next batch of the same partition.
    fn merge(&mut self, next: Job) {
        self.seq = next.seq;
        self.posts.headers.extend(next.posts.headers);
        self.posts.spans.extend(next.posts.spans);
        self.strings.extend(next.strings);
        self.pay_len = next.pay_len;
    }
}

enum Apply {
    Batch(Job),
    /// Answered once every earlier batch is applied.
    Drain(Sender<()>),
}

/// Written batches waiting for the applier. A full queue holds up the writer.
const APPLY_QUEUE: usize = 4;

/// The applier merges queued batches into one apply of at most this many posts.
const MERGE_POSTS: usize = 8192;

/// The writer's handle on the applier thread, which applies written batches to the
/// active partition's layouts in log order and publishes them.
struct Applier {
    tx: Option<SyncSender<Apply>>,
    thread: Option<JoinHandle<()>>,
}

impl Applier {
    fn start(inner: Arc<Inner>) -> Result<Applier> {
        let (tx, rx) = sync_channel(APPLY_QUEUE);
        let thread = std::thread::Builder::new()
            .name("newdb-apply".into())
            .spawn(move || apply_loop(&inner, rx))?;
        Ok(Applier {
            tx: Some(tx),
            thread: Some(thread),
        })
    }

    /// Queues `m`; false when the applier is gone.
    fn send(&self, m: Apply) -> bool {
        self.tx.as_ref().is_some_and(|tx| tx.send(m).is_ok())
    }

    /// Waits until every batch sent so far is applied.
    fn drain(&self) -> Result<()> {
        let (tx, rx) = channel();
        if self.send(Apply::Drain(tx)) && rx.recv().is_ok() {
            return Ok(());
        }
        Err(corrupt("the layout applier stopped"))
    }
}

impl Drop for Applier {
    fn drop(&mut self) {
        self.tx = None;
        if let Some(t) = self.thread.take() {
            let _ = t.join();
        }
    }
}

fn apply_loop(inner: &Inner, rx: Receiver<Apply>) {
    /// Marks the applier stopped however it exits, so readers do not wait for it.
    struct Stopped<'a>(&'a Visible);
    impl Drop for Stopped<'_> {
        fn drop(&mut self) {
            self.0.update(|s| s.stopped = true);
        }
    }
    let _stopped = Stopped(&inner.visible);
    let pool = Pool::new(cores().min(3) - 1);
    let mut next = None;
    loop {
        let msg = match next.take() {
            Some(m) => m,
            None => match rx.recv() {
                Ok(m) => m,
                Err(_) => return,
            },
        };
        match msg {
            Apply::Drain(done) => {
                let _ = done.send(());
            }
            Apply::Batch(mut job) => {
                while job.posts.headers.len() < MERGE_POSTS {
                    match rx.try_recv() {
                        Ok(Apply::Batch(j)) if Arc::ptr_eq(&j.part, &job.part) => job.merge(j),
                        Ok(other) => {
                            next = Some(other);
                            break;
                        }
                        Err(_) => break,
                    }
                }
                let seq = job.seq;
                apply(&pool, job);
                inner.visible.update(|s| s.applied = seq);
            }
        }
    }
}

/// Applies a written batch to its partition's layouts. Readers wait on the write lock
/// meanwhile, so they never see the layouts while their builders are out on the workers.
fn apply(pool: &Pool, job: Job) {
    let Some(live) = job.part.live() else {
        return;
    };
    let mut g = live.write().unwrap_or_else(|e| e.into_inner());
    for s in job.strings {
        g.strings.push(s);
    }
    if let Blob::File { len, .. } = &mut g.blob {
        *len = job.pay_len;
    }
    let posts = Arc::new(job.posts);
    if posts.headers.len() >= PARALLEL_APPLY {
        let groups = pool.run(g.layouts.take(), &posts);
        g.layouts.put(groups);
    } else {
        g.layouts.apply_on(&posts.batch(), 1);
    }
    for h in &posts.headers {
        g.stats.add(h);
        g.keys.insert((h.project, h.trace_id, h.span_id));
    }
    g.stats.spans = g.keys.len() as u64;
}

/// Persistent threads that apply builder groups alongside the applier. A worker gets a
/// group and the batch by value and sends the group back.
struct Pool {
    workers: Vec<Worker>,
}

struct Worker {
    tx: Sender<(Group, Arc<Posts>)>,
    rx: Receiver<Group>,
}

impl Pool {
    fn new(n: usize) -> Pool {
        let workers = (0..n)
            .filter_map(|i| {
                let (tx, work) = channel::<(Group, Arc<Posts>)>();
                let (back, rx) = channel();
                std::thread::Builder::new()
                    .name(format!("newdb-apply-{}", i + 1))
                    .spawn(move || {
                        for (mut g, posts) in work {
                            g.apply(&posts.batch());
                            if back.send(g).is_err() {
                                return;
                            }
                        }
                    })
                    .ok()
                    .map(|_| Worker { tx, rx })
            })
            .collect();
        Pool { workers }
    }

    /// Applies `posts` to every group, the first on this thread and the others on the
    /// workers (or here when there are too few), and returns the groups.
    fn run(&self, groups: impl IntoIterator<Item = Group>, posts: &Arc<Posts>) -> Vec<Group> {
        let mut groups = groups.into_iter();
        let mut here: Vec<Group> = groups.next().into_iter().collect();
        let mut away = Vec::new();
        let mut workers = self.workers.iter();
        for g in groups {
            match workers.next() {
                Some(w) => match w.tx.send((g, posts.clone())) {
                    Ok(()) => away.push(w),
                    Err(e) => here.push(e.0.0),
                },
                None => here.push(g),
            }
        }
        let b = posts.batch();
        here.iter_mut().for_each(|g| g.apply(&b));
        // A worker that panicked dropped its group: the layouts cannot continue.
        here.extend(
            away.into_iter()
                .map(|w| w.rx.recv().expect("layout worker panicked")),
        );
        here
    }
}

/// State recovered from an unsealed partition's log, or the empty state of a new one.
struct Replayed {
    strings: Strings,
    stats: Stats,
    layouts: Layouts,
    keys: HashSet<crate::part::Key>,
    dedup: HashMap<payload::Hash, Vec<PayloadRef>>,
    log_len: u64,
    pay_len: u64,
}

impl Replayed {
    fn empty() -> Replayed {
        Replayed {
            strings: Strings::default(),
            stats: Stats::default(),
            layouts: Layouts::new(),
            keys: HashSet::new(),
            dedup: HashMap::new(),
            log_len: log::HEADER_LEN as u64,
            pay_len: newdb_format::FILE_HEADER_LEN as u64,
        }
    }
}

fn sealed_part(p: &Part, fixed: crate::part::Fixed) -> Arc<Part> {
    Arc::new(Part {
        name: p.name.clone(),
        id: p.id,
        hour: p.hour,
        dir: p.dir.clone(),
        sealed: true,
        body: Body::Fixed(fixed),
    })
}

fn entry(p: &Part, s: &Stats) -> PartitionEntry {
    PartitionEntry {
        name: p.name.clone(),
        id: p.id,
        hour: p.hour,
        posts: s.posts,
        spans: s.spans,
        min_start_ns: s.min_start(),
        max_start_ns: s.max_start(),
        min_version: s.version.map_or(0, |v| v.0),
        max_version: s.max_version(),
        layouts: LAYOUTS
            .iter()
            .map(|d| (d.name.to_string(), d.version))
            .collect(),
    }
}

/// Writes one layout's files into `layouts/<name>.v<N>/` via a temporary directory.
fn write_layout(part_dir: &Path, def: &LayoutDef, files: &LayoutFiles) -> Result<()> {
    let ld = part_dir.join(LAYOUTS_DIR);
    fs::create_dir_all(&ld)?;
    let name = layout_dir_name(def.name, def.version);
    let tmp = ld.join(format!(".{name}.tmp"));
    remove_dir_if_exists(&tmp)?;
    fs::create_dir(&tmp)?;
    for (n, b) in &files.files {
        files::check_file_name(n)?;
        write_synced(&tmp.join(n), b)?;
    }
    sync_dir(&tmp)?;
    let fin = ld.join(&name);
    remove_dir_if_exists(&fin)?;
    fs::rename(&tmp, &fin)?;
    sync_dir(&ld)?;
    sync_dir(part_dir)
}

fn size(spans: &[Span]) -> usize {
    spans
        .iter()
        .map(|s| {
            let p = |x: &Option<Vec<u8>>| x.as_ref().map_or(0, Vec::len);
            200 + p(&s.inputs)
                + p(&s.outputs)
                + p(&s.attributes)
                + p(&s.events)
                + s.error.as_ref().map_or(0, String::len)
        })
        .sum()
}

/// One batch being encoded: its log and payload bytes and what it adds to the
/// partition's string table and payload index.
struct Encoding {
    log: Vec<u8>,
    pay: Vec<u8>,
    /// File offset of `pay[0]`.
    pay_base: u64,
    new_strings: Strings,
    new_dedup: HashMap<payload::Hash, Vec<PayloadRef>>,
}

impl Encoding {
    fn string(&mut self, s: &str, table: &Strings) -> u32 {
        if let Some(id) = table.id(s) {
            return id;
        }
        if let Some(id) = self.new_strings.id(s) {
            return table.len() as u32 + id;
        }
        let id = table.len() as u32 + self.new_strings.push(s.to_string());
        log::append_string(&mut self.log, id, s);
        id
    }

    /// The stored copy of `raw` in the committed part of the payload file, if any.
    fn committed(
        &self,
        raw: &[u8],
        h: payload::Hash,
        committed: &HashMap<payload::Hash, Vec<PayloadRef>>,
        file: &File,
    ) -> Result<Option<PayloadRef>> {
        for &r in committed.get(&h).into_iter().flatten() {
            // Confirm the hash hit by comparing bytes.
            if r.raw_len as usize == raw.len()
                && crate::part::BlobRef::File(file, self.pay_base).read(r)? == raw
            {
                return Ok(Some(r));
            }
        }
        Ok(None)
    }

    /// Encodes a batch: string records and payload entries, then one post record per
    /// span. Payloads are deduplicated against the partition and within the batch first,
    /// then the new ones are appended as the ingesting thread compressed them.
    fn encode(
        &mut self,
        spans: &[Span],
        pre: &[Prepared],
        table: &Strings,
        committed: &HashMap<payload::Hash, Vec<PayloadRef>>,
        file: &File,
    ) -> Result<Vec<SpanHeader>> {
        let mut headers: Vec<SpanHeader> = spans.iter().map(|s| self.scalars(s, table)).collect();
        // Every payload of the batch: (span, field), and where its stored copy comes from.
        let mut jobs: Vec<(usize, u32, Src)> = Vec::new();
        let mut fresh: Vec<(&[u8], &Stored)> = Vec::new();
        let mut by_hash: HashMap<payload::Hash, Vec<usize>> = HashMap::new();
        for (i, (s, p)) in spans.iter().zip(pre).enumerate() {
            for stored in p {
                let (bit, h) = (stored.bit, stored.hash);
                let Some(raw) = payload_field(s, bit) else {
                    return Err(corrupt("prepared payload of an unset field"));
                };
                if let Some(r) = self.committed(raw, h, committed, file)? {
                    jobs.push((i, bit, Src::Stored(r)));
                    continue;
                }
                let same = by_hash.entry(h).or_default();
                let k = match same.iter().find(|&&k| fresh[k].0 == raw) {
                    Some(&k) => k,
                    None => {
                        same.push(fresh.len());
                        fresh.push((raw, stored));
                        fresh.len() - 1
                    }
                };
                jobs.push((i, bit, Src::Fresh(k)));
            }
        }
        let mut refs = Vec::with_capacity(fresh.len());
        for (raw, p) in &fresh {
            let r = payload::append_entry(
                raw,
                p.data.as_deref(),
                p.hash,
                self.pay_base,
                &mut self.pay,
            )?;
            self.new_dedup.entry(p.hash).or_default().push(r);
            refs.push(r);
        }
        for (i, bit, r) in jobs {
            let r = match r {
                Src::Stored(r) => r,
                Src::Fresh(k) => refs[k],
            };
            let h = &mut headers[i];
            h.fields |= bit;
            *payload_slot(h, bit) = r;
        }
        for h in &headers {
            log::append_post(&mut self.log, h);
        }
        Ok(headers)
    }

    /// The header of `s` without its payload references.
    fn scalars(&mut self, s: &Span, table: &Strings) -> SpanHeader {
        let mut h = SpanHeader {
            trace_id: s.trace_id,
            span_id: s.span_id,
            version: s.version,
            ..SpanHeader::default()
        };
        h.project = self.string(&s.project, table);
        let mut f = 0;
        let mut set = |bit: u32, on: bool| {
            if on {
                f |= bit;
            }
        };
        set(PARENT, s.parent_span_id.is_some());
        set(START, s.start_ns.is_some());
        set(END, s.end_ns.is_some());
        set(STATUS, s.status.is_some());
        set(RUN_TYPE, s.run_type.is_some());
        set(PROMPT_TOKENS, s.prompt_tokens.is_some());
        set(COMPLETION_TOKENS, s.completion_tokens.is_some());
        set(COST, s.cost_micro_usd.is_some());
        h.parent_span_id = s.parent_span_id.unwrap_or_default();
        h.start_ns = s.start_ns.unwrap_or(0);
        h.end_ns = s.end_ns.unwrap_or(0);
        h.status = s.status.map_or(0, |x| x as u8);
        h.run_type = s.run_type.map_or(0, |x| x as u8);
        h.prompt_tokens = s.prompt_tokens.unwrap_or(0);
        h.completion_tokens = s.completion_tokens.unwrap_or(0);
        h.cost_micro_usd = s.cost_micro_usd.unwrap_or(0);
        for (bit, v, slot) in [
            (NAME, &s.name, &mut h.name),
            (MODEL, &s.model, &mut h.model),
            (PROVIDER, &s.provider, &mut h.provider),
            (SESSION, &s.session, &mut h.session),
        ] {
            if let Some(v) = v {
                f |= bit;
                *slot = self.string(v, table);
            }
        }
        h.fields = f;
        h
    }
}

/// One payload of a post as the ingesting thread prepared it.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct Stored {
    pub bit: u32,
    pub hash: payload::Hash,
    /// The zstd frame, or `None` to store the bytes raw.
    pub data: Option<Vec<u8>>,
}

/// The payloads of one post.
pub(crate) type Prepared = Vec<Stored>;

/// Below this many payload bytes per thread, splitting a batch costs more than it saves.
const PARALLEL_BYTES: usize = 256 << 10;

/// Hashes and compresses the payloads of `spans` on the calling thread, so concurrent
/// ingests compress in parallel and the writer only deduplicates and appends. A large
/// batch is split across a few scoped threads.
pub(crate) fn prepare(spans: &[Span]) -> Result<Vec<Prepared>> {
    let total: usize = spans.iter().map(|s| size(std::slice::from_ref(s))).sum();
    prepare_on(
        spans,
        crate::part::cores().min(8).min(total / PARALLEL_BYTES + 1),
    )
}

pub(crate) fn prepare_on(spans: &[Span], threads: usize) -> Result<Vec<Prepared>> {
    if threads <= 1 {
        return spans.iter().map(prepare_one).collect();
    }
    let chunk = spans.len().div_ceil(threads);
    std::thread::scope(|sc| {
        let parts: Vec<_> = spans
            .chunks(chunk)
            .map(|c| sc.spawn(move || c.iter().map(prepare_one).collect::<Result<Vec<_>>>()))
            .collect();
        let mut out = Vec::with_capacity(spans.len());
        for p in parts {
            let part = p
                .join()
                .map_err(|_| Error::Io(std::io::Error::other("compression thread panicked")))?;
            out.extend(part?);
        }
        Ok(out)
    })
}

thread_local! {
    static ENCODER: std::cell::RefCell<Option<payload::Encoder>> =
        const { std::cell::RefCell::new(None) };
}

fn prepare_one(s: &Span) -> Result<Prepared> {
    ENCODER.with(|cell| {
        let mut slot = cell.borrow_mut();
        let enc = match slot.as_mut() {
            Some(e) => e,
            None => slot.insert(payload::Encoder::new()?),
        };
        let mut out = Vec::new();
        for bit in PAYLOAD_FIELDS {
            if let Some(raw) = payload_field(s, bit) {
                out.push(Stored {
                    bit,
                    hash: payload::hash(raw),
                    data: enc.compress(raw)?,
                });
            }
        }
        Ok(out)
    })
}

/// Where a batch's payload is stored: an existing entry, or the `k`-th new one.
enum Src {
    Stored(PayloadRef),
    Fresh(usize),
}

fn payload_field(s: &Span, bit: u32) -> Option<&[u8]> {
    match bit {
        INPUTS => s.inputs.as_deref(),
        OUTPUTS => s.outputs.as_deref(),
        ATTRIBUTES => s.attributes.as_deref(),
        EVENTS => s.events.as_deref(),
        _ => s.error.as_deref().map(str::as_bytes),
    }
}

fn payload_slot(h: &mut SpanHeader, bit: u32) -> &mut PayloadRef {
    match bit {
        INPUTS => &mut h.inputs,
        OUTPUTS => &mut h.outputs,
        ATTRIBUTES => &mut h.attributes,
        EVENTS => &mut h.events,
        _ => &mut h.error,
    }
}
