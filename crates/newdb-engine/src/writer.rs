//! The one writer thread: group commit, partition lifecycle (create, recover, seal),
//! retention and layout rebuilds. Everything that changes files runs here, in order.

use std::collections::{HashMap, HashSet};
use std::fs::{self, File, OpenOptions};
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::sync::mpsc::{Receiver, RecvTimeoutError, Sender, TryRecvError};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use newdb_format::manifest::{self, Manifest, PartitionEntry};
use newdb_format::{log, payload, strings as strtbl};
use newdb_layouts::{Batch, LAYOUTS, LayoutDef, LayoutFiles};
use newdb_types::header::field::*;
use newdb_types::{Error, PayloadRef, Result, Span, SpanHeader};

use crate::Inner;
use crate::files::{
    self, HOUR_NS, LAYOUTS_DIR, LOG, PAYLOADS, STRINGS, TRASH, layout_dir_name,
    remove_dir_if_exists, sync_dir, write_synced,
};
use crate::part::{
    Blob, Body, Layouts, Live, Mode, Part, Stats, Strings, corrupt, open_sealed, replay,
};

pub(crate) type Ack = Sender<Result<()>>;

pub(crate) enum Msg {
    Ingest(Vec<Span>, Ack),
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
}

pub(crate) struct Writer {
    inner: Arc<Inner>,
    manifest: Manifest,
    active: Option<Active>,
    enc: payload::Encoder,
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
            inner,
            manifest,
            active: None,
            enc: payload::Encoder::new()?,
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
                Msg::Ingest(spans, ack) => {
                    let mut batch = vec![(spans, ack)];
                    let mut bytes = size(&batch[0].0);
                    let deadline = Instant::now() + self.inner.config.commit_interval;
                    while bytes < self.inner.config.commit_bytes {
                        let wait = deadline.saturating_duration_since(Instant::now());
                        let next = if wait.is_zero() {
                            rx.try_recv().map_err(|e| e == TryRecvError::Disconnected)
                        } else {
                            rx.recv_timeout(wait)
                                .map_err(|e| e == RecvTimeoutError::Disconnected)
                        };
                        match next {
                            Ok(Msg::Ingest(s, a)) => {
                                bytes += size(&s);
                                batch.push((s, a));
                            }
                            Ok(other) => {
                                stashed = Some(other);
                                break;
                            }
                            Err(_) => break,
                        }
                    }
                    self.commit(batch);
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
                    let _ = ack.send(Ok(()));
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

    fn commit(&mut self, batch: Vec<(Vec<Span>, Ack)>) {
        let (spans, acks): (Vec<_>, Vec<_>) = batch.into_iter().unzip();
        let spans: Vec<Span> = spans.into_iter().flatten().collect();
        let res = self.roll().and_then(|_| self.write(spans));
        for ack in acks {
            let _ = ack.send(res.as_ref().map(|_| ()).map_err(dup));
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
        });
        Ok(())
    }

    /// Appends `spans` to the active partition (creating it if needed), syncs, and makes
    /// them visible to readers.
    fn write(&mut self, spans: Vec<Span>) -> Result<()> {
        if spans.is_empty() {
            return Ok(());
        }
        if self.active.is_none() {
            let hour = self.now_hour();
            self.create_active(hour)?;
        }
        let Some(a) = self.active.as_mut() else {
            return Err(corrupt("no active partition"));
        };
        let Some(live) = a.part.live() else {
            return Err(corrupt("active partition is sealed"));
        };
        let mut enc = Encoding {
            log: Vec::new(),
            pay: Vec::new(),
            pay_base: a.pay_len,
            new_strings: Strings::default(),
            new_dedup: HashMap::new(),
        };
        let mut headers = Vec::with_capacity(spans.len());
        let mut spans = spans;
        {
            let g = live.read().unwrap_or_else(|e| e.into_inner());
            for (i, span) in spans.iter_mut().enumerate() {
                span.version = self.next_version + i as u64;
                let h = enc.header(span, &g.strings, &a.dedup, &a.pay, &mut self.enc)?;
                log::append_post(&mut enc.log, &h);
                headers.push(h);
            }
        }
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
        a.log_len += enc.log.len() as u64;
        a.pay_len += enc.pay.len() as u64;
        for (h, refs) in enc.new_dedup {
            a.dedup.entry(h).or_default().extend(refs);
        }
        let mut g = live.write().unwrap_or_else(|e| e.into_inner());
        for s in enc.new_strings.list {
            g.strings.push(s);
        }
        if let Blob::File { len, .. } = &mut g.blob {
            *len = a.pay_len;
        }
        g.layouts.apply(&Batch {
            first_post: a.posts,
            headers: &headers,
            spans: &spans,
        });
        for h in &headers {
            g.stats.add(h);
            g.keys.insert((h.project, h.trace_id, h.span_id));
        }
        g.stats.spans = g.keys.len() as u64;
        a.posts += headers.len() as u32;
        self.next_version += headers.len() as u64;
        Ok(())
    }

    /// Writes the active partition's layouts and string table, lists it in MANIFEST and
    /// reopens it from its files. An empty active partition is removed instead.
    pub fn seal(&mut self) -> Result<()> {
        let Some(a) = self.active.as_ref() else {
            return Ok(());
        };
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

    fn payload(
        &mut self,
        raw: &[u8],
        committed: &HashMap<payload::Hash, Vec<PayloadRef>>,
        file: &File,
        enc: &mut payload::Encoder,
    ) -> Result<PayloadRef> {
        let h = payload::hash(raw);
        let known = committed.get(&h).into_iter().chain(self.new_dedup.get(&h));
        for &r in known.flatten() {
            if r.raw_len as usize != raw.len() {
                continue;
            }
            // Confirm the hash hit by comparing bytes.
            let stored = if r.offset >= self.pay_base {
                let start = (r.offset - self.pay_base) as usize - payload::ENTRY_HEADER_LEN;
                let end = (r.offset - self.pay_base) as usize + r.len as usize;
                payload::decode(&self.pay[start..end], r)?
            } else {
                crate::part::BlobRef::File(file, self.pay_base).read(r)?
            };
            if stored == raw {
                return Ok(r);
            }
        }
        let r = enc.append(raw, h, self.pay_base, &mut self.pay)?;
        self.new_dedup.entry(h).or_default().push(r);
        Ok(r)
    }

    fn header(
        &mut self,
        s: &Span,
        table: &Strings,
        committed: &HashMap<payload::Hash, Vec<PayloadRef>>,
        file: &File,
        enc: &mut payload::Encoder,
    ) -> Result<SpanHeader> {
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
        for (bit, v, slot) in [
            (INPUTS, s.inputs.as_deref(), &mut h.inputs),
            (OUTPUTS, s.outputs.as_deref(), &mut h.outputs),
            (ATTRIBUTES, s.attributes.as_deref(), &mut h.attributes),
            (EVENTS, s.events.as_deref(), &mut h.events),
            (ERROR, s.error.as_deref().map(str::as_bytes), &mut h.error),
        ] {
            if let Some(v) = v {
                f |= bit;
                *slot = self.payload(v, committed, file, enc)?;
            }
        }
        h.fields = f;
        Ok(h)
    }
}
