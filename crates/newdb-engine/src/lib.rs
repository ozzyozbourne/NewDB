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

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use newdb_types::{Read, Result, Run, RunQuery, SearchHit, SearchQuery, Span, Store, TraceQuery};

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
    _path: PathBuf,
}

impl Db {
    /// Opens or creates the database at `path` and recovers the active partition.
    pub fn open(path: impl AsRef<Path>, config: Config) -> Result<Db> {
        let _ = (path.as_ref(), config);
        todo!("lane 1")
    }

    /// Seals the active partition now, as if its hour had ended. For tests and tools.
    pub fn seal_active(&self) -> Result<()> {
        todo!("lane 1")
    }

    /// Applies retention now instead of waiting for the next hourly check.
    pub fn apply_retention(&self) -> Result<()> {
        todo!("lane 1")
    }

    pub fn partitions(&self) -> Result<Vec<PartitionInfo>> {
        todo!("lane 1")
    }

    /// Rebuilds every layout of every sealed partition from its log into a temporary
    /// directory and compares checksums with the stored files; checks log CRCs, payload
    /// refs and MANIFEST against the disk.
    pub fn verify(&self) -> Result<VerifyReport> {
        todo!("lane 1")
    }

    pub fn inspect(&self) -> Result<InspectReport> {
        todo!("lane 1")
    }

    /// Rebuilds `layout` (or every layout when `None`) for each sealed partition from its
    /// log into the layout's current version directory, then switches MANIFEST over.
    pub fn rebuild(&self, layout: Option<&str>) -> Result<()> {
        let _ = layout;
        todo!("lane 1")
    }

    /// Flushes the pending batch and stops the writer. Dropping a `Db` does the same.
    pub fn close(self) -> Result<()> {
        todo!("lane 1")
    }
}

impl Store for Db {
    fn ingest(&self, spans: Vec<Span>) -> Result<()> {
        let _ = spans;
        todo!("lane 1")
    }

    fn trace(&self, query: &TraceQuery) -> Result<Read<Vec<Span>>> {
        let _ = query;
        todo!("lane 1")
    }

    fn runs(&self, query: &RunQuery) -> Result<Read<Vec<Run>>> {
        let _ = query;
        todo!("lane 1")
    }

    fn search(&self, query: &SearchQuery) -> Result<Read<Vec<SearchHit>>> {
        let _ = query;
        todo!("lane 1")
    }
}
