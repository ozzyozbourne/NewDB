//! Tools over any [`Store`] (DESIGN.md section 9): the bench runner and the counting
//! allocator. Works against `newdb_testkit::Oracle` now and `newdb_engine::Db` after
//! integration. The CLI prints `inspect` and `verify` reports itself.
//!
//! `bench` output is one JSON object (written by hand, no serde) so two runs can be compared
//! with `newdb bench --compare a.json b.json`.

use std::time::Duration;

use newdb_testkit::r#gen::GenConfig;
use newdb_types::{Result, Store};

/// What a bench run does.
#[derive(Clone, Debug)]
pub struct BenchConfig {
    pub workload: GenConfig,
    /// Posts per `ingest` call.
    pub batch: usize,
    /// Repetitions of each read, for p50/p99.
    pub iterations: usize,
}

impl Default for BenchConfig {
    fn default() -> Self {
        BenchConfig {
            workload: GenConfig::default(),
            batch: 1000,
            iterations: 50,
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct Latency {
    pub p50: Duration,
    pub p99: Duration,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct BenchReport {
    pub posts: u64,
    pub ingest_posts_per_sec: f64,
    /// `(read name, latency)`, e.g. `("trace_largest_with_payloads", ...)`,
    /// `("runs_newest_50", ...)`, `("runs_errors_50", ...)`, `("search_refund_policy", ...)`.
    pub reads: Vec<(String, Latency)>,
    /// Allocations per read, when the counting allocator is installed.
    pub allocations: Vec<(String, u64)>,
}

/// Ingests the workload into `store`, then times the standard reads.
pub fn run_bench(store: &dyn Store, config: &BenchConfig) -> Result<BenchReport> {
    let _ = (store, config);
    todo!("lane 4")
}

impl BenchReport {
    pub fn to_json(&self) -> String {
        todo!("lane 4")
    }

    pub fn from_json(s: &str) -> Result<BenchReport> {
        let _ = s;
        todo!("lane 4")
    }

    /// Side-by-side table of two reports with the change in percent.
    pub fn compare(&self, other: &BenchReport) -> String {
        let _ = other;
        todo!("lane 4")
    }
}

/// A `GlobalAlloc` wrapper that counts allocations. Install with `#[global_allocator]` in
/// bench builds only.
pub struct CountingAlloc;

impl CountingAlloc {
    /// Allocations since the process started.
    pub fn count() -> u64 {
        todo!("lane 4")
    }
}
