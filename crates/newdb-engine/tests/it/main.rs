//! Engine tests against the public API only (`Db`, `Config`, `ManualClock`, `Store`), with
//! `newdb_testkit::Oracle` as the expected answers. They add to the unit tests in
//! `src/tests.rs` rather than repeat them:
//!
//! - [`pagination`]: `runs` pages with `before` across partitions whose start-time ranges
//!   overlap, with filters, return every run exactly once.
//! - [`randomized`]: 2,000 traces (500 in debug builds), batch sizes 1..=500, random clock
//!   jumps, `seal_active` and reopens (`NEWDB_IT_SEED` reruns one seed, `NEWDB_IT_TRACES`
//!   sets the trace count).
//! - [`concurrent`]: readers during ingest and sealing see some prefix state, never older
//!   than one they saw before.
//! - [`seal_crash`]: a crash between layout writes and the MANIFEST update, simulated
//!   exactly and by SIGKILL of a child process during `seal_active`.
//!
//! Run with `cargo nextest run -p newdb-engine`.

mod common;
mod concurrent;
mod pagination;
mod randomized;
mod seal_crash;
