//! Test support shared by every crate.
//!
//! - [`Oracle`]: an in-memory [`Store`](newdb_types::Store) that answers every query by
//!   brute force, straight from the semantics in `newdb_types::store`. It is slow and
//!   obviously correct; the real engine must give the same answers.
//! - [`gen`]: a deterministic generator of agent-shaped span posts.
//! - [`diff`]: runs the same workload and queries against two stores and reports the
//!   first difference.

pub mod diff;
pub mod r#gen;
mod oracle;

pub use oracle::Oracle;
