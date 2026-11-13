//! Shared types for NewDB. Every other crate depends on this one, so it changes rarely and
//! has one dependency (`bytemuck`, without its derive feature).
//!
//! - [`ids`]: trace and span IDs.
//! - [`span`]: the owned [`Span`] record and the "latest non-null field wins" merge rule.
//! - [`header`]: fixed-size, `#[repr(C)]` on-disk structs ([`SpanHeader`], [`RunSummary`]).
//! - [`store`]: the [`Store`] trait, the query surface shared by the engine, the oracle,
//!   the server, the Python binding and the tools.
//! - [`text`]: the tokenizer that defines what full-text search matches.

#[cfg(not(target_endian = "little"))]
compile_error!("NewDB's on-disk structs are little-endian; big-endian targets are not supported.");

pub mod header;
pub mod ids;
pub mod span;
pub mod store;
pub mod text;

pub use header::{PayloadRef, RunSummary, SpanHeader};
pub use ids::{SpanId, TraceId};
pub use span::{RunType, Span, SpanRef, Status};
pub use store::{
    Read, ReadStats, Run, RunKey, RunQuery, SearchHit, SearchQuery, Store, TraceQuery,
};

use std::fmt;

/// Errors every NewDB crate returns.
#[derive(Debug)]
pub enum Error {
    Io(std::io::Error),
    /// Stored bytes failed a checksum or structural check.
    Corrupt(String),
    /// The caller passed something invalid (bad ID, unknown project, malformed OTLP).
    InvalidInput(String),
    /// The database was opened read-only or has been closed.
    ReadOnly,
}

pub type Result<T> = std::result::Result<T, Error>;

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Io(e) => write!(f, "I/O error: {e}"),
            Error::Corrupt(m) => write!(f, "corrupt data: {m}"),
            Error::InvalidInput(m) => write!(f, "invalid input: {m}"),
            Error::ReadOnly => write!(f, "database is read-only"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::Io(e) => Some(e),
            _ => None,
        }
    }
}

impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        Error::Io(e)
    }
}
