//! Layouts: the data structures NewDB stores, one set per partition, all derived from the
//! partition's log and rebuildable from it (DESIGN.md section 7).
//!
//! This crate depends only on `newdb-types`, so it is built and tested without the engine.
//! Every layout is two halves:
//!
//! - a **builder** that the engine feeds committed [`Batch`]es in log order. While the
//!   partition is active, the engine queries the builder directly (the `query` methods on
//!   each builder). At seal time, [`LayoutBuilder::seal`] writes the layout's files.
//! - a **reader** that opens those files from memory-mapped bytes ([`FileSet`]) and answers
//!   the same queries as the builder, without copying or decoding fixed-size records.
//!
//! # Contract with the engine (lane 1)
//!
//! - **Same answers:** for any sequence of batches, a builder's query methods and the
//!   reader opened from its sealed files return identical results.
//! - **Deterministic:** the same batches always produce byte-identical files.
//!   `verify` rebuilds layouts from the log and compares checksums.
//! - **Within one partition only:** string IDs and payload refs in headers are partition
//!   local. Layouts merge posts of the same span within their partition with
//!   [`SpanHeader::merge_from`]; the engine merges across partitions.
//! - **Alignment:** every array of `#[repr(C)]` structs in a file starts at an offset that
//!   is a multiple of 8, so readers can `bytemuck::cast_slice` mapped bytes. Files start
//!   page-aligned.
//! - Every reader method returns, besides its answer, how many records it looked at and
//!   how many bytes it touched, through [`Cost`], so the engine can fill `ReadStats`.

mod file;
pub mod fts;
pub mod runs_by_time;
pub mod trace_clustered;
pub mod trace_filter;

use newdb_types::{Result, Span, SpanHeader};

/// One committed batch of posts, as the engine hands it to every builder.
///
/// `headers[i]` and `spans[i]` describe the same post. Headers already carry the partition's
/// string IDs, payload refs and the assigned `version`; spans carry the original strings and
/// payload bytes (for layouts such as `fts` that need text).
pub struct Batch<'a> {
    /// Position of `headers[0]` among all posts of the partition, counting from 0.
    pub first_post: u32,
    pub headers: &'a [SpanHeader],
    pub spans: &'a [Span],
}

/// Files a builder writes at seal time: `(file name, bytes)`. Names are relative to the
/// layout's version directory, e.g. `spans.bin`. The engine writes and fsyncs them.
#[derive(Default, Debug, PartialEq, Eq)]
pub struct LayoutFiles {
    pub files: Vec<(String, Vec<u8>)>,
}

impl LayoutFiles {
    pub fn add(&mut self, name: impl Into<String>, bytes: Vec<u8>) {
        self.files.push((name.into(), bytes));
    }
}

/// Sealed files of one layout, mapped by the engine. Readers borrow from it.
#[derive(Default, Debug, Clone, Copy)]
pub struct FileSet<'a> {
    pub files: &'a [(&'a str, &'a [u8])],
}

impl<'a> FileSet<'a> {
    pub fn get(&self, name: &str) -> Result<&'a [u8]> {
        self.files
            .iter()
            .find(|(n, _)| *n == name)
            .map(|(_, b)| *b)
            .ok_or_else(|| newdb_types::Error::Corrupt(format!("layout file {name} is missing")))
    }
}

/// Work a query did, for `ReadStats`.
#[derive(Default, Debug, Clone, Copy, PartialEq, Eq)]
pub struct Cost {
    pub records_visited: u64,
    pub bytes_read: u64,
}

/// What every builder implements. Query methods are concrete per layout.
pub trait LayoutBuilder: Send {
    /// Called for every committed batch of the partition, in log order.
    fn apply(&mut self, batch: &Batch<'_>);
    /// Writes this layout's files. Called once, after the last `apply`.
    fn seal(&self, out: &mut LayoutFiles) -> Result<()>;
}

/// A registered layout. The engine creates one builder per partition from each def.
pub struct LayoutDef {
    pub name: &'static str,
    /// Bump on any change to what the builder writes. Part of the directory name.
    pub version: u32,
    pub new_builder: fn() -> Box<dyn LayoutBuilder>,
}

/// Every layout the engine maintains, in a fixed order.
pub const LAYOUTS: &[LayoutDef] = &[
    LayoutDef {
        name: trace_clustered::NAME,
        version: trace_clustered::VERSION,
        new_builder: || Box::new(trace_clustered::Builder::default()),
    },
    LayoutDef {
        name: runs_by_time::NAME,
        version: runs_by_time::VERSION,
        new_builder: || Box::new(runs_by_time::Builder::default()),
    },
    LayoutDef {
        name: trace_filter::NAME,
        version: trace_filter::VERSION,
        new_builder: || Box::new(trace_filter::Builder::default()),
    },
    LayoutDef {
        name: fts::NAME,
        version: fts::VERSION,
        new_builder: || Box::new(fts::Builder::default()),
    },
];
