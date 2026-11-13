//! Fixed-size on-disk structs. They are `#[repr(C)]`, little-endian, with explicit padding
//! and no implicit padding, so they can be cast straight from memory-mapped bytes with
//! `bytemuck::cast_slice` / `bytemuck::from_bytes` (alignment 8).
//!
//! String fields (`project`, `name`, `model`, `provider`, `session`) are IDs into the
//! partition's string table, so a header only means something together with its partition.
//! ID 0 is reserved for "no string".
//!
//! Changing any layout here is an engine format change: bump [`HEADER_FORMAT_VERSION`].

use bytemuck::{Pod, Zeroable};

use crate::ids::{SpanId, TraceId};
use crate::span::{RunType, Status};

pub const HEADER_FORMAT_VERSION: u32 = 1;

/// Reference to one payload in a partition's payload store. All zero means absent.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
#[repr(C)]
pub struct PayloadRef {
    /// Byte offset of the compressed payload in `payloads.blob`.
    pub offset: u64,
    /// Compressed length in bytes.
    pub len: u32,
    /// Uncompressed length in bytes.
    pub raw_len: u32,
}

impl PayloadRef {
    pub const NONE: PayloadRef = PayloadRef {
        offset: 0,
        len: 0,
        raw_len: 0,
    };

    pub fn is_none(&self) -> bool {
        self.len == 0
    }
}

/// Bits of [`SpanHeader::fields`]: which optional fields this header sets. Merging two
/// headers takes each set field from the newer one, as [`crate::Span::merge_from`] does.
pub mod field {
    pub const PARENT: u32 = 1 << 0;
    pub const NAME: u32 = 1 << 1;
    pub const START: u32 = 1 << 2;
    pub const END: u32 = 1 << 3;
    pub const STATUS: u32 = 1 << 4;
    pub const RUN_TYPE: u32 = 1 << 5;
    pub const MODEL: u32 = 1 << 6;
    pub const PROVIDER: u32 = 1 << 7;
    pub const SESSION: u32 = 1 << 8;
    pub const PROMPT_TOKENS: u32 = 1 << 9;
    pub const COMPLETION_TOKENS: u32 = 1 << 10;
    pub const COST: u32 = 1 << 11;
    pub const INPUTS: u32 = 1 << 12;
    pub const OUTPUTS: u32 = 1 << 13;
    pub const ATTRIBUTES: u32 = 1 << 14;
    pub const EVENTS: u32 = 1 << 15;
    pub const ERROR: u32 = 1 << 16;
}

/// One span post (or several posts merged), as layouts store it. 184 bytes.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[repr(C)]
pub struct SpanHeader {
    pub trace_id: TraceId,
    pub span_id: SpanId,
    /// Meaningful only when `fields & field::PARENT` is set; all zero then still means root.
    pub parent_span_id: SpanId,
    pub start_ns: u64,
    pub end_ns: u64,
    pub version: u64,
    pub cost_micro_usd: u64,
    pub project: u32,
    pub name: u32,
    pub model: u32,
    pub provider: u32,
    pub session: u32,
    pub prompt_tokens: u32,
    pub completion_tokens: u32,
    /// Bitmask of [`field`] constants.
    pub fields: u32,
    pub status: u8,
    pub run_type: u8,
    pub _pad: [u8; 6],
    pub inputs: PayloadRef,
    pub outputs: PayloadRef,
    pub attributes: PayloadRef,
    pub events: PayloadRef,
    pub error: PayloadRef,
}

impl SpanHeader {
    pub fn has(&self, f: u32) -> bool {
        self.fields & f != 0
    }

    pub fn status(&self) -> Status {
        Status::from_u8(self.status).unwrap_or_default()
    }

    pub fn run_type(&self) -> RunType {
        RunType::from_u8(self.run_type).unwrap_or_default()
    }

    /// Root after merging: no post set a non-zero parent.
    pub fn is_root(&self) -> bool {
        !self.has(field::PARENT) || self.parent_span_id.is_zero()
    }

    /// Header version of [`crate::Span::merge_from`]: every field `newer` sets replaces the
    /// current value. Only valid for two headers of the same span in the SAME partition,
    /// because string IDs and payload refs are per partition. Across partitions, resolve
    /// to [`crate::Span`] first and merge those.
    pub fn merge_from(&mut self, newer: &SpanHeader) {
        use field::*;
        macro_rules! take {
            ($($bit:ident => $f:ident),*) => { $( if newer.has($bit) { self.$f = newer.$f; } )* };
        }
        take!(
            PARENT => parent_span_id, NAME => name, START => start_ns, END => end_ns,
            STATUS => status, RUN_TYPE => run_type, MODEL => model, PROVIDER => provider,
            SESSION => session, PROMPT_TOKENS => prompt_tokens,
            COMPLETION_TOKENS => completion_tokens, COST => cost_micro_usd,
            INPUTS => inputs, OUTPUTS => outputs, ATTRIBUTES => attributes, EVENTS => events,
            ERROR => error
        );
        self.fields |= newer.fields;
        self.version = self.version.max(newer.version);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn header_merge_takes_only_fields_the_newer_header_sets() {
        let mut a = SpanHeader {
            start_ns: 10,
            status: 0,
            fields: field::START | field::STATUS,
            version: 1,
            ..SpanHeader::default()
        };
        let b = SpanHeader {
            start_ns: 999, // not in b.fields: must be ignored
            end_ns: 50,
            status: Status::Error as u8,
            fields: field::END | field::STATUS,
            version: 4,
            ..SpanHeader::default()
        };
        a.merge_from(&b);
        assert_eq!(
            (a.start_ns, a.end_ns, a.status(), a.version),
            (10, 50, Status::Error, 4)
        );
        assert_eq!(a.fields, field::START | field::STATUS | field::END);
    }
}

/// One run (root span) in the `runs_by_time` layout: 80 bytes, merged within its partition.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[repr(C)]
pub struct RunSummary {
    pub start_ns: u64,
    /// 0 while open.
    pub end_ns: u64,
    pub cost_micro_usd: u64,
    pub trace_id: TraceId,
    pub span_id: SpanId,
    pub project: u32,
    pub name: u32,
    pub model: u32,
    pub session: u32,
    pub prompt_tokens: u32,
    pub completion_tokens: u32,
    pub status: u8,
    pub run_type: u8,
    /// Low 16 bits of the root header's `fields` mask, so cross-partition merges know
    /// which values this partition actually saw.
    pub fields: u16,
    pub _pad: [u8; 4],
}

// SAFETY: all three structs are repr(C), contain only integers and byte arrays, and have
// no implicit padding (checked by the size asserts below), so every bit pattern is valid.
unsafe impl Zeroable for PayloadRef {}
unsafe impl Pod for PayloadRef {}
unsafe impl Zeroable for TraceId {}
unsafe impl Pod for TraceId {}
unsafe impl Zeroable for SpanId {}
unsafe impl Pod for SpanId {}
unsafe impl Zeroable for SpanHeader {}
unsafe impl Pod for SpanHeader {}
unsafe impl Zeroable for RunSummary {}
unsafe impl Pod for RunSummary {}

const _: () = {
    assert!(size_of::<PayloadRef>() == 16);
    assert!(size_of::<SpanHeader>() == 184);
    assert!(align_of::<SpanHeader>() == 8);
    assert!(size_of::<RunSummary>() == 80);
    assert!(align_of::<RunSummary>() == 8);
};
