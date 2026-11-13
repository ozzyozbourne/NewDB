//! OTLP trace export requests to [`Span`] posts (DESIGN.md sections 5 and 8.3).
//!
//! - Protobuf: `ExportTraceServiceRequest` decoded with prost. The generated code is
//!   checked in under `src/proto/` (no `build.rs`, no protoc at build time).
//! - JSON: the OTLP/JSON encoding (hex IDs, string-encoded int64). Lane 3 picks the parser;
//!   keep it out of the core crates.
//! - Mapping: OTel GenAI semantic conventions first, then OpenInference and OpenLLMetry
//!   attribute names, to `run_type`, `model`, `provider`, tokens, `session`, `inputs`,
//!   `outputs`. The attribute-name mapping and the model pricing table are data files in
//!   `data/`, embedded with `include_str!` and overridable from a path at startup.
//! - Project: resource attribute `newdb.project` (also accept `forge.project`), else
//!   `service.name`, else `default`.
//! - Attributes not mapped to a field go into `attributes` as a JSON object; span events
//!   into `events` as a JSON array. `cost_micro_usd` = tokens × pricing, rounded down.
//!
//! Every exported span becomes one post. An exporter re-sending a span (start, then end)
//! produces two posts; the store merges them.

use newdb_types::{Result, Span};

/// Attribute mapping and pricing, loaded once.
pub struct Normalizer {
    _todo: (),
}

impl Normalizer {
    /// The mapping and pricing embedded from `data/`.
    pub fn builtin() -> Normalizer {
        todo!("lane 3")
    }

    /// Mapping and pricing from files, for overriding without a rebuild.
    pub fn from_files(mapping: &std::path::Path, pricing: &std::path::Path) -> Result<Normalizer> {
        let _ = (mapping, pricing);
        todo!("lane 3")
    }

    /// Body of `POST /v1/traces` with `content-type: application/x-protobuf`.
    pub fn from_protobuf(&self, body: &[u8]) -> Result<Vec<Span>> {
        let _ = body;
        todo!("lane 3")
    }

    /// Body of `POST /v1/traces` with `content-type: application/json`.
    pub fn from_json(&self, body: &[u8]) -> Result<Vec<Span>> {
        let _ = body;
        todo!("lane 3")
    }
}
