//! HTTP server over any [`Store`] (DESIGN.md section 8.3). Tested against
//! `newdb_testkit::Oracle`; the CLI runs it over `newdb_engine::Db`.
//!
//! Endpoints (lane 3 fixes the JSON shapes and documents them in `docs/api.md`):
//! - `POST /v1/traces`: OTLP/HTTP, protobuf or JSON by `content-type`, gzip accepted.
//!   Responds with an empty `ExportTraceServiceResponse` once the store acknowledges.
//! - `GET /v1/traces/{trace_id}?project=&payloads=true|false&stats=1`: the trace's spans
//!   as a nested tree built from `parent_span_id` (orphans at the top level). Payload
//!   bytes are spliced into the response as raw JSON, never parsed.
//! - `GET /v1/runs?project=&status=&run_type=&model=&session=&min_latency_ms=&min_tokens=&min_cost_usd=&since=&cursor=&limit=&stats=1`:
//!   newest first; `cursor` is the opaque key of the last run of the previous page.
//! - `GET /v1/search?project=&q=&limit=&stats=1`.
//! - `GET /healthz`.
//!
//! With `stats=1`, the response includes the `ReadStats` (and asks for timings).
//! Request bodies are capped at `ServerConfig::max_body_bytes`.

use std::net::SocketAddr;
use std::sync::Arc;

use newdb_otlp::Normalizer;
use newdb_types::{Result, Store};

pub struct ServerConfig {
    /// Use port 0 in tests to get a free port; read it from `ServerHandle::addr`.
    pub addr: SocketAddr,
    pub max_body_bytes: usize,
    pub normalizer: Arc<Normalizer>,
}

/// A running server. Dropping it shuts the server down.
pub struct ServerHandle {
    _todo: (),
}

impl ServerHandle {
    pub fn addr(&self) -> SocketAddr {
        todo!("lane 3")
    }

    /// Stops accepting, finishes in-flight requests, and returns.
    pub fn shutdown(self) {
        todo!("lane 3")
    }

    /// Blocks until the server stops (for `newdb serve`).
    pub fn wait(self) {
        todo!("lane 3")
    }
}

/// Starts the server on its own tokio runtime and returns once it is listening.
pub fn start(store: Arc<dyn Store>, config: ServerConfig) -> Result<ServerHandle> {
    let _ = (store, config);
    todo!("lane 3")
}
