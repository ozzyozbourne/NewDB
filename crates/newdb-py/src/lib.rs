//! Python module `newdb` (DESIGN.md section 8.2), built with maturin.
//!
//! ```python
//! import newdb
//! db = newdb.Db.open("./data")          # newdb_engine::Db
//! mem = newdb.Db.memory()               # newdb_testkit::Oracle, for tests before the engine lands
//! db.ingest([{"project": "p", "trace_id": "<32 hex>", "span_id": "<16 hex>", "name": "agent",
//!             "start_ns": 1, "inputs": b'{"q": "refund"}'}])
//! spans = db.trace("p", "<32 hex>", payloads=True)      # list of dicts; payloads as bytes
//! runs = db.runs(project="p", status="error", limit=50)  # list of dicts, newest first
//! hits = db.search("refund policy", project="p", limit=50)
//! ```
//! Every query method also accepts `stats=True` and then returns `(value, stats_dict)`.
//! The tests in `python/tests/` export spans with the real OpenTelemetry SDK to
//! `newdb serve` and check them through this module.

use pyo3::prelude::*;

#[pymodule]
fn newdb(_m: &Bound<'_, PyModule>) -> PyResult<()> {
    todo!("lane 4")
}
