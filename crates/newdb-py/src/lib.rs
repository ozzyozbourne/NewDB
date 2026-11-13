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
//!
//! Span dicts use the field names of [`newdb_types::Span`]. On ingest, `project`,
//! `trace_id` and `span_id` are required, every other key is optional (absent or `None`
//! means "not sent in this post"), and unknown keys are an error. IDs are hex strings,
//! `status` and `run_type` their lowercase names, payloads `bytes` (or `str`, stored as
//! UTF-8). Spans read back carry every key, with `None` for unset fields. Run dicts carry
//! the fields of [`newdb_types::Run`] plus `latency_ns`; pass one as `before=` to get the
//! next page. The store call itself runs without the GIL.

use std::path::PathBuf;
use std::sync::Arc;

use newdb_engine::{Config, Db as EngineDb};
use newdb_testkit::Oracle;
use newdb_types::{
    Error, Read, ReadStats, Run, RunKey, RunQuery, RunType, SearchHit, SearchQuery, Span, SpanId,
    Status, Store, TraceId, TraceQuery,
};
use pyo3::exceptions::{PyOSError, PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyString};

/// Span dict keys accepted on ingest, in [`Span`] field order.
const SPAN_KEYS: &[&str] = &[
    "project",
    "trace_id",
    "span_id",
    "parent_span_id",
    "name",
    "start_ns",
    "end_ns",
    "status",
    "run_type",
    "model",
    "provider",
    "session",
    "prompt_tokens",
    "completion_tokens",
    "cost_micro_usd",
    "inputs",
    "outputs",
    "attributes",
    "events",
    "error",
    "version",
];

/// A NewDB store: the engine on a directory, or the in-memory oracle.
#[pyclass(frozen, module = "newdb")]
struct Db {
    store: Arc<dyn Store>,
}

#[pymethods]
impl Db {
    /// Opens or creates the database directory at `path`. `read_only=True` opens without
    /// the writer, alongside a running `newdb serve` on the same directory.
    #[staticmethod]
    #[pyo3(signature = (path, *, fsync = true, read_only = false))]
    fn open(py: Python<'_>, path: PathBuf, fsync: bool, read_only: bool) -> PyResult<Db> {
        let config = Config {
            fsync,
            read_only,
            ..Config::default()
        };
        let db = py.detach(|| EngineDb::open(path, config)).map_err(to_py)?;
        Ok(Db {
            store: Arc::new(db),
        })
    }

    /// An in-memory reference store (the oracle). Nothing is written to disk.
    #[staticmethod]
    fn memory() -> Db {
        Db {
            store: Arc::new(Oracle::new()),
        }
    }

    /// Ingests an iterable of span dicts, in order. Returns once they are durable.
    fn ingest(&self, py: Python<'_>, spans: &Bound<'_, PyAny>) -> PyResult<()> {
        let mut posts = Vec::new();
        for (i, item) in spans.try_iter()?.enumerate() {
            let item = item?;
            let dict = item
                .cast::<PyDict>()
                .map_err(|_| PyValueError::new_err(format!("span {i} is not a dict")))?;
            posts.push(span_from_dict(dict).map_err(|e| prefix(py, e, &format!("span {i}: ")))?);
        }
        let store = &self.store;
        py.detach(|| store.ingest(posts)).map_err(to_py)
    }

    /// Every span of the trace, merged, sorted by `(start_ns, span_id)`.
    #[pyo3(signature = (project, trace_id, *, payloads = true, stats = false))]
    fn trace(
        &self,
        py: Python<'_>,
        project: String,
        trace_id: &str,
        payloads: bool,
        stats: bool,
    ) -> PyResult<Py<PyAny>> {
        let q = TraceQuery {
            project,
            trace_id: parse_id(trace_id, "trace_id", TraceId::from_hex)?,
            payloads,
            timings: stats,
        };
        let store = &self.store;
        let read = py.detach(|| store.trace(&q)).map_err(to_py)?;
        answer(py, read, stats, span_to_dict)
    }

    /// Runs (root spans) passing every given filter, newest first.
    #[pyo3(signature = (
        *, project = None, status = None, run_type = None, model = None, session = None,
        min_latency_ns = None, min_total_tokens = None, min_cost_micro_usd = None,
        start_after_ns = None, before = None, limit = 50, stats = false
    ))]
    #[allow(clippy::too_many_arguments)]
    fn runs(
        &self,
        py: Python<'_>,
        project: Option<String>,
        status: Option<&str>,
        run_type: Option<&str>,
        model: Option<String>,
        session: Option<String>,
        min_latency_ns: Option<u64>,
        min_total_tokens: Option<u64>,
        min_cost_micro_usd: Option<u64>,
        start_after_ns: Option<u64>,
        before: Option<&Bound<'_, PyDict>>,
        limit: usize,
        stats: bool,
    ) -> PyResult<Py<PyAny>> {
        let q = RunQuery {
            project,
            status: status.map(parse_status).transpose()?,
            run_type: run_type.map(parse_run_type).transpose()?,
            model,
            session,
            min_latency_ns,
            min_total_tokens,
            min_cost_micro_usd,
            start_after_ns,
            before: before.map(run_key).transpose()?,
            limit,
            timings: stats,
        };
        let store = &self.store;
        let read = py.detach(|| store.runs(&q)).map_err(to_py)?;
        answer(py, read, stats, run_to_dict)
    }

    /// Spans whose inputs or outputs contain every token of `text`, newest first.
    #[pyo3(signature = (text, *, project = None, limit = 50, stats = false))]
    fn search(
        &self,
        py: Python<'_>,
        text: String,
        project: Option<String>,
        limit: usize,
        stats: bool,
    ) -> PyResult<Py<PyAny>> {
        let q = SearchQuery {
            project,
            text,
            limit,
            timings: stats,
        };
        let store = &self.store;
        let read = py.detach(|| store.search(&q)).map_err(to_py)?;
        answer(py, read, stats, hit_to_dict)
    }
}

fn to_py(e: Error) -> PyErr {
    match e {
        Error::InvalidInput(m) => PyValueError::new_err(m),
        Error::Io(e) => PyOSError::new_err(e.to_string()),
        e => PyRuntimeError::new_err(e.to_string()),
    }
}

fn prefix(py: Python<'_>, e: PyErr, p: &str) -> PyErr {
    PyValueError::new_err(format!("{p}{}", e.value(py)))
}

fn parse_id<T>(s: &str, what: &str, f: fn(&str) -> Option<T>) -> PyResult<T> {
    f(s).ok_or_else(|| {
        PyValueError::new_err(format!("{what} {s:?} is not valid hex of the right length"))
    })
}

fn parse_status(s: &str) -> PyResult<Status> {
    [Status::Unset, Status::Ok, Status::Error]
        .into_iter()
        .find(|v| v.as_str() == s)
        .ok_or_else(|| {
            PyValueError::new_err(format!("status must be unset, ok or error, not {s:?}"))
        })
}

fn parse_run_type(s: &str) -> PyResult<RunType> {
    RunType::parse(s).ok_or_else(|| PyValueError::new_err(format!("unknown run_type {s:?}")))
}

fn required<'py>(d: &Bound<'py, PyDict>, key: &str) -> PyResult<Bound<'py, PyAny>> {
    d.get_item(key)?
        .filter(|v| !v.is_none())
        .ok_or_else(|| PyValueError::new_err(format!("{key} is required")))
}

fn run_key(d: &Bound<'_, PyDict>) -> PyResult<RunKey> {
    let field = |k: &str| required(d, k).map_err(|e| prefix(d.py(), e, "before: "));
    Ok(RunKey {
        start_ns: field("start_ns")?.extract()?,
        trace_id: parse_id(
            &field("trace_id")?.extract::<String>()?,
            "trace_id",
            TraceId::from_hex,
        )?,
        span_id: parse_id(
            &field("span_id")?.extract::<String>()?,
            "span_id",
            SpanId::from_hex,
        )?,
    })
}

fn span_from_dict(d: &Bound<'_, PyDict>) -> PyResult<Span> {
    let mut s = Span::default();
    for (k, v) in d.iter() {
        let key: String = k
            .extract()
            .map_err(|_| PyValueError::new_err("keys must be strings"))?;
        let err = |e: PyErr| prefix(d.py(), e, &format!("{key}: "));
        let v = (!v.is_none()).then_some(v);
        let text = || -> PyResult<Option<String>> { v.as_ref().map(|v| v.extract()).transpose() };
        let payload = || -> PyResult<Option<Vec<u8>>> {
            v.as_ref()
                .map(|v| match v.cast::<PyString>() {
                    Ok(s) => Ok(s.extract::<String>()?.into_bytes()),
                    Err(_) => Ok(v.cast::<PyBytes>()?.as_bytes().to_vec()),
                })
                .transpose()
        };
        macro_rules! num {
            () => {
                v.as_ref().map(|v| v.extract()).transpose().map_err(err)?
            };
        }
        match key.as_str() {
            "project" => s.project = text().map_err(err)?.unwrap_or_default(),
            "trace_id" => {
                if let Some(t) = text().map_err(err)? {
                    s.trace_id = parse_id(&t, "trace_id", TraceId::from_hex)?;
                }
            }
            "span_id" => {
                if let Some(t) = text().map_err(err)? {
                    s.span_id = parse_id(&t, "span_id", SpanId::from_hex)?;
                }
            }
            "parent_span_id" => {
                s.parent_span_id = text()
                    .map_err(err)?
                    .map(|t| parse_id(&t, "parent_span_id", SpanId::from_hex))
                    .transpose()?
                    // An all-zero parent means "root", as in OTLP.
                    .filter(|p| !p.is_zero());
            }
            "name" => s.name = text().map_err(err)?,
            "start_ns" => s.start_ns = num!(),
            "end_ns" => s.end_ns = num!(),
            "status" => {
                s.status = text()
                    .map_err(err)?
                    .as_deref()
                    .map(parse_status)
                    .transpose()?
            }
            "run_type" => {
                s.run_type = text()
                    .map_err(err)?
                    .as_deref()
                    .map(parse_run_type)
                    .transpose()?
            }
            "model" => s.model = text().map_err(err)?,
            "provider" => s.provider = text().map_err(err)?,
            "session" => s.session = text().map_err(err)?,
            "prompt_tokens" => s.prompt_tokens = num!(),
            "completion_tokens" => s.completion_tokens = num!(),
            "cost_micro_usd" => s.cost_micro_usd = num!(),
            "inputs" => s.inputs = payload().map_err(err)?,
            "outputs" => s.outputs = payload().map_err(err)?,
            "attributes" => s.attributes = payload().map_err(err)?,
            "events" => s.events = payload().map_err(err)?,
            "error" => s.error = text().map_err(err)?,
            // Assigned by the store; accepted so a span read back can be posted again.
            "version" => {}
            _ => {
                return Err(PyValueError::new_err(format!(
                    "unknown key {key:?}; expected one of {}",
                    SPAN_KEYS.join(", ")
                )));
            }
        }
    }
    for key in ["project", "trace_id", "span_id"] {
        required(d, key)?;
    }
    Ok(s)
}

fn answer<T>(
    py: Python<'_>,
    read: Read<Vec<T>>,
    stats: bool,
    to_dict: for<'a> fn(Python<'a>, &T) -> PyResult<Bound<'a, PyDict>>,
) -> PyResult<Py<PyAny>> {
    let items: Vec<_> = read
        .value
        .iter()
        .map(|x| to_dict(py, x))
        .collect::<PyResult<_>>()?;
    let list = items.into_pyobject(py)?.into_any();
    if !stats {
        return Ok(list.unbind());
    }
    let s = stats_to_dict(py, &read.stats)?;
    Ok((list, s).into_pyobject(py)?.into_any().unbind())
}

fn stats_to_dict<'py>(py: Python<'py>, s: &ReadStats) -> PyResult<Bound<'py, PyDict>> {
    let d = PyDict::new(py);
    d.set_item("partitions_touched", s.partitions_touched)?;
    d.set_item("bytes_read", s.bytes_read)?;
    d.set_item("payload_bytes_decompressed", s.payload_bytes_decompressed)?;
    d.set_item("records_visited", s.records_visited)?;
    d.set_item("records_returned", s.records_returned)?;
    d.set_item("phases_ns", s.phases_ns.clone())?;
    d.set_item("allocations", s.allocations)?;
    Ok(d)
}

fn bytes<'py>(py: Python<'py>, b: &Option<Vec<u8>>) -> Option<Bound<'py, PyBytes>> {
    b.as_deref().map(|b| PyBytes::new(py, b))
}

fn span_to_dict<'py>(py: Python<'py>, s: &Span) -> PyResult<Bound<'py, PyDict>> {
    let d = PyDict::new(py);
    d.set_item("project", &s.project)?;
    d.set_item("trace_id", s.trace_id.to_hex())?;
    d.set_item("span_id", s.span_id.to_hex())?;
    d.set_item("parent_span_id", s.parent_span_id.map(|p| p.to_hex()))?;
    d.set_item("name", &s.name)?;
    d.set_item("start_ns", s.start_ns)?;
    d.set_item("end_ns", s.end_ns)?;
    d.set_item("status", s.status.map(Status::as_str))?;
    d.set_item("run_type", s.run_type.map(RunType::as_str))?;
    d.set_item("model", &s.model)?;
    d.set_item("provider", &s.provider)?;
    d.set_item("session", &s.session)?;
    d.set_item("prompt_tokens", s.prompt_tokens)?;
    d.set_item("completion_tokens", s.completion_tokens)?;
    d.set_item("cost_micro_usd", s.cost_micro_usd)?;
    d.set_item("inputs", bytes(py, &s.inputs))?;
    d.set_item("outputs", bytes(py, &s.outputs))?;
    d.set_item("attributes", bytes(py, &s.attributes))?;
    d.set_item("events", bytes(py, &s.events))?;
    d.set_item("error", &s.error)?;
    d.set_item("version", s.version)?;
    Ok(d)
}

fn run_to_dict<'py>(py: Python<'py>, r: &Run) -> PyResult<Bound<'py, PyDict>> {
    let d = PyDict::new(py);
    d.set_item("project", &r.project)?;
    d.set_item("trace_id", r.trace_id.to_hex())?;
    d.set_item("span_id", r.span_id.to_hex())?;
    d.set_item("name", &r.name)?;
    d.set_item("start_ns", r.start_ns)?;
    d.set_item("end_ns", r.end_ns)?;
    d.set_item("latency_ns", r.latency_ns())?;
    d.set_item("status", r.status.as_str())?;
    d.set_item("run_type", r.run_type.as_str())?;
    d.set_item("model", &r.model)?;
    d.set_item("session", &r.session)?;
    d.set_item("prompt_tokens", r.prompt_tokens)?;
    d.set_item("completion_tokens", r.completion_tokens)?;
    d.set_item("cost_micro_usd", r.cost_micro_usd)?;
    Ok(d)
}

fn hit_to_dict<'py>(py: Python<'py>, h: &SearchHit) -> PyResult<Bound<'py, PyDict>> {
    let d = PyDict::new(py);
    d.set_item("project", &h.project)?;
    d.set_item("trace_id", h.trace_id.to_hex())?;
    d.set_item("span_id", h.span_id.to_hex())?;
    d.set_item("start_ns", h.start_ns)?;
    Ok(d)
}

#[pymodule]
fn newdb(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<Db>()?;
    Ok(())
}
