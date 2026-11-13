//! Routing, the endpoint handlers, and the JSON they write (shapes in `docs/api.md`).
//!
//! Responses are written by hand into one buffer. Payloads (`inputs`, `outputs`,
//! `attributes`, `events`) are copied in as the stored bytes, never parsed: they are the
//! JSON that `newdb-otlp` produced at ingest. 64-bit nanosecond fields are JSON strings,
//! as in OTLP/JSON, because JavaScript numbers lose precision above 2^53.

use std::collections::HashMap;
use std::io::Read as _;
use std::sync::Arc;
use std::time::Instant;

use http_body_util::{BodyExt as _, Full, LengthLimitError, Limited};
use hyper::body::{Bytes, Incoming};
use hyper::header::{ALLOW, CONTENT_ENCODING, CONTENT_LENGTH, CONTENT_TYPE, HeaderValue};
use hyper::{Method, Request, Response, StatusCode};
use newdb_otlp::Normalizer;
use newdb_otlp::json::write_str;
use newdb_types::{
    Error, ReadStats, Run, RunKey, RunQuery, RunType, SearchHit, SearchQuery, Span, SpanId, Status,
    Store, TraceId, TraceQuery,
};

use crate::query::{Params, micro};

pub(crate) struct App {
    pub store: Arc<dyn Store>,
    pub normalizer: Arc<Normalizer>,
    pub max_body_bytes: usize,
}

type Resp = Response<Full<Bytes>>;

/// Default and maximum `limit` for runs and search.
const DEFAULT_LIMIT: usize = 50;
const MAX_LIMIT: usize = 1000;

/// A failed request. `otlp` selects the OTLP error encoding (a `google.rpc.Status`, as
/// protobuf or JSON) instead of the API's `{"error": "..."}`.
struct Fail {
    status: StatusCode,
    message: String,
    otlp: Option<Encoding>,
}

fn fail(status: StatusCode, message: impl Into<String>) -> Fail {
    Fail {
        status,
        message: message.into(),
        otlp: None,
    }
}

fn bad_request(message: String) -> Fail {
    fail(StatusCode::BAD_REQUEST, message)
}

/// Store errors: bad input is the caller's fault; I/O may be temporary (503 tells OTLP
/// exporters to retry); corruption and a read-only store are not.
fn store_error(e: Error) -> Fail {
    let status = match e {
        Error::InvalidInput(_) => StatusCode::BAD_REQUEST,
        Error::Io(_) => StatusCode::SERVICE_UNAVAILABLE,
        Error::Corrupt(_) | Error::ReadOnly => StatusCode::INTERNAL_SERVER_ERROR,
    };
    fail(status, e.to_string())
}

#[derive(Clone, Copy, PartialEq)]
enum Encoding {
    Protobuf,
    Json,
}

/// `google.rpc.Status`, the body of an OTLP/HTTP error response.
#[derive(Clone, PartialEq, prost::Message)]
struct RpcStatus {
    #[prost(int32, tag = "1")]
    code: i32,
    #[prost(string, tag = "2")]
    message: String,
}

fn response(status: StatusCode, content_type: &'static str, body: Vec<u8>) -> Resp {
    let mut r = Response::new(Full::new(Bytes::from(body)));
    *r.status_mut() = status;
    r.headers_mut()
        .insert(CONTENT_TYPE, HeaderValue::from_static(content_type));
    r
}

fn json(status: StatusCode, body: Vec<u8>) -> Resp {
    response(status, "application/json", body)
}

impl Fail {
    fn into_response(self) -> Resp {
        match self.otlp {
            Some(Encoding::Protobuf) => {
                use prost::Message as _;
                let s = RpcStatus {
                    code: grpc_code(self.status),
                    message: self.message,
                };
                response(self.status, "application/x-protobuf", s.encode_to_vec())
            }
            Some(Encoding::Json) => {
                let mut b =
                    format!("{{\"code\":{},\"message\":", grpc_code(self.status)).into_bytes();
                write_str(&mut b, &self.message);
                b.push(b'}');
                json(self.status, b)
            }
            None => {
                let mut b = b"{\"error\":".to_vec();
                write_str(&mut b, &self.message);
                b.push(b'}');
                json(self.status, b)
            }
        }
    }
}

fn grpc_code(s: StatusCode) -> i32 {
    match s {
        StatusCode::BAD_REQUEST => 3,            // INVALID_ARGUMENT
        StatusCode::PAYLOAD_TOO_LARGE => 8,      // RESOURCE_EXHAUSTED
        StatusCode::SERVICE_UNAVAILABLE => 14,   // UNAVAILABLE
        StatusCode::UNSUPPORTED_MEDIA_TYPE => 3, // INVALID_ARGUMENT
        _ => 13,                                 // INTERNAL
    }
}

impl App {
    pub async fn handle(self: Arc<Self>, req: Request<Incoming>) -> Resp {
        let path = req.uri().path().to_string();
        let query = req.uri().query().map(str::to_string);
        let method = req.method().clone();
        let get = |allowed: &'static str| -> Result<(), Fail> {
            if method == Method::GET {
                Ok(())
            } else {
                Err(fail(
                    StatusCode::METHOD_NOT_ALLOWED,
                    format!("use {allowed}"),
                ))
            }
        };
        let result = match path.as_str() {
            "/healthz" => {
                get("GET").map(|()| json(StatusCode::OK, b"{\"status\":\"ok\"}".to_vec()))
            }
            "/v1/traces" if method == Method::POST => self.ingest(req).await,
            "/v1/traces" => Err(fail(StatusCode::METHOD_NOT_ALLOWED, "use POST")),
            "/v1/runs" => match get("GET") {
                Ok(()) => self.blocking(move |app| app.runs(query.as_deref())).await,
                Err(e) => Err(e),
            },
            "/v1/search" => match get("GET") {
                Ok(()) => self.blocking(move |app| app.search(query.as_deref())).await,
                Err(e) => Err(e),
            },
            p => match p.strip_prefix("/v1/traces/") {
                Some(id) if !id.contains('/') => match get("GET") {
                    Ok(()) => {
                        let id = id.to_string();
                        self.blocking(move |app| app.trace(&id, query.as_deref()))
                            .await
                    }
                    Err(e) => Err(e),
                },
                _ => Err(fail(StatusCode::NOT_FOUND, format!("no endpoint {p}"))),
            },
        };
        match result {
            Ok(r) => r,
            Err(e) => {
                let allow = (e.status == StatusCode::METHOD_NOT_ALLOWED)
                    .then(|| if path == "/v1/traces" { "POST" } else { "GET" });
                let mut r = e.into_response();
                if let Some(a) = allow {
                    r.headers_mut().insert(ALLOW, HeaderValue::from_static(a));
                }
                r
            }
        }
    }

    /// Runs `f` on the blocking pool: store calls may wait on disk, and rendering large
    /// payloads is CPU work that should not stall the I/O threads.
    async fn blocking(
        self: Arc<Self>,
        f: impl FnOnce(&App) -> Result<Resp, Fail> + Send + 'static,
    ) -> Result<Resp, Fail> {
        tokio::task::spawn_blocking(move || f(&self))
            .await
            .unwrap_or_else(|e| {
                Err(fail(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("handler failed: {e}"),
                ))
            })
    }

    // --- POST /v1/traces --------------------------------------------------------------

    async fn ingest(self: Arc<Self>, req: Request<Incoming>) -> Result<Resp, Fail> {
        let header = |name| {
            req.headers()
                .get(name)
                .and_then(|v: &HeaderValue| v.to_str().ok())
                .map(|v| {
                    v.split(';')
                        .next()
                        .unwrap_or("")
                        .trim()
                        .to_ascii_lowercase()
                })
        };
        let encoding = match header(CONTENT_TYPE).as_deref() {
            Some("application/x-protobuf" | "application/protobuf") => Encoding::Protobuf,
            Some("application/json") => Encoding::Json,
            other => {
                return Err(fail(
                    StatusCode::UNSUPPORTED_MEDIA_TYPE,
                    format!(
                        "content-type must be application/x-protobuf or application/json, got {}",
                        other.unwrap_or("none")
                    ),
                ));
            }
        };
        let otlp = move |mut f: Fail| {
            f.otlp = Some(encoding);
            f
        };
        let gzip = match header(CONTENT_ENCODING).as_deref() {
            None | Some("" | "identity") => false,
            Some("gzip" | "x-gzip") => true,
            Some(other) => {
                return Err(otlp(fail(
                    StatusCode::UNSUPPORTED_MEDIA_TYPE,
                    format!("unsupported content-encoding {other}"),
                )));
            }
        };
        let max = self.max_body_bytes;
        let too_large = move || {
            otlp(fail(
                StatusCode::PAYLOAD_TOO_LARGE,
                format!("request body over {max} bytes"),
            ))
        };
        if header(CONTENT_LENGTH)
            .and_then(|n| n.parse::<u64>().ok())
            .is_some_and(|n| n > max as u64)
        {
            return Err(too_large());
        }
        let body = match Limited::new(req.into_body(), max).collect().await {
            Ok(b) => b.to_bytes(),
            Err(e) if e.downcast_ref::<LengthLimitError>().is_some() => return Err(too_large()),
            Err(e) => return Err(otlp(bad_request(format!("reading the body: {e}")))),
        };
        self.blocking(move |app| {
            let unzipped;
            let raw: &[u8] = if gzip {
                let mut out = Vec::new();
                flate2::read::MultiGzDecoder::new(&body[..])
                    .take(max as u64 + 1)
                    .read_to_end(&mut out)
                    .map_err(|e| otlp(bad_request(format!("gzip: {e}"))))?;
                if out.len() > max {
                    return Err(too_large());
                }
                unzipped = out;
                &unzipped
            } else {
                &body
            };
            let spans = match encoding {
                Encoding::Protobuf => app.normalizer.from_protobuf(raw),
                Encoding::Json => app.normalizer.from_json(raw),
            }
            .map_err(|e| otlp(store_error(e)))?;
            if !spans.is_empty() {
                app.store.ingest(spans).map_err(|e| otlp(store_error(e)))?;
            }
            // An empty ExportTraceServiceResponse: zero bytes, or `{}`.
            Ok(match encoding {
                Encoding::Protobuf => {
                    response(StatusCode::OK, "application/x-protobuf", Vec::new())
                }
                Encoding::Json => json(StatusCode::OK, b"{}".to_vec()),
            })
        })
        .await
    }

    // --- GET /v1/traces/{trace_id} ----------------------------------------------------

    fn trace(&self, id: &str, query: Option<&str>) -> Result<Resp, Fail> {
        let p = Params::parse(query, &["project", "payloads", "stats"]).map_err(bad_request)?;
        let trace_id = TraceId::from_hex(id)
            .ok_or_else(|| bad_request(format!("trace_id must be 32 hex digits, got {id:?}")))?;
        let stats = p.bool("stats").map_err(bad_request)?.unwrap_or(false);
        let q = TraceQuery {
            project: p.str("project").unwrap_or("default").to_string(),
            trace_id,
            payloads: p.bool("payloads").map_err(bad_request)?.unwrap_or(true),
            timings: stats,
        };
        let t0 = Instant::now();
        let read = self.store.trace(&q).map_err(store_error)?;
        let store_ns = elapsed(t0);
        if read.value.is_empty() {
            return Err(fail(
                StatusCode::NOT_FOUND,
                format!("trace {id} not found in project {}", q.project),
            ));
        }
        let t1 = Instant::now();
        let size: usize =
            read.value.iter().map(payload_len).sum::<usize>() + 512 * read.value.len();
        let mut b = Vec::with_capacity(size + 256);
        b.extend_from_slice(b"{\"project\":");
        write_str(&mut b, &q.project);
        b.extend_from_slice(b",\"trace_id\":");
        write_str(&mut b, &trace_id.to_hex());
        b.extend_from_slice(b",\"spans\":");
        write_tree(&mut b, &read.value);
        finish(&mut b, stats.then_some(&read.stats), store_ns, t1);
        Ok(json(StatusCode::OK, b))
    }

    // --- GET /v1/runs -----------------------------------------------------------------

    fn runs(&self, query: Option<&str>) -> Result<Resp, Fail> {
        let p = Params::parse(
            query,
            &[
                "project",
                "status",
                "run_type",
                "model",
                "session",
                "min_latency_ms",
                "min_tokens",
                "min_cost_usd",
                "since",
                "cursor",
                "limit",
                "stats",
            ],
        )
        .map_err(bad_request)?;
        let r = |e| bad_request(e);
        let limit = p.limit(DEFAULT_LIMIT, MAX_LIMIT).map_err(r)?;
        let stats = p.bool("stats").map_err(r)?.unwrap_or(false);
        let q = RunQuery {
            project: p.str("project").map(str::to_string),
            status: p
                .parsed("status", |v| {
                    [Status::Unset, Status::Ok, Status::Error]
                        .into_iter()
                        .find(|s| s.as_str() == v)
                })
                .map_err(r)?,
            run_type: p.parsed("run_type", RunType::parse).map_err(r)?,
            model: p.str("model").map(str::to_string),
            session: p.str("session").map(str::to_string),
            min_latency_ns: p
                .parsed("min_latency_ms", |v| {
                    v.parse::<u64>().ok()?.checked_mul(1_000_000)
                })
                .map_err(r)?,
            min_total_tokens: p.u64("min_tokens").map_err(r)?,
            min_cost_micro_usd: p.parsed("min_cost_usd", micro).map_err(r)?,
            start_after_ns: p.u64("since").map_err(r)?,
            before: p.parsed("cursor", decode_cursor).map_err(r)?,
            // One extra run tells whether there is a next page.
            limit: limit + 1,
            timings: stats,
        };
        let t0 = Instant::now();
        let mut read = self.store.runs(&q).map_err(store_error)?;
        let store_ns = elapsed(t0);
        let t1 = Instant::now();
        let more = read.value.len() > limit;
        read.value.truncate(limit);
        let mut b = Vec::with_capacity(256 + 400 * read.value.len());
        b.extend_from_slice(b"{\"runs\":[");
        for (i, run) in read.value.iter().enumerate() {
            if i > 0 {
                b.push(b',');
            }
            write_run(&mut b, run);
        }
        b.extend_from_slice(b"],\"next_cursor\":");
        match read.value.last().filter(|_| more) {
            Some(last) => write_str(&mut b, &encode_cursor(&last.key())),
            None => b.extend_from_slice(b"null"),
        }
        finish(&mut b, stats.then_some(&read.stats), store_ns, t1);
        Ok(json(StatusCode::OK, b))
    }

    // --- GET /v1/search ---------------------------------------------------------------

    fn search(&self, query: Option<&str>) -> Result<Resp, Fail> {
        let p = Params::parse(query, &["project", "q", "limit", "stats"]).map_err(bad_request)?;
        let text = p.str("q").ok_or_else(|| bad_request("missing q".into()))?;
        let stats = p.bool("stats").map_err(bad_request)?.unwrap_or(false);
        let q = SearchQuery {
            project: p.str("project").map(str::to_string),
            text: text.to_string(),
            limit: p.limit(DEFAULT_LIMIT, MAX_LIMIT).map_err(bad_request)?,
            timings: stats,
        };
        let t0 = Instant::now();
        let read = self.store.search(&q).map_err(store_error)?;
        let store_ns = elapsed(t0);
        let t1 = Instant::now();
        let mut b = Vec::with_capacity(256 + 160 * read.value.len());
        b.extend_from_slice(b"{\"hits\":[");
        for (i, h) in read.value.iter().enumerate() {
            if i > 0 {
                b.push(b',');
            }
            write_hit(&mut b, h);
        }
        b.push(b']');
        finish(&mut b, stats.then_some(&read.stats), store_ns, t1);
        Ok(json(StatusCode::OK, b))
    }
}

fn elapsed(t: Instant) -> u64 {
    u64::try_from(t.elapsed().as_nanos()).unwrap_or(u64::MAX)
}

fn payload_len(s: &Span) -> usize {
    [&s.inputs, &s.outputs, &s.attributes, &s.events]
        .into_iter()
        .flatten()
        .map(Vec::len)
        .sum()
}

/// Cursor: `start_ns` (big-endian), `trace_id`, `span_id` as 64 hex digits. Clients treat
/// it as opaque.
fn encode_cursor(k: &RunKey) -> String {
    let mut s = String::with_capacity(64);
    for b in k.start_ns.to_be_bytes() {
        s.push_str(&format!("{b:02x}"));
    }
    s + &k.trace_id.to_hex() + &k.span_id.to_hex()
}

fn decode_cursor(s: &str) -> Option<RunKey> {
    if s.len() != 64 || !s.is_ascii() {
        return None;
    }
    Some(RunKey {
        start_ns: u64::from_str_radix(&s[..16], 16).ok()?,
        trace_id: TraceId::from_hex(&s[16..48])?,
        span_id: SpanId::from_hex(&s[48..])?,
    })
}

// --- JSON writing ---------------------------------------------------------------------

fn key(b: &mut Vec<u8>, k: &str) {
    b.push(b',');
    write_str(b, k);
    b.push(b':');
}

fn num(b: &mut Vec<u8>, n: u64) {
    b.extend_from_slice(n.to_string().as_bytes());
}

/// A 64-bit nanosecond value, as a JSON string.
fn ns(b: &mut Vec<u8>, n: u64) {
    b.push(b'"');
    num(b, n);
    b.push(b'"');
}

fn opt<T>(b: &mut Vec<u8>, v: Option<T>, f: impl FnOnce(&mut Vec<u8>, T)) {
    match v {
        Some(v) => f(b, v),
        None => b.extend_from_slice(b"null"),
    }
}

fn opt_str(b: &mut Vec<u8>, v: Option<&str>) {
    opt(b, v, write_str);
}

/// Splices stored payload bytes in as they are.
fn raw(b: &mut Vec<u8>, v: Option<&[u8]>) {
    opt(b, v, |b, v| b.extend_from_slice(v));
}

/// Ends the response object: the stats when asked for, with the server's own phases
/// (`server.store`, `server.render`) after the store's.
fn finish(b: &mut Vec<u8>, stats: Option<&ReadStats>, store_ns: u64, render_start: Instant) {
    if let Some(s) = stats {
        b.extend_from_slice(b",\"stats\":{\"partitions_touched\":");
        num(b, s.partitions_touched);
        key(b, "bytes_read");
        num(b, s.bytes_read);
        key(b, "payload_bytes_decompressed");
        num(b, s.payload_bytes_decompressed);
        key(b, "records_visited");
        num(b, s.records_visited);
        key(b, "records_returned");
        num(b, s.records_returned);
        key(b, "allocations");
        num(b, s.allocations);
        key(b, "phases_ns");
        b.push(b'[');
        let server = [
            ("server.store", store_ns),
            ("server.render", elapsed(render_start)),
        ];
        for (i, (name, t)) in s.phases_ns.iter().copied().chain(server).enumerate() {
            if i > 0 {
                b.push(b',');
            }
            b.extend_from_slice(b"{\"phase\":");
            write_str(b, name);
            b.extend_from_slice(b",\"ns\":");
            num(b, t);
            b.push(b'}');
        }
        b.extend_from_slice(b"]}");
    }
    b.push(b'}');
}

fn write_run(b: &mut Vec<u8>, r: &Run) {
    b.extend_from_slice(b"{\"project\":");
    write_str(b, &r.project);
    key(b, "trace_id");
    write_str(b, &r.trace_id.to_hex());
    key(b, "span_id");
    write_str(b, &r.span_id.to_hex());
    key(b, "name");
    opt_str(b, r.name.as_deref());
    key(b, "start_ns");
    ns(b, r.start_ns);
    key(b, "end_ns");
    opt(b, r.end_ns, ns);
    key(b, "latency_ns");
    opt(b, r.latency_ns(), ns);
    key(b, "status");
    write_str(b, r.status.as_str());
    key(b, "run_type");
    write_str(b, r.run_type.as_str());
    key(b, "model");
    opt_str(b, r.model.as_deref());
    key(b, "session");
    opt_str(b, r.session.as_deref());
    key(b, "prompt_tokens");
    num(b, r.prompt_tokens.into());
    key(b, "completion_tokens");
    num(b, r.completion_tokens.into());
    key(b, "total_tokens");
    num(b, r.total_tokens());
    key(b, "cost_micro_usd");
    num(b, r.cost_micro_usd);
    b.push(b'}');
}

fn write_hit(b: &mut Vec<u8>, h: &SearchHit) {
    b.extend_from_slice(b"{\"project\":");
    write_str(b, &h.project);
    key(b, "trace_id");
    write_str(b, &h.trace_id.to_hex());
    key(b, "span_id");
    write_str(b, &h.span_id.to_hex());
    key(b, "start_ns");
    ns(b, h.start_ns);
    b.push(b'}');
}

/// Writes a span's fields and opens its `children` array; [`write_tree`] closes it.
fn open_span(b: &mut Vec<u8>, s: &Span) {
    b.extend_from_slice(b"{\"span_id\":");
    write_str(b, &s.span_id.to_hex());
    key(b, "parent_span_id");
    opt(b, s.parent_span_id, |b, p| write_str(b, &p.to_hex()));
    key(b, "name");
    opt_str(b, s.name.as_deref());
    key(b, "start_ns");
    opt(b, s.start_ns, ns);
    key(b, "end_ns");
    opt(b, s.end_ns, ns);
    key(b, "status");
    opt_str(b, s.status.map(Status::as_str));
    key(b, "run_type");
    opt_str(b, s.run_type.map(RunType::as_str));
    key(b, "model");
    opt_str(b, s.model.as_deref());
    key(b, "provider");
    opt_str(b, s.provider.as_deref());
    key(b, "session");
    opt_str(b, s.session.as_deref());
    key(b, "prompt_tokens");
    opt(b, s.prompt_tokens, |b, n| num(b, n.into()));
    key(b, "completion_tokens");
    opt(b, s.completion_tokens, |b, n| num(b, n.into()));
    key(b, "cost_micro_usd");
    opt(b, s.cost_micro_usd, num);
    key(b, "error");
    opt_str(b, s.error.as_deref());
    key(b, "version");
    num(b, s.version);
    key(b, "inputs");
    raw(b, s.inputs.as_deref());
    key(b, "outputs");
    raw(b, s.outputs.as_deref());
    key(b, "attributes");
    raw(b, s.attributes.as_deref());
    key(b, "events");
    raw(b, s.events.as_deref());
    b.extend_from_slice(b",\"children\":[");
}

/// Writes `spans` (in the store's order) as a forest built from `parent_span_id`.
/// Top level: roots, spans whose parent is not in the trace, and spans that are their own
/// parent; children keep the store's order. Spans in a parent cycle are cut at the first
/// one in store order, which goes to the top level, so every span appears exactly once.
/// Iterative, so a very deep trace cannot overflow the stack.
fn write_tree(b: &mut Vec<u8>, spans: &[Span]) {
    let index: HashMap<SpanId, usize> = spans
        .iter()
        .enumerate()
        .map(|(i, s)| (s.span_id, i))
        .collect();
    let mut children: Vec<Vec<usize>> = vec![Vec::new(); spans.len()];
    let mut tops = Vec::new();
    for (i, s) in spans.iter().enumerate() {
        match s.parent_span_id.and_then(|p| index.get(&p)) {
            Some(&p) if p != i => children[p].push(i),
            _ => tops.push(i),
        }
    }
    let mut seen = vec![false; spans.len()];
    // (span, next child to look at, whether a child has been written)
    let mut stack: Vec<(usize, usize, bool)> = Vec::new();
    b.push(b'[');
    let mut first = true;
    for root in tops.into_iter().chain(0..spans.len()) {
        if seen[root] {
            continue;
        }
        if !first {
            b.push(b',');
        }
        first = false;
        seen[root] = true;
        open_span(b, &spans[root]);
        stack.push((root, 0, false));
        while let Some((node, next, wrote)) = stack.last_mut() {
            let kids = &children[*node];
            while *next < kids.len() && seen[kids[*next]] {
                *next += 1;
            }
            if let Some(&c) = kids.get(*next) {
                if *wrote {
                    b.push(b',');
                }
                *wrote = true;
                *next += 1;
                seen[c] = true;
                open_span(b, &spans[c]);
                stack.push((c, 0, false));
            } else {
                b.extend_from_slice(b"]}");
                stack.pop();
            }
        }
    }
    b.push(b']');
}

#[cfg(test)]
mod tests {
    use super::*;
    use newdb_otlp::json::Json;

    fn span(id: u8, parent: Option<u8>) -> Span {
        Span {
            span_id: SpanId([id; 8]),
            parent_span_id: parent.map(|p| SpanId([p; 8])),
            ..Span::default()
        }
    }

    /// The forest as `id(children...)`.
    fn shape(spans: &[Span]) -> String {
        let mut b = Vec::new();
        write_tree(&mut b, spans);
        fn walk(v: &Json, out: &mut String) {
            for (i, n) in v.as_array().unwrap().iter().enumerate() {
                if i > 0 {
                    out.push(' ');
                }
                out.push_str(&n.get("span_id").unwrap().as_str().unwrap()[..2]);
                let kids = n.get("children").unwrap();
                if !kids.as_array().unwrap().is_empty() {
                    out.push('(');
                    walk(kids, out);
                    out.push(')');
                }
            }
        }
        let mut out = String::new();
        walk(&Json::parse(&b).unwrap(), &mut out);
        out
    }

    #[test]
    fn tree_shapes() {
        // Store order is the sibling order; a child listed before its parent still nests.
        let s = [
            span(3, Some(1)),
            span(1, None),
            span(2, Some(1)),
            span(4, Some(2)),
            span(5, Some(9)),
        ];
        assert_eq!(shape(&s), "01(03 02(04)) 05");
        // Self-parent and a two-span cycle: each span exactly once.
        let s = [
            span(1, Some(1)),
            span(2, Some(3)),
            span(3, Some(2)),
            span(4, Some(3)),
        ];
        assert_eq!(shape(&s), "01 02(03(04))");
        assert_eq!(shape(&[]), "");
        // Deep chains do not recurse.
        let deep: Vec<Span> = (0..100_000u32)
            .map(|i| Span {
                span_id: SpanId((i as u64 + 1).to_be_bytes()),
                parent_span_id: (i > 0).then(|| SpanId((i as u64).to_be_bytes())),
                ..Span::default()
            })
            .collect();
        let mut b = Vec::new();
        write_tree(&mut b, &deep);
        assert!(b.ends_with(&("]}".repeat(100_000) + "]").into_bytes()));
        let text = String::from_utf8(b).unwrap();
        assert_eq!(text.matches("{\"span_id\":").count(), 100_000);
    }

    #[test]
    fn cursor_round_trip() {
        let k = RunKey {
            start_ns: 1_761_148_800_000_000_001,
            trace_id: TraceId([0xab; 16]),
            span_id: SpanId([0x01; 8]),
        };
        assert_eq!(decode_cursor(&encode_cursor(&k)), Some(k));
        assert_eq!(decode_cursor("zz"), None);
        assert_eq!(decode_cursor(&"é".repeat(32)), None);
    }
}
