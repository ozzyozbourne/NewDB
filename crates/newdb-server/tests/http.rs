//! End to end over real HTTP: OTLP requests encoded here (protobuf, and gzipped JSON) are
//! posted to a server on port 0 over `newdb_testkit::Oracle`, then the trace, runs and
//! search endpoints are read back. The client is a plain `TcpStream`, so the test checks
//! the bytes on the wire.

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::sync::Arc;

use newdb_otlp::Normalizer;
use newdb_otlp::json::Json;
use newdb_otlp::proto::collector::trace::v1::ExportTraceServiceRequest;
use newdb_otlp::proto::common::v1::{AnyValue, KeyValue, any_value};
use newdb_otlp::proto::resource::v1::Resource;
use newdb_otlp::proto::trace::v1::{ResourceSpans, ScopeSpans, Span, Status, status};
use newdb_server::{ServerConfig, ServerHandle, start};
use newdb_testkit::Oracle;
use newdb_types::{Store as _, TraceId, TraceQuery};
use prost::Message as _;

const A: [u8; 16] = [0xa1; 16];
const B: [u8; 16] = [0xb2; 16];
const C: [u8; 16] = [0xc3; 16];

fn server(max_body_bytes: usize) -> ServerHandle {
    start(
        Arc::new(Oracle::new()),
        ServerConfig {
            addr: "127.0.0.1:0".parse().unwrap(),
            max_body_bytes,
            normalizer: Arc::new(Normalizer::builtin()),
        },
    )
    .unwrap()
}

struct Reply {
    status: u16,
    headers: String,
    body: Vec<u8>,
}

impl Reply {
    fn json(&self) -> Json {
        Json::parse(&self.body)
            .unwrap_or_else(|e| panic!("{e}: {}", String::from_utf8_lossy(&self.body)))
    }

    fn header(&self, name: &str) -> Option<&str> {
        self.headers.lines().find_map(|l| {
            let (k, v) = l.split_once(':')?;
            k.eq_ignore_ascii_case(name).then(|| v.trim())
        })
    }
}

fn http(
    addr: SocketAddr,
    method: &str,
    path: &str,
    headers: &[(&str, &str)],
    body: &[u8],
) -> Reply {
    let mut s = TcpStream::connect(addr).unwrap();
    let mut req = format!(
        "{method} {path} HTTP/1.1\r\nhost: test\r\nconnection: close\r\ncontent-length: {}\r\n",
        body.len()
    );
    for (k, v) in headers {
        req.push_str(&format!("{k}: {v}\r\n"));
    }
    req.push_str("\r\n");
    s.write_all(req.as_bytes()).unwrap();
    s.write_all(body).unwrap();
    let mut out = Vec::new();
    s.read_to_end(&mut out).unwrap();
    let split = out
        .windows(4)
        .position(|w| w == b"\r\n\r\n")
        .expect("a response head");
    let head = String::from_utf8(out[..split].to_vec()).unwrap();
    let status = head[9..12].parse().unwrap();
    Reply {
        status,
        headers: head,
        body: out[split + 4..].to_vec(),
    }
}

fn get(addr: SocketAddr, path: &str) -> Reply {
    http(addr, "GET", path, &[], b"")
}

fn post_protobuf(addr: SocketAddr, body: &[u8]) -> Reply {
    http(
        addr,
        "POST",
        "/v1/traces",
        &[("content-type", "application/x-protobuf")],
        body,
    )
}

fn kv(key: &str, v: any_value::Value) -> KeyValue {
    KeyValue {
        key: key.into(),
        value: Some(AnyValue { value: Some(v) }),
        ..KeyValue::default()
    }
}

fn s(v: &str) -> any_value::Value {
    any_value::Value::StringValue(v.into())
}

fn int(v: i64) -> any_value::Value {
    any_value::Value::IntValue(v)
}

fn span(trace: [u8; 16], id: u8, parent: Option<u8>, name: &str, start: u64, end: u64) -> Span {
    Span {
        trace_id: trace.to_vec(),
        span_id: vec![id; 8],
        parent_span_id: parent.map(|p| vec![p; 8]).unwrap_or_default(),
        name: name.into(),
        start_time_unix_nano: start,
        end_time_unix_nano: end,
        ..Span::default()
    }
}

fn request(spans: Vec<Span>) -> Vec<u8> {
    ExportTraceServiceRequest {
        resource_spans: vec![ResourceSpans {
            resource: Some(Resource {
                attributes: vec![kv("newdb.project", s("e2e")), kv("service.name", s("svc"))],
                ..Resource::default()
            }),
            scope_spans: vec![ScopeSpans {
                spans,
                ..ScopeSpans::default()
            }],
            ..ResourceSpans::default()
        }],
    }
    .encode_to_vec()
}

/// Trace A: an agent root posted open and later closed, a chat child, a tool grandchild and
/// an orphan. Trace B: an errored chain root. Both as one protobuf request, then the close.
fn first_requests() -> (Vec<u8>, Vec<u8>) {
    let mut root = span(A, 1, None, "invoke_agent planner", 1000, 0);
    root.attributes = vec![kv("gen_ai.operation.name", s("invoke_agent"))];
    let mut chat = span(A, 2, Some(1), "chat gpt-4o", 1100, 1900);
    chat.attributes = vec![
        kv("gen_ai.operation.name", s("chat")),
        kv("gen_ai.request.model", s("gpt-4o")),
        kv("gen_ai.usage.input_tokens", int(1000)),
        kv("gen_ai.usage.output_tokens", int(200)),
        kv(
            "gen_ai.input.messages",
            s(r#"[{"role":"user","content":"Refund order 1234, \"now\"\n"}]"#),
        ),
    ];
    let mut tool = span(A, 3, Some(2), "execute_tool refund", 1200, 1300);
    tool.attributes = vec![
        kv("gen_ai.operation.name", s("execute_tool")),
        kv("gen_ai.tool.call.arguments", s(r#"{"order":"1234"}"#)),
    ];
    let orphan = span(A, 4, Some(0x99), "late", 1300, 1400);
    let mut b_root = span(B, 5, None, "RetrievalQA", 2000, 2500);
    b_root.attributes = vec![
        kv("openinference.span.kind", s("CHAIN")),
        kv("session.id", s("sess-b")),
    ];
    b_root.status = Some(Status {
        code: status::StatusCode::Error as i32,
        message: "boom".into(),
    });
    let first = request(vec![root, chat, tool, orphan, b_root]);

    // The root's end: no name and no attributes, so the merge keeps the first post's.
    let mut close = span(A, 1, None, "", 0, 9000);
    close.status = Some(Status {
        code: status::StatusCode::Ok as i32,
        message: String::new(),
    });
    (first, request(vec![close]))
}

/// Trace C as OTLP/JSON, gzipped: a tool root, the newest run.
fn gzipped_json() -> Vec<u8> {
    let body = format!(
        r#"{{"resourceSpans":[{{"resource":{{"attributes":[{{"key":"newdb.project","value":{{"stringValue":"e2e"}}}}]}},
        "scopeSpans":[{{"spans":[{{"traceId":"{}","spanId":"0606060606060606","name":"lookup",
        "startTimeUnixNano":"3000","endTimeUnixNano":"3500",
        "attributes":[{{"key":"gen_ai.operation.name","value":{{"stringValue":"execute_tool"}}}}]}}]}}]}}]}}"#,
        hex(&C)
    );
    let mut gz = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    gz.write_all(body.as_bytes()).unwrap();
    gz.finish().unwrap()
}

fn hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}

fn at<'a>(v: &'a Json, path: &[&str]) -> &'a Json {
    path.iter()
        .fold(v, |v, p| match (v.as_array(), p.parse::<usize>()) {
            (Some(a), Ok(i)) => a.get(i).unwrap_or_else(|| panic!("no [{i}] in {v:?}")),
            _ => v.get(p).unwrap_or_else(|| panic!("no {p} in {v:?}")),
        })
}

fn str_at<'a>(v: &'a Json, path: &[&str]) -> &'a str {
    at(v, path)
        .as_str()
        .unwrap_or_else(|| panic!("{path:?} is not a string"))
}

fn ids(v: &Json, list: &str, field: &str) -> Vec<String> {
    at(v, &[list])
        .as_array()
        .unwrap()
        .iter()
        .map(|r| r.get(field).unwrap().as_str().unwrap().to_string())
        .collect()
}

fn ingest_all(addr: SocketAddr) {
    let (first, close) = first_requests();
    for body in [first, close] {
        let r = post_protobuf(addr, &body);
        assert_eq!(r.status, 200, "{}", String::from_utf8_lossy(&r.body));
        assert_eq!(r.header("content-type"), Some("application/x-protobuf"));
        assert!(r.body.is_empty(), "an empty ExportTraceServiceResponse");
    }
    let r = http(
        addr,
        "POST",
        "/v1/traces",
        &[
            ("content-type", "application/json; charset=utf-8"),
            ("content-encoding", "gzip"),
        ],
        &gzipped_json(),
    );
    assert_eq!((r.status, r.body.as_slice()), (200, &b"{}"[..]));
}

#[test]
fn ingest_then_read_trace_runs_and_search() {
    let srv = server(1 << 20);
    let addr = srv.addr();
    assert_eq!(
        get(addr, "/healthz").json(),
        Json::parse(br#"{"status":"ok"}"#).unwrap()
    );
    ingest_all(addr);

    // Trace tree: 1 > 2 > 3, and the orphan 4 at the top level.
    let r = get(addr, &format!("/v1/traces/{}?project=e2e", hex(&A)));
    assert_eq!(r.status, 200);
    let t = r.json();
    assert_eq!(str_at(&t, &["trace_id"]), hex(&A));
    assert_eq!(ids(&t, "spans", "span_id"), [hex(&[1; 8]), hex(&[4; 8])]);
    let root = at(&t, &["spans", "0"]);
    assert_eq!(str_at(root, &["name"]), "invoke_agent planner");
    assert_eq!(str_at(root, &["run_type"]), "agent");
    assert_eq!(str_at(root, &["start_ns"]), "1000");
    assert_eq!(str_at(root, &["end_ns"]), "9000");
    assert_eq!(str_at(root, &["status"]), "ok");
    assert_eq!(at(root, &["parent_span_id"]), &Json::Null);
    let chat = at(root, &["children", "0"]);
    assert_eq!(str_at(chat, &["span_id"]), hex(&[2; 8]));
    assert_eq!(str_at(chat, &["model"]), "gpt-4o");
    assert_eq!(at(chat, &["prompt_tokens"]).as_u64(), Some(1000));
    assert_eq!(
        at(chat, &["cost_micro_usd"]).as_u64(),
        Some(1000 * 2 + 1000 / 2 + 200 * 10)
    );
    assert_eq!(
        str_at(chat, &["inputs", "messages", "0", "content"]),
        "Refund order 1234, \"now\"\n"
    );
    assert_eq!(str_at(chat, &["children", "0", "span_id"]), hex(&[3; 8]));
    assert_eq!(
        str_at(chat, &["children", "0", "inputs", "arguments", "order"]),
        "1234"
    );
    assert_eq!(
        str_at(&t, &["spans", "1", "parent_span_id"]),
        hex(&[0x99; 8])
    );
    assert!(t.get("stats").is_none());

    // Payloads are the stored (merged) bytes, spliced in verbatim.
    let (first, close) = first_requests();
    let n = Normalizer::builtin();
    let reference = Oracle::new();
    for body in [first, close] {
        reference.ingest(n.from_protobuf(&body).unwrap()).unwrap();
    }
    let merged = reference
        .trace(&TraceQuery {
            project: "e2e".into(),
            trace_id: TraceId(A),
            payloads: true,
            timings: false,
        })
        .unwrap()
        .value;
    assert_eq!(merged.len(), 4);
    for p in &merged {
        for payload in [&p.inputs, &p.outputs, &p.attributes, &p.events]
            .into_iter()
            .flatten()
        {
            assert!(
                r.body
                    .windows(payload.len())
                    .any(|w| w == payload.as_slice()),
                "payload of span {} not spliced verbatim",
                p.span_id
            );
        }
    }

    // payloads=false drops them; stats=1 adds stats with the server's phases.
    let t = get(
        addr,
        &format!("/v1/traces/{}?project=e2e&payloads=false&stats=1", hex(&A)),
    )
    .json();
    assert_eq!(
        at(&t, &["spans", "0", "children", "0", "inputs"]),
        &Json::Null
    );
    assert_eq!(at(&t, &["stats", "records_returned"]).as_u64(), Some(4));
    let phases: Vec<&str> = at(&t, &["stats", "phases_ns"])
        .as_array()
        .unwrap()
        .iter()
        .map(|p| p.get("phase").unwrap().as_str().unwrap())
        .collect();
    assert_eq!(phases, ["server.store", "server.render"]);

    // Runs, newest first, paged with the cursor.
    let page = get(addr, "/v1/runs?project=e2e&limit=2").json();
    assert_eq!(ids(&page, "runs", "trace_id"), [hex(&C), hex(&B)]);
    let b_run = at(&page, &["runs", "1"]);
    assert_eq!(
        (
            str_at(b_run, &["status"]),
            str_at(b_run, &["run_type"]),
            str_at(b_run, &["session"])
        ),
        ("error", "chain", "sess-b")
    );
    assert_eq!(str_at(b_run, &["latency_ns"]), "500");
    let cursor = str_at(&page, &["next_cursor"]).to_string();
    let page = get(
        addr,
        &format!("/v1/runs?project=e2e&limit=2&cursor={cursor}"),
    )
    .json();
    assert_eq!(ids(&page, "runs", "trace_id"), [hex(&A)]);
    assert_eq!(at(&page, &["next_cursor"]), &Json::Null);
    assert_eq!(str_at(&page, &["runs", "0", "end_ns"]), "9000");

    // Filters.
    let runs = |q: &str| {
        ids(
            &get(addr, &format!("/v1/runs?{q}")).json(),
            "runs",
            "trace_id",
        )
    };
    assert_eq!(runs("status=error"), [hex(&B)]);
    assert_eq!(runs("run_type=agent"), [hex(&A)]);
    assert_eq!(runs("session=sess-b&project=e2e"), [hex(&B)]);
    assert_eq!(runs("min_latency_ms=0&since=2000"), [hex(&C), hex(&B)]);
    assert_eq!(runs("project=other"), Vec::<String>::new());

    // Search: both tokens only in the chat span's inputs.
    let hits = get(addr, "/v1/search?project=e2e&q=refund+1234&stats=1").json();
    assert_eq!(ids(&hits, "hits", "span_id"), [hex(&[2; 8])]);
    assert_eq!(str_at(&hits, &["hits", "0", "trace_id"]), hex(&A));
    assert_eq!(str_at(&hits, &["hits", "0", "start_ns"]), "1100");
    assert_eq!(at(&hits, &["stats", "records_returned"]).as_u64(), Some(1));
    let hits = get(addr, "/v1/search?q=1234").json();
    assert_eq!(ids(&hits, "hits", "span_id"), [hex(&[3; 8]), hex(&[2; 8])]);

    srv.shutdown();
    assert!(
        TcpStream::connect(addr).is_err(),
        "still listening after shutdown"
    );
}

#[test]
fn bad_requests_get_clear_errors() {
    let srv = server(1000);
    let addr = srv.addr();
    let error = |r: Reply| (r.status, str_at(&r.json(), &["error"]).to_string());

    let r = http(
        addr,
        "POST",
        "/v1/traces",
        &[("content-type", "text/plain")],
        b"x",
    );
    assert_eq!(r.status, 415);
    let r = post_protobuf(addr, b"\xff\xff\xff");
    assert_eq!(
        (r.status, r.header("content-type")),
        (400, Some("application/x-protobuf"))
    );
    let r = http(
        addr,
        "POST",
        "/v1/traces",
        &[("content-type", "application/json")],
        b"{\"resourceSpans\":7}",
    );
    assert_eq!(r.status, 400);
    assert!(str_at(&r.json(), &["message"]).contains("resourceSpans"));

    // Over the cap: as sent, and only after gunzip.
    assert_eq!(post_protobuf(addr, &[0u8; 1001]).status, 413);
    let mut gz = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::best());
    gz.write_all(&[b' '; 100_000]).unwrap();
    let bomb = gz.finish().unwrap();
    assert!(bomb.len() < 1000);
    let r = http(
        addr,
        "POST",
        "/v1/traces",
        &[
            ("content-type", "application/json"),
            ("content-encoding", "gzip"),
        ],
        &bomb,
    );
    assert_eq!(r.status, 413);
    let r = http(
        addr,
        "POST",
        "/v1/traces",
        &[
            ("content-type", "application/json"),
            ("content-encoding", "br"),
        ],
        b"{}",
    );
    assert_eq!(r.status, 415);

    assert_eq!(error(get(addr, "/v1/traces/abc")).0, 400);
    assert_eq!(error(get(addr, &format!("/v1/traces/{}", hex(&A)))).0, 404);
    let (status, msg) = error(get(addr, "/v1/runs?stauts=error"));
    assert_eq!(status, 400);
    assert!(msg.contains("stauts"), "{msg}");
    assert_eq!(error(get(addr, "/v1/runs?status=bad")).0, 400);
    assert_eq!(error(get(addr, "/v1/runs?cursor=nope")).0, 400);
    assert_eq!(error(get(addr, "/v1/runs?min_cost_usd=0.0000001")).0, 400);
    assert_eq!(error(get(addr, "/v1/search")).0, 400);
    assert_eq!(error(get(addr, "/nope")).0, 404);
    let r = http(addr, "DELETE", "/v1/runs", &[], b"");
    assert_eq!((r.status, r.header("allow")), (405, Some("GET")));
    let r = get(addr, "/v1/traces");
    assert_eq!((r.status, r.header("allow")), (405, Some("POST")));
    // Dropping the handle stops the server too.
    drop(srv);
    assert!(TcpStream::connect(addr).is_err());
}
