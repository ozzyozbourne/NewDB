//! Golden fixtures ported from Forge Trace DB (`forge-model/tests/fixtures`): OTLP/JSON
//! requests in the shapes OpenLLMetry (LangGraph), OpenInference (LangChain) and the OTel
//! GenAI semantic conventions emit, and the spans they map to. The `.expected.json` files are
//! newdb's rendering of the resulting `Span`s. A missing or different file fails the test;
//! run with `NEWDB_BLESS=1` to rewrite them after a deliberate change, and review the diff.
//!
//! The expected files were checked field by field against Forge's own `.expected.json` for
//! the same fixtures (2026-11-13): run_type, model, provider, prompt/completion tokens, cost,
//! session, parent/root, status, error, name, times, inputs, outputs and events all match.
//! Intentional differences, all from the newdb contract:
//! - Project: newdb reads `newdb.project`, `forge.project`, then `service.name`, so the
//!   OpenInference fixture is `rag-app` (Forge: `rag` from `openinference.project.name`) and
//!   the GenAI fixture is `agent-sdk` (Forge: `default`).
//! - `Span` has no tags, metadata or total-token fields, so `tag.tags`, `metadata`,
//!   `traceloop.association.properties.user_id` and `*.total_tokens` stay in `attributes`
//!   (Forge moved them to `tags`, `metadata` and `total_tokens`).
//! - Spans no rule maps get `run_type: None` (read as `other`); Forge defaulted to `chain`.
//!   No span in these fixtures hits that case.

use std::path::PathBuf;

use newdb_otlp::Normalizer;
use newdb_otlp::json::{Json, write_str};
use newdb_types::{RunType, Span, Status};
use prost::Message as _;

fn fixtures() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures")
}

fn render(spans: &[Span]) -> String {
    let opt_str = |o: &mut Vec<u8>, s: &Option<String>| match s {
        Some(s) => write_str(o, s),
        None => o.extend_from_slice(b"null"),
    };
    let opt_num = |o: &mut Vec<u8>, n: Option<u64>| match n {
        Some(n) => o.extend_from_slice(n.to_string().as_bytes()),
        None => o.extend_from_slice(b"null"),
    };
    let raw = |o: &mut Vec<u8>, b: &Option<Vec<u8>>| match b {
        Some(b) => o.extend_from_slice(b),
        None => o.extend_from_slice(b"null"),
    };
    let mut o = b"[\n".to_vec();
    for (i, s) in spans.iter().enumerate() {
        o.extend_from_slice(b"  {\n");
        let mut f = |name: &str, w: &dyn Fn(&mut Vec<u8>)| {
            o.extend_from_slice(format!("    \"{name}\": ").as_bytes());
            w(&mut o);
            o.extend_from_slice(b",\n");
        };
        f("project", &|o| write_str(o, &s.project));
        f("trace_id", &|o| write_str(o, &s.trace_id.to_hex()));
        f("span_id", &|o| write_str(o, &s.span_id.to_hex()));
        f("parent_span_id", &|o| {
            opt_str(o, &s.parent_span_id.map(|p| p.to_hex()))
        });
        f("name", &|o| opt_str(o, &s.name));
        f("start_ns", &|o| opt_num(o, s.start_ns));
        f("end_ns", &|o| opt_num(o, s.end_ns));
        f("status", &|o| {
            opt_str(o, &s.status.map(|x| x.as_str().into()))
        });
        f("run_type", &|o| {
            opt_str(o, &s.run_type.map(|x| x.as_str().into()))
        });
        f("model", &|o| opt_str(o, &s.model));
        f("provider", &|o| opt_str(o, &s.provider));
        f("session", &|o| opt_str(o, &s.session));
        f("prompt_tokens", &|o| {
            opt_num(o, s.prompt_tokens.map(u64::from))
        });
        f("completion_tokens", &|o| {
            opt_num(o, s.completion_tokens.map(u64::from))
        });
        f("cost_micro_usd", &|o| opt_num(o, s.cost_micro_usd));
        f("error", &|o| opt_str(o, &s.error));
        f("inputs", &|o| raw(o, &s.inputs));
        f("outputs", &|o| raw(o, &s.outputs));
        f("events", &|o| raw(o, &s.events));
        f("attributes", &|o| raw(o, &s.attributes));
        o.truncate(o.len() - 2);
        o.extend_from_slice(b"\n  }");
        if i + 1 < spans.len() {
            o.extend_from_slice(b",\n");
        }
    }
    o.extend_from_slice(b"\n]\n");
    String::from_utf8(o).unwrap()
}

fn check_golden(name: &str) -> Vec<Span> {
    let body = std::fs::read(fixtures().join(format!("{name}.otlp.json"))).unwrap();
    let n = Normalizer::builtin();
    let spans = n.from_json(&body).unwrap();

    // The same request as protobuf maps to the same spans.
    let pb = Normalizer::decode_json(&body).unwrap().encode_to_vec();
    assert_eq!(
        n.from_protobuf(&pb).unwrap(),
        spans,
        "{name}: protobuf and JSON differ"
    );

    // Every payload is valid JSON.
    for s in &spans {
        for p in [&s.inputs, &s.outputs, &s.events, &s.attributes]
            .into_iter()
            .flatten()
        {
            Json::parse(p).unwrap_or_else(|e| panic!("{name}: bad payload {e}"));
        }
    }

    let got = render(&spans);
    let path = fixtures().join(format!("{name}.expected.json"));
    if std::env::var_os("NEWDB_BLESS").is_some() {
        std::fs::write(&path, &got).unwrap();
    }
    let want = std::fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("{}: {e}; create it with NEWDB_BLESS=1", path.display()));
    assert_eq!(
        got, want,
        "{name} mapped differently; rerun with NEWDB_BLESS=1 if intended"
    );
    spans
}

fn json(b: &Option<Vec<u8>>) -> Json {
    Json::parse(b.as_deref().unwrap()).unwrap()
}

fn at<'a>(v: &'a Json, path: &[&str]) -> &'a Json {
    path.iter()
        .fold(v, |v, p| match (v.as_array(), p.parse::<usize>()) {
            (Some(a), Ok(i)) => &a[i],
            _ => v.get(p).unwrap_or_else(|| panic!("no {p} in {v:?}")),
        })
}

fn s(v: &Json) -> &str {
    v.as_str().unwrap()
}

#[test]
fn openllmetry_langgraph() {
    let spans = check_golden("openllmetry_langgraph");
    assert_eq!(spans.len(), 4);
    assert!(spans.iter().all(|r| r.project == "support"));
    let wf = &spans[0];
    assert_eq!(wf.run_type, Some(RunType::Chain));
    assert_eq!(wf.parent_span_id, None);
    assert_eq!(wf.status, Some(Status::Ok));
    assert_eq!(wf.session.as_deref(), Some("thread-42"));
    let attrs = json(&wf.attributes);
    assert_eq!(
        s(at(&attrs, &["traceloop.association.properties.user_id"])),
        "u-7"
    );
    assert_eq!(
        s(at(
            &json(&wf.inputs),
            &["inputs", "messages", "0", "content"]
        )),
        "I need a refund for order 1234"
    );
    let llm = &spans[2];
    assert_eq!(llm.run_type, Some(RunType::ChatModel));
    assert_eq!(llm.model.as_deref(), Some("gpt-4o-mini"));
    assert_eq!(llm.provider.as_deref(), Some("openai"));
    assert_eq!(
        (llm.prompt_tokens, llm.completion_tokens),
        (Some(412), Some(23))
    );
    // 412 × 0.15 + 23 × 0.60 = 75.6 micro-USD, rounded down.
    assert_eq!(llm.cost_micro_usd, Some(75));
    assert_eq!(
        s(at(&json(&llm.inputs), &["messages", "1", "content"])),
        "I need a refund for order 1234"
    );
    assert_eq!(
        s(at(
            &json(&llm.outputs),
            &["messages", "0", "tool_calls", "0", "name"]
        )),
        "issue_refund"
    );
    assert_eq!(spans[3].run_type, Some(RunType::Tool));
    assert_eq!(spans[3].parent_span_id, Some(spans[1].span_id));
}

#[test]
fn openinference_langchain() {
    let spans = check_golden("openinference_langchain");
    // `openinference.project.name` is not a project attribute here; `service.name` is.
    assert!(spans.iter().all(|r| r.project == "rag-app"));
    let chain = &spans[0];
    assert_eq!(chain.run_type, Some(RunType::Chain));
    let attrs = json(&chain.attributes);
    assert_eq!(at(&attrs, &["tag.tags"]).as_array().unwrap().len(), 2);
    assert_eq!(s(at(&attrs, &["metadata"])), "{\"tenant\": \"acme\"}");
    assert_eq!(
        s(at(&json(&chain.inputs), &["query"])),
        "What is the warranty period?"
    );
    assert_eq!(s(at(&json(&chain.outputs), &["output"])), "Two years.");
    assert_eq!(chain.session.as_deref(), Some("sess-9"));
    assert_eq!(spans[1].run_type, Some(RunType::Retriever));
    let llm = &spans[2];
    assert_eq!(llm.run_type, Some(RunType::Llm));
    assert_eq!(llm.model.as_deref(), Some("claude-sonnet-4-20250514"));
    assert_eq!(
        (llm.prompt_tokens, llm.completion_tokens),
        (Some(1200), Some(5))
    );
    assert_eq!(llm.status, Some(Status::Error));
    assert_eq!(llm.error.as_deref(), Some("rate limited, retried"));
    assert_eq!(llm.cost_micro_usd, Some(1200 * 3 + 5 * 15));
    assert_eq!(s(at(&json(&llm.events), &["0", "name"])), "exception");
}

#[test]
fn genai_semconv() {
    let spans = check_golden("genai_semconv");
    assert!(spans.iter().all(|r| r.project == "agent-sdk"));
    assert_eq!(spans[0].run_type, Some(RunType::Agent));
    assert_eq!(spans[0].session.as_deref(), Some("conv-123"));
    let chat = &spans[1];
    assert_eq!(chat.run_type, Some(RunType::ChatModel));
    assert_eq!(
        (chat.prompt_tokens, chat.completion_tokens),
        (Some(50), Some(30))
    );
    assert_eq!(chat.cost_micro_usd, Some(50 * 2 + 30 * 8));
    assert_eq!(
        s(at(
            &json(&chat.inputs),
            &["messages", "0", "parts", "0", "content"]
        )),
        "Plan 3 days in Lisbon"
    );
    let attrs = json(&chat.attributes);
    assert_eq!(
        at(&attrs, &["gen_ai.request.temperature"]).as_f64(),
        Some(0.2)
    );
    assert_eq!(s(at(&attrs, &["resource", "service.name"])), "agent-sdk");
    assert_eq!(s(at(&attrs, &["otel.span.kind"])), "client");
    let tool = &spans[2];
    assert_eq!(tool.run_type, Some(RunType::Tool));
    assert_eq!(s(at(&json(&tool.inputs), &["arguments", "to"])), "LIS");
    assert_eq!(
        at(&json(&tool.outputs), &["result", "flights"]).as_u64(),
        Some(3)
    );
    // Still-open span: no end time; messages from events.
    let open = &spans[3];
    assert_eq!(open.end_ns, None);
    assert_eq!(
        s(at(&json(&open.inputs), &["messages", "0", "content"])),
        "Book the cheapest one"
    );
}

#[test]
fn project_attributes_in_priority_order() {
    let n = Normalizer::builtin();
    let body = |attrs: &str| {
        format!(
            r#"{{"resourceSpans":[{{"resource":{{"attributes":[{attrs}]}},"scopeSpans":[{{"spans":[{{"traceId":"0af7651916cd43dd8448eb211c80319c","spanId":"b7ad6b7169203331"}}]}}]}}]}}"#
        )
    };
    let kv = |k: &str, v: &str| format!(r#"{{"key":"{k}","value":{{"stringValue":"{v}"}}}}"#);
    let project = |attrs: &str| {
        n.from_json(body(attrs).as_bytes()).unwrap()[0]
            .project
            .clone()
    };
    assert_eq!(project(""), "default");
    assert_eq!(project(&kv("service.name", "svc")), "svc");
    assert_eq!(
        project(&[kv("service.name", "svc"), kv("forge.project", "f")].join(",")),
        "f"
    );
    assert_eq!(
        project(
            &[
                kv("forge.project", "f"),
                kv("newdb.project", "n"),
                kv("service.name", "s")
            ]
            .join(",")
        ),
        "n"
    );
}

#[test]
fn rejects_bad_ids_and_bodies() {
    let n = Normalizer::builtin();
    let span = |fields: &str| {
        format!(r#"{{"resourceSpans":[{{"scopeSpans":[{{"spans":[{{{fields}}}]}}]}}]}}"#)
    };
    for bad in [
        span(r#""traceId":"abcd","spanId":"00f067aa0ba902b7""#),
        span(r#""traceId":"0af7651916cd43dd8448eb211c80319c","spanId":"00f067aa""#),
        span(r#""traceId":"00000000000000000000000000000000","spanId":"00f067aa0ba902b7""#),
        span(r#""traceId":"0af7651916cd43dd8448eb211c80319c","spanId":"0000000000000000""#),
        span(
            r#""traceId":"0af7651916cd43dd8448eb211c80319c","spanId":"00f067aa0ba902b7","parentSpanId":"abcd""#,
        ),
        "not json".to_string(),
    ] {
        assert!(n.from_json(bad.as_bytes()).is_err(), "accepted {bad}");
    }
    assert!(n.from_protobuf(b"\xff\xff\xff").is_err());
    // A zero parent means root; an unset status, empty name and zero times mean "not sent".
    let ok = span(
        r#""traceId":"0af7651916cd43dd8448eb211c80319c","spanId":"00f067aa0ba902b7","parentSpanId":"0000000000000000","name":"","startTimeUnixNano":"0","status":{"code":0}"#,
    );
    let s = &n.from_json(ok.as_bytes()).unwrap()[0];
    assert_eq!(
        (s.parent_span_id, &s.name, s.start_ns, s.status, s.run_type),
        (None, &None, None, None, None)
    );
    assert_eq!(n.from_json(b"{}").unwrap(), vec![]);
}

#[test]
fn data_files_load_from_paths() {
    let data = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("data");
    assert!(Normalizer::from_files(&data.join("mapping.json"), &data.join("pricing.json")).is_ok());
    assert!(
        Normalizer::from_files(&data.join("pricing.json"), &data.join("pricing.json")).is_err()
    );
    assert!(
        Normalizer::from_files(&data.join("missing.json"), &data.join("pricing.json")).is_err()
    );
}
