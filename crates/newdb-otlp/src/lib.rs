//! OTLP trace export requests to [`Span`] posts (DESIGN.md sections 5 and 8.3).
//!
//! - Protobuf: `ExportTraceServiceRequest` decoded with prost. The generated code is
//!   checked in under `src/proto/` (no `build.rs`, no protoc at build time).
//! - JSON: the OTLP/JSON encoding (hex IDs, string-encoded int64), parsed by this crate's
//!   own small [`json`] module, so no JSON dependency reaches the core crates.
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
//!
//! Field rules, so that a later post never erases what an earlier one set: empty names,
//! zero times, `STATUS_CODE_UNSET` and attributes that map to nothing all leave the field
//! `None` ("not sent in this post"). In particular `run_type` stays `None` when no rule
//! matches, which reads back as [`RunType::Other`](newdb_types::RunType::Other).
//!
//! `attributes` holds, in order: the span's unmapped attributes, `otel.span.kind`,
//! `otel.links` (when the span has links), `resource` (the resource attributes) and
//! `otel.scope.name` / `otel.scope.version`. Each event in `events` is
//! `{"name", "time_unix_nano", "attributes"}`.

pub mod json;
pub mod proto;

mod base64;
mod mapping;
mod otlp_json;
mod pricing;

use newdb_types::{Error, Result, Span, SpanId, Status, TraceId};
use prost::Message as _;

use crate::json::{Json, insert};
use crate::mapping::{Mapping, Obj};
use crate::pricing::Pricing;
use crate::proto::collector::trace::v1::ExportTraceServiceRequest;
use crate::proto::common::v1::{AnyValue, KeyValue, any_value};
use crate::proto::trace::v1::{span, status};

const MAPPING: &str = include_str!("../data/mapping.json");
const PRICING: &str = include_str!("../data/pricing.json");

/// Attribute mapping and pricing, loaded once.
pub struct Normalizer {
    mapping: Mapping,
    pricing: Pricing,
}

impl Normalizer {
    /// The mapping and pricing embedded from `data/`.
    pub fn builtin() -> Normalizer {
        Normalizer::from_bytes(MAPPING.as_bytes(), PRICING.as_bytes())
            .expect("the embedded data files are valid (checked by tests)")
    }

    /// Mapping and pricing from files, for overriding without a rebuild.
    pub fn from_files(mapping: &std::path::Path, pricing: &std::path::Path) -> Result<Normalizer> {
        let m = std::fs::read(mapping)?;
        let p = std::fs::read(pricing)?;
        Normalizer::from_bytes(&m, &p)
    }

    /// Mapping and pricing from the contents of the two data files.
    pub fn from_bytes(mapping: &[u8], pricing: &[u8]) -> Result<Normalizer> {
        Ok(Normalizer {
            mapping: Mapping::parse(mapping)
                .map_err(|e| Error::InvalidInput(format!("mapping file: {e}")))?,
            pricing: Pricing::parse(pricing)
                .map_err(|e| Error::InvalidInput(format!("pricing file: {e}")))?,
        })
    }

    /// Body of `POST /v1/traces` with `content-type: application/x-protobuf`.
    pub fn from_protobuf(&self, body: &[u8]) -> Result<Vec<Span>> {
        let req = ExportTraceServiceRequest::decode(body)
            .map_err(|e| Error::InvalidInput(format!("OTLP protobuf: {e}")))?;
        self.normalize(req)
    }

    /// Body of `POST /v1/traces` with `content-type: application/json`.
    pub fn from_json(&self, body: &[u8]) -> Result<Vec<Span>> {
        self.normalize(Normalizer::decode_json(body)?)
    }

    /// Decodes an OTLP/JSON body without mapping it, for tools and tests that re-encode it
    /// (for example as protobuf).
    pub fn decode_json(body: &[u8]) -> Result<ExportTraceServiceRequest> {
        let v = Json::parse(body).map_err(|e| Error::InvalidInput(format!("OTLP {e}")))?;
        otlp_json::request(&v).map_err(|e| Error::InvalidInput(format!("OTLP JSON: {e}")))
    }

    /// Maps a decoded request. Any invalid span (bad or zero IDs) rejects the whole request.
    pub fn normalize(&self, req: ExportTraceServiceRequest) -> Result<Vec<Span>> {
        let mut out = Vec::new();
        for rs in req.resource_spans {
            let resource = attrs_to_obj(rs.resource.map(|r| r.attributes).unwrap_or_default());
            let project = self
                .mapping
                .project
                .iter()
                .find_map(|k| mapping::get(&resource, k).and_then(Json::as_str))
                .filter(|p| !p.is_empty())
                .unwrap_or(&self.mapping.default_project)
                .to_string();
            let mut tail: Obj = Vec::new();
            if !resource.is_empty() {
                tail.push(("resource".into(), Json::Object(resource)));
            }
            for ss in rs.scope_spans {
                let mut tail = tail.clone();
                if let Some(s) = &ss.scope {
                    if !s.name.is_empty() {
                        tail.push(("otel.scope.name".into(), Json::String(s.name.clone())));
                    }
                    if !s.version.is_empty() {
                        tail.push(("otel.scope.version".into(), Json::String(s.version.clone())));
                    }
                }
                for sp in ss.spans {
                    out.push(self.span(sp, &project, &tail)?);
                }
            }
        }
        Ok(out)
    }

    fn span(&self, sp: proto::trace::v1::Span, project: &str, tail: &Obj) -> Result<Span> {
        let bad = |m: String| Error::InvalidInput(m);
        let trace_id: [u8; 16] = sp.trace_id.as_slice().try_into().map_err(|_| {
            bad(format!(
                "trace_id must be 16 bytes, got {}",
                sp.trace_id.len()
            ))
        })?;
        let span_id: [u8; 8] = sp
            .span_id
            .as_slice()
            .try_into()
            .map_err(|_| bad(format!("span_id must be 8 bytes, got {}", sp.span_id.len())))?;
        if trace_id == [0; 16] || span_id == [0; 8] {
            return Err(bad("trace_id and span_id must be non-zero".into()));
        }
        let parent_span_id = match <[u8; 8]>::try_from(sp.parent_span_id.as_slice()) {
            _ if sp.parent_span_id.is_empty() => None,
            Ok(p) => (p != [0; 8]).then_some(SpanId(p)),
            Err(_) => {
                return Err(bad(format!(
                    "parent_span_id must be 8 bytes, got {}",
                    sp.parent_span_id.len()
                )));
            }
        };

        let mut attrs = attrs_to_obj(sp.attributes);
        let events: Vec<(String, u64, Obj)> = sp
            .events
            .into_iter()
            .map(|e| (e.name, e.time_unix_nano, attrs_to_obj(e.attributes)))
            .collect();
        let named: Vec<(String, Obj)> = events
            .iter()
            .map(|(n, _, a)| (n.clone(), a.clone()))
            .collect();
        let d = self.mapping.derive(&mut attrs, &named);

        let mut status = None;
        let mut error = None;
        if let Some(st) = &sp.status {
            match status::StatusCode::try_from(st.code).unwrap_or(status::StatusCode::Unset) {
                status::StatusCode::Ok => status = Some(Status::Ok),
                status::StatusCode::Error => {
                    status = Some(Status::Error);
                    error = (!st.message.is_empty()).then(|| st.message.clone());
                }
                status::StatusCode::Unset => {}
            }
        }
        // Exceptions recorded as events.
        if let Some((_, _, ex)) = events.iter().find(|(n, _, _)| n == "exception") {
            if error.is_none() {
                let ty = mapping::get(ex, "exception.type").and_then(Json::as_str);
                let msg = mapping::get(ex, "exception.message").and_then(Json::as_str);
                error = match (ty, msg) {
                    (Some(t), Some(m)) => Some(format!("{t}: {m}")),
                    (None, Some(m)) => Some(m.to_string()),
                    (Some(t), None) => Some(t.to_string()),
                    (None, None) => None,
                };
            }
            status.get_or_insert(Status::Error);
        }

        attrs.push((
            "otel.span.kind".into(),
            Json::String(kind_name(sp.kind).into()),
        ));
        if !sp.links.is_empty() {
            let links = sp
                .links
                .into_iter()
                .map(|l| {
                    Json::Object(vec![
                        ("trace_id".into(), Json::String(hex(&l.trace_id))),
                        ("span_id".into(), Json::String(hex(&l.span_id))),
                        (
                            "attributes".into(),
                            Json::Object(attrs_to_obj(l.attributes)),
                        ),
                    ])
                })
                .collect();
            attrs.push(("otel.links".into(), Json::Array(links)));
        }
        attrs.extend(tail.iter().cloned());

        let events = (!events.is_empty()).then(|| {
            Json::Array(
                events
                    .into_iter()
                    .map(|(name, t, a)| {
                        Json::Object(vec![
                            ("name".into(), Json::String(name)),
                            ("time_unix_nano".into(), Json::Number(t.to_string())),
                            ("attributes".into(), Json::Object(a)),
                        ])
                    })
                    .collect(),
            )
            .to_bytes()
        });

        let cost_micro_usd =
            self.pricing
                .cost_micro_usd(d.model.as_deref(), d.prompt_tokens, d.completion_tokens);
        Ok(Span {
            project: project.to_string(),
            trace_id: TraceId(trace_id),
            span_id: SpanId(span_id),
            parent_span_id,
            name: (!sp.name.is_empty()).then_some(sp.name),
            start_ns: (sp.start_time_unix_nano > 0).then_some(sp.start_time_unix_nano),
            end_ns: (sp.end_time_unix_nano > 0).then_some(sp.end_time_unix_nano),
            status,
            run_type: d.run_type,
            model: d.model,
            provider: d.provider,
            session: d.session,
            prompt_tokens: d.prompt_tokens,
            completion_tokens: d.completion_tokens,
            cost_micro_usd,
            inputs: d.inputs.map(|v| v.to_bytes()),
            outputs: d.outputs.map(|v| v.to_bytes()),
            attributes: Some(Json::Object(attrs).to_bytes()),
            events,
            error,
            version: 0,
        })
    }
}

fn hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}

fn kind_name(kind: i32) -> &'static str {
    match span::SpanKind::try_from(kind).unwrap_or(span::SpanKind::Unspecified) {
        span::SpanKind::Unspecified => "unspecified",
        span::SpanKind::Internal => "internal",
        span::SpanKind::Server => "server",
        span::SpanKind::Client => "client",
        span::SpanKind::Producer => "producer",
        span::SpanKind::Consumer => "consumer",
    }
}

fn any_value_to_json(v: AnyValue) -> Json {
    use any_value::Value;
    match v.value {
        None | Some(Value::StringValueStrindex(_)) => Json::Null,
        Some(Value::StringValue(s)) => Json::String(s),
        Some(Value::BoolValue(b)) => Json::Bool(b),
        Some(Value::IntValue(i)) => Json::Number(i.to_string()),
        Some(Value::DoubleValue(d)) => Json::from_f64(d),
        Some(Value::ArrayValue(a)) => {
            Json::Array(a.values.into_iter().map(any_value_to_json).collect())
        }
        Some(Value::KvlistValue(kv)) => Json::Object(attrs_to_obj(kv.values)),
        Some(Value::BytesValue(b)) => Json::String(base64::encode(&b)),
    }
}

/// OTLP key-values to JSON object entries. A repeated key keeps its last value.
fn attrs_to_obj(attrs: Vec<KeyValue>) -> Obj {
    let mut out = Vec::with_capacity(attrs.len());
    for kv in attrs {
        let v = kv.value.map(any_value_to_json).unwrap_or(Json::Null);
        insert(&mut out, kv.key, v);
    }
    out
}
