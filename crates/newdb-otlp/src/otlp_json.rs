//! OTLP/JSON (https://opentelemetry.io/docs/specs/otlp/#json-protobuf-encoding) into the
//! prost structs, so both encodings share one normalization path.
//!
//! As the spec requires: trace and span IDs are hex strings (any case), 64-bit integers may
//! be strings or numbers, enums are integers (their `SPAN_KIND_*` / `STATUS_CODE_*` names are
//! accepted too), `bytesValue` is base64, and unknown fields are ignored. Field names are
//! lowerCamelCase; snake_case names are accepted as well.

use crate::json::Json;
use crate::proto::collector::trace::v1::ExportTraceServiceRequest;
use crate::proto::common::v1::{
    AnyValue, ArrayValue, InstrumentationScope, KeyValue, KeyValueList, any_value,
};
use crate::proto::resource::v1::Resource;
use crate::proto::trace::v1::{ResourceSpans, ScopeSpans, Span, Status, span};

type R<T> = Result<T, String>;

fn field<'a>(v: &'a Json, camel: &str, snake: &str) -> Option<&'a Json> {
    v.get(camel)
        .or_else(|| v.get(snake))
        .filter(|f| **f != Json::Null)
}

fn list<'a>(v: &'a Json, camel: &str, snake: &str) -> R<&'a [Json]> {
    match field(v, camel, snake) {
        None => Ok(&[]),
        Some(Json::Array(a)) => Ok(a),
        Some(_) => Err(format!("`{camel}` must be an array")),
    }
}

fn string(v: &Json, camel: &str, snake: &str) -> R<String> {
    match field(v, camel, snake) {
        None => Ok(String::new()),
        Some(Json::String(s)) => Ok(s.clone()),
        Some(_) => Err(format!("`{camel}` must be a string")),
    }
}

fn hex(v: &Json, camel: &str, snake: &str) -> R<Vec<u8>> {
    let s = string(v, camel, snake)?;
    let b = s.as_bytes();
    let nibble = |c: u8| (c as char).to_digit(16).map(|d| d as u8);
    if b.len() % 2 != 0 {
        return Err(format!("`{camel}` is not hex: {s:?}"));
    }
    b.chunks(2)
        .map(|p| Some(nibble(p[0])? << 4 | nibble(p[1])?))
        .collect::<Option<_>>()
        .ok_or_else(|| format!("`{camel}` is not hex: {s:?}"))
}

fn u64_field(v: &Json, camel: &str, snake: &str) -> R<u64> {
    match field(v, camel, snake) {
        None => Some(0),
        Some(Json::Number(n)) => n.parse().ok(),
        Some(Json::String(s)) => s.parse().ok(),
        Some(_) => None,
    }
    .ok_or_else(|| format!("`{camel}` must be an unsigned 64-bit integer"))
}

fn u32_field(v: &Json, camel: &str, snake: &str) -> R<u32> {
    u32::try_from(u64_field(v, camel, snake)?).map_err(|_| format!("`{camel}` is out of range"))
}

fn enum_field(v: &Json, camel: &str, names: &[&str]) -> R<i32> {
    match field(v, camel, camel) {
        None => Ok(0),
        Some(Json::Number(n)) => n
            .parse()
            .map_err(|_| format!("`{camel}` must be an integer")),
        Some(Json::String(s)) => names
            .iter()
            .position(|n| n == s)
            .map(|i| i as i32)
            .ok_or_else(|| format!("unknown `{camel}` {s:?}")),
        Some(_) => Err(format!("`{camel}` must be an integer")),
    }
}

const SPAN_KINDS: [&str; 6] = [
    "SPAN_KIND_UNSPECIFIED",
    "SPAN_KIND_INTERNAL",
    "SPAN_KIND_SERVER",
    "SPAN_KIND_CLIENT",
    "SPAN_KIND_PRODUCER",
    "SPAN_KIND_CONSUMER",
];
const STATUS_CODES: [&str; 3] = ["STATUS_CODE_UNSET", "STATUS_CODE_OK", "STATUS_CODE_ERROR"];

fn any_value(v: &Json, depth: usize) -> R<AnyValue> {
    use any_value::Value;
    if depth > 64 {
        return Err("attribute values nested too deep".into());
    }
    let value = if let Some(s) = field(v, "stringValue", "string_value") {
        Value::StringValue(
            s.as_str()
                .ok_or("`stringValue` must be a string")?
                .to_string(),
        )
    } else if let Some(b) = field(v, "boolValue", "bool_value") {
        Value::BoolValue(b.as_bool().ok_or("`boolValue` must be a boolean")?)
    } else if let Some(i) = field(v, "intValue", "int_value") {
        let n = match i {
            Json::Number(n) => n.parse().ok(),
            Json::String(s) => s.parse().ok(),
            _ => None,
        };
        Value::IntValue(n.ok_or("`intValue` must be a 64-bit integer")?)
    } else if let Some(d) = field(v, "doubleValue", "double_value") {
        let f = match d {
            Json::Number(n) => n.parse().ok(),
            Json::String(s) => match s.as_str() {
                "NaN" => Some(f64::NAN),
                "Infinity" => Some(f64::INFINITY),
                "-Infinity" => Some(f64::NEG_INFINITY),
                _ => s.parse().ok(),
            },
            _ => None,
        };
        Value::DoubleValue(f.ok_or("`doubleValue` must be a number")?)
    } else if let Some(a) = field(v, "arrayValue", "array_value") {
        let values = list(a, "values", "values")?
            .iter()
            .map(|x| any_value(x, depth + 1))
            .collect::<R<_>>()?;
        Value::ArrayValue(ArrayValue { values })
    } else if let Some(kv) = field(v, "kvlistValue", "kvlist_value") {
        Value::KvlistValue(KeyValueList {
            values: key_values(list(kv, "values", "values")?, depth + 1)?,
        })
    } else if let Some(b) = field(v, "bytesValue", "bytes_value") {
        let s = b.as_str().ok_or("`bytesValue` must be a base64 string")?;
        Value::BytesValue(crate::base64::decode(s).ok_or("`bytesValue` must be base64")?)
    } else {
        return Ok(AnyValue { value: None });
    };
    Ok(AnyValue { value: Some(value) })
}

fn key_values(a: &[Json], depth: usize) -> R<Vec<KeyValue>> {
    a.iter()
        .map(|kv| {
            Ok(KeyValue {
                key: string(kv, "key", "key")?,
                value: field(kv, "value", "value")
                    .map(|v| any_value(v, depth))
                    .transpose()?,
                ..KeyValue::default()
            })
        })
        .collect()
}

fn attributes(v: &Json) -> R<Vec<KeyValue>> {
    key_values(list(v, "attributes", "attributes")?, 0)
}

fn span(v: &Json) -> R<Span> {
    let events = list(v, "events", "events")?
        .iter()
        .map(|e| {
            Ok(span::Event {
                time_unix_nano: u64_field(e, "timeUnixNano", "time_unix_nano")?,
                name: string(e, "name", "name")?,
                attributes: attributes(e)?,
                dropped_attributes_count: u32_field(
                    e,
                    "droppedAttributesCount",
                    "dropped_attributes_count",
                )?,
            })
        })
        .collect::<R<_>>()?;
    let links = list(v, "links", "links")?
        .iter()
        .map(|l| {
            Ok(span::Link {
                trace_id: hex(l, "traceId", "trace_id")?,
                span_id: hex(l, "spanId", "span_id")?,
                trace_state: string(l, "traceState", "trace_state")?,
                attributes: attributes(l)?,
                dropped_attributes_count: u32_field(
                    l,
                    "droppedAttributesCount",
                    "dropped_attributes_count",
                )?,
                flags: u32_field(l, "flags", "flags")?,
            })
        })
        .collect::<R<_>>()?;
    let status = field(v, "status", "status")
        .map(|s| {
            Ok::<_, String>(Status {
                message: string(s, "message", "message")?,
                code: enum_field(s, "code", &STATUS_CODES)?,
            })
        })
        .transpose()?;
    Ok(Span {
        trace_id: hex(v, "traceId", "trace_id")?,
        span_id: hex(v, "spanId", "span_id")?,
        trace_state: string(v, "traceState", "trace_state")?,
        parent_span_id: hex(v, "parentSpanId", "parent_span_id")?,
        flags: u32_field(v, "flags", "flags")?,
        name: string(v, "name", "name")?,
        kind: enum_field(v, "kind", &SPAN_KINDS)?,
        start_time_unix_nano: u64_field(v, "startTimeUnixNano", "start_time_unix_nano")?,
        end_time_unix_nano: u64_field(v, "endTimeUnixNano", "end_time_unix_nano")?,
        attributes: attributes(v)?,
        dropped_attributes_count: u32_field(
            v,
            "droppedAttributesCount",
            "dropped_attributes_count",
        )?,
        events,
        dropped_events_count: u32_field(v, "droppedEventsCount", "dropped_events_count")?,
        links,
        dropped_links_count: u32_field(v, "droppedLinksCount", "dropped_links_count")?,
        status,
    })
}

/// Converts a parsed OTLP/JSON `ExportTraceServiceRequest`.
pub(crate) fn request(v: &Json) -> R<ExportTraceServiceRequest> {
    if v.as_object().is_none() {
        return Err("the request must be a JSON object".into());
    }
    let resource_spans = list(v, "resourceSpans", "resource_spans")?
        .iter()
        .map(|rs| {
            let resource = field(rs, "resource", "resource")
                .map(|r| {
                    Ok::<_, String>(Resource {
                        attributes: attributes(r)?,
                        ..Resource::default()
                    })
                })
                .transpose()?;
            let scope_spans = list(rs, "scopeSpans", "scope_spans")?
                .iter()
                .map(|ss| {
                    let scope = field(ss, "scope", "scope")
                        .map(|s| {
                            Ok::<_, String>(InstrumentationScope {
                                name: string(s, "name", "name")?,
                                version: string(s, "version", "version")?,
                                attributes: attributes(s)?,
                                dropped_attributes_count: 0,
                            })
                        })
                        .transpose()?;
                    Ok(ScopeSpans {
                        scope,
                        spans: list(ss, "spans", "spans")?
                            .iter()
                            .map(span)
                            .collect::<R<_>>()?,
                        schema_url: string(ss, "schemaUrl", "schema_url")?,
                    })
                })
                .collect::<R<_>>()?;
            Ok(ResourceSpans {
                resource,
                scope_spans,
                schema_url: string(rs, "schemaUrl", "schema_url")?,
            })
        })
        .collect::<R<_>>()?;
    Ok(ExportTraceServiceRequest { resource_spans })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn req(s: &str) -> R<ExportTraceServiceRequest> {
        request(&Json::parse(s.as_bytes()).unwrap())
    }

    #[test]
    fn decodes_spec_encoding() {
        let r = req(r#"{"resourceSpans":[{"resource":{"attributes":[{"key":"service.name","value":{"stringValue":"svc"}}]},
            "scopeSpans":[{"scope":{"name":"s"},"spans":[{"traceId":"5B8EFFF798038103D269B633813FC60C","spanId":"eee19b7ec3c1b174",
            "parentSpanId":"","name":"n","kind":"SPAN_KIND_CLIENT","startTimeUnixNano":"1761148800000000000","endTimeUnixNano":1761148800000000001,
            "attributes":[{"key":"i","value":{"intValue":"-9223372036854775808"}},{"key":"d","value":{"doubleValue":"NaN"}},
            {"key":"b","value":{"bytesValue":"aGk="}},{"key":"kv","value":{"kvlistValue":{"values":[{"key":"x","value":{"arrayValue":{"values":[{"boolValue":true}]}}}]}}},
            {"key":"empty","value":{}}],
            "status":{"code":2,"message":"boom"},"unknownField":{"a":1}}]}]}]}"#)
        .unwrap();
        let sp = &r.resource_spans[0].scope_spans[0].spans[0];
        assert_eq!(sp.trace_id[0], 0x5b);
        assert_eq!(sp.parent_span_id, Vec::<u8>::new());
        assert_eq!(sp.kind, 3);
        assert_eq!(sp.start_time_unix_nano, 1_761_148_800_000_000_000);
        assert_eq!(sp.end_time_unix_nano, 1_761_148_800_000_000_001);
        assert_eq!(
            sp.attributes[0].value.as_ref().unwrap().value,
            Some(any_value::Value::IntValue(i64::MIN))
        );
        assert_eq!(
            sp.attributes[2].value.as_ref().unwrap().value,
            Some(any_value::Value::BytesValue(b"hi".to_vec()))
        );
        assert_eq!(sp.attributes[4].value.as_ref().unwrap().value, None);
        assert_eq!(sp.status.as_ref().unwrap().code, 2);
    }

    #[test]
    fn rejects_malformed_fields() {
        for bad in [
            r#"[]"#,
            r#"{"resourceSpans":{}}"#,
            r#"{"resourceSpans":[{"scopeSpans":[{"spans":[{"traceId":"xyz1"}]}]}]}"#,
            r#"{"resourceSpans":[{"scopeSpans":[{"spans":[{"traceId":"abc"}]}]}]}"#,
            r#"{"resourceSpans":[{"scopeSpans":[{"spans":[{"startTimeUnixNano":"-1"}]}]}]}"#,
            r#"{"resourceSpans":[{"scopeSpans":[{"spans":[{"kind":"SPAN_KIND_NOPE"}]}]}]}"#,
            r#"{"resourceSpans":[{"scopeSpans":[{"spans":[{"attributes":[{"key":"a","value":{"intValue":1.5}}]}]}]}]}"#,
        ] {
            assert!(req(bad).is_err(), "accepted {bad}");
        }
    }
}
