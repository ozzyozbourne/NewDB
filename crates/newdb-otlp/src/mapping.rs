//! The attribute-name mapping (`data/mapping.json`): which span attributes become which
//! [`Span`](newdb_types::Span) fields. Ported from Forge Trace DB's `forge-model/src/genai.rs`;
//! the attribute names moved to the data file, the structure (indexed message lists, message
//! events, `input.value`-style payloads) stays in code here.
//!
//! Keys of the mapping file, all lists in priority order:
//! - `project`, `default_project`: resource attributes that name the project.
//! - `run_type`: rules tried in order. Each reads one `attribute`, looks its string value up
//!   in `values` (lowercased first when `ignore_case`), and falls back to `otherwise` when
//!   the attribute is present but unlisted. `run_type_when_present` applies last: any of its
//!   `attributes` present gives its `run_type`. No match leaves `run_type` unset.
//! - `model`, `provider`, `session`: the first attribute with a non-empty string (or number)
//!   wins and is removed. `prompt_tokens`, `completion_tokens`: the first attribute holding
//!   an integer in `u32` range (number or decimal string) wins and is removed.
//! - `drop`: attributes removed without being stored (redundant with mapped ones).
//! - `inputs`, `outputs`: how the payload object is assembled, in this order: `fields`
//!   (attribute → key, JSON strings parsed), then if no `messages` yet the first
//!   `indexed_messages` prefix present (`<prefix><i>.<inner><path>` attributes, collected into
//!   a list of nested objects ordered by `i`), then if still no `messages` the span events
//!   named in `message_events` (event name → role, or `null` for no role), then the first
//!   `value` attribute: an object becomes the payload if nothing else was found, anything
//!   else goes under `value_key`.
//! - `event_attributes_skipped`: event attributes left out of messages built from events.

use newdb_types::RunType;

use crate::json::{Json, check_keys, insert};

pub(crate) type Obj = Vec<(String, Json)>;

pub(crate) struct Mapping {
    pub project: Vec<String>,
    pub default_project: String,
    run_type: Vec<RunTypeRule>,
    run_type_when_present: (Vec<String>, Option<RunType>),
    model: Vec<String>,
    provider: Vec<String>,
    prompt_tokens: Vec<String>,
    completion_tokens: Vec<String>,
    session: Vec<String>,
    drop: Vec<String>,
    inputs: PayloadRule,
    outputs: PayloadRule,
    event_attributes_skipped: Vec<String>,
}

struct RunTypeRule {
    attribute: String,
    ignore_case: bool,
    values: Vec<(String, RunType)>,
    otherwise: Option<RunType>,
}

struct PayloadRule {
    fields: Vec<(String, String)>,
    indexed_messages: Vec<(String, String)>,
    message_events: Vec<(String, Option<String>)>,
    value: Vec<String>,
    value_key: String,
}

/// What the mapping produced for one span.
#[derive(Debug, Default)]
pub(crate) struct Derived {
    pub run_type: Option<RunType>,
    pub model: Option<String>,
    pub provider: Option<String>,
    pub session: Option<String>,
    pub prompt_tokens: Option<u32>,
    pub completion_tokens: Option<u32>,
    pub inputs: Option<Json>,
    pub outputs: Option<Json>,
}

// --- Loading -------------------------------------------------------------------------------

fn strings(v: &Json, key: &str) -> Result<Vec<String>, String> {
    match v.get(key) {
        None => Ok(Vec::new()),
        Some(Json::Array(a)) => a
            .iter()
            .map(|s| {
                s.as_str()
                    .map(str::to_string)
                    .ok_or_else(|| format!("`{key}` must hold strings"))
            })
            .collect(),
        Some(_) => Err(format!("`{key}` must be a list of strings")),
    }
}

fn object<'a>(v: &'a Json, key: &str) -> Result<&'a [(String, Json)], String> {
    match v.get(key) {
        None => Ok(&[]),
        Some(o) => o
            .as_object()
            .ok_or_else(|| format!("`{key}` must be an object")),
    }
}

fn run_type(s: &str) -> Result<RunType, String> {
    RunType::parse(s).ok_or_else(|| format!("unknown run_type {s:?}"))
}

fn opt_run_type(v: Option<&Json>) -> Result<Option<RunType>, String> {
    match v {
        None | Some(Json::Null) => Ok(None),
        Some(Json::String(s)) => run_type(s).map(Some),
        Some(_) => Err("run_type must be a string".into()),
    }
}

fn payload_rule(v: &Json) -> Result<PayloadRule, String> {
    let str_map = |o: &[(String, Json)], what: &str| -> Result<Vec<(String, String)>, String> {
        o.iter()
            .map(|(k, v)| {
                v.as_str()
                    .map(|s| (k.clone(), s.to_string()))
                    .ok_or_else(|| format!("`{what}` values must be strings"))
            })
            .collect()
    };
    let indexed = match v.get("indexed_messages") {
        None => Vec::new(),
        Some(Json::Array(a)) => a
            .iter()
            .map(
                |r| match (r.get("prefix").and_then(Json::as_str), r.get("inner")) {
                    (Some(p), None) => Ok((p.to_string(), String::new())),
                    (Some(p), Some(Json::String(i))) => Ok((p.to_string(), i.clone())),
                    _ => Err(
                        "`indexed_messages` entries need a string `prefix` and `inner`".to_string(),
                    ),
                },
            )
            .collect::<Result<_, _>>()?,
        Some(_) => return Err("`indexed_messages` must be a list".into()),
    };
    let events = object(v, "message_events")?
        .iter()
        .map(|(k, v)| match v {
            Json::Null => Ok((k.clone(), None)),
            Json::String(r) => Ok((k.clone(), Some(r.clone()))),
            _ => Err("`message_events` values must be a role string or null".to_string()),
        })
        .collect::<Result<_, _>>()?;
    Ok(PayloadRule {
        fields: str_map(object(v, "fields")?, "fields")?,
        indexed_messages: indexed,
        message_events: events,
        value: strings(v, "value")?,
        value_key: v
            .get("value_key")
            .and_then(Json::as_str)
            .unwrap_or("value")
            .to_string(),
    })
}

impl Mapping {
    pub fn parse(bytes: &[u8]) -> Result<Mapping, String> {
        let v = Json::parse(bytes)?;
        check_keys(
            &v,
            &[
                "project",
                "default_project",
                "run_type",
                "run_type_when_present",
                "model",
                "provider",
                "prompt_tokens",
                "completion_tokens",
                "session",
                "drop",
                "inputs",
                "outputs",
                "event_attributes_skipped",
            ],
        )?;
        for k in ["inputs", "outputs"] {
            if let Some(p) = v.get(k) {
                check_keys(
                    p,
                    &[
                        "fields",
                        "indexed_messages",
                        "message_events",
                        "value",
                        "value_key",
                    ],
                )
                .map_err(|e| format!("`{k}`: {e}"))?;
            }
        }
        let mut rules = Vec::new();
        for r in v
            .get("run_type")
            .and_then(Json::as_array)
            .unwrap_or_default()
        {
            let attribute = r
                .get("attribute")
                .and_then(Json::as_str)
                .ok_or("`run_type` rules need an `attribute`")?
                .to_string();
            let ignore_case = r
                .get("ignore_case")
                .and_then(Json::as_bool)
                .unwrap_or(false);
            let values = object(r, "values")?
                .iter()
                .map(|(k, t)| {
                    let k = if ignore_case {
                        k.to_ascii_lowercase()
                    } else {
                        k.clone()
                    };
                    Ok((
                        k,
                        run_type(t.as_str().ok_or("run_type values must be strings")?)?,
                    ))
                })
                .collect::<Result<_, String>>()?;
            rules.push(RunTypeRule {
                attribute,
                ignore_case,
                values,
                otherwise: opt_run_type(r.get("otherwise"))?,
            });
        }
        let when = v.get("run_type_when_present").unwrap_or(&Json::Null);
        let field = |k: &str| -> Result<PayloadRule, String> {
            payload_rule(v.get(k).unwrap_or(&Json::Object(Vec::new())))
                .map_err(|e| format!("`{k}`: {e}"))
        };
        Ok(Mapping {
            project: strings(&v, "project")?,
            default_project: v
                .get("default_project")
                .and_then(Json::as_str)
                .unwrap_or("default")
                .to_string(),
            run_type: rules,
            run_type_when_present: (
                strings(when, "attributes")?,
                opt_run_type(when.get("run_type"))?,
            ),
            model: strings(&v, "model")?,
            provider: strings(&v, "provider")?,
            prompt_tokens: strings(&v, "prompt_tokens")?,
            completion_tokens: strings(&v, "completion_tokens")?,
            session: strings(&v, "session")?,
            drop: strings(&v, "drop")?,
            inputs: field("inputs")?,
            outputs: field("outputs")?,
            event_attributes_skipped: strings(&v, "event_attributes_skipped")?,
        })
    }

    // --- Deriving --------------------------------------------------------------------------

    /// Maps `attrs` in place, removing what it consumes. `events` are the span's events as
    /// `(name, attributes)`, used for message events.
    pub fn derive(&self, attrs: &mut Obj, events: &[(String, Obj)]) -> Derived {
        let mut d = Derived {
            run_type: self.map_run_type(attrs),
            model: take_str(attrs, &self.model),
            provider: take_str(attrs, &self.provider),
            prompt_tokens: take_u32(attrs, &self.prompt_tokens),
            completion_tokens: take_u32(attrs, &self.completion_tokens),
            session: take_str(attrs, &self.session),
            ..Derived::default()
        };
        for k in &self.drop {
            remove(attrs, k);
        }
        d.inputs = self.payload(&self.inputs, attrs, events);
        d.outputs = self.payload(&self.outputs, attrs, events);
        d
    }

    fn map_run_type(&self, attrs: &Obj) -> Option<RunType> {
        for r in &self.run_type {
            let Some(v) = get(attrs, &r.attribute)
                .and_then(Json::as_str)
                .filter(|s| !s.is_empty())
            else {
                continue;
            };
            let v = if r.ignore_case {
                v.to_ascii_lowercase()
            } else {
                v.to_string()
            };
            if let Some((_, t)) = r.values.iter().find(|(k, _)| *k == v) {
                return Some(*t);
            }
            if r.otherwise.is_some() {
                return r.otherwise;
            }
        }
        let (keys, t) = &self.run_type_when_present;
        if keys.iter().any(|k| {
            get(attrs, k)
                .and_then(Json::as_str)
                .is_some_and(|s| !s.is_empty())
        }) {
            return *t;
        }
        None
    }

    fn payload(
        &self,
        rule: &PayloadRule,
        attrs: &mut Obj,
        events: &[(String, Obj)],
    ) -> Option<Json> {
        let mut out: Obj = Vec::new();
        for (attr, key) in &rule.fields {
            if let Some(v) = remove(attrs, attr) {
                insert(&mut out, key.clone(), json_or_string(v));
            }
        }
        let has_messages = |o: &Obj| o.iter().any(|(k, _)| k == "messages");
        if !has_messages(&out) {
            for (prefix, inner) in &rule.indexed_messages {
                if let Some(m) = take_indexed(attrs, prefix, inner) {
                    out.push(("messages".into(), Json::Array(m)));
                    break;
                }
            }
        }
        if !has_messages(&out) {
            let msgs: Vec<Json> = events
                .iter()
                .filter_map(|(name, a)| {
                    let (_, role) = rule.message_events.iter().find(|(n, _)| n == name)?;
                    let mut m: Obj = Vec::new();
                    if let Some(role) = role {
                        m.push(("role".into(), Json::String(role.clone())));
                    }
                    for (k, v) in a {
                        if !self.event_attributes_skipped.contains(k) {
                            insert(&mut m, k.clone(), json_or_string(v.clone()));
                        }
                    }
                    Some(Json::Object(m))
                })
                .collect();
            if !msgs.is_empty() {
                out.push(("messages".into(), Json::Array(msgs)));
            }
        }
        let mut value = None;
        for k in &rule.value {
            if let Some(v) = remove(attrs, k) {
                value = Some(v);
                break;
            }
        }
        if let Some(v) = value {
            match json_or_string(v) {
                Json::Object(m) if out.is_empty() => out = m,
                other => insert(&mut out, rule.value_key.clone(), other),
            }
        }
        (!out.is_empty()).then_some(Json::Object(out))
    }
}

pub(crate) fn get<'a>(attrs: &'a Obj, key: &str) -> Option<&'a Json> {
    attrs.iter().find(|(k, _)| k == key).map(|(_, v)| v)
}

fn remove(attrs: &mut Obj, key: &str) -> Option<Json> {
    let i = attrs.iter().position(|(k, _)| k == key)?;
    Some(attrs.remove(i).1)
}

fn take_str(attrs: &mut Obj, keys: &[String]) -> Option<String> {
    for k in keys {
        let s = match get(attrs, k) {
            Some(Json::String(s)) if !s.is_empty() => s.clone(),
            Some(Json::Number(n)) => n.clone(),
            _ => continue,
        };
        remove(attrs, k);
        return Some(s);
    }
    None
}

fn take_u32(attrs: &mut Obj, keys: &[String]) -> Option<u32> {
    for k in keys {
        let n = match get(attrs, k) {
            Some(v @ Json::Number(_)) => v
                .as_i64()
                .or_else(|| v.as_f64().filter(|f| f.is_finite()).map(|f| f as i64)),
            Some(Json::String(s)) => s.trim().parse::<i64>().ok(),
            _ => None,
        };
        if let Some(n) = n.and_then(|n| u32::try_from(n).ok()) {
            remove(attrs, k);
            return Some(n);
        }
    }
    None
}

/// A string holding a JSON object or array becomes that value; anything else stays.
pub(crate) fn json_or_string(v: Json) -> Json {
    match v {
        Json::String(s) => {
            let t = s.trim_start();
            if t.starts_with('{') || t.starts_with('[') {
                Json::parse(s.as_bytes()).unwrap_or(Json::String(s))
            } else {
                Json::String(s)
            }
        }
        other => other,
    }
}

/// Collects `<prefix><i>.<inner><path>` attributes into a list of objects ordered by `i`.
fn take_indexed(attrs: &mut Obj, prefix: &str, inner: &str) -> Option<Vec<Json>> {
    let mut items: Vec<(usize, Obj)> = Vec::new();
    let mut i = 0;
    while i < attrs.len() {
        let parsed = attrs[i].0.strip_prefix(prefix).and_then(|rest| {
            let (idx, field) = rest.split_once('.')?;
            let idx = idx.parse::<usize>().ok()?;
            Some((idx, field.strip_prefix(inner).unwrap_or(field).to_string()))
        });
        let Some((idx, field)) = parsed else {
            i += 1;
            continue;
        };
        let v = json_or_string(attrs.remove(i).1);
        let slot = match items.iter().position(|(j, _)| *j == idx) {
            Some(p) => p,
            None => {
                items.push((idx, Vec::new()));
                items.len() - 1
            }
        };
        insert_path(&mut items[slot].1, &field, v);
    }
    if items.is_empty() {
        return None;
    }
    items.sort_by_key(|(i, _)| *i);
    Some(items.into_iter().map(|(_, o)| Json::Object(o)).collect())
}

/// Inserts `a.b.c = v` into nested objects.
fn insert_path(obj: &mut Obj, path: &str, v: Json) {
    match path.split_once('.') {
        None => insert(obj, path.to_string(), v),
        Some((head, tail)) => {
            let pos = match obj.iter().position(|(k, _)| k == head) {
                Some(p) => p,
                None => {
                    obj.push((head.to_string(), Json::Object(Vec::new())));
                    obj.len() - 1
                }
            };
            match &mut obj[pos].1 {
                Json::Object(child) => insert_path(child, tail, v),
                _ => insert(obj, path.to_string(), v),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn builtin() -> Mapping {
        Mapping::parse(include_bytes!("../data/mapping.json")).unwrap()
    }

    fn attrs(s: &str) -> Obj {
        Json::parse(s.as_bytes())
            .unwrap()
            .as_object()
            .unwrap()
            .to_vec()
    }

    fn text(v: &Option<Json>) -> String {
        String::from_utf8(v.as_ref().unwrap().to_bytes()).unwrap()
    }

    fn keys(a: &Obj) -> Vec<&str> {
        a.iter().map(|(k, _)| k.as_str()).collect()
    }

    #[test]
    fn genai_semconv() {
        let mut a = attrs(
            r#"{"gen_ai.operation.name":"chat","gen_ai.provider.name":"openai",
            "gen_ai.request.model":"gpt-4o","gen_ai.response.model":"gpt-4o-2024",
            "gen_ai.usage.input_tokens":100,"gen_ai.usage.output_tokens":"20",
            "gen_ai.input.messages":"[{\"role\":\"user\",\"content\":\"refund please\"}]",
            "gen_ai.conversation.id":"conv-1","http.method":"POST"}"#,
        );
        let d = builtin().derive(&mut a, &[]);
        assert_eq!(d.run_type, Some(RunType::ChatModel));
        assert_eq!(d.model.as_deref(), Some("gpt-4o"));
        assert_eq!(d.provider.as_deref(), Some("openai"));
        assert_eq!(
            (d.prompt_tokens, d.completion_tokens),
            (Some(100), Some(20))
        );
        assert_eq!(d.session.as_deref(), Some("conv-1"));
        assert_eq!(
            text(&d.inputs),
            r#"{"messages":[{"role":"user","content":"refund please"}]}"#
        );
        assert_eq!(d.outputs, None);
        assert_eq!(keys(&a), ["gen_ai.operation.name", "http.method"]);
    }

    #[test]
    fn openinference() {
        let mut a = attrs(
            r#"{"openinference.span.kind":"LLM","llm.model_name":"claude-sonnet-4",
            "llm.provider":"anthropic","llm.token_count.prompt":5,"llm.token_count.completion":7,
            "llm.input_messages.1.message.role":"user","llm.input_messages.1.message.content":"hi",
            "llm.input_messages.0.message.role":"system","llm.input_messages.0.message.content":"be nice",
            "llm.output_messages.0.message.role":"assistant","llm.output_messages.0.message.content":"hello",
            "llm.output_messages.0.message.tool_calls.0.tool_call.function.name":"f",
            "session.id":"s1"}"#,
        );
        let d = builtin().derive(&mut a, &[]);
        assert_eq!(d.run_type, Some(RunType::Llm));
        assert_eq!((d.prompt_tokens, d.completion_tokens), (Some(5), Some(7)));
        // Index order, not attribute order.
        assert_eq!(
            text(&d.inputs),
            r#"{"messages":[{"role":"system","content":"be nice"},{"role":"user","content":"hi"}]}"#
        );
        assert_eq!(
            text(&d.outputs),
            r#"{"messages":[{"role":"assistant","content":"hello","tool_calls":{"0":{"tool_call":{"function":{"name":"f"}}}}}]}"#
        );
        assert_eq!(keys(&a), ["openinference.span.kind"]);
    }

    #[test]
    fn openllmetry_and_value_payloads() {
        let m = builtin();
        let mut a = attrs(
            r#"{"llm.request.type":"chat","gen_ai.system":"OpenAI","gen_ai.request.model":"gpt-4o-mini",
            "gen_ai.prompt.0.role":"user","gen_ai.prompt.0.content":"What is 2+2?",
            "gen_ai.completion.0.content":"4","gen_ai.usage.prompt_tokens":9}"#,
        );
        let d = m.derive(&mut a, &[]);
        assert_eq!(d.run_type, Some(RunType::ChatModel));
        assert_eq!(d.provider.as_deref(), Some("OpenAI"));
        assert_eq!(text(&d.outputs), r#"{"messages":[{"content":"4"}]}"#);

        let mut wf = attrs(
            r#"{"traceloop.span.kind":"workflow","traceloop.entity.input":"{\"q\":\"refund\"}",
            "traceloop.entity.output":"done"}"#,
        );
        let d = m.derive(&mut wf, &[]);
        assert_eq!(d.run_type, Some(RunType::Chain));
        assert_eq!(text(&d.inputs), r#"{"q":"refund"}"#);
        assert_eq!(text(&d.outputs), r#"{"output":"done"}"#);
    }

    #[test]
    fn run_type_precedence_and_fallbacks() {
        let m = builtin();
        let rt = |s: &str| m.derive(&mut attrs(s), &[]).run_type;
        // GenAI beats OpenInference.
        assert_eq!(
            rt(r#"{"gen_ai.operation.name":"execute_tool","openinference.span.kind":"LLM"}"#),
            Some(RunType::Tool)
        );
        // An unknown GenAI operation falls through to the next rule.
        assert_eq!(
            rt(r#"{"gen_ai.operation.name":"dance","openinference.span.kind":"agent"}"#),
            Some(RunType::Agent)
        );
        assert_eq!(
            rt(r#"{"langsmith.span.kind":"weird"}"#),
            Some(RunType::Chain)
        );
        assert_eq!(rt(r#"{"langsmith.span.kind":"TOOL"}"#), Some(RunType::Tool));
        assert_eq!(rt(r#"{"llm.model_name":"x"}"#), Some(RunType::Llm));
        assert_eq!(rt(r#"{"http.method":"GET"}"#), None);
    }

    #[test]
    fn tokens_outside_u32_stay_attributes() {
        let mut a = attrs(
            r#"{"gen_ai.usage.input_tokens":-1,"llm.token_count.prompt":"12","gen_ai.usage.output_tokens":4294967296}"#,
        );
        let d = builtin().derive(&mut a, &[]);
        assert_eq!(d.prompt_tokens, Some(12));
        assert_eq!(d.completion_tokens, None);
        assert_eq!(
            keys(&a),
            ["gen_ai.usage.input_tokens", "gen_ai.usage.output_tokens"]
        );
    }

    #[test]
    fn message_events() {
        let mut a = attrs(r#"{"gen_ai.operation.name":"chat"}"#);
        let events = vec![
            (
                "gen_ai.user.message".to_string(),
                attrs(r#"{"content":"hello","gen_ai.system":"openai"}"#),
            ),
            ("other".to_string(), attrs(r#"{"x":1}"#)),
            (
                "gen_ai.choice".to_string(),
                attrs(r#"{"index":0,"message":"{\"content\":\"hey\"}"}"#),
            ),
        ];
        let d = builtin().derive(&mut a, &events);
        assert_eq!(
            text(&d.inputs),
            r#"{"messages":[{"role":"user","content":"hello"}]}"#
        );
        assert_eq!(
            text(&d.outputs),
            r#"{"messages":[{"index":0,"message":{"content":"hey"}}]}"#
        );
    }

    #[test]
    fn rejects_bad_mapping_files() {
        for bad in [
            "[]",
            r#"{"model":"x"}"#,
            r#"{"run_type":[{"attribute":"a","values":{"x":"nope"}}]}"#,
            r#"{"inputs":{"fields":{"a":1}}}"#,
        ] {
            assert!(Mapping::parse(bad.as_bytes()).is_err(), "accepted {bad}");
        }
    }
}
