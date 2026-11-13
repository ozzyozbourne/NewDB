//! Deterministic generator of agent-shaped span posts. Same seed, same posts, on every
//! machine. No `rand` dependency: a SplitMix64 generator is enough here.
//!
//! What it produces, per trace:
//! - a tree up to `max_depth` deep with fan-out up to `max_fanout`, rooted at an agent span;
//! - chat_model, tool, retriever and chain spans with tokens, cost and a model name;
//! - JSON payloads around `payload_median` bytes, with a few large ones;
//! - a share of spans posted twice: once open (start only), once closed (end, status,
//!   outputs), with the closing post delivered later in the stream;
//! - a share of duplicate posts, and out-of-order delivery within a window.

use newdb_types::{RunType, Span, SpanId, Status, TraceId};

#[derive(Clone, Debug)]
pub struct GenConfig {
    pub seed: u64,
    pub traces: usize,
    pub projects: usize,
    pub max_depth: u32,
    pub max_fanout: u32,
    /// Upper bound on spans per trace; the tree stops growing at this size.
    pub max_spans_per_trace: usize,
    pub payload_median: usize,
    /// Probability (0..1) that a payload is large (`large_payload` bytes).
    pub large_payload_rate: f64,
    pub large_payload: usize,
    /// Probability that a span is posted open first and closed later.
    pub open_then_close_rate: f64,
    pub duplicate_rate: f64,
    pub error_rate: f64,
    /// Posts are shuffled within windows of this many posts.
    pub reorder_window: usize,
    /// Start time of the first trace, in ns since the epoch.
    pub base_ns: u64,
    /// Gap between consecutive trace starts.
    pub trace_gap_ns: u64,
}

impl Default for GenConfig {
    fn default() -> Self {
        GenConfig {
            seed: 42,
            traces: 100,
            projects: 2,
            max_depth: 6,
            max_fanout: 4,
            max_spans_per_trace: 200,
            payload_median: 512,
            large_payload_rate: 0.002,
            large_payload: 256 * 1024,
            open_then_close_rate: 0.2,
            duplicate_rate: 0.02,
            error_rate: 0.05,
            reorder_window: 64,
            base_ns: 1_800_000_000_000_000_000,
            trace_gap_ns: 50_000_000,
        }
    }
}

impl GenConfig {
    /// A small workload for unit tests: 20 traces, at most 30 spans each.
    pub fn small(seed: u64) -> GenConfig {
        GenConfig {
            seed,
            traces: 20,
            max_spans_per_trace: 30,
            payload_median: 128,
            ..Default::default()
        }
    }
}

/// SplitMix64.
#[derive(Clone, Debug)]
pub struct Rng(u64);

impl Rng {
    pub fn new(seed: u64) -> Rng {
        Rng(seed ^ 0x9e37_79b9_7f4a_7c15)
    }

    pub fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }

    /// Uniform in `0..n` (n > 0).
    pub fn below(&mut self, n: u64) -> u64 {
        self.next_u64() % n
    }

    pub fn chance(&mut self, p: f64) -> bool {
        ((self.next_u64() >> 11) as f64 / (1u64 << 53) as f64) < p
    }

    pub fn pick<'a, T>(&mut self, xs: &'a [T]) -> &'a T {
        &xs[self.below(xs.len() as u64) as usize]
    }
}

const WORDS: &[&str] = &[
    "refund",
    "policy",
    "order",
    "customer",
    "invoice",
    "shipping",
    "delay",
    "weather",
    "search",
    "summary",
    "contract",
    "renewal",
    "password",
    "reset",
    "billing",
    "account",
    "upgrade",
    "latency",
    "database",
    "error",
    "timeout",
    "retry",
    "report",
    "quarterly",
    "revenue",
    "translate",
    "spanish",
    "calendar",
    "meeting",
    "agenda",
    "flight",
    "hotel",
    "booking",
];
const MODELS: &[&str] = &[
    "gpt-6-sol",
    "claude-opus-5-5",
    "gemini-4-carbon",
    "llama-5-70b",
];
const TOOLS: &[&str] = &[
    "web_search",
    "sql_query",
    "send_email",
    "read_file",
    "calculator",
];

/// Generates the full post stream for `cfg`, in delivery order.
pub fn generate(cfg: &GenConfig) -> Vec<Span> {
    let mut rng = Rng::new(cfg.seed);
    let mut posts = Vec::new();
    let mut late = Vec::new();
    for t in 0..cfg.traces {
        let trace_id = TraceId(id_bytes::<16>(&mut rng));
        let project = format!("project-{}", t % cfg.projects.max(1));
        let session =
            (rng.chance(0.5)).then(|| format!("session-{}", rng.below(cfg.traces as u64 / 3 + 1)));
        let start = cfg.base_ns + t as u64 * cfg.trace_gap_ns;
        let mut count = 0usize;
        gen_span(
            cfg, &mut rng, &project, trace_id, None, 0, start, &session, &mut count, &mut posts,
            &mut late,
        );
        // Deliver some closing posts a few traces later.
        if t % 3 == 2 {
            posts.append(&mut late);
        }
    }
    posts.append(&mut late);
    reorder(&mut rng, &mut posts, cfg.reorder_window);
    posts
}

#[allow(clippy::too_many_arguments)]
fn gen_span(
    cfg: &GenConfig,
    rng: &mut Rng,
    project: &str,
    trace_id: TraceId,
    parent: Option<SpanId>,
    depth: u32,
    start: u64,
    session: &Option<String>,
    count: &mut usize,
    posts: &mut Vec<Span>,
    late: &mut Vec<Span>,
) -> u64 {
    *count += 1;
    let span_id = SpanId(id_bytes::<8>(rng));
    let run_type = if depth == 0 {
        RunType::Agent
    } else {
        *rng.pick(&[
            RunType::ChatModel,
            RunType::ChatModel,
            RunType::Tool,
            RunType::Retriever,
            RunType::Chain,
        ])
    };
    let mut end = start + 1_000_000 + rng.below(50_000_000);
    if depth < cfg.max_depth && matches!(run_type, RunType::Agent | RunType::Chain) {
        let kids = 1 + rng.below(cfg.max_fanout as u64);
        let mut t = start + 100_000;
        for _ in 0..kids {
            if *count >= cfg.max_spans_per_trace {
                break;
            }
            t = gen_span(
                cfg,
                rng,
                project,
                trace_id,
                Some(span_id),
                depth + 1,
                t,
                session,
                count,
                posts,
                late,
            ) + 10_000;
        }
        end = end.max(t);
    }
    let is_llm = run_type == RunType::ChatModel;
    let status = if rng.chance(cfg.error_rate) {
        Status::Error
    } else {
        Status::Ok
    };
    let name = match run_type {
        RunType::Agent => "agent".to_string(),
        RunType::Tool => rng.pick(TOOLS).to_string(),
        RunType::Retriever => "retrieve".to_string(),
        RunType::Chain => "plan".to_string(),
        _ => "chat".to_string(),
    };
    let full = Span {
        project: project.to_string(),
        trace_id,
        span_id,
        parent_span_id: parent,
        name: Some(name),
        start_ns: Some(start),
        end_ns: Some(end),
        status: Some(status),
        run_type: Some(run_type),
        model: is_llm.then(|| rng.pick(MODELS).to_string()),
        provider: is_llm.then(|| "openai".to_string()),
        session: session.clone(),
        prompt_tokens: is_llm.then(|| 50 + rng.below(4000) as u32),
        completion_tokens: is_llm.then(|| 10 + rng.below(1500) as u32),
        cost_micro_usd: is_llm.then(|| rng.below(50_000)),
        inputs: Some(payload(cfg, rng, "input")),
        outputs: Some(payload(cfg, rng, "output")),
        attributes: Some(format!("{{\"depth\":{depth}}}").into_bytes()),
        events: None,
        error: (status == Status::Error).then(|| "tool raised TimeoutError".to_string()),
        version: 0,
    };
    if rng.chance(cfg.open_then_close_rate) {
        let mut open = full.clone();
        open.end_ns = None;
        open.status = Some(Status::Unset);
        open.outputs = None;
        open.error = None;
        open.prompt_tokens = None;
        open.completion_tokens = None;
        open.cost_micro_usd = None;
        let close = Span {
            end_ns: full.end_ns,
            status: full.status,
            outputs: full.outputs.clone(),
            error: full.error.clone(),
            prompt_tokens: full.prompt_tokens,
            completion_tokens: full.completion_tokens,
            cost_micro_usd: full.cost_micro_usd,
            ..id_only(&full)
        };
        posts.push(open);
        late.push(close);
    } else {
        if rng.chance(cfg.duplicate_rate) {
            posts.push(full.clone());
        }
        posts.push(full);
    }
    end
}

fn id_only(s: &Span) -> Span {
    Span {
        project: s.project.clone(),
        trace_id: s.trace_id,
        span_id: s.span_id,
        ..Span::default()
    }
}

fn payload(cfg: &GenConfig, rng: &mut Rng, role: &str) -> Vec<u8> {
    let target = if rng.chance(cfg.large_payload_rate) {
        cfg.large_payload
    } else {
        // Roughly log-normal-ish around the median: median/4 .. median*4.
        let m = cfg.payload_median.max(16) as u64;
        (m / 4 + rng.below(m) + if rng.chance(0.2) { rng.below(3 * m) } else { 0 }) as usize
    };
    let mut text = String::with_capacity(target + 16);
    while text.len() < target {
        if !text.is_empty() {
            text.push(' ');
        }
        text.push_str(rng.pick(WORDS));
        if rng.chance(0.05) {
            text.push_str("\\n");
        }
    }
    format!("{{\"role\":\"{role}\",\"content\":\"{text}\"}}").into_bytes()
}

fn reorder(rng: &mut Rng, posts: &mut [Span], window: usize) {
    if window < 2 {
        return;
    }
    for chunk in posts.chunks_mut(window) {
        for i in (1..chunk.len()).rev() {
            let j = rng.below(i as u64 + 1) as usize;
            chunk.swap(i, j);
        }
    }
}

fn id_bytes<const N: usize>(rng: &mut Rng) -> [u8; N] {
    let mut out = [0u8; N];
    for c in out.chunks_mut(8) {
        let v = rng.next_u64().to_le_bytes();
        c.copy_from_slice(&v[..c.len()]);
    }
    if out == [0u8; N] {
        out[0] = 1;
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn same_seed_same_stream_and_closes_follow_opens() {
        let cfg = GenConfig::small(7);
        let a = generate(&cfg);
        assert_eq!(a, generate(&cfg));
        assert_ne!(a, generate(&GenConfig::small(8)));
        let opens = a
            .iter()
            .filter(|s| s.start_ns.is_some() && s.end_ns.is_none())
            .count();
        let closes = a
            .iter()
            .filter(|s| s.start_ns.is_none() && s.end_ns.is_some())
            .count();
        assert!(opens > 0);
        assert_eq!(opens, closes);
        assert!(
            a.iter()
                .filter(|s| s.parent_span_id.is_none() && s.name.is_some())
                .count()
                >= 20
        );
    }
}
