//! Tools over any [`Store`] (DESIGN.md section 9): the bench runner and the counting
//! allocator. Works against `newdb_testkit::Oracle` now and `newdb_engine::Db` after
//! integration. The CLI prints `inspect` and `verify` reports itself.
//!
//! `bench` output is one JSON object (written by hand, no serde) so two runs can be compared
//! with `newdb bench --compare a.json b.json`:
//!
//! ```text
//! {"posts":1234,"ingest_posts_per_sec":98765.4,
//!  "reads":{"trace_largest_with_payloads":{"p50_ns":1200,"p99_ns":3400},...},
//!  "allocations":{"trace_largest_with_payloads":57,...}}
//! ```

use std::alloc::{GlobalAlloc, Layout, System};
use std::collections::HashMap;
use std::fmt::Write as _;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use newdb_testkit::r#gen::{GenConfig, Rng, generate};
use newdb_types::{
    Error, Result, RunQuery, RunType, SearchQuery, Span, SpanId, Status, Store, TraceId, TraceQuery,
};

/// What a bench run does.
#[derive(Clone, Debug)]
pub struct BenchConfig {
    pub workload: GenConfig,
    /// Posts per `ingest` call.
    pub batch: usize,
    /// Repetitions of each read, for p50/p99.
    pub iterations: usize,
    /// When set, `target_traces` traces of exactly this many spans are spread evenly
    /// through the workload, and the `trace_largest_*` reads load the first (oldest) of
    /// them instead of the largest generated trace.
    pub target_trace_spans: Option<usize>,
    pub target_traces: usize,
    /// Call the seal hook of [`run_bench_with`] after every this many posts (0: never).
    pub seal_every: usize,
    /// Call the seal hook once more after the ingest, before the reads.
    pub seal_at_end: bool,
}

impl Default for BenchConfig {
    fn default() -> Self {
        BenchConfig {
            workload: GenConfig::default(),
            batch: 1000,
            iterations: 50,
            target_trace_spans: None,
            target_traces: 3,
            seal_every: 0,
            seal_at_end: false,
        }
    }
}

impl BenchConfig {
    /// A named workload. `forge` is the Forge Trace DB / xitdb comparison of DESIGN.md
    /// section 1: about 1.2M posts with 2 KB median payloads, 20 traces of exactly 2,000
    /// spans, sealed every 100k posts.
    pub fn preset(name: &str) -> Option<BenchConfig> {
        match name {
            "forge" => Some(BenchConfig {
                workload: GenConfig {
                    traces: FORGE_TRACES,
                    payload_median: 2048,
                    ..GenConfig::default()
                },
                target_trace_spans: Some(2000),
                target_traces: 20,
                seal_every: 100_000,
                ..BenchConfig::default()
            }),
            _ => None,
        }
    }
}

/// Generated traces in `--preset forge`; with 2 KB payloads they make about 1.2M posts.
const FORGE_TRACES: usize = 158_500;

#[derive(Clone, Debug, Default, PartialEq)]
pub struct Latency {
    pub p50: Duration,
    pub p99: Duration,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct BenchReport {
    pub posts: u64,
    /// Partitions after the run and bytes in the data directory; `None` for stores without
    /// them (the oracle). The CLI fills these in.
    pub partitions: Option<u64>,
    pub bytes_on_disk: Option<u64>,
    /// Peak resident set size of the bench process at the end of the run (`VmHWM` from
    /// `/proc/self/status`; `None` where that file does not exist, such as macOS).
    pub peak_rss_bytes: Option<u64>,
    /// Spans the `trace_largest_*` reads return.
    pub trace_spans: u64,
    /// Posts per second spent in `ingest` calls (generation and seals not counted).
    pub ingest_posts_per_sec: f64,
    /// Seal hook calls and their latency.
    pub seals: u64,
    pub seal: Latency,
    /// `(read name, latency)`, e.g. `("trace_largest_with_payloads", ...)`,
    /// `("runs_newest_50", ...)`, `("runs_errors_50", ...)`, `("search_refund_policy", ...)`.
    pub reads: Vec<(String, Latency)>,
    /// Allocations per read, when the counting allocator is installed.
    pub allocations: Vec<(String, u64)>,
}

/// One standard read, run `iterations` times.
type ReadFn<'a> = Box<dyn Fn() -> Result<()> + 'a>;

/// Ingests the workload into `store`, then times the standard reads.
///
/// The reads: the largest trace with and without payloads, the newest 50 runs, the newest
/// 50 error runs, the newest 50 agent runs slower than 20 ms (two filters), and a search
/// for "refund policy". Allocation counts are the median per read and are reported only
/// when [`CountingAlloc`] is the global allocator.
pub fn run_bench(store: &dyn Store, config: &BenchConfig) -> Result<BenchReport> {
    run_bench_with(store, config, &mut || Ok(()))
}

/// [`run_bench`] with a seal hook, called (and timed, outside the ingest rate) after every
/// `config.seal_every` posts and, with `config.seal_at_end`, once after the ingest. The CLI
/// passes `Db::seal_active`, so the reads walk sealed, mapped partitions.
pub fn run_bench_with(
    store: &dyn Store,
    config: &BenchConfig,
    seal: &mut dyn FnMut() -> Result<()>,
) -> Result<BenchReport> {
    let batch = config.batch.max(1);
    let mut total = 0u64;
    let mut ingest_time = Duration::ZERO;
    let mut since_seal = 0usize;
    let mut seal_times = Vec::new();
    let mut timed_seal = |seal_times: &mut Vec<Duration>| -> Result<()> {
        let t = Instant::now();
        seal()?;
        seal_times.push(t.elapsed());
        Ok(())
    };
    let mut send = |posts: Vec<Span>| -> Result<()> {
        let n = posts.len();
        let t = Instant::now();
        store.ingest(posts)?;
        ingest_time += t.elapsed();
        total += n as u64;
        since_seal += n;
        if config.seal_every > 0 && since_seal >= config.seal_every {
            since_seal = 0;
            timed_seal(&mut seal_times)?;
        }
        Ok(())
    };

    // Generated a chunk at a time, so a workload larger than memory streams through.
    let mut largest: Option<(usize, String, TraceId)> = None;
    let mut target: Option<(String, TraceId)> = None;
    CountingAlloc::enable();
    let allocs_before_ingest = CountingAlloc::count();
    let mut pending: Vec<Span> = Vec::new();
    for chunk in chunks(config) {
        let (is_target, posts) = chunk?;
        if is_target {
            if target.is_none() {
                target = posts.first().map(|p| (p.project.clone(), p.trace_id));
            }
        } else {
            largest = largest.max(largest_trace(&posts));
        }
        pending.extend(posts);
        while pending.len() >= batch {
            let rest = pending.split_off(batch);
            send(std::mem::replace(&mut pending, rest))?;
        }
    }
    if !pending.is_empty() {
        send(pending)?;
    }
    let counting = CountingAlloc::count() > allocs_before_ingest;
    if config.seal_at_end {
        timed_seal(&mut seal_times)?;
    }
    let (project, trace_id) = target
        .or(largest.map(|(_, p, t)| (p, t)))
        .ok_or_else(|| Error::InvalidInput("bench workload has no posts".into()))?;
    let ingest_secs = ingest_time.as_secs_f64();

    let trace = |payloads: bool| TraceQuery {
        project: project.clone(),
        trace_id,
        payloads,
        timings: false,
    };
    let runs = |q: RunQuery| RunQuery { limit: 50, ..q };
    let reads: Vec<(&str, ReadFn<'_>)> = vec![
        (
            "trace_largest_with_payloads",
            Box::new(|| store.trace(&trace(true)).map(drop)),
        ),
        (
            "trace_largest_no_payloads",
            Box::new(|| store.trace(&trace(false)).map(drop)),
        ),
        (
            "runs_newest_50",
            Box::new(|| store.runs(&runs(RunQuery::default())).map(drop)),
        ),
        (
            "runs_errors_50",
            Box::new(|| {
                let q = RunQuery {
                    status: Some(Status::Error),
                    ..Default::default()
                };
                store.runs(&runs(q)).map(drop)
            }),
        ),
        (
            "runs_agent_over_20ms_50",
            Box::new(|| {
                let q = RunQuery {
                    run_type: Some(RunType::Agent),
                    min_latency_ns: Some(20_000_000),
                    ..Default::default()
                };
                store.runs(&runs(q)).map(drop)
            }),
        ),
        (
            "search_refund_policy",
            Box::new(|| {
                let q = SearchQuery {
                    text: "refund policy".into(),
                    limit: 50,
                    ..Default::default()
                };
                store.search(&q).map(drop)
            }),
        ),
    ];

    let iterations = config.iterations.max(1);
    seal_times.sort_unstable();
    let mut report = BenchReport {
        posts: total,
        trace_spans: store.trace(&trace(false))?.value.len() as u64,
        ingest_posts_per_sec: if ingest_secs > 0.0 {
            total as f64 / ingest_secs
        } else {
            0.0
        },
        seals: seal_times.len() as u64,
        seal: if seal_times.is_empty() {
            Latency::default()
        } else {
            Latency {
                p50: percentile(&seal_times, 50),
                p99: percentile(&seal_times, 99),
            }
        },
        ..Default::default()
    };
    for (name, read) in &reads {
        read()?; // warm-up
        let mut times = Vec::with_capacity(iterations);
        let mut allocs = Vec::with_capacity(iterations);
        for _ in 0..iterations {
            let a = CountingAlloc::count();
            let t = Instant::now();
            read()?;
            times.push(t.elapsed());
            allocs.push(CountingAlloc::count() - a);
        }
        times.sort_unstable();
        allocs.sort_unstable();
        report.reads.push((
            name.to_string(),
            Latency {
                p50: percentile(&times, 50),
                p99: percentile(&times, 99),
            },
        ));
        if counting {
            report
                .allocations
                .push((name.to_string(), percentile(&allocs, 50)));
        }
    }
    report.peak_rss_bytes = peak_rss_bytes();
    Ok(report)
}

/// `VmHWM` of this process in bytes, on Linux.
fn peak_rss_bytes() -> Option<u64> {
    let status = std::fs::read_to_string("/proc/self/status").ok()?;
    let kb = status
        .lines()
        .find_map(|l| l.strip_prefix("VmHWM:"))?
        .trim()
        .strip_suffix("kB")?
        .trim();
    kb.parse::<u64>().ok().map(|kb| kb * 1024)
}

/// Traces per generated chunk.
const CHUNK_TRACES: usize = 2000;

/// The bench workload in delivery order, a chunk of posts at a time; `true` marks a target
/// trace. Chunk `i` is `generate` over traces `i * CHUNK_TRACES ..`, with its own seed and
/// start times continuing the previous chunk's, so a workload that fits in one chunk is
/// exactly `generate(&config.workload)`. Target traces come before the chunk where they
/// fall when spread evenly, starting with that chunk's first trace.
fn chunks(config: &BenchConfig) -> impl Iterator<Item = Result<(bool, Vec<Span>)>> + '_ {
    let w = &config.workload;
    let n = w.traces.div_ceil(CHUNK_TRACES).max(1);
    let targets = match config.target_trace_spans {
        Some(_) => config.target_traces,
        None => 0,
    };
    (0..n).flat_map(move |i| {
        let first = i * CHUNK_TRACES;
        let base_ns = w.base_ns + first as u64 * w.trace_gap_ns;
        let targets = (0..targets)
            .filter(move |k| k * n / targets == i)
            .map(move |k| target_trace(config, k, base_ns).map(|p| (true, p)));
        let cfg = GenConfig {
            seed: if i == 0 {
                w.seed
            } else {
                mix(w.seed, i as u64)
            },
            traces: w.traces.saturating_sub(first).min(CHUNK_TRACES),
            base_ns,
            ..w.clone()
        };
        targets.chain(std::iter::once_with(move || Ok((false, generate(&cfg)))))
    })
}

/// Target trace `k`: one generated trace of exactly `target_trace_spans` spans. A root
/// fan-out with no practical limit makes the tree grow until the span cap; the rare draw
/// that stops short is retried with the next seed.
fn target_trace(config: &BenchConfig, k: usize, base_ns: u64) -> Result<Vec<Span>> {
    let n = config.target_trace_spans.unwrap_or(0);
    if n == 0 {
        return Err(Error::InvalidInput(
            "target trace spans must be at least 1".into(),
        ));
    }
    for attempt in 0..16u64 {
        let cfg = GenConfig {
            seed: mix(config.workload.seed, (1 << 40) + k as u64 * 16 + attempt),
            traces: 1,
            max_spans_per_trace: n,
            max_depth: 2,
            max_fanout: u32::MAX,
            base_ns,
            ..config.workload.clone()
        };
        let posts = generate(&cfg);
        if largest_trace(&posts).is_some_and(|(spans, _, _)| spans == n) {
            return Ok(posts);
        }
    }
    Err(Error::InvalidInput(format!(
        "could not generate a trace of exactly {n} spans"
    )))
}

fn mix(seed: u64, salt: u64) -> u64 {
    Rng::new(seed ^ salt.wrapping_mul(0x9e37_79b9_7f4a_7c15)).next_u64()
}

/// The trace with the most distinct spans: `(spans, project, trace_id)`, ties broken by
/// the larger `(project, trace_id)`.
fn largest_trace(posts: &[Span]) -> Option<(usize, String, TraceId)> {
    let mut spans_per_trace: HashMap<(&str, TraceId), Vec<SpanId>> = HashMap::new();
    for p in posts {
        spans_per_trace
            .entry((p.project.as_str(), p.trace_id))
            .or_default()
            .push(p.span_id);
    }
    spans_per_trace
        .into_iter()
        .map(|((p, t), mut ids)| {
            ids.sort_unstable();
            ids.dedup();
            (ids.len(), p.to_string(), t)
        })
        .max()
}

/// Nearest-rank percentile of sorted, non-empty `xs`.
fn percentile<T: Copy>(xs: &[T], p: usize) -> T {
    let rank = (xs.len() * p).div_ceil(100).max(1);
    xs[rank - 1]
}

impl BenchReport {
    pub fn to_json(&self) -> String {
        let mut s = String::new();
        let rate = if self.ingest_posts_per_sec.is_finite() {
            self.ingest_posts_per_sec
        } else {
            0.0
        };
        let opt = |v: Option<u64>| v.map_or("null".to_string(), |v| v.to_string());
        let latency = |l: &Latency| {
            format!(
                "{{\"p50_ns\": {}, \"p99_ns\": {}}}",
                l.p50.as_nanos(),
                l.p99.as_nanos()
            )
        };
        let _ = write!(
            s,
            "{{\n  \"posts\": {},\n  \"partitions\": {},\n  \"bytes_on_disk\": {},\n  \
             \"peak_rss_bytes\": {},\n  \"trace_spans\": {},\n  \"ingest_posts_per_sec\": {:?},\n  \"seals\": {},\n  \
             \"seal\": {},\n  \"reads\": {{",
            self.posts,
            opt(self.partitions),
            opt(self.bytes_on_disk),
            opt(self.peak_rss_bytes),
            self.trace_spans,
            rate,
            self.seals,
            latency(&self.seal)
        );
        for (i, (name, l)) in self.reads.iter().enumerate() {
            let _ = write!(
                s,
                "{}\n    {}: {}",
                if i == 0 { "" } else { "," },
                json_string(name),
                latency(l)
            );
        }
        s.push_str(if self.reads.is_empty() {
            "},\n"
        } else {
            "\n  },\n"
        });
        s.push_str("  \"allocations\": {");
        for (i, (name, n)) in self.allocations.iter().enumerate() {
            let _ = write!(
                s,
                "{}\n    {}: {n}",
                if i == 0 { "" } else { "," },
                json_string(name)
            );
        }
        s.push_str(if self.allocations.is_empty() {
            "}\n}\n"
        } else {
            "\n  }\n}\n"
        });
        s
    }

    pub fn from_json(s: &str) -> Result<BenchReport> {
        let mut p = Parser {
            s: s.as_bytes(),
            i: 0,
        };
        let v = p.value()?;
        p.ws();
        if p.i != p.s.len() {
            return Err(p.err("trailing characters"));
        }
        let top = v.object("report")?;
        let mut r = BenchReport::default();
        for (k, v) in top {
            match k.as_str() {
                "posts" => r.posts = v.u64(k)?,
                "partitions" => r.partitions = v.opt_u64(k)?,
                "bytes_on_disk" => r.bytes_on_disk = v.opt_u64(k)?,
                "peak_rss_bytes" => r.peak_rss_bytes = v.opt_u64(k)?,
                "trace_spans" => r.trace_spans = v.u64(k)?,
                "ingest_posts_per_sec" => r.ingest_posts_per_sec = v.f64(k)?,
                "seals" => r.seals = v.u64(k)?,
                "seal" => r.seal = v.latency(k)?,
                "reads" => {
                    for (name, v) in v.object(k)? {
                        r.reads.push((name.clone(), v.latency(name)?));
                    }
                }
                "allocations" => {
                    for (name, v) in v.object(k)? {
                        r.allocations.push((name.clone(), v.u64(name)?));
                    }
                }
                _ => {}
            }
        }
        Ok(r)
    }

    /// Side-by-side table of two reports with the change in percent.
    pub fn compare(&self, other: &BenchReport) -> String {
        let count = |name: &str, x: Option<u64>, y: Option<u64>| -> [String; 4] {
            [
                name.into(),
                x.map_or("-".into(), |v| v.to_string()),
                y.map_or("-".into(), |v| v.to_string()),
                match (x, y) {
                    (Some(x), Some(y)) => change(x as f64, y as f64),
                    _ => "-".into(),
                },
            ]
        };
        let mut rows: Vec<[String; 4]> = vec![
            ["".into(), "a".into(), "b".into(), "change".into()],
            count("posts", Some(self.posts), Some(other.posts)),
            count("partitions", self.partitions, other.partitions),
            count("bytes on disk", self.bytes_on_disk, other.bytes_on_disk),
            count("peak RSS bytes", self.peak_rss_bytes, other.peak_rss_bytes),
            count(
                "trace spans",
                Some(self.trace_spans),
                Some(other.trace_spans),
            ),
            [
                "ingest posts/s".into(),
                format!("{:.0}", self.ingest_posts_per_sec),
                format!("{:.0}", other.ingest_posts_per_sec),
                change(self.ingest_posts_per_sec, other.ingest_posts_per_sec),
            ],
        ];
        if self.seals + other.seals > 0 {
            rows.push(count("seals", Some(self.seals), Some(other.seals)));
            for (pct, x, y) in [
                ("p50", self.seal.p50, other.seal.p50),
                ("p99", self.seal.p99, other.seal.p99),
            ] {
                rows.push([
                    format!("seal {pct}"),
                    fmt_duration(x),
                    fmt_duration(y),
                    change(x.as_secs_f64(), y.as_secs_f64()),
                ]);
            }
        }
        let mut names: Vec<&str> = self.reads.iter().map(|(n, _)| n.as_str()).collect();
        for (n, _) in &other.reads {
            if !names.contains(&n.as_str()) {
                names.push(n);
            }
        }
        let find =
            |r: &BenchReport, n: &str| r.reads.iter().find(|(m, _)| m == n).map(|x| x.1.clone());
        for n in &names {
            let (a, b) = (find(self, n), find(other, n));
            for (pct, get) in [
                ("p50", (|l: &Latency| l.p50) as fn(&Latency) -> Duration),
                ("p99", |l: &Latency| l.p99),
            ] {
                let (x, y) = (a.as_ref().map(get), b.as_ref().map(get));
                rows.push([
                    format!("{n} {pct}"),
                    x.map_or("-".into(), fmt_duration),
                    y.map_or("-".into(), fmt_duration),
                    match (x, y) {
                        (Some(x), Some(y)) => change(x.as_secs_f64(), y.as_secs_f64()),
                        _ => "-".into(),
                    },
                ]);
            }
        }
        let alloc =
            |r: &BenchReport, n: &str| r.allocations.iter().find(|(m, _)| m == n).map(|x| x.1);
        for n in &names {
            let (x, y) = (alloc(self, n), alloc(other, n));
            if x.is_none() && y.is_none() {
                continue;
            }
            rows.push([
                format!("{n} allocs"),
                x.map_or("-".into(), |v| v.to_string()),
                y.map_or("-".into(), |v| v.to_string()),
                match (x, y) {
                    (Some(x), Some(y)) => change(x as f64, y as f64),
                    _ => "-".into(),
                },
            ]);
        }
        let mut w = [0usize; 4];
        for r in &rows {
            for (w, c) in w.iter_mut().zip(r) {
                *w = (*w).max(c.chars().count());
            }
        }
        let mut out = String::new();
        for r in &rows {
            let _ = writeln!(
                out,
                "{:<w0$}  {:>w1$}  {:>w2$}  {:>w3$}",
                r[0],
                r[1],
                r[2],
                r[3],
                w0 = w[0],
                w1 = w[1],
                w2 = w[2],
                w3 = w[3]
            );
        }
        out
    }
}

/// `b` relative to `a`, as a signed percentage.
fn change(a: f64, b: f64) -> String {
    if a == 0.0 {
        return if b == 0.0 { "0.0%".into() } else { "-".into() };
    }
    format!("{:+.1}%", (b - a) / a * 100.0)
}

fn fmt_duration(d: Duration) -> String {
    let ns = d.as_nanos();
    if ns < 1_000 {
        format!("{ns} ns")
    } else if ns < 1_000_000 {
        format!("{:.1} µs", ns as f64 / 1e3)
    } else if ns < 1_000_000_000 {
        format!("{:.2} ms", ns as f64 / 1e6)
    } else {
        format!("{:.2} s", ns as f64 / 1e9)
    }
}

fn json_string(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            c if (c as u32) < 0x20 => {
                let _ = write!(out, "\\u{:04x}", c as u32);
            }
            c => out.push(c),
        }
    }
    out.push('"');
    out
}

/// The subset of JSON a report needs: objects, strings and numbers (kept as text so
/// integers do not lose precision). Arrays, booleans and null parse and are ignored.
enum Json {
    Object(Vec<(String, Json)>),
    String,
    Number(String),
    Null,
    Other,
}

impl Json {
    fn object(&self, what: &str) -> Result<&[(String, Json)]> {
        match self {
            Json::Object(o) => Ok(o),
            _ => Err(Error::InvalidInput(format!(
                "bench report: {what} is not an object"
            ))),
        }
    }

    fn u64(&self, what: &str) -> Result<u64> {
        match self {
            Json::Number(n) => n.parse().ok(),
            _ => None,
        }
        .ok_or_else(|| Error::InvalidInput(format!("bench report: {what} is not an integer")))
    }

    fn opt_u64(&self, what: &str) -> Result<Option<u64>> {
        match self {
            Json::Null => Ok(None),
            v => v.u64(what).map(Some),
        }
    }

    fn latency(&self, what: &str) -> Result<Latency> {
        let mut l = Latency::default();
        for (f, v) in self.object(what)? {
            match f.as_str() {
                "p50_ns" => l.p50 = Duration::from_nanos(v.u64(f)?),
                "p99_ns" => l.p99 = Duration::from_nanos(v.u64(f)?),
                _ => {}
            }
        }
        Ok(l)
    }

    fn f64(&self, what: &str) -> Result<f64> {
        match self {
            Json::Number(n) => n.parse().ok(),
            _ => None,
        }
        .ok_or_else(|| Error::InvalidInput(format!("bench report: {what} is not a number")))
    }
}

struct Parser<'a> {
    s: &'a [u8],
    i: usize,
}

impl Parser<'_> {
    fn err(&self, what: &str) -> Error {
        Error::InvalidInput(format!("bench report: {what} at byte {}", self.i))
    }

    fn ws(&mut self) {
        while self.s.get(self.i).is_some_and(|b| b.is_ascii_whitespace()) {
            self.i += 1;
        }
    }

    fn eat(&mut self, b: u8) -> bool {
        self.ws();
        if self.s.get(self.i) == Some(&b) {
            self.i += 1;
            true
        } else {
            false
        }
    }

    fn value(&mut self) -> Result<Json> {
        self.ws();
        match self.s.get(self.i) {
            Some(b'{') => {
                self.i += 1;
                let mut fields = Vec::new();
                if self.eat(b'}') {
                    return Ok(Json::Object(fields));
                }
                loop {
                    self.ws();
                    let k = self.string()?;
                    if !self.eat(b':') {
                        return Err(self.err("expected ':'"));
                    }
                    fields.push((k, self.value()?));
                    if self.eat(b'}') {
                        return Ok(Json::Object(fields));
                    }
                    if !self.eat(b',') {
                        return Err(self.err("expected ',' or '}'"));
                    }
                }
            }
            Some(b'[') => {
                self.i += 1;
                if self.eat(b']') {
                    return Ok(Json::Other);
                }
                loop {
                    self.value()?;
                    if self.eat(b']') {
                        return Ok(Json::Other);
                    }
                    if !self.eat(b',') {
                        return Err(self.err("expected ',' or ']'"));
                    }
                }
            }
            Some(b'"') => self.string().map(|_| Json::String),
            Some(b'-' | b'0'..=b'9') => {
                let start = self.i;
                while self
                    .s
                    .get(self.i)
                    .is_some_and(|b| matches!(b, b'-' | b'+' | b'.' | b'e' | b'E' | b'0'..=b'9'))
                {
                    self.i += 1;
                }
                // Only ASCII bytes were consumed, so this cannot fail.
                let n = std::str::from_utf8(&self.s[start..self.i]).unwrap_or_default();
                Ok(Json::Number(n.to_string()))
            }
            _ => {
                for (word, v) in [
                    ("true", Json::Other),
                    ("false", Json::Other),
                    ("null", Json::Null),
                ] {
                    if self.s[self.i..].starts_with(word.as_bytes()) {
                        self.i += word.len();
                        return Ok(v);
                    }
                }
                Err(self.err("unexpected character"))
            }
        }
    }

    fn string(&mut self) -> Result<String> {
        if self.s.get(self.i) != Some(&b'"') {
            return Err(self.err("expected a string"));
        }
        self.i += 1;
        let mut out = Vec::new();
        loop {
            let b = *self
                .s
                .get(self.i)
                .ok_or_else(|| self.err("unterminated string"))?;
            self.i += 1;
            match b {
                b'"' => break,
                b'\\' => {
                    let e = *self.s.get(self.i).ok_or_else(|| self.err("bad escape"))?;
                    self.i += 1;
                    match e {
                        b'n' => out.push(b'\n'),
                        b't' => out.push(b'\t'),
                        b'r' => out.push(b'\r'),
                        b'b' => out.push(8),
                        b'f' => out.push(12),
                        b'"' | b'\\' | b'/' => out.push(e),
                        b'u' => {
                            let hex = self
                                .s
                                .get(self.i..self.i + 4)
                                .and_then(|h| std::str::from_utf8(h).ok())
                                .and_then(|h| u32::from_str_radix(h, 16).ok())
                                .ok_or_else(|| self.err("bad \\u escape"))?;
                            self.i += 4;
                            let c = char::from_u32(hex).unwrap_or(char::REPLACEMENT_CHARACTER);
                            out.extend_from_slice(c.encode_utf8(&mut [0; 4]).as_bytes());
                        }
                        _ => return Err(self.err("bad escape")),
                    }
                }
                _ => out.push(b),
            }
        }
        String::from_utf8(out).map_err(|_| self.err("string is not UTF-8"))
    }
}

/// A `GlobalAlloc` wrapper that counts allocations. Install with `#[global_allocator]` in
/// bench builds only.
///
/// Counting starts at the first [`CountingAlloc::enable`] (which [`run_bench`] calls), so a
/// binary that installs it but never benches pays one relaxed load per allocation.
pub struct CountingAlloc;

static ENABLED: AtomicBool = AtomicBool::new(false);
static COUNT: AtomicU64 = AtomicU64::new(0);

impl CountingAlloc {
    /// Allocations since counting was enabled (0 when this is not the global allocator).
    pub fn count() -> u64 {
        COUNT.load(Ordering::Relaxed)
    }

    /// Starts counting. Idempotent.
    pub fn enable() {
        ENABLED.store(true, Ordering::Relaxed);
    }

    #[inline]
    fn tick() {
        if ENABLED.load(Ordering::Relaxed) {
            COUNT.fetch_add(1, Ordering::Relaxed);
        }
    }
}

// SAFETY: every method forwards to `System` with the caller's arguments unchanged.
unsafe impl GlobalAlloc for CountingAlloc {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        Self::tick();
        unsafe { System.alloc(layout) }
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        Self::tick();
        unsafe { System.alloc_zeroed(layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        Self::tick();
        unsafe { System.realloc(ptr, layout, new_size) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }
}

#[cfg(test)]
#[global_allocator]
static ALLOC: CountingAlloc = CountingAlloc;

#[cfg(test)]
mod tests {
    use newdb_testkit::Oracle;

    use super::*;

    fn small() -> BenchConfig {
        BenchConfig {
            workload: GenConfig::small(3),
            batch: 7,
            iterations: 5,
            ..BenchConfig::default()
        }
    }

    #[test]
    fn bench_on_the_oracle_ingests_everything_and_times_every_read() {
        let oracle = Oracle::new();
        let cfg = small();
        let report = run_bench(&oracle, &cfg).unwrap();
        let posts = generate(&cfg.workload);
        assert_eq!(report.posts, posts.len() as u64);
        let mut distinct: Vec<_> = posts
            .iter()
            .map(|s| (&s.project, s.trace_id, s.span_id))
            .collect();
        distinct.sort();
        distinct.dedup();
        assert_eq!(oracle.len(), distinct.len());
        assert!(report.ingest_posts_per_sec > 0.0);
        // Within a factor of two of what the kernel reports for this process right now.
        if let Some(rss) = peak_rss_bytes() {
            let r = report.peak_rss_bytes.unwrap();
            assert!(r > 1 << 20 && r <= rss && r * 2 > rss, "{r} vs {rss}");
        }
        let names: Vec<&str> = report.reads.iter().map(|(n, _)| n.as_str()).collect();
        assert_eq!(
            names,
            [
                "trace_largest_with_payloads",
                "trace_largest_no_payloads",
                "runs_newest_50",
                "runs_errors_50",
                "runs_agent_over_20ms_50",
                "search_refund_policy"
            ]
        );
        for (_, l) in &report.reads {
            assert!(l.p50 <= l.p99 && l.p99 > Duration::ZERO);
        }
        // The test binary installs the counting allocator; the oracle clones every span of
        // the trace, so that read allocates.
        assert_eq!(report.allocations.len(), report.reads.len());
        assert!(report.allocations[0].1 > 0);
    }

    #[test]
    fn large_workloads_stream_in_chunks_with_target_traces_spread_through_them() {
        let cfg = BenchConfig {
            workload: GenConfig {
                traces: CHUNK_TRACES + 5,
                ..GenConfig::small(5)
            },
            target_trace_spans: Some(40),
            target_traces: 3,
            ..BenchConfig::default()
        };
        let chunks: Vec<(bool, Vec<Span>)> = chunks(&cfg).map(Result::unwrap).collect();
        let kinds: Vec<bool> = chunks.iter().map(|c| c.0).collect();
        assert_eq!(kinds, [true, true, false, true, false]);

        // The first chunk is exactly what one generate call gives, so small benches are
        // unchanged; later chunks continue the trace numbering and start times.
        let w = &cfg.workload;
        let one = GenConfig {
            traces: CHUNK_TRACES,
            ..w.clone()
        };
        assert_eq!(chunks[2].1, generate(&one));
        let mut roots: Vec<(u64, TraceId)> = chunks
            .iter()
            .filter(|c| !c.0)
            .flat_map(|c| &c.1)
            .filter(|s| s.is_root() && s.start_ns.is_some())
            .map(|s| (s.start_ns.unwrap(), s.trace_id))
            .collect();
        roots.sort();
        roots.dedup();
        let starts: Vec<u64> = roots.iter().map(|r| r.0).collect();
        let want: Vec<u64> = (0..w.traces as u64)
            .map(|t| w.base_ns + t * w.trace_gap_ns)
            .collect();
        assert_eq!(starts, want);

        for (i, (_, posts)) in chunks.iter().enumerate().filter(|c| c.1.0) {
            let (spans, _, _) = largest_trace(posts).unwrap();
            assert_eq!(spans, 40, "target chunk {i}");
            let root = posts.iter().find(|s| s.is_root() && s.start_ns.is_some());
            let chunk_base = if i < 2 { 0 } else { CHUNK_TRACES as u64 };
            assert_eq!(
                root.unwrap().start_ns,
                Some(w.base_ns + chunk_base * w.trace_gap_ns)
            );
        }
        let ids: std::collections::HashSet<TraceId> = chunks
            .iter()
            .flat_map(|c| &c.1)
            .map(|s| s.trace_id)
            .collect();
        assert_eq!(ids.len(), w.traces + 3, "every trace has its own ID");
    }

    #[test]
    fn target_traces_have_exactly_the_requested_spans_and_are_the_ones_read() {
        let cfg = BenchConfig {
            target_trace_spans: Some(2000),
            ..small()
        };
        let posts = target_trace(&cfg, 0, 7).unwrap();
        assert_eq!(largest_trace(&posts).unwrap().0, 2000);

        let cfg = BenchConfig {
            target_trace_spans: Some(50),
            ..small()
        };
        let report = run_bench(&Oracle::new(), &cfg).unwrap();
        // GenConfig::small caps generated traces at 30 spans.
        assert_eq!(report.trace_spans, 50);
        let largest = largest_trace(&generate(&small().workload)).unwrap().0;
        assert_eq!(
            run_bench(&Oracle::new(), &small()).unwrap().trace_spans,
            largest as u64
        );
        let zero = BenchConfig {
            target_trace_spans: Some(0),
            ..small()
        };
        assert!(run_bench(&Oracle::new(), &zero).is_err());
    }

    #[test]
    fn the_seal_hook_runs_every_n_posts_and_once_at_the_end() {
        let oracle = Oracle::new();
        let cfg = BenchConfig {
            batch: 5,
            seal_every: 10,
            ..small()
        };
        let mut seen = Vec::new();
        let report = run_bench_with(&oracle, &cfg, &mut || {
            seen.push(oracle.len());
            Ok(())
        })
        .unwrap();
        assert_eq!(report.seals, report.posts / 10);
        assert_eq!(seen.len() as u64, report.seals);
        assert!(seen.windows(2).all(|w| w[0] < w[1]), "{seen:?}");
        assert!(report.seal.p50 <= report.seal.p99);

        let at_end = BenchConfig {
            seal_at_end: true,
            ..cfg.clone()
        };
        let mut seals = 0;
        let report = run_bench_with(&Oracle::new(), &at_end, &mut || {
            seals += 1;
            Ok(())
        })
        .unwrap();
        assert_eq!(seals, report.posts / 10 + 1);
        let fail = run_bench_with(&Oracle::new(), &cfg, &mut || Err(Error::ReadOnly));
        assert!(matches!(fail, Err(Error::ReadOnly)));
    }

    #[test]
    fn percentile_is_nearest_rank() {
        let xs: Vec<u32> = (1..=200).collect();
        assert_eq!(percentile(&xs, 50), 100);
        assert_eq!(percentile(&xs, 99), 198);
        assert_eq!(percentile(&[7], 99), 7);
        assert_eq!(percentile(&[1, 2, 3], 50), 2);
    }

    fn sample() -> BenchReport {
        BenchReport {
            posts: 18_446_744_073_709_551_615,
            partitions: Some(3),
            bytes_on_disk: None,
            peak_rss_bytes: Some(1 << 30),
            trace_spans: 2000,
            ingest_posts_per_sec: 123_456.789_012_345,
            seals: 12,
            seal: Latency {
                p50: Duration::from_millis(40),
                p99: Duration::from_millis(90),
            },
            reads: vec![
                (
                    "trace \"q\"\\x".into(),
                    Latency {
                        p50: Duration::from_nanos(1_234),
                        p99: Duration::from_nanos(9_007_199_254_740_993),
                    },
                ),
                (
                    "runs_newest_50".into(),
                    Latency {
                        p50: Duration::from_micros(80),
                        p99: Duration::from_micros(120),
                    },
                ),
            ],
            allocations: vec![("runs_newest_50".into(), 51)],
        }
    }

    #[test]
    fn json_round_trips_exactly() {
        let r = sample();
        assert_eq!(BenchReport::from_json(&r.to_json()).unwrap(), r);
        let empty = BenchReport::default();
        assert_eq!(BenchReport::from_json(&empty.to_json()).unwrap(), empty);
    }

    #[test]
    fn json_reader_accepts_other_writers_and_rejects_garbage() {
        let r = BenchReport::from_json(
            r#" {"extra": [1, true, null, {"x": "y"}], "reads": {"a\u0062": {"p99_ns": 5, "p50_ns": 2}},
               "ingest_posts_per_sec": 1.5e3, "posts": 10, "partitions": null, "bytes_on_disk": 7} "#,
        )
        .unwrap();
        assert_eq!(r.posts, 10);
        assert_eq!(r.ingest_posts_per_sec, 1500.0);
        assert_eq!(r.reads[0].0, "ab");
        assert_eq!(r.reads[0].1.p50, Duration::from_nanos(2));
        assert_eq!((r.partitions, r.bytes_on_disk), (None, Some(7)));
        for bad in [
            "{\"partitions\": true}",
            "{\"seal\": 1}",
            "",
            "{",
            "{\"posts\": 1.5}",
            "{\"posts\": 1} x",
            "{\"reads\": 3}",
            "[]",
        ] {
            assert!(BenchReport::from_json(bad).is_err(), "{bad:?}");
        }
    }

    #[test]
    fn compare_shows_both_sides_and_the_change() {
        let a = sample();
        let mut b = sample();
        b.ingest_posts_per_sec = a.ingest_posts_per_sec * 2.0;
        b.partitions = Some(6);
        b.peak_rss_bytes = Some(3 << 29);
        b.reads[1].1.p50 = Duration::from_micros(60);
        b.reads.push(("new_read".into(), Latency::default()));
        let table = a.compare(&b);
        let line = |prefix: &str| {
            table
                .lines()
                .find(|l| l.starts_with(prefix))
                .unwrap_or_else(|| panic!("no {prefix} in\n{table}"))
                .split_whitespace()
                .collect::<Vec<_>>()
                .join(" ")
        };
        assert_eq!(line("ingest"), "ingest posts/s 123457 246914 +100.0%");
        assert_eq!(line("partitions"), "partitions 3 6 +100.0%");
        assert_eq!(line("bytes on disk"), "bytes on disk - - -");
        assert_eq!(
            line("peak RSS"),
            "peak RSS bytes 1073741824 1610612736 +50.0%"
        );
        assert_eq!(line("trace spans"), "trace spans 2000 2000 +0.0%");
        assert_eq!(line("seal p99"), "seal p99 90.00 ms 90.00 ms +0.0%");
        assert_eq!(
            line("runs_newest_50 p50"),
            "runs_newest_50 p50 80.0 µs 60.0 µs -25.0%"
        );
        assert_eq!(
            line("runs_newest_50 p99"),
            "runs_newest_50 p99 120.0 µs 120.0 µs +0.0%"
        );
        assert_eq!(line("new_read p50"), "new_read p50 - 0 ns -");
        assert_eq!(
            line("runs_newest_50 allocs"),
            "runs_newest_50 allocs 51 51 +0.0%"
        );
    }
}
