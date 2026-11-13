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

use newdb_testkit::r#gen::{GenConfig, generate};
use newdb_types::{
    Error, Result, RunQuery, RunType, SearchQuery, SpanId, Status, Store, TraceId, TraceQuery,
};

/// What a bench run does.
#[derive(Clone, Debug)]
pub struct BenchConfig {
    pub workload: GenConfig,
    /// Posts per `ingest` call.
    pub batch: usize,
    /// Repetitions of each read, for p50/p99.
    pub iterations: usize,
}

impl Default for BenchConfig {
    fn default() -> Self {
        BenchConfig {
            workload: GenConfig::default(),
            batch: 1000,
            iterations: 50,
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct Latency {
    pub p50: Duration,
    pub p99: Duration,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct BenchReport {
    pub posts: u64,
    pub ingest_posts_per_sec: f64,
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
    let posts = generate(&config.workload);
    let total = posts.len() as u64;

    // The largest trace by distinct spans, found before the posts move into the store.
    let mut spans_per_trace: HashMap<(&str, TraceId), Vec<SpanId>> = HashMap::new();
    for p in &posts {
        spans_per_trace
            .entry((p.project.as_str(), p.trace_id))
            .or_default()
            .push(p.span_id);
    }
    let (project, trace_id) = spans_per_trace
        .into_iter()
        .map(|(k, mut ids)| {
            ids.sort_unstable();
            ids.dedup();
            (ids.len(), k)
        })
        .max()
        .map(|(_, (p, t))| (p.to_string(), t))
        .ok_or_else(|| Error::InvalidInput("bench workload has no posts".into()))?;

    CountingAlloc::enable();
    let allocs_before_ingest = CountingAlloc::count();
    let mut posts = posts;
    let batch = config.batch.max(1);
    let t0 = Instant::now();
    while !posts.is_empty() {
        let rest = posts.split_off(batch.min(posts.len()));
        store.ingest(std::mem::replace(&mut posts, rest))?;
    }
    let ingest_secs = t0.elapsed().as_secs_f64();
    let counting = CountingAlloc::count() > allocs_before_ingest;

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
    let mut report = BenchReport {
        posts: total,
        ingest_posts_per_sec: if ingest_secs > 0.0 {
            total as f64 / ingest_secs
        } else {
            0.0
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
    Ok(report)
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
        let _ = write!(
            s,
            "{{\n  \"posts\": {},\n  \"ingest_posts_per_sec\": {:?},\n  \"reads\": {{",
            self.posts, rate
        );
        for (i, (name, l)) in self.reads.iter().enumerate() {
            let _ = write!(
                s,
                "{}\n    {}: {{\"p50_ns\": {}, \"p99_ns\": {}}}",
                if i == 0 { "" } else { "," },
                json_string(name),
                l.p50.as_nanos(),
                l.p99.as_nanos()
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
                "ingest_posts_per_sec" => r.ingest_posts_per_sec = v.f64(k)?,
                "reads" => {
                    for (name, v) in v.object(k)? {
                        let mut l = Latency::default();
                        for (f, v) in v.object(name)? {
                            match f.as_str() {
                                "p50_ns" => l.p50 = Duration::from_nanos(v.u64(f)?),
                                "p99_ns" => l.p99 = Duration::from_nanos(v.u64(f)?),
                                _ => {}
                            }
                        }
                        r.reads.push((name.clone(), l));
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
        let mut rows: Vec<[String; 4]> = vec![
            ["".into(), "a".into(), "b".into(), "change".into()],
            [
                "ingest posts/s".into(),
                format!("{:.0}", self.ingest_posts_per_sec),
                format!("{:.0}", other.ingest_posts_per_sec),
                change(self.ingest_posts_per_sec, other.ingest_posts_per_sec),
            ],
        ];
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
                for word in ["true", "false", "null"] {
                    if self.s[self.i..].starts_with(word.as_bytes()) {
                        self.i += word.len();
                        return Ok(Json::Other);
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
            ingest_posts_per_sec: 123_456.789_012_345,
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
               "ingest_posts_per_sec": 1.5e3, "posts": 10} "#,
        )
        .unwrap();
        assert_eq!(r.posts, 10);
        assert_eq!(r.ingest_posts_per_sec, 1500.0);
        assert_eq!(r.reads[0].0, "ab");
        assert_eq!(r.reads[0].1.p50, Duration::from_nanos(2));
        for bad in [
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
