//! Every layout against the oracle (or a brute-force computation over the same posts):
//! builder answers, reader answers from sealed bytes, and deterministic sealing.

use std::collections::{BTreeMap, BTreeSet, HashMap};

use newdb_layouts::{Batch, FileSet, LayoutBuilder, LayoutFiles, fts, runs_by_time};
use newdb_layouts::{trace_clustered, trace_filter};
use newdb_testkit::Oracle;
use newdb_testkit::r#gen::{GenConfig, Rng, generate};
use newdb_types::header::field;
use newdb_types::text::tokens;
use newdb_types::{
    PayloadRef, Run, RunQuery, RunSummary, RunType, Span, SpanHeader, SpanId, Status, Store,
    TraceId, TraceQuery,
};

/// Fake partition-local string table and payload store.
#[derive(Default, Clone)]
struct Strings {
    ids: HashMap<String, u32>,
    names: Vec<String>,
    payloads: HashMap<Vec<u8>, u64>,
}

impl Strings {
    fn id(&mut self, s: &str) -> u32 {
        if let Some(&i) = self.ids.get(s) {
            return i;
        }
        self.names.push(s.to_string());
        let i = self.names.len() as u32;
        self.ids.insert(s.to_string(), i);
        i
    }

    fn name(&self, id: u32) -> String {
        self.names[id as usize - 1].clone()
    }

    fn payload(&mut self, bytes: &[u8]) -> PayloadRef {
        let n = self.payloads.len() as u64;
        let offset = *self.payloads.entry(bytes.to_vec()).or_insert(n * 1000 + 1);
        PayloadRef {
            offset,
            len: bytes.len().max(1) as u32,
            raw_len: bytes.len() as u32,
        }
    }
}

/// The header the engine would write for `s`. With `junk`, fields `s` does not set hold
/// garbage, so a layout that ignores `fields` fails.
fn header(s: &Span, version: u64, st: &mut Strings, junk: bool) -> SpanHeader {
    let j = if junk { 0xdead } else { 0 };
    let mut h = SpanHeader {
        trace_id: s.trace_id,
        span_id: s.span_id,
        project: st.id(&s.project),
        version,
        parent_span_id: SpanId([j as u8; 8]),
        start_ns: j,
        end_ns: j,
        cost_micro_usd: j,
        name: j as u32,
        model: j as u32,
        session: j as u32,
        prompt_tokens: j as u32,
        completion_tokens: j as u32,
        status: j as u8 & 1,
        run_type: j as u8 & 3,
        ..SpanHeader::default()
    };
    macro_rules! set {
        ($bit:expr, $f:ident, $v:expr) => {{
            h.$f = $v;
            h.fields |= $bit;
        }};
    }
    if let Some(p) = s.parent_span_id {
        set!(field::PARENT, parent_span_id, p);
    }
    if let Some(v) = &s.name {
        set!(field::NAME, name, st.id(v));
    }
    if let Some(v) = s.start_ns {
        set!(field::START, start_ns, v);
    }
    if let Some(v) = s.end_ns {
        set!(field::END, end_ns, v);
    }
    if let Some(v) = s.status {
        set!(field::STATUS, status, v as u8);
    }
    if let Some(v) = s.run_type {
        set!(field::RUN_TYPE, run_type, v as u8);
    }
    if let Some(v) = &s.model {
        set!(field::MODEL, model, st.id(v));
    }
    if let Some(v) = &s.provider {
        set!(field::PROVIDER, provider, st.id(v));
    }
    if let Some(v) = &s.session {
        set!(field::SESSION, session, st.id(v));
    }
    if let Some(v) = s.prompt_tokens {
        set!(field::PROMPT_TOKENS, prompt_tokens, v);
    }
    if let Some(v) = s.completion_tokens {
        set!(field::COMPLETION_TOKENS, completion_tokens, v);
    }
    if let Some(v) = s.cost_micro_usd {
        set!(field::COST, cost_micro_usd, v);
    }
    if let Some(v) = &s.inputs {
        set!(field::INPUTS, inputs, st.payload(v));
    }
    if let Some(v) = &s.outputs {
        set!(field::OUTPUTS, outputs, st.payload(v));
    }
    if let Some(v) = &s.attributes {
        set!(field::ATTRIBUTES, attributes, st.payload(v));
    }
    if let Some(v) = &s.events {
        set!(field::EVENTS, events, st.payload(v));
    }
    if let Some(v) = &s.error {
        set!(field::ERROR, error, st.payload(v.as_bytes()));
    }
    h
}

/// One partition's posts, as the engine hands them to builders.
struct Partition {
    posts: Vec<Span>,
    headers: Vec<SpanHeader>,
    strings: Strings,
}

impl Partition {
    fn new(posts: &[Span]) -> Partition {
        let mut strings = Strings::default();
        let posts: Vec<Span> = posts
            .iter()
            .enumerate()
            .map(|(i, s)| Span {
                version: i as u64 + 1,
                ..s.clone()
            })
            .collect();
        let headers = posts
            .iter()
            .map(|s| header(s, s.version, &mut strings, true))
            .collect();
        Partition {
            posts,
            headers,
            strings,
        }
    }

    /// Feeds every post to `b`, in batches of `sizes` posts (summing to the post count).
    fn feed(&self, b: &mut dyn LayoutBuilder, sizes: &[usize]) {
        let mut first = 0;
        for &n in sizes {
            b.apply(&Batch {
                first_post: first as u32,
                headers: &self.headers[first..first + n],
                spans: &self.posts[first..first + n],
            });
            first += n;
        }
        assert_eq!(first, self.posts.len());
    }

    /// Batch splits a builder must not notice (verify and recovery replay the log in other
    /// chunks than the live commits): one batch, single posts, and random cut points.
    fn splits(&self) -> Vec<Vec<usize>> {
        let n = self.posts.len();
        let mut out = vec![vec![n], vec![1; n]];
        for seed in 0..3 {
            let mut rng = Rng::new(seed ^ n as u64);
            let (mut sizes, mut left) = (Vec::new(), n);
            while left > 0 {
                let k = 1 + rng.below(left.min(50) as u64) as usize;
                sizes.push(k);
                left -= k;
            }
            out.push(sizes);
        }
        out
    }

    fn oracle(&self) -> Oracle {
        let o = Oracle::new();
        o.ingest(self.posts.clone()).unwrap();
        o
    }

    fn trace_ids(&self) -> BTreeSet<TraceId> {
        self.posts.iter().map(|s| s.trace_id).collect()
    }
}

type Sealed = Vec<(String, Vec<u64>, usize)>;

/// One builder per split of `p`'s posts (see [`Partition::splits`]). With `same_bytes`,
/// checks that sealing twice and sealing every split give byte-identical files. Returns the
/// builders and the first one's files copied to 8-aligned memory, as the engine maps them.
fn built<B: LayoutBuilder + Default>(p: &Partition, same_bytes: bool) -> (Vec<B>, Sealed) {
    let builders: Vec<B> = p
        .splits()
        .iter()
        .map(|sizes| {
            let mut b = B::default();
            p.feed(&mut b, sizes);
            b
        })
        .collect();
    let seal = |b: &B| {
        let mut files = LayoutFiles::default();
        b.seal(&mut files).unwrap();
        files
    };
    let files = seal(&builders[0]);
    if same_bytes {
        assert_eq!(files, seal(&builders[0]), "sealing twice differs");
        for (b, sizes) in builders.iter().zip(p.splits()).skip(1) {
            assert_eq!(
                files,
                seal(b),
                "batch sizes {sizes:?} change the sealed bytes"
            );
        }
    }
    let sealed = files
        .files
        .into_iter()
        .map(|(n, bytes)| {
            let mut words = vec![0u64; bytes.len().div_ceil(8)];
            bytemuck::cast_slice_mut::<u64, u8>(&mut words)[..bytes.len()].copy_from_slice(&bytes);
            (n, words, bytes.len())
        })
        .collect();
    (builders, sealed)
}

fn with_files<R>(sealed: &Sealed, f: impl FnOnce(FileSet<'_>) -> R) -> R {
    let files: Vec<(&str, &[u8])> = sealed
        .iter()
        .map(|(n, w, len)| (n.as_str(), &bytemuck::cast_slice::<u64, u8>(w)[..*len]))
        .collect();
    f(FileSet { files: &files })
}

fn workloads() -> Vec<Vec<Span>> {
    let mut out: Vec<Vec<Span>> = (1..=3).map(|s| generate(&GenConfig::small(s))).collect();
    out.push(generate(&GenConfig {
        seed: 9,
        traces: 120,
        projects: 3,
        max_spans_per_trace: 60,
        payload_median: 64,
        open_then_close_rate: 0.4,
        duplicate_rate: 0.1,
        error_rate: 0.2,
        ..GenConfig::default()
    }));
    out
}

/// Each workload as one partition, and split in two so closing posts land in the second
/// partition without their open post.
fn partitions() -> Vec<Partition> {
    let mut out = Vec::new();
    for posts in workloads() {
        let mid = posts.len() / 2;
        out.push(Partition::new(&posts));
        out.push(Partition::new(&posts[..mid]));
        out.push(Partition::new(&posts[mid..]));
    }
    out
}

#[test]
fn trace_clustered_matches_oracle() {
    for p in partitions() {
        let (builders, sealed) = built::<trace_clustered::Builder>(&p, true);
        let oracle = p.oracle();
        with_files(&sealed, |files| {
            let r = trace_clustered::Reader::open(files).unwrap();
            assert_eq!(r.len(), oracle.len());
            let mut strings = p.strings.clone();
            for t in p.trace_ids().into_iter().chain([TraceId([0xee; 16])]) {
                let (built, cost) = builders[0].trace(t);
                let (read, _) = r.trace(t);
                assert_eq!(built, read, "builder and reader differ for {t:?}");
                assert_eq!(cost.records_visited, built.len() as u64);
                for b in &builders[1..] {
                    assert_eq!(b.trace(t).0, built, "batch split changes trace {t:?}");
                }
                let mut expected = Vec::new();
                for project in ["project-0", "project-1", "project-2"] {
                    let spans = oracle
                        .trace(&TraceQuery {
                            project: project.into(),
                            trace_id: t,
                            payloads: true,
                            timings: false,
                        })
                        .unwrap()
                        .value;
                    expected.extend(
                        spans
                            .iter()
                            .map(|s| header(s, s.version, &mut strings, false)),
                    );
                }
                expected.sort_by_key(|h| (h.start_ns, h.span_id, h.project));
                assert_eq!(built, expected, "trace {t:?}");
            }
        });
    }
}

fn run_of(s: &RunSummary, st: &Strings) -> Run {
    let has = |f: u32| s.fields as u32 & f != 0;
    let opt = |f: u32, id: u32| has(f).then(|| st.name(id));
    Run {
        project: st.name(s.project),
        trace_id: s.trace_id,
        span_id: s.span_id,
        name: opt(field::NAME, s.name),
        start_ns: s.start_ns,
        end_ns: has(field::END).then_some(s.end_ns),
        status: Status::from_u8(s.status).unwrap(),
        run_type: RunType::from_u8(s.run_type).unwrap(),
        model: opt(field::MODEL, s.model),
        session: opt(field::SESSION, s.session),
        prompt_tokens: s.prompt_tokens,
        completion_tokens: s.completion_tokens,
        cost_micro_usd: s.cost_micro_usd,
    }
}

#[test]
fn runs_by_time_matches_oracle() {
    let mut error_runs = 0;
    for p in partitions() {
        let (builders, sealed) = built::<runs_by_time::Builder>(&p, true);
        let expected = p
            .oracle()
            .runs(&RunQuery {
                limit: usize::MAX,
                ..Default::default()
            })
            .unwrap()
            .value;
        assert!(!expected.is_empty());
        let (runs, cost) = builders[0].runs();
        assert_eq!(cost.records_visited, runs.len() as u64);
        let got: Vec<Run> = runs.iter().map(|s| run_of(s, &p.strings)).collect();
        assert_eq!(got, expected);
        let errors: Vec<u32> = (0..expected.len() as u32)
            .filter(|&i| expected[i as usize].status == Status::Error)
            .collect();
        error_runs += errors.len();
        for b in &builders {
            assert_eq!(b.runs().0, runs, "batch split changes runs");
            assert_eq!(b.error_positions().0, errors);
        }
        with_files(&sealed, |files| {
            let r = runs_by_time::Reader::open(files).unwrap();
            assert_eq!(r.runs(), runs.as_slice());
            assert_eq!(r.error_positions(), errors.as_slice());
        });
    }
    assert!(error_runs > 10);
}

#[test]
fn trace_filter_has_no_false_negatives() {
    for p in partitions() {
        let (builders, sealed) = built::<trace_filter::Builder>(&p, true);
        let r = with_files(&sealed, |files| trace_filter::Reader::open(files).unwrap());
        for t in p.trace_ids() {
            assert!(r.may_contain(t));
            assert!(builders.iter().all(|b| b.may_contain(t)));
        }
        let absent: Vec<TraceId> = (0..10_000u64)
            .map(|i| {
                let mut id = [0x5a; 16];
                id[..8].copy_from_slice(&i.wrapping_mul(0x9e37_79b9_7f4a_7c15).to_le_bytes());
                TraceId(id)
            })
            .collect();
        assert!(
            absent
                .iter()
                .all(|&t| builders.iter().all(|b| !b.may_contain(t)))
        );
        let fp = absent.iter().filter(|&&t| r.may_contain(t)).count();
        assert!(fp < 100, "{fp} false positives in 10000");
    }
}

#[test]
fn trace_filter_handles_tiny_and_empty_partitions() {
    for n in [0usize, 1, 2, 3, 5, 100] {
        let posts: Vec<Span> = (0..n)
            .map(|i| Span {
                project: "p".into(),
                trace_id: TraceId([i as u8 + 1; 16]),
                span_id: SpanId([1; 8]),
                ..Span::default()
            })
            .collect();
        let p = Partition::new(&posts);
        let (builders, sealed) = built::<trace_filter::Builder>(&p, true);
        let r = with_files(&sealed, |files| trace_filter::Reader::open(files).unwrap());
        assert!(p.trace_ids().iter().all(|&t| r.may_contain(t)), "n = {n}");
        assert!(p.trace_ids().iter().all(|&t| builders[0].may_contain(t)));
        if n == 0 {
            assert!(!r.may_contain(TraceId([1; 16])) && !builders[0].may_contain(TraceId([1; 16])));
        }
    }
}

/// Brute force: token → spans with a post whose inputs or outputs contain it.
fn brute_postings(posts: &[Span]) -> BTreeMap<Vec<u8>, BTreeSet<(TraceId, SpanId)>> {
    let mut out: BTreeMap<Vec<u8>, BTreeSet<(TraceId, SpanId)>> = BTreeMap::new();
    for s in posts {
        for text in [&s.inputs, &s.outputs].into_iter().flatten() {
            for t in tokens(text) {
                out.entry(t).or_default().insert((s.trace_id, s.span_id));
            }
        }
    }
    out
}

#[test]
fn fts_postings_match_brute_force() {
    for p in partitions() {
        let (builders, sealed) = built::<fts::Builder>(&p, true);
        let expected = brute_postings(&p.posts);
        assert!(expected.len() > 30);
        with_files(&sealed, |files| {
            let r = fts::Reader::open(files).unwrap();
            let missing: [&[u8]; 4] = [b"nonexistentword", b"", b"refun", b"REFUND"];
            for (token, want) in expected
                .iter()
                .map(|(t, w)| (t.as_slice(), w.iter().copied().collect::<Vec<_>>()))
                .chain(missing.map(|t| (t, vec![])))
            {
                let (built, cost) = builders[0].postings(token);
                for b in &builders[1..] {
                    assert_eq!(b.postings(token).0, built, "batch split changes postings");
                }
                assert_eq!(
                    built,
                    want,
                    "builder, token {:?}",
                    String::from_utf8_lossy(token)
                );
                assert_eq!(cost.records_visited, want.len() as u64);
                let (read, _) = r.postings(token);
                assert_eq!(
                    read,
                    want,
                    "reader, token {:?}",
                    String::from_utf8_lossy(token)
                );
            }
        });
    }
}

#[test]
fn spans_sharing_ids_across_projects_stay_separate() {
    let span = |project: &str, start: u64, status: Status| Span {
        project: project.into(),
        trace_id: TraceId([7; 16]),
        span_id: SpanId([8; 8]),
        start_ns: Some(start),
        status: Some(status),
        inputs: Some(format!("{{\"q\":\"{project}\"}}").into_bytes()),
        ..Span::default()
    };
    let p = Partition::new(&[
        span("a", 5, Status::Ok),
        span("b", 5, Status::Error),
        span("a", 5, Status::Error),
    ]);
    let mut tc = trace_clustered::Builder::default();
    let mut rb = runs_by_time::Builder::default();
    let mut ft = fts::Builder::default();
    p.feed(&mut tc, &[1, 2]);
    p.feed(&mut rb, &[1, 2]);
    p.feed(&mut ft, &[1, 2]);
    let (spans, _) = tc.trace(TraceId([7; 16]));
    assert_eq!(spans.len(), 2);
    assert_eq!(spans.iter().map(|h| h.version).collect::<Vec<_>>(), [3, 2]);
    let (runs, _) = rb.runs();
    assert_eq!(runs.iter().map(|r| r.project).collect::<Vec<_>>(), [2, 1]);
    assert_eq!(rb.error_positions().0, [0, 1]);
    assert_eq!(ft.postings(b"a").0, [(TraceId([7; 16]), SpanId([8; 8]))]);
    assert_eq!(ft.postings(b"q").0.len(), 1);
}

#[test]
fn a_later_parent_removes_a_run_and_a_later_root_adds_it_back() {
    let post = |parent: Option<SpanId>, start: Option<u64>| Span {
        project: "p".into(),
        trace_id: TraceId([1; 16]),
        span_id: SpanId([2; 8]),
        parent_span_id: parent,
        start_ns: start,
        ..Span::default()
    };
    let posts = [
        post(None, Some(10)),
        post(Some(SpanId([3; 8])), None),
        post(None, Some(20)),
        post(Some(SpanId([0; 8])), None),
    ];
    let mut counts = Vec::new();
    for n in 1..=posts.len() {
        let p = Partition::new(&posts[..n]);
        let mut b = runs_by_time::Builder::default();
        p.feed(&mut b, &[n]);
        counts.push(b.runs().0.iter().map(|r| r.start_ns).collect::<Vec<_>>());
    }
    // An all-zero parent still means root, as in `SpanHeader::is_root`.
    assert_eq!(counts, [vec![10], vec![], vec![], vec![20]]);
}

#[test]
fn readers_reject_truncated_and_foreign_files() {
    let p = Partition::new(&generate(&GenConfig::small(4)));
    for def in newdb_layouts::LAYOUTS {
        let mut b = (def.new_builder)();
        p.feed(b.as_mut(), &[p.posts.len()]);
        let mut files = LayoutFiles::default();
        b.seal(&mut files).unwrap();
        let (name, bytes) = &files.files[0];
        for cut in [0, 8, 20, bytes.len() / 2, bytes.len() - 1] {
            let mut words = vec![0u64; cut.div_ceil(8)];
            bytemuck::cast_slice_mut::<u64, u8>(&mut words)[..cut].copy_from_slice(&bytes[..cut]);
            let bytes = &bytemuck::cast_slice::<u64, u8>(&words)[..cut];
            let set = [(name.as_str(), bytes)];
            let files = FileSet { files: &set };
            let ok = match def.name {
                trace_clustered::NAME => trace_clustered::Reader::open(files).is_ok(),
                runs_by_time::NAME => runs_by_time::Reader::open(files).is_ok(),
                trace_filter::NAME => trace_filter::Reader::open(files).is_ok(),
                fts::NAME => fts::Reader::open(files).is_ok(),
                other => panic!("unknown layout {other}"),
            };
            assert!(!ok, "{} opened a file cut to {cut} bytes", def.name);
        }
    }
}
