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
            let newest: Vec<Run> = b.runs_newest().map(|s| run_of(s, &p.strings)).collect();
            assert_eq!(newest, expected, "runs_newest");
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
        let starts = merged_starts(&p.posts);
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
                let newest = newest_first(&want, &starts);
                for b in &builders {
                    assert_eq!(b.postings_newest(token).0.collect::<Vec<_>>(), newest);
                }
                assert_eq!(r.postings_newest(token).0.collect::<Vec<_>>(), newest);
            }
            // Intersections of token pairs (and one triple), newest first.
            let toks: Vec<&[u8]> = expected.keys().map(Vec::as_slice).collect();
            let mut sets: Vec<Vec<&[u8]>> = toks
                .iter()
                .step_by(3)
                .zip(toks.iter().skip(1).step_by(5))
                .map(|(a, b)| vec![*a, *b])
                .collect();
            sets.push(vec![b"refund", b"policy", b"order"]);
            sets.push(vec![b"refund", b"nonexistentword"]);
            sets.push(vec![]);
            let mut nonempty = 0;
            for set in &sets {
                let mut want: Vec<(TraceId, SpanId)> = Vec::new();
                if let Some((first, rest)) = set.split_first() {
                    want = expected
                        .get(*first)
                        .map_or(vec![], |w| w.iter().copied().collect());
                    want.retain(|k| {
                        rest.iter()
                            .all(|t| expected.get(*t).is_some_and(|w| w.contains(k)))
                    });
                }
                nonempty += !want.is_empty() as usize;
                let newest = newest_first(&want, &starts);
                for b in &builders {
                    assert_eq!(
                        b.intersect_newest(set).0.collect::<Vec<_>>(),
                        newest,
                        "{set:?}"
                    );
                }
                assert_eq!(
                    r.intersect_newest(set).0.collect::<Vec<_>>(),
                    newest,
                    "{set:?}"
                );
            }
            assert!(nonempty > 5);
        });
    }
}

/// The engine's way to apply: `prepare` each post, `begin` the batch, move the shards
/// out, apply each on its own thread (in reverse order), put them back shuffled.
fn feed_like_the_engine(p: &Partition, b: &mut fts::Builder, sizes: &[usize]) {
    let mut first = 0;
    for &n in sizes {
        let spans = &p.posts[first..first + n];
        let batch = Batch {
            first_post: first as u32,
            headers: &p.headers[first..first + n],
            spans,
        };
        let prepared: Vec<fts::Prepared> =
            spans.iter().map(|s| fts::prepare(s, b.shards())).collect();
        let ids = b.begin(&batch);
        let mut shards = b.take_shards();
        std::thread::scope(|sc| {
            for shard in shards.iter_mut().rev() {
                let (prepared, ids) = (&prepared, &ids);
                sc.spawn(move || shard.apply(prepared, ids));
            }
        });
        shards.rotate_left(1);
        b.put_shards(shards);
        first += n;
    }
}

/// A sharded fts builder seals the same bytes and answers the same as one shard, for
/// every batch split, through `apply` (the one-batch split takes its multi-threaded path)
/// and through the engine's prepare/begin/shard steps.
#[test]
fn fts_sealed_bytes_do_not_depend_on_the_shard_count() {
    let seal = |b: &fts::Builder| {
        let mut f = LayoutFiles::default();
        b.seal(&mut f).unwrap();
        f
    };
    for p in partitions() {
        let (one, _) = built::<fts::Builder>(&p, true);
        let expected = seal(&one[0]);
        let tokens: Vec<Vec<u8>> = brute_postings(&p.posts).into_keys().collect();
        for shards in [2, 3, 8] {
            for (n, sizes) in p.splits().into_iter().enumerate() {
                let mut b = fts::Builder::with_shards(shards);
                if n % 2 == 0 {
                    p.feed(&mut b, &sizes);
                } else {
                    feed_like_the_engine(&p, &mut b, &sizes);
                }
                assert_eq!(b.shards(), shards);
                assert!(
                    seal(&b) == expected,
                    "{shards} shards, batches {sizes:?}: sealed bytes differ"
                );
                for t in tokens.iter().step_by(7) {
                    assert_eq!(b.postings(t).0, one[0].postings(t).0);
                    let pair: [&[u8]; 2] = [t, b"refund"];
                    assert!(
                        b.intersect_newest(&pair)
                            .0
                            .eq(one[0].intersect_newest(&pair).0),
                        "{shards} shards: intersect {pair:?}"
                    );
                }
            }
        }
    }
}

/// Each span's merged start in the partition: the latest post that set one, else 0.
fn merged_starts(posts: &[Span]) -> HashMap<(TraceId, SpanId), u64> {
    let mut out = HashMap::new();
    for s in posts {
        let e = out.entry((s.trace_id, s.span_id)).or_insert(0);
        if let Some(v) = s.start_ns {
            *e = v;
        }
    }
    out
}

fn newest_first(
    keys: &[(TraceId, SpanId)],
    starts: &HashMap<(TraceId, SpanId), u64>,
) -> Vec<fts::Hit> {
    let mut v: Vec<fts::Hit> = keys.iter().map(|&(t, s)| (starts[&(t, s)], t, s)).collect();
    v.sort_unstable_by(|a, b| b.cmp(a));
    v
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

/// fts over 100k spans of about 2 KB each, out-of-order start times: apply cost per
/// post, single-token lookups, and newest-50 of a two-token intersection, from the
/// builder and the sealed file. Forge's search took 765 ms on compacted segments against
/// 19 ms uncompacted; here a lookup's cost depends only on the postings involved. The
/// bounds are loose enough for debug builds, which use 20k spans to keep `scripts/check`
/// fast; run with `--release --no-capture` for numbers.
#[test]
fn fts_postings_lookup_timing_100k_spans() {
    use std::time::{Duration, Instant};
    const SPANS: usize = if cfg!(debug_assertions) {
        20_000
    } else {
        100_000
    };
    let mut rng = Rng::new(5);
    let vocab: Vec<String> = (0..2000).map(|i| format!("word{i}")).collect();
    let posts: Vec<Span> = (0..SPANS)
        .map(|i| {
            let mut text = String::with_capacity(2100);
            text.push_str("{\"content\":\"common");
            for (w, p) in [("refund", 0.5), ("policy", 0.4)] {
                if rng.chance(p) {
                    text.push(' ');
                    text.push_str(w);
                }
            }
            if i % 100 == 0 {
                text.push_str(" rare");
            }
            while text.len() < 2048 {
                text.push(' ');
                text.push_str(rng.pick(&vocab));
            }
            text.push_str("\"}");
            Span {
                project: "p".into(),
                trace_id: TraceId([(i / 50) as u8; 16]),
                span_id: SpanId((rng.next_u64() | 1).to_le_bytes()),
                start_ns: Some(rng.below(3_600_000_000_000)),
                inputs: Some(text.into_bytes()),
                ..Span::default()
            }
        })
        .collect();
    let headers = vec![SpanHeader::default(); SPANS];
    // Apply in batches of 1,000 posts with 1, 2, 4 and 8 shards. Wall time per post; on a
    // machine with fewer cores than shards this is the CPU cost, threads included.
    let mut applies = Vec::new();
    let mut b = fts::Builder::default();
    let shard_counts: &[usize] = if cfg!(debug_assertions) {
        &[1, 4]
    } else {
        &[1, 2, 4, 8]
    };
    for &shards in shard_counts {
        let mut sb = fts::Builder::with_shards(shards);
        let t = Instant::now();
        for (h, s) in headers.chunks(1000).zip(posts.chunks(1000)) {
            sb.apply(&Batch {
                first_post: 0,
                headers: h,
                spans: s,
            });
        }
        let per_post = t.elapsed() / SPANS as u32;
        applies.push(format!(
            "{shards} shard(s) {per_post:?}/post = {:.0}k posts/s",
            1e-3 / per_post.as_secs_f64()
        ));
        if shards == 1 {
            b = sb;
        }
    }
    let apply = applies.join(", ");
    let mut files = LayoutFiles::default();
    let t = Instant::now();
    b.seal(&mut files).unwrap();
    let seal = t.elapsed();
    let (name, bytes) = &files.files[0];
    let mut words64 = vec![0u64; bytes.len().div_ceil(8)];
    bytemuck::cast_slice_mut::<u64, u8>(&mut words64)[..bytes.len()].copy_from_slice(bytes);
    let set = [(
        name.as_str(),
        &bytemuck::cast_slice::<u64, u8>(&words64)[..bytes.len()],
    )];
    let r = fts::Reader::open(FileSet { files: &set }).unwrap();

    let time = |f: &dyn Fn() -> usize| {
        let (mut best, mut n) = (Duration::MAX, 0);
        for _ in 0..5 {
            let t = Instant::now();
            n = f();
            best = best.min(t.elapsed());
        }
        (best, n)
    };
    let mut report = format!(
        "fts over {SPANS} spans of 2 KB: seal {seal:?}, file {} KiB\n  apply: {apply}\n",
        bytes.len() / 1024
    );
    for (token, want) in [
        (&b"common"[..], SPANS),
        (b"rare", SPANS / 100),
        (b"missing", 0),
    ] {
        let (tb, nb) = time(&|| b.postings(token).0.len());
        let (tr, nr) = time(&|| r.postings(token).0.len());
        assert_eq!((nb, nr), (want, want));
        report += &format!(
            "  postings {:8} {:6} hits: builder {:>9.3?}, reader {:>9.3?}\n",
            String::from_utf8_lossy(token),
            want,
            tb,
            tr
        );
        assert!(tr < Duration::from_millis(100), "{report}");
    }
    let rare = r.postings(b"rare").1;
    assert!(
        rare.bytes_read < 40 * SPANS as u64 / 100,
        "rare token read {rare:?}"
    );

    // Newest 50 spans mentioning both tokens, against brute force.
    let both: &[&[u8]] = &[b"refund", b"policy"];
    let mut expected: Vec<fts::Hit> = posts
        .iter()
        .filter(|s| {
            let t = tokens(s.inputs.as_deref().unwrap());
            both.iter().all(|w| t.iter().any(|x| x == w))
        })
        .map(|s| (s.start_ns.unwrap(), s.trace_id, s.span_id))
        .collect();
    expected.sort_unstable_by(|a, b| b.cmp(a));
    expected.truncate(50);
    let newest = |hits: Vec<fts::Hit>| {
        assert_eq!(hits, expected);
        hits.len()
    };
    let (tb, _) = time(&|| newest(b.intersect_newest(both).0.take(50).collect()));
    let (tr, _) = time(&|| newest(r.intersect_newest(both).0.take(50).collect()));
    report += &format!("  newest 50 of refund AND policy: builder {tb:>9.3?}, reader {tr:>9.3?}\n");
    assert!(
        tb < Duration::from_millis(100) && tr < Duration::from_millis(100),
        "{report}"
    );
    println!("{report}");
}

/// Newest 50 runs on an active builder holding 200k run candidates that arrived with
/// out-of-order start times (DESIGN.md target: 1 ms p50 for newest 50 with filters).
#[test]
fn runs_newest_50_timing_200k_runs() {
    use std::time::{Duration, Instant};
    const RUNS: usize = 200_000;
    let mut rng = Rng::new(11);
    let headers: Vec<SpanHeader> = (0..RUNS)
        .map(|i| SpanHeader {
            trace_id: TraceId((rng.next_u64() as u128 | 1).to_le_bytes()),
            span_id: SpanId([1; 8]),
            project: 1,
            start_ns: 1_000_000 + rng.below(3_600_000_000_000),
            status: if i % 20 == 0 { 2 } else { 1 },
            fields: field::START | field::STATUS,
            version: i as u64 + 1,
            ..SpanHeader::default()
        })
        .collect();
    let mut b = runs_by_time::Builder::default();
    let t = Instant::now();
    for chunk in headers.chunks(1000) {
        // runs_by_time reads only headers.
        b.apply(&Batch {
            first_post: 0,
            headers: chunk,
            spans: &[],
        });
    }
    let apply = t.elapsed();
    let mut times: Vec<Duration> = (0..21)
        .map(|_| {
            let t = Instant::now();
            let n = b
                .runs_newest()
                .filter(|r| r.status == Status::Error as u8)
                .take(50)
                .count();
            assert_eq!(n, 50);
            t.elapsed()
        })
        .collect();
    times.sort();
    let p50 = times[times.len() / 2];
    let mut expected: Vec<u64> = headers.iter().map(|h| h.start_ns).collect();
    expected.sort_unstable_by(|a, b| b.cmp(a));
    let got: Vec<u64> = b.runs_newest().take(50).map(|r| r.start_ns).collect();
    assert_eq!(got, expected[..50]);
    println!("runs_by_time: {RUNS} runs applied in {apply:?}; newest 50 errors p50 {p50:?}");
    assert!(p50 < Duration::from_millis(5), "newest 50 took {p50:?}");
}

/// A later post that moves a span's start reorders it in the newest-first postings, in
/// the builder and in the sealed file; a post without a start keeps the merged one.
#[test]
fn fts_newest_order_follows_the_latest_start() {
    let post = |s: u8, start: Option<u64>, text: &str| Span {
        project: "p".into(),
        trace_id: TraceId([1; 16]),
        span_id: SpanId([s; 8]),
        start_ns: start,
        inputs: Some(text.as_bytes().to_vec()),
        ..Span::default()
    };
    let p = Partition::new(&[
        post(1, Some(10), "alpha"),
        post(2, Some(20), "alpha beta"),
        post(3, None, "alpha"),
        post(1, Some(30), "beta"),
        post(2, None, "gamma"),
    ]);
    let (builders, sealed) = built::<fts::Builder>(&p, true);
    let hit = |s: u8, start: u64| (start, TraceId([1; 16]), SpanId([s; 8]));
    let alpha = vec![hit(1, 30), hit(2, 20), hit(3, 0)];
    let both = vec![hit(1, 30), hit(2, 20)];
    with_files(&sealed, |files| {
        let r = fts::Reader::open(files).unwrap();
        for b in &builders {
            assert_eq!(b.postings_newest(b"alpha").0.collect::<Vec<_>>(), alpha);
            assert_eq!(
                b.intersect_newest(&[b"beta", b"alpha"])
                    .0
                    .collect::<Vec<_>>(),
                both
            );
        }
        assert_eq!(r.postings_newest(b"alpha").0.collect::<Vec<_>>(), alpha);
        assert_eq!(
            r.intersect_newest(&[b"beta", b"alpha"])
                .0
                .collect::<Vec<_>>(),
            both
        );
    });
}

/// `prepare` keeps each distinct token of a post once, inputs and outputs together, and
/// every token lands in exactly one shard.
#[test]
fn fts_prepare_dedups_tokens_and_routes_each_to_one_shard() {
    let span = Span {
        inputs: Some(br#"{"q":"Refund refund REFUND policy \n order"}"#.to_vec()),
        outputs: Some(br#"{"a":"policy shipping"}"#.to_vec()),
        ..Span::default()
    };
    let mut want: Vec<Vec<u8>> = tokens(span.inputs.as_deref().unwrap());
    want.extend(tokens(span.outputs.as_deref().unwrap()));
    want.sort();
    want.dedup();
    for shards in [1, 2, 5] {
        let p = fts::prepare(&span, shards);
        assert_eq!((p.shards(), p.len()), (shards, want.len()));
        // Applying it to a builder with each shard alone finds every token exactly once.
        let mut b = fts::Builder::with_shards(shards);
        let batch = Batch {
            first_post: 0,
            headers: &[SpanHeader::default()],
            spans: std::slice::from_ref(&span),
        };
        b.apply_prepared(&batch, &[p]);
        for t in &want {
            assert_eq!(
                b.postings(t).0.len(),
                1,
                "{shards} shards, {:?}",
                String::from_utf8_lossy(t)
            );
        }
    }
    assert!(fts::prepare(&Span::default(), 3).is_empty());
}
