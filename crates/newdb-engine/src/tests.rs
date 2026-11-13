//! Differential, recovery and lifecycle tests against `newdb_testkit::Oracle`.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Duration;

use newdb_testkit::Oracle;
use newdb_testkit::diff::{compare, standard_queries};
use newdb_testkit::r#gen::{GenConfig, generate};
use newdb_types::{
    Read, Result, Run, RunQuery, SearchHit, SearchQuery, Span, SpanId, Store, TraceId, TraceQuery,
};

use crate::files::test_dir::TestDir;
use crate::files::{HOUR_NS, LOG};
use crate::{Config, Db, ManualClock};

const HOUR: Duration = Duration::from_secs(3600);

fn config(clock: &Arc<ManualClock>) -> Config {
    Config {
        fsync: false,
        commit_interval: Duration::ZERO,
        retention_days: 0,
        clock: clock.clone(),
        ..Config::default()
    }
}

/// A `Store` that drives the engine through partitions while `compare` ingests: after
/// every `hour_every`-th ingest it moves the clock an hour, after every `seal_every`-th
/// it seals, and after every `reopen_every`-th it closes and reopens the database.
struct Driver {
    dir: PathBuf,
    clock: Arc<ManualClock>,
    db: RwLock<Option<Db>>,
    n: AtomicUsize,
    hour_every: usize,
    seal_every: usize,
    reopen_every: usize,
}

impl Driver {
    fn new(dir: &Path, hour_every: usize, seal_every: usize, reopen_every: usize) -> Driver {
        let clock = Arc::new(ManualClock::new(GenConfig::default().base_ns));
        let db = Db::open(dir, config(&clock)).unwrap();
        Driver {
            dir: dir.to_path_buf(),
            clock,
            db: RwLock::new(Some(db)),
            n: AtomicUsize::new(0),
            hour_every,
            seal_every,
            reopen_every,
        }
    }

    fn reopen(&self, read_only: bool) {
        let mut g = self.db.write().unwrap();
        g.take().unwrap().close().unwrap();
        let cfg = Config {
            read_only,
            ..config(&self.clock)
        };
        *g = Some(Db::open(&self.dir, cfg).unwrap());
    }

    fn with<T>(&self, f: impl FnOnce(&Db) -> T) -> T {
        f(self.db.read().unwrap().as_ref().unwrap())
    }
}

fn every(n: usize, k: usize) -> bool {
    k > 0 && n.is_multiple_of(k)
}

impl Store for Driver {
    fn ingest(&self, spans: Vec<Span>) -> Result<()> {
        self.with(|db| db.ingest(spans))?;
        let n = self.n.fetch_add(1, Ordering::SeqCst) + 1;
        if every(n, self.hour_every) {
            self.clock.advance(HOUR);
        }
        if every(n, self.seal_every) {
            self.with(|db| db.seal_active())?;
        }
        if every(n, self.reopen_every) {
            self.reopen(false);
        }
        Ok(())
    }

    fn trace(&self, q: &TraceQuery) -> Result<Read<Vec<Span>>> {
        self.with(|db| db.trace(q))
    }

    fn runs(&self, q: &RunQuery) -> Result<Read<Vec<Run>>> {
        self.with(|db| db.runs(q))
    }

    fn search(&self, q: &SearchQuery) -> Result<Read<Vec<SearchHit>>> {
        self.with(|db| db.search(q))
    }
}

/// Runs `compare`, then the same queries again after a reopen and a read-only open.
fn check(posts: &[Span], batch: usize, d: &Driver) {
    let oracle = Oracle::new();
    compare(&oracle, d, posts, batch).unwrap();
    d.reopen(false);
    queries_agree(&oracle, d, posts);
    d.reopen(true);
    queries_agree(&oracle, d, posts);
    d.reopen(false);
}

/// `compare` without ingesting: the standard queries plus deep pagination.
fn queries_agree(expected: &dyn Store, actual: &dyn Store, posts: &[Span]) {
    let (tq, rq, sq) = standard_queries(posts);
    for q in &tq {
        assert_eq!(
            expected.trace(q).unwrap().value,
            actual.trace(q).unwrap().value,
            "trace {:?}",
            q.trace_id
        );
    }
    for q in &rq {
        assert_eq!(
            expected.runs(q).unwrap().value,
            actual.runs(q).unwrap().value,
            "{q:?}"
        );
    }
    for q in &sq {
        assert_eq!(
            expected.search(q).unwrap().value,
            actual.search(q).unwrap().value,
            "{q:?}"
        );
    }
    // Walk every run 7 at a time.
    let mut q = RunQuery {
        limit: 7,
        ..RunQuery::default()
    };
    loop {
        let e = expected.runs(&q).unwrap().value;
        assert_eq!(
            e,
            actual.runs(&q).unwrap().value,
            "page before {:?}",
            q.before
        );
        match e.last() {
            Some(last) => q.before = Some(last.key()),
            None => break,
        }
    }
}

#[test]
fn one_partition_matches_the_oracle() {
    let dir = TestDir::new("one");
    let posts = generate(&GenConfig::small(1));
    check(&posts, 17, &Driver::new(dir.path(), 0, 0, 0));
    let parts = Db::open(dir.path(), config(&Arc::new(ManualClock::new(0))))
        .unwrap()
        .partitions()
        .unwrap();
    assert_eq!(parts.len(), 1);
}

#[test]
fn many_partitions_match_the_oracle() {
    for (seed, batch, hour_every, seal_every, reopen_every) in [
        (2, 5, 1, 0, 0),
        (3, 9, 2, 5, 0),
        (4, 13, 1, 0, 7),
        (5, 1, 3, 11, 29),
    ] {
        let dir = TestDir::new("many");
        let cfg = GenConfig {
            traces: 40,
            open_then_close_rate: 0.5,
            ..GenConfig::small(seed)
        };
        let posts = generate(&cfg);
        let d = Driver::new(dir.path(), hour_every, seal_every, reopen_every);
        check(&posts, batch, &d);
        let n = d.with(|db| db.partitions().unwrap().len());
        assert!(n > 10, "seed {seed}: only {n} partitions");
    }
}

fn ids(t: u8, s: u8) -> (TraceId, SpanId) {
    (TraceId([t; 16]), SpanId([s; 8]))
}

fn post(t: u8, s: u8) -> Span {
    let (trace_id, span_id) = ids(t, s);
    Span {
        project: "p".into(),
        trace_id,
        span_id,
        ..Span::default()
    }
}

/// The cases from the brief, built by hand so each one is certain to happen.
#[test]
fn cross_partition_cases() {
    let dir = TestDir::new("cross");
    let d = Driver::new(dir.path(), 1, 0, 0);
    let oracle = Oracle::new();
    let both = |spans: Vec<Span>| {
        oracle.ingest(spans.clone()).unwrap();
        d.ingest(spans).unwrap();
    };
    // A child opened in one partition, closed in the next without a parent field: the
    // closing post must not become a run.
    both(vec![
        Span {
            parent_span_id: Some(SpanId([1; 8])),
            start_ns: Some(500),
            inputs: Some(b"\"refund request\"".to_vec()),
            ..post(1, 2)
        },
        Span {
            start_ns: Some(100),
            name: Some("agent".into()),
            ..post(1, 1)
        },
    ]);
    both(vec![Span {
        end_ns: Some(900),
        outputs: Some(b"\"done\"".to_vec()),
        // A later post replaces the inputs: the old words must stop matching.
        inputs: Some(b"\"shipping question\"".to_vec()),
        ..post(1, 2)
    }]);
    // A root whose summary is split: start here, end and cost later; a root whose start
    // moves later in a later partition.
    both(vec![Span {
        status: Some(newdb_types::Status::Error),
        end_ns: Some(1000),
        cost_micro_usd: Some(20_000),
        ..post(1, 1)
    }]);
    both(vec![
        Span {
            start_ns: Some(50),
            ..post(2, 1)
        },
        Span {
            start_ns: Some(10),
            ..post(3, 1)
        },
    ]);
    both(vec![Span {
        start_ns: Some(150),
        ..post(3, 1)
    }]);
    // A root that never sends a start time.
    both(vec![Span {
        name: Some("no start".into()),
        ..post(4, 1)
    }]);
    // Search tokens only in an earlier partition while a later one moves the start, and
    // tokens split between an open and a close post that both carry the start.
    both(vec![
        Span {
            start_ns: Some(5),
            inputs: Some(b"\"refund shipping\"".to_vec()),
            ..post(5, 1)
        },
        Span {
            start_ns: Some(7),
            inputs: Some(b"\"refund\"".to_vec()),
            ..post(6, 1)
        },
    ]);
    both(vec![
        Span {
            start_ns: Some(10_000),
            ..post(5, 1)
        },
        Span {
            start_ns: Some(7),
            outputs: Some(b"\"shipping\"".to_vec()),
            ..post(6, 1)
        },
    ]);
    let mut posts = Vec::new();
    for t in 1..=6 {
        posts.push(post(t, 1));
    }
    queries_agree(&oracle, &d, &posts);
    let runs = d.runs(&RunQuery {
        limit: 10,
        ..RunQuery::default()
    });
    assert_eq!(runs.unwrap().value.len(), 6);
    for text in [
        "refund",
        "shipping",
        "done",
        "shipping done",
        "refund shipping",
    ] {
        for limit in [1, 2, 10] {
            let q = SearchQuery {
                text: text.into(),
                limit,
                ..Default::default()
            };
            assert_eq!(
                oracle.search(&q).unwrap().value,
                d.search(&q).unwrap().value,
                "{text} limit {limit}"
            );
        }
    }
    d.reopen(false);
    queries_agree(&oracle, &d, &posts);
}

#[test]
fn torn_log_tails_recover_to_a_prefix_of_posts() {
    let src = TestDir::new("torn-src");
    let posts = generate(&GenConfig {
        traces: 6,
        ..GenConfig::small(9)
    });
    {
        let d = Driver::new(src.path(), 0, 0, 0);
        for c in posts.chunks(4) {
            d.ingest(c.to_vec()).unwrap();
        }
    }
    let part = std::fs::read_dir(src.path())
        .unwrap()
        .flatten()
        .find(|e| e.path().join(LOG).exists())
        .unwrap();
    let log = std::fs::read(part.path().join(LOG)).unwrap();
    let ends: Vec<usize> = newdb_format::log::Reader::new(&log)
        .unwrap()
        .map(|r| r.end)
        .collect();
    let step = (log.len() / 40).max(1);
    for cut in (newdb_format::log::HEADER_LEN..=log.len()).step_by(step) {
        let dir = TestDir::new("torn");
        copy_dir(src.path(), dir.path());
        let name = part.file_name();
        let logp = dir.path().join(&name).join(LOG);
        let mut bytes = log[..cut].to_vec();
        // Garbage after the cut, as a torn write leaves it.
        bytes.extend_from_slice(&[0xa5; 37]);
        std::fs::write(&logp, &bytes).unwrap();
        let kept = newdb_format::log::Reader::new(&log[..cut])
            .unwrap()
            .filter(|r| r.kind == newdb_format::log::kind::SPAN_POST)
            .count();
        assert!(ends.contains(&cut) || cut != log.len());
        let oracle = Oracle::new();
        oracle.ingest(posts[..kept].to_vec()).unwrap();
        let d = Driver::new(dir.path(), 0, 0, 0);
        queries_agree(&oracle, &d, &posts);
        // The tail is cut on disk, and new posts continue after it.
        assert!(std::fs::metadata(&logp).unwrap().len() as usize <= cut);
        let extra = vec![Span {
            start_ns: Some(1),
            ..post(9, 9)
        }];
        oracle.ingest(extra.clone()).unwrap();
        d.ingest(extra).unwrap();
        d.reopen(false);
        queries_agree(&oracle, &d, &posts);
    }
}

fn copy_dir(from: &Path, to: &Path) {
    std::fs::create_dir_all(to).unwrap();
    for e in std::fs::read_dir(from).unwrap().flatten() {
        if e.file_name() == "LOCK" {
            continue;
        }
        let t = to.join(e.file_name());
        if e.file_type().unwrap().is_dir() {
            copy_dir(&e.path(), &t);
        } else {
            std::fs::copy(e.path(), t).unwrap();
        }
    }
}

#[test]
fn retention_deletes_old_partitions_only() {
    let dir = TestDir::new("retention");
    let clock = Arc::new(ManualClock::new(100 * 24 * HOUR_NS));
    let db = Db::open(
        dir.path(),
        Config {
            retention_days: 2,
            ..config(&clock)
        },
    )
    .unwrap();
    for i in 0..5u8 {
        db.ingest(vec![Span {
            start_ns: Some(i as u64),
            ..post(i + 1, 1)
        }])
        .unwrap();
        clock.advance(HOUR * 24);
    }
    db.ingest(vec![post(9, 1)]).unwrap();
    db.apply_retention().unwrap();
    let names: Vec<String> = db
        .partitions()
        .unwrap()
        .into_iter()
        .map(|p| p.name)
        .collect();
    // Hours 100d..104d sealed, 105d active; the cutoff is 103d.
    assert_eq!(names.len(), 3, "{names:?}");
    let traces = |db: &Db| {
        (1..=5u8)
            .filter(|&t| {
                let q = TraceQuery {
                    project: "p".into(),
                    trace_id: ids(t, 1).0,
                    ..Default::default()
                };
                !db.trace(&q).unwrap().value.is_empty()
            })
            .collect::<Vec<_>>()
    };
    assert_eq!(traces(&db), vec![4, 5]);
    assert!(!dir.path().join("trash").exists());
    db.close().unwrap();
    let db = Db::open(dir.path(), config(&clock)).unwrap();
    assert_eq!(traces(&db), vec![4, 5]);
    assert!(db.verify().unwrap().problems.is_empty());
}

#[test]
fn verify_finds_damage_and_rebuild_repairs_layouts() {
    let dir = TestDir::new("verify");
    let posts = generate(&GenConfig::small(11));
    let d = Driver::new(dir.path(), 3, 0, 0);
    let oracle = Oracle::new();
    compare(&oracle, &d, &posts, 10).unwrap();
    let report = d.with(|db| db.verify().unwrap());
    assert!(report.problems.is_empty(), "{:?}", report.problems);
    assert!(report.partitions_checked >= 2);
    assert_eq!(report.layouts_checked, report.partitions_checked * 4);

    let info = d.with(|db| db.partitions().unwrap());
    let sealed = info.iter().find(|p| p.sealed).unwrap();
    let file = dir
        .path()
        .join(&sealed.name)
        .join("layouts/runs_by_time.v1/runs_by_time.bin");
    let mut b = std::fs::read(&file).unwrap();
    let at = b.len() - 3;
    b[at] ^= 0xff;
    std::fs::write(&file, b).unwrap();
    let report = d.with(|db| db.verify().unwrap());
    assert_eq!(report.problems.len(), 1, "{:?}", report.problems);
    assert!(report.problems[0].contains("runs_by_time"));

    d.with(|db| db.rebuild(Some("runs_by_time")).unwrap());
    assert!(d.with(|db| db.verify().unwrap()).problems.is_empty());
    queries_agree(&oracle, &d, &posts);
    assert!(d.with(|db| db.rebuild(Some("nope"))).is_err());

    let log = dir.path().join(&sealed.name).join(LOG);
    let mut b = std::fs::read(&log).unwrap();
    let at = b.len() / 2;
    b[at] ^= 1;
    std::fs::write(&log, b).unwrap();
    let report = d.with(|db| db.verify().unwrap());
    assert!(!report.problems.is_empty());

    let inspect = d.with(|db| db.inspect().unwrap());
    assert_eq!(inspect.partitions.len(), info.len());
    assert!(inspect.spans_per_trace.2 >= inspect.spans_per_trace.0);
    assert!(inspect.cross_partition_span_share > 0.0);
    assert!(inspect.reads.iter().any(|r| r.0 == "runs"));
}

#[test]
fn read_only_open_rejects_writes_and_sees_the_active_partition() {
    let dir = TestDir::new("ro");
    let clock = Arc::new(ManualClock::new(5 * HOUR_NS));
    let db = Db::open(dir.path(), config(&clock)).unwrap();
    db.ingest(vec![post(1, 1)]).unwrap();
    // A second writer is refused while the first is open; a reader is not.
    assert!(Db::open(dir.path(), config(&clock)).is_err());
    let ro = Db::open(
        dir.path(),
        Config {
            read_only: true,
            ..config(&clock)
        },
    )
    .unwrap();
    assert!(matches!(
        ro.ingest(vec![post(2, 1)]),
        Err(newdb_types::Error::ReadOnly)
    ));
    assert!(ro.seal_active().is_err());
    let runs = ro.runs(&RunQuery {
        limit: 5,
        ..Default::default()
    });
    assert_eq!(runs.unwrap().value.len(), 1);
}

/// The child half of the kill -9 test: ingests until killed, printing each acknowledged
/// batch. Does nothing unless the parent set `NEWDB_CRASH_DIR`.
#[test]
fn crash_child() {
    let Ok(dir) = std::env::var("NEWDB_CRASH_DIR") else {
        return;
    };
    use std::io::Write;
    let posts = generate(&GenConfig {
        traces: 2000,
        ..GenConfig::small(21)
    });
    let db = Db::open(
        &dir,
        Config {
            fsync: true,
            commit_interval: Duration::from_millis(1),
            retention_days: 0,
            ..Config::default()
        },
    )
    .unwrap();
    let mut out = std::io::stdout();
    for (i, c) in posts.chunks(10).enumerate() {
        db.ingest(c.to_vec()).unwrap();
        writeln!(out, "acked {}", (i + 1) * 10).unwrap();
        out.flush().unwrap();
    }
    std::thread::sleep(Duration::from_secs(60));
}

#[test]
fn kill_9_loses_no_acknowledged_post() {
    use std::io::{BufRead, BufReader};
    use std::process::{Command, Stdio};
    let dir = TestDir::new("kill9");
    let mut child = Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "tests::crash_child",
            "--nocapture",
            "--test-threads=1",
        ])
        .env("NEWDB_CRASH_DIR", dir.path())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    let mut acked = 0;
    for line in BufReader::new(child.stdout.take().unwrap()).lines() {
        if let Some(n) = line.unwrap().strip_prefix("acked ") {
            acked = n.parse().unwrap();
            if acked >= 300 {
                break;
            }
        }
    }
    child.kill().unwrap(); // SIGKILL
    child.wait().unwrap();
    assert!(acked >= 300);
    let posts = generate(&GenConfig {
        traces: 2000,
        ..GenConfig::small(21)
    });
    let clock = Arc::new(ManualClock::new(crate::Clock::now_ns(&crate::SystemClock)));
    let db = Db::open(dir.path(), config(&clock)).unwrap();
    let recovered: u64 = db.partitions().unwrap().iter().map(|p| p.posts).sum();
    assert!(recovered as usize >= acked, "{recovered} < {acked}");
    let oracle = Oracle::new();
    oracle.ingest(posts[..recovered as usize].to_vec()).unwrap();
    queries_agree(&oracle, &db, &posts[..recovered as usize]);
}

#[test]
fn default_workload_with_large_payloads_matches_the_oracle() {
    let dir = TestDir::new("default");
    let posts = generate(&GenConfig {
        large_payload_rate: 0.01,
        ..GenConfig::default()
    });
    check(&posts, 64, &Driver::new(dir.path(), 4, 0, 9));
}

/// The default workload again, as if on 4 cores: layouts are applied in parallel and
/// large traces decompressed in parallel.
#[test]
fn multi_core_paths_match_the_oracle() {
    crate::part::FORCE_CORES.store(4, Ordering::Relaxed);
    let dir = TestDir::new("cores");
    let posts = generate(&GenConfig {
        traces: 30,
        large_payload_rate: 0.05,
        ..GenConfig::default()
    });
    check(&posts, 400, &Driver::new(dir.path(), 2, 0, 0));
}

/// Writers on several threads and readers alongside them. Each writer owns its traces, so
/// the merged spans equal the oracle's except for `version`, which depends on interleaving.
#[test]
fn concurrent_writers_and_readers() {
    let dir = TestDir::new("concurrent");
    let clock = Arc::new(ManualClock::new(GenConfig::default().base_ns));
    let db = Db::open(
        dir.path(),
        Config {
            commit_interval: Duration::from_millis(2),
            ..config(&clock)
        },
    )
    .unwrap();
    let streams: Vec<Vec<Span>> = (0..4)
        .map(|s| generate(&GenConfig::small(100 + s)))
        .collect();
    std::thread::scope(|sc| {
        for posts in &streams {
            let db = &db;
            sc.spawn(move || {
                for (i, c) in posts.chunks(7).enumerate() {
                    db.ingest(c.to_vec()).unwrap();
                    if i % 10 == 9 {
                        db.seal_active().unwrap();
                    }
                }
            });
        }
        for _ in 0..2 {
            sc.spawn(|| {
                for _ in 0..50 {
                    let runs = db.runs(&RunQuery {
                        limit: 20,
                        ..Default::default()
                    });
                    assert!(
                        runs.unwrap()
                            .value
                            .windows(2)
                            .all(|w| w[0].key() > w[1].key())
                    );
                }
            });
        }
    });
    let oracle = Oracle::new();
    let all: Vec<Span> = streams.concat();
    oracle.ingest(all.clone()).unwrap();
    let (tq, rq, sq) = standard_queries(&all);
    let unversioned = |mut v: Vec<Span>| {
        v.iter_mut().for_each(|s| s.version = 0);
        v
    };
    for q in &tq {
        assert_eq!(
            unversioned(oracle.trace(q).unwrap().value),
            unversioned(db.trace(q).unwrap().value)
        );
    }
    for q in &rq {
        assert_eq!(oracle.runs(q).unwrap().value, db.runs(q).unwrap().value);
    }
    for q in &sq {
        assert_eq!(oracle.search(q).unwrap().value, db.search(q).unwrap().value);
    }
    assert!(db.verify().unwrap().problems.is_empty());
}

#[test]
fn an_idle_writer_commits_at_once() {
    let dir = TestDir::new("idle");
    let clock = Arc::new(ManualClock::new(HOUR_NS));
    let db = Db::open(
        dir.path(),
        Config {
            commit_interval: Duration::from_millis(500),
            ..config(&clock)
        },
    )
    .unwrap();
    db.ingest(vec![post(1, 1)]).unwrap();
    let t = std::time::Instant::now();
    for s in 2..12 {
        db.ingest(vec![post(1, s)]).unwrap();
    }
    // Ten sequential ingests take far less than one commit_interval.
    assert!(
        t.elapsed() < Duration::from_millis(250),
        "{:?}",
        t.elapsed()
    );
}

/// Parallel layout apply gives the same bytes as one thread (it only runs on machines
/// with more than one core, so force it here).
#[test]
fn parallel_apply_matches_one_thread() {
    use crate::part::Layouts;
    let posts = generate(&GenConfig::small(31));
    let dir = TestDir::new("apply");
    let d = Driver::new(dir.path(), 0, 0, 0);
    d.ingest(posts.clone()).unwrap();
    let part = d.with(|db| db.inner.parts()[0].clone());
    let mut builders = [Layouts::new(), Layouts::new(), Layouts::new()];
    crate::part::replay(&part.dir, crate::part::Mode::Strict, &mut |b| {
        for (i, l) in builders.iter_mut().enumerate() {
            l.apply_on(b, i + 1);
        }
    })
    .unwrap();
    let sealed: Vec<_> = builders.iter().map(|l| l.seal().unwrap()).collect();
    for s in &sealed[1..] {
        assert_eq!(
            s.iter().map(|(_, f)| f).collect::<Vec<_>>(),
            sealed[0].iter().map(|(_, f)| f).collect::<Vec<_>>()
        );
    }
}

#[test]
fn sealed_payloads_replace_the_blob_through_crashes() {
    use crate::files::{PAYLOADS, SEALED_PAYLOADS};
    let src = TestDir::new("psl-src");
    let posts = generate(&GenConfig::small(23));
    let half = posts.len() / 2;
    let oracle = Oracle::new();
    let active = {
        let d = Driver::new(src.path(), 2, 3, 0);
        compare(&oracle, &d, &posts[..half], 10).unwrap();
        d.with(|db| db.seal_active()).unwrap();
        // The rest stays in the active partition.
        d.clock.advance(HOUR);
        for c in posts[half..].chunks(10) {
            oracle.ingest(c.to_vec()).unwrap();
            d.with(|db| db.ingest(c.to_vec())).unwrap();
        }
        d.with(|db| db.inner.reframe.wait_idle());
        let info = d.with(|db| db.partitions().unwrap());
        let db = d.db.write().unwrap().take().unwrap();
        db.close().unwrap();
        assert!(info.iter().filter(|p| p.sealed).count() >= 2);
        for p in info.iter().filter(|p| p.sealed) {
            let dir = src.path().join(&p.name);
            assert!(dir.join(SEALED_PAYLOADS).exists() && !dir.join(PAYLOADS).exists());
            assert_eq!(
                p.payload_bytes,
                std::fs::metadata(dir.join(SEALED_PAYLOADS)).unwrap().len()
            );
        }
        info.iter().find(|p| !p.sealed).unwrap().name.clone()
    };
    let sealed_ok = |root: &Path, name: &str| {
        let dir = root.join(name);
        dir.join(SEALED_PAYLOADS).exists() && !dir.join(PAYLOADS).exists()
    };

    // A seal that crashed after MANIFEST, before deleting the blob: read-only opens use
    // payloads.sealed, and the next writable open deletes the blob.
    let a = TestDir::new("psl-left");
    copy_dir(src.path(), a.path());
    let blob = std::fs::read(a.path().join(&active).join(PAYLOADS)).unwrap();
    let d = Driver::new(a.path(), 0, 0, 0);
    d.with(|db| db.seal_active()).unwrap();
    let reference = std::fs::read(a.path().join(&active).join(SEALED_PAYLOADS)).unwrap();
    std::fs::write(a.path().join(&active).join(PAYLOADS), &blob).unwrap();
    d.reopen(true);
    queries_agree(&oracle, &d, &posts);
    assert!(!sealed_ok(a.path(), &active));
    d.reopen(false);
    assert!(sealed_ok(a.path(), &active));
    queries_agree(&oracle, &d, &posts);
    let report = d.with(|db| db.verify().unwrap());
    assert!(report.problems.is_empty(), "{:?}", report.problems);
    d.with(|db| db.rebuild(None).unwrap());
    queries_agree(&oracle, &d, &posts);

    // A seal that crashed while writing payloads.sealed, before MANIFEST listed the
    // partition: it recovers from its log and blob, ignoring the stale file, and seals
    // again to the same bytes.
    let b = TestDir::new("psl-torn");
    copy_dir(src.path(), b.path());
    let mut torn = b"NEWDBPSL".to_vec();
    torn.extend_from_slice(&[0x5a; 40]);
    std::fs::write(b.path().join(&active).join(SEALED_PAYLOADS), torn).unwrap();
    let db_b = Driver::new(b.path(), 0, 0, 0);
    queries_agree(&oracle, &db_b, &posts);
    let blob_len = std::fs::metadata(b.path().join(&active).join(PAYLOADS))
        .unwrap()
        .len();
    let info = db_b.with(|db| db.partitions().unwrap());
    let p = info.iter().find(|p| p.name == active).unwrap();
    assert!(!p.sealed && p.payload_bytes == blob_len);
    assert!(db_b.with(|db| db.inspect()).is_ok());
    db_b.with(|db| db.seal_active()).unwrap();
    assert!(sealed_ok(b.path(), &active));
    let resealed = std::fs::read(b.path().join(&active).join(SEALED_PAYLOADS)).unwrap();
    assert!(resealed == reference, "reseal wrote different bytes");
    assert!(db_b.with(|db| db.verify().unwrap()).problems.is_empty());
    db_b.reopen(true);
    queries_agree(&oracle, &db_b, &posts);

    // Re-framing that crashed before renaming its file into place: open removes the
    // .tmp, the partition answers from payloads.blob, and re-framing runs again.
    let c = TestDir::new("psl-tmp");
    copy_dir(a.path(), c.path());
    let dir = c.path().join(&active);
    let mut torn = std::fs::read(dir.join(SEALED_PAYLOADS)).unwrap();
    torn.truncate(torn.len() / 2);
    std::fs::write(dir.join("payloads.sealed.tmp"), torn).unwrap();
    std::fs::remove_file(dir.join(SEALED_PAYLOADS)).unwrap();
    std::fs::write(dir.join(PAYLOADS), &blob).unwrap();
    let db_c = Driver::new(c.path(), 0, 0, 0);
    queries_agree(&oracle, &db_c, &posts);
    db_c.with(|db| db.inner.reframe.wait_idle());
    assert!(sealed_ok(c.path(), &active));
    assert!(!dir.join("payloads.sealed.tmp").exists());
    assert!(std::fs::read(dir.join(SEALED_PAYLOADS)).unwrap() == reference);
    queries_agree(&oracle, &db_c, &posts);
    assert!(db_c.with(|db| db.verify().unwrap()).problems.is_empty());

    // Damaged frame data is a verify problem.
    let file = a.path().join(&active).join(SEALED_PAYLOADS);
    let mut bytes = std::fs::read(&file).unwrap();
    let at = bytes.len() - 2;
    bytes[at] ^= 0x40;
    std::fs::write(&file, bytes).unwrap();
    let report = d.with(|db| db.verify().unwrap());
    assert!(
        report
            .problems
            .iter()
            .any(|p| p.contains("payloads.sealed")),
        "{:?}",
        report.problems
    );
}
