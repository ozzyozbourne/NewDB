//! Shared helpers: temporary directories, a test clock and config, and a query-by-query
//! comparison against the oracle.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use newdb_engine::{Clock, Config, Db, ManualClock};
use newdb_testkit::Oracle;
use newdb_testkit::diff::standard_queries;
use newdb_types::{Run, RunQuery, SearchQuery, Span, SpanId, Store, TraceId, TraceQuery};

pub const HOUR: Duration = Duration::from_secs(3600);
/// Test clock start: one minute into an hour, near the generator's span start times.
pub const T0: u64 = 1_800_000_000_000_000_000 + 60_000_000_000;

/// A directory under the system temp dir, removed on drop. Declare it before the `Db` so
/// the database drops first.
pub struct TempDir(PathBuf);

impl TempDir {
    pub fn new(name: &str) -> TempDir {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.as_nanos());
        let path = std::env::temp_dir().join(format!(
            "newdb-it-{name}-{}-{nanos}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir_all(&path).unwrap();
        TempDir(path)
    }

    pub fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

pub fn clock() -> Arc<ManualClock> {
    Arc::new(ManualClock::new(T0))
}

/// Fast settings for tests: no fsync, short group commit, no retention.
pub fn config(clock: &Arc<ManualClock>) -> Config {
    Config {
        fsync: false,
        commit_interval: Duration::from_millis(1),
        retention_days: 0,
        clock: clock.clone() as Arc<dyn Clock>,
        ..Config::default()
    }
}

pub fn open(dir: &TempDir, clock: &Arc<ManualClock>) -> Db {
    Db::open(dir.path(), config(clock)).expect("Db::open")
}

pub fn open_read_only(dir: &TempDir, clock: &Arc<ManualClock>) -> Db {
    let config = Config {
        read_only: true,
        ..config(clock)
    };
    Db::open(dir.path(), config).expect("Db::open read-only")
}

pub fn oracle_of(posts: &[Span]) -> Oracle {
    let o = Oracle::new();
    o.ingest(posts.to_vec()).unwrap();
    o
}

/// Ingests `posts` in calls of `batch` posts.
pub fn ingest_all(store: &dyn Store, posts: &[Span], batch: usize) {
    for chunk in posts.chunks(batch.max(1)) {
        store.ingest(chunk.to_vec()).expect("ingest");
    }
}

/// Every distinct `(project, trace_id)` of `posts`.
pub fn traces_of(posts: &[Span]) -> Vec<(String, TraceId)> {
    let mut t: Vec<(String, TraceId)> = posts
        .iter()
        .map(|s| (s.project.clone(), s.trace_id))
        .collect();
    t.sort();
    t.dedup();
    t
}

pub fn all_runs(store: &dyn Store) -> Vec<Run> {
    store
        .runs(&RunQuery {
            limit: 1_000_000,
            ..Default::default()
        })
        .expect("runs")
        .value
}

/// Asks both stores the same questions without ingesting anything: the standard
/// differential queries for `posts` (with a page 2 of each runs query), every trace of
/// `posts`, every run, and a search for each generator word. Panics on the first
/// difference, naming `what`.
pub fn assert_same(expected: &dyn Store, actual: &dyn Store, posts: &[Span], what: &str) {
    let (tq, rq, sq) = standard_queries(posts);
    let all_traces = traces_of(posts)
        .into_iter()
        .map(|(project, trace_id)| TraceQuery {
            project,
            trace_id,
            payloads: true,
            timings: false,
        });
    for q in tq.into_iter().chain(all_traces) {
        let e = expected.trace(&q).unwrap().value;
        let a = actual.trace(&q).expect("trace").value;
        assert!(
            e == a,
            "{what}: trace {:?} (project {}, payloads {}): expected {} spans, got {}\n{}",
            q.trace_id,
            q.project,
            q.payloads,
            e.len(),
            a.len(),
            first_diff(&e, &a)
        );
    }
    let everything = RunQuery {
        limit: 1_000_000,
        ..Default::default()
    };
    for q in rq.into_iter().chain([everything]) {
        let e = expected.runs(&q).unwrap().value;
        let a = actual.runs(&q).expect("runs").value;
        assert!(e == a, "{what}: runs {q:?}\n{}", first_diff(&e, &a));
        if let Some(last) = e.last() {
            let page2 = RunQuery {
                before: Some(last.key()),
                ..q.clone()
            };
            let e2 = expected.runs(&page2).unwrap().value;
            let a2 = actual.runs(&page2).expect("runs").value;
            assert!(
                e2 == a2,
                "{what}: runs page 2 {page2:?}\n{}",
                first_diff(&e2, &a2)
            );
        }
    }
    let words = ["policy", "order", "weather", "timeout", "hotel", "content"];
    let word_queries = words.map(|w| SearchQuery {
        text: w.into(),
        limit: 1_000_000,
        ..Default::default()
    });
    for q in sq.into_iter().chain(word_queries) {
        let e = expected.search(&q).unwrap().value;
        let a = actual.search(&q).expect("search").value;
        assert!(
            e == a,
            "{what}: search {:?}\n{}",
            q.text,
            first_diff(&e, &a)
        );
    }
}

pub fn first_diff<T: PartialEq + std::fmt::Debug>(e: &[T], a: &[T]) -> String {
    for (i, (x, y)) in e.iter().zip(a).enumerate() {
        if x != y {
            return format!("first difference at {i}:\n  expected {x:?}\n  got      {y:?}");
        }
    }
    format!("lengths differ: expected {}, got {}", e.len(), a.len())
}

/// A root span with start, end, status and an input naming `word`, alone in its trace.
pub fn root_span(trace: u64, word: &str) -> Span {
    let mut t = [0u8; 16];
    t[..8].copy_from_slice(&trace.to_le_bytes());
    t[8] = 0xa5;
    Span {
        project: "project-0".into(),
        trace_id: TraceId(t),
        span_id: SpanId([0x11; 8]),
        name: Some("agent".into()),
        start_ns: Some(T0 + trace * 1000),
        end_ns: Some(T0 + trace * 1000 + 500),
        status: Some(newdb_types::Status::Ok),
        run_type: Some(newdb_types::RunType::Agent),
        inputs: Some(format!("{{\"content\":\"{word} {}\"}}", "lorem ".repeat(100)).into_bytes()),
        ..Span::default()
    }
}
