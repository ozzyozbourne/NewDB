//! Several threads ingesting at once with fsync on, one of them also sealing, killed with
//! SIGKILL. A batch is acknowledged once it is durable, before its layouts are applied, so
//! this checks both ends of that gap: every read a thread makes after its own `ingest`
//! returns sees the batch, and after the kill every acknowledged batch of every thread is
//! recovered, each thread's posts as a prefix of what it sent, equal to the oracle's.

use std::collections::HashSet;
use std::io::{BufRead, BufReader};
use std::process::{Command, Stdio};
use std::sync::mpsc;
use std::time::Duration;

use newdb_engine::{Config, Db};
use newdb_testkit::Oracle;
use newdb_testkit::diff::standard_queries;
use newdb_testkit::r#gen::{GenConfig, generate};
use newdb_types::{Span, Store, TraceQuery};

use crate::common::*;

const CHILD_DIR: &str = "NEWDB_IT_CONCURRENT_CRASH_DIR";
const STREAMS: usize = 4;
const CHUNK: usize = 7;
/// Kill once every thread has this many posts acknowledged.
const KILL_AFTER: usize = 140;

/// Thread `s`'s posts: its own project and traces, one post per span, so the spans a
/// project has after recovery count its recovered posts.
fn stream(s: usize) -> Vec<Span> {
    let mut seen = HashSet::new();
    generate(&GenConfig {
        traces: 120,
        payload_median: 256,
        ..GenConfig::small(700 + s as u64)
    })
    .into_iter()
    .filter(|p| seen.insert((p.trace_id, p.span_id)))
    .map(|p| Span {
        project: format!("stream-{s}"),
        ..p
    })
    .collect()
}

fn child_config() -> Config {
    Config {
        fsync: true,
        ..config(&clock())
    }
}

/// Child: each thread ingests its stream in small batches, reads its last post back, and
/// prints `ACK <thread> <posts>`; thread 0 also seals now and then.
fn run_child(dir: &str) {
    let db = Db::open(dir, child_config()).expect("child: open");
    std::thread::scope(|sc| {
        for s in 0..STREAMS {
            let db = &db;
            sc.spawn(move || {
                let posts = stream(s);
                for (i, c) in posts.chunks(CHUNK).enumerate() {
                    db.ingest(c.to_vec()).expect("child: ingest");
                    let last = c.last().unwrap();
                    let q = TraceQuery {
                        project: last.project.clone(),
                        trace_id: last.trace_id,
                        payloads: false,
                        timings: false,
                    };
                    let spans = db.trace(&q).expect("child: trace").value;
                    if !spans.iter().any(|x| x.span_id == last.span_id) {
                        println!("STALE {s} {}", i * CHUNK + c.len());
                    }
                    println!("ACK {s} {}", i * CHUNK + c.len());
                    if s == 0 && i % 9 == 8 {
                        db.seal_active().expect("child: seal");
                    }
                }
            });
        }
    });
    println!("DONE");
    std::thread::sleep(Duration::from_secs(60));
}

#[test]
fn sigkill_during_concurrent_ingest_keeps_every_acknowledged_batch() {
    if let Ok(dir) = std::env::var(CHILD_DIR) {
        return run_child(&dir);
    }
    let name = format!(
        "{}::sigkill_during_concurrent_ingest_keeps_every_acknowledged_batch",
        module_path!().split_once("::").map_or("", |m| m.1)
    );
    let dir = TempDir::new("concurrent-kill");
    let mut child = Command::new(std::env::current_exe().unwrap())
        .args([name.as_str(), "--exact", "--nocapture"])
        .env(CHILD_DIR, dir.path())
        .stdout(Stdio::piped())
        .spawn()
        .expect("spawn child");
    let stdout = child.stdout.take().unwrap();
    let (tx, rx) = mpsc::channel();
    let reader = std::thread::spawn(move || {
        for line in BufReader::new(stdout).lines().map_while(|l| l.ok()) {
            let _ = tx.send(line);
        }
    });
    let mut acked = [0usize; STREAMS];
    while acked.iter().any(|&n| n < KILL_AFTER) {
        let line = match rx.recv_timeout(Duration::from_secs(60)) {
            Ok(l) => l,
            Err(e) => {
                let _ = child.kill();
                panic!("child stopped early: {e:?}, acked {acked:?}");
            }
        };
        // libtest prints `test <name> ... ` without a newline before our first line.
        let words: Vec<&str> = line.split_whitespace().collect();
        match words.as_slice() {
            [.., "STALE", s, n] => {
                let _ = child.kill();
                panic!("thread {s} did not see its own batch ending at post {n}");
            }
            [.., "ACK", s, n] => acked[s.parse::<usize>().unwrap()] = n.parse().unwrap(),
            [.., "DONE"] => break,
            _ => {}
        }
    }
    child.kill().expect("SIGKILL");
    child.wait().unwrap();
    reader.join().unwrap();

    let db = Db::open(dir.path(), config(&clock())).expect("reopen after SIGKILL");
    let mut recovered = Vec::new();
    for (s, &acked) in acked.iter().enumerate() {
        let posts = stream(s);
        let spans: usize = traces_of(&posts)
            .into_iter()
            .map(|(project, trace_id)| {
                let q = TraceQuery {
                    project,
                    trace_id,
                    payloads: false,
                    timings: false,
                };
                db.trace(&q).unwrap().value.len()
            })
            .sum();
        assert!(
            spans >= acked,
            "thread {s}: {spans} posts recovered, {acked} acked"
        );
        recovered.extend_from_slice(&posts[..spans]);
    }
    // Versions depend on how the threads interleaved; everything else must match.
    let oracle = Oracle::new();
    oracle.ingest(recovered.clone()).unwrap();
    let unversioned = |mut v: Vec<Span>| {
        v.iter_mut().for_each(|s| s.version = 0);
        v
    };
    let (tq, rq, sq) = standard_queries(&recovered);
    let every_trace = traces_of(&recovered)
        .into_iter()
        .map(|(project, trace_id)| TraceQuery {
            project,
            trace_id,
            payloads: true,
            timings: false,
        });
    for q in tq.into_iter().chain(every_trace) {
        assert_eq!(
            unversioned(oracle.trace(&q).unwrap().value),
            unversioned(db.trace(&q).unwrap().value),
            "trace {:?}",
            q.trace_id
        );
    }
    for q in &rq {
        assert_eq!(oracle.runs(q).unwrap().value, db.runs(q).unwrap().value);
    }
    for q in &sq {
        assert_eq!(oracle.search(q).unwrap().value, db.search(q).unwrap().value);
    }
    db.seal_active().unwrap();
    let report = db.verify().unwrap();
    assert!(report.problems.is_empty(), "{:?}", report.problems);
}
