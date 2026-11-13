//! Readers calling trace, runs and search while the writer ingests, moves across hours and
//! seals. Every answer must equal the answer of the database after some prefix of the
//! posts, at least as far as the batches acknowledged before the read started and at most
//! up to the batch in flight when it ended; and one reader never sees an older prefix
//! after a newer one.

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use newdb_testkit::Oracle;
use newdb_testkit::r#gen::{GenConfig, Rng, generate};
use newdb_types::{RunQuery, SearchQuery, Status, Store, TraceQuery};

use crate::common::*;

/// One read, answered as a comparable string (Debug of the value).
type Query = Box<dyn Fn(&dyn Store) -> String + Send + Sync>;

fn queries(posts: &[newdb_types::Span]) -> Vec<(String, Query)> {
    let mut out: Vec<(String, Query)> = Vec::new();
    let runs =
        |q: RunQuery| -> Query { Box::new(move |s| format!("{:?}", s.runs(&q).unwrap().value)) };
    out.push((
        "runs newest 20".into(),
        runs(RunQuery {
            limit: 20,
            ..Default::default()
        }),
    ));
    out.push((
        "runs errors".into(),
        runs(RunQuery {
            status: Some(Status::Error),
            limit: 1000,
            ..Default::default()
        }),
    ));
    for text in ["refund", "refund policy", "timeout"] {
        let q = SearchQuery {
            text: text.into(),
            limit: 1000,
            ..Default::default()
        };
        out.push((
            format!("search {text}"),
            Box::new(move |s| format!("{:?}", s.search(&q).unwrap().value)),
        ));
    }
    let traces = traces_of(posts);
    for (project, trace_id) in traces.iter().step_by(traces.len() / 6 + 1).cloned() {
        let q = TraceQuery {
            project,
            trace_id,
            payloads: true,
            timings: false,
        };
        out.push((
            format!("trace {trace_id:?}"),
            Box::new(move |s| format!("{:?}", s.trace(&q).unwrap().value)),
        ));
    }
    out
}

#[test]
fn concurrent_reads_see_a_prefix_state_while_writer_ingests_and_seals() {
    let posts = generate(&GenConfig {
        traces: 40,
        max_spans_per_trace: 12,
        payload_median: 64,
        open_then_close_rate: 0.4,
        ..GenConfig::small(41)
    });
    // Batch boundaries, then the expected answer of every query after every post prefix
    // (a batch may become visible post by post).
    let mut rng = Rng::new(41);
    let mut ends = vec![0usize];
    while *ends.last().unwrap() < posts.len() {
        let next = ends.last().unwrap() + 1 + rng.below(12) as usize;
        ends.push(next.min(posts.len()));
    }
    let queries = queries(&posts);
    let oracle = Oracle::new();
    let mut expected: Vec<Vec<String>> = vec![queries.iter().map(|(_, q)| q(&oracle)).collect()];
    for p in &posts {
        oracle.ingest(vec![p.clone()]).unwrap();
        expected.push(queries.iter().map(|(_, q)| q(&oracle)).collect());
    }

    let dir = TempDir::new("concurrent");
    let clock = clock();
    let db = open(&dir, &clock);
    let acked = AtomicUsize::new(0); // batches acknowledged
    let writing = AtomicBool::new(true);
    let reads = AtomicUsize::new(0);
    std::thread::scope(|s| {
        s.spawn(|| {
            let mut rng = Rng::new(42);
            for (b, w) in ends.windows(2).enumerate() {
                db.ingest(posts[w[0]..w[1]].to_vec()).expect("ingest");
                acked.store(b + 1, Ordering::SeqCst);
                if rng.chance(0.2) {
                    clock.advance(HOUR);
                }
                if rng.chance(0.15) {
                    db.seal_active().expect("seal_active");
                }
            }
            writing.store(false, Ordering::SeqCst);
        });
        for reader in 0..3 {
            let (db, ends, queries, expected) = (&db, &ends, &queries, &expected);
            let (acked, writing, reads) = (&acked, &writing, &reads);
            s.spawn(move || {
                let mut floor = 0usize; // lowest post prefix this reader may still see
                let mut i = reader;
                loop {
                    let last_round = !writing.load(Ordering::SeqCst);
                    let (qi, (name, q)) = {
                        let k = i % queries.len();
                        (k, &queries[k])
                    };
                    i += 1;
                    let lo = ends[acked.load(Ordering::SeqCst)];
                    let got = q(db);
                    let hi_batch = (acked.load(Ordering::SeqCst) + 1).min(ends.len() - 1);
                    let hi = ends[hi_batch];
                    let from = lo.max(floor);
                    let Some(p) = (from..=hi).find(|&p| expected[p][qi] == got) else {
                        let older = (0..from).rev().find(|&p| expected[p][qi] == got);
                        panic!(
                            "reader {reader}: {name} matches no post prefix in {from}..={hi} \
                             (acked prefix at start {lo}, reader floor {floor}; matches older \
                             prefix {older:?})\ngot: {got}"
                        );
                    };
                    floor = p;
                    reads.fetch_add(1, Ordering::Relaxed);
                    if last_round && i > queries.len() * 2 {
                        break;
                    }
                }
            });
        }
    });
    assert_same(&oracle_of(&posts), &db, &posts, "after concurrent run");
    assert!(db.partitions().unwrap().len() >= 4);
    println!("{} concurrent reads checked", reads.load(Ordering::Relaxed));
}
