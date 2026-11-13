//! Paging through `runs` with `before` returns every run exactly once, newest first, when
//! runs are spread over partitions whose start-time ranges overlap.

use std::collections::BTreeMap;

use newdb_testkit::r#gen::{GenConfig, Rng, generate};
use newdb_types::{Run, RunQuery, Span, Status, Store, TraceId};

use crate::common::*;

/// Pages through `query` with page size `limit`.
fn paged(store: &dyn Store, query: &RunQuery, limit: usize) -> Vec<Run> {
    let mut out: Vec<Run> = Vec::new();
    loop {
        let page = store
            .runs(&RunQuery {
                limit,
                before: out.last().map(Run::key),
                ..query.clone()
            })
            .expect("runs")
            .value;
        assert!(page.len() <= limit);
        if let (Some(last), Some(first)) = (out.last(), page.first()) {
            assert!(
                first.key() < last.key(),
                "page does not start after its cursor"
            );
        }
        if page.is_empty() {
            return out;
        }
        out.extend(page);
    }
}

#[test]
fn runs_pages_return_every_run_once_across_partitions() {
    let posts = generate(&GenConfig {
        traces: 80,
        max_spans_per_trace: 10,
        ..GenConfig::small(31)
    });
    // Deliver traces in shuffled order, a few per partition, so each partition's
    // start-time range overlaps the others.
    let mut by_trace: BTreeMap<TraceId, Vec<Span>> = BTreeMap::new();
    for s in &posts {
        by_trace.entry(s.trace_id).or_default().push(s.clone());
    }
    let mut groups: Vec<Vec<Span>> = by_trace.into_values().collect();
    let mut rng = Rng::new(31);
    for i in (1..groups.len()).rev() {
        groups.swap(i, rng.below(i as u64 + 1) as usize);
    }
    let delivered: Vec<Span> = groups.concat();

    let dir = TempDir::new("pagination");
    let clock = clock();
    let db = open(&dir, &clock);
    for chunk in groups.chunks(7) {
        db.ingest(chunk.concat()).unwrap();
        clock.advance(HOUR);
    }
    assert!(db.partitions().unwrap().len() >= 10);

    let oracle = oracle_of(&delivered);
    for query in [
        RunQuery::default(),
        RunQuery {
            status: Some(Status::Error),
            ..Default::default()
        },
        RunQuery {
            project: Some("project-1".into()),
            min_latency_ns: Some(5_000_000),
            ..Default::default()
        },
    ] {
        let expected = all_runs_matching(&oracle, &query);
        for limit in [1, 3, 7, 50] {
            let got = paged(&db, &query, limit);
            assert!(
                got == expected,
                "{query:?} in pages of {limit}\n{}",
                first_diff(&expected, &got)
            );
        }
    }
    assert_eq!(paged(&db, &RunQuery::default(), 5).len(), 80);
}

fn all_runs_matching(store: &dyn Store, query: &RunQuery) -> Vec<Run> {
    store
        .runs(&RunQuery {
            limit: 1_000_000,
            ..query.clone()
        })
        .unwrap()
        .value
}
