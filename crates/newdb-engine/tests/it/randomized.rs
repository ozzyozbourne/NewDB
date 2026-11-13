//! A large randomized differential run: 2,000 traces (500 in debug builds, to keep
//! `scripts/check` fast) ingested in batches of 1..=500 posts (scaled with the trace count),
//! with random clock jumps, `seal_active` calls and reopens (some read-only first),
//! checked against the oracle along the way and at the end.
//!
//! Set `NEWDB_IT_SEED` to rerun one seed and `NEWDB_IT_TRACES` to change the trace count;
//! a failure message names the seed and step.

use newdb_testkit::Oracle;
use newdb_testkit::diff::standard_queries;
use newdb_testkit::r#gen::{GenConfig, Rng, generate};
use newdb_types::{Error, Span, Store};

use crate::common::*;

fn traces() -> usize {
    match std::env::var("NEWDB_IT_TRACES") {
        Ok(s) => s.parse().expect("NEWDB_IT_TRACES must be a number"),
        Err(_) if cfg!(debug_assertions) => 500,
        Err(_) => 2000,
    }
}

fn seeds() -> Vec<u64> {
    match std::env::var("NEWDB_IT_SEED") {
        Ok(s) => vec![s.parse().expect("NEWDB_IT_SEED must be a number")],
        Err(_) => vec![1],
    }
}

/// The standard differential queries only (no every-trace sweep), for mid-run checks.
fn light_check(expected: &dyn Store, actual: &dyn Store, posts: &[Span], what: &str) {
    let (tq, rq, sq) = standard_queries(posts);
    for q in &tq {
        let (e, a) = (
            expected.trace(q).unwrap().value,
            actual.trace(q).expect("trace").value,
        );
        assert!(
            e == a,
            "{what}: trace {:?}\n{}",
            q.trace_id,
            first_diff(&e, &a)
        );
    }
    for q in &rq {
        let (e, a) = (
            expected.runs(q).unwrap().value,
            actual.runs(q).expect("runs").value,
        );
        assert!(e == a, "{what}: runs {q:?}\n{}", first_diff(&e, &a));
    }
    for q in &sq {
        let (e, a) = (
            expected.search(q).unwrap().value,
            actual.search(q).expect("search").value,
        );
        assert!(
            e == a,
            "{what}: search {:?}\n{}",
            q.text,
            first_diff(&e, &a)
        );
    }
}

#[test]
fn randomized_clock_jumps_seals_and_reopens_match_oracle() {
    let traces = traces();
    // Batches of 1..=500 posts at 2,000 traces, scaled down with the trace count so every
    // size takes about as many steps (clock jumps, seals, reopens).
    let max_batch = (500 * traces as u64 / 2000).max(1);
    for seed in seeds() {
        let posts = generate(&GenConfig {
            seed,
            traces,
            projects: 3,
            max_spans_per_trace: 20,
            payload_median: 64,
            open_then_close_rate: 0.3,
            duplicate_rate: 0.05,
            error_rate: 0.1,
            reorder_window: 200,
            ..GenConfig::default()
        });
        let dir = TempDir::new("randomized");
        let clock = clock();
        let mut rng = Rng::new(seed);
        let oracle = Oracle::new();
        let mut db = Some(open(&dir, &clock));
        let (mut done, mut step) = (0usize, 0usize);
        let mut events = Vec::new();
        while done < posts.len() {
            step += 1;
            let n = (1 + rng.below(max_batch) as usize).min(posts.len() - done);
            let batch = posts[done..done + n].to_vec();
            oracle.ingest(batch.clone()).unwrap();
            db.as_ref().unwrap().ingest(batch).expect("ingest");
            done += n;
            let what = format!("seed {seed}, step {step} ({done} posts)");
            if rng.chance(0.25) {
                let hours = 1 + rng.below(3) as u32;
                clock.advance(HOUR * hours);
                events.push(format!("{step}: +{hours}h"));
            } else if rng.chance(0.15) {
                // Within or just past the hour.
                clock.advance(std::time::Duration::from_secs(rng.below(3600)));
            }
            if rng.chance(0.12) {
                db.as_ref().unwrap().seal_active().expect("seal_active");
                events.push(format!("{step}: seal"));
            }
            if rng.chance(0.1) {
                let old = db.take().unwrap();
                if rng.chance(0.5) {
                    old.close().expect("close");
                } else {
                    drop(old);
                }
                if rng.chance(0.33) {
                    let ro = open_read_only(&dir, &clock);
                    light_check(&oracle, &ro, &posts[..done], &format!("{what}, read-only"));
                    let refused = ro.ingest(posts[..1].to_vec());
                    assert!(
                        matches!(refused, Err(Error::ReadOnly)),
                        "{what}: {refused:?}"
                    );
                }
                db = Some(open(&dir, &clock));
                events.push(format!("{step}: reopen"));
                light_check(&oracle, db.as_ref().unwrap(), &posts[..done], &what);
            }
        }
        let db = db.unwrap();
        let parts = db.partitions().unwrap();
        assert!(parts.len() >= 10, "seed {seed}: {} partitions", parts.len());
        assert_eq!(
            parts.iter().map(|p| p.posts).sum::<u64>(),
            posts.len() as u64
        );
        let what = format!("seed {seed}, end; events {events:?}");
        assert_same(&oracle, &db, &posts, &what);
        db.close().unwrap();
        let db = open(&dir, &clock);
        assert_same(&oracle, &db, &posts, &format!("{what}, reopened"));
        let report = db.verify().unwrap();
        assert!(
            report.problems.is_empty(),
            "seed {seed}: verify {:?}",
            report.problems
        );
    }
}
