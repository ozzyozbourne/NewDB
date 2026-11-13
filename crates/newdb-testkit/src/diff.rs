//! Differential checks: feed the same posts to two stores and compare every answer.
//! Lanes use this with [`crate::Oracle`] as the expected side; the integrator uses it on
//! the real engine.

use newdb_types::{RunQuery, RunType, SearchQuery, Span, Status, Store, TraceId, TraceQuery};

/// The queries a differential run asks, derived from the posts so they hit real data.
pub fn standard_queries(posts: &[Span]) -> (Vec<TraceQuery>, Vec<RunQuery>, Vec<SearchQuery>) {
    let mut traces: Vec<(String, TraceId)> = posts
        .iter()
        .map(|s| (s.project.clone(), s.trace_id))
        .collect();
    traces.sort();
    traces.dedup();
    let mut tq: Vec<TraceQuery> = traces
        .iter()
        .step_by((traces.len() / 10).max(1))
        .flat_map(|(p, t)| {
            [true, false].map(|payloads| TraceQuery {
                project: p.clone(),
                trace_id: *t,
                payloads,
                timings: false,
            })
        })
        .collect();
    tq.push(TraceQuery {
        project: "project-0".into(),
        trace_id: TraceId([0xee; 16]),
        ..Default::default()
    });

    let base = RunQuery {
        limit: 50,
        ..Default::default()
    };
    let rq = vec![
        base.clone(),
        RunQuery {
            project: Some("project-0".into()),
            ..base.clone()
        },
        RunQuery {
            status: Some(Status::Error),
            ..base.clone()
        },
        RunQuery {
            run_type: Some(RunType::Agent),
            min_latency_ns: Some(20_000_000),
            ..base.clone()
        },
        RunQuery {
            min_total_tokens: Some(1),
            ..base.clone()
        },
        RunQuery {
            min_cost_micro_usd: Some(10_000),
            limit: 5,
            ..base.clone()
        },
        RunQuery {
            limit: 0,
            ..base.clone()
        },
    ];

    let sq = [
        "refund",
        "refund policy",
        "REFUND Policy",
        "nonexistentword",
        "",
        "booking hotel flight",
    ]
    .into_iter()
    .map(|t| SearchQuery {
        text: t.into(),
        limit: 50,
        ..Default::default()
    })
    .chain([SearchQuery {
        project: Some("project-1".into()),
        text: "order".into(),
        limit: 10,
        timings: false,
    }])
    .collect();
    (tq, rq, sq)
}

/// Ingests `posts` into both stores in `batch`-sized calls, then asks the standard queries
/// plus one page-2 runs query. Returns a description of the first difference.
pub fn compare(
    expected: &dyn Store,
    actual: &dyn Store,
    posts: &[Span],
    batch: usize,
) -> Result<(), String> {
    for chunk in posts.chunks(batch.max(1)) {
        expected
            .ingest(chunk.to_vec())
            .map_err(|e| format!("expected ingest: {e}"))?;
        actual
            .ingest(chunk.to_vec())
            .map_err(|e| format!("actual ingest: {e}"))?;
    }
    let (tq, rq, sq) = standard_queries(posts);
    for q in &tq {
        let (e, a) = (expected.trace(q), actual.trace(q));
        let (e, a) = (
            e.map_err(|x| x.to_string())?.value,
            a.map_err(|x| x.to_string())?.value,
        );
        if e != a {
            return Err(format!(
                "trace {:?} payloads={}: expected {} spans, got {}; first diff: {}",
                q.trace_id,
                q.payloads,
                e.len(),
                a.len(),
                first_diff(&e, &a)
            ));
        }
    }
    for q in &rq {
        let e = expected.runs(q).map_err(|x| x.to_string())?.value;
        let a = actual.runs(q).map_err(|x| x.to_string())?.value;
        if e != a {
            return Err(format!("runs {q:?}: first diff: {}", first_diff(&e, &a)));
        }
        if let Some(last) = e.last() {
            let page2 = RunQuery {
                before: Some(last.key()),
                ..q.clone()
            };
            let e2 = expected.runs(&page2).map_err(|x| x.to_string())?.value;
            let a2 = actual.runs(&page2).map_err(|x| x.to_string())?.value;
            if e2 != a2 {
                return Err(format!(
                    "runs page 2 {page2:?}: first diff: {}",
                    first_diff(&e2, &a2)
                ));
            }
        }
    }
    for q in &sq {
        let e = expected.search(q).map_err(|x| x.to_string())?.value;
        let a = actual.search(q).map_err(|x| x.to_string())?.value;
        if e != a {
            return Err(format!(
                "search {:?}: first diff: {}",
                q.text,
                first_diff(&e, &a)
            ));
        }
    }
    Ok(())
}

fn first_diff<T: PartialEq + std::fmt::Debug>(e: &[T], a: &[T]) -> String {
    for (i, (x, y)) in e.iter().zip(a).enumerate() {
        if x != y {
            return format!("index {i}: expected {x:?}, got {y:?}");
        }
    }
    format!("lengths differ: expected {}, got {}", e.len(), a.len())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Oracle;
    use crate::r#gen::{GenConfig, generate};

    #[test]
    fn oracle_agrees_with_itself_and_answers_are_not_trivial() {
        let posts = generate(&GenConfig::small(1));
        let (a, b) = (Oracle::new(), Oracle::new());
        compare(&a, &b, &posts, 17).unwrap();
        let runs = a
            .runs(&RunQuery {
                limit: 50,
                ..Default::default()
            })
            .unwrap()
            .value;
        assert_eq!(runs.len(), 20);
        assert!(runs.windows(2).all(|w| w[0].key() > w[1].key()));
        let hits = a
            .search(&SearchQuery {
                text: "refund".into(),
                limit: 1000,
                ..Default::default()
            })
            .unwrap()
            .value;
        assert!(!hits.is_empty());
    }
}
