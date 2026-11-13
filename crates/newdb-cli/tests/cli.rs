//! Runs the `newdb` binary the way users and scripts do.

use std::process::{Command, Output};

use newdb_tools::BenchReport;

fn newdb(args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_newdb"))
        .args(args)
        .output()
        .expect("run newdb")
}

fn stderr(o: &Output) -> String {
    String::from_utf8_lossy(&o.stderr).into_owned()
}

#[test]
fn bench_on_memory_writes_a_report_that_compare_reads() {
    let dir = std::env::temp_dir().join(format!("newdb-cli-bench-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let a = dir.join("a.json");
    let a = a.to_str().unwrap();
    let o = newdb(&[
        "bench",
        "--memory",
        "--traces=5",
        "--batch",
        "10",
        "--iterations",
        "3",
        "--out",
        a,
    ]);
    assert!(o.status.success(), "{}", stderr(&o));
    let report = BenchReport::from_json(&std::fs::read_to_string(a).unwrap()).unwrap();
    assert_eq!(
        String::from_utf8(o.stdout).unwrap(),
        report.to_json(),
        "stdout and --out hold the same report"
    );
    assert!(report.posts > 0);
    assert_eq!(report.reads.len(), 6);
    // The binary installs the counting allocator.
    assert_eq!(report.allocations.len(), 6);

    let o = newdb(&["bench", "--compare", a, a]);
    assert!(o.status.success(), "{}", stderr(&o));
    let table = String::from_utf8(o.stdout).unwrap();
    assert!(table.contains("ingest posts/s"), "{table}");
    assert!(table.contains("search_refund_policy p99"), "{table}");
    std::fs::remove_dir_all(&dir).unwrap();
}

#[test]
fn bad_arguments_exit_2_with_usage() {
    for args in [
        &[][..],
        &["frobnicate"],
        &["bench"],
        &["bench", "--memory", "--traces", "many"],
        &["bench", "--memory", "--data", "/tmp/x"],
        &["bench", "--compare", "only-one.json"],
        &["serve", "--memory", "--addr", "localhost"],
        &["serve", "--memory", "--mapping", "m.json"],
        &["serve", "--memory", "--pricing", "p.json"],
        &["bench", "--memory", "--seal-every", "10"],
        &["bench", "--memory", "--preset", "forge"],
        &["bench", "--memory", "--preset", "nope"],
        &["bench", "--memory", "--max-depth", "-1"],
        &["inspect"],
        &["verify", "--data"],
        &["rebuild", "--data", "d", "--bogus"],
    ] {
        let o = newdb(args);
        assert_eq!(o.status.code(), Some(2), "{args:?}: {}", stderr(&o));
        assert!(stderr(&o).contains("usage:"), "{args:?}");
    }
}

#[test]
fn compare_of_a_missing_or_corrupt_report_fails_cleanly() {
    let o = newdb(&[
        "bench",
        "--compare",
        "/nonexistent/a.json",
        "/nonexistent/b.json",
    ]);
    assert_eq!(o.status.code(), Some(1));
    assert!(stderr(&o).contains("/nonexistent/a.json"));
}

#[test]
fn serve_rejects_bad_mapping_or_pricing_files_before_opening_the_store() {
    let dir = std::env::temp_dir().join(format!("newdb-cli-serve-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let data = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../newdb-otlp/data");
    let good = |f: &str| data.join(f).to_str().unwrap().to_string();
    let bad = dir.join("bad.json");
    std::fs::write(&bad, "{not json").unwrap();
    let bad = bad.to_str().unwrap();
    let store = dir.join("store");
    let store = store.to_str().unwrap();

    for (mapping, pricing, want) in [
        (bad.to_string(), good("pricing.json"), "mapping file"),
        (good("mapping.json"), bad.to_string(), "pricing file"),
        (
            good("mapping.json"),
            "/nonexistent/p.json".into(),
            "/nonexistent/p.json",
        ),
    ] {
        let o = newdb(&[
            "serve",
            "--data",
            store,
            "--mapping",
            &mapping,
            "--pricing",
            &pricing,
        ]);
        assert_eq!(o.status.code(), Some(1), "{}", stderr(&o));
        assert!(stderr(&o).contains(want), "{want}: {}", stderr(&o));
    }
    assert!(
        !std::path::Path::new(store).exists(),
        "the store was not opened"
    );
    std::fs::remove_dir_all(&dir).unwrap();
}

#[test]
fn bench_on_a_real_db_seals_and_inspect_verify_rebuild_check_it() {
    let dir = std::env::temp_dir().join(format!("newdb-cli-engine-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let data = dir.join("db");
    let data = data.to_str().unwrap();
    let ok = |args: &[&str]| {
        let o = newdb(args);
        assert!(o.status.success(), "{args:?}: {}", stderr(&o));
        String::from_utf8(o.stdout).unwrap()
    };

    let report = ok(&[
        "bench",
        "--data",
        data,
        "--no-fsync",
        "--traces",
        "8",
        "--iterations",
        "2",
    ]);
    let report = BenchReport::from_json(&report).unwrap();
    assert_eq!(report.reads.len(), 6);

    let inspect = ok(&["inspect", "--data", data]);
    let row = inspect.lines().nth(1).unwrap();
    assert!(
        row.split_whitespace().nth(1) == Some("yes"),
        "sealed:\n{inspect}"
    );
    assert!(
        row.split_whitespace().nth(2) == Some(&report.posts.to_string()[..]),
        "{inspect}"
    );
    for layout in [
        "trace_clustered.v1",
        "runs_by_time.v1",
        "trace_filter.v1",
        "fts.v1",
    ] {
        assert!(row.contains(layout), "{layout}:\n{inspect}");
    }
    assert!(inspect.contains("spans per trace: p50"), "{inspect}");

    assert_eq!(
        ok(&["verify", "--data", data]),
        "verified 1 partitions, 4 layouts: 0 problems\n"
    );
    assert_eq!(
        ok(&["rebuild", "--data", data, "--layout", "fts"]),
        "rebuilt fts\n"
    );
    assert_eq!(ok(&["rebuild", "--data", data]), "rebuilt every layout\n");
    assert_eq!(
        ok(&["verify", "--data", data]),
        "verified 1 partitions, 4 layouts: 0 problems\n"
    );

    let o = newdb(&["rebuild", "--data", data, "--layout", "no_such_layout"]);
    assert_eq!(o.status.code(), Some(1), "{}", stderr(&o));

    // Damage one byte of a sealed layout file: verify reports it and exits 1.
    let layouts = std::fs::read_dir(data)
        .unwrap()
        .map(|e| e.unwrap().path().join("layouts/runs_by_time.v1"))
        .find(|p| p.is_dir())
        .unwrap();
    let file = std::fs::read_dir(&layouts)
        .unwrap()
        .next()
        .unwrap()
        .unwrap()
        .path();
    let mut bytes = std::fs::read(&file).unwrap();
    let last = bytes.len() - 1;
    bytes[last] ^= 0xff;
    std::fs::write(&file, bytes).unwrap();
    let o = newdb(&["verify", "--data", data]);
    let out = String::from_utf8_lossy(&o.stdout);
    assert_eq!(o.status.code(), Some(1), "{out}{}", stderr(&o));
    assert!(out.contains("problem: "), "{out}");
    std::fs::remove_dir_all(&dir).unwrap();
}

#[test]
fn bench_seal_every_spreads_the_workload_over_partitions_and_reads_whole_target_traces() {
    let dir = std::env::temp_dir().join(format!("newdb-cli-seal-every-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let data = dir.join("db");
    let data = data.to_str().unwrap();
    let o = newdb(&[
        "bench",
        "--data",
        data,
        "--no-fsync",
        "--traces",
        "30",
        "--payload-median",
        "64",
        "--target-trace-spans",
        "300",
        "--target-traces",
        "2",
        "--seal-every",
        "100",
        "--batch",
        "50",
        "--iterations",
        "2",
    ]);
    assert!(o.status.success(), "{}", stderr(&o));
    let r = BenchReport::from_json(&String::from_utf8(o.stdout).unwrap()).unwrap();
    assert_eq!(r.trace_spans, 300, "the target trace is read back whole");
    assert_eq!(r.seals, r.posts / 100);
    assert!(!r.posts.is_multiple_of(100) && r.seals >= 5, "{r:?}");
    assert_eq!(
        r.partitions,
        Some(r.seals + 1),
        "sealed ones plus the active one"
    );
    assert!(r.bytes_on_disk.unwrap() > 0);

    let o = newdb(&["inspect", "--data", data]);
    let inspect = String::from_utf8(o.stdout).unwrap();
    let rows: Vec<Vec<&str>> = inspect
        .lines()
        .skip(1)
        .take_while(|l| !l.is_empty())
        .map(|l| l.split_whitespace().collect())
        .collect();
    assert_eq!(rows.len() as u64, r.seals + 1, "{inspect}");
    assert_eq!(
        rows.iter().filter(|c| c[1] == "yes").count() as u64,
        r.seals
    );
    let posts: u64 = rows.iter().map(|c| c[2].parse::<u64>().unwrap()).sum();
    assert_eq!(posts, r.posts, "{inspect}");
    let o = newdb(&["verify", "--data", data]);
    assert_eq!(
        String::from_utf8(o.stdout).unwrap(),
        format!(
            "verified {} partitions, {} layouts: 0 problems\n",
            r.seals,
            r.seals * 4
        )
    );
    std::fs::remove_dir_all(&dir).unwrap();
}
