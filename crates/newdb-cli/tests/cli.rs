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
