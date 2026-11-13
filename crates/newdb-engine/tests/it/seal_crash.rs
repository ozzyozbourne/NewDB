//! A crash while sealing: after some or all layout files are written but before MANIFEST
//! lists the partition. Reopening must recover the partition from its log (as unsealed),
//! answer like the oracle, verify clean, and seal it again later.
//!
//! Two tests:
//! - deterministic: seal for real, then put the directory back into the states a crash
//!   leaves behind (old MANIFEST, optionally a `MANIFEST.tmp`, half-written layout
//!   directories, no string table) and reopen;
//! - real: a child process ingests a batch and seals, and the parent SIGKILLs it at a
//!   different delay into `seal_active` each round.

use std::fs;
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::mpsc;
use std::time::{Duration, Instant};

use newdb_engine::{Config, Db};
use newdb_testkit::r#gen::{GenConfig, generate};
use newdb_types::{Span, Store};

use crate::common::*;

/// Partition directories (those holding a `log.seg`), sorted.
fn partition_dirs(root: &Path) -> Vec<PathBuf> {
    let mut out: Vec<PathBuf> = fs::read_dir(root)
        .unwrap()
        .map(|e| e.unwrap().path())
        .filter(|p| p.join("log.seg").exists())
        .collect();
    out.sort();
    out
}

fn listed(root: &Path) -> Vec<String> {
    newdb_format::manifest::read(root)
        .expect("MANIFEST readable")
        .map(|m| m.partitions.into_iter().map(|p| p.name).collect())
        .unwrap_or_default()
}

#[derive(Clone, Copy, Debug)]
enum Crash {
    /// Every layout and the string table written; MANIFEST still the old one.
    LayoutsDone,
    /// As above, and the new MANIFEST written to `MANIFEST.tmp` but not renamed.
    ManifestTmp,
    /// Crashed mid-way: the last layout is still a half-written temp directory, the
    /// string table is missing.
    MidLayouts,
}

#[test]
fn crash_between_layout_writes_and_manifest_update_recovers() {
    let posts = generate(&GenConfig::small(51));
    let third = posts.len() / 3;
    for crash in [Crash::LayoutsDone, Crash::ManifestTmp, Crash::MidLayouts] {
        let what = format!("{crash:?}");
        let dir = TempDir::new("seal-crash");
        let clock = clock();
        let db = open(&dir, &clock);
        ingest_all(&db, &posts[..third], 10);
        db.seal_active().unwrap();
        ingest_all(&db, &posts[third..2 * third], 10);
        db.close().unwrap();
        let old_manifest = fs::read(dir.path().join("MANIFEST")).unwrap();
        let before = listed(dir.path());

        let db = open(&dir, &clock);
        db.seal_active().unwrap();
        db.close().unwrap();
        let after = listed(dir.path());
        let sealed: Vec<&String> = after.iter().filter(|n| !before.contains(n)).collect();
        assert_eq!(sealed.len(), 1, "{what}: seal_active listed {sealed:?}");
        let part = dir.path().join(sealed[0]);

        // Put the disk back into the crash state.
        let new_manifest = fs::read(dir.path().join("MANIFEST")).unwrap();
        fs::write(dir.path().join("MANIFEST"), &old_manifest).unwrap();
        match crash {
            Crash::LayoutsDone => {}
            Crash::ManifestTmp => {
                fs::write(dir.path().join("MANIFEST.tmp"), &new_manifest).unwrap()
            }
            Crash::MidLayouts => {
                let layouts = part.join("layouts");
                let mut dirs: Vec<PathBuf> = fs::read_dir(&layouts)
                    .unwrap()
                    .map(|e| e.unwrap().path())
                    .collect();
                dirs.sort();
                let last = dirs.last().unwrap().clone();
                let tmp = layouts.join(format!(
                    ".{}.tmp",
                    last.file_name().unwrap().to_string_lossy()
                ));
                fs::rename(&last, &tmp).unwrap();
                for f in fs::read_dir(&tmp).unwrap() {
                    let f = f.unwrap().path();
                    let len = fs::metadata(&f).unwrap().len();
                    fs::OpenOptions::new()
                        .write(true)
                        .open(&f)
                        .unwrap()
                        .set_len(len / 2)
                        .unwrap();
                }
                let _ = fs::remove_file(part.join("strings.tbl"));
            }
        }
        assert_eq!(listed(dir.path()), before);

        let db =
            Db::open(dir.path(), config(&clock)).unwrap_or_else(|e| panic!("{what}: reopen: {e}"));
        let so_far = &posts[..2 * third];
        assert_same(&oracle_of(so_far), &db, so_far, &what);
        let report = db.verify().unwrap();
        assert!(
            report.problems.is_empty(),
            "{what}: verify after recovery {:?}",
            report.problems
        );

        // The recovered partition takes posts and seals again.
        ingest_all(&db, &posts[2 * third..], 10);
        db.seal_active().unwrap();
        let report = db.verify().unwrap();
        assert!(
            report.problems.is_empty(),
            "{what}: verify after reseal {:?}",
            report.problems
        );
        db.close().unwrap();
        let db = open(&dir, &clock);
        assert_same(
            &oracle_of(&posts),
            &db,
            &posts,
            &format!("{what}, resealed and reopened"),
        );
    }
}

const CHILD_DIR: &str = "NEWDB_IT_SEAL_CHILD_DIR";
const CHILD_ROUND: &str = "NEWDB_IT_SEAL_CHILD_ROUND";
const POSTS_PER_ROUND: u64 = 300;

/// Where the parent kills the child: as soon as it sees this state on disk (or at once,
/// or after the seal returned). The kill lands at that state or a little later.
#[derive(Clone, Copy, Debug)]
enum Target {
    AtOnce,
    LayoutsDir,
    TmpLayoutDir,
    TwoLayouts,
    StringTable,
    ManifestTmp,
    AfterSeal,
}

const TARGETS: [Target; 7] = [
    Target::AtOnce,
    Target::LayoutsDir,
    Target::TmpLayoutDir,
    Target::TwoLayouts,
    Target::StringTable,
    Target::ManifestTmp,
    Target::AfterSeal,
];
const ROUNDS: u64 = 2 * TARGETS.len() as u64;

fn round_posts(r: u64) -> Vec<Span> {
    (0..POSTS_PER_ROUND)
        .map(|i| root_span(r * POSTS_PER_ROUND + i, &format!("round{r}")))
        .collect()
}

/// Each round runs in its own hour, so each seals a fresh partition of the same size, and
/// a partition left unsealed by a kill is sealed by the next round's hour change.
fn round_config(r: u64) -> Config {
    let clock = clock();
    clock.advance(HOUR * r as u32);
    Config {
        fsync: true,
        ..config(&clock)
    }
}

/// Child: ingest round `r`, print `ACK`, then seal, printing `SEAL` just before.
fn run_child(dir: &str, r: u64) {
    let db = Db::open(dir, round_config(r)).expect("child: open");
    let mut out = std::io::stdout();
    db.ingest(round_posts(r)).expect("child: ingest");
    writeln!(out, "ACK").unwrap();
    writeln!(out, "SEAL").unwrap();
    out.flush().unwrap();
    let t = Instant::now();
    db.seal_active().expect("child: seal");
    writeln!(out, "SEALED {}", t.elapsed().as_micros()).unwrap();
    out.flush().unwrap();
    std::thread::sleep(Duration::from_secs(60));
}

fn layout_entries(part: &Path) -> (usize, usize) {
    let names: Vec<String> = fs::read_dir(part.join("layouts")).map_or(Vec::new(), |d| {
        d.filter_map(|e| e.ok())
            .map(|e| e.file_name().to_string_lossy().into_owned())
            .collect()
    });
    let done = names.iter().filter(|n| !n.starts_with('.')).count();
    (done, names.len() - done)
}

fn reached(target: Target, root: &Path, part: &Path) -> bool {
    match target {
        Target::AtOnce => true,
        Target::LayoutsDir => part.join("layouts").exists(),
        Target::TmpLayoutDir => layout_entries(part).1 > 0,
        Target::TwoLayouts => layout_entries(part).0 >= 2,
        Target::StringTable => part.join("strings.tbl").exists(),
        Target::ManifestTmp => root.join("MANIFEST.tmp").exists(),
        Target::AfterSeal => false,
    }
}

#[test]
fn sigkill_during_seal_active_recovers_every_round() {
    if let (Ok(dir), Ok(r)) = (std::env::var(CHILD_DIR), std::env::var(CHILD_ROUND)) {
        return run_child(&dir, r.parse().unwrap());
    }
    let name = format!(
        "{}::sigkill_during_seal_active_recovers_every_round",
        module_path!().split_once("::").map_or("", |m| m.1)
    );
    let dir = TempDir::new("seal-kill");
    let mut stages = Vec::new();
    let mut mid_seal = 0;
    for r in 0..ROUNDS {
        let target = TARGETS[r as usize % TARGETS.len()];
        let mut child = Command::new(std::env::current_exe().unwrap())
            .args([name.as_str(), "--exact", "--nocapture", "--include-ignored"])
            .env(CHILD_DIR, dir.path())
            .env(CHILD_ROUND, r.to_string())
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
        let mut acked = false;
        loop {
            match rx.recv_timeout(Duration::from_secs(60)) {
                // libtest prints `test <name> ... ` without a newline before our output.
                Ok(l) if l.ends_with("ACK") => acked = true,
                Ok(l) if l.ends_with("SEAL") => break,
                Ok(_) => {}
                Err(e) => {
                    let _ = child.kill();
                    panic!("round {r}: child stopped before sealing: {e:?}");
                }
            }
        }
        assert!(acked);
        // The partition being sealed: this round's hour sorts last.
        let part = partition_dirs(dir.path()).pop().expect("partition dir");
        let t = Instant::now();
        let mut finished = false;
        let mut seen = false;
        while !finished && t.elapsed() < Duration::from_secs(60) {
            if reached(target, dir.path(), &part) {
                seen = true;
                break;
            }
            finished = rx.try_iter().any(|l| l.contains("SEALED "));
        }
        let after = t.elapsed();
        child.kill().expect("SIGKILL");
        child.wait().unwrap();
        reader.join().unwrap();
        finished |= rx.try_iter().any(|l| l.contains("SEALED "));
        let stage = seal_stage(dir.path(), &part, r, target, seen, finished, after);
        let part_name = part.file_name().unwrap().to_string_lossy().into_owned();
        let (done, tmp) = layout_entries(&part);
        if !listed(dir.path()).contains(&part_name) && done + tmp > 0 {
            mid_seal += 1;
        }
        stages.push(stage);

        let db = Db::open(dir.path(), round_config(r))
            .unwrap_or_else(|e| panic!("round {r}: reopen after SIGKILL in seal: {e}"));
        let all: Vec<Span> = (0..=r).flat_map(round_posts).collect();
        let what = format!("round {r}, {}", stages.last().unwrap());
        assert_same(&oracle_of(&all), &db, &all, &what);
        assert_eq!(
            all_runs(&db).len(),
            all.len(),
            "{what}: each post exactly once"
        );
        let report = db.verify().unwrap();
        assert!(
            report.problems.is_empty(),
            "{what}: verify {:?}",
            report.problems
        );
        db.close().unwrap();
    }
    let report = stages.join("\n  ");
    println!("kill points:\n  {report}");
    assert!(
        mid_seal > 0,
        "no kill landed between the first layout write and the MANIFEST update:\n  {report}"
    );
}

/// Where the kill landed, from the disk: whether MANIFEST lists the partition and how far
/// its layout directories got.
fn seal_stage(
    root: &Path,
    part: &Path,
    r: u64,
    target: Target,
    seen: bool,
    finished: bool,
    after: Duration,
) -> String {
    let name = part.file_name().unwrap().to_string_lossy().into_owned();
    let (done, tmp) = layout_entries(part);
    format!(
        "round {r}: target {target:?} seen {seen} after {after:?}, seal finished {finished}, \
         {name} listed {}, layout dirs done {done} tmp {tmp}, strings.tbl {}",
        listed(root).contains(&name),
        part.join("strings.tbl").exists()
    )
}
