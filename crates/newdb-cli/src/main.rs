//! `newdb` (DESIGN.md sections 8.3 and 9). Argument parsing is written by hand; no clap.
//!
//! ```text
//! newdb serve   --data <dir> [--addr 127.0.0.1:4318] [--no-fsync] [--memory]
//!               [--mapping mapping.json --pricing pricing.json]
//! newdb inspect --data <dir>
//! newdb verify  --data <dir>
//! newdb rebuild --data <dir> [--layout <name>]
//! newdb bench   [--data <dir> | --memory] [--no-fsync] [--no-seal] [--traces N] [--batch N] [--iterations N] [--out report.json]
//! newdb bench   --compare a.json b.json
//! ```
//! `--memory` runs against `newdb_testkit::Oracle`, so every command except the engine-only
//! ones (inspect, verify, rebuild) works before the engine lands.
//!
//! `bench --data` seals the active partition after the ingest, so the timed reads walk
//! sealed, memory-mapped layout files as most reads do; `--no-seal` times the in-memory
//! active partition instead. Use a fresh directory: the workload is ingested on top of
//! whatever is there.
//!
//! `inspect` and `verify` open the directory read-only, so they work next to a running
//! `serve`. `rebuild` needs the writer.
//!
//! `serve` normalizes OTLP with the mapping and pricing tables built into `newdb-otlp`, or
//! with the two files given by `--mapping` and `--pricing` (the same format as
//! `crates/newdb-otlp/data/`). It prints `listening on http://<addr>` on stdout once it accepts connections, so
//! `--addr 127.0.0.1:0` can be used to get a free port. Exit codes: 0 success, 1 failure
//! (including `verify` problems), 2 bad arguments.

use std::collections::HashMap;
use std::io::Write;
use std::net::SocketAddr;
use std::process::ExitCode;
use std::sync::Arc;

use newdb_engine::{Config, Db, InspectReport};
use newdb_otlp::Normalizer;
use newdb_testkit::Oracle;
use newdb_tools::{BenchConfig, BenchReport, CountingAlloc};
use newdb_types::Store;

#[global_allocator]
static ALLOC: CountingAlloc = CountingAlloc;

const USAGE: &str = "usage:
  newdb serve   (--data <dir> | --memory) [--addr 127.0.0.1:4318] [--no-fsync]
                [--mapping mapping.json --pricing pricing.json]
  newdb inspect --data <dir>
  newdb verify  --data <dir>
  newdb rebuild --data <dir> [--layout <name>]
  newdb bench   (--data <dir> | --memory) [--no-fsync] [--no-seal] [--traces N] [--batch N] [--iterations N] [--out report.json]
  newdb bench   --compare a.json b.json";

/// What went wrong: bad arguments (exit 2) or a failed command (exit 1).
enum Fail {
    Usage(String),
    Run(String),
}

impl From<newdb_types::Error> for Fail {
    fn from(e: newdb_types::Error) -> Self {
        Fail::Run(e.to_string())
    }
}

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    match run(&args) {
        Ok(code) => code,
        Err(Fail::Usage(m)) => {
            eprintln!("newdb: {m}\n{USAGE}");
            ExitCode::from(2)
        }
        Err(Fail::Run(m)) => {
            eprintln!("newdb: {m}");
            ExitCode::FAILURE
        }
    }
}

fn run(args: &[String]) -> Result<ExitCode, Fail> {
    let Some((cmd, rest)) = args.split_first() else {
        return Err(Fail::Usage("missing command".into()));
    };
    match cmd.as_str() {
        "serve" => serve(&Args::parse(
            rest,
            &["data", "addr", "mapping", "pricing"],
            &["no-fsync", "memory"],
        )?),
        "inspect" => inspect(&Args::parse(rest, &["data"], &[])?),
        "verify" => verify(&Args::parse(rest, &["data"], &[])?),
        "rebuild" => rebuild(&Args::parse(rest, &["data", "layout"], &[])?),
        "bench" => bench(&Args::parse(
            rest,
            &["data", "traces", "batch", "iterations", "out"],
            &["no-fsync", "no-seal", "memory", "compare"],
        )?),
        "help" | "-h" | "--help" => {
            println!("{USAGE}");
            Ok(ExitCode::SUCCESS)
        }
        other => Err(Fail::Usage(format!("unknown command {other:?}"))),
    }
}

/// Parsed `--name value`, `--name=value` and `--switch` options plus positional arguments.
struct Args {
    values: HashMap<String, String>,
    switches: Vec<String>,
    positional: Vec<String>,
}

impl Args {
    fn parse(args: &[String], with_value: &[&str], switches: &[&str]) -> Result<Args, Fail> {
        let mut out = Args {
            values: HashMap::new(),
            switches: Vec::new(),
            positional: Vec::new(),
        };
        let mut it = args.iter();
        while let Some(a) = it.next() {
            let Some(flag) = a.strip_prefix("--") else {
                out.positional.push(a.clone());
                continue;
            };
            let (name, inline) = match flag.split_once('=') {
                Some((n, v)) => (n, Some(v.to_string())),
                None => (flag, None),
            };
            if with_value.contains(&name) {
                let v = match inline {
                    Some(v) => v,
                    None => it
                        .next()
                        .cloned()
                        .ok_or_else(|| Fail::Usage(format!("--{name} needs a value")))?,
                };
                out.values.insert(name.to_string(), v);
            } else if switches.contains(&name) && inline.is_none() {
                out.switches.push(name.to_string());
            } else {
                return Err(Fail::Usage(format!("unknown option {a:?}")));
            }
        }
        Ok(out)
    }

    fn get(&self, name: &str) -> Option<&str> {
        self.values.get(name).map(String::as_str)
    }

    fn has(&self, name: &str) -> bool {
        self.switches.iter().any(|s| s == name)
    }

    fn number(&self, name: &str) -> Result<Option<usize>, Fail> {
        self.get(name)
            .map(|v| {
                v.parse()
                    .map_err(|_| Fail::Usage(format!("--{name} must be a number, not {v:?}")))
            })
            .transpose()
    }

    fn no_positional(&self) -> Result<(), Fail> {
        match self.positional.first() {
            Some(p) => Err(Fail::Usage(format!("unexpected argument {p:?}"))),
            None => Ok(()),
        }
    }

    fn data(&self) -> Result<&str, Fail> {
        self.get("data")
            .ok_or_else(|| Fail::Usage("--data <dir> is required".into()))
    }

    /// The store selected by `--data <dir>` or `--memory`.
    fn store(&self) -> Result<Arc<dyn Store>, Fail> {
        match (self.get("data"), self.has("memory")) {
            (Some(_), true) => Err(Fail::Usage("use --data or --memory, not both".into())),
            (None, false) => Err(Fail::Usage("--data <dir> or --memory is required".into())),
            (None, true) => Ok(Arc::new(Oracle::new())),
            (Some(_), false) => Ok(Arc::new(self.engine()?)),
        }
    }

    /// The engine on `--data <dir>`, honoring `--no-fsync`.
    fn engine(&self) -> Result<Db, Fail> {
        let config = Config {
            fsync: !self.has("no-fsync"),
            ..Config::default()
        };
        Ok(Db::open(self.data()?, config)?)
    }
}

fn open_read_only(dir: &str) -> Result<Db, Fail> {
    let config = Config {
        read_only: true,
        ..Config::default()
    };
    Ok(Db::open(dir, config)?)
}

fn serve(a: &Args) -> Result<ExitCode, Fail> {
    a.no_positional()?;
    let addr: SocketAddr = a
        .get("addr")
        .unwrap_or("127.0.0.1:4318")
        .parse()
        .map_err(|_| Fail::Usage("--addr must be <ip>:<port>".into()))?;
    let normalizer = match (a.get("mapping"), a.get("pricing")) {
        (None, None) => Normalizer::builtin(),
        (Some(m), Some(p)) => Normalizer::from_files(m.as_ref(), p.as_ref())
            .map_err(|e| Fail::Run(format!("loading --mapping {m} --pricing {p}: {e}")))?,
        _ => return Err(Fail::Usage("--mapping and --pricing go together".into())),
    };
    let store = a.store()?;
    let config = newdb_server::ServerConfig {
        addr,
        max_body_bytes: 64 << 20,
        normalizer: Arc::new(normalizer),
    };
    let server = newdb_server::start(store, config)?;
    println!("listening on http://{}", server.addr());
    let _ = std::io::stdout().flush();
    server.wait();
    Ok(ExitCode::SUCCESS)
}

fn inspect(a: &Args) -> Result<ExitCode, Fail> {
    a.no_positional()?;
    let report = open_read_only(a.data()?)?.inspect()?;
    print!("{}", format_inspect(&report));
    Ok(ExitCode::SUCCESS)
}

fn format_inspect(r: &InspectReport) -> String {
    let mut s = format!(
        "{:<16} {:>6} {:>10} {:>10} {:>12} {:>14}  layouts\n",
        "partition", "sealed", "posts", "spans", "log bytes", "payload bytes"
    );
    for p in &r.partitions {
        let layouts: Vec<String> = p
            .layouts
            .iter()
            .map(|(n, v, b)| format!("{n}.v{v} ({b} B)"))
            .collect();
        s += &format!(
            "{:<16} {:>6} {:>10} {:>10} {:>12} {:>14}  {}\n",
            p.name,
            if p.sealed { "yes" } else { "no" },
            p.posts,
            p.spans,
            p.log_bytes,
            p.payload_bytes,
            layouts.join(", ")
        );
    }
    let (a, b, c) = r.spans_per_trace;
    s += &format!("\nspans per trace: p50 {a}, p99 {b}, max {c}\n");
    let (a, b, c) = r.payload_bytes;
    s += &format!("payload bytes:   p50 {a}, p99 {b}, max {c}\n");
    s += &format!(
        "spans updated across partitions: {:.2}%\n",
        r.cross_partition_span_share * 100.0
    );
    if !r.reads.is_empty() {
        s += &format!(
            "\n{:<24} {:>8} {:>12} {:>12}\n",
            "read", "count", "p50 µs", "p99 µs"
        );
        for (op, n, p50, p99) in &r.reads {
            s += &format!(
                "{op:<24} {n:>8} {:>12.1} {:>12.1}\n",
                *p50 as f64 / 1e3,
                *p99 as f64 / 1e3
            );
        }
    }
    s
}

fn verify(a: &Args) -> Result<ExitCode, Fail> {
    a.no_positional()?;
    let report = open_read_only(a.data()?)?.verify()?;
    for p in &report.problems {
        println!("problem: {p}");
    }
    println!(
        "verified {} partitions, {} layouts: {} problems",
        report.partitions_checked,
        report.layouts_checked,
        report.problems.len()
    );
    Ok(if report.problems.is_empty() {
        ExitCode::SUCCESS
    } else {
        ExitCode::FAILURE
    })
}

fn rebuild(a: &Args) -> Result<ExitCode, Fail> {
    a.no_positional()?;
    let db = Db::open(a.data()?, Config::default())?;
    db.rebuild(a.get("layout"))?;
    db.close()?;
    println!("rebuilt {}", a.get("layout").unwrap_or("every layout"));
    Ok(ExitCode::SUCCESS)
}

fn bench(a: &Args) -> Result<ExitCode, Fail> {
    if a.has("compare") {
        let [x, y] = a.positional.as_slice() else {
            return Err(Fail::Usage("--compare needs two report files".into()));
        };
        let load = |path: &str| -> Result<BenchReport, Fail> {
            let text = std::fs::read_to_string(path)
                .map_err(|e| Fail::Run(format!("reading {path}: {e}")))?;
            BenchReport::from_json(&text).map_err(|e| Fail::Run(format!("{path}: {e}")))
        };
        print!("{}", load(x)?.compare(&load(y)?));
        return Ok(ExitCode::SUCCESS);
    }
    a.no_positional()?;
    let mut config = BenchConfig::default();
    if let Some(n) = a.number("traces")? {
        config.workload.traces = n;
    }
    if let Some(n) = a.number("batch")? {
        config.batch = n;
    }
    if let Some(n) = a.number("iterations")? {
        config.iterations = n;
    }
    let report = match (a.get("data"), a.has("memory")) {
        (Some(_), false) => {
            let db = a.engine()?;
            let seal = !a.has("no-seal");
            let report = newdb_tools::run_bench_with(&db, &config, &mut || {
                if seal { db.seal_active() } else { Ok(()) }
            })?;
            db.close()?;
            report
        }
        _ => newdb_tools::run_bench(a.store()?.as_ref(), &config)?,
    };
    let json = report.to_json();
    if let Some(out) = a.get("out") {
        std::fs::write(out, &json).map_err(|e| Fail::Run(format!("writing {out}: {e}")))?;
    }
    print!("{json}");
    Ok(ExitCode::SUCCESS)
}
