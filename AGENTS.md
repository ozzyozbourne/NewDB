# NewDB agent guide

NewDB is a local, embedded trace database for AI-agent telemetry with no query language:
the stored data structures are the API. Read `DESIGN.md` before changing anything.

## Layout

| Crate | Role | Owner |
| --- | --- | --- |
| `newdb-types` | Shared structs, IDs, `Store` trait, merge rule, tokenizer | Integrator |
| `newdb-testkit` | `Oracle` (brute-force reference `Store`), span generator, differential `compare` | Integrator |
| `newdb-format` | Log, payload store, string table, MANIFEST encodings | Lane 1 |
| `newdb-engine` | `Db`: partitions, group commit, recovery, snapshots, retention, verify, inspect, rebuild | Lane 1 |
| `newdb-layouts` | `trace_clustered`, `runs_by_time` (with error positions), `trace_filter`, `fts` | Lane 2 |
| `newdb-otlp` | OTLP protobuf/JSON → `Span`, GenAI mapping, pricing | Lane 3 |
| `newdb-server` | HTTP ingest on 4318 and the trace/runs/search API | Lane 3 |
| `newdb-tools` | bench runner, counting allocator | Lane 4 |
| `newdb-cli` | the `newdb` binary | Lane 4 |
| `newdb-py` | Python module `newdb` and `python/tests/` | Lane 4 |

## Rules

- **Edit only the crates your lane owns.** Workspace files, `newdb-types`, `newdb-testkit`
  and the public items of `newdb-engine/src/lib.rs` are shared contracts. If you need a
  change there, ask the integrator thread; do not edit them on your branch.
- **The oracle defines correct answers.** Semantics live in `newdb-types/src/store.rs`
  (doc comments, `Run::from_root`, `RunQuery::matches`) and `newdb-types/src/text.rs`. Any
  `Store` must return exactly what `newdb_testkit::Oracle` returns for the same posts; test
  with `newdb_testkit::diff::compare`.
- **Code against `&dyn Store`** in the server, tools, CLI and Python binding, so they work
  on the `Oracle` before the engine lands.
- **Core crates** (`types`, `format`, `layouts`, `engine`, `testkit`) take no serde, tokio,
  prost, hyper, pyo3, anyhow, thiserror, clap or rand, and no proc-macros. Encode by hand.
  `scripts/lint-layers` enforces this.
- **No `unwrap`/`expect` on data read from disk or the network.** Return `Error::Corrupt`
  or `Error::InvalidInput`.
- **Layout changes** bump the layout's `VERSION`; builders must write byte-identical files
  for the same batches.
- New dependencies go in `[workspace.dependencies]` in the root `Cargo.toml` via the
  integrator, with `default-features = false` where possible.

## Fast loop

Never build or test the whole workspace while iterating. Use, in order:

```sh
cargo check -p <crate>
cargo nextest run -p <crate> [filter]
scripts/check <crate>...          # fmt, layering lint, clippy -D warnings, tests for those crates
```

Run `scripts/check` (no arguments) before you report work as done or push a branch. It is
the only CI: fmt, lint, clippy, duplicate versions, all tests, and the Python end-to-end
tests once they exist.

`scripts/release` builds the fully optimized binary and runs the bench. Run it rarely.

## In an orb

- Orbs have no swap. Keep `CARGO_BUILD_JOBS` at 6 or lower (`scripts/check` sets it).
- `.agents/setup` pre-builds every dependency into the project snapshot, so a new orb only
  compiles NewDB's own crates.
- Python lives in `.venv` (created by setup): `.venv/bin/maturin develop -m crates/newdb-py/Cargo.toml`.
- Put benchmark datasets under `/tmp` or `target/`, never in the repository.

## Git

- Lane work happens on branch `lane-<n>`. Commit small, push often; each commit message says
  what works and which tests pass. Layout or engine performance changes include `bench`
  numbers in the message.
- The integrator merges lane branches into `main`.
