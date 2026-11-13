# NewDB design

Status: draft for review, decisions in section 13 agreed · 2026-11-13

NewDB is a local, embedded trace database for AI-agent telemetry. It does what Forge Trace DB does (ingest OpenTelemetry spans, load trace trees, list recent runs, search payloads), but it has no query language. The stored data structures are the API: programs and agents read them directly, and agents change the layouts when the workload needs it.

This document covers what the engine guarantees, what layouts are and how they change, the API, the crate layout, and the decisions made so far. It contains no code yet.

## 1. Why

The research behind this design (Forge audit and benchmarks, xitdb, SpacetimeDB, prior art, Zig vs Rust) found:

- **Forge's main queries already skip SQL.** DataFusion was 0.1% of trace-load CPU time. The time went to Vortex decoding (29 of 75 ms), allocation and copying (29% of CPU), building the tree with payload JSON parsing plus serializing the response (26 ms together), and merging row versions.
- **Layout decides speed.** In the same orb, a 2,000-span trace loaded in 45–70 ms from Forge, 12–17 ms from xitdb, and 3.4 ms from a plain append log. Newest 50 errors took 62–85 ms in Forge's runs list and 0.3–0.5 ms with a dedicated index.
- **Forge's complexity comes from SmithDB's distributed design:** object storage, LSM compaction, Arrow, Vortex, DataFusion. About 6k of its 11.8k lines and about 210 of its 436 dependency crates exist for that, and its release build takes 15 minutes.
- **Finding Forge's bottleneck took a separate harness and `perf`,** because the engine's layers hid the costs. A store whose layout is plain code makes each read's cost visible in the code.

So NewDB is a small storage engine plus layouts that are ordinary Rust code. The language is Rust (stable 1.100): it is as fast as Zig, and it has the OTLP, search and Python/TypeScript binding libraries this needs.

## 2. Goals and non-goals

**Goals (v1)**

- One embedded Rust library, usable in-process from Rust and Python (TypeScript later), plus a `newdb` binary that serves OTLP ingest and a small HTTP API.
- Agent-shaped traces: thousands of spans per trace, multi-MB payloads, spans posted at start and again at end, out-of-order arrival, spans open for hours.
- Fast enough that a person never waits on it. Targets are in section 10.
- Simple enough that one person or one agent can read the whole engine in an afternoon. The target is under 5k lines for the engine crates.
- Every performance cost is visible and measurable: each read reports what it did, and `bench` and `verify` ship with the engine.
- Fast builds: a one-line change in a layout rebuilds and tests in seconds, not minutes.

**Non-goals (v1)**

- SQL or any other query language. An optional DuckDB view over run summaries may come later, off the hot paths.
- Object storage, multiple nodes, replication, multi-tenancy beyond a project id.
- Several processes writing to one database. One writer process; readers in that process or in others.
- A trace-viewer UI, evals, metrics or logs.
- Windows (v1 relies on Linux and macOS `mmap` semantics).

## 3. Principles

1. **The raw log is the source of truth and never changes.** Everything else is derived from it and can be rebuilt.
2. **Layouts are code, versioned and rebuildable.** A layout is a Rust module that turns log records into files and reads them back. Changing one means bumping its version and rebuilding it from the log. Stored telemetry is never migrated in place.
3. **The engine owns everything that can lose data.** Durability, crash recovery, file lifecycle and retention are written carefully once and change rarely. Layouts cannot lose data, because the log still has it.
4. **Measure, don't guess.** Each read returns what it cost. `bench` replays a saved workload, and `verify` checks every layout against the log.
5. **Stored bytes are read in place.** Sealed files are memory-mapped, and fixed-size records are read without decoding. Payloads are returned as the bytes that were sent, never parsed.

## 4. Ownership: engine vs layouts

| Engine owns (small, stable, careful) | Layouts own (agents change freely) |
| --- | --- |
| Raw log format, group commit, checksums, crash recovery | Which files a partition has besides the log and payloads |
| Hourly partitions, sealing, retention | How spans are grouped and sorted |
| Payload store | Which fields are indexed |
| Snapshots for readers | How a read walks the files |
| Stats, `bench`, `verify`, `inspect` | Experiments, judged by `bench` and `verify` |

Rule for agents: changing an engine format needs a format version bump and a human review. Changing a layout needs a layout version bump, a passing `verify`, and `bench` numbers in the commit message.

## 5. Data model

### Span

A span as the engine stores it after normalization (OTel GenAI conventions first, then OpenInference and OpenLLMetry names; the attribute-name mapping and the model pricing table are data files loaded at startup, not code):

| Field | Type | Notes |
| --- | --- | --- |
| `project` | u32 | Index into the project table |
| `trace_id` | [u8; 16] | |
| `span_id` | [u8; 8] | |
| `parent_span_id` | [u8; 8] | All zero = root span |
| `start_ns`, `end_ns` | u64 | `end_ns = 0` while open |
| `status` | u8 | unset, ok, error |
| `run_type` | u8 | llm, chat_model, tool, chain, agent, retriever, embedding, other |
| `name`, `model`, `provider`, `session` | u32 | Indexes into the partition's string table |
| `prompt_tokens`, `completion_tokens` | u32 | |
| `cost_micro_usd` | u64 | Computed at ingest |
| `version` | u64 | Ingest sequence number; highest wins per field |
| `inputs`, `outputs`, `attributes`, `events`, `error` | PayloadRef | Offset, length and hash in the payload store; zero = absent |

The fixed part (`SpanHeader`) is a `#[repr(C)]` struct of 184 bytes, little-endian, with explicit padding. It is cast directly from mapped bytes (`bytemuck` without its derive feature), so reading a span header costs no decoding.

### Updates to a span

Agents often post a span at start and again at end. Each post is a new log record with a higher `version`. The rule is "latest non-null field wins", as in Forge, now about 50 lines on plain structs instead of 180 on Arrow arrays:

- Within the active partition, the merge happens in memory as records arrive.
- When the end of a span arrives after its start's partition has sealed, the end goes into the current partition. A trace load merges the pieces by `span_id`. `inspect` reports how often this happens; a layout can re-cluster such traces later if it matters.

## 6. Engine

### 6.1 Partitions

Data lives in one directory per hour of **arrival time**, so the log is append-only:

```
data/
  MANIFEST                  format version, partitions (with min/max span start_ns), layout versions
  2026-11-13T01/
    log.seg                 raw records (engine)
    payloads.blob           compressed payloads (engine)
    strings.tbl             string table (engine)
    layouts/
      trace_clustered.v1/   layout files
      runs_by_time.v1/
      fts.v1/
  2026-11-13T02/            active partition: log and payloads on disk, layouts in memory
```

- **Active partition:** one per hour. The log and payload files are on disk and fsynced; layouts are built in memory as records arrive.
- **Sealing:** as soon as the writer switches to the new hour's log, the engine asks each layout of the old partition to write its files, fsyncs them, and records them in `MANIFEST`. There is no grace period: records are assigned by arrival, so waiting would not keep traces together. Sealed partitions never change. A layout rebuild writes a new version directory next to the old one.
- **Spans arriving late** go to the current partition; see the merge rule above.
- **Time bounds:** `MANIFEST` records each partition's oldest and newest span `start_ns`. Imported data and late spans therefore cannot break newest-first reads: a newest-N walk visits partitions in order of their newest start time and stops once a partition's newest start is older than the Nth result.

### 6.2 Log format

- File header: magic, format version, partition id.
- Each record: length (u32), CRC32C (u32), kind (u8: span post, delete, project), sequence (u64), then the body: the fixed span struct plus inline strings and payload references.
- **Group commit:** one writer thread collects records for up to 10 ms or 1 MiB, writes them, calls `fdatasync`, and only then acknowledges the batch. In-process writers can choose to wait or not.
- **Recovery:** on open, scan the active partition's log and cut it at the first record with a bad length or CRC, then rebuild the active partition's in-memory layouts from the log. Sealed partitions need no recovery.

### 6.3 Payload store

- Prompts, completions, tool arguments, attributes and events go into `payloads.blob`: zstd-compressed with a per-partition dictionary, addressed by an xxh3-128 hash.
- Within a partition, identical payloads are stored once. A hash hit is confirmed by comparing bytes, so a collision cannot return the wrong payload.
- Reads return the original JSON bytes. The HTTP API splices them into the response without parsing them. In Forge, tree building with payload parsing plus response serialization was 26 of 75 ms.

### 6.4 Snapshots and concurrency

- One writer thread per database.
- Readers take a snapshot: the list of sealed partitions plus a read-only view of the active partition at the last commit. Holding a snapshot never blocks the writer. Sealed files are memory-mapped, and deleting a partition while a snapshot still maps it is safe on Linux and macOS.
- Readers in other processes open the same directory read-only. They see sealed partitions plus the active log as of open time, replaying it into their own in-memory layouts. Live tailing from other processes is a v2 question.

### 6.5 Retention

Retention deletes whole partition directories older than N days (default 30): first a rename to `trash/`, then deletion. No compaction, no file rewrites, no garbage collection.

## 7. Layouts

### 7.1 The interface

A layout is two halves: a builder that receives records, and a reader that opens the files the builder wrote.

The interface lives in `newdb-layouts`, which depends only on `newdb-types`, so layouts are built and tested without the engine:

```rust
pub struct Batch<'a> { pub first_post: u32, pub headers: &'a [SpanHeader], pub spans: &'a [Span] }

pub trait LayoutBuilder: Send {
    /// Called for every committed batch of the partition, in log order.
    fn apply(&mut self, batch: &Batch<'_>);
    /// Writes this layout's files (name, bytes); the engine writes and fsyncs them.
    fn seal(&self, out: &mut LayoutFiles) -> Result<()>;
}

pub struct LayoutDef {
    pub name: &'static str,
    pub version: u32,
    pub new_builder: fn() -> Box<dyn LayoutBuilder>,
}
```

- Each layout has a builder, which answers queries for the active partition from memory, and a reader, which answers the same queries from the sealed files mapped into memory (`FileSet`). Both are concrete types with methods suited to the layout, not a generic trait.
- Layouts are registered in the static `LAYOUTS` list in `newdb-layouts`. Plain function pointers and `dyn` keep this out of generic code, which helps build times.
- **Determinism rule:** given the same log, a builder must write byte-identical files. `verify` depends on it.
- **Same-answer rule:** a builder's queries and the reader opened from its sealed files return identical results.

### 7.2 Changing a layout

1. Edit the layout and bump its `version`.
2. `scripts/check` runs `verify` on the fixture data.
3. `newdb bench --compare` replays the saved workload against the old and new versions.
4. `newdb rebuild <layout>` rebuilds each sealed partition from its log into `<name>.v<N+1>/`, in the background. Readers keep using the old version until a partition's rebuild finishes.
5. When every partition is rebuilt, `MANIFEST` switches over and the old directories are deleted.

Adding a layout works the same way, starting from nothing. Removing one deletes its directories.

### 7.3 Initial layouts

| Layout | What it stores | Serves |
| --- | --- | --- |
| `trace_clustered` | Span structs sorted by (trace_id, start_ns), plus a sorted trace_id → range index | Trace tree: one binary search plus one sequential read per partition |
| `runs_by_time` | One fixed 80-byte summary per root span (start, end, trace_id, status, run_type, tokens, cost, model, session), newest first, plus the positions of error runs | Newest-N runs, filtered by status, type, latency, tokens, cost or session: a linear scan that stops early; error runs without scanning. 1M runs is 80 MB |
| `trace_filter` | Per-partition binary fuse filter over trace_ids, kept in memory (about 9 bits per trace, about 34 MB for 30 days at 1M traces/day) | Finding which partitions hold a trace without opening the others |
| `fts` | Per-partition FST term dictionary plus roaring bitmaps of span positions. The active partition keeps its postings in memory; scanning its payloads would be too slow (about 144 GB an hour at 20k spans/s) | Full-text search over inputs and outputs, newest hits first (no relevance ranking) |

All layouts are per partition; there are no global indexes, so retention stays "delete the directory". The tokenizer and FST + roaring code are ported from Forge after finding why Forge's search slowed down on compacted segments (765 ms vs 19 ms).

A dedicated `sessions` layout (session → traces) waits until `inspect` shows session reads are slow; until then the session filter scans `runs_by_time`.

These are starting points chosen from the benchmarks. The design expects agents to replace them as `inspect` shows what the real data and reads look like.

## 8. Access API

### 8.1 Rust (in-process, zero copy)

```rust
let db = Db::open("./data", Config::default())?;
db.ingest(span)?;                        // group-committed; returns once durable

let snap = db.snapshot();
for span in snap.trace(trace_id)? {      // SpanRef borrows mapped bytes
    println!("{} {:?}", span.name(), span.inputs_raw());
}

let errors = snap
    .runs_newest()                       // walks runs_by_time across partitions
    .filter(|r| r.status() == Status::Error && r.cost_micro_usd() > 10_000)
    .take(50);

// Direct access to any layout's files
for p in snap.partitions() {
    let tc = layouts::TraceClustered::open(&p)?;
    // read tc's structs directly
}
```

The convenience methods (`trace`, `runs_newest`, `search`) are short functions over the layout readers, so anyone can read exactly what they do.

### 8.2 Python and TypeScript

The same calls through PyO3 in v1. A napi-rs binding for TypeScript comes after Python; TypeScript agents export OTLP over HTTP until then. Values are copied across the binding. Iterators stay lazy, so a scan that stops after 50 rows copies 50 rows. Users can write one-off scans in Python without compiling any Rust.

### 8.3 Network

- OTLP over HTTP (protobuf and JSON) on port 4318 only. SDKs select it with `OTEL_EXPORTER_OTLP_PROTOCOL=http/protobuf`. No gRPC in v1: that drops tonic and the HTTP/2 stack. Users who need gRPC can run an OpenTelemetry Collector in front, or a leaf crate can add it later.
- A fixed HTTP API for viewers: trace, runs list, search. Each endpoint is a thin wrapper over the Rust API.

### 8.4 Why queries are code, not a query language

A question is a program: iterators, loops and conditionals over the layout readers.

**What this gains:**
- **Questions query languages express badly become easy,** especially questions about tree shape: "traces where a tool call failed, the model retried more than three times, and the run still succeeded" is a short loop over a trace's spans.
- **The cost is visible.** The access path is whatever the code walks, with no planner choosing a different one, and `ReadStats` confirms it.
- **Ordinary tools apply:** type checking, a debugger, unit tests, profilers. Agents write and change code more reliably than they tune planner hints.

**What it costs, and the answer to each:**
- **No optimizer.** A loop over every partition is slow where a planner would have used an index. Answer: the convenience methods pick good paths, `ReadStats` shows records visited against records returned, and `inspect` flags slow reads.
- **Common aggregations are longer to write** than `GROUP BY`. Answer: a few iterator helpers (`group_by`, `sum`, `percentile`) in Rust and Python, and later an optional DuckDB view over `runs_by_time`.
- **A Rust question needs a compile.** Answer: fast builds (section 11), and Python scans for one-off questions.
- **Code cannot be accepted from the network.** Remote clients, such as a viewer UI, get the fixed HTTP endpoints only. Running arbitrary programs is for people and agents with access to the data directory.
- **Code cannot drive live subscriptions.** SpacetimeDB keeps SQL for this: a declarative query can be updated incrementally, a loop cannot. Live tailing in v2 would need a small declarative filter (fields and comparisons), not a full query language.

## 9. Measurement

### 9.1 Stats for every read

Every snapshot method can return a `ReadStats` alongside its result:

- **Always collected** (a few integer adds per read): partitions touched, bytes read from mapped files, payload bytes decompressed, records visited and returned.
- **On request** (`?stats=1` on HTTP, or a flag in the API): wall time per phase.
- **Bench builds only:** allocations, counted by a counting allocator.

### 9.2 `newdb inspect`

Describes the data and the workload so an agent can choose layouts from facts:

- Data: spans per trace (distribution), payload sizes, field cardinalities, the share of spans updated across partitions.
- Reads: the engine keeps a rolling log of reads (operation, parameter shape, ReadStats). `inspect` summarizes which reads are frequent and which are slow.

### 9.3 `newdb bench`

- Replays a saved workload: an OTLP capture under `bench/workloads/`, plus a list of reads.
- Reports ingest throughput and p50/p99 for each read as JSON.
- `--compare` runs two layout versions or two builds and prints the difference.
- Bench numbers in commits come from this command, always on a release build.

### 9.4 `newdb verify`

For each sealed partition, rebuild every layout from the log into a temporary directory and compare checksums with the stored files. Also check engine invariants: log CRCs, payload references in range, `MANIFEST` matches the disk. `scripts/check` runs it on fixtures. Run it on real data after any layout change.

### 9.5 Deterministic tests

- The clock and the file system sit behind a small trait in the engine.
- Crash recovery, torn writes, out-of-order spans, start/end updates across a seal, and retention while snapshots are open run as fast in-memory simulations.
- One real `kill -9` test stays.

## 10. Targets

Targets for v1 on this orb (8 vCPU, 15 GiB, local disk). They come from the measurements in section 1 and will be checked with `bench`.

| Operation | Target | Forge measured |
| --- | --- | --- |
| Ingest, group commit | ≥ 100k spans/s | 23–28k spans/s |
| 2,000-span trace with payloads, warm | ≤ 5 ms p50 | 45–70 ms |
| Same trace, cold (page cache dropped) | ≤ 50 ms p50 | not measured |
| Newest 50 runs with two filters | ≤ 1 ms p50 | 62–85 ms |
| Full-text search, newest 50 hits | ≤ 20 ms p50 | 19 ms uncompacted, 765 ms compacted |
| Open a database | ≤ 50 ms plus replaying the active hour's log | — |
| Idle memory | ≤ 30 MiB | 30 MiB |
| One-line change in a layout, rebuild + its tests | ≤ 10 s | — |
| Clean release build | ≤ 2 min | 15 min |

## 11. Code layout and builds

```
crates/
  newdb-types/     fixed structs, IDs, enums            no dependencies
  newdb-format/    log, payload and string-table encoding   crc32c, zstd, xxhash-rust
  newdb-engine/    partitions, group commit, snapshots, retention, stats
  newdb-layouts/   the layouts and their registry       fst, roaring (fts), xorf (trace_filter)
  newdb-otlp/      OTLP decoding, GenAI mapping (checked-in prost code)   prost
  newdb-server/    HTTP ingest and API                  hyper or axum, tokio
  newdb-py/        Python binding                       pyo3
  newdb-cli/       newdb serve / inspect / bench / verify / rebuild
```

- Daily work touches `newdb-layouts` and `newdb-engine`, which never depend on tokio, prost or PyO3.
- No proc-macros or serde in `newdb-types`, `newdb-format`, `newdb-engine` or `newdb-layouts`. Encoding is written by hand, and a source lint in `scripts/check` enforces it.
- Builds happen only in Amp orbs; there is no CI. `.agents/setup` installs the pinned toolchain, `mold` and `cargo-nextest`, then pre-builds every dependency so the project snapshot starts warm. `scripts/check` is the full suite: fmt, the layering lint, clippy, `cargo tree -d`, nextest, `verify` on fixtures, and the Python tests. `scripts/release` does the LTO build and the bench run.
- Profiles: dependencies at `opt-level = 2` in dev, line-tables-only debug info, no LTO outside `scripts/release`, `panic = "abort"` only for `newdb-cli`.

## 12. What this costs

- **The layout is a public API.** Programs that read layout files directly break when a layout changes. Layout versions, and the convenience methods as the stable surface, are the answer.
- **Every new question is code.** "Cost by model this week" is a loop, not a query. Fast builds and Python scans keep this cheap, and an optional DuckDB view over `runs_by_time` can come later.
- **Every new filter is an index someone maintains.** This is Jaeger-on-Badger's lesson. The `runs_by_time` scan covers most filters without a new index.
- **Python and TypeScript users pay a copy per value.** Only in-process Rust gets zero copy.
- **No live tailing across processes in v1.**

## 13. Decisions

Agreed on 2026-11-13:

1. **No gRPC.** OTLP over HTTP only; in-process ingest through the bindings.
2. **Partitions by arrival time,** with per-partition span time bounds in `MANIFEST`.
3. **Our own search index** (FST + roaring, ported from Forge), not tantivy.
4. **No global indexes.** Everything is per partition; trace lookup uses in-memory per-partition filters.
5. **No grace period** before sealing.
6. **Read stats:** counters always on, timings on request, allocation counts in bench builds.
7. **TypeScript after Python.**

## References

- Research findings: https://ampcode.com/artifacts/forge-trace-db-vs-an-xitdb-style-store-research-findings-47f4279f-be9e-42a0-b1ae-947ae52585fb
- Fast-build plan: https://ampcode.com/artifacts/fast-compiles-for-the-data-structure-first-rust-db-bd7c0e09-3a4e-4d7d-a961-5f48bbf7a706
- Forge Trace DB: https://github.com/ozzyozbourne/Forge-Trace-DB
- xitdb: https://github.com/xit-vcs/xitdb
- SpacetimeDB: https://github.com/clockworklabs/SpacetimeDB
