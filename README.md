# NewDB

A local, embedded trace database for AI-agent telemetry. It ingests OpenTelemetry spans
and serves trace trees, newest runs and full-text search, with no query language: the
stored data structures are the API, and the layouts are ordinary Rust code that agents
change and measure.

Status: v1 is complete. The engine, the layouts, full-text search, OTLP/HTTP ingest, the
HTTP API, the `newdb` CLI, the Python module and the bench are built and tested. See
[DESIGN.md](DESIGN.md) for the design and [AGENTS.md](AGENTS.md) for how the code is
split and how to work on it.

## Build and test

```sh
.agents/setup         # toolchain, mold, nextest, Python venv, pre-built dependencies
scripts/check         # fmt, layering lint, clippy, every Rust test, Python end-to-end tests
scripts/release       # LTO build (target/dist/newdb) and a reference bench run
```

For a quick binary: `cargo build --profile bench -p newdb-cli --locked`, which writes
`target/release/newdb`.

## Use it

### Server (OTLP in, HTTP API out)

```sh
newdb serve --data ./data                 # 127.0.0.1:4318, fsync on
newdb serve --memory                      # nothing on disk
```

Point any OpenTelemetry SDK at it with `OTEL_EXPORTER_OTLP_ENDPOINT=http://127.0.0.1:4318`
and `OTEL_EXPORTER_OTLP_PROTOCOL=http/protobuf` (JSON works too; gRPC does not). GenAI
attributes become the model, provider, tokens and cost; `--mapping` and `--pricing`
override the defaults.

Reads, documented in [crates/newdb-server/docs/api.md](crates/newdb-server/docs/api.md):

```sh
curl 'localhost:4318/v1/traces/<trace_id>?project=default&payloads=false'
curl 'localhost:4318/v1/runs?status=error&limit=50'
curl 'localhost:4318/v1/search?q=refund+policy'
```

### Rust, in process

```rust
use newdb_engine::{Config, Db};
use newdb_types::{RunQuery, Status, Store, TraceQuery};

let db = Db::open("./data", Config::default())?;
db.ingest(spans)?; // returns once the batch is durable and visible
let errors = db.runs(&RunQuery { status: Some(Status::Error), limit: 50, ..Default::default() })?;
let trace = db.trace(&TraceQuery { project: "default".into(), trace_id, payloads: true, timings: false })?;
```

`db.with_trace(&query, &mut |spans| ...)` passes the same spans as `SpanRef`s that borrow
their strings and payloads from the database instead of copying them; the HTTP server
writes trace JSON this way.

Every read returns its answer with stats (partitions touched, bytes read, records visited,
optional per-phase timings).

### Python

```sh
.venv/bin/maturin develop -m crates/newdb-py/Cargo.toml
```

```python
import newdb
db = newdb.Db.open("./data", read_only=True)   # works next to a running `newdb serve`
db.runs(status="error", limit=50)
db.trace("default", "0af7651916cd43dd8448eb211c80319c", payloads=False)
db.search("refund policy", limit=50)
```

### Tools

```sh
newdb inspect --data ./data          # partitions, layouts, sizes
newdb verify  --data ./data          # rebuilds every layout from the log and compares
newdb rebuild --data ./data [--layout fts]
newdb bench   --preset forge --data /tmp/nb --out report.json
newdb bench   --compare before.json after.json
```

## Performance

Measured on 2026-11-13 at `main` 5bfe432 with `newdb bench --preset forge`: 1.19M posts,
158k traces plus 20 traces of 2,000 spans, 2 KB median payloads, 12 hourly partitions,
1.6 GB on disk. The machine is an Amp orb with 8 vCPU (4 cores with 2 hyperthreads each,
Xeon at 2.6 GHz) and 16 GB of RAM, unless a row says otherwise. Forge numbers are from
[DESIGN.md](DESIGN.md) section 1. The two workload generators differ, so compare orders of
magnitude, not percentages.

| Operation | Target | NewDB | Forge |
| --- | --- | --- | --- |
| Ingest, fsync on, 4 concurrent callers | ≥ 100k posts/s | 99–102k posts/s | 23–28k |
| Ingest, fsync on, 1 caller | | 67–71k posts/s | |
| Ingest, fsync off, 4 callers | | 119–126k posts/s | |
| 2,000-span trace with payloads, warm, p50 | ≤ 5 ms | 2.4 ms on a 6-core Apple M4; 7.1 ms on the orb | 45–70 ms |
| Same trace, no payloads | | 1.2 ms | 23–27 ms |
| Same trace, cold (page cache dropped), first read | ≤ 50 ms | 24–40 ms | not measured |
| Newest 50 runs | ≤ 1 ms | 0.027 ms (errors only: 0.15–0.27 ms) | 62–85 ms |
| Same, cold, all partitions sealed | | 0.6 ms | |
| Full-text search, newest 50 hits | ≤ 20 ms | 5–12 ms; cold 58–87 ms | 19–765 ms |
| Open a database (all partitions sealed) | ≤ 50 ms + replay | 100–170 ms, plus replay at ~42k posts/s | — |
| Idle memory (`newdb serve`) | ≤ 30 MiB | 4.3 MB | 30 MiB |
| One-line layout change: rebuild + its tests | ≤ 10 s | ~1 s + 7 s | — |
| Clean release (LTO) build | ≤ 2 min | 42 s, 2.9 MB binary | 15 min |

Notes:

- **Ingest** is durable when `ingest` returns, by design. A single caller is bounded by
  preparing a batch and then waiting for its `fdatasync`. Several callers overlap the two
  and reach the target. Ingest numbers are sustained rates, timed until a read sees every
  post, on a dedicated 8-vCPU orb.
- **Trace load with payloads** is bound by zstd decompression (about 12 MB of payloads
  for the 2,000-span trace). It is split across a read pool and meets the target on a
  developer laptop. On the 4-core hyperthreaded VM it stops at about 7–8 ms, because
  every thread decompresses about 1.5× slower when all 8 run at once. Meeting the target
  on the laptop counts as met.
- **Seals** happen hourly. Payloads are then re-framed per trace in the background
  (64 KiB zstd frames), which cut disk use by 16%. Ingest does not wait for it. Large
  trace loads that overlap it (about 1–2 s per partition here) can reach a p99 of about
  20 ms.
- **Opening** replays the active (unsealed) hour's log to rebuild its in-memory layouts.
  With about 92k unsealed posts that took 2.2–2.8 s, against 100–170 ms with everything
  sealed. A restart late in a busy hour pays that cost.
- **Cold** means the page cache was dropped (`echo 3 > /proc/sys/vm/drop_caches`) and the
  read is the first in a new read-only process. The opening time is listed separately.
- Peak memory of the whole forge bench process, workload generation included, is about
  425 MB.

NewDB has no SQL. A new question is a Rust or Python loop over runs or layout readers.
