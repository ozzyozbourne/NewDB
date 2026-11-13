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

For a quick optimized binary: `cargo build --profile bench -p newdb-cli --locked`, which
writes `target/release/newdb`.

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

Measured on 2026-11-13 at `main` c7a615d, built with `--profile bench` (thin LTO, the
same optimization as release builds). Most rows use `newdb bench --preset forge`: 1.19M
posts, 158k traces plus 20 traces of 2,000 spans, 2 KB median payloads, 12 hourly
partitions, 1.6 GB on disk. The machine is an Amp orb with 8 vCPU (4 cores with 2
hyperthreads each, Xeon at 2.6 GHz) and 16 GB of RAM, unless a row says otherwise. Forge
numbers are from [DESIGN.md](DESIGN.md) section 1. The two workload generators differ, so
compare orders of magnitude, not percentages.

| Operation | Target | NewDB | Forge |
| --- | --- | --- | --- |
| Ingest, fsync on, 4 concurrent callers | ≥ 100k posts/s | 113–116k posts/s | 23–28k |
| Ingest, fsync on, 1 caller | | 72–73k posts/s | |
| Ingest, fsync off, 1 / 4 callers | | 86–88k / 107–118k posts/s | |
| 2,000-span trace with payloads, warm, p50 | ≤ 5 ms | 3.4–3.9 ms (p99 3.9–4.6) | 45–70 ms |
| Same trace, no payloads | | 0.74 ms | 23–27 ms |
| Same trace, cold (page cache dropped), first read | ≤ 50 ms | 62–92 ms (missed, see notes) | not measured |
| Newest 50 runs | ≤ 1 ms | 0.022 ms (errors only: 0.13–0.14 ms) | 62–85 ms |
| Same, cold | | 0.65–0.83 ms | |
| Full-text search, newest 50 hits | ≤ 20 ms | 0.96–1.0 ms (p99 1.0–1.2); cold 67–82 ms | 19–765 ms |
| Same search with 688 partitions (96k traces sealed every 1,000 posts) | | 1.15 ms (p99 1.6) | |
| Open a database, all partitions sealed | ≤ 50 ms + replay | 1.9 ms warm, 16 ms cold | — |
| Open with the active hour unsealed (92k posts to replay) | | 1.7–1.9 s warm, 1.9–2.0 s cold | — |
| Idle memory (`newdb serve`, forge data open) | ≤ 30 MiB | 6.4 MB | 30 MiB |
| One-line layout change: rebuild + its tests | ≤ 10 s | about 1 s + 7.5 s | — |
| Clean release (LTO) build | ≤ 2 min | 45 s, 2.9 MB binary | 15 min |

Notes:

- **Ingest** is durable when `ingest` returns, by design. The ingest rows are sustained
  rates without seals: 218k pre-generated posts in batches of 1,000, timed until a read
  sees every post. A single caller is bounded by preparing a batch and then waiting for
  its `fdatasync`; several callers overlap the two and reach the target. Ingest inside
  the forge bench, which also seals every 100k posts, is 66–69k posts/s with one caller.
- **Trace load with payloads** is bound by zstd decompression (about 12 MB of payloads
  for the 2,000-span trace), split across a read pool. In process, `Db::with_trace`
  skips copying the payloads out.
- **Cold trace load** misses its target since sealed partitions validate their payload
  index on first use instead of at open. Cold open dropped from 160–210 ms (on the
  earlier non-LTO build) to 16 ms, and open plus first read is faster overall (about
  75–105 ms, against about 185–255 ms), but the first read now reads the index from
  disk. With the index pages
  already cached, the same cold load takes 36 ms.
- **Search** keeps spread traces (traces with posts in more than one partition) merged
  across sealed partitions between queries, so it stays near 1 ms as partitions
  accumulate. The first search after opening builds that cache: about 110 ms at 688
  partitions.
- **Seals** happen hourly (here every 100k posts): 0.93–0.96 s p50, and ingest does not
  wait for them. Payloads are then re-framed per trace in the background (64 KiB zstd
  level-3 frames). Large trace loads that overlap re-framing can reach a p99 of about
  20 ms.
- **Opening** replays the active (unsealed) hour's log to rebuild its in-memory layouts,
  at about 50k posts/s on one thread. A restart late in a busy hour pays that cost.
- **Cold** means the page cache was dropped (`echo 3 > /proc/sys/vm/drop_caches`) and the
  read is the first in a new read-only process. The opening time is listed separately.
- Peak memory of the whole forge bench process, workload generation included, is about
  460 MB.

NewDB has no SQL. A new question is a Rust or Python loop over runs or layout readers.
