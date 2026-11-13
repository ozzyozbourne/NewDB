# NewDB HTTP API

`newdb serve` listens on `127.0.0.1:4318` by default. It serves HTTP/1.1 only, with no TLS and no
gRPC. Every endpoint is a thin wrapper over the `Store` trait in `newdb-types`, which defines
what each read returns.

Conventions for every response below:

- IDs are lowercase hex: `trace_id` has 32 digits, `span_id` has 16.
- 64-bit nanosecond times (`start_ns`, `end_ns`, `latency_ns`) are **JSON strings** of
  decimal digits, as in OTLP/JSON, because JavaScript numbers lose precision above 2^53.
  Token counts, costs, versions and stats are plain JSON numbers.
- `cost_micro_usd` is an integer in millionths of a US dollar, computed at ingest from the
  pricing table and rounded down.
- Absent values are `null`.
- Payloads (`inputs`, `outputs`, `attributes`, `events`) are the JSON bytes stored at
  ingest, spliced into the response unparsed.
- Query parameters are form-encoded (`%20` or `+` for a space). An unknown parameter name,
  or a value that does not parse, returns 400. An empty value counts as absent. If a
  parameter repeats, the last value wins.
- Booleans accept `true`/`false` and `1`/`0`.
- `stats=1` adds a `stats` object (see [Stats](#stats)) and asks the store for timings.

API errors (every endpoint except OTLP ingest) return a JSON body:

```json
{"error": "unknown parameter \"stauts\" (expected one of project, status, ...)"}
```

| Status | When |
| --- | --- |
| 400 | Bad parameter, bad trace ID or cursor, missing `q` |
| 404 | Unknown path, or a trace with no spans in that project |
| 405 | Wrong method; the `Allow` header names the right one |
| 500 | The store reported corruption or is read-only |
| 503 | The store reported an I/O error (may be temporary) |

## `POST /v1/traces`: OTLP/HTTP ingest

The body is an OTLP `ExportTraceServiceRequest`.

- `content-type: application/x-protobuf` (or `application/protobuf`): binary protobuf.
- `content-type: application/json`: OTLP/JSON. Parameters such as `; charset=utf-8` are ignored.
- `content-encoding: gzip` (or `x-gzip`) is accepted; `identity` or no header means
  uncompressed.

The body is capped at the server's `max_body_bytes` (64 MiB in `newdb serve`) both as sent
and after gunzip. `newdb-otlp` maps the spans (GenAI conventions, then OpenInference, then
OpenLLMetry) and the store ingests them. The response comes once the store acknowledges:

- protobuf: `200`, `content-type: application/x-protobuf`, an empty body (an empty
  `ExportTraceServiceResponse`);
- JSON: `200`, `content-type: application/json`, body `{}`.

A request with one invalid span (malformed, a wrong-length or all-zero ID) is rejected
whole and nothing is stored. Error responses follow OTLP/HTTP: a `google.rpc.Status` in the
request's encoding, protobuf or `{"code": 3, "message": "..."}`.

| Status | gRPC `code` | When |
| --- | --- | --- |
| 400 | 3 | Malformed protobuf, JSON or gzip, or an invalid span |
| 413 | 8 | Body over `max_body_bytes`, as sent or after gunzip |
| 415 | 3 | Other `content-type` or `content-encoding` (a wrong `content-type` gets an API error body, because the encoding is unknown) |
| 503 | 14 | Store I/O error. OTLP exporters retry on 503 |
| 500 | 13 | Store corrupt or read-only |

Posting the same span again (for example at start and at end) adds another post. Reads
return the merge, where the latest post that set a field wins. Fields that a post leaves
out, such as an empty name, a zero time or an unset status, do not erase earlier values.

## `GET /v1/traces/{trace_id}`

| Parameter | Default | Meaning |
| --- | --- | --- |
| `project` | `default` | Project the trace belongs to |
| `payloads` | `true` | `false` returns `inputs`, `outputs`, `attributes`, `events` as `null` |
| `stats` | `false` | Add `stats` |

Returns 404 when the trace has no spans in that project.

```json
{
  "project": "e2e",
  "trace_id": "a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1",
  "spans": [
    {
      "span_id": "0101010101010101",
      "parent_span_id": null,
      "name": "invoke_agent planner",
      "start_ns": "1000",
      "end_ns": "9000",
      "status": "ok",
      "run_type": "agent",
      "model": null,
      "provider": null,
      "session": null,
      "prompt_tokens": null,
      "completion_tokens": null,
      "cost_micro_usd": null,
      "error": null,
      "version": 6,
      "inputs": null,
      "outputs": null,
      "attributes": {"gen_ai.operation.name": "invoke_agent", "otel.span.kind": "unspecified", "resource": {"newdb.project": "e2e"}},
      "events": null,
      "children": [ { "span_id": "0202020202020202", "...": "...", "children": [] } ]
    }
  ]
}
```

- `spans` is a forest built from `parent_span_id`. The top level holds the roots and any
  span whose parent is not in the trace (orphans; their `parent_span_id` is kept). Siblings
  follow the store's order, which is `start_ns` (missing = 0) and then `span_id`, ascending.
  Every span appears exactly once. A parent cycle is cut at its first span in store order,
  which goes to the top level.
- Span fields are the merged values. `status` is `unset`, `ok` or `error`. `run_type` is
  `llm`, `chat_model`, `tool`, `chain`, `agent`, `retriever`, `embedding` or `other`.
  `status` and `run_type` are `null` when no post set them.
- `inputs` and `outputs`: an object assembled from the GenAI, OpenInference or OpenLLMetry
  attributes, such as `{"messages": [...]}`, `{"arguments": ...}` or `{"input": ...}`.
- `attributes`: the unmapped span attributes, then `otel.span.kind`, `otel.links` (when the
  span has links), `resource` (the resource attributes), `otel.scope.name` and
  `otel.scope.version`.
- `events`: `[{"name": "...", "time_unix_nano": 123, "attributes": {...}}]`.
- `error`: the status message of an error span, or `"<type>: <message>"` from an
  `exception` event.

## `GET /v1/runs`

A run is a root span (merged `parent_span_id` is `null`). Runs come newest first, by
`(start_ns, trace_id, span_id)` descending.

| Parameter | Meaning |
| --- | --- |
| `project` | Only this project (default: all projects) |
| `status` | `unset`, `ok` or `error` |
| `run_type` | One of the run types above (a run with no type counts as `other`) |
| `model`, `session` | Exact match |
| `min_latency_ms` | `end_ns - start_ns` at least this many milliseconds; open runs never match |
| `min_tokens` | `prompt_tokens + completion_tokens` at least this |
| `min_cost_usd` | Decimal USD with at most 6 decimals, such as `0.01` |
| `since` | `start_ns` at least this (ns since the epoch) |
| `cursor` | `next_cursor` from the previous page |
| `limit` | Default 50, at most 1000 |
| `stats` | Add `stats` |

```json
{
  "runs": [
    {
      "project": "e2e",
      "trace_id": "b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2",
      "span_id": "0505050505050505",
      "name": "RetrievalQA",
      "start_ns": "2000",
      "end_ns": "2500",
      "latency_ns": "500",
      "status": "error",
      "run_type": "chain",
      "model": null,
      "session": "sess-b",
      "prompt_tokens": 0,
      "completion_tokens": 0,
      "total_tokens": 0,
      "cost_micro_usd": 0
    }
  ],
  "next_cursor": "00000000000007d0b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b20505050505050505"
}
```

- The fields are the root span's own values, not totals over the trace. `start_ns` is `"0"`
  if the root never sent one. `end_ns` and `latency_ns` are `null` while the run is open.
- `next_cursor` is `null` on the last page. Otherwise pass it back as `cursor` with the same
  filters to get the next page. Treat it as opaque.

## `GET /v1/search`

Full-text search over span `inputs` and `outputs`. A span matches when every token of `q`
appears in its merged inputs or outputs. Tokens are runs of ASCII letters and digits (plus
any non-ASCII bytes), lowercased; see `newdb_types::text`. A `q` with no tokens matches
nothing. Hits come newest first, like runs.

| Parameter | Meaning |
| --- | --- |
| `q` | Required |
| `project` | Only this project (default: all projects) |
| `limit` | Default 50, at most 1000 |
| `stats` | Add `stats` |

```json
{
  "hits": [
    {"project": "e2e", "trace_id": "a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1", "span_id": "0202020202020202", "start_ns": "1100"}
  ]
}
```

To show a hit, load its trace with `GET /v1/traces/{trace_id}?project=...`.

## `GET /healthz`

`200 {"status":"ok"}` while the server is accepting requests.

## Stats

With `stats=1`, trace, runs and search responses end with:

```json
"stats": {
  "partitions_touched": 1,
  "bytes_read": 0,
  "payload_bytes_decompressed": 0,
  "records_visited": 5,
  "records_returned": 4,
  "allocations": 0,
  "phases_ns": [{"phase": "server.store", "ns": 41000}, {"phase": "server.render", "ns": 9000}]
}
```

The counters are the store's `ReadStats`. `phases_ns` lists the store's own phases first,
then two that the server adds: `server.store` (the whole store call) and `server.render`
(writing the response body, up to the stats themselves). `allocations` is non-zero only in
bench builds with a counting allocator.
