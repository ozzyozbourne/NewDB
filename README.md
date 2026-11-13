# NewDB

A local, embedded trace database for AI-agent telemetry. It ingests OpenTelemetry spans
and serves trace trees, newest runs and full-text search, with no query language: the
stored data structures are the API, and the layouts are ordinary Rust code that agents
change and measure.

Status: under construction. The contracts compile; the implementations are being written
in parallel. See [DESIGN.md](DESIGN.md) for the design and [AGENTS.md](AGENTS.md) for how
the work is split.

```sh
.agents/setup         # toolchain, mold, nextest, Python venv, pre-built dependencies
scripts/check         # the full check suite
```
