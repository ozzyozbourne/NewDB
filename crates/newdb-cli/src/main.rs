//! `newdb` (DESIGN.md sections 8.3 and 9). Argument parsing is written by hand; no clap.
//!
//! ```text
//! newdb serve   --data <dir> [--addr 127.0.0.1:4318] [--no-fsync] [--memory]
//! newdb inspect --data <dir>
//! newdb verify  --data <dir>
//! newdb rebuild --data <dir> [--layout <name>]
//! newdb bench   [--data <dir> | --memory] [--no-fsync] [--traces N] [--batch N] [--out report.json]
//! newdb bench   --compare a.json b.json
//! ```
//! `--memory` runs against `newdb_testkit::Oracle`, so every command except the engine-only
//! ones (inspect, verify, rebuild) works before the engine lands.

fn main() {
    todo!("lane 4")
}
