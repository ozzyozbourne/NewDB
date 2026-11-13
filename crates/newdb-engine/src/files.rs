//! File helpers: mapped bytes, durable writes, partition directory names.

use std::fs::{self, File};
use std::io::Write;
use std::ops::Deref;
use std::path::Path;

use newdb_types::{Error, Result};

pub(crate) const HOUR_NS: u64 = 3_600_000_000_000;
pub(crate) const LOG: &str = "log.seg";
pub(crate) const PAYLOADS: &str = "payloads.blob";
pub(crate) const SEALED_PAYLOADS: &str = "payloads.sealed";
pub(crate) const SEALED_PAYLOADS_TMP: &str = "payloads.sealed.tmp";
pub(crate) const STRINGS: &str = "strings.tbl";
pub(crate) const OVERLAP: &str = "overlap.bin";
pub(crate) const LAYOUTS_DIR: &str = "layouts";
pub(crate) const TRASH: &str = "trash";
pub(crate) const LOCK: &str = "LOCK";

/// Bytes of a file: memory-mapped, or owned when the file is empty.
pub(crate) enum Bytes {
    Map(memmap2::Mmap),
    Owned(Vec<u8>),
}

impl Deref for Bytes {
    type Target = [u8];
    fn deref(&self) -> &[u8] {
        match self {
            Bytes::Map(m) => m,
            Bytes::Owned(v) => v,
        }
    }
}

/// Maps `path` read-only. Sealed files never change; for the active partition's files the
/// map covers the bytes present now.
pub(crate) fn map(path: &Path) -> Result<Bytes> {
    let f = File::open(path)?;
    if f.metadata()?.len() == 0 {
        return Ok(Bytes::Owned(Vec::new()));
    }
    // SAFETY: NewDB never modifies a file's existing bytes after writing them (logs and
    // payload files are append-only and truncated only by the single writer during
    // recovery, before anything maps them; sealed files are immutable).
    let m = unsafe { memmap2::Mmap::map(&f)? };
    Ok(Bytes::Map(m))
}

pub(crate) fn sync_dir(path: &Path) -> Result<()> {
    File::open(path)?.sync_all()?;
    Ok(())
}

/// Writes `bytes` to `path` and fsyncs the file (not the directory).
pub(crate) fn write_synced(path: &Path, bytes: &[u8]) -> Result<()> {
    let mut f = File::create(path)?;
    f.write_all(bytes)?;
    f.sync_all()?;
    Ok(())
}

pub(crate) fn remove_file_if_exists(path: &Path) -> Result<()> {
    match fs::remove_file(path) {
        Err(e) if e.kind() != std::io::ErrorKind::NotFound => Err(e.into()),
        _ => Ok(()),
    }
}

pub(crate) fn remove_dir_if_exists(path: &Path) -> Result<()> {
    match fs::remove_dir_all(path) {
        Err(e) if e.kind() != std::io::ErrorKind::NotFound => Err(e.into()),
        _ => Ok(()),
    }
}

/// Total size of the regular files directly in `dir` (0 if it does not exist).
pub(crate) fn dir_bytes(dir: &Path) -> u64 {
    fs::read_dir(dir)
        .map(|it| {
            it.filter_map(|e| e.ok()?.metadata().ok())
                .filter(|m| m.is_file())
                .map(|m| m.len())
                .sum()
        })
        .unwrap_or(0)
}

pub(crate) fn file_len(path: &Path) -> u64 {
    fs::metadata(path).map(|m| m.len()).unwrap_or(0)
}

/// `2026-11-13T01` for the hour starting at `hour * 3600` seconds after the epoch.
pub(crate) fn hour_name(hour: u64) -> String {
    let days = (hour / 24) as i64;
    let (y, m, d) = civil_from_days(days);
    format!("{y:04}-{m:02}-{d:02}T{:02}", hour % 24)
}

/// Howard Hinnant's days-since-epoch to (year, month, day).
fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    let y = yoe + era * 400 + i64::from(m <= 2);
    (y, m, d)
}

/// Layout file names are plain names inside the layout's directory.
pub(crate) fn check_file_name(name: &str) -> Result<()> {
    if name.is_empty() || name.starts_with('.') || name.contains(['/', '\\']) {
        return Err(Error::InvalidInput(format!(
            "bad layout file name {name:?}"
        )));
    }
    Ok(())
}

pub(crate) fn layout_dir_name(name: &str, version: u32) -> String {
    format!("{name}.v{version}")
}

#[cfg(test)]
pub(crate) mod test_dir {
    use std::path::{Path, PathBuf};
    use std::sync::atomic::{AtomicU32, Ordering};

    /// A fresh directory under the system temp dir, removed on drop.
    pub struct TestDir(pub PathBuf);

    impl TestDir {
        pub fn new(tag: &str) -> TestDir {
            static N: AtomicU32 = AtomicU32::new(0);
            let p = std::env::temp_dir().join(format!(
                "newdb-{tag}-{}-{}",
                std::process::id(),
                N.fetch_add(1, Ordering::SeqCst)
            ));
            let _ = std::fs::remove_dir_all(&p);
            std::fs::create_dir_all(&p).unwrap();
            TestDir(p)
        }

        pub fn path(&self) -> &Path {
            &self.0
        }
    }

    impl Drop for TestDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hour_names_are_utc_dates() {
        assert_eq!(hour_name(0), "1970-01-01T00");
        // 2026-11-13T01:00:00Z = 1_794_531_600 s.
        assert_eq!(hour_name(1_794_531_600 / 3600), "2026-11-13T01");
        assert_eq!(hour_name((951_782_400 / 3600) + 23), "2000-02-29T23");
    }
}
