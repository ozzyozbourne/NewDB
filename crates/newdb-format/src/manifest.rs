//! `MANIFEST`: the list of sealed partitions. A partition directory that is not listed is
//! the active partition (or one whose sealing did not finish), recovered from its log.
//!
//! ```text
//! magic "NEWDBMAN" | FORMAT_VERSION u32 | 0 u32
//! count u32
//! per partition:
//!   name_len u16 | name | id u64 | hour u64 | posts u64 | spans u64
//!   min_start_ns u64 | max_start_ns u64 | min_version u64 | max_version u64
//!   layout_count u16 | per layout: name_len u16 | name | version u32
//! crc32c u32 of everything before it
//! ```
//!
//! Replaced atomically: write `MANIFEST.tmp`, fsync it, rename over `MANIFEST`, fsync the
//! directory.

use std::fs;
use std::io::Write;
use std::path::Path;

use newdb_types::Result;

use crate::{Cursor, FILE_HEADER_LEN, MAGIC_MANIFEST, check_file_header, corrupt, file_header};

pub const FILE_NAME: &str = "MANIFEST";
const TMP_NAME: &str = "MANIFEST.tmp";

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Manifest {
    /// Sealed partitions in creation order (which is also version order).
    pub partitions: Vec<PartitionEntry>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct PartitionEntry {
    /// Directory name.
    pub name: String,
    /// Creation order, as in the partition's log header.
    pub id: u64,
    /// Arrival hour, in hours since the Unix epoch.
    pub hour: u64,
    pub posts: u64,
    pub spans: u64,
    pub min_start_ns: u64,
    pub max_start_ns: u64,
    pub min_version: u64,
    pub max_version: u64,
    /// `(layout name, version)` of the layout directories this partition has.
    pub layouts: Vec<(String, u32)>,
}

pub fn encode(m: &Manifest) -> Vec<u8> {
    fn name(out: &mut Vec<u8>, s: &str) {
        out.extend_from_slice(&(s.len() as u16).to_le_bytes());
        out.extend_from_slice(s.as_bytes());
    }
    let mut out = file_header(MAGIC_MANIFEST, 0).to_vec();
    out.extend_from_slice(&(m.partitions.len() as u32).to_le_bytes());
    for p in &m.partitions {
        name(&mut out, &p.name);
        for v in [
            p.id,
            p.hour,
            p.posts,
            p.spans,
            p.min_start_ns,
            p.max_start_ns,
            p.min_version,
            p.max_version,
        ] {
            out.extend_from_slice(&v.to_le_bytes());
        }
        out.extend_from_slice(&(p.layouts.len() as u16).to_le_bytes());
        for (l, v) in &p.layouts {
            name(&mut out, l);
            out.extend_from_slice(&v.to_le_bytes());
        }
    }
    let crc = crc32c::crc32c(&out);
    out.extend_from_slice(&crc.to_le_bytes());
    out
}

pub fn decode(bytes: &[u8]) -> Result<Manifest> {
    check_file_header(bytes, MAGIC_MANIFEST, "MANIFEST")?;
    let body_len = bytes
        .len()
        .checked_sub(4)
        .ok_or_else(|| corrupt("MANIFEST: truncated"))?;
    let (body, crc) = bytes.split_at(body_len);
    if crc32c::crc32c(body).to_le_bytes() != crc {
        return Err(corrupt("MANIFEST: bad CRC"));
    }
    let mut c = Cursor::new(&body[FILE_HEADER_LEN..], "MANIFEST");
    let count = c.u32()?;
    let mut m = Manifest::default();
    for _ in 0..count {
        let n = c.u16()? as usize;
        let name = c.str(n)?.to_string();
        let mut v = [0u64; 8];
        for x in &mut v {
            *x = c.u64()?;
        }
        let mut layouts = Vec::new();
        for _ in 0..c.u16()? {
            let n = c.u16()? as usize;
            let l = c.str(n)?.to_string();
            layouts.push((l, c.u32()?));
        }
        let [
            id,
            hour,
            posts,
            spans,
            min_start_ns,
            max_start_ns,
            min_version,
            max_version,
        ] = v;
        m.partitions.push(PartitionEntry {
            name,
            id,
            hour,
            posts,
            spans,
            min_start_ns,
            max_start_ns,
            min_version,
            max_version,
            layouts,
        });
    }
    if c.remaining() != 0 {
        return Err(corrupt("MANIFEST: trailing bytes"));
    }
    Ok(m)
}

/// Reads `dir/MANIFEST`; `None` if it does not exist.
pub fn read(dir: &Path) -> Result<Option<Manifest>> {
    match fs::read(dir.join(FILE_NAME)) {
        Ok(b) => decode(&b).map(Some),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e.into()),
    }
}

/// Replaces `dir/MANIFEST` atomically and durably.
pub fn write(dir: &Path, m: &Manifest) -> Result<()> {
    let tmp = dir.join(TMP_NAME);
    let mut f = fs::File::create(&tmp)?;
    f.write_all(&encode(m))?;
    f.sync_all()?;
    drop(f);
    fs::rename(&tmp, dir.join(FILE_NAME))?;
    fs::File::open(dir)?.sync_all()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> Manifest {
        Manifest {
            partitions: vec![
                PartitionEntry {
                    name: "2026-11-13T01".into(),
                    id: 3,
                    hour: 491_234,
                    posts: 10,
                    spans: 7,
                    min_start_ns: 5,
                    max_start_ns: 9,
                    min_version: 1,
                    max_version: 10,
                    layouts: vec![("trace_clustered".into(), 1), ("fts".into(), 3)],
                },
                PartitionEntry {
                    name: "2026-11-13T02".into(),
                    ..PartitionEntry::default()
                },
            ],
        }
    }

    #[test]
    fn round_trips_and_rejects_damage() {
        let m = sample();
        let b = encode(&m);
        assert_eq!(decode(&b).unwrap(), m);
        for cut in 0..b.len() {
            assert!(decode(&b[..cut]).is_err());
        }
        for i in 0..b.len() {
            let mut bad = b.clone();
            bad[i] ^= 1;
            assert!(decode(&bad).is_err(), "flip {i}");
        }
    }

    #[test]
    fn write_replaces_atomically() {
        let dir = std::env::temp_dir().join(format!("newdb-manifest-{}", std::process::id()));
        fs::create_dir_all(&dir).unwrap();
        assert_eq!(read(&dir).unwrap(), None);
        write(&dir, &Manifest::default()).unwrap();
        write(&dir, &sample()).unwrap();
        assert_eq!(read(&dir).unwrap(), Some(sample()));
        assert!(!dir.join(TMP_NAME).exists());
        fs::remove_dir_all(&dir).unwrap();
    }
}
