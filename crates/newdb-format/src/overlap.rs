//! `overlap.bin`: the trace IDs of a sealed partition that also have posts in an earlier
//! partition, sorted. The engine unions these into the set of traces spread over several
//! partitions, whose spans its queries merge across partitions.
//!
//! ```text
//! magic "NEWDBOVL" | FORMAT_VERSION u32 | count u32 | trace ids [count][16] | crc32c u32
//! ```

use newdb_types::{Result, TraceId};

use crate::{FILE_HEADER_LEN, MAGIC_OVERLAP, check_file_header, corrupt, file_header};

pub fn encode(traces: &[TraceId]) -> Vec<u8> {
    let mut out = file_header(MAGIC_OVERLAP, traces.len() as u32).to_vec();
    for t in traces {
        out.extend_from_slice(&t.0);
    }
    let crc = crc32c::crc32c(&out);
    out.extend_from_slice(&crc.to_le_bytes());
    out
}

pub fn decode(bytes: &[u8]) -> Result<Vec<TraceId>> {
    let n = check_file_header(bytes, MAGIC_OVERLAP, "overlap")? as usize;
    let body = n
        .checked_mul(16)
        .and_then(|b| b.checked_add(FILE_HEADER_LEN))
        .filter(|&b| b + 4 == bytes.len())
        .ok_or_else(|| corrupt("overlap: bad length"))?;
    if crc32c::crc32c(&bytes[..body]).to_le_bytes() != bytes[body..] {
        return Err(corrupt("overlap: bad CRC"));
    }
    Ok(bytes[FILE_HEADER_LEN..body]
        .as_chunks::<16>()
        .0
        .iter()
        .map(|c| TraceId(*c))
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trips_and_rejects_damage() {
        let ts = [TraceId([1; 16]), TraceId([9; 16])];
        let b = encode(&ts);
        assert_eq!(decode(&b).unwrap(), ts);
        assert_eq!(decode(&encode(&[])).unwrap(), []);
        for i in 0..b.len() {
            let mut bad = b.clone();
            bad[i] ^= 1;
            assert!(decode(&bad).is_err(), "flip {i}");
            assert!(decode(&b[..i]).is_err(), "cut {i}");
        }
    }
}
