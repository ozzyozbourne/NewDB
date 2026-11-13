//! A fast non-cryptographic hasher for in-memory builder maps: FxHash's multiply-rotate
//! (as in rustc) with a splitmix64 finalizer. Builder maps hold partition-local keys from our own log, so hash
//! flooding is not a concern; std's SipHash was the top of the ingest profile.

use std::collections::HashMap;
use std::hash::{BuildHasherDefault, Hasher};

const K: u64 = 0x517c_c1b7_2722_0a95;

#[derive(Default, Clone, Copy)]
pub(crate) struct FxHasher(u64);

impl FxHasher {
    #[inline]
    fn add(&mut self, word: u64) {
        self.0 = (self.0.rotate_left(5) ^ word).wrapping_mul(K);
    }
}

impl Hasher for FxHasher {
    #[inline]
    fn write(&mut self, bytes: &[u8]) {
        let (words, rest) = bytes.as_chunks::<8>();
        for w in words {
            self.add(u64::from_le_bytes(*w));
        }
        if !rest.is_empty() {
            let mut b = [0u8; 8];
            b[..rest.len()].copy_from_slice(rest);
            self.add(u64::from_le_bytes(b));
        }
    }

    #[inline]
    fn write_u32(&mut self, v: u32) {
        self.add(v as u64);
    }

    #[inline]
    fn write_u64(&mut self, v: u64) {
        self.add(v);
    }

    #[inline]
    fn write_usize(&mut self, v: usize) {
        self.add(v as u64);
    }

    /// Multiplication only carries bits upward, so the raw state's low bits depend only
    /// on the low bytes of each word (a token's first letters), and hashbrown picks
    /// buckets from the low bits. The splitmix64 finalizer spreads every bit.
    #[inline]
    fn finish(&self) -> u64 {
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }
}

/// The hash of `bytes` from a hasher started at `seed`: independent of the hashes the
/// maps use for the same bytes when `seed` differs from 0.
pub(crate) fn hash_with_seed(bytes: &[u8], seed: u64) -> u64 {
    let mut h = FxHasher(seed);
    h.write(bytes);
    h.finish()
}

pub(crate) type FxHashMap<K, V> = HashMap<K, V, BuildHasherDefault<FxHasher>>;
