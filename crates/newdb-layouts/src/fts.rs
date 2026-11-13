//! `fts`: full-text index over span `inputs` and `outputs`, per partition.
//!
//! Tokens come from `newdb_types::text::for_each_token` (the same tokenizer the oracle
//! uses). For each token, the index stores the spans of this partition that have at least
//! one post whose `inputs` or `outputs` contain it.
//!
//! Sealed form: an FST term dictionary mapping each token to a postings list, and postings
//! as roaring bitmaps over span positions in a sorted `(trace_id, span_id)` table.
//!
//! # Why Forge's search slowed down on compacted segments
//!
//! Forge's index (`forge-index/src/fts.rs`) is cheap: one FST lookup and one bitmap per
//! term. The cost is in what its caller (`forge-query/src/search.rs`) does with the
//! answer. Forge's postings are row ids inside a segment, in arbitrary order, so to find
//! the newest hits it reads the key and start columns of *every* candidate row from the
//! segment, sorts them, and then verifies the newest chunks by scanning, with payloads,
//! every segment whose trace-id range overlaps the chunk. After compaction one segment
//! holds the whole hour with a trace range that overlaps every chunk, so each step decodes
//! far more rows than the result needs. (This is from reading the code; we did not rerun
//! Forge.)
//!
//! Here postings are positions in a key table stored in this file, sorted by
//! `(trace_id, span_id)`, so a position turns into its key with one array index and no
//! data read, a bitmap decodes into keys already in sorted order, and the cost of a lookup
//! depends only on the size of that token's postings, not on how the partition was built.
//!
//! The index answers "which spans in this partition mention token T". The engine turns
//! that into search results: union per token across partitions, intersect across tokens,
//! then confirm each candidate against its merged inputs/outputs, because a later post can
//! replace a payload.
//!
//! File `fts.bin` (framing in `file.rs`), five sections:
//! 1. `[TraceId]` and 2. `[SpanId]`: the key table, sorted by `(trace_id, span_id)`;
//! 3. `[u64]`, one more than the number of tokens: token `i`'s postings are bytes
//!    `offsets[i]..offsets[i + 1]` of section 5;
//! 4. the FST, mapping each token to `i`;
//! 5. the postings, serialized roaring bitmaps of key positions, in token order.

use std::collections::HashMap;

use fst::Map;
use newdb_types::text::for_each_token;
use newdb_types::{Error, Result, SpanId, TraceId};
use roaring::RoaringBitmap;

use crate::{Batch, Cost, FileSet, LayoutBuilder, LayoutFiles, file};

pub const NAME: &str = "fts";
pub const VERSION: u32 = 1;

const FILE: &str = "fts.bin";
const MAGIC: &[u8; 8] = b"NDBFTS\0\0";
const KEY_BYTES: u64 = (size_of::<TraceId>() + size_of::<SpanId>()) as u64;

type Key = (TraceId, SpanId);

#[derive(Default)]
pub struct Builder {
    /// Spans by arrival id: the order in which this builder first saw them.
    keys: Vec<Key>,
    ids: HashMap<Key, u32>,
    /// Token → (arrival ids of spans mentioning it, last id inserted).
    terms: HashMap<Box<[u8]>, (RoaringBitmap, u32)>,
}

impl Builder {
    /// Spans of this partition mentioning `token` (already tokenized: lowercase ASCII),
    /// sorted ascending, without duplicates.
    pub fn postings(&self, token: &[u8]) -> (Vec<(TraceId, SpanId)>, Cost) {
        let Some((ids, _)) = self.terms.get(token) else {
            return (Vec::new(), Cost::default());
        };
        let mut out: Vec<Key> = ids.iter().map(|i| self.keys[i as usize]).collect();
        out.sort_unstable();
        let cost = Cost {
            records_visited: out.len() as u64,
            bytes_read: ids.serialized_size() as u64 + out.len() as u64 * KEY_BYTES,
        };
        (out, cost)
    }

    fn add_text(&mut self, id: u32, text: &[u8]) {
        let terms = &mut self.terms;
        for_each_token(text, |t| match terms.get_mut(t) {
            Some((ids, last)) => {
                if *last != id {
                    ids.insert(id);
                    *last = id;
                }
            }
            None => {
                let mut ids = RoaringBitmap::new();
                ids.insert(id);
                terms.insert(t.into(), (ids, id));
            }
        });
    }
}

impl LayoutBuilder for Builder {
    fn apply(&mut self, batch: &Batch<'_>) {
        for span in batch.spans {
            if span.inputs.is_none() && span.outputs.is_none() {
                continue;
            }
            let key = (span.trace_id, span.span_id);
            let next = self.keys.len() as u32;
            let id = *self.ids.entry(key).or_insert(next);
            if id == next {
                self.keys.push(key);
            }
            for text in [&span.inputs, &span.outputs].into_iter().flatten() {
                self.add_text(id, text);
            }
        }
    }

    fn seal(&self, out: &mut LayoutFiles) -> Result<()> {
        let mut order: Vec<u32> = (0..self.keys.len() as u32).collect();
        order.sort_unstable_by_key(|&i| self.keys[i as usize]);
        let mut position = vec![0u32; order.len()];
        for (p, &id) in order.iter().enumerate() {
            position[id as usize] = p as u32;
        }
        let traces: Vec<TraceId> = order.iter().map(|&i| self.keys[i as usize].0).collect();
        let spans: Vec<SpanId> = order.iter().map(|&i| self.keys[i as usize].1).collect();

        let mut terms: Vec<(&[u8], &RoaringBitmap)> =
            self.terms.iter().map(|(t, (ids, _))| (&**t, ids)).collect();
        terms.sort_unstable_by_key(|(t, _)| *t);
        let mut offsets: Vec<u64> = Vec::with_capacity(terms.len() + 1);
        let mut postings: Vec<u8> = Vec::new();
        let mut dict = fst::MapBuilder::memory();
        let mut positions: Vec<u32> = Vec::new();
        for (i, (token, ids)) in terms.iter().enumerate() {
            positions.clear();
            positions.extend(ids.iter().map(|id| position[id as usize]));
            positions.sort_unstable();
            let mut bitmap = RoaringBitmap::from_sorted_iter(positions.iter().copied())
                .map_err(|e| Error::InvalidInput(format!("{FILE}: {e}")))?;
            bitmap.optimize();
            offsets.push(postings.len() as u64);
            bitmap.serialize_into(&mut postings)?;
            dict.insert(token, i as u64)
                .map_err(|e| Error::InvalidInput(format!("{FILE}: {e}")))?;
        }
        offsets.push(postings.len() as u64);
        let dict = dict
            .into_inner()
            .map_err(|e| Error::InvalidInput(format!("{FILE}: {e}")))?;
        out.add(
            FILE,
            file::write(
                MAGIC,
                VERSION,
                &[
                    bytemuck::cast_slice(&traces),
                    bytemuck::cast_slice(&spans),
                    bytemuck::cast_slice(&offsets),
                    &dict,
                    &postings,
                ],
            ),
        );
        Ok(())
    }
}

pub struct Reader<'a> {
    traces: &'a [TraceId],
    spans: &'a [SpanId],
    offsets: &'a [u64],
    dict: Map<&'a [u8]>,
    postings: &'a [u8],
}

impl<'a> Reader<'a> {
    pub fn open(files: FileSet<'a>) -> Result<Reader<'a>> {
        let [traces, spans, offsets, dict, postings] =
            file::read(files.get(FILE)?, MAGIC, VERSION, FILE)?;
        let corrupt = |what: &str| Error::Corrupt(format!("{FILE}: {what}"));
        let r = Reader {
            traces: file::cast(traces, FILE)?,
            spans: file::cast(spans, FILE)?,
            offsets: file::cast(offsets, FILE)?,
            dict: Map::new(dict).map_err(|e| corrupt(&e.to_string()))?,
            postings,
        };
        let ok = r.traces.len() == r.spans.len()
            && r.offsets.len() as u64 == r.dict.len() as u64 + 1
            && r.offsets.first() == Some(&0)
            && r.offsets.last() == Some(&(postings.len() as u64))
            && r.offsets.windows(2).all(|w| w[0] <= w[1]);
        if !ok {
            return Err(corrupt("bad postings offsets"));
        }
        Ok(r)
    }

    /// Same as [`Builder::postings`]. Corrupt postings (which `verify` reports) yield the
    /// keys that decode, never a panic.
    pub fn postings(&self, token: &[u8]) -> (Vec<(TraceId, SpanId)>, Cost) {
        let Some(i) = self.dict.get(token) else {
            return (Vec::new(), Cost::default());
        };
        let i = i as usize;
        let bytes = match (self.offsets.get(i), self.offsets.get(i + 1)) {
            (Some(&a), Some(&b)) => &self.postings[a as usize..b as usize],
            _ => &[],
        };
        let positions = RoaringBitmap::deserialize_from(bytes).unwrap_or_default();
        let out: Vec<Key> = positions
            .iter()
            .filter_map(|p| Some((*self.traces.get(p as usize)?, self.spans[p as usize])))
            .collect();
        let cost = Cost {
            records_visited: out.len() as u64,
            bytes_read: bytes.len() as u64 + out.len() as u64 * KEY_BYTES,
        };
        (out, cost)
    }
}
