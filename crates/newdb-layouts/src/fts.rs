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
//! Here postings are positions in a key table stored in this file, ordered newest first
//! by `(start_ns, trace_id, span_id)` descending, the order search results are returned
//! in. A position turns into its key and start time with one array index and no data
//! read; a bitmap, or the intersection of several, iterates newest first, so the engine
//! can stop after the first matches; and a lookup's cost depends only on the size of the
//! postings involved, not on how the partition was built.
//!
//! A span's `start_ns` here is its merged start within this partition: the latest post
//! in this partition that set one, else 0. Spans are keyed by `(trace_id, span_id)`, so
//! two projects sharing those IDs share one entry.
//!
//! The index answers "which spans in this partition mention token T". The engine turns
//! that into search results: union per token across partitions, intersect across tokens,
//! then confirm each candidate against its merged inputs/outputs, because a later post can
//! replace a payload.
//!
//! File `fts.bin` (framing in `file.rs`), six sections:
//! 1. `[u64]` start_ns, 2. `[TraceId]`, 3. `[SpanId]`: the key table, one entry per span
//!    of the partition, newest first;
//! 4. `[u64]`, one more than the number of tokens: token `i`'s postings are bytes
//!    `offsets[i]..offsets[i + 1]` of section 6;
//! 5. the FST, mapping each token to `i`;
//! 6. the postings, serialized roaring bitmaps of key-table positions, in token order.

use std::borrow::Cow;
use std::cmp::Reverse;
use std::collections::BTreeSet;

use fst::Map;
use newdb_types::text::for_each_token;
use newdb_types::{Error, Result, SpanId, TraceId};
use roaring::RoaringBitmap;

use crate::fx::FxHashMap;
use crate::{Batch, Cost, FileSet, LayoutBuilder, LayoutFiles, file};

pub const NAME: &str = "fts";
pub const VERSION: u32 = 2;

const FILE: &str = "fts.bin";
const MAGIC: &[u8; 8] = b"NDBFTS\0\0";
const KEY_BYTES: u64 = (8 + size_of::<TraceId>() + size_of::<SpanId>()) as u64;

type Key = (TraceId, SpanId);
/// A search hit within the partition: `(start_ns, trace_id, span_id)`.
pub type Hit = (u64, TraceId, SpanId);

/// Below this share of the partition's spans, a newest-first query sorts its hits;
/// above it, it walks the partition's newest-first order and filters, which finds each
/// hit within about `DENSE` steps.
const DENSE: u64 = 32;

#[derive(Default)]
pub struct Builder {
    /// Spans by id, in order of first arrival.
    keys: Vec<Key>,
    /// Merged start of each span (0 until a post sets one).
    starts: Vec<u64>,
    ids: FxHashMap<Key, u32>,
    /// Every span, newest first.
    order: BTreeSet<Reverse<(u64, TraceId, SpanId, u32)>>,
    /// Token → token id.
    tokens: FxHashMap<Box<[u8]>, u32>,
    /// By token id.
    terms: Vec<Term>,
    /// Token ids with pending span ids in the batch being applied.
    touched: Vec<u32>,
    posts: u32,
}

#[derive(Default)]
struct Term {
    /// Ids of spans mentioning the token.
    postings: RoaringBitmap,
    /// Span ids of the current batch, added to `postings` at the end of `apply`: one pass
    /// per bitmap while it is in cache, instead of a cache miss per insert.
    pending: Vec<u32>,
    /// The last post (counting from 1) that added to `pending`.
    stamp: u32,
}

impl Builder {
    /// Spans of this partition mentioning `token` (already tokenized: lowercase ASCII),
    /// sorted ascending, without duplicates.
    pub fn postings(&self, token: &[u8]) -> (Vec<(TraceId, SpanId)>, Cost) {
        let Some(ids) = self.bitmap(token) else {
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

    /// Spans mentioning `token`, newest first: descending `(start_ns, trace_id, span_id)`,
    /// with each span's merged start in this partition. Lazy: stop when you have enough.
    /// `Cost` covers the setup; count the hits you take as records visited.
    pub fn postings_newest<'s>(
        &'s self,
        token: &[u8],
    ) -> (impl Iterator<Item = Hit> + use<'s>, Cost) {
        self.intersect_newest(&[token])
    }

    /// Spans mentioning every one of `tokens`, newest first, as [`Builder::postings_newest`].
    /// No tokens, or a token with no postings, gives nothing.
    pub fn intersect_newest<'s>(
        &'s self,
        tokens: &[&[u8]],
    ) -> (impl Iterator<Item = Hit> + use<'s>, Cost) {
        let mut cost = Cost::default();
        let mut maps: Vec<&'s RoaringBitmap> = Vec::with_capacity(tokens.len());
        for t in tokens {
            match self.bitmap(t) {
                Some(b) => maps.push(b),
                None => maps.clear(),
            }
            if maps.is_empty() {
                break;
            }
        }
        maps.sort_by_key(|b| b.len());
        let hits: Cow<'s, RoaringBitmap> = match maps.split_first() {
            None => Cow::Owned(RoaringBitmap::new()),
            Some((first, [])) => Cow::Borrowed(*first),
            Some((first, rest)) => {
                let mut acc = (*first).clone();
                for b in rest {
                    acc &= *b;
                }
                Cow::Owned(acc)
            }
        };
        cost.bytes_read = maps.iter().map(|b| b.serialized_size() as u64).sum();
        let k = hits.len();
        let iter: Box<dyn Iterator<Item = Hit> + 's> = if k * DENSE >= self.keys.len() as u64 {
            Box::new(
                self.order
                    .iter()
                    .filter(move |Reverse((_, _, _, id))| hits.contains(*id))
                    .map(|Reverse((s, t, sp, _))| (*s, *t, *sp)),
            )
        } else {
            let mut v: Vec<Hit> = hits
                .iter()
                .map(|id| {
                    let (t, s) = self.keys[id as usize];
                    (self.starts[id as usize], t, s)
                })
                .collect();
            v.sort_unstable_by(|a, b| b.cmp(a));
            cost.records_visited = v.len() as u64;
            Box::new(v.into_iter())
        };
        (iter, cost)
    }

    fn bitmap(&self, token: &[u8]) -> Option<&RoaringBitmap> {
        self.tokens
            .get(token)
            .map(|&t| &self.terms[t as usize].postings)
    }

    /// The span's id, creating it, and its merged start updated with `start`.
    fn span(&mut self, key: Key, start: Option<u64>) -> u32 {
        match self.ids.get(&key) {
            Some(&id) => {
                let old = self.starts[id as usize];
                if let Some(s) = start.filter(|&s| s != old) {
                    self.order.remove(&Reverse((old, key.0, key.1, id)));
                    self.order.insert(Reverse((s, key.0, key.1, id)));
                    self.starts[id as usize] = s;
                }
                id
            }
            None => {
                let id = self.keys.len() as u32;
                let s = start.unwrap_or(0);
                self.keys.push(key);
                self.starts.push(s);
                self.ids.insert(key, id);
                self.order.insert(Reverse((s, key.0, key.1, id)));
                id
            }
        }
    }
}

impl LayoutBuilder for Builder {
    fn apply(&mut self, batch: &Batch<'_>) {
        for span in batch.spans {
            let id = self.span((span.trace_id, span.span_id), span.start_ns);
            self.posts += 1;
            let stamp = self.posts;
            let Builder {
                tokens,
                terms,
                touched,
                ..
            } = self;
            for text in [&span.inputs, &span.outputs].into_iter().flatten() {
                for_each_token(text, |t| {
                    let tid = match tokens.get(t) {
                        Some(&tid) => tid,
                        None => {
                            let tid = terms.len() as u32;
                            tokens.insert(t.into(), tid);
                            terms.push(Term::default());
                            tid
                        }
                    };
                    // Each post adds to a token's postings once.
                    let term = &mut terms[tid as usize];
                    if term.stamp != stamp {
                        term.stamp = stamp;
                        if term.pending.is_empty() {
                            touched.push(tid);
                        }
                        term.pending.push(id);
                    }
                });
            }
        }
        for tid in self.touched.drain(..) {
            let Term {
                postings, pending, ..
            } = &mut self.terms[tid as usize];
            pending.sort_unstable();
            pending.dedup();
            // New spans have ids above every earlier one: append those in bulk. `append`
            // takes only ids above the current max, so anything it refuses is inserted.
            let bulk = postings.max().is_none_or(|max| pending[0] > max);
            if !bulk || postings.append(pending.iter().copied()).is_err() {
                postings.extend(pending.iter().copied());
            }
            pending.clear();
        }
    }

    fn seal(&self, out: &mut LayoutFiles) -> Result<()> {
        let n = self.keys.len();
        let mut position = vec![0u32; n];
        let mut starts: Vec<u64> = Vec::with_capacity(n);
        let mut traces: Vec<TraceId> = Vec::with_capacity(n);
        let mut spans: Vec<SpanId> = Vec::with_capacity(n);
        for (p, Reverse((s, t, sp, id))) in self.order.iter().enumerate() {
            position[*id as usize] = p as u32;
            starts.push(*s);
            traces.push(*t);
            spans.push(*sp);
        }

        let mut terms: Vec<(&[u8], u32)> = self.tokens.iter().map(|(t, &i)| (&**t, i)).collect();
        terms.sort_unstable();
        let mut offsets: Vec<u64> = Vec::with_capacity(terms.len() + 1);
        let mut postings: Vec<u8> = Vec::new();
        let mut dict = fst::MapBuilder::memory();
        let mut positions: Vec<u32> = Vec::new();
        for (i, (token, tid)) in terms.iter().enumerate() {
            positions.clear();
            positions.extend(
                self.terms[*tid as usize]
                    .postings
                    .iter()
                    .map(|id| position[id as usize]),
            );
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
                    bytemuck::cast_slice(&starts),
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
    starts: &'a [u64],
    traces: &'a [TraceId],
    spans: &'a [SpanId],
    offsets: &'a [u64],
    dict: Map<&'a [u8]>,
    postings: &'a [u8],
}

impl<'a> Reader<'a> {
    pub fn open(files: FileSet<'a>) -> Result<Reader<'a>> {
        let [starts, traces, spans, offsets, dict, postings] =
            file::read(files.get(FILE)?, MAGIC, VERSION, FILE)?;
        let corrupt = |what: &str| Error::Corrupt(format!("{FILE}: {what}"));
        let r = Reader {
            starts: file::cast(starts, FILE)?,
            traces: file::cast(traces, FILE)?,
            spans: file::cast(spans, FILE)?,
            offsets: file::cast(offsets, FILE)?,
            dict: Map::new(dict).map_err(|e| corrupt(&e.to_string()))?,
            postings,
        };
        let ok = r.traces.len() == r.spans.len()
            && r.starts.len() == r.spans.len()
            && r.offsets.len() as u64 == r.dict.len() as u64 + 1
            && r.offsets.first() == Some(&0)
            && r.offsets.last() == Some(&(postings.len() as u64))
            && r.offsets.windows(2).all(|w| w[0] <= w[1]);
        if !ok {
            return Err(corrupt("bad key table or postings offsets"));
        }
        Ok(r)
    }

    /// Positions of spans mentioning `token`, and the bytes decoded. Corrupt postings
    /// (which `verify` reports) decode as empty, never a panic.
    fn bitmap(&self, token: &[u8]) -> Option<(RoaringBitmap, u64)> {
        let i = self.dict.get(token)? as usize;
        let bytes = match (self.offsets.get(i), self.offsets.get(i + 1)) {
            (Some(&a), Some(&b)) => &self.postings[a as usize..b as usize],
            _ => &[],
        };
        let positions = RoaringBitmap::deserialize_from(bytes).unwrap_or_default();
        Some((positions, bytes.len() as u64))
    }

    fn hit(&self, p: u32) -> Option<Hit> {
        let p = p as usize;
        Some((*self.starts.get(p)?, self.traces[p], self.spans[p]))
    }

    /// Same as [`Builder::postings`].
    pub fn postings(&self, token: &[u8]) -> (Vec<(TraceId, SpanId)>, Cost) {
        let Some((positions, bytes)) = self.bitmap(token) else {
            return (Vec::new(), Cost::default());
        };
        let mut out: Vec<Key> = positions
            .iter()
            .filter_map(|p| self.hit(p).map(|(_, t, s)| (t, s)))
            .collect();
        out.sort_unstable();
        let cost = Cost {
            records_visited: out.len() as u64,
            bytes_read: bytes + out.len() as u64 * KEY_BYTES,
        };
        (out, cost)
    }

    /// Same as [`Builder::postings_newest`]; borrows only the mapped file.
    pub fn postings_newest(&self, token: &[u8]) -> (impl Iterator<Item = Hit> + use<'a>, Cost) {
        self.intersect_newest(&[token])
    }

    /// Same as [`Builder::intersect_newest`]: the positions iterate newest first.
    pub fn intersect_newest(
        &self,
        tokens: &[&[u8]],
    ) -> (impl Iterator<Item = Hit> + use<'a>, Cost) {
        let mut cost = Cost::default();
        let mut maps: Vec<RoaringBitmap> = Vec::with_capacity(tokens.len());
        for t in tokens {
            let Some((b, bytes)) = self.bitmap(t) else {
                maps.clear();
                break;
            };
            cost.bytes_read += bytes;
            maps.push(b);
        }
        maps.sort_by_key(|b| b.len());
        let mut maps = maps.into_iter();
        let mut hits = maps.next().unwrap_or_default();
        for b in maps {
            hits &= b;
        }
        let (starts, traces, spans) = (self.starts, self.traces, self.spans);
        let hit = move |p: u32| {
            let p = p as usize;
            Some((*starts.get(p)?, traces[p], spans[p]))
        };
        (hits.into_iter().filter_map(hit), cost)
    }
}
