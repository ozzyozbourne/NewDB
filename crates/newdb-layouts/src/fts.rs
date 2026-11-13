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

use crate::fx::{FxHashMap, hash_with_seed};
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

/// A batch whose posts carry at least this many bytes of text per worker thread is
/// tokenized and indexed on several threads; smaller ones are not worth starting threads.
const BYTES_PER_WORKER: usize = 64 * 1024;

/// The in-memory index of the active partition.
///
/// Tokens are split by hash into shards ([`Builder::with_shards`]); each token and its
/// postings live in one shard, so shards index a batch independently, on different
/// threads. The sealed file and every answer are the same for every shard count and every
/// split of posts into batches.
///
/// [`LayoutBuilder::apply`] does everything, on scoped threads for large batches. An
/// engine that runs the steps on its own threads uses:
///
/// 1. [`prepare`] on each post, anywhere (it is pure), with this builder's shard count;
/// 2. [`Builder::begin`] for the batch, on the builder's thread (cheap, serial);
/// 3. [`Shard::apply`] of every shard to the batch's prepared posts, on any threads, with
///    the shards moved out by [`Builder::take_shards`] and back by [`Builder::put_shards`]
///    before the next `begin`, query or seal.
pub struct Builder {
    /// Spans by id, in order of first arrival.
    keys: Vec<Key>,
    /// Merged start of each span (0 until a post sets one).
    starts: Vec<u64>,
    ids: FxHashMap<Key, u32>,
    /// Every span, newest first.
    order: BTreeSet<Reverse<(u64, TraceId, SpanId, u32)>>,
    shards: Vec<Shard>,
    /// How many shards this builder has, also while they are taken out.
    shard_count: usize,
}

impl Default for Builder {
    fn default() -> Builder {
        Builder::with_shards(1)
    }
}

/// One shard of the token dictionary. Moved out of its builder with
/// [`Builder::take_shards`] so it can apply a batch on another thread.
pub struct Shard {
    index: usize,
    count: usize,
    /// Token → token id (within this shard).
    tokens: FxHashMap<Box<[u8]>, u32>,
    /// By token id.
    terms: Vec<Term>,
    /// Token ids with pending span ids in the batch being applied.
    touched: Vec<u32>,
    /// Posts this shard has applied, so each post adds to a token's postings once.
    posts: u32,
}

/// The span id of each post of a batch, from [`Builder::begin`].
pub struct SpanIds {
    ids: Vec<u32>,
    shards: usize,
}

/// The distinct tokens of one post's `inputs` and `outputs`, grouped by the shard that
/// indexes them: the output of [`prepare`].
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Prepared {
    /// Every token of the post, lowercased, back to back (repeats included).
    bytes: Vec<u8>,
    /// `(start, end)` in `bytes` of each distinct token, shard 0's first.
    tokens: Vec<(u32, u32)>,
    /// Shard `k`'s tokens are `tokens[first[k]..first[k + 1]]`.
    first: Vec<u32>,
}

impl Prepared {
    /// The shard count this was prepared for.
    pub fn shards(&self) -> usize {
        self.first.len().saturating_sub(1)
    }

    /// Number of distinct tokens.
    pub fn len(&self) -> usize {
        self.tokens.len()
    }

    pub fn is_empty(&self) -> bool {
        self.tokens.is_empty()
    }

    /// Token bytes this carries, to size work.
    pub fn bytes(&self) -> usize {
        self.bytes.len()
    }

    fn shard_tokens(&self, k: usize) -> impl Iterator<Item = &[u8]> {
        let range = self.first[k] as usize..self.first[k + 1] as usize;
        self.tokens[range]
            .iter()
            .map(|&(a, b)| &self.bytes[a as usize..b as usize])
    }
}

/// Reused per thread by [`prepare`], so a post costs no table allocation.
#[derive(Default)]
struct Scratch {
    /// `(hash, start, end)` of every token of the post.
    toks: Vec<(u64, u32, u32)>,
    /// Open-addressing set over `toks` indices: `(generation, index)`.
    table: Vec<(u32, u32)>,
    generation: u32,
    distinct: Vec<u32>,
}

thread_local! {
    static SCRATCH: std::cell::RefCell<Scratch> = std::cell::RefCell::default();
}

/// Tokenizes one post for a builder with `shards` shards: each distinct token of its
/// `inputs` and `outputs` once, routed to its shard. Pure, so the engine can run it on
/// the ingesting threads; the result is the same on every machine.
pub fn prepare(span: &newdb_types::Span, shards: usize) -> Prepared {
    let shards = shards.max(1);
    let len = |x: &Option<Vec<u8>>| x.as_ref().map_or(0, Vec::len);
    let mut bytes: Vec<u8> = Vec::with_capacity(len(&span.inputs) + len(&span.outputs));
    SCRATCH.with(|cell| {
        let Scratch {
            toks,
            table,
            generation,
            distinct,
        } = &mut *cell.borrow_mut();
        toks.clear();
        distinct.clear();
        for text in [&span.inputs, &span.outputs].into_iter().flatten() {
            for_each_token(text, |t| {
                let start = bytes.len() as u32;
                bytes.extend_from_slice(t);
                toks.push((hash_with_seed(t, ROUTE_SEED), start, bytes.len() as u32));
            });
        }
        // Drop repeats with an open-addressing set; a new generation empties it.
        let cap = (toks.len() * 2).next_power_of_two().max(16);
        if table.len() < cap {
            table.resize(cap, (0, 0));
        }
        *generation = generation.wrapping_add(1);
        if *generation == 0 {
            table.fill((0, 0));
            *generation = 1;
        }
        let tok = |&(_, a, b): &(u64, u32, u32)| &bytes[a as usize..b as usize];
        let mut first = vec![0u32; shards + 1];
        for (i, t) in toks.iter().enumerate() {
            let mut slot = (t.0 as usize) & (cap - 1);
            loop {
                let (g, j) = table[slot];
                if g != *generation {
                    table[slot] = (*generation, i as u32);
                    distinct.push(i as u32);
                    first[route(t.0, shards) + 1] += 1;
                    break;
                }
                let u = &toks[j as usize];
                if u.0 == t.0 && tok(u) == tok(t) {
                    break;
                }
                slot = (slot + 1) & (cap - 1);
            }
        }
        // Counting sort by shard.
        for k in 0..shards {
            first[k + 1] += first[k];
        }
        let mut next = first.clone();
        let mut tokens = vec![(0u32, 0u32); distinct.len()];
        for &i in distinct.iter() {
            let (h, a, b) = toks[i as usize];
            let k = route(h, shards);
            tokens[next[k] as usize] = (a, b);
            next[k] += 1;
        }
        Prepared {
            bytes,
            tokens,
            first,
        }
    })
}

/// Seed of the shard-routing hash, so it is independent of the hash the shard maps use.
const ROUTE_SEED: u64 = 0x9e37_79b9_7f4a_7c15;

fn route(hash: u64, shards: usize) -> usize {
    ((hash as u128 * shards as u128) >> 64) as usize
}

fn shard_of(token: &[u8], shards: usize) -> usize {
    if shards == 1 {
        return 0;
    }
    route(hash_with_seed(token, ROUTE_SEED), shards)
}

impl Shard {
    fn new(index: usize, count: usize) -> Shard {
        Shard {
            index,
            count,
            tokens: FxHashMap::default(),
            terms: Vec::new(),
            touched: Vec::new(),
            posts: 0,
        }
    }

    pub fn index(&self) -> usize {
        self.index
    }

    /// Indexes this shard's tokens of a batch: `prepared[i]` is the batch's post `i`,
    /// prepared for this builder's shard count, and `ids` came from [`Builder::begin`]
    /// for the same batch.
    ///
    /// # Panics
    /// If `prepared` was made for another shard count or another batch.
    pub fn apply(&mut self, prepared: &[Prepared], ids: &SpanIds) {
        assert_eq!(ids.shards, self.count, "fts: SpanIds from another builder");
        assert_eq!(
            prepared.len(),
            ids.ids.len(),
            "fts: prepared posts and batch differ"
        );
        for (p, &id) in prepared.iter().zip(&ids.ids) {
            assert_eq!(
                p.shards(),
                self.count,
                "fts: prepared for another shard count"
            );
            self.posts += 1;
            for t in p.shard_tokens(self.index) {
                self.add(t, id);
            }
        }
        self.flush();
    }

    /// Adds span `id` to `token`'s postings, once per post.
    #[inline]
    fn add(&mut self, token: &[u8], id: u32) {
        let tid = match self.tokens.get(token) {
            Some(&tid) => tid,
            None => {
                let tid = self.terms.len() as u32;
                self.tokens.insert(token.into(), tid);
                self.terms.push(Term::default());
                tid
            }
        };
        let term = &mut self.terms[tid as usize];
        if term.stamp != self.posts {
            term.stamp = self.posts;
            if term.pending.is_empty() {
                self.touched.push(tid);
            }
            term.pending.push(id);
        }
    }

    /// Moves the batch's pending span ids into the postings.
    fn flush(&mut self) {
        for tid in self.touched.drain(..) {
            let Term {
                postings, pending, ..
            } = &mut self.terms[tid as usize];
            // Posts of the same span in one batch add its id more than once.
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
}

#[derive(Default)]
struct Term {
    /// Ids of spans mentioning the token.
    postings: RoaringBitmap,
    /// Span ids of the current batch, added to `postings` at the end of `apply`: one pass
    /// per bitmap while it is in cache, instead of a cache miss per insert.
    pending: Vec<u32>,
    /// The shard's post count when this term was last added to.
    stamp: u32,
}

impl Builder {
    /// A builder whose token dictionary is split into `shards` shards (at least 1). The
    /// sealed bytes and every answer are the same for every shard count.
    pub fn with_shards(shards: usize) -> Builder {
        let n = shards.max(1);
        Builder {
            keys: Vec::new(),
            starts: Vec::new(),
            ids: FxHashMap::default(),
            order: BTreeSet::new(),
            shards: (0..n).map(|k| Shard::new(k, n)).collect(),
            shard_count: n,
        }
    }

    pub fn shards(&self) -> usize {
        self.shard_count
    }

    /// Records the batch's spans and their start times, and returns each post's span id
    /// for [`Shard::apply`]. Call once per batch, in log order, before its shards apply it.
    pub fn begin(&mut self, batch: &Batch<'_>) -> SpanIds {
        let ids = batch
            .spans
            .iter()
            .map(|s| self.span((s.trace_id, s.span_id), s.start_ns))
            .collect();
        SpanIds {
            ids,
            shards: self.shard_count,
        }
    }

    /// Moves the shards out, to apply on other threads. Queries and `seal` need them back.
    pub fn take_shards(&mut self) -> Vec<Shard> {
        std::mem::take(&mut self.shards)
    }

    /// Puts back every shard [`Builder::take_shards`] moved out, in any order.
    ///
    /// # Panics
    /// If a shard is missing or belongs to a builder with another shard count.
    pub fn put_shards(&mut self, mut shards: Vec<Shard>) {
        shards.sort_by_key(|s| s.index);
        let ok = shards.len() == self.shard_count
            && shards
                .iter()
                .enumerate()
                .all(|(k, s)| s.index == k && s.count == self.shard_count);
        assert!(ok, "fts: put_shards needs exactly this builder's shards");
        self.shards = shards;
    }

    /// `begin` and every shard's `apply`, on scoped threads when the batch is large.
    pub fn apply_prepared(&mut self, batch: &Batch<'_>, prepared: &[Prepared]) {
        let ids = self.begin(batch);
        let bytes: usize = prepared.iter().map(Prepared::bytes).sum();
        if self.shard_count == 1 || bytes < BYTES_PER_WORKER {
            for shard in &mut self.shards {
                shard.apply(prepared, &ids);
            }
            return;
        }
        let ids = &ids;
        std::thread::scope(|sc| {
            for shard in &mut self.shards {
                sc.spawn(move || shard.apply(prepared, ids));
            }
        });
    }

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
        let shard = &self.shards[shard_of(token, self.shards.len())];
        shard
            .tokens
            .get(token)
            .map(|&t| &shard.terms[t as usize].postings)
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
        let spans = batch.spans;
        let n = self.shard_count;
        let text = |s: &newdb_types::Span| {
            s.inputs.as_ref().map_or(0, Vec::len) + s.outputs.as_ref().map_or(0, Vec::len)
        };
        if n == 1 {
            // One shard: index tokens as the tokenizer finds them, without `prepare`.
            let ids = self.begin(batch);
            let shard = &mut self.shards[0];
            for (span, &id) in spans.iter().zip(&ids.ids) {
                shard.posts += 1;
                for text in [&span.inputs, &span.outputs].into_iter().flatten() {
                    for_each_token(text, |t| shard.add(t, id));
                }
            }
            shard.flush();
            return;
        }
        let bytes: usize = spans.iter().map(text).sum();
        let workers = (bytes / BYTES_PER_WORKER).clamp(1, n);
        let prepared: Vec<Prepared> = if workers == 1 {
            spans.iter().map(|s| prepare(s, n)).collect()
        } else {
            let chunk = spans.len().div_ceil(workers);
            std::thread::scope(|sc| {
                let parts: Vec<_> = spans
                    .chunks(chunk)
                    .map(|c| sc.spawn(move || c.iter().map(|s| prepare(s, n)).collect::<Vec<_>>()))
                    .collect();
                parts
                    .into_iter()
                    .flat_map(|p| p.join().unwrap_or_else(|e| std::panic::resume_unwind(e)))
                    .collect()
            })
        };
        self.apply_prepared(batch, &prepared);
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

        let mut terms: Vec<(&[u8], &RoaringBitmap)> = self
            .shards
            .iter()
            .flat_map(|sh| {
                sh.tokens
                    .iter()
                    .map(|(t, &i)| (&**t, &sh.terms[i as usize].postings))
            })
            .collect();
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
