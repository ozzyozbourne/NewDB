//! Background re-framing: after a partition is sealed (listed in MANIFEST, answering from
//! `payloads.blob`), one thread copies its payloads into `payloads.sealed` and swaps the
//! partition over, so sealing never waits for it.
//!
//! Each step is crash-safe: the file is written to `payloads.sealed.tmp`, fsynced, and
//! renamed into place (then the directory is fsynced). Only then does the partition
//! switch to it and `payloads.blob` get deleted. [`crate::Db::open`] removes a leftover
//! `.tmp`, deletes a blob that a `payloads.sealed` replaced, and queues every listed
//! partition that has no `payloads.sealed` yet.

use std::collections::VecDeque;
use std::fs;
use std::sync::{Arc, Condvar, Mutex};

use newdb_types::Result;

use crate::Inner;
use crate::files::{PAYLOADS, SEALED_PAYLOADS, SEALED_PAYLOADS_TMP, sync_dir};
use crate::part::{Blob, Body, Part, reopen_sealed, write_sealed_payloads};

#[derive(Default)]
struct State {
    ids: VecDeque<u64>,
    /// Partitions ever queued, and those the thread has finished with (or given up on).
    pushed: u64,
    finished: u64,
    busy: bool,
    stop: bool,
}

/// Partition IDs waiting to be re-framed, in order.
#[derive(Default)]
pub(crate) struct Queue {
    state: Mutex<State>,
    cv: Condvar,
}

impl Queue {
    fn lock(&self) -> std::sync::MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(|e| e.into_inner())
    }

    pub fn push(&self, id: u64) {
        let mut s = self.lock();
        s.ids.push_back(id);
        s.pushed += 1;
        self.cv.notify_all();
    }

    /// Makes the thread stop after the partition it is on (abandoning it).
    pub fn stop(&self) {
        self.lock().stop = true;
        self.cv.notify_all();
    }

    fn stopping(&self) -> bool {
        self.lock().stop
    }

    fn next(&self) -> Option<u64> {
        let mut s = self.lock();
        if s.busy {
            s.busy = false;
            s.finished += 1;
            self.cv.notify_all();
        }
        loop {
            if s.stop {
                return None;
            }
            if let Some(id) = s.ids.pop_front() {
                s.busy = true;
                return Some(id);
            }
            s = self.cv.wait(s).unwrap_or_else(|e| e.into_inner());
        }
    }

    /// Waits until every partition queued so far is re-framed (or the thread stopped).
    pub fn wait_idle(&self) {
        let mut s = self.lock();
        let target = s.pushed;
        while !s.stop && s.finished < target {
            s = self.cv.wait(s).unwrap_or_else(|e| e.into_inner());
        }
    }
}

/// The re-framing thread's loop.
pub(crate) fn run(inner: Arc<Inner>) {
    while let Some(id) = inner.reframe.next() {
        // A failure leaves the partition on payloads.blob, which stays correct; the next
        // open tries again.
        let _ = reframe(&inner, id);
    }
}

fn reframe(inner: &Inner, id: u64) -> Result<()> {
    let Some(p) = inner.parts().into_iter().find(|p| p.id == id) else {
        return Ok(()); // deleted by retention
    };
    let Body::Fixed(f) = &p.body else {
        return Ok(());
    };
    if !p.sealed || matches!(f.blob, Blob::Sealed(..)) {
        return Ok(());
    }
    let tmp = p.dir.join(SEALED_PAYLOADS_TMP);
    if let Err(e) = write_sealed_payloads(&p.dir, &tmp, &|| inner.reframe.stopping()) {
        let _ = fs::remove_file(&tmp);
        return Err(e);
    }
    fs::rename(&tmp, p.dir.join(SEALED_PAYLOADS))?;
    sync_dir(&p.dir)?;
    let fixed = reopen_sealed(&p.dir, f)?;
    inner.replace(Arc::new(Part {
        name: p.name.clone(),
        id: p.id,
        hour: p.hour,
        dir: p.dir.clone(),
        sealed: true,
        body: Body::Fixed(fixed),
    }));
    // Readers that still hold the old partition keep their mapping of the blob.
    fs::remove_file(p.dir.join(PAYLOADS))?;
    sync_dir(&p.dir)
}
