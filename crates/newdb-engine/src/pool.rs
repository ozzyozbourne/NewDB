//! Persistent threads for work that borrows the caller's data, such as decompressing a
//! trace's payloads from mapped files. Spawning threads per query cost more than the work
//! they shared on large traces, and each new thread warmed a fresh malloc arena.
//!
//! [`each`] queues tasks, runs the first on the calling thread, then runs queued tasks
//! (its own or another caller's) until its own are done. With no worker threads (one core)
//! the caller runs every task itself.

use std::collections::VecDeque;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::{Arc, Condvar, Mutex, OnceLock};

use crate::part::cores;

type Job = Box<dyn FnOnce() + Send + 'static>;

struct Queue {
    jobs: Mutex<VecDeque<Job>>,
    ready: Condvar,
}

impl Queue {
    fn pop(&self) -> Option<Job> {
        self.jobs
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .pop_front()
    }
}

/// Counts a caller's tasks still queued or running.
struct Latch {
    left: Mutex<usize>,
    done: Condvar,
}

impl Latch {
    fn count_down(&self) {
        let mut left = self.left.lock().unwrap_or_else(|e| e.into_inner());
        *left -= 1;
        if *left == 0 {
            self.done.notify_all();
        }
    }

    fn is_done(&self) -> bool {
        *self.left.lock().unwrap_or_else(|e| e.into_inner()) == 0
    }

    fn wait(&self) {
        let mut left = self.left.lock().unwrap_or_else(|e| e.into_inner());
        while *left > 0 {
            left = self.done.wait(left).unwrap_or_else(|e| e.into_inner());
        }
    }
}

/// Waits for the latch when dropped, so the borrowed task outlives its queued copies even
/// when the caller's own task panics.
struct WaitOnDrop<'a>(&'a Latch);

impl Drop for WaitOnDrop<'_> {
    fn drop(&mut self) {
        self.0.wait();
    }
}

/// Threads the pool runs besides each caller: one fewer than the cores, at most 7.
pub(crate) fn workers() -> usize {
    cores().clamp(1, 8) - 1
}

fn queue() -> &'static Queue {
    static QUEUE: OnceLock<&'static Queue> = OnceLock::new();
    QUEUE.get_or_init(|| {
        let q: &'static Queue = Box::leak(Box::new(Queue {
            jobs: Mutex::new(VecDeque::new()),
            ready: Condvar::new(),
        }));
        for i in 0..workers() {
            let _ = std::thread::Builder::new()
                .name(format!("newdb-read-{}", i + 1))
                .spawn(move || {
                    loop {
                        let job = {
                            let mut jobs = q.jobs.lock().unwrap_or_else(|e| e.into_inner());
                            loop {
                                match jobs.pop_front() {
                                    Some(j) => break j,
                                    None => {
                                        jobs = q.ready.wait(jobs).unwrap_or_else(|e| e.into_inner())
                                    }
                                }
                            }
                        };
                        job();
                    }
                });
        }
        q
    })
}

/// Runs `f(0)`, …, `f(n - 1)`, spread over the pool and the calling thread, and returns
/// their results in order; `None` for a task that panicked.
pub(crate) fn each<T: Send>(n: usize, f: &(dyn Fn(usize) -> T + Sync)) -> Vec<Option<T>> {
    let slots: Vec<Mutex<Option<T>>> = (0..n).map(|_| Mutex::new(None)).collect();
    let run = |i: usize| {
        let v = f(i);
        *slots[i].lock().unwrap_or_else(|e| e.into_inner()) = Some(v);
    };
    if n > 1 {
        let q = queue();
        let latch = Arc::new(Latch {
            left: Mutex::new(n - 1),
            done: Condvar::new(),
        });
        let wait = WaitOnDrop(&latch);
        let task: &(dyn Fn(usize) + Sync) = &run;
        // SAFETY: every queued job calls `task` and then counts the latch down; `wait`
        // (dropped at the end of this block, also on unwind) returns only once the latch
        // is zero, so no job uses `task` (or what it borrows) after this function returns.
        // The latch itself is shared through an `Arc`.
        let task: &'static (dyn Fn(usize) + Sync) = unsafe { std::mem::transmute(task) };
        {
            let mut jobs = q.jobs.lock().unwrap_or_else(|e| e.into_inner());
            for i in 1..n {
                let latch = latch.clone();
                jobs.push_back(Box::new(move || {
                    let _ = catch_unwind(AssertUnwindSafe(|| task(i)));
                    latch.count_down();
                }));
            }
        }
        q.ready.notify_all();
        run(0);
        while !latch.is_done() {
            match q.pop() {
                Some(job) => job(),
                None => break,
            }
        }
        drop(wait);
    } else if n == 1 {
        run(0);
    }
    slots
        .into_iter()
        .map(|s| s.into_inner().unwrap_or_else(|e| e.into_inner()))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn runs_every_task_once_and_reports_panics() {
        let data: Vec<u64> = (0..1000).collect();
        for n in [0, 1, 2, 9, 40] {
            let out = each(n, &|i| data[i * 10..i * 10 + 10].iter().sum::<u64>());
            let want: Vec<Option<u64>> = (0..n as u64)
                .map(|i| Some((i * 10..i * 10 + 10).sum()))
                .collect();
            assert_eq!(out, want);
        }
        let out = each(5, &|i| if i == 3 { panic!("task 3") } else { i });
        assert_eq!(out, vec![Some(0), Some(1), Some(2), None, Some(4)]);
        // Concurrent callers share the workers.
        std::thread::scope(|s| {
            for k in 0..4u64 {
                s.spawn(move || {
                    let out = each(16, &|i| i as u64 * k);
                    assert_eq!(out.iter().flatten().sum::<u64>(), k * 120);
                });
            }
        });
    }
}
