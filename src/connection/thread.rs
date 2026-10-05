//! Synchronization state shared with a Connection worker thread.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Condvar, Mutex};

pub(super) struct BaseThread {
    running: AtomicBool,
    wake_sequence: AtomicU64,
    mutex: Mutex<()>,
    condition: Condvar,
}

impl BaseThread {
    pub(super) fn new() -> Self {
        Self {
            running: AtomicBool::new(false),
            wake_sequence: AtomicU64::new(0),
            mutex: Mutex::new(()),
            condition: Condvar::new(),
        }
    }

    pub(super) fn running(&self) -> &AtomicBool {
        &self.running
    }

    pub(super) fn mutex(&self) -> &Mutex<()> {
        &self.mutex
    }

    pub(super) fn wake_sequence(&self) -> u64 {
        self.wake_sequence.load(Ordering::Acquire)
    }

    pub(super) fn notify_one(&self) {
        self.wake_sequence.fetch_add(1, Ordering::AcqRel);
        self.condition.notify_one();
    }

    pub(super) fn notify_all(&self) {
        self.wake_sequence.fetch_add(1, Ordering::AcqRel);
        self.condition.notify_all();
    }

    pub(super) fn condition(&self) -> &Condvar {
        &self.condition
    }
}

impl Drop for BaseThread {
    fn drop(&mut self) {
        self.running.store(false, Ordering::Release);
        self.notify_all();
    }
}
