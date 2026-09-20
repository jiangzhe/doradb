use event_listener::{Event, Listener, listener};
use parking_lot::RawMutex;
use parking_lot::lock_api::{
    GuardSend, RawMutex as RawMutexApi, RawRwLock as RawRwLockApi,
    RawRwLockDowngrade as RawRwLockDowngradeApi,
};
use std::mem;
use std::sync::atomic::{AtomicUsize, Ordering};

const WRITER_BIT: usize = 1;
const ONE_READER: usize = 2;

/// A simple RWLock with additional async methods.
pub(super) struct RawRwLock {
    /// Acquired by the writer
    mu: RawMutex,
    /// Event triggered when last reader is dropped.
    no_readers: Event,
    /// Event triggered when writer is dropped.
    no_writer: Event,
    /// Current state of the lock.
    ///
    /// The least significant bit (`WRITER_BIT`) is set to 1 when a writer is holding the lock or
    /// trying to acquire it.
    ///
    /// The upper bits contain the number of currently active readers. Each active reader
    /// increments the state by `ONE_READER`.
    state: AtomicUsize,
}

impl RawRwLock {
    /// Create a new RawRWLock.
    #[inline]
    pub(super) const fn new() -> Self {
        RawRwLock {
            mu: RawMutex::INIT,
            no_readers: Event::new(),
            no_writer: Event::new(),
            state: AtomicUsize::new(0),
        }
    }

    #[inline]
    fn try_lock_shared_with_ord(&self, ord: Ordering) -> bool {
        let mut state = self.state.load(ord);
        loop {
            if state & WRITER_BIT != 0 {
                return false;
            }
            match self.state.compare_exchange(
                state,
                state + ONE_READER,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return true,
                Err(s) => state = s,
            }
        }
    }

    /// Get a read latch in async way.
    #[inline]
    pub(super) async fn lock_shared_async(&self) {
        if self.try_lock_shared() {
            return;
        }
        // slow path: setup listener and wait for no_writer signal.
        loop {
            listener!(self.no_writer => listener);
            if self.try_lock_shared_with_ord(Ordering::SeqCst) {
                self.no_writer.notify(2); // notify other readers.
                return;
            }
            listener.await;
        }
    }

    /// Get a write latch in async way.
    #[inline]
    pub(super) async fn lock_exclusive_async(&self) {
        if self.mu.try_lock() {
            let new_state = self.state.fetch_or(WRITER_BIT, Ordering::SeqCst);
            if new_state & !WRITER_BIT == 0 {
                // no reader means lock is acquired successfully.
                return;
            }
            // slow path: setup listener and wait for no_readers signal.
            let rb = WriteGuardRollback(self);
            loop {
                listener!(self.no_readers => listener);
                let new_state = self.state.fetch_or(WRITER_BIT, Ordering::SeqCst);
                if new_state & !WRITER_BIT == 0 {
                    // cancel rollback action as we acquire the lock successfully.
                    mem::forget(rb);
                    return;
                }
                listener.await;
            }
        }
        // Waiting for writer to quit.
        loop {
            listener!(self.no_writer => no_writer);
            // `no_writer` must only be notified after the releasing writer has
            // both cleared `WRITER_BIT` and unlocked `mu`. Otherwise this
            // retry can observe the old mutex state, fail `try_lock()`, and go
            // back to sleep after consuming the only wakeup.
            if self.mu.try_lock() {
                let new_state = self.state.fetch_or(WRITER_BIT, Ordering::SeqCst);
                if new_state & !WRITER_BIT == 0 {
                    return;
                }
                let rb = WriteGuardRollback(self);
                loop {
                    listener!(self.no_readers => listener);
                    let new_state = self.state.fetch_or(WRITER_BIT, Ordering::SeqCst);
                    if new_state & !WRITER_BIT == 0 {
                        mem::forget(rb);
                        return;
                    }
                    listener.await;
                }
            }
            no_writer.await;
        }
    }
}

// SAFETY: `RawRwLock` maintains the `parking_lot` raw-mutex contract and its
// reader/writer state transitions with atomics plus event ordering.
unsafe impl RawRwLockApi for RawRwLock {
    const INIT: RawRwLock = RawRwLock::new();

    type GuardMarker = GuardSend;

    #[inline]
    fn try_lock_shared(&self) -> bool {
        self.try_lock_shared_with_ord(Ordering::Acquire)
    }

    #[inline]
    fn lock_shared(&self) {
        if self.try_lock_shared() {
            return;
        }
        // slow path: setup listener and wait for no_writer signal.
        loop {
            listener!(self.no_writer => listener);
            if self.try_lock_shared() {
                self.no_writer.notify(2); // notify other readers.
                return;
            }
            listener.wait();
        }
    }

    #[inline]
    fn try_lock_exclusive(&self) -> bool {
        if !self.mu.try_lock() {
            return false;
        }
        // `try_lock_exclusive()` must leave `mu` unlocked on every false
        // return. Callers pair failed try-locks with a wait on `no_writer`,
        // so they rely on the unlock side to notify only after the mutex is
        // actually available again.
        if self
            .state
            .compare_exchange(0, WRITER_BIT, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            return true; // no reader, no writer
        }
        // SAFETY: the failed exclusive try-lock path owns `mu` from the
        // successful `try_lock()` above and must release it before returning.
        unsafe {
            self.mu.unlock();
        }
        false
    }

    #[inline]
    fn lock_exclusive(&self) {
        self.mu.lock();
        let new_state = self.state.fetch_or(WRITER_BIT, Ordering::SeqCst);
        if new_state & !WRITER_BIT == 0 {
            // no reader means lock is acquired successfully.
            return;
        }
        // slow path, setup listener and wait for no_readers signal.
        loop {
            listener!(self.no_readers => listener);
            let new_state = self.state.fetch_or(WRITER_BIT, Ordering::SeqCst);
            if new_state & !WRITER_BIT == 0 {
                // no reader means lock is acquired successfully.
                return;
            }
            listener.wait();
        }
    }

    #[inline]
    unsafe fn unlock_shared(&self) {
        let state = self.state.fetch_sub(ONE_READER, Ordering::SeqCst);
        if state & !WRITER_BIT == ONE_READER {
            // last reader should trigger "no_readers" event.
            self.no_readers.notify(1);
        }
    }

    #[inline]
    unsafe fn unlock_exclusive(&self) {
        self.state.fetch_and(!WRITER_BIT, Ordering::SeqCst);
        // SAFETY: the caller holds the exclusive lock, so this unlock is paired
        // with a prior `lock_exclusive*` acquisition.
        unsafe {
            // Publish the writer-free state before waking waiters, then unlock
            // `mu` before notifying. This guarantees the waiter-side
            // `mu.try_lock()` retry cannot consume a wakeup while the mutex is
            // still locked by the releasing writer.
            self.mu.unlock();
        }
        self.no_writer.notify(1);
    }

    #[inline]
    fn is_locked(&self) -> bool {
        self.state.load(Ordering::Acquire) != 0
    }

    #[inline]
    fn is_locked_exclusive(&self) -> bool {
        self.state.load(Ordering::Acquire) == WRITER_BIT
    }
}

// SAFETY: downgrade preserves the raw-rwlock contract by converting one held
// exclusive lock into one held shared lock under the same synchronization.
unsafe impl RawRwLockDowngradeApi for RawRwLock {
    #[inline]
    unsafe fn downgrade(&self) {
        // SAFETY: callers only invoke downgrade while owning the exclusive lock,
        // so adding one reader and delegating to `unlock_exclusive` is valid.
        unsafe {
            debug_assert!(self.state.load(Ordering::Acquire) & !WRITER_BIT == 0);
            self.state.fetch_add(ONE_READER, Ordering::SeqCst);
            self.unlock_exclusive();
        }
    }
}

struct WriteGuardRollback<'a>(&'a RawRwLock);

impl Drop for WriteGuardRollback<'_> {
    #[inline]
    fn drop(&mut self) {
        // SAFETY: this rollback helper only exists after `mu` was locked and the
        // writer bit was set, so clearing the bit and unlocking `mu` is paired.
        unsafe {
            // rollback writer bit.
            self.0.state.fetch_and(!WRITER_BIT, Ordering::SeqCst);
            // Release `mu` before notifying `no_writer`.
            //
            // Waiters retry `mu.try_lock()` immediately after a wake. If we
            // notify first, one waiter can consume the event, still observe
            // the mutex as locked, and then sleep forever waiting for another
            // writer release that never comes.
            self.0.mu.unlock();
            self.0.no_writer.notify(1);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::future::join3;
    use parking_lot::lock_api::RawRwLock as RawRwLockApi;
    use smol::Timer;
    use smol::future::or;
    use std::cell::UnsafeCell;
    use std::future::Future;
    use std::pin::pin;
    use std::sync::Arc;
    use std::sync::atomic::AtomicBool;
    use std::task::{Context, Wake, Waker};
    use std::thread::spawn;
    use std::time::Duration;

    struct WakeFlag(AtomicBool);

    impl Wake for WakeFlag {
        fn wake(self: Arc<Self>) {
            self.0.store(true, Ordering::SeqCst);
        }
    }

    struct Counter {
        data: UnsafeCell<usize>,
        mu: RawRwLock,
    }
    impl Counter {
        #[inline]
        fn new() -> Self {
            Counter {
                data: UnsafeCell::new(0),
                mu: RawRwLock::new(),
            }
        }

        #[inline]
        fn inc(&self) {
            // SAFETY: this helper holds the exclusive lock while mutating the
            // counter stored in `UnsafeCell`.
            unsafe {
                self.mu.lock_exclusive();
                *self.data.get() += 1;
                self.mu.unlock_exclusive();
            }
        }

        #[inline]
        async fn inc_async(&self) {
            // SAFETY: this helper holds the exclusive lock while mutating the
            // counter stored in `UnsafeCell`.
            unsafe {
                self.mu.lock_exclusive_async().await;
                *self.data.get() += 1;
                self.mu.unlock_exclusive();
            }
        }

        #[inline]
        fn val(&self) -> usize {
            // SAFETY: tests only read the counter after all worker activity is
            // quiesced, so no concurrent mutation remains.
            unsafe { *self.data.get() }
        }
    }
    // SAFETY: shared references are synchronized by `RawRwLock`.
    unsafe impl Sync for Counter {}

    #[test]
    fn test_raw_rwlock_ops() {
        for exclusive in [false, true] {
            let rw = RawRwLock::new();
            rw.lock_exclusive();
            assert!(rw.is_locked());
            assert!(rw.is_locked_exclusive());
            assert!(!rw.try_lock_shared());
            assert!(!rw.try_lock_exclusive());

            let notified = Arc::new(WakeFlag(AtomicBool::new(false)));
            let waker = Waker::from(Arc::clone(&notified));
            let mut cx = Context::from_waker(&waker);
            let mut waiter = pin!(async {
                if exclusive {
                    rw.lock_exclusive_async().await;
                } else {
                    rw.lock_shared_async().await;
                }
            });
            assert!(waiter.as_mut().poll(&mut cx).is_pending());
            assert!(!notified.0.swap(false, Ordering::SeqCst));
            // SAFETY: the initial exclusive acquisition above is still held.
            unsafe { rw.unlock_exclusive() };
            assert!(
                notified.0.swap(false, Ordering::SeqCst),
                "exclusive={exclusive}: waiter was not woken"
            );
            assert!(waiter.as_mut().poll(&mut cx).is_ready());
            assert!(rw.is_locked());
            assert_eq!(rw.is_locked_exclusive(), exclusive);
            // SAFETY: the ready future acquired exactly the matching lock mode.
            unsafe {
                if exclusive {
                    rw.unlock_exclusive();
                } else {
                    rw.unlock_shared();
                }
            }
            assert!(!rw.is_locked());
            assert!(!rw.is_locked_exclusive());
        }
    }

    #[test]
    fn test_raw_rwlock_sync() {
        let counter = Arc::new(Counter::new());
        let mut threads = vec![];
        for _ in 0..10 {
            let counter = Arc::clone(&counter);
            let handle = spawn(move || {
                for _ in 0..10 {
                    counter.inc();
                }
            });
            threads.push(handle);
        }

        for th in threads {
            th.join().unwrap();
        }
        println!("val={:?}", counter.val());
        assert!(counter.val() == 100);
    }

    #[test]
    fn test_raw_rwlock_async() {
        let counter = Arc::new(Counter::new());
        let mut threads = vec![];
        for _ in 0..10 {
            let counter = Arc::clone(&counter);
            let handle = spawn(move || {
                smol::block_on(async {
                    for _ in 0..10 {
                        counter.inc_async().await;
                    }
                });
            });
            threads.push(handle);
        }
        for th in threads {
            th.join().unwrap();
        }
        println!("val={:?}", counter.val());
        assert!(counter.val() == 100);
    }

    #[test]
    fn test_raw_rwlock_async_waiting_writers_progress_after_single_unlock() {
        const ITERS: usize = 128;
        smol::block_on(async {
            for _ in 0..ITERS {
                let rw = Arc::new(RawRwLock::new());
                rw.lock_exclusive();
                let waiter1 = {
                    let rw = Arc::clone(&rw);
                    async move {
                        rw.lock_exclusive_async().await;
                        // SAFETY: this waiter unlocks only after its acquire
                        // completes.
                        unsafe {
                            rw.unlock_exclusive();
                        }
                    }
                };
                let waiter2 = {
                    let rw = Arc::clone(&rw);
                    async move {
                        rw.lock_exclusive_async().await;
                        // SAFETY: this waiter unlocks only after its acquire
                        // completes.
                        unsafe {
                            rw.unlock_exclusive();
                        }
                    }
                };
                let release = {
                    let rw = Arc::clone(&rw);
                    async move {
                        Timer::after(Duration::from_millis(1)).await;
                        // SAFETY: the test still owns the initial exclusive
                        // lock until this release path runs.
                        unsafe {
                            rw.unlock_exclusive();
                        }
                    }
                };
                let all = async {
                    join3(waiter1, waiter2, release).await;
                };
                or(all, async {
                    Timer::after(Duration::from_secs(1)).await;
                    panic!("waiting writers failed to make progress after writer unlock");
                })
                .await;
            }
        });
    }
}
