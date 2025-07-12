use event_listener::{listener, Event, Listener};
use parking_lot::lock_api::{
    GuardSend, RawMutex as RawMutexApi, RawRwLock as RawRwLockApi,
    RawRwLockDowngrade as RawRwLockDowngradeApi,
};
use parking_lot::RawMutex;
use std::mem;
use std::sync::atomic::{AtomicUsize, Ordering};

const WRITER_BIT: usize = 1;
const ONE_READER: usize = 2;

/// A simple RWLock with additional async methods.
pub struct RawRwLock {
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
    pub const fn new() -> Self {
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
    pub async fn lock_shared_async(&self) {
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
    pub async fn lock_exclusive_async(&self) {
        if self.mu.try_lock() {
            let new_state = self.state.fetch_or(WRITER_BIT, Ordering::SeqCst);
            if new_state & !WRITER_BIT == 0 {
                // no reader means lock is acquired successfully.
                return;
            }
            // slow path: setup listener and wait for no_readers signal.
            loop {
                listener!(self.no_readers => listener);
                let new_state = self.state.fetch_or(WRITER_BIT, Ordering::SeqCst);
                if new_state & !WRITER_BIT == 0 {
                    return;
                }
                // Here we already acquired mutex.
                // If async runtime drop the future at yield point,
                // we need to make sure the mutex is unlocked.
                let du = DeferUnlock(&self.mu);
                listener.await;
                mem::forget(du);
            }
        }
        // Waiting for writer to quit.
        loop {
            listener!(self.no_writer => no_writer);
            if self.mu.try_lock() {
                let new_state = self.state.fetch_or(WRITER_BIT, Ordering::SeqCst);
                if new_state & !WRITER_BIT == 0 {
                    return;
                }
                loop {
                    listener!(self.no_readers => listener);
                    let new_state = self.state.fetch_or(WRITER_BIT, Ordering::SeqCst);
                    if new_state & !WRITER_BIT == 0 {
                        return;
                    }
                    let du = DeferUnlock(&self.mu);
                    listener.await;
                    mem::forget(du);
                }
            }
            no_writer.await;
        }
    }
}

struct DeferUnlock<'a>(&'a RawMutex);

impl Drop for DeferUnlock<'_> {
    #[inline]
    fn drop(&mut self) {
        unsafe {
            self.0.unlock();
        }
    }
}

unsafe impl RawRwLockApi for RawRwLock {
    #[allow(clippy::declare_interior_mutable_const)]
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
        if self
            .state
            .compare_exchange(0, WRITER_BIT, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            return true; // no reader, no writer
        }
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
        self.no_writer.notify(1);
        unsafe {
            self.mu.unlock();
        }
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

unsafe impl RawRwLockDowngradeApi for RawRwLock {
    #[inline]
    unsafe fn downgrade(&self) {
        unsafe {
            debug_assert!(self.state.load(Ordering::Acquire) & !WRITER_BIT == 0);
            self.state.fetch_add(ONE_READER, Ordering::SeqCst);
            self.unlock_exclusive();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use parking_lot::lock_api::RawRwLock as RawRwLockApi;
    use parking_lot::RawRwLock as ParkingLotRawRwLock;
    use std::cell::UnsafeCell;
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    #[test]
    fn test_raw_rwlock_ops() {
        smol::block_on(async {
            let rw = Arc::new(RawRwLock::new());
            rw.lock_exclusive();
            assert!(rw.is_locked());
            assert!(rw.is_locked_exclusive());
            assert!(!rw.try_lock_shared());
            assert!(!rw.try_lock_exclusive());
            {
                let rw = Arc::clone(&rw);
                smol::spawn(async move {
                    rw.lock_exclusive_async().await;
                    unsafe {
                        rw.unlock_exclusive();
                    }
                })
                .detach();
            }
            {
                let rw = Arc::clone(&rw);
                smol::spawn(async move {
                    rw.lock_shared_async().await;
                    unsafe {
                        rw.unlock_shared();
                    }
                })
                .detach();
            }
            smol::Timer::after(Duration::from_millis(100)).await;
            unsafe {
                rw.unlock_exclusive();
            }
            smol::Timer::after(Duration::from_millis(100)).await;
        })
    }

    #[test]
    fn test_raw_rwlock_sync() {
        let counter = Arc::new(Counter::new());
        let mut threads = vec![];
        for _ in 0..10 {
            let counter = Arc::clone(&counter);
            let handle = std::thread::spawn(move || {
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
            let handle = std::thread::spawn(move || {
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
    fn test_raw_rwlock_single_thread() {
        const COUNT: usize = 10_000_000;
        smol::block_on(async {
            let counter = Counter::new();
            let start = Instant::now();
            for _ in 0..COUNT {
                counter.inc();
            }
            let dur1 = start.elapsed();
            println!(
                "sync inc, dur={:?}, tps={}",
                dur1,
                COUNT as f64 * 1_000_000_000f64 / dur1.as_nanos() as f64
            );
        });

        smol::block_on(async {
            let counter = Counter::new();
            let start = Instant::now();
            for _ in 0..COUNT {
                counter.inc_async().await;
            }
            let dur1 = start.elapsed();
            println!(
                "async inc, dur={:?}, tps={}",
                dur1,
                COUNT as f64 * 1_000_000_000f64 / dur1.as_nanos() as f64
            );
        });

        smol::block_on(async {
            let counter = ParkingLotCounter::new();
            let start = Instant::now();
            for _ in 0..COUNT {
                counter.inc();
            }
            let dur1 = start.elapsed();
            println!(
                "parking_lot inc, dur={:?}, tps={}",
                dur1,
                COUNT as f64 * 1_000_000_000f64 / dur1.as_nanos() as f64
            );
        });
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
            unsafe {
                self.mu.lock_exclusive();
                *self.data.get() += 1;
                self.mu.unlock_exclusive();
            }
        }

        #[inline]
        async fn inc_async(&self) {
            unsafe {
                self.mu.lock_exclusive_async().await;
                *self.data.get() += 1;
                self.mu.unlock_exclusive();
            }
        }

        #[inline]
        fn val(&self) -> usize {
            unsafe { *self.data.get() }
        }
    }
    unsafe impl Send for Counter {}
    unsafe impl Sync for Counter {}

    struct ParkingLotCounter {
        data: UnsafeCell<usize>,
        mu: ParkingLotRawRwLock,
    }
    impl ParkingLotCounter {
        #[inline]
        fn new() -> Self {
            ParkingLotCounter {
                data: UnsafeCell::new(0),
                mu: ParkingLotRawRwLock::INIT,
            }
        }

        #[inline]
        fn inc(&self) {
            unsafe {
                self.mu.lock_exclusive();
                *self.data.get() += 1;
                self.mu.unlock_exclusive();
            }
        }
    }
}
