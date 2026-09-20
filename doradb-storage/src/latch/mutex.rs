use event_listener::{Event, IntoNotification, listener};
use parking_lot::RawMutex as ParkingLotRawMutex;
use parking_lot::lock_api::RawMutex as ParkingLotRawMutexAPI;
use std::cell::UnsafeCell;
use std::fmt::{self, Debug};
use std::marker::PhantomData;
use std::ops::{Deref, DerefMut};

pub(super) struct Mutex<T> {
    raw: RawMutex,
    data: UnsafeCell<T>,
}

// SAFETY: `Mutex<T>` transfers ownership of `T` behind the raw mutex and only
// exposes it through lock-guard access.
unsafe impl<T: Send> Send for Mutex<T> {}
// SAFETY: shared references are safe because the raw mutex serializes access to
// the inner `UnsafeCell<T>`.
unsafe impl<T: Send> Sync for Mutex<T> {}

impl<T> Mutex<T> {
    /// Create a new mutex.
    #[inline]
    #[cfg_attr(not(test), expect(dead_code, reason = "reserved new"))]
    pub(super) const fn new(val: T) -> Mutex<T> {
        Mutex {
            raw: RawMutex::new(),
            data: UnsafeCell::new(val),
        }
    }

    /// Returns underlying data.
    #[inline]
    #[cfg_attr(not(test), expect(dead_code, reason = "reserved into_inner"))]
    pub(super) fn into_inner(self) -> T {
        self.data.into_inner()
    }

    #[inline]
    #[cfg_attr(not(test), expect(dead_code, reason = "reserved lock"))]
    pub(super) fn lock(&self) -> MutexGuard<'_, T> {
        self.raw.lock();
        MutexGuard {
            mutex: self,
            marker: PhantomData,
        }
    }

    #[inline]
    pub(super) fn try_lock(&self) -> Option<MutexGuard<'_, T>> {
        if self.raw.try_lock() {
            Some(MutexGuard {
                mutex: self,
                marker: PhantomData,
            })
        } else {
            None
        }
    }

    #[inline]
    #[expect(dead_code, reason = "reserved lock_async")]
    pub(super) async fn lock_async(&self) -> MutexGuard<'_, T> {
        if let Some(g) = self.try_lock() {
            return g;
        }
        loop {
            // create on-stack listener.
            listener!(self.raw.event => listener);

            if let Some(g) = self.try_lock() {
                return g;
            }
            listener.await;
        }
    }

    #[inline]
    #[cfg_attr(not(test), expect(dead_code, reason = "reserved is_locked"))]
    pub(super) fn is_locked(&self) -> bool {
        self.raw.is_locked()
    }
}

/// A simple RawMutex with additional async lock method.
/// The caller should guarantee lock/unlock calls are paired
/// even in async environment.
pub(super) struct RawMutex {
    inner: ParkingLotRawMutex,
    event: Event,
}

impl RawMutex {
    /// Create a new async RawMutex.
    #[inline]
    pub(super) const fn new() -> RawMutex {
        RawMutex {
            inner: ParkingLotRawMutex::INIT,
            event: Event::new(),
        }
    }

    /// Lock this mutex in sync way.
    #[inline]
    pub(super) fn lock(&self) {
        self.inner.lock();
    }

    #[inline]
    pub(super) fn is_locked(&self) -> bool {
        self.inner.is_locked()
    }

    /// Try lock this mutex in non-blocking way.
    /// Returns false if lock can not be acquired.
    #[inline]
    pub(super) fn try_lock(&self) -> bool {
        self.inner.try_lock()
    }

    /// Unlock the mutex.
    ///
    /// # Safety
    ///
    /// Callers must have acquired this mutex and must pair each unlock with a
    /// previous successful lock operation.
    #[inline]
    pub(super) unsafe fn unlock(&self) {
        // SAFETY: the caller upholds the lock/unlock pairing contract, and the
        // event notification happens only after releasing the raw mutex.
        unsafe {
            self.inner.unlock();
            self.event.notify(1usize.relaxed());
        }
    }

    /// Lock this mutex in async way.
    #[inline]
    #[cfg(test)]
    pub async fn lock_async(&self) {
        if self.try_lock() {
            return;
        }
        loop {
            // create on-stack listener.
            listener!(self.event => listener);

            if self.try_lock() {
                return;
            }
            listener.await;
        }
    }
}

#[must_use = "if unused the Mutex will immediately unlock"]
pub(super) struct MutexGuard<'a, T> {
    mutex: &'a Mutex<T>,
    marker: PhantomData<&'a mut T>,
}

// SAFETY: the guard only exposes shared access to `T` when `T: Sync`, and it
// retains the mutex ownership for the whole guard lifetime.
unsafe impl<'a, T: Sync + 'a> Sync for MutexGuard<'a, T> {}

impl<'a, T: 'a> Deref for MutexGuard<'a, T> {
    type Target = T;
    #[inline]
    fn deref(&self) -> &Self::Target {
        // SAFETY: holding the guard proves the mutex is locked, so shared access
        // to the inner `UnsafeCell<T>` is valid for the guard lifetime.
        unsafe { &*self.mutex.data.get() }
    }
}

impl<'a, T: 'a> DerefMut for MutexGuard<'a, T> {
    #[inline]
    fn deref_mut(&mut self) -> &mut Self::Target {
        // SAFETY: `&mut self` plus the held mutex lock guarantees unique access
        // to the inner `UnsafeCell<T>`.
        unsafe { &mut *self.mutex.data.get() }
    }
}

impl<'a, T: 'a> Drop for MutexGuard<'a, T> {
    #[inline]
    fn drop(&mut self) {
        // SAFETY: `MutexGuard` is only created after a successful lock and drops
        // exactly once, so this unlock is correctly paired.
        unsafe { self.mutex.raw.unlock() }
    }
}

impl<'a, T: fmt::Debug + 'a> fmt::Debug for MutexGuard<'a, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        Debug::fmt(&**self, f)
    }
}

impl<'a, T: fmt::Display + 'a> fmt::Display for MutexGuard<'a, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        (**self).fmt(f)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::UnsafeCell;
    use std::sync::Arc;
    use std::thread::spawn;

    struct Counter {
        data: UnsafeCell<usize>,
        mu: RawMutex,
    }

    impl Counter {
        #[inline]
        fn new() -> Self {
            Counter {
                data: UnsafeCell::new(0),
                mu: RawMutex::new(),
            }
        }

        #[inline]
        fn inc(&self) -> usize {
            // SAFETY: this helper acquires `mu` before touching `data` and
            // releases it before returning.
            unsafe {
                self.mu.lock();
                *self.data.get() += 1;
                let v = *self.data.get();
                self.mu.unlock();
                v
            }
        }

        #[inline]
        async fn inc_async(&self) -> usize {
            // SAFETY: this helper acquires `mu` asynchronously before touching
            // `data` and releases it before returning.
            unsafe {
                self.mu.lock_async().await;
                *self.data.get() += 1;
                let v = *self.data.get();
                self.mu.unlock();
                v
            }
        }

        #[inline]
        fn val(&self) -> usize {
            // SAFETY: tests only read the counter after all worker activity is
            // quiesced, so no concurrent mutation remains.
            unsafe { *self.data.get() }
        }
    }
    // SAFETY: shared references are synchronized by `RawMutex`.
    unsafe impl Sync for Counter {}

    #[test]
    fn test_mutex_ops() {
        let mu = Mutex::new(42i32);
        assert!(!mu.is_locked());
        let mut g = mu.lock();
        *g += 1;
        assert!(mu.is_locked());
        assert!(mu.try_lock().is_none());
        drop(g);
        assert!(mu.try_lock().is_some());
        let v = mu.into_inner();
        assert!(v == 43);
    }

    #[test]
    fn test_raw_mutex_sync() {
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
    fn test_raw_mutex_async() {
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
}
