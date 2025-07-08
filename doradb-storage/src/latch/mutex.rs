use event_listener::{Event, IntoNotification};
use parking_lot::lock_api::RawMutex as ParkingLotRawMutexAPI;
use parking_lot::RawMutex as ParkingLotRawMutex;
use std::cell::UnsafeCell;
use std::fmt;
use std::marker::PhantomData;
use std::ops::{Deref, DerefMut};

pub struct Mutex<T> {
    raw: RawMutex,
    data: UnsafeCell<T>,
}

unsafe impl<T: Send> Send for Mutex<T> {}
unsafe impl<T: Send> Sync for Mutex<T> {}

impl<T> Mutex<T> {
    /// Create a new mutex.
    #[inline]
    pub const fn new(val: T) -> Mutex<T> {
        Mutex {
            raw: RawMutex::new(),
            data: UnsafeCell::new(val),
        }
    }

    /// Returns underlying data.
    #[inline]
    pub fn into_inner(self) -> T {
        self.data.into_inner()
    }

    #[inline]
    pub fn lock(&self) -> MutexGuard<'_, T> {
        self.raw.lock();
        MutexGuard {
            mutex: self,
            marker: PhantomData,
        }
    }

    #[inline]
    pub fn try_lock(&self) -> Option<MutexGuard<'_, T>> {
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
    pub async fn lock_async(&self) -> MutexGuard<'_, T> {
        self.raw.lock_async().await;
        MutexGuard {
            mutex: self,
            marker: PhantomData,
        }
    }

    #[inline]
    pub fn is_locked(&self) -> bool {
        self.raw.is_locked()
    }
}

/// A simple RawMutex with additional async lock method.
/// The caller should guarantee lock/unlock calls are paired
/// even in async environment.
pub struct RawMutex {
    inner: ParkingLotRawMutex,
    event: Event,
}

unsafe impl Send for RawMutex {}
unsafe impl Sync for RawMutex {}

impl RawMutex {
    /// Create a new async RawMutex.
    #[inline]
    pub const fn new() -> RawMutex {
        RawMutex {
            inner: ParkingLotRawMutex::INIT,
            event: Event::new(),
        }
    }

    /// Lock this mutex in sync way.
    #[inline]
    pub fn lock(&self) {
        self.inner.lock();
    }

    #[inline]
    pub fn is_locked(&self) -> bool {
        self.inner.is_locked()
    }

    /// Try lock this mutex in non-blocking way.
    /// Returns false if lock can not be acquired.
    #[inline]
    pub fn try_lock(&self) -> bool {
        self.inner.try_lock()
    }

    /// Unlock the mutex.
    #[inline]
    pub unsafe fn unlock(&self) {
        self.inner.unlock();
        self.event.notify(1usize.relaxed());
    }

    /// Lock this mutex in async way.
    #[inline]
    pub async fn lock_async(&self) {
        while !self.try_lock() {
            let listener = self.event.listen();

            // re-check
            if self.try_lock() {
                return;
            }

            listener.await;
        }
    }
}

#[must_use = "if unused the Mutex will immediately unlock"]
pub struct MutexGuard<'a, T> {
    mutex: &'a Mutex<T>,
    marker: PhantomData<&'a mut T>,
}

unsafe impl<'a, T: Sync + 'a> Sync for MutexGuard<'a, T> {}

impl<'a, T: 'a> Deref for MutexGuard<'a, T> {
    type Target = T;
    #[inline]
    fn deref(&self) -> &Self::Target {
        unsafe { &*self.mutex.data.get() }
    }
}

impl<'a, T: 'a> DerefMut for MutexGuard<'a, T> {
    #[inline]
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe { &mut *self.mutex.data.get() }
    }
}

impl<'a, T: 'a> Drop for MutexGuard<'a, T> {
    #[inline]
    fn drop(&mut self) {
        unsafe { self.mutex.raw.unlock() }
    }
}

impl<'a, T: fmt::Debug + 'a> fmt::Debug for MutexGuard<'a, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&**self, f)
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
    use std::time::Instant;

    #[test]
    fn test_raw_mutex_sync() {
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
    fn test_raw_mutex_async() {
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
    fn test_raw_mutex_single_thread() {
        const COUNT: usize = 1_000_000;
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
    }

    struct Counter {
        data: UnsafeCell<usize>,
        mu: RawMutex,
    }

    impl Counter {
        fn new() -> Self {
            Counter {
                data: UnsafeCell::new(0),
                mu: RawMutex::new(),
            }
        }

        fn inc(&self) {
            unsafe {
                self.mu.lock();
                *self.data.get() += 1;
                self.mu.unlock();
            }
        }

        async fn inc_async(&self) {
            unsafe {
                self.mu.lock_async().await;
                *self.data.get() += 1;
                self.mu.unlock();
            }
        }

        fn val(&self) -> usize {
            unsafe { *self.data.get() }
        }
    }
    unsafe impl Send for Counter {}
    unsafe impl Sync for Counter {}
}
