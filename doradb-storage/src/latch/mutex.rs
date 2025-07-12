use event_listener::{listener, Event, IntoNotification};
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
        unsafe {
            self.inner.unlock();
            self.event.notify(1usize.relaxed());
        }
    }

    /// Lock this mutex in async way.
    #[inline]
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
    use futures::stream::{FuturesUnordered, StreamExt};
    use std::cell::UnsafeCell;
    use std::sync::Arc;
    use std::time::Instant;

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
                "parkinglot inc, dur={:?}, tps={}",
                dur1,
                COUNT as f64 * 1_000_000_000f64 / dur1.as_nanos() as f64
            );
        });

        smol::block_on(async {})
    }

    #[test]
    fn test_raw_mutex_multi_threads() {
        const COUNT: usize = 3_000_000;

        let threads = std::env::var("RAW_MUTEX_THREADS")
            .map_err(|_| ())
            .and_then(|s| s.parse::<usize>().map_err(|_| ()))
            .unwrap_or(4usize);

        let sys_threads = std::env::var("RAW_MUTEX_SYS_THREADS")
            .map_err(|_| ())
            .and_then(|s| s.parse::<usize>().map_err(|_| ()))
            .unwrap_or(1usize);
        assert!(threads % sys_threads == 0);

        // sync lock
        let start = Instant::now();
        let mut handles = Vec::with_capacity(threads);
        let c1 = Arc::new(Counter::new());
        for _ in 0..threads {
            let c1 = Arc::clone(&c1);
            let handle = std::thread::spawn(move || {
                smol::block_on(async {
                    for _ in 0..COUNT {
                        if c1.inc() >= COUNT {
                            break;
                        }
                    }
                })
            });
            handles.push(handle);
        }
        for h in handles {
            h.join().unwrap();
        }
        let dur = start.elapsed();
        println!(
            "sync inc, threads={}, dur={:?}, tps={}",
            threads,
            dur,
            COUNT as f64 * 1_000_000_000f64 / dur.as_nanos() as f64
        );

        // async lock with lightweight tasks.
        let tasks_per_thread = threads / sys_threads;
        let start = Instant::now();
        let mut handles = Vec::with_capacity(sys_threads);
        for _ in 0..sys_threads {
            let c2 = Arc::new(Counter::new());
            let h = std::thread::spawn(move || {
                smol::block_on(async {
                    let mut futs = FuturesUnordered::new();
                    for _ in 0..tasks_per_thread {
                        let c2 = Arc::clone(&c2);
                        let task = smol::spawn(async move {
                            for _ in 0..COUNT {
                                if c2.inc_async().await >= COUNT {
                                    break;
                                }
                            }
                        });
                        futs.push(task);
                    }
                    while futs.next().await.is_some() {}
                })
            });
            handles.push(h);
        }
        for h in handles {
            h.join().unwrap();
        }
        let dur = start.elapsed();
        println!(
            "async inc, sys_threads={}, threads={}, dur={:?}, tps={}",
            sys_threads,
            threads,
            dur,
            COUNT as f64 * 1_000_000_000f64 / dur.as_nanos() as f64
        );

        let mut handles = Vec::with_capacity(threads);
        let start = Instant::now();
        let c3 = Arc::new(ParkingLotCounter::new());
        for _ in 0..threads {
            let c3 = Arc::clone(&c3);
            let handle = std::thread::spawn(move || {
                smol::block_on(async {
                    for _ in 0..COUNT {
                        if c3.inc() >= COUNT {
                            break;
                        }
                    }
                })
            });
            handles.push(handle);
        }
        for h in handles {
            h.join().unwrap();
        }
        let dur = start.elapsed();
        println!(
            "parking_lot inc, threads={}, dur={:?}, tps={}",
            threads,
            dur,
            COUNT as f64 * 1_000_000_000f64 / dur.as_nanos() as f64
        );
    }

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
            unsafe { *self.data.get() }
        }
    }
    unsafe impl Send for Counter {}
    unsafe impl Sync for Counter {}

    struct ParkingLotCounter {
        data: UnsafeCell<usize>,
        mu: ParkingLotRawMutex,
    }

    impl ParkingLotCounter {
        #[inline]
        fn new() -> Self {
            ParkingLotCounter {
                data: UnsafeCell::new(0),
                mu: ParkingLotRawMutex::INIT,
            }
        }

        #[inline]
        fn inc(&self) -> usize {
            unsafe {
                self.mu.lock();
                *self.data.get() += 1;
                let v = *self.data.get();
                self.mu.unlock();
                v
            }
        }
    }

    unsafe impl Send for ParkingLotCounter {}
    unsafe impl Sync for ParkingLotCounter {}
}
