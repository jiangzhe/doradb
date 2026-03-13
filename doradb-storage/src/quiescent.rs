use std::ops::Deref;
use std::pin::Pin;
use std::ptr::{NonNull, addr_of_mut};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread;
use std::time::Duration;

// Match Arc's soft refcount ceiling while leaving headroom above the panic
// threshold for the fetch-add rollback path.
const MAX_GUARD_COUNT: usize = isize::MAX as usize;
const OWNER_DROP_SPIN_LIMIT: u32 = 64;
const OWNER_DROP_YIELD_LIMIT: u32 = 128;
const OWNER_DROP_INITIAL_SLEEP_US: u64 = 50;
const OWNER_DROP_MAX_SLEEP_US: u64 = 1_000;

struct QuiescentInner<T> {
    guard_count: AtomicUsize,
    value: T,
}

impl<T> QuiescentInner<T> {
    #[inline]
    fn new(value: T) -> Self {
        Self {
            guard_count: AtomicUsize::new(0),
            value,
        }
    }

    #[inline]
    fn acquire_guard(&self) {
        let old_count = self.guard_count.fetch_add(1, Ordering::Relaxed);
        if old_count >= MAX_GUARD_COUNT {
            self.guard_count.fetch_sub(1, Ordering::Relaxed);
            guard_count_overflow();
        }
    }

    #[inline]
    fn release_guard(&self) {
        // Guard release must not touch quiescent metadata after the decrement.
        // Once owner teardown observes zero, it is free to reclaim the entire
        // allocation immediately.
        let old_count = self.guard_count.fetch_sub(1, Ordering::Release);
        if old_count == 0 {
            self.guard_count.fetch_add(1, Ordering::Relaxed);
            guard_count_underflow();
        }
    }

    #[inline]
    fn value_ptr(inner: NonNull<Self>) -> NonNull<T> {
        // SAFETY: `inner` points to a live `QuiescentInner<T>` allocation.
        // Projecting the `value` field with `addr_of_mut!` does not create an
        // intermediate reference and preserves the stable heap address.
        let ptr = unsafe { addr_of_mut!((*inner.as_ptr()).value) };
        NonNull::new(ptr).expect("quiescent inner value pointer")
    }
}

#[cold]
fn guard_count_overflow() -> ! {
    panic!("quiescent guard count overflow");
}

#[cold]
fn guard_count_underflow() -> ! {
    panic!("quiescent guard count underflow");
}

/// Owns a heap-allocated value that can be shared by quiescent guards.
///
/// The owner allocation is pinned for the full lifetime of the box, so the
/// stored value stays at a stable heap address while guards exist. Dropping the
/// owner blocks until all outstanding guards have been released. Teardown is a
/// cold polling path with bounded spin/yield and capped sleep backoff so guard
/// release stays on a single-atomic hot path. Callers must therefore avoid
/// dropping the owner while still holding guards themselves, or teardown will
/// block forever.
pub struct QuiescentBox<T> {
    inner: Pin<Box<QuiescentInner<T>>>,
}

impl<T> QuiescentBox<T> {
    /// Creates a new quiescent owner around `value`.
    #[inline]
    pub fn new(value: T) -> Self {
        Self {
            inner: Box::pin(QuiescentInner::new(value)),
        }
    }

    #[inline]
    fn inner_ptr(&self) -> NonNull<QuiescentInner<T>> {
        NonNull::from(self.inner.as_ref().get_ref())
    }

    /// Creates a shared keepalive guard to the owned value.
    ///
    /// Guard creation is intentionally cheap: it increments one keepalive
    /// counter and stores raw pointers back to the owner allocation.
    #[inline]
    pub fn guard(&self) -> QuiescentGuard<T> {
        QuiescentGuard::new(self.inner_ptr())
    }
}

impl<T> Deref for QuiescentBox<T> {
    type Target = T;

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.inner.as_ref().get_ref().value
    }
}

impl<T> Drop for QuiescentBox<T> {
    #[inline]
    fn drop(&mut self) {
        let inner = self.inner.as_ref().get_ref();
        let mut attempts = 0u32;
        // Owner teardown is cold, so use backoff here and keep guard release as
        // a single atomic decrement on the hot path.
        while inner.guard_count.load(Ordering::Acquire) != 0 {
            if attempts < OWNER_DROP_SPIN_LIMIT {
                std::hint::spin_loop();
            } else if attempts < OWNER_DROP_YIELD_LIMIT {
                thread::yield_now();
            } else {
                let sleep_shift = (attempts - OWNER_DROP_YIELD_LIMIT).min(5);
                let sleep_us =
                    (OWNER_DROP_INITIAL_SLEEP_US << sleep_shift).min(OWNER_DROP_MAX_SLEEP_US);
                thread::sleep(Duration::from_micros(sleep_us));
            }
            attempts = attempts.saturating_add(1);
        }
    }
}

/// Cloneable shared access handle for a [`QuiescentBox`]-owned value.
///
/// Each guard keeps the owner allocation alive until the guard is dropped.
/// Guards only provide shared access and dereference to `&T`.
pub struct QuiescentGuard<T> {
    ptr: NonNull<T>,
    inner: NonNull<QuiescentInner<T>>,
}

impl<T> QuiescentGuard<T> {
    #[inline]
    fn new(inner: NonNull<QuiescentInner<T>>) -> Self {
        // SAFETY: `inner` originates from a live `QuiescentBox` allocation and
        // remains valid while the acquired keepalive count is held by the guard.
        let inner_ref = unsafe { inner.as_ref() };
        inner_ref.acquire_guard();
        Self {
            ptr: QuiescentInner::value_ptr(inner),
            inner,
        }
    }

    #[inline]
    fn inner_ref(&self) -> &QuiescentInner<T> {
        // SAFETY: guards increment the keepalive count on creation and release
        // it only in `Drop`, so the owner allocation remains live here.
        unsafe { self.inner.as_ref() }
    }

    /// Returns the raw pointer to the guarded value.
    #[inline]
    pub fn as_ptr(&self) -> *const T {
        self.ptr.as_ptr() as *const T
    }
}

impl<T> Clone for QuiescentGuard<T> {
    #[inline]
    fn clone(&self) -> Self {
        Self::new(self.inner)
    }
}

impl<T> Deref for QuiescentGuard<T> {
    type Target = T;

    #[inline]
    fn deref(&self) -> &Self::Target {
        // SAFETY: the guard holds one keepalive count for the owner
        // allocation, so the pointee stays valid for the full guard lifetime.
        unsafe { self.ptr.as_ref() }
    }
}

impl<T> Drop for QuiescentGuard<T> {
    #[inline]
    fn drop(&mut self) {
        self.inner_ref().release_guard();
    }
}

// SAFETY: moving a guard to another thread only exposes shared `&T` access,
// which is thread-safe exactly when `T: Sync`.
unsafe impl<T: Sync> Send for QuiescentGuard<T> {}

// SAFETY: sharing references to guards is equivalent to sharing references to
// `&T`, so this is sound exactly when `T: Sync`.
unsafe impl<T: Sync> Sync for QuiescentGuard<T> {}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::mpsc;
    use std::thread;
    use std::time::Duration;
    use std::{panic, panic::AssertUnwindSafe};

    struct DropSpy {
        dropped: Arc<AtomicBool>,
    }

    impl Drop for DropSpy {
        fn drop(&mut self) {
            self.dropped.store(true, Ordering::Release);
        }
    }

    #[test]
    fn test_quiescent_guard_deref_and_as_ptr() {
        let owner = QuiescentBox::new(String::from("hello"));
        let guard = owner.guard();
        let owner_ptr = std::ptr::from_ref::<String>(&owner);
        assert_eq!(&*owner, "hello");
        assert_eq!(&*guard, "hello");
        assert_eq!(guard.as_ptr(), owner_ptr);
    }

    #[test]
    fn test_quiescent_guard_clone_keeps_same_pointer() {
        let owner = QuiescentBox::new(vec![1u64, 2, 3, 4]);
        let guard = owner.guard();
        let guard_clone = guard.clone();
        let owner_ptr = std::ptr::from_ref::<Vec<u64>>(&owner);
        assert_eq!(guard.as_ptr(), owner_ptr);
        assert_eq!(guard_clone.as_ptr(), owner_ptr);
        assert_eq!(guard.iter().sum::<u64>(), 10);
        assert_eq!(guard_clone.iter().sum::<u64>(), 10);
    }

    #[test]
    fn test_quiescent_guard_is_send_for_sync_types() {
        let owner = QuiescentBox::new(vec![1u64, 2, 3, 4]);
        let owner_ptr = std::ptr::from_ref::<Vec<u64>>(&owner) as usize;
        let mut handles = Vec::new();
        for _ in 0..4 {
            let guard = owner.guard();
            handles.push(thread::spawn(move || {
                assert_eq!(guard.iter().sum::<u64>(), 10);
                guard.as_ptr() as usize
            }));
        }
        for handle in handles {
            assert_eq!(handle.join().unwrap(), owner_ptr);
        }
    }

    #[test]
    fn test_quiescent_guard_overflow_panics_without_mutating_count() {
        let owner = QuiescentBox::new(7u64);
        let inner = owner.inner.as_ref().get_ref();
        inner.guard_count.store(MAX_GUARD_COUNT, Ordering::Relaxed);

        let res = panic::catch_unwind(AssertUnwindSafe(|| owner.guard()));
        assert!(res.is_err());
        assert_eq!(inner.guard_count.load(Ordering::Relaxed), MAX_GUARD_COUNT);

        inner.guard_count.store(0, Ordering::Relaxed);
    }

    #[test]
    fn test_quiescent_box_drop_waits_for_last_guard() {
        let dropped = Arc::new(AtomicBool::new(false));
        let owner = QuiescentBox::new(DropSpy {
            dropped: Arc::clone(&dropped),
        });
        let guard = owner.guard();
        let (started_tx, started_rx) = mpsc::channel();
        let (done_tx, done_rx) = mpsc::channel();
        let handle = thread::spawn(move || {
            started_tx.send(()).unwrap();
            drop(owner);
            done_tx.send(()).unwrap();
        });

        started_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        assert!(!dropped.load(Ordering::Acquire));
        assert!(done_rx.recv_timeout(Duration::from_millis(100)).is_err());

        drop(guard);

        done_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        assert!(dropped.load(Ordering::Acquire));
        handle.join().unwrap();
    }

    #[test]
    fn test_quiescent_box_drop_waits_for_all_guard_clones() {
        let dropped = Arc::new(AtomicBool::new(false));
        let owner = QuiescentBox::new(DropSpy {
            dropped: Arc::clone(&dropped),
        });
        let guard = owner.guard();
        let guard_clone = guard.clone();
        let (release_tx, release_rx) = mpsc::channel();
        let (started_tx, started_rx) = mpsc::channel();
        let (done_tx, done_rx) = mpsc::channel();

        let clone_handle = thread::spawn(move || {
            release_rx.recv().unwrap();
            drop(guard_clone);
        });
        let owner_handle = thread::spawn(move || {
            started_tx.send(()).unwrap();
            drop(owner);
            done_tx.send(()).unwrap();
        });

        started_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        drop(guard);
        assert!(!dropped.load(Ordering::Acquire));
        assert!(done_rx.recv_timeout(Duration::from_millis(100)).is_err());

        release_tx.send(()).unwrap();

        done_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        assert!(dropped.load(Ordering::Acquire));
        clone_handle.join().unwrap();
        owner_handle.join().unwrap();
    }
}
