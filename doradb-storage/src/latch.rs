use crate::error::{Error, Result, Validation, Validation::Invalid, Validation::Valid};
use parking_lot::lock_api::RawRwLock as RawRwLockApi;
use parking_lot::RawRwLock;
use std::str::FromStr;
use std::sync::atomic::{AtomicU64, Ordering};

pub const LATCH_EXCLUSIVE_BIT: u64 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LatchFallbackMode {
    Shared,
    Exclusive,
    Spin,
}

impl FromStr for LatchFallbackMode {
    type Err = Error;
    #[inline]
    fn from_str(s: &str) -> Result<Self> {
        let res = match s.to_lowercase().as_str() {
            "spin" => LatchFallbackMode::Spin,
            "shared" => LatchFallbackMode::Shared,
            "exclusive" => LatchFallbackMode::Exclusive,
            _ => return Err(Error::InvalidArgument),
        };
        Ok(res)
    }
}

/// A HybridLatch combines optimisitic lock(version validation) and
/// pessimistic lock(tranditional mutex) to support high-performance
/// on current operations.
///
/// It has three lock modes.
///
/// 1. optimisitic. Optimistic mode does not block read or write.
///    but once the inner data is read, version must be validated
///    to ensure no writer updated it.
///
/// 2. shared. Same as read lock, it can exist with
///    multiple reader but mutually exclusive with writer.
///
/// 3. exclusive. Same as write lock. Once the writer acquired the lock,
///    it first increment version and before unlocking, it also
///    increment version.
///
#[repr(C, align(64))]
pub struct HybridLatch {
    version: AtomicU64,
    lock: RawRwLock,
}

impl HybridLatch {
    #[inline]
    pub const fn new() -> Self {
        HybridLatch {
            version: AtomicU64::new(0),
            lock: RawRwLock::INIT,
        }
    }

    /// Returns current version with atomic load.
    #[inline]
    pub fn version_seqcst(&self) -> u64 {
        self.version.load(Ordering::SeqCst)
    }

    #[inline]
    pub fn version_acq(&self) -> u64 {
        self.version.load(Ordering::Acquire)
    }

    /// Returns whether the latch is already exclusive locked.
    #[inline]
    pub fn is_exclusive_latched(&self) -> bool {
        let ver = self.version_acq();
        (ver & LATCH_EXCLUSIVE_BIT) == LATCH_EXCLUSIVE_BIT
    }

    /// Returns whether the current version matches given one.
    #[inline]
    pub fn version_match(&self, version: u64) -> bool {
        let ver = self.version_acq();
        ver == version
    }

    #[inline]
    pub fn optimistic_fallback(&self, mode: LatchFallbackMode) -> HybridGuard<'_> {
        match mode {
            LatchFallbackMode::Spin => self.optimistic_spin(),
            LatchFallbackMode::Shared => self.optimistic_or_shared(),
            LatchFallbackMode::Exclusive => self.optimistic_or_exclusive(),
        }
    }

    /// Returns an optimistic lock guard via spin wait
    /// until exclusive lock is released.
    #[inline]
    pub fn optimistic_spin(&self) -> HybridGuard<'_> {
        let mut ver: u64;
        loop {
            ver = self.version_acq();
            if (ver & LATCH_EXCLUSIVE_BIT) != LATCH_EXCLUSIVE_BIT {
                break;
            }
            std::hint::spin_loop();
        }
        HybridGuard::new(self, GuardState::Optimistic, ver)
    }

    /// Try to acquire an optimistic lock.
    /// Fail if the lock is exclusive locked.
    #[inline]
    pub fn try_optimistic(&self) -> Option<HybridGuard<'_>> {
        let ver = self.version_acq();
        if (ver & LATCH_EXCLUSIVE_BIT) == LATCH_EXCLUSIVE_BIT {
            None
        } else {
            Some(HybridGuard::new(self, GuardState::Optimistic, ver))
        }
    }

    /// Get a read lock if lock is exclusive locked(blocking wait).
    /// Otherwise get an optimistic lock.
    #[inline]
    pub fn optimistic_or_shared(&self) -> HybridGuard<'_> {
        let ver = self.version_acq();
        if (ver & LATCH_EXCLUSIVE_BIT) == LATCH_EXCLUSIVE_BIT {
            self.lock.lock_shared();
            let ver = self.version_acq();
            HybridGuard::new(self, GuardState::Shared, ver)
        } else {
            HybridGuard::new(self, GuardState::Optimistic, ver)
        }
    }

    /// Get a write lock if lock is exclusive locked(blocking wait).
    /// Otherwise get an optimistic lock.
    /// This use case is rare.
    #[inline]
    pub fn optimistic_or_exclusive(&self) -> HybridGuard<'_> {
        let ver = self.version_acq();
        if (ver & LATCH_EXCLUSIVE_BIT) == LATCH_EXCLUSIVE_BIT {
            self.lock.lock_exclusive();
            let ver = self.version_acq() + LATCH_EXCLUSIVE_BIT;
            self.version.store(ver, Ordering::Release);
            HybridGuard::new(self, GuardState::Exclusive, ver)
        } else {
            HybridGuard::new(self, GuardState::Optimistic, ver)
        }
    }

    /// Get a write lock.
    #[inline]
    pub fn exclusive(&self) -> HybridGuard<'_> {
        self.lock.lock_exclusive(); // may block
        let ver = self.version_acq() + LATCH_EXCLUSIVE_BIT;
        self.version.store(ver, Ordering::Release);
        HybridGuard::new(self, GuardState::Exclusive, ver)
    }

    /// Get a shared lock.
    #[inline]
    pub fn shared(&self) -> HybridGuard<'_> {
        self.lock.lock_shared(); // may block
        let ver = self.version_acq();
        HybridGuard::new(self, GuardState::Shared, ver)
    }

    /// Try to get a write lock.
    #[inline]
    pub fn try_exclusive(&self) -> Option<HybridGuard<'_>> {
        if self.lock.try_lock_exclusive() {
            let ver = self.version_acq() + LATCH_EXCLUSIVE_BIT;
            self.version.store(ver, Ordering::Release);
            return Some(HybridGuard::new(self, GuardState::Exclusive, ver));
        }
        None
    }

    /// Try to get a read lock.
    #[inline]
    pub fn try_shared(&self) -> Option<HybridGuard<'_>> {
        if self.lock.try_lock_shared() {
            let ver = self.version_acq();
            return Some(HybridGuard::new(self, GuardState::Shared, ver));
        }
        None
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum GuardState {
    Optimistic,
    Shared,
    Exclusive,
}

/// HybridGuard is the union of three kinds of locks.
/// The common usage is to acquire optimistic lock first
/// and then upgrade to shared lock or exclusive lock.
///
/// An additional validation must be executed to for lock
/// upgrade, because the protected object may be entirely
/// rewritten to another object, e.g. frames in buffer pool
/// can be swapped to disk and reload another data page.
/// So any time we save an optimistic guard and would like
/// restore it later, we have to check the protected object
/// is still same, for simplicity we just check whether the
/// version is changed inbetween.
pub struct HybridGuard<'a> {
    lock: &'a HybridLatch,
    pub state: GuardState,
    // initial version when guard is created.
    pub version: u64,
}

impl<'a> HybridGuard<'a> {
    #[inline]
    fn new(lock: &'a HybridLatch, state: GuardState, version: u64) -> Self {
        HybridGuard {
            lock,
            state,
            version,
        }
    }

    /// Validate version is not changed.
    #[inline]
    pub fn validate(&self) -> bool {
        self.lock.version_match(self.version)
    }

    /// Validate whether transition from optimistic to shared is valid.
    #[inline]
    fn validate_shared_internal(&self) -> bool {
        debug_assert!(self.state == GuardState::Optimistic);
        self.lock.version_match(self.version)
    }

    #[inline]
    fn validate_exclusive_internal(&self) -> bool {
        debug_assert!(self.state == GuardState::Optimistic);
        // as we already acquire exclusive lock, current version is added by 1.
        self.lock.version_match(self.version + LATCH_EXCLUSIVE_BIT)
    }

    /// Convert lock mode to optimistic.
    #[inline]
    pub fn downgrade(mut self) -> Self {
        match self.state {
            GuardState::Exclusive => {
                let ver = self.version + LATCH_EXCLUSIVE_BIT;
                self.lock.version.store(ver, Ordering::Release);
                unsafe {
                    self.lock.lock.unlock_exclusive();
                }
                self.version = ver;
                self.state = GuardState::Optimistic;
                HybridGuard {
                    lock: self.lock,
                    version: self.version,
                    state: self.state,
                }
            }
            GuardState::Shared => {
                unsafe {
                    self.lock.lock.unlock_shared();
                }
                self.state = GuardState::Optimistic;
                HybridGuard {
                    lock: self.lock,
                    version: self.version,
                    state: self.state,
                }
            }
            GuardState::Optimistic => self,
        }
    }

    /// Try to convert a guard to shared mode.
    #[inline]
    pub fn try_shared(&mut self) -> Validation<()> {
        match self.state {
            GuardState::Optimistic => {
                // use try shared is ok.
                // because only when there is a exclusive lock, this try will fail.
                // and optimistic lock must be retried as vesion won't be matched.
                // an additional validation is required, because other thread may
                // gain the exclusive lock inbetween.
                if let Some(g) = self.lock.try_shared() {
                    if self.validate_shared_internal() {
                        *self = g;
                        return Valid(());
                    } // otherwise drop the read lock and notify caller to retry
                    debug_assert!(self.version != g.version);
                }
                Invalid
            }
            GuardState::Shared => Valid(()),
            GuardState::Exclusive => panic!("try shared on exclusive lock is not allowed"),
        }
    }

    #[inline]
    pub fn block_until_shared(self) -> Self {
        debug_assert!(self.state == GuardState::Optimistic);
        self.lock.shared()
    }

    /// Convert a guard to exclusive mode.
    /// return false if fail.(shared to exclusive will fail)
    #[inline]
    pub fn try_exclusive(&mut self) -> Validation<()> {
        match self.state {
            GuardState::Optimistic => {
                if let Some(g) = self.lock.try_exclusive() {
                    if self.validate_exclusive_internal() {
                        *self = g;
                        return Valid(());
                    }
                    debug_assert!(self.version + LATCH_EXCLUSIVE_BIT != g.version);
                }
                Invalid
            }
            GuardState::Shared => panic!("try exclusive on shared lock is not allowed"),
            GuardState::Exclusive => Valid(()),
        }
    }

    #[inline]
    pub fn block_until_exclusive(self) -> Self {
        debug_assert!(self.state == GuardState::Optimistic);
        self.lock.exclusive()
    }

    #[inline]
    pub fn optimistic_clone(&self) -> Result<Self> {
        if self.state == GuardState::Optimistic {
            return Ok(HybridGuard {
                lock: self.lock,
                state: self.state,
                version: self.version,
            });
        }
        Err(Error::InvalidState)
    }

    #[inline]
    pub fn refresh_version(&mut self) {
        debug_assert!(self.state == GuardState::Optimistic);
        self.version = self.lock.version_acq();
    }
}

impl<'a> Drop for HybridGuard<'a> {
    #[inline]
    fn drop(&mut self) {
        match self.state {
            GuardState::Exclusive => {
                let ver = self.version + LATCH_EXCLUSIVE_BIT;
                self.lock.version.store(ver, Ordering::Release);
                unsafe {
                    self.lock.lock.unlock_exclusive();
                }
            }
            GuardState::Shared => unsafe {
                self.lock.lock.unlock_shared();
            },
            GuardState::Optimistic => (),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_hybrid_lock() {
        let boxed = Box::new(HybridLatch::new());
        let latch: &'static mut HybridLatch = Box::leak(boxed);
        assert!(!latch.is_exclusive_latched());
        let ver = latch.version_seqcst();
        assert!(latch.version_match(ver));
        // optimistic guard
        let opt_g1 = latch.optimistic_spin();
        assert!(opt_g1.validate());
        drop(opt_g1);
        // optimistic or shared
        let opt_g2 = latch.optimistic_or_shared();
        assert!(opt_g2.validate());
        drop(opt_g2);
        let opt_g3 = latch.optimistic_or_exclusive();
        assert!(opt_g3.validate());
        drop(opt_g3);
        let shared_g1 = latch.shared();
        assert!(shared_g1.state == GuardState::Shared);
        drop(shared_g1);
        let shared_g2 = latch.try_shared().unwrap();
        assert!(shared_g2.state == GuardState::Shared);
        drop(shared_g2);
        let exclusive_g1 = latch.exclusive();
        assert!(exclusive_g1.state == GuardState::Exclusive);
        let ver2 = latch.version_seqcst();
        assert!(ver2 == ver + 1);
        drop(exclusive_g1);
        let ver3 = latch.version_seqcst();
        assert!(ver3 == ver2 + 1);
        let exclusive_g2 = latch.try_exclusive().unwrap();
        assert!(exclusive_g2.state == GuardState::Exclusive);
        drop(exclusive_g2);
    }
}
