use crate::error::{
    ConfigError, Error, Result, Validation, Validation::Invalid, Validation::Valid,
};
use error_stack::Report;
use parking_lot::lock_api::{
    RawRwLock as RawRwLockApi, RawRwLockDowngrade as RawRwLockDowngradeAPI,
};
// use parking_lot::RawRwLock;
use crate::latch::rwlock::RawRwLock;
use std::hint::spin_loop;
use std::marker::PhantomData;
use std::ptr::NonNull;
use std::str::FromStr;
use std::sync::atomic::{AtomicU64, Ordering};

/// Bit marking exclusive ownership in the latch version word.
pub(crate) const LATCH_EXCLUSIVE_BIT: u64 = 1;

/// Fallback strategy used when an optimistic latch observes an exclusive owner.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LatchFallbackMode {
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
            _ => {
                return Err(Report::new(ConfigError::InvalidLatchFallbackMode)
                    .attach(format!("value={s}"))
                    .into());
            }
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
pub(crate) struct HybridLatch {
    version: AtomicU64,
    lock: RawRwLock,
}

impl HybridLatch {
    /// Creates an unlocked latch with version 0.
    #[inline]
    pub(crate) const fn new() -> Self {
        HybridLatch {
            version: AtomicU64::new(0),
            lock: RawRwLock::INIT,
        }
    }

    /// Loads the current latch version with acquire ordering.
    #[inline]
    pub(crate) fn version_acq(&self) -> u64 {
        self.version.load(Ordering::Acquire)
    }

    /// Returns whether the latch is already exclusive locked.
    #[inline]
    #[cfg_attr(not(test), expect(dead_code, reason = "reserved is_exclusive_latched"))]
    pub(crate) fn is_exclusive_latched(&self) -> bool {
        let ver = self.version_acq();
        (ver & LATCH_EXCLUSIVE_BIT) == LATCH_EXCLUSIVE_BIT
    }

    /// Returns whether the current version matches given one.
    #[inline]
    pub(crate) fn version_match(&self, version: u64) -> bool {
        let ver = self.version_acq();
        ver == version
    }

    /// Returns an optimistic lock guard via spin wait
    /// until exclusive lock is released.
    #[inline]
    pub(crate) fn optimistic_spin(&self) -> HybridGuard<'_> {
        HybridGuard::from_raw(self.optimistic_spin_raw())
    }

    /// Returns a raw optimistic guard after spinning while exclusive ownership is present.
    #[inline]
    pub(crate) fn optimistic_spin_raw(&self) -> RawHybridGuard {
        let mut ver: u64;
        loop {
            ver = self.version_acq();
            if (ver & LATCH_EXCLUSIVE_BIT) != LATCH_EXCLUSIVE_BIT {
                break;
            }
            spin_loop();
        }
        RawHybridGuard::new(self, GuardState::Optimistic, ver)
    }

    /// Reads data under optimistic mode and retries until version validates.
    ///
    /// The callback may be invoked multiple times. It must be side-effect free
    /// and should return owned/copied data instead of references.
    #[inline]
    pub(crate) fn optimistic_read<R, F>(&self, mut read: F) -> R
    where
        F: FnMut() -> R,
    {
        loop {
            let g = self.optimistic_spin();
            let out = read();
            if g.validate() {
                return out;
            }
        }
    }

    /// Returns an optimistic raw guard, or waits for a shared raw guard when exclusive-locked.
    #[inline]
    pub(crate) async fn optimistic_or_shared_raw(&self) -> RawHybridGuard {
        let ver = self.version_acq();
        if (ver & LATCH_EXCLUSIVE_BIT) == LATCH_EXCLUSIVE_BIT {
            self.lock.lock_shared_async().await;
            let ver = self.version_acq();
            RawHybridGuard::new(self, GuardState::Shared, ver)
        } else {
            RawHybridGuard::new(self, GuardState::Optimistic, ver)
        }
    }

    /// Returns an optimistic raw guard, or waits for an exclusive raw guard when exclusive-locked.
    #[inline]
    pub(crate) async fn optimistic_or_exclusive_raw(&self) -> RawHybridGuard {
        let ver = self.version_acq();
        if (ver & LATCH_EXCLUSIVE_BIT) == LATCH_EXCLUSIVE_BIT {
            self.lock.lock_exclusive_async().await;
            let ver = self
                .version
                .fetch_add(LATCH_EXCLUSIVE_BIT, Ordering::AcqRel);
            RawHybridGuard::new(self, GuardState::Exclusive, ver + LATCH_EXCLUSIVE_BIT)
        } else {
            RawHybridGuard::new(self, GuardState::Optimistic, ver)
        }
    }

    /// Returns a raw guard using the requested optimistic fallback strategy.
    #[inline]
    pub(crate) async fn optimistic_fallback_raw(&self, mode: LatchFallbackMode) -> RawHybridGuard {
        match mode {
            LatchFallbackMode::Spin => self.optimistic_spin_raw(),
            LatchFallbackMode::Shared => self.optimistic_or_shared_raw().await,
            LatchFallbackMode::Exclusive => self.optimistic_or_exclusive_raw().await,
        }
    }

    /// Get a write lock in async way.
    #[inline]
    pub(crate) async fn exclusive_async(&self) -> HybridGuard<'_> {
        HybridGuard::from_raw(self.exclusive_async_raw().await)
    }

    /// Acquires and returns a raw exclusive guard asynchronously.
    #[inline]
    pub(crate) async fn exclusive_async_raw(&self) -> RawHybridGuard {
        self.lock.lock_exclusive_async().await;
        let ver = self
            .version
            .fetch_add(LATCH_EXCLUSIVE_BIT, Ordering::AcqRel);
        RawHybridGuard::new(self, GuardState::Exclusive, ver + LATCH_EXCLUSIVE_BIT)
    }

    /// Acquires and returns a raw shared guard asynchronously.
    #[inline]
    pub(crate) async fn shared_async_raw(&self) -> RawHybridGuard {
        self.lock.lock_shared_async().await;
        let ver = self.version_acq();
        RawHybridGuard::new(self, GuardState::Shared, ver)
    }

    /// Attempts to acquire a raw exclusive guard without blocking.
    #[inline]
    pub(crate) fn try_exclusive_raw(&self) -> Option<RawHybridGuard> {
        if self.lock.try_lock_exclusive() {
            let ver = self
                .version
                .fetch_add(LATCH_EXCLUSIVE_BIT, Ordering::AcqRel);
            return Some(RawHybridGuard::new(
                self,
                GuardState::Exclusive,
                ver + LATCH_EXCLUSIVE_BIT,
            ));
        }
        None
    }

    /// Attempts to acquire a raw shared guard without blocking.
    #[inline]
    pub(crate) fn try_shared_raw(&self) -> Option<RawHybridGuard> {
        if self.lock.try_lock_shared() {
            let ver = self.version_acq();
            return Some(RawHybridGuard::new(self, GuardState::Shared, ver));
        }
        None
    }
}

/// Acquisition mode currently held by a hybrid latch guard.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub(crate) enum GuardState {
    Optimistic,
    Shared,
    Exclusive,
}

/// Raw detached latch state shared by borrowed latch guards and retained page guards.
///
/// This type is crate-private on purpose: it stores the latch pointer, guard
/// state, and version, but it is not itself a public lifetime proof that the
/// pointed-to latch remains valid.
pub(crate) struct RawHybridGuard {
    lock: NonNull<HybridLatch>,
    state: GuardState,
    version: u64,
}

impl RawHybridGuard {
    #[inline]
    fn new(lock: &HybridLatch, state: GuardState, version: u64) -> Self {
        Self {
            lock: NonNull::from(lock),
            state,
            version,
        }
    }

    /// Returns the current guard state.
    #[inline]
    pub(crate) fn state(&self) -> GuardState {
        self.state
    }

    #[inline]
    fn lock_ref(&self) -> &HybridLatch {
        // SAFETY: callers construct `RawHybridGuard` from a live latch, and
        // retained page guards declare the detached raw guard before the retained
        // arena keepalive, so latch unlock-on-drop runs before arena
        // memory can be reclaimed.
        unsafe { self.lock.as_ref() }
    }

    /// Returns whether the guard version still matches the latch version.
    #[inline]
    pub(crate) fn validate(&self) -> bool {
        self.lock_ref().version_match(self.version)
    }

    /// Releases pessimistic ownership and downgrades this guard to optimistic mode.
    #[inline]
    pub(crate) fn downgrade(mut self) -> Self {
        match self.state {
            GuardState::Exclusive => {
                let ver = self.version + LATCH_EXCLUSIVE_BIT;
                self.lock_ref().version.store(ver, Ordering::Release);
                self.unlock_exclusive_raw();
                self.version = ver;
                self.state = GuardState::Optimistic;
                self
            }
            GuardState::Shared => {
                self.unlock_shared_raw();
                self.state = GuardState::Optimistic;
                self
            }
            GuardState::Optimistic => self,
        }
    }

    /// Converts an exclusive guard into a shared guard.
    #[inline]
    pub(crate) fn downgrade_exclusive_to_shared(mut self) -> Self {
        debug_assert!(self.state == GuardState::Exclusive);
        let ver = self.version + LATCH_EXCLUSIVE_BIT;
        self.lock_ref().version.store(ver, Ordering::Release);
        self.downgrade_exclusive_raw();
        self.version = ver;
        self.state = GuardState::Shared;
        self
    }

    /// Attempts to validate and promote this guard to shared mode without blocking.
    #[inline]
    pub(crate) fn try_shared(&mut self) -> Validation<()> {
        match self.state {
            GuardState::Optimistic => {
                if let Some(g) = self.lock_ref().try_shared_raw() {
                    if self.lock_ref().version_match(self.version) {
                        *self = g;
                        return Valid(());
                    }
                    debug_assert!(self.version != g.version);
                }
                Invalid
            }
            GuardState::Shared => Valid(()),
            GuardState::Exclusive => panic!("try shared on exclusive lock is not allowed"),
        }
    }

    /// Asynchronously validates and promotes this guard to shared mode.
    #[inline]
    pub(crate) async fn verify_shared_async<const PRE_VERIFY: bool>(&mut self) -> Validation<()> {
        match self.state {
            GuardState::Optimistic => {
                if PRE_VERIFY && !self.lock_ref().version_match(self.version) {
                    return Invalid;
                }
                let g = self.lock_ref().shared_async_raw().await;
                if !self.lock_ref().version_match(self.version) {
                    return Invalid;
                }
                *self = g;
                Valid(())
            }
            GuardState::Shared => Valid(()),
            GuardState::Exclusive => panic!("verify shared async on exclusive lock is not allowed"),
        }
    }

    /// Acquires shared ownership for an optimistic guard.
    #[inline]
    pub(crate) async fn shared_async(self) -> Self {
        debug_assert!(self.state == GuardState::Optimistic);
        self.lock_ref().shared_async_raw().await
    }

    /// Attempts to validate and promote this guard to exclusive mode without blocking.
    #[inline]
    pub(crate) fn try_exclusive(&mut self) -> Validation<()> {
        match self.state {
            GuardState::Optimistic => {
                if let Some(g) = self.lock_ref().try_exclusive_raw() {
                    if self
                        .lock_ref()
                        .version_match(self.version + LATCH_EXCLUSIVE_BIT)
                    {
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

    /// Asynchronously validates and promotes this guard to exclusive mode.
    #[inline]
    pub(crate) async fn verify_exclusive_async<const PRE_VERIFY: bool>(
        &mut self,
    ) -> Validation<()> {
        match self.state {
            GuardState::Optimistic => {
                if PRE_VERIFY && !self.lock_ref().version_match(self.version) {
                    return Invalid;
                }
                let g = self.lock_ref().exclusive_async_raw().await;
                if !self
                    .lock_ref()
                    .version_match(self.version + LATCH_EXCLUSIVE_BIT)
                {
                    g.rollback_exclusive_bit();
                    return Invalid;
                }
                *self = g;
                Valid(())
            }
            GuardState::Shared => panic!("verify exclusive async on shared lock is not allowed"),
            GuardState::Exclusive => Valid(()),
        }
    }

    /// Rolls back an exclusive acquisition and releases the exclusive raw lock.
    #[inline]
    pub(crate) fn rollback_exclusive_bit(mut self) {
        assert!(
            self.state == GuardState::Exclusive,
            "rollback_exclusive_bit requires exclusive guard"
        );
        self.lock_ref()
            .version
            .fetch_sub(LATCH_EXCLUSIVE_BIT, Ordering::AcqRel);
        self.unlock_exclusive_raw();
        self.state = GuardState::Optimistic;
    }

    /// Releases a shared raw lock and leaves this guard in optimistic mode.
    #[inline]
    pub(crate) fn rollback_shared_lock_in_place(&mut self) {
        assert!(
            self.state == GuardState::Shared,
            "rollback_shared_lock_in_place requires shared guard"
        );
        self.unlock_shared_raw();
        self.state = GuardState::Optimistic;
    }

    /// Rolls back the exclusive bit in place and leaves this guard in optimistic mode.
    #[inline]
    pub(crate) fn rollback_exclusive_bit_in_place(&mut self) {
        assert!(
            self.state == GuardState::Exclusive,
            "rollback_exclusive_bit_in_place requires exclusive guard"
        );
        self.lock_ref()
            .version
            .fetch_sub(LATCH_EXCLUSIVE_BIT, Ordering::AcqRel);
        self.unlock_exclusive_raw();
        self.version -= LATCH_EXCLUSIVE_BIT;
        self.state = GuardState::Optimistic;
    }

    /// Acquires exclusive ownership for an optimistic guard.
    #[inline]
    pub(crate) async fn exclusive_async(self) -> Self {
        debug_assert!(self.state == GuardState::Optimistic);
        self.lock_ref().exclusive_async_raw().await
    }

    /// Refreshes an optimistic guard with the latch's current version.
    #[inline]
    pub(crate) fn refresh_version(&mut self) {
        debug_assert!(self.state == GuardState::Optimistic);
        self.version = self.lock_ref().version_acq();
    }

    #[inline]
    fn unlock_exclusive_raw(&self) {
        // SAFETY: callers only invoke this helper when the guard currently owns
        // one exclusive raw lock acquisition.
        unsafe {
            self.lock_ref().lock.unlock_exclusive();
        }
    }

    #[inline]
    fn unlock_shared_raw(&self) {
        // SAFETY: callers only invoke this helper when the guard currently owns
        // one shared raw lock acquisition.
        unsafe {
            self.lock_ref().lock.unlock_shared();
        }
    }

    #[inline]
    fn downgrade_exclusive_raw(&self) {
        // SAFETY: callers only invoke this helper when the guard is in exclusive state.
        unsafe {
            self.lock_ref().lock.downgrade();
        }
    }
}

impl Drop for RawHybridGuard {
    #[inline]
    fn drop(&mut self) {
        match self.state {
            GuardState::Exclusive => {
                let ver = self.version + LATCH_EXCLUSIVE_BIT;
                self.lock_ref().version.store(ver, Ordering::Release);
                self.unlock_exclusive_raw();
            }
            GuardState::Shared => self.unlock_shared_raw(),
            GuardState::Optimistic => (),
        }
    }
}

// SAFETY: the raw detached guard only carries a stable latch pointer plus the
// owned latch state/version, and drop preserves the required unlock protocol.
unsafe impl Send for RawHybridGuard {}
// SAFETY: sharing references to the raw detached guard does not duplicate ownership of
// the latch state; mutation still requires `&mut self`.
unsafe impl Sync for RawHybridGuard {}

/// Borrowed RAII guard returned by non-buffer [`HybridLatch`] APIs.
///
/// Buffer page guards use [`RawHybridGuard`] directly so they can carry a pool
/// keepalive beside the detached latch state.
pub(crate) struct HybridGuard<'a> {
    raw: RawHybridGuard,
    _marker: PhantomData<&'a HybridLatch>,
}

impl<'a> HybridGuard<'a> {
    #[inline]
    fn from_raw(raw: RawHybridGuard) -> Self {
        HybridGuard {
            raw,
            _marker: PhantomData,
        }
    }

    /// Validate version is not changed.
    #[inline]
    pub(crate) fn validate(&self) -> bool {
        self.raw.validate()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_hybrid_lock() {
        smol::block_on(async {
            let boxed = Box::new(HybridLatch::new());
            let latch: &'static mut HybridLatch = Box::leak(boxed);
            assert!(!latch.is_exclusive_latched());
            let ver = latch.version_acq();
            assert!(latch.version_match(ver));
            // optimistic guard
            let opt_g1 = latch.optimistic_spin();
            assert!(opt_g1.validate());
            drop(opt_g1);
            let read = latch.optimistic_read(|| 123usize);
            assert_eq!(read, 123);
            // optimistic or shared
            let opt_g2 = latch
                .optimistic_fallback_raw(LatchFallbackMode::Shared)
                .await;
            assert!(opt_g2.validate());
            drop(opt_g2);
            let opt_g3 = latch
                .optimistic_fallback_raw(LatchFallbackMode::Exclusive)
                .await;
            assert!(opt_g3.validate());
            drop(opt_g3);
            let shared_g1 = latch.shared_async_raw().await;
            assert_eq!(shared_g1.state(), GuardState::Shared);
            drop(shared_g1);
            let shared_g2 = latch.try_shared_raw().unwrap();
            assert_eq!(shared_g2.state(), GuardState::Shared);
            drop(shared_g2);
            let exclusive_g1 = latch.exclusive_async().await;
            assert!(latch.is_exclusive_latched());
            let ver2 = latch.version_acq();
            assert!(ver2 == ver + 1);
            drop(exclusive_g1);
            let ver3 = latch.version_acq();
            assert!(ver3 == ver2 + 1);
            let exclusive_g2 = latch.try_exclusive_raw().unwrap();
            assert_eq!(exclusive_g2.state(), GuardState::Exclusive);
            drop(exclusive_g2);
        })
    }
}
