use crate::buffer::PoolGuard;
use crate::buffer::frame::{BufferFrame, FrameContext};
use crate::buffer::page::{PageID, VersionedPageID};
use crate::error::{
    Validation,
    Validation::{Invalid, Valid},
};
use crate::latch::{GuardState, HybridGuardCore, LatchFallbackMode};
use crate::ptr::UnsafePtr;
use either::Either;
use std::future::Future;
use std::marker::PhantomData;

pub trait PageGuard<T: 'static> {
    fn page(&self) -> &T;
}

pub trait LockStrategy {
    type Page;
    type Guard;

    const MODE: LatchFallbackMode;

    fn try_lock(guard: &mut FacadePageGuard<Self::Page>) -> Validation<()>;

    fn must_locked(guard: FacadePageGuard<Self::Page>) -> Self::Guard;

    fn verify_lock_async<const PRE_VERIFY: bool>(
        guard: FacadePageGuard<Self::Page>,
    ) -> impl Future<Output = Validation<Self::Guard>>;
}

pub struct SharedLockStrategy<T: 'static> {
    _marker: PhantomData<T>,
}

impl<T: 'static> LockStrategy for SharedLockStrategy<T> {
    type Page = T;
    type Guard = PageSharedGuard<T>;

    const MODE: LatchFallbackMode = LatchFallbackMode::Shared;

    #[inline]
    fn try_lock(guard: &mut FacadePageGuard<T>) -> Validation<()> {
        guard.try_shared()
    }

    #[inline]
    fn must_locked(guard: FacadePageGuard<T>) -> Self::Guard {
        guard.must_shared()
    }

    #[inline]
    fn verify_lock_async<const PRE_VERIFY: bool>(
        guard: FacadePageGuard<T>,
    ) -> impl Future<Output = Validation<Self::Guard>> {
        guard.verify_shared_async::<PRE_VERIFY>()
    }
}

pub struct OptimisticLockStrategy<T: 'static> {
    _marker: PhantomData<T>,
}

impl<T: 'static> LockStrategy for OptimisticLockStrategy<T> {
    type Page = T;
    type Guard = FacadePageGuard<T>;

    const MODE: LatchFallbackMode = LatchFallbackMode::Spin;

    #[inline]
    fn try_lock(_guard: &mut FacadePageGuard<T>) -> Validation<()> {
        Valid(())
    }

    #[inline]
    fn must_locked(guard: FacadePageGuard<T>) -> Self::Guard {
        guard
    }

    #[inline]
    fn verify_lock_async<const PRE_VERIFY: bool>(
        guard: FacadePageGuard<T>,
    ) -> impl Future<Output = Validation<Self::Guard>> {
        std::future::ready(Valid(guard))
    }
}

pub struct ExclusiveLockStrategy<T: 'static> {
    _marker: PhantomData<T>,
}

impl<T: 'static> LockStrategy for ExclusiveLockStrategy<T> {
    type Page = T;
    type Guard = PageExclusiveGuard<T>;

    const MODE: LatchFallbackMode = LatchFallbackMode::Exclusive;

    #[inline]
    fn try_lock(guard: &mut FacadePageGuard<T>) -> Validation<()> {
        guard.try_exclusive()
    }

    #[inline]
    fn must_locked(guard: FacadePageGuard<T>) -> Self::Guard {
        guard.must_exclusive()
    }

    #[inline]
    fn verify_lock_async<const PRE_VERIFY: bool>(
        guard: FacadePageGuard<T>,
    ) -> impl Future<Output = Validation<Self::Guard>> {
        guard.verify_exclusive_async::<PRE_VERIFY>()
    }
}

pub(crate) struct PageLatchGuard {
    // Field order is part of the safety contract. Drop the detached latch core
    // before releasing the retained pool keepalive so unlock runs while pool
    // provenance and quiescent liveness are still held.
    core: HybridGuardCore,
    keepalive: PoolGuard,
}

impl PageLatchGuard {
    #[inline]
    pub(crate) fn new(keepalive: PoolGuard, core: HybridGuardCore) -> Self {
        Self { core, keepalive }
    }

    #[inline]
    fn state(&self) -> GuardState {
        self.core.state()
    }

    #[inline]
    fn validate(&self) -> bool {
        self.core.validate()
    }

    #[inline]
    fn downgrade(self) -> Self {
        let Self { core, keepalive } = self;
        Self {
            core: core.downgrade(),
            keepalive,
        }
    }

    #[inline]
    fn downgrade_exclusive_to_shared(self) -> Self {
        let Self { core, keepalive } = self;
        Self {
            core: core.downgrade_exclusive_to_shared(),
            keepalive,
        }
    }

    #[inline]
    fn try_shared(&mut self) -> Validation<()> {
        self.core.try_shared()
    }

    #[inline]
    async fn verify_shared_async<const PRE_VERIFY: bool>(&mut self) -> Validation<()> {
        self.core.verify_shared_async::<PRE_VERIFY>().await
    }

    #[inline]
    async fn shared_async(self) -> Self {
        let Self { core, keepalive } = self;
        Self {
            core: core.shared_async().await,
            keepalive,
        }
    }

    #[inline]
    fn try_exclusive(&mut self) -> Validation<()> {
        self.core.try_exclusive()
    }

    #[inline]
    async fn verify_exclusive_async<const PRE_VERIFY: bool>(&mut self) -> Validation<()> {
        self.core.verify_exclusive_async::<PRE_VERIFY>().await
    }

    #[inline]
    fn rollback_exclusive_bit(self) {
        self.core.rollback_exclusive_bit();
    }

    #[inline]
    async fn exclusive_async(self) -> Self {
        let Self { core, keepalive } = self;
        Self {
            core: core.exclusive_async().await,
            keepalive,
        }
    }

    #[inline]
    fn refresh_version(&mut self) {
        self.core.refresh_version();
    }
}

pub struct FacadePageGuard<T: 'static> {
    latch: PageLatchGuard,
    bf: UnsafePtr<BufferFrame>,
    captured_generation: u64,
    _marker: PhantomData<T>,
}

impl<T: 'static> FacadePageGuard<T> {
    #[inline]
    pub(crate) fn new(latch: PageLatchGuard, bf: UnsafePtr<BufferFrame>) -> Self {
        let captured_generation = frame_ref(&bf).generation();
        FacadePageGuard {
            latch,
            bf,
            captured_generation,
            _marker: PhantomData,
        }
    }

    #[inline]
    pub fn page_id(&self) -> PageID {
        self.bf().page_id
    }

    #[inline]
    pub fn versioned_page_id(&self) -> VersionedPageID {
        VersionedPageID {
            page_id: self.page_id(),
            generation: self.captured_generation,
        }
    }

    #[inline]
    pub fn bf(&self) -> &BufferFrame {
        frame_ref(&self.bf)
    }

    #[inline]
    pub fn try_exclusive_either(mut self) -> Either<PageExclusiveGuard<T>, PageOptimisticGuard<T>> {
        match self.try_exclusive() {
            Valid(()) => Either::Left(PageExclusiveGuard {
                latch: self.latch,
                bf: self.bf,
                captured_generation: self.captured_generation,
                _marker: PhantomData,
            }),
            Invalid => Either::Right(PageOptimisticGuard {
                latch: self.latch,
                bf: self.bf,
                captured_generation: self.captured_generation,
                _marker: PhantomData,
            }),
        }
    }

    #[inline]
    pub fn try_exclusive(&mut self) -> Validation<()> {
        match self.latch.try_exclusive() {
            Valid(()) => Valid(()),
            Invalid => {
                self.latch.refresh_version();
                Invalid
            }
        }
    }

    #[inline]
    pub async fn verify_exclusive_async<const PRE_VERIFY: bool>(
        mut self,
    ) -> Validation<PageExclusiveGuard<T>> {
        let res = self.latch.verify_exclusive_async::<PRE_VERIFY>().await;
        verify!(res);
        Valid(self.must_exclusive())
    }

    #[inline]
    pub fn must_exclusive(self) -> PageExclusiveGuard<T> {
        debug_assert!(self.is_exclusive());
        PageExclusiveGuard {
            latch: self.latch,
            bf: self.bf,
            captured_generation: self.captured_generation,
            _marker: PhantomData,
        }
    }

    #[inline]
    pub async fn lock_exclusive_async(self) -> Option<PageExclusiveGuard<T>> {
        match self.latch.state() {
            GuardState::Optimistic => {
                let latch = self.latch;
                let bf = self.bf;
                let captured_generation = self.captured_generation;
                let latch = latch.exclusive_async().await;
                if frame_ref(&bf).generation() != captured_generation {
                    latch.rollback_exclusive_bit();
                    return None;
                }
                Some(PageExclusiveGuard {
                    latch,
                    bf,
                    captured_generation,
                    _marker: PhantomData,
                })
            }
            GuardState::Shared => panic!("block until exclusive by shared lock is not allowed"),
            GuardState::Exclusive => {
                debug_assert!(self.bf().generation() == self.captured_generation);
                Some(PageExclusiveGuard {
                    latch: self.latch,
                    bf: self.bf,
                    captured_generation: self.captured_generation,
                    _marker: PhantomData,
                })
            }
        }
    }

    #[inline]
    pub fn try_shared_either(mut self) -> Either<PageSharedGuard<T>, PageOptimisticGuard<T>> {
        match self.try_shared() {
            Valid(()) => Either::Left(PageSharedGuard {
                latch: self.latch,
                bf: self.bf,
                captured_generation: self.captured_generation,
                _marker: PhantomData,
            }),
            Invalid => Either::Right(PageOptimisticGuard {
                latch: self.latch,
                bf: self.bf,
                captured_generation: self.captured_generation,
                _marker: PhantomData,
            }),
        }
    }

    #[inline]
    pub fn try_shared(&mut self) -> Validation<()> {
        match self.latch.try_shared() {
            Valid(()) => Valid(()),
            Invalid => {
                self.latch.refresh_version();
                Invalid
            }
        }
    }

    #[inline]
    pub async fn verify_shared_async<const PRE_VERIFY: bool>(
        mut self,
    ) -> Validation<PageSharedGuard<T>> {
        let res = self.latch.verify_shared_async::<PRE_VERIFY>().await;
        verify!(res);
        Valid(self.must_shared())
    }

    #[inline]
    pub fn must_shared(self) -> PageSharedGuard<T> {
        debug_assert!(self.is_shared());
        PageSharedGuard {
            latch: self.latch,
            bf: self.bf,
            captured_generation: self.captured_generation,
            _marker: PhantomData,
        }
    }

    #[inline]
    pub async fn lock_shared_async(self) -> Option<PageSharedGuard<T>> {
        match self.latch.state() {
            GuardState::Optimistic => {
                let latch = self.latch;
                let bf = self.bf;
                let captured_generation = self.captured_generation;
                let latch = latch.shared_async().await;
                if frame_ref(&bf).generation() != captured_generation {
                    return None;
                }
                Some(PageSharedGuard {
                    latch,
                    bf,
                    captured_generation,
                    _marker: PhantomData,
                })
            }
            GuardState::Shared => {
                debug_assert!(self.bf().generation() == self.captured_generation);
                Some(PageSharedGuard {
                    latch: self.latch,
                    bf: self.bf,
                    captured_generation: self.captured_generation,
                    _marker: PhantomData,
                })
            }
            GuardState::Exclusive => panic!("block until exclusive by shared lock is not allowed"),
        }
    }

    #[inline]
    pub fn try_into_shared(self) -> Option<PageSharedGuard<T>> {
        if self.latch.state() != GuardState::Shared {
            return None;
        }
        if self.bf().generation() != self.captured_generation {
            return None;
        }
        Some(PageSharedGuard {
            latch: self.latch,
            bf: self.bf,
            captured_generation: self.captured_generation,
            _marker: PhantomData,
        })
    }

    #[inline]
    pub fn try_into_exclusive(self) -> Option<PageExclusiveGuard<T>> {
        if self.latch.state() != GuardState::Exclusive {
            return None;
        }
        if self.bf().generation() != self.captured_generation {
            return None;
        }
        Some(PageExclusiveGuard {
            latch: self.latch,
            bf: self.bf,
            captured_generation: self.captured_generation,
            _marker: PhantomData,
        })
    }

    #[inline]
    pub fn downgrade(self) -> PageOptimisticGuard<T> {
        PageOptimisticGuard {
            latch: self.latch.downgrade(),
            bf: self.bf,
            captured_generation: self.captured_generation,
            _marker: PhantomData,
        }
    }

    #[inline]
    pub fn is_optimistic(&self) -> bool {
        self.latch.state() == GuardState::Optimistic
    }

    #[inline]
    pub fn is_exclusive(&self) -> bool {
        self.latch.state() == GuardState::Exclusive
    }

    #[inline]
    pub fn is_shared(&self) -> bool {
        self.latch.state() == GuardState::Shared
    }

    #[inline]
    pub fn validate(&self) -> Validation<()> {
        if self.latch.validate() {
            Valid(())
        } else {
            Invalid
        }
    }

    #[inline]
    pub fn validate_bool(&self) -> bool {
        self.latch.validate()
    }

    #[inline]
    pub fn with_page_ref_validated<R, F>(&self, f: F) -> Validation<R>
    where
        F: for<'a> FnOnce(&'a T) -> R,
    {
        let out = f(page_ref(self.bf()));
        if self.latch.validate() {
            Valid(out)
        } else {
            Invalid
        }
    }

    #[inline]
    pub fn rollback_exclusive_version_change(self) {
        assert!(
            self.latch.state() == GuardState::Exclusive,
            "rollback_exclusive_version_change requires exclusive guard"
        );
        self.latch.rollback_exclusive_bit();
    }
}

// SAFETY: the guard keeps the frame allocation alive and only exposes shared
// page access unless it owns the exclusive latch bit internally.
unsafe impl<T: Sync + 'static> Send for FacadePageGuard<T> {}
// SAFETY: sharing references to this guard only shares latch-protected frame
// metadata plus shared access to `T`.
unsafe impl<T: Sync + 'static> Sync for FacadePageGuard<T> {}

pub struct PageOptimisticGuard<T: 'static> {
    latch: PageLatchGuard,
    bf: UnsafePtr<BufferFrame>,
    captured_generation: u64,
    _marker: PhantomData<T>,
}

impl<T> PageOptimisticGuard<T> {
    #[inline]
    pub fn page_id(&self) -> PageID {
        frame_ref(&self.bf).page_id
    }

    #[inline]
    pub fn versioned_page_id(&self) -> VersionedPageID {
        VersionedPageID {
            page_id: self.page_id(),
            generation: self.captured_generation,
        }
    }

    #[inline]
    pub fn try_shared(mut self) -> Validation<PageSharedGuard<T>> {
        self.latch.try_shared().map(|_| PageSharedGuard {
            latch: self.latch,
            bf: self.bf,
            captured_generation: self.captured_generation,
            _marker: PhantomData,
        })
    }

    #[inline]
    pub async fn shared_async(self) -> PageSharedGuard<T> {
        let latch = self.latch;
        let bf = self.bf;
        let captured_generation = self.captured_generation;
        let latch = latch.shared_async().await;
        PageSharedGuard {
            latch,
            bf,
            captured_generation,
            _marker: PhantomData,
        }
    }

    #[inline]
    pub fn try_exclusive(mut self) -> Validation<PageExclusiveGuard<T>> {
        self.latch.try_exclusive().map(|_| PageExclusiveGuard {
            latch: self.latch,
            bf: self.bf,
            captured_generation: self.captured_generation,
            _marker: PhantomData,
        })
    }

    #[inline]
    pub async fn exclusive_async(self) -> PageExclusiveGuard<T> {
        let latch = self.latch;
        let bf = self.bf;
        let captured_generation = self.captured_generation;
        let latch = latch.exclusive_async().await;
        PageExclusiveGuard {
            latch,
            bf,
            captured_generation,
            _marker: PhantomData,
        }
    }

    #[inline]
    pub fn validate(&self) -> Validation<()> {
        if !self.latch.validate() {
            debug_assert!(self.latch.state() == GuardState::Optimistic);
            return Invalid;
        }
        Valid(())
    }

    #[inline]
    pub fn facade(self) -> FacadePageGuard<T> {
        FacadePageGuard {
            latch: self.latch,
            bf: self.bf,
            captured_generation: self.captured_generation,
            _marker: PhantomData,
        }
    }
}

// SAFETY: the guard owns a version-validated retained latch guard plus the
// frame keepalive, and it only exposes shared page access.
unsafe impl<T: Sync + 'static> Send for PageOptimisticGuard<T> {}
// SAFETY: sharing references to the optimistic guard preserves the same
// read-only page access and validated frame lifetime.
unsafe impl<T: Sync + 'static> Sync for PageOptimisticGuard<T> {}

pub struct PageSharedGuard<T: 'static> {
    latch: PageLatchGuard,
    bf: UnsafePtr<BufferFrame>,
    captured_generation: u64,
    _marker: PhantomData<T>,
}

impl<T: 'static> PageGuard<T> for PageSharedGuard<T> {
    #[inline]
    fn page(&self) -> &T {
        page_ref(self.bf())
    }
}

impl<T: 'static> PageSharedGuard<T> {
    #[inline]
    pub fn downgrade(self) -> PageOptimisticGuard<T> {
        PageOptimisticGuard {
            latch: self.latch.downgrade(),
            bf: self.bf,
            captured_generation: self.captured_generation,
            _marker: PhantomData,
        }
    }

    #[inline]
    pub fn bf(&self) -> &BufferFrame {
        frame_ref(&self.bf)
    }

    #[inline]
    pub fn page_id(&self) -> PageID {
        self.bf().page_id
    }

    #[inline]
    pub fn versioned_page_id(&self) -> VersionedPageID {
        VersionedPageID {
            page_id: self.page_id(),
            generation: self.captured_generation,
        }
    }

    #[inline]
    pub fn ctx_and_page(&self) -> (&FrameContext, &T) {
        let bf = self.bf();
        let undo_map = bf.ctx.as_ref().unwrap();
        let page = page_ref(bf);
        (undo_map, page)
    }

    #[inline]
    pub fn facade(self, dirty: bool) -> FacadePageGuard<T> {
        let bf = self.bf();
        if dirty && !bf.is_dirty() {
            bf.set_dirty(true);
        }
        FacadePageGuard {
            latch: self.latch,
            bf: self.bf,
            captured_generation: self.captured_generation,
            _marker: PhantomData,
        }
    }

    #[inline]
    pub fn set_dirty(self) {
        let bf = self.bf();
        if !bf.is_dirty() {
            bf.set_dirty(true);
        }
    }
}

// SAFETY: the guard owns one shared latch acquisition plus the pool keepalive,
// so moving it between threads preserves the same shared-access contract.
unsafe impl<T: Sync + 'static> Send for PageSharedGuard<T> {}
// SAFETY: sharing references to this guard only shares latch-protected frame
// metadata and shared access to `T`.
unsafe impl<T: Sync + 'static> Sync for PageSharedGuard<T> {}

pub struct PageExclusiveGuard<T: 'static> {
    latch: PageLatchGuard,
    bf: UnsafePtr<BufferFrame>,
    captured_generation: u64,
    _marker: PhantomData<T>,
}

impl<T: 'static> PageGuard<T> for PageExclusiveGuard<T> {
    #[inline]
    fn page(&self) -> &T {
        page_ref(self.bf())
    }
}

impl<T: 'static> PageExclusiveGuard<T> {
    #[inline]
    fn frame_mut(&mut self) -> &mut BufferFrame {
        debug_assert!(self.latch.state() == GuardState::Exclusive);
        // SAFETY: `PageExclusiveGuard` is only constructed from an exclusive
        // retained latch guard, `&mut self` guarantees one mutable borrow at a
        // time, and the retained quiescent guard keeps the arena allocation alive.
        unsafe { frame_mut(self.bf.clone()) }
    }

    #[inline]
    pub fn downgrade(self) -> PageOptimisticGuard<T> {
        PageOptimisticGuard {
            latch: self.latch.downgrade(),
            bf: self.bf,
            captured_generation: self.captured_generation,
            _marker: PhantomData,
        }
    }

    #[inline]
    pub fn downgrade_shared(self) -> PageSharedGuard<T> {
        PageSharedGuard {
            latch: self.latch.downgrade_exclusive_to_shared(),
            bf: self.bf,
            captured_generation: self.captured_generation,
            _marker: PhantomData,
        }
    }

    #[inline]
    pub fn page_id(&self) -> PageID {
        self.bf().page_id
    }

    #[inline]
    pub fn versioned_page_id(&self) -> VersionedPageID {
        VersionedPageID {
            page_id: self.page_id(),
            generation: self.captured_generation,
        }
    }

    #[inline]
    pub fn page_mut(&mut self) -> &mut T {
        page_mut(self.frame_mut())
    }

    #[inline]
    pub fn bf(&self) -> &BufferFrame {
        frame_ref(&self.bf)
    }

    #[inline]
    pub fn bf_mut(&mut self) -> &mut BufferFrame {
        self.frame_mut()
    }

    #[inline]
    pub fn ctx_and_page_mut(&mut self) -> (&mut FrameContext, &mut T) {
        let bf = self.frame_mut();
        let ctx = bf.ctx.as_mut().unwrap().as_mut();
        // SAFETY: the exclusive page guard owns the latch exclusively and the
        // retained pool guard keeps the page allocation alive while this mutable borrow
        // exists.
        let page = unsafe { &mut *(bf.page as *mut T) };
        (ctx, page)
    }

    #[inline]
    pub fn facade(self, dirty: bool) -> FacadePageGuard<T> {
        let bf = self.bf();
        if dirty && !bf.is_dirty() {
            bf.set_dirty(true);
        }
        FacadePageGuard {
            latch: self.latch,
            bf: self.bf,
            captured_generation: self.captured_generation,
            _marker: PhantomData,
        }
    }

    #[inline]
    pub fn set_dirty(self) {
        let bf = self.bf();
        if !bf.is_dirty() {
            bf.set_dirty(true);
        }
    }

    #[inline]
    pub fn is_dirty(&self) -> bool {
        self.bf().is_dirty()
    }
}

// SAFETY: the guard owns the exclusive latch state and the frame keepalive, so
// moving it transfers that unique mutable-access capability to another thread.
unsafe impl<T: Send + 'static> Send for PageExclusiveGuard<T> {}
// SAFETY: shared references to the guard do not duplicate mutable access;
// mutable page access still requires `&mut self` while metadata stays latched.
unsafe impl<T: Sync + 'static> Sync for PageExclusiveGuard<T> {}

#[inline]
fn page_ref<T>(bf: &BufferFrame) -> &T {
    // SAFETY: the owning guard keeps the arena allocation alive and callers
    // choose `T` that matches the page type stored in this frame.
    unsafe { &*(bf.page as *const T) }
}

#[inline]
fn page_mut<T>(bf: &mut BufferFrame) -> &mut T {
    // SAFETY: the owning guard keeps the arena allocation alive, the exclusive
    // latch guarantees uniqueness, and callers choose the correct page type.
    unsafe { &mut *(bf.page as *mut T) }
}

#[inline]
fn frame_ref(bf: &UnsafePtr<BufferFrame>) -> &BufferFrame {
    // SAFETY: the owning page guard retains a quiescent keepalive proving the
    // frame allocation remains live for the full guard lifetime.
    unsafe { &*bf.0 }
}

#[inline]
unsafe fn frame_mut<'a>(bf: UnsafePtr<BufferFrame>) -> &'a mut BufferFrame {
    // SAFETY: callers only use this from exclusive page guards, which own the
    // latch exclusively and retain the arena keepalive for the full borrow.
    unsafe { &mut *bf.0 }
}
