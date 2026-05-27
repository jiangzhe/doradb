use crate::buffer::PoolGuard;
use crate::buffer::frame::{BufferFrame, FrameContext};
use crate::buffer::page::{PAGE_SIZE, PageID, VersionedPageID};
use crate::error::{
    Validation,
    Validation::{Invalid, Valid},
};
use crate::latch::{GuardState, LatchFallbackMode, RawHybridGuard};
use crate::ptr::UnsafePtr;
use either::Either;
use std::future::Future;
use std::marker::PhantomData;
use std::mem;

pub(crate) trait PageGuard<T: 'static> {
    fn page(&self) -> &T;
}

pub(crate) trait LockStrategy {
    type Page;
    type Guard;

    const MODE: LatchFallbackMode;

    fn try_lock(guard: &mut FacadePageGuard<Self::Page>) -> Validation<()>;

    fn must_locked(guard: FacadePageGuard<Self::Page>) -> Self::Guard;

    fn verify_lock_async<const PRE_VERIFY: bool>(
        guard: FacadePageGuard<Self::Page>,
    ) -> impl Future<Output = Validation<Self::Guard>>;
}

pub(crate) struct SharedLockStrategy<T: 'static> {
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

pub(crate) struct OptimisticLockStrategy<T: 'static> {
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

pub(crate) struct ExclusiveLockStrategy<T: 'static> {
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
    // SAFETY: field order is part of the contract. `raw` must drop before
    // `keepalive` so unlock runs while pool provenance and quiescent liveness
    // are still held. Do not reorder these fields. When destructuring `Self`,
    // also bind `keepalive` before `raw` so reverse local-drop order preserves
    // the same guarantee across async suspension or future refactors.
    raw: RawHybridGuard,
    keepalive: PoolGuard,
}

impl PageLatchGuard {
    #[inline]
    pub(crate) fn new(keepalive: PoolGuard, raw: RawHybridGuard) -> Self {
        Self { raw, keepalive }
    }

    #[inline]
    fn state(&self) -> GuardState {
        self.raw.state()
    }

    #[inline]
    fn validate(&self) -> bool {
        self.raw.validate()
    }

    #[inline]
    fn downgrade(self) -> Self {
        let Self { keepalive, raw } = self;
        Self {
            raw: raw.downgrade(),
            keepalive,
        }
    }

    #[inline]
    fn downgrade_exclusive_to_shared(self) -> Self {
        let Self { keepalive, raw } = self;
        Self {
            raw: raw.downgrade_exclusive_to_shared(),
            keepalive,
        }
    }

    #[inline]
    fn try_shared(&mut self) -> Validation<()> {
        self.raw.try_shared()
    }

    #[inline]
    async fn verify_shared_async<const PRE_VERIFY: bool>(&mut self) -> Validation<()> {
        self.raw.verify_shared_async::<PRE_VERIFY>().await
    }

    #[inline]
    async fn shared_async(self) -> Self {
        // Keep `keepalive` bound before `raw` so reverse local-drop order still
        // drops the raw latch state first if this future is cancelled.
        let Self { keepalive, raw } = self;
        Self {
            raw: raw.shared_async().await,
            keepalive,
        }
    }

    #[inline]
    fn try_exclusive(&mut self) -> Validation<()> {
        self.raw.try_exclusive()
    }

    #[inline]
    async fn verify_exclusive_async<const PRE_VERIFY: bool>(&mut self) -> Validation<()> {
        self.raw.verify_exclusive_async::<PRE_VERIFY>().await
    }

    #[inline]
    fn rollback_exclusive_bit(self) {
        self.raw.rollback_exclusive_bit();
    }

    #[inline]
    fn rollback_shared_lock_in_place(&mut self) {
        self.raw.rollback_shared_lock_in_place();
    }

    #[inline]
    fn rollback_exclusive_bit_in_place(&mut self) {
        self.raw.rollback_exclusive_bit_in_place();
    }

    #[inline]
    async fn exclusive_async(self) -> Self {
        // Keep `keepalive` bound before `raw` so reverse local-drop order still
        // drops the raw latch state first if this future is cancelled.
        let Self { keepalive, raw } = self;
        Self {
            raw: raw.exclusive_async().await,
            keepalive,
        }
    }

    #[inline]
    fn refresh_version(&mut self) {
        self.raw.refresh_version();
    }
}

pub(crate) struct FacadePageGuard<T: 'static> {
    raw: PageLatchGuard,
    bf: UnsafePtr<BufferFrame>,
    captured_generation: u64,
    _marker: PhantomData<T>,
}

impl<T: 'static> FacadePageGuard<T> {
    #[inline]
    pub(crate) fn new(raw: PageLatchGuard, bf: UnsafePtr<BufferFrame>) -> Self {
        let captured_generation = frame_ref(&bf).generation();
        FacadePageGuard {
            raw,
            bf,
            captured_generation,
            _marker: PhantomData,
        }
    }

    #[inline]
    #[cfg_attr(
        not(test),
        expect(dead_code, reason = "test-only facade page identity")
    )]
    pub(crate) fn page_id(&self) -> PageID {
        self.bf().page_id
    }

    #[inline]
    pub(crate) fn bf(&self) -> &BufferFrame {
        frame_ref(&self.bf)
    }

    #[inline]
    fn generation_matches(&self) -> bool {
        generation_matches(&self.bf, self.captured_generation)
    }

    #[inline]
    fn into_shared_guard(self) -> PageSharedGuard<T> {
        debug_assert!(self.is_shared());
        debug_assert!(self.generation_matches());
        PageSharedGuard {
            raw: self.raw,
            bf: self.bf,
            captured_generation: self.captured_generation,
            _marker: PhantomData,
        }
    }

    #[inline]
    fn into_exclusive_guard(self) -> PageExclusiveGuard<T> {
        debug_assert!(self.is_exclusive());
        debug_assert!(self.generation_matches());
        PageExclusiveGuard {
            raw: self.raw,
            bf: self.bf,
            captured_generation: self.captured_generation,
            _marker: PhantomData,
        }
    }

    #[inline]
    pub(crate) fn try_exclusive(&mut self) -> Validation<()> {
        let was_optimistic = self.raw.state() == GuardState::Optimistic;
        match self.raw.try_exclusive() {
            Valid(()) => {
                if self.generation_matches() {
                    Valid(())
                } else {
                    if was_optimistic {
                        self.raw.rollback_exclusive_bit_in_place();
                    }
                    Invalid
                }
            }
            Invalid => {
                self.raw.refresh_version();
                Invalid
            }
        }
    }

    #[inline]
    pub(crate) async fn verify_exclusive_async<const PRE_VERIFY: bool>(
        mut self,
    ) -> Validation<PageExclusiveGuard<T>> {
        let was_optimistic = self.raw.state() == GuardState::Optimistic;
        let res = self.raw.verify_exclusive_async::<PRE_VERIFY>().await;
        verify!(res);
        if !self.generation_matches() {
            if was_optimistic {
                self.raw.rollback_exclusive_bit_in_place();
            }
            return Invalid;
        }
        Valid(self.must_exclusive())
    }

    #[inline]
    pub(crate) fn must_exclusive(self) -> PageExclusiveGuard<T> {
        self.into_exclusive_guard()
    }

    #[inline]
    pub(crate) async fn lock_exclusive_async(self) -> Option<PageExclusiveGuard<T>> {
        match self.raw.state() {
            GuardState::Optimistic => self.downgrade().lock_exclusive_async().await,
            GuardState::Shared => panic!("block until exclusive by shared lock is not allowed"),
            GuardState::Exclusive => {
                if !self.generation_matches() {
                    return None;
                }
                Some(self.into_exclusive_guard())
            }
        }
    }

    #[inline]
    pub(crate) fn try_shared_either(
        mut self,
    ) -> Validation<Either<PageSharedGuard<T>, PageOptimisticGuard<T>>> {
        let was_optimistic = self.raw.state() == GuardState::Optimistic;
        match self.raw.try_shared() {
            Valid(()) => {
                if !self.generation_matches() {
                    if was_optimistic {
                        self.raw.rollback_shared_lock_in_place();
                    }
                    return Invalid;
                }
                Valid(Either::Left(self.into_shared_guard()))
            }
            Invalid => {
                self.raw.refresh_version();
                Valid(Either::Right(PageOptimisticGuard {
                    raw: self.raw,
                    bf: self.bf,
                    captured_generation: self.captured_generation,
                    _marker: PhantomData,
                }))
            }
        }
    }

    #[inline]
    pub(crate) fn try_shared(&mut self) -> Validation<()> {
        let was_optimistic = self.raw.state() == GuardState::Optimistic;
        match self.raw.try_shared() {
            Valid(()) => {
                if self.generation_matches() {
                    Valid(())
                } else {
                    if was_optimistic {
                        self.raw.rollback_shared_lock_in_place();
                    }
                    Invalid
                }
            }
            Invalid => {
                self.raw.refresh_version();
                Invalid
            }
        }
    }

    #[inline]
    pub(crate) async fn verify_shared_async<const PRE_VERIFY: bool>(
        mut self,
    ) -> Validation<PageSharedGuard<T>> {
        let was_optimistic = self.raw.state() == GuardState::Optimistic;
        let res = self.raw.verify_shared_async::<PRE_VERIFY>().await;
        verify!(res);
        if !self.generation_matches() {
            if was_optimistic {
                self.raw.rollback_shared_lock_in_place();
            }
            return Invalid;
        }
        Valid(self.must_shared())
    }

    #[inline]
    pub(crate) fn must_shared(self) -> PageSharedGuard<T> {
        self.into_shared_guard()
    }

    #[inline]
    pub(crate) async fn lock_shared_async(self) -> Option<PageSharedGuard<T>> {
        match self.raw.state() {
            GuardState::Optimistic => self.downgrade().lock_shared_async().await,
            GuardState::Shared => {
                if !self.generation_matches() {
                    return None;
                }
                Some(self.into_shared_guard())
            }
            GuardState::Exclusive => panic!("block until exclusive by shared lock is not allowed"),
        }
    }

    #[inline]
    #[cfg_attr(not(test), expect(dead_code, reason = "reserved try_into_shared"))]
    pub(crate) fn try_into_shared(self) -> Option<PageSharedGuard<T>> {
        if self.raw.state() != GuardState::Shared {
            return None;
        }
        if !self.generation_matches() {
            return None;
        }
        Some(self.into_shared_guard())
    }

    #[inline]
    #[cfg_attr(not(test), expect(dead_code, reason = "reserved try_into_exclusive"))]
    pub(crate) fn try_into_exclusive(self) -> Option<PageExclusiveGuard<T>> {
        if self.raw.state() != GuardState::Exclusive {
            return None;
        }
        if !self.generation_matches() {
            return None;
        }
        Some(self.into_exclusive_guard())
    }

    #[inline]
    pub(crate) fn downgrade(self) -> PageOptimisticGuard<T> {
        PageOptimisticGuard {
            raw: self.raw.downgrade(),
            bf: self.bf,
            captured_generation: self.captured_generation,
            _marker: PhantomData,
        }
    }

    #[inline]
    pub(crate) fn is_exclusive(&self) -> bool {
        self.raw.state() == GuardState::Exclusive
    }

    #[inline]
    pub(crate) fn is_shared(&self) -> bool {
        self.raw.state() == GuardState::Shared
    }

    #[inline]
    pub(crate) fn validate(&self) -> Validation<()> {
        if self.validate_bool() {
            Valid(())
        } else {
            Invalid
        }
    }

    #[inline]
    pub(crate) fn validate_bool(&self) -> bool {
        self.raw.validate() && self.generation_matches()
    }

    #[inline]
    pub(crate) fn with_page_ref_validated<R, F>(&self, f: F) -> Validation<R>
    where
        F: for<'a> FnOnce(&'a T) -> R,
    {
        let out = f(page_ref(self.bf()));
        if self.validate_bool() {
            Valid(out)
        } else {
            Invalid
        }
    }

    #[inline]
    pub(crate) fn rollback_exclusive_version_change(self) {
        assert!(
            self.raw.state() == GuardState::Exclusive,
            "rollback_exclusive_version_change requires exclusive guard"
        );
        self.raw.rollback_exclusive_bit();
    }
}

// SAFETY: the guard keeps the frame allocation alive and only exposes shared
// page access unless it owns the exclusive latch bit internally.
unsafe impl<T: Sync + 'static> Send for FacadePageGuard<T> {}
// SAFETY: sharing references to this guard only shares latch-protected frame
// metadata plus shared access to `T`.
unsafe impl<T: Sync + 'static> Sync for FacadePageGuard<T> {}

pub(crate) struct PageOptimisticGuard<T: 'static> {
    raw: PageLatchGuard,
    bf: UnsafePtr<BufferFrame>,
    captured_generation: u64,
    _marker: PhantomData<T>,
}

impl<T> PageOptimisticGuard<T> {
    #[inline]
    #[cfg_attr(
        not(test),
        expect(dead_code, reason = "test-only optimistic page identity")
    )]
    pub(crate) fn page_id(&self) -> PageID {
        frame_ref(&self.bf).page_id
    }

    #[inline]
    fn generation_matches(&self) -> bool {
        generation_matches(&self.bf, self.captured_generation)
    }

    #[inline]
    pub(crate) async fn lock_shared_async(self) -> Option<PageSharedGuard<T>> {
        let raw = self.raw;
        let bf = self.bf;
        let captured_generation = self.captured_generation;
        let raw = raw.shared_async().await;
        if !generation_matches(&bf, captured_generation) {
            return None;
        }
        Some(PageSharedGuard {
            raw,
            bf,
            captured_generation,
            _marker: PhantomData,
        })
    }

    #[inline]
    pub(crate) fn try_exclusive(mut self) -> Validation<PageExclusiveGuard<T>> {
        verify!(self.raw.try_exclusive());
        if !self.generation_matches() {
            self.raw.rollback_exclusive_bit_in_place();
            return Invalid;
        }
        Valid(PageExclusiveGuard {
            raw: self.raw,
            bf: self.bf,
            captured_generation: self.captured_generation,
            _marker: PhantomData,
        })
    }

    #[inline]
    pub(crate) async fn lock_exclusive_async(self) -> Option<PageExclusiveGuard<T>> {
        let raw = self.raw;
        let bf = self.bf;
        let captured_generation = self.captured_generation;
        let raw = raw.exclusive_async().await;
        if !generation_matches(&bf, captured_generation) {
            raw.rollback_exclusive_bit();
            return None;
        }
        Some(PageExclusiveGuard {
            raw,
            bf,
            captured_generation,
            _marker: PhantomData,
        })
    }

    #[inline]
    #[cfg_attr(not(test), expect(dead_code, reason = "pending dead-code audit"))]
    pub(crate) fn facade(self) -> FacadePageGuard<T> {
        FacadePageGuard {
            raw: self.raw,
            bf: self.bf,
            captured_generation: self.captured_generation,
            _marker: PhantomData,
        }
    }
}

// SAFETY: the guard owns a version-validated retained raw guard plus the
// frame keepalive, and it only exposes shared page access.
unsafe impl<T: Sync + 'static> Send for PageOptimisticGuard<T> {}
// SAFETY: sharing references to the optimistic guard preserves the same
// read-only page access and validated frame lifetime.
unsafe impl<T: Sync + 'static> Sync for PageOptimisticGuard<T> {}

pub(crate) struct PageSharedGuard<T: 'static> {
    raw: PageLatchGuard,
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
    #[cfg_attr(not(test), expect(dead_code, reason = "pending dead-code audit"))]
    pub(crate) fn downgrade(self) -> PageOptimisticGuard<T> {
        PageOptimisticGuard {
            raw: self.raw.downgrade(),
            bf: self.bf,
            captured_generation: self.captured_generation,
            _marker: PhantomData,
        }
    }

    #[inline]
    pub(crate) fn bf(&self) -> &BufferFrame {
        frame_ref(&self.bf)
    }

    #[inline]
    pub(crate) fn page_id(&self) -> PageID {
        self.bf().page_id
    }

    #[inline]
    pub(crate) fn versioned_page_id(&self) -> VersionedPageID {
        VersionedPageID {
            page_id: self.page_id(),
            generation: self.captured_generation,
        }
    }

    #[inline]
    pub(crate) fn ctx_and_page(&self) -> (&FrameContext, &T) {
        let bf = self.bf();
        let undo_map = bf.ctx.as_ref().unwrap();
        let page = page_ref(bf);
        (undo_map, page)
    }

    #[inline]
    pub(crate) fn facade(self, dirty: bool) -> FacadePageGuard<T> {
        let bf = self.bf();
        if dirty && !bf.is_dirty() {
            bf.set_dirty(true);
        }
        FacadePageGuard {
            raw: self.raw,
            bf: self.bf,
            captured_generation: self.captured_generation,
            _marker: PhantomData,
        }
    }

    #[inline]
    pub(crate) fn set_dirty(self) {
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

pub(crate) struct PageExclusiveGuard<T: 'static> {
    raw: PageLatchGuard,
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
        debug_assert!(self.raw.state() == GuardState::Exclusive);
        // SAFETY: `PageExclusiveGuard` is only constructed from an exclusive
        // retained raw guard, `&mut self` guarantees one mutable borrow at a
        // time, and the retained quiescent guard keeps the arena allocation alive.
        unsafe { frame_mut(self.bf.clone()) }
    }

    #[inline]
    pub(crate) fn downgrade(self) -> PageOptimisticGuard<T> {
        PageOptimisticGuard {
            raw: self.raw.downgrade(),
            bf: self.bf,
            captured_generation: self.captured_generation,
            _marker: PhantomData,
        }
    }

    #[inline]
    pub(crate) fn downgrade_shared(self) -> PageSharedGuard<T> {
        PageSharedGuard {
            raw: self.raw.downgrade_exclusive_to_shared(),
            bf: self.bf,
            captured_generation: self.captured_generation,
            _marker: PhantomData,
        }
    }

    #[inline]
    pub(crate) fn page_id(&self) -> PageID {
        self.bf().page_id
    }

    #[inline]
    #[cfg_attr(not(test), expect(dead_code, reason = "pending dead-code audit"))]
    pub(crate) fn versioned_page_id(&self) -> VersionedPageID {
        VersionedPageID {
            page_id: self.page_id(),
            generation: self.captured_generation,
        }
    }

    #[inline]
    pub(crate) fn page_mut(&mut self) -> &mut T {
        page_mut(self.frame_mut())
    }

    #[inline]
    pub(crate) fn bf(&self) -> &BufferFrame {
        frame_ref(&self.bf)
    }

    #[inline]
    pub(crate) fn bf_mut(&mut self) -> &mut BufferFrame {
        self.frame_mut()
    }

    #[inline]
    pub(crate) fn ctx_and_page_mut(&mut self) -> (&mut FrameContext, &mut T) {
        let bf = self.frame_mut();
        debug_assert_page_cast::<T>(bf);
        let ctx = bf.ctx.as_mut().unwrap().as_mut();
        // SAFETY: the exclusive page guard owns the latch exclusively, the
        // retained pool guard keeps the page allocation alive, and callers only
        // construct typed guards after validating the frame's logical page kind.
        let page = unsafe { &mut *(bf.page.cast::<T>()) };
        (ctx, page)
    }

    #[inline]
    #[cfg_attr(not(test), expect(dead_code, reason = "pending dead-code audit"))]
    pub(crate) fn facade(self, dirty: bool) -> FacadePageGuard<T> {
        let bf = self.bf();
        if dirty && !bf.is_dirty() {
            bf.set_dirty(true);
        }
        FacadePageGuard {
            raw: self.raw,
            bf: self.bf,
            captured_generation: self.captured_generation,
            _marker: PhantomData,
        }
    }

    #[inline]
    pub(crate) fn set_dirty(self) {
        let bf = self.bf();
        if !bf.is_dirty() {
            bf.set_dirty(true);
        }
    }

    #[inline]
    pub(crate) fn is_dirty(&self) -> bool {
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
    debug_assert_page_cast::<T>(bf);
    // SAFETY: the owning guard keeps the arena allocation alive, typed
    // foreground access validates the frame's logical page kind before guard
    // construction, and internal raw-page IO guards only request `Page` while
    // holding the frame latch according to their access mode.
    unsafe { &*(bf.page.cast::<T>()) }
}

#[inline]
fn page_mut<T>(bf: &mut BufferFrame) -> &mut T {
    debug_assert_page_cast::<T>(bf);
    // SAFETY: the owning guard keeps the arena allocation alive, the exclusive
    // latch guarantees uniqueness for mutable access, typed foreground access
    // validates the frame's logical page kind before guard construction, and
    // internal raw-page IO guards only request `Page` under exclusive ownership.
    unsafe { &mut *(bf.page.cast::<T>()) }
}

#[inline]
fn debug_assert_page_cast<T>(bf: &BufferFrame) {
    debug_assert!(!bf.page.is_null(), "buffer frame page pointer is null");
    debug_assert_eq!(
        mem::size_of::<T>(),
        PAGE_SIZE,
        "buffer guard page casts require page-sized T"
    );
    debug_assert_eq!(
        (bf.page as usize) % mem::align_of::<T>(),
        0,
        "buffer frame page pointer is not aligned for T"
    );
}

#[inline]
fn frame_ref(bf: &UnsafePtr<BufferFrame>) -> &BufferFrame {
    // SAFETY: the owning page guard retains a quiescent keepalive proving the
    // frame allocation remains live for the full guard lifetime.
    unsafe { &*bf.0 }
}

#[inline]
fn generation_matches(bf: &UnsafePtr<BufferFrame>, captured_generation: u64) -> bool {
    // A stable latch version only proves the current frame contents were not
    // concurrently modified. The frame generation also has to match so an old
    // optimistic guard cannot be upgraded after the slot is reused.
    frame_ref(bf).generation() == captured_generation
}

#[inline]
unsafe fn frame_mut<'a>(bf: UnsafePtr<BufferFrame>) -> &'a mut BufferFrame {
    // SAFETY: callers only use this from exclusive page guards, which own the
    // latch exclusively and retain the arena keepalive for the full borrow.
    unsafe { &mut *bf.0 }
}
