use crate::buffer::arena::ArenaLease;
use crate::buffer::frame::{BufferFrame, FrameContext};
use crate::buffer::page::PageID;
use crate::error::{
    Validation,
    Validation::{Invalid, Valid},
};
use crate::latch::{GuardState, HybridGuardRaw, LatchFallbackMode};
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

pub struct FacadePageGuard<T: 'static> {
    arena: ArenaLease,
    bf: UnsafePtr<BufferFrame>,
    guard: HybridGuardRaw,
    captured_generation: u64,
    _marker: PhantomData<T>,
}

impl<T: 'static> FacadePageGuard<T> {
    #[inline]
    pub(crate) fn new(
        arena: ArenaLease,
        bf: UnsafePtr<BufferFrame>,
        guard: HybridGuardRaw,
    ) -> Self {
        let captured_generation = arena.frame_ref(bf.clone()).generation();
        FacadePageGuard {
            arena,
            bf,
            guard,
            captured_generation,
            _marker: PhantomData,
        }
    }

    #[inline]
    pub fn page_id(&self) -> PageID {
        self.bf().page_id
    }

    #[inline]
    pub fn bf(&self) -> &BufferFrame {
        self.arena.frame_ref(self.bf.clone())
    }

    #[inline]
    pub fn try_exclusive_either(mut self) -> Either<PageExclusiveGuard<T>, PageOptimisticGuard<T>> {
        match self.try_exclusive() {
            Valid(()) => Either::Left(PageExclusiveGuard {
                arena: self.arena,
                bf: self.bf,
                guard: self.guard,
                captured_generation: self.captured_generation,
                _marker: PhantomData,
            }),
            Invalid => Either::Right(PageOptimisticGuard {
                arena: self.arena,
                bf: self.bf,
                guard: self.guard,
                captured_generation: self.captured_generation,
                _marker: PhantomData,
            }),
        }
    }

    #[inline]
    pub fn try_exclusive(&mut self) -> Validation<()> {
        match self.guard.try_exclusive() {
            Valid(()) => Valid(()),
            Invalid => {
                self.guard.refresh_version();
                Invalid
            }
        }
    }

    #[inline]
    pub async fn verify_exclusive_async<const PRE_VERIFY: bool>(
        mut self,
    ) -> Validation<PageExclusiveGuard<T>> {
        let res = self.guard.verify_exclusive_async::<PRE_VERIFY>().await;
        verify!(res);
        Valid(self.must_exclusive())
    }

    #[inline]
    pub fn must_exclusive(self) -> PageExclusiveGuard<T> {
        debug_assert!(self.is_exclusive());
        PageExclusiveGuard {
            arena: self.arena,
            bf: self.bf,
            guard: self.guard,
            captured_generation: self.captured_generation,
            _marker: PhantomData,
        }
    }

    #[inline]
    pub async fn lock_exclusive_async(self) -> Option<PageExclusiveGuard<T>> {
        match self.guard.state {
            GuardState::Optimistic => {
                let arena = self.arena;
                let bf = self.bf;
                let captured_generation = self.captured_generation;
                let guard = self.guard.exclusive_async().await;
                if arena.frame_ref(bf.clone()).generation() != captured_generation {
                    guard.rollback_exclusive_bit();
                    return None;
                }
                Some(PageExclusiveGuard {
                    arena,
                    bf,
                    guard,
                    captured_generation,
                    _marker: PhantomData,
                })
            }
            GuardState::Shared => panic!("block until exclusive by shared lock is not allowed"),
            GuardState::Exclusive => {
                debug_assert!(self.bf().generation() == self.captured_generation);
                Some(PageExclusiveGuard {
                    arena: self.arena,
                    bf: self.bf,
                    guard: self.guard,
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
                arena: self.arena,
                bf: self.bf,
                guard: self.guard,
                captured_generation: self.captured_generation,
                _marker: PhantomData,
            }),
            Invalid => Either::Right(PageOptimisticGuard {
                arena: self.arena,
                bf: self.bf,
                guard: self.guard,
                captured_generation: self.captured_generation,
                _marker: PhantomData,
            }),
        }
    }

    #[inline]
    pub fn try_shared(&mut self) -> Validation<()> {
        match self.guard.try_shared() {
            Valid(()) => Valid(()),
            Invalid => {
                self.guard.refresh_version();
                Invalid
            }
        }
    }

    #[inline]
    pub async fn verify_shared_async<const PRE_VERIFY: bool>(
        mut self,
    ) -> Validation<PageSharedGuard<T>> {
        let res = self.guard.verify_shared_async::<PRE_VERIFY>().await;
        verify!(res);
        Valid(self.must_shared())
    }

    #[inline]
    pub fn must_shared(self) -> PageSharedGuard<T> {
        debug_assert!(self.is_shared());
        PageSharedGuard {
            arena: self.arena,
            bf: self.bf,
            guard: self.guard,
            captured_generation: self.captured_generation,
            _marker: PhantomData,
        }
    }

    #[inline]
    pub async fn lock_shared_async(self) -> Option<PageSharedGuard<T>> {
        match self.guard.state {
            GuardState::Optimistic => {
                let arena = self.arena;
                let bf = self.bf;
                let captured_generation = self.captured_generation;
                let guard = self.guard.shared_async().await;
                if arena.frame_ref(bf.clone()).generation() != captured_generation {
                    return None;
                }
                Some(PageSharedGuard {
                    arena,
                    bf,
                    guard,
                    captured_generation,
                    _marker: PhantomData,
                })
            }
            GuardState::Shared => {
                debug_assert!(self.bf().generation() == self.captured_generation);
                Some(PageSharedGuard {
                    arena: self.arena,
                    bf: self.bf,
                    guard: self.guard,
                    captured_generation: self.captured_generation,
                    _marker: PhantomData,
                })
            }
            GuardState::Exclusive => panic!("block until exclusive by shared lock is not allowed"),
        }
    }

    #[inline]
    pub fn try_into_shared(self) -> Option<PageSharedGuard<T>> {
        if self.guard.state != GuardState::Shared {
            return None;
        }
        if self.bf().generation() != self.captured_generation {
            return None;
        }
        Some(PageSharedGuard {
            arena: self.arena,
            bf: self.bf,
            guard: self.guard,
            captured_generation: self.captured_generation,
            _marker: PhantomData,
        })
    }

    #[inline]
    pub fn try_into_exclusive(self) -> Option<PageExclusiveGuard<T>> {
        if self.guard.state != GuardState::Exclusive {
            return None;
        }
        if self.bf().generation() != self.captured_generation {
            return None;
        }
        Some(PageExclusiveGuard {
            arena: self.arena,
            bf: self.bf,
            guard: self.guard,
            captured_generation: self.captured_generation,
            _marker: PhantomData,
        })
    }

    #[inline]
    pub fn downgrade(self) -> PageOptimisticGuard<T> {
        PageOptimisticGuard {
            arena: self.arena,
            bf: self.bf,
            guard: self.guard.downgrade(),
            captured_generation: self.captured_generation,
            _marker: PhantomData,
        }
    }

    #[inline]
    pub fn is_optimistic(&self) -> bool {
        self.guard.state == GuardState::Optimistic
    }

    #[inline]
    pub fn is_exclusive(&self) -> bool {
        self.guard.state == GuardState::Exclusive
    }

    #[inline]
    pub fn is_shared(&self) -> bool {
        self.guard.state == GuardState::Shared
    }

    #[inline]
    pub fn validate(&self) -> Validation<()> {
        if self.guard.validate() {
            Valid(())
        } else {
            Invalid
        }
    }

    #[inline]
    pub fn validate_bool(&self) -> bool {
        self.guard.validate()
    }

    #[inline]
    pub fn with_page_ref_validated<R, F>(&self, f: F) -> Validation<R>
    where
        F: for<'a> FnOnce(&'a T) -> R,
    {
        let out = f(page_ref(self.bf()));
        if self.guard.validate() {
            Valid(out)
        } else {
            Invalid
        }
    }

    #[inline]
    pub fn rollback_exclusive_version_change(self) {
        assert!(
            self.guard.state == GuardState::Exclusive,
            "rollback_exclusive_version_change requires exclusive guard"
        );
        self.guard.rollback_exclusive_bit();
    }
}

unsafe impl<T: Sync + 'static> Send for FacadePageGuard<T> {}
unsafe impl<T: Sync + 'static> Sync for FacadePageGuard<T> {}

pub struct PageOptimisticGuard<T: 'static> {
    arena: ArenaLease,
    bf: UnsafePtr<BufferFrame>,
    guard: HybridGuardRaw,
    captured_generation: u64,
    _marker: PhantomData<T>,
}

impl<T> PageOptimisticGuard<T> {
    #[inline]
    pub fn page_id(&self) -> PageID {
        self.arena.frame_ref(self.bf.clone()).page_id
    }

    #[inline]
    pub fn try_shared(mut self) -> Validation<PageSharedGuard<T>> {
        self.guard.try_shared().map(|_| PageSharedGuard {
            arena: self.arena,
            bf: self.bf,
            guard: self.guard,
            captured_generation: self.captured_generation,
            _marker: PhantomData,
        })
    }

    #[inline]
    pub async fn shared_async(self) -> PageSharedGuard<T> {
        let arena = self.arena;
        let bf = self.bf;
        let captured_generation = self.captured_generation;
        let guard = self.guard.shared_async().await;
        PageSharedGuard {
            arena,
            bf,
            guard,
            captured_generation,
            _marker: PhantomData,
        }
    }

    #[inline]
    pub fn try_exclusive(mut self) -> Validation<PageExclusiveGuard<T>> {
        self.guard.try_exclusive().map(|_| PageExclusiveGuard {
            arena: self.arena,
            bf: self.bf,
            guard: self.guard,
            captured_generation: self.captured_generation,
            _marker: PhantomData,
        })
    }

    #[inline]
    pub async fn exclusive_async(self) -> PageExclusiveGuard<T> {
        let arena = self.arena;
        let bf = self.bf;
        let captured_generation = self.captured_generation;
        let guard = self.guard.exclusive_async().await;
        PageExclusiveGuard {
            arena,
            bf,
            guard,
            captured_generation,
            _marker: PhantomData,
        }
    }

    #[inline]
    pub fn validate(&self) -> Validation<()> {
        if !self.guard.validate() {
            debug_assert!(self.guard.state == GuardState::Optimistic);
            return Invalid;
        }
        Valid(())
    }

    #[inline]
    pub fn facade(self) -> FacadePageGuard<T> {
        FacadePageGuard {
            arena: self.arena,
            bf: self.bf,
            guard: self.guard,
            captured_generation: self.captured_generation,
            _marker: PhantomData,
        }
    }
}

unsafe impl<T: Sync + 'static> Send for PageOptimisticGuard<T> {}
unsafe impl<T: Sync + 'static> Sync for PageOptimisticGuard<T> {}

pub struct PageSharedGuard<T: 'static> {
    arena: ArenaLease,
    bf: UnsafePtr<BufferFrame>,
    guard: HybridGuardRaw,
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
            arena: self.arena,
            bf: self.bf,
            guard: self.guard.downgrade(),
            captured_generation: self.captured_generation,
            _marker: PhantomData,
        }
    }

    #[inline]
    pub fn bf(&self) -> &BufferFrame {
        self.arena.frame_ref(self.bf.clone())
    }

    #[inline]
    pub fn page_id(&self) -> PageID {
        self.bf().page_id
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
            arena: self.arena,
            bf: self.bf,
            guard: self.guard,
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

unsafe impl<T: Sync + 'static> Send for PageSharedGuard<T> {}
unsafe impl<T: Sync + 'static> Sync for PageSharedGuard<T> {}

pub struct PageExclusiveGuard<T: 'static> {
    arena: ArenaLease,
    bf: UnsafePtr<BufferFrame>,
    guard: HybridGuardRaw,
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
        debug_assert!(self.guard.state == GuardState::Exclusive);
        debug_assert!(self.arena.contains_frame_ptr(self.bf.clone()));
        // SAFETY: `PageExclusiveGuard` is only constructed from an exclusive
        // raw latch guard, `&mut self` guarantees one mutable borrow at a time,
        // and the arena lease keeps the frame allocation alive.
        unsafe { &mut *self.bf.0 }
    }

    #[inline]
    pub fn downgrade(self) -> PageOptimisticGuard<T> {
        PageOptimisticGuard {
            arena: self.arena,
            bf: self.bf,
            guard: self.guard.downgrade(),
            captured_generation: self.captured_generation,
            _marker: PhantomData,
        }
    }

    #[inline]
    pub fn downgrade_shared(self) -> PageSharedGuard<T> {
        PageSharedGuard {
            arena: self.arena,
            bf: self.bf,
            guard: self.guard.downgrade_exclusive_to_shared(),
            captured_generation: self.captured_generation,
            _marker: PhantomData,
        }
    }

    #[inline]
    pub fn page_id(&self) -> PageID {
        self.bf().page_id
    }

    #[inline]
    pub fn page_mut(&mut self) -> &mut T {
        page_mut(self.frame_mut())
    }

    #[inline]
    pub fn bf(&self) -> &BufferFrame {
        self.arena.frame_ref(self.bf.clone())
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
        // arena lease keeps the page allocation alive while this mutable borrow
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
            arena: self.arena,
            bf: self.bf,
            guard: self.guard,
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

unsafe impl<T: Send + 'static> Send for PageExclusiveGuard<T> {}
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
