use crate::buffer::frame::{BufferFrame, FrameContext};
use crate::buffer::page::PageID;
use crate::error::{
    Validation,
    Validation::{Invalid, Valid},
};
use crate::latch::{GuardState, HybridGuard, LatchFallbackMode};
use crate::ptr::UnsafePtr;
use either::Either;
use std::future::Future;
use std::marker::PhantomData;
use std::mem;

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

// With optimistic lock, it's meaningless to verify version
// after locking, becase it's actually not locked.
// That means we need to already verify version after we
// use some value of the protected object.
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

// facade of lock guard, which encapsulate all three lock modes.
pub struct FacadePageGuard<T: 'static> {
    bf: UnsafePtr<BufferFrame>,
    guard: HybridGuard<'static>,
    _marker: PhantomData<&'static T>,
}

impl<T: 'static> FacadePageGuard<T> {
    #[inline]
    pub fn new(bf: UnsafePtr<BufferFrame>, guard: HybridGuard<'static>) -> Self {
        FacadePageGuard {
            bf,
            guard,
            _marker: PhantomData,
        }
    }

    /// Returns id of this page.
    #[inline]
    pub fn page_id(&self) -> PageID {
        unsafe { (*self.bf.0).page_id }
    }

    /// Try exclusive lock will do additional version check after the lock acquisition to ensure
    /// during the optimistic lock and shared lock, there is no change on protected object.
    /// If lock acquisition fails, refresh version of the optimistic lock, so next acquisition
    /// may succeed.
    #[inline]
    pub fn try_exclusive_either(mut self) -> Either<PageExclusiveGuard<T>, PageOptimisticGuard<T>> {
        match self.try_exclusive() {
            Valid(()) => Either::Left(PageExclusiveGuard {
                bf: unsafe { &mut *self.bf.0 },
                guard: self.guard,
                _marker: PhantomData,
            }),
            Invalid => Either::Right(PageOptimisticGuard {
                bf: self.bf,
                guard: self.guard,
                _marker: PhantomData,
            }),
        }
    }

    /// Try exclusive lock and fail if the lock can not be
    /// acquired immediately.
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
            bf: unsafe { &mut *self.bf.0 },
            guard: self.guard,
            _marker: PhantomData,
        }
    }

    #[inline]
    pub async fn exclusive_async(self) -> PageExclusiveGuard<T> {
        match self.guard.state {
            GuardState::Optimistic => {
                let guard = self.guard.exclusive_async().await;
                PageExclusiveGuard {
                    bf: unsafe { &mut *self.bf.0 },
                    guard,
                    _marker: PhantomData,
                }
            }
            GuardState::Shared => panic!("block until exclusive by shared lock is not allowed"),
            GuardState::Exclusive => PageExclusiveGuard {
                bf: unsafe { &mut *self.bf.0 },
                guard: self.guard,
                _marker: PhantomData,
            },
        }
    }

    /// Try shared lock will do additional version check after the lock acquisition to ensure
    /// during the optimistic lock and shared lock, there is no change on protected object.
    /// If lock acquisition fails, refresh version of the optimistic lock, so next acquisition
    /// may succeed.
    #[inline]
    pub fn try_shared_either(mut self) -> Either<PageSharedGuard<T>, PageOptimisticGuard<T>> {
        match self.try_shared() {
            Valid(()) => Either::Left(PageSharedGuard {
                bf: unsafe { mem::transmute::<UnsafePtr<BufferFrame>, &BufferFrame>(self.bf) },
                guard: self.guard,
                _marker: PhantomData,
            }),
            Invalid => Either::Right(PageOptimisticGuard {
                bf: self.bf,
                guard: self.guard,
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
            bf: unsafe { &*self.bf.0 },
            guard: self.guard,
            _marker: PhantomData,
        }
    }

    #[inline]
    pub async fn shared_async(self) -> PageSharedGuard<T> {
        match self.guard.state {
            GuardState::Optimistic => {
                let guard = self.guard.shared_async().await;
                PageSharedGuard {
                    bf: unsafe { &*self.bf.0 },
                    guard,
                    _marker: PhantomData,
                }
            }
            GuardState::Shared => PageSharedGuard {
                bf: unsafe { &*self.bf.0 },
                guard: self.guard,
                _marker: PhantomData,
            },
            GuardState::Exclusive => panic!("block until exclusive by shared lock is not allowed"),
        }
    }

    #[inline]
    pub fn downgrade(self) -> PageOptimisticGuard<T> {
        PageOptimisticGuard {
            bf: self.bf,
            guard: self.guard.downgrade(),
            _marker: PhantomData,
        }
    }

    /// Returns whether the guard is in optimistic mode.
    #[inline]
    pub fn is_optimistic(&self) -> bool {
        self.guard.state == GuardState::Optimistic
    }

    /// Returns whether the guard is in exclusive mode.
    #[inline]
    pub fn is_exclusive(&self) -> bool {
        self.guard.state == GuardState::Exclusive
    }

    /// Returns whether the guard is in shared mode.
    #[inline]
    pub fn is_shared(&self) -> bool {
        self.guard.state == GuardState::Shared
    }

    /// Treat this guard as shared guard.
    ///
    /// # Safety
    ///
    /// Caller must guarantee this guard is shared.
    #[inline]
    pub unsafe fn as_shared(&self) -> &PageSharedGuard<T> {
        unsafe { mem::transmute(self) }
    }

    /// Treat this guard as exclusive guard.
    ///
    /// # Safety
    ///
    /// Caller must guarantee this guard is exclusive.
    #[inline]
    pub unsafe fn as_exclusive(&mut self) -> &mut PageExclusiveGuard<T> {
        unsafe { mem::transmute(self) }
    }

    /// Returns page with optimistic read.
    ///
    /// # Safety
    ///
    /// All values must be validated before use.
    #[inline]
    pub unsafe fn page_unchecked(&self) -> &T {
        unsafe {
            let page = (*self.bf.0).page;
            &*(page as *mut T)
        }
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

    /// Rollback version change by exclusive lock acquisition.
    ///
    /// # Safety
    ///
    /// Caller must guarantee the lock is in exclusive mode.
    #[inline]
    pub unsafe fn rollback_exclusive_version_change(self) {
        unsafe {
            debug_assert!(self.guard.state == GuardState::Exclusive);
            self.guard.rollback_exclusive_bit();
        }
    }
}

unsafe impl<T: 'static> Send for FacadePageGuard<T> {}
unsafe impl<T: 'static> Sync for FacadePageGuard<T> {}

pub struct PageOptimisticGuard<T: 'static> {
    bf: UnsafePtr<BufferFrame>,
    guard: HybridGuard<'static>,
    _marker: PhantomData<&'static T>,
}

impl<T> PageOptimisticGuard<T> {
    #[inline]
    pub fn page_id(&self) -> PageID {
        unsafe { (*self.bf.0).page_id }
    }

    #[inline]
    pub fn try_shared(mut self) -> Validation<PageSharedGuard<T>> {
        self.guard.try_shared().map(|_| PageSharedGuard {
            bf: unsafe { &*self.bf.0 },
            guard: self.guard,
            _marker: PhantomData,
        })
    }

    #[inline]
    pub async fn shared_async(self) -> PageSharedGuard<T> {
        let guard = self.guard.shared_async().await;
        PageSharedGuard {
            bf: unsafe { &*self.bf.0 },
            guard,
            _marker: PhantomData,
        }
    }

    #[inline]
    pub fn try_exclusive(mut self) -> Validation<PageExclusiveGuard<T>> {
        self.guard.try_exclusive().map(|_| PageExclusiveGuard {
            bf: unsafe { &mut *self.bf.0 },
            guard: self.guard,
            _marker: PhantomData,
        })
    }

    #[inline]
    pub async fn exclusive_async(self) -> PageExclusiveGuard<T> {
        let guard = self.guard.exclusive_async().await;
        PageExclusiveGuard {
            bf: unsafe { &mut *self.bf.0 },
            guard,
            _marker: PhantomData,
        }
    }

    /// Returns page with optimistic read.
    ///
    /// # Safety
    ///
    /// All values must be validated before use.
    #[inline]
    pub unsafe fn page_unchecked(&self) -> &T {
        unsafe {
            let page = (*self.bf.0).page;
            &*(page as *mut T)
        }
    }

    /// Validates version not change.
    /// In optimistic mode, this means no other thread change
    /// the protected object inbetween.
    /// In shared/exclusive mode, the validation will always
    /// succeed because no one can change the protected object
    /// (acquire exclusive lock) at the same time.
    #[inline]
    pub fn validate(&self) -> Validation<()> {
        if !self.guard.validate() {
            debug_assert!(self.guard.state == GuardState::Optimistic);
            return Invalid;
        }
        Valid(())
    }

    /// Returns a copy of optimistic guard.
    /// Otherwise fail.
    #[inline]
    pub fn copy_keepalive(&self) -> FacadePageGuard<T> {
        let guard = self.guard.optimistic_clone().expect("copy optimistic lock");
        FacadePageGuard {
            bf: self.bf.clone(),
            guard,
            _marker: PhantomData,
        }
    }

    /// Returns facade guard.
    #[inline]
    pub fn facade(self) -> FacadePageGuard<T> {
        FacadePageGuard {
            bf: self.bf,
            guard: self.guard,
            _marker: PhantomData,
        }
    }
}

unsafe impl<T: 'static> Send for PageOptimisticGuard<T> {}
unsafe impl<T: 'static> Sync for PageOptimisticGuard<T> {}

pub struct PageSharedGuard<T: 'static> {
    bf: &'static BufferFrame,
    guard: HybridGuard<'static>,
    _marker: PhantomData<&'static T>,
}

impl<T: 'static> PageGuard<T> for PageSharedGuard<T> {
    #[inline]
    fn page(&self) -> &T {
        let page = self.bf.page;
        unsafe { &*(page as *mut T) }
    }
}

impl<T: 'static> PageSharedGuard<T> {
    /// Convert a page shared guard to optimistic guard
    /// with long lifetime.
    #[inline]
    pub fn downgrade(self) -> PageOptimisticGuard<T> {
        PageOptimisticGuard {
            bf: unsafe { mem::transmute::<&BufferFrame, UnsafePtr<BufferFrame>>(self.bf) },
            guard: self.guard.downgrade(),
            _marker: PhantomData,
        }
    }

    /// Returns the buffer frame current page associated.
    #[inline]
    pub fn bf(&self) -> &BufferFrame {
        self.bf
    }

    /// Returns current page id.
    #[inline]
    pub fn page_id(&self) -> PageID {
        self.bf.page_id
    }

    #[inline]
    pub fn ctx_and_page(&self) -> (&FrameContext, &T) {
        let bf = self.bf();
        let undo_map = bf.ctx.as_ref().unwrap();
        let page = bf.page;
        let page = unsafe { &*(page as *mut T) };
        (undo_map, page)
    }

    /// Returns facade guard.
    #[inline]
    pub fn facade(self, dirty: bool) -> FacadePageGuard<T> {
        if dirty && !self.bf.is_dirty() {
            self.bf.set_dirty(true);
        }
        FacadePageGuard {
            bf: unsafe { mem::transmute::<&BufferFrame, UnsafePtr<BufferFrame>>(self.bf) },
            guard: self.guard,
            _marker: PhantomData,
        }
    }

    #[inline]
    pub fn set_dirty(self) {
        if !self.bf.is_dirty() {
            self.bf.set_dirty(true);
        }
    }
}

unsafe impl<T: 'static> Send for PageSharedGuard<T> {}
unsafe impl<T: 'static> Sync for PageSharedGuard<T> {}

pub struct PageExclusiveGuard<T: 'static> {
    bf: &'static mut BufferFrame,
    guard: HybridGuard<'static>,
    _marker: PhantomData<&'static mut T>,
}

impl<T: 'static> PageGuard<T> for PageExclusiveGuard<T> {
    #[inline]
    fn page(&self) -> &T {
        let page = self.bf.page;
        unsafe { &*(page as *mut T) }
    }
}

impl<T: 'static> PageExclusiveGuard<T> {
    /// Convert a page exclusive guard to optimistic guard
    /// with long lifetime.
    #[inline]
    pub fn downgrade(self) -> PageOptimisticGuard<T> {
        PageOptimisticGuard {
            bf: unsafe { mem::transmute::<&mut BufferFrame, UnsafePtr<BufferFrame>>(self.bf) },
            guard: self.guard.downgrade(),
            _marker: PhantomData,
        }
    }

    #[inline]
    pub fn downgrade_shared(self) -> PageSharedGuard<T> {
        PageSharedGuard {
            bf: self.bf,
            guard: self.guard.downgrade_exclusive_to_shared(),
            _marker: PhantomData,
        }
    }

    /// Returns current page id.
    #[inline]
    pub fn page_id(&self) -> PageID {
        self.bf.page_id
    }

    /// Returns mutable page.
    #[inline]
    pub fn page_mut(&mut self) -> &mut T {
        let page = self.bf.page;
        unsafe { &mut *(page as *mut T) }
    }

    /// Returns current buffer frame.
    #[inline]
    pub fn bf(&self) -> &BufferFrame {
        self.bf
    }

    /// Returns mutable buffer frame.
    #[inline]
    pub fn bf_mut(&mut self) -> &mut BufferFrame {
        self.bf
    }

    /// Set next free page.
    #[inline]
    pub fn set_next_free(&mut self, next_free: PageID) {
        self.bf.next_free = next_free;
    }

    #[inline]
    pub fn ctx_and_page_mut(&mut self) -> (&mut FrameContext, &mut T) {
        let bf = &mut self.bf;
        let ctx = bf.ctx.as_mut().unwrap().as_mut();
        let page = bf.page;
        let page = unsafe { &mut *(page as *mut T) };
        (ctx, page)
    }

    /// Returns facade guard.
    #[inline]
    pub fn facade(self, dirty: bool) -> FacadePageGuard<T> {
        if dirty && !self.bf.is_dirty() {
            self.bf.set_dirty(true);
        }
        FacadePageGuard {
            bf: unsafe { mem::transmute::<&mut BufferFrame, UnsafePtr<BufferFrame>>(self.bf) },
            guard: self.guard,
            _marker: PhantomData,
        }
    }

    #[inline]
    pub fn set_dirty(self) {
        if !self.bf.is_dirty() {
            self.bf.set_dirty(true);
        }
    }

    #[inline]
    pub fn is_dirty(&self) -> bool {
        self.bf.is_dirty()
    }
}

unsafe impl<T: 'static> Send for PageExclusiveGuard<T> {}
unsafe impl<T: 'static> Sync for PageExclusiveGuard<T> {}
