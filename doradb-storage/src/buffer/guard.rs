use crate::buffer::frame::BufferFrame;
use crate::buffer::page::PageID;
use crate::error::{
    Validation,
    Validation::{Invalid, Valid},
};
use crate::latch::GuardState;
use crate::latch::HybridGuard;
use crate::trx::undo::UndoMap;
use either::Either;
use std::cell::UnsafeCell;
use std::marker::PhantomData;
use std::mem;

// facade of lock guard, which encapsulate all three lock modes.
pub struct PageGuard<'a, T> {
    bf: &'a UnsafeCell<BufferFrame>,
    guard: HybridGuard<'a>,
    _marker: PhantomData<&'a T>,
}

impl<'a, T> PageGuard<'a, T> {
    #[inline]
    pub fn new(bf: &'a UnsafeCell<BufferFrame>, guard: HybridGuard<'a>) -> Self {
        PageGuard {
            bf,
            guard,
            _marker: PhantomData,
        }
    }

    /// Try exclusive lock will do additional version check after the lock acquisition to ensure
    /// during the optimistic lock and shared lock, there is no change on protected object.
    /// If lock acquisition fails, refresh version of the optimistic lock, so next acquisition
    /// may succeed.
    #[inline]
    pub fn try_exclusive_either(
        mut self,
    ) -> Either<PageExclusiveGuard<'a, T>, PageOptimisticGuard<'a, T>> {
        match self.try_exclusive() {
            Valid(()) => Either::Left(PageExclusiveGuard {
                bf: unsafe { &mut *self.bf.get() },
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
    pub fn block_until_exclusive(self) -> PageExclusiveGuard<'a, T> {
        match self.guard.state {
            GuardState::Optimistic => {
                let guard = self.guard.block_until_exclusive();
                PageExclusiveGuard {
                    bf: unsafe { &mut *self.bf.get() },
                    guard,
                    _marker: PhantomData,
                }
            }
            GuardState::Shared => panic!("block until exclusive by shared lock is not allowed"),
            GuardState::Exclusive => PageExclusiveGuard {
                bf: unsafe { &mut *self.bf.get() },
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
    pub fn try_shared_either(
        mut self,
    ) -> Either<PageSharedGuard<'a, T>, PageOptimisticGuard<'a, T>> {
        match self.try_shared() {
            Valid(()) => Either::Left(PageSharedGuard {
                bf: unsafe { mem::transmute(self.bf) },
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
    pub fn block_until_shared(self) -> PageSharedGuard<'a, T> {
        match self.guard.state {
            GuardState::Optimistic => {
                let guard = self.guard.block_until_shared();
                PageSharedGuard {
                    bf: unsafe { &*self.bf.get() },
                    guard,
                    _marker: PhantomData,
                }
            }
            GuardState::Shared => PageSharedGuard {
                bf: unsafe { &*self.bf.get() },
                guard: self.guard,
                _marker: PhantomData,
            },
            GuardState::Exclusive => panic!("block until exclusive by shared lock is not allowed"),
        }
    }

    #[inline]
    pub fn downgrade(self) -> PageOptimisticGuard<'a, T> {
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

    #[inline]
    pub unsafe fn as_shared(&self) -> &PageSharedGuard<'a, T> {
        mem::transmute(self)
    }

    #[inline]
    pub unsafe fn as_exclusive(&mut self) -> &mut PageExclusiveGuard<'a, T> {
        mem::transmute(self)
    }

    /// Returns page with optimistic read.
    /// All values must be validated before use.
    #[inline]
    pub unsafe fn page_unchecked(&self) -> &T {
        let bf = self.bf.get();
        let page = (*bf).page;
        &*(page as *mut T)
    }

    #[inline]
    pub fn validate(&self) -> Validation<()> {
        if self.guard.validate() {
            Valid(())
        } else {
            Invalid
        }
    }
}

pub struct PageOptimisticGuard<'a, T> {
    bf: &'a UnsafeCell<BufferFrame>,
    guard: HybridGuard<'a>,
    _marker: PhantomData<&'a T>,
}

impl<'a, T> PageOptimisticGuard<'a, T> {
    #[inline]
    pub unsafe fn page_id(&self) -> PageID {
        (*self.bf.get()).page_id
    }

    #[inline]
    pub fn try_shared(mut self) -> Validation<PageSharedGuard<'a, T>> {
        self.guard.try_shared().map(|_| PageSharedGuard {
            bf: unsafe { &*self.bf.get() },
            guard: self.guard,
            _marker: PhantomData,
        })
    }

    #[inline]
    pub fn block_until_shared(self) -> PageSharedGuard<'a, T> {
        let guard = self.guard.block_until_shared();
        PageSharedGuard {
            bf: unsafe { &*self.bf.get() },
            guard,
            _marker: PhantomData,
        }
    }

    #[inline]
    pub fn try_exclusive(mut self) -> Validation<PageExclusiveGuard<'a, T>> {
        self.guard.try_exclusive().map(|_| PageExclusiveGuard {
            bf: unsafe { &mut *self.bf.get() },
            guard: self.guard,
            _marker: PhantomData,
        })
    }

    #[inline]
    pub fn block_until_exclusive(self) -> PageExclusiveGuard<'a, T> {
        let guard = self.guard.block_until_exclusive();
        PageExclusiveGuard {
            bf: unsafe { &mut *self.bf.get() },
            guard,
            _marker: PhantomData,
        }
    }

    /// Returns page with optimistic read.
    /// All values must be validated before use.
    #[inline]
    pub unsafe fn page_unchecked(&self) -> &T {
        let bf = self.bf.get();
        let page = (*bf).page;
        &*(page as *mut T)
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
    pub fn copy_keepalive(&self) -> PageGuard<'a, T> {
        let guard = self.guard.optimistic_clone().expect("copy optimistic lock");
        PageGuard {
            bf: self.bf,
            guard,
            _marker: PhantomData,
        }
    }

    /// Returns facade guard.
    #[inline]
    pub fn facade(self) -> PageGuard<'a, T> {
        PageGuard {
            bf: self.bf,
            guard: self.guard,
            _marker: PhantomData,
        }
    }
}

pub struct PageSharedGuard<'a, T> {
    bf: &'a BufferFrame,
    guard: HybridGuard<'a>,
    _marker: PhantomData<&'a T>,
}

impl<'a, T> PageSharedGuard<'a, T> {
    /// Convert a page shared guard to optimistic guard
    /// with long lifetime.
    #[inline]
    pub fn downgrade(self) -> PageOptimisticGuard<'a, T> {
        PageOptimisticGuard {
            bf: unsafe { mem::transmute(self.bf) },
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

    /// Returns shared page.
    #[inline]
    pub fn page(&self) -> &T {
        let page = self.bf.page;
        unsafe { &*(page as *mut T) }
    }

    #[inline]
    pub fn undo_map_and_page(&self) -> (&UndoMap, &T) {
        let bf = self.bf();
        let undo_map = bf.undo_map.as_ref().unwrap();
        let page = bf.page;
        let page = unsafe { &*(page as *mut T) };
        (undo_map, page)
    }

    /// Returns facade guard.
    #[inline]
    pub fn facade(self) -> PageGuard<'a, T> {
        PageGuard {
            bf: unsafe { mem::transmute(self.bf) },
            guard: self.guard,
            _marker: PhantomData,
        }
    }
}

pub struct PageExclusiveGuard<'a, T> {
    bf: &'a mut BufferFrame,
    guard: HybridGuard<'a>,
    _marker: PhantomData<&'a mut T>,
}

impl<'a, T> PageExclusiveGuard<'a, T> {
    /// Convert a page exclusive guard to optimistic guard
    /// with long lifetime.
    #[inline]
    pub fn downgrade(self) -> PageOptimisticGuard<'a, T> {
        PageOptimisticGuard {
            bf: unsafe { mem::transmute(self.bf) },
            guard: self.guard.downgrade(),
            _marker: PhantomData,
        }
    }

    /// Returns current page id.
    #[inline]
    pub fn page_id(&self) -> PageID {
        self.bf.page_id
    }

    /// Returns current page.
    #[inline]
    pub fn page(&self) -> &T {
        let page = self.bf.page;
        unsafe { &*(page as *mut T) }
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
        &self.bf
    }

    #[inline]
    pub fn undo_map_and_page(&self) -> (&UndoMap, &T) {
        let bf = self.bf();
        let undo_map = bf.undo_map.as_ref().unwrap();
        let page = bf.page;
        let page = unsafe { &*(page as *mut T) };
        (undo_map, page)
    }

    /// Returns mutable buffer frame.
    #[inline]
    pub fn bf_mut(&mut self) -> &mut BufferFrame {
        &mut self.bf
    }

    /// Set next free page.
    #[inline]
    pub fn set_next_free(&mut self, next_free: PageID) {
        self.bf.next_free = next_free;
    }

    /// Returns facade guard.
    #[inline]
    pub fn facade(self) -> PageGuard<'a, T> {
        PageGuard {
            bf: unsafe { mem::transmute(self.bf) },
            guard: self.guard,
            _marker: PhantomData,
        }
    }
}
