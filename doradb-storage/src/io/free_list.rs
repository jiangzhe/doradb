use std::ops::{Deref, DerefMut};
use std::sync::atomic::{AtomicPtr, Ordering};

pub struct FreeList<T> {
    head: AtomicPtr<FreeElem<T>>,
}

impl<T> FreeList<T> {
    /// Push data into free list.
    #[inline]
    pub fn push(&self, data: T) {
        let elem = FreeElem::new(data);
        self.push_elem(Box::new(elem));
    }

    /// Push an element into free list. The element may be borrowed from free list.
    #[inline]
    pub fn push_elem(&self, elem: Box<FreeElem<T>>) {
        // Leak the data as we reset next pointer to head of the free list.
        // So we assume original next pointer points to nothing.
        debug_assert!(elem.next.load(Ordering::Relaxed).is_null());
        let new = Box::into_raw(elem);
        loop {
            let ptr = self.head.load(Ordering::Relaxed);
            unsafe {
                (*new).next.store(ptr, Ordering::Relaxed);
            }
            if self
                .head
                .compare_exchange_weak(ptr, new, Ordering::SeqCst, Ordering::Relaxed)
                .is_ok()
            {
                return;
            }
        }
    }

    /// Pop data from free list.
    #[inline]
    pub fn pop(&self) -> Option<T> {
        self.pop_elem().map(|elem| elem.into_inner())
    }

    /// Pop an an element from free list.
    #[inline]
    pub fn pop_elem(&self) -> Option<Box<FreeElem<T>>> {
        loop {
            let ptr = self.head.load(Ordering::Relaxed);
            if ptr.is_null() {
                return None;
            }
            let next = unsafe { (*ptr).next.load(Ordering::Relaxed) };
            if self
                .head
                .compare_exchange_weak(ptr, next, Ordering::SeqCst, Ordering::Relaxed)
                .is_ok()
            {
                let elem = unsafe { Box::from_raw(ptr) };
                elem.next.store(std::ptr::null_mut(), Ordering::Relaxed);
                return Some(elem);
            }
        }
    }
}

impl<T> Default for FreeList<T> {
    #[inline]
    fn default() -> Self {
        FreeList {
            head: AtomicPtr::new(std::ptr::null_mut()),
        }
    }
}

/// Convenient wrapper to combine free list and factory function.
/// So user can just fetch data only from cache layer.
pub struct FreeListWithFactory<T> {
    free_list: FreeList<T>,
    factory: Box<dyn Fn() -> T + Send + Sync + 'static>,
}

impl<T> Deref for FreeListWithFactory<T> {
    type Target = FreeList<T>;
    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.free_list
    }
}

impl<T> FreeListWithFactory<T> {
    /// Create a new free list with factory.
    #[inline]
    pub fn new<F>(factory: F) -> Self
    where
        F: Fn() -> T,
        F: Send + Sync + 'static,
    {
        let free_list = FreeList::default();
        FreeListWithFactory {
            free_list,
            factory: Box::new(factory),
        }
    }

    /// Create a new free list and prefill given number of data.
    #[inline]
    pub fn prefill<F>(len: usize, factory: F) -> Self
    where
        F: Fn() -> T,
        F: Send + Sync + 'static,
    {
        let this = Self::new(factory);
        for _ in 0..len {
            let data = this.new_data();
            this.push(data);
        }
        this
    }

    /// Create a new data.
    #[inline]
    pub fn new_data(&self) -> T {
        (self.factory)()
    }

    /// Pop single data from free list, if not exists create a new one.
    #[inline]
    pub fn pop_or_new(&self) -> T {
        self.pop().unwrap_or_else(|| self.new_data())
    }

    /// Pop cachable element from free list, if not exists create a new one.
    #[inline]
    pub fn pop_elem_or_new(&self) -> Box<FreeElem<T>> {
        self.pop_elem()
            .unwrap_or_else(|| Box::new(FreeElem::new(self.new_data())))
    }
}

/// Simple wrapper on element in free list.
/// The push and pop operation requires additional allocation.
/// We can embed this type directly into other data structure to avoid
/// performance degradation.
pub struct FreeElem<T> {
    data: T,
    next: AtomicPtr<FreeElem<T>>,
}

impl<T> Deref for FreeElem<T> {
    type Target = T;
    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.data
    }
}

impl<T> DerefMut for FreeElem<T> {
    #[inline]
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.data
    }
}

impl<T> FreeElem<T> {
    #[inline]
    pub fn new(data: T) -> Self {
        FreeElem {
            data,
            next: AtomicPtr::new(std::ptr::null_mut()),
        }
    }

    #[inline]
    pub fn into_inner(self) -> T {
        self.data
    }
}
