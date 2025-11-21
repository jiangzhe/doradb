use crate::notify::EventNotifyOnDrop;
use event_listener::{listener, Listener};
use std::ops::{Deref, DerefMut};
use std::sync::atomic::{AtomicPtr, Ordering};

/// Simple FreeList backed by linked list.
pub struct FreeList<T> {
    head: AtomicPtr<FreeElem<T>>,
    ev: EventNotifyOnDrop,
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
                self.ev.notify(1);
                return;
            }
        }
    }

    /// Pop data from free list.
    #[inline]
    pub fn pop(&self) -> Option<T> {
        self.pop_elem().map(|elem| elem.into_inner())
    }

    /// Pop an element from free list.
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
                self.ev.notify(1);
                return Some(elem);
            }
        }
    }

    /// Pop an element from free list and block if empty.
    #[inline]
    pub fn pop_elem_blocking(&self) -> Box<FreeElem<T>> {
        loop {
            if let Some(elem) = self.pop_elem() {
                return elem;
            }
            listener!(self.ev => listener);
            if let Some(elem) = self.pop_elem() {
                return elem;
            }
            listener.wait();
        }
    }

    /// Pop an element in async way, yield if list is empty.
    #[inline]
    pub async fn pop_elem_async(&self) -> Box<FreeElem<T>> {
        loop {
            if let Some(elem) = self.pop_elem() {
                return elem;
            }
            listener!(self.ev => listener);
            if let Some(elem) = self.pop_elem() {
                return elem;
            }
            listener.await;
        }
    }
}

impl<T> Default for FreeList<T> {
    #[inline]
    fn default() -> Self {
        FreeList {
            head: AtomicPtr::new(std::ptr::null_mut()),
            ev: EventNotifyOnDrop::new(),
        }
    }
}

impl<T> Drop for FreeList<T> {
    #[inline]
    fn drop(&mut self) {
        while let Some(elem) = self.pop() {
            drop(elem);
        }
    }
}

impl<T> FromIterator<T> for FreeList<T> {
    #[inline]
    fn from_iter<I: IntoIterator<Item = T>>(iter: I) -> Self {
        let res = FreeList::default();
        for v in iter {
            res.push(v);
        }
        res
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::thread;
    use std::time::Duration;

    #[test]
    fn test_free_list_basic() {
        let list = FreeList::<i32>::default();
        assert!(list.pop().is_none());

        list.push(1);
        list.push(2);
        assert_eq!(list.pop(), Some(2));
        assert_eq!(list.pop(), Some(1));
        assert!(list.pop().is_none());
    }

    #[test]
    fn test_free_list_concurrent() {
        let list = Arc::new(FreeList::<i32>::default());
        let mut handles = vec![];

        for i in 0..10 {
            let list = list.clone();
            handles.push(thread::spawn(move || {
                list.push(i);
            }));
        }

        for handle in handles {
            handle.join().unwrap();
        }

        let mut results = vec![];
        while let Some(val) = list.pop() {
            results.push(val);
        }
        results.sort();
        assert_eq!(results, (0..10).collect::<Vec<_>>());
    }

    #[test]
    fn test_free_list_with_factory() {
        let factory = || 42;
        let list = FreeListWithFactory::new(factory);
        assert_eq!(list.pop_or_new(), 42);

        let list = FreeListWithFactory::prefill(3, factory);
        assert_eq!(list.pop().unwrap(), 42);
        assert_eq!(list.pop().unwrap(), 42);
        assert_eq!(list.pop().unwrap(), 42);
        assert_eq!(list.pop_or_new(), 42);
    }

    #[test]
    fn test_free_elem() {
        let elem = FreeElem::new(100);
        assert_eq!(*elem, 100);
        assert_eq!(elem.into_inner(), 100);
    }

    #[test]
    fn test_free_list_blocking() {
        let free_list = Arc::new(FreeList::<i32>::default());
        let handle = {
            let free_list = Arc::clone(&free_list);
            thread::spawn(move || {
                let elem = free_list.pop_elem_blocking();
                println!("elem is {:?}", **elem);
            })
        };
        thread::sleep(Duration::from_millis(100));
        free_list.push(42);
        handle.join().unwrap();
    }

    #[test]
    fn test_free_list_async() {
        smol::block_on(async {
            let free_list = Arc::new(FreeList::<i32>::default());
            {
                let free_list = Arc::clone(&free_list);
                smol::spawn(async move {
                    let elem = free_list.pop_elem_async().await;
                    println!("elem is {:?}", **elem);
                })
                .detach();
            }

            smol::Timer::after(Duration::from_millis(100)).await;
            free_list.push(42);
            smol::Timer::after(Duration::from_millis(100)).await;
        })
    }
}
