use parking_lot::Mutex;

/// Simple FreeList backed by linked list.
pub(crate) struct FreeList<T> {
    data: Mutex<Vec<T>>,
    max_size: usize,
    factory: Box<dyn Fn() -> T + Send + Sync + 'static>,
}

impl<T> FreeList<T> {
    /// Create a new free list with initial size, maximum size
    /// and factory function.
    #[inline]
    pub(crate) fn new<F: Fn() -> T + Send + Sync + 'static>(
        init_size: usize,
        max_size: usize,
        factory: F,
    ) -> Self {
        debug_assert!(max_size > 0);
        debug_assert!(init_size <= max_size);
        let data: Vec<_> = (0..init_size).map(|_| factory()).collect();
        FreeList {
            data: Mutex::new(data),
            max_size,
            factory: Box::new(factory),
        }
    }

    /// Push data into free list.
    #[inline]
    pub(crate) fn push(&self, data: T) {
        let mut g = self.data.lock();
        if g.len() >= self.max_size {
            return;
        }
        g.push(data);
    }

    /// Push a batch of data into free list.
    #[inline]
    pub(crate) fn push_batch(&self, mut data: Vec<T>) {
        if data.is_empty() {
            return;
        }
        let mut g = self.data.lock();
        let remaining = self.max_size - g.len();
        let retain_count = remaining.min(data.len());
        g.extend(data.drain(..retain_count));
    }

    /// Pop an element from free list, or create a new one when empty.
    #[inline]
    #[cfg_attr(not(test), expect(dead_code, reason = "reserved single-element reuse"))]
    pub(crate) fn pop(&self) -> T {
        let data = self.data.lock().pop();
        data.unwrap_or_else(|| (self.factory)())
    }

    /// Pop a batch of elements from free list, or create missing ones when empty.
    #[inline]
    pub(crate) fn pop_batch(&self, count: usize) -> Vec<T> {
        let mut data = Vec::with_capacity(count);
        {
            let mut g = self.data.lock();
            let reuse_count = count.min(g.len());
            for _ in 0..reuse_count {
                if let Some(item) = g.pop() {
                    data.push(item);
                }
            }
        }
        for _ in data.len()..count {
            data.push((self.factory)());
        }
        data
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::thread;

    #[test]
    fn test_free_list_basic() {
        let next = Arc::new(AtomicUsize::new(0));
        let factory_next = Arc::clone(&next);
        let list = FreeList::new(2, 10, move || factory_next.fetch_add(1, Ordering::SeqCst));

        assert_eq!(next.load(Ordering::SeqCst), 2);
        assert_eq!(list.pop(), 1);
        assert_eq!(list.pop(), 0);
        assert_eq!(list.pop(), 2);
        assert_eq!(next.load(Ordering::SeqCst), 3);
    }

    #[test]
    fn test_free_list_push_retains_up_to_max_size() {
        let next = Arc::new(AtomicUsize::new(100));
        let factory_next = Arc::clone(&next);
        let list = FreeList::new(0, 2, move || factory_next.fetch_add(1, Ordering::SeqCst));

        list.push(1);
        list.push(2);
        list.push(3);

        assert_eq!(list.pop(), 2);
        assert_eq!(list.pop(), 1);
        assert_eq!(list.pop(), 100);
    }

    #[test]
    fn test_free_list_push_recycles_elements() {
        let list = FreeList::new(0, 10, || 42i32);

        list.push(1);
        list.push(2);

        assert_eq!(list.pop(), 2);
        assert_eq!(list.pop(), 1);
        assert_eq!(list.pop(), 42);
    }

    #[test]
    fn test_free_list_pop_batch_reuses_and_creates_missing_elements() {
        let next = Arc::new(AtomicUsize::new(100));
        let factory_next = Arc::clone(&next);
        let list = FreeList::new(0, 10, move || factory_next.fetch_add(1, Ordering::SeqCst));

        list.push(1);
        list.push(2);

        let batch = list.pop_batch(4);

        assert_eq!(batch, vec![2, 1, 100, 101]);
        assert_eq!(next.load(Ordering::SeqCst), 102);
    }

    #[test]
    fn test_free_list_pop_batch_zero_count() {
        let next = Arc::new(AtomicUsize::new(100));
        let factory_next = Arc::clone(&next);
        let list = FreeList::new(0, 10, move || factory_next.fetch_add(1, Ordering::SeqCst));

        assert!(list.pop_batch(0).is_empty());
        assert_eq!(next.load(Ordering::SeqCst), 100);
    }

    #[test]
    fn test_free_list_push_batch_retains_up_to_max_size() {
        let next = Arc::new(AtomicUsize::new(100));
        let factory_next = Arc::clone(&next);
        let list = FreeList::new(0, 2, move || factory_next.fetch_add(1, Ordering::SeqCst));

        list.push(1);
        list.push_batch(vec![2, 3, 4]);

        assert_eq!(list.pop_batch(3), vec![2, 1, 100]);
    }

    #[test]
    fn test_free_list_concurrent() {
        let free_list = Arc::new(FreeList::<i32>::new(0, 10, || 42i32));
        let mut handles = vec![];

        for i in 0..10 {
            let free_list = Arc::clone(&free_list);
            handles.push(thread::spawn(move || {
                free_list.push(i);
            }));
        }

        for handle in handles {
            handle.join().unwrap();
        }

        let mut results = (0..10).map(|_| free_list.pop()).collect::<Vec<_>>();
        results.sort_unstable();
        assert_eq!(results, (0..10).collect::<Vec<_>>());
    }
}
