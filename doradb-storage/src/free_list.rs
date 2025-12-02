use crate::notify::EventNotifyOnDrop;
use event_listener::{listener, Listener};
use parking_lot::Mutex;

/// Simple FreeList backed by linked list.
pub struct FreeList<T> {
    data: Mutex<(Vec<T>, usize)>,
    max_size: usize,
    factory: Box<dyn Fn() -> T + Send + Sync + 'static>,
    ev: EventNotifyOnDrop,
}

impl<T> FreeList<T> {
    /// Create a new free list with initial size, maximum size
    /// and factory function.
    #[inline]
    pub fn new<F: Fn() -> T + Send + Sync + 'static>(
        init_size: usize,
        max_size: usize,
        factory: F,
    ) -> Self {
        debug_assert!(max_size > 0);
        debug_assert!(init_size <= max_size);
        let free_size = max_size - init_size;
        let data: Vec<_> = (0..init_size).map(|_| factory()).collect();
        FreeList {
            data: Mutex::new((data, free_size)),
            max_size,
            factory: Box::new(factory),
            ev: EventNotifyOnDrop::new(),
        }
    }
    /// Push data into free list.
    #[inline]
    pub fn push(&self, data: T) {
        let mut g = self.data.lock();
        if g.0.len() >= self.max_size {
            // drop element if full
            return;
        }
        if g.0.is_empty() {
            self.ev.notify(1);
        }
        g.0.push(data);
    }

    /// Try pop data from free list.
    #[inline]
    pub fn try_pop(&self, create_if_not_exists: bool) -> Option<T> {
        let mut g = self.data.lock();
        if !g.0.is_empty() {
            return g.0.pop();
        }
        if !create_if_not_exists {
            return None;
        }
        if g.1 > 0 {
            // construct free element.
            g.1 -= 1;
            drop(g); // here we can release the lock because number is already decreased.
            let res = (self.factory)();
            return Some(res);
        }
        None
    }

    /// Pop an element from free list, block if there is no element.
    #[inline]
    pub fn pop(&self, create_if_not_exists: bool) -> T {
        loop {
            let mut g = self.data.lock();
            if !g.0.is_empty() {
                return g.0.pop().unwrap();
            }
            if create_if_not_exists && g.1 > 0 {
                g.1 -= 1;
                drop(g);
                let res = (self.factory)();
                return res;
            }
            listener!(self.ev => listener);
            // release lock once listener is created.
            drop(g);
            listener.wait();
        }
    }

    /// Pop an element from free list in async way.
    #[allow(clippy::await_holding_lock)]
    #[inline]
    pub async fn pop_async(&self, create_if_not_exists: bool) -> T {
        loop {
            let mut g = self.data.lock();
            if !g.0.is_empty() {
                return g.0.pop().unwrap();
            }
            if create_if_not_exists && g.1 > 0 {
                g.1 -= 1;
                drop(g);
                let res = (self.factory)();
                return res;
            }
            listener!(self.ev => listener);
            // release lock once listener is created.
            drop(g);
            listener.await;
        }
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
        let list = FreeList::new(0, 10, || 42i32);
        assert!(list.try_pop(false).is_none());
        assert_eq!(list.try_pop(true), Some(42));

        list.push(1);
        list.push(2);
        assert_eq!(list.try_pop(false), Some(2));
        assert_eq!(list.pop(false), 1);
        assert!(list.try_pop(false).is_none());
    }

    #[test]
    fn test_free_list_concurrent() {
        let list = Arc::new(FreeList::<i32>::new(0, 10, || 42i32));
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
        while let Some(val) = list.try_pop(false) {
            results.push(val);
        }
        results.sort();
        assert_eq!(results, (0..10).collect::<Vec<_>>());
    }

    #[test]
    fn test_free_list_blocking() {
        let free_list = Arc::new(FreeList::<i32>::new(0, 10, || 42i32));
        let handle = {
            let free_list = Arc::clone(&free_list);
            thread::spawn(move || {
                let elem = free_list.pop(false);
                println!("elem is {:?}", elem);
            })
        };
        thread::sleep(Duration::from_millis(100));
        free_list.push(42);
        handle.join().unwrap();
    }

    #[test]
    fn test_free_list_async() {
        smol::block_on(async {
            let free_list = Arc::new(FreeList::<i32>::new(0, 1, || 42i32));
            {
                let free_list = Arc::clone(&free_list);
                smol::spawn(async move {
                    let elem = free_list.pop_async(false).await;
                    println!("elem is {:?}", elem);
                })
                .detach();
            }

            smol::Timer::after(Duration::from_millis(100)).await;
            free_list.push(42);
            smol::Timer::after(Duration::from_millis(100)).await;
        })
    }
}
