use std::any::TypeId;
use std::cell::RefCell;
use std::marker::PhantomData;
use std::ops::Deref;

/// Utility trait to support static lifetime.
/// This is used for component which lifetime goes
/// through entire program, and other threads may access it
/// concurrently.
///
/// Use static lifetime can eliminate performance panalty
/// on reference counter maintainance.
///
/// # Safety
///
/// We must guarantee after the destruction, no thread
/// will access the leaked static reference.
pub unsafe trait StaticLifetime: Sized {
    /// Create a leaked static reference from given instance.
    fn new_static(this: Self) -> &'static Self {
        Box::leak(Box::new(this))
    }

    /// Drop the leaked reference as it's actually owned object.
    ///
    /// # Safety
    ///
    /// This method is marked as unsafe because caller must guarantee
    /// No thread will access this reference after it's dropped.
    /// If multiple objects of static lifetime has dependencies.
    /// The drop order is important.
    unsafe fn drop_static(this: &'static Self) {
        unsafe {
            drop(Box::from_raw(this as *const Self as *mut Self));
        }
    }
}

/// A scoped teardown helper for leaked static references used by tests.
///
/// Registered objects are dropped in reverse registration order when the scope
/// is dropped.
///
/// # Safety Contract
///
/// This helper only centralizes unsafe teardown logic; it does not change the
/// underlying requirement of [`StaticLifetime::drop_static`]. Callers must
/// still guarantee no thread accesses registered references after scope drop.
#[derive(Default)]
pub struct StaticLifetimeScope {
    entries: RefCell<Vec<ScopeEntry>>,
}

impl StaticLifetimeScope {
    #[inline]
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a leaked static reference for scoped teardown.
    ///
    /// The same pointer (for the same concrete type) cannot be registered
    /// twice in one scope.
    #[inline]
    pub fn adopt<'a, T>(&'a self, r: &'static T) -> StaticLifetimeScopeRef<'a, T>
    where
        T: StaticLifetime + 'static,
    {
        let type_id = TypeId::of::<T>();
        let ptr = r as *const T as *const ();
        let mut entries = self.entries.borrow_mut();
        if entries
            .iter()
            .any(|entry| entry.type_id == type_id && entry.ptr == ptr)
        {
            panic!("static lifetime pointer already registered in scope");
        }
        entries.push(ScopeEntry {
            type_id,
            ptr,
            drop_fn: drop_scope_entry::<T>,
        });
        StaticLifetimeScopeRef {
            inner: r,
            _scope: PhantomData,
        }
    }
}

impl Drop for StaticLifetimeScope {
    #[inline]
    fn drop(&mut self) {
        let entries = self.entries.get_mut();
        while let Some(entry) = entries.pop() {
            // SAFETY: each entry is registered via `adopt` with a matching
            // concrete drop function and pointer value.
            unsafe {
                (entry.drop_fn)(entry.ptr);
            }
        }
    }
}

#[derive(Clone, Copy)]
pub struct StaticLifetimeScopeRef<'a, T: StaticLifetime + 'static> {
    inner: &'static T,
    _scope: PhantomData<&'a StaticLifetimeScope>,
}

impl<T: StaticLifetime + 'static> StaticLifetimeScopeRef<'_, T> {
    #[inline]
    pub fn as_static(&self) -> &'static T {
        self.inner
    }
}

impl<T: StaticLifetime + 'static> Deref for StaticLifetimeScopeRef<'_, T> {
    type Target = T;

    #[inline]
    fn deref(&self) -> &Self::Target {
        self.inner
    }
}

struct ScopeEntry {
    type_id: TypeId,
    ptr: *const (),
    drop_fn: unsafe fn(*const ()),
}

unsafe fn drop_scope_entry<T>(ptr: *const ())
where
    T: StaticLifetime + 'static,
{
    // SAFETY: `ptr` is created from a valid `&'static T` in `adopt`,
    // and `drop_scope_entry::<T>` is only paired with entries of the same `T`.
    let r = unsafe { &*(ptr as *const T) };
    // SAFETY: callers of StaticLifetimeScope uphold the same post-drop no-use
    // invariant required by StaticLifetime::drop_static.
    unsafe {
        StaticLifetime::drop_static(r);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};

    struct TestStatic {
        id: u8,
        drops: Arc<Mutex<Vec<u8>>>,
    }

    impl Drop for TestStatic {
        fn drop(&mut self) {
            self.drops.lock().unwrap().push(self.id);
        }
    }

    unsafe impl StaticLifetime for TestStatic {}

    #[test]
    fn test_scope_drops_in_reverse_order() {
        let drops = Arc::new(Mutex::new(vec![]));
        let first = StaticLifetime::new_static(TestStatic {
            id: 1,
            drops: Arc::clone(&drops),
        });
        let second = StaticLifetime::new_static(TestStatic {
            id: 2,
            drops: Arc::clone(&drops),
        });

        {
            let scope = StaticLifetimeScope::new();
            let _first = scope.adopt(first);
            let _second = scope.adopt(second);
        }

        assert_eq!(*drops.lock().unwrap(), vec![2, 1]);
    }

    #[test]
    #[should_panic(expected = "static lifetime pointer already registered in scope")]
    fn test_scope_panics_on_duplicate_registration() {
        let drops = Arc::new(Mutex::new(vec![]));
        let value = StaticLifetime::new_static(TestStatic {
            id: 7,
            drops: Arc::clone(&drops),
        });

        let scope = StaticLifetimeScope::new();
        let _ref1 = scope.adopt(value);
        let _ref2 = scope.adopt(value);
    }

    #[test]
    fn test_scope_ref_deref_and_as_static() {
        let drops = Arc::new(Mutex::new(vec![]));
        let value = StaticLifetime::new_static(TestStatic {
            id: 5,
            drops: Arc::clone(&drops),
        });

        {
            let scope = StaticLifetimeScope::new();
            let value_ref = scope.adopt(value);
            assert_eq!(value_ref.id, 5);
            assert!(std::ptr::eq(value_ref.as_static(), value));
        }

        assert_eq!(*drops.lock().unwrap(), vec![5]);
    }
}
