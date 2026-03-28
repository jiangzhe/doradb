/// Type to allow send raw pointer across multiple threads.
#[repr(transparent)]
pub struct UnsafePtr<T>(pub(crate) *mut T);

impl<T> Clone for UnsafePtr<T> {
    #[inline]
    fn clone(&self) -> Self {
        UnsafePtr(self.0)
    }
}

// SAFETY: `UnsafePtr<T>` only transports the raw pointer between threads; the
// caller remains responsible for the pointee lifetime and synchronization.
unsafe impl<T: Send> Send for UnsafePtr<T> {}
